# Dispatch Task Leak on Disconnect

**Status:** identified, repro test added, fix not yet implemented.
**Severity:** high — production observed 14.4 GB RSS in 6 hours in a downstream application.
**Repro test:** [`tests/test_dispatch_task_leak_on_disconnect.py`](../tests/test_dispatch_task_leak_on_disconnect.py) — currently FAILS, must PASS after fix.

## TL;DR

Every incoming `MSG_REQUEST` / `MSG_ASYNC_REQUEST` schedules a `_dispatch_request_async` coroutine via `asyncio.run_coroutine_threadsafe(...)` on the connection's loop. The handler inside that coroutine may `await` something that never completes (e.g. a netref reverse-call to a peer that has just died). When that happens:

* the `Task` is parked forever on the inner `await`,
* `Connection.close()` / `aclose()` does **not** cancel it,
* nothing in `protocol.py` keeps a reference to the task — it stays alive only via `asyncio._all_tasks` weakrefs,
* every subsequent stuck dispatch adds another permanent task.

In a long-lived process with intermittent peer disconnects (a downstream application with several clients and occasional restarts) this monotonically grows the task set. Each Task drags along its Future, Context, RLock, Condition, Timeout, AsyncResult, FutureIter, cell, frame, deque, coroutine — roughly **5 KB of Python heap per stuck dispatch**. Live dump from a leaking process: 869 012 stuck dispatch tasks, 4.3 GB Python heap, 14.4 GB RSS counting native buffers.

## Where (exact lines)

**File:** `rpyc/core/protocol.py`

```python
# ~ line 1822 — _dispatch reads a MSG_REQUEST and schedules the handler
asyncio.run_coroutine_threadsafe(
    self._dispatch_request_async(seq, args),
    self._asyncio_loop,
)
# Returned concurrent.futures.Future is discarded.
# Connection has no reference to the resulting Task.
```

```python
# ~ line 1723 — inside _dispatch_request_async the actual await happens
res = await handler_func(self, *args)
# If handler_func parks (dead peer, netref, race), this Task never finishes.
```

`Connection.close()` flips `self._closed = True` and runs `_cleanup`, but it does **not** walk the active dispatch tasks for this connection and cancel them. The connection object itself can be GC'd, but each stuck Task still has a reference to the bound `self._dispatch_request_async` method, which keeps the Connection alive — so neither side releases.

## Live evidence (prod incident)

* `gc.get_objects()` from a gdb-injected `PyRun_SimpleString` showed:
  * `asyncio.Task count: 869 012`
  * `asyncio.Future count: 869 016`
  * Top types by size: `deque` 660 MB, `function` 559 MB, `coroutine` 542 MB, `cell` 417 MB, `Future` 167 MB, `Task` 167 MB, `RLock` 56 MB, `AsyncResult` 70 MB.
  * **Every** dispatch task was parked at `protocol.py:1723` with `wait_for=<Future pending cb=[Task.task_wakeup()]>` and outer callback `_chain_future._call_set_state` (i.e. the wrapper from `run_coroutine_threadsafe`).
* The `_request_callbacks` dict (outgoing requests, `{seq: AsyncResult}`) was 41 MB at the time — a separate but related growth path.

## Why the previous fix (`b8f20e6` PID-namespaced `id_pack` seq) did not cover this

That earlier fix closed cross-process `id_pack` collisions which produced ~10 GB in 6 minutes. The current bug is independent — it grows **after** that fix is applied (rate measured at 700 MB/min under reconnection storm, drops to ~140 KB/min at idle). The dump confirmed: `_dispatch_request_async` accumulation is the dominant reservoir, not `id_pack` collisions.

## Reproducer

`tests/test_dispatch_task_leak_on_disconnect.py`:

```
AssertionError: 10 != 0 :
LEAK: 10 dispatch tasks still pending after Connection.close()/_closed=True.
They will live as long as the event loop runs and accumulate every time
a peer disconnects mid-handler.
```

The test:
1. Builds a `Connection` with a stub channel.
2. Replaces `HANDLE_ASYNC_CALL` with a handler that `await`s a Future no one will ever set (mimics dead peer / never-returning reverse RPC).
3. Schedules N (=10) dispatch tasks via `loop.create_task(conn._dispatch_request_async(seq, raw_args))`.
4. After all tasks are parked, sets `conn._closed = True` and calls `channel.close()`.
5. Yields to the loop with several `await asyncio.sleep` ticks.
6. Asserts that **zero** dispatch tasks remain pending for that connection.

Today this assertion fails — all 10 tasks are still in `asyncio.all_tasks()`.

## Fix space (any of these would make the test pass)

### Option 1 — Track and cancel
```python
# Connection.__init__:
self._dispatch_tasks: set[asyncio.Task] = set()

# In _dispatch (around the run_coroutine_threadsafe call):
fut = asyncio.run_coroutine_threadsafe(coro, self._asyncio_loop)
# Bridge the concurrent.futures.Future back to the asyncio.Task on the loop:
def _track(loop_task_holder):
    # the run_coroutine_threadsafe Future itself does not yield the Task,
    # but we can wrap the coroutine: see option 1b below.
    ...
```
Cleaner variant — schedule via `loop.call_soon_threadsafe(loop.create_task, coro)` and add a done-callback that removes from the set; in `close()` cancel everything in the set.

### Option 2 — Timeout on the inner await
```python
# inside _dispatch_request_async, replace
res = await handler_func(self, *args)
# with
timeout = self._config.get("async_dispatch_timeout", 60.0)
res = await asyncio.wait_for(handler_func(self, *args), timeout=timeout)
```
Simple; requires picking a sensible default. Default of 60 s would have prevented the production incident (no real handler legitimately takes that long) at the cost of breaking arbitrarily long async user RPCs unless the user knows to bump the config.

### Option 3 — EOF-driven cancellation
When the underlying channel observes EOF on read (the existing `_serve` loop already detects this and calls `self.close()`), additionally walk the `_dispatch_tasks` set (option 1 must land first) and `task.cancel()` each one. This is the most surgical: nothing changes for healthy traffic; only the disconnect path frees stuck tasks.

Recommended combination: **option 1 + option 3** (track + cancel-on-EOF). Option 2 is a useful additional safety net but should not be the only line of defence.

## Related code that already partially defends

* `_send_async_result_safe` (~line 1645) — added earlier — swallows `EOFError` from `_send` so the coroutine doesn't propagate the error out into a discarded Future. This protects against log spam from unawaited-coroutine warnings, but does **not** stop accumulation when the handler itself awaits forever (the coroutine never reaches the final `_send`).

## Test invocation

```bash
python3 -m pytest tests/test_dispatch_task_leak_on_disconnect.py -xvs
```

Expected before fix: FAIL (the assertion above).
Expected after fix: PASS.
