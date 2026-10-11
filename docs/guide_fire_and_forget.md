# Fire and Forget Guide

## Overview

The `fire_and_forget_call()`, `fire_and_forget_async()` and `fire_and_forget()` utilities allow you to execute async operations in the background without blocking, with proper timeout handling and optional callbacks.

| | takes | callbacks | use when |
|---|---|---|---|
| `fire_and_forget_call()` *(1.1+)* | a factory | `async def` | **remote calls** — `timeout=` covers the whole call |
| `fire_and_forget_async()` | an awaitable | `async def` | local coroutines, or an awaitable you already hold |
| `fire_and_forget()` | an awaitable | plain `def` | the callback is pure CPU work and cannot `await` |

**For a remote call, use `fire_and_forget_call()` with
`netref.async_call()`** — see [Remote Calls](#remote-calls) right below for
why the obvious form `fire_and_forget_async(conn.root.method())` is not safe.

**Otherwise, reach for `fire_and_forget_async()` by default.** A sync callback cannot
`await`, so it cannot do the one thing callbacks usually need to do: talk to
something. If you find yourself calling `asyncio.create_task()` from inside a
sync callback, you wanted `fire_and_forget_async()`. The examples below use
`fire_and_forget()` where the callback is trivial; swap in
`fire_and_forget_async()` and `async def` callbacks as soon as it is not.

**Key Features:**
- Non-blocking execution of async operations
- Timeout support with local enforcement
- Success and error callbacks
- Works with both local coroutines and cross-process async RPC
- Handles hung/dead connections gracefully

**Critical Requirements:**
- Must be called from within a running event loop (async context)
- For cross-process RPC: **AsyncioServer is REQUIRED**
- All operations occur in the main application event loop (no additional loops/threads)

---

## Remote Calls

### The hazard: the call runs before the helper does

```python
# Looks bounded by timeout=5.0 -- it is not
task = fire_and_forget_async(conn.root.get_last_response(), timeout=5.0)
```

Python evaluates arguments before entering the callee. `conn.root.get_last_response`
is an attribute lookup on a netref, and **every netref attribute lookup is a
synchronous network round-trip** (`HANDLE_GETATTR`). It runs right there, on
your event loop, before the background task exists and outside `timeout`.
While it waits, the loop serves nothing else — not other coroutines, not even
its own timers — until the peer answers or `sync_request_timeout` (30 s by
default) expires. The peer does not have to be dead: a peer that is merely
slow to answer is enough.

### The safe form

```python
from rpyc_async.core.netref import async_call
from rpyc_async.utils.helpers import fire_and_forget_call

task = fire_and_forget_call(
    lambda: async_call(conn.root, "get_last_response"),
    timeout=5.0,
    success_callback=on_success,
    error_callback=on_error,
)
```

- `async_call(proxy, "name", *args, **kwargs)` never resolves the attribute
  locally. It sends one `HANDLE_ASYNC_CALLATTR` request, so the lookup *and*
  the call happen on the peer, and the reply arrives event-driven. It works
  for both `async def` and plain `def` remote methods. It is also the safe
  replacement for `await conn.root.method()` outside fire-and-forget:
  `await async_call(conn.root, "method")`.
- `fire_and_forget_call()` takes a **factory** (a zero-argument callable
  returning an awaitable) and invokes it inside the task, so `timeout=` covers
  creating the call as well as waiting for it. Passing an awaitable instead —
  including the result of `async_call(...)` — raises `TypeError` that names the
  right form.
- In a loop, bind the variable: `lambda i=i: async_call(conn.root, "job", i)`.
  A bare `lambda: ...job(i)` sees only the last value of `i`.
- When the callbacks must be plain `def`, pass the awaitable from
  `async_call()` to `fire_and_forget()`: `async_call()` itself never blocks,
  so nothing runs outside the task except sending the request.

**Requirement:** the peer must be an `AsyncioServer` — for plain `def` methods
too, because `HANDLE_ASYNC_CALLATTR` is itself an async request. A
`ThreadedServer` peer cannot dispatch it: its handler raises
`RuntimeError: Async method requires persistent event loop` and drops the
connection, while the caller just never gets a reply. Always pass `timeout=`
so that case ends in `TimeoutError`. For a `ThreadedServer` peer, see
[Troubleshooting](#error-sync_request-was-called-from-the-asyncio-loop).

`fire_and_forget_async(conn.root.method())` keeps working exactly as before —
it is not deprecated, and it is perfectly safe for local coroutines. It is just
the wrong tool when the awaitable is a remote call.

---

## Basic Usage

### With Local Coroutines

```python
import asyncio
from rpyc_async.utils.helpers import fire_and_forget

async def my_async_task(value: int) -> int:
    await asyncio.sleep(1)
    return value * 2

async def main():
    # Fire and forget - returns immediately
    task = fire_and_forget(
        my_async_task(10),
        timeout=5.0,
        success_callback=lambda result: print(f"Got: {result}"),
        error_callback=lambda exc: print(f"Error: {exc}"),
    )

    # Continue doing other work
    print("Task started in background")

    # Optionally wait for completion. `await task` yields None, never 20 -
    # the result is delivered to success_callback.
    await task

asyncio.run(main())
```

### With Cross-Process RPC

```python
import asyncio
from rpyc_async.core.async_connect import async_connect
from rpyc_async.core.netref import async_call
from rpyc_async.utils.helpers import fire_and_forget_call

async def on_result(result):
    await store(result)

async def on_failure(exc):
    await report(exc)

async def main():
    # Connect to an AsyncioServer. `exposed_long_running_task` may be
    # `async def` or a plain `def`.
    conn = await async_connect("localhost", 18861)

    # Fire and forget remote call - returns immediately; nothing touches the
    # network on this line, the factory runs inside the task.
    task = fire_and_forget_call(
        lambda: async_call(conn.root, "long_running_task", arg1, arg2),
        timeout=30.0,
        success_callback=on_result,
        error_callback=on_failure,
    )

    # Continue immediately
    print("RPC call started in background")
    await asyncio.sleep(1)

    # Settle the task first. Closing with a call in flight strands it: the reply
    # can never arrive, so neither callback ever runs.
    await asyncio.gather(task, return_exceptions=True)
    await conn.aclose()

asyncio.run(main())
```

---

## Async Callbacks

For callbacks that need to perform async operations, use `fire_and_forget_async()`:

```python
from rpyc_async.utils.helpers import fire_and_forget_async

async def on_success(result):
    # Can await here
    await log_to_database(result)
    await send_notification(result)

async def on_error(exc):
    await log_error_to_file(str(exc))

async def main():
    task = fire_and_forget_async(
        my_async_task(10),
        success_callback=on_success,
        error_callback=on_error,
    )

    await task  # Wait if needed; the value is None, not the result
```

The callbacks are awaited by the helper's own task, so they run on the same
event loop and may make further RPC calls.

---

## Timeout Handling

### Local Timeout Enforcement

Timeouts are enforced **locally** by the event loop, even if the remote process hangs:

```python
async def main():
    conn = await async_connect("localhost", 18861)

    async def on_timeout(exc):
        print(f"Timed out: {exc!r}")

    # This will timeout after 5 seconds, even if the server hangs
    task = fire_and_forget_call(
        lambda: async_call(conn.root, "potentially_hanging_call"),
        timeout=5.0,
        error_callback=on_timeout,
    )

    # Fire-and-forget completes in ~5 seconds, not forever
    await task
```

**Key Insight:** The timeout fires in your local event loop — which is why
nothing may block that loop before the timeout is armed. With
`fire_and_forget_call()` and `async_call()` nothing does. You don't need to
wait for a hung remote process to respond, and the task's slot is released when
it fires. The exception delivered to `error_callback` is `TimeoutError` (in
Python 3.11+ `asyncio.TimeoutError is TimeoutError`).

### No Timeout

If you don't specify a timeout, the call waits indefinitely:

```python
task = fire_and_forget(
    my_async_task(10),
    timeout=None,  # No timeout
    success_callback=handle_success,
)
```

For a local coroutine that is fine. For an RPC it is a leak: if the peer never
replies, the task never finishes, and the helper's internal registry keeps it
alive for the lifetime of the process. Pass a `timeout=`.

---

## Server Setup

`AsyncioServer` is required for `async def exposed_*` methods. Under
`ThreadedServer` such a method raises
`RuntimeError: Async method requires persistent event loop` in the handler
thread, which kills the connection; the client then sees
`EOFError: stream has been closed`.

`AsyncioServer` is also required for `async_call()`, whatever the remote
method looks like. A `ThreadedServer` with ordinary `def exposed_*` methods can
still be used with fire-and-forget through `rpyc.async_()`, at the cost of one
blocking attribute lookup per call — see Troubleshooting.

### Server Side

```python
import asyncio
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer

class MyService(rpyc.Service):
    async def exposed_long_task(self, value: int) -> int:
        await asyncio.sleep(5)
        return value * 2

    async def exposed_quick_task(self, value: int) -> int:
        return value + 10

async def main():
    server = AsyncioServer(
        MyService,
        port=18861,
        protocol_config={"allow_all_attrs": True},
    )

    print("Server starting...")
    await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
```

### Client Side

```python
import asyncio
from rpyc_async.core.async_connect import async_connect
from rpyc_async.core.netref import async_call
from rpyc_async.utils.helpers import fire_and_forget

async def main():
    # async_connect automatically enables asyncio serving
    conn = await async_connect("localhost", 18861)

    # Fire and forget. The callbacks are plain `def`, so the awaitable from
    # async_call() goes to fire_and_forget(); creating it never blocks.
    task1 = fire_and_forget(
        async_call(conn.root, "long_task", 100),
        timeout=10.0,
        success_callback=lambda r: print(f"Task 1: {r}"),
    )

    task2 = fire_and_forget(
        async_call(conn.root, "quick_task", 50),
        timeout=2.0,
        success_callback=lambda r: print(f"Task 2: {r}"),
    )

    # Both tasks run concurrently. Settle them before closing.
    await asyncio.gather(task1, task2, return_exceptions=True)

    await conn.aclose()

asyncio.run(main())
```

---

## Bidirectional Async Calls

Fire-and-forget works with bidirectional async calls (server calling client callbacks):

### Server

```python
class MyService(rpyc.Service):
    async def exposed_process_with_callback(self, callback, value: int) -> int:
        # Server calls client's async callback
        result = await callback(value * 2)
        return result + 10
```

### Client

```python
async def main():
    conn = await async_connect("localhost", 18861)

    # Define async callback
    async def my_callback(x: int) -> int:
        await asyncio.sleep(0.1)
        return x + 5

    # Fire and forget - server will call our callback
    task = fire_and_forget(
        async_call(conn.root, "process_with_callback", my_callback, 10),
        timeout=5.0,
        success_callback=lambda r: print(f"Final result: {r}"),
    )

    await task          # yields None
    await conn.aclose()
```

Verified end to end: the callback prints `Final result: 35` (an `int`, not a
netref), while `task.result()` is `None`.

---

## Best Practices

### 1. Always Provide Error Callbacks

Without an error callback the exception is printed to stderr as
`ERROR: Unhandled exception in fire_and_forget: …` and then swallowed. It is
**not** re-raised by `await task`, so nothing downstream will notice.

```python
# Good
task = fire_and_forget_async(
    risky_operation(),
    error_callback=handle_error,
)

# Bad - the exception is printed and lost
task = fire_and_forget_async(risky_operation())
```

### 2. Set Reasonable Timeouts for RPC

Always use timeouts for cross-process calls to prevent hanging:

```python
# Good
task = fire_and_forget_call(
    lambda: async_call(conn.root, "remote_call"),
    timeout=10.0,  # Explicit timeout, covering the whole call
    error_callback=handle_timeout,
)

# Risky - no timeout; if the peer never replies the task is pinned forever
task = fire_and_forget_call(lambda: async_call(conn.root, "remote_call"))

# Wrong - conn.root.remote_call() runs on your loop before the helper,
# outside the timeout (see Remote Calls)
task = fire_and_forget_async(conn.root.remote_call(), timeout=10.0)
```

### 3. Task Management

You do **not** need to keep a reference to stop the task being garbage
collected — both helpers already pin it internally for its lifetime and release
it on completion. Discarding the return value is safe.

Keep the task only when you intend to `await` or `cancel()` it, and always
settle outstanding tasks before closing the connection:

```python
tasks = [
    fire_and_forget_call(lambda i=i: async_call(conn.root, "job", i), timeout=10)
    for i in range(5)
]
await asyncio.gather(*tasks, return_exceptions=True)
await conn.aclose()
```

### 4. Cancellation

You can cancel fire-and-forget tasks:

```python
task = fire_and_forget_async(long_running_task())

# Later...
if should_cancel:
    task.cancel()
```

Note: cancellation propagates `CancelledError`, which is **not** passed to
`error_callback`. Cancelling is the other way (besides `timeout=`) to release a
task that is waiting on a reply that will never come.

---

## Common Patterns

### Pattern 1: Multiple Concurrent Background Tasks

```python
async def main():
    conn = await async_connect("localhost", 18861)

    results = []
    tasks = [
        fire_and_forget(
            async_call(conn.root, "process_item", i),
            timeout=5.0,
            success_callback=results.append,
        )
        for i in range(10)
    ]

    # Settle every task before closing; gather() yields Nones, not results.
    await asyncio.gather(*tasks, return_exceptions=True)
    print(results)

    await conn.aclose()
```

`success_callback=results.append` is the one case where the sync helper is the
right tool: the callback is pure CPU work and has nothing to `await`.

### Pattern 2: Fire and Continue

```python
async def report_failure(exc):
    await alert_service.send(f"Analytics failed: {exc}")

async def main():
    conn = await async_connect("localhost", 18861)

    # Start background task. Discarding the Task is safe - the helper pins it.
    fire_and_forget_call(
        lambda: async_call(conn.root, "log_analytics", data),
        timeout=2.0,
        error_callback=report_failure,
    )

    # Continue immediately without waiting
    return await async_call(conn.root, "get_user_data")
```

### Pattern 3: With State Tracking

```python
class RequestHandler:
    def __init__(self):
        self.completed = 0
        self.failed = 0

    def on_success(self, result):
        self.completed += 1

    def on_error(self, exc):
        self.failed += 1

    async def process_batch(self, conn, items):
        tasks = []
        for item in items:
            task = fire_and_forget(
                async_call(conn.root, "process", item),
                timeout=10.0,
                success_callback=self.on_success,
                error_callback=self.on_error,
            )
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)
        print(f"Completed: {self.completed}, Failed: {self.failed}")
```

---

## Troubleshooting

### Error: "RuntimeError: no running event loop"

**Cause:** Called `fire_and_forget()` or `fire_and_forget_async()` outside of an
async context. Both need a running loop to schedule the task on.

**Solution:** Ensure you're calling from within an async function:

```python
# Wrong
def sync_function():
    task = fire_and_forget_async(...)  # RuntimeError: no running event loop

# Correct
async def async_function():
    task = fire_and_forget_async(...)  # OK
```

### Callbacks Not Firing

**Not the cause: garbage collection.** Both helpers keep a strong reference to
the task internally until it finishes, and drop it the moment it does. You can
discard the return value; the task still runs and its callbacks still fire.

**The actual cause: the connection was closed while the call was in flight.**
The reply can no longer arrive, so the task never completes and *neither*
callback ever runs — verified: `task.done()` stays `False` and
`task.cancelled()` stays `False`, forever.

**Solution:** settle the tasks before closing.

```python
tasks = [
    fire_and_forget_call(lambda i=i: async_call(conn.root, "job", i))
    for i in range(5)
]
await asyncio.gather(*tasks, return_exceptions=True)
await conn.aclose()
```

If you never want to wait, give every call a `timeout=` so it cannot outlive
the connection indefinitely.

### Error: "sync_request() was called from the asyncio loop"

**Cause:** The remote method is a plain `def`, so touching it produces a
`HANDLE_CALL` round-trip *before* `fire_and_forget()` is ever reached. The
argument is evaluated first, and evaluating it is what blows up:

```python
# Wrong - conn.root.slow(10) runs sync_request() right here
task = fire_and_forget(conn.root.slow(10), timeout=2.0)
```

**Solution:** with an `AsyncioServer` peer, use `async_call()` — it works for
plain `def` and `async def` methods alike and never blocks:

```python
task = fire_and_forget(async_call(conn.root, "slow", 10), timeout=2.0)
```

With a `ThreadedServer` peer `async_call()` cannot work (see
[Remote Calls](#remote-calls)). Wrap the sync netref with `rpyc.async_()` so
the call becomes an awaitable instead of a blocking request:

```python
# Remote method is a plain `def exposed_slow` on a ThreadedServer
task = fire_and_forget(rpyc.async_(conn.root.slow)(10), timeout=2.0)
```

The call itself is then bounded by the local event loop, even against a
handler stuck in `time.sleep()` — verified: `fire_and_forget()` returns in
`0.000s` and the `error_callback` receives `TimeoutError` after `2.0s`. The
attribute lookup `conn.root.slow` is still a blocking round-trip on your loop
before the helper runs; it is cheap only while the server answers promptly.

### Hung Connections

**Symptom:** Fire-and-forget hangs forever despite the peer being gone.

**Cause:** No `timeout=` was given, and the reply never arrives. The task then
parks in the event loop indefinitely and the helper's internal registry grows
with it.

**Solution:** always pass `timeout=` for cross-process calls, or `cancel()` the
task yourself. Either one releases the slot.

Note that `enable_asyncio_serving()` is only for connections you built by hand
from a channel/stream — `async_connect()` and `AsyncioServer` already call it
for you. `rpyc.connect()` is not an alternative to `async_connect()`: it is
synchronous and raises `RuntimeError` when called from a running event loop.

Close the connection with `await conn.aclose()`. The synchronous `conn.close()`
issues a **blocking** `sync_request(HANDLE_CLOSE)` and waits for a reply that,
as the implementation notes, usually never arrives — the peer tears the
connection down before emitting it. The wait therefore runs to
`sync_request_timeout` (30 s by default), **freezing the event loop** for that
long. `aclose()` sends the same message event-driven and never blocks.

---

## Performance Considerations

### Overhead

Fire-and-forget has minimal overhead:
- Creates one Task object per call
- Uses event loop's built-in timeout mechanism
- No polling or busy-waiting

### Concurrency

You can safely fire thousands of concurrent tasks — 10 000 was measured without
trouble:

```python
tasks = [
    fire_and_forget(async_call(conn.root, "process", i), timeout=5.0)
    for i in range(10000)
]
await asyncio.gather(*tasks)
```

Remember that `await task` yields `None`, never the call's result, so
`asyncio.gather()` here only tells you the tasks have settled. Results arrive
through `success_callback`.

### Memory

Each task costs roughly 1 KB (measured with `tracemalloc`: ~1032 bytes per task
over 10 000 tasks). The helpers drop their internal reference as soon as a task
finishes, so completed tasks are collected without any action from you.

The one thing to remember: **set `timeout=`**. A call whose reply never arrives
never finishes, so its task is pinned for the lifetime of the process.

---

## API Reference

### fire_and_forget()

```python
def fire_and_forget(
    awaitable: Awaitable[T],
    *,
    timeout: float | None = None,
    success_callback: Callable[[T], None] | None = None,
    error_callback: Callable[[BaseException], None] | None = None,
    name: str | None = None,
) -> asyncio.Task[None]: ...
```

Execute an awaitable in the background with sync callbacks.

**Parameters:**
- `awaitable`: Coroutine or AsyncResult to execute
- `timeout`: Optional timeout in seconds (None = no timeout)
- `success_callback`: Synchronous function called with result on success
- `error_callback`: Synchronous function called with the exception on error
- `name`: Optional task name for debugging

**Returns:**
- `asyncio.Task` that resolves to `None` — never to the awaitable's result

**Raises:**
- `RuntimeError`: If no event loop is running

### fire_and_forget_async()

```python
def fire_and_forget_async(
    awaitable: Awaitable[T],
    *,
    timeout: float | None = None,
    success_callback: Callable[[T], Awaitable[None]] | None = None,
    error_callback: Callable[[BaseException], Awaitable[None]] | None = None,
    name: str | None = None,
) -> asyncio.Task[None]: ...
```

Execute an awaitable in the background with async callbacks.

**Parameters:**
- `awaitable`: Coroutine or AsyncResult to execute
- `timeout`: Optional timeout in seconds (None = no timeout)
- `success_callback`: Async function called with result on success
- `error_callback`: Async function called with the exception on error
- `name`: Optional task name for debugging

**Returns:**
- `asyncio.Task` that resolves to `None` — never to the awaitable's result

**Raises:**
- `RuntimeError`: If no event loop is running

### fire_and_forget_call()

*Added in 1.1.*

```python
def fire_and_forget_call(
    make_awaitable: Callable[[], Awaitable[T]],
    *,
    timeout: float | None = None,
    success_callback: Callable[[T], Awaitable[None]] | None = None,
    error_callback: Callable[[BaseException], Awaitable[None]] | None = None,
    name: str | None = None,
) -> asyncio.Task[None]: ...
```

Like `fire_and_forget_async()`, but takes a factory that is invoked inside the
background task.

**Parameters:**
- `make_awaitable`: Zero-argument callable returning a coroutine or AsyncResult
- `timeout`: Optional timeout in seconds, covering creation AND await
- `success_callback`: Async function called with result on success
- `error_callback`: Async function called with the exception on error
- `name`: Optional task name for debugging

**Returns:**
- `asyncio.Task` that resolves to `None` — never to the call's result

**Raises:**
- `TypeError`: If given an awaitable (including an `AsyncResult`) instead of a callable
- `RuntimeError`: If no event loop is running

### netref.async_call()

*Added in 1.1.*

```python
def async_call(proxy, name: str, *args, **kwargs) -> Awaitable: ...
```

Call `proxy.<name>(*args, **kwargs)` on the peer without resolving the
attribute locally. Returns an awaitable resolving to the remote method's return
value; cancellable and boundable with `asyncio.wait_for`. Requires asyncio
serving on the connection (`async_connect()` enables it) and an
`AsyncioServer` peer. Plain (non-netref) objects fall back to a direct, lazily
invoked local call.

### Callback semantics (all three helpers)

- `error_callback` is invoked for `Exception` subclasses only. A bare
  `BaseException` such as `KeyboardInterrupt` bypasses it and propagates out of
  the task, despite the `BaseException` annotation. `CancelledError` likewise
  bypasses it and propagates to whoever awaits the task.
- Without an `error_callback` the exception is printed as
  `ERROR: Unhandled exception in fire_and_forget[_async]: <exc>` and swallowed.
  `await task` does **not** re-raise it.
- An exception raised *inside* a callback is caught and printed as
  `ERROR: Exception in fire_and_forget[_async] {success,error}_callback: <exc>`.
  The task still completes successfully — verified: `task.done()` is `True` and
  `task.exception()` is `None`.
- Each helper keeps the task in an internal set for its lifetime and removes it
  via `add_done_callback` on completion. You never need to hold a reference to
  keep it alive.

---

## Summary

Fire-and-forget utilities provide a clean way to execute async operations in the
background:

- **Non-blocking** — returns immediately
- **Timeout support** — enforced by the local event loop, so a hung peer cannot hang you
- **Cross-process RPC** — works with async RPC calls
- **Error handling** — optional success/error callbacks
- **Single event loop** — no additional threads or loops

**Remember:**
- For remote calls use `fire_and_forget_call(lambda: async_call(conn.root, "method", ...))` — never `fire_and_forget_async(conn.root.method())`
- Otherwise prefer `fire_and_forget_async()`; use `fire_and_forget()` only when the callback cannot `await`
- `AsyncioServer` is required for `async def` remote methods and for `async_call()`; against a `ThreadedServer`, wrap plain `def` ones in `rpyc.async_()`
- Set a `timeout=` on every cross-process call
- Provide an error callback — without one, exceptions are printed to stderr and swallowed
- `await task` yields `None`; results come from `success_callback`
- Do **not** keep a reference just to defeat the garbage collector — the helpers already pin the task
- Settle in-flight tasks before `await conn.aclose()`, or they never complete
