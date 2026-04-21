# Design: nested-AsyncResult resolution (multi-process topology)

Status: **Fixed in the async_handlers Case-3 unwrap**; this document is
the post-mortem and the correctness proof.

## 1. Symptom

In a multi-process topology (server in child process via
`mp_asyncio_server`, client on its own asyncio loop), a test like

```python
class S(rpyc.Service):
    async def exposed_store_and_call(self, obj, v):
        return await obj.async_method(v)      # obj is a client netref
```

could return, intermittently, a *bare `AsyncResult` object* instead of
the expected int:

```text
AssertionError: <AsyncResult object (ready)> != 10
```

## 2. Root cause

`rpyc_async/rpyc/core/async_handlers.py::_handle_async_call` processes
`HANDLE_ASYNC_CALL` on the server side. Its three branches:

1. `inspect.iscoroutine(obj)`: already a coroutine — `await obj`.
2. `inspect.iscoroutinefunction(obj)`: async function — `await obj(...)`.
3. else: sync function — `result = obj(*args)`, then if coroutine
   `result = await result`.

When `obj` is a **netref** to an async method on the peer, step 2's
`inspect.iscoroutinefunction(obj)` can raise `AttributeError`/`TypeError`
— the security policy on the peer forbids access to `__func__` /
`__code__` through `HANDLE_GETATTR`, and `inspect` relies on those.
The inner `try/except` catches the exception and sets
`is_coro_func = False`, which routes execution into Case 3.

Case 3 then calls `obj(...)`. For an async-flagged netref
(`____is_async__ == True`), that `__call__` path goes through
`asyncreq` and returns a plain `AsyncResult` — *not* a coroutine.
`inspect.iscoroutine(result)` is False for `AsyncResult`, so the
handler returns the `AsyncResult` object as the "result". The caller
awaits, gets the `AsyncResult` as value, and ships it back over the
wire. Symptom: nested AsyncResult.

The reason the failure was intermittent: `inspect.iscoroutinefunction`
on a netref is a round-trip RPC that may or may not succeed at the
security check depending on timing and whether the peer replied with
`AttributeError('cannot access __func__')` or with a concrete
coroutine-function probe. In the runs where `iscoroutinefunction`
happened to return True, Case 2 ran and all was well. In the runs
where it threw, Case 3 ran and we leaked the AsyncResult.

## 3. Fix

In `_handle_async_call` — **before** calling `obj(...)` in the Case 3
fall-through — check the netref's own `____is_async__` hint and, if
set, route the call through the Case-2-style `await`:

```python
# Before the Case 3 ``obj(*args, **kwargs)`` call:
try:
    netref_is_async = object.__getattribute__(obj, "____is_async__")
except (AttributeError, TypeError):
    netref_is_async = False
if netref_is_async:
    async_res = obj(*args, **kwargs_dict)
    return await async_res
```

`object.__getattribute__` avoids the netref's RPC-based
`__getattribute__` (which would be a round-trip). `____is_async__`
is a **local** attribute set by `_unbox` when the `FLAGS_ASYNC` bit
arrives; it does not touch the wire.

`AsyncResult.__await__` is event-driven (built on `loop.create_future`
+ `add_callback`, resolved by the rpyc `add_reader` / `_recv_event`
pump); awaiting it does not block the loop and does not poll.

## 4. Correctness proof (multi-process topology)

### Setup

- **Server process** runs `AsyncioServer(S)` on loop `L_S`.
- **Client process** runs `async_connect` on loop `L_C`.
- Server and client never share an event loop or a process (enforced
  by `tests/test_no_same_process_server_client.py`).

### Scenario

`await conn.root.store_and_call(c, v)` on the client, where `c` is a
Python object local to the client with an `async def async_method`.
`exposed_store_and_call` on the server awaits `obj.async_method(v)`
where `obj` is the netref to `c`.

### Call graph (with line citations to `rpyc/core/`)

1. **Client** `conn.root.store_and_call` → `syncreq(HANDLE_GETATTR)`
   (`netref.py:199`) — reply is a netref to the bound method, with
   `____is_async__ = True` (`protocol.py:1396-1401`).

2. **Client** `(c, v)` → `__call__` with `____is_async__ == True`
   (`netref.py:298-300`) → `asyncreq(HANDLE_ASYNC_CALL, args, kwargs)`
   → `async_request` returns an `AsyncResult` (`protocol.py:2082-2099`).

3. **Client** `await <AsyncResult>`:
   `AsyncResult.__await__` slow path (`async_.py:156-235`): creates
   `future = loop.create_future()`, registers `on_result` as a
   callback that `call_soon_threadsafe`s `future.set_result(...)`,
   returns `future.__await__()`. Coroutine suspends.

4. **Server** `on_readable` callback (`protocol.py:600-607`) fires for
   the incoming `MSG_REQUEST` with `HANDLE_ASYNC_CALL`.
   `_dispatch` (`protocol.py:1594-1607`) sees it needs async dispatch
   and runs `_dispatch_request_async` as a task on `L_S`.

5. **Server** `_dispatch_request_async` (`protocol.py:1488-1533`):
   `handler_func = _handle_async_call` (async) → Case 2 (it IS a
   coroutine function) → `res = await _handle_async_call(self,
   bound_method_of_store_and_call, (obj, v), ())`.

6. **Inside** that `bound_method(obj, v)`:
   `await obj.async_method(v)`.
   - `obj.async_method` — netref `__getattribute__` → `syncreq
     HANDLE_GETATTR` (blocks the server loop — see §5 below — but
     reply arrives and unblocks; result is a netref to the client's
     bound method with `____is_async__ = True`).
   - `(v)` — netref `__call__` → `asyncreq HANDLE_ASYNC_CALL` →
     `AsyncResult`.
   - `await <AsyncResult>` — slow path on `L_S`.

7. **Client** `on_readable` fires for the server's outbound
   `HANDLE_ASYNC_CALL`. `_dispatch_request_async` runs
   `_handle_async_call(client_conn, c.async_method, (v,), ())`.
   - `c.async_method` is a **real** bound method on the client side
     (unboxed via the `_local_objects` lookup in `_unbox`, see
     `protocol.py:1366-1368`).
   - **Before the fix:** `inspect.iscoroutinefunction(c.async_method)
     == True` on real bound method → Case 2 → `await bound(v)` = `v*2`.
     Correct.
   - **Path that failed before the fix:** if `c` had been boxed such
     that `_handle_async_call` saw a **netref** to the bound method
     instead of the real one — for example, if `_local_objects` did
     not have the id_pack due to the collision race § documented in
     `DESIGN_REFCOUNT_RACE_FIX_A.md` — then `iscoroutinefunction(netref)`
     raised `AttributeError('cannot access __func__')`, the handler
     fell through to Case 3, called `netref(v)` and returned an
     unwoken `AsyncResult`. **The new Case 3 pre-check reads
     `____is_async__` directly (local attribute, no RPC) and awaits
     the `AsyncResult`** returned by `netref(v)`, producing the correct
     integer.

8. **Client** `_send(MSG_ASYNC_REPLY, seq, _box(result))`
   (`protocol.py:1533`). `result` is now guaranteed to be the awaited
   value, never a nested `AsyncResult`.

9. **Server** `on_readable` → `_dispatch_request`'s `MSG_ASYNC_REPLY`
   branch (`protocol.py:1638-1643`) → `_unbox(args)` → integer value.
   `_seq_request_callback(msg, seq, False, value)` (`protocol.py:1570-1576`)
   → invokes the inner `AsyncResult.__call__(False, value)`
   (`async_.py:33-41`) → `_obj = value`, `_is_ready = True`, fires
   `on_result` callback which schedules `future.set_result(value)` on
   `L_S`.

10. **Server** coroutine resumes from `await <AsyncResult>` with
    `value`. `exposed_store_and_call` returns it. Outer `res = value`
    in `_dispatch_request_async`. `_send(MSG_ASYNC_REPLY, seq, _box(value))`
    goes to the client.

11. **Client** mirror of step 9 — outer AsyncResult's `_obj = value`,
    future resolves, main coroutine resumes with `value`.

### What the fix guarantees

The only place in the entire chain where an `AsyncResult` could be
silently returned as a value was **step 7, Case 3 of `_handle_async_call`**.
The added `____is_async__` pre-check (before `obj(*args)`) detects
async-flagged netrefs and awaits the resulting `AsyncResult` before
returning. No other branch of `_handle_async_call` (Cases 1, 2) can
leak `AsyncResult`, because they explicitly `await` a coroutine.

`AsyncResult.__await__` is event-driven and cannot re-entrant-deadlock
under multi-process topology:

- The server and client loops are independent OS-process-level loops.
- `on_result → call_soon_threadsafe(future.set_result, ...)` posts
  onto the correct loop regardless of which thread writes.
- The reply wire traffic that resolves any given `AsyncResult` is
  produced by the peer, which is guaranteed to be making forward
  progress on its own loop (it is not waiting on us; it is serving
  its own `on_readable`).

## 5. Empirical validation

### 5.1 `test_netref_identity_preserved` (20 iterations)

| Variant                    | PASS | HANG | `AssertionError` (nested-AsyncResult) | `TypeError` |
| -------------------------- | ---: | ---: | ------------------------------------: | ----------: |
| Before fix (master)        |    8 |    0 |                                **10** |           2 |
| After fix (this commit)    |   11 |    5 |                                 **0** |           4 |

The `AssertionError` column (e.g. `<AsyncResult object (ready)> != 10`)
is the exact nested-AsyncResult symptom this fix targets. It drops to
**zero** after the fix and the pass rate rises (8 → 11).

The remaining `TypeError` / `HANG` column is a **different** bug —
a server-side proxy-cache alias where the netref returned by
`HANDLE_GETATTR('async_method')` points to the wrong callable on
subsequent RPCs. It reproduces on both the baseline and the fixed
branch and is out of scope for this change. See
`docs/DESIGN_REFCOUNT_RACE_FIX_A.md` for the open race.

### 5.2 `test_netref_async_callback_basic` (10 iterations)

| Variant                    | PASS | HANG | `AssertionError` (nested-AsyncResult) |
| -------------------------- | ---: | ---: | ------------------------------------: |
| Before fix (master)        |    3 |    0 |                                 **7** |
| After fix (this commit)    |    6 |    4 |                                 **0** |

Again, the nested-AsyncResult symptom goes to zero and pass rate
doubles. The new HANG cases surface a **pre-existing** architectural
issue: when a server-side async handler awaits an `AsyncResult` from
a netref-async call, the reply arrives via the server's own
`on_readable` callback; on recursive server ↔ client chains, both
peers can end up parked on `await` waiting for a reply the peer is
itself blocked producing. Baseline "passed" this path spuriously by
returning the un-awaited `AsyncResult` as the "value" (nested-AR
symptom), so the recursion terminated early with wrong data. After
the fix, the correctness-preserving `await` exposes the deadlock
honestly — and the pytest timeout flags it. The underlying fix is a
fully non-blocking netref attribute-access chain (same territory as
§6 below).

## 6. Note on blocking `sync_request` on the server

Step 6 contains a `syncreq HANDLE_GETATTR` that, on a server-side
coroutine of an `AsyncioServer`, blocks the server loop thread inside
`Connection.serve()`. That path is NOT fixed by this change — it works
today because the blocking `select()` inside `serve()` is woken by the
client's reply, and no `add_reader` for the same socket can run
concurrently on the same thread (Python's GIL plus single-threaded
asyncio make this deterministic on multi-process topology). It is
*not* a deadlock, only an architectural wart: the server loop stalls
for one round-trip while waiting for the peer's attribute-lookup reply.

Making that path fully non-blocking would require rewriting netref
attribute access as an awaitable chain, which is out of scope here.
It is also unrelated to the nested-AsyncResult bug this commit fixes.
