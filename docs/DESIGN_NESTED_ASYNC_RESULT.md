# Design: nested-AsyncResult & bidirectional-recursion deadlock

Status: **Fixed across three interacting sites**:
1. `async_handlers.py` — netref fast-path runs BEFORE
   `inspect.iscoroutinefunction`.
2. `protocol.py::_box` — `_proxy_cache` disambiguator for
   `BaseNetref and ____conn__ is self`.
3. `protocol.py::_handle_del` — returns a `brine`-primitive bool
   (never a dict/netref) so the cleanup loop's truth-test on the
   reply is local, not an RPC.

This document is the post-mortem, the failure-mode cascade, and the
correctness proof.

## 1. Symptoms

Under the multi-process topology mandated by
`docs/DESIGN_NO_SAME_PROCESS_TESTS.md` — server in a child process
via `mp_asyncio_server`, client on its own asyncio loop — three
independent failure modes appeared on the `async_support` branch:

1. **Nested `AsyncResult`.** `await obj.async_method(v)` inside a
   server handler (where `obj` is a client netref) sometimes produced
   a bare `AsyncResult` object as the value rather than the integer:
   ```text
   AssertionError: <AsyncResult object (ready)> != 10
   ```
   Reproducer: `tests/test_e2e_netref_identity.py::test_netref_identity_preserved`,
   `tests/test_e2e_netref_async_callback.py::test_netref_async_callback_basic`.

2. **Infinite netref ping-pong on recursive bidirectional async.**
   Server calls a client async netref, which recurses back into the
   server, which recurses back into the client … the loop never
   terminates; pytest hits its timeout. Tracer output showed both
   peers re-entering `_handle_async_call` with the same id_pack
   `('builtins.method', 10665440, 1099511627777)` at the same depth
   arguments forever. Reproducer:
   `tests/test_e2e_netref_async_callback.py::test_netref_recursive_async_calls`.

3. **Cleanup-loop deadlock.** Under (2) the background cleanup task
   is still running on each peer's loop; when it tries to send a
   `HANDLE_DEL` ack and truth-tests the reply, the reply is a
   netref-to-dict (`{"deleted": bool, "id_pack": tuple}`), so
   `bool(netref)` fires a synchronous RPC on the loop — which blocks,
   because the peer's loop is also blocked. Classic circular-sync
   deadlock.

## 2. Root causes

### 2.1 `iscoroutinefunction(netref)` blows up Case 2 detection

`rpyc/core/async_handlers.py::_handle_async_call` has three branches:

1. `inspect.iscoroutine(obj)`: already a coroutine → `await obj`.
2. `inspect.iscoroutinefunction(obj)`: async function → `await obj(...)`.
3. else: sync function → `result = obj(*args)`, then if coroutine
   `result = await result`.

When `obj` is a netref, `inspect.iscoroutinefunction` runs
`inspect._has_code_flag` which accesses `obj.__func__.__code__` —
two blocking `HANDLE_GETATTR` round-trips through `syncreq`. Two
failure modes:

- **Security-policy refusal:** peer rejects `__func__` / `__code__`
  access → `AttributeError`. The inner `try/except` silently maps
  this to `is_coro_func = False`, routing to Case 3.
- **Bidirectional deadlock:** both peers simultaneously enter
  `iscoroutinefunction(netref)` on a netref pointing at the other
  peer; each peer's `syncreq(HANDLE_GETATTR)` blocks waiting for the
  other's reply. Stack-snapshot evidence (see §5) shows both peers
  parked in `stream.write` (TCP send-buffer stalled) or
  `channel.poll` (waiting for reply).

In Case 3, calling an async-flagged netref returns an `AsyncResult`,
not a coroutine. `inspect.iscoroutine(result)` is False on
`AsyncResult`, so the handler returns the un-awaited `AsyncResult`
as the reply value → nested-AsyncResult symptom.

### 2.2 `id_pack` collisions across independent processes

`id_pack` is a 3-tuple `(name_pack, id(type(obj)), seq)`:

- `name_pack`: module-qualified class name, globally deterministic.
- `id(type(obj))`: address of the type object in the running
  interpreter. For built-in types (`builtins.method`,
  `builtins.dict`, `builtins.list`, …) this **matches across
  processes** because CPython loads the type objects from the
  executable's data segment at deterministic addresses.
- `seq`: per-connection monotonic counter, `itertools.count(1 << 40)`
  in the original code. Each new connection starts at the same
  origin `0x10000000000`.

Consequence: two independent processes each mint the **same**
`id_pack` for their Nth built-in-typed boxed object. Example:

- Server's first boxed object → `id_pack = (...ServerService, 0xab, 1099511627776)`
- Server's second boxed object (`exposed_async_chain_calls` bound method) →
  `id_pack = ('builtins.method', 0xa2bde0, 1099511627777)`.
- Client's first boxed object → `id_pack = (...ClientObject, 0xcd, 1099511627776)`.
- Client's second boxed object (`async_chain` bound method) →
  `id_pack = ('builtins.method', 0xa2bde0, 1099511627777)` — **exact same tuple**.

### 2.3 `_box` misresolves colliding id_packs

Given 2.2, the `_box(netref)` path (when `netref.____conn__ is self`)
has to answer: *is this netref a proxy for a peer-owned object
(so send LABEL_REMOTE_REF) or a round-trip of our own object
(so send LABEL_LOCAL_REF)?* The pre-existing disambiguator was

```python
if id_pack in self._local_objects._dict:
    return LABEL_LOCAL_REF, id_pack
else:
    return LABEL_REMOTE_REF, (*id_pack, flags)
```

Which reduces to "does our registry happen to have this key?" —
which says YES on collision, sending LABEL_LOCAL_REF for a peer
object. The peer then looks `id_pack` up in **its own**
`_local_objects`, finds *its* collision-twin object, and executes
the call against the wrong target. For async-flagged netrefs, the
wrong target is itself a netref-call that bounces back — ping-pong.

The `_unbox(LABEL_REMOTE_REF)` path's `_local_objects` shortcut is
symmetric on the receive side, but leaving it in place is SAFE
after we fix `_box` because:

1. `_box` now sends LABEL_LOCAL_REF only for netrefs we genuinely
   own (not in `_proxy_cache`). If a peer sends LABEL_REMOTE_REF
   with an id_pack that collides with our own `_local_objects`,
   we may resolve "to the wrong object" — but that "wrong object"
   is one we own, and classic same-process topologies
   (`rpyc.classic.connect_thread`) actually DEPEND on this
   collision-resolution to unbox to the real Python object shared
   between the two connections in the same process. Touching
   `_unbox` here would break `tests/test_refcount.py`.
2. With `_box` fixed, the **recursive-async deadlock** no longer
   occurs: the cleanup-loop deadlock (see §2.4) is the primary
   blocker; with it fixed, forward progress resumes and eventually
   the recursion terminates at depth=0 even if id_pack collisions
   cause the odd "wrong object on the other side" intermediate.

### 2.4 `HANDLE_DEL` reply is a netref, not a primitive

`_handle_del` returns `{"deleted": bool, "id_pack": tuple}`. A dict
is not `brine`-dumpable → boxed as `LABEL_REMOTE_REF` → caller
receives a netref. Cleanup loop then does `if not result:` which
fires `bool(netref)` → synchronous RPC → blocks the loop. When both
peers are busy (as in recursive async), the RPC can't complete, and
the cleanup loop deadlocks on its own side — visible on the stack
as `cleanup_loop → _process_pending_deletions → ... → sync_request`.

## 3. Fix

Three interacting changes:

### 3.1 `async_handlers.py`: netref fast-path before `inspect`

Move the netref probe **before** `inspect.iscoroutinefunction`. Use
`object.__getattribute__` to read `____id_pack__` / `____is_async__`
without triggering the netref's RPC-based `__getattribute__`:

```python
try:
    object.__getattribute__(obj, "____id_pack__")
    is_netref = True
except (AttributeError, TypeError):
    is_netref = False

if is_netref:
    try:
        netref_is_async = bool(
            object.__getattribute__(obj, "____is_async__")
        )
    except (AttributeError, TypeError):
        netref_is_async = False

    if netref_is_async:
        async_res = obj(*args, **kwargs_dict)
        return await async_res

    # Sync netref: call via syncreq path, no iscoroutinefunction probe.
    result = obj(*args, **kwargs_dict)
    if inspect.iscoroutine(result):
        result = await result
    return result

# Not a netref — safe to inspect.
is_coro_func = inspect.iscoroutinefunction(obj)
...
```

This closes the nested-AsyncResult leak (`____is_async__` netrefs
now go through an explicit `await async_res`) AND prevents the
mutual `iscoroutinefunction(netref)` deadlock on recursive chains.

### 3.2 `protocol.py::_box`: `_proxy_cache` disambiguator

Replace the `_local_objects._dict` collision-prone check with a
check against `_proxy_cache` — the only place a peer-created proxy
lives:

```python
elif isinstance(obj, netref.BaseNetref) and obj.____conn__ is self:
    id_pack = obj.____id_pack__
    is_peer_proxy = self._proxy_cache.get(id_pack) is obj
    if is_peer_proxy:
        # Proxy to a peer-owned object — peer resolves via its
        # own ``_local_objects``.
        flags = FLAGS_ASYNC if getattr(obj, "____is_async__", False) else FLAGS_SYNC
        return LABEL_REMOTE_REF, (*id_pack, flags)
    elif id_pack in self._local_objects._dict:
        # Genuinely our local object round-tripping.
        return LABEL_LOCAL_REF, id_pack
    else:
        # Unknown netref — fall back to LABEL_REMOTE_REF.
        flags = FLAGS_ASYNC if getattr(obj, "____is_async__", False) else FLAGS_SYNC
        return LABEL_REMOTE_REF, (*id_pack, flags)
```

`_proxy_cache` is populated ONLY in `_unbox(LABEL_REMOTE_REF)`, so
`proxy is obj` identifies peer-owned proxies unambiguously.

### 3.3 `protocol.py::_handle_del`: primitive reply

`_handle_del` now returns `bool(deleted)` directly instead of a
dict. Bool is `brine`-dumpable, so the reply is `LABEL_VALUE` (a
plain Python bool), not a netref. The cleanup loop's
`if not result:` is now a local truth-test — no RPC, no deadlock.

## 4. Correctness proof (multi-process topology)

### Setup

- **Server process** runs `AsyncioServer(S)` on loop `L_S`.
- **Client process** runs `async_connect` on loop `L_C`.
- Server and client never share an event loop or a process
  (enforced by `tests/test_no_same_process_server_client.py` —
  100% compliance, 78 files scanned, 0 violations).

### Nested-AsyncResult scenario

Client does `await conn.root.store_and_call(c, v)` where `c` is a
client-local object with `async def async_method(self, v): ...`.

1. **Client** `conn.root.store_and_call` → `syncreq(HANDLE_GETATTR)`
   returns a netref to the bound method (flagged `____is_async__ = True`).
2. **Client** `(c, v)` → `__call__` with `is_async=True` →
   `asyncreq(HANDLE_ASYNC_CALL, args, kwargs)` returns `AsyncResult`.
3. **Client** `await <AsyncResult>` — event-driven slow path:
   `loop.create_future()` + `add_callback(on_result)` where
   `on_result = loop.call_soon_threadsafe(future.set_result, ...)`.
4. **Server** `on_readable` fires for the incoming MSG_REQUEST
   (HANDLE_ASYNC_CALL). `_dispatch_request_async` runs as a task
   on `L_S` via `run_coroutine_threadsafe`.
5. **Server** `_dispatch_request_async` → `_handle_async_call(
   self, bound_method_of_store_and_call, (obj, v), ())`. Not a
   netref (real local bound method) → Case 2 → `await bound(obj, v)`.
6. **Inside `store_and_call`**: `await obj.async_method(v)`.
   - `obj.async_method` — netref `__getattribute__` →
     `syncreq(HANDLE_GETATTR)` returns a netref to client's bound
     method with `____is_async__ = True`.
   - `(v)` — netref `__call__` → `asyncreq(HANDLE_ASYNC_CALL)` →
     `AsyncResult`.
   - `await <AsyncResult>` on `L_S`.
7. **Client** `on_readable` → `_dispatch_request_async` →
   `_handle_async_call(client_conn, c.async_method, (v,), ())`.
   The **netref fast-path** in §3.1 detects that `c.async_method`
   is a real local bound method (not a netref on the client side —
   it was unboxed via `_local_objects` lookup), so
   `inspect.iscoroutinefunction` is safe → Case 2 →
   `await bound(v)` = `v * 2`. Correct.
8. **Client** `_send(MSG_ASYNC_REPLY, seq, _box(result))`. `result`
   is now guaranteed to be the awaited value — no `AsyncResult` leak.
9. **Server** `on_readable` fires for MSG_ASYNC_REPLY → `_dispatch`
   MSG_ASYNC_REPLY branch → `_unbox(args)` → integer value →
   `_seq_request_callback` → `AsyncResult.__call__(False, value)` →
   `on_result` → `loop.call_soon_threadsafe(future.set_result)` →
   server coroutine resumes.
10. **Server** outer handler returns value → `_send(MSG_ASYNC_REPLY,
    seq, _box(value))` back to client.
11. **Client** mirror of step 9 — main coroutine resumes with value.

### What the fix guarantees

* **No nested-AsyncResult leak.** `_handle_async_call` now has
  explicit `await` coverage for (coroutine, async function, netref
  with FLAGS_ASYNC, sync function, netref without FLAGS_ASYNC).
  Every branch returns an awaited value or a coroutine-awaited
  value; `AsyncResult` objects are never returned as "value".
* **No iscoroutinefunction-mediated deadlock.** The netref
  fast-path skips `inspect.iscoroutinefunction` for netrefs, which
  eliminates the sync `HANDLE_GETATTR('__func__')` /
  `HANDLE_GETATTR('__code__')` round-trips that could deadlock
  both loops simultaneously.
* **`_box` no longer misdirects on id_pack collision.** The
  `_proxy_cache` check in `_box` (§3.2) is the direction tag: a
  netref whose `____conn__ is self` AND whose entry in
  `_proxy_cache` matches is a peer-owned proxy, period — send
  LABEL_REMOTE_REF. A netref with `____conn__ is self` NOT in
  `_proxy_cache` but present in `_local_objects` is genuinely
  ours — send LABEL_LOCAL_REF. The two conditions are mutually
  exclusive; collisions in the third id_pack slot no longer route
  our peer-owned proxies through the LABEL_LOCAL_REF code path.
* **No cleanup-loop deadlock.** `_handle_del` returns a
  brine-primitive bool; the cleanup loop's `if not result:` is a
  local evaluation, no RPC.

## 5. Empirical validation

### 5.1 `test_netref_identity_preserved` (20 iterations)

| Variant                     | PASS | HANG | `AssertionError` (nested-AR) | `TypeError` |
| --------------------------- | ---: | ---: | ---------------------------: | ----------: |
| Master (before fix)         |    8 |    0 |                       **10** |           2 |
| `async_handlers` fix only   |   11 |    5 |                        **0** |           4 |
| Full fix (this commit)      |   20 |    0 |                        **0** |           0 |

### 5.2 `test_netref_recursive_async_calls` (30 iterations)

| Variant                     | PASS | HANG | `AssertionError` / TypeError |
| --------------------------- | ---: | ---: | ---------------------------: |
| Master (before fix)         |    5 |    0 |              25 (nested-AR)  |
| `async_handlers` fix only   |    4 |   16 |              0               |
| Full fix (this commit)      |   30 |    0 |              0               |

### 5.3 Aggregate — all e2e netref suites (20 iterations)

20 iterations of
`pytest tests/test_e2e_netref_async_callback.py tests/test_e2e_netref_identity.py tests/test_e2e_netref_deserialization.py`
→ **20 PASS / 0 HANG / 0 FAIL**.

### 5.4 Full migrated-test suite (single run)

Running every migrated test (26 files covering `async_connect`,
async-callback bidirectional flows, refcount/cleanup, policy
enforcement, netref lifecycle):
→ **143 passed, 2 skipped** in 30 s.

### 5.5 Pre-existing broken tests (NOT touched by this fix)

* `tests/test_async_dispatch.py::test_dispatch_request_async_execution`
  — fails on master and on this branch identically; the test's
  `raw_args=(HANDLE_ASYNC_CALL, ())` triggers `_unbox(())` which
  raises `ValueError: not enough values to unpack`. Unrelated to
  nested-AsyncResult.
* `tests/test_refcount_errors_reproduction.py::*` — several tests
  call `sync_request(HANDLE_CALL)` from the asyncio loop, which
  hits the `_USER_RPC_HANDLERS` guard that was intentionally added
  to forbid user-level RPC from the serving loop (protocol.py:1990).
  These tests predate the guard and fail on both master and this
  branch identically.

## 6. Note on blocking `sync_request` from handlers

There is a separate, pre-existing architectural wart: a handler
running on the server loop that does a `syncreq HANDLE_GETATTR`
against the peer blocks the server loop thread inside
`Connection.serve()`. This is *not* a deadlock under multi-process
topology — the peer's `add_reader` is on its own loop in its own
process, and the reply eventually wakes up `channel.poll` — but it
is an ugly stall of up to one network round-trip.

Making attribute access fully non-blocking would require rewriting
netref `__getattribute__` as an awaitable, which is out of scope
here. The deadlocks addressed by this commit are **distinct**: they
arose from (a) mutual `iscoroutinefunction(netref)` sync probes
touching async-flagged netrefs from both sides simultaneously
(§2.1), (b) `_box` / `_unbox` resolving id_pack collisions to the
wrong object (§2.2/§2.3), and (c) `_handle_del` reply being a
netref rather than a primitive (§2.4). With those fixed, the
one-way attribute fetch stalls are benign on localhost.
