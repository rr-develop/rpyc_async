# Bidirectional async bugs — post-mortem for future agents

> **Read this before touching** `rpyc/core/async_handlers.py`,
> `rpyc/core/protocol.py::_box`, `rpyc/core/protocol.py::_unbox`,
> or `rpyc/core/protocol.py::_handle_del`. These files interact in
> subtle ways. Three interlocking bugs were fixed together in
> commits `62d508e` and `fe14779`; reverting or "simplifying" any
> one of the fixes reopens the failure.

## TL;DR

Three separate bugs produced four visible symptoms. Each fix closes
ONE bug and is independently necessary. Do not collapse them.

| # | Bug                                                    | File                          | Symptom                                                        |
| - | ------------------------------------------------------ | ----------------------------- | -------------------------------------------------------------- |
| 1 | `inspect.iscoroutinefunction(netref)` blocks on `syncreq`, deadlocks under recursion | `async_handlers.py::_handle_async_call` | `test_netref_recursive_async_calls` HANG (pytest timeout)      |
| 2 | Async-flagged netref leaks `AsyncResult` as "value"    | `async_handlers.py::_handle_async_call` | `<AsyncResult object (ready)> != 10` AssertionError             |
| 3 | `id_pack` collides across processes → `_box` misroutes peer proxies as LABEL_LOCAL_REF | `protocol.py::_box`           | Infinite netref ping-pong at the same id_pack and args          |
| 4 | `_handle_del` returns a dict → cleanup loop's `if not result:` fires sync RPC, deadlocks the loop | `protocol.py::_handle_del`    | `cleanup_loop → _process_pending_deletions → syncreq` deadlock  |

## 1. `inspect.iscoroutinefunction(netref)` is a hidden sync RPC

### Mechanism

`inspect.iscoroutinefunction(obj)` internally calls
`inspect._has_code_flag(obj, CO_COROUTINE)` which accesses:

```python
obj.__func__            # attribute access
obj.__func__.__code__   # attribute access
obj.__func__.__code__.co_flags  # attribute access
```

When `obj` is a netref, **each attribute access is a blocking
`syncreq(HANDLE_GETATTR)`** — the netref class overrides
`__getattribute__` to route non-LOCAL_ATTRS names through
`syncreq`, which internally does `async_request(...).value` →
`AsyncResult.wait()` → `Connection.serve()` → `channel.poll()` →
blocking socket poll.

### Why recursion deadlocks

In a bidirectional recursive async chain — server calls a client
netref, the client's handler calls a server netref, which calls a
client netref, and so on — BOTH peers can simultaneously enter
`inspect.iscoroutinefunction(netref)` on netrefs pointing at each
other. Each peer then tries to `syncreq` the other, but both are
parked in outbound `stream.write` (TCP send-buffer full because
neither is reading) or inbound `channel.poll` (waiting for a reply
that can't arrive). Stack-snapshot verified:

```
# Peer A (server)
File "rpyc/core/stream.py", line 288, in write
  count = self.sock.send(data[:self.MAX_IO_CHUNK])   ← blocked
File "rpyc/core/channel.py", line 78, in send
File "rpyc/core/protocol.py", line 1238, in _send
File "rpyc/core/protocol.py", line 2074, in _async_request
File "rpyc/core/protocol.py", line 2092, in async_request
File "rpyc/core/protocol.py", line 2018, in sync_request
File "rpyc/core/netref.py", line 63, in syncreq
File "rpyc/core/netref.py", line 199, in __getattribute__
File "/usr/lib/python3.12/inspect.py", line 389, in _has_code_flag
File "/usr/lib/python3.12/inspect.py", line 426, in iscoroutinefunction
File "rpyc/core/async_handlers.py", line 73, in _handle_async_call
File "rpyc/core/protocol.py", line 1506, in _dispatch_request_async

# Peer B (client) — identical stack, symmetric
```

### Fix (`async_handlers.py::_handle_async_call`)

Move netref detection **BEFORE** `inspect.iscoroutinefunction`.
Identify netrefs via a purely local slot probe:

```python
try:
    object.__getattribute__(obj, "____id_pack__")
    is_netref = True
except (AttributeError, TypeError):
    is_netref = False
```

- `object.__getattribute__` is load-bearing: it bypasses the
  netref's overridden `__getattribute__` (which would re-trigger
  the RPC). Reading any slot in `LOCAL_ATTRS` with
  `object.__getattribute__` is O(1), local, side-effect-free.
- `____id_pack__` is set on every netref in `BaseNetref.__init__`
  and absent on every non-netref Python object. So the probe
  gives a binary signal.

If `is_netref`, the function takes a dedicated async-or-sync-netref
branch that **does not call `inspect.iscoroutinefunction`**. The
branch instead reads `____is_async__` (another LOCAL_ATTRS slot
set by `_unbox` when `FLAGS_ASYNC` arrived) to decide between
awaiting an `AsyncResult` (async-flagged netref) or routing through
the sync `syncreq` path (sync netref).

### Regression traps

- **DO NOT** call `hasattr(obj, "____id_pack__")`. `hasattr` uses
  `getattr` which fires the netref's overridden `__getattribute__`
  and re-enters the deadlock.
- **DO NOT** use `isinstance(obj, BaseNetref)`. Works correctly
  but pulls the `netref` module into a tight hot path (import
  visible at call-time, circular-import risk). The slot probe is
  faster and matches the duck-typed style of the rest of the
  netref layer.
- **DO NOT** merge the netref branch and the non-netref branch
  back into a single `inspect.iscoroutinefunction` call "for
  simplicity". The separation is the fix.
- **DO NOT** reorder: the netref check must come *before* any
  other code that could trigger a netref `__getattribute__`.

### Repro

```bash
for i in $(seq 1 30); do
  timeout 12 python3 -m pytest \
    tests/test_e2e_netref_async_callback.py::TestE2ENetrefAsyncCallback::test_netref_recursive_async_calls \
    2>&1 > /tmp/r$i.out
done
```

Before fix: master PASS=5 HANG=25. After fix: PASS=30 HANG=0.

## 2. Nested `AsyncResult` leak

### Mechanism

Same code site as bug 1. When `inspect.iscoroutinefunction(netref)`
happened to raise `AttributeError` (peer's security policy
refusing `__func__` access) instead of blocking, the surrounding
`try/except` set `is_coro_func = False` and the code fell into the
"sync function" Case 3:

```python
result = obj(*args, **kwargs_dict)
if inspect.iscoroutine(result):
    result = await result
return result
```

For an async-flagged netref, `obj(*args)` invokes the netref's
`__call__`, which — because `____is_async__` is True — routes
through `asyncreq(HANDLE_ASYNC_CALL, ...)` and returns an
**`AsyncResult`** object (NOT a coroutine).

`inspect.iscoroutine(AsyncResult)` is False. So the handler
returned the **un-awaited `AsyncResult`** as the reply value.
`_dispatch_request_async` then boxed and sent it back over the
wire, and the original `await` resolved to an `AsyncResult` object
rather than the expected value. Classic symptom:

```
AssertionError: <AsyncResult object (ready) at 0x...> != 10
```

### Fix

Same netref fast-path as bug 1. For `____is_async__ == True`
netrefs, the dedicated branch does `await async_res` explicitly:

```python
if netref_is_async:
    async_res = obj(*args, **kwargs_dict)
    return await async_res
```

`AsyncResult.__await__` is event-driven (uses
`loop.create_future()` + `add_callback(on_result)`, resolved when
the peer's reply fires `on_readable`). No polling, no blocking.

### Regression traps

- **DO NOT** remove the explicit `await async_res` in the
  async-flagged-netref branch. Returning `async_res` directly
  reopens the leak.
- **DO NOT** try to handle both `AsyncResult` and coroutine
  outcomes in a single `if inspect.iscoroutine(result):` check
  downstream. `AsyncResult` is not a coroutine and
  `inspect.iscoroutine` correctly returns False for it.

### Repro

```bash
timeout 12 python3 -m pytest \
  tests/test_e2e_netref_identity.py::TestNetrefIdentity::test_netref_identity_preserved
```

Before fix: ~60% AssertionError. After fix: 0 AssertionError.

## 3. `id_pack` collisions across independent processes

### Mechanism

`id_pack` is a 3-tuple that identifies a Python object across
the wire:

```python
id_pack = (name_pack, id(type(obj)), seq)
```

- `name_pack` — `"{module}.{qualname}"` of `type(obj)`. Deterministic
  by class name; identical across processes.
- `id(type(obj))` — memory address of the type object in RAM.
  **For built-in types this is identical across CPython
  processes**. CPython loads built-in type objects from the
  executable's data segment at deterministic addresses. Empirical
  proof:

  ```
  $ python3 -c 'class C:
                  def m(self): pass
                print(id(type(C().m)))'
  10665440

  $ python3 -c 'class C:
                  def m(self): pass
                print(id(type(C().m)))'
  10665440     ← same address in both processes
  ```

  Affects `builtins.method`, `builtins.dict`, `builtins.list`,
  `builtins.function`, `builtins.tuple`, ...

- `seq` — per-connection monotonic counter from
  `itertools.count(1 << 40)`. **Each new `Connection` starts at
  the same origin `0x10000000000`.** So any two peers
  independently allocate seq `1099511627776`, `1099511627777`,
  ... for their Nth boxed object.

**Consequence**: two independent processes EACH mint
`('builtins.method', 10665440, 1099511627777)` for their own 2nd
boxed bound-method-typed object. These are completely unrelated
Python objects that happen to share a bit-identical id_pack.

### Where it caused infinite ping-pong

Originally `_box(netref)` with `netref.____conn__ is self`
disambiguated "our object round-tripping" from "peer-owned proxy"
by checking `id_pack in self._local_objects._dict`. On collision,
this check SAID YES for a peer-owned proxy (because our own
`_local_objects` coincidentally held an unrelated object under
the same id_pack), so we sent `LABEL_LOCAL_REF` — "this is YOUR
object back, peer" — when we should have sent `LABEL_REMOTE_REF`
— "this is MY object, proxy it".

Peer unboxed `LABEL_LOCAL_REF` → looked up its own
`_local_objects[id_pack]` → got its own colliding object → called
it → the call returned through the same path → peer sent
`LABEL_LOCAL_REF` back → we resolved to OUR colliding slot → call
it → ... **infinite ping-pong** between peers with identical
`args` and identical `id_pack`, each thinking the other is
sending back "my own object".

Diagnostic log during the bug:

```
[NETREF:pid_server] id_pack=('builtins.method', 10665440, 1099511627777) args=(11, 2) is_async=True
[NETREF:pid_client] id_pack=('builtins.method', 10665440, 1099511627777) args=(11, 2) is_async=True
[NETREF:pid_server] id_pack=('builtins.method', 10665440, 1099511627777) args=(11, 2) is_async=True
[NETREF:pid_client] id_pack=('builtins.method', 10665440, 1099511627777) args=(11, 2) is_async=True
# ... forever
```

### Fix (`protocol.py::_box`)

Replace the collision-prone `_local_objects` check with a
direction-unambiguous check using `_proxy_cache`:

```python
id_pack = obj.____id_pack__
is_peer_proxy = self._proxy_cache.get(id_pack) is obj
if is_peer_proxy:
    return LABEL_REMOTE_REF, (*id_pack, flags)
elif id_pack in self._local_objects._dict:
    return LABEL_LOCAL_REF, id_pack
else:
    return LABEL_REMOTE_REF, (*id_pack, flags)  # safe fallback
```

- `_proxy_cache` is populated **exclusively** by
  `_unbox(LABEL_REMOTE_REF)`. Every entry there is, by
  construction, a proxy to a peer-owned object.
- The `is obj` identity check — not just membership — guarantees
  we match the exact netref we're boxing, not some stale
  WeakValueDict entry.
- Priority matters: check peer-proxy FIRST. If we fall through to
  the `_local_objects` check, we can only reach the
  `LABEL_LOCAL_REF` branch when the netref is *not* a known peer
  proxy. That's genuine ownership.

### Why we kept `_unbox(LABEL_REMOTE_REF)`'s `_local_objects` shortcut

The symmetric shortcut on the receive side —
`if id_pack in self._local_objects._dict: return self._local_objects[id_pack]`
in `_unbox(LABEL_REMOTE_REF)` — HAS the same collision risk in
theory. But:

1. With `_box` fixed, the ping-pong cycle is broken on the send
   side. A collision on the receive side can still produce "wrong
   object returned", but without the ping-pong it just results in
   a benign wrong-method-call that either raises a clear error or
   returns wrong data — caught by test assertions, not a hang.
2. **Classic `rpyc.classic.connect_thread()` DEPENDS on this
   collision-resolution.** Both sides of the thread-pair run in
   the same process, and the "collision" is actually how the
   shortcut unboxes LABEL_REMOTE_REF back to the real local
   object shared between the two connections. Breaking the
   shortcut breaks `tests/test_refcount.py` (which uses
   `classic.connect_thread`). Verified: changing `_unbox` or
   randomizing `_id_pack_seq` origin breaks that test.

So the recv-side shortcut stays. The send-side fix in `_box` is
sufficient to close the deadlock.

### Regression traps

- **DO NOT** revert to `id_pack in self._local_objects._dict` as
  the sole check in `_box`. Collision hazard.
- **DO NOT** randomize `_id_pack_seq` origin per connection.
  Tried it — the random offset makes `connect_thread` fail because
  classic topology relies on deterministic seq alignment between
  the two in-process connections.
- **DO NOT** use `id_pack in self._proxy_cache` instead of
  `_proxy_cache.get(id_pack) is obj`. `WeakValueDict` keys can
  outlive their values (garbage-collected proxy); the identity
  check is what actually matches.
- **DO NOT** "simplify" by collapsing the three-way branch into
  two. The fallback LABEL_REMOTE_REF in the `else` handles
  evicted/hand-crafted netrefs; dropping it would raise inside
  `_box` on edge cases.

### Repro

```bash
timeout 12 python3 -m pytest \
  tests/test_e2e_netref_async_callback.py::TestE2ENetrefAsyncCallback::test_netref_recursive_async_calls
```

Before fix: HANG with infinite trace log showing the collision.
After fix: PASS.

## 4. Cleanup loop self-deadlock via netref-returning `_handle_del`

### Mechanism

The background cleanup loop is an asyncio task that drains
pending netref deletions, sends each as `HANDLE_DEL` with an
acknowledgment, and logs any failure:

```python
# rpyc/core/protocol.py (cleanup_loop path)
result = await self._async_request_with_ack(
    consts.HANDLE_DEL, id_pack, total_refcount,
    timeout=self._cleanup_ack_timeout
)
if not result:
    log_warning(...)
```

Pre-existing `_handle_del` returned a **dict**:

```python
return {"deleted": deleted, "id_pack": id_pack}
```

`dict` is NOT `brine.dumpable`. The boxing layer sent it as
`LABEL_REMOTE_REF` — a netref to the peer's dict. Caller's
`result` was thus a netref, not a Python value.

Then `if not result:` fired `bool(result)` which, on a netref,
calls `__bool__` via `syncreq(HANDLE_CALLATTR, '__bool__', ...)`
— a **synchronous RPC on the SAME event loop that was already
draining deletions**. Under recursive bidirectional async traffic,
both peers hit this at the same time and park in
`channel.poll` / `stream.write`:

```
File "rpyc/core/stream.py", line 288, in write
File "rpyc/core/channel.py", line 78, in send
File "rpyc/core/protocol.py", line 1238, in _send
File "rpyc/core/protocol.py", line 2074, in _async_request
File "rpyc/core/protocol.py", line 2092, in async_request
File "rpyc/core/protocol.py", line 2018, in sync_request
File "rpyc/core/netref.py", line 63, in syncreq
File "rpyc/core/netref.py", line 329, in method  # __bool__
File "rpyc/core/protocol.py", line 959, in _process_pending_deletions   # `if not result:`
File "rpyc/core/protocol.py", line 802, in cleanup_loop
```

The cleanup loop parks its own event loop waiting for a reply from
the peer — but the peer is in exactly the same state.

### Fix (`protocol.py::_handle_del`)

Return a brine-dumpable primitive:

```python
return bool(deleted)
```

`bool` goes over the wire as `LABEL_VALUE`. Caller receives a
plain Python bool. `if not result:` is a local C-level check. No
RPC, no deadlock.

### Regression traps

- **DO NOT** return a dict, list, namedtuple, or dataclass from
  `_handle_del`. Any non-primitive gets boxed as `LABEL_REMOTE_REF`
  and the truth-test will fire sync RPC.
- **DO NOT** "enhance" the ack by adding structured fields. If you
  genuinely need more information back from `_handle_del`, either:
  - Return a tuple of primitives: `(True, 0)` — brine-dumpable.
  - Define a new separate handler for the richer query.
- **DO NOT** remove the ``bool(...)`` cast — keep it as a
  defensive guarantee that the return type is always primitive
  even if `RefCountingColl.decref` ever changes to return
  something truthy-but-non-bool.

### Repro

Same as §3: `test_netref_recursive_async_calls`. Without this
fix AND §3 fix, the test reliably hangs. Both fixes are needed.

## 5. How the four symptoms overlap

The four symptoms interact, and early partial fixes showed
confusing reshuffled failure modes:

| State                                   | nested-AR | PASS | HANG | Notes                                       |
| --------------------------------------- | --------: | ---: | ---: | ------------------------------------------- |
| Master (all bugs present)               |      ~70% |    5 |    0 | nested-AR hides the deadlock (returns bad data fast) |
| Only §1/§2 fix (async_handlers)         |        0% |    4 |   16 | explicit `await` exposes the deadlock       |
| Only §1/§2 + §3 (`_box`) fix            |        0% |    6 |    9 | deadlock narrows to cleanup-loop self-deadlock |
| Full fix (§1/§2 + §3 + §4)              |        0% |   30 |    0 | all four bugs closed                        |

In particular: the master codebase "passes" some recursive tests
by returning garbage fast (nested-`AsyncResult` in the result
string instead of the integer, assertion fails quickly). After
§1/§2 the handler actually `await`s — which forces the
`iscoroutinefunction` probe to actually complete, which triggers
§1's deadlock. After §3, one common deadlock path closes but
§4's cleanup-loop deadlock remains. All four fixes are
independently necessary.

## 6. Empirical validation

30-iteration stability sweeps on the fixed branch:

* `test_netref_recursive_async_calls` — **30/30 PASS**, 0 HANG, 0 FAIL.
* `test_netref_identity_preserved` — **20/20 PASS**, 0 HANG, 0 FAIL.
* `test_netref_async_callback_basic` — **20/20 PASS**.
* `test_different_objects_get_different_netrefs` — **20/20 PASS**.

Aggregate over the three netref e2e files (all tests in each):
**20/20 PASS** / 0 HANG / 0 FAIL.

Full migrated-test suite (26 files, `async_connect` /
`async_server` / refcount / cleanup / policy):
**143 passed, 2 skipped** in 30 s.

Pre-existing failures **not** caused by these fixes (confirmed
identical behavior on master):

* `test_async_dispatch.py::test_dispatch_request_async_execution`
  — `_unbox(())` raises `ValueError: not enough values to unpack`.
* `test_refcount_errors_reproduction.py::*` — tests that call
  `sync_request(HANDLE_CALL)` from the asyncio loop, blocked by
  the `_USER_RPC_HANDLERS` guard in `sync_request` (the guard
  predates these fixes).

## 7. The rules (for future agents)

If you find yourself touching any of these files, read the
relevant section above first. If you are tempted to:

- "simplify" the ordering of branches in `_handle_async_call` →
  DON'T. §1, §2.
- remove the `try/except` around `object.__getattribute__` → DON'T.
  The `TypeError` branch catches pathological objects; removing
  it crashes on edge cases. §1.
- replace `_proxy_cache.get(id_pack) is obj` with a simpler check
  → DON'T. §3.
- randomize `_id_pack_seq` origin → DON'T; breaks
  `classic.connect_thread`. §3.
- make `_handle_del` return a richer structure → DON'T; use a
  tuple of primitives or a new handler. §4.
- remove the `bool(...)` cast in `_handle_del` → DON'T; defensive.
  §4.
- move the `inspect.iscoroutinefunction` call before the netref
  probe "for consistency" → DON'T; re-opens §1 and §2.

If a genuine new requirement demands reshaping any of these
paths, re-run the full repro matrix in §6 (30 iterations each)
and update the empirical table. If any number shifts, you have a
regression.

## 8. Related reading

- `docs/DESIGN_NESTED_ASYNC_RESULT.md` — earlier, narrower design
  doc focused on bug §2 only. This document supersedes it for
  cross-bug context; the older doc is retained for the correctness
  proof of the event-driven `AsyncResult.__await__` path.
- `docs/DESIGN_NO_SAME_PROCESS_TESTS.md` — why all `AsyncioServer`
  tests must run with the server in a separate `multiprocessing.Process`.
  Same-process topologies mask these bugs entirely.
- `docs/DESIGN_REFCOUNT_RACE_FIX_A.md` — why `id_pack`'s third
  slot is a stable monotonic seq (variant A). The collision
  described here in §3 is an **orthogonal** cross-process concern:
  variant A fixes intra-connection id() reuse, not cross-connection
  id_pack symmetry.
