# Design: PID-namespaced `id_pack` — kill cross-process collisions at
# the source; drop the collision-resolution shortcut in `_unbox`

Status: **Proposed**. Supersedes the "keep the `_unbox` shortcut for
`classic.connect_thread`" compromise in
[`DESIGN_BIDIRECTIONAL_ASYNC_FIXES.md`](./DESIGN_BIDIRECTIONAL_ASYNC_FIXES.md) §3.

## 1. The bug we are closing

Inbound callback on this project infinitely ping-pongs between two
processes and causes unbounded memory growth on **both** sides. Minimal
repro on a fresh `AsyncioServer` + `async_connect` pair where the web
side registers `MessageCallbackService` with the agent side via
`subscribe_messages(callback)`. Later, the agent calls
`callback.on_message("status_change payload")`. Occasionally (observed
stable when first-boxed bound methods happen to line up), the call is
resolved against the wrong object on the receiver side, that object's
call returns a value that triggers the same pattern again, and the two
peers chase each other.

Observed live: web-side RSS grew from 3 MB to 10.5 GB in roughly
6 minutes, agent-side RSS doubled from ~1 GB to ~3.5 GB in the same
window. Every `py-spy dump` of the web during the leak window shows
the call stack `_dispatch_request_async → _handle_async_call →
netref.__call__ → asyncreq → _async_request → _send` with `obj` a
netref carrying `id_pack=("builtins.method", 10665440, 1099511627777)`
and `args=("{"type": "status_change", ...}")`. Id-packed bound method
that we are about to call remotely is *our own*
`MessageCallbackService.exposed_on_message` — received back as if it
were an agent-side object.

## 2. Why it happens

`id_pack` is a 3-tuple `(name_pack, id(type(obj)), seq)` that lives
on the wire as the identity of a remote-referenced object. Every
wire-level reference carries one of two tags:

* `LABEL_LOCAL_REF` — "peer, this is **your** local; look it up in
  your own `_local_objects`".
* `LABEL_REMOTE_REF` — "peer, this is **my** local; hold a netref
  back to me".

Both tags use the same `id_pack` shape. The shape is **not
globally unique between peers**:

| slot | content | cross-process behaviour |
| ---- | ------- | ----------------------- |
| 0 | `name_pack = f"{module}.{class_name}"` | **Identical** whenever two processes happen to box an object of the same class (trivially true for builtin classes — `"builtins.method"`, `"builtins.dict"`, `"pathlib.PosixPath"` — and also for any user class that both sides have in their modules, including the classes of RPyC itself). |
| 1 | `id(type(obj))` — address of the type object in process memory | **Identical for builtin types across processes** spawned from the same CPython binary. CPython loads builtin type objects from a fixed offset in the executable's data segment. In Python 3.12 on this host, `id(builtins.method) == 10665440` in every process — deterministically. |
| 2 | seq from `itertools.count(start=1 << 40)` — allocated by `_alloc_stable_obj_id` (variant A, see [`DESIGN_REFCOUNT_RACE_FIX_A.md`](./DESIGN_REFCOUNT_RACE_FIX_A.md)) | **Deterministic** per-connection counter, starts at the same `1<<40` value on every connection in every process. Two peers that box their N-th bound method at the "same time" get the **same** seq. |

The receive-side shortcut in `_unbox(LABEL_REMOTE_REF)`
(`protocol.py:1495`) was — and still is — written to paper over a
different feature: in `rpyc.classic.connect_thread()` two connections
live in the *same* process sharing a single Python heap, and this
shortcut is the mechanism that preserves object identity when an
object round-trips between the two halves of the pair. The shortcut
returns the receiver's *own* local object whenever `id_pack` happens
to be present in its `_local_objects`.

In `classic.connect_thread`, id_pack collision is *by design*. In
every other topology (two separate processes over TCP, which is what
`AsyncioServer` + `async_connect` is used for), collision is
*catastrophic*. [`DESIGN_BIDIRECTIONAL_ASYNC_FIXES.md`](./DESIGN_BIDIRECTIONAL_ASYNC_FIXES.md) §3
plugged one half of the collision hazard (the send-side in `_box`,
via the new `_proxy_cache.get(id_pack) is obj` disambiguator). It
explicitly left the **receive-side** shortcut in place, arguing:

> "A collision on the receive side can still produce 'wrong object
> returned', but without the ping-pong it just results in a benign
> wrong-method-call that either raises a clear error or returns
> wrong data — caught by test assertions, not a hang."

This argument is incorrect for the case where the *result* of the
wrong call re-packs yet another bound method with a colliding
id_pack: the wrong-method-call becomes the driver of a sustaining
ping-pong. Empirically verified — see §1.

## 3. Design decision

Make `id_pack` **globally unique between processes by construction**
so that the receive-side shortcut cannot false-positive. Delete
`classic.connect_thread` along with its only test, because this
project does **not** and **will not** use one-process client+server
topology — that constraint is already documented in
[`DESIGN_NO_SAME_PROCESS_TESTS.md`](./DESIGN_NO_SAME_PROCESS_TESTS.md)
for the test suite, and we now extend it to production code: the
client+server-in-one-process mode is removed entirely.

The new seq allocation rule:

```python
seq_start = (os.getpid() << 32) + 1
self._id_pack_seq = itertools.count(start=seq_start)
```

### Why this works

* **Live process uniqueness.** The kernel guarantees no two *live*
  processes share a PID. Different live PIDs → different
  `seq_start` → disjoint seq ranges of size `2**32` each. Two
  independent peers **cannot** produce the same seq, therefore the
  third slot of `id_pack` is guaranteed different.
* **id_pack uniqueness follows.** A tuple is equal iff every
  element is equal. With slot 2 guaranteed different between live
  peers, the whole tuple is guaranteed different. The receive-side
  shortcut `id_pack in self._local_objects._dict` cannot
  false-positive on a peer's id_pack — the shortcut returns the
  local object only when the id_pack is genuinely ours.
* **PID reuse is not a concern** in our topology. A PID is reused
  only after the previous process exited. If that previous process
  was an rpyc peer, its TCP connection is already dead and any
  netrefs the current process held against it are defunct. If the
  reused PID belongs to a completely unrelated process, it never
  enters our rpyc graph.
* **Same-process multiple connections still align.** All
  connections inside one process share the same `seq_start`, so
  two in-process connections (should they ever exist — but see §5)
  allocate seqs from the same stream. This preserves the classic
  `connect_thread` alignment property as a side effect, not that we
  rely on it.

### Why `pid << 32` specifically

PID on Linux is at most 22 bits wide (`kernel.pid_max` ≤ `2^22` on
all reasonable configurations, typically `2^16` or `2^15`). Shifting
by 32 gives:

* Each process gets a private range of `2^32 ≈ 4.3 billion` seqs
  before overflowing into the next process's range. No realistic
  connection churn approaches this.
* The result fits into a signed 64-bit int even for `pid_max = 2^22`
  (`2^22 << 32 = 2^54`), leaving 10 bits of headroom. Python ints are
  unbounded so this is only a wire-encoding concern, and `brine`
  handles arbitrary ints; the 64-bit bound is maintained for
  readability of diagnostic traces, not correctness.
* Log grep'ability: a seq is trivially factorable as `seq >> 32 == pid`,
  which is useful when reading a cross-process trace.

We deliberately do **not** use a random offset (`os.urandom(4)` per
connection). Random offsets would also work for cross-process
disambiguation but would make logs hostile to manual correlation and
break the (now-deleted, see §5) single-process classic mode. PID is
the cleaner primitive — it *is* the process identity.

## 4. Remove the collision-resolution shortcut

Once id_pack is unique, the `_unbox(LABEL_REMOTE_REF)` shortcut is
*correct without the collision hazard* — but also unnecessary for
safety. We keep the shortcut because it's a genuine performance
optimization for "peer is sending our own id_pack back" (mostly the
case where an object round-trips through an RPC call); removing it
would force every receiver to build a netref that immediately
resolves on first use. Not a regression in terms of correctness,
but a small throughput tax.

However, we make the shortcut explicit about what it's doing and
add a defensive invariant: the retrieved object's `id_pack` slot 2
must match our `pid`, and we assert this in debug mode:

```python
if id_pack in self._local_objects._dict:
    owner_pid = id_pack[2] >> 32
    if owner_pid != os.getpid():
        # With PID-namespaced seq this cannot happen. If it ever
        # does, the allocator is buggy — fail loudly instead of
        # silently returning the wrong object.
        raise ValueError(
            f"id_pack {id_pack} matches a local slot but slot[2] "
            f"encodes pid={owner_pid}, not our pid={os.getpid()}"
        )
    return self._local_objects[id_pack]
```

The check is cheap (one shift + compare) and runs only on the
shortcut-hit path.

## 5. Delete `rpyc.classic.connect_thread`

It is incompatible with PID-namespaced seq (both connections in one
process now share a `seq_start`, so in-process object-identity
preservation through id_pack collision no longer works as that
legacy feature expected).

Paths to remove:

* `rpyc/utils/factory.py::connect_thread` (≈20 LOC, plus
  `_server` / `spawn` helper wiring if they become dead).
* `rpyc/utils/classic.py::connect_thread` (wrapper).
* `rpyc/__init__.py` — drop `connect_thread` from the re-export.
* `tests/test_refcount.py` — the one consumer. Delete the file; its
  refcount invariants are already covered by
  `tests/test_e2e_netref_*.py` and `tests/test_async_boxing.py`
  against real multiprocess topology.

The `DESIGN_NO_SAME_PROCESS_TESTS.md` document is updated with a
note that this applies to production code too, not only tests.

## 6. What changes in `protocol.py`

One-line swap in `Connection.__init__`:

```diff
-        self._id_pack_seq = itertools.count(1 << 40)
+        # PID-namespaced start: see docs/DESIGN_PID_NAMESPACED_ID_PACK.md.
+        # Two live processes have different PIDs by kernel guarantee,
+        # therefore their seq streams are disjoint, therefore their
+        # id_pack tuples are globally unique.
+        self._id_pack_seq = itertools.count(start=(os.getpid() << 32) + 1)
```

All three occurrences of the `1 << 40` constant in documentation
(`DESIGN_REFCOUNT_RACE_FIX_A.md` §2, §5, passim) get updated
references and cross-links to this document.

The defensive assert in the `_unbox(LABEL_REMOTE_REF)` shortcut
(§4 above) is gated by `self._config.get("debug_refcounting", False)`
to keep it out of the hot path in production. With that flag on
(and the flag is on in test suites), a collision hazard would
produce a loud `ValueError` rather than the silent wrong-object
return we have now.

No changes to:

* `LABEL_LOCAL_REF` / `LABEL_REMOTE_REF` semantics on the wire.
* `_box` (the existing send-side disambiguator via
  `_proxy_cache.get(id_pack) is obj` stays — it's correct regardless
  of whether id_pack is globally unique, and it's the faster path).
* `_alloc_stable_obj_id` internals — only its seed changes.
* `name_pack` / `id(type(obj))` — those slots are kept for their
  original purposes (netref class synthesis, `_handle_instancecheck`
  cache key, etc.). They don't need to carry identity; slot 2 does.

## 7. Test plan

### New tests (TDD, red before fix)

1. **`test_id_pack_seq_starts_with_pid.py`**. Spawn a `Connection`
   and assert `conn._id_pack_seq` first value is `(os.getpid() << 32) + 1`.

2. **`test_id_pack_unique_across_processes.py`**. Multiprocess:
   start two subprocess rpyc servers, have each box a bound method,
   retrieve the id_packs, assert the slot-2 halves don't overlap.
   Assert `(id_pack[2] >> 32) == subprocess_pid` on each side.

3. **`test_cross_process_bound_method_callback_no_ping_pong.py`**.
   The regression test that reproduces the leak. Spin up an
   `AsyncioServer` in a subprocess, have a client register a
   callback service and trigger a path where the server calls
   `callback.on_message("payload")` in a loop 100 times. Measure
   client-process RSS before/after with `resource.getrusage`. Assert
   RSS grew by less than 50 MB (current leak produces GB/min).

4. **`test_shortcut_sanity_check.py`** (debug mode). With
   `debug_refcounting=True`, feed `_unbox` a hand-crafted
   `LABEL_REMOTE_REF, id_pack_with_wrong_pid_prefix` that collides
   with a local slot. Assert `ValueError` with the diagnostic
   message.

### Existing tests that must stay green

* All `tests/test_async_*.py` (async dispatch pipeline unchanged).
* All `tests/test_e2e_netref_*.py` (bidirectional netref behaviour
  unchanged in the normal case — only the collision-hazard path
  changes, and no existing test depends on that hazard for
  correctness).

### Existing tests that will be deleted

* `tests/test_refcount.py` — only user of `connect_thread`. Its
  invariants are re-covered by the multiprocess tests above; the
  `connect_thread`-specific parts are dead.

## 8. Rollout

Single commit. No feature flag, no migration window. PID-namespaced
seq is a pure internal change from the allocator's perspective;
nothing on the wire changes except the numeric value of slot 2.

Peers running the old rpyc_async (seq starting at `1<<40`) and peers
running the new one (seq starting at `pid<<32`) can still
interoperate: the slot 2 values are still brine-encodable ints, the
tuple comparison is unchanged, and the shortcut on each side resolves
against its own local state — it just resolves correctly now instead
of false-positively. No version negotiation needed.

However: **while old peers are in the graph, they can still emit
colliding id_packs** at their end — the new peer protects itself,
but the old peer can still be deceived. Pragmatic plan: land this
fix in rpyc_async, then ensure all processes that use the library
are restarted with the new code before declaring the leak closed.
In a typical deployment this means every server process and
client process that uses the library.

## 9. Rollback

Single commit; `git revert` restores the `1 << 40` seed and
un-deletes `connect_thread`. No data format change, no on-disk
state touched. The revert reintroduces the leak.

## 10. Out of scope

* `LABEL_*` tag values and semantics. Keeping them for wire
  backwards compatibility with upstream rpyc peers (should any
  ever need to connect), explicitness of protocol intent, and ease
  of debugging. Semantically they become derivable from id_pack
  ownership once uniqueness is guaranteed, but sending them
  costs one byte and removing them is a separate wire-format
  change with no operational payoff.
* `id(type(obj))` in slot 1. Kept for its original uses (netref
  class cache key, instancecheck). It doesn't participate in
  identity once slot 2 is unique.
* Random-offset alternative. Discussed in §3, rejected: PID is the
  right primitive (it's the process identity; random is noise).
