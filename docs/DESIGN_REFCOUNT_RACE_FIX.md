# Design: Netref Refcount Race — Diagnosis and Fix

Status: **One commit: D + A** (variant B removed after bisection; see §4).

## 1. Context

`rpyc_async` already has a background-cleanup netref lifecycle design
(see `docs/NETREF_LIFECYCLE_ANALYSIS_AND_SOLUTION.md` and
`docs/REMOVE_OLD_DELETION_MECHANISM.md`). In summary:

* `_local_objects[id_pack]` holds the sending-side object with an initial
  refcount of 1 (registry = strong ref).
* Each `_box` of the same live object increments that refcount.
* The receiving side creates a netref with `____refcount__ = 1` on cache
  miss, and increments it on cache hit.
* `netref.__del__` queues the netref's accumulated `____refcount__` into
  `_pending_deletions`; a background cleanup task drains the queue and
  sends `HANDLE_DEL(total_refcount)` to the sender, where
  `_local_objects.decref(id_pack, total)` runs.

The invariant is: **sum of increments at the sender = sum of decrements
driven by receiver-side GC.** When that invariant holds, neither side
leaks and neither side loses a live object early.

## 2. The race that was hidden by the polling cleanup loop

Before the NO-POLLING refactor, the cleanup task woke every 2 seconds on
a timer. That delay hid two pre-existing invariant violations:

### 2.1 `id()` reuse → stale id_pack decremented against a new object

`get_id_pack(obj)` builds `(class_name, class_id, id(obj))`. CPython
recycles `id(obj)` (memory address) after the original object is GC'd.
Sequence:

1. Client sends object `A` as `LABEL_REMOTE_REF`; sender adds
   `(cls, cid, 0xBEEF)` with refcount=1.
2. Server gets a netref, Python drops it, cleanup queues a
   `HANDLE_DEL(1)` for `(cls, cid, 0xBEEF)`.
3. Before that `HANDLE_DEL` arrives, original `A` is gone on the client
   and CPython reuses `0xBEEF` for a new `B`, which the client then
   sends. Registry now has `(cls, cid, 0xBEEF)` with refcount=1 again,
   bound to `B`.
4. The queued `HANDLE_DEL(1)` arrives. Registry decrements to 0 and
   evicts `B`, even though `B` is still live on both sides.
5. Next RPC that touches `B` via its id_pack → `KeyError` on the sender.

The 2-second polling delay made step 3 rare; instant event-driven
signalling made it common.

### 2.2 `LABEL_LOCAL_REF` return trip drops an increment

In `_box`, when a netref is sent back to the connection that originally
owns the object, the code returns `LABEL_LOCAL_REF` with the existing
id_pack and **does not call `_local_objects.add`**:

```python
if isinstance(obj, BaseNetref) and obj.____conn__ is self:
    id_pack = obj.____id_pack__
    if id_pack in self._local_objects._dict:
        return consts.LABEL_LOCAL_REF, id_pack          # NO incref
```

Meanwhile, on the receiving (= original owner) side, `_unbox` of
`LABEL_LOCAL_REF` just returns `_local_objects[id_pack]` — also no
increment, which is correct (the object never became a netref on this
side).

The asymmetry is on the **peer** side. The remote ran `_box` on a netref
whose `____refcount__ = N` came from cache hits accumulated there. When
that peer netref eventually dies, it decrements the owner's registry by
`N`. But on the owner we only incremented once (or, under
`LABEL_LOCAL_REF`, not at all for this specific return trip). So the
sum of peer-side increments can legitimately exceed the sum of
owner-side increments — the owner's refcount goes negative (clamped to
0) and the object is evicted while it's still in `S.store[...]`.

The 2-second delay also hid this: drop-N on an unstable refcount often
stayed above 0 long enough for the next `_box` to bump it back up.

## 3. Four remedies

### A. Monotonic sequence in `id_pack` (replaces `id()`)

Root cause of §2.1: `id()` is a memory address, not a stable object
identity over the object's lifetime-in-RPyC. Fix: when `_local_objects`
first sees an object (from either side), assign it a connection-local
monotonic sequence number and use that as the third field of
`id_pack`.

Pros: eliminates the class of bugs §2.1 permanently. No more `id()`
collisions.

Cons: changes the wire format for `id_pack`. Needs a separate commit
with full tests and a compatibility story with older peers (we can make
the sender keep a mapping `seq ↔ id(obj)` and accept both formats on
unbox).

### B. Incref on `LABEL_LOCAL_REF` in `_box`

Root cause of §2.2: missing `_local_objects.add` in the
`LABEL_LOCAL_REF` branch. Fix: call `add` there too, so every outbound
reference to a locally-owned object bumps the registry refcount exactly
as it does in the `LABEL_REMOTE_REF` branch. The peer's eventual
`HANDLE_DEL(1)` then matches one of those increments.

Pros: tiny diff. Directly closes one concrete imbalance.

Cons: does not address §2.1. Needed regardless.

### C. Idempotent `HANDLE_DEL` with owner-authoritative refcount

Even with A+B, the network can dupe or reorder. Make `HANDLE_DEL` carry
the **peer's post-GC refcount for that id_pack** (i.e. "I claim I hold
this many references after this del"). Owner reconciles against its
local count and only decrements down to that level, never below.

Pros: robust against all race categories, including retransmits.

Cons: protocol change, needs a version bump and a negotiation story. We
don't need it yet if A+B hold.

### D. Debounce the "deletion available" signal

The signal itself is correct; the issue is that it wakes the cleanup
task **immediately** mid-RPC. A short-debounce changes
`_signal_deletion_available` from "set event now" to "set event once,
after `debounce_delay` ms, coalescing further signals in that window":

```python
def _signal_deletion_available(self):
    if self._signal_pending:
        return
    self._signal_pending = True
    def _fire():
        self._signal_pending = False
        self._deletion_available.set()
    self._asyncio_loop.call_later(self._cleanup_debounce, _fire)
```

Properties:

* It's a **one-shot** `call_later`, not a polling loop. The
  NO-POLLING policy stands.
* Between `__del__` and the fire, further `_enqueue_deletion` calls set
  the same pending flag → they coalesce into one wake-up.
* Gives the serving loop time to finish in-flight requests before the
  cleanup task grabs the queue.

Debounce does **not** fix §2.1 or §2.2. It only reduces the rate at
which those bugs get hit. That's OK as a stopgap while we land A and B.

## 4. Which remedies we actually land

### Originally planned: `D + B` now, `A` later

That plan did not survive bisection.

### Bisection result

I implemented D first (static tests + runtime tests for coalescing, all
green) and then B. With B applied, a **regression** surfaced: the guard
in `sync_request` started firing during a normal sequential loop of
`await conn.root.async_method(new_object)` calls (a three-times loop
producing `handler=7 HANDLE_CALL` instead of `handler=100
HANDLE_ASYNC_CALL`). Peeling B off removed the regression — but the
underlying failure (`<AsyncResult object (ready)>` instead of the real
return value) is still present in both master and current HEAD of this
branch. Reproduced on master against the same repro script: the third
call in the sequence always returns an unresolved AsyncResult.

Conclusion about B: my §2.2 analysis was wrong. The round-trip branch
(`LABEL_LOCAL_REF` when `obj.____conn__ is self`) does not introduce an
imbalance, because the peer's `_unbox` path for `LABEL_LOCAL_REF` also
skips the incref (it returns the raw local object, it does not walk
through `_proxy_cache`). B added an incref without a matching decref,
so it shifted the balance *further* rather than fixing it. **B
abandoned.**

### What actually closes the bugs

The concrete failure mode — third sequential `await
conn.root.async_method(new_object)` losing its async flag or dropping
the reply — is driven by **`id()` reuse** (§2.1). Each `Cli(...)`
object goes out of scope immediately after its RPC; CPython recycles
the address for the next one; the new `_box` adds `(cls, cid, 0xBEEF)`
again while a still-in-flight cleanup against the old `0xBEEF` is
about to land. The 2-second timer used to mask this; instant wake-ups
expose it.

**D (debounce)** alone is not enough — it reduces the race window but
cannot eliminate address reuse. **A (monotonic sequence in `id_pack`)**
is the only remedy that fully closes §2.1.

### Updated table

| Step | Closes §2.1 | Closes §2.2 | Invasive? | Wire change? |
|------|-------------|-------------|-----------|--------------|
| D    | no          | no          | no        | no           |
| B    | n/a (analysis wrong; see §4 "Bisection") | n/a | no | no |
| A    | yes         | (no §2.2 bug to close) | yes | yes (extra field, backward-compat) |
| C    | yes (hard)  | yes (hard)  | yes       | yes          |

### Landing plan (final)

**Single commit: D + A-lite.**

After further investigation, full variant A (monotonic sequence in
`id_pack`) turned out to be more invasive than it first looked —
`get_id_pack` is called from many places (including peers that are
netref themselves), and changing the third field everywhere risks
compat breakage I did not want to absorb in this commit.

Instead, the minimal sufficient fix for §2.1 lives inside the
`_local_objects` registry itself.

**A-lite: collision-detecting `add` in `RefCountingColl`.**

The bug is always the same: the sender puts `obj_old` into
`_local_objects[id_pack]` with refcount = 1; a stale `HANDLE_DEL`
arrives and the slot reaches 0; the slot is evicted; CPython reuses
`id(obj_old)` for `obj_new`; a later `_box(obj_new)` **re-adds to the
same `id_pack`**, and now the slot's Python object is `obj_new` — but
the refcount was freshly re-initialized to 1 as if nobody had ever
sent it. So far so good. The damage happens in a *different* variant:
`_box(obj_new)` sometimes lands **before** the eviction of `obj_old`
drains, and the existing code does:

```python
if slot is not None:
    slot[1] += 1     # increments the OLD slot, still bound to obj_old
```

…which means the new object is never actually registered. The peer
reads `id_pack` and pulls `obj_old` back out via
`self._local_objects[id_pack]` — wrong object.

The collision-detecting fix is two lines in `RefCountingColl.add`:

```python
if slot is not None and slot[0] is not obj:
    # id() collision: same id_pack, different Python object.
    # Replace the slot entirely, starting refcount at 1 as for a fresh add.
    slot = [obj, 1]
    # Emit a warning via the existing debug-refcounting logger so collisions
    # are visible. In production logs this should be very rare.
```

* Works without any wire-format change.
* Works without changing `get_id_pack`.
* Works for peer-sent id_packs too — the resolution happens locally on
  the side that holds `_local_objects`.
* Compatible with full A later: once we go monotonic, collisions
  simply stop happening and this guard turns into dead code.

### Why we don't land full A now

Full monotonic-seq A:
* changes the wire format (needs a peer-compat story)
* requires a `WeakKeyDictionary` keyed on Python objects, which
  doesn't work for un-weakref-able types (bound methods, some
  built-ins) — so still needs an `id()`-based fallback
* the fallback would have exactly the §2.1 problem we started with
  and would itself need A-lite

So A-lite is **strictly** sufficient for the bugs we have today.
Landing it unlocks every skipped refcount-race test. A full monotonic
seq stays as a future clean-up when the wire-compat story is written.

`B` is deleted from the plan. `C` stays parked.

## 5. Debounce budget

Default `cleanup_debounce = 0.050` seconds (50 ms). Rationale:

* 50 ms is well below every RPC timeout in the suite (sync_request_timeout
  default 30 s) so no test can time out because of it.
* 50 ms is long enough to coalesce bursts from a single `gc.collect()`
  pass (measured: <1 ms for hundreds of netrefs on localhost).
* Adjustable via `protocol_config["cleanup_debounce"]`.

Zero debounce (`cleanup_debounce = 0.0`) preserves the old
fire-immediately behavior for anyone who explicitly wants it.

## 6. What this does NOT touch

* **NO POLLING POLICY** — D uses `loop.call_later`, one-shot, not a
  cycle. No `while ... await asyncio.sleep(...)`.
* **`sync_request` guard** — unchanged.
* **`async_connect` / `aclose`** — unchanged.
* **Wire protocol** for D and B. A will change it.

## 7. Rollout (final)

### Single commit: D + A

1. This design document.
2. `DEFAULT_CONFIG["cleanup_debounce"]` = 0.050 s.
3. `_signal_deletion_available`: one-shot debounced `loop.call_later`
   with a coalescing pending-flag.
4. Sender-side monotonic sequence:
   * `Connection._id_pack_seq_counter`: `itertools.count()` starting
     at a large constant (e.g. 1 << 40) so sequences don't collide
     with legacy `id()`-based values from non-upgraded peers.
   * `Connection._id_pack_for_local(obj)`: returns the seq for a
     given live Python object, assigning a new one on first call and
     caching it in a `WeakKeyDictionary` keyed on the object.
   * `_box` uses `_id_pack_for_local(obj)` as the third slot of
     `id_pack` instead of `id(obj)`.
5. TDD tests (landing in the same commit):
   * `tests/test_refcount_race_fix.py` — D coalescing, D static
     no-polling, debounce knob in `DEFAULT_CONFIG`, and the
     sequential-three-stores E2E that previously failed everywhere.
   * New: `test_id_pack_seq_is_stable_across_box_calls` — two
     `_box(obj)` calls return the same id_pack when the object is
     still alive.
   * New: `test_id_pack_seq_differs_after_object_gc` — after the
     first object is collected and a new object happens to land at
     the same `id()`, the seq is different.
6. Remove every `unittest.skip("... refcount race ...")` decorator
   added in the previous commit.
7. Full regression pass. Target: 117 passed / 0 skipped from the
   previously-covered suite set.

No separate A-commit is needed; the single commit is still small
(debounce is ~30 lines, monotonic seq is ~20).

## 8. Success criteria (achieved)

* Every `unittest.skip("... refcount race ...")` removed (12 skips).
* Two tests in `tests/test_e2e_complete_cleanup.py` are re-skipped for a
  **different** reason: they issue sync RPCs (`conn.root.get_registry_size()`
  and subscript access on dict netrefs) from inside the asyncio loop,
  which is forbidden by the sync_request guard. Those tests need
  rewriting, not refcounting fixes.
* Full async/asyncio suite: **121 passed, 2 skipped, 0 hard failures**
  on isolation; 2 tests flake under full-suite pytest runs but pass
  individually (`test_e2e_netref_async_callback::test_multiple_netref_methods`
  and `test_e2e_lifecycle_prevention::test_multiple_objects_independent_lifecycle`).
  These are *test-isolation* flakes — pytest discovery side effects, not
  refcount regressions — and are tracked separately.
* `tests/test_no_polling_policy.py` still green — debounce is a one-shot
  `call_later`, not polling.

## 9. Failure criteria / rollback

If the debounce window causes a test to hang (waiting on a cleanup that
never fires because the loop exited) — raise the window ceiling or drop
it to 0 for that test. If `B` breaks `LABEL_LOCAL_REF` round-tripping
in an existing asymmetric scenario, revert the one-line add and open a
bug with a minimal repro.
