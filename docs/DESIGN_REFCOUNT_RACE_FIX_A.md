# Design: Full variant A — monotonic `id_pack` sequence

Status: **Landing now**. Replaces the A-lite guard introduced in the
previous commit. See the parent design `DESIGN_REFCOUNT_RACE_FIX.md`
for context.

No backward compatibility concerns: this project is used only on this
host.

## 1. Motivation

A-lite (collision detector in `RefCountingColl.add`) patches the
**symptom** of `id()` reuse — if a reused address lands in the registry
while a stale slot was still there, A-lite rebinds the slot to the new
object. It works in practice, but:

* The collision is still *happening*. Every rebind is a latent bug that
  we only survive because we detect it.
* The logged warning `[REFCOUNT] id() COLLISION on ...` fires in normal
  operation, which is noise.
* Third-slot equality (`id_pack[2] == 0` means "class", `!= 0` means
  "instance") is currently a convention that piggy-backs on the fact
  that `id()` is never 0. Once we use monotonic sequences, we can keep
  that convention explicitly.

Variant A eliminates the collision at the source: the third slot of
`id_pack` becomes a **connection-local monotonic sequence** that is
stable for the lifetime of the Python object, and is never reused after
the object is collected.

## 2. Contract for `id_pack[2]`

| Object kind                | Third slot                                  |
|----------------------------|---------------------------------------------|
| class / type (module-level)| `0` (preserved from the old contract)       |
| instance, weakref-able     | connection-local seq, stable for obj's life |
| instance, not weakref-able | connection-local seq, kept alive by the     |
|                            | `_local_objects` strong ref                 |

> **Updated 2026-04-25:** the fixed ``start=1 << 40`` origin below was
> replaced by a PID-namespaced seed ``(os.getpid() << 32) + 1``. See
> [`DESIGN_PID_NAMESPACED_ID_PACK.md`](./DESIGN_PID_NAMESPACED_ID_PACK.md)
> for the rationale. The old constant survived only as long as there
> was one process involved; once two independent processes needed
> globally-unique id_packs, the fixed seed became the direct cause of
> the cross-process ping-pong leak, and a deterministic PID-namespaced
> seed replaces it. The rest of this document describes the allocator
> contract (which is unchanged) — only the starting value moved.

The seq is drawn from `itertools.count(start=1 << 40)`. The high
starting value:

* keeps seqs distinct from any legitimate CPython `id()` on 64-bit
  (we never match an outgoing seq against an incoming legacy `id()`,
  but the separation makes mixed traces readable),
* leaves room for billions of seqs per connection,
* still fits easily in a Python int (no performance concern).

## 3. Allocator

`Connection._alloc_stable_obj_id(obj)` is the single entry point.

```python
def _alloc_stable_obj_id(self, obj) -> int:
    # Classes keep id_pack[2] == 0 — old contract.
    if inspect.isclass(obj):
        return 0
    # Fast path: already-seen weakref-able object.
    try:
        return self._obj_to_seq_weak[obj]
    except (KeyError, TypeError):
        pass
    except AttributeError:
        # Some objects raise AttributeError from __eq__/__hash__ when
        # hashed before fully constructed — fall through.
        pass
    # Weakref-able path
    try:
        weakref.ref(obj)
    except TypeError:
        weakrefable = False
    else:
        weakrefable = True
    if weakrefable:
        try:
            seq = self._obj_to_seq_weak[obj]
        except KeyError:
            seq = next(self._id_pack_seq)
            self._obj_to_seq_weak[obj] = seq
        return seq
    # id-fallback path for un-weakref-able objects (bound methods,
    # some built-ins). The registry already holds a strong ref through
    # _local_objects, so we can key the fallback map by id(obj) and
    # have _local_objects-driven cleanup keep it in sync.
    py_id = id(obj)
    entry = self._obj_to_seq_by_id.get(py_id)
    if entry is not None and entry[0] is obj:
        return entry[1]
    # Collision or first-seen.
    seq = next(self._id_pack_seq)
    self._obj_to_seq_by_id[py_id] = (obj, seq)
    return seq
```

Data structures on `Connection`:

* `_id_pack_seq: itertools.count` — monotonic counter.
* `_obj_to_seq_weak: WeakKeyDictionary[obj, seq]` — stable seq for
  weakref-able objects. Automatic cleanup when the object dies.
* `_obj_to_seq_by_id: dict[int, (obj, seq)]` — id-fallback for
  un-weakref-able objects. Because `_local_objects` holds a strong ref
  while the seq is meaningful (registry refcount ≥ 1), `id()` reuse
  cannot hit this map with a live entry.

### Why the id-fallback is safe

The only risk would be: a bound method at id `X` gets seq `17`; the
method goes away; CPython reuses `X` for a different bound method; we
look up `X` and hand out seq `17`, bound now to the wrong object.

This can't happen because the **registry keeps the original object
alive as long as its seq is reachable**:

* `_box(obj)` → `_alloc_stable_obj_id(obj)` returns seq `17`.
* Same call → `_local_objects.add((cls, cid, 17), obj)` → slot
  `[obj, refcount=1]`. Strong ref on `obj`.
* While that slot exists, `obj` cannot be collected, therefore `id(obj)`
  cannot be reused.
* When `_handle_del` drives the refcount to 0, the slot is removed,
  `obj` becomes collectable — and **at the same point** we must also
  remove the entry from `_obj_to_seq_by_id`. See §4.

### Mapping cleanup

In `_handle_del` (receiver of `HANDLE_DEL`, i.e. the local-owner side):

```python
def _handle_del(self, obj, count=1):
    id_pack = obj  # obj is the id_pack tuple
    deleted = self._local_objects.decref(id_pack, count)
    if deleted:
        self._forget_stable_obj_id(id_pack)  # NEW
    ...
```

`_forget_stable_obj_id` drops the entry from `_obj_to_seq_by_id` by
scanning for matching seq (or keeping a reverse `seq → py_id` map; the
scan is fine — this list is bounded by the number of live
un-weakref-able boxed objects per connection).

For weakref-able objects there is nothing to do: `WeakKeyDictionary`
evicts on its own when the object is collected.

## 4. `_box` wiring

Replace the direct `id()`-based builder with the allocator:

```python
def _box(self, obj):
    ...
    elif isinstance(obj, netref.BaseNetref) and obj.____conn__ is self:
        id_pack = obj.____id_pack__
        if id_pack in self._local_objects._dict:
            return consts.LABEL_LOCAL_REF, id_pack
        # (non-LOCAL_REF branch unchanged — falls through to else)
    else:
        # Build id_pack with a stable seq, not id(obj).
        obj_cls = getattr(obj, '__class__', type(obj))
        if inspect.isclass(obj):
            name_pack = f'{obj.__module__}.{obj.__name__}'
            id_pack = (name_pack, self._alloc_stable_obj_id(obj), 0)
        elif inspect.ismodule(obj):
            name_pack = _module_name_pack(obj)  # same helper as before
            id_pack = (name_pack, id(type(obj)), self._alloc_stable_obj_id(obj))
        else:
            name_pack = f'{obj_cls.__module__}.{obj_cls.__name__}'
            id_pack = (name_pack, id(type(obj)), self._alloc_stable_obj_id(obj))
        self._local_objects.add(id_pack, obj)
        ...
```

Key invariant preserved: **`id_pack[2] == 0` iff `obj` is a class**.
Everything else uses the allocator, which returns a non-zero seq.

Note: `id(type(obj))` (the second slot) is still the legacy address of
the *class object*. Classes are module-level and typically live
forever, so `id(type(obj))` is stable in practice. If class recycling
ever becomes an issue we'd promote the second slot to a seq too — not
today.

`get_id_pack()` in `rpyc/lib/__init__.py` stays as is. It's used at
module import time for the builtin-classes cache (classes → stable
`id()` anyway) and in `spawn()` for thread naming. Neither path hits
the race.

## 5. What A-lite becomes after A

Dead code. Removed in the same commit:

```python
elif slot[0] is not obj:
    # id() collision …
    slot = [obj, 1]
```

An explanatory comment stays at the call site so the next reader knows
why A-lite existed and why it's gone.

## 6. Debounce after A

Full A closes the concrete race A-lite worked around. Debounce
(variant D) remains in the tree as a one-shot `loop.call_later`
coalescer — useful for reducing HANDLE_DEL fan-out under heavy churn —
but the default drops from 50 ms to `0.0` (fire immediately). Users
who see pathological HANDLE_DEL floods can still tune it back up.

Why drop the default:

* The original 50 ms was chosen to paper over the id() race. Once A
  removes the race, there's no correctness reason for it.
* Firing immediately has better deletion latency, which benefits
  tight memory budgets and tests that assert prompt cleanup.
* The code path is still there; the behaviour is just opt-in now.

## 7. Test plan (TDD)

Red-phase tests in `tests/test_refcount_race_fix_full_a.py`:

1. **Stable seq across multiple `_box` calls on the same live object.**
   Two `_box(obj)` calls return the same third slot while `obj` is
   alive.
2. **Different seq after GC and id() reuse.** Box `a`, drop `a`, force
   GC, box `b` that happens to land at the same `id()` — third slots
   must differ.
3. **Classes keep third slot = 0.** `_box(SomeClass)` → `id_pack[2] == 0`.
4. **Un-weakref-able object path.** Box a bound method (no weakref
   possible) twice on the same bound method; seq stable until the
   method goes out of scope and its `_local_objects` slot is evicted.
5. **`RefCountingColl.add` no longer needs the A-lite guard.** Same
   unit test we had for A-lite, but now flipped: `add(key_with_seq,
   a); add(key_with_seq, b)` on the **same key** now only happens for
   two actually-equal objects (caller bug) — we fall through to the
   old behavior `slot[1] += 1`; the A-lite warning is gone.
6. **E2E regression** — the previously-skipped three-store test stays
   green without A-lite.

## 8. Rollback

This is a single commit. Reverting it restores A-lite + debounce. No
intermediate state needed.

## 9. Out of scope

* Propagating the seq to `get_id_pack()` callers outside of `_box` —
  not needed (`spawn()`, builtin-classes cache are not in the race).
* Keeping C (idempotent HANDLE_DEL) parked as before.
