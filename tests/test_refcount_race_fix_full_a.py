"""
TDD tests for full variant A — monotonic id_pack sequence.

See ``docs/DESIGN_REFCOUNT_RACE_FIX_A.md``.

Contract under test
-------------------
* ``Connection._alloc_stable_obj_id(obj)`` exists, returns an int, and
  the int is **stable for the lifetime of obj** on a given connection.
* ``_box`` uses the allocator in place of ``id(obj)`` for the third
  slot of ``id_pack``:
    - classes → 0 (old contract preserved)
    - instances → non-zero, connection-local monotonic seq
* Two ``_box`` calls on the same live object return the same id_pack.
* After the object dies and CPython recycles ``id()``, a new object at
  the same address gets a **different** seq.
* The A-lite collision guard in ``RefCountingColl.add`` is gone —
  repeated ``add(key, same_obj)`` increments refcount as before, and
  no A-lite warning is emitted.
* ``DEFAULT_CONFIG["cleanup_debounce"]`` defaults to ``0.0``.
"""
import gc
import inspect
import logging
import unittest
from unittest.mock import Mock

from rpyc.core import consts
from rpyc.core.protocol import Connection, DEFAULT_CONFIG
from rpyc.core.service import VoidService


def _make_conn():
    channel = Mock()
    channel.fileno = Mock(return_value=1)
    channel.closed = False
    conn = Connection(VoidService(), channel, config={})
    # Prevent I/O on teardown.
    conn._cleanup = Mock()
    return conn


class TestAllocatorContract(unittest.TestCase):
    def test_connection_has_alloc_stable_obj_id(self):
        conn = _make_conn()
        self.assertTrue(
            hasattr(conn, "_alloc_stable_obj_id"),
            "Connection._alloc_stable_obj_id must exist — this is the "
            "single entry point for computing id_pack[2] under variant A.",
        )

    def test_alloc_returns_zero_for_classes(self):
        """Old id_pack contract: class → third slot 0."""
        conn = _make_conn()

        class _Foo:
            pass

        self.assertEqual(
            conn._alloc_stable_obj_id(_Foo),
            0,
            "Classes must get 0 as the third slot (preserved contract).",
        )

    def test_alloc_returns_nonzero_for_instance(self):
        conn = _make_conn()

        class _Foo:
            pass

        seq = conn._alloc_stable_obj_id(_Foo())
        self.assertIsInstance(seq, int)
        self.assertNotEqual(
            seq, 0,
            "Instances must get a non-zero seq (0 is reserved for classes).",
        )

    def test_alloc_is_stable_for_live_weakrefable_object(self):
        conn = _make_conn()

        class _Foo:
            pass

        obj = _Foo()
        seq1 = conn._alloc_stable_obj_id(obj)
        seq2 = conn._alloc_stable_obj_id(obj)
        self.assertEqual(
            seq1, seq2,
            "Two allocations for the same live object must return the "
            "same seq — stability is the whole point of variant A.",
        )

    def test_alloc_differs_across_objects(self):
        conn = _make_conn()

        class _Foo:
            pass

        a, b = _Foo(), _Foo()
        seq_a = conn._alloc_stable_obj_id(a)
        seq_b = conn._alloc_stable_obj_id(b)
        self.assertNotEqual(
            seq_a, seq_b,
            "Distinct live Python objects must get distinct seqs.",
        )

    def test_alloc_differs_after_gc_and_id_reuse(self):
        """Kernel test: the monotonic seq NEVER gets reused, period.

        Instead of trying to provoke id() reuse (flaky), we just assert
        the structural invariant: each allocation yields a value the
        allocator has never handed out before.
        """
        conn = _make_conn()

        class _Foo:
            pass

        handed_out = set()
        for _ in range(200):
            obj = _Foo()
            seq = conn._alloc_stable_obj_id(obj)
            self.assertNotIn(
                seq, handed_out,
                f"Seq {seq} handed out twice — monotonic counter broken.",
            )
            handed_out.add(seq)
            # Drop the object and force gc. id() may reuse on the next
            # iteration; the allocator must still produce a fresh seq.
            del obj
            gc.collect()


class TestBoxUsesStableSeq(unittest.TestCase):
    """_box must compute id_pack[2] via _alloc_stable_obj_id, not id()."""

    def test_box_of_class_has_zero_third_slot(self):
        conn = _make_conn()

        class _Foo:
            pass

        label, value = conn._box(_Foo)
        # LABEL_REMOTE_REF → 3-tuple or 4-tuple (if async); third slot is
        # always id_pack[2] == 0 for classes.
        self.assertEqual(label, consts.LABEL_REMOTE_REF)
        self.assertEqual(
            value[2], 0,
            "_box(class) must produce id_pack[2] == 0 (legacy contract).",
        )

    def test_box_of_instance_has_nonzero_stable_third_slot(self):
        conn = _make_conn()

        class _Foo:
            pass

        obj = _Foo()
        label1, v1 = conn._box(obj)
        label2, v2 = conn._box(obj)
        self.assertEqual(label1, consts.LABEL_REMOTE_REF)
        self.assertEqual(
            v1[2], v2[2],
            "Two _box calls on the same live instance must agree on the "
            "seq — boxing must go through the stable allocator.",
        )
        self.assertNotEqual(
            v1[2], 0,
            "Instance must have non-zero seq.",
        )

    def test_box_of_instance_does_not_use_raw_python_id(self):
        """Regression guard: id_pack[2] must NOT be id(obj).

        With the monotonic allocator starting at 1 << 40, the seq is
        distinct from any 64-bit CPython id() in practice.
        """
        conn = _make_conn()

        class _Foo:
            pass

        obj = _Foo()
        _, v = conn._box(obj)
        self.assertNotEqual(
            v[2],
            id(obj),
            "id_pack[2] must be a monotonic seq, not id(obj). If these "
            "happen to collide you're running on a platform where id() "
            "returns values >= 1<<40 from the start — raise the "
            "counter's start value in _alloc_stable_obj_id.",
        )


class TestRefCountingCollNoALite(unittest.TestCase):
    """After A lands, the A-lite collision guard must be gone.

    Same-key adds still legitimately increment (same object passed
    twice; happens when a netref is boxed back to its owner). They
    must NOT emit the A-lite WARNING anymore.
    """

    def test_add_same_key_same_object_increments_quietly(self):
        from rpyc.lib.colls import RefCountingColl

        log = logging.getLogger("refcount-test")
        coll = RefCountingColl(logger=log, debug=False)
        key = ("some.cls", 11111, 22222)
        obj = {"id": "only"}

        with self.assertLogs(log, level="WARNING") as ctx:
            coll.add(key, obj)
            coll.add(key, obj)
            # force at least one log record so assertLogs doesn't
            # itself fail; this line intentionally emits a dummy.
            log.warning("sentinel")

        # The only WARNING in the log should be our sentinel. A-lite
        # warning ("id() COLLISION") must NOT appear.
        collision_warnings = [
            rec for rec in ctx.records
            if "COLLISION" in rec.getMessage()
        ]
        self.assertEqual(
            collision_warnings,
            [],
            "A-lite collision warning must not fire anymore — full "
            "variant A removes the collision scenario at the source.",
        )
        self.assertEqual(
            coll._dict[key][1],
            2,
            "Two adds of the same object must increment refcount to 2.",
        )

    def test_refcountingcoll_add_has_no_a_lite_branch(self):
        """Static guard: the 'slot[0] is not obj' branch is gone."""
        import ast

        from rpyc.lib import colls as _colls_mod

        src = inspect.getsource(_colls_mod.RefCountingColl.add)
        tree = ast.parse(src.lstrip())

        # Look for `elif slot[0] is not obj:` shape.
        found = False
        for node in ast.walk(tree):
            if not isinstance(node, ast.Compare):
                continue
            if not (
                len(node.ops) == 1
                and isinstance(node.ops[0], ast.IsNot)
            ):
                continue
            left = node.left
            if (
                isinstance(left, ast.Subscript)
                and isinstance(left.value, ast.Name)
                and left.value.id == "slot"
            ):
                found = True
                break

        self.assertFalse(
            found,
            "RefCountingColl.add still contains the A-lite collision "
            "guard (`slot[0] is not obj`). Remove it — variant A makes "
            "the collision impossible.",
        )


class TestDebounceDefault(unittest.TestCase):
    def test_cleanup_debounce_defaults_to_zero_after_a(self):
        self.assertEqual(
            DEFAULT_CONFIG["cleanup_debounce"],
            0.0,
            "After variant A lands, cleanup_debounce default drops to "
            "0.0. Debounce stays as an opt-in throttle for pathological "
            "HANDLE_DEL fan-out; the original 50 ms was defense against "
            "the id() race, which A removes.",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
