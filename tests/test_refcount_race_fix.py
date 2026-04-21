"""
TDD tests for the refcount race fix (variants D + A-lite).

See ``docs/DESIGN_REFCOUNT_RACE_FIX.md``. These tests land together with
the implementation.

Scope
-----
* **D (debounce).** ``_signal_deletion_available`` must coalesce a burst
  of deletion signals into a single wake-up of the cleanup task using a
  **one-shot** ``loop.call_later`` (not a polling loop). NO-POLLING policy
  still applies; ``test_signal_deletion_available_is_not_a_polling_loop``
  enforces this statically.
* **A-lite (id() collision guard in ``RefCountingColl.add``).** When a
  slot already exists for an id_pack but holds a DIFFERENT Python object
  (``id()`` was reused), replace the slot fresh rather than incrementing
  the refcount of the wrong object.
"""
import ast
import asyncio
import inspect
import re
import time
import unittest
from multiprocessing import Process, Queue
from unittest.mock import Mock

import rpyc
from rpyc.core import consts
from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService
from rpyc.utils.async_server import AsyncioServer
from tests.support import get_free_port


# ---------------------------------------------------------------------------
# A-lite — id() collision guard in RefCountingColl.add
# ---------------------------------------------------------------------------


class TestRefCountingCollAddIncrementsSameObject(unittest.TestCase):
    """Legitimate reuse (same object boxed twice) must still incref.

    Historical note: an earlier commit had a separate
    ``TestIdCollisionGuard::test_add_with_same_key_but_different_object_rebinds_slot``
    covering the A-lite collision-detection branch. Variant A (full —
    stable monotonic ``id_pack[2]``) removed the possibility of that
    collision, and the A-lite branch was deleted along with its test.
    What remains is the positive case: adding the same object twice
    must still produce refcount = 2.
    """

    def test_add_with_same_key_and_same_object_still_increments(self):
        """Legitimate reuse (same object boxed twice) must still incref."""
        from rpyc.lib.colls import RefCountingColl

        coll = RefCountingColl()
        key = ("fake.cls", 11111, 22222)
        obj = {"id": "only"}

        coll.add(key, obj)
        coll.add(key, obj)  # same obj

        self.assertEqual(
            coll._dict[key][1],
            2,
            "Two adds of the same live object must yield refcount = 2; "
            "the collision guard must not mistake this for a collision.",
        )


# ---------------------------------------------------------------------------
# D — debounce of the deletion-available signal
# ---------------------------------------------------------------------------


class TestDebounceNoPollingStatic(unittest.TestCase):
    """Static guard: the debounced signal path does NOT reintroduce polling.

    The implementation is one-shot ``loop.call_later``. It must NOT be a
    ``while ...: await asyncio.sleep(...)`` or similar loop.
    """

    def test_signal_deletion_available_is_not_a_polling_loop(self):
        src = inspect.getsource(Connection._signal_deletion_available)
        tree = ast.parse(src.lstrip())

        def _has_sleep_loop(node: ast.AST) -> bool:
            for loop_node in ast.walk(node):
                if not isinstance(loop_node, (ast.While, ast.For, ast.AsyncFor)):
                    continue
                for inner in ast.walk(loop_node):
                    if not isinstance(inner, ast.Await):
                        continue
                    call = inner.value
                    if not isinstance(call, ast.Call):
                        continue
                    fn = call.func
                    if (
                        isinstance(fn, ast.Attribute)
                        and fn.attr == "sleep"
                        and isinstance(fn.value, ast.Name)
                        and fn.value.id == "asyncio"
                    ):
                        return True
            return False

        self.assertFalse(
            _has_sleep_loop(tree),
            "_signal_deletion_available contains `while ...: await "
            "asyncio.sleep(...)` — forbidden by the NO POLLING POLICY. "
            "Use one-shot `loop.call_later(debounce, event.set)` instead.",
        )

    def test_signal_uses_call_later_not_immediate_set(self):
        """Debounce must route through loop.call_later (one-shot timer)."""
        src = inspect.getsource(Connection._signal_deletion_available)
        self.assertIn(
            "call_later",
            src,
            "Debounce implementation must use loop.call_later so a burst "
            "of signals coalesces into a single wake-up. Direct "
            "event.set() from _enqueue_deletion races with in-flight RPCs.",
        )


class TestDebounceCoalescesBursts(unittest.IsolatedAsyncioTestCase):
    """Runtime guard: many _enqueue_deletion calls in a tight burst must
    wake the cleanup task **once**, not N times."""

    async def test_many_enqueues_produce_one_wakeup(self):
        channel = Mock()
        channel.fileno = Mock(return_value=1)
        channel.closed = False
        conn = Connection(
            VoidService(),
            channel,
            config={"cleanup_debounce": 0.03},
        )
        try:
            loop = asyncio.get_running_loop()
            conn._asyncio_loop = loop
            conn._asyncio_enabled = True

            wakeups = []

            async def fake_process():
                wakeups.append(time.monotonic())

            conn._process_pending_deletions = fake_process
            conn._start_cleanup_task()

            # Fire 50 deletions in a tight loop. Without debounce, each
            # one sets the event (idempotent) — in practice the cleanup
            # task wakes once anyway, clears, and blocks again. But the
            # *property* we enforce is: over the whole burst and through
            # the debounce window, the task processes at most once, and
            # exactly once when there is work. 50 wake-ups would be a
            # regression; 1 is required; 0 would also be a regression.
            for i in range(50):
                conn._enqueue_deletion(("fake.cls", i, i), 1)

            # Wait past the debounce window plus a small scheduler margin.
            await asyncio.sleep(0.15)

            self.assertEqual(
                len(wakeups),
                1,
                f"Expected exactly 1 wake-up per burst (debounced); got "
                f"{len(wakeups)}. Debounce is not coalescing signals.",
            )
        finally:
            conn._stop_cleanup_task()
            await asyncio.sleep(0.05)
            conn._closed = True

    async def test_signals_across_windows_wake_multiple_times(self):
        """Debounce must NOT swallow signals forever: a signal after the
        first window has fired must produce a second wake-up."""
        channel = Mock()
        channel.fileno = Mock(return_value=1)
        channel.closed = False
        conn = Connection(
            VoidService(),
            channel,
            config={"cleanup_debounce": 0.03},
        )
        try:
            loop = asyncio.get_running_loop()
            conn._asyncio_loop = loop
            conn._asyncio_enabled = True

            wakeups = []

            async def fake_process():
                wakeups.append(time.monotonic())

            conn._process_pending_deletions = fake_process
            conn._start_cleanup_task()

            conn._enqueue_deletion(("a", 1, 1), 1)
            await asyncio.sleep(0.12)  # first window fires

            conn._enqueue_deletion(("b", 2, 2), 1)
            await asyncio.sleep(0.12)  # second window fires

            self.assertEqual(
                len(wakeups),
                2,
                "Signals in separate debounce windows must each produce a "
                "wake-up; got {0}. Pending-flag is not being cleared after "
                "the debounce fires.".format(len(wakeups)),
            )
        finally:
            conn._stop_cleanup_task()
            await asyncio.sleep(0.05)
            conn._closed = True


class TestDebounceDefaultConfigPresent(unittest.TestCase):
    def test_default_config_has_cleanup_debounce(self):
        from rpyc.core.protocol import DEFAULT_CONFIG

        self.assertIn(
            "cleanup_debounce",
            DEFAULT_CONFIG,
            "DEFAULT_CONFIG must expose `cleanup_debounce` so users can "
            "tune or disable the debounce.",
        )
        self.assertIsInstance(DEFAULT_CONFIG["cleanup_debounce"], (int, float))


# ---------------------------------------------------------------------------
# E2E — the race tests that were skipped must start passing
# ---------------------------------------------------------------------------


def _policy_e2e_server(port, ready_queue):
    class _Svc(rpyc.Service):
        async def exposed_store(self, key, obj):
            return "stored"

    async def _main():
        srv = AsyncioServer(
            _Svc,
            hostname="localhost",
            port=port,
            protocol_config={"allow_all_attrs": True},
        )
        await srv.start()
        ready_queue.put("ready")
        try:
            await asyncio.Event().wait()
        finally:
            await srv.close()

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass


class TestMultipleStoredNetrefsSurviveGC(unittest.TestCase):
    """End-to-end repro of the race the previous commit had to skip.

    Three client objects stored on the server, client drops local refs,
    server still uses them. With D+B in place this must pass cleanly.
    """

    def setUp(self):
        self.port = get_free_port()
        self.ready_queue = Queue()
        self.proc = Process(
            target=_policy_e2e_server,
            args=(self.port, self.ready_queue),
            daemon=True,
        )
        self.proc.start()
        self.assertEqual(self.ready_queue.get(timeout=5.0), "ready")
        time.sleep(0.1)

    def tearDown(self):
        if self.proc.is_alive():
            self.proc.terminate()
            self.proc.join(timeout=2.0)
            if self.proc.is_alive():
                self.proc.kill()
                self.proc.join(timeout=1.0)

    def test_three_sequential_stores_with_short_lived_clients(self):
        """Three sequential `await conn.root.store(key, NewClientObj(...))`
        calls must all succeed.

        Pre-fix (master with eager cleanup signals): the third call would
        return an unresolved `AsyncResult` because the short-lived client
        object's ``id()`` collided with a prior slot in ``_local_objects``
        and the old code's `slot[1] += 1` incremented the wrong object's
        refcount. With the A-lite collision guard in
        ``RefCountingColl.add``, the slot is rebound to the new object and
        the RPC goes through.
        """
        async def _go():
            class Cli:
                def __init__(self, v):
                    self.v = v

                async def get_value(self):
                    await asyncio.sleep(0.005)
                    return self.v

            conn = await rpyc.async_connect(
                "127.0.0.1", self.port, timeout=5.0
            )
            try:
                for i in range(3):
                    r = await conn.root.store(f"k{i}", Cli(i * 10 + 10))
                    self.assertEqual(
                        r, "stored",
                        f"store #{i} returned {r!r} — id() collision guard "
                        f"missing or broken in RefCountingColl.add",
                    )
            finally:
                await conn.aclose()

        asyncio.run(_go())


if __name__ == "__main__":
    unittest.main(verbosity=2)
