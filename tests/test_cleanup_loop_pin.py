"""Regression tests: ``Connection``'s background cleanup_loop task
must survive GC of the Connection's last user-held reference, and
must run its finally-drain to completion before going away.

Production observation (observed in a downstream service, 2026-05-13):
the log printed

    asyncio - ERROR - Task was destroyed but it is pending!
    task: <Task pending name='Task-3309'
           coro=<Connection._start_cleanup_task.<locals>.cleanup_loop()
                  running at rpyc_async/core/protocol.py:939>
           wait_for=<Future pending cb=[Task.task_wakeup()]>>
    WARNING: Failed to delete remote object
             ('...BoundAgentService', ...).
             Possible memory leak on remote side.

Mechanism: ``cleanup_loop`` and the Connection that owns it form
a strong-reference cycle (Connection → _cleanup_task → Task →
cleanup_loop coroutine → frame.f_locals['self'] → Connection).
Python's cycle collector breaks the cycle as soon as no EXTERNAL
strong reference holds either end. ``asyncio._all_tasks`` is a
``WeakSet`` (does not count). When a downstream service evicts a
torn-down Connection from its connection registry (the
``is_connected`` liveness fix from 2026-04-27) and the last
application reference goes away, the whole cycle becomes
collectible. The cleanup_loop Task is destroyed mid-await,
asyncio emits the warning, and any in-flight HANDLE_DEL on the
queue is silently dropped → remote netrefs leak on the peer.

Fix this guards:

  1. A module-level strong-ref set ``_CLEANUP_LOOPS`` holds the
     Task across GC of its Connection. The set is the ONE strong
     reference that asyncio's WeakSet can't provide.
  2. The cleanup_loop coroutine must hold the Connection only
     via ``weakref``, otherwise we trade one leak (cleanup_loop
     dies early) for another (Connection lives forever).
  3. ``weakref.finalize`` on Connection drives an orderly
     shutdown of cleanup_loop when Connection is GC'd: the
     finalize callback sets the wake-up event so the loop exits
     its ``await event.wait()`` and runs its ``finally:``-block
     drain.

Result: cleanup_loop always finishes its work, even when the
Connection it serves goes away via plain GC without an explicit
``close()`` / ``aclose()`` call.
"""
from __future__ import annotations

import asyncio
import gc
import unittest
import weakref

from rpyc_async.core import protocol
from rpyc_async.core.protocol import Connection
from rpyc_async.core.service import VoidService


class _SilentChannel:
    def __init__(self) -> None:
        self.closed = False

    def send(self, data):
        return None

    async def asend(self, data):
        return None

    def recv(self):
        raise EOFError("stream has been closed")

    def close(self):
        self.closed = True

    def fileno(self):
        return -1

    def poll(self, timeout):
        return False


def _make_connection(test_case):
    """Build a bare Connection.

    NOTE: we deliberately do NOT use ``test_case.addCleanup(setattr,
    conn, ...)`` — that closure would keep a strong reference to
    ``conn`` for the lifetime of the test, defeating the GC-ability
    assertion in ``test_cleanup_loop_does_not_pin_connection_forever``.
    The tests in this file are responsible for tearing the Connection
    down themselves.
    """
    conn = Connection(VoidService(), _SilentChannel(), config={})
    conn._asyncio_enabled = True
    conn._asyncio_loop = asyncio.get_event_loop()
    return conn


class TestCleanupLoopPin(unittest.IsolatedAsyncioTestCase):

    async def test_cleanup_loops_set_exists(self) -> None:
        """``protocol._CLEANUP_LOOPS`` MUST exist as a module-
        level strong-ref set, mirroring ``_INFLIGHT`` and
        ``_DISPATCH_INFLIGHT``. Without it, asyncio's WeakSet is
        the only reference and a GC cycle can destroy
        cleanup_loop pending."""
        self.assertTrue(
            hasattr(protocol, "_CLEANUP_LOOPS"),
            "rpyc_async.core.protocol must expose a module-level set "
            "named _CLEANUP_LOOPS that holds strong refs to "
            "cleanup_loop Tasks across GC of their Connection. "
            "See a related internal incident analysis (not included here)."
        )
        self.assertIsInstance(protocol._CLEANUP_LOOPS, set)

    async def test_cleanup_loop_task_added_to_set_on_start(self) -> None:
        """``_start_cleanup_task`` MUST register the resulting
        Task in ``_CLEANUP_LOOPS`` before returning."""
        before = len(protocol._CLEANUP_LOOPS)
        conn = _make_connection(self)
        conn._start_cleanup_task()
        # One tick to let create_task settle.
        await asyncio.sleep(0)

        try:
            after = len(protocol._CLEANUP_LOOPS)
            self.assertGreater(
                after, before,
                "_start_cleanup_task did not register its Task in "
                "_CLEANUP_LOOPS. asyncio's WeakSet is then the "
                "only reference, and a GC pass can destroy the "
                "Task pending."
            )
        finally:
            # Tear down so we don't pollute the set across tests.
            conn._cleanup_running = False
            conn._signal_deletion_available()
            if conn._cleanup_task is not None:
                conn._cleanup_task.cancel()
                try:
                    await conn._cleanup_task
                except (asyncio.CancelledError, Exception):
                    pass

    async def test_cleanup_loop_survives_connection_gc(self) -> None:
        """The 2026-05-13 production failure mode: the last
        application reference to a Connection goes away (e.g.
        a downstream service's connection registry evicts
        a torn-down conn). The cleanup_loop Task MUST run its
        ``finally:`` drain to completion — NOT be destroyed
        pending.

        We verify via:
          (a) a weakref on the cleanup_loop Task: it must still
              be alive after the Connection is collected
              (because _CLEANUP_LOOPS holds a strong ref);
          (b) the Task eventually transitions to ``done()``
              (i.e. its finally block ran), not destroyed pending.
        """
        conn = _make_connection(self)
        conn._start_cleanup_task()
        await asyncio.sleep(0)

        # Hold a weakref to the Task and to the Connection.
        task = conn._cleanup_task
        self.assertIsNotNone(task)
        task_wref = weakref.ref(task)
        conn_wref = weakref.ref(conn)

        # Drop our strong refs.
        task = None
        del conn

        # Force GC. Without the fix the cycle would collapse and
        # both the Connection and the Task would die immediately
        # in pending state.
        for _ in range(3):
            gc.collect()

        # The Task MUST still be alive (pinned by _CLEANUP_LOOPS).
        live_task = task_wref()
        self.assertIsNotNone(
            live_task,
            "cleanup_loop Task was collected together with its "
            "Connection. _CLEANUP_LOOPS did not hold the strong "
            "ref it needs to. Production symptom: asyncio emits "
            "'Task was destroyed but it is pending!' and any "
            "in-flight HANDLE_DEL queue entry is silently "
            "dropped → remote netrefs leak on the peer."
        )

        # Now wait for the Task to finish naturally — finalize on
        # the Connection should have triggered the shutdown signal.
        try:
            await asyncio.wait_for(live_task, timeout=2.0)
        except asyncio.TimeoutError:
            self.fail(
                "cleanup_loop Task did not finish within 2s after "
                "its Connection was GC'd. The finalize hook on "
                "Connection must signal cleanup_loop to exit so "
                "its finally-block drain runs."
            )
        except asyncio.CancelledError:
            # Acceptable termination — explicit cancel ran.
            pass

    async def test_cleanup_loop_does_not_pin_connection_forever(
        self,
    ) -> None:
        """The OPPOSITE failure mode we must not introduce. If
        cleanup_loop closes over a strong reference to its
        Connection, the Connection lives forever (because the
        Task in _CLEANUP_LOOPS holds it). The fix must use a
        weakref so the Connection is collectible the moment all
        application refs drop.
        """
        conn = _make_connection(self)
        conn._start_cleanup_task()
        await asyncio.sleep(0)

        conn_wref = weakref.ref(conn)
        task = conn._cleanup_task
        del conn

        for _ in range(3):
            gc.collect()
            await asyncio.sleep(0)

        # After GC, the Connection must be collectible. If it
        # still alive, cleanup_loop is pinning it forever, which
        # would be a memory leak on its own.
        self.assertIsNone(
            conn_wref(),
            "Connection is still alive after all user references "
            "dropped — cleanup_loop's coroutine closes over a "
            "STRONG reference to it instead of a weakref. This "
            "is the dual of the original bug: we'd fix "
            "cleanup_loop-GC'd-pending only to trade it for "
            "Connection-lives-forever. The cleanup_loop closure "
            "must use weakref.ref(self) / weakref.proxy(self)."
        )

        # Give the (now-dangling) Task a chance to notice via
        # weakref and exit cleanly.
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass


if __name__ == "__main__":
    unittest.main()
