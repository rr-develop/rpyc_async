"""
Single-object cleanup verification.

Server returns a dict (which boxes as a netref on the client side).
We drop the reference, yield once to let the event-driven
`cleanup_loop` drain, and then assert both registries are empty and
no pending deletions remain. Server and client live in separate
processes per `docs/DESIGN_NO_SAME_PROCESS_TESTS.md`.

Rewrite history
---------------
The previous version used `rpyc.connect(...) + enable_asyncio_serving()`
and then sync netref ops (`conn.root.get_stats()`, `result[...]`)
from inside the asyncio loop — blocked by the sync_request guard in
`protocol.py:2129`. It also only *printed* warnings instead of
asserting. The rewrite uses `await rpyc.async_connect(...)`, calls
only async exposed methods (or wraps sync ones in `rpyc.async_()`),
and converts every trace printout into a real assertion.
"""
import asyncio
import gc
import unittest

import rpyc
from rpyc.utils.async_server import AsyncioServer

from tests.support import mp_asyncio_server


class _CleanupTraceService(rpyc.Service):
    """Minimal service: returns one dict per call + a getter for
    server-side `_local_objects` size."""

    def on_connect(self, conn):
        # Capture the serving connection so we can introspect its
        # registry from within exposed methods.
        self._conn = conn

    async def exposed_create_dict(self):
        """Return a fresh dict. It will be boxed as a netref on the
        client side, and entered into this server's `_local_objects`."""
        return {"value": 42, "data": "test"}

    async def exposed_stats_tuple(self):
        """Introspect the server-side Connection state as a brine-
        dumpable tuple of primitives — avoids boxing a dict netref
        that the client would have to dereference field-by-field via
        sync RPCs."""
        return (
            int(len(self._conn._local_objects._dict)),
            int(self._conn._pending_deletions.qsize()),
            bool(self._conn._cleanup_running),
        )


class TestCleanupSimpleTrace(unittest.TestCase):
    """Verify one-object round-trip cleanup is clean on both sides."""

    def test_single_object_cleanup_is_clean(self):
        async def body():
            with mp_asyncio_server(_CleanupTraceService) as port:
                conn = await rpyc.async_connect("127.0.0.1", port)
                try:
                    # Client-side cleanup task must be running —
                    # async_connect enabled asyncio serving.
                    self.assertTrue(conn._cleanup_running)
                    self.assertIsNotNone(conn._cleanup_task)

                    # Server: idle state.
                    local_before, pending_before, running_before = \
                        await conn.root.stats_tuple()
                    self.assertTrue(running_before)
                    self.assertEqual(pending_before, 0)

                    # Create a server-owned dict, receive it as a netref.
                    result = await conn.root.create_dict()
                    self.assertIsNotNone(result)

                    # Server now holds the dict strongly in its
                    # _local_objects; the client holds a netref.
                    local_held, pending_held, _ = \
                        await conn.root.stats_tuple()
                    self.assertGreaterEqual(
                        local_held, 1,
                        "server must hold at least the returned dict"
                    )

                    # Drop the reference; the netref __del__ queues a
                    # HANDLE_DEL and wakes the event-driven cleanup_loop.
                    del result
                    gc.collect()

                    # Yield once so cleanup_loop can pick up the signal
                    # and drain its queue — NO polling.
                    await asyncio.sleep(0.3)

                    # Client side: registry empty (we never owned
                    # server-side objects), queue drained.
                    self.assertEqual(
                        len(conn._local_objects._dict), 0,
                        "client _local_objects must be empty — it never "
                        "hosts server-owned objects"
                    )
                    self.assertEqual(
                        conn._pending_deletions.qsize(), 0,
                        "client pending_deletions must have drained after "
                        "one event-loop yield"
                    )

                    # Server side: dict netref released → refcount hit 0
                    # → slot removed from _local_objects.
                    local_after, pending_after, _ = \
                        await conn.root.stats_tuple()
                    self.assertEqual(
                        pending_after, 0,
                        "server pending_deletions must be 0 after cleanup"
                    )
                    self.assertLess(
                        local_after, local_held,
                        "server _local_objects_count must have decreased "
                        "after the netref was dropped"
                    )
                finally:
                    await conn.aclose()

        asyncio.run(body())


if __name__ == "__main__":
    unittest.main()
