"""
Cleanup-mechanism end-to-end verification.

Builds a batch of server-side objects via a single async RPC, drops
the reference on the client, lets the event-driven cleanup_loop
drain, and asserts the visible invariants:

  * server- and client-side `_local_objects` registries have dropped
    back to baseline (or below the post-creation peak),
  * client `_pending_deletions` queue is empty,
  * client-side cleanup task is running,
  * no REFCOUNT / Failed-to-delete warnings appear on stderr.

Server and client run in different processes per
`docs/DESIGN_NO_SAME_PROCESS_TESTS.md`; the helper
`tests/support.py::mp_asyncio_server` wires that up.

Rewrite history
---------------
The previous version used `rpyc.connect(...) + enable_asyncio_serving(loop)`
and issued sync RPCs (`conn.root.check_cleanup_task()`,
`conn.root.get_registry_size()`, list-of-dicts dereferencing) from
inside the asyncio loop. All of that is now blocked by the
sync_request guard in `protocol.py:2129`. The rewrite:
  * uses `await rpyc.async_connect(...)`,
  * exposes server introspection as `async def` returning brine-
    primitive tuples (so the client doesn't need to dereference
    field-by-field via more sync RPCs),
  * replaces every "print a trace and hope it's fine" with a real
    assertion.
"""
import asyncio
import gc
import io
import sys
import unittest
from contextlib import redirect_stderr

import rpyc_async as rpyc

from tests.support import mp_asyncio_server


class _DebugCleanupService(rpyc.Service):
    """Creates temp objects on demand and exposes async introspection."""

    def on_connect(self, conn):
        self._conn = conn

    async def exposed_create_objects(self, count):
        """Return a list of `count` fresh dicts. The list and each
        dict become netrefs on the client."""
        return [{"value": i, "data": f"item_{i}"} for i in range(count)]

    async def exposed_registry_size(self) -> int:
        return int(len(self._conn._local_objects._dict))

    async def exposed_cleanup_status_tuple(self):
        """Serialize the cleanup state as a brine-dumpable tuple.
        `(cleanup_running, task_exists, pending_queue_size)`."""
        return (
            bool(self._conn._cleanup_running),
            bool(self._conn._cleanup_task is not None),
            int(self._conn._pending_deletions.qsize()),
        )


class TestCleanupDebug(unittest.TestCase):

    def test_cleanup_after_batch_returns_to_baseline(self):
        async def body():
            with mp_asyncio_server(_DebugCleanupService) as port:
                # Capture stderr so we can assert zero refcount warnings.
                stderr_capture = io.StringIO()
                real_stderr = sys.stderr
                try:
                    sys.stderr = stderr_capture
                    conn = await rpyc.async_connect("127.0.0.1", port)
                    try:
                        # Baselines.
                        server_baseline = int(await conn.root.registry_size())
                        client_baseline = len(conn._local_objects._dict)

                        cleanup_running_c, task_exists_c, pending_c = (
                            bool(conn._cleanup_running),
                            bool(conn._cleanup_task is not None),
                            int(conn._pending_deletions.qsize()),
                        )
                        cleanup_running_s, task_exists_s, pending_s = \
                            await conn.root.cleanup_status_tuple()

                        self.assertTrue(cleanup_running_c)
                        self.assertTrue(task_exists_c)
                        self.assertEqual(pending_c, 0)
                        self.assertTrue(cleanup_running_s)
                        self.assertTrue(task_exists_s)
                        self.assertEqual(pending_s, 0)

                        # Create 10 server-side objects in one call.
                        # The return value is ONE netref (to the list);
                        # the inner dicts are lazily netref'd only if
                        # we dereference them (we deliberately don't —
                        # we're testing the cleanup of the list itself).
                        result = await conn.root.create_objects(10)
                        self.assertIsNotNone(result)

                        server_held = int(await conn.root.registry_size())
                        self.assertGreaterEqual(
                            server_held, server_baseline + 1,
                            "server _local_objects must have grown after "
                            "create_objects(10)"
                        )

                        # Drop the netref; event-driven cleanup_loop wakes.
                        del result
                        gc.collect()
                        await asyncio.sleep(0.3)

                        # Post-cleanup invariants.
                        self.assertEqual(
                            conn._pending_deletions.qsize(), 0,
                            "client pending_deletions must have drained"
                        )
                        self.assertEqual(
                            len(conn._local_objects._dict), client_baseline,
                            "client _local_objects must return to baseline "
                            "(we never owned any server-side objects)"
                        )
                        server_after = int(await conn.root.registry_size())
                        self.assertLess(
                            server_after, server_held,
                            "server _local_objects must have shrunk after "
                            "the netref was dropped"
                        )
                    finally:
                        await conn.aclose()
                finally:
                    sys.stderr = real_stderr

                captured = stderr_capture.getvalue()
                self.assertEqual(
                    captured.count("DECREF on missing key"), 0,
                    f"unexpected DECREF warnings:\n{captured}"
                )
                self.assertEqual(
                    captured.count("Failed to delete remote object"), 0,
                    f"unexpected Failed-to-delete warnings:\n{captured}"
                )

        asyncio.run(body())


if __name__ == "__main__":
    unittest.main()
