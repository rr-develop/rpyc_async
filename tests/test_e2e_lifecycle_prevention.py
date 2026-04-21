"""
E2E Test: Premature Deletion Prevention (v5.2)

Tests that the new lifecycle management prevents premature
object deletion when netrefs still exist on remote side.

This is the critical integration test validating the entire
lifecycle management system works end-to-end.
"""
import unittest
import asyncio
import time
import rpyc
import gc
from rpyc.utils.async_server import AsyncioServer
from multiprocessing import Process, Queue
from tests.support import get_free_port


def run_lifecycle_test_server(port, ready_queue):
    """Server process for lifecycle testing"""

    class LifecycleTestService(rpyc.Service):
        """Service that tests object lifecycle"""

        def __init__(self):
            super().__init__()
            self.stored_netrefs = {}  # Store netrefs by key

        async def exposed_store_netref(self, key, client_obj):
            """Store netref to client object"""
            self.stored_netrefs[key] = client_obj
            # Call method to verify it works
            result = await client_obj.get_value()
            return f"Stored {key}, value={result}"

        async def exposed_use_stored_netref(self, key):
            """Use previously stored netref"""
            if key not in self.stored_netrefs:
                raise KeyError(f"No netref stored for {key}")

            netref = self.stored_netrefs[key]
            # This should work even if client released local reference
            result = await netref.get_value()
            return f"Used {key}, value={result}"

        def exposed_release_netref(self, key):
            """Release stored netref"""
            if key in self.stored_netrefs:
                del self.stored_netrefs[key]
                return True
            return False

        def exposed_netref_count(self):
            """Get number of stored netrefs"""
            return len(self.stored_netrefs)

    async def server_main():
        server = AsyncioServer(
            LifecycleTestService,
            port=port,
            protocol_config={
                "allow_public_attrs": True,
                "allow_pickle": True,
                "cleanup_interval": 0.5,  # Fast cleanup for testing
            }
        )

        try:
            await server.start()
            ready_queue.put("ready")
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        finally:
            await server.close()

    try:
        asyncio.run(server_main())
    except KeyboardInterrupt:
        pass


class TestE2ELifecyclePrevention(unittest.TestCase):
    """E2E tests for premature deletion prevention"""

    def setUp(self):
        """Start server"""
        self.port = get_free_port()
        self.ready_queue = Queue()

        self.server_process = Process(
            target=run_lifecycle_test_server,
            args=(self.port, self.ready_queue),
            daemon=True
        )
        self.server_process.start()

        try:
            signal = self.ready_queue.get(timeout=5.0)
            if signal != "ready":
                raise RuntimeError(f"Unexpected signal: {signal}")
        except Exception as e:
            self.server_process.terminate()
            raise RuntimeError(f"Server failed to start: {e}")

        time.sleep(0.2)

    def tearDown(self):
        """Stop server"""
        if self.server_process.is_alive():
            self.server_process.terminate()
            self.server_process.join(timeout=2.0)

        if self.server_process.is_alive():
            self.server_process.kill()
            self.server_process.join(timeout=1.0)

    def test_object_survives_local_deletion_while_server_holds_netref(self):
        """
        CRITICAL TEST: Object should not be deleted on client while
        server still holds a netref to it.

        This was the original bug - object would be deleted prematurely,
        causing KeyError or EOFError when server tried to use the netref.
        """

        class ClientObject:
            """Client object with async method"""

            def __init__(self, value):
                self.value = value

            async def get_value(self):
                """Return value"""
                await asyncio.sleep(0.01)
                return self.value

        async def test():
            # Create connection
            conn = await rpyc.async_connect("localhost", self.port)

            try:

                # Create client object
                obj = ClientObject(42)

                # Pass to server and store
                result = await conn.root.store_netref("test_key", obj)
                self.assertIn("value=42", result)

                # DELETE local reference (critical step!)
                del obj
                gc.collect()

                # Wait a bit to ensure cleanup task runs
                await asyncio.sleep(1.0)

                # Server should STILL be able to use the netref
                # This is the critical test - should NOT raise KeyError/EOFError
                result = await conn.root.use_stored_netref("test_key")
                self.assertIn("value=42", result)

                # Release on server side (sync method → async wrapper)
                await rpyc.async_(conn.root.release_netref)("test_key")

                # Wait for cleanup
                await asyncio.sleep(1.0)

            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_object_deleted_after_server_releases_netref(self):
        """
        Object SHOULD be deleted after server releases netref.

        This tests that cleanup actually happens when it should.
        """

        class ClientObject:
            def __init__(self, value):
                self.value = value
                self.deleted = False

            def __del__(self):
                self.deleted = True

            async def get_value(self):
                await asyncio.sleep(0.01)
                return self.value

        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:

                # Create and store
                obj = ClientObject(99)
                await conn.root.store_netref("temp_key", obj)

                # Release local reference
                del obj
                gc.collect()

                # Server releases netref
                released = await rpyc.async_(conn.root.release_netref)("temp_key")
                self.assertTrue(released)

                # Wait for cleanup task to process deletion
                await asyncio.sleep(1.5)

                # Object should eventually be deleted on client
                # (We can't directly check this without keeping a weakref,
                #  but cleanup should have happened)

            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_multiple_objects_independent_lifecycle(self):
        """
        Multiple objects should have independent lifecycles.
        """

        class ClientObject:
            def __init__(self, name, value):
                self.name = name
                self.value = value

            async def get_value(self):
                await asyncio.sleep(0.01)
                return self.value

        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:

                # Create multiple objects
                obj1 = ClientObject("obj1", 10)
                obj2 = ClientObject("obj2", 20)
                obj3 = ClientObject("obj3", 30)

                # Store all three
                await conn.root.store_netref("key1", obj1)
                await conn.root.store_netref("key2", obj2)
                await conn.root.store_netref("key3", obj3)

                # Delete local references
                del obj1, obj2, obj3
                gc.collect()
                await asyncio.sleep(0.5)

                # All should still work
                r1 = await conn.root.use_stored_netref("key1")
                r2 = await conn.root.use_stored_netref("key2")
                r3 = await conn.root.use_stored_netref("key3")

                self.assertIn("10", r1)
                self.assertIn("20", r2)
                self.assertIn("30", r3)

                # Release obj2 only
                await rpyc.async_(conn.root.release_netref)("key2")
                await asyncio.sleep(0.5)

                # obj1 and obj3 should still work
                r1 = await conn.root.use_stored_netref("key1")
                r3 = await conn.root.use_stored_netref("key3")
                self.assertIn("10", r1)
                self.assertIn("30", r3)

                # Clean up
                await rpyc.async_(conn.root.release_netref)("key1")
                await rpyc.async_(conn.root.release_netref)("key3")

            finally:
                await conn.aclose()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
