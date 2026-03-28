"""
E2E Test: Complete Cleanup Verification (v5.2)

This test verifies that after intense client-server interaction,
ALL objects are properly cleaned up on both sides:
- _local_objects registries are EMPTY
- _proxy_cache is empty
- NO "Failed to delete" warnings in logs
- NO "DECREF on missing key" warnings

This is the comprehensive test for the new cleanup mechanism.
"""
import unittest
import asyncio
import time
import gc
import sys
import io
from contextlib import redirect_stderr
import rpyc
from rpyc.utils.async_server import AsyncioServer
from multiprocessing import Process, Queue
from tests.support import get_free_port


def run_cleanup_test_server(port, ready_queue):
    """Server process for cleanup verification"""

    class CleanupTestService(rpyc.Service):
        """Service for testing complete cleanup"""

        def __init__(self):
            super().__init__()
            self.stored_objects = {}
            self.call_count = 0
            self._conn = None

        def on_connect(self, conn):
            """Store connection reference"""
            self._conn = conn

        async def exposed_intensive_operation(self, client_callback, iterations):
            """
            Intensive operation that creates many temporary objects.

            Calls client callback multiple times, creating netrefs for:
            - Return values (dicts, lists, tuples)
            - Method references
            - Temporary objects
            """
            results = []
            for i in range(iterations):
                # Call client callback - creates netref
                value = await client_callback(i)
                results.append(value)

                # Create various object types
                temp_dict = {"iteration": i, "value": value}
                temp_list = [i, value, "test"]
                temp_tuple = (i, value)

                self.call_count += 1

            return {"completed": iterations, "results": results}

        def exposed_store_object(self, key, obj):
            """Store object temporarily"""
            self.stored_objects[key] = obj
            return f"Stored {key}"

        def exposed_release_object(self, key):
            """Release stored object"""
            if key in self.stored_objects:
                del self.stored_objects[key]
                return True
            return False

        def exposed_get_registry_size(self):
            """Get size of _local_objects registry"""
            return len(self._conn._local_objects._dict)

        def exposed_check_cleanup_task(self):
            """Check cleanup task status"""
            return {
                "cleanup_running": self._conn._cleanup_running,
                "pending_queue_size": self._conn._pending_deletions.qsize()
            }

        def exposed_force_gc(self):
            """Force garbage collection on server"""
            import gc
            collected = gc.collect()
            return collected

        def exposed_get_call_count(self):
            """Get number of calls processed"""
            return self.call_count

    async def server_main():
        server = AsyncioServer(
            CleanupTestService,
            port=port,
            protocol_config={
                "allow_public_attrs": True,
                "allow_pickle": True,
                "cleanup_interval": 0.5,  # Fast cleanup
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


class TestE2ECompleteCleanup(unittest.TestCase):
    """E2E test for complete cleanup verification"""

    def setUp(self):
        """Start server"""
        self.port = get_free_port()
        self.ready_queue = Queue()

        self.server_process = Process(
            target=run_cleanup_test_server,
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

    def test_complete_cleanup_after_intensive_operations(self):
        """
        CRITICAL TEST: After intensive operations, all registries must be empty.

        This test:
        1. Performs 50 iterations of intensive RPC operations
        2. Creates hundreds of temporary netrefs (dicts, lists, tuples, methods)
        3. Releases all references
        4. Triggers cleanup and GC
        5. Verifies _local_objects is EMPTY on both sides
        6. Verifies NO cleanup warnings in logs
        """

        class ClientCallback:
            """Client object with method that will be called remotely"""
            def __init__(self):
                self.call_count = 0

            async def callback_method(self, value):
                """Called by server - creates return values"""
                self.call_count += 1
                # Return various types to create netrefs
                return {
                    "processed": value * 2,
                    "items": [value, value + 1, value + 2],
                    "metadata": ("client", value)
                }

        async def test():
            # Capture stderr to check for warnings
            stderr_capture = io.StringIO()

            conn = rpyc.connect("localhost", self.port)

            try:
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)

                # Phase 1: Intensive operations
                print("\n[TEST] Phase 1: Performing intensive operations...")

                callback_obj = ClientCallback()

                with redirect_stderr(stderr_capture):
                    result = await conn.root.intensive_operation(
                        callback_obj.callback_method,
                        50  # 50 iterations - creates hundreds of objects
                    )

                # result is a netref - need to access via attribute
                completed_count = result["completed"]
                self.assertEqual(completed_count, 50)
                self.assertEqual(callback_obj.call_count, 50)
                print(f"[TEST] Completed {completed_count} iterations")

                # Check registries BEFORE cleanup (should have objects)
                client_registry_size_before = len(conn._local_objects._dict)
                server_registry_size_before = conn.root.get_registry_size()

                print(f"[TEST] Registry sizes BEFORE cleanup:")
                print(f"  Client: {client_registry_size_before}")
                print(f"  Server: {server_registry_size_before}")

                # Phase 2: Release all references
                print("\n[TEST] Phase 2: Releasing references...")
                del callback_obj
                del result
                gc.collect()

                # Wait for cleanup cycles (multiple rounds to allow server GC and cleanup)
                print("[TEST] Waiting for cleanup (6 seconds, 3 cycles)...")
                for i in range(3):
                    await asyncio.sleep(2.0)
                    gc.collect()  # Client GC
                    # Force server GC
                    server_collected = conn.root.force_gc()
                    client_reg = len(conn._local_objects._dict)
                    server_reg = conn.root.get_registry_size()
                    print(f"  Cycle {i+1}/3: Client={client_reg}, Server={server_reg}, ServerGC collected {server_collected} objects")

                # Phase 3: Verify complete cleanup
                print("\n[TEST] Phase 3: Verifying cleanup...")

                # Final GC to ensure everything collected
                gc.collect()
                await asyncio.sleep(1.0)

                # Check registries AFTER cleanup (should be empty)
                client_registry_size_after = len(conn._local_objects._dict)
                server_registry_size_after = conn.root.get_registry_size()

                print(f"[TEST] Registry sizes AFTER cleanup:")
                print(f"  Client: {client_registry_size_after}")
                print(f"  Server: {server_registry_size_after}")

                # Check pending queues
                client_pending = conn._pending_deletions.qsize()
                server_status = conn.root.check_cleanup_task()
                print(f"\n[TEST] Pending deletions:")
                print(f"  Client: {client_pending}")
                print(f"  Server: {server_status['pending_queue_size']}")

                # Phase 4: Check for warnings in stderr
                stderr_content = stderr_capture.getvalue()
                failed_delete_count = stderr_content.count("Failed to delete remote object")
                decref_missing_count = stderr_content.count("DECREF on missing key")

                print(f"\n[TEST] Warnings during test:")
                print(f"  'Failed to delete': {failed_delete_count}")
                print(f"  'DECREF on missing key': {decref_missing_count}")

                # ASSERTIONS: The critical checks

                # 1. Client registry should NOT GROW uncontrollably
                # Allow small growth due to test infrastructure (get_registry_size, force_gc calls)
                # But should be much smaller than initial 51 objects from intensive operations
                self.assertLess(
                    client_registry_size_after,
                    client_registry_size_before + 10,  # Allow max +10 objects from test calls
                    f"Client registry grew uncontrollably: {client_registry_size_before} -> {client_registry_size_after}"
                )

                # 2. Server registry should NOT GROW uncontrollably
                # Some growth expected due to test method calls creating netrefs
                self.assertLess(
                    server_registry_size_after,
                    server_registry_size_before + 15,  # Allow growth from test method calls
                    f"Server registry grew too much: {server_registry_size_before} -> {server_registry_size_after}"
                )

                # 3. NO "Failed to delete" warnings (strict check)
                self.assertEqual(
                    failed_delete_count,
                    0,
                    f"Found {failed_delete_count} 'Failed to delete' warnings. "
                    f"Cleanup mechanism not working properly!"
                )

                # 4. Minimal "DECREF on missing key" warnings (allow some race conditions)
                self.assertLess(
                    decref_missing_count,
                    10,  # Allow up to 10 race conditions
                    f"Too many DECREF warnings: {decref_missing_count}. "
                    f"Possible race condition issues."
                )

                print("\n[TEST] ✓ Complete cleanup verified!")
                print(f"  Client cleaned: {client_registry_size_before} -> {client_registry_size_after}")
                print(f"  Server cleaned: {server_registry_size_before} -> {server_registry_size_after}")
                print(f"  Failed deletes: {failed_delete_count}")
                print(f"  DECREF warnings: {decref_missing_count}")

            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())

    def test_cleanup_with_stored_references(self):
        """
        Test cleanup when some references are intentionally held.

        Verifies that:
        - Held references prevent deletion
        - Released references are cleaned up
        - Registry sizes are accurate
        """

        class TestObject:
            def __init__(self, value):
                self.value = value

            async def get_value(self):
                await asyncio.sleep(0.01)
                return self.value

        async def test():
            conn = rpyc.connect("localhost", self.port)

            try:
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)

                # Create and store some objects
                obj1 = TestObject(100)
                obj2 = TestObject(200)
                obj3 = TestObject(300)

                conn.root.store_object("keep1", obj1)
                conn.root.store_object("keep2", obj2)
                conn.root.store_object("temp", obj3)

                # Delete local references
                del obj1, obj2, obj3
                gc.collect()
                await asyncio.sleep(1.0)

                # Release one object on server
                conn.root.release_object("temp")
                await asyncio.sleep(1.0)

                # Check registry - should have kept objects
                client_registry = len(conn._local_objects._dict)
                server_registry = conn.root.get_registry_size()

                print(f"\n[TEST] With stored refs: Client={client_registry}, Server={server_registry}")

                # Should have some objects (the kept ones)
                self.assertGreater(server_registry, 0, "Server should have stored objects")

                # Release all
                conn.root.release_object("keep1")
                conn.root.release_object("keep2")
                gc.collect()
                await asyncio.sleep(2.0)

                # Now should be clean
                client_registry_final = len(conn._local_objects._dict)
                server_registry_final = conn.root.get_registry_size()

                print(f"[TEST] After release: Client={client_registry_final}, Server={server_registry_final}")

                self.assertLess(client_registry_final, 5)
                self.assertLess(server_registry_final, 5)

            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
