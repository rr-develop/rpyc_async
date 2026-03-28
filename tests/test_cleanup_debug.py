"""
E2E Test: Cleanup Mechanism Debug Tracing

This test enables extensive debug logging to trace the cleanup mechanism:
1. Is _cleanup_task running?
2. Is _pending_deletions queue being processed?
3. Does _handle_del() actually decrement refcounts?
4. What are the actual registry contents?
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


def run_debug_test_server(port, ready_queue):
    """Server process with debug logging"""

    class DebugTestService(rpyc.Service):
        """Service for cleanup debugging"""

        def __init__(self):
            super().__init__()
            self._conn = None

        def on_connect(self, conn):
            """Store connection reference"""
            self._conn = conn

        async def exposed_create_objects(self, count):
            """Create temporary objects"""
            results = []
            for i in range(count):
                temp_dict = {"value": i, "data": f"item_{i}"}
                results.append(temp_dict)
            return results

        def exposed_get_registry_size(self):
            """Get size of _local_objects registry"""
            return len(self._conn._local_objects._dict)

        def exposed_get_registry_contents(self):
            """Get actual registry contents (id_packs and refcounts)"""
            items = []
            for id_pack, obj_tuple in self._conn._local_objects._dict.items():
                # obj_tuple is (obj, refcount)
                items.append({
                    "id_pack": str(id_pack),
                    "refcount": obj_tuple[1]
                })
            return items

        def exposed_check_cleanup_task(self):
            """Check if cleanup task is running"""
            return {
                "cleanup_running": self._conn._cleanup_running,
                "cleanup_task_exists": self._conn._cleanup_task is not None,
                "cleanup_interval": self._conn._cleanup_interval,
                "pending_queue_size": self._conn._pending_deletions.qsize()
            }

    async def server_main():
        server = AsyncioServer(
            DebugTestService,
            port=port,
            protocol_config={
                "allow_public_attrs": True,
                "allow_pickle": True,
                "cleanup_interval": 0.5,  # Fast cleanup
                "debug_refcounting": True,  # Enable debug logging
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


class TestCleanupDebug(unittest.TestCase):
    """Debug test for cleanup mechanism"""

    def setUp(self):
        """Start server"""
        self.port = get_free_port()
        self.ready_queue = Queue()

        self.server_process = Process(
            target=run_debug_test_server,
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

    def test_cleanup_mechanism_debug_trace(self):
        """
        Debug test: Trace cleanup mechanism step by step
        """

        async def test():
            # Enable debug output
            stderr_capture = io.StringIO()

            conn = rpyc.connect(
                "localhost",
                self.port,
                config={
                    "debug_refcounting": True  # Enable debug logging
                }
            )

            try:
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)

                print("\n" + "="*70)
                print("PHASE 1: Check if cleanup tasks are running")
                print("="*70)

                # Check client-side cleanup task
                client_status = {
                    "cleanup_running": conn._cleanup_running,
                    "cleanup_task_exists": conn._cleanup_task is not None,
                    "cleanup_interval": conn._cleanup_interval,
                    "pending_queue_size": conn._pending_deletions.qsize()
                }
                print(f"[CLIENT] Cleanup status: {client_status}")

                # Check server-side cleanup task
                server_status = conn.root.check_cleanup_task()
                print(f"[SERVER] Cleanup status: {server_status}")

                print("\n" + "="*70)
                print("PHASE 2: Create objects and check registries")
                print("="*70)

                # Create some objects
                with redirect_stderr(stderr_capture):
                    result = await conn.root.create_objects(10)

                # Check registries BEFORE cleanup
                client_before = len(conn._local_objects._dict)
                server_before = conn.root.get_registry_size()

                print(f"[BEFORE] Client registry: {client_before} objects")
                print(f"[BEFORE] Server registry: {server_before} objects")

                # Print actual contents
                print(f"\n[CLIENT] Registry contents:")
                for id_pack, obj_tuple in list(conn._local_objects._dict.items())[:5]:
                    print(f"  {id_pack}: refcount={obj_tuple[1]}")

                server_contents = conn.root.get_registry_contents()
                print(f"\n[SERVER] Registry contents (first 5):")
                for item in server_contents[:5]:
                    print(f"  {item['id_pack']}: refcount={item['refcount']}")

                print("\n" + "="*70)
                print("PHASE 3: Release references and trigger cleanup")
                print("="*70)

                # Release references
                del result
                gc.collect()

                print(f"[CLIENT] Pending deletions queued: {conn._pending_deletions.qsize()}")

                # Wait for cleanup cycle
                print("[TEST] Waiting 3 seconds for cleanup cycle...")
                await asyncio.sleep(3.0)

                print("\n" + "="*70)
                print("PHASE 4: Check registries AFTER cleanup")
                print("="*70)

                # Force GC
                gc.collect()
                await asyncio.sleep(1.0)

                # Check registries AFTER cleanup
                client_after = len(conn._local_objects._dict)
                server_after = conn.root.get_registry_size()

                print(f"[AFTER] Client registry: {client_after} objects (was {client_before})")
                print(f"[AFTER] Server registry: {server_after} objects (was {server_before})")

                # Print pending queue status
                print(f"[CLIENT] Pending deletions remaining: {conn._pending_deletions.qsize()}")
                server_status_after = conn.root.check_cleanup_task()
                print(f"[SERVER] Pending deletions remaining: {server_status_after['pending_queue_size']}")

                print("\n" + "="*70)
                print("PHASE 5: Check stderr for debug messages")
                print("="*70)

                stderr_content = stderr_capture.getvalue()
                lines = stderr_content.split('\n')

                # Count specific debug messages
                cleanup_task_msgs = [l for l in lines if 'CLEANUP' in l]
                decref_msgs = [l for l in lines if 'DECREF' in l or 'decref' in l]
                handle_del_msgs = [l for l in lines if 'HANDLE_DEL' in l]

                print(f"Cleanup task messages: {len(cleanup_task_msgs)}")
                print(f"Decref messages: {len(decref_msgs)}")
                print(f"HANDLE_DEL messages: {len(handle_del_msgs)}")

                if cleanup_task_msgs:
                    print("\nFirst 5 cleanup messages:")
                    for msg in cleanup_task_msgs[:5]:
                        print(f"  {msg}")

                if decref_msgs:
                    print("\nFirst 5 decref messages:")
                    for msg in decref_msgs[:5]:
                        print(f"  {msg}")

                print("\n" + "="*70)
                print("PHASE 6: Analysis")
                print("="*70)

                # Analyze results
                cleanup_works = client_after < client_before or server_after < server_before
                print(f"Did cleanup reduce registry sizes? {cleanup_works}")

                if not cleanup_works:
                    print("\n⚠️  PROBLEM DETECTED: Cleanup did not reduce registry sizes!")
                    print("\nPossible causes:")
                    if not client_status["cleanup_running"]:
                        print("  - Client cleanup task not running")
                    if not server_status["cleanup_running"]:
                        print("  - Server cleanup task not running")
                    if conn._pending_deletions.qsize() > 0:
                        print(f"  - Client has {conn._pending_deletions.qsize()} deletions still pending")
                    if server_status_after['pending_queue_size'] > 0:
                        print(f"  - Server has {server_status_after['pending_queue_size']} deletions still pending")
                    if len(decref_msgs) == 0:
                        print("  - No decref messages in stderr (decref not being called?)")
                    if len(handle_del_msgs) == 0:
                        print("  - No HANDLE_DEL messages in stderr (_handle_del not being called?)")

                # Show full stderr for debugging
                print("\n" + "="*70)
                print("FULL STDERR OUTPUT:")
                print("="*70)
                print(stderr_content if stderr_content else "(empty)")

            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
