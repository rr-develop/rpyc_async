"""
Simplified cleanup test with extensive print statements
"""
import unittest
import asyncio
import time
import gc
import rpyc
from rpyc.utils.async_server import AsyncioServer
from multiprocessing import Process, Queue
from tests.support import get_free_port


def run_simple_server(port, ready_queue):
    """Simple server"""

    class SimpleService(rpyc.Service):
        def __init__(self):
            super().__init__()
            self._conn = None

        def on_connect(self, conn):
            self._conn = conn
            print(f"[SERVER] Connection established")

        async def exposed_create_dict(self):
            """Create a simple dict"""
            result = {"value": 42, "data": "test"}
            print(f"[SERVER] Created dict: {result}")
            return result

        def exposed_get_stats(self):
            """Get server stats"""
            return {
                "registry_size": len(self._conn._local_objects._dict),
                "cleanup_running": self._conn._cleanup_running,
                "pending_queue": self._conn._pending_deletions.qsize(),
            }

    async def server_main():
        server = AsyncioServer(
            SimpleService,
            port=port,
            protocol_config={
                "allow_public_attrs": True,
                "cleanup_interval": 0.5,  # Fast cleanup
            }
        )

        try:
            await server.start()
            ready_queue.put("ready")
            print(f"[SERVER] Server started on port {port}")
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        finally:
            await server.close()

    try:
        asyncio.run(server_main())
    except KeyboardInterrupt:
        pass


class TestCleanupSimpleTrace(unittest.TestCase):
    def setUp(self):
        self.port = get_free_port()
        self.ready_queue = Queue()
        self.server_process = Process(
            target=run_simple_server,
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
        if self.server_process.is_alive():
            self.server_process.terminate()
            self.server_process.join(timeout=2.0)

        if self.server_process.is_alive():
            self.server_process.kill()
            self.server_process.join(timeout=1.0)

    def test_single_object_cleanup_trace(self):
        """Trace cleanup of a single object"""

        async def test():
            print("\n" + "="*70)
            print("TEST: Single Object Cleanup Trace")
            print("="*70)

            conn = rpyc.connect("localhost", self.port)

            try:
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)

                print(f"\n[CLIENT] Connected")
                print(f"[CLIENT] Cleanup running: {conn._cleanup_running}")
                print(f"[CLIENT] Cleanup task exists: {conn._cleanup_task is not None}")
                print(f"[CLIENT] Cleanup interval: {conn._cleanup_interval}s")

                # Get server stats
                server_stats = conn.root.get_stats()
                print(f"\n[SERVER] Stats: {server_stats}")

                print("\n--- Phase 1: Create object ---")
                result = await conn.root.create_dict()
                print(f"[CLIENT] Received: {result}")

                # Check queue
                print(f"\n[CLIENT] Pending deletions: {conn._pending_deletions.qsize()}")

                # Check registries
                client_reg = len(conn._local_objects._dict)
                server_stats = conn.root.get_stats()
                print(f"[CLIENT] Registry size: {client_reg}")
                print(f"[SERVER] Registry size: {server_stats['registry_size']}")

                print("\n--- Phase 2: Delete reference ---")
                del result
                gc.collect()

                # Check queue immediately after deletion
                print(f"[CLIENT] Pending deletions (after del): {conn._pending_deletions.qsize()}")

                print("\n--- Phase 3: Wait for cleanup (3 seconds) ---")
                # Force GC multiple times
                for i in range(3):
                    gc.collect()
                    await asyncio.sleep(1.0)
                    print(f"[CLIENT] After {i+1}s: pending={conn._pending_deletions.qsize()}")

                # Final GC
                gc.collect()

                # Check queue after cleanup
                print(f"[CLIENT] Pending deletions (final): {conn._pending_deletions.qsize()}")

                # Check registries after cleanup
                client_reg_after = len(conn._local_objects._dict)
                server_stats_after = conn.root.get_stats()
                print(f"[CLIENT] Registry size (after): {client_reg_after}")
                print(f"[SERVER] Registry size (after): {server_stats_after['registry_size']}")
                print(f"[SERVER] Pending queue (after): {server_stats_after['pending_queue']}")

                print("\n--- Analysis ---")
                if conn._pending_deletions.qsize() > 0:
                    print(f"⚠️  WARNING: Client still has {conn._pending_deletions.qsize()} pending deletions!")
                    print("   This means cleanup task is NOT processing the queue!")

                if server_stats_after['pending_queue'] > 0:
                    print(f"⚠️  WARNING: Server still has {server_stats_after['pending_queue']} pending deletions!")

                if client_reg == client_reg_after:
                    print(f"✓ Client registry unchanged: {client_reg} (expected - no local objects)")

                if server_stats['registry_size'] == server_stats_after['registry_size']:
                    print(f"⚠️  WARNING: Server registry unchanged: {server_stats['registry_size']} -> {server_stats_after['registry_size']}")
                    print("   Objects not being cleaned up!")
                else:
                    print(f"✓ Server registry changed: {server_stats['registry_size']} -> {server_stats_after['registry_size']}")

            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
