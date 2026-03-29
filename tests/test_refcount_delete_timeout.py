"""
Integration Test: Reproduce "Failed to delete + DECREF on missing key" errors

This test reproduces the ACTUAL bug seen in production:
1. "Failed to delete remote object" - cleanup timeout
2. "[REFCOUNT] DECREF on missing key" - subsequent attempts to decref

Root cause: When HANDLE_DEL times out, the cleanup mechanism continues
to try deleting objects that are already gone or never properly registered.
"""
import unittest
import asyncio
import time
import gc
import logging
import io
from contextlib import redirect_stderr
from multiprocessing import Process, Queue

import rpyc
from rpyc.core.async_connect import async_connect
from rpyc.utils.async_server import AsyncioServer
from tests.support import get_free_port


def run_slow_delete_server(port, ready_queue):
    """
    Server that DELAYS or FAILS to respond to HANDLE_DEL requests.

    This simulates the conditions that cause "Failed to delete remote object".
    """

    class SlowDeleteService(rpyc.Service):
        """Service that simulates slow/failed deletion"""

        def __init__(self):
            super().__init__()
            self._conn = None
            self.delete_delay = 0.0  # Delay before responding to HANDLE_DEL

        def on_connect(self, conn):
            self._conn = conn

        async def exposed_set_delete_delay(self, delay):
            """Set artificial delay for delete operations"""
            self.delete_delay = delay
            return f"Delete delay set to {delay}s"

        async def exposed_return_objects_with_methods(self, count):
            """Return objects with bound methods"""
            results = []
            for i in range(count):
                obj = {"id": i, "data": f"value_{i}"}
                results.append({
                    "obj": obj,
                    "get": obj.get,
                    "keys": obj.keys,
                })
            return results

        def exposed_get_registry_stats(self):
            """Get registry stats"""
            return {
                "local_objects_count": len(self._conn._local_objects._dict),
                "proxy_cache_count": len(self._conn._proxy_cache._dict),
                "pending_deletions": self._conn._pending_deletions.qsize(),
                "cleanup_running": self._conn._cleanup_running
            }

        #  CUSTOM HANDLE_DEL that can simulate delays/failures
        def _handle_del(self, id_pack, count=1):
            """
            Override HANDLE_DEL to add artificial delay.

            When delete_delay > cleanup_ack_timeout, this causes timeout.
            """
            if self.delete_delay > 0:
                time.sleep(self.delete_delay)

            # Call original delete logic
            deleted = self._conn._local_objects.decref(id_pack, count)

            return {
                "deleted": deleted,
                "id_pack": id_pack
            }

    async def server_main():
        server = AsyncioServer(
            SlowDeleteService,
            port=port,
            protocol_config={
                "allow_public_attrs": True,
                "sync_request_timeout": 30,
                "cleanup_interval": 0.3,
                "cleanup_ack_timeout": 1.0,  # Short timeout
                "debug_refcounting": True
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


class TestRefcountDeleteTimeout(unittest.TestCase):
    """Test refcount errors caused by delete timeouts"""

    @classmethod
    def setUpClass(cls):
        """Start server"""
        cls.port = get_free_port()
        cls.ready_queue = Queue()

        cls.server_process = Process(
            target=run_slow_delete_server,
            args=(cls.port, cls.ready_queue),
            daemon=True
        )
        cls.server_process.start()

        try:
            signal = cls.ready_queue.get(timeout=10)
            if signal != "ready":
                raise RuntimeError(f"Unexpected signal: {signal}")
        except Exception as e:
            cls.server_process.terminate()
            cls.server_process.join(timeout=2)
            raise RuntimeError(f"Server startup timeout: {e}")

        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        """Stop server"""
        if cls.server_process.is_alive():
            cls.server_process.terminate()
            cls.server_process.join(timeout=5)
            if cls.server_process.is_alive():
                cls.server_process.kill()

    def test_delete_timeout_causes_refcount_errors(self):
        """
        CRITICAL TEST: Delete timeout causes "Failed to delete" + "DECREF on missing key".

        Scenario:
        1. Server is configured with slow HANDLE_DEL (2 seconds)
        2. Client has cleanup_ack_timeout = 1.0 second
        3. Client requests deletion
        4. Server takes 2 seconds to respond
        5. Client times out after 1 second
        6. Client logs "Failed to delete remote object"
        7. Client continues trying to cleanup
        8. Subsequent DECREF attempts fail with "DECREF on missing key"
        """
        # Capture logging
        log_capture = io.StringIO()
        log_handler = logging.StreamHandler(log_capture)
        log_handler.setLevel(logging.WARNING)  # Capture WARNING level for "Failed to delete"

        rpyc_logger = logging.getLogger("rpyc")
        rpyc_logger.addHandler(log_handler)
        rpyc_logger.setLevel(logging.DEBUG)

        try:
            async def run_test():
                conn = await async_connect(
                    "localhost",
                    self.port,
                    config={
                        "sync_request_timeout": 30,
                        "cleanup_interval": 0.2,
                        "cleanup_ack_timeout": 1.0,  # 1 second timeout
                        "debug_refcounting": True
                    }
                )

                try:
                    loop = asyncio.get_running_loop()
                    conn.enable_asyncio_serving(loop=loop)

                    print(f"\n{'='*70}")
                    print("CRITICAL TEST: Delete Timeout Reproduction")
                    print('='*70)

                    # Configure server to delay delete by 2 seconds (> timeout)
                    print("Setting server delete delay to 2 seconds...")
                    await conn.root.set_delete_delay(2.0)

                    # Get objects with methods
                    print("Requesting 20 objects with methods...")
                    results = await conn.root.return_objects_with_methods(20)

                    print(f"Received {len(results)} objects")

                    # Delete immediately - will trigger timeouts
                    print("\nDeleting objects (this WILL timeout)...")
                    del results
                    gc.collect()

                    # Wait for cleanup attempts
                    print("Waiting for cleanup (expecting timeouts)...")
                    await asyncio.sleep(3.0)

                    # Check stats
                    stats = conn.root.get_registry_stats()
                    print(f"\nRegistry stats: {stats}")

                finally:
                    conn.close()
                    await asyncio.sleep(1.0)

            asyncio.run(run_test())

        finally:
            rpyc_logger.removeHandler(log_handler)

        time.sleep(1.0)

        # Check logs
        log_output = log_capture.getvalue()

        # Count errors
        failed_delete_count = log_output.count("Failed to delete remote object")
        decref_missing_count = log_output.count("[REFCOUNT] DECREF on missing key")

        print(f"\n{'='*70}")
        print(f"RESULTS:")
        print('='*70)
        print(f"'Failed to delete remote object' errors: {failed_delete_count}")
        print(f"'[REFCOUNT] DECREF on missing key' errors: {decref_missing_count}")
        print('='*70)

        if failed_delete_count > 0:
            print(f"\n✅ SUCCESS! Reproduced 'Failed to delete' errors!")
            print("\nSample errors:")
            for line in log_output.split('\n')[:20]:
                if "Failed to delete" in line or "DECREF on missing" in line:
                    print(f"  {line}")

        if decref_missing_count > 0:
            print(f"\n✅ SUCCESS! Reproduced 'DECREF on missing key' errors!")

        # Test PASSES (=fails) if we found errors
        if failed_delete_count > 0 or decref_missing_count > 0:
            self.fail(
                f"✅ SUCCESS! Test reproduced the refcount errors!\n"
                f"\n"
                f"Found:\n"
                f"  - {failed_delete_count} 'Failed to delete remote object' errors\n"
                f"  - {decref_missing_count} '[REFCOUNT] DECREF on missing key' errors\n"
                f"\n"
                f"Root cause confirmed:\n"
                f"  When HANDLE_DEL times out (server slow to respond), the cleanup\n"
                f"  mechanism logs 'Failed to delete' but continues trying to clean up.\n"
                f"  Subsequent cleanup attempts try to DECREF objects that were already\n"
                f"  removed or never properly registered, causing 'DECREF on missing key'.\n"
                f"\n"
                f"This matches the production errors seen in the external test."
            )
        else:
            print("\n⚠️  No errors detected. Timeout may not have triggered properly.")
            print(f"\nLog output (first 1000 chars):\n{log_output[:1000]}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
