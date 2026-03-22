"""
CRITICAL TEST: Bidirectional Async Recursive Calls

This is the most important test - it verifies that:
1. Server async method can call client async callback
2. Client async callback can call server async method (recursion)
3. All uses existing event loops (NO new threads)
4. Bidirectional connection works correctly

Scenario:
Server.async_process(callback, depth) →
    calls client.async_callback(value) →
        calls server.async_process(callback, depth-1) →
            ... recursive until depth=0
"""
import unittest
import asyncio
import rpyc
from rpyc.utils.async_server import AsyncioServer
from threading import Thread
import time
from tests.support import get_free_port


class ServerService(rpyc.Service):
    """Server service with async method that calls client callback."""

    async def exposed_async_process_with_callback(self, callback, value, depth):
        """
        Async method that recursively calls client callback.

        Args:
            callback: Client async callback function
            value: Current value
            depth: Recursion depth

        Returns:
            Final result after recursion
        """
        print(f"[SERVER] async_process_with_callback(value={value}, depth={depth})")

        # Simulate async work
        await asyncio.sleep(0.01)

        if depth <= 0:
            return f"Final: {value}"

        # Call client's async callback - this should work!
        print(f"[SERVER] Calling client callback with value={value * 2}")
        result = await callback(value * 2, depth - 1)

        return f"Server processed: {result}"

    async def exposed_simple_async(self, x):
        """Simple async method for testing."""
        await asyncio.sleep(0.01)
        return x * 2


class ClientService(rpyc.Service):
    """Client service (acts as server for callbacks)."""

    def __init__(self, server_conn):
        super().__init__()
        self.server_conn = server_conn

    async def exposed_async_callback(self, value, depth):
        """
        Client async callback that calls back to server.

        This is the critical part - async callback calling async server method.
        """
        print(f"[CLIENT] async_callback(value={value}, depth={depth})")

        # Simulate async work
        await asyncio.sleep(0.01)

        if depth <= 0:
            return f"Client finished: {value}"

        # Recursive call back to server - CRITICAL!
        print(f"[CLIENT] Calling server.async_process_with_callback recursively")
        result = await self.server_conn.root.async_process_with_callback(
            self.exposed_async_callback,
            value + 10,
            depth - 1
        )

        return f"Client processed: {result}"


class TestCriticalBidirectionalAsync(unittest.TestCase):
    """
    CRITICAL TEST: Bidirectional async with recursion.

    This test MUST PASS for the implementation to be valid.

    Each test gets its own isolated server to prevent race conditions.
    """

    def setUp(self):
        """Start AsyncioServer in background event loop for this test."""
        # Get free port dynamically to avoid conflicts
        self.server_port = get_free_port()

        # Create event loop for server
        self.server_loop = asyncio.new_event_loop()

        async def run_server():
            self.server = AsyncioServer(
                ServerService,
                hostname='localhost',
                port=self.server_port,
                protocol_config={
                    'allow_all_attrs': True,
                    'allow_public_attrs': True,
                }
            )
            await self.server.start()

        # Start server in background thread with its own event loop
        def start_server():
            asyncio.set_event_loop(self.server_loop)
            self.server_loop.run_until_complete(run_server())
            self.server_loop.run_forever()

        self.server_thread = Thread(target=start_server, daemon=True)
        self.server_thread.start()
        time.sleep(0.5)  # Give server time to start

    def tearDown(self):
        """Stop server after this test."""
        async def stop_server():
            await self.server.close()

        # Schedule close and wait for it
        future = asyncio.run_coroutine_threadsafe(stop_server(), self.server_loop)
        try:
            future.result(timeout=2.0)  # Wait for clean shutdown
        except:
            pass  # Ignore timeout errors on shutdown

        # Stop loop
        self.server_loop.call_soon_threadsafe(self.server_loop.stop)
        time.sleep(0.1)  # Brief pause for cleanup

    def test_simple_async_call_first(self):
        """Test simple async call works (baseline)."""
        async def test():
            # Connect to server
            server_conn = rpyc.connect("localhost", self.server_port)

            try:
                # Enable asyncio serving on client side
                loop = asyncio.get_running_loop()
                server_conn.enable_asyncio_serving(loop=loop)

                # Simple async call
                result = await server_conn.root.simple_async(5)
                self.assertEqual(result, 10)
                print(f"✓ Simple async call works: {result}")
            finally:
                server_conn.disable_asyncio_serving()
                server_conn.close()

        asyncio.run(test())

    def test_bidirectional_async_with_recursion_depth_3(self):
        """
        CRITICAL TEST: Bidirectional async callbacks with recursion.

        This is THE most important test. If this doesn't work,
        the implementation is incomplete.
        """
        async def test():
            print("\n" + "="*60)
            print("CRITICAL TEST: Bidirectional async with recursion (depth=3)")
            print("="*60)

            # Connect to server
            server_conn = rpyc.connect("localhost", self.server_port)

            try:
                # Enable asyncio serving on server connection
                # This is CRITICAL for bidirectional async
                loop = asyncio.get_running_loop()
                server_conn.enable_asyncio_serving(loop=loop)
                print("✓ Server connection: asyncio serving enabled")

                # Create client service for callbacks
                client_service = ClientService(server_conn)

                # Get the async callback method
                async_callback = client_service.exposed_async_callback

                print("\n[TEST] Starting recursive async call chain...")
                print("[TEST] Server → Client → Server → Client → ... (depth=3)\n")

                # Call server method with client callback
                # Server will call client, client will call server, etc.
                result = await server_conn.root.async_process_with_callback(
                    async_callback,
                    value=1,
                    depth=3
                )

                print(f"\n[TEST] Final result: {result}")
                print("="*60)

                # Verify result structure
                self.assertIn("Server processed", result)
                self.assertIn("Client", result)

                print("✓ CRITICAL TEST PASSED!")

            finally:
                server_conn.disable_asyncio_serving()
                server_conn.close()

        asyncio.run(test())

    def test_bidirectional_async_depth_5(self):
        """Test deeper recursion (depth=5)."""
        async def test():
            print("\n" + "="*60)
            print("CRITICAL TEST: Bidirectional async with recursion (depth=5)")
            print("="*60)

            server_conn = rpyc.connect("localhost", self.server_port)

            try:
                loop = asyncio.get_running_loop()
                server_conn.enable_asyncio_serving(loop=loop)

                client_service = ClientService(server_conn)
                async_callback = client_service.exposed_async_callback

                print("\n[TEST] Starting deep recursive call chain (depth=5)...\n")

                result = await server_conn.root.async_process_with_callback(
                    async_callback,
                    value=1,
                    depth=5
                )

                print(f"\n[TEST] Final result: {result}")
                print("="*60)

                self.assertIn("Server processed", result)
                print("✓ Deep recursion test PASSED!")

            finally:
                server_conn.disable_asyncio_serving()
                server_conn.close()

        asyncio.run(test())

    def test_verify_no_thread_creation(self):
        """
        Verify that event loops are reused, not creating new threads.

        This test checks that we're using existing event loops.
        """
        async def test():
            import threading

            initial_thread_count = threading.active_count()
            print(f"\n[TEST] Initial thread count: {initial_thread_count}")

            server_conn = rpyc.connect("localhost", self.server_port)

            try:
                loop = asyncio.get_running_loop()
                server_conn.enable_asyncio_serving(loop=loop)

                # Check thread count after enabling asyncio
                after_enable_count = threading.active_count()
                print(f"[TEST] After enable_asyncio_serving: {after_enable_count}")

                # Should not create new threads (might be ±1 due to thread pool)
                self.assertLessEqual(
                    after_enable_count - initial_thread_count,
                    1,
                    "Too many threads created!"
                )

                client_service = ClientService(server_conn)
                async_callback = client_service.exposed_async_callback

                # Execute recursive calls
                result = await server_conn.root.async_process_with_callback(
                    async_callback,
                    value=1,
                    depth=3
                )

                # Check thread count after execution
                after_exec_count = threading.active_count()
                print(f"[TEST] After execution: {after_exec_count}")

                # Should not have created many new threads
                self.assertLessEqual(
                    after_exec_count - initial_thread_count,
                    2,
                    "Too many threads created during execution!"
                )

                print("✓ No excessive thread creation detected")

            finally:
                server_conn.disable_asyncio_serving()
                server_conn.close()

        asyncio.run(test())


if __name__ == '__main__':
    # Run with verbose output
    unittest.main(verbosity=2)
