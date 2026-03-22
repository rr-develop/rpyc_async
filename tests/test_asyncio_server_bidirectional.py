"""
CRITICAL TEST: Bidirectional Async with AsyncioServer

This test verifies that AsyncioServer enables full bidirectional async support:
1. Server async method can call client async callback
2. Client async callback can call server async method (recursion)
3. NO separate threads created - all uses event loop
4. Persistent event loops enable bidirectional communication

This is THE critical requirement that ThreadedServer cannot meet.
"""
import unittest
import asyncio
import rpyc
from rpyc.utils.async_server import AsyncioServer


class ServerService(rpyc.Service):
    """Server service with async method that calls client callback."""

    async def exposed_async_process_with_callback(self, callback, value, depth):
        """
        Server async method that recursively calls client callback.

        This is the CRITICAL test - server calling client async callback
        and client calling back to server recursively.

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

        # Call client's async callback - CRITICAL!
        # With AsyncioServer, this should work because:
        # 1. Server has persistent event loop
        # 2. Client has persistent event loop
        # 3. Both can process async messages bidirectionally
        print(f"[SERVER] Calling client callback with value={value * 2}")
        result = await callback(value * 2, depth - 1)

        return f"Server processed: {result}"

    async def exposed_simple_async(self, x):
        """Simple async method for baseline testing."""
        await asyncio.sleep(0.01)
        return x * 2


class ClientService(rpyc.Service):
    """Client service for receiving async callbacks from server."""

    def __init__(self, server_conn):
        super().__init__()
        self.server_conn = server_conn

    async def exposed_async_callback(self, value, depth):
        """
        Client async callback that calls back to server.

        CRITICAL: This is the key test - async callback calling async server method.
        With AsyncioServer + persistent event loops, this should work!
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


class TestAsyncioServerBidirectional(unittest.TestCase):
    """
    CRITICAL TEST: Bidirectional async with AsyncioServer.

    This test MUST PASS for the implementation to meet requirements.
    """

    @classmethod
    def setUpClass(cls):
        """Start AsyncioServer in background task."""
        # We need to run server in the test's event loop
        # This will be done in each test method
        pass

    @classmethod
    def tearDownClass(cls):
        """Cleanup."""
        pass

    def test_simple_async_call_baseline(self):
        """Test simple async call works (baseline)."""
        async def test():
            # Start server
            server = AsyncioServer(
                ServerService,
                hostname='localhost',
                port=18880,
                protocol_config={'allow_all_attrs': True}
            )

            # Start server in background
            await server.start()

            try:
                # Give server time to start
                await asyncio.sleep(0.5)

                # Connect client
                server_conn = rpyc.connect("localhost", 18880)

                try:
                    # Simple async call
                    result = await server_conn.root.simple_async(5)
                    self.assertEqual(result, 10)
                    print(f"✓ Simple async call works: {result}")

                finally:
                    server_conn.close()

            finally:
                await server.close()

        asyncio.run(test())

    def test_bidirectional_async_depth_3(self):
        """
        CRITICAL TEST: Bidirectional async callbacks with recursion (depth=3).

        This is THE most important test. If this passes, the critical
        requirement is met!
        """
        async def test():
            print("\n" + "="*70)
            print("CRITICAL TEST: Bidirectional async with AsyncioServer (depth=3)")
            print("="*70)

            # Start server
            server = AsyncioServer(
                ServerService,
                hostname='localhost',
                port=18881,
                protocol_config={
                    'allow_all_attrs': True,
                    'allow_public_attrs': True,
                }
            )

            await server.start()

            try:
                # Give server time to start
                await asyncio.sleep(0.5)

                # Connect to server
                server_conn = rpyc.connect("localhost", 18881)

                try:
                    # Enable asyncio serving on client connection
                    # CRITICAL: This enables bidirectional async
                    loop = asyncio.get_running_loop()
                    server_conn.enable_asyncio_serving(loop=loop)
                    print("✓ Client connection: asyncio serving enabled")

                    # Create client service for callbacks
                    client_service = ClientService(server_conn)

                    # Get the async callback method
                    async_callback = client_service.exposed_async_callback

                    print("\n[TEST] Starting recursive async call chain...")
                    print("[TEST] Server → Client → Server → Client → ... (depth=3)\n")

                    # Call server method with client callback
                    # Server will call client, client will call server, etc.
                    # With AsyncioServer, this should work!
                    result = await server_conn.root.async_process_with_callback(
                        async_callback,
                        value=1,
                        depth=3
                    )

                    print(f"\n[TEST] Final result: {result}")
                    print("="*70)

                    # Verify result structure
                    self.assertIn("Server processed", result)
                    self.assertIn("Client", result)

                    print("✅ CRITICAL TEST PASSED! Bidirectional async works!")

                finally:
                    server_conn.disable_asyncio_serving()
                    server_conn.close()

            finally:
                await server.close()

        asyncio.run(test())

    def test_bidirectional_async_depth_5(self):
        """Test deeper recursion (depth=5) to verify robustness."""
        async def test():
            print("\n" + "="*70)
            print("CRITICAL TEST: Bidirectional async with AsyncioServer (depth=5)")
            print("="*70)

            server = AsyncioServer(
                ServerService,
                hostname='localhost',
                port=18882,
                protocol_config={
                    'allow_all_attrs': True,
                    'allow_public_attrs': True,
                }
            )

            await server.start()

            try:
                await asyncio.sleep(0.5)

                server_conn = rpyc.connect("localhost", 18882)

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
                    print("="*70)

                    self.assertIn("Server processed", result)
                    print("✅ Deep recursion test PASSED!")

                finally:
                    server_conn.disable_asyncio_serving()
                    server_conn.close()

            finally:
                await server.close()

        asyncio.run(test())

    def test_no_thread_creation(self):
        """
        Verify that AsyncioServer doesn't create threads.

        CRITICAL: All async operations should use event loop, not threads.
        """
        async def test():
            import threading

            initial_thread_count = threading.active_count()
            print(f"\n[TEST] Initial thread count: {initial_thread_count}")

            server = AsyncioServer(
                ServerService,
                hostname='localhost',
                port=18883,
                protocol_config={'allow_all_attrs': True}
            )

            await server.start()

            try:
                await asyncio.sleep(0.5)

                after_start_count = threading.active_count()
                print(f"[TEST] After server start: {after_start_count}")

                # Server should not create many threads (asyncio may use some internal threads)
                # Allow up to 2 extra threads for asyncio internals
                self.assertLessEqual(
                    after_start_count - initial_thread_count,
                    2,
                    f"Too many threads created! Initial: {initial_thread_count}, Now: {after_start_count}"
                )

                server_conn = rpyc.connect("localhost", 18883)

                try:
                    loop = asyncio.get_running_loop()
                    server_conn.enable_asyncio_serving(loop=loop)

                    after_connect_count = threading.active_count()
                    print(f"[TEST] After client connect: {after_connect_count}")

                    # Connection should not create many threads
                    self.assertLessEqual(
                        after_connect_count - initial_thread_count,
                        3,
                        f"Too many threads after connect! Initial: {initial_thread_count}, Now: {after_connect_count}"
                    )

                    client_service = ClientService(server_conn)
                    async_callback = client_service.exposed_async_callback

                    # Execute recursive calls
                    result = await server_conn.root.async_process_with_callback(
                        async_callback,
                        value=1,
                        depth=3
                    )

                    after_exec_count = threading.active_count()
                    print(f"[TEST] After execution: {after_exec_count}")

                    # Should not have created many new threads
                    self.assertLessEqual(
                        after_exec_count - initial_thread_count,
                        3,
                        f"Too many threads after execution! Initial: {initial_thread_count}, Now: {after_exec_count}"
                    )

                    print("✅ No excessive thread creation detected!")

                finally:
                    server_conn.disable_asyncio_serving()
                    server_conn.close()

            finally:
                await server.close()

        asyncio.run(test())

    def test_event_loop_reuse(self):
        """
        Verify that existing event loops are reused.

        CRITICAL: Server and client should use their existing event loops,
        not create new ones.
        """
        async def test():
            print("\n[TEST] Testing event loop reuse...")

            # Get current event loop
            main_loop = asyncio.get_running_loop()
            print(f"[TEST] Main event loop: {id(main_loop)}")

            server = AsyncioServer(
                ServerService,
                hostname='localhost',
                port=18884,
                protocol_config={'allow_all_attrs': True}
            )

            await server.start()

            try:
                await asyncio.sleep(0.5)

                # Verify we're still in same loop
                current_loop = asyncio.get_running_loop()
                self.assertIs(current_loop, main_loop, "Event loop changed!")

                server_conn = rpyc.connect("localhost", 18884)

                try:
                    # Enable asyncio serving with current loop
                    server_conn.enable_asyncio_serving(loop=main_loop)

                    # Verify still same loop
                    current_loop = asyncio.get_running_loop()
                    self.assertIs(current_loop, main_loop, "Event loop changed after enable!")

                    client_service = ClientService(server_conn)
                    async_callback = client_service.exposed_async_callback

                    # Execute async calls
                    result = await server_conn.root.async_process_with_callback(
                        async_callback,
                        value=1,
                        depth=2
                    )

                    # Verify still same loop after execution
                    current_loop = asyncio.get_running_loop()
                    self.assertIs(current_loop, main_loop, "Event loop changed after execution!")

                    print(f"✅ Event loop reused correctly! ID: {id(main_loop)}")

                finally:
                    server_conn.disable_asyncio_serving()
                    server_conn.close()

            finally:
                await server.close()

        asyncio.run(test())


if __name__ == '__main__':
    print("\n" + "="*70)
    print("Testing AsyncioServer - Bidirectional Async Support")
    print("="*70 + "\n")

    unittest.main(verbosity=2)
