"""
E2E Test: Async Callbacks

Tests async callbacks from server back to client.

Scenario:
1. Client exposes async callback method
2. Client calls server method, passing callback as argument
3. Server calls client's async callback
4. Client executes async callback and returns result
5. Server receives result and returns to original client call

This tests bidirectional async RPC.
"""
import unittest
import asyncio
import time
import rpyc
from rpyc.utils.async_server import AsyncioServer
from threading import Thread
from tests.support import get_free_port


class CallbackService(rpyc.Service):
    """Server service that calls back to client."""

    async def exposed_process_with_callback(self, callback, value):
        """
        Async method that calls back to client.

        Args:
            callback: Client-provided async callback
            value: Value to process

        Returns:
            Result from callback
        """
        await asyncio.sleep(0.01)  # Simulate async work

        # Call client's async callback
        result = await callback(value * 2)

        return f"Server processed: {result}"



class TestE2EAsyncCallbacks(unittest.TestCase):
    """Test E2E async callbacks."""

    def setUp(self):
        """Start AsyncioServer in background event loop for this test."""
        # Get free port dynamically to avoid conflicts
        self.port = get_free_port()

        # Create event loop for server
        self.server_loop = asyncio.new_event_loop()

        async def run_server():
            self.server = AsyncioServer(
                CallbackService,
                hostname='localhost',
                port=self.port,
                protocol_config={'allow_all_attrs': True}
            )
            await self.server.start()

        # Start server in background thread with its own event loop
        def start_server():
            asyncio.set_event_loop(self.server_loop)
            self.server_loop.run_until_complete(run_server())
            self.server_loop.run_forever()

        self.server_thread = Thread(target=start_server, daemon=True)
        self.server_thread.start()
        time.sleep(0.5)

    def tearDown(self):
        """Stop RPyC server after this test."""
        async def stop_server():
            await self.server.close()

        # Schedule close and wait
        future = asyncio.run_coroutine_threadsafe(stop_server(), self.server_loop)
        try:
            future.result(timeout=2.0)
        except:
            pass

        # Stop loop
        self.server_loop.call_soon_threadsafe(self.server_loop.stop)
        time.sleep(0.1)

    def test_async_callback_basic(self):
        """Test basic async callback from server to client."""
        async def test():
            conn = rpyc.connect("localhost", self.port)

            # Enable asyncio serving on client to handle callbacks
            loop = asyncio.get_running_loop()
            conn.enable_asyncio_serving(loop=loop)

            try:
                # Define client-side async callback
                async def my_callback(value):
                    """Client async callback."""
                    await asyncio.sleep(0.01)
                    return f"Client got: {value}"

                # Call server method with async callback
                result = await conn.root.process_with_callback(my_callback, 5)

                self.assertEqual(result, "Server processed: Client got: 10")
            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())

    def test_callback_exception(self):
        """Test exception in async callback."""
        async def test():
            conn = rpyc.connect("localhost", self.port)
            loop = asyncio.get_running_loop()
            conn.enable_asyncio_serving(loop=loop)

            try:
                async def failing_callback(value):
                    await asyncio.sleep(0.01)
                    raise ValueError(f"Callback error: {value}")

                # Server calls callback, callback raises exception
                with self.assertRaises(ValueError) as ctx:
                    await conn.root.process_with_callback(failing_callback, 7)

                self.assertIn("Callback error: 14", str(ctx.exception))
            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
