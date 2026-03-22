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
from rpyc.utils.server import ThreadedServer
from threading import Thread


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

    def exposed_sync_with_async_callback(self, callback, value):
        """
        Sync method that calls async callback.

        This tests sync method calling async callback.
        """
        # Note: In sync context, we need to run async callback synchronously
        # This requires asyncio.run() or similar
        import asyncio
        result = asyncio.run(callback(value + 10))
        return f"Sync server got: {result}"


class TestE2EAsyncCallbacks(unittest.TestCase):
    """Test E2E async callbacks."""

    @classmethod
    def setUpClass(cls):
        """Start RPyC server in background thread."""
        cls.server = ThreadedServer(
            CallbackService,
            port=18866,
            protocol_config={'allow_all_attrs': True}
        )

        cls.server_thread = Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()

        # Wait for server to start
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        """Stop RPyC server."""
        cls.server.close()

    def test_async_callback_basic(self):
        """Test basic async callback from server to client."""
        async def test():
            conn = rpyc.connect("localhost", 18866)

            # Enable asyncio serving on client to handle callbacks
            conn.enable_asyncio_serving()

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

    def test_sync_method_with_async_callback(self):
        """Test sync server method calling async callback."""
        async def test():
            conn = rpyc.connect("localhost", 18866)
            conn.enable_asyncio_serving()

            try:
                async def my_callback(value):
                    await asyncio.sleep(0.01)
                    return value * 3

                # Sync method calling async callback
                result = conn.root.sync_with_async_callback(my_callback, 5)

                self.assertEqual(result, "Sync server got: 45")
            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())

    def test_callback_exception(self):
        """Test exception in async callback."""
        async def test():
            conn = rpyc.connect("localhost", 18866)
            conn.enable_asyncio_serving()

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
