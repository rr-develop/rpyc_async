"""
E2E Test: Async Exposed Method

Tests end-to-end async method calls from client to server.

Scenario:
1. Server exposes async method `async_hello(name)`
2. Server runs with asyncio event loop
3. Client connects and enables asyncio serving
4. Client calls conn.root.async_hello("world")
5. Server executes async method and returns result
6. Client awaits AsyncResult and gets "Hello, world!"

Requirements:
- Server must expose async method
- Server must have asyncio event loop running
- Client must enable asyncio serving
- AsyncResult must be awaitable
- Result must propagate correctly
"""
import unittest
import asyncio
import time
import rpyc
from rpyc.utils.async_server import AsyncioServer
from threading import Thread
from tests.support import get_free_port


class AsyncService(rpyc.Service):
    """Service with async methods for E2E testing."""

    async def exposed_async_hello(self, name):
        """Async method that returns greeting after delay."""
        await asyncio.sleep(0.01)  # Simulate async work
        return f"Hello, {name}!"

    def exposed_sync_hello(self, name):
        """Sync method for comparison."""
        return f"Sync hello, {name}!"

    async def exposed_async_add(self, a, b):
        """Async method that adds two numbers."""
        await asyncio.sleep(0.01)
        return a + b

    async def exposed_async_error(self):
        """Async method that raises exception."""
        await asyncio.sleep(0.01)
        raise ValueError("Intentional async error")


class TestE2EAsyncMethod(unittest.TestCase):
    """Test E2E async method calls."""

    def setUp(self):
        """Start AsyncioServer in background event loop for this test."""
        # Get free port dynamically to avoid conflicts
        self.port = get_free_port()

        # Create event loop for server
        self.server_loop = asyncio.new_event_loop()

        async def run_server():
            self.server = AsyncioServer(
                AsyncService,
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

    def test_async_method_basic(self):
        """Test basic async method call."""
        async def test():
            conn = rpyc.connect("localhost", self.port)

            try:
                # Enable asyncio serving to handle async calls
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)

                # Call async method and await result
                result = await conn.root.async_hello("world")
                self.assertEqual(result, "Hello, world!")
            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())

    def test_async_method_with_args(self):
        """Test async method with multiple arguments."""
        async def test():
            conn = rpyc.connect("localhost", self.port)

            try:
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)

                result = await conn.root.async_add(5, 3)
                self.assertEqual(result, 8)
            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())

    def test_async_method_exception(self):
        """Test async method that raises exception."""
        async def test():
            conn = rpyc.connect("localhost", self.port)

            try:
                # Enable asyncio serving to handle async calls
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)

                with self.assertRaises(ValueError) as ctx:
                    await conn.root.async_error()

                self.assertIn("Intentional async error", str(ctx.exception))
            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())

    def test_mixed_sync_async(self):
        """Test calling both sync and async methods."""
        async def test():
            conn = rpyc.connect("localhost", self.port)

            try:
                # Enable asyncio serving to handle async calls
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)

                # Call sync method (should work normally)
                sync_result = conn.root.sync_hello("sync")
                self.assertEqual(sync_result, "Sync hello, sync!")

                # Call async method
                async_result = await conn.root.async_hello("async")
                self.assertEqual(async_result, "Hello, async!")
            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())

    def test_multiple_async_calls(self):
        """Test multiple concurrent async method calls."""
        async def test():
            conn = rpyc.connect("localhost", self.port)

            try:
                # Enable asyncio serving to handle async calls
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)

                # Launch multiple async calls concurrently
                tasks = [
                    conn.root.async_add(i, i)
                    for i in range(5)
                ]

                # Await all results
                results = await asyncio.gather(*tasks)

                # Verify results
                expected = [0, 2, 4, 6, 8]
                self.assertEqual(results, expected)
            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
