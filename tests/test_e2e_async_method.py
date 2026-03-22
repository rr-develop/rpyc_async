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
from rpyc.utils.server import ThreadedServer
from threading import Thread


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

    @classmethod
    def setUpClass(cls):
        """Start RPyC server in background thread."""
        # Use standard ThreadedServer - async dispatch will create temp event loops
        cls.server = ThreadedServer(
            AsyncService,
            port=18861,
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

    def test_async_method_basic(self):
        """Test basic async method call."""
        async def test():
            conn = rpyc.connect("localhost", 18861)
            conn.enable_asyncio_serving()

            try:
                # Call async method
                result = await conn.root.async_hello("world")
                self.assertEqual(result, "Hello, world!")
            finally:
                conn.close()

        asyncio.run(test())

    def test_async_method_with_args(self):
        """Test async method with multiple arguments."""
        async def test():
            conn = rpyc.connect("localhost", 18861)
            conn.enable_asyncio_serving()

            try:
                result = await conn.root.async_add(5, 3)
                self.assertEqual(result, 8)
            finally:
                conn.close()

        asyncio.run(test())

    def test_async_method_exception(self):
        """Test async method that raises exception."""
        async def test():
            conn = rpyc.connect("localhost", 18861)
            conn.enable_asyncio_serving()

            try:
                with self.assertRaises(ValueError) as ctx:
                    await conn.root.async_error()

                self.assertIn("Intentional async error", str(ctx.exception))
            finally:
                conn.close()

        asyncio.run(test())

    def test_mixed_sync_async(self):
        """Test calling both sync and async methods."""
        async def test():
            conn = rpyc.connect("localhost", 18861)
            conn.enable_asyncio_serving()

            try:
                # Call sync method (should work normally)
                sync_result = conn.root.sync_hello("sync")
                self.assertEqual(sync_result, "Sync hello, sync!")

                # Call async method
                async_result = await conn.root.async_hello("async")
                self.assertEqual(async_result, "Hello, async!")
            finally:
                conn.close()

        asyncio.run(test())

    def test_multiple_async_calls(self):
        """Test multiple concurrent async method calls."""
        async def test():
            conn = rpyc.connect("localhost", 18861)
            conn.enable_asyncio_serving()

            try:
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
                conn.close()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
