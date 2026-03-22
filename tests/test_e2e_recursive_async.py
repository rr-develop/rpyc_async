"""
E2E Test: Recursive Async Calls

Tests recursive async method calls up to depth 10.

Scenario:
1. Client calls server async method with depth=10
2. Server async method calls itself recursively
3. Each level awaits the next level
4. Results propagate back up the call stack

This tests deep async call chains.
"""
import unittest
import asyncio
import time
import rpyc
from rpyc.utils.async_server import AsyncioServer
from threading import Thread
from tests.support import get_free_port


class RecursiveService(rpyc.Service):
    """Server service with recursive async methods."""

    async def exposed_async_countdown(self, n):
        """
        Recursive async countdown.

        Args:
            n: Count from n down to 0

        Returns:
            List of countdown values
        """
        await asyncio.sleep(0.001)  # Small delay per level

        if n <= 0:
            return [0]

        # Recursive call
        rest = await self.exposed_async_countdown(n - 1)
        return [n] + rest

    async def exposed_async_fibonacci(self, n):
        """
        Async Fibonacci (recursive).

        Args:
            n: Fibonacci number to calculate

        Returns:
            Fibonacci(n)
        """
        await asyncio.sleep(0.001)

        if n <= 1:
            return n

        # Two recursive async calls
        a = await self.exposed_async_fibonacci(n - 1)
        b = await self.exposed_async_fibonacci(n - 2)

        return a + b

    async def exposed_async_factorial(self, n):
        """
        Async factorial (recursive).

        Args:
            n: Number to calculate factorial of

        Returns:
            n!
        """
        await asyncio.sleep(0.001)

        if n <= 1:
            return 1

        rest = await self.exposed_async_factorial(n - 1)
        return n * rest


class TestE2ERecursiveAsync(unittest.TestCase):
    """Test E2E recursive async calls."""

    def setUp(self):
        """Start AsyncioServer in background event loop for this test."""
        # Get free port dynamically to avoid conflicts
        self.port = get_free_port()

        # Create event loop for server
        self.server_loop = asyncio.new_event_loop()

        async def run_server():
            self.server = AsyncioServer(
                RecursiveService,
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

    def test_recursive_countdown_depth_10(self):
        """Test recursive async countdown to depth 10."""
        async def test():
            conn = rpyc.connect("localhost", self.port)

            try:
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)
                result = await conn.root.async_countdown(10)

                # Should return [10, 9, 8, ..., 1, 0]
                # Note: result is a netref, convert to local list
                expected = list(range(10, -1, -1))
                self.assertEqual(list(result), expected)
            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())

    def test_recursive_fibonacci(self):
        """Test recursive async Fibonacci."""
        async def test():
            conn = rpyc.connect("localhost", self.port)

            try:
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)
                # Fibonacci(10) = 55
                result = await conn.root.async_fibonacci(10)
                self.assertEqual(result, 55)

                # Fibonacci(5) = 5
                result = await conn.root.async_fibonacci(5)
                self.assertEqual(result, 5)
            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())

    def test_recursive_factorial(self):
        """Test recursive async factorial."""
        async def test():
            conn = rpyc.connect("localhost", self.port)

            try:
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)
                # 5! = 120
                result = await conn.root.async_factorial(5)
                self.assertEqual(result, 120)

                # 10! = 3628800
                result = await conn.root.async_factorial(10)
                self.assertEqual(result, 3628800)
            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())

    def test_deep_recursion_depth_20(self):
        """Test deep recursion (depth 20)."""
        async def test():
            conn = rpyc.connect("localhost", self.port)

            try:
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)
                result = await conn.root.async_countdown(20)

                expected = list(range(20, -1, -1))
                self.assertEqual(list(result), expected)
            finally:
                conn.disable_asyncio_serving()
                conn.close()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
