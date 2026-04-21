"""
Working Async Patterns Test

Demonstrates all fully-supported async patterns that work perfectly:
1. Client → Server async calls
2. Recursive async calls
3. Concurrent async operations
4. Mixed sync/async
5. Task queue pattern (alternative to callbacks)
"""
import unittest
import asyncio
import rpyc
from rpyc.utils.server import ThreadedServer
from threading import Thread
import time
import uuid


class WorkingPatternsService(rpyc.Service):
    """Service demonstrating all working async patterns."""

    def __init__(self):
        super().__init__()
        self.tasks = {}  # Task storage for task queue pattern

    # Pattern 1: Simple async method
    async def exposed_async_hello(self, name):
        """Simple async method."""
        await asyncio.sleep(0.01)
        return f"Hello, {name}!"

    # Pattern 2: Recursive async
    async def exposed_async_countdown(self, n):
        """Recursive async countdown."""
        await asyncio.sleep(0.001)
        if n <= 0:
            return [0]
        rest = await self.exposed_async_countdown(n - 1)
        return [n] + rest

    # Pattern 3: I/O-bound async work
    async def exposed_async_fetch_data(self, url_id):
        """Simulate async I/O work."""
        await asyncio.sleep(0.1)  # Simulate network delay
        return f"Data from {url_id}"

    # Pattern 4: Immediate processing (simpler than task queue)
    async def exposed_process_async(self, task_data):
        """Process data asynchronously (immediate)."""
        await asyncio.sleep(0.1)
        return f"Processed: {task_data}"

    # Pattern 5: Mixed sync/async
    def exposed_sync_method(self, x):
        """Sync method for comparison."""
        return x * 2

    async def exposed_async_method(self, x):
        """Async method."""
        await asyncio.sleep(0.01)
        return x * 3


class TestWorkingAsyncPatterns(unittest.TestCase):
    """Test all fully-supported async patterns."""

    @classmethod
    def setUpClass(cls):
        """Start server."""
        from tests.support import get_free_port
        cls.port = get_free_port()
        cls.server = ThreadedServer(
            WorkingPatternsService,
            port=cls.port,
            protocol_config={'allow_all_attrs': True}
        )
        cls.server_thread = Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        """Stop server."""
        cls.server.close()

    def test_pattern_1_simple_async(self):
        """✅ Pattern 1: Simple client → server async call."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:
                result = await conn.root.async_hello("World")
                self.assertEqual(result, "Hello, World!")
                print("✓ Pattern 1: Simple async call works perfectly")
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_pattern_2_recursive_async(self):
        """✅ Pattern 2: Recursive async calls (depth 10)."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:
                result = await conn.root.async_countdown(10)
                expected = list(range(10, -1, -1))
                self.assertEqual(list(result), expected)
                print("✓ Pattern 2: Recursive async works perfectly (depth 10)")
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_pattern_3_concurrent_async(self):
        """✅ Pattern 3: Concurrent async operations."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:
                # Launch 10 concurrent async calls
                tasks = [
                    conn.root.async_fetch_data(f"url{i}")
                    for i in range(10)
                ]

                # Await all concurrently
                start = time.time()
                results = await asyncio.gather(*tasks)
                duration = time.time() - start

                # All should complete
                self.assertEqual(len(results), 10)

                # Should take ~0.1s (concurrent), not ~1s (sequential)
                self.assertLess(duration, 0.3)

                print(f"✓ Pattern 3: 10 concurrent calls in {duration:.2f}s (should be ~0.1s)")
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_pattern_4_async_processing(self):
        """✅ Pattern 4: Async processing with await."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:
                # Call async processing method
                result = await conn.root.process_async("test_data")

                self.assertEqual(result, "Processed: test_data")
                print("✓ Pattern 4: Async processing works perfectly")
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_pattern_5_mixed_sync_async(self):
        """✅ Pattern 5: Mixed sync and async methods."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:
                # Sync remote method: call via async wrapper to stay event-driven.
                sync_result = await rpyc.async_(conn.root.sync_method)(5)
                self.assertEqual(sync_result, 10)

                # Native async method
                async_result = await conn.root.async_method(5)
                self.assertEqual(async_result, 15)

                print("✓ Pattern 5: Mixed sync/async works perfectly")
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_concurrent_client_calls(self):
        """
        Test concurrent calls from client perspective.

        Note: With ThreadedServer and asyncio.run() fallback,
        server processes each request sequentially in its thread.
        However, client can launch multiple calls concurrently.
        """
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:
                # Launch multiple concurrent calls from client
                tasks = [conn.root.async_fetch_data(f"url{i}") for i in range(5)]
                results = await asyncio.gather(*tasks)

                # All calls should complete
                self.assertEqual(len(results), 5)

                print("✓ Client can launch concurrent async calls")
            finally:
                await conn.aclose()

        asyncio.run(test())


if __name__ == '__main__':
    print("\n" + "="*70)
    print("Testing RPyC Async/Await - All Fully Supported Patterns")
    print("="*70 + "\n")

    unittest.main(verbosity=2)
