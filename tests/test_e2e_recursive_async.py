"""
E2E Test: Recursive Async Calls

Tests recursive async method calls up to depth 20 using REAL multiprocessing.

Scenario:
1. Client calls server async method with depth=10 from SEPARATE PROCESS
2. Server async method calls itself recursively within server process
3. Each level awaits the next level
4. Results propagate back up the call stack across process boundary

This tests deep async call chains with true cross-process communication.

ARCHITECTURE:
=============
Process 1 (Server):         Process 2 (Client/Test):
├─ AsyncioServer            ├─ unittest.TestCase
├─ event loop (main)        ├─ event loop (main)
├─ RecursiveService         ├─ rpyc.connect()
└─ exposed_async_*()        └─ await conn.root.async_*()
    ├─ calls self
    └─ recursive depth

NO THREADS are created by tests! Each process uses its own event loop in main thread.
"""
import unittest
import asyncio
import time
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer
from multiprocessing import Process, Queue
from tests.support import get_free_port


def run_recursive_server(port, ready_queue):
    """
    Server process entry point.

    Runs in SEPARATE PROCESS. Creates its own event loop in MAIN THREAD.

    Args:
        port: Port number to bind to
        ready_queue: Queue to signal server is ready
    """
    # Define service in server process
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

    async def server_main():
        """Server main coroutine."""
        server = AsyncioServer(
            RecursiveService,
            hostname='localhost',
            port=port,
            protocol_config={'allow_all_attrs': True}
        )
        await server.start()

        # Signal that server is ready
        ready_queue.put("ready")

        try:
            # Run forever (until process is terminated)
            await asyncio.Event().wait()
        finally:
            await server.close()

    # Run server in event loop (main thread of this process)
    try:
        asyncio.run(server_main())
    except KeyboardInterrupt:
        pass


class TestE2ERecursiveAsync(unittest.TestCase):
    """Test E2E recursive async calls using real multiprocessing."""

    def setUp(self):
        """Start AsyncioServer in SEPARATE PROCESS for this test."""
        # Get free port dynamically to avoid conflicts
        self.port = get_free_port()

        # Create queue for server readiness signaling
        self.ready_queue = Queue()

        # Start server in separate process
        self.server_process = Process(
            target=run_recursive_server,
            args=(self.port, self.ready_queue),
            daemon=True
        )
        self.server_process.start()

        # Wait for server to be ready (with timeout)
        try:
            ready_signal = self.ready_queue.get(timeout=5.0)
            if ready_signal != "ready":
                raise RuntimeError(f"Unexpected ready signal: {ready_signal}")
        except:
            self.server_process.terminate()
            self.server_process.join(timeout=1.0)
            raise RuntimeError("Server failed to start within 5 seconds")

        # Give server a moment to fully initialize
        time.sleep(0.2)

    def tearDown(self):
        """Stop server process after this test."""
        if self.server_process and self.server_process.is_alive():
            self.server_process.terminate()
            self.server_process.join(timeout=2.0)

            # Force kill if still alive
            if self.server_process.is_alive():
                self.server_process.kill()
                self.server_process.join(timeout=1.0)

    def test_recursive_countdown_depth_10(self):
        """Test recursive async countdown to depth 10 across processes."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:
                result = await conn.root.async_countdown(10)

                # Should return [10, 9, 8, ..., 1, 0]
                # Note: result is a netref, convert to local list
                expected = list(range(10, -1, -1))
                self.assertEqual(list(result), expected)
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_recursive_fibonacci(self):
        """Test recursive async Fibonacci across processes."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:
                # Fibonacci(10) = 55
                result = await conn.root.async_fibonacci(10)
                self.assertEqual(result, 55)

                # Fibonacci(5) = 5
                result = await conn.root.async_fibonacci(5)
                self.assertEqual(result, 5)
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_recursive_factorial(self):
        """Test recursive async factorial across processes."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:
                # 5! = 120
                result = await conn.root.async_factorial(5)
                self.assertEqual(result, 120)

                # 10! = 3628800
                result = await conn.root.async_factorial(10)
                self.assertEqual(result, 3628800)
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_deep_recursion_depth_20(self):
        """Test deep recursion (depth 20) across processes."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:
                result = await conn.root.async_countdown(20)

                expected = list(range(20, -1, -1))
                self.assertEqual(list(result), expected)
            finally:
                await conn.aclose()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
