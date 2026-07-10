"""
E2E Test: Async Exposed Method

Tests end-to-end async method calls from client to server using REAL multiprocessing.

Scenario:
1. Server exposes async method `async_hello(name)` in SEPARATE PROCESS
2. Server runs with asyncio event loop in its own process
3. Client connects from MAIN PROCESS and enables asyncio serving
4. Client calls conn.root.async_hello("world")
5. Server executes async method and returns result
6. Client awaits AsyncResult and gets "Hello, world!"

Requirements:
- Server must run in separate process (NOT thread!)
- Server must expose async method
- Server must have asyncio event loop running
- Client must enable asyncio serving in main process
- AsyncResult must be awaitable
- Result must propagate correctly across process boundary

ARCHITECTURE:
=============
Process 1 (Server):         Process 2 (Client/Test):
├─ AsyncioServer            ├─ unittest.TestCase
├─ event loop (main)        ├─ event loop (main)
├─ AsyncService             ├─ rpyc.connect()
└─ exposed_async_*()        └─ await conn.root.async_*()

NO THREADS are created by tests! Each process uses its own event loop in main thread.
"""
import unittest
import asyncio
import time
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer
from multiprocessing import Process, Queue
from tests.support import get_free_port


def run_async_server(port, ready_queue):
    """
    Server process entry point.

    Runs in SEPARATE PROCESS. Creates its own event loop in MAIN THREAD.

    Args:
        port: Port number to bind to
        ready_queue: Queue to signal server is ready
    """
    # Define service in server process
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

    async def server_main():
        """Server main coroutine."""
        server = AsyncioServer(
            AsyncService,
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


class TestE2EAsyncMethod(unittest.TestCase):
    """Test E2E async method calls using real multiprocessing."""

    def setUp(self):
        """Start AsyncioServer in SEPARATE PROCESS for this test."""
        # Get free port dynamically to avoid conflicts
        self.port = get_free_port()

        # Create queue for server readiness signaling
        self.ready_queue = Queue()

        # Start server in separate process
        self.server_process = Process(
            target=run_async_server,
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

    def test_async_method_basic(self):
        """Test basic async method call across processes."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:

                # Call async method and await result
                result = await conn.root.async_hello("world")
                self.assertEqual(result, "Hello, world!")
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_async_method_with_args(self):
        """Test async method with multiple arguments across processes."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:

                result = await conn.root.async_add(5, 3)
                self.assertEqual(result, 8)
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_async_method_exception(self):
        """Test async method that raises exception across processes."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:

                with self.assertRaises(ValueError) as ctx:
                    await conn.root.async_error()

                self.assertIn("Intentional async error", str(ctx.exception))
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_mixed_sync_async(self):
        """Test calling both sync and async methods across processes.

        Sync remote methods still go through HANDLE_CALL, which from
        inside a running event loop would block. Use ``rpyc.async_()`` to
        get an async wrapper; this keeps the call event-driven.
        """
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

            try:
                # Sync remote method, called via async wrapper.
                sync_result = await rpyc.async_(conn.root.sync_hello)("sync")
                self.assertEqual(sync_result, "Sync hello, sync!")

                # Native async remote method.
                async_result = await conn.root.async_hello("async")
                self.assertEqual(async_result, "Hello, async!")
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_multiple_async_calls(self):
        """Test multiple concurrent async method calls across processes."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)

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
                await conn.aclose()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
