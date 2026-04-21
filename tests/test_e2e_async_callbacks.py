"""
E2E Test: Async Callbacks

Tests async callbacks from server back to client using REAL multiprocessing.

Scenario:
1. Server process exposes async method that accepts callback
2. Client process exposes async callback method (via bidirectional connection)
3. Client calls server method, passing callback as argument
4. Server calls client's async callback ACROSS PROCESS BOUNDARY
5. Client executes async callback in its own process and returns result
6. Server receives result and returns to original client call

This tests bidirectional async RPC across true process boundaries.

ARCHITECTURE:
=============
Process 1 (Server):                Process 2 (Client/Test):
├─ AsyncioServer                   ├─ unittest.TestCase
├─ event loop (main thread)        ├─ event loop (main thread)
├─ CallbackService                 ├─ rpyc.connect() (bidirectional!)
├─ exposed_process_with_callback   ├─ async def my_callback(...)
└─ await callback(value) ─────────→└─ executes in client process

NO THREADS are created by tests! Each process uses its own event loop in main thread.
"""
import unittest
import asyncio
import time
import rpyc
from rpyc.utils.async_server import AsyncioServer
from multiprocessing import Process, Queue
from tests.support import get_free_port


def run_callback_server(port, ready_queue):
    """
    Server process entry point.

    Runs in SEPARATE PROCESS. Creates its own event loop in MAIN THREAD.

    Args:
        port: Port number to bind to
        ready_queue: Queue to signal server is ready
    """
    # Define service in server process
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

            # Call client's async callback ACROSS PROCESS BOUNDARY
            result = await callback(value * 2)

            return f"Server processed: {result}"

    async def server_main():
        """Server main coroutine."""
        server = AsyncioServer(
            CallbackService,
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


class TestE2EAsyncCallbacks(unittest.TestCase):
    """Test E2E async callbacks using real multiprocessing."""

    def setUp(self):
        """Start AsyncioServer in SEPARATE PROCESS for this test."""
        # Get free port dynamically to avoid conflicts
        self.port = get_free_port()

        # Create queue for server readiness signaling
        self.ready_queue = Queue()

        # Start server in separate process
        self.server_process = Process(
            target=run_callback_server,
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

    def test_async_callback_basic(self):
        """Test basic async callback from server to client across processes."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)
            try:
                async def my_callback(value):
                    await asyncio.sleep(0.01)
                    return f"Client got: {value}"

                result = await conn.root.process_with_callback(my_callback, 5)
                self.assertEqual(result, "Server processed: Client got: 10")
            finally:
                await conn.aclose()

        asyncio.run(test())

    def test_callback_exception(self):
        """Test exception in async callback across processes."""
        async def test():
            conn = await rpyc.async_connect("localhost", self.port)
            try:
                async def failing_callback(value):
                    await asyncio.sleep(0.01)
                    raise ValueError(f"Callback error: {value}")

                with self.assertRaises(ValueError) as ctx:
                    await conn.root.process_with_callback(failing_callback, 7)

                self.assertIn("Callback error: 14", str(ctx.exception))
            finally:
                await conn.aclose()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
