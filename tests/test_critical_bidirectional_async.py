"""
CRITICAL TEST: Bidirectional Async Recursive Calls

This is the MOST IMPORTANT test - it verifies that:
1. Server async method can call client async callback ACROSS PROCESSES
2. Client async callback can call server async method (recursion) ACROSS PROCESSES
3. All uses existing event loops (NO new threads)
4. Bidirectional connection works correctly
5. TRUE multiprocessing is used (NOT threading emulation!)

Scenario:
Server.async_process(callback, depth) →
    calls client.async_callback(value) → [PROCESS BOUNDARY]
        calls server.async_process(callback, depth-1) → [PROCESS BOUNDARY]
            ... recursive until depth=0

ARCHITECTURE:
=============
Process 1 (Server):                    Process 2 (Client/Test):
├─ AsyncioServer                       ├─ unittest.TestCase
├─ event loop (main thread only!)     ├─ event loop (main thread only!)
├─ ServerService                       ├─ rpyc.connect() (bidirectional!)
├─ exposed_async_process_...          ├─ ClientService (for callbacks)
│  └─ await callback(value) ───────────→├─ exposed_async_callback
│                                       │  └─ await conn.root.async_process... (recursive!)
│  ←─────────────────────────────────────┘
└─ NO threads created!                 └─ NO threads created!

IMPORTANT - Port Allocation Best Practices:
==========================================
This test uses DYNAMIC PORT ALLOCATION to prevent conflicts.

✅ CORRECT PATTERN (used here):
    def setUp(self):
        self.server_port = get_free_port()  # Unique port per test
        self.server = AsyncioServer(..., port=self.server_port)

❌ WRONG - DO NOT USE:
    @classmethod
    def setUpClass(cls):
        cls.server_port = get_free_port()  # Shared port = race conditions!

    # Or worse:
    server = AsyncioServer(..., port=18870)  # Hardcoded = conflicts!

WHY:
- Each test needs its own isolated server instance
- Shared ports cause race conditions between sequential tests
- Hardcoded ports conflict with parallel tests and other processes
- setUp/tearDown provides proper test isolation

See tests/support.py::get_free_port() for detailed documentation.
"""
import unittest
import asyncio
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer
from multiprocessing import Process, Queue
import time
from tests.support import get_free_port


def run_bidirectional_server(port, ready_queue):
    """
    Server process entry point.

    Runs in SEPARATE PROCESS. Creates its own event loop in MAIN THREAD.

    Args:
        port: Port number to bind to
        ready_queue: Queue to signal server is ready
    """
    # Define service in server process
    class ServerService(rpyc.Service):
        """Server service with async method that calls client callback."""

        async def exposed_async_process_with_callback(self, callback, value, depth):
            """
            Async method that recursively calls client callback.

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

            # Call client's async callback ACROSS PROCESS BOUNDARY - this should work!
            print(f"[SERVER] Calling client callback with value={value * 2}")
            result = await callback(value * 2, depth - 1)

            return f"Server processed: {result}"

        async def exposed_simple_async(self, x):
            """Simple async method for testing."""
            await asyncio.sleep(0.01)
            return x * 2

    async def server_main():
        """Server main coroutine."""
        server = AsyncioServer(
            ServerService,
            hostname='localhost',
            port=port,
            protocol_config={
                'allow_all_attrs': True,
                'allow_public_attrs': True,
            }
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


class TestCriticalBidirectionalAsync(unittest.TestCase):
    """
    CRITICAL TEST: Bidirectional async with recursion using REAL multiprocessing.

    This test MUST PASS for the implementation to be valid.

    Each test gets its own isolated server to prevent race conditions.
    """

    def setUp(self):
        """Start AsyncioServer in SEPARATE PROCESS for this test."""
        # Get free port dynamically to avoid conflicts
        self.server_port = get_free_port()

        # Create queue for server readiness signaling
        self.ready_queue = Queue()

        # Start server in separate process
        self.server_process = Process(
            target=run_bidirectional_server,
            args=(self.server_port, self.ready_queue),
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

    def test_simple_async_call_first(self):
        """Test simple async call works (baseline) across processes."""
        async def test():
            # Connect to server
            server_conn = await rpyc.async_connect("localhost", self.server_port)

            try:

                # Simple async call
                result = await server_conn.root.simple_async(5)
                self.assertEqual(result, 10)
                print(f"✓ Simple async call works: {result}")
            finally:
                await server_conn.aclose()

        asyncio.run(test())

    def test_bidirectional_async_with_recursion_depth_3(self):
        """
        CRITICAL TEST: Bidirectional async callbacks with recursion across processes.

        This is THE most important test. If this doesn't work,
        the implementation is incomplete.
        """
        async def test():
            print("\n" + "="*60)
            print("CRITICAL TEST: Bidirectional async with recursion (depth=3)")
            print("="*60)

            # Connect to server
            server_conn = await rpyc.async_connect("localhost", self.server_port)

            try:
                print("✓ Server connection established (async_connect, asyncio serving on)")

                # Define async callback that will run in CLIENT process
                # This callback CALLS BACK to server (recursion!)
                async def async_callback(value, depth):
                    """
                    Client async callback that calls back to server.

                    This is the critical part - async callback calling async server method.
                    """
                    print(f"[CLIENT] async_callback(value={value}, depth={depth})")

                    # Simulate async work
                    await asyncio.sleep(0.01)

                    if depth <= 0:
                        return f"Client finished: {value}"

                    # Recursive call back to server ACROSS PROCESS BOUNDARY - CRITICAL!
                    print(f"[CLIENT] Calling server.async_process_with_callback recursively")
                    result = await server_conn.root.async_process_with_callback(
                        async_callback,
                        value + 10,
                        depth - 1
                    )

                    return f"Client processed: {result}"

                print("\n[TEST] Starting recursive async call chain...")
                print("[TEST] Server → Client → Server → Client → ... (depth=3)\n")

                # Call server method with client callback
                # Server will call client, client will call server, etc.
                result = await server_conn.root.async_process_with_callback(
                    async_callback,
                    value=1,
                    depth=3
                )

                print(f"\n[TEST] Final result: {result}")
                print("="*60)

                # Verify result structure
                self.assertIn("Server processed", result)
                self.assertIn("Client", result)

                print("✓ CRITICAL TEST PASSED!")

            finally:
                await server_conn.aclose()

        asyncio.run(test())

    def test_bidirectional_async_depth_5(self):
        """Test deeper recursion (depth=5) across processes."""
        async def test():
            print("\n" + "="*60)
            print("CRITICAL TEST: Bidirectional async with recursion (depth=5)")
            print("="*60)

            server_conn = await rpyc.async_connect("localhost", self.server_port)

            try:
                async def async_callback(value, depth):
                    """Client async callback."""
                    print(f"[CLIENT] async_callback(value={value}, depth={depth})")
                    await asyncio.sleep(0.01)

                    if depth <= 0:
                        return f"Client finished: {value}"

                    result = await server_conn.root.async_process_with_callback(
                        async_callback,
                        value + 10,
                        depth - 1
                    )
                    return f"Client processed: {result}"

                print("\n[TEST] Starting deep recursive call chain (depth=5)...\n")

                result = await server_conn.root.async_process_with_callback(
                    async_callback,
                    value=1,
                    depth=5
                )

                print(f"\n[TEST] Final result: {result}")
                print("="*60)

                self.assertIn("Server processed", result)
                print("✓ Deep recursion test PASSED!")

            finally:
                await server_conn.aclose()

        asyncio.run(test())

    def test_verify_no_thread_creation(self):
        """
        Verify that event loops are reused, not creating new threads.

        This test checks that we're using existing event loops in main threads only.
        """
        async def test():
            import threading

            initial_thread_count = threading.active_count()
            print(f"\n[TEST] Initial thread count: {initial_thread_count}")

            server_conn = await rpyc.async_connect("localhost", self.server_port)

            try:
                # Check thread count after connecting via async_connect
                after_enable_count = threading.active_count()
                print(f"[TEST] After async_connect: {after_enable_count}")

                # Should not create new threads (might be ±1 due to thread pool)
                self.assertLessEqual(
                    after_enable_count - initial_thread_count,
                    1,
                    "Too many threads created!"
                )

                async def async_callback(value, depth):
                    """Client async callback."""
                    await asyncio.sleep(0.01)

                    if depth <= 0:
                        return f"Client finished: {value}"

                    result = await server_conn.root.async_process_with_callback(
                        async_callback,
                        value + 10,
                        depth - 1
                    )
                    return f"Client processed: {result}"

                # Execute recursive calls
                result = await server_conn.root.async_process_with_callback(
                    async_callback,
                    value=1,
                    depth=3
                )

                # Check thread count after execution
                after_exec_count = threading.active_count()
                print(f"[TEST] After execution: {after_exec_count}")

                # Should not have created many new threads
                self.assertLessEqual(
                    after_exec_count - initial_thread_count,
                    2,
                    "Too many threads created during execution!"
                )

                print("✓ No excessive thread creation detected")

            finally:
                await server_conn.aclose()

        asyncio.run(test())


if __name__ == '__main__':
    # Run with verbose output
    unittest.main(verbosity=2)
