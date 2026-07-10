"""
Integration Test: AsyncioServer Client Reconnection

Tests that multiple sequential client connections work correctly with AsyncioServer.

CRITICAL BUG REPRODUCTION:
=========================
Bug report: Second sequential connection to AsyncioServer hangs/freezes.

Scenario:
1. Server runs in separate process with AsyncioServer
2. Client 1 connects, performs operation, disconnects
3. Client 2 connects to same server - THIS SHOULD NOT HANG!
4. Client 2 performs operation, disconnects
5. Multiple reconnections should work

ARCHITECTURE:
=============
Process 1 (Server):              Process 2 (Test/Clients):
├─ AsyncioServer                 ├─ unittest.TestCase
├─ event loop (main thread)      ├─ event loop (main thread)
├─ Service                       ├─ Client 1: connect → test → close
└─ Accepts multiple connections  ├─ Client 2: connect → test → close
                                 ├─ Client 3: connect → test → close
                                 └─ ... (multiple reconnections)

NO THREADS are created by tests! Each process uses its own event loop in main thread.
"""
import unittest
import asyncio
import time
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer
from multiprocessing import Process, Queue
from tests.support import get_free_port


def run_reconnection_test_server(port, ready_queue):
    """
    Server process entry point for reconnection testing.

    Runs in SEPARATE PROCESS. Creates its own event loop in MAIN THREAD.

    Args:
        port: Port number to bind to
        ready_queue: Queue to signal server is ready
    """
    # Define service in server process
    class TestService(rpyc.Service):
        """Simple service for reconnection testing."""

        def __init__(self):
            super().__init__()
            self.connection_count = 0

        def on_connect(self, conn):
            """Called when client connects."""
            self.connection_count += 1
            print(f"[SERVER] Client connected (total connections: {self.connection_count})")
            super().on_connect(conn)

        def on_disconnect(self, conn):
            """Called when client disconnects."""
            print(f"[SERVER] Client disconnected")
            super().on_disconnect(conn)

        def exposed_ping(self, message):
            """Simple ping method."""
            return f"pong: {message}"

        async def exposed_async_ping(self, message):
            """Async ping method."""
            await asyncio.sleep(0.01)
            return f"async pong: {message}"

    async def server_main():
        """Server main coroutine."""
        server = AsyncioServer(
            TestService,
            hostname='localhost',
            port=port,
            protocol_config={'allow_all_attrs': True}
        )
        await server.start()

        # Signal that server is ready
        ready_queue.put("ready")
        print(f"[SERVER] Ready on port {port}")

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


class TestAsyncioServerReconnection(unittest.TestCase):
    """Test client reconnection to AsyncioServer using real multiprocessing."""

    def setUp(self):
        """Start AsyncioServer in SEPARATE PROCESS for this test."""
        # Get free port dynamically to avoid conflicts
        self.port = get_free_port()

        # Create queue for server readiness signaling
        self.ready_queue = Queue()

        # Start server in separate process
        self.server_process = Process(
            target=run_reconnection_test_server,
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

    def test_single_connection_baseline(self):
        """Baseline test: single connection works."""
        conn = rpyc.connect("localhost", self.port)
        try:
            result = conn.root.ping("test")
            self.assertEqual(result, "pong: test")
        finally:
            conn.close()

    def test_two_sequential_connections_sync(self):
        """
        CRITICAL BUG TEST: Second connection should not hang!

        This reproduces the bug where second sequential connection hangs.
        """
        print("\n[TEST] First connection...")
        conn1 = rpyc.connect("localhost", self.port)
        try:
            result1 = conn1.root.ping("first")
            self.assertEqual(result1, "pong: first")
            print(f"[TEST] First connection OK: {result1}")
        finally:
            conn1.close()
            print("[TEST] First connection closed")

        # Small delay to ensure first connection is fully closed
        time.sleep(0.1)

        print("\n[TEST] Second connection... (THIS IS WHERE BUG OCCURS)")
        # THIS IS WHERE THE BUG MANIFESTS - second connection hangs!
        conn2 = rpyc.connect("localhost", self.port)
        try:
            result2 = conn2.root.ping("second")
            self.assertEqual(result2, "pong: second")
            print(f"[TEST] Second connection OK: {result2}")
        finally:
            conn2.close()
            print("[TEST] Second connection closed")

    def test_two_sequential_connections_async(self):
        """Test two sequential connections with async methods."""
        async def test():
            print("\n[TEST] First async connection...")
            conn1 = await rpyc.async_connect("localhost", self.port)
            try:
                result1 = await conn1.root.async_ping("first")
                self.assertEqual(result1, "async pong: first")
                print(f"[TEST] First async connection OK: {result1}")
            finally:
                await conn1.aclose()
                print("[TEST] First async connection closed")

            # Small delay
            await asyncio.sleep(0.1)

            print("\n[TEST] Second async connection...")
            conn2 = await rpyc.async_connect("localhost", self.port)
            try:
                result2 = await conn2.root.async_ping("second")
                self.assertEqual(result2, "async pong: second")
                print(f"[TEST] Second async connection OK: {result2}")
            finally:
                await conn2.aclose()
                print("[TEST] Second async connection closed")

        asyncio.run(test())

    def test_multiple_sequential_connections(self):
        """Test multiple (5) sequential connections."""
        for i in range(5):
            print(f"\n[TEST] Connection {i+1}/5...")
            conn = rpyc.connect("localhost", self.port)
            try:
                result = conn.root.ping(f"connection_{i+1}")
                self.assertEqual(result, f"pong: connection_{i+1}")
                print(f"[TEST] Connection {i+1} OK: {result}")
            finally:
                conn.close()
                print(f"[TEST] Connection {i+1} closed")

            # Small delay between connections
            time.sleep(0.05)

    def test_rapid_reconnections(self):
        """Test rapid reconnections without delays."""
        print("\n[TEST] Rapid reconnections test (10 connections)...")
        for i in range(10):
            conn = rpyc.connect("localhost", self.port)
            try:
                result = conn.root.ping(f"rapid_{i}")
                self.assertEqual(result, f"pong: rapid_{i}")
            finally:
                conn.close()
        print("[TEST] All 10 rapid reconnections completed successfully")

    def test_concurrent_connections(self):
        """Test that server can handle concurrent connections."""
        async def test():
            print("\n[TEST] Concurrent connections test...")

            # Create 3 concurrent connections via the async-native path.
            conn1, conn2, conn3 = await asyncio.gather(
                rpyc.async_connect("localhost", self.port),
                rpyc.async_connect("localhost", self.port),
                rpyc.async_connect("localhost", self.port),
            )

            try:
                # Make concurrent async calls
                results = await asyncio.gather(
                    conn1.root.async_ping("conn1"),
                    conn2.root.async_ping("conn2"),
                    conn3.root.async_ping("conn3"),
                )

                self.assertEqual(results[0], "async pong: conn1")
                self.assertEqual(results[1], "async pong: conn2")
                self.assertEqual(results[2], "async pong: conn3")
                print(f"[TEST] Concurrent connections OK: {results}")

            finally:
                await asyncio.gather(
                    conn1.aclose(), conn2.aclose(), conn3.aclose()
                )

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main(verbosity=2)
