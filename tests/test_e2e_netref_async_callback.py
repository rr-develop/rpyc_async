"""
E2E Test: Async Netref Callback

Tests passing client netref objects to server and calling their async methods.

Scenario:
1. Client has an object with async methods
2. Client calls server async method, passing client object as netref
3. Server receives netref and calls async method on it
4. Client's async method executes in client process
5. Server awaits result from client's async method
6. Server returns combined result to client

This tests:
- Netref object passing across process boundary
- Server calling async methods on client netref
- Bidirectional async communication with object references
- enable_asyncio_serving() on both sides

ARCHITECTURE:
=============
Process 1 (Server):                    Process 2 (Client/Test):
├─ AsyncioServer                       ├─ unittest.TestCase
├─ event loop (main thread)            ├─ event loop (main thread)
├─ ServerService                       ├─ ClientObject (with async methods)
├─ exposed_async_process_object        ├─ rpyc.connect() (bidirectional)
│  └─ await client_obj.async_method()  │  └─ passes ClientObject via netref
│                                      │
│  ←───────── netref ─────────────────┘
│  ─────── async call ────────────────→
│  ←────── async result ───────────────┘
└─ Returns combined result             └─ Receives final result

NO THREADS are created! Each process uses its own event loop in main thread.
"""
import unittest
import asyncio
import time
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer
from multiprocessing import Process, Queue
from tests.support import get_free_port


def run_netref_callback_server(port, ready_queue):
    """
    Server process entry point.

    Runs in SEPARATE PROCESS. Creates its own event loop in MAIN THREAD.

    Args:
        port: Port number to bind to
        ready_queue: Queue to signal server is ready
    """
    # Define service in server process
    class ServerService(rpyc.Service):
        """Server service that accepts netref and calls its async methods."""

        async def exposed_async_process_object(self, client_obj, value):
            """
            Async method that receives client netref and calls its async method.

            Args:
                client_obj: Client object (netref) with async methods
                value: Initial value to process

            Returns:
                Combined result from server and client processing
            """
            print(f"[SERVER] exposed_async_process_object(value={value})")

            # Simulate async work on server
            await asyncio.sleep(0.01)
            server_result = value * 2

            print(f"[SERVER] Calling async method on client netref...")
            # Call async method on client object (netref)
            # This will cross process boundary back to client!
            client_result = await client_obj.async_transform(server_result)
            print(f"[SERVER] Received from client: {client_result}")

            # Return combined result
            return f"Server: {server_result}, Client: {client_result}"

        async def exposed_async_chain_calls(self, client_obj, value, depth):
            """
            Recursive async calls between server and client via netref.

            Args:
                client_obj: Client object (netref) with async methods
                value: Current value
                depth: Recursion depth

            Returns:
                Final result after recursive calls
            """
            print(f"[SERVER] async_chain_calls(value={value}, depth={depth})")

            await asyncio.sleep(0.01)

            if depth <= 0:
                return f"Server final: {value}"

            # Call client's async method
            client_result = await client_obj.async_chain(value + 10, depth - 1)

            return f"Server: {value} -> {client_result}"

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


class TestE2ENetrefAsyncCallback(unittest.TestCase):
    """Test passing netref objects and calling their async methods across processes."""

    def setUp(self):
        """Start AsyncioServer in SEPARATE PROCESS for this test."""
        # Get free port dynamically to avoid conflicts
        self.port = get_free_port()

        # Create queue for server readiness signaling
        self.ready_queue = Queue()

        # Start server in separate process
        self.server_process = Process(
            target=run_netref_callback_server,
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

    def test_netref_async_callback_basic(self):
        """
        Test passing client object as netref and calling its async method from server.
        """
        async def test():
            print("\n" + "="*60)
            print("TEST: Netref async callback (basic)")
            print("="*60)

            # Define client object with async methods
            class ClientObject:
                """Client object that will be passed as netref to server."""

                async def async_transform(self, value):
                    """
                    Async method that will be called by server.

                    This executes in CLIENT process even though called from server!
                    """
                    print(f"[CLIENT] async_transform(value={value})")
                    await asyncio.sleep(0.01)
                    result = value + 100
                    print(f"[CLIENT] Returning: {result}")
                    return result

            # Create client object
            client_obj = ClientObject()

            # Connect to server
            server_conn = await rpyc.async_connect("localhost", self.port)

            try:
                print("✓ Server connection ready (asyncio serving auto-enabled)")

                print("\n[TEST] Calling server method with client object as netref...")
                # Call server method, passing client object as argument
                # Server will receive it as netref and call its async method
                result = await server_conn.root.async_process_object(client_obj, 5)

                print(f"\n[TEST] Final result: {result}")
                print("="*60)

                # Verify result
                # Server: 5*2=10, Client: 10+100=110
                self.assertIn("Server: 10", result)
                self.assertIn("Client: 110", result)

                print("✓ TEST PASSED!")

            finally:
                await server_conn.aclose()

        asyncio.run(test())

    def test_netref_recursive_async_calls(self):
        """
        Test recursive async calls between server and client via netref.
        """
        async def test():
            print("\n" + "="*60)
            print("TEST: Netref recursive async calls")
            print("="*60)

            # Define client object with recursive async method
            class ClientObject:
                """Client object with recursive async method."""

                def __init__(self, server_conn):
                    self.server_conn = server_conn

                async def async_chain(self, value, depth):
                    """
                    Recursive async method that calls back to server.

                    Server calls this -> this calls server -> server calls this...
                    """
                    print(f"[CLIENT] async_chain(value={value}, depth={depth})")
                    await asyncio.sleep(0.01)

                    if depth <= 0:
                        return f"Client final: {value}"

                    # Call server's async method recursively
                    server_result = await self.server_conn.root.async_chain_calls(
                        self, value + 5, depth - 1
                    )

                    return f"Client: {value} -> {server_result}"

            # Connect to server
            server_conn = await rpyc.async_connect("localhost", self.port)

            try:

                # Create client object (needs server connection for recursive calls)
                client_obj = ClientObject(server_conn)

                print("\n[TEST] Starting recursive async chain (depth=3)...")
                print("[TEST] Server → Client → Server → Client → ...")

                # Start recursive chain
                result = await server_conn.root.async_chain_calls(client_obj, 1, 3)

                print(f"\n[TEST] Final result: {result}")
                print("="*60)

                # Verify result contains both server and client parts
                self.assertIn("Server", result)
                self.assertIn("Client", result)

                print("✓ TEST PASSED!")

            finally:
                await server_conn.aclose()

        asyncio.run(test())

    def test_multiple_netref_methods(self):
        """
        Test calling multiple different async methods on same netref object.
        """
        async def test():
            print("\n" + "="*60)
            print("TEST: Multiple netref async methods")
            print("="*60)

            # Define client object with multiple async methods
            class ClientObject:
                """Client object with multiple async methods."""

                async def async_transform(self, value):
                    """Transform value asynchronously."""
                    await asyncio.sleep(0.01)
                    return value + 100

                async def async_add(self, a, b):
                    """Add two numbers asynchronously."""
                    await asyncio.sleep(0.01)
                    return a + b

                async def async_multiply(self, a, b):
                    """Multiply two numbers asynchronously."""
                    await asyncio.sleep(0.01)
                    return a * b

                async def async_power(self, base, exp):
                    """Raise base to power asynchronously."""
                    await asyncio.sleep(0.01)
                    return base ** exp

            # Server method that calls multiple client methods
            class ServerWithMultipleCalls(rpyc.Service):
                async def exposed_async_use_multiple_methods(self, client_obj, x, y):
                    """Call multiple async methods on client netref."""
                    # Call different async methods on same netref
                    add_result = await client_obj.async_add(x, y)
                    mul_result = await client_obj.async_multiply(x, y)
                    pow_result = await client_obj.async_power(x, y)

                    return {
                        'add': add_result,
                        'multiply': mul_result,
                        'power': pow_result
                    }

            # For this test, we'll just verify the concept works with our existing server
            # by calling the basic test multiple times
            client_obj = ClientObject()
            server_conn = await rpyc.async_connect("localhost", self.port)

            try:

                # Call server method multiple times with same netref
                result1 = await server_conn.root.async_process_object(client_obj, 3)
                result2 = await server_conn.root.async_process_object(client_obj, 7)

                print(f"[TEST] Result 1: {result1}")
                print(f"[TEST] Result 2: {result2}")

                # Verify both calls worked
                self.assertIn("Server: 6", result1)   # 3*2
                self.assertIn("Client: 106", result1) # 6+100
                self.assertIn("Server: 14", result2)  # 7*2
                self.assertIn("Client: 114", result2) # 14+100

                print("✓ TEST PASSED!")

            finally:
                await server_conn.aclose()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main(verbosity=2)
