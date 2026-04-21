"""
E2E Test: Netref Deserialization in Async Callbacks

Tests that when a client object is passed to server as netref,
and server passes it back to client via async callback,
the client receives the ORIGINAL object (not a netref).

Scenario:
1. Client has an object and an async callback
2. Client calls server async method, passing both object and callback as netrefs
3. Server receives netref to client object and netref to callback
4. Server calls the callback, passing the client object netref back
5. Client's callback receives the argument
6. TEST: Client verifies the received argument is the ORIGINAL object, not a netref

This tests:
- Netref deserialization on the original side
- Objects should be unwrapped to originals when returning to their home process
- Async callbacks with netref arguments
- enable_asyncio_serving() on both sides

ARCHITECTURE:
=============
Process 1 (Server):                    Process 2 (Client/Test):
├─ AsyncioServer                       ├─ unittest.TestCase
├─ event loop (main thread)            ├─ event loop (main thread)
├─ ServerService                       ├─ ClientObject (original)
├─ exposed_async_call_with_callback    ├─ async callback function
│  └─ await callback(client_obj)       │  └─ receives original object!
│                                      │
│  ←────── netref(obj) ────────────────┘
│  ←────── netref(callback) ───────────┘
│  ─────── callback(obj_netref) ───────→
│  ←────── result ─────────────────────┘
└─ Returns result                      └─ Verifies obj is original

NO THREADS are created! Each process uses its own event loop in main thread.
"""
import unittest
import asyncio
import time
import rpyc
from rpyc.utils.async_server import AsyncioServer
from multiprocessing import Process, Queue
from tests.support import get_free_port


def run_netref_deserialization_server(port, ready_queue):
    """
    Server process entry point.

    Runs in SEPARATE PROCESS. Creates its own event loop in MAIN THREAD.

    Args:
        port: Port number to bind to
        ready_queue: Queue to signal server is ready
    """
    # Define service in server process
    class ServerService(rpyc.Service):
        """Server service that calls callback with netref argument."""

        async def exposed_async_call_with_callback(self, client_obj, callback):
            """
            Async method that receives client object and callback as netrefs,
            then calls the callback passing the client object.

            Args:
                client_obj: Client object (netref)
                callback: Client async callback (netref)

            Returns:
                Result from callback
            """
            print(f"[SERVER] exposed_async_call_with_callback()")
            print(f"[SERVER] client_obj type: {type(client_obj)}")
            print(f"[SERVER] callback type: {type(callback)}")

            # Simulate async work
            await asyncio.sleep(0.01)

            print(f"[SERVER] Calling callback with client_obj...")
            # Call the callback, passing the client object back
            # The client should receive the ORIGINAL object, not a netref!
            result = await callback(client_obj)
            print(f"[SERVER] Callback returned: {result}")

            return result

        async def exposed_async_multiple_callbacks(self, obj1, obj2, callback):
            """
            Test calling callback multiple times with different objects.

            Args:
                obj1: First client object (netref)
                obj2: Second client object (netref)
                callback: Client async callback (netref)

            Returns:
                List of results from callbacks
            """
            print(f"[SERVER] exposed_async_multiple_callbacks()")

            await asyncio.sleep(0.01)

            # Call callback with different objects
            result1 = await callback(obj1)
            result2 = await callback(obj2)

            return [result1, result2]

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


class TestE2ENetrefDeserialization(unittest.TestCase):
    """Test that netrefs are deserialized back to originals on their home process."""

    def setUp(self):
        """Start AsyncioServer in SEPARATE PROCESS for this test."""
        # Get free port dynamically to avoid conflicts
        self.port = get_free_port()

        # Create queue for server readiness signaling
        self.ready_queue = Queue()

        # Start server in separate process
        self.server_process = Process(
            target=run_netref_deserialization_server,
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

    def test_netref_deserialized_to_original(self):
        """
        Test that when server passes client object back via callback,
        client receives the ORIGINAL object, not a netref.
        """
        async def test():
            print("\n" + "="*60)
            print("TEST: Netref deserialization to original object")
            print("="*60)

            # Define client object
            class ClientObject:
                """Client object that will be passed as netref to server."""
                def __init__(self, value):
                    self.value = value
                    self.id = id(self)  # Store object id for verification

                def get_value(self):
                    return self.value

            # Create client object
            client_obj = ClientObject("test_value")
            print(f"[CLIENT] Created ClientObject with id={id(client_obj)}, value={client_obj.value}")

            # Track what we receive in callback
            received_obj = None
            received_type = None
            is_same_object = None

            # Define async callback
            async def my_callback(obj):
                """
                Async callback that receives object from server.

                CRITICAL TEST: obj should be the ORIGINAL client_obj,
                not a netref!
                """
                nonlocal received_obj, received_type, is_same_object

                print(f"\n[CALLBACK] Received object")
                print(f"[CALLBACK] Type: {type(obj)}")
                print(f"[CALLBACK] Type name: {type(obj).__name__}")
                print(f"[CALLBACK] id(obj): {id(obj)}")
                print(f"[CALLBACK] id(client_obj): {id(client_obj)}")
                print(f"[CALLBACK] obj is client_obj: {obj is client_obj}")

                received_obj = obj
                received_type = type(obj).__name__
                is_same_object = (obj is client_obj)

                # Try to access value
                if hasattr(obj, 'value'):
                    print(f"[CALLBACK] obj.value: {obj.value}")

                await asyncio.sleep(0.01)
                return "callback_executed"

            # Connect to server
            server_conn = await rpyc.async_connect("localhost", self.port)

            try:
                print("✓ Server connection ready (asyncio serving auto-enabled)")

                print("\n[TEST] Calling server method with client object and callback...")
                # Call server method, passing client object and callback
                # Server will call callback with the client object
                result = await server_conn.root.async_call_with_callback(client_obj, my_callback)

                print(f"\n[TEST] Server returned: {result}")
                print(f"[TEST] Callback received type: {received_type}")
                print(f"[TEST] Is same object: {is_same_object}")
                print("="*60)

                # CRITICAL ASSERTIONS
                self.assertIsNotNone(received_obj, "Callback should have received an object")

                # The received object should be the ORIGINAL ClientObject, not a netref
                self.assertEqual(received_type, "ClientObject",
                    f"Expected original ClientObject, got {received_type}")

                # It should be the exact same object instance (same id)
                self.assertTrue(is_same_object,
                    "Received object should be the exact same instance as original")

                # Verify we can access original attributes directly
                self.assertEqual(received_obj.value, "test_value")
                self.assertEqual(received_obj.get_value(), "test_value")

                print("✓ TEST PASSED! Object was deserialized to original.")

            finally:
                await server_conn.aclose()

        asyncio.run(test())

    def test_multiple_objects_deserialization(self):
        """
        Test that multiple different client objects are correctly deserialized.
        """
        async def test():
            print("\n" + "="*60)
            print("TEST: Multiple objects deserialization")
            print("="*60)

            # Define client objects
            class ClientObject:
                def __init__(self, name):
                    self.name = name

            obj1 = ClientObject("first")
            obj2 = ClientObject("second")

            print(f"[CLIENT] Created obj1: {obj1.name}, id={id(obj1)}")
            print(f"[CLIENT] Created obj2: {obj2.name}, id={id(obj2)}")

            # Track received objects
            received_objects = []

            async def my_callback(obj):
                """Callback that tracks received objects."""
                print(f"[CALLBACK] Received: type={type(obj).__name__}, name={obj.name}, id={id(obj)}")
                received_objects.append({
                    'obj': obj,
                    'type': type(obj).__name__,
                    'is_obj1': obj is obj1,
                    'is_obj2': obj is obj2,
                })
                await asyncio.sleep(0.01)
                return f"received_{obj.name}"

            # Connect to server
            server_conn = await rpyc.async_connect("localhost", self.port)

            try:

                print("\n[TEST] Calling server with two objects...")
                results = await server_conn.root.async_multiple_callbacks(obj1, obj2, my_callback)

                print(f"\n[TEST] Server returned: {results}")
                print(f"[TEST] Received {len(received_objects)} objects in callbacks")

                # Verify we got both objects
                self.assertEqual(len(received_objects), 2)

                # Verify first object
                self.assertEqual(received_objects[0]['type'], 'ClientObject')
                self.assertTrue(received_objects[0]['is_obj1'])
                self.assertFalse(received_objects[0]['is_obj2'])
                self.assertEqual(received_objects[0]['obj'].name, 'first')

                # Verify second object
                self.assertEqual(received_objects[1]['type'], 'ClientObject')
                self.assertFalse(received_objects[1]['is_obj1'])
                self.assertTrue(received_objects[1]['is_obj2'])
                self.assertEqual(received_objects[1]['obj'].name, 'second')

                print("✓ TEST PASSED! Multiple objects deserialized correctly.")

            finally:
                await server_conn.aclose()

        asyncio.run(test())

    def test_netref_with_methods(self):
        """
        Test that deserialized object methods work correctly.
        """
        async def test():
            print("\n" + "="*60)
            print("TEST: Deserialized object methods")
            print("="*60)

            # Define client object with methods
            class ClientObject:
                def __init__(self, value):
                    self.value = value
                    self.counter = 0

                def increment(self):
                    self.counter += 1
                    return self.counter

                async def async_get_info(self):
                    await asyncio.sleep(0.01)
                    return f"Value: {self.value}, Counter: {self.counter}"

            client_obj = ClientObject(42)
            print(f"[CLIENT] Created object with value={client_obj.value}")

            # Callback that tests methods
            async def my_callback(obj):
                print(f"[CALLBACK] Testing methods on received object...")

                # Call sync method
                count = obj.increment()
                print(f"[CALLBACK] increment() returned: {count}")

                # Call async method
                info = await obj.async_get_info()
                print(f"[CALLBACK] async_get_info() returned: {info}")

                return "methods_tested"

            # Connect to server
            server_conn = await rpyc.async_connect("localhost", self.port)

            try:

                print("\n[TEST] Calling server...")
                result = await server_conn.root.async_call_with_callback(client_obj, my_callback)

                print(f"\n[TEST] Result: {result}")
                print(f"[TEST] Client object counter after callback: {client_obj.counter}")

                # Verify methods were called on original object
                self.assertEqual(client_obj.counter, 1,
                    "Methods should have been called on original object")

                print("✓ TEST PASSED! Methods work on deserialized object.")

            finally:
                await server_conn.aclose()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main(verbosity=2)
