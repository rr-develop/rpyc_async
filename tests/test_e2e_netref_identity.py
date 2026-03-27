"""
E2E Test: Netref Identity Preservation

Tests that when a client passes the same object to the server multiple times,
the server receives the SAME netref object (identical, not just equal).

Scenario:
1. Client has a local object
2. Client calls server async method twice, passing the same object both times
3. Server receives netref to client object on first call and stores it
4. Server receives netref to client object on second call
5. Server checks that second netref IS (identity check) the first netref

This tests:
- Netref object identity preservation across multiple RPC calls
- Connection's netref caching mechanism (_id_to_local_obj mapping)
- That the same client object always maps to the same netref on server side

ARCHITECTURE:
=============
Process 1 (Server):                    Process 2 (Client/Test):
├─ AsyncioServer                       ├─ unittest.TestCase
├─ event loop (main thread)            ├─ event loop (main thread)
├─ ServerService                       ├─ ClientObject
├─ exposed_store_object (1st call)     ├─ rpyc.connect()
│  └─ stores netref1                   │  └─ passes same ClientObject
├─ exposed_check_identity (2nd call)   │      in both calls
│  └─ compares netref2 with netref1    │
│     using "is" operator              │
│                                      │
│  ←───────── netref1 (1st) ──────────┘
│  ←───────── netref2 (2nd) ──────────┘
│  [checks: netref1 is netref2]
└─ Returns identity check result       └─ Asserts True
"""
import unittest
import asyncio
import time
import rpyc
from rpyc.utils.async_server import AsyncioServer
from multiprocessing import Process, Queue
from tests.support import get_free_port


def run_netref_identity_server(port, ready_queue):
    """
    Server process entry point.

    Runs in SEPARATE PROCESS. Creates its own event loop in MAIN THREAD.
    """

    # Stored netref from first call
    stored_netref = None

    class NetrefIdentityService(rpyc.Service):
        """
        Service that stores a netref and checks identity on subsequent calls.
        """

        async def exposed_store_and_call(self, client_obj, value):
            """
            First call: Store the netref and call a method on it.

            Args:
                client_obj: Netref to client object (first reference)
                value: Value to pass to client method

            Returns:
                Result from calling client method
            """
            nonlocal stored_netref
            stored_netref = client_obj

            # Call async method on the netref
            result = await client_obj.async_method(value)
            return result

        async def exposed_check_identity_and_call(self, client_obj, value):
            """
            Second call: Check if the new netref is identical to stored one.

            Args:
                client_obj: Netref to client object (second reference)
                value: Value to pass to client method

            Returns:
                dict with:
                    - is_identical: True if netrefs are identical (same object)
                    - is_equal: True if netrefs are equal (== comparison)
                    - stored_id: id() of stored netref
                    - new_id: id() of new netref
                    - result: Result from calling client method
            """
            nonlocal stored_netref

            if stored_netref is None:
                raise RuntimeError("No netref stored yet! Call store_and_call first")

            # Check identity (is operator - same object)
            is_identical = stored_netref is client_obj

            # Check equality (== operator - equal values)
            is_equal = stored_netref == client_obj

            # Get object IDs for debugging
            stored_id = id(stored_netref)
            new_id = id(client_obj)

            # Call method on the netref
            result = await client_obj.async_method(value)

            return {
                "is_identical": is_identical,
                "is_equal": is_equal,
                "stored_id": stored_id,
                "new_id": new_id,
                "result": result
            }

    async def server_main():
        """Main server coroutine - runs AsyncioServer"""
        server = AsyncioServer(
            NetrefIdentityService,
            port=port,
            protocol_config={
                "allow_public_attrs": True,
                "allow_pickle": True,
                "sync_request_timeout": 30,
            }
        )

        try:
            await server.start()

            # Signal that server is ready
            ready_queue.put("ready")

            # Run forever (until terminated by parent)
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        finally:
            await server.close()

    # Run server event loop
    try:
        asyncio.run(server_main())
    except KeyboardInterrupt:
        pass


class TestNetrefIdentity(unittest.TestCase):
    """
    E2E tests for netref identity preservation.

    Tests run in SEPARATE PROCESS from server:
    - Server: Process 1 (AsyncioServer in main thread event loop)
    - Client: Process 2 (test code in main thread event loop)
    """

    def setUp(self):
        """
        Start server in separate process.

        CRITICAL: Must use get_free_port() in setUp() (per-test), NOT setUpClass()
        to avoid port conflicts when tests run in parallel.
        """
        # Get unique port for this test instance
        self.port = get_free_port()

        # Queue for server readiness signaling
        self.ready_queue = Queue()

        # Start server in separate process
        self.server_process = Process(
            target=run_netref_identity_server,
            args=(self.port, self.ready_queue),
            daemon=True  # Ensure cleanup if test fails
        )
        self.server_process.start()

        # Wait for server to signal ready (with timeout)
        try:
            signal = self.ready_queue.get(timeout=5.0)
            if signal != "ready":
                self.server_process.terminate()
                raise RuntimeError(f"Unexpected signal from server: {signal}")
        except Exception as e:
            self.server_process.terminate()
            raise RuntimeError(f"Server failed to start: {e}")

        # Give server a moment to fully initialize
        time.sleep(0.2)

    def tearDown(self):
        """Stop server process"""
        if self.server_process.is_alive():
            self.server_process.terminate()
            self.server_process.join(timeout=2.0)

        # Force kill if still alive
        if self.server_process.is_alive():
            self.server_process.kill()
            self.server_process.join(timeout=1.0)

    def test_netref_identity_preserved(self):
        """
        Test that passing the same object twice results in identical netrefs.

        The server should receive the SAME netref object (not just equal),
        meaning the connection properly caches netrefs by object ID.
        """

        class ClientObject:
            """Client object with async method"""

            def __init__(self, name):
                self.name = name
                self.call_count = 0

            async def async_method(self, value):
                """Simple async method that returns modified value"""
                self.call_count += 1
                await asyncio.sleep(0.01)  # Simulate async work
                return value * 2

        async def test():
            # Create client object
            client_obj = ClientObject("test_object")

            # Connect to server
            conn = rpyc.connect("localhost", self.port)

            try:
                # Enable asyncio serving (CRITICAL for async calls)
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)

                # First call: Pass object to server and call method
                result1 = await conn.root.store_and_call(client_obj, 5)
                self.assertEqual(result1, 10)  # 5 * 2
                self.assertEqual(client_obj.call_count, 1)

                # Second call: Pass SAME object again
                result_dict = await conn.root.check_identity_and_call(client_obj, 7)

                # Verify results
                self.assertEqual(result_dict["result"], 14)  # 7 * 2
                self.assertEqual(client_obj.call_count, 2)

                # Print debug info
                print(f"\n=== Netref Identity Check ===")
                print(f"Is identical (is operator): {result_dict['is_identical']}")
                print(f"Is equal (== operator): {result_dict['is_equal']}")
                print(f"Stored netref id(): {result_dict['stored_id']}")
                print(f"New netref id(): {result_dict['new_id']}")
                print(f"IDs match: {result_dict['stored_id'] == result_dict['new_id']}")

                # CRITICAL ASSERTION: Check identity
                self.assertTrue(
                    result_dict["is_identical"],
                    f"Netrefs should be identical! "
                    f"stored_id={result_dict['stored_id']}, "
                    f"new_id={result_dict['new_id']}"
                )

                # They should also be equal (weaker condition)
                self.assertTrue(result_dict["is_equal"])

                # IDs should match
                self.assertEqual(
                    result_dict["stored_id"],
                    result_dict["new_id"],
                    "Netref IDs should be identical"
                )

            finally:
                conn.disable_asyncio_serving()
                conn.close()

        # Run async test
        asyncio.run(test())

    def test_different_objects_get_different_netrefs(self):
        """
        Test that different objects result in different netrefs.

        This is a sanity check to ensure the identity test is meaningful.
        """

        class ClientObject:
            """Client object with async method"""

            def __init__(self, name):
                self.name = name

            async def async_method(self, value):
                """Simple async method"""
                await asyncio.sleep(0.01)
                return value * 2

        async def test():
            # Create two different client objects
            client_obj1 = ClientObject("object_1")
            client_obj2 = ClientObject("object_2")

            # Connect to server
            conn = rpyc.connect("localhost", self.port)

            try:
                loop = asyncio.get_running_loop()
                conn.enable_asyncio_serving(loop=loop)

                # First call: Pass first object
                result1 = await conn.root.store_and_call(client_obj1, 5)
                self.assertEqual(result1, 10)

                # Second call: Pass DIFFERENT object
                result_dict = await conn.root.check_identity_and_call(client_obj2, 7)

                # Verify results
                self.assertEqual(result_dict["result"], 14)

                # Print debug info
                print(f"\n=== Different Objects Check ===")
                print(f"Is identical (is operator): {result_dict['is_identical']}")
                print(f"Is equal (== operator): {result_dict['is_equal']}")
                print(f"Stored netref id(): {result_dict['stored_id']}")
                print(f"New netref id(): {result_dict['new_id']}")
                print(f"IDs match: {result_dict['stored_id'] == result_dict['new_id']}")

                # CRITICAL: These should NOT be identical
                self.assertFalse(
                    result_dict["is_identical"],
                    f"Different objects should get different netrefs! "
                    f"stored_id={result_dict['stored_id']}, "
                    f"new_id={result_dict['new_id']}"
                )

                # IDs should be different
                self.assertNotEqual(
                    result_dict["stored_id"],
                    result_dict["new_id"],
                    "Different objects should have different netref IDs"
                )

            finally:
                conn.disable_asyncio_serving()
                conn.close()

        # Run async test
        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
