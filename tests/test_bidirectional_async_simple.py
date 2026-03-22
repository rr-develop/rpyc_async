"""
Simplified Bidirectional Async Test

Tests that async callbacks work in simpler scenario:
- Server calls client async method directly (not as callback parameter)
- Uses classic client for simpler setup
"""
import unittest
import asyncio
import rpyc
from rpyc.utils.server import ThreadedServer
from rpyc.utils.classic import DEFAULT_SERVER_PORT
from threading import Thread
import time


class ServerService(rpyc.Service):
    """Server that will call back to client."""

    async def exposed_process_with_client_call(self, client_conn_root, value):
        """
        Server async method that calls client async method.

        Instead of callback parameter, we get client connection root.
        """
        print(f"[SERVER] Processing value={value}")
        await asyncio.sleep(0.01)

        # Call client's async method
        print(f"[SERVER] Calling client.async_process({value * 2})")
        result = await client_conn_root.async_process(value * 2)

        return f"Server got from client: {result}"


class ClientService(rpyc.Service):
    """Client service (for receiving calls from server)."""

    async def exposed_async_process(self, value):
        """Client async method that server will call."""
        print(f"[CLIENT] async_process called with value={value}")
        await asyncio.sleep(0.01)
        return f"Client processed: {value}"


class TestSimpleBidirectionalAsync(unittest.TestCase):
    """Test simple bidirectional async without callback parameters."""

    @classmethod
    def setUpClass(cls):
        """Start server."""
        cls.server = ThreadedServer(
            ServerService,
            port=18872,
            protocol_config={'allow_all_attrs': True}
        )
        cls.server_thread = Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        """Stop server."""
        cls.server.close()

    def test_server_calls_client_async_method(self):
        """
        Test that server can call client's async method.

        This is simpler than callback parameters but tests the same concept.
        """
        async def test():
            print("\n" + "="*60)
            print("TEST: Server calls client async method")
            print("="*60)

            # Connect to server
            server_conn = rpyc.connect("localhost", 18872)
            server_conn.enable_asyncio_serving()

            # Create client service
            client_service = ClientService()

            # Create a mock client connection that server can use
            # In real bidirectional setup, this would be automatic
            # For this test, we'll use a trick: expose the client service via server connection

            # Alternative approach: Use bg serving thread for client
            from rpyc.utils.server import ThreadedServer as ClientServer

            # Start client as server too
            client_server = ClientServer(
                ClientService,
                port=18873,
                protocol_config={'allow_all_attrs': True}
            )
            client_server_thread = Thread(target=client_server.start, daemon=True)
            client_server_thread.start()
            time.sleep(0.5)

            # Server connects to client
            client_conn = rpyc.connect("localhost", 18873)

            try:
                # Now server can call client
                # But we need to pass client_conn.root to server
                result = await server_conn.root.process_with_client_call(
                    client_conn.root,
                    value=5
                )

                print(f"\n[TEST] Result: {result}")
                self.assertIn("Client processed", result)
                print("✓ Server successfully called client async method!")

            finally:
                client_conn.close()
                server_conn.disable_asyncio_serving()
                server_conn.close()
                client_server.close()

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main(verbosity=2)
