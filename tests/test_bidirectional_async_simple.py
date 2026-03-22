"""
Simplified Bidirectional Async Test

Tests that async callbacks work in simpler scenario:
- Server calls client async method directly (not as callback parameter)
- Uses classic client for simpler setup
"""
import unittest
import asyncio
import rpyc
from rpyc.utils.async_server import AsyncioServer
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
        """Start AsyncioServer in background event loop."""
        # Create event loop for server
        cls.server_loop = asyncio.new_event_loop()

        async def run_server():
            cls.server = AsyncioServer(
                ServerService,
                hostname='localhost',
                port=18872,
                protocol_config={'allow_all_attrs': True}
            )
            await cls.server.start()

        # Start server in background thread with its own event loop
        def start_server():
            asyncio.set_event_loop(cls.server_loop)
            cls.server_loop.run_until_complete(run_server())
            cls.server_loop.run_forever()

        cls.server_thread = Thread(target=start_server, daemon=True)
        cls.server_thread.start()
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        """Stop server."""
        async def stop_server():
            await cls.server.close()

        # Schedule close and stop loop
        asyncio.run_coroutine_threadsafe(stop_server(), cls.server_loop)
        cls.server_loop.call_soon_threadsafe(cls.server_loop.stop)

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
            loop = asyncio.get_running_loop()
            server_conn.enable_asyncio_serving(loop=loop)

            # Create client service
            client_service = ClientService()

            # Create a mock client connection that server can use
            # In real bidirectional setup, this would be automatic
            # For this test, we'll use a trick: expose the client service via server connection

            # Alternative approach: Use AsyncioServer for client
            # Create event loop for client server
            client_server_loop = asyncio.new_event_loop()

            async def run_client_server():
                client_server = AsyncioServer(
                    ClientService,
                    hostname='localhost',
                    port=18873,
                    protocol_config={'allow_all_attrs': True}
                )
                await client_server.start()
                return client_server

            # Start client server in background thread with its own event loop
            def start_client_server():
                asyncio.set_event_loop(client_server_loop)
                client_server = client_server_loop.run_until_complete(run_client_server())
                client_server_loop.run_forever()

            client_server_thread = Thread(target=start_client_server, daemon=True)
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
                # Stop client server
                client_server_loop.call_soon_threadsafe(client_server_loop.stop)

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main(verbosity=2)
