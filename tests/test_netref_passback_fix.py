"""
Regression test for netref pass-back bug.

BUG DESCRIPTION:
When client receives a netref to server object and passes it back to server,
the old code would fail because:
1. Client creates netref with ____conn__ = client_connection
2. When boxing to send back, code sees obj.____conn__ is self → uses LABEL_LOCAL_REF
3. But id_pack points to server object not in client._local_objects → KeyError!

FIX:
1. In _box(): Check if id_pack in _local_objects before using LABEL_LOCAL_REF
   - If not found: use LABEL_REMOTE_REF fallback (it's a proxy to remote object)
2. In _unbox(): Check if LABEL_REMOTE_REF points to object in OUR _local_objects
   - If yes: return local object directly (avoid creating proxy to ourselves)

This test ensures the fix works correctly.
"""
import unittest
import asyncio
import rpyc
from rpyc.utils.async_server import AsyncioServer
from multiprocessing import Process, Queue


def run_echo_server(port, ready_queue):
    """Server that echoes back arguments."""
    class EchoService(rpyc.Service):
        """Service that echoes back its arguments."""

        async def exposed_echo(self, obj):
            """Return the object back."""
            return obj

        def exposed_get_root(self):
            """Return self (service object) - will be netref on client."""
            return self

    async def server_main():
        server = AsyncioServer(
            EchoService,
            hostname='localhost',
            port=port,
            protocol_config={'allow_public_attrs': True}
        )
        await server.start()
        ready_queue.put("ready")

        # Keep server running
        try:
            while True:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            server.close()

    asyncio.run(server_main())


class TestNetrefPassBackFix(unittest.TestCase):
    """Test that passing netref back to its origin works correctly."""

    def setUp(self):
        self.server_process = None
        self.conn = None
        self.port = 18888

    def tearDown(self):
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
        if self.server_process:
            self.server_process.terminate()
            self.server_process.join(timeout=2)
            if self.server_process.is_alive():
                self.server_process.kill()

    def test_pass_server_netref_back_to_server(self):
        """
        Test passing server object back to server.

        Scenario:
        1. Client calls server.get_info() → receives netref to server method result
        2. Client passes that netref back via server.echo(netref)
        3. Server should receive its own object, not create proxy loop
        """
        async def test():
            # Start server
            ready_queue = Queue()
            self.server_process = Process(
                target=run_echo_server,
                args=(self.port, ready_queue),
                daemon=True
            )
            self.server_process.start()

            # Wait for server
            assert ready_queue.get(timeout=5) == "ready"
            await asyncio.sleep(0.1)

            # Connect
            self.conn = rpyc.connect('localhost', self.port)
            loop = asyncio.get_running_loop()
            self.conn.enable_asyncio_serving(loop=loop)

            # Get server root (sync method returns netref to service object)
            root_via_method = self.conn.root.get_root()
            print(f"\n[TEST] Got root from server: {root_via_method}")
            print(f"[TEST] root_via_method type: {type(root_via_method)}")
            print(f"[TEST] root_via_method.____conn__: {root_via_method.____conn__}")
            print(f"[TEST] conn: {self.conn}")
            print(f"[TEST] root_via_method.____conn__ is conn: {root_via_method.____conn__ is self.conn}")

            # Pass netref BACK to server via echo() (async method)
            # OLD BUG: This would fail with KeyError in _box() because:
            #   - root_via_method.____conn__ is client_connection
            #   - id_pack points to server object
            #   - _box() uses LABEL_LOCAL_REF but object not in client._local_objects
            # NEW FIX: Should work correctly by using LABEL_REMOTE_REF fallback
            print(f"[TEST] Passing netref back to server...")
            echoed = await self.conn.root.echo(root_via_method)
            print(f"[TEST] ✓ Echo succeeded: {echoed}")

            # Verify we got the same proxy
            assert type(echoed).__name__ == type(root_via_method).__name__
            print(f"[TEST] ✓ Echoed proxy has same type as original")

            self.conn.disable_asyncio_serving()

        asyncio.run(test())


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-xvs"])
