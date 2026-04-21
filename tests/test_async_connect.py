"""
Tests for async_connect module.

Verifies that ``async_connect`` provides fully async, non-blocking socket
connections for RPyC talking to an ``AsyncioServer``. The formerly-tested
``AsyncioStream`` shim is gone (dead code — see the design document
docs/DESIGN_ASYNC_CONNECT_POLICY.md).

After the sync_request guard landed, synchronous RPC like
``conn.root.echo()`` from inside a running event loop now raises
``RuntimeError`` by policy. These tests therefore talk to the connection
exclusively via the async path (``await conn.root.*``).
"""
import asyncio
import unittest
from rpyc.core.async_connect import async_connect
from rpyc.core.service import VoidService
from rpyc.utils.server import ThreadedServer


class TestAsyncConnect(unittest.IsolatedAsyncioTestCase):
    """Test async_connect function with real RPyC server."""

    @classmethod
    def setUpClass(cls):
        """Start RPyC server for testing."""
        # Create a simple service
        class TestService(VoidService):
            def exposed_echo(self, msg):
                return f"echo: {msg}"

            def exposed_add(self, a, b):
                return a + b

        # Start server in background thread
        cls.server = ThreadedServer(
            TestService,
            port=0,  # Random port
            protocol_config={"allow_public_attrs": True}
        )
        cls.server_thread = cls.server._start_in_thread()

        # Get actual port
        cls.server_port = cls.server.port

    @classmethod
    def tearDownClass(cls):
        """Stop RPyC server."""
        cls.server.close()
        cls.server_thread.join(timeout=5)

    async def _aclose(self, conn):
        """Close helper that uses aclose if present, else raw cleanup.

        After the sync_request guard landed, sync `close()` on an
        asyncio-enabled connection running on the current loop would raise.
        Tests use the event-driven `aclose()` path.
        """
        if hasattr(conn, "aclose"):
            await conn.aclose()
        else:  # pragma: no cover — should not happen after refactor
            conn.close()

    async def test_async_connect_basic(self):
        """Test basic async_connect() functionality."""
        conn = await async_connect("127.0.0.1", self.server_port, timeout=5.0)
        self.assertIsNotNone(conn)
        self.assertFalse(conn.closed)
        await self._aclose(conn)

    async def test_async_connect_no_blocking(self):
        """async_connect() must not block the event loop."""
        start_time = asyncio.get_event_loop().time()
        conn = await async_connect("127.0.0.1", self.server_port, timeout=5.0)
        elapsed = asyncio.get_event_loop().time() - start_time
        self.assertLess(elapsed, 0.5, f"Connection took {elapsed}s - might be blocking!")
        self.assertFalse(conn.closed)
        await self._aclose(conn)

    async def test_async_connect_timeout(self):
        """Test that timeout parameter works correctly."""
        with self.assertRaises(ConnectionError) as ctx:
            await async_connect("192.0.2.1", 9999, timeout=0.5)
        self.assertIn("timed out", str(ctx.exception).lower())

    async def test_async_connect_connection_refused(self):
        """Test error handling for connection refused."""
        with self.assertRaises(ConnectionError) as ctx:
            await async_connect("127.0.0.1", 1, timeout=1.0)
        self.assertIn("failed to connect", str(ctx.exception).lower())

    async def test_async_connect_rpc_calls_via_async_wrapper(self):
        """RPC calls from async code go through the awaitable async path.

        Synchronous ``conn.root.echo("test")`` from inside a running event
        loop is now forbidden by the ``sync_request`` guard (it would block
        the loop). Use ``rpyc.async_(proxy)`` to get an async wrapper whose
        result can be awaited.
        """
        import rpyc
        conn = await async_connect("127.0.0.1", self.server_port, timeout=5.0)
        try:
            async_echo = rpyc.async_(conn.root.echo)
            async_add = rpyc.async_(conn.root.add)

            result = await async_echo("test")
            self.assertEqual(result, "echo: test")

            result = await async_add(2, 3)
            self.assertEqual(result, 5)
        finally:
            await self._aclose(conn)

    async def test_async_connect_has_asyncio_attributes(self):
        """async_connect() must auto-enable asyncio serving on the connection."""
        conn = await async_connect("127.0.0.1", self.server_port, timeout=5.0)
        try:
            self.assertTrue(hasattr(conn, '_asyncio_enabled'))
            self.assertTrue(hasattr(conn, '_asyncio_loop'))
            self.assertTrue(hasattr(conn, '_loop_fd_registered'))
            self.assertTrue(
                conn._asyncio_enabled,
                "async_connect() must auto-enable asyncio serving",
            )
        finally:
            await self._aclose(conn)

    async def test_async_connect_multiple_concurrent(self):
        """Multiple concurrent connections must not block each other."""
        import rpyc
        tasks = [
            async_connect("127.0.0.1", self.server_port, timeout=5.0)
            for _ in range(20)
        ]
        start_time = asyncio.get_event_loop().time()
        conns = await asyncio.gather(*tasks)
        elapsed = asyncio.get_event_loop().time() - start_time
        self.assertLess(elapsed, 2.0, f"20 connections took {elapsed}s - likely blocking!")
        self.assertEqual(len(conns), 20)
        for conn in conns:
            self.assertFalse(conn.closed)

        # Exercise each connection through the async RPC path.
        results = await asyncio.gather(
            *[rpyc.async_(c.root.add)(1, 1) for c in conns]
        )
        self.assertEqual(list(results), [2] * 20)

        for conn in conns:
            await self._aclose(conn)

    async def test_async_connect_custom_config(self):
        """Test async_connect() with custom config."""
        custom_config = {
            "allow_public_attrs": True,
            "allow_safe_attrs": True,
        }
        conn = await async_connect(
            "127.0.0.1", self.server_port, config=custom_config, timeout=5.0
        )
        try:
            self.assertEqual(conn._config["allow_public_attrs"], True)
            self.assertEqual(conn._config["allow_safe_attrs"], True)
        finally:
            await self._aclose(conn)

    async def test_async_connect_accepts_loop_parameter(self):
        """Test that async_connect() accepts loop parameter without error."""
        loop = asyncio.get_running_loop()
        conn = await async_connect(
            "127.0.0.1", self.server_port, loop=loop, timeout=5.0
        )
        try:
            self.assertIsNotNone(conn)
            self.assertFalse(conn.closed)
        finally:
            await self._aclose(conn)

    async def test_async_connect_root_ready_immediately(self):
        """Verify that _remote_root is pre-fetched during async_connect()."""
        conn = await async_connect("127.0.0.1", self.server_port, timeout=5.0)
        try:
            self.assertIsNotNone(
                conn._remote_root,
                "Bug detected: _remote_root is None after async_connect! "
                "This will cause blocking sync_request on first conn.root access.",
            )
            root = conn.root
            self.assertIsNotNone(root)
        finally:
            await self._aclose(conn)

    async def test_async_connect_no_blocking_on_root_access(self):
        """Accessing conn.root must not block the event loop (eager handshake)."""
        conn = await async_connect("127.0.0.1", self.server_port, timeout=5.0)

        async def fast_task():
            await asyncio.sleep(0.01)
            return "completed"

        task = asyncio.create_task(fast_task())

        try:
            start_time = asyncio.get_event_loop().time()
            _ = conn.root  # Should be instant — no RPC needed.
            elapsed = asyncio.get_event_loop().time() - start_time
            self.assertLess(
                elapsed,
                0.01,
                f"Accessing conn.root took {elapsed}s - likely doing sync_request!",
            )
            await asyncio.wait_for(task, timeout=0.1)
            self.assertEqual(await task, "completed")
        finally:
            await self._aclose(conn)




if __name__ == "__main__":
    unittest.main()
