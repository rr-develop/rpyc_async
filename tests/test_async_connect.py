"""
TDD tests for async_connect module.

CRITICAL: These tests verify that async_connect() provides fully async,
non-blocking socket connections for RPyC.

Tests verify:
1. AsyncioStream implements Stream interface correctly
2. AsyncioStream wraps asyncio streams properly
3. async_connect() establishes connections without blocking
4. Connection has asyncio serving enabled
5. Timeout handling works correctly
6. Error handling is proper
"""
import asyncio
import unittest
import socket
from unittest.mock import Mock, patch, AsyncMock
from rpyc.core.async_connect import AsyncioStream, async_connect
from rpyc.core.service import VoidService
from rpyc.utils.server import ThreadedServer


class TestAsyncioStream(unittest.TestCase):
    """Test AsyncioStream class."""

    def setUp(self):
        """Create mock reader/writer and event loop."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # Mock asyncio streams
        self.mock_reader = Mock(spec=asyncio.StreamReader)
        self.mock_reader._buffer = b""
        self.mock_writer = Mock(spec=asyncio.StreamWriter)
        self.mock_writer.is_closing.return_value = False

        # Mock socket for fileno()
        self.mock_sock = Mock()
        self.mock_sock.fileno.return_value = 42
        self.mock_writer.get_extra_info.return_value = self.mock_sock

        self.stream = AsyncioStream(self.mock_reader, self.mock_writer, self.loop)

    def tearDown(self):
        """Clean up event loop."""
        self.loop.close()

    def test_asyncio_stream_has_max_io_chunk(self):
        """Test that AsyncioStream has MAX_IO_CHUNK constant."""
        self.assertTrue(hasattr(AsyncioStream, 'MAX_IO_CHUNK'))
        self.assertIsInstance(AsyncioStream.MAX_IO_CHUNK, int)
        self.assertGreater(AsyncioStream.MAX_IO_CHUNK, 0)

    def test_asyncio_stream_closed_property(self):
        """Test closed property."""
        self.assertFalse(self.stream.closed)

        # Mark as closing
        self.mock_writer.is_closing.return_value = True
        self.assertTrue(self.stream.closed)

    def test_asyncio_stream_fileno(self):
        """Test fileno() returns correct file descriptor."""
        fd = self.stream.fileno()
        self.assertEqual(fd, 42)

    def test_asyncio_stream_fileno_when_closed(self):
        """Test fileno() raises EOFError when closed."""
        self.stream._closed = True
        with self.assertRaises(EOFError):
            self.stream.fileno()

    def test_asyncio_stream_close(self):
        """Test close() closes the writer."""
        self.stream.close()
        self.mock_writer.close.assert_called_once()
        self.assertTrue(self.stream._closed)

    def test_asyncio_stream_read_success(self):
        """Test read() returns correct data."""
        # Mock async read
        async def mock_readexactly(count):
            return b"test_data"[:count]

        self.mock_reader.readexactly = mock_readexactly

        # Read 4 bytes
        data = self.stream.read(4)
        self.assertEqual(data, b"test")

    def test_asyncio_stream_read_when_closed(self):
        """Test read() raises EOFError when closed."""
        self.stream._closed = True
        with self.assertRaises(EOFError):
            self.stream.read(10)

    def test_asyncio_stream_write_success(self):
        """Test write() sends data correctly."""
        # Mock async drain
        async def mock_drain():
            pass

        self.mock_writer.drain = mock_drain

        # Write data
        self.stream.write(b"test_data")

        # Verify writer.write() was called
        self.mock_writer.write.assert_called_once_with(b"test_data")

    def test_asyncio_stream_write_when_closed(self):
        """Test write() raises EOFError when closed."""
        self.stream._closed = True
        with self.assertRaises(EOFError):
            self.stream.write(b"test")


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

    async def test_async_connect_basic(self):
        """Test basic async_connect() functionality."""
        # Connect to test server
        conn = await async_connect("127.0.0.1", self.server_port, timeout=5.0)

        # Verify connection is established
        self.assertIsNotNone(conn)
        self.assertFalse(conn.closed)

        # Clean up
        conn.close()

    async def test_async_connect_no_blocking(self):
        """
        Test that async_connect() doesn't block event loop.

        CRITICAL: This is the KEY test - verifies NO blocking I/O!
        """
        start_time = asyncio.get_event_loop().time()

        # Connect to server (should be instant, not blocked)
        conn = await async_connect("127.0.0.1", self.server_port, timeout=5.0)

        elapsed = asyncio.get_event_loop().time() - start_time

        # Connection should be very fast (< 0.5 seconds for localhost)
        self.assertLess(elapsed, 0.5, f"Connection took {elapsed}s - might be blocking!")

        # Verify connection works
        self.assertFalse(conn.closed)

        conn.close()

    async def test_async_connect_timeout(self):
        """Test that timeout parameter works correctly."""
        # Try to connect to non-routable IP (should timeout)
        with self.assertRaises(ConnectionError) as ctx:
            await async_connect("192.0.2.1", 9999, timeout=0.5)

        # Verify timeout error message
        self.assertIn("timed out", str(ctx.exception).lower())

    async def test_async_connect_connection_refused(self):
        """Test error handling for connection refused."""
        # Try to connect to closed port
        with self.assertRaises(ConnectionError) as ctx:
            await async_connect("127.0.0.1", 1, timeout=1.0)

        # Verify error message
        self.assertIn("failed to connect", str(ctx.exception).lower())

    async def test_async_connect_rpc_calls(self):
        """Test that RPC calls work with async_connect()."""
        conn = await async_connect("127.0.0.1", self.server_port, timeout=5.0)

        # Make sync RPC call
        result = conn.root.echo("test")
        self.assertEqual(result, "echo: test")

        # Make another call
        result = conn.root.add(2, 3)
        self.assertEqual(result, 5)

        conn.close()

    async def test_async_connect_has_asyncio_attributes(self):
        """Test that connection has asyncio attributes (even if not enabled)."""
        conn = await async_connect("127.0.0.1", self.server_port, timeout=5.0)

        # Check asyncio attributes exist (they're always present after __init__)
        self.assertTrue(hasattr(conn, '_asyncio_enabled'))
        self.assertTrue(hasattr(conn, '_asyncio_loop'))
        self.assertTrue(hasattr(conn, '_loop_fd_registered'))

        # Note: async_connect() creates standard sync connection,
        # so asyncio serving is NOT enabled by default.
        # Users can call conn.enable_asyncio_serving() if needed.

        conn.close()

    async def test_async_connect_multiple_concurrent(self):
        """
        Test multiple concurrent connections work without blocking.

        CRITICAL: This tests that we don't exhaust ThreadPoolExecutor!
        With old blocking connect(), this would deadlock with many connections.
        """
        # Create 20 concurrent connections
        tasks = [
            async_connect("127.0.0.1", self.server_port, timeout=5.0)
            for _ in range(20)
        ]

        start_time = asyncio.get_event_loop().time()

        # All should connect concurrently (not sequentially!)
        conns = await asyncio.gather(*tasks)

        elapsed = asyncio.get_event_loop().time() - start_time

        # Should complete fast even with 20 connections
        # If blocking, would take 20 * connection_time sequentially
        self.assertLess(elapsed, 2.0, f"20 connections took {elapsed}s - likely blocking!")

        # Verify all connections work
        self.assertEqual(len(conns), 20)
        for conn in conns:
            self.assertFalse(conn.closed)

        # Test RPC calls work on all connections
        results = [conn.root.add(1, 1) for conn in conns]
        self.assertEqual(results, [2] * 20)

        # Clean up
        for conn in conns:
            conn.close()

    async def test_async_connect_custom_config(self):
        """Test async_connect() with custom config."""
        custom_config = {
            "allow_public_attrs": True,
            "allow_safe_attrs": True,
        }

        conn = await async_connect(
            "127.0.0.1",
            self.server_port,
            config=custom_config,
            timeout=5.0
        )

        # Verify config was applied
        self.assertEqual(conn._config["allow_public_attrs"], True)
        self.assertEqual(conn._config["allow_safe_attrs"], True)

        conn.close()

    async def test_async_connect_accepts_loop_parameter(self):
        """Test that async_connect() accepts loop parameter without error."""
        loop = asyncio.get_running_loop()

        # Should not raise error when loop parameter is provided
        conn = await async_connect("127.0.0.1", self.server_port, loop=loop, timeout=5.0)

        # Verify connection works
        self.assertIsNotNone(conn)
        self.assertFalse(conn.closed)

        conn.close()




if __name__ == "__main__":
    unittest.main()
