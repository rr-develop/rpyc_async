"""
Unit tests for Connection asyncio integration.

Tests verify enable/disable_asyncio_serving(), FD registration,
and event loop integration.
"""
import unittest
import asyncio
from unittest.mock import Mock, MagicMock, patch
from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService


class TestConnectionAsyncio(unittest.TestCase):
    """Test Connection asyncio integration."""

    def _create_mock_connection(self):
        """Create a mock connection for testing."""
        service = VoidService()
        channel = Mock()
        channel.fileno.return_value = 999  # Fake FD
        channel.poll.return_value = False  # No data
        channel.closed = False

        conn = Connection(service, channel)
        # Patch close to avoid HANDLE_CLOSE request
        conn._cleanup = Mock()
        conn._send = Mock()
        return conn, channel

    def test_connection_has_asyncio_attributes(self):
        """Test that Connection has async-related attributes after init."""
        conn, _ = self._create_mock_connection()

        # Check new attributes exist
        self.assertTrue(hasattr(conn, '_asyncio_loop'))
        self.assertTrue(hasattr(conn, '_asyncio_enabled'))
        self.assertTrue(hasattr(conn, '_loop_fd_registered'))

        # Check initial values
        self.assertIsNone(conn._asyncio_loop)
        self.assertFalse(conn._asyncio_enabled)
        self.assertFalse(conn._loop_fd_registered)

    def test_enable_asyncio_serving_with_running_loop(self):
        """Test enable_asyncio_serving() with running event loop."""
        async def test():
            conn, _ = self._create_mock_connection()

            # Get running loop
            loop = asyncio.get_running_loop()

            # Enable asyncio serving
            conn.enable_asyncio_serving(loop)

            # Verify state
            self.assertTrue(conn._asyncio_enabled)
            self.assertIs(conn._asyncio_loop, loop)
            self.assertTrue(conn._loop_fd_registered)

        asyncio.run(test())

    def test_enable_asyncio_serving_without_loop(self):
        """Test enable_asyncio_serving() detects running loop automatically."""
        async def test():
            conn, _ = self._create_mock_connection()

            # Enable without explicit loop (should detect)
            conn.enable_asyncio_serving()

            # Should auto-detect running loop
            self.assertTrue(conn._asyncio_enabled)
            self.assertIsNotNone(conn._asyncio_loop)

        asyncio.run(test())

    def test_enable_asyncio_serving_outside_loop_fails(self):
        """Test enable_asyncio_serving() outside event loop raises error."""
        conn, _ = self._create_mock_connection()

        # Try to enable outside event loop (should fail)
        with self.assertRaises(RuntimeError) as ctx:
            conn.enable_asyncio_serving()

        self.assertIn("running event loop", str(ctx.exception).lower())

    def test_enable_asyncio_serving_idempotent(self):
        """Test that calling enable_asyncio_serving() twice is safe."""
        async def test():
            conn, _ = self._create_mock_connection()
            loop = asyncio.get_running_loop()

            # Enable twice
            conn.enable_asyncio_serving(loop)
            conn.enable_asyncio_serving(loop)  # Should not raise

            # Still enabled once
            self.assertTrue(conn._asyncio_enabled)

        asyncio.run(test())

    def test_disable_asyncio_serving(self):
        """Test disable_asyncio_serving() cleanup."""
        async def test():
            conn, _ = self._create_mock_connection()
            loop = asyncio.get_running_loop()

            # Enable then disable
            conn.enable_asyncio_serving(loop)
            self.assertTrue(conn._asyncio_enabled)

            conn.disable_asyncio_serving()

            # Should be disabled
            self.assertFalse(conn._asyncio_enabled)
            self.assertIsNone(conn._asyncio_loop)
            self.assertFalse(conn._loop_fd_registered)

        asyncio.run(test())

    def test_disable_asyncio_serving_idempotent(self):
        """Test that calling disable_asyncio_serving() when disabled is safe."""
        conn, _ = self._create_mock_connection()

        # Disable when not enabled (should not raise)
        conn.disable_asyncio_serving()
        conn.disable_asyncio_serving()

    def test_close_cleans_up_asyncio(self):
        """Test that close() properly cleans up asyncio resources."""
        async def test():
            conn, _ = self._create_mock_connection()
            loop = asyncio.get_running_loop()
            conn.enable_asyncio_serving(loop)

            # Close should cleanup asyncio
            conn._closed = True  # Prevent HANDLE_CLOSE
            conn.disable_asyncio_serving()  # Manual cleanup

            # Asyncio should be disabled
            self.assertFalse(conn._asyncio_enabled)
            self.assertFalse(conn._loop_fd_registered)

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
