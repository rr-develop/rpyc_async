"""
Simplified unit tests for Connection asyncio integration.

Tests verify enable/disable_asyncio_serving() methods exist and work.
"""
import unittest
from unittest.mock import Mock
from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService


class TestConnectionAsyncioSimple(unittest.TestCase):
    """Simplified asyncio integration tests."""

    def setUp(self):
        """Create mock connection."""
        service = VoidService()
        channel = Mock()
        channel.fileno.return_value = 999
        channel.poll.return_value = False
        channel.closed = False

        self.conn = Connection(service, channel)
        self.conn._cleanup = Mock()  # Prevent real cleanup

    def test_connection_has_asyncio_attributes(self):
        """Test that Connection has async attributes after init."""
        self.assertTrue(hasattr(self.conn, '_asyncio_loop'))
        self.assertTrue(hasattr(self.conn, '_asyncio_enabled'))
        self.assertTrue(hasattr(self.conn, '_loop_fd_registered'))

        # Check initial values
        self.assertIsNone(self.conn._asyncio_loop)
        self.assertFalse(self.conn._asyncio_enabled)
        self.assertFalse(self.conn._loop_fd_registered)

    def test_has_enable_asyncio_serving_method(self):
        """Test that enable_asyncio_serving method exists."""
        self.assertTrue(hasattr(self.conn, 'enable_asyncio_serving'))
        self.assertTrue(callable(self.conn.enable_asyncio_serving))

    def test_has_disable_asyncio_serving_method(self):
        """Test that disable_asyncio_serving method exists."""
        self.assertTrue(hasattr(self.conn, 'disable_asyncio_serving'))
        self.assertTrue(callable(self.conn.disable_asyncio_serving))

    def test_disable_asyncio_serving_when_not_enabled(self):
        """Test disable_asyncio_serving when not enabled is safe."""
        # Should not raise
        self.conn.disable_asyncio_serving()
        self.conn.disable_asyncio_serving()  # Twice

        # Still disabled
        self.assertFalse(self.conn._asyncio_enabled)

    def test_enable_outside_event_loop_raises(self):
        """Test enable_asyncio_serving outside event loop raises error."""
        with self.assertRaises(RuntimeError) as ctx:
            self.conn.enable_asyncio_serving()

        self.assertIn("event loop", str(ctx.exception).lower())


if __name__ == '__main__':
    unittest.main()
