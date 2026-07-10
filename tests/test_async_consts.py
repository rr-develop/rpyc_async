"""
Unit tests for async protocol constants.

Tests verify that new async constants are defined correctly
and don't conflict with existing constants.
"""
import unittest
from rpyc_async.core import consts


class TestAsyncConstants(unittest.TestCase):
    """Test async protocol constants."""

    def test_async_message_types_defined(self):
        """Test that async message types are defined."""
        self.assertTrue(hasattr(consts, 'MSG_ASYNC_REQUEST'))
        self.assertTrue(hasattr(consts, 'MSG_ASYNC_REPLY'))
        self.assertTrue(hasattr(consts, 'MSG_ASYNC_EXCEPTION'))

    def test_async_message_types_unique(self):
        """Test that async message types don't conflict with existing."""
        async_msgs = {
            consts.MSG_ASYNC_REQUEST,
            consts.MSG_ASYNC_REPLY,
            consts.MSG_ASYNC_EXCEPTION
        }
        existing_msgs = {
            consts.MSG_REQUEST,
            consts.MSG_REPLY,
            consts.MSG_EXCEPTION
        }

        # No overlap between async and sync message types
        self.assertEqual(len(async_msgs & existing_msgs), 0)

    def test_async_message_types_values(self):
        """Test that async message types have correct values."""
        # Should start at 10 to avoid conflicts
        self.assertEqual(consts.MSG_ASYNC_REQUEST, 10)
        self.assertEqual(consts.MSG_ASYNC_REPLY, 11)
        self.assertEqual(consts.MSG_ASYNC_EXCEPTION, 12)

    def test_async_handlers_defined(self):
        """Test that async handlers are defined."""
        self.assertTrue(hasattr(consts, 'HANDLE_ASYNC_CALL'))
        self.assertTrue(hasattr(consts, 'HANDLE_ASYNC_CALLATTR'))

    def test_async_handlers_unique(self):
        """Test that async handlers don't conflict with existing."""
        async_handlers = {
            consts.HANDLE_ASYNC_CALL,
            consts.HANDLE_ASYNC_CALLATTR
        }

        # Check all existing handlers
        existing_handlers = set()
        for name in dir(consts):
            if name.startswith('HANDLE_') and not name.startswith('HANDLE_ASYNC_'):
                existing_handlers.add(getattr(consts, name))

        # No overlap
        self.assertEqual(len(async_handlers & existing_handlers), 0)

    def test_async_handlers_values(self):
        """Test that async handlers have correct values."""
        # Should start at 100 for clear separation
        self.assertEqual(consts.HANDLE_ASYNC_CALL, 100)
        self.assertEqual(consts.HANDLE_ASYNC_CALLATTR, 101)

    def test_flags_defined(self):
        """Test that object flags are defined."""
        self.assertTrue(hasattr(consts, 'FLAGS_SYNC'))
        self.assertTrue(hasattr(consts, 'FLAGS_ASYNC'))

    def test_flags_values(self):
        """Test that flags have correct bitmask values."""
        self.assertEqual(consts.FLAGS_SYNC, 0x00)
        self.assertEqual(consts.FLAGS_ASYNC, 0x01)

    def test_flags_are_bitmasks(self):
        """Test that flags can be combined with bitwise OR."""
        # FLAGS_ASYNC should be a single bit
        self.assertEqual(bin(consts.FLAGS_ASYNC).count('1'), 1)

        # Can combine flags
        combined = consts.FLAGS_SYNC | consts.FLAGS_ASYNC
        self.assertEqual(combined, 0x01)

    def test_protocol_version_defined(self):
        """Test that protocol version is defined."""
        self.assertTrue(hasattr(consts, 'PROTOCOL_VERSION'))

    def test_protocol_version_value(self):
        """Test that protocol version is bumped to 5.1."""
        version = consts.PROTOCOL_VERSION
        self.assertIsInstance(version, tuple)
        self.assertEqual(len(version), 2)
        self.assertEqual(version, (5, 1))


if __name__ == '__main__':
    unittest.main()
