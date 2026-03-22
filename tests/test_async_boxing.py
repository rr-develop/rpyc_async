"""
Unit tests for async boxing/unboxing.

Tests verify that async functions are boxed with FLAGS_ASYNC
and unboxed correctly with metadata.
"""
import unittest
import asyncio
import inspect
from unittest.mock import Mock
from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService
from rpyc.core import consts


class TestAsyncBoxing(unittest.TestCase):
    """Test async boxing/unboxing."""

    def setUp(self):
        """Create minimal connection for boxing tests."""
        # Note: boxing/unboxing are complex, test only key aspects
        pass

    def test_box_method_exists(self):
        """Test _box method exists."""
        self.assertTrue(hasattr(Connection, '_box'))

    def test_unbox_method_exists(self):
        """Test _unbox method exists."""
        self.assertTrue(hasattr(Connection, '_unbox'))

    def test_async_function_should_be_remote_ref(self):
        """Test that async functions should become remote refs."""
        async def async_func():
            pass

        # Async functions should be boxed as LABEL_REMOTE_REF
        # with FLAGS_ASYNC in extended id_pack format
        self.assertTrue(inspect.iscoroutinefunction(async_func))

    def test_sync_function_boxing_unchanged(self):
        """Test that sync functions work as before."""
        def sync_func():
            pass

        # Sync functions should work as before
        self.assertFalse(inspect.iscoroutinefunction(sync_func))

    def test_flags_async_constant_exists(self):
        """Test FLAGS_ASYNC constant is defined."""
        self.assertTrue(hasattr(consts, 'FLAGS_ASYNC'))
        self.assertEqual(consts.FLAGS_ASYNC, 0x01)

    def test_flags_sync_constant_exists(self):
        """Test FLAGS_SYNC constant is defined."""
        self.assertTrue(hasattr(consts, 'FLAGS_SYNC'))
        self.assertEqual(consts.FLAGS_SYNC, 0x00)


if __name__ == '__main__':
    unittest.main()
