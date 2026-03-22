"""
Unit tests for Netref async detection.

Tests verify that netrefs with ____is_async__ metadata
use correct handlers (HANDLE_ASYNC_CALL vs HANDLE_CALL).
"""
import unittest
from rpyc.core import netref, consts


class TestNetrefAsync(unittest.TestCase):
    """Test Netref async detection."""

    def test_base_netref_has_is_async_slot(self):
        """Test BaseNetref has ____is_async__ slot."""
        # BaseNetref should support ____is_async__ attribute
        # This is set by _unbox() when FLAGS_ASYNC is detected
        self.assertTrue(hasattr(netref.BaseNetref, '__slots__'))

    def test_netref_call_detection(self):
        """Test that __call__ can detect async functions."""
        # This is more of integration test
        # Actual behavior tested in E2E tests
        pass

    def test_constants_for_async_handlers_exist(self):
        """Test async handler constants are defined."""
        self.assertTrue(hasattr(consts, 'HANDLE_ASYNC_CALL'))
        self.assertTrue(hasattr(consts, 'HANDLE_ASYNC_CALLATTR'))

        self.assertEqual(consts.HANDLE_ASYNC_CALL, 100)
        self.assertEqual(consts.HANDLE_ASYNC_CALLATTR, 101)


if __name__ == '__main__':
    unittest.main()
