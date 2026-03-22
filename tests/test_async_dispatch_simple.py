"""
Simplified tests for async dispatch pipeline.

Tests verify methods exist without full Connection initialization.
"""
import unittest
import inspect
from rpyc.core.protocol import Connection
from rpyc.core import consts


class TestAsyncDispatchSimple(unittest.TestCase):
    """Simplified async dispatch tests."""

    def test_connection_has_is_async_handler_method(self):
        """Test _is_async_handler method exists."""
        self.assertTrue(hasattr(Connection, '_is_async_handler'))

    def test_connection_has_needs_async_dispatch_method(self):
        """Test _needs_async_dispatch method exists."""
        self.assertTrue(hasattr(Connection, '_needs_async_dispatch'))

    def test_connection_has_dispatch_request_async_method(self):
        """Test _dispatch_request_async method exists."""
        self.assertTrue(hasattr(Connection, '_dispatch_request_async'))

    def test_dispatch_request_async_is_coroutine_function(self):
        """Test _dispatch_request_async is async."""
        method = getattr(Connection, '_dispatch_request_async')
        self.assertTrue(inspect.iscoroutinefunction(method))

    def test_async_handlers_registered(self):
        """Test async handlers are in _request_handlers."""
        handlers = Connection._request_handlers()

        self.assertIn(consts.HANDLE_ASYNC_CALL, handlers)
        self.assertIn(consts.HANDLE_ASYNC_CALLATTR, handlers)

    def test_async_handlers_are_coroutine_functions(self):
        """Test async handlers are actually async."""
        handlers = Connection._request_handlers()

        async_call_handler = handlers[consts.HANDLE_ASYNC_CALL]
        async_callattr_handler = handlers[consts.HANDLE_ASYNC_CALLATTR]

        self.assertTrue(inspect.iscoroutinefunction(async_call_handler))
        self.assertTrue(inspect.iscoroutinefunction(async_callattr_handler))


if __name__ == '__main__':
    unittest.main()
