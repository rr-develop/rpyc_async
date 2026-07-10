"""
Unit tests for async handler implementations.

Tests verify that async handlers can execute async functions,
await coroutines, and handle sync functions correctly.
"""
import unittest
import asyncio
import inspect
from unittest.mock import Mock, MagicMock
from rpyc_async.core import async_handlers


class TestAsyncHandlers(unittest.TestCase):
    """Test async handler functions."""

    def setUp(self):
        """Set up test fixtures."""
        self.conn = Mock()
        self.conn._box = lambda x: x
        self.conn._unbox = lambda x: x

    def test_handle_async_call_with_async_function(self):
        """Test _handle_async_call with async function."""
        async def async_func(x, y):
            await asyncio.sleep(0.001)
            return x + y

        # Execute handler
        result = asyncio.run(
            async_handlers._handle_async_call(self.conn, async_func, (3, 4), ())
        )

        self.assertEqual(result, 7)

    def test_handle_async_call_with_coroutine(self):
        """Test _handle_async_call with pre-created coroutine."""
        async def async_func(x):
            await asyncio.sleep(0.001)
            return x * 2

        # Create coroutine
        coro = async_func(5)
        self.assertTrue(inspect.iscoroutine(coro))

        # Execute handler
        result = asyncio.run(
            async_handlers._handle_async_call(self.conn, coro, (), ())
        )

        self.assertEqual(result, 10)

    def test_handle_async_call_with_sync_function(self):
        """Test _handle_async_call with sync function (fallback)."""
        def sync_func(x, y):
            return x * y

        # Execute handler
        result = asyncio.run(
            async_handlers._handle_async_call(self.conn, sync_func, (3, 4), ())
        )

        self.assertEqual(result, 12)

    def test_handle_async_call_with_kwargs(self):
        """Test _handle_async_call with keyword arguments."""
        async def async_func(a, b, c=10):
            await asyncio.sleep(0.001)
            return a + b + c

        # Execute with kwargs as list of tuples
        result = asyncio.run(
            async_handlers._handle_async_call(
                self.conn, async_func, (1, 2), [('c', 3)]
            )
        )

        self.assertEqual(result, 6)

    def test_handle_async_call_exception_propagation(self):
        """Test that exceptions are properly propagated."""
        async def failing_func():
            await asyncio.sleep(0.001)
            raise ValueError("Test error")

        with self.assertRaises(ValueError) as ctx:
            asyncio.run(
                async_handlers._handle_async_call(self.conn, failing_func, (), ())
            )

        self.assertEqual(str(ctx.exception), "Test error")

    def test_handle_async_call_sync_returning_coroutine(self):
        """Test sync function that returns a coroutine (should await it)."""
        async def inner_async():
            await asyncio.sleep(0.001)
            return 42

        def sync_wrapper():
            # Sync function that returns coroutine
            return inner_async()

        result = asyncio.run(
            async_handlers._handle_async_call(self.conn, sync_wrapper, (), ())
        )

        self.assertEqual(result, 42)

    def test_handle_async_callattr(self):
        """Test _handle_async_callattr."""
        class TestObj:
            async def async_method(self, x):
                await asyncio.sleep(0.001)
                return x * 3

        obj = TestObj()

        result = asyncio.run(
            async_handlers._handle_async_callattr(
                self.conn, obj, 'async_method', (5,), ()
            )
        )

        self.assertEqual(result, 15)

    def test_handle_async_callattr_attribute_error(self):
        """Test _handle_async_callattr with non-existent attribute."""
        obj = object()

        with self.assertRaises(AttributeError):
            asyncio.run(
                async_handlers._handle_async_callattr(
                    self.conn, obj, 'nonexistent', (), ()
                )
            )

    def test_register_async_handlers(self):
        """Test that register_async_handlers adds handlers to connection."""
        from rpyc_async.core import consts

        # Create mock connection
        conn = Mock()
        conn._HANDLERS = {}

        # Register handlers
        async_handlers.register_async_handlers(conn)

        # Verify handlers registered
        self.assertIn(consts.HANDLE_ASYNC_CALL, conn._HANDLERS)
        self.assertIn(consts.HANDLE_ASYNC_CALLATTR, conn._HANDLERS)

        # Verify they are coroutine functions
        self.assertTrue(
            inspect.iscoroutinefunction(
                conn._HANDLERS[consts.HANDLE_ASYNC_CALL]
            )
        )
        self.assertTrue(
            inspect.iscoroutinefunction(
                conn._HANDLERS[consts.HANDLE_ASYNC_CALLATTR]
            )
        )


if __name__ == '__main__':
    unittest.main()
