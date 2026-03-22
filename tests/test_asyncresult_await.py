"""
Unit tests for AsyncResult.__await__() implementation.

Tests verify that AsyncResult can be awaited in async context.
"""
import unittest
import asyncio
from unittest.mock import Mock
from rpyc.core.async_ import AsyncResult


class TestAsyncResultAwait(unittest.TestCase):
    """Test AsyncResult.__await__() method."""

    def test_asyncresult_has_await_method(self):
        """Test that AsyncResult has __await__ method."""
        conn = Mock()
        async_res = AsyncResult(conn)

        self.assertTrue(hasattr(async_res, '__await__'))
        self.assertTrue(callable(async_res.__await__))

    def test_await_with_ready_result(self):
        """Test awaiting AsyncResult that's already ready."""
        async def test():
            conn = Mock()
            async_res = AsyncResult(conn)

            # Set result immediately
            async_res(False, 42)  # is_exc=False, obj=42

            # Await should return immediately
            result = await async_res
            self.assertEqual(result, 42)

        asyncio.run(test())

    def test_await_with_ready_exception(self):
        """Test awaiting AsyncResult with exception."""
        async def test():
            conn = Mock()
            async_res = AsyncResult(conn)

            # Set exception
            exc = ValueError("test error")
            async_res(True, exc)  # is_exc=True

            # Await should raise exception
            with self.assertRaises(ValueError) as ctx:
                await async_res

            self.assertEqual(str(ctx.exception), "test error")

        asyncio.run(test())

    def test_await_with_pending_result(self):
        """Test awaiting AsyncResult that becomes ready later."""
        async def test():
            conn = Mock()
            async_res = AsyncResult(conn)

            # Simulate async result arriving after delay
            async def set_result_later():
                await asyncio.sleep(0.01)
                async_res(False, "delayed_result")

            # Start task to set result
            asyncio.create_task(set_result_later())

            # Await should wait for result
            result = await async_res
            self.assertEqual(result, "delayed_result")

        asyncio.run(test())

    def test_await_returns_awaitable(self):
        """Test that __await__ returns proper awaitable."""
        conn = Mock()
        async_res = AsyncResult(conn)

        # Set result
        async_res(False, "test")

        # __await__() should return awaitable (iterator)
        awaitable = async_res.__await__()
        self.assertTrue(hasattr(awaitable, '__iter__'))
        self.assertTrue(hasattr(awaitable, '__next__'))


if __name__ == '__main__':
    unittest.main()
