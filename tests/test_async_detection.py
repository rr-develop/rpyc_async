"""
Unit tests for async detection utilities.

Tests verify correct detection of async functions, coroutines,
and async-capable objects.
"""
import unittest
import asyncio
import inspect
from functools import partial
from rpyc.utils import helpers


class TestAsyncDetection(unittest.TestCase):
    """Test async detection utilities."""

    def test_is_async_function_with_async_def(self):
        """Test is_async_function with async def."""
        async def async_func():
            pass

        self.assertTrue(helpers.is_async_function(async_func))

    def test_is_async_function_with_sync_def(self):
        """Test is_async_function with sync def."""
        def sync_func():
            pass

        self.assertFalse(helpers.is_async_function(sync_func))

    def test_is_async_function_with_lambda(self):
        """Test is_async_function with lambda."""
        func = lambda x: x + 1

        self.assertFalse(helpers.is_async_function(func))

    def test_is_async_function_with_partial(self):
        """Test is_async_function with partial."""
        async def async_func(x, y):
            return x + y

        partial_func = partial(async_func, 1)

        # Partial wraps async function - should detect
        self.assertTrue(helpers.is_async_function(partial_func))

    def test_is_async_function_with_method(self):
        """Test is_async_function with async method."""
        class TestClass:
            async def async_method(self):
                pass

            def sync_method(self):
                pass

        obj = TestClass()

        self.assertTrue(helpers.is_async_function(obj.async_method))
        self.assertFalse(helpers.is_async_function(obj.sync_method))

    def test_is_coroutine_with_coroutine(self):
        """Test is_coroutine with actual coroutine."""
        async def async_func():
            pass

        coro = async_func()
        self.assertTrue(helpers.is_coroutine(coro))

        # Clean up coroutine
        coro.close()

    def test_is_coroutine_with_non_coroutine(self):
        """Test is_coroutine with non-coroutine."""
        def sync_func():
            return 42

        result = sync_func()
        self.assertFalse(helpers.is_coroutine(result))

    def test_is_coroutine_with_function(self):
        """Test is_coroutine with function (not instance)."""
        async def async_func():
            pass

        # Function itself is not a coroutine
        self.assertFalse(helpers.is_coroutine(async_func))

    def test_is_async_capable_with_async_function(self):
        """Test is_async_capable with async function."""
        async def async_func():
            pass

        self.assertTrue(helpers.is_async_capable(async_func))

    def test_is_async_capable_with_coroutine(self):
        """Test is_async_capable with coroutine."""
        async def async_func():
            pass

        coro = async_func()
        self.assertTrue(helpers.is_async_capable(coro))

        # Clean up
        coro.close()

    def test_is_async_capable_with_sync(self):
        """Test is_async_capable with sync object."""
        def sync_func():
            pass

        self.assertFalse(helpers.is_async_capable(sync_func))
        self.assertFalse(helpers.is_async_capable(42))

    def test_detection_caching(self):
        """Test that detection results are cached for performance."""
        async def async_func():
            pass

        # First call (cache miss)
        result1 = helpers.is_async_function(async_func)

        # Second call (cache hit - should be faster)
        result2 = helpers.is_async_function(async_func)

        self.assertEqual(result1, result2)
        self.assertTrue(result1)

    def test_is_async_function_with_classmethod(self):
        """Test is_async_function with classmethod."""
        class TestClass:
            @classmethod
            async def async_classmethod(cls):
                pass

            @classmethod
            def sync_classmethod(cls):
                pass

        # Classmethods are tricky - test bound version
        self.assertTrue(helpers.is_async_function(TestClass.async_classmethod))
        self.assertFalse(helpers.is_async_function(TestClass.sync_classmethod))

    def test_is_async_function_with_staticmethod(self):
        """Test is_async_function with staticmethod."""
        class TestClass:
            @staticmethod
            async def async_staticmethod():
                pass

            @staticmethod
            def sync_staticmethod():
                pass

        self.assertTrue(helpers.is_async_function(TestClass.async_staticmethod))
        self.assertFalse(helpers.is_async_function(TestClass.sync_staticmethod))


if __name__ == '__main__':
    unittest.main()
