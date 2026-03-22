"""
Unit tests for async dispatch pipeline.

Tests verify async dispatch routing, execution, and reply handling.
"""
import unittest
import asyncio
import inspect
from unittest.mock import Mock, MagicMock, AsyncMock
from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService
from rpyc.core import consts


class TestAsyncDispatch(unittest.TestCase):
    """Test async dispatch pipeline."""

    def setUp(self):
        """Create mock connection."""
        service = VoidService()
        channel = Mock()
        channel.fileno.return_value = 999
        channel.poll.return_value = False
        channel.closed = False

        self.conn = Connection(service, channel)
        self.conn._cleanup = Mock()
        self.conn._send = Mock()

    def test_has_is_async_handler_method(self):
        """Test that _is_async_handler method exists."""
        self.assertTrue(hasattr(self.conn, '_is_async_handler'))
        self.assertTrue(callable(self.conn._is_async_handler))

    def test_has_needs_async_dispatch_method(self):
        """Test that _needs_async_dispatch method exists."""
        self.assertTrue(hasattr(self.conn, '_needs_async_dispatch'))
        self.assertTrue(callable(self.conn._needs_async_dispatch))

    def test_has_dispatch_request_async_method(self):
        """Test that _dispatch_request_async method exists."""
        self.assertTrue(hasattr(self.conn, '_dispatch_request_async'))
        self.assertTrue(callable(self.conn._dispatch_request_async))

    def test_dispatch_request_async_is_coroutine_function(self):
        """Test that _dispatch_request_async is async function."""
        self.assertTrue(
            inspect.iscoroutinefunction(self.conn._dispatch_request_async)
        )

    def test_is_async_handler_with_async_call(self):
        """Test _is_async_handler recognizes HANDLE_ASYNC_CALL."""
        result = self.conn._is_async_handler(consts.HANDLE_ASYNC_CALL)
        self.assertTrue(result)

    def test_is_async_handler_with_async_callattr(self):
        """Test _is_async_handler recognizes HANDLE_ASYNC_CALLATTR."""
        result = self.conn._is_async_handler(consts.HANDLE_ASYNC_CALLATTR)
        self.assertTrue(result)

    def test_is_async_handler_with_sync_handler(self):
        """Test _is_async_handler returns False for sync handlers."""
        result = self.conn._is_async_handler(consts.HANDLE_CALL)
        self.assertFalse(result)

    def test_needs_async_dispatch_with_async_request(self):
        """Test _needs_async_dispatch detects MSG_ASYNC_REQUEST."""
        result = self.conn._needs_async_dispatch(
            consts.MSG_ASYNC_REQUEST,
            (consts.HANDLE_CALL, ())
        )
        self.assertTrue(result)

    def test_needs_async_dispatch_with_async_handler(self):
        """Test _needs_async_dispatch detects async handler in MSG_REQUEST."""
        result = self.conn._needs_async_dispatch(
            consts.MSG_REQUEST,
            (consts.HANDLE_ASYNC_CALL, ())
        )
        self.assertTrue(result)

    def test_needs_async_dispatch_with_sync_request(self):
        """Test _needs_async_dispatch returns False for sync."""
        result = self.conn._needs_async_dispatch(
            consts.MSG_REQUEST,
            (consts.HANDLE_CALL, ())
        )
        self.assertFalse(result)

    def test_dispatch_request_async_execution(self):
        """Test _dispatch_request_async can execute async handler."""
        async def test():
            # Register mock async handler
            async def mock_handler(conn, *args):
                await asyncio.sleep(0.001)
                return "async_result"

            self.conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = mock_handler

            # Execute async dispatch
            await self.conn._dispatch_request_async(
                seq=1,
                raw_args=(consts.HANDLE_ASYNC_CALL, ())
            )

            # Verify reply was sent
            self.conn._send.assert_called_once()
            call_args = self.conn._send.call_args
            self.assertEqual(call_args[0][0], consts.MSG_ASYNC_REPLY)
            self.assertEqual(call_args[0][1], 1)  # seq

        asyncio.run(test())

    def test_dispatch_request_async_with_sync_handler(self):
        """Test _dispatch_request_async works with sync handler too."""
        async def test():
            # Register sync handler
            def sync_handler(conn, *args):
                return "sync_result"

            self.conn._HANDLERS[999] = sync_handler

            # Execute async dispatch with sync handler
            await self.conn._dispatch_request_async(
                seq=2,
                raw_args=(999, ())
            )

            # Verify reply was sent
            self.assertTrue(self.conn._send.called)

        asyncio.run(test())

    def test_dispatch_request_async_exception_handling(self):
        """Test _dispatch_request_async sends exceptions as MSG_ASYNC_EXCEPTION."""
        async def test():
            # Register failing handler
            async def failing_handler(conn, *args):
                raise ValueError("test error")

            self.conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = failing_handler

            # Execute async dispatch
            await self.conn._dispatch_request_async(
                seq=3,
                raw_args=(consts.HANDLE_ASYNC_CALL, ())
            )

            # Verify exception was sent
            self.conn._send.assert_called_once()
            call_args = self.conn._send.call_args
            self.assertEqual(call_args[0][0], consts.MSG_ASYNC_EXCEPTION)
            self.assertEqual(call_args[0][1], 3)  # seq

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
