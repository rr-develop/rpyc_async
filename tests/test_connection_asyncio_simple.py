"""
Simplified unit tests for Connection asyncio integration.

These are shape-checks against the Connection class: the wiring
methods must exist, be callable, and advertise the correct coroutine
signature where applicable. Behavioural tests for the same methods
live in `tests/test_connection_asyncio.py` (real AsyncioServer in a
child process).

Rewrite history
---------------
Previous version built Connection on a `unittest.mock.Mock()`
channel; at interpreter teardown the Mock could not respond to
`Connection.close() → sync_request(HANDLE_CLOSE)` and every test
deadlocked. The shape-level assertions have been lifted to operate
directly on the `Connection` *class*, which removes the need for a
channel at all.
"""
import inspect
import unittest

from rpyc_async.core.protocol import Connection


class TestConnectionAsyncioSimple(unittest.TestCase):
    """Shape-level checks on the Connection class."""

    def test_has_enable_asyncio_serving_method(self):
        self.assertTrue(hasattr(Connection, "enable_asyncio_serving"))
        self.assertTrue(callable(Connection.enable_asyncio_serving))
        # It's a plain (synchronous) method — serving IS the loop
        # interaction, not a coroutine. Keep this explicit so an
        # accidental `async def` on the method would fail this test.
        self.assertFalse(
            inspect.iscoroutinefunction(Connection.enable_asyncio_serving),
            "enable_asyncio_serving must be sync (it interacts with a "
            "loop via loop.add_reader, not via await)"
        )

    def test_has_disable_asyncio_serving_method(self):
        self.assertTrue(hasattr(Connection, "disable_asyncio_serving"))
        self.assertTrue(callable(Connection.disable_asyncio_serving))
        self.assertFalse(
            inspect.iscoroutinefunction(Connection.disable_asyncio_serving)
        )

    def test_has_aclose_coroutine(self):
        """Connection.aclose must be a coroutine — it's the async close path."""
        self.assertTrue(hasattr(Connection, "aclose"))
        self.assertTrue(inspect.iscoroutinefunction(Connection.aclose))

    def test_has_async_request_with_ack_coroutine(self):
        """HANDLE_DEL-ack path must be async."""
        self.assertTrue(hasattr(Connection, "_async_request_with_ack"))
        self.assertTrue(
            inspect.iscoroutinefunction(Connection._async_request_with_ack)
        )

    def test_has_process_pending_deletions_coroutine(self):
        """Background cleanup path must be async."""
        self.assertTrue(hasattr(Connection, "_process_pending_deletions"))
        self.assertTrue(
            inspect.iscoroutinefunction(Connection._process_pending_deletions)
        )


if __name__ == "__main__":
    unittest.main()
