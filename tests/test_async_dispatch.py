"""
Unit tests for async dispatch pipeline.

Two layers:

1. Shape checks on the Connection CLASS (no instance required). They
   fail fast if someone renames/removes `_dispatch_request_async`,
   `_is_async_handler`, or `_needs_async_dispatch`, or if their
   sync/async nature flips.

2. Behavioural checks via a real AsyncioServer in a child process
   (per `docs/DESIGN_NO_SAME_PROCESS_TESTS.md`). They assert the
   visible consequence of `_dispatch_request_async` — that an
   `async def exposed_*` round-trips correctly and that exceptions
   propagate.

Rewrite history
---------------
The previous version built a `Connection` on top of a
`unittest.mock.Mock()` channel and mocked `_send` / `_cleanup`. At
process teardown Connection's `__del__ → close() →
sync_request(HANDLE_CLOSE)` could not finish against a Mock and the
test process deadlocked. Mock-of-the-wire is incompatible with the
current Connection lifecycle; the assertions are now either
reflection on the class itself or real-server observations.
"""
import asyncio
import inspect
import unittest

import rpyc_async as rpyc
from rpyc_async.core.protocol import Connection
from rpyc_async.core import consts

from tests.support import mp_asyncio_server


# ---------------------------------------------------------------------------
# Layer 1: class-level shape checks. No Connection instance needed.
# ---------------------------------------------------------------------------


class TestAsyncDispatchShape(unittest.TestCase):
    """Shape / contract checks on Connection class."""

    def test_has_is_async_handler_method(self):
        self.assertTrue(hasattr(Connection, "_is_async_handler"))
        self.assertTrue(callable(Connection._is_async_handler))

    def test_has_needs_async_dispatch_method(self):
        self.assertTrue(hasattr(Connection, "_needs_async_dispatch"))
        self.assertTrue(callable(Connection._needs_async_dispatch))

    def test_dispatch_request_async_is_coroutine(self):
        self.assertTrue(hasattr(Connection, "_dispatch_request_async"))
        self.assertTrue(callable(Connection._dispatch_request_async))
        self.assertTrue(
            inspect.iscoroutinefunction(Connection._dispatch_request_async)
        )

    def test_handler_registry_contains_async_handlers(self):
        """The class-level `_request_handlers()` classmethod returns a
        dict that includes HANDLE_ASYNC_CALL / HANDLE_ASYNC_CALLATTR.

        `_HANDLERS` itself is populated per-instance (`protocol.py:189`,
        `self._HANDLERS = self._request_handlers()`); `_request_handlers()`
        is the class-level source of truth. No instance / no I/O.
        """
        handlers = Connection._request_handlers()
        self.assertIsInstance(handlers, dict)
        self.assertIn(consts.HANDLE_ASYNC_CALL, handlers)
        self.assertIn(consts.HANDLE_ASYNC_CALLATTR, handlers)
        # Async-call handler must be a coroutine function.
        self.assertTrue(
            inspect.iscoroutinefunction(handlers[consts.HANDLE_ASYNC_CALL])
        )


# ---------------------------------------------------------------------------
# Layer 2: behavioural end-to-end against a real AsyncioServer.
# ---------------------------------------------------------------------------


class _DispatchService(rpyc.Service):
    """Service with both sync and async exposed methods, plus a
    deliberately-raising one for exception-propagation checks."""

    def exposed_sync_echo(self, value):
        return ("sync", value)

    async def exposed_async_echo(self, value):
        return ("async", value)

    async def exposed_async_raises(self):
        raise ValueError("deliberate")


class TestAsyncDispatchE2E(unittest.TestCase):
    """Async dispatch behavioural round-trip (client-server in
    different processes)."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def test_async_def_method_round_trips(self):
        """An `async def exposed_*` round-trips and returns the value."""
        async def body():
            with mp_asyncio_server(_DispatchService) as port:
                conn = await rpyc.async_connect("127.0.0.1", port)
                try:
                    result = await conn.root.async_echo("hello")
                    # result is a netref-tuple; compare its pieces
                    # through async_()-wrapped indexing OR unpack via
                    # a round-trip serialize. Simplest: stringify.
                    self.assertEqual(
                        tuple(await conn.root.async_echo("hello")),
                        ("async", "hello"),
                    )
                    _ = result
                finally:
                    await conn.aclose()

        self._run(body())

    def test_sync_def_method_round_trips(self):
        """A plain sync `exposed_*` method also round-trips via
        rpyc.async_() on the netref attribute."""
        async def body():
            with mp_asyncio_server(_DispatchService) as port:
                conn = await rpyc.async_connect("127.0.0.1", port)
                try:
                    sync_echo = rpyc.async_(conn.root.sync_echo)
                    result = await sync_echo("hi")
                    self.assertEqual(tuple(result), ("sync", "hi"))
                finally:
                    await conn.aclose()

        self._run(body())

    def test_async_def_exception_propagates(self):
        """An exception in an `async def exposed_*` propagates to
        the client through the async-reply path."""
        async def body():
            with mp_asyncio_server(_DispatchService) as port:
                conn = await rpyc.async_connect("127.0.0.1", port)
                try:
                    with self.assertRaises(ValueError) as ctx:
                        await conn.root.async_raises()
                    self.assertIn("deliberate", str(ctx.exception))
                finally:
                    await conn.aclose()

        self._run(body())


if __name__ == "__main__":
    unittest.main()
