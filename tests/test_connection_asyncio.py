"""
Unit tests for Connection asyncio integration.

Tests verify enable/disable_asyncio_serving(), FD registration, and
event loop integration — against a real Connection instance obtained
via `async_connect` to an `AsyncioServer` running in a child process
(per `docs/DESIGN_NO_SAME_PROCESS_TESTS.md`).

Rewrite history
---------------
This file previously built a `Connection` on top of
`unittest.mock.Mock()` channel. At interpreter teardown the Mock
channel could not answer `Connection.__del__ → close() →
sync_request(HANDLE_CLOSE)` and every test deadlocked. The Mock-based
shortcut is not compatible with the current Connection lifecycle, so
each test now connects to a real AsyncioServer in a separate process
(the project's canonical test topology via `mp_asyncio_server`).
"""
import asyncio
import unittest

import rpyc_async as rpyc
from rpyc_async.core.protocol import Connection

from tests.support import mp_asyncio_server


class _QuietService(rpyc.Service):
    """Minimal service — all we need is a live RPyC endpoint."""


class TestConnectionAsyncio(unittest.TestCase):
    """Test Connection asyncio integration (real channel, child-proc server)."""

    @staticmethod
    def _run(coro):
        return asyncio.run(coro)

    def test_connection_has_asyncio_attributes(self):
        """After init, Connection exposes its asyncio wiring attributes."""
        async def body():
            with mp_asyncio_server(_QuietService) as port:
                conn = await rpyc.async_connect("127.0.0.1", port)
                try:
                    self.assertTrue(hasattr(conn, "_asyncio_loop"))
                    self.assertTrue(hasattr(conn, "_asyncio_enabled"))
                    self.assertTrue(hasattr(conn, "_loop_fd_registered"))
                    # async_connect auto-enables asyncio serving (see
                    # rpyc_async/core/async_connect.py commit 41b062e). So by
                    # the time we can `await` on conn, it IS enabled —
                    # this is the supported state.
                    self.assertTrue(conn._asyncio_enabled)
                    self.assertIs(conn._asyncio_loop, asyncio.get_running_loop())
                    self.assertTrue(conn._loop_fd_registered)
                finally:
                    await conn.aclose()

        self._run(body())

    def test_enable_asyncio_serving_is_idempotent(self):
        """Calling enable_asyncio_serving() a second time is a no-op."""
        async def body():
            with mp_asyncio_server(_QuietService) as port:
                conn = await rpyc.async_connect("127.0.0.1", port)
                try:
                    loop = asyncio.get_running_loop()
                    # Already enabled by async_connect.
                    self.assertTrue(conn._asyncio_enabled)
                    # Second call must not raise and must not re-register.
                    conn.enable_asyncio_serving(loop)
                    self.assertTrue(conn._asyncio_enabled)
                    self.assertIs(conn._asyncio_loop, loop)
                finally:
                    await conn.aclose()

        self._run(body())

    def test_enable_asyncio_serving_autodetects_loop(self):
        """enable_asyncio_serving() without loop= detects the running one."""
        async def body():
            with mp_asyncio_server(_QuietService) as port:
                conn = await rpyc.async_connect("127.0.0.1", port)
                try:
                    # Re-invoking is idempotent, and with loop=None it
                    # must detect the currently-running loop.
                    # First disable so we exercise the enable path on a
                    # real, live Connection.
                    conn.disable_asyncio_serving()
                    self.assertFalse(conn._asyncio_enabled)
                    conn.enable_asyncio_serving()  # no explicit loop
                    self.assertTrue(conn._asyncio_enabled)
                    self.assertIs(conn._asyncio_loop, asyncio.get_running_loop())
                finally:
                    await conn.aclose()

        self._run(body())

    def test_disable_asyncio_serving_resets_state(self):
        """disable_asyncio_serving() drops the FD registration and flags."""
        async def body():
            with mp_asyncio_server(_QuietService) as port:
                conn = await rpyc.async_connect("127.0.0.1", port)
                try:
                    self.assertTrue(conn._asyncio_enabled)
                    conn.disable_asyncio_serving()
                    self.assertFalse(conn._asyncio_enabled)
                    self.assertIsNone(conn._asyncio_loop)
                    self.assertFalse(conn._loop_fd_registered)
                finally:
                    # Re-enable before aclose so the final drain can
                    # send HANDLE_CLOSE over the live loop.
                    conn.enable_asyncio_serving()
                    await conn.aclose()

        self._run(body())

    def test_disable_asyncio_serving_is_idempotent(self):
        """Calling disable_asyncio_serving() twice is safe."""
        async def body():
            with mp_asyncio_server(_QuietService) as port:
                conn = await rpyc.async_connect("127.0.0.1", port)
                try:
                    conn.disable_asyncio_serving()
                    conn.disable_asyncio_serving()  # no-op on second call
                    self.assertFalse(conn._asyncio_enabled)
                finally:
                    conn.enable_asyncio_serving()
                    await conn.aclose()

        self._run(body())

    def test_enable_asyncio_serving_outside_loop_fails(self):
        """enable_asyncio_serving() outside a running loop raises RuntimeError.

        This is a pure code-path contract: the call has to refuse to
        operate when `asyncio.get_running_loop()` would fail and no
        loop was passed in. The Connection-object shape for this check
        doesn't matter — we only need *some* Connection. We obtain one
        via a real connect/disable cycle so Connection.close() at exit
        still has a valid channel to talk to.
        """
        # Note: this entire body runs OUTSIDE of asyncio.run() — the
        # point is exactly to have no running loop.
        with mp_asyncio_server(_QuietService) as port:
            conn = asyncio.run(rpyc.async_connect("127.0.0.1", port))
            try:
                # Take the conn offline from its previous loop so a
                # fresh enable_asyncio_serving() in this no-loop scope
                # is a real test.
                conn.disable_asyncio_serving()
                with self.assertRaises(RuntimeError) as ctx:
                    conn.enable_asyncio_serving()
                self.assertIn("event loop", str(ctx.exception).lower())
            finally:
                # aclose() needs a loop; run it in a fresh one.
                async def _close():
                    conn.enable_asyncio_serving()
                    await conn.aclose()
                asyncio.run(_close())

    def test_close_cleans_up_asyncio(self):
        """aclose() must leave _asyncio_enabled and fd registration cleared."""
        async def body():
            with mp_asyncio_server(_QuietService) as port:
                conn = await rpyc.async_connect("127.0.0.1", port)
                self.assertTrue(conn._asyncio_enabled)
                await conn.aclose()
                self.assertFalse(conn._asyncio_enabled)
                self.assertFalse(conn._loop_fd_registered)

        self._run(body())

    def test_connection_class_exposes_wiring_methods(self):
        """Pure shape-check on the class — no Connection instance needed."""
        self.assertTrue(callable(getattr(Connection, "enable_asyncio_serving", None)))
        self.assertTrue(callable(getattr(Connection, "disable_asyncio_serving", None)))
        self.assertTrue(callable(getattr(Connection, "close", None)))
        self.assertTrue(callable(getattr(Connection, "aclose", None)))


if __name__ == "__main__":
    unittest.main()
