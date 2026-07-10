"""
Policy tests: `AsyncioServer` clients MUST use `rpyc.async_connect`,
not synchronous `rpyc.connect`.

These tests are the TDD red-phase for the design in
``docs/DESIGN_ASYNC_CONNECT_POLICY.md``:

1. ``rpyc.async_connect`` exists at the top level of the package.
2. After ``await rpyc.async_connect(...)``, ``conn.root`` is ready without
   any ``sync_request`` — the eager handshake is real.
3. Calling ``conn.sync_request(...)`` from inside the loop that serves
   the connection raises a clear ``RuntimeError`` naming the async
   alternative.
4. Sync callers (``rpyc.connect`` + a worker thread) keep working.
5. ``conn.aclose()`` exists as an async close path that does NOT go
   through blocking ``sync_request``.
6. The dead ``AsyncioStream`` class has been removed from
   ``rpyc.core.async_connect``.

The server used here is the real ``AsyncioServer`` running in a child
process, same style as ``tests/test_asyncio_server_reconnection.py``.
"""
import asyncio
import inspect
import time
import unittest
from multiprocessing import Process, Queue
from unittest import mock

import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer
from tests.support import get_free_port


# ---- server process entry point ------------------------------------------

def _run_policy_server(port, ready_queue):
    class _PolicySvc(rpyc.Service):
        def exposed_ping(self, x):
            return f"pong:{x}"

        async def exposed_apipe(self, x):
            await asyncio.sleep(0.005)
            return x * 2

    async def _main():
        server = AsyncioServer(
            _PolicySvc,
            hostname="localhost",
            port=port,
            protocol_config={"allow_all_attrs": True},
        )
        await server.start()
        ready_queue.put("ready")
        try:
            await asyncio.Event().wait()
        finally:
            await server.close()

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass


class _PolicyServerMixin:
    """Mixin: start a real AsyncioServer in a child process for each test."""

    def setUp(self):
        self.port = get_free_port()
        self.ready_queue = Queue()
        self.server_proc = Process(
            target=_run_policy_server,
            args=(self.port, self.ready_queue),
            daemon=True,
        )
        self.server_proc.start()
        signal = self.ready_queue.get(timeout=5.0)
        assert signal == "ready"
        time.sleep(0.1)

    def tearDown(self):
        if self.server_proc.is_alive():
            self.server_proc.terminate()
            self.server_proc.join(timeout=2.0)
            if self.server_proc.is_alive():
                self.server_proc.kill()
                self.server_proc.join(timeout=1.0)


# ---- 1. top-level export --------------------------------------------------

class TestAsyncConnectExport(unittest.TestCase):
    def test_rpyc_has_async_connect(self):
        self.assertTrue(
            hasattr(rpyc, "async_connect"),
            "rpyc.async_connect is not exported. AsyncioServer clients "
            "must have an obvious top-level entry point — add it in "
            "rpyc/__init__.py.",
        )

    def test_rpyc_async_connect_is_coroutine_function(self):
        self.assertTrue(
            hasattr(rpyc, "async_connect"), "rpyc.async_connect missing"
        )
        self.assertTrue(
            inspect.iscoroutinefunction(rpyc.async_connect),
            "rpyc.async_connect must be an `async def` coroutine function.",
        )


# ---- 2. eager handshake ---------------------------------------------------

class TestEagerHandshake(_PolicyServerMixin, unittest.TestCase):
    def test_remote_root_ready_after_async_connect(self):
        async def _go():
            conn = await rpyc.async_connect(
                "127.0.0.1", self.port, timeout=5.0
            )
            try:
                self.assertIsNotNone(
                    conn._remote_root,
                    "Eager handshake not done: _remote_root is None. "
                    "async_connect must pre-fetch root via async_request "
                    "so that conn.root does not block the loop later.",
                )
            finally:
                # Use aclose if available; fall back to close for first run.
                if hasattr(conn, "aclose"):
                    await conn.aclose()
                else:
                    conn.close()

        asyncio.run(_go())

    def test_conn_root_does_not_trigger_sync_request(self):
        async def _go():
            conn = await rpyc.async_connect(
                "127.0.0.1", self.port, timeout=5.0
            )
            try:
                # After eager handshake, conn.root must NOT call sync_request.
                with mock.patch.object(
                    type(conn),
                    "sync_request",
                    side_effect=AssertionError(
                        "conn.root called sync_request — eager handshake is "
                        "not actually pre-fetching _remote_root."
                    ),
                ):
                    root = conn.root
                    self.assertIsNotNone(root)
            finally:
                if hasattr(conn, "aclose"):
                    await conn.aclose()
                else:
                    conn.close()

        asyncio.run(_go())


# ---- 3. sync_request guard -----------------------------------------------

class TestSyncRequestGuard(_PolicyServerMixin, unittest.TestCase):
    def test_sync_request_raises_for_user_rpc_from_serving_loop(self):
        """User-level RPC (HANDLE_CALL) from the serving loop must be refused.

        Protocol-level fast-path handlers (HANDLE_INSPECT, HANDLE_GETATTR,
        etc.) are NOT refused — netref construction needs them and they
        are cheap localhost hops. See Connection.sync_request for the
        exact policy.
        """
        from rpyc_async.core import consts

        async def _go():
            conn = await rpyc.async_connect(
                "127.0.0.1", self.port, timeout=5.0
            )
            try:
                with self.assertRaises(RuntimeError) as ctx:
                    # HANDLE_CALL = user-level RPC; guard must fire.
                    conn.sync_request(consts.HANDLE_CALL, conn._remote_root, (), ())
                msg = str(ctx.exception).lower()
                self.assertIn("async", msg)
                self.assertTrue(
                    "async_request" in msg or "aclose" in msg or "async_" in msg,
                    f"Guard error must suggest async alternative; got: {ctx.exception!r}",
                )
            finally:
                await conn.aclose()

        asyncio.run(_go())

    def test_sync_request_allowed_in_threaded_sync_client(self):
        """Sync callers must NOT be affected by the new guard.

        A traditional sync client runs outside any asyncio loop; sync_request
        must work normally (this is exercised via rpyc.connect + conn.ping
        from the test thread, which has no running loop).
        """
        conn = rpyc.connect("127.0.0.1", self.port)
        try:
            # Trivial sync call — must succeed, i.e. not raise the guard.
            result = conn.root.ping("x")
            self.assertEqual(result, "pong:x")
        finally:
            conn.close()


# ---- 4. async close path --------------------------------------------------

class TestAsyncClose(_PolicyServerMixin, unittest.TestCase):
    def test_aclose_closes_connection_without_blocking(self):
        async def _go():
            conn = await rpyc.async_connect(
                "127.0.0.1", self.port, timeout=5.0
            )
            self.assertFalse(conn.closed)

            self.assertTrue(
                hasattr(conn, "aclose"),
                "Connection.aclose() is required — an async close path "
                "that does NOT use sync_request.",
            )
            self.assertTrue(
                inspect.iscoroutinefunction(type(conn).aclose),
                "Connection.aclose must be an `async def` coroutine function.",
            )

            # aclose itself must not reach sync_request on the running loop.
            original_sync = type(conn).sync_request

            calls = []

            def tracking_sync_request(self, *a, **kw):
                calls.append(a)
                return original_sync(self, *a, **kw)

            with mock.patch.object(
                type(conn), "sync_request", tracking_sync_request
            ):
                await conn.aclose()

            self.assertTrue(conn.closed)
            self.assertEqual(
                calls,
                [],
                f"aclose() must not route through sync_request; got {calls!r}",
            )

        asyncio.run(_go())


# ---- 5. dead code removed -------------------------------------------------

class TestDeadCodeRemoved(unittest.TestCase):
    def test_asyncio_stream_class_is_gone(self):
        import rpyc_async.core.async_connect as ac
        self.assertFalse(
            hasattr(ac, "AsyncioStream"),
            "AsyncioStream is dead code: async_connect() uses SocketStream "
            "(with asyncio serving re-routing reads via add_reader). The "
            "AsyncioStream class must be removed so readers don't get the "
            "wrong mental model of how the async I/O path works.",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
