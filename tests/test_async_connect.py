"""
Tests for the ``async_connect`` module.

Verifies that ``async_connect`` provides fully async, non-blocking socket
connections for RPyC talking to an ``AsyncioServer``. The formerly-tested
``AsyncioStream`` shim is gone (dead code — see the design document
``docs/DESIGN_ASYNC_CONNECT_POLICY.md``).

POLICY
------
After the sync_request guard landed, synchronous RPC like
``conn.root.echo()`` from inside a running event loop raises
``RuntimeError`` by design. These tests therefore talk to the connection
exclusively via the async path (``rpyc.async_(proxy)`` wrappers).

NO SAME-PROCESS SERVER+CLIENT — see ``tests/support.py`` policy block.
The server is run in a child process via ``mp_asyncio_server``; the
client runs in the test's own ``asyncio.run(...)`` loop.
"""
from __future__ import annotations

import asyncio
import unittest

import rpyc_async as rpyc
from rpyc_async.core.async_connect import async_connect
from tests.support import mp_asyncio_server


# ─── Picklable service (module scope, spawn-safe) ───────────────────────────

class _EchoService(rpyc.Service):
    """Trivial echo/add service used by the tests below.

    Must live at module scope so it survives pickling when the server
    child process is spawned.
    """

    def exposed_echo(self, msg: str) -> str:
        return f"echo: {msg}"

    def exposed_add(self, a: int, b: int) -> int:
        return a + b


def _service_factory() -> type[rpyc.Service]:
    return _EchoService


class TestAsyncConnect(unittest.TestCase):
    """``async_connect`` happy-path tests against a real out-of-process server.

    Each test spins up a fresh ``AsyncioServer`` child process through
    ``mp_asyncio_server``. That is the only supported topology in this
    codebase; see ``tests/support.py`` for why.
    """

    def _run(self, coro_factory) -> None:
        """Start a server child process, then run the coroutine factory
        on a fresh asyncio loop with the bound port.
        """
        with mp_asyncio_server(_service_factory) as port:
            asyncio.run(coro_factory(port))

    # ── connection establishment ───────────────────────────────────────────

    def test_async_connect_basic(self) -> None:
        async def _go(port: int) -> None:
            conn = await async_connect("127.0.0.1", port, timeout=5.0)
            self.assertIsNotNone(conn)
            self.assertFalse(conn.closed)
            await conn.aclose()

        self._run(_go)

    def test_async_connect_no_blocking(self) -> None:
        async def _go(port: int) -> None:
            start = asyncio.get_event_loop().time()
            conn = await async_connect("127.0.0.1", port, timeout=5.0)
            elapsed = asyncio.get_event_loop().time() - start
            try:
                self.assertLess(
                    elapsed, 0.5,
                    f"Connection took {elapsed}s — likely blocking!",
                )
                self.assertFalse(conn.closed)
            finally:
                await conn.aclose()

        self._run(_go)

    # ── failure paths (do not need a server at all) ────────────────────────

    def test_async_connect_timeout(self) -> None:
        async def _go() -> None:
            with self.assertRaises(ConnectionError) as ctx:
                await async_connect("192.0.2.1", 9999, timeout=0.5)
            self.assertIn("timed out", str(ctx.exception).lower())

        asyncio.run(_go())

    def test_async_connect_connection_refused(self) -> None:
        async def _go() -> None:
            with self.assertRaises(ConnectionError) as ctx:
                await async_connect("127.0.0.1", 1, timeout=1.0)
            self.assertIn("failed to connect", str(ctx.exception).lower())

        asyncio.run(_go())

    # ── RPC round-trips through the async wrapper ──────────────────────────

    def test_async_connect_rpc_calls_via_async_wrapper(self) -> None:
        async def _go(port: int) -> None:
            conn = await async_connect("127.0.0.1", port, timeout=5.0)
            try:
                async_echo = rpyc.async_(conn.root.echo)
                async_add = rpyc.async_(conn.root.add)
                self.assertEqual(await async_echo("test"), "echo: test")
                self.assertEqual(await async_add(2, 3), 5)
            finally:
                await conn.aclose()

        self._run(_go)

    # ── asyncio-integration attribute contract ─────────────────────────────

    def test_async_connect_has_asyncio_attributes(self) -> None:
        async def _go(port: int) -> None:
            conn = await async_connect("127.0.0.1", port, timeout=5.0)
            try:
                self.assertTrue(hasattr(conn, "_asyncio_enabled"))
                self.assertTrue(hasattr(conn, "_asyncio_loop"))
                self.assertTrue(hasattr(conn, "_loop_fd_registered"))
                self.assertTrue(
                    conn._asyncio_enabled,
                    "async_connect() must auto-enable asyncio serving",
                )
            finally:
                await conn.aclose()

        self._run(_go)

    # ── concurrency ─────────────────────────────────────────────────────────

    def test_async_connect_multiple_concurrent(self) -> None:
        """20 concurrent ``async_connect`` calls + async RPCs — must not
        serialize and must not block.
        """
        async def _go(port: int) -> None:
            start = asyncio.get_event_loop().time()
            conns = await asyncio.gather(
                *[async_connect("127.0.0.1", port, timeout=5.0) for _ in range(20)]
            )
            elapsed = asyncio.get_event_loop().time() - start
            try:
                self.assertLess(
                    elapsed, 2.0,
                    f"20 connections took {elapsed}s — likely blocking!",
                )
                self.assertEqual(len(conns), 20)
                for conn in conns:
                    self.assertFalse(conn.closed)
                results = await asyncio.gather(
                    *[rpyc.async_(c.root.add)(1, 1) for c in conns]
                )
                self.assertEqual(list(results), [2] * 20)
            finally:
                await asyncio.gather(*(c.aclose() for c in conns))

        self._run(_go)

    # ── config / loop / root ───────────────────────────────────────────────

    def test_async_connect_custom_config(self) -> None:
        async def _go(port: int) -> None:
            conn = await async_connect(
                "127.0.0.1", port,
                config={"allow_public_attrs": True, "allow_safe_attrs": True},
                timeout=5.0,
            )
            try:
                self.assertEqual(conn._config["allow_public_attrs"], True)
                self.assertEqual(conn._config["allow_safe_attrs"], True)
            finally:
                await conn.aclose()

        self._run(_go)

    def test_async_connect_accepts_loop_parameter(self) -> None:
        async def _go(port: int) -> None:
            loop = asyncio.get_running_loop()
            conn = await async_connect(
                "127.0.0.1", port, loop=loop, timeout=5.0,
            )
            try:
                self.assertIsNotNone(conn)
                self.assertFalse(conn.closed)
            finally:
                await conn.aclose()

        self._run(_go)

    def test_async_connect_root_ready_immediately(self) -> None:
        async def _go(port: int) -> None:
            conn = await async_connect("127.0.0.1", port, timeout=5.0)
            try:
                self.assertIsNotNone(
                    conn._remote_root,
                    "Bug: _remote_root is None after async_connect — eager "
                    "handshake must pre-fetch it to avoid a blocking "
                    "sync_request on first conn.root access.",
                )
                self.assertIsNotNone(conn.root)
            finally:
                await conn.aclose()

        self._run(_go)

    def test_async_connect_no_blocking_on_root_access(self) -> None:
        """Accessing ``conn.root`` must not block the event loop."""
        async def _go(port: int) -> None:
            conn = await async_connect("127.0.0.1", port, timeout=5.0)

            async def fast_task() -> str:
                await asyncio.sleep(0.01)
                return "completed"

            task = asyncio.create_task(fast_task())
            try:
                start = asyncio.get_event_loop().time()
                _ = conn.root  # instant — pre-fetched by async_connect
                elapsed = asyncio.get_event_loop().time() - start
                self.assertLess(
                    elapsed, 0.01,
                    f"Accessing conn.root took {elapsed}s — likely "
                    f"doing sync_request!",
                )
                self.assertEqual(await asyncio.wait_for(task, 0.1), "completed")
            finally:
                await conn.aclose()

        self._run(_go)


if __name__ == "__main__":
    unittest.main()
