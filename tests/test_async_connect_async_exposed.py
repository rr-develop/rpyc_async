"""Test async_connect + AsyncioServer with ``async def exposed_*`` methods.

This file replaces an earlier version that put the ``AsyncioServer`` and the
``async_connect`` client on the SAME event loop — a configuration this
project does not support (see ``tests/support.py`` policy block). That
older test hung and was misreported as a "nested AsyncResult deadlock".

The real invariant is: ``AsyncioServer`` and its rpyc client MUST live in
different processes. Any test that puts them on one loop deadlocks on the
first round-trip by architecture, not by bug.
"""
from __future__ import annotations

import asyncio
import unittest

import rpyc_async as rpyc
from rpyc_async.core.async_ import AsyncResult
from rpyc_async.core.async_connect import async_connect
from tests.support import mp_asyncio_server


# ─── Picklable server service factory (top-level — spawn-safe) ──────────────

class _AsyncExposedService(rpyc.Service):
    """Simple service exercising async and sync exposed methods.

    NOTE: This class MUST live at module scope (not inside a test method)
    so it can be pickled when the server child process is spawned.
    """

    async def exposed_async_hello(self) -> str:
        return "hello from async"

    async def exposed_async_add(self, a: int, b: int) -> int:
        return a + b

    def exposed_sync_hello(self) -> str:
        return "hello from sync"


def _service_factory() -> type[rpyc.Service]:
    return _AsyncExposedService


class TestAsyncConnectAsyncExposed(unittest.TestCase):
    """``await conn.root.<async_exposed>()`` must return the value, not
    an unresolved ``AsyncResult`` — over a real rpyc wire, with server in
    a child process."""

    def test_async_method_returns_value_not_async_result(self) -> None:
        async def _go(port: int) -> None:
            conn = await async_connect("127.0.0.1", port, timeout=5.0)
            try:
                result = await asyncio.wait_for(
                    conn.root.async_hello(), timeout=5.0,
                )
                self.assertNotIsInstance(
                    result,
                    AsyncResult,
                    msg=(
                        f"await returned a bare AsyncResult instead of the "
                        f"string value (nested AsyncResult bug). "
                        f"type={type(result).__name__}, value={result!r}"
                    ),
                )
                self.assertEqual(result, "hello from async")
            finally:
                await conn.aclose()

        with mp_asyncio_server(_service_factory) as port:
            asyncio.run(_go(port))

    def test_async_method_with_args(self) -> None:
        async def _go(port: int) -> None:
            conn = await async_connect("127.0.0.1", port, timeout=5.0)
            try:
                result = await asyncio.wait_for(
                    conn.root.async_add(3, 4), timeout=5.0,
                )
                self.assertNotIsInstance(result, AsyncResult)
                self.assertEqual(result, 7)
            finally:
                await conn.aclose()

        with mp_asyncio_server(_service_factory) as port:
            asyncio.run(_go(port))

    def test_sync_method_via_async_connect(self) -> None:
        """A sync exposed method via ``async_connect``: must go through the
        async wrapper (``rpyc.async_(...)``) from a running loop — the
        sync_request guard would otherwise refuse a direct sync netref call.
        """
        async def _go(port: int) -> None:
            conn = await async_connect("127.0.0.1", port, timeout=5.0)
            try:
                a_sync_hello = rpyc.async_(conn.root.sync_hello)
                result = await asyncio.wait_for(a_sync_hello(), timeout=5.0)
                self.assertNotIsInstance(result, AsyncResult)
                self.assertEqual(result, "hello from sync")
            finally:
                await conn.aclose()

        with mp_asyncio_server(_service_factory) as port:
            asyncio.run(_go(port))


if __name__ == "__main__":
    unittest.main(verbosity=2)
