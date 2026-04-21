"""
Working Async Patterns — end-to-end with real rpyc + AsyncioServer.

Demonstrates all fully-supported async patterns:

1. Client → Server async calls
2. Recursive async calls
3. Concurrent async operations
4. Mixed sync/async (via ``rpyc.async_`` wrapper)
5. Server-side immediate processing

POLICY
------
NO SAME-PROCESS SERVER+CLIENT — see ``tests/support.py`` and
``docs/DESIGN_NO_SAME_PROCESS_TESTS.md``. The server is started in a
child process via ``mp_asyncio_server``.
"""
from __future__ import annotations

import asyncio
import time
import unittest

import rpyc
from tests.support import mp_asyncio_server


# ─── Picklable service (spawn-safe) ─────────────────────────────────────────

class _WorkingPatternsService(rpyc.Service):
    """Service demonstrating all working async patterns.

    Must live at module scope so the ``multiprocessing.Process`` spawn
    start method can pickle the class reference.
    """

    # Pattern 1: Simple async method
    async def exposed_async_hello(self, name: str) -> str:
        await asyncio.sleep(0.01)
        return f"Hello, {name}!"

    # Pattern 2: Recursive async
    async def exposed_async_countdown(self, n: int) -> list[int]:
        await asyncio.sleep(0.001)
        if n <= 0:
            return [0]
        rest = await self.exposed_async_countdown(n - 1)
        return [n] + rest

    # Pattern 3: I/O-bound async work
    async def exposed_async_fetch_data(self, url_id: str) -> str:
        await asyncio.sleep(0.1)  # simulated network delay
        return f"Data from {url_id}"

    # Pattern 4: Async processing
    async def exposed_process_async(self, task_data: str) -> str:
        await asyncio.sleep(0.1)
        return f"Processed: {task_data}"

    # Pattern 5: Mixed sync/async
    def exposed_sync_method(self, x: int) -> int:
        return x * 2

    async def exposed_async_method(self, x: int) -> int:
        await asyncio.sleep(0.01)
        return x * 3


def _service_factory() -> type[rpyc.Service]:
    return _WorkingPatternsService


class TestWorkingAsyncPatterns(unittest.TestCase):
    """All supported async patterns, server in child process."""

    def _run(self, coro_factory) -> None:
        with mp_asyncio_server(_service_factory) as port:
            asyncio.run(coro_factory(port))

    def test_pattern_1_simple_async(self) -> None:
        async def _go(port: int) -> None:
            conn = await rpyc.async_connect("localhost", port)
            try:
                self.assertEqual(
                    await conn.root.async_hello("World"),
                    "Hello, World!",
                )
            finally:
                await conn.aclose()

        self._run(_go)

    def test_pattern_2_recursive_async(self) -> None:
        async def _go(port: int) -> None:
            conn = await rpyc.async_connect("localhost", port)
            try:
                result = await conn.root.async_countdown(10)
                self.assertEqual(list(result), list(range(10, -1, -1)))
            finally:
                await conn.aclose()

        self._run(_go)

    def test_pattern_3_concurrent_async(self) -> None:
        async def _go(port: int) -> None:
            conn = await rpyc.async_connect("localhost", port)
            try:
                tasks = [
                    conn.root.async_fetch_data(f"url{i}")
                    for i in range(10)
                ]
                start = time.time()
                results = await asyncio.gather(*tasks)
                duration = time.time() - start
                self.assertEqual(len(results), 10)
                # Concurrent execution must be much faster than the
                # sequential 10 × 0.1s = 1s upper bound.
                self.assertLess(duration, 0.3)
            finally:
                await conn.aclose()

        self._run(_go)

    def test_pattern_4_async_processing(self) -> None:
        async def _go(port: int) -> None:
            conn = await rpyc.async_connect("localhost", port)
            try:
                self.assertEqual(
                    await conn.root.process_async("test_data"),
                    "Processed: test_data",
                )
            finally:
                await conn.aclose()

        self._run(_go)

    def test_pattern_5_mixed_sync_async(self) -> None:
        async def _go(port: int) -> None:
            conn = await rpyc.async_connect("localhost", port)
            try:
                # Sync remote method: go through rpyc.async_ wrapper to
                # avoid the sync_request guard.
                self.assertEqual(
                    await rpyc.async_(conn.root.sync_method)(5),
                    10,
                )
                # Native async method — just await.
                self.assertEqual(await conn.root.async_method(5), 15)
            finally:
                await conn.aclose()

        self._run(_go)

    def test_concurrent_client_calls(self) -> None:
        async def _go(port: int) -> None:
            conn = await rpyc.async_connect("localhost", port)
            try:
                tasks = [
                    conn.root.async_fetch_data(f"url{i}")
                    for i in range(5)
                ]
                results = await asyncio.gather(*tasks)
                self.assertEqual(len(results), 5)
            finally:
                await conn.aclose()

        self._run(_go)


if __name__ == "__main__":
    unittest.main(verbosity=2)
