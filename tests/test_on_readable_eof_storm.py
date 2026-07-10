"""Regression: ``on_readable`` must not livelock/log-storm when the stream
closes, and must take its reader OFF the loop on EOF.

Two historical incidents this guards (both observed in a downstream application):

  * 2026-05-23 — ~1.7 GB of ``EOFError: stream has been closed`` written in
    minutes. The OLD poll-based reader let an EOFError from the ``while
    self._channel.poll(0)`` CONDITION escape the callback; ``close()`` was
    never reached, the reader stayed armed, asyncio re-fired it instantly →
    a tight log-spamming livelock.
  * 2026-05-27 — 99.9% CPU. A half-closed inbound socket (CLOSE-WAIT, pending
    EOF) is *permanently* readable to epoll; the poll-based reader spun
    ~34 000×/sec issuing ZERO recv syscalls.

Both are fixed by the EVENT-DRIVEN, buffered, NO-POLL reader
(DESIGN_NO_POLLING_ASYNCIO_READ.md): one non-blocking ``recv_available()`` per
readable event, frame from an in-memory buffer, and ``_close_and_remove_reader``
on EOF so the reader can never re-fire.

These tests use a REAL ``socketpair`` (NO mocks) — the only faithful way to
exercise EOF / half-close behaviour.
"""

from __future__ import annotations

import asyncio
import socket
import unittest
from typing import Any

from rpyc_async.core.channel import Channel
from rpyc_async.core.protocol import Connection
from rpyc_async.core.service import VoidService
from rpyc_async.core.stream import SocketStream


class TestOnReadableEofStorm(unittest.IsolatedAsyncioTestCase):
    async def _arm_closed(self) -> tuple[Connection, Any, dict]:
        """Build a Connection over a real socket whose peer has fully closed,
        capture the registered reader + add/remove activity + close()."""
        a, b = socket.socketpair()
        self.addCleanup(self._safe_close, a)
        b.close()  # peer GONE → 'a' will read EOF (b'') immediately

        conn = Connection(VoidService(), Channel(SocketStream(a)), config={})
        loop = asyncio.get_running_loop()
        state: dict[str, Any] = {"reader": None, "added_fd": None,
                                 "removed_fds": [], "close_calls": 0}
        real_add, real_remove = loop.add_reader, loop.remove_reader

        def fake_add(fd, cb, *_a):  # type: ignore[no-untyped-def]
            state["added_fd"] = fd
            state["reader"] = cb

        def fake_remove(fd):  # type: ignore[no-untyped-def]
            state["removed_fds"].append(fd)
            return True

        loop.add_reader = fake_add  # type: ignore[method-assign]
        loop.remove_reader = fake_remove  # type: ignore[method-assign]
        try:
            conn.enable_asyncio_serving(loop=loop)
        finally:
            loop.add_reader = real_add  # type: ignore[method-assign]
            loop.remove_reader = real_remove  # type: ignore[method-assign]

        conn._dispatch = lambda data: None  # type: ignore[method-assign,assignment]
        orig_close = conn.close

        def counting_close():  # type: ignore[no-untyped-def]
            state["close_calls"] += 1
            return orig_close()

        conn.close = counting_close  # type: ignore[method-assign]
        return conn, a, state

    @staticmethod
    def _safe_close(s: socket.socket) -> None:
        try:
            s.close()
        except OSError:
            pass

    async def test_on_readable_does_not_raise_on_closed_stream(self) -> None:
        conn, _a, state = await self._arm_closed()
        reader = state["reader"]
        self.assertIsNotNone(reader, "enable_asyncio_serving must register a reader")
        # Firing on a fully-closed peer must NOT raise (the 2026-05-23 storm
        # was an EOFError escaping the callback).
        try:
            reader()
        except Exception as exc:  # noqa: BLE001
            self.fail(f"on_readable raised on a closed stream: {exc!r}")

    async def test_on_readable_closes_and_removes_reader_on_eof(self) -> None:
        conn, _a, state = await self._arm_closed()
        state["reader"]()
        self.assertGreaterEqual(
            state["close_calls"], 1,
            "on_readable must close the connection on EOF so the reader is "
            "removed and cannot re-fire (no storm)",
        )
        self.assertIn(
            state["added_fd"], state["removed_fds"],
            "the registered fd must be removed from the loop on EOF",
        )

    async def test_on_readable_is_idempotent_after_close(self) -> None:
        """Even if the loop fires the stale reader again before removal
        propagates, a second call must not raise and must not spin."""
        conn, _a, state = await self._arm_closed()
        reader = state["reader"]
        reader()
        try:
            reader()  # second spurious fire
        except Exception as exc:  # noqa: BLE001
            self.fail(f"second on_readable fire raised: {exc!r}")


if __name__ == "__main__":
    unittest.main()
