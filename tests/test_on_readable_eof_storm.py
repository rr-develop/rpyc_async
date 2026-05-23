"""Regression: ``on_readable`` must not livelock-log when the stream closes.

Incident (a downstream application log storm): ~1.7 GB of
``EOFError: stream has been closed`` written to the server log within
minutes (three full 512 MB log rotations in ~6 min).

Root cause: in ``Connection.enable_asyncio_serving`` the reader callback is

    def on_readable():
        while self._channel.poll(0):     # <-- condition
            try:
                data = self._channel.recv()
                ...
            except EOFError:
                self.close()
                break

When the underlying socket is already closed, ``stream.fileno()`` raises
``EOFError`` — and ``poll()`` calls ``fileno()``. So the EOFError is raised by
the ``while`` CONDITION, which is OUTSIDE the inner ``try``. It escapes
``on_readable`` entirely, asyncio logs "Exception in callback", and crucially
``self.close()`` is never reached — so the reader is NEVER removed from the
loop. The loop keeps the fd armed, immediately re-fires ``on_readable``, which
immediately raises again: a tight log-spamming livelock that only stops when
the process is killed.

The fix: an EOFError from ``poll()`` (the while condition) must be handled the
same as one from ``recv()`` — close the connection (which calls
``disable_asyncio_serving`` → ``loop.remove_reader``) and stop. The reader
must come off the loop so it cannot re-fire.
"""

from __future__ import annotations

import asyncio
import unittest
from typing import Any

from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService


class _ClosedStreamChannel:
    """Channel whose ``poll()`` raises EOFError — exactly what a real
    channel does once its stream is closed (``stream.poll`` →
    ``stream.fileno`` → ``EOFError``)."""

    def __init__(self) -> None:
        self.closed = False
        self.poll_calls = 0

    def fileno(self) -> int:
        return -1  # a harmless fd for add_reader registration

    def poll(self, timeout: float) -> bool:
        self.poll_calls += 1
        raise EOFError("stream has been closed")

    def recv(self) -> bytes:
        raise EOFError("stream has been closed")

    def send(self, data: bytes) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class TestOnReadableEofStorm(unittest.IsolatedAsyncioTestCase):
    async def _make_conn_capturing_reader(self) -> tuple[Connection, Any, dict]:
        """Build a Connection on a closed-stream channel, capture the
        ``on_readable`` callback that ``enable_asyncio_serving`` registers,
        and record loop.add_reader / remove_reader activity."""
        chan = _ClosedStreamChannel()
        conn = Connection(VoidService(), chan, config={})

        loop = asyncio.get_running_loop()
        state: dict = {"reader": None, "removed_fds": [], "added_fd": None}

        real_add = loop.add_reader
        real_remove = loop.remove_reader

        def fake_add(fd, cb, *a):  # type: ignore[no-untyped-def]
            state["added_fd"] = fd
            state["reader"] = cb
            # Do NOT actually arm the loop on this bogus fd.

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

        # Track close().
        state["close_calls"] = 0
        orig_close = conn.close

        def counting_close():  # type: ignore[no-untyped-def]
            state["close_calls"] += 1
            return orig_close()

        conn.close = counting_close  # type: ignore[method-assign]
        return conn, chan, state

    async def test_on_readable_does_not_raise_on_closed_stream(self) -> None:
        conn, chan, state = await self._make_conn_capturing_reader()
        reader = state["reader"]
        self.assertIsNotNone(reader, "enable_asyncio_serving must register a reader")

        # Fire the reader exactly as the event loop would when the (now
        # closed) fd reports readable. This MUST NOT raise — previously the
        # EOFError from the while-condition escaped here.
        try:
            reader()
        except EOFError:
            self.fail(
                "on_readable let an EOFError from poll() escape — this is "
                "the log-storm livelock (observed in a downstream application)"
            )

    async def test_on_readable_closes_and_removes_reader_on_eof(self) -> None:
        conn, chan, state = await self._make_conn_capturing_reader()
        reader = state["reader"]

        reader()

        # The connection must have been closed (which removes the reader),
        # so the loop can never re-fire the callback → no storm.
        self.assertGreaterEqual(
            state["close_calls"], 1,
            "on_readable must close the connection when the stream is EOF, "
            "so the reader is removed and cannot re-fire (no log storm)",
        )
        self.assertIn(
            state["added_fd"], state["removed_fds"],
            "the registered fd must be removed from the loop on EOF",
        )

    async def test_on_readable_is_idempotent_after_close(self) -> None:
        """Even if the loop fires the stale reader once more before removal
        propagates, a second call must still not raise."""
        conn, chan, state = await self._make_conn_capturing_reader()
        reader = state["reader"]
        reader()
        try:
            reader()  # second spurious fire
        except EOFError:
            self.fail("second on_readable fire raised EOFError")


if __name__ == "__main__":
    unittest.main()
