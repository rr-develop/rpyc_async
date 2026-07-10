"""TDD: the asyncio reader is buffered + EVENT-DRIVEN — NO poll, NO busy-loop.

DESIGN_NO_POLLING_ASYNCIO_READ.md §3. These are the regression tests for the
99.9%-CPU incident (observed in a downstream application, 2026-05-27): a half-closed inbound socket
made ``on_readable`` fire ~34 000×/sec with ZERO recv syscalls.

NO MOCKS for the socket: every test drives ``enable_asyncio_serving`` against a
REAL ``socketpair`` on a REAL running event loop. We capture the registered
reader callback and the loop's add_reader/remove_reader activity to assert the
reader comes OFF the loop on EOF and does not spin.

⚠️ NO POLLING / NO BUSY-LOOP: the reader must do ONE non-blocking recv per
readable event, frame from an in-memory buffer, and remove itself on EOF.
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


def _frame(payload: bytes) -> bytes:
    header = Channel.FRAME_HEADER.pack(len(payload), 0)
    return header + payload + Channel.FLUSHER


class _AsyncReaderHarness(unittest.IsolatedAsyncioTestCase):
    async def _arm(self, sock: socket.socket) -> tuple[Connection, dict]:
        """Build a Connection over ``sock`` and capture its add_reader cb +
        the loop's reader add/remove activity. Does NOT actually arm the loop
        on the fd (we fire the captured callback manually, deterministically)."""
        conn = Connection(VoidService(), Channel(SocketStream(sock)), config={})
        loop = asyncio.get_running_loop()
        state: dict[str, Any] = {
            "reader": None, "added_fd": None, "removed_fds": [],
            "dispatched": [], "close_calls": 0,
        }
        real_add, real_remove = loop.add_reader, loop.remove_reader

        def fake_add(fd, cb, *a):  # type: ignore[no-untyped-def]
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

        # Capture dispatched frames instead of running the real dispatcher.
        conn._dispatch = lambda data: state["dispatched"].append(data)  # type: ignore[method-assign,assignment]
        orig_close = conn.close

        def counting_close():  # type: ignore[no-untyped-def]
            state["close_calls"] += 1
            return orig_close()

        conn.close = counting_close  # type: ignore[method-assign]
        return conn, state


class TestAsyncReaderNoPoll(_AsyncReaderHarness):
    async def test_dispatches_whole_frames(self) -> None:
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        conn, state = await self._arm(a)

        b.sendall(_frame(b"hello") + _frame(b"world"))
        state["reader"]()  # one readable event

        self.assertEqual(state["dispatched"], [b"hello", b"world"])
        self.assertEqual(state["close_calls"], 0)

    async def test_partial_frame_waits_then_completes(self) -> None:
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        conn, state = await self._arm(a)

        wire = _frame(b"a payload that we split")
        b.sendall(wire[:6])          # partial frame
        state["reader"]()
        self.assertEqual(state["dispatched"], [], "partial frame dispatched early")
        self.assertEqual(state["close_calls"], 0)

        b.sendall(wire[6:])          # rest arrives
        state["reader"]()
        self.assertEqual(state["dispatched"], [b"a payload that we split"])

    async def test_half_closed_partial_frame_closes_no_spin(self) -> None:
        """THE regression: peer writes a partial frame then FIN. The reader
        must close (remove itself) and NOT busy-loop. We fire it a few times to
        mimic asyncio re-firing; recv must hit EOF and close, not spin."""
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        conn, state = await self._arm(a)

        b.sendall(b"\x00\x00\x00")   # partial header (3 < 5 bytes)
        b.shutdown(socket.SHUT_WR)   # FIN → 'a' in CLOSE-WAIT, EOF pending

        # Fire as the loop would. Even if fired several times before removal
        # propagates, it must converge to closed — never raise, never spin.
        for _ in range(5):
            state["reader"]()

        self.assertGreaterEqual(state["close_calls"], 1,
                                "half-closed fd must close the connection")
        self.assertIn(state["added_fd"], state["removed_fds"],
                      "the reader fd must be removed from the loop on EOF")

    async def test_orderly_eof_closes(self) -> None:
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        conn, state = await self._arm(a)

        b.sendall(_frame(b"final"))
        b.shutdown(socket.SHUT_WR)
        state["reader"]()  # reads frame
        state["reader"]()  # reads EOF
        self.assertIn(b"final", state["dispatched"])
        self.assertGreaterEqual(state["close_calls"], 1)
        self.assertIn(state["added_fd"], state["removed_fds"])

    async def test_benign_empty_wakeup_does_not_close(self) -> None:
        """A spurious readable wakeup with nothing in the buffer (EAGAIN) must
        NOT close a healthy connection and must NOT spin."""
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        conn, state = await self._arm(a)

        # Nothing sent by peer; fire the reader → recv_available() returns None.
        state["reader"]()
        self.assertEqual(state["close_calls"], 0,
                         "benign empty wakeup must not close a live connection")
        self.assertFalse(conn.closed,
                         "a healthy connection must stay open on a benign "
                         "empty (EAGAIN) wakeup")

    async def test_reader_uses_recv_available_not_poll(self) -> None:
        """Behavioural guarantee that the reader is the buffered, non-poll
        path: a frame arriving in TWO writes is dispatched only after BOTH —
        proving on_readable buffers (does not block-read a whole frame, does
        not poll-drain). The static "no poll(0) in source" guarantee lives in
        tests/test_no_polling_policy.py."""
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        conn, state = await self._arm(a)

        wire = _frame(b"two-part-frame")
        b.sendall(wire[:7]); state["reader"]()
        self.assertEqual(state["dispatched"], [])         # buffered, not blocked
        b.sendall(wire[7:]); state["reader"]()
        self.assertEqual(state["dispatched"], [b"two-part-frame"])


if __name__ == "__main__":
    unittest.main()
