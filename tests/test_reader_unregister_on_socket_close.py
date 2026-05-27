"""TDD: the asyncio reader's lifetime is BOUND TO THE SOCKET.

Root cause of a production 100%-CPU spin
(a related internal incident analysis, not included here):

  The socket layer (``SocketStream.close()`` → ``sock.close()``) and the
  asyncio reader registration (``Connection.disable_asyncio_serving`` →
  ``loop.remove_reader``) lived in DIFFERENT layers with no link. A close
  path that bypassed ``Connection._cleanup`` freed the fd while the
  ``add_reader`` callback was still armed. The event loop holds the callback,
  the callback closes over the Connection, so the Connection could never be
  GC'd and the reader leaked forever. Once the freed fd was recycled by
  uvicorn's own transport (same process, same loop, shared fd table), the
  orphaned reader fired on a foreign fd and ``remove_reader`` raised
  ``RuntimeError: fd is used by transport`` forever → busy-loop.

THE FIX (the guarantees these tests pin):
  1. Closing the STREAM/SOCKET (by ANY path) takes the reader OFF the loop,
     synchronously, while the fd is still valid (before ``sock.close()``).
  2. The reader self-defends: once its stream is closed it must not operate on
     the (possibly recycled) fd — it disarms instead of reading blindly.
  3. ``remove_reader`` failures from uvloop (``RuntimeError`` 'used by
     transport') are swallowed, never escape into the hot path.

These tests use REAL sockets + a REAL asyncio loop. No socket mocks.
"""

from __future__ import annotations

import asyncio
import socket
import unittest
from typing import Any

from rpyc.core.channel import Channel
from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService
from rpyc.core.stream import SocketStream


class _Harness(unittest.IsolatedAsyncioTestCase):
    async def _arm(self, sock: socket.socket) -> tuple[Connection, dict]:
        """Build a Connection over ``sock`` on the running loop, capturing the
        loop's add_reader / remove_reader activity."""
        conn = Connection(VoidService(), Channel(SocketStream(sock)), config={})
        loop = asyncio.get_running_loop()
        state: dict[str, Any] = {"added_fd": None, "removed_fds": [], "reader": None}
        real_add, real_remove = loop.add_reader, loop.remove_reader

        def fake_add(fd, cb, *a):  # type: ignore[no-untyped-def]
            state["added_fd"] = fd
            state["reader"] = cb
            return real_add(fd, cb, *a)

        def fake_remove(fd):  # type: ignore[no-untyped-def]
            state["removed_fds"].append(fd)
            return real_remove(fd)

        loop.add_reader = fake_add        # type: ignore[method-assign]
        loop.remove_reader = fake_remove  # type: ignore[method-assign]
        conn.enable_asyncio_serving(loop=loop)
        # enable_asyncio_serving does a DEFENSIVE remove_reader(fd) before
        # add_reader (the "fd reused before cleanup" guard). Discard that so
        # removed_fds reflects ONLY what happens AFTER arming.
        state["removed_fds"].clear()
        # Keep the fakes installed through the test body so we observe the
        # removal triggered by the close path. Restore on cleanup.
        self.addCleanup(lambda: setattr(loop, "add_reader", real_add))
        self.addCleanup(lambda: setattr(loop, "remove_reader", real_remove))
        return conn, state


class TestReaderUnregisteredOnSocketClose(_Harness):
    async def test_stream_close_removes_reader(self) -> None:
        """Closing the STREAM directly (bypassing Connection.close /_cleanup)
        MUST still take the reader off the loop, while the fd is valid."""
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        conn, state = await self._arm(a)
        fd = state["added_fd"]
        self.assertIsNotNone(fd)

        # Close ONLY the stream layer — do NOT call conn.close()/aclose().
        conn._channel.stream.close()

        self.assertIn(
            fd, state["removed_fds"],
            "closing the socket/stream must unregister the reader from the "
            "loop (it is bound to the socket's lifetime)",
        )

    async def test_channel_close_removes_reader(self) -> None:
        """Channel.close() → stream.close() must also unregister the reader."""
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        conn, state = await self._arm(a)
        fd = state["added_fd"]

        conn._channel.close()

        self.assertIn(fd, state["removed_fds"],
                      "Channel.close() must unregister the reader")

    async def test_reader_does_not_outlive_socket_for_gc(self) -> None:
        """After the socket closes, the reader must be OFF the loop so the
        Connection is no longer pinned by the loop->callback->conn cycle.
        We assert the fd is unregistered (the precondition for GC)."""
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        conn, state = await self._arm(a)
        fd = state["added_fd"]

        conn._channel.stream.close()
        self.assertIn(fd, state["removed_fds"])
        # And firing a stale reference must not re-read or raise.
        reader = state["reader"]
        try:
            reader()  # should self-detect closed stream and no-op/disarm
        except Exception as exc:  # noqa: BLE001
            self.fail(f"stale reader fire after socket close raised: {exc!r}")

    async def test_reader_self_defends_on_closed_stream(self) -> None:
        """If the reader fires after the stream is closed (e.g. a spurious
        late wakeup), it must NOT touch the socket blindly — it disarms and
        returns without raising."""
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        conn, state = await self._arm(a)
        reader = state["reader"]

        # Close the stream out from under the reader.
        conn._channel.stream.close()
        # Fire repeatedly as a stuck loop would.
        for _ in range(5):
            try:
                reader()
            except Exception as exc:  # noqa: BLE001
                self.fail(f"reader on closed stream raised: {exc!r}")

    async def test_remove_reader_runtimeerror_is_swallowed(self) -> None:
        """uvloop raises RuntimeError('fd is used by transport') when the fd
        was recycled by another transport. The unregister path must swallow it
        (not propagate into the close/reader path)."""
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        conn, state = await self._arm(a)
        loop = asyncio.get_running_loop()

        def boom(_fd):  # type: ignore[no-untyped-def]
            raise RuntimeError(f"File descriptor {_fd} is used by transport <x>")

        loop.remove_reader = boom  # type: ignore[method-assign]
        try:
            # Must not raise despite remove_reader blowing up.
            conn._channel.stream.close()
        finally:
            pass  # loop is torn down per-test by IsolatedAsyncioTestCase


if __name__ == "__main__":
    unittest.main()
