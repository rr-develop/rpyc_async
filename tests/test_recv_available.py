"""TDD: SocketStream.recv_available — ONE non-blocking recv, no poll.

DESIGN_NO_POLLING_ASYNCIO_READ.md §3.1. The event-driven async reader reads
whatever is in the socket buffer right now, in a single syscall, and decides
from the RESULT — never by polling:

  * data (non-empty bytes)         -> return those bytes
  * peer closed (recv returns b'') -> return b''  (real EOF)
  * EAGAIN / EWOULDBLOCK           -> return None (nothing right now; benign)
  * hard socket error              -> raise EOFError (and close)

⚠️ NO POLLING / NO BUSY-LOOP: ``recv_available`` MUST NOT call ``poll``,
``select``, or ``MSG_PEEK``. It is the single non-blocking read the
add_reader callback issues per readable event.

Real sockets only — NO mocks.
"""

from __future__ import annotations

import socket
import unittest

from rpyc_async.core.stream import SocketStream


class TestRecvAvailable(unittest.TestCase):
    def setUp(self) -> None:
        self.a, self.b = socket.socketpair()
        self.addCleanup(self._safe_close, self.a)
        self.addCleanup(self._safe_close, self.b)
        self.stream = SocketStream(self.a)

    @staticmethod
    def _safe_close(s: socket.socket) -> None:
        try:
            s.close()
        except OSError:
            pass

    def test_returns_data_when_available(self) -> None:
        self.b.sendall(b"payload-bytes")
        # tiny settle: socketpair delivery is synchronous on localhost
        got = self.stream.recv_available()
        self.assertEqual(got, b"payload-bytes")

    def test_returns_none_on_eagain(self) -> None:
        """Nothing sent → non-blocking recv must report 'nothing now' as None,
        NOT block and NOT poll."""
        got = self.stream.recv_available()
        self.assertIsNone(got)

    def test_returns_empty_bytes_on_peer_close(self) -> None:
        """Peer closed its write end (FIN) with no pending data → real EOF
        signalled as b''."""
        self.b.shutdown(socket.SHUT_WR)
        got = self.stream.recv_available()
        self.assertEqual(got, b"")

    def test_data_then_eof_in_sequence(self) -> None:
        """Pending data is returned first; the subsequent read sees EOF."""
        self.b.sendall(b"last-bytes")
        self.b.shutdown(socket.SHUT_WR)
        first = self.stream.recv_available()
        self.assertEqual(first, b"last-bytes")
        second = self.stream.recv_available()
        self.assertEqual(second, b"")  # now EOF

    def test_hard_error_raises_eoferror(self) -> None:
        """A closed/errored socket surfaces as EOFError, not a silent spin."""
        self.a.close()  # our own end closed → recv errors (EBADF)
        with self.assertRaises(EOFError):
            self.stream.recv_available()

    def test_does_not_block_forever(self) -> None:
        """Hard guarantee: with no data and an open peer, the call returns
        promptly (None) rather than blocking (which would be a busy/blocking
        antipattern in the async callback)."""
        import time
        t0 = time.time()
        got = self.stream.recv_available()
        self.assertIsNone(got)
        self.assertLess(time.time() - t0, 1.0, "recv_available must not block")


if __name__ == "__main__":
    unittest.main()
