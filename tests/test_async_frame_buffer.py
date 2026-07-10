"""TDD: pure, in-memory incremental frame buffer for the asyncio read path.

DESIGN_NO_POLLING_ASYNCIO_READ.md §3.2. The event-driven reader must assemble
whole rpyc frames from a byte BUFFER — never by polling the socket. A complete
frame on the wire is::

    FRAME_HEADER (Struct "!LB", 5 bytes: uint32 length + uint8 compressed)
    + payload (`length` bytes)
    + FLUSHER (b"\\n", 1 byte)

i.e. ``5 + length + 1`` bytes, fully self-delimiting (length is in the header).
``Connection._extract_frames(chunk)`` appends ``chunk`` to the per-connection
buffer and returns every COMPLETE payload now available, leaving any partial
remainder buffered for the next real readable event. It NEVER touches a socket
and NEVER polls.

⚠️ NO POLLING / NO BUSY-LOOP: this framer exists precisely so the asyncio
reader needs neither ``poll(0)`` nor a ``while poll(...)`` drain.
"""

from __future__ import annotations

import unittest
import zlib

from rpyc_async.core.channel import Channel
from rpyc_async.core.protocol import Connection
from rpyc_async.core.service import VoidService


def _frame(payload: bytes, *, compress: bool = False) -> bytes:
    """Build a wire frame exactly as Channel.send does."""
    compressed = 0
    body = payload
    if compress:
        compressed = 1
        body = zlib.compress(payload, Channel.COMPRESSION_LEVEL)
    header = Channel.FRAME_HEADER.pack(len(body), compressed)
    return header + body + Channel.FLUSHER


class _NullStream:
    """A stream stub with no socket — the framer must never call into it."""

    MAX_IO_CHUNK = 64 * 1024

    def fileno(self) -> int:
        return -1

    def poll(self, timeout):  # noqa: ANN001
        raise AssertionError("framer must NOT poll the stream")

    def read(self, count):  # noqa: ANN001
        raise AssertionError("framer must NOT read the stream")

    def close(self) -> None:
        pass


def _make_conn() -> Connection:
    return Connection(VoidService(), Channel(_NullStream()), config={})


class TestAsyncFrameBuffer(unittest.TestCase):
    def test_empty_chunk_yields_nothing(self) -> None:
        conn = _make_conn()
        self.assertEqual(conn._extract_frames(b""), [])

    def test_partial_header_buffers_no_frame(self) -> None:
        conn = _make_conn()
        # Fewer than FRAME_HEADER.size (5) bytes — cannot even read length.
        self.assertEqual(conn._extract_frames(b"\x00\x00"), [])

    def test_single_whole_frame(self) -> None:
        conn = _make_conn()
        frames = conn._extract_frames(_frame(b"hello"))
        self.assertEqual(frames, [b"hello"])

    def test_one_and_a_half_frames(self) -> None:
        conn = _make_conn()
        whole = _frame(b"first")
        half = _frame(b"second")[:4]  # only part of the second frame
        frames = conn._extract_frames(whole + half)
        self.assertEqual(frames, [b"first"])
        # The half stays buffered; completing it yields the second frame.
        rest = _frame(b"second")[4:]
        self.assertEqual(conn._extract_frames(rest), [b"second"])

    def test_frame_split_across_many_chunks(self) -> None:
        conn = _make_conn()
        wire = _frame(b"a moderately sized payload")
        # Feed it one byte at a time: every chunk but the last yields nothing.
        out: list[bytes] = []
        for i, byte in enumerate(wire):
            got = conn._extract_frames(bytes([byte]))
            out.extend(got)
            if i < len(wire) - 1:
                self.assertEqual(got, [], f"frame completed early at byte {i}")
        self.assertEqual(out, [b"a moderately sized payload"])

    def test_multiple_whole_frames_in_one_chunk(self) -> None:
        conn = _make_conn()
        chunk = _frame(b"one") + _frame(b"two") + _frame(b"three")
        self.assertEqual(conn._extract_frames(chunk), [b"one", b"two", b"three"])

    def test_compressed_frame_round_trips(self) -> None:
        conn = _make_conn()
        payload = b"x" * 5000  # > COMPRESSION_THRESHOLD-ish; compress explicitly
        frames = conn._extract_frames(_frame(payload, compress=True))
        self.assertEqual(frames, [payload])

    def test_buffer_does_not_grow_unbounded_across_calls(self) -> None:
        """Completed frames are removed from the buffer (no leak)."""
        conn = _make_conn()
        for _ in range(100):
            conn._extract_frames(_frame(b"tick"))
        # After consuming 100 whole frames, nothing should remain buffered.
        self.assertEqual(len(conn._async_inbuf), 0)


if __name__ == "__main__":
    unittest.main()
