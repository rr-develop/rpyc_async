"""
Unit tests for HANDLE_DEL suppression on closed connection.

Regression tests for the storm bug documented in
a related internal incident analysis (not included here)
and fixed per
``rpyc_async/docs/DESIGN_HANDLE_DEL_SUPPRESS_ON_CLOSED.md``.

Before the fix the cleanup loop would, on every iteration after a
peer disconnect, attempt to send a HANDLE_DEL via
``_async_request_with_ack`` on a dead connection, receive ``False``,
and emit two log lines (stderr ``print`` + ``logger.warning``) per
failed deletion. Across thousands of GC cycles for a single id_pack
this produced 128 MB stderr logs.

The fix adds two guards in
``rpyc_async.core.protocol.Connection._process_pending_deletions``:

1. Top-of-function: if ``self.closed`` — drain the queue
   silently, emit one ``logger.debug`` summary, return.
2. Post-await: if ``self.closed`` flipped while we were awaiting
   ack — ``continue`` without emitting the WARNING.
"""

import asyncio
import io
import logging
import sys
import unittest
from unittest.mock import Mock

from rpyc_async.core import consts
from rpyc_async.core.protocol import Connection
from rpyc_async.core.service import VoidService


def _make_conn():
    """Build a minimal Connection suitable for direct method testing.

    Mirrors the pattern used in ``test_batch_deletion.py`` and
    ``test_background_cleanup.py`` — Mock channel, VoidService, no
    real socket. The Connection is fully constructed and its
    ``_pending_deletions`` queue is writable, which is all we need
    for these tests.
    """
    mock_channel = Mock()
    mock_channel.closed = False
    mock_channel.fileno = Mock(return_value=1)
    return Connection(VoidService(), mock_channel, config={})


class _DebugLogCapture(logging.Handler):
    """Tiny in-memory log handler capturing every record at DEBUG+."""

    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def find(self, level, needle):
        return [
            r for r in self.records
            if r.levelno == level and needle in r.getMessage()
        ]


class TestHandleDelSuppressOnClosed(unittest.TestCase):
    """Regression suite for the HANDLE_DEL storm-after-disconnect bug."""

    def setUp(self):
        self.conn = _make_conn()
        # Attach a logger to the connection's config so the
        # _process_pending_deletions code path can route messages
        # to our capture handler. The production code reads
        # ``self._config.get("logger")``.
        self.logger = logging.getLogger(
            "test_handle_del_suppress.%s" % id(self)
        )
        self.logger.setLevel(logging.DEBUG)
        # Detach any pre-existing handlers from prior tests just in case.
        self.logger.handlers = []
        self.cap = _DebugLogCapture()
        self.logger.addHandler(self.cap)
        # Don't propagate to root — we don't want to flood pytest output.
        self.logger.propagate = False
        self.conn._config["logger"] = self.logger

        # Capture stderr so we can assert NO ``WARNING: Failed to
        # delete remote object …`` print() output leaks through.
        self._orig_stderr = sys.stderr
        sys.stderr = self.stderr_buf = io.StringIO()

    def tearDown(self):
        sys.stderr = self._orig_stderr
        self.conn._closed = True

    # ──────────────────────────────────────────────────────────────
    # Test A — main guard: closed conn → no warning, queue drained.
    # ──────────────────────────────────────────────────────────────
    def test_no_warning_when_closed_and_queue_drained(self):
        """Closed conn: HANDLE_DEL is not sent, no WARNING emitted,
        queue is fully drained, one DEBUG summary is logged."""

        async def run():
            # Pre-populate with several distinct id_packs (use >1
            # so the drain assertion is meaningful, per reviewer rec).
            id_packs = [
                ("builtins.method", 10665440, 6985923220734123),
                ("builtins.method", 10665440, 6985923220734124),
                ("builtins.method", 10665441, 6985923220734125),
            ]
            for ip in id_packs:
                self.conn._pending_deletions.put((ip, 1))
            self.assertEqual(self.conn._pending_deletions.qsize(), 3)

            # Simulate post-close state — same write Connection.close()
            # performs (protocol.py:769). The public reader is
            # ``conn.closed`` (property at protocol.py:788).
            self.conn._closed = True
            self.assertTrue(self.conn.closed)

            # Sentinel — if the code under test ever calls this on a
            # closed conn, the test fails loudly.
            sent = []

            async def must_not_be_called(handler, *args, **kwargs):
                sent.append((handler, args))
                return False

            self.conn._async_request_with_ack = must_not_be_called

            # Exercise.
            await self.conn._process_pending_deletions()

            # Assertions.
            self.assertEqual(
                sent, [],
                "HANDLE_DEL must NOT be dispatched on a closed conn",
            )
            self.assertEqual(
                self.stderr_buf.getvalue(), "",
                "Storm warning must not be printed to stderr "
                "(this is the regression of the HANDLE_DEL storm-after-disconnect bug)",
            )
            warns = self.cap.find(
                logging.WARNING, "Failed to delete remote object"
            )
            self.assertEqual(
                warns, [],
                "logger.warning fail-to-delete must not fire on "
                "closed conn",
            )
            # Drain: queue must be empty (prevents unbounded growth
            # across repeated cleanup_loop iterations).
            self.assertTrue(
                self.conn._pending_deletions.empty(),
                "Queue must be drained on closed-conn fast-path",
            )
            # Exactly one DEBUG summary with the dropped count.
            debugs = self.cap.find(
                logging.DEBUG, "Dropping 3 pending HANDLE_DELs"
            )
            self.assertEqual(
                len(debugs), 1,
                "Expect exactly one DEBUG summary with the count",
            )

        asyncio.run(run())

    # ──────────────────────────────────────────────────────────────
    # Test A2 — second iteration on already-drained queue is silent.
    # ──────────────────────────────────────────────────────────────
    def test_second_iteration_on_empty_queue_is_silent(self):
        """After first drain, subsequent cleanup_loop iterations on
        the still-closed connection must NOT emit any debug noise."""

        async def run():
            self.conn._pending_deletions.put(
                (("builtins.method", 10665440, 1), 1)
            )
            self.conn._closed = True
            self.conn._async_request_with_ack = Mock()

            # First iteration — drains and emits one debug.
            await self.conn._process_pending_deletions()
            first_round = list(self.cap.records)
            self.assertTrue(
                any(
                    r.levelno == logging.DEBUG
                    and "Dropping 1 pending HANDLE_DELs" in r.getMessage()
                    for r in first_round
                ),
                "First iteration must log the drop count",
            )

            # Reset capture and run again — queue is empty now.
            self.cap.records.clear()
            await self.conn._process_pending_deletions()
            self.assertEqual(
                self.cap.records, [],
                "Second iteration on empty queue must be silent "
                "(no `Dropping 0` noise on every cleanup_loop tick)",
            )
            self.assertEqual(self.stderr_buf.getvalue(), "")

        asyncio.run(run())

    # ──────────────────────────────────────────────────────────────
    # Test B — regression guard: live conn still warns on ack fail.
    # ──────────────────────────────────────────────────────────────
    def test_warning_still_fires_when_open_and_ack_fails(self):
        """Live conn with a failing ack must still emit the WARNING.

        This is the negative test that proves the fix has NOT
        regressed the documented "DO NOT REMOVE THIS LOGGING"
        behaviour for live-connection failures.
        """

        async def run():
            id_pack = ("test.LiveClass", 123, 9999999)
            self.conn._pending_deletions.put((id_pack, 1))
            # Sanity: conn is NOT closed.
            self.assertFalse(self.conn.closed)

            calls = []

            async def fail_ack(handler, *args, **kwargs):
                calls.append((handler, args))
                return False  # Real ack failure on a live conn.

            self.conn._async_request_with_ack = fail_ack

            await self.conn._process_pending_deletions()

            self.assertEqual(
                len(calls), 1,
                "On a live conn the ack call must still happen",
            )
            self.assertEqual(calls[0][0], consts.HANDLE_DEL)
            self.assertIn(
                "Failed to delete remote object",
                self.stderr_buf.getvalue(),
                "Live-conn ack failure must still print the "
                "stderr WARNING (do not regress the "
                "DO-NOT-REMOVE-THIS-LOGGING contract)",
            )
            warns = self.cap.find(
                logging.WARNING, "Failed to delete remote object"
            )
            self.assertEqual(
                len(warns), 1,
                "Live-conn ack failure must also log via logger.warning",
            )

        asyncio.run(run())

    # ──────────────────────────────────────────────────────────────
    # Test C — belt-and-braces: conn closes mid-await.
    # ──────────────────────────────────────────────────────────────
    def test_no_warning_when_closes_mid_await(self):
        """If conn closes between batch-collect and post-ack check,
        the second guard suppresses the would-be stale warning."""

        async def run():
            id_pack = ("test.RaceClass", 555, 12345)
            self.conn._pending_deletions.put((id_pack, 1))
            self.assertFalse(self.conn.closed)

            async def closes_then_fails(handler, *args, **kwargs):
                # Simulate the race: conn closes while we're
                # awaiting ack, then ack returns False.
                self.conn._closed = True
                return False

            self.conn._async_request_with_ack = closes_then_fails

            await self.conn._process_pending_deletions()

            self.assertEqual(
                self.stderr_buf.getvalue(), "",
                "Stale post-close warning must be suppressed by "
                "the post-await guard (belt-and-braces)",
            )
            warns = self.cap.find(
                logging.WARNING, "Failed to delete remote object"
            )
            self.assertEqual(
                warns, [],
                "logger.warning must also be suppressed in the "
                "mid-await close race",
            )

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
