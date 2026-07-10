"""Regression tests for per-Connection inbound dispatch backpressure
(quarantine on overload).

Bug (production, downstream application, 2026-05-16,
~12.88 GB RSS in 73 min):

    A malformed websocket client (a downstream web front-end) was caught in
    a ``while True`` loop sending RPyC requests while ignoring its
    own callbacks. The rpyc agent kept accepting every MSG_REQUEST,
    spawning a dispatch task per request, each parked on
    ``_handle_async_call:172`` (``await async_res``) waiting for a
    callback that the client would never resolve. With no upper
    bound, the agent's ``_DISPATCH_INFLIGHT`` grew to **2 024 936**
    tasks pinning **12.88 GB** before it was killed by OOM-guard.

    None of the six prior rpyc_async fixes apply: the channel is
    OPEN (``_closed = False``), the Tasks are correctly strong-
    ref'd (``_DISPATCH_INFLIGHT`` is doing its job), no Task is
    being GC'd pending. The peer is alive and well, just
    structurally broken. Existing defences assume "eventually the
    channel closes / Task transitions to terminal state" — neither
    is true here.

The fix this file guards: ``Connection._dispatch`` MUST enforce a
per-Connection cap on simultaneously-inflight inbound dispatch
tasks. Once exceeded, the Connection enters terminal quarantine —
further MSG_REQUEST silently dropped, parked dispatch tasks
cancelled, outbound ``_request_callbacks`` cleared, one ERROR
logged with full diagnostic context. Channel stays open; peer can
keep sending into the kernel buffer; we drain and drop.

Design: docs/DESIGN_INBOUND_BACKPRESSURE.md.

Robustness:
  * No real TCP / socket / RPyC handshake.
  * The Connection is built directly against a no-op channel stub
    (same ``_SilentChannel`` shape as test_dispatch_strong_ref.py).
  * MSG_REQUESTs are hand-constructed via brine.
  * The asyncio loop is the unittest one; tests are
    ``IsolatedAsyncioTestCase``.
"""
from __future__ import annotations

import asyncio
import gc
import logging
import unittest

from rpyc_async.core import brine, consts, protocol
from rpyc_async.core.protocol import Connection
from rpyc_async.core.service import VoidService


# --------------------------------------------------------------------
# Channel stub
# --------------------------------------------------------------------

class _SilentChannel:
    """Channel that never produces real frames and never errors on
    sends. Lets us drive ``_dispatch`` directly without standing up
    a socket pair."""

    def __init__(self) -> None:
        self.closed = False

    def send(self, data):
        return None

    async def asend(self, data):
        return None

    def recv(self):
        raise EOFError("stream has been closed")

    def close(self):
        self.closed = True

    def fileno(self):
        return -1

    def poll(self, timeout):
        return False


def _make_connection(test_case, *, max_inbound_inflight: int | None = None):
    """Build a stub Connection with asyncio enabled.

    ``max_inbound_inflight`` overrides the default config knob the
    fix is required to honour.
    """
    config: dict = {}
    if max_inbound_inflight is not None:
        config["max_inbound_inflight"] = max_inbound_inflight
    conn = Connection(VoidService(), _SilentChannel(), config=config)
    conn._asyncio_enabled = True
    conn._asyncio_loop = asyncio.get_event_loop()
    test_case.addCleanup(setattr, conn, "_closed", True)
    return conn


def _msg_request_bytes(seq: int, handler_id: int) -> bytes:
    """Hand-build an inbound MSG_REQUEST frame the way ``_dispatch``
    expects to read it (matches the framing in test_dispatch_strong_ref.py).
    """
    return brine.I1.pack(consts.MSG_REQUEST) + brine.dump(
        (seq, (handler_id, (consts.LABEL_TUPLE, ())))
    )


async def _drain_loop(ticks: int = 5) -> None:
    """Yield enough times for asyncio to drain pending callbacks/tasks
    after a ``call_soon_threadsafe`` round-trip."""
    for _ in range(ticks):
        await asyncio.sleep(0)


# --------------------------------------------------------------------
# Contract: per-Connection state
# --------------------------------------------------------------------

class TestQuarantineState(unittest.IsolatedAsyncioTestCase):
    """``Connection`` must expose per-conn backpressure state."""

    async def test_connection_has_inflight_counter(self) -> None:
        conn = _make_connection(self)
        self.assertTrue(
            hasattr(conn, "_inbound_inflight"),
            "Connection must expose ``_inbound_inflight`` "
            "(per-conn integer counter) — see "
            "docs/DESIGN_INBOUND_BACKPRESSURE.md.",
        )
        self.assertEqual(conn._inbound_inflight, 0)

    async def test_connection_has_quarantine_flag(self) -> None:
        conn = _make_connection(self)
        self.assertTrue(hasattr(conn, "_inbound_quarantined"))
        self.assertFalse(conn._inbound_quarantined)

    async def test_default_config_has_threshold(self) -> None:
        """``max_inbound_inflight`` must be in DEFAULT_CONFIG with the
        agreed default of 10_000."""
        self.assertIn(
            "max_inbound_inflight", protocol.DEFAULT_CONFIG,
            "DEFAULT_CONFIG must define ``max_inbound_inflight``.",
        )
        self.assertEqual(
            protocol.DEFAULT_CONFIG["max_inbound_inflight"], 10_000,
            "Per-design default threshold is 10_000.",
        )


# --------------------------------------------------------------------
# Contract: counter accounting
# --------------------------------------------------------------------

class TestCounterAccounting(unittest.IsolatedAsyncioTestCase):
    """``_inbound_inflight`` must increment on schedule, decrement on
    Task completion."""

    async def test_counter_increments_while_dispatch_parked(self) -> None:
        loop = asyncio.get_running_loop()
        conn = _make_connection(self, max_inbound_inflight=100)

        hang = loop.create_future()

        async def _hanging_handler(self_arg, *args, **kwargs):
            await hang
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _hanging_handler

        before = conn._inbound_inflight
        conn._dispatch(_msg_request_bytes(1, consts.HANDLE_ASYNC_CALL))
        await _drain_loop()
        after_park = conn._inbound_inflight

        self.assertEqual(
            after_park, before + 1,
            "scheduling one parked dispatch must increment "
            "_inbound_inflight by exactly 1",
        )

        # Release.
        if not hang.done():
            hang.set_result(None)
        await _drain_loop()
        self.assertEqual(
            conn._inbound_inflight, before,
            "counter must decrement after the Task completes",
        )

    async def test_counter_decrements_on_task_done(self) -> None:
        """Threshold of 10; let 9 dispatches complete fully, then send
        a 10th — it must be handled (counter is back to 0..1, not 10).
        """
        loop = asyncio.get_running_loop()
        conn = _make_connection(self, max_inbound_inflight=10)

        ran = 0

        async def _fast(self_arg, *args, **kwargs):
            nonlocal ran
            ran += 1
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _fast

        for i in range(9):
            conn._dispatch(_msg_request_bytes(i, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=10)

        self.assertEqual(ran, 9)
        self.assertEqual(conn._inbound_inflight, 0)
        self.assertFalse(conn._inbound_quarantined)

        conn._dispatch(_msg_request_bytes(99, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=10)
        self.assertEqual(
            ran, 10,
            "10th request must be processed — prior 9 completed and "
            "released their counter slots",
        )
        self.assertFalse(conn._inbound_quarantined)


# --------------------------------------------------------------------
# Contract: quarantine triggers and effects
# --------------------------------------------------------------------

class TestQuarantineTrigger(unittest.IsolatedAsyncioTestCase):
    """When ``_inbound_inflight`` first reaches ``max_inbound_inflight``,
    the Connection MUST transition into terminal quarantine and the
    next MSG_REQUEST MUST be dropped."""

    async def test_quarantine_drops_excess_inbound(self) -> None:
        loop = asyncio.get_running_loop()
        conn = _make_connection(self, max_inbound_inflight=10)

        hang = loop.create_future()
        handled = 0

        async def _hanging_handler(self_arg, *args, **kwargs):
            nonlocal handled
            handled += 1
            await hang
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _hanging_handler

        # Park 10 dispatch tasks (== threshold).
        for i in range(10):
            conn._dispatch(_msg_request_bytes(i, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=10)
        self.assertEqual(conn._inbound_inflight, 10)
        self.assertEqual(handled, 10)
        self.assertFalse(conn._inbound_quarantined)

        # The 11th MUST be dropped, quarantine MUST engage.
        conn._dispatch(_msg_request_bytes(10, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=10)

        self.assertTrue(
            conn._inbound_quarantined,
            "crossing the threshold must engage quarantine",
        )
        # Handler must NOT have run for the 11th — the count is
        # whatever it was before (10), modulo the cancellations from
        # quarantine entry (handled stays at 10, but counter goes to 0
        # once cancellations propagate).
        # We only assert the *new* request never reached the handler.
        # If handled became 11, the drop didn't happen.
        # NOTE: cancellation of the parked handlers can race with this
        # assertion — what matters is that ``handled`` did NOT become
        # 11 (the 11th request never started). It's still 10.
        self.assertEqual(
            handled, 10,
            "the 11th MSG_REQUEST must NOT have reached the handler",
        )

    async def test_quarantine_cancels_parked_tasks(self) -> None:
        loop = asyncio.get_running_loop()
        conn = _make_connection(self, max_inbound_inflight=5)

        hang = loop.create_future()

        async def _hanging_handler(self_arg, *args, **kwargs):
            try:
                await hang
            except asyncio.CancelledError:
                raise
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _hanging_handler

        for i in range(5):
            conn._dispatch(_msg_request_bytes(i, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=10)
        parked_count_before = conn._inbound_inflight
        self.assertEqual(parked_count_before, 5)

        # Snapshot the actual Task objects for this Connection so we
        # can verify they transition to cancelled. We find them via
        # the same per-conn scan _cleanup uses.
        my_tasks = [
            t for t in list(protocol._DISPATCH_INFLIGHT)
            if (
                getattr(t.get_coro(), "cr_frame", None) is not None
                and t.get_coro().cr_frame.f_locals.get("self") is conn
            )
        ]
        self.assertGreaterEqual(len(my_tasks), 5)
        self.assertTrue(all(not t.done() for t in my_tasks))

        # Trigger quarantine.
        conn._dispatch(_msg_request_bytes(99, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=20)

        self.assertTrue(conn._inbound_quarantined)
        self.assertTrue(
            all(t.done() or t.cancelled() for t in my_tasks),
            "parked dispatch tasks for this Connection must be "
            "cancelled at quarantine entry; alive tasks: %r" % (
                [t.get_name() for t in my_tasks if not t.done()],
            ),
        )

    async def test_quarantine_clears_request_callbacks(self) -> None:
        """Outbound AsyncResults — by definition stale once we accept
        the peer has stopped responding — must be dropped from
        ``_request_callbacks`` to release their associated AR-chain
        memory."""
        conn = _make_connection(self, max_inbound_inflight=2)

        # Populate _request_callbacks with sentinel callables.
        for seq in (1001, 1002, 1003, 1004, 1005):
            conn._request_callbacks[seq] = lambda a, b: None
        self.assertEqual(len(conn._request_callbacks), 5)

        loop = asyncio.get_running_loop()
        hang = loop.create_future()

        async def _hanging_handler(self_arg, *args, **kwargs):
            await hang
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _hanging_handler

        for i in range(2):
            conn._dispatch(_msg_request_bytes(i, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=10)
        conn._dispatch(_msg_request_bytes(2, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=10)

        self.assertTrue(conn._inbound_quarantined)
        self.assertEqual(
            len(conn._request_callbacks), 0,
            "outbound AsyncResults must be cleared on quarantine "
            "entry — they will never resolve",
        )


# --------------------------------------------------------------------
# Contract: quarantine is terminal
# --------------------------------------------------------------------

class TestQuarantineTerminal(unittest.IsolatedAsyncioTestCase):
    async def test_quarantine_is_terminal(self) -> None:
        """After quarantine, *every* subsequent MSG_REQUEST must drop,
        regardless of whether the inflight load has dropped to 0."""
        loop = asyncio.get_running_loop()
        conn = _make_connection(self, max_inbound_inflight=2)

        handled = 0

        async def _h(self_arg, *args, **kwargs):
            nonlocal handled
            handled += 1
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _h

        # Park 2 then trip the 3rd.
        # Use a hanging handler to *park* them, then trip with the fast one.
        hang = loop.create_future()

        async def _hang(self_arg, *args, **kwargs):
            await hang
            return None

        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _hang
        for i in range(2):
            conn._dispatch(_msg_request_bytes(i, consts.HANDLE_ASYNC_CALL))
        await _drain_loop()
        conn._dispatch(_msg_request_bytes(2, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=10)
        self.assertTrue(conn._inbound_quarantined)

        # Drain everything.
        if not hang.done():
            hang.set_result(None)
        await _drain_loop(ticks=20)

        # Counter must have fallen but quarantine must remain.
        self.assertTrue(conn._inbound_quarantined)

        # Switch to a fast handler that would otherwise succeed,
        # then send 100 more. None must run.
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _h
        for i in range(100):
            conn._dispatch(_msg_request_bytes(1000 + i, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=20)

        self.assertEqual(
            handled, 0,
            "quarantine is terminal — no further inbound MSG_REQUEST "
            "may reach a handler on this Connection",
        )


# --------------------------------------------------------------------
# Contract: blast radius is bounded to one Connection
# --------------------------------------------------------------------

class TestBlastRadius(unittest.IsolatedAsyncioTestCase):
    async def test_other_connections_unaffected(self) -> None:
        """Quarantining Connection A must NOT affect Connection B."""
        loop = asyncio.get_running_loop()
        conn_a = _make_connection(self, max_inbound_inflight=2)
        conn_b = _make_connection(self, max_inbound_inflight=2)

        a_handled = 0
        b_handled = 0
        hang = loop.create_future()

        async def _hang_a(self_arg, *args, **kwargs):
            nonlocal a_handled
            a_handled += 1
            await hang
            return None

        async def _fast_b(self_arg, *args, **kwargs):
            nonlocal b_handled
            b_handled += 1
            return None

        conn_a._HANDLERS = dict(conn_a._HANDLERS)
        conn_a._HANDLERS[consts.HANDLE_ASYNC_CALL] = _hang_a
        conn_b._HANDLERS = dict(conn_b._HANDLERS)
        conn_b._HANDLERS[consts.HANDLE_ASYNC_CALL] = _fast_b

        # Quarantine A.
        for i in range(2):
            conn_a._dispatch(_msg_request_bytes(i, consts.HANDLE_ASYNC_CALL))
        await _drain_loop()
        conn_a._dispatch(_msg_request_bytes(99, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=10)
        self.assertTrue(conn_a._inbound_quarantined)

        # B should work normally.
        for i in range(5):
            conn_b._dispatch(_msg_request_bytes(i, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=10)

        self.assertFalse(conn_b._inbound_quarantined)
        self.assertEqual(b_handled, 5)


# --------------------------------------------------------------------
# Contract: 0 disables
# --------------------------------------------------------------------

class TestThresholdZeroDisables(unittest.IsolatedAsyncioTestCase):
    async def test_threshold_zero_disables_cap(self) -> None:
        """``max_inbound_inflight=0`` MUST disable backpressure entirely
        (legacy behaviour for callers that want the old semantics)."""
        loop = asyncio.get_running_loop()
        conn = _make_connection(self, max_inbound_inflight=0)

        hang = loop.create_future()

        async def _hang(self_arg, *args, **kwargs):
            await hang
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _hang

        # Push well past any reasonable cap.
        N = 200
        for i in range(N):
            conn._dispatch(_msg_request_bytes(i, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=20)

        self.assertFalse(
            conn._inbound_quarantined,
            "max_inbound_inflight=0 must disable quarantine",
        )
        self.assertEqual(conn._inbound_inflight, N)

        # Cleanup so we don't leak parked tasks across tests.
        if not hang.done():
            hang.set_result(None)
        await _drain_loop(ticks=20)


# --------------------------------------------------------------------
# Contract: log-once behaviour
# --------------------------------------------------------------------

class TestLogOnce(unittest.IsolatedAsyncioTestCase):
    async def test_log_emitted_once_per_quarantine(self) -> None:
        """The diagnostic log MUST be emitted exactly once per
        Connection transition into quarantine, even under a flood of
        further dropped requests."""
        loop = asyncio.get_running_loop()

        records: list[logging.LogRecord] = []

        class _ListHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        captured_logger = logging.getLogger(
            "rpyc.test.inbound_backpressure_log_once"
        )
        captured_logger.setLevel(logging.DEBUG)
        h = _ListHandler(level=logging.DEBUG)
        captured_logger.addHandler(h)
        self.addCleanup(captured_logger.removeHandler, h)

        # Inject our logger into the conn config.
        conn = Connection(
            VoidService(),
            _SilentChannel(),
            config={
                "max_inbound_inflight": 2,
                "logger": captured_logger,
            },
        )
        conn._asyncio_enabled = True
        conn._asyncio_loop = asyncio.get_event_loop()
        self.addCleanup(setattr, conn, "_closed", True)

        hang = loop.create_future()

        async def _hang(self_arg, *args, **kwargs):
            await hang
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _hang

        for i in range(2):
            conn._dispatch(_msg_request_bytes(i, consts.HANDLE_ASYNC_CALL))
        await _drain_loop()

        for i in range(1_000):
            conn._dispatch(_msg_request_bytes(100 + i, consts.HANDLE_ASYNC_CALL))
        await _drain_loop(ticks=20)

        # Count quarantine log records (ERROR or higher mentioning
        # "quarantine" — design specifies this is logger.error).
        quarantine_records = [
            r for r in records
            if r.levelno >= logging.ERROR
            and "quarantine" in r.getMessage().lower()
        ]
        self.assertEqual(
            len(quarantine_records), 1,
            "exactly one quarantine log expected, got %d: %r" % (
                len(quarantine_records),
                [r.getMessage() for r in quarantine_records[:5]],
            ),
        )


# --------------------------------------------------------------------
# Contract: close-path regression (the helper extraction must not
# regress per-conn isolation in _cleanup)
# --------------------------------------------------------------------

class TestClosePathRegression(unittest.IsolatedAsyncioTestCase):
    async def test_close_path_only_cancels_own_tasks(self) -> None:
        """Extracting the per-conn cancel loop into
        ``_drain_inbound_dispatch`` must not cancel Tasks belonging to
        OTHER Connections."""
        loop = asyncio.get_running_loop()
        conn_a = _make_connection(self, max_inbound_inflight=0)
        conn_b = _make_connection(self, max_inbound_inflight=0)

        hang = loop.create_future()

        async def _hang(self_arg, *args, **kwargs):
            await hang
            return None

        conn_a._HANDLERS = dict(conn_a._HANDLERS)
        conn_a._HANDLERS[consts.HANDLE_ASYNC_CALL] = _hang
        conn_b._HANDLERS = dict(conn_b._HANDLERS)
        conn_b._HANDLERS[consts.HANDLE_ASYNC_CALL] = _hang

        for i in range(3):
            conn_a._dispatch(_msg_request_bytes(i, consts.HANDLE_ASYNC_CALL))
            conn_b._dispatch(_msg_request_bytes(i, consts.HANDLE_ASYNC_CALL))
        await _drain_loop()

        b_tasks_before = [
            t for t in list(protocol._DISPATCH_INFLIGHT)
            if (
                getattr(t.get_coro(), "cr_frame", None) is not None
                and t.get_coro().cr_frame.f_locals.get("self") is conn_b
            )
        ]
        self.assertEqual(len(b_tasks_before), 3)

        # Close conn_a — should cancel A's tasks only, not B's.
        conn_a._cleanup(_anyway=True)
        await _drain_loop(ticks=10)

        b_tasks_after = [
            t for t in list(protocol._DISPATCH_INFLIGHT)
            if (
                getattr(t.get_coro(), "cr_frame", None) is not None
                and t.get_coro().cr_frame.f_locals.get("self") is conn_b
            )
        ]
        self.assertTrue(
            all(not t.done() and not t.cancelled() for t in b_tasks_after),
            "conn_a._cleanup must not cancel conn_b's tasks",
        )

        # Cleanup.
        if not hang.done():
            hang.set_result(None)
        await _drain_loop(ticks=10)


if __name__ == "__main__":
    unittest.main()
