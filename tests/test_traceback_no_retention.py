"""Regression tests: no traceback frame may be retained by a
long-lived object after the exception has been handled.

Production observation (observed in a downstream service, 2026-05-12,
~17.8 GB RSS in 19 hours): heap walk found 4 038 032 ``AsyncResult`` instances
all pinned by 8 076 060 ``frame`` objects whose qualname was
``_handle_async_call`` at file
``rpyc_async/core/async_handlers.py:172``. The ``Connection._request_callbacks``
dict was empty, ``_INFLIGHT`` and ``_DISPATCH_INFLIGHT`` were
empty — every prior leak channel had been closed by the fixes in
commits ``c40fb00``, ``079e80e``, ``0858bd3``, ``e039692``. But
each AR was still kept alive by the traceback chain attached to
a ``CancelledError`` raised when ``Connection._cleanup`` called
``task.cancel()`` on an in-flight dispatch task.

The retention chain was:

  Connection._last_traceback
    → tb (TracebackType)
    → tb.tb_frame (frame of _dispatch_request_async)
    → tb.tb_next.tb_frame (frame of _handle_async_call:172)
    → frame.f_locals["async_res"]  ← the AR
    → AR._conn (back to the same Connection)

This is a textbook traceback-retention bug: a long-lived object
(Connection) stores a TracebackType across the ``except:``
boundary, and the traceback's frames retain every local variable
that was on the await chain at the point of the exception. On a
busy bidirectional-async deployment with many in-flight requests
at the moment of cleanup, the frame's ``async_res`` local is one
of millions of AsyncResult objects that all get pinned
simultaneously.

The fix is universal: NO long-lived object may store a
``TracebackType``. Tracebacks are for **logging** and
**post-mortem inspection at the point of the exception**, never
for retention.
"""
from __future__ import annotations

import asyncio
import gc
import sys
import unittest
import weakref

from rpyc_async.core import consts
from rpyc_async.core.protocol import Connection
from rpyc_async.core.service import VoidService


class _SilentChannel:
    def __init__(self) -> None:
        self.closed = False
        self.sent: list = []

    def send(self, data):
        self.sent.append(data)

    async def asend(self, data):
        self.sent.append(data)

    def recv(self):
        raise EOFError("stream has been closed")

    def close(self):
        self.closed = True

    def fileno(self):
        return -1

    def poll(self, timeout):
        return False


def _make_connection(test_case):
    conn = Connection(VoidService(), _SilentChannel(), config={})
    conn._asyncio_enabled = True
    conn._asyncio_loop = asyncio.get_event_loop()
    test_case.addCleanup(setattr, conn, "_closed", True)
    return conn


class TestTracebackNoRetention(unittest.IsolatedAsyncioTestCase):

    async def test_dispatch_request_async_does_not_retain_handler_frame(
        self,
    ) -> None:
        """When ``_dispatch_request_async``'s catch-all fires,
        ``Connection._last_traceback`` MUST NOT keep the handler's
        frame alive. The frame holds every local variable from
        the await chain — most importantly any AsyncResult the
        handler was awaiting on. On 2026-05-12 this pinned
        ~4 M AsyncResult instances."""
        conn = _make_connection(self)

        async def _raiser(self_arg, *args, **kwargs):
            raise RuntimeError("synthetic")

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _raiser

        from rpyc_async.core import brine
        seq = 0xABCDE
        data = brine.I1.pack(consts.MSG_REQUEST) + brine.dump(
            (seq, (consts.HANDLE_ASYNC_CALL, (consts.LABEL_TUPLE, ())))
        )
        conn._dispatch(data)

        for _ in range(20):
            await asyncio.sleep(0)
        gc.collect()

        last_tb = getattr(conn, "_last_traceback", None)
        if last_tb is not None:
            tb = last_tb
            while tb is not None:
                qn = tb.tb_frame.f_code.co_qualname
                self.assertNotIn(
                    "_raiser", qn,
                    f"Connection._last_traceback retains the "
                    f"handler frame {qn!r}. This pins every local "
                    f"variable in that frame, including any "
                    f"AsyncResult the handler was awaiting on. "
                    f"See a related internal incident analysis (not included here)."
                )
                self.assertNotIn(
                    "_dispatch_request_async", qn,
                    f"Connection._last_traceback retains the "
                    f"dispatch frame {qn!r} — same retention bug.",
                )
                tb = tb.tb_next

    async def test_dispatch_request_sync_does_not_retain_handler_frame(
        self,
    ) -> None:
        """Same invariant for the sync ``_dispatch_request`` path
        (protocol.py:1896 catch-all). Both paths assigned
        ``self._last_traceback = tb`` and both must be fixed."""
        conn = _make_connection(self)

        def _raiser_sync(self_arg, *args, **kwargs):
            raise RuntimeError("synthetic sync")

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_CALL] = _raiser_sync

        try:
            conn._dispatch_request(
                seq=42,
                raw_args=(consts.HANDLE_CALL, (consts.LABEL_TUPLE, ())),
            )
        except Exception:
            pass
        gc.collect()

        last_tb = getattr(conn, "_last_traceback", None)
        if last_tb is not None:
            tb = last_tb
            while tb is not None:
                qn = tb.tb_frame.f_code.co_qualname
                self.assertNotIn(
                    "_raiser_sync", qn,
                    f"_last_traceback retains sync handler frame "
                    f"{qn!r} — pins every local in it.",
                )
                self.assertNotIn(
                    "_dispatch_request", qn,
                    f"_last_traceback retains sync dispatch frame "
                    f"{qn!r} — pins every local in it.",
                )
                tb = tb.tb_next

    async def test_handler_frame_locals_collectible_after_exception(
        self,
    ) -> None:
        """End-to-end: after a dispatch handler raises and its
        exception has been processed, the handler's frame MUST
        be collectible. We put a sentinel object in the handler's
        ``f_locals`` and weakref it; after dispatch finishes and a
        GC pass, the weakref must die.

        This is the strict form of the invariant. ANY retention
        anywhere in the exception-handling pipeline (Connection
        attr, closure capturing sys.exc_info, exception chain
        through __context__) shows up here as a live weakref."""
        conn = _make_connection(self)

        sentinel_wref_holder: list = []

        class _Token:
            pass

        async def _handler_with_sentinel(self_arg, *args, **kwargs):
            tok = _Token()
            sentinel_wref_holder.append(weakref.ref(tok))
            # Force ``tok`` into the frame's locals — it is already
            # there as a local variable. Raise so the exception
            # path picks up the frame's traceback.
            raise RuntimeError(f"synthetic, sentinel id={id(tok)}")

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _handler_with_sentinel

        from rpyc_async.core import brine
        seq = 0xFEEDFACE
        data = brine.I1.pack(consts.MSG_REQUEST) + brine.dump(
            (seq, (consts.HANDLE_ASYNC_CALL, (consts.LABEL_TUPLE, ())))
        )
        conn._dispatch(data)

        for _ in range(30):
            await asyncio.sleep(0)
        for _ in range(3):
            gc.collect()

        self.assertEqual(
            len(sentinel_wref_holder), 1,
            "handler did not run — test setup error",
        )
        wref = sentinel_wref_holder[0]
        self.assertIsNone(
            wref(),
            "handler frame's local sentinel is still alive after "
            "exception handling completed. Something in the "
            "exception-handling path retains the traceback (most "
            "likely Connection._last_traceback). Every retained "
            "frame pins every local in it — for production this "
            "means each cancelled dispatch leaks a full AsyncResult "
            "chain. See a related internal incident analysis (not included here)."
        )

    async def test_cancellation_does_not_retain_handler_frame(
        self,
    ) -> None:
        """The 2026-05-12 production scenario: a dispatch task is
        running inside ``_handle_async_call`` (or any other async
        handler that ``await``-s on an AsyncResult), and the
        Connection's ``_cleanup`` cancels it. The resulting
        ``CancelledError`` must NOT pin any frame after the
        exception unwinds.

        This is the canonical retention path. Even without
        ``Connection._last_traceback`` storing it, the catch-all
        in ``_dispatch_request_async`` does
        ``self._send_async_result_safe(MSG_ASYNC_EXCEPTION, seq,
        lambda: self._box_exc(t, v, tb))`` — the closure captures
        ``t, v, tb``. If the lambda outlives the dispatch
        coroutine (e.g. queued for retry, logged, kept anywhere),
        the frame chain is retained.

        Verifies via the same weakref technique."""
        conn = _make_connection(self)

        sentinel_wref_holder: list = []
        cancel_event = asyncio.Event()

        class _Token:
            pass

        async def _handler_that_parks(self_arg, *args, **kwargs):
            tok = _Token()
            sentinel_wref_holder.append(weakref.ref(tok))
            # Park here forever — emulates awaiting a never-replying RPC.
            await cancel_event.wait()
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _handler_that_parks

        from rpyc_async.core import brine
        seq = 0xC0FFEE
        data = brine.I1.pack(consts.MSG_REQUEST) + brine.dump(
            (seq, (consts.HANDLE_ASYNC_CALL, (consts.LABEL_TUPLE, ())))
        )
        conn._dispatch(data)

        # Let the dispatch task reach the inner await.
        for _ in range(10):
            await asyncio.sleep(0)

        # Cancel via _cleanup walk (this is what production does).
        # Walk _DISPATCH_INFLIGHT and cancel any task whose self is
        # this conn — mirrors Connection._cleanup's behaviour.
        from rpyc_async.core import protocol as _p
        for _task in list(_p._DISPATCH_INFLIGHT):
            try:
                _coro = _task.get_coro()
                _frame = getattr(_coro, "cr_frame", None)
                if _frame is not None and \
                   _frame.f_locals.get("self") is conn and \
                   not _task.done():
                    _task.cancel()
            except Exception:
                pass

        # Let cancellation propagate.
        for _ in range(30):
            await asyncio.sleep(0)
        for _ in range(3):
            gc.collect()

        self.assertEqual(
            len(sentinel_wref_holder), 1,
            "handler did not run — test setup error",
        )
        wref = sentinel_wref_holder[0]
        self.assertIsNone(
            wref(),
            "after _DISPATCH_INFLIGHT-driven cancellation, the "
            "handler frame's sentinel is still alive. This is the "
            "production failure mode of 2026-05-12: each cancelled "
            "dispatch pins its handler frame's f_locals — which "
            "includes any AsyncResult the handler was awaiting on. "
            "Multiply by the number of in-flight requests at "
            "cleanup time and you get the multi-GB AR leak we "
            "observed."
        )


if __name__ == "__main__":
    unittest.main()
