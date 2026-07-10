"""Regression test: pending ``_dispatch_request_async`` tasks must not
accumulate when the peer disconnects mid-handler.

Bug (production, downstream application, 2026-04-25 incident, ~14.4 GB RAM in
6 hours):

    Each incoming MSG_REQUEST/MSG_ASYNC_REQUEST schedules
    ``_dispatch_request_async`` via
    ``asyncio.run_coroutine_threadsafe(..., self._asyncio_loop)``
    inside ``_dispatch`` (protocol.py:1822). The returned Future is
    discarded — the only strong reference to the resulting Task is the
    event-loop internal ``_all_tasks`` weak-ref set, which keeps the
    Task alive only as long as the coroutine has not finished.

    If the handler awaits something that never completes (e.g.
    a netref reverse-call to a peer that has just died, or an
    ``asyncio.Future`` whose setter was on the dead peer's side),
    the Task is parked on that inner await forever — no error
    propagates, no cancellation is requested, no Connection.close()
    cancels it. ``_request_callbacks`` (the ``{seq: AsyncResult}``
    registry) is unrelated; the leak is purely on the SERVER side
    of the request/response — incoming dispatch tasks parked on
    inner awaits of dead peers.

    Stack-snapshot of the pathological process showed 869 012
    pending tasks, every single one stopped at
    ``protocol.py:1723 res = await handler_func(self, *args)`` with
    ``wait_for=<Future pending cb=[Task.task_wakeup()]>``.

The fix space (multiple options under discussion, NOT in this test
file):

  1. Track every dispatch task on ``self._dispatch_tasks`` and
     ``cancel()`` them all in ``Connection.close()`` /
     ``aclose()``.
  2. Wrap the inner await in ``asyncio.wait_for(...,
     timeout=self._config['async_dispatch_timeout'])``.
  3. Cancel any unfinished dispatch tasks when the channel observes
     EOF on read.

This test asserts the END STATE that any of those fixes must
deliver: after a peer disconnects, the count of pending
``_dispatch_request_async`` tasks belonging to the dead Connection
must drop to zero within a short grace period.
"""
import asyncio
import gc
import unittest
import weakref

from rpyc_async.core.protocol import Connection
from rpyc_async.core.service import VoidService


class _BlockingChannel:
    """Channel stub: send/recv both park forever on a Future that is
    never set. ``close()`` flips ``closed`` and resolves the Future
    with EOFError, so any handler awaiting our send/recv unblocks
    via the exception path.

    This models a TCP socket whose peer has just been SIGKILL'd: the
    local side does not yet know the peer is dead, but the moment we
    notice (e.g. read returns 0) we fail every pending I/O.
    """

    def __init__(self, loop):
        self._loop = loop
        self._eof = loop.create_future()
        self.closed = False

    def send(self, data):
        return None

    async def asend(self, data):
        # No-op send so handler success path doesn't itself fail
        return None

    def recv(self):
        raise EOFError("stream has been closed")

    def close(self):
        self.closed = True
        if not self._eof.done():
            self._eof.set_exception(EOFError("stream has been closed"))

    def fileno(self):
        return -1

    def poll(self, timeout):
        return False


def _make_connection(test_case, channel):
    """Build a Connection bypassing handshake."""
    conn = Connection(VoidService(), channel, config={})
    # Pretend asyncio serving is enabled so _dispatch_request_async
    # doesn't bail; assign a real loop too.
    conn._asyncio_enabled = True
    conn._asyncio_loop = asyncio.get_event_loop()
    test_case.addCleanup(setattr, conn, "_closed", True)
    return conn


def _count_pending_dispatch_tasks(conn):
    """Count asyncio Tasks currently parked on
    ``Connection._dispatch_request_async`` for the given conn."""
    n = 0
    for task in asyncio.all_tasks():
        if task.done():
            continue
        try:
            coro = task.get_coro()
        except Exception:
            continue
        # Match by qualname AND by self-binding to OUR connection
        qn = getattr(getattr(coro, "cr_code", None), "co_qualname", "")
        if "_dispatch_request_async" not in qn:
            continue
        # Walk the coroutine frame's f_locals to find ``self``
        frame = getattr(coro, "cr_frame", None)
        if frame is None:
            continue
        if frame.f_locals.get("self") is conn:
            n += 1
    return n


class TestDispatchTaskLeakOnDisconnect(unittest.IsolatedAsyncioTestCase):
    """Pending ``_dispatch_request_async`` tasks must NOT outlive the
    Connection they belong to."""

    @unittest.expectedFailure
    async def test_pending_handler_is_cancelled_on_close(self):
        """Schedule N dispatch tasks whose handlers park on a Future
        that no one will ever set. Then ``conn.close()`` (or
        ``aclose()``). Within a grace period, every such Task must
        leave ``asyncio.all_tasks()`` (cancelled or finished).

        Today this test FAILS: closing the connection does not cancel
        in-flight dispatch tasks, so after close they remain pending
        forever. This is the leak.

        Marked ``expectedFailure`` because the fix is not yet
        implemented (see the module docstring's "fix space"). When the
        leak is fixed this becomes an unexpected success (XPASS), which
        is the signal to remove this decorator.
        """
        loop = asyncio.get_running_loop()
        channel = _BlockingChannel(loop)
        conn = _make_connection(self, channel)

        # Hang-forever handler. Each dispatched request parks here
        # until someone cancels its enclosing Task.
        from rpyc_async.core import consts
        hang_signal = loop.create_future()  # never set

        async def _hanging_handler(self, *args, **kwargs):
            # Park indefinitely.
            await hang_signal
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _hanging_handler

        # Schedule N dispatch tasks the same way the real server does:
        # via run_coroutine_threadsafe on this loop. (Using
        # loop.create_task instead also reproduces; the bug is not in
        # the scheduling primitive.)
        N = 10
        scheduled = []
        for seq in range(1, N + 1):
            raw_args = (consts.HANDLE_ASYNC_CALL, (consts.LABEL_TUPLE, ()))
            t = loop.create_task(
                conn._dispatch_request_async(seq=seq, raw_args=raw_args)
            )
            scheduled.append(t)

        # Yield so they all reach the inner await.
        await asyncio.sleep(0.05)

        pending_before_close = _count_pending_dispatch_tasks(conn)
        self.assertEqual(
            pending_before_close, N,
            f"expected all {N} dispatch tasks parked on handler; "
            f"got {pending_before_close}"
        )

        # Now sever the connection — this is what
        # ``aclose()`` / ``close()`` is supposed to do for cleanup.
        # We use the explicit ``_closed = True`` flag (which the rest
        # of protocol.py honours) plus channel.close() which flips
        # the EOF future.
        conn._closed = True
        channel.close()

        # Give the loop a few ticks to propagate cancellation /
        # cleanup. A correct implementation cancels the dispatch
        # tasks on close so they leave all_tasks(). 0.5 s is generous
        # — a real fix would be near-instant.
        for _ in range(20):
            await asyncio.sleep(0.025)
            gc.collect()
            if _count_pending_dispatch_tasks(conn) == 0:
                break

        pending_after_close = _count_pending_dispatch_tasks(conn)

        # ASSERTION OF THE FIX: no leftover dispatch tasks for a
        # closed connection. Today this is N (== 10) because
        # nothing cancels them.
        self.assertEqual(
            pending_after_close, 0,
            f"LEAK: {pending_after_close} dispatch tasks still pending "
            f"after Connection.close()/_closed=True. They will live "
            f"as long as the event loop runs and accumulate every "
            f"time a peer disconnects mid-handler."
        )

        # Belt and suspenders: also assert the scheduled tasks
        # themselves are done.
        for t in scheduled:
            if not t.done():
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, EOFError, Exception):
                    pass


if __name__ == "__main__":
    unittest.main()
