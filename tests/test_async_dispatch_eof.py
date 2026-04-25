"""Regression test: EOFError from _send inside _dispatch_request_async.

Bug (seen in production): when the remote peer disconnects,
any in-flight async handler that raises — or succeeds — reaches the final
`self._send(MSG_ASYNC_{REPLY,EXCEPTION}, ...)` on a dead channel and
EOFError propagates OUT of the coroutine. The coroutine was scheduled via
`asyncio.run_coroutine_threadsafe` and nobody awaits its Future, so Python
reports the unhandled exception as "Exception ignored in: <coroutine ...>"
on every subsequent incoming dispatch — 419k log lines and 21 GB RAM in
the post-mortem. See a related internal incident analysis (not included here).

The coroutine MUST complete normally when the channel is closed: the
peer is gone, there is nothing to reply to. No polling, no threads, no
sync code — just stop trying to write to a dead pipe.
"""
import unittest

from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService


class _ClosedChannel:
    """Minimal channel stub that mimics rpyc.core.channel.Channel but
    raises EOFError on every send — the shape a real channel takes on
    once its underlying stream is closed (see rpyc/core/stream.py:96)."""

    def __init__(self):
        self.closed = False
        self.send_call_count = 0

    def send(self, data):
        self.send_call_count += 1
        raise EOFError("stream has been closed")

    def recv(self):
        raise EOFError("stream has been closed")

    def close(self):
        self.closed = True

    def fileno(self):
        return -1

    def poll(self, timeout):
        return False


def _make_connection(test_case, channel):
    """Build a Connection bypassing normal handshake so we can exercise
    _dispatch_request_async directly. Registers a cleanup that short-
    circuits __del__/close() so GC at teardown cannot touch the dead
    channel."""
    conn = Connection(VoidService(), channel, config={})
    test_case.addCleanup(setattr, conn, "_closed", True)
    return conn


class TestAsyncDispatchEofRegression(unittest.IsolatedAsyncioTestCase):
    """The coroutine must not let EOFError escape when the channel is
    dead. It may (and should) close the connection; it must not raise."""

    async def test_send_reply_on_closed_channel_does_not_raise(self):
        """Successful handler + closed channel — the MSG_ASYNC_REPLY
        `_send` call fails. Coroutine must swallow it, not propagate."""
        channel = _ClosedChannel()
        conn = _make_connection(self, channel)
        # Replace the async-call handler with a trivial async function
        # whose return value is brine-dumpable (so _box succeeds and we
        # reach the _send on the success branch).
        from rpyc.core import consts

        async def _noop_handler(self, *args, **kwargs):
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _noop_handler

        # raw_args for HANDLE_ASYNC_CALL: (handler_id, args). args will
        # be unboxed — use an already-unboxable empty tuple shape.
        raw_args = (consts.HANDLE_ASYNC_CALL, (consts.LABEL_TUPLE, ()))

        # MUST NOT RAISE. This is the regression: today it raises EOFError
        # from inside the `else:` branch's _send.
        await conn._dispatch_request_async(seq=1, raw_args=raw_args)

        # The _send was attempted exactly once and it must not retry.
        self.assertEqual(channel.send_call_count, 1)

    async def test_send_exception_on_closed_channel_does_not_raise(self):
        """Handler raises + closed channel — the MSG_ASYNC_EXCEPTION
        `_send` inside `except:` fails. Coroutine must swallow it."""
        channel = _ClosedChannel()
        conn = _make_connection(self, channel)
        from rpyc.core import consts

        async def _raising_handler(self, *args, **kwargs):
            raise ValueError("deliberate handler failure")

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _raising_handler

        raw_args = (consts.HANDLE_ASYNC_CALL, (consts.LABEL_TUPLE, ()))

        # MUST NOT RAISE. Today: handler raises ValueError → bare
        # `except:` catches it → `_send(MSG_ASYNC_EXCEPTION, ...)` on the
        # closed channel raises EOFError → propagates out of the coroutine.
        await conn._dispatch_request_async(seq=2, raw_args=raw_args)

        self.assertEqual(channel.send_call_count, 1)

    async def test_repeated_dispatch_on_dead_channel_bounded(self):
        """Under the bug, every dispatch leaves an unawaited Future and
        the event loop logs `Exception ignored in: <coroutine ...>`. With
        the fix, N dispatches produce **at most one** doomed send: the
        first failing send marks the connection closed so subsequent
        dispatches short-circuit instead of repeating the I/O failure.
        Zero dispatches may raise."""
        channel = _ClosedChannel()
        conn = _make_connection(self, channel)
        from rpyc.core import consts

        async def _noop_handler(self, *args, **kwargs):
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _noop_handler

        raw_args = (consts.HANDLE_ASYNC_CALL, (consts.LABEL_TUPLE, ()))

        # Fire 50 dispatches back-to-back; none of them may raise.
        for seq in range(50):
            await conn._dispatch_request_async(seq=seq, raw_args=raw_args)

        # Exactly one send attempt — after the first EOFError the
        # connection self-marks closed and the rest short-circuit.
        self.assertEqual(channel.send_call_count, 1)
        self.assertTrue(conn._closed)


if __name__ == "__main__":
    unittest.main()
