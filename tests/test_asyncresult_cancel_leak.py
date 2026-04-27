"""Regression test: an outbound async RPC whose awaiter is cancelled
must not leak its ``AsyncResult`` slot in
``Connection._request_callbacks``.

Bug (production incident, ~9.4 GB RAM in 95 minutes):

    Every ``await conn.root.foo()`` goes through
    ``AsyncResult.__await__``. That method:

      1. Builds an ``asyncio.Future``.
      2. Registers an ``on_result`` closure on the AsyncResult so the
         future is set when a reply arrives.
      3. Returns ``future.__await__()``.

    Meanwhile ``Connection._async_request`` has parked the
    ``AsyncResult`` itself in ``self._request_callbacks[seq]`` so the
    inbound dispatcher can hand the reply to the right awaiter.

    If the future is cancelled — e.g. ``asyncio.wait_for(...,
    timeout=...)`` fires, or the surrounding Task is cancelled, or
    a peer dies and we time out — nothing tells the AsyncResult /
    Connection. The slot in ``_request_callbacks`` stays occupied
    until the peer finally replies. If the peer is dead, that never
    happens; if the caller is in a tight retry loop on a dying peer
    (a downstream application's ``fire_and_forget_async`` path), the table
    grows unboundedly. A heap dump of the production process showed
    ~115 000 leaked AsyncResult chains pinning ~8 GB of Python heap.

    See a related internal incident analysis (not included here)
    for the full evidence trail.

This test asserts the END STATE the fix must guarantee: when
``await asyncio.wait_for(conn.async_request(...), timeout=tiny)``
times out, ``conn._request_callbacks`` must drop the entry within a
short grace period. The happy path — the reply arrives normally —
must continue to leave the table empty (no double-pop, no late
cleanup).
"""
import asyncio
import gc
import unittest

from rpyc.core import consts
from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService


class _SilentChannel:
    """Channel stub: ``send`` is a no-op, ``recv`` raises EOF.

    Models a TCP socket whose peer accepts our writes (so
    ``_async_request`` succeeds in registering ``_request_callbacks``)
    but never produces a reply. ``close()`` flips ``closed`` so the
    connection's local-cleanup path is reachable from the test.
    """

    def __init__(self):
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


def _make_connection(test_case):
    """Build a Connection bypassing handshake, with asyncio serving
    pretend-enabled so ``AsyncResult.__await__`` does not refuse to
    proceed."""
    conn = Connection(VoidService(), _SilentChannel(), config={})
    conn._asyncio_enabled = True
    conn._asyncio_loop = asyncio.get_event_loop()
    test_case.addCleanup(setattr, conn, "_closed", True)
    return conn


class TestAsyncResultCancelDoesNotLeak(unittest.IsolatedAsyncioTestCase):
    """``_request_callbacks`` must not retain entries whose awaiter
    was cancelled."""

    async def test_wait_for_timeout_releases_request_callbacks(self):
        """``asyncio.wait_for(async_result, timeout=...)`` that times
        out must leave ``_request_callbacks`` empty.

        Without the fix this leaves N entries (one per call). With
        the fix the table returns to size 0 after the awaiter is
        gone.
        """
        conn = _make_connection(self)
        # Sanity: empty to start with.
        self.assertEqual(len(conn._request_callbacks), 0)

        N = 10
        for _ in range(N):
            res = conn.async_request(consts.HANDLE_PING)
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(res, timeout=0.01)

        # Allow scheduled cleanups (add_done_callback or similar)
        # to run before we assert.
        for _ in range(20):
            await asyncio.sleep(0.005)
            gc.collect()
            if len(conn._request_callbacks) == 0:
                break

        self.assertEqual(
            len(conn._request_callbacks),
            0,
            f"LEAK: {len(conn._request_callbacks)} AsyncResult entries "
            f"still in _request_callbacks after {N} cancelled awaits. "
            f"They will live until the connection is closed and "
            f"accumulate one chain per cancelled outbound RPC.",
        )

    async def test_explicit_task_cancel_releases_request_callbacks(self):
        """Task-level cancel (not via wait_for) must also drain the
        table. ``wait_for`` is one path; an outer ``task.cancel()`` is
        a second equally-common one (e.g. on connection-shutdown
        cleanup), and the fix must cover both.
        """
        conn = _make_connection(self)

        async def _await_one():
            res = conn.async_request(consts.HANDLE_PING)
            await res  # parks until reply or cancel

        tasks = [asyncio.create_task(_await_one()) for _ in range(5)]

        # Let them all reach the inner await.
        await asyncio.sleep(0.02)
        self.assertEqual(
            len(conn._request_callbacks),
            5,
            "test setup: expected 5 in-flight requests",
        )

        for t in tasks:
            t.cancel()
        for t in tasks:
            with self.assertRaises(asyncio.CancelledError):
                await t

        for _ in range(20):
            await asyncio.sleep(0.005)
            gc.collect()
            if len(conn._request_callbacks) == 0:
                break

        self.assertEqual(
            len(conn._request_callbacks),
            0,
            f"LEAK on task.cancel(): "
            f"{len(conn._request_callbacks)} entries remain.",
        )

    async def test_happy_path_still_releases_request_callbacks(self):
        """The fix must not double-pop or otherwise misbehave on the
        normal reply path. After a reply arrives via
        ``_seq_request_callback``, the table is empty — if the cancel
        cleanup re-pops or asserts, this would fail.
        """
        conn = _make_connection(self)

        # async_request → res registered under some seq.
        res = conn.async_request(consts.HANDLE_PING)
        self.assertEqual(len(conn._request_callbacks), 1)
        seq = next(iter(conn._request_callbacks))

        # Simulate a reply arriving from the wire: the dispatcher
        # would normally call _seq_request_callback. We do it
        # directly to keep the test small.
        async def _await_and_reply():
            # Schedule the reply slightly after the awaiter parks.
            await asyncio.sleep(0.005)
            conn._seq_request_callback(consts.MSG_REPLY, seq, False, "pong")

        replier = asyncio.create_task(_await_and_reply())
        result = await res
        await replier

        self.assertEqual(result, "pong")
        self.assertEqual(
            len(conn._request_callbacks),
            0,
            "happy path: reply must clear the slot exactly once",
        )

    async def test_callbacks_list_cleared_on_cancel(self):
        """``AsyncResult._callbacks`` holds the ``on_result`` closure
        that closes over the (cancelled) future. If we leave that
        list populated, the AsyncResult retains ``future`` →
        ``future`` retains its frame → contextvars Context, etc. The
        whole chain stays pinned even after the slot is gone from
        ``_request_callbacks``. The fix must clear ``_callbacks``
        too.
        """
        conn = _make_connection(self)
        res = conn.async_request(consts.HANDLE_PING)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(res, timeout=0.01)

        # Allow scheduled cleanup to run.
        for _ in range(10):
            await asyncio.sleep(0.005)
            if not res._callbacks:
                break

        self.assertEqual(
            res._callbacks,
            [],
            "AsyncResult._callbacks must be cleared on cancel "
            "so the closure over the cancelled future can be GC'd",
        )


if __name__ == "__main__":
    unittest.main()
