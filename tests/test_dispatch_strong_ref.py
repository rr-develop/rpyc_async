"""Regression tests for inbound ``_dispatch_request_async`` Task
strong-ref pin and Connection.close cleanup.

Bug (production, a downstream application incident, ~12.1 GB RAM
in 3 days):

    rpyc/core/protocol.py:1822 schedules an inbound MSG_REQUEST via

        asyncio.run_coroutine_threadsafe(
            self._dispatch_request_async(seq, args),
            self._asyncio_loop,
        )

    The returned ``concurrent.futures.Future`` is DISCARDED. The
    only strong reference to the resulting asyncio.Task is the
    event loop's internal ``_all_tasks`` weak-set (explicit weak
    reference, see
    https://docs.python.org/3/library/asyncio-task.html#asyncio.Task —
    "Save a reference to the result of this function, to avoid a
    task disappearing mid-execution").

    On a long-running busy process under bidirectional async
    traffic (e.g. a downstream application ↔ peer), GC eventually
    collects an inbound dispatch task that was parked on
    ``await handler_func(...)``. The handler's coroutine frame is
    destroyed mid-await; if the handler was itself waiting on an
    outbound AsyncResult issued via ``conn.root.method()``, that
    outbound AsyncResult is left orphan in
    ``Connection._request_callbacks``. The earlier cancel-aware
    fix on ``AsyncResult.__await__`` cannot help because the inner
    ``future`` never reaches a done state — the parent Task that
    would have completed it is gone.

    Heap dump of the affected process showed 1 941 735 leaked
    AsyncResult chains pinning 12.1 GB of Python heap. See a related
    internal incident analysis (not included here).

The fix this file guards: ``_dispatch`` must add every Task it
schedules into a module-level strong-ref set, and remove it via
``Task.add_done_callback(...)`` when the Task finishes. This is
the same pattern that protects ``fire_and_forget_async`` on the
outbound side (``rpyc.utils.helpers._INFLIGHT``).

Robustness:
  * No real TCP / socket / RPyC handshake.
  * The Connection is built directly against a no-op channel stub.
  * Tasks are explicitly scheduled and then we check the
    book-keeping set.
"""
from __future__ import annotations

import asyncio
import gc
import unittest

from rpyc.core import consts, protocol
from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService


# --------------------------------------------------------------------
# Channel stub identical to test_dispatch_task_leak_on_disconnect.py
# --------------------------------------------------------------------

class _SilentChannel:
    """Channel stub: ``send`` / ``recv`` never produce anything useful,
    ``close()`` flips the closed flag. We never read a real message
    from it; the test drives the dispatch path directly."""

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


def _make_connection(test_case):
    conn = Connection(VoidService(), _SilentChannel(), config={})
    conn._asyncio_enabled = True
    conn._asyncio_loop = asyncio.get_event_loop()
    test_case.addCleanup(setattr, conn, "_closed", True)
    return conn


# --------------------------------------------------------------------
# The contract
# --------------------------------------------------------------------

class TestDispatchStrongRef(unittest.IsolatedAsyncioTestCase):
    """``_dispatch`` MUST hold a strong reference to every Task it
    schedules via ``asyncio.run_coroutine_threadsafe`` /
    ``loop.create_task``, until that Task is done.

    This prevents the production failure mode where a GC cycle
    collects an in-flight inbound dispatch Task and leaks the
    outbound AsyncResult chain the handler was awaiting on.
    """

    async def test_dispatch_inflight_set_exists(self) -> None:
        """The contract begins with: ``protocol._DISPATCH_INFLIGHT``
        is a module-level set used to pin inbound dispatch tasks."""
        self.assertTrue(
            hasattr(protocol, "_DISPATCH_INFLIGHT"),
            "rpyc.core.protocol must expose a module-level set "
            "named _DISPATCH_INFLIGHT to hold strong refs to "
            "inbound dispatch Tasks. Without it, asyncio's "
            "_all_tasks (WeakSet) is the only reference, and a "
            "GC cycle can collect a parked dispatch Task, "
            "leaking the outbound AsyncResult chain the handler "
            "was awaiting. See "
            "a related internal incident analysis."
        )
        inflight = protocol._DISPATCH_INFLIGHT
        self.assertIsInstance(
            inflight, set,
            f"_DISPATCH_INFLIGHT must be a set, got "
            f"{type(inflight).__name__}",
        )

    async def test_dispatch_task_is_added_to_inflight_set(self) -> None:
        """While a dispatch task is parked on an inner await, it
        MUST appear in ``_DISPATCH_INFLIGHT``. Otherwise asyncio's
        WeakSet is the only reference and GC can collect it
        mid-flight."""
        loop = asyncio.get_running_loop()
        conn = _make_connection(self)

        # Hang-forever handler so the dispatch task stays parked.
        hang = loop.create_future()  # never set

        async def _hanging_handler(self_arg, *args, **kwargs):
            await hang
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _hanging_handler

        inflight = protocol._DISPATCH_INFLIGHT
        before = len(inflight)

        # Drive the actual _dispatch path with a real MSG_REQUEST
        # frame. We hand-construct the body the same way the wire
        # protocol does: brine-encoded (seq, (handler_id, args)).
        from rpyc.core import brine
        seq = 0xDEAD_BEEF
        data = brine.I1.pack(consts.MSG_REQUEST) + brine.dump(
            (seq, (consts.HANDLE_ASYNC_CALL, (consts.LABEL_TUPLE, ())))
        )
        conn._dispatch(data)

        # Yield so the scheduled coroutine reaches its inner await.
        for _ in range(5):
            await asyncio.sleep(0)

        after = len(inflight)
        try:
            self.assertGreater(
                after, before,
                "scheduling an inbound MSG_REQUEST through "
                "_dispatch must add the resulting Task to "
                "_DISPATCH_INFLIGHT (we saw before=%d after=%d)" % (
                    before, after,
                ),
            )
        finally:
            # Cleanup — cancel anything we may have left behind so
            # the test doesn't leak across runs.
            if not hang.done():
                hang.set_exception(asyncio.CancelledError())
            await asyncio.sleep(0)

    async def test_dispatch_task_is_auto_removed_when_done(self) -> None:
        """The strong ref MUST be auto-released when the Task
        finishes — otherwise we trade a silent leak for an
        unbounded set.

        Symmetry with ``helpers._INFLIGHT``: ``add_done_callback``
        runs once per Task on any terminal state. After completion
        the set must NOT still contain the Task.
        """
        loop = asyncio.get_running_loop()
        conn = _make_connection(self)

        completed = loop.create_future()

        async def _fast_handler(self_arg, *args, **kwargs):
            completed.set_result(None)
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _fast_handler

        inflight = protocol._DISPATCH_INFLIGHT
        before = len(inflight)

        from rpyc.core import brine
        seq = 0x1234
        data = brine.I1.pack(consts.MSG_REQUEST) + brine.dump(
            (seq, (consts.HANDLE_ASYNC_CALL, (consts.LABEL_TUPLE, ())))
        )
        conn._dispatch(data)

        # Let the handler complete.
        await asyncio.wait_for(completed, timeout=2.0)
        # One extra tick for asyncio to drain done-callbacks.
        for _ in range(5):
            await asyncio.sleep(0)
        gc.collect()

        self.assertEqual(
            len(inflight), before,
            "after a dispatch Task completes, it MUST have been "
            "removed from _DISPATCH_INFLIGHT via the auto-discard "
            "done-callback. _INFLIGHT contained: " + repr([
                t.get_name() for t in list(inflight)[:10]
            ]),
        )

    async def test_dispatch_does_not_call_asyncio_all_tasks(self) -> None:
        """Regression for the O(N²) dispatch livelock.

        The first cut of the strong-ref pin (commit 0858bd3) used
        a post-hoc lookup: after ``run_coroutine_threadsafe``
        scheduled the dispatch task, a callback on the event loop
        scanned ``asyncio.all_tasks()`` to find the just-created
        task by matching its coroutine's qualname and the
        ``self`` / ``seq`` locals on its frame. That's O(N) where
        N is the number of tasks in the loop. On a busy
        bidirectional-async deployment N grows linearly with the
        ``_DISPATCH_INFLIGHT`` set itself (we pin every dispatch,
        so the set IS the task list), making each new dispatch
        O(N), i.e. the whole pipeline O(N²) in the number of
        dispatches processed.

        Production observation: a downstream application
        hit livelock after ~26 minutes with N≈32 600 — every
        new MSG_REQUEST cost ~30 000 task-iterations,
        ``run_forever`` could no longer drain events fast enough
        to make progress, the process burned 60-80 % CPU producing
        zero useful work.

        The contract this test enforces: ``_dispatch`` MUST NOT
        rely on ``asyncio.all_tasks()`` to identify the task it
        just scheduled. The correct shape is to keep a direct
        handle on the Task at the moment of creation (via
        ``loop.create_task`` inside a ``call_soon_threadsafe``
        bridge if cross-thread), so pinning is O(1).
        """
        loop = asyncio.get_running_loop()
        conn = _make_connection(self)

        done_event = loop.create_future()

        async def _fast_handler(self_arg, *args, **kwargs):
            done_event.set_result(None)
            return None

        conn._HANDLERS = dict(conn._HANDLERS)
        conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _fast_handler

        # Spy on asyncio.all_tasks — if the fix uses it, this
        # counter will spike.
        import asyncio as _asy
        original_all_tasks = _asy.all_tasks
        call_count = [0]

        def counting_all_tasks(*args, **kwargs):
            call_count[0] += 1
            return original_all_tasks(*args, **kwargs)

        _asy.all_tasks = counting_all_tasks
        try:
            from rpyc.core import brine
            seq = 0xCAFE
            data = brine.I1.pack(consts.MSG_REQUEST) + brine.dump(
                (seq, (consts.HANDLE_ASYNC_CALL, (consts.LABEL_TUPLE, ())))
            )
            conn._dispatch(data)
            await asyncio.wait_for(done_event, timeout=2.0)
            # One extra tick for done-callbacks.
            for _ in range(3):
                await asyncio.sleep(0)
        finally:
            _asy.all_tasks = original_all_tasks

        self.assertEqual(
            call_count[0], 0,
            f"_dispatch called asyncio.all_tasks() {call_count[0]} "
            f"time(s) while scheduling a single MSG_REQUEST. "
            f"This is the O(N²) livelock pattern from commit "
            f"0858bd3 and a related production incident — "
            f"see a related internal incident analysis. "
            f"Pin the Task at the moment of creation instead "
            f"(loop.create_task in a call_soon_threadsafe bridge), "
            f"not by post-hoc all_tasks() scan."
        )


if __name__ == "__main__":
    unittest.main()
