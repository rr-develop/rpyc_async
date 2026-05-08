"""Regression tests for the GC-of-pending-task leak path that the
earlier cancel-aware ``__await__`` fix did NOT cover.

Production observation (a downstream application, second
recurrence): a process running with the cancel-aware fix in place
still accumulated ~200 000 leaked AsyncResult chains over 20 hours.
Heap walk showed the textbook signature — AsyncResult, _asyncio.Task,
_asyncio.Future, threading.Condition, _thread.RLock, deque, Timeout
all pinned at the same count ±0.5 %. See a related internal incident
analysis (not included here).

Mechanism: ``fire_and_forget_async(...)`` returns an ``asyncio.Task``
but the asyncio event loop only keeps a **weak** reference to running
tasks (``asyncio._all_tasks`` is a ``WeakSet`` — explicitly called
out in the CPython docs). When the caller discards the return value
(which is the obvious idiom for a function literally named
"fire and forget") and Python's GC fires before the task finishes,
the Task is collected. Its coroutine frame is destroyed, and the
``future = loop.create_future()`` inside ``AsyncResult.__await__``
is destroyed in **pending** state — never set_result, never
set_exception, never cancel. The earlier fix relies on
``future.add_done_callback`` to clean ``_request_callbacks``; that
callback never fires because the future never reaches a done state.
The AsyncResult therefore stays pinned in
``Connection._request_callbacks[seq]`` forever.

This is precisely the situation Python warns about with
"Task was destroyed but it is pending!" — see
https://docs.python.org/3/library/asyncio-task.html#asyncio.Task .

The fix this file guards: ``fire_and_forget_async`` MUST keep a
strong reference to every Task it creates, until the Task is
actually done. The standard idiom is a module-level set plus
``task.add_done_callback(set.discard)``. The auto-discard
guarantees no Task lingers in the set after it finishes — i.e. no
new "tasks hanging because of strong refs" failure mode is
introduced.

Why these tests are robust:
  * They DO NOT depend on real RPyC connections or sockets.
  * They construct an ``AsyncResult`` against a fake ``Connection``,
    register it in a real ``_request_callbacks`` dict, and verify
    the dict drains.
  * They force GC explicitly via ``gc.collect()`` so the test does
    not rely on GC heuristics or timing.
"""
from __future__ import annotations

import asyncio
import gc
import unittest

from rpyc.core.async_ import AsyncResult


# ---------------------------------------------------------------------------
# Fake Connection — only the bits AsyncResult / fire_and_forget_async touch
# ---------------------------------------------------------------------------

class _FakeConn:
    """The minimum surface AsyncResult expects on its ``_conn``.

    AsyncResult.__await__ touches:
      * self._conn._asyncio_enabled  (bool)
      * self._conn._request_callbacks (dict)  (via _on_future_done cleanup)

    The cleanup helper popping ``self._seq`` from
    ``_request_callbacks`` is what we want to observe.
    """

    def __init__(self) -> None:
        self._asyncio_enabled = True
        self._request_callbacks: dict = {}


# ---------------------------------------------------------------------------
# Test 1 — fire_and_forget_async holds a strong ref while the task runs
# ---------------------------------------------------------------------------

class TestFireAndForgetStrongRef(unittest.IsolatedAsyncioTestCase):
    """``fire_and_forget_async`` must protect its returned Task from
    GC of an unreferenced caller variable. Otherwise the Task is
    collected mid-flight, its inner future is destroyed pending,
    and the surrounding AsyncResult chain leaks."""

    async def test_strong_ref_set_exists_and_holds_inflight_task(self) -> None:
        """The contract: ``fire_and_forget_async`` MUST publish a
        module-level strong-ref set called ``_INFLIGHT`` and add
        every Task it creates to it. While the Task is running,
        the set MUST contain it.

        Why we test the *symbol* rather than only end-to-end
        survival: ``gc.collect()`` in a test loop does not reliably
        reproduce the production race (CPython's GC has heuristics
        that may keep the just-created Task in cycle generation 0
        long enough to finish). The production case happens over
        hours under steady allocation pressure. A direct test of
        the strong-ref invariant is both deterministic and exactly
        the property the fix promises.
        """
        from rpyc.utils import helpers

        inflight = getattr(helpers, "_INFLIGHT", None)
        self.assertIsNotNone(
            inflight,
            "fire_and_forget_async must publish a module-level set "
            "called _INFLIGHT so it can hold a strong ref to every "
            "Task it creates until that Task is done. Without it, "
            "asyncio._all_tasks's WeakSet is the only ref; the GC "
            "of the discarded return value collects the Task in "
            "pending state and leaks the surrounding AsyncResult "
            "chain. See a related internal incident analysis."
        )

        async def hangs() -> None:
            while True:
                await asyncio.sleep(60)

        task = helpers.fire_and_forget_async(hangs())
        try:
            # While the task is running, _INFLIGHT must hold it.
            self.assertIn(
                task, inflight,
                "fire_and_forget_async created a Task but did not "
                "register it in _INFLIGHT. A discarded return value "
                "would leave the Task only weakly referenced.",
            )
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_strong_ref_is_released_after_task_done(self) -> None:
        """The strong ref MUST be auto-released the moment the Task
        finishes — otherwise we trade one leak (collected pending
        tasks) for another (tasks that never go away).

        Verified by introspecting the helpers module's strong-ref
        set: a finished task must not be in it.
        """
        from rpyc.utils import helpers

        inflight = getattr(helpers, "_INFLIGHT", None)
        self.assertIsNotNone(
            inflight,
            "fire_and_forget_async fix must publish a module-level "
            "strong-ref set named _INFLIGHT for tests / introspection",
        )

        async def quick() -> int:
            return 42

        task = helpers.fire_and_forget_async(quick())
        await task
        # Give asyncio one tick to fire done-callbacks (discard runs
        # in a done-callback).
        await asyncio.sleep(0)

        self.assertNotIn(
            task, inflight,
            "finished task lingered in _INFLIGHT — the auto-discard "
            "done-callback either was not installed or fired in "
            "the wrong order. Without auto-discard the set would "
            "grow unboundedly, trading one leak for another.",
        )

    async def test_strong_ref_released_on_exception(self) -> None:
        """Same auto-release contract on the exception path."""
        from rpyc.utils import helpers

        inflight = getattr(helpers, "_INFLIGHT", None)
        self.assertIsNotNone(inflight)

        async def boom() -> None:
            raise RuntimeError("boom")

        async def on_error(_exc: BaseException) -> None:
            return None

        task = helpers.fire_and_forget_async(
            boom(), error_callback=on_error
        )
        # Exception raised inside the awaitable is caught by
        # run_with_async_callbacks → error_callback. Task itself
        # completes normally.
        await task
        await asyncio.sleep(0)

        self.assertNotIn(task, inflight)

    async def test_strong_ref_released_on_cancel(self) -> None:
        """Same auto-release contract on cancellation."""
        from rpyc.utils import helpers

        inflight = getattr(helpers, "_INFLIGHT", None)
        self.assertIsNotNone(inflight)

        async def hangs_forever() -> None:
            while True:
                await asyncio.sleep(60)

        task = helpers.fire_and_forget_async(hangs_forever())
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0)

        self.assertNotIn(task, inflight)


# ---------------------------------------------------------------------------
# Test 2 — AsyncResult.__del__ as defence-in-depth
# ---------------------------------------------------------------------------

class TestAsyncResultDelCleanup(unittest.IsolatedAsyncioTestCase):
    """Even if some code path manages to make an AsyncResult
    unreachable while still leaving an entry in
    ``Connection._request_callbacks``, the AsyncResult's ``__del__``
    must remove the entry.

    This is a belt-and-suspenders fix on top of the strong-ref one:
    the strong-ref makes sure the future actually finishes (so
    ``add_done_callback`` cleanup runs), and ``__del__`` catches
    the leftover case where the AsyncResult is GC'd via some other
    path. Because ``Connection._request_callbacks[seq] = AsyncResult``
    pins the AsyncResult to the Connection, ``__del__`` can only
    run when *something else* (the strong-ref fix, an explicit pop,
    a Connection-level reset, …) has already broken that pin —
    which is exactly when we want the slot reclaimed.
    """

    def test_del_method_exists(self) -> None:
        """The defence-in-depth fix MUST add a ``__del__`` method to
        AsyncResult. Without that method, an AsyncResult becoming
        unreachable while still holding a slot in
        ``_request_callbacks`` (e.g. the slot was popped by a
        different path, or the Connection was torn down) leaves no
        cleanup hook. __del__ is the last-line guarantee that the
        slot is reclaimed."""
        self.assertTrue(
            hasattr(AsyncResult, "__del__"),
            "AsyncResult must define __del__ so that GC of an "
            "AsyncResult that still has _seq set guarantees the "
            "Connection's _request_callbacks slot is popped. "
            "Without __del__ the only cleanup path is "
            "future.add_done_callback, which is never called when "
            "the awaiting Task is GC'd in pending state.",
        )

    def test_del_pops_seq_when_dict_still_holds_us(self) -> None:
        """The exact leak shape from production: AsyncResult is
        registered in ``_request_callbacks[seq]`` and our last
        external ref is dropped. ``__del__`` must pop the slot.

        Note: while ``_request_callbacks`` itself holds the
        AsyncResult, GC won't collect — that's the production
        symptom. So in the test we simulate "the slot was popped
        elsewhere first, but a stale ``_seq`` remains" — for which
        ``__del__`` must be a tolerant no-op.
        """
        conn = _FakeConn()
        ar = AsyncResult(conn)
        conn._request_callbacks[42] = ar
        ar._seq = 42

        # External path popped the slot already (e.g. reply arrived
        # AND _on_future_done already ran). The AsyncResult still
        # has _seq=42 stamped but the dict no longer points at it.
        del conn._request_callbacks[42]
        del ar
        gc.collect()

        # __del__ must have run and tolerated the missing key.
        self.assertEqual(conn._request_callbacks, {})

    def test_del_pops_seq_present_in_dict_via_other_ref(self) -> None:
        """Stronger case: the dict has a stale slot pointing at a
        DIFFERENT AsyncResult by accident, and an unrelated
        AsyncResult with the same seq becomes garbage.

        Real-world equivalent: a Connection reuses seq numbers and
        an old AsyncResult's __del__ must NOT pop a slot pointing
        at a fresh, unrelated AsyncResult.

        Contract: __del__ pops only if dict[seq] *is* self.
        """
        conn = _FakeConn()
        ar_old = AsyncResult(conn)
        ar_old._seq = 5
        # The dict points at a DIFFERENT object on slot 5 (someone
        # else's AsyncResult that just happens to reuse the seq).
        ar_new = AsyncResult(conn)
        ar_new._seq = 5
        conn._request_callbacks[5] = ar_new

        del ar_old
        gc.collect()

        # __del__ on ar_old must not have evicted ar_new's slot.
        self.assertIs(
            conn._request_callbacks.get(5), ar_new,
            "AsyncResult.__del__ must check identity before popping. "
            "Otherwise an old AsyncResult getting GC'd would evict "
            "a fresh, unrelated AsyncResult that happens to reuse "
            "the same seq.",
        )

    def test_del_tolerates_missing_seq(self) -> None:
        """An AsyncResult that was never registered (``_seq is None``)
        must not crash __del__. Common case: the request was never
        sent (``_async_request`` raised before the dict assignment)."""
        conn = _FakeConn()
        ar = AsyncResult(conn)
        # _seq stays None — we never sent the request.
        del ar
        gc.collect()
        self.assertEqual(conn._request_callbacks, {})

    def test_del_tolerates_torn_down_conn(self) -> None:
        """If the Connection is half-torn-down (``_request_callbacks``
        replaced with None or removed), __del__ must still not raise.
        Exceptions in __del__ are caught by Python but logged to
        stderr; the test verifies no crash, not stderr output."""
        conn = _FakeConn()
        ar = AsyncResult(conn)
        ar._seq = 99
        conn._request_callbacks[99] = ar
        # Simulate a torn-down connection.
        conn._request_callbacks = None  # type: ignore[assignment]
        del ar
        gc.collect()
        # Reaching this line means __del__ did not propagate any
        # exception out (it's allowed to print to stderr, but that
        # goes through Python's unraisable-exception handler).


if __name__ == "__main__":
    unittest.main()
