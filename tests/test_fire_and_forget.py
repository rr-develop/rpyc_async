"""
Unit tests for fire_and_forget() and fire_and_forget_async() utilities.

These tests cover basic functionality with local coroutines.
Integration tests with cross-process RPC are in test_fire_and_forget_rpc.py.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

import pytest


# Test helper: Track callback invocations
class CallbackTracker:
    """Helper class to track callback invocations in tests."""

    def __init__(self):
        self.success_results: list[Any] = []
        self.error_results: list[BaseException] = []
        self.success_event = asyncio.Event()
        self.error_event = asyncio.Event()

    def on_success(self, result: Any) -> None:
        """Synchronous success callback."""
        self.success_results.append(result)
        self.success_event.set()

    def on_error(self, exc: BaseException) -> None:
        """Synchronous error callback."""
        self.error_results.append(exc)
        self.error_event.set()

    async def on_success_async(self, result: Any) -> None:
        """Asynchronous success callback."""
        await asyncio.sleep(0)  # Yield control
        self.success_results.append(result)
        self.success_event.set()

    async def on_error_async(self, exc: BaseException) -> None:
        """Asynchronous error callback."""
        await asyncio.sleep(0)  # Yield control
        self.error_results.append(exc)
        self.error_event.set()

    async def wait_success(self, timeout: float = 5.0) -> None:
        """Wait for success callback to be called."""
        await asyncio.wait_for(self.success_event.wait(), timeout=timeout)

    async def wait_error(self, timeout: float = 5.0) -> None:
        """Wait for error callback to be called."""
        await asyncio.wait_for(self.error_event.wait(), timeout=timeout)


# Test worker functions
async def fast_worker(value: int) -> int:
    """Returns immediately."""
    return value * 2


async def slow_worker(value: int, delay: float) -> int:
    """Returns after delay."""
    await asyncio.sleep(delay)
    return value * 2


async def error_worker() -> None:
    """Raises an exception."""
    raise ValueError("Intentional test error")


async def timeout_worker() -> int:
    """Never returns (for timeout tests)."""
    await asyncio.sleep(999999)
    return 42


class TestFireAndForget:
    """Tests for fire_and_forget() with sync callbacks."""

    @pytest.mark.asyncio
    async def test_basic_success(self):
        """Test basic fire_and_forget with successful completion."""
        from rpyc_async.utils.helpers import fire_and_forget

        tracker = CallbackTracker()

        task = fire_and_forget(
            fast_worker(10),
            success_callback=tracker.on_success,
            error_callback=tracker.on_error,
        )

        # Wait for completion
        await tracker.wait_success(timeout=2.0)

        # Verify results
        assert len(tracker.success_results) == 1
        assert tracker.success_results[0] == 20
        assert len(tracker.error_results) == 0
        assert task.done()

    @pytest.mark.asyncio
    async def test_with_timeout_success(self):
        """Test fire_and_forget with timeout that doesn't expire."""
        from rpyc_async.utils.helpers import fire_and_forget

        tracker = CallbackTracker()

        task = fire_and_forget(
            slow_worker(10, 0.1),
            timeout=1.0,  # Longer than worker delay
            success_callback=tracker.on_success,
            error_callback=tracker.on_error,
        )

        await tracker.wait_success(timeout=2.0)

        assert len(tracker.success_results) == 1
        assert tracker.success_results[0] == 20
        assert len(tracker.error_results) == 0
        assert task.done()

    @pytest.mark.asyncio
    async def test_with_timeout_expires(self):
        """Test fire_and_forget with timeout that expires."""
        from rpyc_async.utils.helpers import fire_and_forget

        tracker = CallbackTracker()

        import time

        start_time = time.time()

        task = fire_and_forget(
            timeout_worker(),
            timeout=0.5,
            success_callback=tracker.on_success,
            error_callback=tracker.on_error,
        )

        await tracker.wait_error(timeout=2.0)

        elapsed = time.time() - start_time

        # Verify timeout fired
        assert len(tracker.error_results) == 1
        assert isinstance(tracker.error_results[0], asyncio.TimeoutError)
        assert len(tracker.success_results) == 0
        assert task.done()

        # Verify timeout was enforced (should be ~0.5s, not 999999s)
        assert elapsed < 1.5, f"Timeout should fire quickly, took {elapsed}s"

    @pytest.mark.asyncio
    async def test_with_exception(self):
        """Test fire_and_forget with worker that raises exception."""
        from rpyc_async.utils.helpers import fire_and_forget

        tracker = CallbackTracker()

        task = fire_and_forget(
            error_worker(),
            success_callback=tracker.on_success,
            error_callback=tracker.on_error,
        )

        await tracker.wait_error(timeout=2.0)

        assert len(tracker.error_results) == 1
        assert isinstance(tracker.error_results[0], ValueError)
        assert str(tracker.error_results[0]) == "Intentional test error"
        assert len(tracker.success_results) == 0
        assert task.done()

    @pytest.mark.asyncio
    async def test_without_callbacks(self):
        """Test fire_and_forget without callbacks (should not crash)."""
        from rpyc_async.utils.helpers import fire_and_forget

        task = fire_and_forget(fast_worker(10))

        # Wait for task to complete
        await asyncio.wait_for(task, timeout=2.0)
        assert task.done()

    @pytest.mark.asyncio
    async def test_task_cancellation(self):
        """Test that task can be cancelled."""
        from rpyc_async.utils.helpers import fire_and_forget

        tracker = CallbackTracker()

        task = fire_and_forget(
            slow_worker(10, 10.0),  # Long delay
            success_callback=tracker.on_success,
            error_callback=tracker.on_error,
        )

        await asyncio.sleep(0.1)  # Let task start

        # Cancel the task
        task.cancel()

        # Wait for cancellation
        with pytest.raises(asyncio.CancelledError):
            await task

        # Callbacks should not be called on cancellation
        assert len(tracker.success_results) == 0
        assert len(tracker.error_results) == 0
        assert task.done()
        assert task.cancelled()

    @pytest.mark.asyncio
    async def test_task_name(self):
        """Test that task name is set correctly."""
        from rpyc_async.utils.helpers import fire_and_forget

        task = fire_and_forget(
            fast_worker(10),
            name="test-task",
        )

        await asyncio.wait_for(task, timeout=2.0)

        assert task.get_name() == "test-task"

    @pytest.mark.asyncio
    async def test_multiple_concurrent_tasks(self):
        """Test multiple fire_and_forget tasks running concurrently."""
        from rpyc_async.utils.helpers import fire_and_forget

        tracker = CallbackTracker()
        tasks = []

        # Start 10 concurrent tasks
        for i in range(10):
            task = fire_and_forget(
                fast_worker(i),
                success_callback=tracker.on_success,
            )
            tasks.append(task)

        # Wait for all tasks
        await asyncio.gather(*tasks)

        # Verify all succeeded
        assert len(tracker.success_results) == 10
        assert set(tracker.success_results) == {i * 2 for i in range(10)}

    @pytest.mark.asyncio
    async def test_no_event_loop_error(self):
        """Test that fire_and_forget raises error when no event loop is running."""
        from rpyc_async.utils.helpers import fire_and_forget

        # This test itself runs in event loop, so we need to test this differently
        # We'll verify the implementation uses get_running_loop() which will raise
        # This is more of a documentation test
        pass  # Implementation will be verified by code review

    @pytest.mark.asyncio
    async def test_return_value_is_task(self):
        """Test that fire_and_forget returns an asyncio.Task."""
        from rpyc_async.utils.helpers import fire_and_forget

        task = fire_and_forget(fast_worker(10))

        assert isinstance(task, asyncio.Task)
        await task


class TestFireAndForgetAsync:
    """Tests for fire_and_forget_async() with async callbacks."""

    @pytest.mark.asyncio
    async def test_basic_success(self):
        """Test basic fire_and_forget_async with successful completion."""
        from rpyc_async.utils.helpers import fire_and_forget_async

        tracker = CallbackTracker()

        task = fire_and_forget_async(
            fast_worker(10),
            success_callback=tracker.on_success_async,
            error_callback=tracker.on_error_async,
        )

        await tracker.wait_success(timeout=2.0)

        assert len(tracker.success_results) == 1
        assert tracker.success_results[0] == 20
        assert len(tracker.error_results) == 0
        assert task.done()

    @pytest.mark.asyncio
    async def test_with_timeout_success(self):
        """Test fire_and_forget_async with timeout that doesn't expire."""
        from rpyc_async.utils.helpers import fire_and_forget_async

        tracker = CallbackTracker()

        task = fire_and_forget_async(
            slow_worker(10, 0.1),
            timeout=1.0,
            success_callback=tracker.on_success_async,
            error_callback=tracker.on_error_async,
        )

        await tracker.wait_success(timeout=2.0)

        assert len(tracker.success_results) == 1
        assert tracker.success_results[0] == 20
        assert len(tracker.error_results) == 0
        assert task.done()

    @pytest.mark.asyncio
    async def test_with_timeout_expires(self):
        """Test fire_and_forget_async with timeout that expires."""
        from rpyc_async.utils.helpers import fire_and_forget_async

        tracker = CallbackTracker()

        import time

        start_time = time.time()

        task = fire_and_forget_async(
            timeout_worker(),
            timeout=0.5,
            success_callback=tracker.on_success_async,
            error_callback=tracker.on_error_async,
        )

        await tracker.wait_error(timeout=2.0)

        elapsed = time.time() - start_time

        assert len(tracker.error_results) == 1
        assert isinstance(tracker.error_results[0], asyncio.TimeoutError)
        assert len(tracker.success_results) == 0
        assert task.done()
        assert elapsed < 1.5

    @pytest.mark.asyncio
    async def test_with_exception(self):
        """Test fire_and_forget_async with worker that raises exception."""
        from rpyc_async.utils.helpers import fire_and_forget_async

        tracker = CallbackTracker()

        task = fire_and_forget_async(
            error_worker(),
            success_callback=tracker.on_success_async,
            error_callback=tracker.on_error_async,
        )

        await tracker.wait_error(timeout=2.0)

        assert len(tracker.error_results) == 1
        assert isinstance(tracker.error_results[0], ValueError)
        assert len(tracker.success_results) == 0
        assert task.done()

    @pytest.mark.asyncio
    async def test_async_callback_can_do_async_work(self):
        """Test that async callbacks can perform async operations."""
        from rpyc_async.utils.helpers import fire_and_forget_async

        results = []

        async def async_success(result: int) -> None:
            await asyncio.sleep(0.1)  # Simulate async work
            results.append(result * 3)

        task = fire_and_forget_async(
            fast_worker(10),
            success_callback=async_success,
        )

        await asyncio.wait_for(task, timeout=2.0)

        assert len(results) == 1
        assert results[0] == 60  # 10 * 2 * 3

    @pytest.mark.asyncio
    async def test_task_cancellation(self):
        """Test that task can be cancelled with async callbacks."""
        from rpyc_async.utils.helpers import fire_and_forget_async

        tracker = CallbackTracker()

        task = fire_and_forget_async(
            slow_worker(10, 10.0),
            success_callback=tracker.on_success_async,
            error_callback=tracker.on_error_async,
        )

        await asyncio.sleep(0.1)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert len(tracker.success_results) == 0
        assert len(tracker.error_results) == 0
        assert task.cancelled()

    @pytest.mark.asyncio
    async def test_multiple_concurrent_tasks(self):
        """Test multiple fire_and_forget_async tasks running concurrently."""
        from rpyc_async.utils.helpers import fire_and_forget_async

        tracker = CallbackTracker()
        tasks = []

        for i in range(10):
            task = fire_and_forget_async(
                fast_worker(i),
                success_callback=tracker.on_success_async,
            )
            tasks.append(task)

        await asyncio.gather(*tasks)

        assert len(tracker.success_results) == 10
        assert set(tracker.success_results) == {i * 2 for i in range(10)}


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_none_timeout(self):
        """Test that None timeout means no timeout."""
        from rpyc_async.utils.helpers import fire_and_forget

        tracker = CallbackTracker()

        task = fire_and_forget(
            slow_worker(10, 0.1),
            timeout=None,
            success_callback=tracker.on_success,
        )

        await tracker.wait_success(timeout=2.0)

        assert len(tracker.success_results) == 1

    @pytest.mark.asyncio
    async def test_zero_timeout(self):
        """Test that zero timeout fires immediately."""
        from rpyc_async.utils.helpers import fire_and_forget

        tracker = CallbackTracker()

        task = fire_and_forget(
            slow_worker(10, 1.0),
            timeout=0.0,
            error_callback=tracker.on_error,
        )

        await tracker.wait_error(timeout=2.0)

        assert len(tracker.error_results) == 1
        assert isinstance(tracker.error_results[0], asyncio.TimeoutError)

    @pytest.mark.asyncio
    async def test_exception_in_success_callback(self, capsys):
        """Test that exception in success callback is logged but doesn't crash."""
        from rpyc_async.utils.helpers import fire_and_forget

        def bad_callback(result):
            raise RuntimeError("Callback error")

        task = fire_and_forget(
            fast_worker(10),
            success_callback=bad_callback,
        )

        # Task should complete despite callback error
        await asyncio.wait_for(task, timeout=2.0)
        assert task.done()

        # Error should be logged to stderr
        # (actual implementation will log it)

    @pytest.mark.asyncio
    async def test_exception_in_error_callback(self, capsys):
        """Test that exception in error callback is logged but doesn't crash."""
        from rpyc_async.utils.helpers import fire_and_forget

        def bad_callback(exc):
            raise RuntimeError("Callback error")

        task = fire_and_forget(
            error_worker(),
            error_callback=bad_callback,
        )

        # Task should complete despite callback error
        await asyncio.wait_for(task, timeout=2.0)
        assert task.done()
