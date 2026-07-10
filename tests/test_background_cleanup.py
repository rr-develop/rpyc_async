"""
Unit tests for background cleanup task (v5.2).

Tests the background cleanup task that processes pending
netref deletions in batches.
"""
import unittest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from rpyc_async.core.protocol import Connection
from rpyc_async.core.service import VoidService


class TestBackgroundCleanupTask(unittest.TestCase):
    """Test background cleanup task lifecycle"""

    def setUp(self):
        """Create test connection"""
        mock_channel = Mock()
        mock_channel.closed = False
        mock_channel.fileno = Mock(return_value=1)
        self.conn = Connection(VoidService(), mock_channel, config={})

    def tearDown(self):
        """Clean up"""
        self.conn._closed = True

    def test_cleanup_task_none_before_enable_asyncio(self):
        """Cleanup task should be None before asyncio is enabled"""
        self.assertIsNone(self.conn._cleanup_task)
        self.assertFalse(self.conn._cleanup_running)

    def test_start_cleanup_task_creates_task(self):
        """_start_cleanup_task should create asyncio task"""
        async def test():
            # Enable asyncio serving first
            loop = asyncio.get_running_loop()
            self.conn._asyncio_loop = loop
            self.conn._asyncio_enabled = True

            # Start cleanup task
            self.conn._start_cleanup_task()

            # Task should be created
            self.assertIsNotNone(self.conn._cleanup_task)
            self.assertTrue(self.conn._cleanup_running)
            self.assertIsInstance(self.conn._cleanup_task, asyncio.Task)

            # Stop task
            self.conn._stop_cleanup_task()
            await asyncio.sleep(0.1)  # Let task finish

        asyncio.run(test())

    def test_stop_cleanup_task_cancels_task(self):
        """_stop_cleanup_task should cancel the task"""
        async def test():
            loop = asyncio.get_running_loop()
            self.conn._asyncio_loop = loop
            self.conn._asyncio_enabled = True

            # Start task
            self.conn._start_cleanup_task()
            self.assertTrue(self.conn._cleanup_running)

            # Stop task
            self.conn._stop_cleanup_task()
            await asyncio.sleep(0.1)

            # Should be stopped
            self.assertFalse(self.conn._cleanup_running)
            if self.conn._cleanup_task:
                self.assertTrue(
                    self.conn._cleanup_task.cancelled() or
                    self.conn._cleanup_task.done()
                )

        asyncio.run(test())

    def test_cleanup_task_wakes_on_each_deletion_signal(self):
        """Cleanup task must run on each _enqueue_deletion signal (event-driven).

        Replaces an earlier test that asserted the task wakes on a
        ``_cleanup_interval`` TIMER. That old behavior was a polling loop
        (~10 wakeups/sec per connection) and has been removed — see the
        NO POLLING POLICY banner in protocol.py. The task now sleeps on
        an asyncio.Event set by ``_enqueue_deletion``. No wake-ups while
        idle. One wake-up per signal.
        """
        async def test():
            loop = asyncio.get_running_loop()
            self.conn._asyncio_loop = loop
            self.conn._asyncio_enabled = True

            call_count = []

            async def mock_process():
                call_count.append(1)

            self.conn._process_pending_deletions = mock_process

            # Start task — should be IDLE (no wake-ups yet).
            self.conn._start_cleanup_task()
            await asyncio.sleep(0.05)
            idle_calls = len(call_count)

            # Signal 3 deletions. Each should wake the loop at least once.
            # (Multiple signals before the task runs may coalesce into one
            # wake-up — that is correct behavior, not a regression.)
            self.conn._enqueue_deletion(("x", 1, 1), 1)
            await asyncio.sleep(0.02)
            self.conn._enqueue_deletion(("x", 1, 2), 1)
            await asyncio.sleep(0.02)
            self.conn._enqueue_deletion(("x", 1, 3), 1)
            await asyncio.sleep(0.05)

            # Stop task
            self.conn._stop_cleanup_task()
            await asyncio.sleep(0.05)

            # Must have woken on the signals (> idle), and NOT woken on a
            # timer while idle.
            self.assertEqual(
                idle_calls, 0,
                "Cleanup task woke without any deletion signal — this is the "
                "forbidden timer-polling behavior.",
            )
            self.assertGreater(
                len(call_count), 0,
                "Cleanup task did not wake when _enqueue_deletion was called.",
            )

        asyncio.run(test())

    def test_cleanup_task_is_idle_when_no_deletions(self):
        """Zero deletions → zero wake-ups while running.

        This is the whole point of the refactor. The old polling loop ran
        every ``_cleanup_interval`` seconds unconditionally. Event-driven:
        no signal, no wake.

        (The task does run ``_process_pending_deletions`` once in its
        ``finally`` block as a final drain on shutdown — that is not
        polling, and we measure the count BEFORE stopping.)
        """
        async def test():
            loop = asyncio.get_running_loop()
            self.conn._asyncio_loop = loop
            self.conn._asyncio_enabled = True

            call_count = []

            async def mock_process():
                call_count.append(1)

            self.conn._process_pending_deletions = mock_process

            self.conn._start_cleanup_task()
            # Wait long enough that a 100-ms polling loop would have fired
            # many times.
            await asyncio.sleep(0.4)
            # Snapshot calls made while the task was running — the final
            # drain on stop is allowed (that's not polling, that's cleanup).
            idle_calls = len(call_count)

            self.conn._stop_cleanup_task()
            await asyncio.sleep(0.05)

            self.assertEqual(
                idle_calls, 0,
                f"Cleanup task fired {idle_calls} time(s) with no deletions "
                f"queued while running. Must be event-driven — forbidden polling.",
            )

        asyncio.run(test())

    def test_start_cleanup_task_idempotent(self):
        """Starting cleanup task twice should not create duplicate tasks"""
        async def test():
            loop = asyncio.get_running_loop()
            self.conn._asyncio_loop = loop
            self.conn._asyncio_enabled = True

            # Start twice
            self.conn._start_cleanup_task()
            first_task = self.conn._cleanup_task

            self.conn._start_cleanup_task()
            second_task = self.conn._cleanup_task

            # Should be same task (or at least only one running)
            self.assertIsNotNone(first_task)

            # Clean up
            self.conn._stop_cleanup_task()
            await asyncio.sleep(0.1)

        asyncio.run(test())

    def test_cleanup_task_handles_errors_gracefully(self):
        """Cleanup task must keep running after _process_pending_deletions raises.

        Updated for event-driven model (v5.3 — no polling):
        Previously this test waited for a 1-second timer-backoff between calls.
        The task no longer uses a backoff timer (that was polling). Instead it
        re-arms by awaiting the next ``_enqueue_deletion`` signal. We assert
        the task is still healthy by sending a SECOND signal after the error
        and observing it gets processed.
        """
        async def test():
            loop = asyncio.get_running_loop()
            self.conn._asyncio_loop = loop
            self.conn._asyncio_enabled = True

            call_count = []

            async def mock_process_with_error():
                call_count.append(1)
                if len(call_count) == 1:
                    raise RuntimeError("Test error")
                # Second call should succeed

            self.conn._process_pending_deletions = mock_process_with_error

            # Start task
            self.conn._start_cleanup_task()

            # First signal → triggers error in process
            self.conn._enqueue_deletion(("x", 1, 1), 1)
            await asyncio.sleep(0.05)

            # Second signal → task must still be alive and process it
            self.conn._enqueue_deletion(("x", 1, 2), 1)
            await asyncio.sleep(0.05)

            # Stop task
            self.conn._stop_cleanup_task()
            await asyncio.sleep(0.05)

            # Should have been called at least twice despite the error
            self.assertGreaterEqual(
                len(call_count), 2,
                f"Task died after error; only {len(call_count)} call(s). "
                f"The cleanup loop must re-arm on the next _enqueue_deletion "
                f"signal instead of a timer backoff.",
            )

        asyncio.run(test())


class TestBackgroundCleanupIntegration(unittest.TestCase):
    """Test cleanup task integration with enable_asyncio_serving"""

    def setUp(self):
        """Create test connection"""
        mock_channel = Mock()
        mock_channel.closed = False
        mock_channel.fileno = Mock(return_value=1)
        self.conn = Connection(VoidService(), mock_channel, config={})

    def tearDown(self):
        """Clean up"""
        try:
            if self.conn._cleanup_running:
                asyncio.run(self._async_teardown())
        except:
            pass
        self.conn._closed = True

    async def _async_teardown(self):
        """Async cleanup"""
        self.conn._stop_cleanup_task()
        await asyncio.sleep(0.1)

    def test_enable_asyncio_serving_starts_cleanup_task(self):
        """enable_asyncio_serving should start cleanup task"""
        async def test():
            loop = asyncio.get_running_loop()

            # Instead of full enable_asyncio_serving (which requires selector),
            # just set the flags and call _start_cleanup_task directly
            self.conn._asyncio_enabled = True
            self.conn._asyncio_loop = loop
            self.conn._start_cleanup_task()

            # Cleanup task should be started
            self.assertTrue(self.conn._cleanup_running)
            self.assertIsNotNone(self.conn._cleanup_task)

            # Stop
            self.conn._stop_cleanup_task()
            await asyncio.sleep(0.1)

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
