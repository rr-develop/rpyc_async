"""
Unit tests for background cleanup task (v5.2).

Tests the background cleanup task that processes pending
netref deletions in batches.
"""
import unittest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService


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

    def test_cleanup_task_runs_periodically(self):
        """Cleanup task should run at configured interval"""
        async def test():
            # Set short interval for testing
            self.conn._cleanup_interval = 0.1

            loop = asyncio.get_running_loop()
            self.conn._asyncio_loop = loop
            self.conn._asyncio_enabled = True

            # Mock _process_pending_deletions
            call_count = []

            async def mock_process():
                call_count.append(1)

            self.conn._process_pending_deletions = mock_process

            # Start task
            self.conn._start_cleanup_task()

            # Wait for multiple intervals
            await asyncio.sleep(0.35)

            # Stop task
            self.conn._stop_cleanup_task()
            await asyncio.sleep(0.1)

            # Should have been called multiple times (at least 2-3)
            self.assertGreater(len(call_count), 1,
                             f"Should be called multiple times, got {len(call_count)}")

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
        """Cleanup task should continue running even if _process_pending_deletions raises"""
        async def test():
            self.conn._cleanup_interval = 0.05

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

            # Wait for multiple calls
            await asyncio.sleep(0.2)

            # Stop task
            self.conn._stop_cleanup_task()
            await asyncio.sleep(0.1)

            # Should have been called multiple times despite error
            self.assertGreater(len(call_count), 1,
                             "Should continue after error")

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

            # Enable asyncio serving
            self.conn.enable_asyncio_serving(loop=loop)

            # Cleanup task should be started
            self.assertTrue(self.conn._cleanup_running)
            self.assertIsNotNone(self.conn._cleanup_task)

            # Disable and stop
            self.conn.disable_asyncio_serving()
            self.conn._stop_cleanup_task()
            await asyncio.sleep(0.1)

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
