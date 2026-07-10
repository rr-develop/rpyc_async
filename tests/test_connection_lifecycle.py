"""
Unit tests for Connection netref lifecycle management (v5.2).

Tests the new infrastructure for improved netref garbage collection:
- _pending_deletions queue
- Background cleanup configuration
"""
import unittest
from queue import Queue
from unittest.mock import Mock
from rpyc_async.core.protocol import Connection
from rpyc_async.core.service import VoidService


class TestConnectionLifecycleInfrastructure(unittest.TestCase):
    """Test basic lifecycle infrastructure initialization"""

    def setUp(self):
        """Create a test connection with mocked channel"""
        # Mock channel to avoid real network I/O
        mock_channel = Mock()
        mock_channel.closed = False
        mock_channel.fileno = Mock(return_value=1)

        # Create connection
        self.conn = Connection(VoidService(), mock_channel, config={})

    def tearDown(self):
        """Clean up connection"""
        try:
            # Skip actual network cleanup since we're using mocks
            self.conn._closed = True
        except:
            pass

    def test_pending_deletions_queue_initialized(self):
        """_pending_deletions should be initialized as Queue"""
        self.assertIsInstance(
            self.conn._pending_deletions,
            Queue,
            "_pending_deletions should be a Queue"
        )

    def test_pending_deletions_queue_empty_initially(self):
        """_pending_deletions should be empty on init"""
        self.assertTrue(
            self.conn._pending_deletions.empty(),
            "_pending_deletions should be empty initially"
        )

    def test_cleanup_task_none_initially(self):
        """_cleanup_task should be None until asyncio serving enabled"""
        self.assertIsNone(
            self.conn._cleanup_task,
            "_cleanup_task should be None initially"
        )

    def test_cleanup_running_false_initially(self):
        """_cleanup_running should be False initially"""
        self.assertFalse(
            self.conn._cleanup_running,
            "_cleanup_running should be False initially"
        )

    def test_cleanup_interval_default_value(self):
        """cleanup_interval should default to 2.0 seconds"""
        self.assertEqual(
            self.conn._cleanup_interval,
            2.0,
            "Default cleanup_interval should be 2.0"
        )

    def test_cleanup_ack_timeout_default_value(self):
        """cleanup_ack_timeout should default to 5.0 seconds"""
        self.assertEqual(
            self.conn._cleanup_ack_timeout,
            5.0,
            "Default cleanup_ack_timeout should be 5.0"
        )

    def test_custom_cleanup_interval(self):
        """Should be able to set custom cleanup_interval via config"""
        mock_channel = Mock()
        mock_channel.closed = False
        mock_channel.fileno = Mock(return_value=1)

        custom_conn = Connection(
            VoidService(),
            mock_channel,
            config={"cleanup_interval": 1.5}
        )

        try:
            self.assertEqual(
                custom_conn._cleanup_interval,
                1.5,
                "Should use custom cleanup_interval"
            )
        finally:
            custom_conn._closed = True

    def test_custom_cleanup_ack_timeout(self):
        """Should be able to set custom cleanup_ack_timeout via config"""
        mock_channel = Mock()
        mock_channel.closed = False
        mock_channel.fileno = Mock(return_value=1)

        custom_conn = Connection(
            VoidService(),
            mock_channel,
            config={"cleanup_ack_timeout": 10.0}
        )

        try:
            self.assertEqual(
                custom_conn._cleanup_ack_timeout,
                10.0,
                "Should use custom cleanup_ack_timeout"
            )
        finally:
            custom_conn._closed = True

    def test_pending_deletions_queue_operations(self):
        """Should be able to put/get from _pending_deletions queue"""
        id_pack = ("test.MyClass", 123, 456789)
        refcount = 1

        # Put item in queue
        self.conn._pending_deletions.put((id_pack, refcount))

        # Queue should not be empty
        self.assertFalse(self.conn._pending_deletions.empty())

        # Get item from queue
        retrieved = self.conn._pending_deletions.get()

        self.assertEqual(retrieved, (id_pack, refcount))
        self.assertTrue(self.conn._pending_deletions.empty())

    def test_multiple_items_in_pending_deletions(self):
        """Should handle multiple pending deletions"""
        items = [
            (("test.Class1", 1, 100), 1),
            (("test.Class2", 2, 200), 2),
            (("test.Class3", 3, 300), 1),
        ]

        # Add all items
        for item in items:
            self.conn._pending_deletions.put(item)

        # Retrieve and verify
        retrieved = []
        while not self.conn._pending_deletions.empty():
            retrieved.append(self.conn._pending_deletions.get())

        self.assertEqual(retrieved, items)


class TestConnectionLifecycleWithDebugRefcounting(unittest.TestCase):
    """Test lifecycle with debug_refcounting enabled"""

    def test_debug_refcounting_config_passed_to_refcountingcoll(self):
        """debug_refcounting config should be passed to RefCountingColl"""
        import logging

        logger = logging.getLogger("test_refcount")
        logger.setLevel(logging.DEBUG)

        mock_channel = Mock()
        mock_channel.closed = False
        mock_channel.fileno = Mock(return_value=1)

        conn = Connection(
            VoidService(),
            mock_channel,
            config={
                "debug_refcounting": True,
                "logger": logger
            }
        )

        try:
            # Check that _local_objects has debug enabled
            self.assertTrue(
                conn._local_objects._debug,
                "RefCountingColl should have debug=True"
            )
            self.assertIs(
                conn._local_objects._logger,
                logger,
                "RefCountingColl should have logger reference"
            )
        finally:
            conn._closed = True


if __name__ == '__main__':
    unittest.main()
