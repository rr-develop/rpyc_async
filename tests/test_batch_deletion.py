"""
Unit tests for batch deletion processing (v5.2).

Tests the _process_pending_deletions method that batches
multiple netref deletions and sends them with acknowledgment.
"""
import unittest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService
from rpyc.core import consts


class TestBatchDeletionProcessing(unittest.TestCase):
    """Test batch deletion processing functionality"""

    def setUp(self):
        """Create test connection"""
        mock_channel = Mock()
        mock_channel.closed = False
        mock_channel.fileno = Mock(return_value=1)
        self.conn = Connection(VoidService(), mock_channel, config={})

    def tearDown(self):
        """Clean up"""
        self.conn._closed = True

    def test_process_empty_queue(self):
        """Processing empty queue should not raise errors"""
        async def test():
            # Queue is empty
            self.assertTrue(self.conn._pending_deletions.empty())

            # Should not raise
            await self.conn._process_pending_deletions()

        asyncio.run(test())

    def test_process_single_deletion(self):
        """Should process single pending deletion"""
        async def test():
            id_pack = ("test.MyClass", 123, 456)
            refcount = 1

            # Add to queue
            self.conn._pending_deletions.put((id_pack, refcount))

            # Mock _async_request_with_ack
            sent_requests = []

            async def mock_request_with_ack(handler, *args, **kwargs):
                sent_requests.append((handler, args))
                return {"deleted": True, "id_pack": id_pack}

            self.conn._async_request_with_ack = mock_request_with_ack

            # Process
            await self.conn._process_pending_deletions()

            # Should have sent one HANDLE_DEL
            self.assertEqual(len(sent_requests), 1)
            handler, args = sent_requests[0]
            self.assertEqual(handler, consts.HANDLE_DEL)
            self.assertIn(id_pack, args)
            self.assertIn(refcount, args)

            # Queue should be empty
            self.assertTrue(self.conn._pending_deletions.empty())

        asyncio.run(test())

    def test_process_multiple_deletions_batched(self):
        """Should batch multiple deletions to same object"""
        async def test():
            id_pack = ("test.MyClass", 123, 456)

            # Add same object multiple times with different refcounts
            self.conn._pending_deletions.put((id_pack, 1))
            self.conn._pending_deletions.put((id_pack, 2))
            self.conn._pending_deletions.put((id_pack, 1))

            sent_requests = []

            async def mock_request_with_ack(handler, *args, **kwargs):
                sent_requests.append((handler, args))
                return {"deleted": True, "id_pack": id_pack}

            self.conn._async_request_with_ack = mock_request_with_ack

            # Process
            await self.conn._process_pending_deletions()

            # Should have sent ONE request with summed refcount
            self.assertEqual(len(sent_requests), 1)
            handler, args = sent_requests[0]
            self.assertEqual(handler, consts.HANDLE_DEL)

            # Total refcount should be 1+2+1=4
            self.assertIn(4, args)

        asyncio.run(test())

    def test_process_different_objects_separately(self):
        """Should process different objects as separate requests"""
        async def test():
            id_pack1 = ("test.Class1", 1, 100)
            id_pack2 = ("test.Class2", 2, 200)

            self.conn._pending_deletions.put((id_pack1, 1))
            self.conn._pending_deletions.put((id_pack2, 2))

            sent_requests = []

            async def mock_request_with_ack(handler, *args, **kwargs):
                sent_requests.append((handler, args))
                return {"deleted": True}

            self.conn._async_request_with_ack = mock_request_with_ack

            # Process
            await self.conn._process_pending_deletions()

            # Should have sent TWO requests
            self.assertEqual(len(sent_requests), 2)

        asyncio.run(test())

    def test_resurrection_check_cancels_deletion(self):
        """Should check if netref was resurrected before deletion"""
        async def test():
            id_pack = ("test.MyClass", 123, 456)

            # Queue deletion
            self.conn._pending_deletions.put((id_pack, 1))

            # Simulate resurrection - add proxy back to cache
            mock_proxy = Mock()
            self.conn._proxy_cache._dict[id_pack] = lambda: mock_proxy

            sent_requests = []

            async def mock_request_with_ack(handler, *args, **kwargs):
                sent_requests.append((handler, args))
                return {"deleted": True}

            self.conn._async_request_with_ack = mock_request_with_ack

            # Process
            await self.conn._process_pending_deletions()

            # Should NOT have sent request (resurrection detected)
            self.assertEqual(len(sent_requests), 0)

        asyncio.run(test())

    def test_failed_deletion_logged(self):
        """Failed deletion should be logged but not raise"""
        async def test():
            id_pack = ("test.MyClass", 123, 456)
            self.conn._pending_deletions.put((id_pack, 1))

            # Mock request that fails
            async def mock_request_with_ack(handler, *args, **kwargs):
                return False  # Failure

            self.conn._async_request_with_ack = mock_request_with_ack

            # Should not raise
            await self.conn._process_pending_deletions()

        asyncio.run(test())


class TestAsyncRequestWithAck(unittest.TestCase):
    """Test async request with acknowledgment"""

    def setUp(self):
        """Create test connection"""
        mock_channel = Mock()
        mock_channel.closed = False
        mock_channel.fileno = Mock(return_value=1)
        self.conn = Connection(VoidService(), mock_channel, config={})

    def tearDown(self):
        """Clean up"""
        self.conn._closed = True

    def test_async_request_with_ack_sends_and_waits(self):
        """Should send async request and wait for response"""
        async def test():
            # Mock async_request
            async_results = []

            def mock_async_request(handler, args, async_result):
                async_results.append(async_result)
                # Simulate immediate response
                async_result._is_ready = True
                async_result._obj = {"status": "ok"}

            self.conn.async_request = mock_async_request

            # Make request
            result = await self.conn._async_request_with_ack(
                consts.HANDLE_DEL,
                ("test", 1, 2),
                1
            )

            # Should have result
            self.assertEqual(result, {"status": "ok"})

        asyncio.run(test())

    def test_async_request_with_ack_timeout(self):
        """Should return False on timeout"""
        async def test():
            # Mock async_request that never completes
            def mock_async_request(handler, args, async_result):
                pass  # Never set result

            self.conn.async_request = mock_async_request

            # Make request with short timeout
            result = await self.conn._async_request_with_ack(
                consts.HANDLE_DEL,
                ("test", 1, 2),
                1,
                timeout=0.1
            )

            # Should return False (timeout)
            self.assertFalse(result)

        asyncio.run(test())


if __name__ == '__main__':
    unittest.main()
