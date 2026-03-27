"""
Unit tests for netref cleanup callbacks (v5.2).

Tests the weak reference callback mechanism that queues
netref deletions for background cleanup instead of blocking
in __del__.
"""
import unittest
import gc
import weakref
from unittest.mock import Mock, patch, MagicMock
from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService
from rpyc.core import consts, netref


class TestNetrefCleanupCallbacks(unittest.TestCase):
    """Test weak reference cleanup callback registration"""

    def setUp(self):
        """Create test connection with mocked channel"""
        mock_channel = Mock()
        mock_channel.closed = False
        mock_channel.fileno = Mock(return_value=1)
        self.conn = Connection(VoidService(), mock_channel, config={})

        # Mock _netref_factory to avoid actual RPC calls
        self.original_netref_factory = self.conn._netref_factory
        self.conn._netref_factory = self._mock_netref_factory

    def _mock_netref_factory(self, id_pack):
        """Create a simple netref without RPC calls"""
        # Create a minimal netref-like object
        proxy = netref.BaseNetref(self.conn, id_pack)
        return proxy

    def tearDown(self):
        """Clean up"""
        self.conn._closed = True

    def test_unbox_registers_finalizer_for_new_netref(self):
        """_unbox should register weakref.finalize for new netrefs"""
        # Create a LABEL_REMOTE_REF package
        id_pack = ("test.MyClass", 123, 456789)
        package = (consts.LABEL_REMOTE_REF, (*id_pack, consts.FLAGS_SYNC))

        # Unbox should create netref and register finalizer
        proxy = self.conn._unbox(package)

        # Proxy should be created
        self.assertIsNotNone(proxy)

        # Check that proxy is in cache
        self.assertIn(id_pack, self.conn._proxy_cache._dict)

        # Initially, pending_deletions should be empty
        self.assertTrue(self.conn._pending_deletions.empty())

        # Delete proxy - this should trigger finalizer
        del proxy
        gc.collect()

        # Now pending_deletions should have an entry
        self.assertFalse(
            self.conn._pending_deletions.empty(),
            "Finalizer should queue deletion"
        )

        # Get the queued deletion
        queued_id_pack, refcount = self.conn._pending_deletions.get()
        self.assertEqual(queued_id_pack, id_pack)
        self.assertEqual(refcount, 1, "Initial refcount should be 1")

    def test_cached_netref_does_not_register_duplicate_finalizer(self):
        """Cached netref should not register duplicate finalizers"""
        id_pack = ("test.MyClass", 123, 456789)
        package = (consts.LABEL_REMOTE_REF, (*id_pack, consts.FLAGS_SYNC))

        # First unbox - creates netref and registers finalizer
        proxy1 = self.conn._unbox(package)

        # Keep a reference so proxy doesn't get collected
        self.assertIsNotNone(proxy1)

        # Second unbox - should return cached proxy
        proxy2 = self.conn._unbox(package)

        # Should be same object
        self.assertIs(proxy1, proxy2, "Should return cached proxy")

        # Refcount should be incremented
        self.assertEqual(proxy1.____refcount__, 2)

        # Delete first reference
        del proxy1
        gc.collect()

        # pending_deletions should still be empty (proxy2 still alive)
        self.assertTrue(
            self.conn._pending_deletions.empty(),
            "Deletion should not be queued while proxy still referenced"
        )

        # Delete second reference
        del proxy2
        gc.collect()

        # Now deletion should be queued with final refcount=2
        self.assertFalse(self.conn._pending_deletions.empty())
        queued_id_pack, refcount = self.conn._pending_deletions.get()
        self.assertEqual(queued_id_pack, id_pack)
        self.assertEqual(refcount, 2, "Final refcount should be 2")

    def test_finalizer_captures_correct_refcount(self):
        """Finalizer should capture the final refcount value"""
        id_pack = ("test.MyClass", 123, 456789)
        package = (consts.LABEL_REMOTE_REF, (*id_pack, consts.FLAGS_SYNC))

        # Create and cache proxy multiple times
        proxy = self.conn._unbox(package)
        proxy = self.conn._unbox(package)
        proxy = self.conn._unbox(package)

        # Refcount should be 3
        self.assertEqual(proxy.____refcount__, 3)

        # Delete proxy
        del proxy
        gc.collect()

        # Check queued deletion has correct refcount
        queued_id_pack, refcount = self.conn._pending_deletions.get()
        self.assertEqual(refcount, 3, "Should capture final refcount of 3")

    def test_multiple_different_netrefs_queue_independently(self):
        """Multiple different netrefs should queue deletions independently"""
        id_pack1 = ("test.Class1", 1, 100)
        id_pack2 = ("test.Class2", 2, 200)

        package1 = (consts.LABEL_REMOTE_REF, (*id_pack1, consts.FLAGS_SYNC))
        package2 = (consts.LABEL_REMOTE_REF, (*id_pack2, consts.FLAGS_SYNC))

        # Create two netrefs
        proxy1 = self.conn._unbox(package1)
        proxy2 = self.conn._unbox(package2)

        # Delete first proxy
        del proxy1
        gc.collect()

        # Should have one queued deletion
        self.assertEqual(self.conn._pending_deletions.qsize(), 1)

        # Delete second proxy
        del proxy2
        gc.collect()

        # Should have two queued deletions
        self.assertEqual(self.conn._pending_deletions.qsize(), 2)

        # Check both are queued correctly
        deletions = []
        while not self.conn._pending_deletions.empty():
            deletions.append(self.conn._pending_deletions.get())

        deletion_ids = [d[0] for d in deletions]
        self.assertIn(id_pack1, deletion_ids)
        self.assertIn(id_pack2, deletion_ids)

    def test_netref_for_async_object_queues_deletion(self):
        """Async netrefs should also queue deletions properly"""
        id_pack = ("test.AsyncClass", 123, 456789)
        package = (consts.LABEL_REMOTE_REF, (*id_pack, consts.FLAGS_ASYNC))

        proxy = self.conn._unbox(package)

        # Verify async flag is set
        self.assertTrue(proxy.____is_async__)

        # Delete proxy
        del proxy
        gc.collect()

        # Should be queued
        self.assertFalse(self.conn._pending_deletions.empty())
        queued_id_pack, refcount = self.conn._pending_deletions.get()
        self.assertEqual(queued_id_pack, id_pack)

    def test_local_object_passback_does_not_queue_deletion(self):
        """When LABEL_REMOTE_REF points to local object, should not queue deletion"""
        # Add object to local_objects first
        obj = object()
        id_pack = ("test.LocalClass", 123, id(obj))
        self.conn._local_objects.add(id_pack, obj)

        # Create LABEL_REMOTE_REF pointing to this local object
        package = (consts.LABEL_REMOTE_REF, (*id_pack, consts.FLAGS_SYNC))

        # Unbox should return local object, not create proxy
        result = self.conn._unbox(package)
        self.assertIs(result, obj, "Should return local object")

        # No proxy should be in cache
        self.assertNotIn(id_pack, self.conn._proxy_cache._dict)

        # No deletion should be queued
        self.assertTrue(self.conn._pending_deletions.empty())


class TestNetrefCleanupCallbackEdgeCases(unittest.TestCase):
    """Test edge cases in cleanup callback mechanism"""

    def setUp(self):
        """Create test connection"""
        mock_channel = Mock()
        mock_channel.closed = False
        mock_channel.fileno = Mock(return_value=1)
        self.conn = Connection(VoidService(), mock_channel, config={})

    def tearDown(self):
        """Clean up"""
        self.conn._closed = True


        # Should have ~30 queued deletions (3 threads * 10 each)
        self.assertGreater(self.conn._pending_deletions.qsize(), 0)


if __name__ == '__main__':
    unittest.main()
