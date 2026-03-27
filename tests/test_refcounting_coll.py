"""
Unit tests for RefCountingColl with improved lifecycle management.

Tests the new behavior where:
- Initial refcount is 1 (registry acts as strong reference)
- decref() returns deletion status
- Defensive checks prevent KeyError on missing keys
"""
import unittest
import logging
from rpyc.lib.colls import RefCountingColl


class TestRefCountingCollBasics(unittest.TestCase):
    """Test basic RefCountingColl functionality"""

    def test_initial_refcount_is_one(self):
        """Initial refcount should be 1 (registry counts as reference)"""
        coll = RefCountingColl()
        obj = object()
        key = ("test", 1, id(obj))

        coll.add(key, obj)

        # Check internal state
        self.assertIn(key, coll._dict)
        slot = coll._dict[key]
        self.assertEqual(slot[0], obj, "Object should be stored")
        self.assertEqual(slot[1], 1, "Initial refcount should be 1")

    def test_add_increments_refcount_on_second_add(self):
        """Adding same object again should increment refcount"""
        coll = RefCountingColl()
        obj = object()
        key = ("test", 1, id(obj))

        # First add - refcount should be 1
        coll.add(key, obj)
        self.assertEqual(coll._dict[key][1], 1)

        # Second add - refcount should be 2
        coll.add(key, obj)
        self.assertEqual(coll._dict[key][1], 2)

        # Third add - refcount should be 3
        coll.add(key, obj)
        self.assertEqual(coll._dict[key][1], 3)

    def test_decref_returns_deletion_status(self):
        """decref() should return True when object deleted, False otherwise"""
        coll = RefCountingColl()
        obj = object()
        key = ("test", 1, id(obj))

        coll.add(key, obj)
        # refcount = 1

        # Decref by 1 - should delete (refcount reaches 0)
        deleted = coll.decref(key, count=1)
        self.assertTrue(deleted, "Should return True when object deleted")
        self.assertNotIn(key, coll._dict, "Object should be removed")

    def test_decref_returns_false_when_not_deleted(self):
        """decref() should return False when object still has references"""
        coll = RefCountingColl()
        obj = object()
        key = ("test", 1, id(obj))

        # Add twice - refcount = 2
        coll.add(key, obj)
        coll.add(key, obj)
        self.assertEqual(coll._dict[key][1], 2)

        # Decref by 1 - should NOT delete
        deleted = coll.decref(key, count=1)
        self.assertFalse(deleted, "Should return False when object still alive")
        self.assertIn(key, coll._dict, "Object should still exist")
        self.assertEqual(coll._dict[key][1], 1, "Refcount should be 1")

        # Decref by 1 again - should delete
        deleted = coll.decref(key, count=1)
        self.assertTrue(deleted, "Should return True when object deleted")
        self.assertNotIn(key, coll._dict)

    def test_decref_on_missing_key_returns_false(self):
        """decref() on missing key should not raise, return False"""
        coll = RefCountingColl()
        key = ("test", 1, 12345)

        # Should not raise KeyError
        deleted = coll.decref(key, count=1)
        self.assertFalse(deleted, "Should return False for missing key")

    def test_decref_with_count_greater_than_refcount(self):
        """decref() with count > refcount should delete object"""
        coll = RefCountingColl()
        obj = object()
        key = ("test", 1, id(obj))

        coll.add(key, obj)
        # refcount = 1

        # Decref by 5 (greater than refcount) - should delete
        deleted = coll.decref(key, count=5)
        self.assertTrue(deleted, "Should delete when count > refcount")
        self.assertNotIn(key, coll._dict)

    def test_getitem_returns_object(self):
        """__getitem__ should return stored object"""
        coll = RefCountingColl()
        obj = object()
        key = ("test", 1, id(obj))

        coll.add(key, obj)
        retrieved = coll[key]

        self.assertIs(retrieved, obj, "Should return same object")

    def test_clear_removes_all_objects(self):
        """clear() should remove all objects"""
        coll = RefCountingColl()

        obj1 = object()
        obj2 = object()
        key1 = ("test", 1, id(obj1))
        key2 = ("test", 2, id(obj2))

        coll.add(key1, obj1)
        coll.add(key2, obj2)

        self.assertEqual(len(coll._dict), 2)

        coll.clear()
        self.assertEqual(len(coll._dict), 0)


class TestRefCountingCollWithLogging(unittest.TestCase):
    """Test RefCountingColl with debug logging enabled"""

    def setUp(self):
        """Set up logger for testing"""
        self.logger = logging.getLogger("test_refcount")
        self.logger.setLevel(logging.DEBUG)
        self.handler = logging.StreamHandler()
        self.handler.setLevel(logging.DEBUG)
        self.logger.addHandler(self.handler)

    def tearDown(self):
        """Clean up logger"""
        self.logger.removeHandler(self.handler)

    def test_debug_logging_on_add(self):
        """Debug mode should log ADD operations"""
        coll = RefCountingColl(logger=self.logger, debug=True)
        obj = [1, 2, 3]  # Use list for readable repr
        key = ("builtins.list", 1, id(obj))

        with self.assertLogs(self.logger, level='DEBUG') as cm:
            coll.add(key, obj)

        # Should log ADD
        self.assertTrue(any("[REFCOUNT] ADD" in msg for msg in cm.output))

    def test_debug_logging_on_incref(self):
        """Debug mode should log INCREF operations"""
        coll = RefCountingColl(logger=self.logger, debug=True)
        obj = [1, 2, 3]
        key = ("builtins.list", 1, id(obj))

        coll.add(key, obj)

        with self.assertLogs(self.logger, level='DEBUG') as cm:
            coll.add(key, obj)  # Second add = INCREF

        # Should log INCREF
        self.assertTrue(any("[REFCOUNT] INCREF" in msg for msg in cm.output))

    def test_debug_logging_on_decref(self):
        """Debug mode should log DECREF operations"""
        coll = RefCountingColl(logger=self.logger, debug=True)
        obj = [1, 2, 3]
        key = ("builtins.list", 1, id(obj))

        coll.add(key, obj)
        coll.add(key, obj)  # refcount = 2

        with self.assertLogs(self.logger, level='DEBUG') as cm:
            coll.decref(key, count=1)  # refcount = 1, not deleted

        # Should log DECREF
        self.assertTrue(any("[REFCOUNT] DECREF" in msg for msg in cm.output))

    def test_debug_logging_on_delete(self):
        """Debug mode should log DELETE operations"""
        coll = RefCountingColl(logger=self.logger, debug=True)
        obj = [1, 2, 3]
        key = ("builtins.list", 1, id(obj))

        coll.add(key, obj)

        with self.assertLogs(self.logger, level='DEBUG') as cm:
            coll.decref(key, count=1)  # refcount = 0, deleted

        # Should log DELETE
        self.assertTrue(any("[REFCOUNT] DELETE" in msg for msg in cm.output))


class TestRefCountingCollThreadSafety(unittest.TestCase):
    """Test thread-safety of RefCountingColl"""

    def test_concurrent_add_and_decref(self):
        """Test that concurrent operations are thread-safe"""
        import threading

        coll = RefCountingColl()
        obj = object()
        key = ("test", 1, id(obj))

        # Add initial reference
        coll.add(key, obj)

        errors = []

        def add_many():
            try:
                for _ in range(100):
                    coll.add(key, obj)
            except Exception as e:
                errors.append(e)

        def decref_many():
            try:
                for _ in range(50):
                    coll.decref(key, count=1)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=add_many),
            threading.Thread(target=add_many),
            threading.Thread(target=decref_many),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # No errors should occur
        self.assertEqual(len(errors), 0, f"Thread-safety errors: {errors}")

        # Final refcount should be predictable: 1 (initial) + 200 (adds) - 50 (decrefs) = 151
        if key in coll._dict:
            self.assertEqual(coll._dict[key][1], 151)


class TestRefCountingCollEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""

    def test_object_with_broken_repr(self):
        """Objects with broken __repr__ should be handled gracefully"""

        class BrokenRepr:
            def __repr__(self):
                raise RuntimeError("Broken repr")

        logger = logging.getLogger("test_broken_repr")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        logger.addHandler(handler)

        coll = RefCountingColl(logger=logger, debug=True)
        obj = BrokenRepr()
        key = ("test", 1, id(obj))

        # Should not raise - fallback to <ClassName at 0x...>
        try:
            with self.assertLogs(logger, level='DEBUG'):
                coll.add(key, obj)
        finally:
            logger.removeHandler(handler)

    def test_very_long_repr_is_truncated(self):
        """Very long repr should be truncated to 200 chars"""

        class LongRepr:
            def __repr__(self):
                return "x" * 500  # 500 chars

        logger = logging.getLogger("test_long_repr")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        logger.addHandler(handler)

        coll = RefCountingColl(logger=logger, debug=True)
        obj = LongRepr()
        key = ("test", 1, id(obj))

        try:
            with self.assertLogs(logger, level='DEBUG') as cm:
                coll.add(key, obj)

            # Check truncation
            log_msg = cm.output[0]
            self.assertIn("...", log_msg, "Long repr should be truncated")
            # Repr in log should be ~200 chars, not 500
        finally:
            logger.removeHandler(handler)

    def test_decref_on_empty_collection(self):
        """decref on empty collection should return False"""
        coll = RefCountingColl()
        key = ("test", 1, 12345)

        deleted = coll.decref(key, count=1)
        self.assertFalse(deleted)

    def test_multiple_objects_independence(self):
        """Multiple objects should have independent refcounts"""
        coll = RefCountingColl()

        obj1 = object()
        obj2 = object()
        key1 = ("test", 1, id(obj1))
        key2 = ("test", 2, id(obj2))

        coll.add(key1, obj1)
        coll.add(key1, obj1)  # refcount = 2
        coll.add(key2, obj2)  # refcount = 1

        # Decref obj1 once
        deleted1 = coll.decref(key1, count=1)
        self.assertFalse(deleted1, "obj1 should still exist")
        self.assertEqual(coll._dict[key1][1], 1)

        # obj2 should be unaffected
        self.assertEqual(coll._dict[key2][1], 1)

        # Delete obj2
        deleted2 = coll.decref(key2, count=1)
        self.assertTrue(deleted2, "obj2 should be deleted")
        self.assertNotIn(key2, coll._dict)

        # obj1 should still exist
        self.assertIn(key1, coll._dict)


if __name__ == '__main__':
    unittest.main()
