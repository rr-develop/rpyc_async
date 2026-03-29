"""
Test: Verify that refcount errors are ALWAYS logged to stderr,
even without logger in config.

This test artificially triggers errors to verify the fix.
"""
import unittest
import sys
import io
from rpyc.lib.colls import RefCountingColl


class TestErrorLoggingAlwaysOn(unittest.TestCase):
    """Test that errors are always logged to stderr"""

    def test_decref_missing_key_always_logged(self):
        """
        Test that DECREF on missing key is logged to stderr
        even WITHOUT logger in config.
        """
        # Create RefCountingColl WITHOUT logger
        coll = RefCountingColl(logger=None, debug=False)

        # Capture stderr
        captured_stderr = io.StringIO()
        original_stderr = sys.stderr
        sys.stderr = captured_stderr

        try:
            # Try to decref non-existent key
            key = ("test.Class", 12345, 67890)
            result = coll.decref(key, count=1)

            # Should return False (not found)
            self.assertFalse(result)

        finally:
            sys.stderr = original_stderr

        # Check stderr output
        stderr_output = captured_stderr.getvalue()

        # CRITICAL: Error MUST be in stderr even without logger!
        self.assertIn("[REFCOUNT] DECREF on missing key", stderr_output)
        self.assertIn("test.Class", stderr_output)

        print("\n✅ SUCCESS: DECREF error logged to stderr WITHOUT logger!")
        print(f"Stderr output: {stderr_output.strip()}")

    def test_decref_missing_key_with_logger(self):
        """
        Test that DECREF on missing key is ALSO logged via logger if present.
        """
        import logging
        import io

        # Create logger with capture
        log_capture = io.StringIO()
        handler = logging.StreamHandler(log_capture)
        handler.setLevel(logging.WARNING)
        logger = logging.getLogger("test_refcount")
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)

        # Create RefCountingColl WITH logger
        coll = RefCountingColl(logger=logger, debug=False)

        # Capture stderr
        captured_stderr = io.StringIO()
        original_stderr = sys.stderr
        sys.stderr = captured_stderr

        try:
            # Try to decref non-existent key
            key = ("test.Class", 12345, 67890)
            result = coll.decref(key, count=1)
            self.assertFalse(result)

        finally:
            sys.stderr = original_stderr
            logger.removeHandler(handler)

        # Check BOTH stderr AND logger
        stderr_output = captured_stderr.getvalue()
        log_output = log_capture.getvalue()

        # Error should be in BOTH places
        self.assertIn("[REFCOUNT] DECREF on missing key", stderr_output)
        self.assertIn("[REFCOUNT] DECREF on missing key", log_output)

        print("\n✅ SUCCESS: DECREF error logged to BOTH stderr AND logger!")
        print(f"Stderr: {stderr_output.strip()}")
        print(f"Logger: {log_output.strip()}")

    def test_normal_operations_no_error_messages(self):
        """
        Test that normal operations don't produce error messages.
        """
        coll = RefCountingColl(logger=None, debug=False)

        # Capture stderr
        captured_stderr = io.StringIO()
        original_stderr = sys.stderr
        sys.stderr = captured_stderr

        try:
            # Normal operations
            obj = object()
            key = ("test.Object", 1, id(obj))

            # Add object
            coll.add(key, obj)

            # Decref existing key
            result = coll.decref(key, count=1)
            self.assertTrue(result)  # Should be deleted

        finally:
            sys.stderr = original_stderr

        # Stderr should be EMPTY (no errors)
        stderr_output = captured_stderr.getvalue()
        self.assertEqual("", stderr_output)

        print("\n✅ SUCCESS: Normal operations produce no error messages!")


if __name__ == "__main__":
    unittest.main(verbosity=2)
