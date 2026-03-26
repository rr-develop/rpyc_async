"""
Simple test for debug_refcounting mode.
"""
import logging
from io import StringIO
from rpyc.lib.colls import RefCountingColl


def test_refcounting_coll_debug_mode():
    """Test RefCountingColl logs object lifecycle when debug=True."""

    # Setup logger
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.DEBUG)
    logger = logging.getLogger("test_refcount")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    # Create RefCountingColl with debug mode
    coll = RefCountingColl(logger=logger, debug=True)

    # Add some objects
    list_obj = [1, 2, 3, "hello"]
    dict_obj = {"key": "value", "number": 42}

    key1 = ("builtins.list", id(type(list_obj)), id(list_obj))
    key2 = ("builtins.dict", id(type(dict_obj)), id(dict_obj))

    coll.add(key1, list_obj)
    coll.add(key2, dict_obj)

    # Increment refcount
    coll.add(key1, list_obj)

    # Decrement refcount
    coll.decref(key1)

    # Delete object
    coll.decref(key2)

    # Get logs
    log_output = log_stream.getvalue()

    print("\n=== RefCountingColl Debug Logs ===")
    print(log_output)
    print("=" * 40)

    # Verify logs
    assert "[REFCOUNT] ADD" in log_output, "Should log ADD operations"
    assert "[1, 2, 3, 'hello']" in log_output, "Should log list repr"
    assert "key" in log_output or "value" in log_output, "Should log dict repr"
    assert "[REFCOUNT] INCREF" in log_output, "Should log INCREF operations"
    assert "[REFCOUNT] DECREF" in log_output or "[REFCOUNT] DELETE" in log_output, \
        "Should log DECREF/DELETE operations"

    logger.removeHandler(handler)


if __name__ == "__main__":
    test_refcounting_coll_debug_mode()
    print("\n✓ Test passed!")
