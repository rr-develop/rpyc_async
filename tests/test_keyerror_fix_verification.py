"""
VERIFICATION TEST: Confirm KeyError fix works correctly.

This test verifies that the fix prevents KeyError by:
1. Using LABEL_REMOTE_REF fallback in _box() when object is missing
2. Providing better error message in _unbox() if it still happens
"""
import pytest
import socket

import rpyc
from rpyc.core import consts
from rpyc.core.protocol import Connection
from rpyc.core.channel import Channel
from rpyc.core.netref import BaseNetref
from rpyc.lib import get_id_pack


def test_box_uses_remote_ref_fallback_when_object_missing():
    """
    VERIFICATION: _box() should use LABEL_REMOTE_REF when object missing from _local_objects.

    This prevents KeyError by detecting missing objects at boxing time.
    """
    class ClientData:
        def __init__(self, value):
            self.value = value

    obj = ClientData("test123")

    # Create a minimal Connection for testing
    server_sock, client_sock = socket.socketpair()
    chan = Channel(client_sock)
    config = {"allow_public_attrs": True}
    conn = Connection(rpyc.SlaveService, chan, config=config)

    try:
        # Add object to _local_objects
        id_pack = get_id_pack(obj)
        conn._local_objects.add(id_pack, obj)

        # Create a netref to this object
        # (In real scenario, this would be a netref received from remote side)
        # For testing, we'll manually create a netref-like object
        class MockNetref(BaseNetref):
            __slots__ = ()

        # Create mock netref
        netref_obj = object.__new__(MockNetref)
        object.__setattr__(netref_obj, "____conn__", conn)
        object.__setattr__(netref_obj, "____id_pack__", id_pack)
        object.__setattr__(netref_obj, "____is_async__", False)

        print(f"\n[TEST] Created mock netref for object {id_pack}")

        # Box it while object EXISTS - should use LABEL_LOCAL_REF
        label, value = conn._box(netref_obj)
        assert label == consts.LABEL_LOCAL_REF, f"Expected LABEL_LOCAL_REF, got {label}"
        assert value == id_pack
        print(f"[TEST] ✓ _box() uses LABEL_LOCAL_REF when object exists")

        # Now REMOVE the object
        conn._local_objects.decref(id_pack, count=1)
        assert id_pack not in conn._local_objects._dict
        print(f"[TEST] ✓ Object removed from _local_objects")

        # Box it again while object is MISSING - should use LABEL_REMOTE_REF fallback
        label, value = conn._box(netref_obj)
        assert label == consts.LABEL_REMOTE_REF, f"Expected LABEL_REMOTE_REF fallback, got {label}"
        print(f"[TEST] ✓ _box() uses LABEL_REMOTE_REF fallback when object missing")

        # Value should be extended id_pack with flags
        assert len(value) == 4, f"Expected 4-element id_pack, got {len(value)}"
        assert value[:3] == id_pack, f"id_pack mismatch"
        assert value[3] == consts.FLAGS_SYNC, f"Expected FLAGS_SYNC"
        print(f"[TEST] ✓ LABEL_REMOTE_REF uses correct extended id_pack format")

        print(f"\n{'='*70}")
        print(f"FIX VERIFIED!")
        print(f"{'='*70}")
        print(f"The _box() method now detects missing objects and uses")
        print(f"LABEL_REMOTE_REF fallback instead of LABEL_LOCAL_REF.")
        print(f"This prevents KeyError in _unbox()!")
        print(f"{'='*70}\n")

    finally:
        server_sock.close()
        client_sock.close()


def test_unbox_provides_better_error_message():
    """
    VERIFICATION: _unbox() now provides better error message instead of KeyError.
    """
    class ClientData:
        def __init__(self, value):
            self.value = value

    obj = ClientData("test123")

    # Create a minimal Connection for testing
    server_sock, client_sock = socket.socketpair()
    chan = Channel(client_sock)
    config = {"allow_public_attrs": True}
    conn = Connection(rpyc.SlaveService, chan, config=config)

    try:
        id_pack = get_id_pack(obj)

        # Create package with LABEL_LOCAL_REF for non-existent object
        package = (consts.LABEL_LOCAL_REF, id_pack)

        print(f"\n[TEST] Attempting _unbox() with missing object...")

        try:
            result = conn._unbox(package)
            pytest.fail("Expected ValueError was not raised!")
        except ValueError as e:
            error_msg = str(e)
            print(f"[TEST] ✓ ValueError raised (not KeyError)")
            print(f"[TEST] Error message: {error_msg}")

            # Verify error message is descriptive
            assert "not found in _local_objects" in error_msg
            assert "garbage collected" in error_msg or "premature decref" in error_msg
            assert "race condition" in error_msg
            print(f"[TEST] ✓ Error message is descriptive and helpful")

        except KeyError as e:
            pytest.fail(f"Got KeyError instead of ValueError: {e}")

        print(f"\n{'='*70}")
        print(f"ERROR HANDLING VERIFIED!")
        print(f"{'='*70}")
        print(f"The _unbox() method now catches KeyError and provides")
        print(f"a clear, actionable ValueError with context.")
        print(f"{'='*70}\n")

    finally:
        server_sock.close()
        client_sock.close()


if __name__ == "__main__":
    pytest.main([__file__, "-xvs"])
