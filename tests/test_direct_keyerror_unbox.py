"""
DIRECT REPRODUCER: Demonstrate KeyError in _unbox() when object missing from _local_objects.

This test proves the bug exists by directly manipulating _local_objects.
"""
import pytest
import socket

import rpyc
from rpyc.core import consts
from rpyc.core.protocol import Connection
from rpyc.core.channel import Channel
from rpyc.lib import get_id_pack


def test_keyerror_when_object_removed_from_local_objects():
    """
    CRITICAL BUG REPRODUCER: KeyError in _unbox() when object is missing.

    This test demonstrates that _unbox() will raise KeyError if:
    1. Package has LABEL_LOCAL_REF
    2. Object ID is not in _local_objects

    This is exactly what happens in production when decref removes an object
    while it's still being used in async calls.
    """
    # Create a test object
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
        # Get object ID
        id_pack = get_id_pack(obj)
        print(f"\n[TEST] Object ID: {id_pack}")

        # Add object to _local_objects
        conn._local_objects.add(id_pack, obj)
        print(f"[TEST] ✓ Object added to _local_objects")

        # Verify it's there
        assert id_pack in conn._local_objects._dict
        print(f"[TEST] ✓ Object found in _local_objects")

        # Create package with LABEL_LOCAL_REF (this is what RPyC does internally)
        package = (consts.LABEL_LOCAL_REF, id_pack)

        # This SHOULD work (object is in _local_objects)
        result = conn._unbox(package)
        assert result is obj
        print(f"[TEST] ✓ _unbox() works when object is present")

        # Now REMOVE the object (this is what decref does)
        conn._local_objects.decref(id_pack, count=1)
        print(f"[TEST] ✓ Object removed via decref")

        # Verify it's gone
        assert id_pack not in conn._local_objects._dict
        print(f"[TEST] ✓ Object no longer in _local_objects")

        # Now try to unbox the SAME package again
        # BUG: This will raise KeyError!
        print(f"[TEST] Attempting _unbox() after object removed...")

        try:
            result = conn._unbox(package)
            print(f"[TEST] ✗ UNEXPECTED: No KeyError (result: {result})")
            pytest.fail("Expected KeyError was NOT raised!")

        except KeyError as e:
            print(f"[TEST] ✓✗ KeyError REPRODUCED: {e}")
            print(f"\n{'='*70}")
            print(f"BUG CONFIRMED!")
            print(f"{'='*70}")
            print(f"Location: rpyc/core/protocol.py:486 in _unbox()")
            print(f"Issue: Direct dict access without error handling")
            print(f"Code: return self._local_objects[value]")
            print(f"Error: {e}")
            print(f"{'='*70}\n")

            # This demonstrates the bug!
            # In production, this happens when:
            # 1. Netref object is passed to callback
            # 2. Decref is called (object removed from _local_objects)
            # 3. Callback tries to use the object
            # 4. _unbox() fails with KeyError!

            pytest.fail(
                f"✗ BUG REPRODUCED!\n"
                f"KeyError in _unbox() when LABEL_LOCAL_REF points to missing object.\n"
                f"This happens in production when callbacks receive objects that were\n"
                f"prematurely removed from _local_objects due to decref.\n\n"
                f"Error: {e}"
            )

    finally:
        server_sock.close()
        client_sock.close()


if __name__ == "__main__":
    pytest.main([__file__, "-xvs"])
