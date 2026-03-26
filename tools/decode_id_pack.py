#!/usr/bin/env python3
"""
Utility to decode RPyC id_pack identifiers from error logs.

id_pack structure: (name_pack, type_id, object_id)
- name_pack: String like "builtins.method" or "module.Class"
- type_id: id(type(obj)) - memory address of the TYPE object
- object_id: id(obj) - memory address of the INSTANCE object

Example usage:
    python3 decode_id_pack.py 'builtins.method' 10665440 131284697598976

Or parse from log line:
    python3 decode_id_pack.py --parse-log "LABEL_LOCAL_REF points to missing object ('builtins.method', 10665440, 131284697598976)"
"""
import sys
import re
import ctypes


def decode_id_pack(name_pack, type_id, object_id):
    """
    Decode id_pack to human-readable information.

    Args:
        name_pack: String like "builtins.method"
        type_id: id(type(obj))
        object_id: id(obj)

    Returns:
        dict with decoded information
    """
    result = {
        "name_pack": name_pack,
        "type_id": type_id,
        "type_id_hex": hex(type_id),
        "object_id": object_id,
        "object_id_hex": hex(object_id),
    }

    # Parse name_pack
    if '.' in name_pack:
        module, name = name_pack.rsplit('.', 1)
        result["module"] = module
        result["name"] = name
    else:
        result["module"] = None
        result["name"] = name_pack

    # Explain what the IDs mean
    result["explanation"] = (
        f"Object: {name_pack}\n"
        f"  - Type ID: {type_id} (0x{type_id:x}) - Memory address of type({name_pack})\n"
        f"  - Instance ID: {object_id} (0x{object_id:x}) - Memory address of this specific instance\n"
        f"\n"
        f"These are Python's id() values - unique identifiers for objects in memory.\n"
        f"They change between runs and cannot be used to find the object after it's deleted.\n"
        f"\n"
        f"To debug this error, you need to:\n"
        f"  1. Find WHERE in your code {name_pack} objects are created\n"
        f"  2. Check if they're being passed to RPyC calls\n"
        f"  3. Ensure proper reference counting (not calling decref prematurely)\n"
    )

    # Cannot access deleted objects - IDs are only valid while object exists
    result["access_note"] = (
        "NOTE: Cannot access object from ID after it's been deleted.\n"
        "Memory addresses (IDs) are only valid while the object exists.\n"
        "Once deleted, the memory may be reused for other objects."
    )

    return result


def parse_log_line(line):
    """
    Parse RPyC error log line to extract id_pack.

    Example:
        "LABEL_LOCAL_REF points to missing object ('builtins.method', 10665440, 131284697598976)"
    """
    pattern = r"'([^']+)',\s*(\d+),\s*(\d+)"
    match = re.search(pattern, line)

    if not match:
        return None

    name_pack = match.group(1)
    type_id = int(match.group(2))
    object_id = int(match.group(3))

    return decode_id_pack(name_pack, type_id, object_id)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    if sys.argv[1] == "--parse-log":
        if len(sys.argv) < 3:
            print("Error: --parse-log requires a log line argument")
            sys.exit(1)

        log_line = sys.argv[2]
        result = parse_log_line(log_line)

        if result is None:
            print(f"Error: Could not parse log line: {log_line}")
            sys.exit(1)
    else:
        if len(sys.argv) < 4:
            print("Error: Requires 3 arguments: name_pack type_id object_id")
            print(__doc__)
            sys.exit(1)

        name_pack = sys.argv[1]
        type_id = int(sys.argv[2])
        object_id = int(sys.argv[3])

        result = decode_id_pack(name_pack, type_id, object_id)

    # Pretty print result
    print("\n" + "="*70)
    print("RPyC id_pack Decoder")
    print("="*70)
    print(f"\nName Pack: {result['name_pack']}")
    if result['module']:
        print(f"  Module: {result['module']}")
        print(f"  Name: {result['name']}")
    print(f"\nType ID: {result['type_id']} ({result['type_id_hex']})")
    print(f"Object ID: {result['object_id']} ({result['object_id_hex']})")

    print("\n" + "-"*70)
    print("EXPLANATION")
    print("-"*70)
    print(result['explanation'])

    print("\n" + "-"*70)
    print("ACCESS NOTE")
    print("-"*70)
    print(result['access_note'])
    print("="*70 + "\n")


if __name__ == "__main__":
    main()
