# Debug Refcounting Mode

## Problem

When errors like the following appear in the logs:

```
LABEL_LOCAL_REF points to missing object ('builtins.method', 10665440, 131284697598976).
Object may have been garbage collected or improperly reference counted.
```

The identifiers `10665440` and `131284697598976` are memory addresses (Python `id()` values), which are useless for debugging because:
1. They change between runs
2. Once an object is deleted, there is no way to tell what it was
3. It is impossible to determine which specific object was lost

## Solution: Debug Refcounting Mode

RPyC Async supports a debug mode that logs **human-readable representations of objects** when they are added to and removed from `_local_objects`.

### Enabling debug mode

```python
import logging
import rpyc_async as rpyc

# 1. Configure the logger
logger = logging.getLogger("rpyc.debug")
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()  # or FileHandler to write to a file
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

# 2. Enable debug_refcounting in the configuration
config = {
    "debug_refcounting": True,
    "logger": logger,
}

# 3. Create a server with this configuration
from rpyc_async.utils.server import AsyncioServer

server = AsyncioServer(
    MyService,
    port=18861,
    protocol_config=config
)
```

### Example output

With `debug_refcounting` enabled, you will see the following in the logs:

```
[REFCOUNT] ADD ('builtins.list', 10725536, 139237804419008) -> [1, 2, 3, 'hello'] (refcount=0)
[REFCOUNT] ADD ('builtins.dict', 10733632, 139237804418112) -> {'key': 'value', 'number': 42} (refcount=0)
[REFCOUNT] INCREF ('builtins.list', 10725536, 139237804419008) -> [1, 2, 3, 'hello'] (refcount=1)
[REFCOUNT] DECREF ('builtins.list', 10725536, 139237804419008) -> [1, 2, 3, 'hello'] (refcount=0)
[REFCOUNT] DELETE ('builtins.dict', 10733632, 139237804418112) -> {'key': 'value', 'number': 42} (refcount was 0, decref by 1)
```

Now, instead of useless ids, you can see:
- **What the object is**: `[1, 2, 3, 'hello']` instead of `131284697598976`
- **When it was added**: `[REFCOUNT] ADD`
- **When it was deleted**: `[REFCOUNT] DELETE`
- **Current refcount**: `(refcount=0)`

### Using it in your application

To use it in your own application:

```python
# In your server-manager settings, or wherever the RPyC server is created:

import logging

# Configure a logger for stderr
stderr_logger = logging.getLogger("rpyc.agent")
stderr_logger.setLevel(logging.DEBUG)

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.DEBUG)
stderr_logger.addHandler(stderr_handler)

# Server configuration
config = rpyc.core.protocol.DEFAULT_CONFIG.copy()
config["debug_refcounting"] = True  # ENABLE debug mode
config["logger"] = stderr_logger

# Create the server
server = AsyncioServer(
    MyServiceFactory(service),
    hostname="127.0.0.1",
    port=port,
    protocol_config=config
)
```

Now the error-log files will show **human-readable object names** instead of memory addresses.

### Disabling it in production

In production, it is recommended to disable `debug_refcounting=False` because it:
1. Generates a lot of logs
2. Calls `repr()` for every object (which can be slow)
3. Long reprs are truncated to 200 characters

### Log format

```
[REFCOUNT] <OPERATION> <id_pack> -> <repr(obj)> (refcount=N)
```

Where:
- `<OPERATION>`: `ADD`, `INCREF`, `DECREF`, `DELETE`
- `<id_pack>`: `(name_pack, type_id, object_id)` - the object's identifier
- `<repr(obj)>`: Human-readable representation of the object (truncated to 200 characters)
- `refcount=N`: Current reference count

### Common debugging patterns

#### 1. Object deleted prematurely

```
[REFCOUNT] ADD (..., 12345, 67890) -> MyObject(id=42) (refcount=0)
[REFCOUNT] DELETE (..., 12345, 67890) -> MyObject(id=42) (refcount was 0, decref by 1)
# Later:
LABEL_LOCAL_REF points to missing object (..., 12345, 67890)
```

**Cause**: The object was deleted (decref), but the remote side is still trying to access it.

#### 2. Reference leak

```
[REFCOUNT] ADD (..., 12345, 67890) -> MyObject(id=42) (refcount=0)
[REFCOUNT] INCREF (..., 12345, 67890) -> MyObject(id=42) (refcount=1)
[REFCOUNT] INCREF (..., 12345, 67890) -> MyObject(id=42) (refcount=2)
[REFCOUNT] INCREF (..., 12345, 67890) -> MyObject(id=42) (refcount=3)
# Never decreases...
```

**Cause**: The refcount keeps growing but decref is never called → memory leak.

## See also

- `tools/decode_id_pack.py` - utility for decoding id_pack from old logs
- `rpyc/lib/colls.py` - RefCountingColl implementation
- `rpyc/core/protocol.py` - usage of RefCountingColl
