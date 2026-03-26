# Debug Refcounting Mode

## Problem

When errors like the following appear in the logs:

```
LABEL_LOCAL_REF points to missing object ('builtins.method', 10665440, 131284697598976).
Object may have been garbage collected or improperly reference counted.
```

The identifiers `10665440` and `131284697598976` are memory addresses (Python `id()` values), which are useless for debugging because:
1. They change between runs
2. After the object is deleted, there is no way to know what it was
3. It is impossible to tell which specific object was lost

## Solution: Debug Refcounting Mode

RPyC Async supports a debug mode that logs **readable representations of objects** when they are added to and removed from `_local_objects`.

### Enabling debug mode

```python
import logging
import rpyc

# 1. Configure a logger
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
from rpyc.utils.server import AsyncioServer

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
- **The current refcount**: `(refcount=0)`

### Using it in your application

To use this in your own application:

```python
# In the code where you create the RPyC server:

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

Now your error logs will show **readable object names** instead of memory addresses.
