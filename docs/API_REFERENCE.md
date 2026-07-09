# rpyc-async API Reference

## Overview

`rpyc-async` is an asyncio-native fork of RPyC. It provides native async/await support, allowing you to write asynchronous remote procedures that can be awaited just like local async functions.

**Version:** 1.0.0
**Install:** `pip install rpyc-async`
**Import name:** `import rpyc`
**Requires:** Python 3.10+

> `rpyc-async` does not guarantee wire or API compatibility with classic synchronous RPyC.

## Table of Contents

- [Connection Methods](#connection-methods)
- [AsyncResult](#asyncresult)
- [Service Methods](#service-methods)
- [Protocol Constants](#protocol-constants)
- [Type Hints](#type-hints)

---

## Connection Methods

### `enable_asyncio_serving(loop=None)`

Enable asyncio-based serving for the connection.

**Parameters:**
- `loop` (asyncio.AbstractEventLoop, optional): Event loop to use. If None, uses `asyncio.get_running_loop()`.

**Returns:** None

**Example:**
```python
import asyncio
import rpyc

async def main():
    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving()

    # Now connection can handle incoming async requests
    result = await conn.root.async_method()

    conn.disable_asyncio_serving()
    conn.close()

asyncio.run(main())
```

**Notes:**
- Only needed when server calls back to client with async methods
- Registers file descriptor with event loop using `loop.add_reader()`
- Automatically processes incoming messages without blocking

---

### `disable_asyncio_serving()`

Disable asyncio-based serving for the connection.

**Parameters:** None

**Returns:** None

**Example:**
```python
conn.disable_asyncio_serving()
```

**Notes:**
- Removes file descriptor from event loop
- Cleans up asyncio resources
- Safe to call multiple times

---

## AsyncResult

### `AsyncResult.__await__()`

Make AsyncResult awaitable in async context.

**Returns:** Awaitable that resolves to the result value

**Raises:**
- Exception: If remote call raised an exception
- AsyncResultTimeout: If result doesn't arrive before timeout

**Example:**
```python
async def main():
    conn = rpyc.connect("localhost", 18861)

    # Call async method - returns AsyncResult
    async_result = conn.root.async_method()

    # Await the result
    result = await async_result
    print(result)
```

**Behavior:**
- **Fast path**: If result already ready, returns immediately
- **Slow path**: Creates asyncio.Future and registers callback
- **Background serving**: Automatically polls connection for incoming messages
- **Thread-safe**: Uses `call_soon_threadsafe()` for cross-thread communication

---

### `AsyncResult.value`

Get the result value (blocking).

**Returns:** Result value

**Raises:**
- Exception: If remote call raised an exception
- AsyncResultTimeout: If result doesn't arrive before timeout

**Example:**
```python
# Blocking usage (classic synchronous RPyC style)
result = conn.root.async_method()
value = result.value  # Blocks until result arrives
```

---

### `AsyncResult.ready`

Check if result has arrived.

**Returns:** bool - True if result ready, False otherwise

**Example:**
```python
result = conn.root.async_method()
if result.ready:
    print(result.value)
else:
    print("Still waiting...")
```

---

### `AsyncResult.add_callback(func)`

Add callback to be invoked when result arrives.

**Parameters:**
- `func` (callable): Callback function that takes AsyncResult as argument

**Returns:** None

**Example:**
```python
def on_result(async_result):
    try:
        value = async_result.value
        print(f"Got result: {value}")
    except Exception as e:
        print(f"Got exception: {e}")

result = conn.root.async_method()
result.add_callback(on_result)
```

---

### `AsyncResult.set_expiry(timeout)`

Set timeout for result arrival.

**Parameters:**
- `timeout` (float): Timeout in seconds, or None for no timeout

**Returns:** None

**Example:**
```python
result = conn.root.async_method()
result.set_expiry(5.0)  # Timeout after 5 seconds

try:
    value = result.value
except AsyncResultTimeout:
    print("Request timed out!")
```

---

## Service Methods

### Defining Async Service Methods

**Example:**
```python
import asyncio
import rpyc

class MyService(rpyc.Service):
    async def exposed_async_hello(self, name):
        """Async service method."""
        await asyncio.sleep(1)  # Async work
        return f"Hello, {name}!"

    def exposed_sync_hello(self, name):
        """Plain sync service method."""
        return f"Sync hello, {name}!"
```

**Rules:**
- Prefix with `exposed_` to make remotely callable
- Use `async def` for async methods
- Can mix sync and async methods in same service
- Async methods automatically detected and handled

---

### Client-Side Usage

**Calling Async Methods:**
```python
async def main():
    conn = rpyc.connect("localhost", 18861)

    # Call async method - returns AsyncResult
    result = await conn.root.async_hello("world")
    print(result)  # "Hello, world!"

    # Call sync method - returns value directly
    result = conn.root.sync_hello("world")
    print(result)  # "Sync hello, world!"
```

**Concurrent Async Calls:**
```python
async def main():
    conn = rpyc.connect("localhost", 18861)

    # Launch multiple async calls concurrently
    results = await asyncio.gather(
        conn.root.async_method1(),
        conn.root.async_method2(),
        conn.root.async_method3(),
    )
```

---

## Protocol Constants

### Message Types

**Async Messages:**
- `MSG_ASYNC_REQUEST = 10` - Async request
- `MSG_ASYNC_REPLY = 11` - Async reply
- `MSG_ASYNC_EXCEPTION = 12` - Async exception

**Sync Messages:**
- `MSG_REQUEST = 0` - Sync request
- `MSG_REPLY = 1` - Sync reply
- `MSG_EXCEPTION = 2` - Sync exception

---

### Handler Constants

**Async Handlers:**
- `HANDLE_ASYNC_CALL = 100` - Call async function
- `HANDLE_ASYNC_CALLATTR = 101` - Call async method attribute

**Sync Handlers:**
- `HANDLE_CALL = 5` - Call sync function
- `HANDLE_CALLATTR = 6` - Call sync method attribute

---

### Object Flags

**Async Flags:**
- `FLAGS_SYNC = 0x00` - Sync object (default)
- `FLAGS_ASYNC = 0x01` - Async object

These flags are automatically set during object boxing/unboxing.

---

## Type Hints

### Annotating Async Methods

```python
from typing import Awaitable
import rpyc

class MyService(rpyc.Service):
    async def exposed_async_add(self, a: int, b: int) -> int:
        await asyncio.sleep(0.1)
        return a + b
```

### Annotating Client Calls

```python
async def call_remote_async(conn: rpyc.Connection) -> str:
    result: str = await conn.root.async_hello("world")
    return result
```

---

## Error Handling

### Async Exceptions

Exceptions in async methods are propagated to caller:

```python
async def main():
    conn = rpyc.connect("localhost", 18861)

    try:
        result = await conn.root.async_method_that_fails()
    except ValueError as e:
        print(f"Remote raised ValueError: {e}")
    except Exception as e:
        print(f"Remote raised exception: {e}")
```

### Remote Tracebacks

Remote tracebacks are preserved:

```python
try:
    await conn.root.async_error()
except Exception as e:
    print(str(e))  # Shows remote traceback
```

---

## Performance Considerations

### When to Use Async

**Use async methods when:**
- Method performs I/O (network, disk, database)
- Method calls other async functions
- Method needs to handle concurrent operations
- Method has high latency

**Use sync methods when:**
- Method is CPU-bound computation
- Method is simple getter/setter
- Method completes immediately

### Overhead

**Async overhead:**
- ~1-2ms per async call (event loop scheduling)
- Temporary event loop creation if not using `enable_asyncio_serving()`
- Background polling task for awaiting results

**Optimization tips:**
- Reuse connections instead of creating new ones
- Use `enable_asyncio_serving()` for better performance
- Batch multiple calls with `asyncio.gather()`

---

## Compatibility

`rpyc-async` is an independent, asyncio-native fork with its own versioning starting at 1.0.0.
Interoperability with classic synchronous RPyC is **not** guaranteed — neither at the wire
protocol level nor at the API level. Both peers must run `rpyc-async`.

### Compatibility Matrix

| Client     | Server     | Async Support | Sync Support |
|------------|------------|---------------|--------------|
| rpyc-async | rpyc-async | ✅ Yes        | ✅ Yes       |

**Notes:**
- Async methods require `rpyc-async` on both client and server
- Within `rpyc-async`, a service may expose sync and async methods side by side
- Connecting to or from a classic synchronous RPyC peer is unsupported

---

## Advanced Features

### Recursive Async Calls

Async methods can call themselves recursively:

```python
class RecursiveService(rpyc.Service):
    async def exposed_countdown(self, n):
        if n <= 0:
            return [0]
        await asyncio.sleep(0.01)
        rest = await self.exposed_countdown(n - 1)
        return [n] + rest
```

```python
# Client
result = await conn.root.countdown(10)
print(result)  # [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
```

### Mixed Sync/Async

Services can expose both sync and async methods:

```python
class MixedService(rpyc.Service):
    async def exposed_async_work(self):
        await asyncio.sleep(1)
        return "async result"

    def exposed_sync_work(self):
        return "sync result"
```

Both methods work transparently from client perspective.

---

## See Also

- [Usage Examples](EXAMPLES.md)
- [Migration Guide](MIGRATION_GUIDE.md)
- [Implementation Design](./IMPLEMENTATION_DESIGN.md)
