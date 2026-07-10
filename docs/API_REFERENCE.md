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
- [Error Handling](#error-handling)
- [Performance Considerations](#performance-considerations)
- [Compatibility](#compatibility)

---

## Connection Methods

### `await async_connect(host, port, *, service=VoidService, config=None, timeout=None, loop=None)`

Establish a connection from async code. **This is the entry point for `rpyc-async`.**

**Parameters:**
- `host` (str), `port` (int): where to connect
- `service` (Service, optional): the *client-side* service exposed to the server
  (needed for server→client callbacks). Defaults to `VoidService`.
- `config` (dict, optional): RPyC protocol config
- `timeout` (float, optional): connect timeout in seconds. Wraps only the TCP
  handshake, not the whole call.
- `loop` (asyncio.AbstractEventLoop, optional): defaults to `asyncio.get_running_loop()`

**Returns:** `Connection`

**Example:**
```python
import asyncio
import rpyc_async as rpyc

async def main():
    conn = await rpyc.async_connect("localhost", 18861, timeout=5.0)
    try:
        result = await conn.root.async_method()
    finally:
        await conn.aclose()

asyncio.run(main())
```

**Notes:**
- Performs a non-blocking TCP connect and an **eager handshake**, so the first
  access to `conn.root` never blocks the event loop.
- **Enables asyncio serving automatically.** You do *not* need to call
  `enable_asyncio_serving()` yourself.

> **Do not call `rpyc.connect()` from async code.** It is synchronous and would
> block the running event loop, so it raises `RuntimeError` and points you here.

---

### `await conn.aclose()`

Close the connection from async code.

**Parameters:** None &nbsp;&nbsp; **Returns:** None

**Example:**
```python
try:
    result = await conn.root.async_method()
finally:
    await conn.aclose()
```

**Notes:**
- **Use this, not `conn.close()`, in async code.** `close()` issues a *blocking*
  `sync_request(HANDLE_CLOSE)` and waits for a reply that usually never arrives
  (the peer cleans up before emitting it). That wait runs to
  `sync_request_timeout` — **30 seconds by default** — with the event loop
  frozen for the whole time. Measured: `close()` 3.00 s at
  `sync_request_timeout=3`, `aclose()` 0.00 s.
- Drains pending netref deletions, sends `HANDLE_CLOSE` event-driven, then
  cleans up locally. Never blocks the loop. Safe to call more than once.

---

### `enable_asyncio_serving(loop=None)`

Enable asyncio-based serving on an existing connection.

**Parameters:**
- `loop` (asyncio.AbstractEventLoop, optional): Event loop to use. If None, uses `asyncio.get_running_loop()`.

**Returns:** None

**Notes:**
- **You rarely need this.** `async_connect()` already enables serving, and
  `AsyncioServer` enables it on every accepted connection.
- It exists for connections built by hand from a channel/stream.
- Registers the socket with the event loop via `loop.add_reader()` and processes
  incoming messages without blocking.

---

### `disable_asyncio_serving()`

Disable asyncio-based serving for the connection.

**Parameters:** None

**Returns:** None

**Notes:**
- Removes the file descriptor from the event loop and cleans up asyncio state.
- Safe to call multiple times. Not needed before `aclose()`, which cleans up
  on its own.

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
    conn = await rpyc.async_connect("localhost", 18861)

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
import rpyc_async as rpyc

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
    conn = await rpyc.async_connect("localhost", 18861)
    try:
        # Native async method - just await it
        result = await conn.root.async_hello("world")
        print(result)  # "Hello, world!"

        # Sync remote method: wrap it, otherwise the blocking call is
        # rejected by the sync_request guard on the serving loop.
        a_sync_hello = rpyc.async_(conn.root.sync_hello)
        print(await a_sync_hello("world"))  # "Sync hello, world!"
    finally:
        await conn.aclose()
```

> Store the wrapper returned by `rpyc.async_()` in a variable rather than
> calling it inline: the wrapper is cached behind a weak reference.

**Concurrent Async Calls:**
```python
async def main():
    conn = await rpyc.async_connect("localhost", 18861)
    try:
        # Launch multiple async calls concurrently
        results = await asyncio.gather(
            conn.root.async_method1(),
            conn.root.async_method2(),
            conn.root.async_method3(),
        )
    finally:
        await conn.aclose()
```

**Fire-and-forget:** to start a remote async call and not wait for it, use
`fire_and_forget_async()` (async callbacks) or, when the callback cannot
`await`, `fire_and_forget()` (sync callbacks). Both return an `asyncio.Task`
that resolves to `None`, never to the call's result — read the value from
`success_callback`. You do **not** need to keep a reference to the task: each
helper pins it internally until it settles. Keep it only to `await` or
`cancel()` it.

```python
from rpyc_async.utils.helpers import fire_and_forget_async

async def on_success(result):
    await log(result)

task = fire_and_forget_async(
    conn.root.long_running_task(),
    timeout=30.0,
    success_callback=on_success,
    error_callback=log_error,
)
```

Both require a running event loop, and are imported from `rpyc.utils.helpers`,
not from the `rpyc` top level.

The peer needs `AsyncioServer` only if the remote method is `async def`. A
`ThreadedServer` exposing a plain `def` works too — wrap the netref so the call
becomes an awaitable rather than a blocking `sync_request`:

```python
task = fire_and_forget_async(rpyc.async_(conn.root.sync_method)(arg), timeout=5.0)
```

Always pass `timeout=`: without it, a call whose reply never arrives leaves its
task pinned for the lifetime of the process.

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
import rpyc_async as rpyc

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
    conn = await rpyc.async_connect("localhost", 18861)

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
- No polling: replies are delivered by `loop.add_reader()` on the connection's
  socket, so awaiting a result costs nothing while it is pending

**Optimization tips:**
- Reuse connections instead of creating new ones
- Batch multiple calls with `asyncio.gather()`
- Use `fire_and_forget()` when you do not need the result

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
