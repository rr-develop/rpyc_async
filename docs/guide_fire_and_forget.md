# Fire and Forget Guide

## Overview

The `fire_and_forget()` and `fire_and_forget_async()` utilities allow you to execute async operations in the background without blocking, with proper timeout handling and optional callbacks.

**Key Features:**
- Non-blocking execution of async operations
- Timeout support with local enforcement
- Success and error callbacks
- Works with both local coroutines and cross-process async RPC
- Handles hung/dead connections gracefully

**Critical Requirements:**
- Must be called from within a running event loop (async context)
- For cross-process RPC: **AsyncioServer is REQUIRED**
- All operations occur in the main application event loop (no additional loops/threads)

---

## Basic Usage

### With Local Coroutines

```python
import asyncio
from rpyc.utils.helpers import fire_and_forget

async def my_async_task(value: int) -> int:
    await asyncio.sleep(1)
    return value * 2

async def main():
    # Fire and forget - returns immediately
    task = fire_and_forget(
        my_async_task(10),
        timeout=5.0,
        success_callback=lambda result: print(f"Got: {result}"),
        error_callback=lambda exc: print(f"Error: {exc}"),
    )

    # Continue doing other work
    print("Task started in background")

    # Optionally wait for completion
    await task

asyncio.run(main())
```

### With Cross-Process RPC

```python
import asyncio
from rpyc.core.async_connect import async_connect
from rpyc.utils.helpers import fire_and_forget

async def main():
    # Connect to AsyncioServer
    conn = await async_connect("localhost", 18861)

    # Fire and forget remote call
    task = fire_and_forget(
        conn.root.long_running_task(arg1, arg2),
        timeout=30.0,
        success_callback=lambda result: print(f"Result: {result}"),
        error_callback=lambda exc: print(f"Failed: {exc}"),
    )

    # Continue immediately
    print("RPC call started in background")

    # Do other work...
    await asyncio.sleep(1)

    conn.close()

asyncio.run(main())
```

---

## Async Callbacks

For callbacks that need to perform async operations, use `fire_and_forget_async()`:

```python
from rpyc.utils.helpers import fire_and_forget_async

async def on_success(result):
    # Can await here
    await log_to_database(result)
    await send_notification(result)

async def on_error(exc):
    await log_error_to_file(str(exc))

async def main():
    task = fire_and_forget_async(
        my_async_task(10),
        success_callback=on_success,
        error_callback=on_error,
    )

    await task  # Wait if needed
```

---

## Timeout Handling

### Local Timeout Enforcement

Timeouts are enforced **locally** by the event loop, even if the remote process hangs:

```python
async def main():
    conn = await async_connect("localhost", 18861)

    # This will timeout after 5 seconds, even if server hangs
    task = fire_and_forget(
        conn.root.potentially_hanging_call(),
        timeout=5.0,
        error_callback=lambda exc: print(f"Timed out: {exc}"),
    )

    # Fire-and-forget completes in ~5 seconds, not forever
    await task
```

**Key Insight:** The timeout fires in your local event loop using `asyncio.wait_for()`. You don't need to wait for a hung remote process to respond.

### No Timeout

If you don't specify a timeout, the call waits indefinitely:

```python
task = fire_and_forget(
    my_async_task(10),
    timeout=None,  # No timeout
    success_callback=handle_success,
)
```

---

## Server Setup (AsyncioServer Required)

**CRITICAL:** Cross-process async RPC requires `AsyncioServer`. ThreadedServer is **NOT** supported.

### Server Side

```python
import asyncio
import rpyc
from rpyc.utils.async_server import AsyncioServer

class MyService(rpyc.Service):
    async def exposed_long_task(self, value: int) -> int:
        await asyncio.sleep(5)
        return value * 2

    async def exposed_quick_task(self, value: int) -> int:
        return value + 10

async def main():
    server = AsyncioServer(
        MyService,
        port=18861,
        protocol_config={"allow_all_attrs": True},
    )

    print("Server starting...")
    await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
```

### Client Side

```python
import asyncio
from rpyc.core.async_connect import async_connect
from rpyc.utils.helpers import fire_and_forget

async def main():
    # async_connect automatically enables asyncio serving
    conn = await async_connect("localhost", 18861)

    # Fire and forget
    task1 = fire_and_forget(
        conn.root.long_task(100),
        timeout=10.0,
        success_callback=lambda r: print(f"Task 1: {r}"),
    )

    task2 = fire_and_forget(
        conn.root.quick_task(50),
        timeout=2.0,
        success_callback=lambda r: print(f"Task 2: {r}"),
    )

    # Both tasks run concurrently
    await asyncio.gather(task1, task2)

    conn.close()

asyncio.run(main())
```

---

## Bidirectional Async Calls

Fire-and-forget works with bidirectional async calls (server calling client callbacks):

### Server

```python
class MyService(rpyc.Service):
    async def exposed_process_with_callback(self, callback, value: int) -> int:
        # Server calls client's async callback
        result = await callback(value * 2)
        return result + 10
```

### Client

```python
async def main():
    conn = await async_connect("localhost", 18861)

    # Define async callback
    async def my_callback(x: int) -> int:
        await asyncio.sleep(0.1)
        return x + 5

    # Fire and forget - server will call our callback
    task = fire_and_forget(
        conn.root.process_with_callback(my_callback, 10),
        timeout=5.0,
        success_callback=lambda r: print(f"Final result: {r}"),
    )

    await task
    conn.close()
```

---

## Best Practices

### 1. Always Provide Error Callbacks

Without an error callback, exceptions are only logged to stderr:

```python
# Good
task = fire_and_forget(
    risky_operation(),
    error_callback=handle_error,
)

# Bad - errors only logged
task = fire_and_forget(risky_operation())
```

### 2. Set Reasonable Timeouts for RPC

Always use timeouts for cross-process calls to prevent hanging:

```python
# Good
task = fire_and_forget(
    conn.root.remote_call(),
    timeout=10.0,  # Explicit timeout
    error_callback=handle_timeout,
)

# Risky - no timeout, may hang forever if connection dies
task = fire_and_forget(conn.root.remote_call())
```

### 3. Task Management

Keep references to important tasks to prevent garbage collection:

```python
# Store tasks you care about
active_tasks = set()

task = fire_and_forget(...)
active_tasks.add(task)
task.add_done_callback(active_tasks.discard)

# Wait for all tasks
await asyncio.gather(*active_tasks, return_exceptions=True)
```

### 4. Cancellation

You can cancel fire-and-forget tasks:

```python
task = fire_and_forget(long_running_task())

# Later...
if should_cancel:
    task.cancel()
```

Note: Cancellation propagates `CancelledError`, which is **not** passed to error_callback.

---

## Common Patterns

### Pattern 1: Multiple Concurrent Background Tasks

```python
async def main():
    conn = await async_connect("localhost", 18861)

    tasks = []
    for i in range(10):
        task = fire_and_forget(
            conn.root.process_item(i),
            timeout=5.0,
            success_callback=lambda r: results.append(r),
        )
        tasks.append(task)

    # Wait for all
    await asyncio.gather(*tasks)

    conn.close()
```

### Pattern 2: Fire and Continue

```python
async def main():
    conn = await async_connect("localhost", 18861)

    # Start background task
    fire_and_forget(
        conn.root.log_analytics(data),
        timeout=2.0,
        error_callback=lambda exc: print(f"Analytics failed: {exc}"),
    )

    # Continue immediately without waiting
    result = await conn.root.get_user_data()
    return result
```

### Pattern 3: With State Tracking

```python
class RequestHandler:
    def __init__(self):
        self.completed = 0
        self.failed = 0

    def on_success(self, result):
        self.completed += 1

    def on_error(self, exc):
        self.failed += 1

    async def process_batch(self, conn, items):
        tasks = []
        for item in items:
            task = fire_and_forget(
                conn.root.process(item),
                timeout=10.0,
                success_callback=self.on_success,
                error_callback=self.on_error,
            )
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)
        print(f"Completed: {self.completed}, Failed: {self.failed}")
```

---

## Troubleshooting

### Error: "RuntimeError: no running event loop"

**Cause:** Called `fire_and_forget()` outside of async context.

**Solution:** Ensure you're calling from within an async function:

```python
# Wrong
def sync_function():
    task = fire_and_forget(...)  # Error!

# Correct
async def async_function():
    task = fire_and_forget(...)  # OK
```

### Callbacks Not Firing

**Cause:** Task might be garbage collected or connection closed.

**Solution:** Keep reference to task and ensure connection stays alive:

```python
# Keep reference
task = fire_and_forget(...)
await task  # Wait for completion

# Or store in set
tasks = set()
task = fire_and_forget(...)
tasks.add(task)
```

### Timeouts Not Working

**Cause:** Might be using ThreadedServer instead of AsyncioServer.

**Solution:** Use AsyncioServer for all async functionality:

```python
# Wrong
from rpyc.utils.server import ThreadedServer
server = ThreadedServer(...)  # Does not support async!

# Correct
from rpyc.utils.async_server import AsyncioServer
server = AsyncioServer(...)  # Required for async
```

### Hung Connections

**Symptom:** Fire-and-forget hangs forever despite timeout.

**Cause:** Connection not using asyncio serving.

**Solution:** Use `async_connect()` which automatically enables asyncio serving:

```python
# Correct way
from rpyc.core.async_connect import async_connect
conn = await async_connect("localhost", 18861)  # Auto-enables asyncio serving

# Manual way
conn = rpyc.connect("localhost", 18861)
conn.enable_asyncio_serving()  # Must call this!
```

---

## Performance Considerations

### Overhead

Fire-and-forget has minimal overhead:
- Creates one Task object per call
- Uses event loop's built-in timeout mechanism
- No polling or busy-waiting

### Concurrency

You can safely fire thousands of concurrent tasks:

```python
tasks = [
    fire_and_forget(conn.root.process(i), timeout=5.0)
    for i in range(10000)
]
await asyncio.gather(*tasks)
```

The event loop handles scheduling efficiently.

### Memory

Each task consumes minimal memory (~1KB). Remember to:
- Set timeouts to prevent indefinite accumulation
- Remove references to completed tasks
- Use `add_done_callback` for automatic cleanup

---

## API Reference

### fire_and_forget()

```python
def fire_and_forget(
    awaitable: Awaitable[T],
    *,
    timeout: float | None = None,
    success_callback: Callable[[T], None] | None = None,
    error_callback: Callable[[BaseException], None] | None = None,
    name: str | None = None,
) -> asyncio.Task[None]
```

Execute an awaitable in the background with sync callbacks.

**Parameters:**
- `awaitable`: Coroutine or AsyncResult to execute
- `timeout`: Optional timeout in seconds (None = no timeout)
- `success_callback`: Synchronous function called with result on success
- `error_callback`: Synchronous function called with exception on error (except CancelledError)
- `name`: Optional task name for debugging

**Returns:**
- `asyncio.Task` that can be awaited or cancelled

**Raises:**
- `RuntimeError`: If no event loop is running

### fire_and_forget_async()

```python
def fire_and_forget_async(
    awaitable: Awaitable[T],
    *,
    timeout: float | None = None,
    success_callback: Callable[[T], Awaitable[None]] | None = None,
    error_callback: Callable[[BaseException], Awaitable[None]] | None = None,
    name: str | None = None,
) -> asyncio.Task[None]
```

Execute an awaitable in the background with async callbacks.

**Parameters:**
- `awaitable`: Coroutine or AsyncResult to execute
- `timeout`: Optional timeout in seconds (None = no timeout)
- `success_callback`: Async function called with result on success
- `error_callback`: Async function called with exception on error (except CancelledError)
- `name`: Optional task name for debugging

**Returns:**
- `asyncio.Task` that can be awaited or cancelled

**Raises:**
- `RuntimeError`: If no event loop is running

---

## Summary

Fire-and-forget utilities provide a clean, reliable way to execute async operations in the background:

✅ **Non-blocking** - Returns immediately
✅ **Timeout support** - Local enforcement, no hanging
✅ **Cross-process RPC** - Works with async RPC calls
✅ **Bidirectional async** - Server can call client callbacks
✅ **Error handling** - Optional success/error callbacks
✅ **Single event loop** - No additional threads or loops
✅ **Production ready** - Comprehensive test coverage

**Remember:**
- Always use `AsyncioServer` for async RPC
- Set timeouts for cross-process calls
- Provide error callbacks for production code
- Keep task references to prevent garbage collection
