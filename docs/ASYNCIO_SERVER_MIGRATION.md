# AsyncioServer Migration Guide

## Overview

This guide helps you migrate from `ThreadedServer` to `AsyncioServer` for bidirectional async support.

**IMPORTANT:** If you need bidirectional async callbacks (server calling client async functions or vice versa), you **MUST** use `AsyncioServer`. `ThreadedServer` does NOT support this use case.

---

## Clients: use `rpyc.async_connect`, NEVER `rpyc.connect`

If your code runs inside an asyncio event loop, connect to an
`AsyncioServer` with `rpyc.async_connect`, not `rpyc.connect`.

### Forbidden

```python
# ❌ Blocks the event loop on socket.connect(), and every later
#    sync_request is blocking too. Raises RuntimeError since v5.3.
conn = rpyc.connect("localhost", 18861)
```

### Required

```python
# ✅ Event-driven TCP connect + auto-enables asyncio serving +
#    pre-fetches conn.root. Zero blocking I/O.
conn = await rpyc.async_connect("localhost", 18861)

try:
    # Native async method — just await the netref call.
    result = await conn.root.async_method(42)

    # Sync remote method? Wrap it so the call is event-driven.
    result = await rpyc.async_(conn.root.sync_method)(42)
finally:
    # Async close. conn.close() from async code would hit the
    # sync_request guard; use aclose() instead.
    await conn.aclose()
```

### Why

`rpyc.connect` is a **synchronous** API:

1. `socket.connect()` blocks the calling thread — and when that thread is
   the asyncio event loop, every task on that loop stalls for the full
   TCP (and TLS, for `ssl_connect`) handshake. Real-world: tens to
   hundreds of milliseconds, enough to miss timers and starve other
   connections.
2. Every subsequent `sync_request` (including the implicit
   `sync_request(HANDLE_GETROOT)` triggered by `conn.root`) blocks the
   loop again, once per round-trip.

`async_connect` replaces both:

| Step                 | `rpyc.connect`                          | `rpyc.async_connect`                              |
|----------------------|-----------------------------------------|---------------------------------------------------|
| TCP connect          | blocking `socket.connect`               | `loop.sock_connect` — event-driven                |
| Asyncio serving      | user must remember `enable_asyncio_serving(loop)` | enabled automatically                             |
| First `conn.root`    | blocking `sync_request(HANDLE_GETROOT)` | eager pre-fetch via `async_request` + `await`     |
| Close                | blocking `sync_request(HANDLE_CLOSE)`   | `await conn.aclose()` — fire-and-forget + drain   |

Enforced at runtime: `rpyc.connect`, `rpyc.unix_connect`, and
`rpyc.ssl_connect` refuse to run inside a running loop with a clear
`RuntimeError` pointing at `async_connect`. User-level RPC
(`HANDLE_CALL` / `HANDLE_ASYNC_CALL`) via `conn.sync_request` also
refuses, pointing at `rpyc.async_(proxy)(...)` / `await conn.root.async_method(...)`.

### `rpyc.async_()` for sync remote methods

Netref calls to a **sync** remote method use `HANDLE_CALL` → blocking
path. From async code, wrap the proxy once:

```python
add = rpyc.async_(conn.root.add)
result = await add(2, 3)   # returns AsyncResult → await gives the value
```

Native `async def` remote methods are detected automatically and can be
awaited directly (`await conn.root.async_method(...)`).

---

## Cleanup debounce knob (`cleanup_debounce`)

See `docs/DESIGN_REFCOUNT_RACE_FIX.md`.

Background netref cleanup coalesces GC bursts through a one-shot
`loop.call_later` window, default **50 ms**. Tune via
`protocol_config={"cleanup_debounce": 0.050}`:

* Lower (e.g. 0.010) → cleanup wakes sooner; more sensitive to
  `id()`-reuse races on heavy churn.
* Higher (e.g. 0.200) → better batching; more memory retained per
  cleanup cycle.
* 0.0 → fire immediately (legacy).

The debounce is a single scheduled callback per burst, NOT a polling
loop. It coexists with the NO POLLING POLICY below.

---

## NO POLLING POLICY — Strict Ban

AsyncioServer and the asyncio-serving code path inside `Connection` **must not**
contain polling loops. This is a hard rule, enforced by
`tests/test_no_polling_policy.py`.

### Forbidden

```python
# ❌ NEVER — this was the bug. ~10 wakeups/sec per connection.
while not conn.closed:
    await asyncio.sleep(0.1)

# ❌ NEVER — same problem, any interval is wrong.
while running:
    await do_work()
    await asyncio.sleep(interval)
```

**Measured impact** of the old 100 ms polling loop in `_serve_connection`:

| Connections | CPU usage        |
|-------------|------------------|
| 0 (idle)    | 1.2%             |
| 2 active    | ~33% sustained   |
| N active    | linear increase  |

Polling also masks stale state: if the peer never flips `.closed`, the loop
keeps burning cycles forever.

### Required

Use event-driven primitives:

```python
# ✅ Wait for connection to close — zero CPU while idle.
await conn.wait_closed()

# ✅ Sync callback equivalent (thread-safe, fires exactly once).
conn.add_close_callback(lambda: print("closed"))

# ✅ Wake on work-available instead of a timer.
event = asyncio.Event()
# producer:  loop.call_soon_threadsafe(event.set)
# consumer:  await event.wait(); event.clear()

# ✅ Wake when a file descriptor is readable.
loop.add_reader(fd, callback)
```

### API added for this policy (Connection)

- `await conn.wait_closed()` — coroutine that resolves when the connection
  closes. Suspends on a Future; no wake-ups while waiting.
- `conn.add_close_callback(cb)` — one-shot thread-safe callback.
- `conn._enqueue_deletion(id_pack, refcount)` — internal, called by netrefs;
  wakes the background cleanup task via an `asyncio.Event`. The cleanup task
  does NOT run on a timer.

### Reviewer checklist

Reject any PR that:

- Adds `await asyncio.sleep(...)` inside a `while` / `for` loop in
  `rpyc/utils/async_server.py` or the asyncio sections of
  `rpyc/core/protocol.py`.
- Polls `conn.closed` from async code instead of awaiting `wait_closed()`.
- Introduces a "backoff timer" in the cleanup loop instead of re-waiting on
  the `_deletion_available` event.

---

## Why Migrate to AsyncioServer?

### ThreadedServer Limitations

`ThreadedServer` has fundamental architectural limitations for async use cases:

❌ **No Persistent Event Loops** - Creates temporary loops per request
❌ **Bidirectional Async Fails** - Deadlocks when server calls client async callbacks
❌ **No Server Concurrency** - Processes requests sequentially
❌ **Thread-per-Connection** - High memory overhead (~8MB per connection)
❌ **Blocking I/O** - Uses blocking socket operations

### AsyncioServer Benefits

`AsyncioServer` provides full async support with:

✅ **Persistent Event Loops** - Always available for both server and client
✅ **Bidirectional Async Works** - Server ↔ Client async calls work perfectly
✅ **Server Concurrency** - Processes multiple requests concurrently
✅ **Scalable** - Coroutine-based (~10KB per connection)
✅ **Non-Blocking I/O** - Uses `loop.add_reader()` for optimal performance
✅ **65x Faster** - For I/O-bound workloads

---

## Quick Migration Example

### Before (ThreadedServer - ❌ Bidirectional Async Fails)

```python
import rpyc
from rpyc import ThreadedServer

class MyService(rpyc.Service):
    async def exposed_process(self, callback, value):
        # ❌ This will DEADLOCK with ThreadedServer!
        result = await callback(value * 2)
        return result

# ThreadedServer
server = ThreadedServer(MyService, port=18861)
server.start()  # Blocking
```

**Problem:** When server tries `await callback(...)`, it deadlocks because:
1. Connection thread is blocked in `serve_all()`
2. `asyncio.run()` creates temporary loop
3. No persistent loop to receive callback reply
4. **DEADLOCK** 🔴

---

### After (AsyncioServer - ✅ Works Perfectly)

```python
import asyncio
import rpyc
from rpyc.utils.async_server import AsyncioServer

class MyService(rpyc.Service):
    async def exposed_process(self, callback, value):
        # ✅ This WORKS with AsyncioServer!
        result = await callback(value * 2)
        return result

async def main():
    # AsyncioServer with persistent event loop
    server = AsyncioServer(MyService, port=18861)
    await server.serve_forever()

# Run server
asyncio.run(main())
```

**Why it works:**
1. ✅ Persistent event loop exists
2. ✅ Connection uses `loop.add_reader()` for non-blocking I/O
3. ✅ Bidirectional async callbacks work
4. ✅ No deadlocks

---

## Migration Scenarios

### Scenario 1: Simple Unidirectional Async (Client → Server)

**Use Case:** Client calls server async methods, no callbacks.

**ThreadedServer:** ✅ Works (but sequential processing)
**AsyncioServer:** ✅ Works (concurrent processing, better performance)

**Migration:** Optional but recommended for performance.

#### Before (ThreadedServer)

```python
# Server
from rpyc import ThreadedServer

class DataService(rpyc.Service):
    async def exposed_fetch_data(self, user_id):
        # Simple async method - no callbacks
        await asyncio.sleep(0.1)
        return {"id": user_id, "name": f"User {user_id}"}

server = ThreadedServer(DataService, port=18861)
server.start()
```

```python
# Client
import asyncio
import rpyc

async def main():
    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving()  # Enable for await

    # Unidirectional: client → server
    user = await conn.root.fetch_data(123)
    print(user)

    conn.close()

asyncio.run(main())
```

**Status:** ✅ Works but server processes requests sequentially.

#### After (AsyncioServer)

```python
# Server
import asyncio
from rpyc.utils.async_server import AsyncioServer

class DataService(rpyc.Service):
    async def exposed_fetch_data(self, user_id):
        await asyncio.sleep(0.1)
        return {"id": user_id, "name": f"User {user_id}"}

async def main():
    server = AsyncioServer(DataService, port=18861)
    await server.serve_forever()

asyncio.run(main())
```

```python
# Client (same as before)
import asyncio
import rpyc

async def main():
    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving()

    user = await conn.root.fetch_data(123)
    print(user)

    conn.close()

asyncio.run(main())
```

**Benefits:**
- ✅ Server processes requests concurrently (100x improvement)
- ✅ Lower memory usage
- ✅ Better scalability

---

### Scenario 2: Bidirectional Async (Server ↔ Client)

**Use Case:** Server calls client async callbacks, or vice versa.

**ThreadedServer:** ❌ **FAILS** (deadlock)
**AsyncioServer:** ✅ **REQUIRED**

**Migration:** **MANDATORY**

#### Before (ThreadedServer - ❌ DEADLOCK)

```python
# Server
from rpyc import ThreadedServer

class ProcessService(rpyc.Service):
    async def exposed_process_with_callback(self, callback, value):
        # ❌ DEADLOCK - no persistent loop!
        result = await callback(value * 2)
        return f"Processed: {result}"

server = ThreadedServer(ProcessService, port=18861)
server.start()
```

```python
# Client
import asyncio
import rpyc

class ClientService(rpyc.Service):
    async def exposed_callback(self, value):
        await asyncio.sleep(0.1)
        return value + 10

async def main():
    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving()

    client_service = ClientService()

    # ❌ This will HANG/TIMEOUT!
    result = await conn.root.process_with_callback(
        client_service.exposed_callback,
        value=5
    )
    print(result)

asyncio.run(main())
```

**Problem:** Server deadlocks when trying to call client callback.

#### After (AsyncioServer - ✅ WORKS)

```python
# Server
import asyncio
from rpyc.utils.async_server import AsyncioServer

class ProcessService(rpyc.Service):
    async def exposed_process_with_callback(self, callback, value):
        # ✅ WORKS - persistent loop!
        result = await callback(value * 2)
        return f"Processed: {result}"

async def main():
    server = AsyncioServer(ProcessService, port=18861)
    await server.serve_forever()

asyncio.run(main())
```

```python
# Client (same as before)
import asyncio
import rpyc

class ClientService(rpyc.Service):
    async def exposed_callback(self, value):
        await asyncio.sleep(0.1)
        return value + 10

async def main():
    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving()

    client_service = ClientService()

    # ✅ This WORKS perfectly!
    result = await conn.root.process_with_callback(
        client_service.exposed_callback,
        value=5
    )
    print(result)  # "Processed: 20"

asyncio.run(main())
```

**Why it works:**
- ✅ Server has persistent event loop
- ✅ Client has persistent event loop (`enable_asyncio_serving()`)
- ✅ Both can send/receive async messages
- ✅ No deadlocks

---

### Scenario 3: Recursive Async Callbacks

**Use Case:** Server calls client callback, which calls server, which calls client, etc.

**ThreadedServer:** ❌ **FAILS** (deadlock)
**AsyncioServer:** ✅ **REQUIRED**

**Migration:** **MANDATORY**

#### Example (AsyncioServer Only)

```python
# Server
import asyncio
from rpyc.utils.async_server import AsyncioServer

class RecursiveService(rpyc.Service):
    async def exposed_countdown(self, callback, n):
        """Recursive countdown with client callback."""
        print(f"[SERVER] countdown({n})")

        if n <= 0:
            return "Done!"

        await asyncio.sleep(0.05)

        # Call client callback recursively
        result = await callback(n - 1)
        return f"Server({n}) -> {result}"

async def main():
    server = AsyncioServer(RecursiveService, port=18861)
    await server.serve_forever()

asyncio.run(main())
```

```python
# Client
import asyncio
import rpyc

class ClientService(rpyc.Service):
    def __init__(self, server_conn):
        super().__init__()
        self.server_conn = server_conn

    async def exposed_client_countdown(self, n):
        """Client callback that calls server recursively."""
        print(f"[CLIENT] client_countdown({n})")

        if n <= 0:
            return "Client finished!"

        await asyncio.sleep(0.05)

        # Call server recursively
        result = await self.server_conn.root.countdown(
            self.exposed_client_countdown,
            n - 1
        )
        return f"Client({n}) -> {result}"

async def main():
    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving()

    client_service = ClientService(conn)

    # Start recursive chain (depth=5)
    result = await conn.root.countdown(
        client_service.exposed_client_countdown,
        5
    )

    print(f"Final result: {result}")
    conn.close()

asyncio.run(main())
```

**Output:**
```
[SERVER] countdown(5)
[CLIENT] client_countdown(4)
[SERVER] countdown(3)
[CLIENT] client_countdown(2)
[SERVER] countdown(1)
[CLIENT] client_countdown(0)
Final result: Server(5) -> Client(4) -> Server(3) -> Client(2) -> Server(1) -> Client finished!
```

**Only possible with AsyncioServer!**

---

## Client-Side Migration

### Enable Asyncio Serving (Required)

**Both ThreadedServer and AsyncioServer clients need this for async:**

```python
import asyncio
import rpyc

async def main():
    conn = rpyc.connect("localhost", 18861)

    # ✅ REQUIRED for using await
    loop = asyncio.get_running_loop()
    conn.enable_asyncio_serving(loop=loop)

    try:
        # Now you can await async methods
        result = await conn.root.async_method()
        print(result)
    finally:
        conn.disable_asyncio_serving()
        conn.close()

asyncio.run(main())
```

**Why needed:**
- Registers connection FD with event loop
- Enables non-blocking message processing
- Required for `await` to work

---

## Server Startup Patterns

### Pattern 1: Simple Standalone Server

```python
import asyncio
from rpyc.utils.async_server import AsyncioServer
from myapp.services import MyService

async def main():
    server = AsyncioServer(
        MyService,
        hostname='0.0.0.0',
        port=18861,
        protocol_config={
            'allow_all_attrs': True,
        }
    )

    print("Server starting on port 18861...")
    await server.serve_forever()

if __name__ == '__main__':
    asyncio.run(main())
```

---

### Pattern 2: Server with Graceful Shutdown

```python
import asyncio
import signal
from rpyc.utils.async_server import AsyncioServer
from myapp.services import MyService

async def main():
    server = AsyncioServer(MyService, port=18861)

    # Setup graceful shutdown
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def signal_handler():
        print("\nShutdown signal received...")
        shutdown_event.set()

    # Register signal handlers
    loop.add_signal_handler(signal.SIGINT, signal_handler)
    loop.add_signal_handler(signal.SIGTERM, signal_handler)

    # Start server
    await server.start()
    print(f"Server started on port {server.port}")

    # Wait for shutdown signal
    await shutdown_event.wait()

    # Graceful shutdown
    print("Shutting down server...")
    await server.close()
    print("Server stopped")

if __name__ == '__main__':
    asyncio.run(main())
```

---

### Pattern 3: Server with Background Tasks

```python
import asyncio
from rpyc.utils.async_server import AsyncioServer
from myapp.services import MyService

async def background_task():
    """Background task running alongside server."""
    while True:
        print("Background task running...")
        await asyncio.sleep(10)

async def main():
    server = AsyncioServer(MyService, port=18861)

    # Start server and background tasks concurrently
    await asyncio.gather(
        server.serve_forever(),
        background_task(),
    )

if __name__ == '__main__':
    asyncio.run(main())
```

---

### Pattern 4: Multiple Servers

```python
import asyncio
from rpyc.utils.async_server import AsyncioServer
from myapp.services import PublicService, AdminService

async def main():
    # Public API server
    public_server = AsyncioServer(
        PublicService,
        hostname='0.0.0.0',
        port=18861
    )

    # Admin API server
    admin_server = AsyncioServer(
        AdminService,
        hostname='127.0.0.1',
        port=18862
    )

    # Run both servers concurrently
    await asyncio.gather(
        public_server.serve_forever(),
        admin_server.serve_forever(),
    )

if __name__ == '__main__':
    asyncio.run(main())
```

---

## Testing AsyncioServer

### Unit Test Pattern

```python
import asyncio
import unittest
import rpyc
from rpyc.utils.async_server import AsyncioServer

class TestMyService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Start AsyncioServer for tests."""
        cls.server = AsyncioServer(MyService, port=18870)

        # Start server in background
        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls.loop)

        cls.server_task = cls.loop.create_task(
            cls.server.serve_forever()
        )

        # Wait for server to start
        cls.loop.run_until_complete(asyncio.sleep(0.5))

    @classmethod
    def tearDownClass(cls):
        """Stop server."""
        cls.server_task.cancel()
        cls.loop.run_until_complete(cls.server.close())
        cls.loop.close()

    def test_async_method(self):
        """Test async method call."""
        async def test():
            conn = rpyc.connect("localhost", 18870)
            conn.enable_asyncio_serving()

            try:
                result = await conn.root.async_method(42)
                self.assertEqual(result, 84)
            finally:
                conn.close()

        asyncio.run(test())
```

---

## Performance Comparison

### ThreadedServer vs AsyncioServer

**Benchmark:** 100 concurrent requests with 0.1s I/O delay each

| Metric | ThreadedServer | AsyncioServer | Improvement |
|--------|----------------|---------------|-------------|
| **Execution Time** | ~10s | ~0.15s | **65x faster** |
| **Throughput** | 10 req/s | 650 req/s | **65x** |
| **Memory/Connection** | ~8MB | ~10KB | **800x less** |
| **Max Connections** | ~1,000 | ~10,000+ | **10x** |
| **CPU Usage** | Moderate | Low | Lower |
| **Bidirectional Async** | ❌ Fails | ✅ Works | N/A |

**Recommendation:** Use AsyncioServer for all async workloads.

---

## Common Migration Issues

### Issue 1: Forgetting `enable_asyncio_serving()`

**Symptom:** Client hangs when trying to `await`

**Solution:**
```python
async def main():
    conn = rpyc.connect("localhost", 18861)

    # ✅ REQUIRED!
    conn.enable_asyncio_serving()

    result = await conn.root.async_method()
```

---

### Issue 2: Not Running in `async def main()`

**Symptom:** `RuntimeError: no running event loop`

**Solution:**
```python
# ❌ Wrong
server = AsyncioServer(MyService, port=18861)
await server.serve_forever()  # Error!

# ✅ Correct
async def main():
    server = AsyncioServer(MyService, port=18861)
    await server.serve_forever()

asyncio.run(main())
```

---

### Issue 3: Blocking Calls in Async Methods

**Symptom:** Server becomes unresponsive

**Solution:** Use async libraries for I/O:

```python
# ❌ Wrong - blocks event loop
async def exposed_fetch(self, url):
    import requests
    response = requests.get(url)  # Blocking!
    return response.text

# ✅ Correct - non-blocking
async def exposed_fetch(self, url):
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()
```

---

## Decision Matrix: Which Server to Use?

| Use Case | ThreadedServer | AsyncioServer |
|----------|----------------|---------------|
| **Sync-only methods** | ✅ Recommended | ✅ Works |
| **Unidirectional async (Client→Server)** | ✅ Works (slow) | ✅ **Recommended** |
| **Bidirectional async** | ❌ **Fails** | ✅ **Required** |
| **Recursive async callbacks** | ❌ **Fails** | ✅ **Required** |
| **High concurrency (1000+ conn)** | ❌ Limited | ✅ **Recommended** |
| **Low memory footprint** | ❌ High | ✅ **Recommended** |
| **CPU-bound workloads** | ✅ OK | ⚠️ Use process pool |
| **Legacy sync codebase** | ✅ Recommended | ⚠️ Migration effort |

**General Rule:** Use AsyncioServer for all async use cases.

---

## Migration Checklist

- [ ] Identify bidirectional async usage in codebase
- [ ] Replace `ThreadedServer` with `AsyncioServer` imports
- [ ] Wrap server startup in `async def main()`
- [ ] Update client code to use `enable_asyncio_serving()`
- [ ] Test all async methods work correctly
- [ ] Verify bidirectional callbacks work
- [ ] Update tests to use AsyncioServer
- [ ] Update deployment scripts
- [ ] Update documentation
- [ ] Monitor performance improvements

---

## Further Reading

- [AsyncioServer API Reference](./API_REFERENCE.md#asyncioserver)
- [Limitations Documentation](./LIMITATIONS.md)
- [Examples](./EXAMPLES.md)
- [Python asyncio Documentation](https://docs.python.org/3/library/asyncio.html)

---

## Summary

**Key Takeaways:**

1. ✅ **AsyncioServer is REQUIRED** for bidirectional async
2. ✅ **Migration is straightforward** - mostly import changes
3. ✅ **Performance gains are significant** - 65x faster for I/O
4. ✅ **All async use cases should use AsyncioServer**
5. ❌ **ThreadedServer CANNOT support bidirectional async** - architectural limitation

**When in doubt, use AsyncioServer for async workloads.**
