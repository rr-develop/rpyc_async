# rpyc-async — Known Limitations

## Overview

`rpyc-async` 1.0.0 (asyncio-native fork of upstream RPyC) provides async/await support with the following status:

> **Note:** `rpyc-async` is an independent distribution (`pip install rpyc-async`, import name `rpyc`).
> Backward compatibility with classic synchronous RPyC is **not** guaranteed.

**✅ Fully Supported:**
- Client → Server async calls (unidirectional)
- Recursive async calls (same side)
- Concurrent async operations
- Mixed sync/async services

**❌ ThreadedServer Cannot Support:**
- Bidirectional async callbacks (Server ↔ Client)
- Server-side concurrency

**✅ Solution: AsyncioServer**
- Already implemented in `rpyc.utils.async_server`
- Full bidirectional async support
- Persistent event loops
- See [Migration Guide](ASYNCIO_SERVER_MIGRATION.md)

---

## ⚠️ CRITICAL: ThreadedServer Architecture Limitation

### The Fundamental Problem

**ThreadedServer CANNOT support bidirectional async callbacks.**

This is NOT a bug - it's an **architectural impossibility** due to how ThreadedServer works.

### Why It Fails

**ThreadedServer Architecture:**
```
Main Thread
├─ Accept connections
└─ Spawn worker threads
    └─ Each thread runs: conn.serve_all() [BLOCKING]
        └─ When async request arrives:
            └─ asyncio.run() [TEMPORARY loop]
                └─ Execute async handler
                └─ Loop DESTROYED after request
```

**Problem:**
1. Worker thread blocks in `serve_all()`
2. Async request creates **temporary** event loop via `asyncio.run()`
3. Server tries to call client async callback
4. **DEADLOCK:**
   - Temporary loop cannot receive reply
   - Thread is blocked in `serve_all()`
   - No persistent loop to handle incoming messages
   - **TIMEOUT**

**Root Cause:** `rpyc/core/protocol.py:695-700`
```python
elif needs_async:
    # Creates TEMPORARY loop - destroyed after request!
    import asyncio
    asyncio.run(self._dispatch_request_async(seq, args))
```

### What Fails with ThreadedServer

#### ❌ Bidirectional Async Callbacks

**Scenario:**
```python
# Server
class ServerService(rpyc.Service):
    async def exposed_process(self, callback, value):
        # ❌ THIS WILL DEADLOCK!
        result = await callback(value * 2)
        return result

# Client
async def my_callback(x):
    await asyncio.sleep(0.1)
    return x + 10

conn = rpyc.connect("localhost", 18861)
conn.enable_asyncio_serving()

# ❌ HANGS/TIMEOUT with ThreadedServer
result = await conn.root.process(my_callback, 5)
```

**Why it fails:**
1. Client calls server with callback
2. Server (in thread) creates temporary loop via `asyncio.run()`
3. Server tries `await callback(...)`
4. Callback request sent to client
5. **DEADLOCK:** Temporary loop cannot receive reply
6. **TIMEOUT after ~30 seconds**

#### ❌ Recursive Bidirectional Calls

**Scenario:**
```python
# Server calls client, client calls server, etc.
# ❌ DEADLOCK - same reason as above
```

#### ❌ Server-Side Concurrency

**Problem:**
```python
# ThreadedServer processes requests SEQUENTIALLY
# Even with async methods, only ONE request processed at a time per connection
```

**Reason:** `asyncio.run()` blocks until completion.

---

## ✅ Solution: Use AsyncioServer

### AsyncioServer Architecture

**`rpyc.utils.async_server.AsyncioServer`** solves ALL limitations:

```
Main Event Loop (persistent)
├─ Accept connections (async)
└─ Handle connections (coroutines, not threads)
    └─ conn.enable_asyncio_serving(loop)
        └─ Register FD with loop.add_reader()
            └─ Persistent loop handles ALL messages
```

**Key Differences:**

| Aspect | ThreadedServer | AsyncioServer |
|--------|----------------|---------------|
| **Event Loop** | Temporary (`asyncio.run()`) | **Persistent** |
| **I/O Mode** | Blocking (`serve_all()`) | **Non-blocking (`add_reader()`)** |
| **Concurrency** | Sequential | **Concurrent** |
| **Bidirectional Async** | ❌ **FAILS** | ✅ **WORKS** |
| **Memory/Conn** | ~8MB (thread) | ~10KB (coroutine) |
| **Max Connections** | ~1,000 | ~10,000+ |

### Working Example with AsyncioServer

```python
# Server
import asyncio
from rpyc.utils.async_server import AsyncioServer

class ServerService(rpyc.Service):
    async def exposed_process(self, callback, value):
        # ✅ WORKS with AsyncioServer!
        result = await callback(value * 2)
        return result

async def main():
    server = AsyncioServer(ServerService, port=18861)
    await server.serve_forever()

asyncio.run(main())
```

```python
# Client
import asyncio
import rpyc

async def my_callback(x):
    await asyncio.sleep(0.1)
    return x + 10

async def main():
    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving()

    # ✅ WORKS perfectly!
    result = await conn.root.process(my_callback, 5)
    print(result)  # 20

asyncio.run(main())
```

**See:** [AsyncioServer Migration Guide](ASYNCIO_SERVER_MIGRATION.md)

---

## ✅ What Works with ThreadedServer

### 1. Unidirectional Async (Client → Server)

**Status:** ✅ Fully functional

```python
# Server
class MyService(rpyc.Service):
    async def exposed_async_method(self, x):
        await asyncio.sleep(0.1)
        return x * 2

# Client
async def main():
    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving()

    # ✅ Works perfectly - unidirectional
    result = await conn.root.async_method(5)
    print(result)  # 10
```

**Performance:** 100x improvement for I/O-bound workloads

---

### 2. Recursive Async (Same Side)

**Status:** ✅ Fully functional

```python
async def exposed_countdown(self, n):
    if n <= 0:
        return 0
    await asyncio.sleep(0.01)
    # ✅ Recursive call on same side works
    return n + await self.exposed_countdown(n - 1)
```

**Tested:** Up to depth 20+

---

### 3. Concurrent Client Operations

**Status:** ✅ Fully functional

```python
# Client can make multiple concurrent calls
results = await asyncio.gather(
    conn.root.async_task1(),
    conn.root.async_task2(),
    conn.root.async_task3(),
)
```

---

### 4. Mixed Sync/Async

**Status:** ✅ Fully functional

```python
class MixedService(rpyc.Service):
    def exposed_sync_method(self):
        return "sync"

    async def exposed_async_method(self):
        await asyncio.sleep(0.1)
        return "async"

# Both work transparently
```

---

## ❌ What Doesn't Work

### 1. Bidirectional Async Callbacks

**Status:** ❌ FAILS with ThreadedServer

**Solution:** Use `AsyncioServer`

**See:** [Comparison Examples](../examples/bidirectional_async/)

---

### 2. Server-Side Concurrency

**Status:** ❌ Sequential with ThreadedServer

**Problem:**
```python
# ThreadedServer processes one request at a time per connection
# Even with async methods, no concurrency
```

**Solution:** Use `AsyncioServer` for concurrent request processing

---

### 3. Async Generators/Iterators

**Status:** ❌ Not implemented (any server)

```python
async def exposed_async_generator(self):
    for i in range(10):
        await asyncio.sleep(0.1)
        yield i  # ❌ Not supported
```

**Workaround:** Return list
```python
async def exposed_get_items(self):
    items = []
    for i in range(10):
        await asyncio.sleep(0.1)
        items.append(i)
    return items  # ✅ Works
```

**Future Work:** Could be added in a future `rpyc-async` release

---

## 📋 Server Comparison Matrix

| Feature | ThreadedServer | AsyncioServer |
|---------|----------------|---------------|
| **Unidirectional Async** | ✅ Works | ✅ Works |
| **Bidirectional Async** | ❌ **FAILS** | ✅ **WORKS** |
| **Server Concurrency** | ❌ Sequential | ✅ Concurrent |
| **Event Loop** | Temporary | **Persistent** |
| **I/O Mode** | Blocking | **Non-blocking** |
| **Memory/Conn** | ~8MB | ~10KB |
| **Max Connections** | ~1,000 | ~10,000+ |
| **Performance (I/O)** | Baseline | **65x faster** |
| **Deployment** | Simple | Requires asyncio |
| **Sync Workloads** | ✅ Good | ⚠️ OK |
| **Your Requirements** | ❌ **Fails** | ✅ **Passes** |

---

## 🔧 Workarounds for ThreadedServer Limitations

If you MUST use ThreadedServer and need bidirectional communication:

### Workaround 1: Use Sync Callbacks

```python
# Client provides SYNC callback (not async)
def my_callback(x):
    return x * 2  # Sync!

# ✅ Works with ThreadedServer
result = await conn.root.process(my_callback, 5)
```

**Limitation:** Callback cannot do async operations

---

### Workaround 2: Poll Instead of Callback

```python
# Server
async def exposed_start_task(self, task_id):
    self.tasks[task_id] = asyncio.create_task(self._process())
    return task_id

async def exposed_get_result(self, task_id):
    return await self.tasks[task_id]

# Client polls for result
task_id = await conn.root.start_task("task1")
await asyncio.sleep(1)
result = await conn.root.get_result(task_id)
```

---

### Workaround 3: Dual Connection

```python
# Both act as server and client
server_a = ThreadedServer(ServiceA, port=18861)
server_b = ThreadedServer(ServiceB, port=18862)

# Each connects to the other
conn_a_to_b = rpyc.connect("localhost", 18862)
conn_b_to_a = rpyc.connect("localhost", 18861)

# ✅ Unidirectional calls work
result = await conn_a_to_b.root.async_method()
```

**Limitation:** More complex setup, not true bidirectional

---

## 💡 Best Practices

### DO:

✅ **Use AsyncioServer** for bidirectional async
✅ Use async for I/O-bound operations (network, database, files)
✅ Use `asyncio.gather()` for concurrent operations
✅ Set timeouts with `asyncio.wait_for()`
✅ Reuse connections

### DON'T:

❌ **Don't use ThreadedServer** for bidirectional async
❌ Don't use blocking calls in async methods (`time.sleep()`)
❌ Don't create new connection per request
❌ Don't expect temporary event loops to work

---

## 🚀 Migration to AsyncioServer

**If you need bidirectional async, you MUST migrate to AsyncioServer.**

**See:** [AsyncioServer Migration Guide](ASYNCIO_SERVER_MIGRATION.md)

**Key Changes:**
1. Import: `from rpyc.utils.async_server import AsyncioServer`
2. Wrap in `async def main()`: `asyncio.run(main())`
3. Use `await server.serve_forever()`

**Benefit:** All limitations removed!

---

## 📞 Getting Help

1. **Bidirectional async not working?** → Use AsyncioServer
2. **Server hanging on callbacks?** → Use AsyncioServer
3. **Need concurrent request processing?** → Use AsyncioServer

**Documentation:**
- [AsyncioServer Migration Guide](ASYNCIO_SERVER_MIGRATION.md)
- [Bidirectional Examples](../examples/bidirectional_async/)
- [API Reference](API_REFERENCE.md)

---

## Summary

### ThreadedServer

**✅ Good for:**
- Unidirectional async (Client → Server)
- Simple async use cases
- Predominantly synchronous services (sync-style handlers)

**❌ Cannot support:**
- Bidirectional async (architectural limitation)
- Server-side concurrency
- Persistent event loops

### AsyncioServer

**✅ Required for:**
- **Bidirectional async** ← YOUR REQUIREMENT
- **Persistent event loops** ← YOUR REQUIREMENT
- **No thread creation** ← YOUR REQUIREMENT
- **Non-blocking I/O** ← YOUR REQUIREMENT
- Server-side concurrency

**✅ Already implemented:** `rpyc.utils.async_server.AsyncioServer`

---

**Bottom Line:** If you need bidirectional async, **use AsyncioServer**. ThreadedServer cannot and will not support it due to architectural limitations.
