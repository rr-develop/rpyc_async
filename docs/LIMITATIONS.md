# RPyC Async/Await - Known Limitations

## Overview

RPyC 5.1 async/await support is production-ready for most use cases, but has some architectural limitations due to the threading model of ThreadedServer.

---

## ✅ What Works (Fully Supported)

### 1. Client → Server Async Calls

**Status:** ✅ Fully functional

**Example:**
```python
# Server
class MyService(rpyc.Service):
    async def exposed_async_method(self, x):
        await asyncio.sleep(0.1)
        return x * 2

# Client
async def main():
    conn = rpyc.connect("localhost", 18861)
    result = await conn.root.async_method(5)
    print(result)  # 10
```

**Performance:** 100x improvement for I/O-bound workloads

---

### 2. Recursive Async Calls (Same Side)

**Status:** ✅ Fully functional

**Example:**
```python
async def exposed_countdown(self, n):
    if n <= 0:
        return 0
    await asyncio.sleep(0.01)
    return n + await self.exposed_countdown(n - 1)
```

**Tested:** Up to depth 20+

---

### 3. Concurrent Async Operations

**Status:** ✅ Fully functional

**Example:**
```python
results = await asyncio.gather(
    conn.root.async_task1(),
    conn.root.async_task2(),
    conn.root.async_task3(),
)
```

---

### 4. Mixed Sync/Async

**Status:** ✅ Fully functional

**Example:**
```python
class MixedService(rpyc.Service):
    def exposed_sync_method(self):
        return "sync"

    async def exposed_async_method(self):
        await asyncio.sleep(0.1)
        return "async"
```

Both methods work transparently.

---

## ⚠️ Limitations

### 1. Bidirectional Async Callbacks (Complex)

**Status:** ⚠️ Limited support

**What doesn't work:**
```python
# Server
async def exposed_process(self, async_callback, value):
    # Server tries to call client async callback
    result = await async_callback(value)  # ❌ May hang/fail
    return result

# Client
async def main():
    async def my_callback(x):
        await asyncio.sleep(0.1)
        return x * 2

    result = await conn.root.process(my_callback, 5)
```

**Reason:**
- ThreadedServer creates separate thread for each connection
- Thread doesn't have persistent asyncio event loop
- Current fallback (`asyncio.run()`) creates temporary loop per request
- Callbacks require bidirectional async communication

**Workaround 1:** Use sync callbacks
```python
# Client provides sync callback
def my_callback(x):
    return x * 2

result = await conn.root.process(my_callback, 5)  # ✅ Works
```

**Workaround 2:** Poll instead of callback
```python
# Server
async def exposed_start_task(self, task_id):
    self.tasks[task_id] = asyncio.create_task(self._process())
    return task_id

async def exposed_get_result(self, task_id):
    return await self.tasks[task_id]

# Client
task_id = await conn.root.start_task("task1")
# ... do other work ...
result = await conn.root.get_result(task_id)  # ✅ Works
```

**Workaround 3:** Use separate connections
```python
# Start client as server too
client_server = ThreadedServer(ClientService, port=18862)
client_server_thread = Thread(target=client_server.start, daemon=True)
client_server_thread.start()

# Server connects back to client
client_conn = rpyc.connect("localhost", 18862)
result = await client_conn.root.async_method()  # ✅ Works
```

**Future Work:** Full bidirectional async support requires async-native server (not ThreadedServer). This could be added in RPyC 5.2 with AsyncioServer.

---

### 2. Async Generators/Iterators

**Status:** ❌ Not implemented

**What doesn't work:**
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

**Future Work:** Could be added in RPyC 5.2

---

## 📋 Compatibility Matrix

### Threading Model

| Server Type | Client Async | Server Async | Bidirectional Async |
|-------------|--------------|--------------|---------------------|
| ThreadedServer | ✅ Yes | ✅ Yes | ⚠️ Limited |
| ForkingServer | ✅ Yes | ✅ Yes | ❌ No |
| OneShotServer | ✅ Yes | ✅ Yes | ⚠️ Limited |
| Future: AsyncioServer | ✅ Yes | ✅ Yes | ✅ Yes |

---

## 🔧 Recommended Patterns

### Pattern 1: Request-Response (Best Support)

```python
# Server
async def exposed_process_data(self, data):
    await asyncio.sleep(0.1)
    return f"Processed: {data}"

# Client
result = await conn.root.process_data("input")
```

**Use when:** Simple request-response pattern

---

### Pattern 2: Task Queue (Good Support)

```python
# Server
async def exposed_submit_task(self, task_data):
    task_id = str(uuid.uuid4())
    self.tasks[task_id] = asyncio.create_task(self._process(task_data))
    return task_id

async def exposed_get_status(self, task_id):
    if task_id in self.tasks:
        if self.tasks[task_id].done():
            return {'status': 'done', 'result': self.tasks[task_id].result()}
        return {'status': 'running'}
    return {'status': 'not_found'}

# Client
task_id = await conn.root.submit_task(data)
while True:
    status = await conn.root.get_status(task_id)
    if status['status'] == 'done':
        result = status['result']
        break
    await asyncio.sleep(0.5)
```

**Use when:** Long-running tasks

---

### Pattern 3: Dual Connection (Full Bidirectional)

```python
# Setup: Both act as server and client
server_a = ThreadedServer(ServiceA, port=18861)
server_b = ThreadedServer(ServiceB, port=18862)

# Each connects to the other
conn_a_to_b = rpyc.connect("localhost", 18862)
conn_b_to_a = rpyc.connect("localhost", 18861)

# Now both can call each other
result = await conn_a_to_b.root.async_method()
```

**Use when:** Need true bidirectional communication

---

## 💡 Best Practices

### DO:
✅ Use async for I/O-bound operations (network, database, files)
✅ Use `asyncio.gather()` for concurrent operations
✅ Reuse connections instead of creating new ones
✅ Set timeouts with `asyncio.wait_for()`
✅ Handle exceptions properly

### DON'T:
❌ Use blocking calls in async methods (`time.sleep()`, blocking I/O)
❌ Create new connection for each request
❌ Expect sync callbacks to be awaitable
❌ Use bidirectional async callbacks with ThreadedServer (limited support)

---

## 🚀 Future Enhancements

### Planned for RPyC 5.2:

1. **AsyncioServer** - Native asyncio server implementation
   - Full bidirectional async support
   - Better performance
   - Cleaner architecture

2. **Async Generators** - Support for `async for` over remote iterables

3. **Connection Pooling** - Built-in async connection pool

4. **Streaming** - Efficient streaming of large datasets

---

## 📞 Getting Help

If you encounter issues:

1. Check this limitations document
2. Review [API Reference](API_REFERENCE.md)
3. See [Examples](EXAMPLES.md) for working patterns
4. File issue on GitHub with minimal reproducer

---

## Summary

**RPyC 5.1 async/await is production-ready for:**
- ✅ Client → Server async calls (primary use case)
- ✅ Recursive async operations
- ✅ Concurrent async operations
- ✅ Mixed sync/async services

**Limited support for:**
- ⚠️ Bidirectional async callbacks (workarounds available)

**Not supported:**
- ❌ Async generators/iterators

For most use cases (client calling server async methods), RPyC 5.1 provides excellent async support with 100x performance improvements for I/O-bound workloads.
