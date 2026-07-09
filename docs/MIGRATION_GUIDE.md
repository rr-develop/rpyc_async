# Migration Guide: moving to the asyncio-native API (rpyc-async)

This guide helps you move existing RPC code onto **rpyc-async 1.0.0**, the asyncio-native
fork of RPyC (forked from upstream RPyC 6.0.1).

## Overview

`rpyc-async` is a **separate product**, not a drop-in upgrade of classic synchronous RPyC.
It is built around `async_connect()` and `AsyncioServer`, and a persistent event loop drives
the connection on both ends.

**Key Points:**
- Backward compatibility with classic synchronous RPyC is **not guaranteed**
- The import name stays `import rpyc`; only the distribution name changes
- Async services, async clients and bidirectional async callbacks are first-class
- Requires Python 3.10 or newer

**Installation:**

```bash
pip install rpyc-async     # distribution name
```

```python
import rpyc                # import name is unchanged
```

If you need the classic synchronous behaviour, install upstream RPyC instead
(`pip install rpyc`). Both distributions provide the same import name, so install only one
of them per environment.

---

## Quick Start

### Before (classic synchronous RPyC)

```python
# server.py
import rpyc
from rpyc.utils.server import ThreadedServer
import time

class MyService(rpyc.Service):
    def exposed_slow_operation(self):
        time.sleep(1)  # Blocks thread!
        return "done"

ThreadedServer(MyService, port=18861).start()
```

```python
# client.py
import rpyc

conn = rpyc.connect("localhost", 18861)
result = conn.root.slow_operation()  # Blocks for 1 second
print(result)
```

### After (rpyc-async)

```python
# server.py
import rpyc
import asyncio
from rpyc.utils.async_server import AsyncioServer

class MyService(rpyc.Service):
    async def exposed_slow_operation(self):
        await asyncio.sleep(1)  # Non-blocking!
        return "done"

async def main():
    server = AsyncioServer(MyService, hostname="localhost", port=18861)
    await server.serve_forever()

asyncio.run(main())
```

```python
# client.py
import rpyc
import asyncio
from rpyc.core.async_connect import async_connect

async def main():
    conn = await async_connect("localhost", 18861)
    result = await conn.root.slow_operation()  # Awaitable!
    print(result)

asyncio.run(main())
```

`async_connect`, `AsyncioServer` and `run_async_server` are also re-exported from the
top-level package (`rpyc.async_connect`, `rpyc.AsyncioServer`, `rpyc.run_async_server`).

---

## Migration Strategies

### Strategy 1: Gradual Migration (Recommended)

Port one method at a time while the service keeps running.

**Step 1:** Add async methods alongside the existing ones

```python
class MyService(rpyc.Service):
    # Existing synchronous method (still callable while you migrate)
    def exposed_fetch_data(self, url):
        import requests
        response = requests.get(url)
        return response.text

    # New async method
    async def exposed_async_fetch_data(self, url):
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.text()
```

**Step 2:** Update call sites gradually

```python
# Not-yet-migrated call sites keep using the sync method
result = conn.root.fetch_data("https://example.com")

# Migrated call sites use the async method
result = await conn.root.async_fetch_data("https://example.com")
```

**Step 3:** Remove the synchronous methods once every call site is migrated

Synchronous methods still block the server's event loop. Treat them as a temporary bridge,
not a supported long-term mode.

---

### Strategy 2: Parallel Services

Run the legacy service and the asyncio-native service on different ports during the
transition, with the legacy one served by upstream RPyC in its own environment.

```python
# server_legacy.py (port 18860) - classic synchronous RPyC, separate environment
class LegacyService(rpyc.Service):
    def exposed_method(self):
        return "sync"

# server_async.py (port 18861) - rpyc-async + AsyncioServer
class AsyncService(rpyc.Service):
    async def exposed_method(self):
        return "async"
```

Clients connect to the port matching the stack they have been migrated to.

---

### Strategy 3: Feature Flag

Use a feature flag to switch a call path over to the async implementation.

```python
class HybridService(rpyc.Service):
    def __init__(self, enable_async=False):
        self.enable_async = enable_async

    async def exposed_process(self, data):
        if self.enable_async:
            return await self._async_process(data)
        else:
            return self._sync_process(data)

    async def _async_process(self, data):
        await asyncio.sleep(0.1)
        return f"Async: {data}"

    def _sync_process(self, data):
        import time
        time.sleep(0.1)  # Blocks the event loop - migrate this away
        return f"Sync: {data}"
```

---

## Converting Common Patterns

### I/O-Bound Operations

**Before (blocking):**
```python
import requests

class DataService(rpyc.Service):
    def exposed_fetch_user(self, user_id):
        response = requests.get(f"https://api.example.com/users/{user_id}")
        return response.json()
```

**After (async):**
```python
import aiohttp

class DataService(rpyc.Service):
    async def exposed_fetch_user(self, user_id):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://api.example.com/users/{user_id}") as response:
                return await response.json()
```

---

### Database Operations

**Before (blocking):**
```python
import psycopg2

class DBService(rpyc.Service):
    def exposed_query(self, sql):
        conn = psycopg2.connect(self.dsn)
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        conn.close()
        return results
```

**After (async):**
```python
import asyncpg
from rpyc.utils.async_server import AsyncioServer

class DBService(rpyc.Service):
    def __init__(self, pool):
        self.pool = pool

    async def exposed_query(self, sql):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql)
            return [dict(row) for row in rows]

async def main(dsn):
    pool = await asyncpg.create_pool(dsn)          # await once, before serving
    server = AsyncioServer(DBService(pool), hostname="localhost", port=18861)
    await server.serve_forever()
```

There is **no async `on_connect` hook**. `Service.on_connect(conn)` is called
synchronously and its return value is never awaited, so a coroutine defined there
would never run. Acquire async resources (pools, sessions, clients) *before*
starting the server and inject them into the service, as shown above.

---

### File Operations

**Before (blocking):**
```python
class FileService(rpyc.Service):
    def exposed_read_file(self, path):
        with open(path, 'r') as f:
            return f.read()
```

**After (async):**
```python
import aiofiles

class FileService(rpyc.Service):
    async def exposed_read_file(self, path):
        async with aiofiles.open(path, 'r') as f:
            return await f.read()
```

---

### Concurrent Operations

**Before (sequential):**
```python
class BatchService(rpyc.Service):
    def exposed_process_batch(self, items):
        results = []
        for item in items:
            result = self._process_one(item)  # Sequential!
            results.append(result)
        return results

    def _process_one(self, item):
        import time
        time.sleep(1)
        return item * 2
```

**After (concurrent):**
```python
class BatchService(rpyc.Service):
    async def exposed_process_batch(self, items):
        tasks = [self._process_one(item) for item in items]
        results = await asyncio.gather(*tasks)  # Concurrent!
        return results

    async def _process_one(self, item):
        await asyncio.sleep(1)
        return item * 2
```

---

## Client Migration

### Basic Client

**Before:**
```python
import rpyc

conn = rpyc.connect("localhost", 18861)
result = conn.root.method()
conn.close()
```

**After:**
```python
import rpyc
import asyncio
from rpyc.core.async_connect import async_connect

async def main():
    conn = await async_connect("localhost", 18861)
    try:
        result = await conn.root.method()
    finally:
        conn.close()

asyncio.run(main())
```

`async_connect()` performs a non-blocking TCP connect and an eager handshake, so the first
access to `conn.root` never blocks the event loop.

---

### Multiple Calls

**Before (sequential):**
```python
result1 = conn.root.method1()
result2 = conn.root.method2()
result3 = conn.root.method3()
```

**After (concurrent):**
```python
results = await asyncio.gather(
    conn.root.method1(),
    conn.root.method2(),
    conn.root.method3(),
)
result1, result2, result3 = results
```

---

### Error Handling

**Before:**
```python
try:
    result = conn.root.method()
except Exception as e:
    print(f"Error: {e}")
```

**After:**
```python
try:
    result = await conn.root.method()
except Exception as e:
    print(f"Error: {e}")
```

Error handling is the same!

---

## Testing

**Before:**
```python
import unittest

class TestMyService(unittest.TestCase):
    def test_method(self):
        conn = rpyc.connect("localhost", 18861)
        result = conn.root.method()
        self.assertEqual(result, "expected")
        conn.close()
```

**After:**
```python
import unittest
from rpyc.core.async_connect import async_connect

class TestMyService(unittest.IsolatedAsyncioTestCase):
    async def test_method(self):
        conn = await async_connect("localhost", 18861)
        try:
            result = await conn.root.method()
            self.assertEqual(result, "expected")
        finally:
            conn.close()
```

`unittest.IsolatedAsyncioTestCase` is the recommended base class; it is available on every
supported Python version (3.10+).

---

## Performance Considerations

### What benefits most

**Async pays off when:**
- The method performs I/O (network, disk, database)
- The method calls other async APIs
- You need to handle many concurrent requests efficiently
- Methods have high latency

**Async gains little when:**
- Methods are CPU-bound computations (offload them to a thread or process pool)
- Methods are simple getters/setters
- Methods complete immediately (<1ms)
- No async dependency is available for the underlying library

---

### Performance Comparison

**Benchmark: 100 concurrent requests**

```python
# Blocking implementation (sequential processing)
# Time: ~100 seconds (1 second per request)

class SyncService(rpyc.Service):
    def exposed_process(self, data):
        time.sleep(1)
        return data
```

```python
# Async implementation (concurrent processing)
# Time: ~1 second (all concurrent)

class AsyncService(rpyc.Service):
    async def exposed_process(self, data):
        await asyncio.sleep(1)
        return data
```

**Result:** large improvement for I/O-bound workloads.

---

## Compatibility

Both ends of the connection must run `rpyc-async`: an async client created with
`async_connect()` talking to a server built on `AsyncioServer`. Mixing `rpyc-async` with
upstream RPyC on the other end of the wire is not a supported configuration.

---

## Common Pitfalls

### 1. Forgetting `await`

**Wrong:**
```python
result = conn.root.async_method()  # Returns AsyncResult, not value!
print(result)  # Prints <AsyncResult object>
```

**Correct:**
```python
result = await conn.root.async_method()  # Awaits and gets value
print(result)  # Prints actual result
```

---

### 2. Blocking in Async Context

**Wrong:**
```python
async def exposed_method(self):
    time.sleep(1)  # Blocks event loop!
    return "done"
```

**Correct:**
```python
async def exposed_method(self):
    await asyncio.sleep(1)  # Non-blocking
    return "done"
```

For unavoidable blocking calls, use `await asyncio.to_thread(blocking_call, ...)`.

---

### 3. Mixing Sync/Async Incorrectly

**Wrong:**
```python
def sync_method(self):
    result = await self.async_helper()  # SyntaxError!
```

**Correct:**
```python
async def async_method(self):
    result = await self.async_helper()  # OK
```

Do not call `asyncio.run()` from inside a coroutine or from an exposed method that is
already being served by the running loop - it raises `RuntimeError`.

---

### 4. Not Handling Connection Lifecycle

**Wrong:**
```python
async def process_many(items):
    for item in items:
        conn = await async_connect("localhost", 18861)  # Creates 1000 connections!
        await conn.root.process(item)
        conn.close()
```

**Correct:**
```python
async def process_many(items):
    conn = await async_connect("localhost", 18861)  # Reuse connection
    try:
        for item in items:
            await conn.root.process(item)
    finally:
        conn.close()
```

---

## Troubleshooting

### Issue: "RuntimeError: no running event loop"

**Cause:** Calling async code outside an async context

**Solution:**
```python
# Wrong
result = await conn.root.async_method()

# Correct
async def main():
    conn = await async_connect("localhost", 18861)
    result = await conn.root.async_method()

asyncio.run(main())
```

---

### Issue: AsyncResult never completes

**Cause:** The connection is not serving incoming messages on the event loop.

**Solution:** Connect with `async_connect()` and serve the peer with `AsyncioServer`, so
inbound messages are dispatched by the running loop. For a connection created outside
`async_connect()`, call `conn.enable_asyncio_serving()` before awaiting results.

---

### Issue: Performance worse after migration

**Cause:** Awaiting each call in a loop instead of running them concurrently.

**Solution:**
```python
# Fast (concurrent)
results = await asyncio.gather(*[
    conn.root.process(item) for item in items
])

# Slow (sequential)
results = []
for item in items:
    result = await conn.root.process(item)
    results.append(result)
```

---

## Switching Back

`rpyc-async` is a distinct distribution, so there is no in-place downgrade path. To return
to classic synchronous RPyC, uninstall `rpyc-async` and install upstream RPyC:

```bash
pip uninstall rpyc-async
pip install rpyc
```

Because both distributions expose the same `rpyc` import name, install only one of them per
environment. To ease the transition, keep the synchronous method variants until every call
site has been migrated, and test both paths in isolated environments before removing them.

---

## Further Reading

- [API Reference](API_REFERENCE.md)
- [Examples](EXAMPLES.md)
- [AsyncioServer Migration](ASYNCIO_SERVER_MIGRATION.md)
- [Implementation Design](./IMPLEMENTATION_DESIGN.md)
- [Limitations](LIMITATIONS.md)
