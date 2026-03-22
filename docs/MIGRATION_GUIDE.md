# Migration Guide: RPyC 5.0 → 5.1 (Async/Await)

This guide helps you migrate from RPyC 5.0 to 5.1 with async/await support.

## Overview

**RPyC 5.1** adds native async/await support while maintaining **100% backward compatibility** with v5.0.

**Key Points:**
- ✅ All existing v5.0 code continues to work unchanged
- ✅ Sync and async methods can coexist in same service
- ✅ No breaking changes
- ✅ Opt-in async support

---

## Quick Start

### Before (RPyC 5.0)

```python
# server.py
import rpyc
import time

class MyService(rpyc.Service):
    def exposed_slow_operation(self):
        time.sleep(1)  # Blocks thread!
        return "done"
```

```python
# client.py
import rpyc

conn = rpyc.connect("localhost", 18861)
result = conn.root.slow_operation()  # Blocks for 1 second
print(result)
```

### After (RPyC 5.1)

```python
# server.py
import rpyc
import asyncio

class MyService(rpyc.Service):
    async def exposed_slow_operation(self):
        await asyncio.sleep(1)  # Non-blocking!
        return "done"
```

```python
# client.py
import rpyc
import asyncio

async def main():
    conn = rpyc.connect("localhost", 18861)
    result = await conn.root.slow_operation()  # Awaitable!
    print(result)

asyncio.run(main())
```

---

## Migration Strategies

### Strategy 1: Gradual Migration (Recommended)

Migrate one method at a time while keeping service running.

**Step 1:** Add async methods alongside sync methods

```python
class MyService(rpyc.Service):
    # Existing sync method (keep for backward compat)
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

**Step 2:** Update clients gradually

```python
# Old clients still use sync method
result = conn.root.fetch_data("https://example.com")

# New clients use async method
result = await conn.root.async_fetch_data("https://example.com")
```

**Step 3:** Deprecate sync methods after all clients migrated

---

### Strategy 2: Parallel Services

Run both v5.0 and v5.1 services on different ports.

```python
# server_v5.py (port 18860) - Legacy
class LegacyService(rpyc.Service):
    def exposed_method(self):
        return "sync"

# server_v51.py (port 18861) - Async
class AsyncService(rpyc.Service):
    async def exposed_method(self):
        return "async"
```

Clients connect to appropriate port based on their version.

---

### Strategy 3: Feature Flag

Use feature flag to enable async behavior.

```python
class HybridService(rpyc.Service):
    def __init__(self, enable_async=False):
        self.enable_async = enable_async

    def exposed_process(self, data):
        if self.enable_async:
            import asyncio
            return asyncio.run(self._async_process(data))
        else:
            return self._sync_process(data)

    async def _async_process(self, data):
        await asyncio.sleep(0.1)
        return f"Async: {data}"

    def _sync_process(self, data):
        import time
        time.sleep(0.1)
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

class DBService(rpyc.Service):
    async def on_connect_async(self):
        self.pool = await asyncpg.create_pool(self.dsn)

    async def exposed_query(self, sql):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql)
            return [dict(row) for row in rows]
```

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

async def main():
    conn = rpyc.connect("localhost", 18861)
    result = await conn.root.method()
    conn.close()

asyncio.run(main())
```

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

### Unit Tests

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
import asyncio

class TestMyService(unittest.TestCase):
    def test_method(self):
        async def run_test():
            conn = rpyc.connect("localhost", 18861)
            result = await conn.root.method()
            self.assertEqual(result, "expected")
            conn.close()

        asyncio.run(run_test())
```

Or use `unittest.IsolatedAsyncioTestCase` (Python 3.8+):

```python
import unittest

class TestMyService(unittest.IsolatedAsyncioTestCase):
    async def test_method(self):
        conn = rpyc.connect("localhost", 18861)
        result = await conn.root.method()
        self.assertEqual(result, "expected")
        conn.close()
```

---

## Performance Considerations

### When to Migrate

**Migrate to async when:**
- ✅ Service performs I/O operations (network, disk, database)
- ✅ Service calls other async APIs
- ✅ Need to handle many concurrent requests efficiently
- ✅ Methods have high latency

**Keep sync when:**
- ❌ Methods are CPU-bound computations
- ❌ Methods are simple getters/setters
- ❌ Methods complete immediately (<1ms)
- ❌ No async dependencies available

---

### Performance Comparison

**Benchmark: 100 concurrent requests**

```python
# Sync version (sequential processing)
# Time: ~100 seconds (1 second per request)

class SyncService(rpyc.Service):
    def exposed_process(self, data):
        time.sleep(1)
        return data
```

```python
# Async version (concurrent processing)
# Time: ~1 second (all concurrent)

class AsyncService(rpyc.Service):
    async def exposed_process(self, data):
        await asyncio.sleep(1)
        return data
```

**Result:** 100x improvement for I/O-bound workloads!

---

## Compatibility Matrix

### Client vs Server

| Client Version | Server Version | Sync Methods | Async Methods |
|----------------|----------------|--------------|---------------|
| 5.1            | 5.1            | ✅ Works     | ✅ Works      |
| 5.1            | 5.0            | ✅ Works     | ❌ Fails      |
| 5.0            | 5.1 (sync)     | ✅ Works     | N/A           |
| 5.0            | 5.1 (async)    | ❌ Fails     | N/A           |

**Recommendation:** Upgrade both client and server to v5.1 for full async support.

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

---

### 3. Mixing Sync/Async Incorrectly

**Wrong:**
```python
def sync_method(self):
    result = await self.async_helper()  # SyntaxError!
```

**Correct:**
```python
def sync_method(self):
    result = asyncio.run(self.async_helper())  # OK

async def async_method(self):
    result = await self.async_helper()  # OK
```

---

### 4. Not Handling Connection Lifecycle

**Wrong:**
```python
async def process_many():
    for item in items:
        conn = rpyc.connect("localhost", 18861)  # Creates 1000 connections!
        await conn.root.process(item)
        conn.close()
```

**Correct:**
```python
async def process_many():
    conn = rpyc.connect("localhost", 18861)  # Reuse connection
    try:
        for item in items:
            await conn.root.process(item)
    finally:
        conn.close()
```

---

## Troubleshooting

### Issue: "RuntimeError: no running event loop"

**Cause:** Calling async code outside async context

**Solution:**
```python
# Wrong
result = await conn.root.async_method()

# Correct
async def main():
    result = await conn.root.async_method()

asyncio.run(main())
```

---

### Issue: AsyncResult never completes

**Cause:** Connection not processing incoming messages

**Solution:** AsyncResult automatically handles this in v5.1 via background polling

---

### Issue: Performance worse after migration

**Cause:** Not using concurrent operations

**Solution:**
```python
# Before (fast with asyncio.gather)
results = await asyncio.gather(*[
    conn.root.process(item) for item in items
])

# Wrong (slow - sequential)
results = []
for item in items:
    result = await conn.root.process(item)
    results.append(result)
```

---

## Rollback Plan

If you need to rollback to v5.0:

1. **Keep old sync methods** during migration
2. **Test thoroughly** before removing sync methods
3. **Use version detection** in clients:

```python
def is_async_supported(conn):
    """Check if server supports async."""
    try:
        # Try to get protocol version
        version = conn._config.get('protocol_version', (5, 0))
        return version >= (5, 1)
    except:
        return False

# Client usage
if is_async_supported(conn):
    result = await conn.root.async_method()
else:
    result = conn.root.sync_method()
```

---

## Further Reading

- [API Reference](API_REFERENCE.md)
- [Examples](EXAMPLES.md)
- [Implementation Design](../IMPLEMENTATION_DESIGN.md)

---

## Getting Help

- GitHub Issues: https://github.com/tomerfiliba-org/rpyc/issues
- Documentation: https://rpyc.readthedocs.io/
- Community: RPyC mailing list
