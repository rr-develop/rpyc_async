# rpyc-async Documentation

**rpyc-async 1.0.0** is an asyncio-native fork of RPyC, providing native
async/await support for reliable and efficient asynchronous remote procedure
calls. It is versioned independently of upstream RPyC (forked from RPyC 6.0.1).

## Quick Links

- **[API Reference](API_REFERENCE.md)** - Complete API documentation
- **[Examples](EXAMPLES.md)** - Practical code examples
- **[Migration Guide](MIGRATION_GUIDE.md)** - Moving to the asyncio-native API

### Design & Proposals

- **[Implementation Design](IMPLEMENTATION_DESIGN.md)** - Detailed technical design
- **[Implementation Summary](IMPLEMENTATION_SUMMARY.md)** - High-level summary
- **[Async Support Proposal](ASYNC_SUPPORT_PROPOSAL.md)** / **[V2](ASYNC_SUPPORT_PROPOSAL_V2.md)** - Original proposals
- **[Async Dispatch Pipeline Explained](ASYNC_DISPATCH_PIPELINE_EXPLAINED.md)** - Dispatch internals

### Analysis Notes

- **[Async Callbacks Analysis](analysis/ASYNC_CALLBACKS_ANALYSIS.md)**
- **[Final Analysis](analysis/FINAL_ANALYSIS.md)**
- **[Refcount Monitoring](analysis/REFCOUNT_MONITORING.md)**

---

## Quick Start

### Installation

```bash
# Install rpyc-async
pip install rpyc-async
```

### Simple Example

**Server:**
```python
# server.py
import asyncio
import rpyc
from rpyc.utils.server import ThreadedServer

class MyService(rpyc.Service):
    async def exposed_async_hello(self, name):
        await asyncio.sleep(0.1)  # Simulate async work
        return f"Hello, {name}!"

if __name__ == "__main__":
    server = ThreadedServer(MyService, port=18861)
    print("Server started on port 18861")
    server.start()
```

**Client:**
```python
# client.py
import asyncio
import rpyc

async def main():
    conn = rpyc.connect("localhost", 18861)

    try:
        # Call async method and await result
        result = await conn.root.async_hello("World")
        print(result)  # "Hello, World!"
    finally:
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())
```

Run:
```bash
# Terminal 1
python server.py

# Terminal 2
python client.py
# Output: Hello, World!
```

---

## Features

### ✅ Native Async/Await Support

Call async methods remotely with `await` syntax:

```python
result = await conn.root.async_method()
```

### ✅ Concurrent Operations

Execute multiple async calls concurrently:

```python
results = await asyncio.gather(
    conn.root.async_task1(),
    conn.root.async_task2(),
    conn.root.async_task3(),
)
```

### ⚠️ Not a drop-in replacement for synchronous RPyC

rpyc-async targets the asyncio-native `AsyncioServer` and the async client
(`async_connect`). Backward compatibility with the classic synchronous RPyC
API is **not guaranteed** — code written against upstream sync RPyC may need
changes. See the [Migration Guide](MIGRATION_GUIDE.md).

### Mixed Sync/Async services

A service may still expose both sync and async methods:

```python
class MixedService(rpyc.Service):
    def exposed_sync_method(self):
        return "sync"

    async def exposed_async_method(self):
        await asyncio.sleep(0.1)
        return "async"
```

### ✅ Exception Handling

Async exceptions propagate naturally:

```python
try:
    result = await conn.root.async_method()
except ValueError as e:
    print(f"Remote error: {e}")
```

### ✅ Recursive Calls

Async methods can call themselves recursively:

```python
async def exposed_countdown(self, n):
    if n <= 0:
        return 0
    await asyncio.sleep(0.01)
    return n + await self.exposed_countdown(n - 1)
```

---

## Use Cases

### I/O-Bound Operations

Perfect for network, database, and file operations:

```python
async def exposed_fetch_data(self, url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()
```

### Database Queries

Efficient async database access:

```python
async def exposed_query(self, sql):
    async with self.db_pool.acquire() as conn:
        rows = await conn.fetch(sql)
        return [dict(row) for row in rows]
```

### Concurrent Processing

Process multiple items concurrently:

```python
async def exposed_process_batch(self, items):
    tasks = [self._process_one(item) for item in items]
    return await asyncio.gather(*tasks)
```

---

## Performance

### Benchmark Results

**100 concurrent I/O-bound calls:**

| Mode | Execution Time | Throughput |
|---------|---------------|------------|
| Classic sync RPyC | ~100s | 1 req/s |
| rpyc-async (async) | ~1s | 100 req/s |

**Result:** 100x improvement for I/O-bound workloads!

---

## Architecture

### Protocol Changes

**New Message Types:**
- `MSG_ASYNC_REQUEST` - Async RPC request
- `MSG_ASYNC_REPLY` - Async RPC reply
- `MSG_ASYNC_EXCEPTION` - Async RPC exception

**New Handlers:**
- `HANDLE_ASYNC_CALL` - Execute async function
- `HANDLE_ASYNC_CALLATTR` - Execute async method

### How It Works

1. **Client calls async method** → Returns AsyncResult
2. **AsyncResult is awaitable** → Can use `await`
3. **Server executes async** → Uses asyncio event loop
4. **Result propagates back** → Through MSG_ASYNC_REPLY
5. **Client awaits completion** → Gets final value

---

## Requirements

- **Python:** 3.10+
- **Optional:** aiohttp, asyncpg, aiofiles (for async I/O)

---

## Compatibility

### Python Versions

| Python Version | Supported |
|---------------|-----------|
| 3.10+         | ✅ Yes    |
| 3.9 and below | ❌ No     |

---

## Documentation

### For New Users

1. Start with [Examples](EXAMPLES.md)
2. Read [API Reference](API_REFERENCE.md)
3. Check [Migration Guide](MIGRATION_GUIDE.md) for best practices

### For Existing Users

1. Read [Migration Guide](MIGRATION_GUIDE.md)
2. Review [Examples](EXAMPLES.md) for patterns
3. Consult [API Reference](API_REFERENCE.md) as needed

---

## Contributing

See [Implementation Design](./IMPLEMENTATION_DESIGN.md) for technical details.

---

## License

`rpyc-async` is released under the MIT License, as is the upstream RPyC code it
is derived from. See [LICENSE](../LICENSE).

---

## Support

- **Issues:** https://github.com/rr-develop/rpyc_async/issues
- **Source:** https://github.com/rr-develop/rpyc_async

For questions about *classic synchronous* RPyC, refer to the upstream project at
https://rpyc.readthedocs.io/ instead.
