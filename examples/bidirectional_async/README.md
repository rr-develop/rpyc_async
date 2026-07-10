# Bidirectional Async Examples

This directory contains comprehensive examples demonstrating bidirectional async support with `AsyncioServer`.

## Examples

### Basic Examples

1. **`01_simple_bidirectional.py`** - Simple server calling client callback
2. **`02_recursive_callbacks.py`** - Recursive server ↔ client calls
3. **`03_concurrent_callbacks.py`** - Multiple concurrent async callbacks

### Advanced Examples

4. **`04_real_world_data_processing.py`** - Real-world data processing pipeline
5. **`05_progress_monitoring.py`** - Progress monitoring with callbacks
6. **`06_distributed_computation.py`** - Distributed computation example

### Comparison Examples

7. **`threaded_server_failure.py`** - Demonstrates ThreadedServer limitation (deadlock)
8. **`asyncio_server_success.py`** - Same scenario working with AsyncioServer

## Running Examples

### Basic Usage

```bash
# Terminal 1: Start server
python3 examples/bidirectional_async/01_simple_bidirectional.py server

# Terminal 2: Start client
python3 examples/bidirectional_async/01_simple_bidirectional.py client
```

### All-in-One

Some examples can run server and client in same process:

```bash
python3 examples/bidirectional_async/02_recursive_callbacks.py
```

## Requirements

- Python 3.8+
- rpyc (with AsyncioServer support)
- asyncio

## Key Concepts

### Persistent Event Loops

All examples use persistent event loops:

**Server:**
```python
from rpyc_async.utils.async_server import AsyncioServer

async def main():
    server = AsyncioServer(MyService, port=18861)
    await server.serve_forever()  # ← Persistent loop

asyncio.run(main())
```

**Client:**
```python
async def main():
    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving()  # ← Register with persistent loop

    result = await conn.root.async_method()
```

### Bidirectional Communication

Both server and client can call each other's async methods:

```
Client ─────→ Server.exposed_method(client_callback)
         ↖───── Server calls: await client_callback(data)
Client receives callback ─────→ Processes data
         └─────→ Can call server again!
```

## Common Patterns

### Pattern 1: Callback for Progress Updates

Server calls client callback to report progress.

```python
# Server
async def exposed_long_task(self, progress_callback):
    for i in range(100):
        await asyncio.sleep(0.1)
        await progress_callback(i + 1)  # Report progress
    return "Done"

# Client
async def on_progress(percent):
    print(f"Progress: {percent}%")

result = await conn.root.long_task(on_progress)
```

### Pattern 2: Distributed Map-Reduce

Server distributes work, client processes and returns results.

```python
# Server
async def exposed_map_reduce(self, worker_callback, data):
    # Map: distribute work to client
    results = []
    for item in data:
        result = await worker_callback(item)
        results.append(result)

    # Reduce
    return sum(results)

# Client
async def worker(item):
    # Process item
    return item * 2

total = await conn.root.map_reduce(worker, [1, 2, 3, 4, 5])
```

### Pattern 3: Recursive Tree Traversal

Server and client recursively traverse data structure.

```python
# Server
async def exposed_process_node(self, callback, node, depth):
    if node is None:
        return None

    # Process current node
    value = await callback(node.value, depth)

    # Recurse to children
    left = await self.exposed_process_node(callback, node.left, depth + 1)
    right = await self.exposed_process_node(callback, node.right, depth + 1)

    return {"value": value, "left": left, "right": right}
```

## Troubleshooting

### Issue: Client Hangs

**Cause:** Forgot to enable asyncio serving

**Solution:**
```python
conn.enable_asyncio_serving()  # ← Required!
```

### Issue: Server Deadlock

**Cause:** Using ThreadedServer instead of AsyncioServer

**Solution:**
```python
# ❌ Wrong
from rpyc_async import ThreadedServer
server = ThreadedServer(MyService, port=18861)

# ✅ Correct
from rpyc_async.utils.async_server import AsyncioServer
server = AsyncioServer(MyService, port=18861)
```

### Issue: RuntimeError: No Running Event Loop

**Cause:** Not running in async context

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

## Further Reading

- [AsyncioServer Migration Guide](../../docs/ASYNCIO_SERVER_MIGRATION.md)
- [API Reference](../../docs/API_REFERENCE.md)
- [Limitations](../../docs/LIMITATIONS.md)
