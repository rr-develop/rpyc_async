# RPyC Async/Await Examples

This document provides practical examples of using async/await with RPyC.

## Table of Contents

- [Basic Usage](#basic-usage)
- [Server Setup](#server-setup)
- [Client Usage](#client-usage)
- [Advanced Patterns](#advanced-patterns)
- [Real-World Examples](#real-world-examples)

---

## Basic Usage

### Simple Async Server

> **Read this first.** Every example below uses `AsyncioServer` and
> `await rpyc.async_connect(...)`.
>
> - `ThreadedServer` **cannot** run `async def exposed_*` methods: without a
>   persistent event loop the call raises `RuntimeError`. Use it only for
>   purely synchronous services.
> - `rpyc.connect()` raises `RuntimeError` when called from a running event
>   loop, because it would block it. Use `await rpyc.async_connect(...)`.
> - Close with `await conn.aclose()`, never `conn.close()` — the latter blocks the
>   event loop waiting for a `HANDLE_CLOSE` reply that usually never arrives,
>   until `sync_request_timeout` (30 s) expires.
> - The server must run in a **separate OS process** from the client.

```python
# server.py
import asyncio
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer

class AsyncCalculator(rpyc.Service):
    async def exposed_async_add(self, a, b):
        """Async addition with simulated delay."""
        await asyncio.sleep(0.1)  # Simulate async work
        return a + b

    async def exposed_async_multiply(self, a, b):
        """Async multiplication."""
        await asyncio.sleep(0.1)
        return a * b

async def main():
    server = AsyncioServer(AsyncCalculator, hostname="localhost", port=18861)
    print("Server started on port 18861")
    await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
```

`rpyc.run_async_server(AsyncCalculator, port=18861)` is a one-line shorthand for
the `main()` above.

### Simple Async Client

```python
# client.py
import asyncio
import rpyc_async as rpyc

async def main():
    # Connect to server (non-blocking; enables asyncio serving for you)
    conn = await rpyc.async_connect("localhost", 18861)

    try:
        # Call async methods and await results
        result1 = await conn.root.async_add(5, 3)
        print(f"5 + 3 = {result1}")  # 8

        result2 = await conn.root.async_multiply(4, 7)
        print(f"4 * 7 = {result2}")  # 28
    finally:
        await conn.aclose()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## Server Setup

### Mixed Sync/Async Service

```python
import asyncio
import time
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer

class MixedService(rpyc.Service):
    """Service with both sync and async methods."""

    # Plain sync method
    def exposed_sync_hello(self, name):
        return f"Sync hello, {name}!"

    # Async method
    async def exposed_async_hello(self, name):
        await asyncio.sleep(0.1)
        return f"Async hello, {name}!"

    # CPU-bound sync method
    def exposed_compute_pi(self, digits):
        """Compute pi (CPU-bound, use sync)."""
        # ... computation ...
        return 3.14159

    # I/O-bound async method
    async def exposed_fetch_data(self, url):
        """Fetch data from URL (I/O-bound, use async)."""
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.text()

if __name__ == "__main__":
    rpyc.run_async_server(MixedService, port=18861)
```

---

## Client Usage

### Concurrent Async Calls

```python
import asyncio
import rpyc_async as rpyc

async def main():
    conn = await rpyc.async_connect("localhost", 18861)

    try:
        # Launch multiple async calls concurrently
        results = await asyncio.gather(
            conn.root.async_add(1, 2),
            conn.root.async_add(3, 4),
            conn.root.async_add(5, 6),
            conn.root.async_multiply(2, 3),
            conn.root.async_multiply(4, 5),
        )

        print(f"Results: {results}")
        # Results: [3, 7, 11, 6, 20]
    finally:
        await conn.aclose()

asyncio.run(main())
```

### Error Handling

```python
import asyncio
import rpyc_async as rpyc

async def main():
    conn = await rpyc.async_connect("localhost", 18861)

    try:
        # Call method that may raise exception
        result = await conn.root.async_divide(10, 0)
    except ZeroDivisionError as e:
        print(f"Remote raised ZeroDivisionError: {e}")
    except Exception as e:
        print(f"Remote raised exception: {e}")
    finally:
        await conn.aclose()

asyncio.run(main())
```

### Timeout Handling

```python
import asyncio
import rpyc_async as rpyc

async def main():
    conn = await rpyc.async_connect("localhost", 18861)

    try:
        # Set timeout using asyncio.wait_for
        result = await asyncio.wait_for(
            conn.root.async_slow_method(),
            timeout=5.0  # 5 second timeout
        )
        print(f"Result: {result}")
    except asyncio.TimeoutError:
        print("Request timed out!")
    finally:
        await conn.aclose()

asyncio.run(main())
```

---

## Advanced Patterns

### Async Context Manager

```python
import asyncio
import rpyc_async as rpyc

class AsyncRPyCConnection:
    """Async context manager for RPyC connection."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.conn = None

    async def __aenter__(self):
        self.conn = await rpyc.async_connect(self.host, self.port)
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            await self.conn.aclose()
        return False

# Usage
async def main():
    async with AsyncRPyCConnection("localhost", 18861) as conn:
        result = await conn.root.async_method()
        print(result)

asyncio.run(main())
```

### Connection Pool

```python
import asyncio
import rpyc_async as rpyc
from typing import List

class AsyncRPyCPool:
    """Simple connection pool for RPyC."""

    def __init__(self, host: str, port: int, pool_size: int = 5):
        self.host = host
        self.port = port
        self.pool_size = pool_size
        self.connections: List[rpyc.Connection] = []
        self.semaphore = asyncio.Semaphore(pool_size)

    async def get_connection(self):
        """Get connection from pool."""
        await self.semaphore.acquire()

        if self.connections:
            return self.connections.pop()
        else:
            return await rpyc.async_connect(self.host, self.port)

    async def release_connection(self, conn):
        """Return connection to pool."""
        self.connections.append(conn)
        self.semaphore.release()

    async def execute(self, func_name, *args, **kwargs):
        """Execute remote function using pooled connection."""
        conn = await self.get_connection()
        try:
            method = getattr(conn.root, func_name)
            result = await method(*args, **kwargs)
            return result
        finally:
            await self.release_connection(conn)

# Usage
async def main():
    pool = AsyncRPyCPool("localhost", 18861, pool_size=10)

    # Execute many concurrent calls
    results = await asyncio.gather(*[
        pool.execute("async_add", i, i)
        for i in range(100)
    ])

    print(f"Processed {len(results)} calls")

asyncio.run(main())
```

### Fire-and-Forget Calls

Sometimes you want to start a remote call and move on without awaiting its
result. Both helpers schedule the awaitable on the running loop and hand you
the `asyncio.Task`, routing the outcome to callbacks.

> Import from `rpyc.utils.helpers` — these helpers are **not** re-exported at
> the `rpyc` top level.

| | callbacks | use when |
|---|---|---|
| `fire_and_forget_async()` | `async def` | almost always — anything that logs, alerts, or makes another RPC |
| `fire_and_forget()` | plain `def` | the callback is pure CPU work and cannot `await` |

**Reach for `fire_and_forget_async()` by default.** A sync callback cannot
`await`, so it cannot do the one thing callbacks usually need to do: talk to
something. If you find yourself calling `asyncio.create_task()` from inside a
sync callback, you wanted `fire_and_forget_async()`.

#### Keeping a caller off the hot path

The motivating case: a server broadcasting to subscribers. One dead subscriber
must not freeze the others, and must not freeze the caller.

```python
import asyncio
import json
import rpyc_async as rpyc
from rpyc_async.utils.helpers import fire_and_forget_async

PUSH_TIMEOUT = 10.0

class Hub(rpyc.Service):
    def __init__(self):
        self._subscribers = []

    # `async def`, not `def`: an async client awaiting a *sync* exposed method
    # goes through sync_request and hits the "called from the asyncio loop"
    # guard. Every method an async client calls must be `async def`.
    async def exposed_subscribe(self, subscriber):
        self._subscribers.append(subscriber)

    async def exposed_broadcast(self, message):
        # Serialise ONCE, here. Handing the subscriber a netref would force
        # its loop to walk the object back over the wire, field by field.
        payload = json.dumps(message, default=str)

        for subscriber in list(self._subscribers):
            async def on_error(exc, sub=subscriber):
                # An async callback can do real work — here, drop the peer.
                await self._drop(sub, exc)

            fire_and_forget_async(
                subscriber.on_message(payload),
                timeout=PUSH_TIMEOUT,
                error_callback=on_error,
                name=f"push-{id(subscriber)}",
            )
        # Returns immediately. A hung subscriber cannot block this call.

    async def _drop(self, subscriber, exc):
        if subscriber in self._subscribers:
            self._subscribers.remove(subscriber)
```

Three things are load-bearing here:

1. **The call returns immediately.** `exposed_broadcast` does not await any
   subscriber, so one hung peer cannot stall the rest — or the caller.
   Measured with one healthy and one permanently-hung subscriber:
   `broadcast()` returns in 0.05 s, not in 3600 s.
2. **`timeout=` is not optional.** Without it a subscriber whose `on_message`
   never resolves parks its task *forever*, and every later broadcast adds
   another. With it, each task settles and releases its slot — the hung peer
   above raises `TimeoutError` into `on_error` and gets dropped.
3. **`async def on_error`.** Dropping a dead subscriber may itself need to
   `await`. `fire_and_forget()` could not express this.

Two footguns this example sidesteps:

- **Do not name an `exposed_` method after an attribute.** A `self.dropped`
  list plus an `exposed_dropped()` method means `conn.root.dropped` resolves to
  the *list* netref; calling it raises the `sync_request()` guard from async
  code, with an error that points nowhere near the real cause.
- **Serialise exactly once.** `exposed_broadcast` above already produced a
  JSON string; a subscriber that calls `json.dumps()` on it again gets
  `'"\\"msg\\""'`, not `'"msg"'`. Encode the original object on the sending
  side and treat the payload as opaque thereafter.

#### The basics

```python
import asyncio
import rpyc_async as rpyc
from rpyc_async.utils.helpers import fire_and_forget_async

async def on_success(result):
    await log_to_db(result)

async def on_error(exc):
    await alert(f"{type(exc).__name__}: {exc}")

async def main():
    conn = await rpyc.async_connect("localhost", 18861)
    try:
        # 1. Start a call and keep going.
        fire_and_forget_async(
            conn.root.slow_job(4),
            success_callback=on_success,
            error_callback=on_error,
            name="slow_job",
        )

        # 2. Give up after 0.3 s — on_error receives a TimeoutError.
        fire_and_forget_async(
            conn.root.very_slow_job(),
            timeout=0.3,
            error_callback=on_error,
        )

        # 3. The returned Task is a normal asyncio.Task: await it or cancel it.
        task = fire_and_forget_async(conn.root.slow_job(1))
        await task

        other = fire_and_forget_async(conn.root.very_slow_job())
        other.cancel()  # CancelledError is NOT routed to error_callback

        await asyncio.sleep(1)
    finally:
        await conn.aclose()

asyncio.run(main())
```

Use `fire_and_forget()` only when the callbacks are plain functions:

```python
from rpyc_async.utils.helpers import fire_and_forget

def on_success(result):
    counters["done"] += 1     # pure CPU, no await needed

fire_and_forget(conn.root.slow_job(7), success_callback=on_success)
```

**Notes:**
- Both helpers raise `RuntimeError` if no event loop is running.
- **You do not need to hold the returned task.** Both helpers keep a strong
  reference to it internally until it finishes, so a GC pass cannot silently
  kill a half-finished call. Discarding the return value is safe; keep it only
  if you intend to `await` or `cancel()` it.
- The task resolves to **`None`**, never to the call's result. Awaiting it only
  tells you the work finished; to read the value you must pass a
  `success_callback`.
- Awaiting the task does **not** re-raise a failure either. Without an
  `error_callback` the exception is logged as
  `ERROR: Unhandled exception in fire_and_forget_async: ...` and swallowed.
- `error_callback` is invoked for `Exception` subclasses only. `CancelledError`
  — and any other bare `BaseException` — bypasses it and propagates to whoever
  awaits the task, despite the `BaseException` annotation.
- An exception *inside* a callback is caught and printed to stderr; it never
  kills the task, which still reports `done=True`, `exception()=None`.
- **Do not close the connection while a call is in flight.** Its reply can no
  longer arrive, so the task never completes and *neither* callback ever runs —
  it just hangs.

Await the tasks (or cancel them) before `await conn.aclose()`:

```python
tasks = [fire_and_forget_async(conn.root.slow_job(i)) for i in range(5)]
await asyncio.gather(*tasks, return_exceptions=True)
await conn.aclose()
```

**See:** [Fire-and-Forget Guide](guide_fire_and_forget.md) for backpressure,
bidirectional callbacks and error-handling patterns.

---

### Streaming Results

```python
# Server
class StreamingService(rpyc.Service):
    async def exposed_stream_numbers(self, count):
        """Stream numbers one by one."""
        results = []
        for i in range(count):
            await asyncio.sleep(0.1)
            results.append(i)
        return results

# Client
async def main():
    conn = await rpyc.async_connect("localhost", 18861)

    try:
        # Get all results at once
        results = await conn.root.stream_numbers(10)
        for num in results:
            print(f"Received: {num}")
    finally:
        await conn.aclose()

asyncio.run(main())
```

---

## Real-World Examples

### Async Database Service

```python
# server.py
import asyncio
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer
import asyncpg  # PostgreSQL async driver

class AsyncDatabaseService(rpyc.Service):
    """Async database service using asyncpg."""

    def on_connect(self, conn):
        """Initialize database connection pool."""
        self.db_pool = None

    async def exposed_init_db(self, dsn):
        """Initialize database connection pool."""
        self.db_pool = await asyncpg.create_pool(dsn)

    async def exposed_query(self, sql, *params):
        """Execute SQL query."""
        if not self.db_pool:
            raise RuntimeError("Database not initialized")

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
            # Convert to list of dicts
            return [dict(row) for row in rows]

    async def exposed_execute(self, sql, *params):
        """Execute SQL command."""
        if not self.db_pool:
            raise RuntimeError("Database not initialized")

        async with self.db_pool.acquire() as conn:
            result = await conn.execute(sql, *params)
            return result

if __name__ == "__main__":
    rpyc.run_async_server(AsyncDatabaseService, port=18861)
```

```python
# client.py
import asyncio
import rpyc_async as rpyc

async def main():
    conn = await rpyc.async_connect("localhost", 18861)

    try:
        # Initialize database
        await conn.root.init_db("postgresql://user:pass@localhost/db")

        # Execute queries
        rows = await conn.root.query(
            "SELECT * FROM users WHERE age > $1",
            25
        )

        for row in rows:
            print(f"User: {row['name']}, Age: {row['age']}")

        # Execute command
        result = await conn.root.execute(
            "INSERT INTO logs (message) VALUES ($1)",
            "Test log entry"
        )
        print(f"Executed: {result}")
    finally:
        await conn.aclose()

asyncio.run(main())
```

### Async Web Scraper Service

```python
# server.py
import asyncio
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer
import aiohttp
from bs4 import BeautifulSoup

class AsyncScraperService(rpyc.Service):
    """Async web scraper service."""

    async def exposed_fetch_url(self, url):
        """Fetch URL content."""
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.text()

    async def exposed_fetch_multiple(self, urls):
        """Fetch multiple URLs concurrently."""
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_one(session, url) for url in urls]
            results = await asyncio.gather(*tasks)
            return results

    async def _fetch_one(self, session, url):
        """Fetch one URL."""
        try:
            async with session.get(url) as response:
                return {
                    'url': url,
                    'status': response.status,
                    'content': await response.text()
                }
        except Exception as e:
            return {
                'url': url,
                'error': str(e)
            }

    async def exposed_scrape_titles(self, urls):
        """Scrape page titles from multiple URLs."""
        results = await self.exposed_fetch_multiple(urls)

        titles = []
        for result in results:
            if 'error' in result:
                titles.append({'url': result['url'], 'error': result['error']})
            else:
                soup = BeautifulSoup(result['content'], 'html.parser')
                title = soup.find('title')
                titles.append({
                    'url': result['url'],
                    'title': title.text if title else 'No title'
                })

        return titles

if __name__ == "__main__":
    rpyc.run_async_server(AsyncScraperService, port=18861)
```

```python
# client.py
import asyncio
import rpyc_async as rpyc

async def main():
    conn = await rpyc.async_connect("localhost", 18861)

    try:
        # Scrape titles from multiple URLs
        urls = [
            "https://example.com",
            "https://python.org",
            "https://github.com",
        ]

        titles = await conn.root.scrape_titles(urls)

        for item in titles:
            if 'error' in item:
                print(f"{item['url']}: ERROR - {item['error']}")
            else:
                print(f"{item['url']}: {item['title']}")
    finally:
        await conn.aclose()

asyncio.run(main())
```

### Async Task Queue

```python
# server.py
import asyncio
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer
from typing import Dict, Any
import uuid

class AsyncTaskQueue(rpyc.Service):
    """Async task queue service."""

    def on_connect(self, conn):
        """Initialize task storage."""
        self.tasks: Dict[str, asyncio.Task] = {}
        self.results: Dict[str, Any] = {}

    async def exposed_submit_task(self, func_name, *args, **kwargs):
        """Submit async task and return task ID."""
        task_id = str(uuid.uuid4())

        # Define task
        async def run_task():
            await asyncio.sleep(1)  # Simulate work
            result = f"Processed {func_name} with {args}"
            self.results[task_id] = {'status': 'completed', 'result': result}

        # Create and store task
        task = asyncio.create_task(run_task())
        self.tasks[task_id] = task

        return task_id

    async def exposed_get_task_status(self, task_id):
        """Get task status."""
        if task_id in self.results:
            return self.results[task_id]
        elif task_id in self.tasks:
            return {'status': 'running'}
        else:
            return {'status': 'not_found'}

    async def exposed_wait_for_task(self, task_id):
        """Wait for task to complete and return result."""
        if task_id not in self.tasks:
            raise ValueError(f"Task {task_id} not found")

        await self.tasks[task_id]
        return self.results[task_id]

if __name__ == "__main__":
    rpyc.run_async_server(AsyncTaskQueue, port=18861)
```

```python
# client.py
import asyncio
import rpyc_async as rpyc

async def main():
    conn = await rpyc.async_connect("localhost", 18861)

    try:
        # Submit task
        task_id = await conn.root.submit_task("process_data", [1, 2, 3])
        print(f"Submitted task: {task_id}")

        # Poll status
        while True:
            status = await conn.root.get_task_status(task_id)
            print(f"Status: {status['status']}")

            if status['status'] == 'completed':
                print(f"Result: {status['result']}")
                break

            await asyncio.sleep(0.5)

        # Or wait directly
        result = await conn.root.wait_for_task(task_id)
        print(f"Final result: {result}")
    finally:
        await conn.aclose()

asyncio.run(main())
```

---

## See Also

- [API Reference](API_REFERENCE.md)
- [Migration Guide](MIGRATION_GUIDE.md)
