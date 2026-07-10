# rpyc-async — Known Limitations

## Overview

`rpyc-async` 1.0.0 (asyncio-native fork of upstream RPyC) provides async/await support with the following status:

> **Note:** `rpyc-async` is an independent distribution (`pip install rpyc-async`, import name `rpyc_async`; use `import rpyc_async as rpyc` to keep the shorter spelling in existing code).
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

**ThreadedServer CANNOT run `async def exposed_*` methods at all** — neither
bidirectional callbacks nor plain unidirectional `await`.

This is NOT a bug - it's an **architectural impossibility** due to how ThreadedServer works.

### Why It Fails

**ThreadedServer Architecture:**
```
Main Thread
├─ Accept connections
└─ Spawn worker threads
    └─ Each thread runs: conn.serve_all() [BLOCKING]
        └─ No event loop lives in this thread,
           and none can outlive a single request.
```

Dispatching an `async def exposed_*` needs a loop that stays alive across the
whole request — long enough to `await` I/O, and (for callbacks) long enough to
receive the peer's reply while the handler is suspended. A worker thread that
is blocked inside `serve_all()` can never provide one.

**Root Cause:** `Connection._dispatch` in `rpyc/core/protocol.py`

```python
if needs_async and self._asyncio_enabled:
    ...  # schedule on the persistent loop
elif needs_async:
    # No persistent event loop available — reject up front.
    raise RuntimeError(
        "Async method requires persistent event loop. ..."
    )
else:
    self._dispatch_request(seq, args)
```

So the failure is **immediate and explicit**, not a deadlock.

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

async def main():
    conn = await async_connect("localhost", 18861)

    # ❌ ThreadedServer raises RuntimeError and drops the connection;
    #    the client sees EOFError: stream has been closed
    result = await conn.root.process(my_callback, 5)
```

**Why it fails:**
1. Client calls `process`, passing an async callback.
2. `ThreadedServer` serves the connection from a plain worker thread. That
   thread has **no running event loop**, and none can be created that would
   outlive a single request.
3. `Connection._dispatch` sees that `exposed_process` is a coroutine function,
   finds no persistent loop, and raises immediately:

   ```text
   RuntimeError: Async method requires persistent event loop.
   ```

4. The worker thread dies, the socket is closed, and the client's pending
   `AsyncResult` fails with `EOFError: stream has been closed`.

It is a **fast, loud failure**, not a hang. Earlier revisions of this document
claimed a 30-second deadlock caused by a temporary `asyncio.run()` loop; the
implementation now rejects the call up front instead.

#### ❌ Recursive Bidirectional Calls

Same cause: the server-side `async def exposed_*` never gets to run.

#### ❌ Server-Side Concurrency

`ThreadedServer` gives you one thread per connection, and within a connection
requests are served one at a time. Since it cannot run `async def exposed_*`
at all, `asyncio.gather()` on the client buys you no server-side concurrency.

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
| **Event Loop** | None | **Persistent** |
| **I/O Mode** | Blocking (`serve_all()`) | **Non-blocking (`add_reader()`)** |
| **Concurrency** | Sequential | **Concurrent** |
| **`async def exposed_*`** | ❌ **`RuntimeError`** | ✅ **WORKS** |
| **Bidirectional Async** | ❌ **FAILS** | ✅ **WORKS** |
| **Cost per connection** | One OS thread | One coroutine |

### Working Example with AsyncioServer

```python
# Server
import asyncio
from rpyc_async.utils.async_server import AsyncioServer

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
from rpyc_async.core.async_connect import async_connect

async def my_callback(x):
    await asyncio.sleep(0.1)
    return x + 10

async def main():
    conn = await async_connect("localhost", 18861)
    try:
        # ✅ WORKS perfectly!
        result = await conn.root.process(my_callback, 5)
        print(result)  # 20
    finally:
        await conn.aclose()

asyncio.run(main())
```

> **Never call `rpyc.connect()` from async code.** It is synchronous and would
> block the running event loop during the TCP handshake, so it raises
> `RuntimeError` and points you at `async_connect()`. `async_connect()` also
> calls `conn.enable_asyncio_serving()` for you — you never do that by hand.
> Close with `await conn.aclose()`, never `conn.close()`: the latter issues a
> blocking `sync_request(HANDLE_CLOSE)` and freezes the loop until
> `sync_request_timeout` (30 s by default) expires.

**See:** [AsyncioServer Migration Guide](ASYNCIO_SERVER_MIGRATION.md)

---

## ✅ What Works with ThreadedServer

`ThreadedServer` remains the right choice for **purely synchronous services**.
It has no event loop, so it cannot run *any* `async def exposed_*` method — not
just bidirectional ones.

### 1. Synchronous services

**Status:** ✅ Fully functional

```python
import rpyc_async as rpyc
from rpyc_async.utils.server import ThreadedServer

class MyService(rpyc.Service):
    def exposed_double(self, x):
        return x * 2

ThreadedServer(MyService, port=18861).start()
```

A synchronous client talks to it the usual way:

```python
import rpyc_async as rpyc

conn = rpyc.connect("localhost", 18861)
try:
    print(conn.root.double(5))  # 10
finally:
    conn.close()  # sync context: close() is correct here
```

---

### 2. What does **not** work: any `async def exposed_*`

**Status:** ❌ Raises `RuntimeError`

```python
# Server — ThreadedServer
class MyService(rpyc.Service):
    async def exposed_async_method(self, x):   # ❌
        await asyncio.sleep(0.1)
        return x * 2
```

The very first call to `async_method` makes the server raise, and the
connection is torn down:

```text
RuntimeError: Async method requires persistent event loop. Either:
1. Use AsyncioServer for server-side: from rpyc_async.utils.async_server import AsyncioServer
2. Enable asyncio serving for client-side: conn.enable_asyncio_serving()
```

The client then sees `EOFError: stream has been closed`. This applies to
*unidirectional* calls too — there is no "async subset" of `ThreadedServer`
that works.

---

### 3. Async services: use `AsyncioServer`

`AsyncioServer` owns a persistent event loop, so every construction below works:
unidirectional `await`, recursion on the same side, `asyncio.gather()` of
concurrent calls, and mixing sync and async `exposed_*` in one service.

```python
import asyncio
from rpyc_async.utils.async_server import AsyncioServer
from rpyc_async.core.async_connect import async_connect

class MixedService(rpyc.Service):
    def exposed_sync_method(self):
        return "sync"

    async def exposed_async_method(self):
        await asyncio.sleep(0.1)
        return "async"

    async def exposed_countdown(self, n):
        if n <= 0:
            return 0
        await asyncio.sleep(0.01)
        return n + await self.exposed_countdown(n - 1)

# Server process
async def serve():
    await AsyncioServer(MixedService, port=18861).serve_forever()

# Client process
async def main():
    conn = await async_connect("localhost", 18861)
    try:
        print(await conn.root.sync_method())   # "sync"
        print(await conn.root.async_method())  # "async"
        print(await conn.root.countdown(20))   # recursion, depth 20+
        a, b = await asyncio.gather(
            conn.root.async_method(),
            conn.root.countdown(5),
        )
    finally:
        await conn.aclose()
```

> Client and `AsyncioServer` must live in **different OS processes**. Running
> both in one process is rejected by the library — see
> [DESIGN_NO_SAME_PROCESS_TESTS.md](DESIGN_NO_SAME_PROCESS_TESTS.md).

---

## ❌ What Doesn't Work

### 1. Bidirectional Async Callbacks

**Status:** ❌ FAILS with ThreadedServer

**Solution:** Use `AsyncioServer`

**See:** [Comparison Examples](../examples/bidirectional_async/)

---

### 2. Server-Side Concurrency

**Status:** ❌ Sequential with ThreadedServer

`ThreadedServer` serves one request at a time per connection, and it cannot run
`async def exposed_*` at all — so there is no way to overlap work on the server.

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

The call does not fail on the server. `exposed_async_generator` is an *async
generator function*, so calling it just builds an async-generator object, which
is handed back as a netref. Draining that netref from the client needs a
round-trip per item, and the client raises:

```text
RuntimeError: sync_request() was called from the asyncio loop that serves this
connection for a user-level RPC (handler=7). This would block the loop for the
full remote round-trip and can deadlock.
```

**Workaround:** build the list on the server and return it by value.

```python
async def exposed_get_items(self):
    items = []
    for i in range(10):
        await asyncio.sleep(0.1)
        items.append(i)
    return items  # ✅ Works — arrives as a real list, not a netref
```

**Future Work:** Could be added in a future `rpyc-async` release

---

## 📋 Server Comparison Matrix

| Feature | ThreadedServer | AsyncioServer |
|---------|----------------|---------------|
| **Sync `exposed_*`** | ✅ Works | ✅ Works |
| **Unidirectional Async** | ❌ **`RuntimeError`** | ✅ Works |
| **Bidirectional Async** | ❌ **`RuntimeError`** | ✅ **WORKS** |
| **Server Concurrency** | ❌ Sequential | ✅ Concurrent |
| **Event Loop** | None | **Persistent** |
| **I/O Mode** | Blocking | **Non-blocking** |
| **Client entry point** | `rpyc.connect()` | `await async_connect()` |
| **Close** | `conn.close()` | `await conn.aclose()` |
| **Deployment** | Simple | Requires asyncio |
| **Sync Workloads** | ✅ Good | ⚠️ OK |

Memory-per-connection and throughput figures depend entirely on your workload;
measure them yourself rather than trusting a table.

---

## 🔧 There Are No ThreadedServer Workarounds

There is no way to keep `ThreadedServer` and get async. The moment a service
declares a single `async def exposed_*`, the first call to it raises
`RuntimeError` and drops the connection — see
[§2 above](#2-what-does-not-work-any-async-def-exposed_).

The only two supported shapes are:

| You need | Server | Client |
|---|---|---|
| Sync service, sync clients | `ThreadedServer` | `rpyc.connect()` + `conn.close()` |
| Any `async def exposed_*` | `AsyncioServer` | `await async_connect()` + `await conn.aclose()` |

If you cannot migrate the server yet, keep every `exposed_*` method
**synchronous** and do the async work on the client side instead.

> **Do not poll.** An earlier revision of this document suggested a
> "start a task, sleep, then ask for the result" pattern. That is forbidden
> in this project: replies are delivered through `loop.add_reader()`, and the
> ban is enforced by `tests/test_no_polling_policy.py`. If you want a call
> whose result you never collect, use
> [`fire_and_forget`](guide_fire_and_forget.md) instead.

---

## 💡 Best Practices

### DO:

✅ **Use AsyncioServer** for bidirectional async
✅ Use async for I/O-bound operations (network, database, files)
✅ Use `asyncio.gather()` for concurrent operations
✅ Set timeouts with `asyncio.wait_for()`
✅ Reuse connections

### DON'T:

❌ **Don't use ThreadedServer** for *any* `async def exposed_*` method
❌ Don't call `rpyc.connect()` from async code — use `await async_connect()`
❌ Don't call `conn.close()` from async code — use `await conn.aclose()`
❌ Don't call `conn.enable_asyncio_serving()` by hand — `async_connect()` does it
❌ Don't use blocking calls in async methods (`time.sleep()`)
❌ Don't create new connection per request
❌ Don't poll for results — replies arrive via `loop.add_reader()`

---

## 🚀 Migration to AsyncioServer

**If you need any async `exposed_*` method, you MUST migrate to AsyncioServer.**

**See:** [AsyncioServer Migration Guide](ASYNCIO_SERVER_MIGRATION.md)

**Key Changes:**
1. Server: `from rpyc_async.utils.async_server import AsyncioServer`
2. Wrap in `async def main()`: `asyncio.run(main())`
3. Use `await server.serve_forever()`
4. Client: `from rpyc_async.core.async_connect import async_connect`, then
   `conn = await async_connect(...)` and `await conn.aclose()`
5. Run the client in a **different OS process** from the server

**Benefit:** All limitations removed!

---

## 📞 Getting Help

1. **`RuntimeError: Async method requires persistent event loop`?** → Your server is a `ThreadedServer`; use `AsyncioServer`
2. **`RuntimeError: rpyc.connect() is synchronous ...`?** → Use `await async_connect()`
3. **Bidirectional async not working?** → Use AsyncioServer
4. **Need concurrent request processing?** → Use AsyncioServer

**Documentation:**
- [AsyncioServer Migration Guide](ASYNCIO_SERVER_MIGRATION.md)
- [Bidirectional Examples](../examples/bidirectional_async/)
- [API Reference](API_REFERENCE.md)

---

## Summary

### ThreadedServer

**✅ Good for:**
- Fully synchronous services (every `exposed_*` is a plain `def`)
- Simple deployments with no asyncio anywhere

**❌ Cannot support:**
- Any `async def exposed_*` — raises `RuntimeError` on the first call
- Bidirectional async (architectural limitation)
- Server-side concurrency within a connection
- Persistent event loops

### AsyncioServer

**✅ Required for:**
- Any `async def exposed_*`, unidirectional or bidirectional
- Persistent event loops
- Non-blocking I/O (`loop.add_reader()`, no polling)
- Server-side concurrency

**✅ Already implemented:** `rpyc.utils.async_server.AsyncioServer`

---

**Bottom Line:** the moment a service has one `async def exposed_*`, **use
AsyncioServer**. `ThreadedServer` cannot and will not support it due to
architectural limitations.
