# What Is the Async Dispatch Pipeline and Why Is It Needed?

## 🎯 Simple Explanation

**Async Dispatch Pipeline** is a mechanism for handling incoming RPC requests that **can execute async handlers** without blocking the event loop.

### Analogy

Imagine a restaurant:

**Regular Sync Dispatch (current RPyC):**
```
Customer ordered → Waiter wrote it down → Went to the kitchen → Waits until it's cooked → Came back
                                                      ↑
                                         Blocks serving
                                         other tables!
```

**Async Dispatch Pipeline:**
```
Customer ordered → Waiter wrote it down → Handed off to the kitchen → Serves others
                                                      ↓
                                    The kitchen cooks asynchronously
                                                      ↓
                                    Ready → Bell → Waiter picked it up
```

---

## 🔍 The Technical Problem

### Current RPyC (Sync Dispatch)

```python
# protocol.py
def _dispatch_request(self, seq, raw_args):
    """Handle an incoming request - a SYNC function!"""
    try:
        handler, args = raw_args
        args = self._unbox(args)

        # Call the handler
        res = self._HANDLERS[handler](self, *args)  # ← BLOCKS here!
    except:
        self._send(consts.MSG_EXCEPTION, seq, ...)
    else:
        self._send(consts.MSG_REPLY, seq, self._box(res))
```

**Problem:** If `handler` is an async function, we get a **coroutine** (not awaited):

```python
# Server
class MyService(rpyc.Service):
    async def exposed_fetch_data(self, url):  # ← async def!
        await asyncio.sleep(1)  # Simulating I/O
        return "data"

# When the client calls:
# conn.root.fetch_data("http://...")

# On the server:
handler = self._handle_call
res = handler(self, exposed_fetch_data, args)
# res = <coroutine object exposed_fetch_data>  ← NOT awaited!

# We send the coroutine to the client (pointless!)
self._send(consts.MSG_REPLY, seq, self._box(res))
```

**Result:**
- ❌ The async function did not execute
- ❌ The client received a coroutine object instead of a result
- ❌ Warning: "coroutine was never awaited"

### Why Can't We Just `await`?

```python
def _dispatch_request(self, seq, raw_args):
    handler, args = raw_args
    args = self._unbox(args)

    res = self._HANDLERS[handler](self, *args)

    # Maybe it's a coroutine?
    if inspect.iscoroutine(res):
        res = await res  # ❌ SyntaxError: 'await' outside async function
```

**We need to make `_dispatch_request` async:**

```python
async def _dispatch_request(self, seq, raw_args):  # ← async def
    handler, args = raw_args
    args = self._unbox(args)

    res = self._HANDLERS[handler](self, *args)

    if inspect.iscoroutine(res):
        res = await res  # ✅ Works!

    self._send(consts.MSG_REPLY, seq, self._box(res))
```

**But a new problem:** Who will call `_dispatch_request()`? It is now a coroutine!

---

## 🔄 Async Dispatch Pipeline

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ 1. A message arrived (socket readable)                      │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. _dispatch(data) - SYNC function                          │
│    • Unpack the msg type                                    │
│    • Determine: sync or async handler?                      │
└───────────────────────┬─────────────────────────────────────┘
                        │
            ┌───────────┴──────────┐
            │                      │
            ▼                      ▼
┌─────────────────────┐  ┌──────────────────────────────────┐
│ SYNC Handler        │  │ ASYNC Handler                    │
│                     │  │                                  │
│ _dispatch_request() │  │ _dispatch_request_async()        │
│ • Call directly     │  │ • Schedule on the event loop     │
│ • Blocks            │  │ • Does NOT block                 │
└─────────────────────┘  └──────────────────────────────────┘
```

### Detailed Code

```python
class Connection:
    def _dispatch(self, data):
        """Main dispatcher - a SYNC function."""
        msg, = brine.I1.unpack(data[:1])

        if msg == consts.MSG_REQUEST:
            seq, args = brine.load(data[1:])
            handler, _ = args

            # ✅ KEY POINT: Determine whether async dispatch is needed
            needs_async = self._is_async_handler(handler)

            if needs_async and self._asyncio_loop:
                # ═══════════════════════════════════════════
                # ASYNC DISPATCH PIPELINE
                # ═══════════════════════════════════════════
                # Schedule async handling on the event loop
                asyncio.run_coroutine_threadsafe(
                    self._dispatch_request_async(seq, args),
                    self._asyncio_loop
                )
                # The function returns IMMEDIATELY (does not block!)
            else:
                # Sync dispatch as before
                self._dispatch_request(seq, args)

    async def _dispatch_request_async(self, seq, raw_args):
        """Async version of dispatch - can await handlers!"""
        try:
            handler, args = raw_args
            args = self._unbox(args)

            handler_func = self._HANDLERS[handler]

            # Call the handler
            if inspect.iscoroutinefunction(handler_func):
                # Async handler - await!
                res = await handler_func(self, *args)  # ✅ Works!
            else:
                # Sync handler - a regular call
                res = handler_func(self, *args)
        except:
            t, v, tb = sys.exc_info()
            self._send(consts.MSG_EXCEPTION, seq, self._box_exc(t, v, tb))
        else:
            self._send(consts.MSG_REPLY, seq, self._box(res))
```

---

## 📊 Comparison: Before vs After

### Before (Sync Dispatch Only)

```python
# Call
_dispatch(data)
   ↓
_dispatch_request(seq, args)  # SYNC
   ↓
handler = _handle_call
   ↓
res = obj(*args)  # BLOCKS if long-running!
   ↓
_send(MSG_REPLY, res)
```

**Execution time:** If `obj(*args)` takes 5 seconds → the whole dispatch is blocked for 5 seconds.

### After (Async Dispatch Pipeline)

```python
# Call
_dispatch(data)
   ↓
Determine: async handler?
   ↓ YES
run_coroutine_threadsafe(
    _dispatch_request_async(seq, args)
)  # ← Returns IMMEDIATELY!
   ↓
We keep handling other requests!

# In parallel on the event loop:
_dispatch_request_async():
   await handler()  # Does NOT block the event loop
   _send(MSG_REPLY, res)
```

**Execution time:** Dispatch returns immediately. The handler runs asynchronously. We can handle **thousands** of concurrent requests!

---

## 🎯 Why It Is Needed: Practical Scenarios

### Scenario 1: Async Callbacks

**Without the Async Dispatch Pipeline:**

```python
# Client
async def my_callback(value):
    await asyncio.sleep(1)  # Async operation
    return value * 2

conn.root.process(my_callback)  # Pass the callback

# Server
class MyService(rpyc.Service):
    def exposed_process(self, callback):
        # Call the callback
        result = callback(42)  # ← What comes back?
        # result = <coroutine> ← NOT awaited!
```

**Problem:**
- The server calls `callback(42)` → the client receives a request
- Client: `_dispatch_request()` → `_handle_call(my_callback, (42,))`
- `_handle_call` calls `my_callback(42)` → returns a coroutine
- The coroutine is NOT awaited → the result is not obtained
- The client sends the coroutine back to the server (pointless!)

**With the Async Dispatch Pipeline:**

```python
# The client receives HANDLE_ASYNC_CALL
_dispatch(data):
    needs_async = True  # HANDLE_ASYNC_CALL
    run_coroutine_threadsafe(
        _dispatch_request_async(seq, args)
    )

# On the event loop:
_dispatch_request_async():
    res = await _handle_async_call(my_callback, (42,))
    # _handle_async_call:
    #   coro = my_callback(42)
    #   return await coro  # ✅ Properly awaited!
    # res = 84
    _send(MSG_REPLY, 84)
```

**Result:**
- ✅ The callback is properly awaited
- ✅ The server receives the result 84
- ✅ Does NOT block the client's event loop

### Scenario 2: Multiple Concurrent Requests

**Without Async Dispatch:**

```python
# The client makes 100 concurrent requests
for i in range(100):
    rpyc.async_(conn.root.slow_method)(i)

# Server
def exposed_slow_method(self, i):
    time.sleep(5)  # Blocks for 5 seconds!
    return i * 2

# Result: Handled ONE AT A TIME (5 sec × 100 = 500 seconds!)
```

**With Async Dispatch + Async Methods:**

```python
# Client
for i in range(100):
    asyncio.create_task(
        conn.root.slow_method(i)
    )

# Server
async def exposed_slow_method(self, i):
    await asyncio.sleep(5)  # Does NOT block!
    return i * 2

# Result: All 100 requests are handled CONCURRENTLY (5 seconds total!)
```

---

## 🔧 Components of the Async Dispatch Pipeline

### 1. Detection: Identifying Async Handlers

```python
def _is_async_handler(self, handler_id):
    """Checks whether a handler requires async execution."""
    if handler_id in [
        consts.HANDLE_ASYNC_CALL,
        consts.HANDLE_ASYNC_CALLATTR
    ]:
        return True

    # Check the function
    handler_func = self._HANDLERS.get(handler_id)
    return inspect.iscoroutinefunction(handler_func)
```

### 2. Scheduling: Scheduling on the Event Loop

```python
# In _dispatch()
if needs_async and self._asyncio_loop:
    # Schedule on the event loop (a different thread is OK!)
    future = asyncio.run_coroutine_threadsafe(
        self._dispatch_request_async(seq, args),
        self._asyncio_loop
    )
    # We do NOT wait for future.result() - we return immediately
```

**Key point:** `run_coroutine_threadsafe()` can be called from **any thread**, even if the event loop is in another one!

### 3. Async Execution: Running with Await

```python
async def _dispatch_request_async(self, seq, raw_args):
    handler_func = self._HANDLERS[handler]

    if inspect.iscoroutinefunction(handler_func):
        res = await handler_func(self, *args)  # ✅ Await works!
    else:
        res = handler_func(self, *args)  # Sync fallback

    self._send(MSG_REPLY, seq, self._box(res))
```

### 4. Async Handlers

```python
# New handler for async functions
async def _handle_async_call(self, obj, args, kwargs=()):
    """Async handler - can await!"""
    if inspect.iscoroutinefunction(obj):
        result = await obj(*args, **dict(kwargs))  # ✅ Await!
    else:
        result = obj(*args, **dict(kwargs))
    return result
```

---

## 🎨 Visualization: Message Flow

### Sync Dispatch (Current)

```
Time →

Thread 1 (serving):
[Receive] → [Dispatch] → [Handler (blocking 5s)] → [Send Reply]
                              ↑
                         Blocks here!
                         Cannot handle
                         other requests!

Total: 5 seconds of blocking
```

### Async Dispatch Pipeline

```
Time →

Thread 1 (serving):
[Receive] → [Dispatch] → [Schedule] → [Receive next] → [Dispatch] → ...
                              ↓                              ↓
                         Does not block!             Does not block!

Event Loop (may be the same thread):
              [Handler (await 5s)] → [Send Reply]
                    ↓
              Does not block other tasks!
              Can run 1000+ concurrently!

Total: 5 seconds for a single request, but THOUSANDS in parallel!
```

---

## ✅ Overall Advantages

### 1. Does Not Block the Event Loop
```python
# Before:
result = conn.root.slow_method()  # Blocks for 5 seconds

# After:
result = await conn.root.slow_method()  # Does NOT block!
```

### 2. Support for Async Callbacks
```python
# Before: does NOT work
async def callback(x):
    await asyncio.sleep(1)
    return x * 2

# After: WORKS!
result = await conn.root.process(callback)
```

### 3. Scalability
```python
# Before: 1000 requests = 1000 threads (a disaster!)

# After: 1000 requests = 1 event loop (efficient!)
tasks = [conn.root.method(i) for i in range(1000)]
results = await asyncio.gather(*tasks)
```

### 4. Recursion Without Deadlocks
```python
# Before: recursion with async callbacks → deadlock

# After: works naturally!
async def callback(depth, server_func):
    if depth > 0:
        return await server_func(depth-1, callback)
    return "done"

result = await conn.root.recursive(5, callback)
```

---

## 🔒 Backward Compatibility

### Sync code keeps working:

```python
# Old sync code - WITHOUT changes!
conn = rpyc.connect("localhost", 18861)
result = conn.root.add(3, 4)  # Works as before

# Sync dispatch is used automatically
```

### Async requires explicit activation:

```python
# New async code - requires enable_asyncio_serving()
conn = rpyc.connect("localhost", 18861)
conn.enable_asyncio_serving()  # ← Activate the async pipeline

result = await conn.root.async_add(3, 4)  # Now it works
```

---

## 📋 Summary

**Async Dispatch Pipeline** is:

1. **Detection** - determine async vs sync handler
2. **Routing** - sync → regular dispatch, async → async pipeline
3. **Scheduling** - `run_coroutine_threadsafe()` for async handlers
4. **Execution** - `await` in `_dispatch_request_async()`

**Why it is needed:**
- ✅ Execute async handlers without blocking
- ✅ Support async callbacks
- ✅ Scale to thousands of concurrent requests
- ✅ Integrate with asyncio applications
- ✅ Preserve backward compatibility

**Without it:**
- ❌ Async exposed methods do not work
- ❌ Async callbacks return coroutine objects
- ❌ Blocking of the event loop
- ❌ Poor scalability
