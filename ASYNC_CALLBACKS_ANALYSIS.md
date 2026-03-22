# Analysis: Async Callback Support in RPyC

## ✅ What Is Already Covered in the Proposal

### 1. Boxing Async Functions

`ASYNC_SUPPORT_PROPOSAL.md` contains:

```python
def _box(self, obj):
    # ...

    # ✅ NEW: Check for an async function
    if inspect.iscoroutinefunction(obj):
        # This is an async function - pack it as REMOTE_REF
        # but mark with metadata that it is async
        id_pack = get_id_pack(obj)
        self._local_objects.add(id_pack, obj)
        # Add an "is_async" flag to id_pack
        return consts.LABEL_REMOTE_REF, (*id_pack, {'async': True})
```

**Problem #1:** How do we pass the metadata `{'async': True}` through Brine?

**Solution:** Brine does not support dict. We need to use a tuple of flags:

```python
# Instead of:
return consts.LABEL_REMOTE_REF, (*id_pack, {'async': True})

# Use:
return consts.LABEL_REMOTE_REF, (*id_pack, True)  # 4th element = is_async flag
```

### 2. Unboxing Async Functions

```python
def _unbox(self, package):
    label, value = package

    if label == consts.LABEL_REMOTE_REF:
        # Check metadata: async function?
        if isinstance(value, tuple) and len(value) == 4:
            id_pack, metadata = value[:3], value[3]
            if metadata.get('async'):
                # This is an async function - create an async proxy
                return self._create_async_proxy(id_pack)
```

**Problem #2:** What is `_create_async_proxy()`? Not implemented!

### 3. Calling an Async Callback on the Server

The proposal has an example:

```python
class MyService(rpyc.Service):
    async def exposed_process_with_callback(self, callback):
        """Accepts an async callback."""
        # Call the callback
        result = await callback(42)  # ← await works!
        return f"Callback returned: {result}"
```

**Problem #3:** How does the server know that `callback` is an async function?

- If `callback` is a netref (proxy) to a client-side function
- We need to call `HANDLE_ASYNC_CALL` instead of `HANDLE_CALL`
- But how do we determine this?

---

## ❌ What Is Missing: The Detailed Mechanism

### Scenario: The Client Passes an Async Callback

```python
# Client
async def my_callback(value):
    await asyncio.sleep(0.1)
    return value * 2

# Pass it as an argument
result = await conn.root.process_with_callback(my_callback)
```

**What happens step by step:**

#### Step 1: Boxing on the Client

```python
# In client._box(my_callback)
def _box(self, obj):
    if inspect.iscoroutinefunction(obj):
        # obj = my_callback (async function)
        id_pack = get_id_pack(obj)
        self._local_objects.add(id_pack, obj)

        # ⚠️ PROBLEM: How do we pass the is_async flag through Brine?
        # Brine supports only: int, bool, str, float, bytes, tuple, frozenset

        # SOLUTION 1: Extend id_pack
        return consts.LABEL_REMOTE_REF, (id_pack[0], id_pack[1], id_pack[2], True)
        #                                 ↑ class   ↑ id      ↑ version  ↑ is_async
```

**id_pack format:**
- Current: `(class_name, obj_id, class_version)`  # 3 elements
- New: `(class_name, obj_id, class_version, is_async)`  # 4 elements

#### Step 2: Unboxing on the Server

```python
# In server._unbox(package)
def _unbox(self, package):
    label, value = package

    if label == consts.LABEL_REMOTE_REF:
        # value = (class_name, obj_id, class_version, is_async)

        if len(value) == 4:
            # New format with is_async
            id_pack = (value[0], value[1], value[2])
            is_async = value[3]

            proxy = self._netref_factory(id_pack)

            # ✅ NEW: Mark the proxy as async
            if is_async:
                proxy.____is_async__ = True

            return proxy
        elif len(value) == 3:
            # Old format (backward compatibility)
            id_pack = value
            proxy = self._netref_factory(id_pack)
            proxy.____is_async__ = False
            return proxy
```

#### Step 3: Calling the Callback on the Server

```python
# Server
async def exposed_process_with_callback(self, callback):
    # callback = netref proxy to the client-side async function
    # callback.____is_async__ = True

    # How do we call it?
    result = await callback(42)  # ← Should work!
```

**What should happen:**

```python
# In netref.BaseNetref.__call__()
def __call__(self, *args, **kwargs):
    if hasattr(self, '____is_async__') and self.____is_async__:
        # This is an async function - use the async handler
        async_result = self.____conn__.async_request(
            consts.HANDLE_ASYNC_CALL,
            self,
            args,
            kwargs
        )
        # Return an AsyncResult with __await__()
        return async_result
    else:
        # Sync function - as before
        return self.____conn__.sync_request(
            consts.HANDLE_CALL,
            self,
            args,
            kwargs
        )
```

**Problem #4:** `await callback(42)` will invoke `BaseNetref.__call__()`, which returns an `AsyncResult`.

But `AsyncResult.__await__()` requires an event loop and `enable_asyncio_serving()`.

#### Step 4: Handling on the Client

```python
# The client receives a HANDLE_ASYNC_CALL request
def _handle_async_call(self, obj, args, kwargs):
    # obj = my_callback (async function from _local_objects)

    if inspect.iscoroutinefunction(obj):
        # Call the async function
        coro = obj(*args, **dict(kwargs))

        # ⚠️ PROBLEM: What to do with the coroutine?
        # Option 1: Return the coroutine (who will await it?)
        # Option 2: Await immediately (but in what context?)

        # SOLUTION: Schedule it on the client's event loop
        loop = asyncio.get_running_loop()
        future = asyncio.ensure_future(coro)

        # Wait for completion (BUT HOW? We are in a sync handler!)
        # This is exactly the main problem!
```

---

## 🔍 The Main Problem: Async Handler Execution

### The Problem

When the server calls `await callback(42)`, the following happens:

1. The server sends `MSG_REQUEST` with `HANDLE_ASYNC_CALL` to the client
2. The client receives the request in `_dispatch_request()`
3. The client calls `_handle_async_call(obj, args, kwargs)`
4. **But `_handle_async_call()` is a SYNC function!**

```python
def _dispatch_request(self, seq, raw_args):
    try:
        handler, args = raw_args
        args = self._unbox(args)
        res = self._HANDLERS[handler](self, *args)  # ← SYNC call!
    except:
        self._send(consts.MSG_EXCEPTION, seq, ...)
    else:
        self._send(consts.MSG_REPLY, seq, self._box(res))
```

**How do we execute `await obj(*args)` in a sync context?**

### Solution 1: Asyncio.run() in the Handler (Bad!)

```python
def _handle_async_call(self, obj, args, kwargs):
    if inspect.iscoroutinefunction(obj):
        coro = obj(*args, **dict(kwargs))

        # ❌ DOES NOT WORK if already inside an event loop!
        return asyncio.run(coro)  # RuntimeError!
```

### Solution 2: Schedule and Wait (Complicated!)

```python
def _handle_async_call(self, obj, args, kwargs):
    if inspect.iscoroutinefunction(obj):
        coro = obj(*args, **dict(kwargs))

        # Get the current event loop
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No event loop - create a temporary one
            return asyncio.run(coro)

        # There is an event loop - schedule the task
        future = asyncio.ensure_future(coro)

        # ⚠️ PROBLEM: How do we wait for the future in a sync function?
        # WE CAN'T: await future (sync function)
        # WE CAN'T: loop.run_until_complete(future) (loop is already running)

        # SOLUTION: Block and wait via threading.Event
        import threading
        event = threading.Event()
        result_container = {}

        def on_done(f):
            try:
                result_container['result'] = f.result()
            except Exception as e:
                result_container['exception'] = e
            finally:
                event.set()

        future.add_done_callback(on_done)

        # Block the current thread until it completes
        event.wait()

        if 'exception' in result_container:
            raise result_container['exception']
        return result_container['result']
```

**Problem:** We block the thread where `_dispatch_request()` runs. If it is the same thread as the event loop - **DEADLOCK!**

### Solution 3: Async Dispatch (Correct!)

**Key idea:** `_dispatch_request()` should be async when the handler is async!

```python
async def _dispatch_request_async(self, seq, raw_args):
    """Async version of _dispatch_request for async handlers."""
    try:
        handler, args = raw_args
        args = self._unbox(args)

        # Check: is the handler async?
        handler_func = self._HANDLERS[handler]

        if inspect.iscoroutinefunction(handler_func):
            # Async handler - await
            res = await handler_func(self, *args)
        else:
            # Sync handler - a regular call
            res = handler_func(self, *args)
    except:
        t, v, tb = sys.exc_info()
        self._last_traceback = tb
        # ... error handling
        self._send(consts.MSG_EXCEPTION, seq, self._box_exc(t, v, tb))
    else:
        self._send(consts.MSG_REPLY, seq, self._box(res))
```

**But how do we call `_dispatch_request_async()` from the sync `_dispatch()`?**

```python
def _dispatch(self, data):
    msg, = brine.I1.unpack(data[:1])

    if msg == consts.MSG_REQUEST:
        # ... release locks ...
        seq, args = brine.load(data[1:])

        # ✅ SOLUTION: Schedule the async dispatch on the event loop
        if self._asyncio_loop:
            # Asyncio serving is enabled - schedule it
            asyncio.run_coroutine_threadsafe(
                self._dispatch_request_async(seq, args),
                self._asyncio_loop
            )
        else:
            # Sync serving - regular sync dispatch
            self._dispatch_request(seq, args)
```

**Problem:** `run_coroutine_threadsafe()` returns a Future, but we do not wait for its completion!

---

## ✅ The Correct Solution: A Fully Async Pipeline

### Architecture

When `enable_asyncio_serving()` is enabled:

1. **Server with an async callback:**
   ```python
   async def exposed_method(self, callback):
       result = await callback(42)  # ← Calling an async callback
   ```

2. **The client handles the request:**
   - `_dispatch()` sees `MSG_REQUEST` with `HANDLE_ASYNC_CALL`
   - Schedule `_dispatch_request_async()` on the event loop
   - The handler executes `await obj(*args)`
   - The result is sent back via `MSG_ASYNC_REPLY`

3. **The server receives the result:**
   - `_dispatch()` receives `MSG_ASYNC_REPLY`
   - `AsyncResult` gets the result
   - `future.set_result()` unblocks `await callback(42)`

### Detailed Implementation

```python
class Connection:
    def _dispatch(self, data):
        msg, = brine.I1.unpack(data[:1])

        if msg == consts.MSG_REQUEST or msg == consts.MSG_ASYNC_REQUEST:
            seq, args = brine.load(data[1:])

            # Determine: is async dispatch needed?
            handler, _ = args
            handler_func = self._HANDLERS.get(handler)

            needs_async = (
                inspect.iscoroutinefunction(handler_func) or
                msg == consts.MSG_ASYNC_REQUEST
            )

            if needs_async and self._asyncio_loop:
                # ✅ Async dispatch via the event loop
                task = asyncio.run_coroutine_threadsafe(
                    self._dispatch_request_async(seq, args),
                    self._asyncio_loop
                )
                # We do NOT wait for task.result() - this is a non-blocking call
            else:
                # Sync dispatch
                self._dispatch_request(seq, args)

        # ... handling REPLY/EXCEPTION
```

### Async Handler

```python
async def _handle_async_call(self, obj, args, kwargs=()):
    """Async handler - runs in the event loop!"""

    if inspect.iscoroutine(obj):
        # Already a coroutine
        result = await obj
    elif inspect.iscoroutinefunction(obj):
        # Async function - call it and await
        result = await obj(*args, **dict(kwargs))
    else:
        # Fallback to sync
        result = obj(*args, **dict(kwargs))

    return result
```

**Key point:** The handler itself is `async def`, so it can use `await`!

---

## 📝 Additions to the Proposal

### 1. Extending id_pack for the is_async Flag

**Current format:**
```python
id_pack = (class_name, obj_id, class_version)  # 3 elements
```

**New format (optional):**
```python
id_pack = (class_name, obj_id, class_version, is_async)  # 4 elements
```

**Backward compatibility:**
- If `len(id_pack) == 3` → old format, `is_async = False`
- If `len(id_pack) == 4` → new format, `is_async = id_pack[3]`

### 2. Async Dispatch Pipeline

**Add to `protocol.py`:**

```python
async def _dispatch_request_async(self, seq, raw_args):
    """Async version of _dispatch_request for async handlers."""
    try:
        handler, args = raw_args
        args = self._unbox(args)

        handler_func = self._HANDLERS[handler]

        if inspect.iscoroutinefunction(handler_func):
            res = await handler_func(self, *args)
        else:
            res = handler_func(self, *args)
    except:
        t, v, tb = sys.exc_info()
        self._send(consts.MSG_EXCEPTION, seq, self._box_exc(t, v, tb))
    else:
        self._send(consts.MSG_REPLY, seq, self._box(res))
```

### 3. Netref with Async Support

**Add to `netref.py`:**

```python
class BaseNetref:
    def __call__(self, *args, **kwargs):
        """Call the proxy object."""

        # Check: is this an async function?
        is_async = getattr(self, '____is_async__', False)

        if is_async:
            # Async call - return an awaitable AsyncResult
            return self.____conn__.async_request(
                consts.HANDLE_ASYNC_CALL,
                self,
                args,
                kwargs
            )
        else:
            # Sync call
            return self.____conn__.sync_request(
                consts.HANDLE_CALL,
                self,
                args,
                kwargs
            )
```

### 4. Schedule Async Dispatch

**Modify `_dispatch()`:**

```python
def _dispatch(self, data):
    msg, = brine.I1.unpack(data[:1])

    if msg == consts.MSG_REQUEST:
        seq, args = brine.load(data[1:])

        # Determine whether async dispatch is needed
        handler, _ = args
        needs_async = handler in [
            consts.HANDLE_ASYNC_CALL,
            consts.HANDLE_ASYNC_CALLATTR
        ]

        if needs_async and self._asyncio_loop:
            # Schedule the async dispatch
            asyncio.run_coroutine_threadsafe(
                self._dispatch_request_async(seq, args),
                self._asyncio_loop
            )
        else:
            # Sync dispatch
            self._dispatch_request(seq, args)
```

---

## 🎯 Final Diagram: Async Callback Flow

```
CLIENT                                    SERVER
  |                                         |
  | 1. Pass an async callback               |
  |    conn.root.method(async_callback)     |
  |---------------------------------------->|
  |    _box(async_callback)                 |
  |    → LABEL_REMOTE_REF                   |
  |      (class, id, ver, is_async=True)    |
  |                                         |
  |                                         | 2. Receive the callback proxy
  |                                         |    _unbox() → netref
  |                                         |    netref.____is_async__ = True
  |                                         |
  |                                         | 3. Call the callback
  |                                         |    await callback(42)
  |                                         |    netref.__call__()
  |                                         |    → async_request(HANDLE_ASYNC_CALL)
  |                                         |
  | 4. Receive HANDLE_ASYNC_CALL            |
  |<----------------------------------------|
  |    _dispatch()                          |
  |    → run_coroutine_threadsafe(          |
  |         _dispatch_request_async()       |
  |      )                                  |
  |                                         |
  | 5. Execute the async handler            |
  |    _handle_async_call()                 |
  |    → await async_callback(42)           |
  |    ↓                                    |
  |    result = 84                          |
  |                                         |
  | 6. Send the result                      |
  |    MSG_ASYNC_REPLY                      |
  |---------------------------------------->|
  |                                         |
  |                                         | 7. Receive the result
  |                                         |    AsyncResult.set_result(84)
  |                                         |    future.set_result(84)
  |                                         |    await is unblocked
  |                                         |    result = 84
  |                                         |
```

---

## ✅ Conclusion

### What is already in the Proposal:
1. ✅ Boxing async functions with the is_async flag
2. ✅ Unboxing async functions as netrefs
3. ✅ Examples of using async callbacks

### What needs to be added/clarified:
1. ⚠️ **Extending the id_pack** format: `(class, id, ver, is_async)`
2. ⚠️ **Async dispatch pipeline**: `_dispatch_request_async()`
3. ⚠️ **Schedule mechanism**: `run_coroutine_threadsafe()` for async handlers
4. ⚠️ **Netref async detection**: `____is_async__` attribute
5. ⚠️ **Handler async support**: all handlers must support async/await

### The main difficulty:
**Async handlers must run in the event loop**, whereas `_dispatch()` is called from `serve()`, which may run in any thread.

**Solution:**
- If `enable_asyncio_serving()` is enabled → handlers run via `run_coroutine_threadsafe()`
- If disabled → async handlers use `asyncio.run()` (as currently done in tests)

---

## 📋 Recommendations

1. **Add details to the Proposal** about the async dispatch pipeline
2. **Define a strategy** for handlers: always async or detect?
3. **Test** nested callbacks (A→B→A→B...)
4. **Document** the limitations (requires `enable_asyncio_serving()` for nested async)
