# RPyC Async/Await Support - Technical Proposal

## 📋 Goal

Add support for asyncio-compatible `async def exposed_*` methods and async callbacks **without breaking changes** to the existing RPyC protocol.

**Key requirements:**
1. ✅ Backward compatibility - all existing code works without changes
2. ✅ Do not block the event loop on either the client or the server
3. ✅ Do not require mandatory additional threads or nest_asyncio
4. ✅ Support for async exposed methods
5. ✅ Support for async callbacks
6. ✅ Cross-process recursion works

---

## 🔍 Analysis of the Current Protocol

### Current Architecture

#### Message Types (`consts.py`)
```python
MSG_REQUEST = 1      # Request from client to server
MSG_REPLY = 2        # Server reply to client
MSG_EXCEPTION = 3    # Exception from server to client
```

#### Packet Format (in `protocol.py:_send()`)
```python
# Without bind_threads:
data = brine.I1.pack(msg) + brine.dump((seq, args))

# With bind_threads:
data = brine.I8I8.pack(local_thread_id, remote_thread_id) + \
       brine.I1.pack(msg) + \
       brine.dump((seq, args))
```

**Structure:**
```
[thread_ids (optional 16 bytes)] + [msg_type (1 byte)] + [seq + args (brine)]
```

#### Request Handling (`protocol.py:_dispatch_request()`)
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

**Problem:** `_HANDLERS[handler]()` is called synchronously and waits for completion.

#### Handlers (`protocol.py:_request_handlers()`)
```python
HANDLE_PING = 1
HANDLE_CLOSE = 2
HANDLE_GETROOT = 3
HANDLE_CALL = 7        # ← Function call
HANDLE_CALLATTR = 8    # ← Method call
# ... and others
```

#### Calling an Exposed Method (`protocol.py:_handle_call()`)
```python
def _handle_call(self, obj, args, kwargs=()):
    return obj(*args, **dict(kwargs))  # ← Direct SYNC call!
```

**Problem:** If `obj` is an `async def` function, a coroutine is returned (not awaited).

#### AsyncResult (`async_.py`)
```python
class AsyncResult:
    def wait(self):
        while self._waiting():
            self._conn.serve(self._ttl, waiting=self._waiting)  # ← Blocks!

    @property
    def value(self):
        self.wait()  # ← Blocking call
        if self._is_exc:
            raise self._obj
        return self._obj
```

**Problem:** `AsyncResult.value` blocks until the reply is received. There is no `__await__()`.

---

## 🎯 Proposal: Extending the Protocol

### Principle: Opt-In via Metadata

We will add **optional metadata** to the existing protocol to mark async functions/results.

### 1. New Constants (consts.py)

```python
# New message types (backward compatible - old clients ignore them)
MSG_ASYNC_REQUEST = 4      # Async request (the result will be a coroutine)
MSG_ASYNC_REPLY = 5        # Async reply (the result was awaited)
MSG_ASYNC_EXCEPTION = 6    # Async exception

# New labels for boxing (complement the existing ones)
LABEL_COROUTINE = 5        # The object is a coroutine
LABEL_ASYNC_RESULT = 6     # A result from an async function

# New handlers (complement the existing ones)
HANDLE_ASYNC_CALL = 21     # Call an async function (await)
HANDLE_ASYNC_CALLATTR = 22 # Call an async method (await)
```

**Backward compatibility:** Old clients/servers simply do not know about these constants and use the old ones.

### 2. Detecting Async Functions

#### On the Server

```python
# Current approach (works as before):
class MyService(rpyc.Service):
    def exposed_sync_add(self, a, b):
        return a + b

# New approach (adding async):
class MyService(rpyc.Service):
    async def exposed_async_add(self, a, b):  # ← async def!
        await asyncio.sleep(0.001)  # Can await
        return a + b
```

**How do we detect this?**

```python
import inspect

def is_async_function(func):
    """Checks whether a function is async def."""
    return inspect.iscoroutinefunction(func)
```

#### On the Client (Callbacks)

```python
# Current approach (works as before):
def my_callback(value):
    return value * 2

result = conn.root.process(my_callback)

# New approach (async callback):
async def my_async_callback(value):  # ← async def!
    await asyncio.sleep(0.001)
    return value * 2

result = await conn.root.process(my_async_callback)  # ← await!
```

### 3. Extending Boxing/Unboxing

#### Detecting Coroutines during Boxing

```python
# In protocol.py:_box()
def _box(self, obj):
    if brine.dumpable(obj):
        return consts.LABEL_VALUE, obj

    # ✅ NEW: Check for a coroutine
    if inspect.iscoroutine(obj):
        # A coroutine cannot be transmitted over the network directly
        # Instead, we return an AsyncResult that can be awaited
        async_result = AsyncResult(self)
        # Schedule execution of the coroutine on the event loop
        self._schedule_coroutine(obj, async_result)
        return consts.LABEL_ASYNC_RESULT, async_result.____id_pack__

    # ✅ NEW: Check for an async function
    if inspect.iscoroutinefunction(obj):
        # This is an async function - pack it as REMOTE_REF
        # but mark with metadata that it is async
        id_pack = get_id_pack(obj)
        self._local_objects.add(id_pack, obj)
        # Add an "is_async" flag to id_pack
        return consts.LABEL_REMOTE_REF, (*id_pack, {'async': True})

    # The rest as before
    ...
```

#### Unboxing with Async Support

```python
# In protocol.py:_unbox()
def _unbox(self, package):
    label, value = package

    if label == consts.LABEL_ASYNC_RESULT:
        # This is an AsyncResult from an async function on the other side
        # We create a local AsyncResult that can be awaited
        return self._create_awaitable_result(value)

    if label == consts.LABEL_REMOTE_REF:
        # Check metadata: async function?
        if isinstance(value, tuple) and len(value) == 4:
            id_pack, metadata = value[:3], value[3]
            if metadata.get('async'):
                # This is an async function - create an async proxy
                return self._create_async_proxy(id_pack)
        # A regular sync function
        ...

    # The rest as before
    ...
```

### 4. New Handlers for Async Calls

```python
# In protocol.py:_request_handlers()
@classmethod
def _request_handlers(cls):
    return {
        # ... existing handlers ...
        consts.HANDLE_CALL: cls._handle_call,          # Sync call
        consts.HANDLE_ASYNC_CALL: cls._handle_async_call,  # ✅ NEW: Async call
        consts.HANDLE_CALLATTR: cls._handle_callattr,  # Sync method
        consts.HANDLE_ASYNC_CALLATTR: cls._handle_async_callattr,  # ✅ NEW: Async method
    }
```

#### Handler for Async Functions

```python
# In protocol.py
def _handle_async_call(self, obj, args, kwargs=()):
    """Handle async function call - does NOT block!"""
    import inspect

    # obj is an async function or a coroutine
    if inspect.iscoroutine(obj):
        coro = obj
    elif inspect.iscoroutinefunction(obj):
        coro = obj(*args, **dict(kwargs))
    else:
        # Fallback to sync
        return self._handle_call(obj, args, kwargs)

    # Return the coroutine (not awaited!)
    # It will be packed as LABEL_COROUTINE in _box()
    return coro
```

**Key difference:** We return a coroutine, not a result!

### 5. Extending AsyncResult with `__await__()`

```python
# In async_.py
class AsyncResult:
    """Extended AsyncResult with await support."""

    # ... existing methods ...

    # ✅ NEW: Support for async/await
    def __await__(self):
        """Makes AsyncResult awaitable in asyncio.

        Usage:
            result = await async_result
        """
        import asyncio

        # If the result is already ready - return it immediately
        if self._is_ready:
            if self._is_exc:
                raise self._obj
            return self._obj

        # Create an asyncio Future and bind it to the AsyncResult
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def on_result(async_res):
            """Callback when the result arrives."""
            if not future.done():
                if async_res._is_exc:
                    loop.call_soon_threadsafe(future.set_exception, async_res._obj)
                else:
                    loop.call_soon_threadsafe(future.set_result, async_res._obj)

        self.add_callback(on_result)

        # Return a generator for await
        return future.__await__()
```

**Problem:** But who will call `conn.serve()`?

### 6. Asyncio-Native Serving (as in arpyc)

For `await` to work without blocking, event-driven serving is needed:

```python
# In protocol.py
class Connection:
    def __init__(self, root, channel, config={}):
        # ... existing code ...

        # ✅ NEW: Asyncio integration
        self._asyncio_loop = None
        self._asyncio_reader_installed = False

    def enable_asyncio_serving(self, loop=None):
        """Enable asyncio-native serving (optional).

        After calling this method, connection.serve() will be called
        automatically when data is available in the event loop.

        Args:
            loop: asyncio event loop (or None for the current one)
        """
        import asyncio

        if loop is None:
            loop = asyncio.get_running_loop()

        self._asyncio_loop = loop

        if not self._asyncio_reader_installed:
            # Install a reader as in arpyc/session_native.py
            fd = self._channel.fileno()

            def on_readable():
                """Callback when the socket is readable."""
                try:
                    # Process ALL available messages
                    while self._channel.poll(0):
                        data = self._channel.recv()
                        self._dispatch(data)
                except EOFError:
                    self.close()
                except Exception as e:
                    if self._config["logger"]:
                        self._config["logger"].exception("Error in asyncio reader")

            loop.add_reader(fd, on_readable)
            self._asyncio_reader_installed = True
```

**How to use it:**

```python
# Client
import asyncio
import rpyc

async def main():
    conn = rpyc.connect("localhost", 18861)

    # ✅ NEW: Enable asyncio serving
    conn.enable_asyncio_serving()

    # Now you can await:
    result = await rpyc.async_(conn.root.async_method)(args)

    # Or if the method is marked as async:
    result = await conn.root.async_method(args)

asyncio.run(main())
```

### 7. Smart Detection of the Call Mode

The client must **automatically detect** a sync vs async method:

```python
# In netref.py (BaseNetref)
class BaseNetref:
    def __call__(self, *args, **kwargs):
        """Call the proxy object."""
        # Request metadata: is this an async function?
        is_async = self.____conn__._is_async_callable(self.____id_pack__)

        if is_async:
            # Use HANDLE_ASYNC_CALL
            return self.____conn__.async_request(
                consts.HANDLE_ASYNC_CALL,
                self.____id_pack__,
                args,
                kwargs
            )
        else:
            # Use the regular HANDLE_CALL
            return self.____conn__.sync_request(
                consts.HANDLE_CALL,
                self.____id_pack__,
                args,
                kwargs
            )
```

**Problem:** How do we know `is_async` without an additional request?

**Solution:** Cache the metadata in `id_pack` or in `HANDLE_INSPECT`.

```python
# In protocol.py:_handle_inspect()
def _handle_inspect(self, id_pack):
    """Returns the methods AND metadata of the object."""
    obj = self._local_objects[id_pack]

    methods = get_methods(netref.LOCAL_ATTRS, obj)

    # ✅ NEW: Add information about async methods
    async_methods = set()
    for method_name in methods:
        method = getattr(obj, method_name, None)
        if inspect.iscoroutinefunction(method):
            async_methods.add(method_name)

    return (methods, async_methods)  # ← Tuple instead of only methods
```

---

## 🔄 Full Flow: Sync vs Async

### Current Sync Flow (remains unchanged)

```
Client                          Server
  |                               |
  | sync_request(HANDLE_CALL)     |
  |------------------------------>|
  |                               | _handle_call(obj, args)
  |                               | result = obj(*args)
  |      MSG_REPLY                |
  |<------------------------------|
  | AsyncResult.value             |
  | (blocks, serve())             |
  |                               |
  V                               V
```

### New Async Flow (added)

```
Client (asyncio)                Server (asyncio)
  |                               |
  | async_request(HANDLE_ASYNC_CALL)
  |------------------------------>|
  |                               | _handle_async_call(obj, args)
  |                               | coro = obj(*args)  ← async def!
  |                               | asyncio.create_task(coro)
  |                               | ↓ await coro
  |      MSG_ASYNC_REPLY          | result = ...
  |<------------------------------|
  | AsyncResult.__await__()       |
  | (does not block!)             |
  | ↓ asyncio Future              |
  | ↓ loop.add_reader()           |
  | ↓ automatic serve             |
  |                               |
  V result                        V
```

**Key difference:**
- **Sync**: `serve()` is called manually in `AsyncResult.wait()` (blocks)
- **Async**: `serve()` is called automatically via `loop.add_reader()` (does not block)

---

## 📦 Code Changes (High-Level)

### 1. `rpyc/core/consts.py`
```python
# Add new constants
MSG_ASYNC_REQUEST = 4
MSG_ASYNC_REPLY = 5
LABEL_COROUTINE = 5
LABEL_ASYNC_RESULT = 6
HANDLE_ASYNC_CALL = 21
HANDLE_ASYNC_CALLATTR = 22
```

### 2. `rpyc/core/protocol.py`

**Add methods:**
- `enable_asyncio_serving(loop=None)` - install the asyncio reader
- `_handle_async_call(obj, args, kwargs)` - handler for async functions
- `_handle_async_callattr(obj, name, args, kwargs)` - handler for async methods
- `_is_async_callable(id_pack)` - check async via the cache

**Modify methods:**
- `_box(obj)` - add handling of coroutines and async functions
- `_unbox(package)` - add unpacking of LABEL_COROUTINE
- `_handle_inspect(id_pack)` - return metadata about async methods

### 3. `rpyc/core/async_.py`

**Add method:**
- `__await__(self)` - make AsyncResult awaitable

### 4. `rpyc/core/netref.py`

**Modify:**
- `BaseNetref.__call__()` - detect async vs sync and choose the handler
- `BaseNetref.__getattr__()` - cache async metadata

### 5. New file: `rpyc/core/asyncio_compat.py`

```python
"""Asyncio compatibility layer for RPyC."""
import asyncio
import inspect

def install_asyncio_reader(connection, loop=None):
    """Install asyncio reader for automatic serving."""
    # Implementation as in the proposal above
    ...

def create_awaitable_result(async_result, loop=None):
    """Create asyncio Future from AsyncResult."""
    # Implementation of the __await__() logic
    ...
```

---

## ✅ Backward Compatibility

### Guarantees

1. **Old client + Old server**: Works as before (100% compatibility)

2. **New client + Old server**:
   - Sync methods work as before
   - Async methods are unavailable (the old server does not support them)
   - No errors, async methods are simply called as sync

3. **Old client + New server**:
   - Sync methods work as before
   - Async methods are called as sync (the old client does not know about async)
   - The server executes the async method via `asyncio.run()` if needed

4. **New client + New server**:
   - ✅ Sync methods work
   - ✅ Async methods work with await
   - ✅ Async callbacks work
   - ✅ Recursion works

### Capability Detection Mechanism

```python
# During the handshake
class Connection:
    def __init__(self, root, channel, config={}):
        # ...
        self._async_support = False  # Disabled by default

        # Check async support on the other side
        try:
            self._async_support = self.sync_request(consts.HANDLE_PING, "__async_support__")
        except:
            self._async_support = False
```

---

## 🚀 Usage Examples

### Example 1: Async Exposed Method

**Server:**
```python
import asyncio
import rpyc

class MyService(rpyc.Service):
    async def exposed_fetch_data(self, url):
        """Async method - can use await!"""
        await asyncio.sleep(1)  # I/O simulation
        return f"Data from {url}"

rpyc.ThreadedServer(MyService, port=18861).start()
```

**Client (New - with async/await):**
```python
import asyncio
import rpyc

async def main():
    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving()  # ✅ Enable asyncio

    # Async call - does NOT block the event loop!
    result = await conn.root.fetch_data("http://example.com")
    print(result)

asyncio.run(main())
```

**Client (Old - sync):**
```python
import rpyc

conn = rpyc.connect("localhost", 18861)

# Sync call - works as before
result = conn.root.fetch_data("http://example.com")
print(result)
```

### Example 2: Async Callbacks

**Server:**
```python
class MyService(rpyc.Service):
    async def exposed_process_with_callback(self, callback):
        """Accepts an async callback."""
        # Call the callback
        result = await callback(42)  # ← await works!
        return f"Callback returned: {result}"
```

**Client:**
```python
async def main():
    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving()

    # Async callback
    async def my_callback(value):
        await asyncio.sleep(0.1)
        return value * 2

    result = await conn.root.process_with_callback(my_callback)
    print(result)  # "Callback returned: 84"
```

### Example 3: Recursion with Async

**Server:**
```python
class AsyncRecursiveService(rpyc.Service):
    async def exposed_recursive_call(self, depth, callback):
        """Async recursive method."""
        await asyncio.sleep(0.001)

        if depth > 0:
            result = await callback(depth - 1, self.exposed_recursive_call)
            return f"Server: {result}"
        return "Server: done"
```

**Client:**
```python
async def main():
    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving()

    async def client_callback(depth, server_func):
        """Async callback - can await!"""
        await asyncio.sleep(0.001)

        if depth > 0:
            result = await server_func(depth - 1, client_callback)
            return f"Client: {result}"
        return "Client: done"

    # Start the recursion
    result = await conn.root.recursive_call(5, client_callback)
    print(result)
```

**Key difference from the current approach:**
- ✅ Does NOT block the event loop
- ✅ Does NOT require BgServingThread
- ✅ Does NOT require nest_asyncio
- ✅ Recursion works naturally via await

---

## 🎯 Implementation Priorities

### Phase 1: Basic support for async methods
1. Add constants to `consts.py`
2. Implement `AsyncResult.__await__()`
3. Implement `enable_asyncio_serving()`
4. Add `_handle_async_call()`
5. Tests for async exposed methods

### Phase 2: Async callbacks
1. Extend `_box()/_unbox()` for coroutines
2. Implement passing async functions as callbacks
3. Tests for async callbacks

### Phase 3: Auto-detection and optimizations
1. Implement `_handle_inspect()` with async metadata
2. Automatic detection of async vs sync
3. Metadata caching
4. Tests for backward compatibility

### Phase 4: Documentation and examples
1. Update the documentation
2. Create usage examples
3. Migration guide

---

## 🔒 Security

Async support **does not add new vulnerabilities**:

1. **The same access checks**: `allow_exposed_attrs`, `exposed_prefix` remain
2. **The same restrictions**: `allow_pickle`, `allow_all_attrs` work as before
3. **Isolation**: Async code runs in the same context as sync

---

## 📊 Performance

### Expected Improvements

**With asyncio serving (vs BgServingThread):**
- ✅ **~100-150x faster** for multiple concurrent calls
- ✅ **No polling delays** (0.1s in BgServingThread)
- ✅ **Event-driven** instead of polling

**With async exposed methods:**
- ✅ The server can handle **thousands** of concurrent I/O operations
- ✅ A single event loop instead of many threads

---

## ❓ Open Questions

1. **How do we handle `asyncio.run()` in a sync context?**
   - **Solution**: If the server is async-capable but the client is sync, the server executes it via `asyncio.run()`

2. **What if the event loop is not running?**
   - **Solution**: `enable_asyncio_serving()` raises `RuntimeError` if there is no event loop

3. **How do we work with older Python versions?**
   - **Solution**: Python 3.7+ is required for async support (as with current RPyC)

4. **Thread safety with asyncio?**
   - **Solution**: Asyncio serving runs in a single thread; thread safety is ensured via `loop.call_soon_threadsafe()`

---

## 📚 References

1. [PEP 492 - Coroutines with async and await](https://www.python.org/dev/peps/pep-0492/)
2. [asyncio Documentation](https://docs.python.org/3/library/asyncio.html)
3. [RPyC Issue #506 - Async Coroutine Support](https://github.com/tomerfiliba-org/rpyc/issues/506)
4. An internal research document on arpyc async recursion (not included here)

---

## ✨ Overall Value

**For users:**
- ✅ Natural integration with asyncio applications
- ✅ Significant performance improvement
- ✅ Ease of use (just add `async def` and `await`)
- ✅ Full backward compatibility

**For RPyC:**
- ✅ Modern asyncio support
- ✅ Competitiveness with other RPC frameworks
- ✅ Preservation of unique advantages (transparency, symmetry)
- ✅ Expansion of use cases (async web frameworks, high-concurrency apps)
