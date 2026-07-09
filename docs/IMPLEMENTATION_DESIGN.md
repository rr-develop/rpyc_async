# rpyc-async — Detailed Implementation Design

> **Product**: `rpyc-async` (distribution name), the import name remains `rpyc`.
> This is an asyncio-native fork, split off from upstream RPyC 6.0.1 and developed
> as an independent project with its own version **1.0.0**.
> Backward compatibility with classic synchronous RPyC is **not guaranteed**.
> The minimum supported Python version is **3.10**.

## 📋 Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Module Structure](#module-structure)
3. [Detailed Component Design](#detailed-component-design)
4. [Protocol Changes](#protocol-changes)
5. [Interoperability Strategy](#interoperability-strategy)
6. [Implementation Plan](#implementation-plan)
7. [Testing Strategy](#testing-strategy)
8. [Risks and Mitigation](#risks-and-mitigation)

---

## Architecture Overview

### Key Principles

1. **Opt-In Design**: Async functionality is enabled explicitly via `enable_asyncio_serving()`
2. **Zero-Cost Abstraction**: Sync code does not pay for async capabilities
3. **Graceful Degradation**: Peers with different protocol capabilities work correctly
4. **Thread-Safe**: All operations are safe when used from different threads

### Architectural Layers

```
┌─────────────────────────────────────────────────────────┐
│  Application Layer (User Code)                          │
│  • async def exposed_method()                           │
│  • await conn.root.method()                             │
│  • async callbacks                                      │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│  Service Layer (rpyc.core.service)                      │
│  • Service class (unchanged)                            │
│  • Async method detection                               │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│  Protocol Layer (rpyc.core.protocol)                    │
│  • Connection class (enhanced)                          │
│  • Async dispatch pipeline                              │
│  • Message routing                                      │
└────────────────────┬────────────────────────────────────┘
                     │
         ┌───────────┴──────────┐
         │                      │
┌────────▼─────────┐  ┌─────────▼──────────┐
│  Sync Dispatch   │  │  Async Dispatch    │
│  (existing)      │  │  (new)             │
│  _dispatch_      │  │  _dispatch_        │
│  request()       │  │  request_async()   │
└────────┬─────────┘  └─────────┬──────────┘
         │                      │
         │            ┌─────────▼──────────┐
         │            │  Event Loop        │
         │            │  Integration       │
         │            └────────────────────┘
         │
┌────────▼────────────────────────────────────────────────┐
│  Transport Layer (rpyc.core.channel)                    │
│  • Socket I/O                                           │
│  • Frame protocol                                       │
└─────────────────────────────────────────────────────────┘
```

---

## Module Structure

### New Files

```
rpyc/
├── core/
│   ├── protocol.py          # [MODIFIED] Connection class
│   ├── async_.py            # [MODIFIED] AsyncResult.__await__()
│   ├── consts.py            # [MODIFIED] New constants
│   ├── netref.py            # [MODIFIED] Async proxy support
│   ├── service.py           # [UNCHANGED] Service definitions
│   ├── brine.py             # [UNCHANGED] Serialization
│   └── async_handlers.py    # [NEW] Async handler implementations
│
├── utils/
│   ├── helpers.py           # [MODIFIED] Async detection utils
│   └── asyncio_helpers.py   # [NEW] Asyncio integration utilities
│
└── lib/
    └── compat.py            # [MODIFIED] Async compatibility checks
```

### Modified Modules

| File | Changes | LOC (est.) | Risk |
|------|-----------|------------|------|
| `core/protocol.py` | Async dispatch, event loop integration | +300 | Medium |
| `core/async_.py` | `__await__()`, async callbacks | +150 | Low |
| `core/consts.py` | New constants | +20 | Low |
| `core/netref.py` | Async proxy metadata | +50 | Low |
| `utils/helpers.py` | Async detection | +80 | Low |
| `core/async_handlers.py` | **NEW** Async handlers | +200 | Medium |
| `utils/asyncio_helpers.py` | **NEW** Event loop utils | +150 | Low |

**Total**: ~950 LOC added/modified

---

## Detailed Component Design

### 1. Constants Extension (`core/consts.py`)

#### New Constants

```python
# rpyc/core/consts.py

# ═══════════════════════════════════════════════════════
# NEW: Async Message Types
# ═══════════════════════════════════════════════════════
MSG_ASYNC_REQUEST = 10      # Async RPC request
MSG_ASYNC_REPLY = 11        # Async RPC reply
MSG_ASYNC_EXCEPTION = 12    # Async RPC exception

# ═══════════════════════════════════════════════════════
# NEW: Async Handlers
# ═══════════════════════════════════════════════════════
HANDLE_ASYNC_CALL = 100          # Call async function
HANDLE_ASYNC_CALLATTR = 101      # Call async method/attribute

# ═══════════════════════════════════════════════════════
# NEW: Object Flags (for id_pack extension)
# ═══════════════════════════════════════════════════════
FLAGS_SYNC = 0x00          # Default: sync object
FLAGS_ASYNC = 0x01         # Bit 0: async function/coroutine
# Reserved for future:
# FLAGS_GENERATOR = 0x02   # Bit 1: generator
# FLAGS_CONTEXT = 0x04     # Bit 2: context manager

# ═══════════════════════════════════════════════════════
# NEW: Protocol Version
# ═══════════════════════════════════════════════════════
# Wire-protocol revision advertised by rpyc-async peers.
# Independent of the package version (rpyc-async 1.0.0).
PROTOCOL_VERSION_ASYNC = (1, 0)  # classic synchronous RPyC advertises none
```

**Design Decisions:**
- Message type IDs start at 10 to avoid conflicts
- Handler IDs start at 100 for clear separation
- Flags use bitmask for future extensibility
- Async-capable protocol revision is advertised explicitly; peers speaking the
  classic synchronous RPyC protocol simply do not expose it

---

### 2. Protocol Layer (`core/protocol.py`)

#### 2.1 Connection Class Enhancement

##### New Attributes

```python
class Connection:
    def __init__(self, service, channel, config={}):
        # ... existing init ...

        # ═══════════════════════════════════════════════════
        # NEW: Asyncio Support
        # ═══════════════════════════════════════════════════
        self._asyncio_loop = None           # Event loop reference
        self._asyncio_enabled = False       # Async serving enabled?
        self._loop_fd_registered = False    # FD registered in loop?
        self._async_dispatch_lock = threading.Lock()  # Thread safety

        # Async handler registry
        self._async_handlers = {}
        self._register_async_handlers()
```

**Thread Safety**: `_async_dispatch_lock` protects concurrent access to async dispatch state.

##### New Methods: Asyncio Integration

```python
def enable_asyncio_serving(self, loop=None):
    """
    Enable asyncio-native serving for this connection.

    Args:
        loop: asyncio event loop to use (default: get_running_loop())

    Raises:
        RuntimeError: If no event loop is running

    Example:
        conn = rpyc.connect("localhost", 18861)
        conn.enable_asyncio_serving()  # Use current loop
        await conn.root.async_method()
    """
    import asyncio

    if self._asyncio_enabled:
        return  # Already enabled

    # Get event loop
    if loop is None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                "enable_asyncio_serving() must be called from within "
                "a running event loop, or pass loop explicitly"
            )

    self._asyncio_loop = loop
    self._asyncio_enabled = True

    # Register file descriptor for read events
    fd = self._channel.fileno()

    def on_readable():
        """Called when socket has data to read."""
        with self._async_dispatch_lock:
            # Read all available data (edge-triggered behavior)
            while self._channel.poll(0):
                try:
                    data = self._channel.recv()
                    self._dispatch(data)
                except EOFError:
                    self.close()
                    break
                except Exception as e:
                    # Log and continue
                    self._handle_dispatch_error(e)

    loop.add_reader(fd, on_readable)
    self._loop_fd_registered = True


def disable_asyncio_serving(self):
    """
    Disable asyncio-native serving.

    Removes FD from event loop and disables async dispatch.
    Safe to call multiple times.
    """
    if not self._asyncio_enabled:
        return

    if self._loop_fd_registered and self._asyncio_loop:
        fd = self._channel.fileno()
        self._asyncio_loop.remove_reader(fd)
        self._loop_fd_registered = False

    self._asyncio_enabled = False
    self._asyncio_loop = None


def close(self):
    """Enhanced close() to cleanup asyncio resources."""
    self.disable_asyncio_serving()  # NEW: Cleanup
    # ... existing close logic ...
```

**Design Decisions:**
- `enable_asyncio_serving()` must be called from event loop context
- Uses `add_reader()` for edge-triggered, non-blocking I/O
- `on_readable()` drains all available data (while loop)
- Proper cleanup in `close()`

##### New Methods: Async Detection

```python
def _is_async_handler(self, handler_id):
    """
    Check if handler requires async dispatch.

    Args:
        handler_id: Handler constant (HANDLE_CALL, etc.)

    Returns:
        bool: True if handler is async
    """
    # Explicit async handlers
    if handler_id in (
        consts.HANDLE_ASYNC_CALL,
        consts.HANDLE_ASYNC_CALLATTR
    ):
        return True

    # Check handler function signature
    handler_func = self._HANDLERS.get(handler_id)
    if handler_func is None:
        return False

    return inspect.iscoroutinefunction(handler_func)


def _needs_async_dispatch(self, msg_type, args):
    """
    Determine if message requires async dispatch pipeline.

    Args:
        msg_type: Message type constant
        args: Message arguments (handler_id, ...)

    Returns:
        bool: True if async dispatch needed
    """
    # Explicit async messages
    if msg_type in (
        consts.MSG_ASYNC_REQUEST,
        consts.MSG_ASYNC_REPLY,
        consts.MSG_ASYNC_EXCEPTION
    ):
        return True

    # Check handler type
    if msg_type == consts.MSG_REQUEST:
        handler_id, _ = args
        return self._is_async_handler(handler_id)

    return False
```

**Design Decisions:**
- Two-level check: explicit async messages + handler inspection
- Cached handler lookup for performance
- Extensible for future message types

#### 2.2 Async Dispatch Pipeline

##### Modified `_dispatch()` Method

```python
def _dispatch(self, data):
    """
    Main dispatcher - routes to sync or async pipeline.

    This is the ONLY entry point for incoming messages.
    MUST remain sync (called from I/O thread or event loop).
    """
    # Unpack message type
    msg, = brine.I1.unpack(data[:1])

    # Parse message
    if msg == consts.MSG_REQUEST:
        seq, args = brine.load(data[1:])

        # ═══════════════════════════════════════════════════
        # ROUTING DECISION: Sync vs Async
        # ═══════════════════════════════════════════════════
        needs_async = self._needs_async_dispatch(msg, args)

        if needs_async and self._asyncio_enabled and self._asyncio_loop:
            # ═══════════════════════════════════════════════
            # ASYNC DISPATCH PIPELINE
            # ═══════════════════════════════════════════════
            import asyncio

            # Schedule async execution in event loop
            future = asyncio.run_coroutine_threadsafe(
                self._dispatch_request_async(seq, args),
                self._asyncio_loop
            )
            # IMPORTANT: Do NOT wait for future.result()
            # Function returns immediately!

            # Optional: Store future for error tracking
            self._track_async_dispatch(seq, future)
        else:
            # ═══════════════════════════════════════════════
            # SYNC DISPATCH (existing behavior)
            # ═══════════════════════════════════════════════
            self._dispatch_request(seq, args)

    elif msg == consts.MSG_REPLY:
        # ... existing reply handling ...
        pass

    elif msg == consts.MSG_EXCEPTION:
        # ... existing exception handling ...
        pass

    elif msg == consts.MSG_ASYNC_REQUEST:
        # NEW: Explicit async request
        seq, args = brine.load(data[1:])
        if self._asyncio_enabled and self._asyncio_loop:
            import asyncio
            asyncio.run_coroutine_threadsafe(
                self._dispatch_request_async(seq, args),
                self._asyncio_loop
            )
        else:
            # Fallback to sync dispatch (degradation)
            self._dispatch_request(seq, args)

    elif msg == consts.MSG_ASYNC_REPLY:
        # NEW: Async reply handling
        seq, obj = brine.load(data[1:])
        self._dispatch_async_reply(seq, obj)

    elif msg == consts.MSG_ASYNC_EXCEPTION:
        # NEW: Async exception handling
        seq, obj = brine.load(data[1:])
        self._dispatch_async_exception(seq, obj)

    else:
        # Unknown message type
        raise ValueError(f"Invalid message type: {msg}")


def _track_async_dispatch(self, seq, future):
    """
    Track async dispatch future for error handling.

    Args:
        seq: Request sequence number
        future: concurrent.futures.Future from run_coroutine_threadsafe
    """
    def on_done(fut):
        try:
            fut.result()  # Re-raise exceptions
        except Exception as e:
            # Log error (already sent to client in _dispatch_request_async)
            self._log_async_dispatch_error(seq, e)

    future.add_done_callback(on_done)
```

**Critical Design Decisions:**
- `_dispatch()` MUST remain sync (I/O callback)
- `run_coroutine_threadsafe()` is thread-safe (can call from any thread)
- Do NOT call `future.result()` - would block!
- Error tracking via callbacks, not blocking waits

##### New `_dispatch_request_async()` Method

```python
async def _dispatch_request_async(self, seq, raw_args):
    """
    Async version of _dispatch_request() - can await handlers!

    Args:
        seq: Request sequence number
        raw_args: Raw (handler_id, args) tuple

    This method runs in the event loop and can safely await.
    """
    try:
        handler_id, args = raw_args

        # Unbox arguments
        args = self._unbox(args)

        # Get handler function
        handler_func = self._HANDLERS.get(handler_id)
        if handler_func is None:
            raise AttributeError(f"Unknown handler: {handler_id}")

        # ═══════════════════════════════════════════════════
        # EXECUTE HANDLER (async or sync)
        # ═══════════════════════════════════════════════════
        if inspect.iscoroutinefunction(handler_func):
            # Handler is async - await it!
            res = await handler_func(self, *args)
        else:
            # Handler is sync - call normally
            # (might be HANDLE_CALL calling async function)
            res = handler_func(self, *args)

            # Check if result is coroutine (from async function call)
            if inspect.iscoroutine(res):
                res = await res

    except Exception:
        # Exception during execution
        t, v, tb = sys.exc_info()

        # Box exception
        exc_data = self._box_exc(t, v, tb)

        # Send async exception reply
        self._send(consts.MSG_ASYNC_EXCEPTION, seq, exc_data)
    else:
        # Success - box result
        boxed_res = self._box(res)

        # Send async reply
        self._send(consts.MSG_ASYNC_REPLY, seq, boxed_res)
```

**Design Decisions:**
- Dual execution path: await async handlers, call sync handlers
- Result inspection: await coroutines returned by sync handlers
- Error handling: send MSG_ASYNC_EXCEPTION on failure
- Boxing/unboxing: reuse existing methods (no duplication)

#### 2.3 Async Reply Handling

```python
def _dispatch_async_reply(self, seq, obj):
    """
    Handle MSG_ASYNC_REPLY message.

    Args:
        seq: Request sequence number
        obj: Boxed result object
    """
    # Unbox result
    result = self._unbox(obj)

    # Find pending AsyncResult
    async_res = self._async_calls.get(seq)
    if async_res is None:
        # Sequence not found (already timeout/cancelled?)
        return

    # Set result (triggers callbacks)
    async_res._set_result(result)

    # Cleanup
    del self._async_calls[seq]


def _dispatch_async_exception(self, seq, obj):
    """
    Handle MSG_ASYNC_EXCEPTION message.

    Args:
        seq: Request sequence number
        obj: Boxed exception object
    """
    # Unbox exception
    exc = self._unbox(obj)

    # Find pending AsyncResult
    async_res = self._async_calls.get(seq)
    if async_res is None:
        return

    # Set exception (triggers callbacks)
    async_res._set_exception(exc)

    # Cleanup
    del self._async_calls[seq]
```

---

### 3. Async Handlers (`core/async_handlers.py`)

**NEW FILE**: Async handler implementations.

```python
"""
Async handler implementations for RPyC protocol.

This module provides async-aware handlers that can execute
async functions and coroutines without blocking.
"""

import inspect


async def _handle_async_call(conn, obj, args, kwargs=()):
    """
    Handler for HANDLE_ASYNC_CALL.

    Executes async function calls and awaits coroutines.

    Args:
        conn: Connection instance
        obj: Callable or coroutine to execute
        args: Positional arguments tuple
        kwargs: Keyword arguments list of (key, value) tuples

    Returns:
        Result of function call

    Raises:
        Exception: Any exception raised by the function
    """
    kwargs_dict = dict(kwargs)  # Convert list to dict

    # Case 1: obj is already a coroutine (pre-created)
    if inspect.iscoroutine(obj):
        result = await obj
        return result

    # Case 2: obj is async function - call and await
    if inspect.iscoroutinefunction(obj):
        coro = obj(*args, **kwargs_dict)
        result = await coro
        return result

    # Case 3: obj is sync function - call normally
    # (This handles sync callbacks passed to async exposed methods)
    result = obj(*args, **kwargs_dict)

    # If sync function returned coroutine, await it
    if inspect.iscoroutine(result):
        result = await result

    return result


async def _handle_async_callattr(conn, obj, name, args, kwargs=()):
    """
    Handler for HANDLE_ASYNC_CALLATTR.

    Gets attribute and calls it asynchronously if needed.

    Args:
        conn: Connection instance
        obj: Object to get attribute from
        name: Attribute name
        args: Positional arguments tuple
        kwargs: Keyword arguments list of (key, value) tuples

    Returns:
        Result of method call

    Raises:
        AttributeError: If attribute doesn't exist
        Exception: Any exception raised by the method
    """
    # Get attribute
    attr = getattr(obj, name)

    # Call via _handle_async_call
    return await _handle_async_call(conn, attr, args, kwargs)


def register_async_handlers(conn):
    """
    Register async handlers in connection.

    Args:
        conn: Connection instance to register handlers on
    """
    conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _handle_async_call
    conn._HANDLERS[consts.HANDLE_ASYNC_CALLATTR] = _handle_async_callattr
```

**Design Decisions:**
- Three-case logic: coroutine, async function, sync function
- Symmetric with sync handlers (`_handle_call`, `_handle_callattr`)
- Reuses connection's boxing/unboxing logic
- Registered via `register_async_handlers()` (clean separation)

---

### 4. Boxing/Unboxing Enhancement (`core/protocol.py`)

#### 4.1 Modified `_box()` Method

```python
def _box(self, obj):
    """
    Box an object for transmission (ENHANCED for async).

    Args:
        obj: Object to box

    Returns:
        (label, value) tuple
    """
    # ... existing boxing logic for primitives ...

    # ═══════════════════════════════════════════════════════
    # NEW: Async Function Boxing
    # ═══════════════════════════════════════════════════════
    if inspect.iscoroutinefunction(obj):
        # This is an async function - create netref with metadata
        id_pack = self._get_id_pack(obj)

        # Store in local objects registry
        self._local_objects[id_pack] = obj

        # Add async flag to id_pack
        id_pack_with_flags = (*id_pack, consts.FLAGS_ASYNC)

        return consts.LABEL_REMOTE_REF, id_pack_with_flags

    # ═══════════════════════════════════════════════════════
    # NEW: Coroutine Boxing (runtime instance)
    # ═══════════════════════════════════════════════════════
    if inspect.iscoroutine(obj):
        # WARNING: Boxing a coroutine instance is unusual!
        # Typically indicates a bug (forgot to await).
        # We box it as remote ref but log warning.
        import warnings
        warnings.warn(
            f"Boxing coroutine object {obj!r}. "
            f"Did you forget to await?",
            RuntimeWarning,
            stacklevel=2
        )

        id_pack = self._get_id_pack(obj)
        self._local_objects[id_pack] = obj
        id_pack_with_flags = (*id_pack, consts.FLAGS_ASYNC)

        return consts.LABEL_REMOTE_REF, id_pack_with_flags

    # ... existing boxing logic for other types ...
```

**Design Decisions:**
- Separate handling for async functions vs coroutines
- Warning on coroutine boxing (likely bug)
- Extended id_pack format: `(class, id, ver, flags)`
- Tolerant decoding: upstream RPyC peers send a 3-tuple, rpyc-async handles both shapes

#### 4.2 Modified `_unbox()` Method

```python
def _unbox(self, package):
    """
    Unbox an object from transmission (ENHANCED for async).

    Args:
        package: (label, value) tuple

    Returns:
        Unboxed object
    """
    label, value = package

    # ... existing unboxing logic ...

    if label == consts.LABEL_REMOTE_REF:
        # ═══════════════════════════════════════════════════
        # ENHANCED: Handle extended id_pack format
        # ═══════════════════════════════════════════════════

        # Check if extended format (4 elements) or legacy format (3)
        if len(value) == 4:
            # rpyc-async format: (class, id, ver, flags)
            id_pack = (value[0], value[1], value[2])
            flags = value[3]
        elif len(value) == 3:
            # classic synchronous RPyC format: (class, id, ver)
            id_pack = value
            flags = consts.FLAGS_SYNC  # Default: sync object
        else:
            raise ValueError(f"Invalid id_pack length: {len(value)}")

        # Create netref proxy
        proxy = self._netref_factory(id_pack)

        # ═══════════════════════════════════════════════════
        # NEW: Attach async metadata to proxy
        # ═══════════════════════════════════════════════════
        if flags & consts.FLAGS_ASYNC:
            # Mark proxy as async
            # This metadata is used by sync_request/async_request
            proxy.____is_async__ = True

        return proxy

    # ... existing unboxing logic ...
```

**Design Decisions:**
- Tolerant decoding: handles both 3-tuple and 4-tuple id_packs
- Metadata stored in proxy: `____is_async__` attribute
- Bitmask flags for future extensibility (generators, context managers)

---

### 5. AsyncResult Enhancement (`core/async_.py`)

#### 5.1 New `__await__()` Method

```python
class AsyncResult(object):
    """
    Async result object (ENHANCED with __await__).
    """

    __slots__ = [
        # ... existing slots ...
        '_awaiter_callbacks',  # NEW: Callbacks for awaiters
    ]

    def __init__(self, conn):
        # ... existing init ...
        self._awaiter_callbacks = []  # NEW

    def __await__(self):
        """
        Make AsyncResult awaitable in async context.

        Usage:
            result = await conn.root.async_method()

        Returns:
            Result value if ready, otherwise waits asynchronously.

        Raises:
            Exception: If remote call raised exception
        """
        import asyncio

        # Fast path: result already ready
        if self._is_ready:
            if self._is_exc:
                # Exception ready - raise it
                async def _raise_exc():
                    raise self._obj
                return _raise_exc().__await__()
            else:
                # Value ready - return it
                async def _return_value():
                    return self._obj
                return _return_value().__await__()

        # Slow path: result not ready yet
        # Create a Future and register callback
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def on_result(async_res):
            """Callback when result becomes ready."""
            if future.done():
                return  # Already resolved (timeout/cancel)

            if async_res._is_exc:
                # Exception - set exception on future
                loop.call_soon_threadsafe(
                    future.set_exception,
                    async_res._obj
                )
            else:
                # Success - set result on future
                loop.call_soon_threadsafe(
                    future.set_result,
                    async_res._obj
                )

        # Register callback
        self.add_callback(on_result)

        # Track for cleanup
        self._awaiter_callbacks.append(on_result)

        # Return future's awaitable
        return future.__await__()

    def _set_result(self, value):
        """
        Set result value (ENHANCED to call awaiter callbacks).

        Args:
            value: Result value
        """
        self._obj = value
        self._is_ready = True
        self._is_exc = False

        # Call registered callbacks
        self._invoke_callbacks()

    def _set_exception(self, exc):
        """
        Set exception (ENHANCED to call awaiter callbacks).

        Args:
            exc: Exception object
        """
        self._obj = exc
        self._is_ready = True
        self._is_exc = True

        # Call registered callbacks
        self._invoke_callbacks()
```

**Design Decisions:**
- Fast path optimization: immediate return if ready
- Slow path: Future-based waiting with callbacks
- Thread-safe: `call_soon_threadsafe()` for cross-thread communication
- Memory management: track awaiter callbacks for cleanup

---

### 6. Netref Proxy Enhancement (`core/netref.py`)

#### 6.1 Async Proxy Call Detection

```python
class BaseNetref(object):
    """
    Base netref proxy (ENHANCED for async detection).
    """

    __slots__ = [
        # ... existing slots ...
        '____is_async__',  # NEW: Async metadata flag
    ]

    def __call__(self, *args, **kwargs):
        """
        Call remote function (ENHANCED for async detection).

        Returns:
            AsyncResult that can be awaited if function is async
        """
        # Check if this is an async function
        is_async = getattr(self, '____is_async__', False)

        if is_async:
            # Use async handler
            handler = consts.HANDLE_ASYNC_CALL
        else:
            # Use sync handler
            handler = consts.HANDLE_CALL

        # Make async request
        return self.____conn__.async_request(
            handler,
            self.____oid__,
            args,
            tuple(kwargs.items())
        )
```

**Design Decisions:**
- Metadata-driven: `____is_async__` flag from unboxing
- Transparent: no API changes, just smarter routing
- AsyncResult returned in both cases (await optional for sync)

---

## Protocol Changes

### Message Format Extensions

#### Legacy Format (classic synchronous RPyC)

```
┌──────────┬─────────┬─────────────────┐
│ msg_type │   seq   │  args (brine)   │
│ (1 byte) │ (vary)  │     (vary)      │
└──────────┴─────────┴─────────────────┘
```

#### rpyc-async Format — Tolerant to Legacy Peers

**Request Messages:**
```
┌──────────┬─────────┬─────────────────────────────────┐
│ msg_type │   seq   │  (handler_id, args) (brine)    │
│          │         │                                 │
│ MSG_     │         │  args may contain extended      │
│ [ASYNC_] │         │  id_pack: (cls, id, ver, flags) │
│ REQUEST  │         │                                 │
└──────────┴─────────┴─────────────────────────────────┘
```

**id_pack Extension:**
```
Legacy (classic sync RPyC): (class_name, obj_id, class_version)
                            ↓
rpyc-async:                 (class_name, obj_id, class_version, flags)
                                                                 ↑
                                                          Bit 0: FLAGS_ASYNC
                                                          Bit 1-7: Reserved
```

**Wire Interoperability (best-effort, not a compatibility guarantee):**
- Legacy client → rpyc-async server: 3-tuple id_pack, flags=0x00 assumed
- rpyc-async client → legacy server: async features unavailable; the async
  protocol revision is absent, so the client must not use async paths
- Both directions: Protocol negotiation via HANDLE_GETATTR on ____protocol_version__

### Protocol Negotiation Flow

```python
# Client connects to server
conn = rpyc.connect("localhost", 18861)

# Check server protocol version
try:
    server_version = conn.root.____protocol_version__
    if server_version >= consts.PROTOCOL_VERSION_ASYNC:
        # Server speaks the rpyc-async protocol
        conn.enable_asyncio_serving()
        supports_async = True
    else:
        # Peer predates the async protocol revision
        supports_async = False
except AttributeError:
    # classic synchronous RPyC server (no async protocol attribute)
    supports_async = False

# Use appropriate API
if supports_async:
    result = await conn.root.async_method()
else:
    result = conn.root.sync_method()
```

---

## Interoperability Strategy

> **Important**: rpyc-async is an independent project. Compatibility with classic
> synchronous RPyC at the API and protocol level is **not guaranteed**. The table
> below describes the observed behaviour with mixed peers, not a contract.

### Interoperability Matrix

| Client | Server | Async Calls | Async Callbacks | Notes |
|--------|--------|-------------|-----------------|-------|
| rpyc-async | rpyc-async | ✅ Full | ✅ Full | Optimal performance |
| rpyc-async | classic sync RPyC | ❌ Degrade | ❌ Fail | Client detects, falls back to sync |
| classic sync RPyC | rpyc-async | N/A | N/A | Legacy client unaware of async |
| classic sync RPyC | classic sync RPyC | N/A | N/A | Out of scope for this project |

### Graceful Degradation Strategies

#### Strategy 1: Client Detection

```python
class AsyncAwareClient:
    def __init__(self, host, port):
        self.conn = rpyc.connect(host, port)
        self.async_supported = self._detect_async_support()

        if self.async_supported:
            self.conn.enable_asyncio_serving()

    def _detect_async_support(self):
        """Check if server supports the rpyc-async protocol."""
        try:
            version = self.conn.root.____protocol_version__
            return version >= consts.PROTOCOL_VERSION_ASYNC
        except AttributeError:
            return False

    async def call_method(self, method_name, *args):
        """Smart call: async if supported, sync otherwise."""
        if self.async_supported:
            return await getattr(self.conn.root, method_name)(*args)
        else:
            return getattr(self.conn.root, method_name)(*args)
```

#### Strategy 2: Server Compatibility Mode

```python
class CompatibleService(rpyc.Service):
    """Service that tolerates both legacy and rpyc-async clients."""

    def exposed_process(self, callback):
        """
        Process callback (sync or async).

        Works with:
        - Legacy client + sync callback: ✅
        - rpyc-async client + async callback: ✅
        - Legacy client + async callback: ❌ (client error)
        """
        # Check if callback is async
        is_async = getattr(callback, '____is_async__', False)

        if is_async:
            # Must await (requires async context)
            return self._process_async(callback)
        else:
            # Sync callback
            result = callback(42)
            return result

    async def _process_async(self, callback):
        """Helper for async callback processing."""
        result = await callback(42)
        return result
```

### Migration Path

#### Phase 1: Infrastructure (Week 1-2)
- ✅ Add new constants
- ✅ Implement async handlers
- ✅ Add protocol version attribute

#### Phase 2: Opt-In Features (Week 3-4)
- ✅ Implement `enable_asyncio_serving()`
- ✅ Add `AsyncResult.__await__()`
- ✅ Enhanced boxing/unboxing
- ⚠️ Async features are opt-in within rpyc-async

#### Phase 3: Testing & Validation (Week 5-6)
- ✅ Integration tests for all interoperability scenarios
- ✅ Performance benchmarks
- ✅ Security audit
- ✅ Documentation updates

#### Phase 4: Rollout (Week 7+)
- ✅ Release rpyc-async 1.0.0
- ✅ Update examples and tutorials
- ✅ Monitor production deployments
- ⚠️ A future major release may make async the default execution mode

---

## Implementation Plan

### Phase 1: Core Infrastructure (5-7 days)

#### Task 1.1: Constants & Protocol Version
**File**: `rpyc/core/consts.py`
**Estimate**: 2 hours
**Dependencies**: None

- [ ] Add `MSG_ASYNC_REQUEST`, `MSG_ASYNC_REPLY`, `MSG_ASYNC_EXCEPTION`
- [ ] Add `HANDLE_ASYNC_CALL`, `HANDLE_ASYNC_CALLATTR`
- [ ] Add `FLAGS_SYNC`, `FLAGS_ASYNC`
- [ ] Bump `PROTOCOL_VERSION` to (5, 1)
- [ ] Add unit tests for constants

**Acceptance Criteria:**
- All constants defined with correct values
- No conflicts with existing constants
- Tests pass

#### Task 1.2: Async Handlers Module
**File**: `rpyc/core/async_handlers.py` (NEW)
**Estimate**: 1 day
**Dependencies**: Task 1.1

- [ ] Implement `_handle_async_call()`
- [ ] Implement `_handle_async_callattr()`
- [ ] Implement `register_async_handlers()`
- [ ] Add docstrings with examples
- [ ] Unit tests for both handlers

**Acceptance Criteria:**
- Handlers execute async functions correctly
- Handlers await coroutines
- Handlers handle sync functions (fallback)
- Error propagation works
- Tests cover all code paths

#### Task 1.3: Detection Utilities
**File**: `rpyc/utils/helpers.py`
**Estimate**: 4 hours
**Dependencies**: None

- [ ] Implement `is_async_function()`
- [ ] Implement `is_coroutine()`
- [ ] Implement `is_async_capable()`
- [ ] Add caching for performance
- [ ] Unit tests

**Acceptance Criteria:**
- Correctly detects async functions
- Correctly detects coroutines
- Performance: <1μs per check (cached)
- Tests cover edge cases (partials, lambdas, etc.)

### Phase 2: Protocol Layer (7-10 days)

#### Task 2.1: Connection Enhancement - Asyncio Integration
**File**: `rpyc/core/protocol.py`
**Estimate**: 2 days
**Dependencies**: Task 1.1, 1.2, 1.3

- [ ] Add `_asyncio_loop`, `_asyncio_enabled` attributes
- [ ] Implement `enable_asyncio_serving()`
- [ ] Implement `disable_asyncio_serving()`
- [ ] Modify `close()` for cleanup
- [ ] Unit tests for enable/disable

**Acceptance Criteria:**
- `enable_asyncio_serving()` registers FD correctly
- `disable_asyncio_serving()` cleans up properly
- Works with external event loop
- Fails gracefully if no event loop running
- Thread-safe
- Tests pass

#### Task 2.2: Connection Enhancement - Async Dispatch
**File**: `rpyc/core/protocol.py`
**Estimate**: 3 days
**Dependencies**: Task 2.1

- [ ] Implement `_is_async_handler()`
- [ ] Implement `_needs_async_dispatch()`
- [ ] Modify `_dispatch()` for routing
- [ ] Implement `_dispatch_request_async()`
- [ ] Implement `_track_async_dispatch()`
- [ ] Add async reply/exception handlers
- [ ] Integration tests

**Acceptance Criteria:**
- Routing logic correct (sync vs async)
- `_dispatch_request_async()` can await handlers
- Error handling works (exceptions sent as MSG_ASYNC_EXCEPTION)
- No deadlocks or race conditions
- Performance: <100μs overhead for async dispatch
- Tests cover all message types

#### Task 2.3: Boxing/Unboxing Enhancement
**File**: `rpyc/core/protocol.py`
**Estimate**: 2 days
**Dependencies**: Task 1.1

- [ ] Modify `_box()` for async functions
- [ ] Add coroutine boxing with warning
- [ ] Modify `_unbox()` for extended id_pack
- [ ] Tolerant decoding of 3-tuple id_pack
- [ ] Unit tests

**Acceptance Criteria:**
- Async functions boxed with FLAGS_ASYNC
- Legacy 3-tuple id_packs handled correctly
- Extended 4-tuple id_packs created correctly
- Warning emitted for coroutine boxing
- Tests cover both directions (legacy↔rpyc-async)

### Phase 3: AsyncResult & Netref (5-7 days)

#### Task 3.1: AsyncResult.__await__()
**File**: `rpyc/core/async_.py`
**Estimate**: 3 days
**Dependencies**: Task 2.2

- [ ] Implement `__await__()` method
- [ ] Modify `_set_result()` for callbacks
- [ ] Modify `_set_exception()` for callbacks
- [ ] Add `_awaiter_callbacks` tracking
- [ ] Integration tests

**Acceptance Criteria:**
- `await async_result` works correctly
- Fast path optimization (immediate return)
- Slow path uses Future
- Thread-safe callback invocation
- Memory cleanup (no leaks)
- Tests cover timeout, cancellation

#### Task 3.2: Netref Async Detection
**File**: `rpyc/core/netref.py`
**Estimate**: 2 days
**Dependencies**: Task 2.3, 3.1

- [ ] Add `____is_async__` slot
- [ ] Modify `__call__()` for handler selection
- [ ] Modify `_make_method()` for async methods
- [ ] Integration tests

**Acceptance Criteria:**
- Async functions detected correctly
- Correct handler selected (HANDLE_ASYNC_CALL)
- AsyncResult returned
- Can be awaited
- Tests cover netref chains

### Phase 4: Integration & Testing (7-10 days)

#### Task 4.1: End-to-End Integration Tests
**Estimate**: 4 days
**Dependencies**: All previous tasks

Test scenarios:
- [ ] Async exposed method (client await)
- [ ] Async callback (server await)
- [ ] Recursive async calls (depth 10)
- [ ] Mixed sync/async calls
- [ ] Exception propagation
- [ ] Concurrent calls (1000+)
- [ ] Legacy client ↔ rpyc-async server
- [ ] rpyc-async client ↔ legacy server
- [ ] Connection close during async call
- [ ] Event loop shutdown during call

**Acceptance Criteria:**
- All scenarios pass
- No deadlocks
- No memory leaks
- Proper cleanup on errors

#### Task 4.2: Performance Benchmarks
**Estimate**: 2 days
**Dependencies**: Task 4.1

Benchmarks:
- [ ] Sync call latency (baseline)
- [ ] Async call latency (overhead)
- [ ] Throughput (calls/sec)
- [ ] Memory usage (1000 concurrent calls)
- [ ] Event loop integration overhead

**Acceptance Criteria:**
- Async overhead <10% vs sync
- Throughput: 10,000+ calls/sec (localhost)
- Memory: <1MB for 1000 concurrent AsyncResults
- Benchmark results documented

#### Task 4.3: Documentation
**Estimate**: 3 days
**Dependencies**: Task 4.1

- [ ] API reference updates
- [ ] Tutorial: "Async RPyC Guide"
- [ ] Migration guide (classic synchronous RPyC → rpyc-async)
- [ ] Examples directory
- [ ] Changelog entry

**Deliverables:**
- `docs/async_guide.rst`
- `docs/migration_to_rpyc_async.rst`
- `examples/async_server.py`
- `examples/async_client.py`
- `examples/async_callbacks.py`

### Phase 5: Security & Hardening (3-5 days)

#### Task 5.1: Security Audit
**Estimate**: 2 days

- [ ] Review all async code paths for vulnerabilities
- [ ] Check for DoS vectors (resource exhaustion)
- [ ] Validate exception handling (info leaks)
- [ ] Review timeout behavior
- [ ] Thread safety audit

**Focus Areas:**
- Unbounded async call accumulation
- Exception object serialization (pickle)
- Event loop starvation
- Deadlock scenarios

#### Task 5.2: Error Handling Hardening
**Estimate**: 2 days

- [ ] Graceful degradation tests
- [ ] Timeout handling
- [ ] Partial failure scenarios
- [ ] Resource cleanup verification

**Acceptance Criteria:**
- No crashes on malformed async messages
- Proper cleanup on all error paths
- Clear error messages for users
- No resource leaks

---

## Testing Strategy

### Test Pyramid

```
         ┌─────────────────┐
         │   E2E Tests     │  10% (Integration)
         │   (20 tests)    │
         ├─────────────────┤
         │ Integration     │  30% (Cross-component)
         │ (60 tests)      │
         ├─────────────────┤
         │  Unit Tests     │  60% (Component)
         │  (120 tests)    │
         └─────────────────┘
```

### Test Categories

#### 1. Unit Tests (120 tests, ~60% coverage)

**Constants & Detection** (10 tests)
- Constant value correctness
- Async detection (functions, coroutines, generators)
- Edge cases (partial, lambda, classmethod)

**Async Handlers** (20 tests)
- `_handle_async_call()` with async function
- `_handle_async_call()` with coroutine
- `_handle_async_call()` with sync function
- Error propagation
- Kwargs handling

**Boxing/Unboxing** (30 tests)
- Box async function → 4-tuple id_pack
- Unbox 4-tuple → proxy with metadata
- Unbox 3-tuple → proxy without metadata (legacy peer)
- Box coroutine → warning emitted
- Round-trip tests

**AsyncResult** (30 tests)
- `__await__()` with ready result
- `__await__()` with ready exception
- `__await__()` with pending result
- Callback registration
- Thread-safety tests
- Memory leak tests

**Netref** (20 tests)
- Async function call → HANDLE_ASYNC_CALL
- Sync function call → HANDLE_CALL
- Metadata propagation
- Chained calls

**Protocol Helpers** (10 tests)
- `_is_async_handler()`
- `_needs_async_dispatch()`
- Message type routing

#### 2. Integration Tests (60 tests, ~30% coverage)

**Asyncio Integration** (15 tests)
- `enable_asyncio_serving()` success
- `enable_asyncio_serving()` outside loop → error
- `disable_asyncio_serving()` cleanup
- FD registration/unregistration
- Multiple connections in same loop

**Async Dispatch Pipeline** (20 tests)
- Async exposed method execution
- Sync exposed method (no regression)
- Async callback execution
- Exception propagation (async)
- Concurrent async calls (10, 100, 1000)
- Mixed sync/async calls

**Cross-Peer Interoperability** (15 tests)
- rpyc-async client → legacy server (graceful degradation)
- Legacy client → rpyc-async server (tolerant decoding)
- Protocol negotiation
- Protocol revision detection

**Error Handling** (10 tests)
- Connection close during async call
- Event loop shutdown during call
- Timeout handling
- Malformed async messages

#### 3. End-to-End Tests (20 tests, ~10% coverage)

**Real-World Scenarios** (10 tests)
- Async exposed method (simple)
- Async exposed method (I/O bound)
- Async callback (client → server → client)
- Recursive async calls (depth 10)
- Async generator streaming (future work)
- Async context manager (future work)

**Performance** (5 tests)
- Latency benchmark (async vs sync)
- Throughput benchmark (1000 calls)
- Memory usage (1000 concurrent)
- Event loop integration overhead

**Stress Tests** (5 tests)
- 10,000 concurrent async calls
- Long-running async calls (60s+)
- Rapid connect/disconnect with async calls
- Exception storm (100 failures/sec)

### Test Infrastructure

#### Fixtures

```python
# tests/conftest.py

import pytest
import asyncio
import rpyc
from threading import Thread

@pytest.fixture
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def async_service():
    """Sample async service for testing."""
    class TestService(rpyc.Service):
        async def exposed_async_add(self, a, b):
            await asyncio.sleep(0.01)  # Simulate I/O
            return a + b

        def exposed_sync_add(self, a, b):
            return a + b

        async def exposed_process_callback(self, callback, value):
            result = await callback(value)
            return result * 2

    return TestService

@pytest.fixture
def async_server(async_service):
    """Start async RPyC server in background thread."""
    from rpyc.utils.server import ThreadedServer

    server = ThreadedServer(
        async_service,
        port=0,  # Dynamic port
        protocol_config={"allow_all_attrs": True}
    )

    # Start server thread
    thread = Thread(target=server.start, daemon=True)
    thread.start()

    yield server

    server.close()
    thread.join(timeout=5)

@pytest.fixture
def async_connection(async_server, event_loop):
    """Create async-enabled connection to test server."""
    conn = rpyc.connect(
        "localhost",
        async_server.port,
        config={"sync_request_timeout": 10}
    )

    conn.enable_asyncio_serving(event_loop)

    yield conn

    conn.close()
```

#### Example Test

```python
# tests/integration/test_async_calls.py

import pytest
import asyncio

@pytest.mark.asyncio
async def test_async_exposed_method(async_connection):
    """Test calling async exposed method with await."""
    result = await async_connection.root.async_add(3, 4)
    assert result == 7

@pytest.mark.asyncio
async def test_async_callback(async_connection):
    """Test passing async callback to server."""

    # Define async callback
    async def my_callback(x):
        await asyncio.sleep(0.01)
        return x * 2

    # Server will await callback and double result
    result = await async_connection.root.process_callback(my_callback, 5)

    # Expected: my_callback(5) = 10, server doubles = 20
    assert result == 20

@pytest.mark.asyncio
async def test_concurrent_async_calls(async_connection):
    """Test 100 concurrent async calls."""
    tasks = [
        async_connection.root.async_add(i, i)
        for i in range(100)
    ]

    results = await asyncio.gather(*tasks)

    assert results == [i * 2 for i in range(100)]

def test_sync_call_no_regression(async_connection):
    """Ensure sync calls still work (no await)."""
    result = async_connection.root.sync_add(3, 4)
    assert result == 7  # Sync call
```

---

## Risks and Mitigation

### Risk Matrix

| Risk | Likelihood | Impact | Mitigation |
|------|-------------|---------|-----------|
| **Silent misbehaviour against legacy peers** | Medium | Critical | • Extensive interoperability tests<br>• Protocol negotiation<br>• Explicit errors instead of silent fallback |
| **Event loop integration bugs** | High | High | • Thorough testing with different loops<br>• Use established patterns (add_reader)<br>• Code review by asyncio experts |
| **Deadlock in async dispatch** | Medium | High | • Careful lock analysis<br>• Deadlock detection tests<br>• Timeout mechanisms |
| **Memory leaks (AsyncResult)** | Medium | Medium | • Memory profiling tests<br>• Weak references where appropriate<br>• Cleanup on close |
| **Performance regression** | Low | Medium | • Benchmark suite<br>• Zero-cost for sync paths<br>• Performance budgets |
| **Security vulnerabilities** | Low | Critical | • Security audit<br>• Fuzz testing<br>• Limited async call queue |
| **Documentation gaps** | High | Low | • Comprehensive docs plan<br>• Examples for all scenarios<br>• Migration guide |

### Detailed Mitigation Strategies

#### 1. Interoperability with Legacy Peers

**Risks:**
- Legacy clients break against an rpyc-async server
- Legacy servers reject rpyc-async clients
- Subtle behavioral changes go unnoticed

**Note**: rpyc-async does not promise compatibility with classic synchronous
RPyC. The goal here is *predictable failure* and best-effort degradation, not
a compatibility contract.

**Mitigation:**
- **Interoperability Test Matrix**: Test all 4 combinations (legacy/rpyc-async × client/server)
- **Protocol Negotiation**: Clients detect server capabilities before using async
- **Graceful Degradation**: rpyc-async clients fall back to sync against legacy servers
- **Beta Testing**: Release rpyc-async 1.0.0b1 for 4+ weeks before stable
- **Clear Errors**: Fail loudly when an async feature is unavailable on the peer

**Test Coverage:**
```python
# tests/interop/test_peers.py

def test_rpyc_async_client_legacy_server():
    """rpyc-async client should degrade against a legacy server (no async)."""
    # Mock classic synchronous RPyC server without async support
    # Client detects and uses sync mode

def test_legacy_client_rpyc_async_server():
    """Legacy client should still interoperate with an rpyc-async server."""
    # Use an actual upstream RPyC client
    # Server handles 3-tuple id_packs correctly
```

#### 2. Event Loop Integration

**Risks:**
- Conflicts with user's event loop
- Multiple event loops in same process
- Event loop closed during operation

**Mitigation:**
- **Explicit Activation**: `enable_asyncio_serving()` must be called explicitly
- **Loop Parameter**: Accept loop argument (don't assume `get_running_loop()`)
- **Cleanup Handlers**: Proper FD unregistration on close
- **Thread Safety**: Use `call_soon_threadsafe()` for cross-thread communication

**Best Practices:**
```python
# Recommended pattern
async def main():
    loop = asyncio.get_running_loop()

    conn = rpyc.connect("localhost", 18861)
    conn.enable_asyncio_serving(loop)  # Explicit loop

    try:
        result = await conn.root.method()
    finally:
        conn.close()  # Cleanup

asyncio.run(main())
```

#### 3. Deadlock Prevention

**Risks:**
- A→B→A recursive calls with sync wait
- Lock ordering issues
- Event loop blocking

**Mitigation:**
- **Async Recursion**: Use `await` for recursive calls (no blocking)
- **Lock Analysis**: Document lock ordering, use lock hierarchy
- **Timeout Mechanisms**: All async operations have timeouts
- **Deadlock Detection Tests**: Automated tests for known deadlock scenarios

**Lock Hierarchy:**
```
1. _async_dispatch_lock (lowest)
2. _local_objects_lock
3. _async_calls_lock (highest)

Rule: Never acquire lower lock while holding higher lock
```

#### 4. Memory Management

**Risks:**
- AsyncResult objects not garbage collected
- Event loop references prevent cleanup
- Growing async call registry

**Mitigation:**
- **Explicit Cleanup**: Remove AsyncResult from registry after completion
- **Weak References**: Use `weakref` for event loop callbacks
- **Memory Profiling**: Regular memory tests with 1000+ concurrent calls
- **Resource Limits**: Max concurrent async calls (configurable)

**Memory Test:**
```python
@pytest.mark.asyncio
async def test_no_memory_leak():
    """Ensure AsyncResult doesn't leak memory."""
    import gc
    import psutil

    process = psutil.Process()
    initial_memory = process.memory_info().rss

    # Create 1000 async calls
    tasks = [conn.root.method(i) for i in range(1000)]
    await asyncio.gather(*tasks)

    # Force GC
    gc.collect()

    final_memory = process.memory_info().rss
    growth = final_memory - initial_memory

    assert growth < 10 * 1024 * 1024  # <10MB growth
```

#### 5. Performance

**Risks:**
- Async overhead too high
- Event loop integration slows down sync calls
- Boxing/unboxing performance regression

**Mitigation:**
- **Zero-Cost Principle**: Sync calls don't pay for async infrastructure
- **Performance Budget**: Async overhead <10% of sync latency
- **Microbenchmarks**: Measure each component separately
- **Profiling**: Regular profiling with cProfile/py-spy

**Performance Budget:**

| Operation | Budget | Measured | Status |
|-----------|--------|----------|--------|
| Sync call (baseline) | 100μs | TBD | - |
| Async call (localhost) | <110μs | TBD | - |
| Boxing async function | <5μs | TBD | - |
| Async detection | <1μs | TBD | - |

---

## Deliverables Checklist

### Code

- [ ] `rpyc/core/consts.py` - New constants
- [ ] `rpyc/core/async_handlers.py` - Async handler implementations
- [ ] `rpyc/core/protocol.py` - Enhanced Connection class
- [ ] `rpyc/core/async_.py` - AsyncResult.__await__()
- [ ] `rpyc/core/netref.py` - Async proxy support
- [ ] `rpyc/utils/helpers.py` - Async detection utilities
- [ ] `rpyc/utils/asyncio_helpers.py` - Event loop helpers

### Tests

- [ ] `tests/unit/test_async_handlers.py` - Handler unit tests
- [ ] `tests/unit/test_async_detection.py` - Detection unit tests
- [ ] `tests/unit/test_boxing_async.py` - Boxing/unboxing tests
- [ ] `tests/unit/test_asyncresult.py` - AsyncResult tests
- [ ] `tests/integration/test_async_dispatch.py` - Dispatch tests
- [ ] `tests/integration/test_async_calls.py` - E2E async calls
- [ ] `tests/integration/test_async_callbacks.py` - Callback tests
- [ ] `tests/interop/test_peers.py` - Legacy peer interoperability
- [ ] `tests/performance/benchmark_async.py` - Performance benchmarks

### Documentation

- [ ] `docs/async_guide.rst` - Complete async guide
- [ ] `docs/migration_to_rpyc_async.rst` - Migration guide
- [ ] `docs/api/async_.rst` - API reference updates
- [ ] `examples/async_server.py` - Example async server
- [ ] `examples/async_client.py` - Example async client
- [ ] `examples/async_callbacks.py` - Example callbacks
- [ ] `examples/async_recursion.py` - Example recursion
- [ ] `CHANGELOG.md` - rpyc-async 1.0.0 entry
- [ ] `README.md` - Async feature mention

### Release

- [ ] Version bump to 1.0.0 (distribution: `rpyc-async`)
- [ ] Beta release (1.0.0b1)
- [ ] Beta testing period (4 weeks)
- [ ] Security audit
- [ ] Performance validation
- [ ] Stable release (1.0.0)

---

## Timeline

### Estimated Duration: 6-8 weeks

```
Week 1-2: Core Infrastructure (Phase 1)
├── Constants & protocol version
├── Async handlers module
└── Detection utilities

Week 3-4: Protocol Layer (Phase 2)
├── Asyncio integration
├── Async dispatch pipeline
└── Boxing/unboxing enhancement

Week 5: AsyncResult & Netref (Phase 3)
├── AsyncResult.__await__()
└── Netref async detection

Week 6-7: Integration & Testing (Phase 4)
├── E2E integration tests
├── Performance benchmarks
└── Documentation

Week 8: Security & Hardening (Phase 5)
├── Security audit
├── Error handling
└── Beta release preparation
```

### Milestones

| Milestone | Date | Deliverables |
|-----------|------|--------------|
| M1: Infrastructure Complete | Week 2 | Core async components working |
| M2: Protocol Enhancement | Week 4 | Async dispatch pipeline functional |
| M3: API Complete | Week 5 | AsyncResult.__await__() working |
| M4: Integration Tested | Week 7 | All tests passing, docs complete |
| M5: Beta Release | Week 8 | rpyc-async 1.0.0b1 published |
| M6: Stable Release | Week 12 | rpyc-async 1.0.0 stable after beta testing |

---

## Success Criteria

### Functional

- ✅ Async exposed methods can be called with `await`
- ✅ Async callbacks can be passed and executed
- ✅ Recursive async calls work (depth 10+)
- ✅ Legacy peers either interoperate or fail with a clear, documented error
- ✅ All 4 peer combinations behave as described in the interoperability matrix

### Performance

- ✅ Async overhead <10% vs sync baseline
- ✅ Throughput: 10,000+ async calls/sec (localhost)
- ✅ Memory: <1MB for 1000 concurrent AsyncResults
- ✅ No memory leaks (1M+ calls)

### Quality

- ✅ Test coverage: >90% for new code
- ✅ All tests passing (200+ tests)
- ✅ Zero critical security issues
- ✅ Documentation: 100% API coverage
- ✅ Examples for all major scenarios

### Adoption

- ✅ Beta testing: 10+ production deployments
- ✅ No critical bugs reported during beta (4 weeks)
- ✅ Community feedback: >80% positive
- ✅ Migration: <1 day for typical project

---

## Appendix

### A. Code Review Checklist

**For Each PR:**
- [ ] All tests passing
- [ ] Code coverage >90%
- [ ] No new security warnings
- [ ] Documentation updated
- [ ] Changelog entry added
- [ ] Performance budget met
- [ ] Legacy-peer interoperability verified
- [ ] Thread-safety reviewed
- [ ] Memory management reviewed

### B. Security Checklist

- [ ] No unbounded resource allocation
- [ ] Timeout on all async operations
- [ ] Exception objects properly sanitized
- [ ] No pickle of untrusted data (async context)
- [ ] Event loop DoS prevention
- [ ] Input validation for all async messages

### C. Performance Profiling Checklist

- [ ] cProfile baseline (sync calls)
- [ ] cProfile async calls
- [ ] Memory profiler (concurrent calls)
- [ ] py-spy flamegraph (async dispatch)
- [ ] Benchmark comparison (classic synchronous RPyC vs rpyc-async)

### D. References

- [PEP 492 - Coroutines with async/await](https://www.python.org/dev/peps/pep-0492/)
- [asyncio Event Loop Docs](https://docs.python.org/3/library/asyncio-eventloop.html)
- [Upstream RPyC Protocol Spec](https://rpyc.readthedocs.io/)
- [ASYNC_SUPPORT_PROPOSAL_V2.md](./ASYNC_SUPPORT_PROPOSAL_V2.md)
- [ASYNC_DISPATCH_PIPELINE_EXPLAINED.md](./ASYNC_DISPATCH_PIPELINE_EXPLAINED.md)

---

**Document Version**: 1.0
**Last Updated**: 2026-03-22
**Author**: Design Team
**Status**: Ready for Implementation
