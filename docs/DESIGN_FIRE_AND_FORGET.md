# Design Document: fire_and_forget() Utilities for rpyc-async

**Document Version:** 1.1
**Target:** `rpyc-async` 1.0.0 (distribution `rpyc-async`, import name `rpyc`)
**Date:** 2026-04-06
**Status:** Implementation Phase

---

## 1. Overview

This document describes the design and implementation of `fire_and_forget()` and `fire_and_forget_async()` utilities for the rpyc_async project. These utilities enable non-blocking execution of cross-process async RPC calls with proper timeout handling and error callbacks, even when the remote connection is hung or dead.

**CRITICAL DESIGN PRINCIPLE:** All functionality MUST operate within the main event loop of the application. No additional event loops or threads are permitted. This constraint is non-negotiable - creating additional event loops or threads has historically led to deadlocks and difficult-to-trace errors in async RPC scenarios.

---

## 2. Requirements

### 2.1 Functional Requirements

1. **Synchronous Wrapper (`fire_and_forget()`):**
   - Accept a coroutine/awaitable as worker parameter
   - Support both local coroutines and cross-process async RPC calls (netref async functions)
   - Support optional timeout parameter
   - Support optional success callback (synchronous)
   - Support optional error callback (synchronous)
   - Return immediately (non-blocking)
   - Return `asyncio.Task` for optional tracking

2. **Asynchronous Wrapper (`fire_and_forget_async()`):**
   - Accept a coroutine/awaitable as worker parameter
   - Support both local coroutines and cross-process async RPC calls (netref async functions)
   - Support optional timeout parameter
   - Support optional async success callback
   - Support optional async error callback
   - Return immediately (non-blocking)
   - Return `asyncio.Task` for optional tracking

3. **Cross-Process RPC Support:**
   - Must work with netref async functions (proxied functions from remote process)
   - Must detect and handle cross-process call failures
   - Must not block on hung remote connections

4. **Hung Connection Handling:**
   - **Critical:** Must complete instantly even if remote process is hung/dead
   - Must trigger error callback on timeout
   - Must not leave dangling resources
   - **REQUIRES AsyncioServer** - ThreadedServer is not supported for async functionality

### 2.2 Non-Functional Requirements

1. **Performance:**
   - No polling (event-driven only)
   - Minimal overhead for local operations
   - Support for many concurrent fire-and-forget calls

2. **Reliability:**
   - No silent failures
   - Proper exception propagation to error callbacks
   - No memory leaks from uncollected tasks

3. **Event Loop Requirements:**
   - **MUST use the main application event loop only**
   - **MUST NOT create additional event loops**
   - **MUST NOT spawn threads for async operations**
   - All operations via `asyncio.create_task()` in the running loop
   - Uses `asyncio.get_running_loop()` to ensure loop exists

4. **Server Requirements:**
   - **AsyncioServer is REQUIRED** for cross-process async RPC
   - ThreadedServer MUST NOT be used for async functionality
   - Documentation must clearly state this requirement
   - Consider adding runtime warnings if ThreadedServer is detected

5. **Testing:**
   - Real multiprocessing tests (not mocks)
   - Tests with intentionally hung connections
   - Tests with killed remote processes
   - Tests with network failures
   - All tests must use AsyncioServer exclusively

---

## 3. Architecture

### 3.1 Core Components

```
fire_and_forget() / fire_and_forget_async()
    ↓
run_with_callbacks() / run_with_async_callbacks()
    ↓
asyncio.wait_for() (timeout wrapper)
    ↓
worker coroutine (local or cross-process RPC)
    ↓
success_callback / error_callback (on completion)
```

### 3.2 Integration Points

1. **rpyc.core.protocol.Connection:**
   - Uses existing `_asyncio_enabled` flag
   - Uses existing `enable_asyncio_serving()` mechanism
   - Uses existing `AsyncResult` awaitable infrastructure

2. **rpyc.core.netref.BaseNetref:**
   - Uses existing async call detection (`____is_async__`)
   - Uses existing `asyncreq()` for cross-process calls

3. **rpyc.utils.async_server.AsyncioServer:**
   - **REQUIRED** server for all async functionality
   - Provides persistent event loop (the main application event loop)
   - Handles bidirectional async calls
   - ThreadedServer creates temporary event loops per request, causing deadlocks - DO NOT USE

4. **Main Event Loop:**
   - **MUST use `asyncio.get_running_loop()`** - ensures we're in the main loop
   - **MUST NOT create new loops** via `asyncio.new_event_loop()` or `asyncio.run()`
   - **MUST NOT spawn threads** for async operations
   - Uses `asyncio.create_task()` to schedule tasks in the current running loop
   - Uses `asyncio.wait_for()` for timeout enforcement within the same loop

---

## 4. Detailed Design

### 4.1 API Specification

#### 4.1.1 Synchronous API

```python
def fire_and_forget(
    awaitable: Awaitable[T],
    *,
    timeout: float | None = None,
    success_callback: Callable[[T], None] | None = None,
    error_callback: Callable[[BaseException], None] | None = None,
    name: str | None = None,
) -> asyncio.Task[None]:
    """
    Execute an awaitable in the background without blocking.

    Supports both local coroutines and cross-process async RPC calls.

    Args:
        awaitable: Coroutine or AsyncResult to execute
        timeout: Optional timeout in seconds
        success_callback: Synchronous callback called with result on success
        error_callback: Synchronous callback called with exception on failure
        name: Optional task name for debugging

    Returns:
        asyncio.Task that can be optionally awaited or cancelled

    Raises:
        RuntimeError: If no event loop is running

    Requirements:
        - Must be called from within a running event loop (async context)
        - For cross-process RPC: connection must use AsyncioServer
        - For cross-process RPC: conn.enable_asyncio_serving() must be called

    Example:
        >>> # Local coroutine
        >>> task = fire_and_forget(
        ...     my_async_func(arg1, arg2),
        ...     timeout=5.0,
        ...     success_callback=lambda result: print(f"Got {result}"),
        ...     error_callback=lambda exc: print(f"Error: {exc}"),
        ... )

        >>> # Cross-process RPC call (requires AsyncioServer)
        >>> conn = await async_connect("localhost", 18861)
        >>> # async_connect automatically enables asyncio serving
        >>> task = fire_and_forget(
        ...     conn.root.remote_async_func(arg1),
        ...     timeout=10.0,
        ...     error_callback=handle_rpc_error,
        ... )
    """
```

#### 4.1.2 Asynchronous API

```python
def fire_and_forget_async(
    awaitable: Awaitable[T],
    *,
    timeout: float | None = None,
    success_callback: Callable[[T], Awaitable[None]] | None = None,
    error_callback: Callable[[BaseException], Awaitable[None]] | None = None,
    name: str | None = None,
) -> asyncio.Task[None]:
    """
    Execute an awaitable in the background with async callbacks.

    Supports both local coroutines and cross-process async RPC calls.

    Args:
        awaitable: Coroutine or AsyncResult to execute
        timeout: Optional timeout in seconds
        success_callback: Async callback called with result on success
        error_callback: Async callback called with exception on failure
        name: Optional task name for debugging

    Returns:
        asyncio.Task that can be optionally awaited or cancelled

    Raises:
        RuntimeError: If no event loop is running

    Requirements:
        - Must be called from within a running event loop (async context)
        - For cross-process RPC: connection must use AsyncioServer
        - For cross-process RPC: conn.enable_asyncio_serving() must be called

    Example:
        >>> async def on_success(result):
        ...     await log_result(result)
        ...
        >>> # Requires AsyncioServer on the other side
        >>> task = fire_and_forget_async(
        ...     conn.root.long_running_task(),
        ...     timeout=30.0,
        ...     success_callback=on_success,
        ...     error_callback=log_error,
        ... )
    """
```

### 4.2 Implementation Strategy

#### 4.2.1 Core Wrapper Functions

```python
async def run_with_callbacks(
    awaitable: Awaitable[T],
    *,
    timeout: float | None = None,
    success_callback: Callable[[T], None] | None = None,
    error_callback: Callable[[BaseException], None] | None = None,
) -> None:
    """Internal: Execute awaitable with timeout and sync callbacks."""
    try:
        if timeout is not None:
            result = await asyncio.wait_for(awaitable, timeout=timeout)
        else:
            result = await awaitable

        if success_callback is not None:
            success_callback(result)

    except asyncio.CancelledError:
        # Allow task cancellation to propagate
        raise
    except Exception as exc:
        if error_callback is not None:
            error_callback(exc)
        else:
            # Log unhandled error to prevent silent failure
            import sys
            print(f"ERROR: Unhandled exception in fire_and_forget: {exc}",
                  file=sys.stderr)


async def run_with_async_callbacks(
    awaitable: Awaitable[T],
    *,
    timeout: float | None = None,
    success_callback: Callable[[T], Awaitable[None]] | None = None,
    error_callback: Callable[[BaseException], Awaitable[None]] | None = None,
) -> None:
    """Internal: Execute awaitable with timeout and async callbacks."""
    try:
        if timeout is not None:
            result = await asyncio.wait_for(awaitable, timeout=timeout)
        else:
            result = await awaitable

        if success_callback is not None:
            await success_callback(result)

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        if error_callback is not None:
            await error_callback(exc)
        else:
            import sys
            print(f"ERROR: Unhandled exception in fire_and_forget_async: {exc}",
                  file=sys.stderr)
```

#### 4.2.2 Public API Functions

```python
def fire_and_forget(
    awaitable: Awaitable[T],
    *,
    timeout: float | None = None,
    success_callback: Callable[[T], None] | None = None,
    error_callback: Callable[[BaseException], None] | None = None,
    name: str | None = None,
) -> asyncio.Task[None]:
    """Create background task with sync callbacks.

    CRITICAL: This function uses asyncio.get_running_loop() to ensure
    we are operating in the main event loop. It will raise RuntimeError
    if no event loop is running. Never creates additional loops or threads.
    """
    # Verify we're in a running event loop (will raise if not)
    loop = asyncio.get_running_loop()

    # Create task in the current running loop
    task = loop.create_task(
        run_with_callbacks(
            awaitable,
            timeout=timeout,
            success_callback=success_callback,
            error_callback=error_callback,
        ),
        name=name,
    )
    return task


def fire_and_forget_async(
    awaitable: Awaitable[T],
    *,
    timeout: float | None = None,
    success_callback: Callable[[T], Awaitable[None]] | None = None,
    error_callback: Callable[[BaseException], Awaitable[None]] | None = None,
    name: str | None = None,
) -> asyncio.Task[None]:
    """Create background task with async callbacks.

    CRITICAL: This function uses asyncio.get_running_loop() to ensure
    we are operating in the main event loop. It will raise RuntimeError
    if no event loop is running. Never creates additional loops or threads.
    """
    # Verify we're in a running event loop (will raise if not)
    loop = asyncio.get_running_loop()

    # Create task in the current running loop
    task = loop.create_task(
        run_with_async_callbacks(
            awaitable,
            timeout=timeout,
            success_callback=success_callback,
            error_callback=error_callback,
        ),
        name=name,
    )
    return task
```

### 4.3 Location in Codebase

**Proposed location:** `rpyc/utils/helpers.py`

This module already contains utility functions and is a natural fit for fire-and-forget utilities.

Alternative: Create new module `rpyc/utils/fire_and_forget.py` if helpers.py becomes too large.

---

## 5. Cross-Process RPC Considerations

### 5.1 How Cross-Process Calls Work

When calling a remote async function:

```python
# Client side
conn = await async_connect("localhost", 18861)
result = await conn.root.remote_async_func(arg)
```

Under the hood:
1. `conn.root.remote_async_func` returns a `BaseNetref` proxy with `____is_async__ = True`
2. Calling the netref invokes `asyncreq()` which returns an `AsyncResult`
3. `AsyncResult.__await__()` creates a Future and registers it with the event loop
4. The RPC request is sent via `MSG_REQUEST` / `MSG_ASYNC_REQUEST`
5. The event loop's `on_readable()` callback processes the reply when it arrives
6. The Future is resolved, completing the `await`

### 5.2 Timeout Behavior with Cross-Process Calls

**Critical requirement:** Timeout must trigger instantly, even if remote is hung.

**How `asyncio.wait_for()` achieves this:**

```python
# In run_with_callbacks()
result = await asyncio.wait_for(awaitable, timeout=5.0)
```

Internally, `wait_for()`:
1. Schedules a timeout callback in the event loop
2. Starts waiting for the awaitable
3. **When timeout fires:** Cancels the awaitable and raises `asyncio.TimeoutError`
4. This happens **locally** - no need to wait for remote response

**Key insight:** The timeout is enforced by the **local** event loop, not the remote process. Therefore:
- If remote is hung, timeout fires locally after specified duration
- The RPC call's `AsyncResult` gets cancelled
- Error callback is invoked with `asyncio.TimeoutError`
- No blocking occurs

### 5.3 Connection State After Timeout

When a cross-process call times out:

1. **The underlying connection may still be alive:**
   - The remote process might still be working on the request
   - Eventually a reply may arrive (too late)

2. **Connection cleanup:**
   - rpyc's event loop (`on_readable()`) will handle late replies
   - They are dispatched but the `AsyncResult` is already cancelled
   - No memory leak occurs (callbacks already removed)

3. **Detection of truly dead connections:**
   - If the socket is closed/broken, `on_readable()` gets `EOFError`
   - Connection auto-closes via `self.close()` in exception handler
   - This is independent of fire-and-forget timeout

### 5.4 Hung Connection Test Strategy

**Test scenario 1: Remote process hangs indefinitely**

```python
# Server side
async def exposed_hang_forever(self):
    """Simulate hung process."""
    await asyncio.sleep(999999)  # Never returns

# Client side test
async def test_hung_connection():
    conn = await async_connect("localhost", 18861)

    error_fired = asyncio.Event()
    timeout_exc = None

    def on_error(exc):
        nonlocal timeout_exc
        timeout_exc = exc
        error_fired.set()

    # This MUST complete in ~1 second, not hang
    task = fire_and_forget(
        conn.root.hang_forever(),
        timeout=1.0,
        error_callback=on_error,
    )

    # Wait for error callback
    await asyncio.wait_for(error_fired.wait(), timeout=2.0)

    assert isinstance(timeout_exc, asyncio.TimeoutError)
    assert task.done()
```

**Test scenario 2: Remote process killed during call**

```python
async def test_killed_process():
    # Start server in subprocess
    server_proc = multiprocessing.Process(target=run_server)
    server_proc.start()

    await asyncio.sleep(0.5)  # Let server start

    conn = await async_connect("localhost", 18861)

    # Start long-running call
    task = fire_and_forget(
        conn.root.long_task(),
        timeout=10.0,
        error_callback=handle_error,
    )

    await asyncio.sleep(0.1)  # Let call start

    # Kill server
    server_proc.kill()
    server_proc.join()

    # Task should complete quickly with error, not hang
    await asyncio.wait_for(task, timeout=2.0)
```

**Test scenario 3: Network partition / socket freeze**

This is harder to test directly. Options:
1. Use `iptables` to drop packets (requires root)
2. Use socket-level mocking
3. Use process suspension (`SIGSTOP`) to simulate freeze

```python
async def test_frozen_connection():
    server_proc = multiprocessing.Process(target=run_server)
    server_proc.start()

    conn = await async_connect("localhost", 18861)

    # Freeze server process
    os.kill(server_proc.pid, signal.SIGSTOP)

    error_fired = False
    def on_error(exc):
        nonlocal error_fired
        error_fired = True

    # Should timeout, not hang
    task = fire_and_forget(
        conn.root.some_func(),
        timeout=1.0,
        error_callback=on_error,
    )

    await asyncio.wait_for(task, timeout=2.0)
    assert error_fired

    # Cleanup
    os.kill(server_proc.pid, signal.SIGCONT)
    server_proc.kill()
```

---

## 6. Error Handling

### 6.1 Error Categories

1. **Timeout errors:**
   - `asyncio.TimeoutError` from `wait_for()`
   - Passed to `error_callback`

2. **Connection errors:**
   - `EOFError` - connection closed
   - `ConnectionError` - network failure
   - Passed to `error_callback`

3. **Remote exceptions:**
   - Exception raised by remote function
   - Propagated via `MSG_EXCEPTION` / `MSG_ASYNC_EXCEPTION`
   - Passed to `error_callback`

4. **Local exceptions:**
   - Exception in callback itself
   - Logged to stderr (cannot be caught by user)

### 6.2 Cancellation Handling

`asyncio.CancelledError` is treated specially:
- **Not** passed to `error_callback`
- Propagated immediately
- Allows graceful task cancellation

```python
task = fire_and_forget(...)
await asyncio.sleep(1)
task.cancel()  # Clean cancellation
```

### 6.3 Unhandled Errors

If no `error_callback` is provided:
- Error is logged to stderr
- Task completes with exception set
- Does **not** crash the event loop

This follows asyncio best practices for background tasks.

---

## 7. Testing Strategy

### 7.1 Unit Tests

**File:** `tests/test_fire_and_forget.py`

1. **Basic functionality:**
   - Test with simple async function
   - Test with timeout (success case)
   - Test with timeout (timeout case)
   - Test with exception in worker
   - Test success callback invocation
   - Test error callback invocation

2. **Task management:**
   - Test task return value
   - Test task cancellation
   - Test multiple concurrent tasks

3. **Edge cases:**
   - Test with no callbacks
   - Test with None timeout
   - Test with zero timeout
   - Test with negative timeout (should fail)

### 7.2 Integration Tests (Cross-Process)

**File:** `tests/test_fire_and_forget_rpc.py`

These tests MUST use real multiprocessing, not mocks.

1. **Basic RPC:**
   - Test fire_and_forget with remote async function
   - Test fire_and_forget_async with remote async function
   - Test callback receives correct result

2. **Hung connection:**
   - Test with remote function that never returns
   - Verify timeout fires within expected time
   - Verify error callback receives TimeoutError
   - **Critical:** Measure actual elapsed time, must be ~timeout duration

3. **Killed process:**
   - Test with process killed during call
   - Verify error callback fires
   - Verify no hanging or resource leak

4. **Bidirectional async:**
   - Client calls server with async callback
   - Server calls client's async callback
   - Use fire_and_forget for the top-level call
   - Verify everything works with AsyncioServer

5. **Heavy load:**
   - Fire 100+ concurrent calls
   - Mix of fast and slow calls
   - Mix of successes and timeouts
   - Verify all callbacks fire correctly

### 7.3 Performance Tests

1. **Overhead measurement:**
   - Measure time for 1000 fire_and_forget calls with instant workers
   - Should be minimal (microseconds per call)

2. **Memory usage:**
   - Fire 1000 tasks
   - Measure memory before and after completion
   - Check for leaks

3. **Timeout accuracy:**
   - Fire tasks with 1.0s timeout
   - Measure actual time to error callback
   - Should be within ±50ms

### 7.4 Test Utilities

```python
# Test helper: Server with controllable behavior
class TestService(rpyc.Service):
    async def exposed_fast(self, value):
        """Returns immediately."""
        return value * 2

    async def exposed_slow(self, value, delay):
        """Returns after delay."""
        await asyncio.sleep(delay)
        return value * 2

    async def exposed_hang(self):
        """Never returns."""
        await asyncio.sleep(999999)

    async def exposed_error(self):
        """Raises exception."""
        raise ValueError("Intentional error")

    async def exposed_with_callback(self, callback, value):
        """Calls callback (test bidirectional)."""
        result = await callback(value)
        return result * 2

# Test helper: Track callback invocations
class CallbackTracker:
    def __init__(self):
        self.success_results = []
        self.error_results = []
        self.success_event = asyncio.Event()
        self.error_event = asyncio.Event()

    def on_success(self, result):
        self.success_results.append(result)
        self.success_event.set()

    def on_error(self, exc):
        self.error_results.append(exc)
        self.error_event.set()

    async def wait_success(self, timeout=5.0):
        await asyncio.wait_for(self.success_event.wait(), timeout)

    async def wait_error(self, timeout=5.0):
        await asyncio.wait_for(self.error_event.wait(), timeout)
```

---

## 8. Documentation

### 8.1 API Documentation

- Docstrings for all public functions (already in design)
- Type hints for all parameters and return values
- Examples in docstrings
- Link to this design document

### 8.2 User Guide

Create `docs/guide/fire_and_forget.md` covering:

1. **Introduction:**
   - What is fire-and-forget
   - When to use it
   - When NOT to use it

2. **Basic usage:**
   - Simple example with local async function
   - Example with timeout
   - Example with callbacks

3. **Cross-process RPC:**
   - Example with remote async function
   - Handling timeouts with hung connections
   - Bidirectional async calls

4. **Best practices:**
   - Always provide error_callback
   - Set reasonable timeouts for RPC calls
   - Task management strategies
   - Avoiding memory leaks

5. **Troubleshooting:**
   - "RuntimeError: no running event loop"
   - Callbacks not firing
   - Timeouts not working as expected

### 8.3 Changelog Entry

Add to `CHANGELOG.md`:

```markdown
## [Version X.X.X] - YYYY-MM-DD

### Added
- `fire_and_forget()` utility for non-blocking execution with sync callbacks
- `fire_and_forget_async()` utility for non-blocking execution with async callbacks
- Support for cross-process async RPC calls in fire-and-forget mode
- Proper timeout handling for hung/dead connections
```

---

## 9. Implementation Plan

### Phase 1: Core Implementation
1. Implement `run_with_callbacks()` in `rpyc/utils/helpers.py`
2. Implement `run_with_async_callbacks()` in `rpyc/utils/helpers.py`
3. Implement `fire_and_forget()` in `rpyc/utils/helpers.py`
4. Implement `fire_and_forget_async()` in `rpyc/utils/helpers.py`
5. Add type hints and docstrings
6. Export from `rpyc/utils/__init__.py`

### Phase 2: Unit Tests
1. Create `tests/test_fire_and_forget.py`
2. Implement basic functionality tests
3. Implement task management tests
4. Implement edge case tests
5. Ensure 100% code coverage for new functions

### Phase 3: Integration Tests
1. Create `tests/test_fire_and_forget_rpc.py`
2. Implement TestService with controllable behaviors
3. Implement basic RPC tests
4. **Critical:** Implement hung connection test
5. Implement killed process test
6. Implement bidirectional async test
7. Implement heavy load test

### Phase 4: Performance Tests
1. Add performance test suite
2. Measure overhead
3. Measure memory usage
4. Measure timeout accuracy
5. Document performance characteristics

### Phase 5: Documentation
1. Write user guide
2. Add examples to docs
3. Update README if needed
4. Add changelog entry

### Phase 6: Code Review & Refinement
1. Review for edge cases
2. Review error handling
3. Review memory management
4. Review compatibility with existing code
5. Final testing on all supported platforms

---

## 10. Potential Issues & Mitigations

### Issue 1: Event loop not running

**Problem:** User calls `fire_and_forget()` when no event loop exists.

**Mitigation:**
- Check for running loop with `asyncio.get_running_loop()`
- Raise clear error message if not found
- Document requirement in docstring

```python
def fire_and_forget(...):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        raise RuntimeError(
            "fire_and_forget() requires a running event loop. "
            "Call from async function or use asyncio.run()."
        )
```

### Issue 2: Task garbage collection

**Problem:** Background tasks may be garbage collected if not referenced.

**Resolution (implemented):** the "optional enhancement" below was adopted, so
the problem does not reach the caller. Both helpers register every task they
create in a module-level `_INFLIGHT` set before returning it, and discard it
from that set via `add_done_callback` once it settles. The task is therefore
strongly referenced for exactly as long as it is running.

Consequently the caller **must not** be told to hold the returned task in order
to keep it alive — that advice was written against the pre-mitigation design and
is obsolete. Hold the task only to `await` or `cancel()` it. See
`rpyc/utils/helpers.py` (`_INFLIGHT`) and `guide_fire_and_forget.md`.

```python
# Adopted; see rpyc/utils/helpers.py
_INFLIGHT: set[asyncio.Task] = set()

def fire_and_forget(...):
    task = asyncio.create_task(...)
    _INFLIGHT.add(task)
    task.add_done_callback(_INFLIGHT.discard)
    return task
```

### Issue 3: Exception in callback

**Problem:** User's callback raises exception.

**Mitigation:**
- Wrap callback invocation in try/except
- Log to stderr (cannot propagate to user)
- Document that callbacks should be exception-safe

```python
if success_callback is not None:
    try:
        success_callback(result)
    except Exception as exc:
        print(f"ERROR: Exception in success_callback: {exc}",
              file=sys.stderr)
```

### Issue 4: Connection closed before timeout

**Problem:** Connection closes mid-call due to network error.

**Mitigation:**
- This is already handled by rpyc's `on_readable()` callback
- `EOFError` is raised, caught by wrapper, passed to `error_callback`
- No special handling needed

### Issue 5: AsyncioServer Requirement (CRITICAL)

**Statement:** AsyncioServer is REQUIRED. ThreadedServer MUST NOT be used for async functionality.

**Rationale:**
- ThreadedServer creates temporary event loops per request via `asyncio.run()`
- This causes deadlocks when trying to await cross-process callbacks
- Multiple event loops lead to race conditions and hard-to-debug errors
- Historical experience in this project has shown this is a critical constraint

**Mitigation:**
- Documentation MUST clearly state AsyncioServer requirement
- All examples MUST use AsyncioServer only
- Tests MUST use AsyncioServer exclusively (no ThreadedServer tests)
- Consider adding runtime detection and clear error messages

```python
# Runtime detection (optional enhancement)
def _check_asyncio_enabled(awaitable):
    """Check if awaitable is AsyncResult with asyncio enabled."""
    from rpyc_async.core.async_ import AsyncResult
    if isinstance(awaitable, AsyncResult):
        if not awaitable._conn._asyncio_enabled:
            raise RuntimeError(
                "Cross-process async RPC requires AsyncioServer with asyncio serving enabled. "
                "ThreadedServer is NOT supported for async functionality. "
                "Use async_connect() or call conn.enable_asyncio_serving()."
            )
```

### Issue 6: Late replies causing issues

**Problem:** Timeout fires, but reply arrives later.

**Mitigation:**
- rpyc already handles this correctly
- `AsyncResult` is removed from callbacks
- Late reply is ignored
- No action needed

---

## 11. Compatibility

### 11.1 Python Versions

- Requires Python 3.10+ (minimum supported version; `float | None` syntax is used directly)

**Recommendation:** `from __future__ import annotations` may still be used for consistency, but it is not required on 3.10+.

### 11.2 rpyc-async Versions

- Requires `rpyc-async` 1.0.0+ (asyncio-native; import name remains `rpyc`)
- Uses existing `AsyncResult`, `enable_asyncio_serving()`, etc.
- Compatibility with classic synchronous RPyC (upstream RPyC) is not guaranteed

### 11.3 Asyncio Backend

- Works with standard asyncio
- Should work with uvloop (untested)
- Should work with other event loop implementations (untested)

**Recommendation:** Test with uvloop in CI.

---

## 12. Future Enhancements

### 12.1 Priority Queue Support

Allow specifying priority for fire-and-forget tasks:

```python
fire_and_forget(..., priority=10)
```

### 12.2 Rate Limiting

Limit concurrent fire-and-forget tasks:

```python
limiter = FireAndForgetLimiter(max_concurrent=10)
limiter.fire_and_forget(...)
```

### 12.3 Retry Logic

Automatic retry on failure:

```python
fire_and_forget(
    ...,
    retry_count=3,
    retry_delay=1.0,
)
```

### 12.4 Progress Callbacks

Callback for progress updates:

```python
fire_and_forget(
    ...,
    progress_callback=lambda pct: print(f"{pct}% done"),
)
```

### 12.5 Result Collection

Convenience function to collect results from multiple tasks:

```python
tasks = [fire_and_forget(...) for _ in range(10)]
results = await gather_fire_and_forget(tasks)
```

---

## 13. Security Considerations

### 13.1 Denial of Service

**Risk:** User creates unlimited fire-and-forget tasks, exhausting memory.

**Mitigation:** Document best practices, optionally provide rate limiting.

### 13.2 Exception Information Leakage

**Risk:** Error callbacks might log sensitive information from exceptions.

**Mitigation:** Document that callbacks should sanitize exceptions before logging.

### 13.3 Remote Code Execution

**Risk:** Accepting arbitrary callables from remote process.

**Mitigation:**
- Not a concern for fire_and_forget itself
- rpyc's existing security model applies
- User must trust remote process

---

## 14. Metrics & Observability

### 14.1 Logging

Add optional logging:

```python
import logging
logger = logging.getLogger("rpyc.fire_and_forget")

async def run_with_callbacks(...):
    logger.debug(f"Starting fire-and-forget task: {name}")
    try:
        ...
        logger.debug(f"Task completed successfully: {name}")
    except Exception as exc:
        logger.warning(f"Task failed: {name}: {exc}")
```

### 14.2 Metrics

Consider adding metrics:
- Total tasks created
- Tasks completed successfully
- Tasks failed
- Tasks timed out
- Average execution time

Implementation using Prometheus or similar is out of scope for initial version.

---

## 15. Conclusion

This design provides a robust, production-ready fire-and-forget implementation for rpyc_async that:

1. ✅ Supports both sync and async callbacks
2. ✅ Works with cross-process async RPC calls
3. ✅ Handles hung/dead connections correctly via event-loop timeouts
4. ✅ Follows asyncio best practices
5. ✅ **Operates exclusively in the main event loop (no additional loops/threads)**
6. ✅ **Requires AsyncioServer (ThreadedServer not supported)**
7. ✅ Includes comprehensive testing strategy
8. ✅ Provides clear documentation
9. ✅ Integrates cleanly with existing rpyc architecture

### Key Design Principles

1. **Single Event Loop:** All operations occur in the main application event loop via `asyncio.get_running_loop()`. No additional event loops or threads are created. This prevents deadlocks and race conditions.

2. **AsyncioServer Only:** ThreadedServer creates temporary event loops that cause deadlocks. Only AsyncioServer with persistent event loops is supported.

3. **Local Timeout Enforcement:** `asyncio.wait_for()` provides **local** timeout enforcement, which means hung remote connections cannot block the client. This combined with rpyc's event-driven I/O model ensures that fire-and-forget calls complete instantly even in failure scenarios.

Implementation can proceed with confidence that this design addresses all requirements while maintaining the critical constraint of single event loop operation.

---

## 16. References

1. Python asyncio documentation: https://docs.python.org/3/library/asyncio-task.html
2. Protocol implementation: `rpyc/core/protocol.py`
3. AsyncioServer implementation: `rpyc/utils/async_server.py`
4. Fire-and-forget pattern reference notes (internal design notes)

---

**Document Path:** `docs/DESIGN_FIRE_AND_FORGET.md`
