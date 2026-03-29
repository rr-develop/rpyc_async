# RPyC Refcount Error Reproduction Test

## Overview

This test suite (`test_refcount_errors_reproduction.py`) is designed to detect reference counting errors in RPyC's garbage collection mechanism by creating intensive, concurrent operations that stress the netref lifecycle management system.

## What Problems Does This Test Detect?

### Primary Errors Targeted

1. **`[REFCOUNT] DECREF on missing key`** - Attempting to decrement reference count for an object that's already been removed from the registry
2. **`Failed to delete remote object`** - Cleanup acknowledgment failures during netref deletion
3. **Memory leaks** - Objects remaining in registries after they should have been cleaned up
4. **Race conditions** - Concurrent netref creation/deletion causing inconsistent state

### Root Causes

These errors typically occur due to:
- Race conditions between netref creation and cleanup
- Weak reference lifecycle timing issues
- Background cleanup task running during active RPC operations
- Bidirectional object passing creating circular dependencies
- Premature deletion of objects still referenced remotely

## Test Architecture

### Server Process (`RefcountTestService`)

Exposes methods designed to trigger refcount race conditions:

- **`exposed_rapid_object_creation(count)`** - Creates many temporary objects rapidly
- **`exposed_nested_callback_chain(callback, depth)`** - Deep callback chains with nested netrefs
- **`exposed_rapid_store_release(key_prefix, iterations)`** - Rapid store/release cycles
- **`exposed_callback_burst(callback, burst_size)`** - Concurrent callback execution
- **`exposed_create_and_pass_back(callback, iterations)`** - Bidirectional object passing
- **`exposed_get_registry_stats()`** - Reports internal registry state

### Test Cases

#### 1. `test_rapid_object_creation_triggers_refcount_errors`

**Purpose:** Tests netref creation/deletion under high volume

**Operation:**
- Creates 100 objects rapidly on server
- Returns them to client (creates 100 netrefs)
- Immediately deletes all objects
- Forces garbage collection
- Waits for cleanup cycles

**Expected Issues:**
- Pending deletions queue up faster than cleanup can process
- Potential race between new netref creation and cleanup of old ones

#### 2. `test_nested_callbacks_trigger_refcount_errors`

**Purpose:** Tests cleanup of nested proxy references

**Operation:**
- Creates deep callback chain (depth=10)
- Each level creates new objects passed to client
- Client processes and returns new objects
- Tests nested netref cleanup

**Expected Issues:**
- Complex object graphs may have cleanup ordering issues
- Nested callbacks create bidirectional netref traffic

#### 3. `test_rapid_store_release_cycle`

**Purpose:** Tests race between storage and cleanup

**Operation:**
- Rapidly stores objects (50 iterations)
- Small delay to allow netref creation
- Immediate deletion
- Tests very fast object lifecycle

**Expected Issues:**
- Object deleted before netref fully established
- Cleanup triggered before registration complete

#### 4. `test_concurrent_callback_burst`

**Purpose:** Tests concurrent netref creation/deletion

**Operation:**
- Fires 30 concurrent callbacks
- All callbacks process and return objects simultaneously
- Small delays increase race condition likelihood

**Expected Issues:**
- Concurrent operations on shared netref cache
- Multiple cleanup requests for same object

#### 5. `test_bidirectional_object_passing_stress`

**Purpose:** Tests heavy netref traffic in both directions

**Operation:**
- 100 iterations of bidirectional object passing
- Server creates object → sends to client
- Client processes → returns new object
- Both objects immediately discarded
- Aggressive GC every 10 iterations

**Expected Issues:**
- Heavy traffic in both `_local_objects` registries
- Many pending deletions on both sides
- Potential cleanup desynchronization

#### 6. `test_extreme_concurrent_operations_with_forced_cleanup`

**Purpose:** Maximum chaos test - multiple concurrent operations

**Operation:**
- 3 concurrent tasks running simultaneously:
  - Task 1: Rapid object creation (5x50 objects)
  - Task 2: Bidirectional passing (3x20 iterations)
  - Task 3: Rapid store/release (3x20 iterations)
- Very aggressive cleanup (0.1s interval)
- Short cleanup timeout (0.5s)

**Expected Issues:**
- Maximum stress on cleanup mechanism
- Highest likelihood of race conditions
- Tests cleanup during active operations

## How to Run

### Run All Tests

```bash
# from the repository root
python3 -m pytest tests/test_refcount_errors_reproduction.py -v -s
```

### Run Specific Test

```bash
python3 -m pytest tests/test_refcount_errors_reproduction.py::TestRefcountErrorReproduction::test_extreme_concurrent_operations_with_forced_cleanup -v -s
```

### Run Without External Dependencies

This test is completely self-contained and uses only rpyc_async code:
- No external projects required
- Uses multiprocessing for server isolation
- All service methods defined in test file

## Interpreting Results

### Success Criteria (Test FAILS = Bug Found)

The test is designed to FAIL when it detects refcount errors:

```
❌ TEST FAILED: Found 5 RPyC refcount error(s) in agent logs.

ERROR DETAILS:
1. [REFCOUNT] DECREF on missing key ('builtins.dict', 123, 456)
   Context: ...
```

This means **the test successfully reproduced the bug**.

### No Errors Detected

```
⚠️  WARNING: No refcount errors detected. Bug may be fixed or test needs adjustment.
PASSED
```

This could mean:
1. The bug has been fixed
2. The test needs more aggressive conditions
3. The race condition didn't trigger in this run

### Diagnostic Information

The test always prints registry statistics:

```
Stats after concurrent operations: {
    'local_objects_count': 78,      # Objects in server registry
    'proxy_cache_count': 0,          # Cached netrefs on client
    'pending_deletions': 59,         # Queued cleanup requests
    'cleanup_running': True          # Background cleanup active
}
```

**Warning Signs:**
- `local_objects_count` > 10 after operations complete = potential leak
- `pending_deletions` > 0 after cleanup cycles = slow/failed cleanup
- `pending_deletions` growing over time = cleanup not processing

## Error Detection Mechanism

### Patterns Searched

The test captures stderr and logging output, searching for:

```python
patterns = {
    "decref_missing_key": r'\[REFCOUNT\]\s+DECREF on missing key',
    "failed_delete": r'Failed to delete remote object',
    "refcount_missing": r'REFCOUNT.*missing',
    "delete_failed": r'delete.*remote.*object.*failed',
}
```

### Logging Configuration

Tests enable debug refcounting:

```python
config={
    "debug_refcounting": True,  # Enable [REFCOUNT] logging
    "cleanup_interval": 0.5,     # Fast cleanup cycles
    "cleanup_ack_timeout": 2.0,  # Timeout for deletion acks
}
```

## Garbage Collection Principles (Expected Behavior)

From the user's requirements, the GC should follow these principles:

### 1. Object Registry

- Registry (`_local_objects`) maintains strong references to passed objects
- Adding to registry increments refcount automatically (Python behavior)
- Object stays in registry until refcount reaches 0

### 2. Netref Creation

- Remote references create netrefs on receiving side
- Weak references to netrefs cached in `_proxy_cache`
- Multiple passes of same object reuse cached netref

### 3. Cleanup Process

- Background GC thread runs periodically
- Detects dead weak references
- Calls async method to remove from remote registry
- Waits for acknowledgment before deleting weak reference
- Must handle race conditions gracefully

### 4. Connection Close

- Registry must be cleared to prevent leaks
- Pending deletions must be processed
- No objects should remain after close

## Current Implementation Status

Based on test results, the current implementation shows:

### ✅ Working Correctly
- Background cleanup task starts and runs
- Pending deletions are queued
- No crashes or exceptions during operations

### ⚠️ Potential Issues
- High `pending_deletions` count after operations
- `local_objects_count` doesn't return to baseline
- Some deletions may not be processed

### ❓ Unclear
- Whether "[REFCOUNT] DECREF on missing key" errors occur (not observed in current tests)
- Whether the defensive code (returning False on missing key) masks the underlying issue

## Integration with External Project

This test was created to reproduce issues seen in a downstream application's
integration test suite.

That external test detected refcount errors in production usage. This test provides:
1. Self-contained reproduction (no external dependencies)
2. More aggressive stress testing
3. Direct registry inspection
4. Isolated test environment

## Future Enhancements

To make the test more effective:

1. **Add assertion on final registry state:**
   ```python
   assert stats['local_objects_count'] <= 2, "Memory leak detected"
   assert stats['pending_deletions'] == 0, "Cleanup not processing"
   ```

2. **Add timing-based race condition triggers:**
   - Interrupt cleanup mid-cycle
   - Create objects during deletion
   - Force concurrent access to registry

3. **Add stress multiplier:**
   - Run 1000+ object iterations
   - Longer test duration
   - More concurrent tasks

## Troubleshooting

### Server Won't Start

```
ConnectionRefusedError: [Errno 111] Connect call failed
```

**Solution:** Check if port is already in use:
```bash
lsof -i :PORT_NUMBER
```

### Tests Hang

**Solution:** Check for deadlocks:
```bash
ps aux | grep pytest
kill -9 PID
```

### No Errors Despite Known Bug

**Solution:** Try:
1. Increase iteration counts
2. Reduce cleanup intervals
3. Add more concurrent tasks
4. Enable full debug logging

## Conclusion

This test suite provides comprehensive coverage of refcount error scenarios in RPyC's garbage collection system. It can detect race conditions, memory leaks, and cleanup failures that may only appear under heavy concurrent load.

The test demonstrates that while the current implementation handles operations without crashing, there are clear signs of cleanup inefficiency (high pending deletion counts, objects remaining in registries). Further investigation needed to determine if this is acceptable behavior or indicates bugs.
