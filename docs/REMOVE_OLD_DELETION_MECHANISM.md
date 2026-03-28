# Design: Complete Removal of Old Deletion Mechanism

**Version**: 1.0
**Date**: 2026-03-28
**Status**: Design Proposal

---

## Executive Summary

The old deletion mechanism (`asyncreq(self, consts.HANDLE_DEL)`) in netref `__del__()` is the root cause of premature object deletion bugs. This document describes the complete removal of the old mechanism, ensuring the new queue-based cleanup system works universally without fallbacks.

**Goal**: ONE deletion mechanism that works 100% of the time, out of the box.

---

## Problem Statement

### Current State (Broken)

Two mechanisms coexist:

1. **NEW mechanism** (v5.2):
   - Queues deletion in `_pending_deletions`
   - Background cleanup task with acknowledgment
   - Prevents premature deletion
   - **BUT**: Only works if `_cleanup_connection` is set

2. **OLD mechanism** (legacy):
   - Fire-and-forget `asyncreq(HANDLE_DEL)` in `__del__()`
   - No acknowledgment, no retry
   - **Causes bugs**: premature deletion, race conditions
   - **Currently active** as fallback when cleanup callback not registered

### Root Cause of Bugs

The old mechanism has fundamental flaws:
- Blocking I/O in `__del__()` (forbidden in Python)
- Fire-and-forget (no confirmation object was deleted)
- Race conditions (object deleted locally while remote still holds netref)
- No retry on failure

### Why Both Mechanisms Exist

Lines 144-153 in `netref.py`:
```python
if cleanup_conn is not None and refcount_holder is not None:
    # NEW mechanism - queue for background cleanup
    cleanup_conn._pending_deletions.put((id_pack, refcount))
else:
    # OLD mechanism - FALLBACK (this is the problem!)
    asyncreq(self, consts.HANDLE_DEL, self.____refcount__)
```

This fallback keeps the bugs alive.

---

## Design: Complete Removal

### Phase 1: Ensure New Mechanism Always Active

#### 1.1 Remove Conditional Logic from `__del__()`

**File**: `rpyc/core/netref.py`

**Current code** (lines 132-158):
```python
def __del__(self):
    try:
        cleanup_conn = object.__getattribute__(self, "_cleanup_connection")
        refcount_holder = object.__getattribute__(self, "_refcount_holder")

        if cleanup_conn is not None and refcount_holder is not None:
            # NEW mechanism
            refcount_holder["refcount"] = self.____refcount__
            cleanup_conn._pending_deletions.put((
                refcount_holder["id_pack"],
                self.____refcount__
            ))
        else:
            # OLD mechanism - FALLBACK (DELETE THIS!)
            asyncreq(self, consts.HANDLE_DEL, self.____refcount__)
    except Exception:
        pass
```

**NEW code** (no fallback):
```python
def __del__(self):
    """
    Netref destructor (v5.2 - cleanup callback only).

    Queue deletion for background cleanup. No fallback to synchronous deletion.
    If cleanup callback not registered, this is a bug in _unbox().
    """
    try:
        cleanup_conn = object.__getattribute__(self, "_cleanup_connection")
        refcount_holder = object.__getattribute__(self, "_refcount_holder")

        # STRICT: Must have cleanup callback registered
        if cleanup_conn is None or refcount_holder is None:
            # This should NEVER happen if _unbox() works correctly
            import sys
            print(
                f"ERROR: Netref {self.____id_pack__} has no cleanup callback! "
                f"This is a bug in _unbox(). Object will leak.",
                file=sys.stderr
            )
            return

        # Queue for background cleanup (ONLY mechanism)
        refcount_holder["refcount"] = self.____refcount__
        cleanup_conn._pending_deletions.put((
            refcount_holder["id_pack"],
            self.____refcount__
        ))
    except Exception:
        # Errors in __del__ cannot be raised (Python limitation)
        pass
```

**Key changes**:
- ✅ **Removed**: `asyncreq(self, consts.HANDLE_DEL)` fallback
- ✅ **Added**: Error message if cleanup callback missing (exposes bugs)
- ✅ **Single path**: Only new mechanism

---

#### 1.2 Guarantee Cleanup Callback Registration

**File**: `rpyc/core/protocol.py`

**Problem**: `_unbox()` might not always register cleanup callback.

**Solution**: Make cleanup callback registration **mandatory** for all netrefs.

**Current code** (lines ~730-780 in `_unbox()`):
```python
def _unbox(self, package):
    # ... existing code ...

    if isinstance(obj, BaseNetref):
        # NEW (v5.2): Register cleanup callback
        if self._asyncio_enabled:  # <-- CONDITIONAL (problem!)
            refcount_holder = {"id_pack": id_pack, "refcount": proxy.____refcount__}
            object.__setattr__(proxy, "_refcount_holder", refcount_holder)
            object.__setattr__(proxy, "_cleanup_connection", self)
```

**NEW code** (unconditional):
```python
def _unbox(self, package):
    # ... existing code ...

    if isinstance(obj, BaseNetref):
        # NEW (v5.2): ALWAYS register cleanup callback
        # No conditional - this must work for all netrefs
        refcount_holder = {
            "id_pack": id_pack,
            "refcount": proxy.____refcount__
        }
        object.__setattr__(proxy, "_refcount_holder", refcount_holder)
        object.__setattr__(proxy, "_cleanup_connection", self)
```

**Key changes**:
- ✅ **Removed**: `if self._asyncio_enabled` conditional
- ✅ **Always set**: cleanup callback for every netref
- ✅ **Guarantee**: No netref without cleanup callback

---

#### 1.3 Ensure Cleanup Infrastructure Always Available

**File**: `rpyc/core/protocol.py`

**Problem**: `_pending_deletions` queue and cleanup task only exist if asyncio enabled.

**Solution**: Make cleanup infrastructure **always present**, even without asyncio.

**Current code** (`__init__`):
```python
def __init__(self, ...):
    # ... existing code ...
    # NEW (v5.2): Only initialized if asyncio enabled (problem!)
    if config.get("some_flag"):
        self._pending_deletions = Queue()
        self._cleanup_task = None
```

**NEW code** (`__init__`):
```python
def __init__(self, ...):
    # ... existing code ...

    # NEW (v5.2): ALWAYS initialize cleanup infrastructure
    self._pending_deletions: Queue[Tuple[Tuple[str, int, int], int]] = Queue()
    self._cleanup_task: Optional[asyncio.Task] = None
    self._cleanup_running: bool = False
    self._cleanup_interval: float = config.get("cleanup_interval", 2.0)

    # Note: Cleanup task will be started when asyncio serving is enabled
    # But queue must exist even without asyncio for backward compat
```

**Key changes**:
- ✅ **Always present**: `_pending_deletions` queue exists on all connections
- ✅ **Lazy start**: Cleanup task starts only when asyncio enabled
- ✅ **Graceful degradation**: If no asyncio, deletions queue up (processed on close)

---

### Phase 2: Handle Non-Asyncio Connections

**Challenge**: What if connection doesn't use asyncio?

**Solution 1** (Recommended): Process queue on connection close

**File**: `rpyc/core/protocol.py`

Add to `close()` method:
```python
def close(self):
    """Close connection and process any pending deletions."""

    # NEW (v5.2): Process pending deletions before closing
    if not self._pending_deletions.empty():
        logger = self._config.get("logger")
        if logger:
            logger.debug(
                f"Processing {self._pending_deletions.qsize()} "
                f"pending deletions before close"
            )

        # Process all pending deletions synchronously
        while not self._pending_deletions.empty():
            try:
                id_pack, refcount = self._pending_deletions.get_nowait()
                # Send deletion directly (no acknowledgment needed on close)
                self._send(consts.MSG_REQUEST, (
                    self._get_seq_id(),
                    (consts.HANDLE_DEL, (id_pack, refcount))
                ))
            except Exception as e:
                if logger:
                    logger.warning(f"Failed to send deletion on close: {e}")

    # ... existing close code ...
```

**Solution 2** (Alternative): Require asyncio for cleanup

Document that proper cleanup requires asyncio:
```python
def __init__(self, ...):
    # ... existing code ...

    if not config.get("allow_sync_mode"):
        # Strict mode: asyncio required for proper cleanup
        self._pending_deletions = Queue()
    else:
        # Legacy mode: allow sync connections, cleanup on close only
        self._pending_deletions = Queue()
```

**Recommendation**: Use Solution 1 (process on close) for maximum compatibility.

---

### Phase 3: Testing & Migration

#### 3.1 Update All Tests

**Tests to update**:
1. `tests/test_netref_cleanup_callbacks.py` - ensure no fallback path tested
2. `tests/test_e2e_lifecycle_prevention.py` - verify works without asyncio
3. Add new test: `test_cleanup_without_asyncio.py` - verify sync mode

**New test**:
```python
def test_netref_cleanup_without_asyncio():
    """Netref cleanup must work even without asyncio serving."""
    # Create connection without asyncio
    conn = Connection(VoidService(), mock_channel, config={})

    # Create netref
    id_pack = ("test.Class", 1, 100)
    proxy = conn._unbox((consts.LABEL_REMOTE_REF, (*id_pack, 0)))

    # Verify cleanup callback registered
    assert object.__getattribute__(proxy, "_cleanup_connection") is conn
    assert object.__getattribute__(proxy, "_refcount_holder") is not None

    # Delete proxy
    del proxy
    gc.collect()

    # Verify queued (not sent immediately)
    assert not conn._pending_deletions.empty()

    # Close connection - should process queue
    conn.close()

    # Verify queue processed
    assert conn._pending_deletions.empty()
```

#### 3.2 Remove Old Code

**Files to modify**:
1. `rpyc/core/netref.py`:
   - Remove fallback in `__del__()`
   - Remove import of `asyncreq` if unused elsewhere

2. `rpyc/core/protocol.py`:
   - Remove conditional cleanup callback registration
   - Make `_pending_deletions` always present
   - Add cleanup processing in `close()`

3. Documentation:
   - Update `NETREF_LIFECYCLE_ANALYSIS_AND_SOLUTION.md`
   - Remove mentions of "fallback mechanism"

#### 3.3 Rollout Plan

**Step 1**: Add error detection (one commit)
- Add error message in `__del__()` for missing cleanup callback
- Run tests, identify any paths where callback not set
- Fix those paths

**Step 2**: Make cleanup callback unconditional (one commit)
- Remove `if asyncio_enabled` from `_unbox()`
- Ensure `_pending_deletions` always exists
- Run full test suite

**Step 3**: Remove old mechanism completely (one commit)
- Remove fallback from `__del__()`
- Add cleanup processing in `close()`
- Update tests

**Step 4**: Validation (one commit)
- Run full test suite including E2E tests
- Verify no "Failed to delete" warnings
- Verify no "DECREF on missing key" warnings
- Performance testing

---

## Code Changes Summary

### Files to Modify

| File | Changes | Lines |
|------|---------|-------|
| `rpyc/core/netref.py` | Remove fallback, add error detection | 132-158 |
| `rpyc/core/protocol.py` | Unconditional cleanup callback, always-present queue, close() processing | 730-780, 200-220, 1400+ |
| `rpyc/core/protocol.py` | `__init__` - always init cleanup infra | ~200 |

### Lines to Delete

**rpyc/core/netref.py:152-153**:
```python
else:
    asyncreq(self, consts.HANDLE_DEL, self.____refcount__)
```

**rpyc/core/protocol.py** (remove conditionals):
```python
if self._asyncio_enabled:  # <-- DELETE THIS CHECK
    refcount_holder = ...
```

### Lines to Add

**rpyc/core/protocol.py** in `close()`:
```python
# Process pending deletions before close (~15 lines)
while not self._pending_deletions.empty():
    ...
```

---

## Expected Outcomes

### After Implementation

✅ **ONE deletion mechanism** - no fallbacks, no dual systems

✅ **Always works** - out of the box, no configuration needed

✅ **No more bugs**:
- No "Failed to delete remote object" warnings
- No premature deletions
- No race conditions
- No KeyError/EOFError when using netrefs

✅ **Clean logs**:
- No warnings about failed deletions
- Only intentional debug logs (if enabled)

✅ **Performance**:
- No blocking I/O in `__del__()`
- Batched deletions reduce network overhead
- Background processing doesn't block main thread

### Metrics

Before (current):
- 452 warnings in logs
- 14 "Failed to delete" messages in one test
- 10 "DECREF on missing key" warnings

After (target):
- 0 warnings
- 0 failed deletions
- Clean operation

---

## Risk Analysis

### Low Risk

- New mechanism already tested and working (E2E tests pass)
- Changes are mostly **removing** code (less code = fewer bugs)
- Error detection helps catch issues early

### Medium Risk

- Non-asyncio connections need special handling
- Existing code might rely on fallback behavior
- Migration needs careful testing

### Mitigation

1. **Staged rollout** - 4 separate commits, test after each
2. **Error detection first** - identify problems before breaking changes
3. **Comprehensive testing** - E2E tests validate real scenarios
4. **Clear error messages** - if something breaks, we know why

---

## Backward Compatibility

### Breaking Changes

**NONE** - from user perspective, everything works the same:
- Same API
- Same behavior
- Better reliability

### Internal Changes

- Netref `__del__()` behavior changes (but users don't call `__del__` directly)
- Cleanup queue always present (transparent to users)
- Processing happens on close (transparent to users)

---

## Success Criteria

### Must Have

- [ ] No old fallback mechanism in code
- [ ] All netrefs have cleanup callback registered
- [ ] All tests pass (51/51)
- [ ] E2E test passes with 0 warnings
- [ ] No "Failed to delete" messages

### Nice to Have

- [ ] Performance benchmark shows no regression
- [ ] Memory leak test shows proper cleanup
- [ ] Documentation updated

---

## Timeline

| Phase | Duration | Effort |
|-------|----------|--------|
| Phase 1: Remove fallback | 2 hours | Medium |
| Phase 2: Handle non-asyncio | 1 hour | Low |
| Phase 3: Testing | 2 hours | High |
| **Total** | **5 hours** | **Medium** |

---

## Alternatives Considered

### Alternative 1: Keep Fallback with Warnings

**Rejected**: Keeps the buggy code alive, doesn't solve the problem

### Alternative 2: Two Separate Code Paths

**Rejected**: Complexity increases, hard to maintain, bugs persist

### Alternative 3: Require Asyncio Always

**Rejected**: Too breaking, kills backward compatibility

### Chosen: Remove Fallback, Universal Cleanup

**Why**: Simple, clean, solves the root cause, minimal risk

---

## Conclusion

The old deletion mechanism is the root cause of all premature deletion bugs. By removing it completely and ensuring the new queue-based cleanup works universally, we eliminate an entire class of bugs.

**Key principle**: ONE mechanism that works 100% of the time is better than TWO mechanisms where each works 50% of the time.

**Next steps**:
1. Review this design
2. Implement Phase 1 (remove fallback)
3. Test thoroughly
4. Implement Phase 2 (non-asyncio handling)
5. Validate with E2E tests

**No fallbacks. No compromises. One clean solution.**
