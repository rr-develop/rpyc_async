# Design — Suppress `HANDLE_DEL` storm on closed connection

**Status:** Approved (with two
non-blocking recommendations, both folded in below: drain queue on
`closed`, `logger.debug` instead of silent).
**Related incident:** a related internal incident analysis (not included here).
**Companion docs:** `DESIGN_BIDIRECTIONAL_ASYNC_FIXES.md`,
`DESIGN_REFCOUNT_RACE_FIX.md`,
`DESIGN_NO_POLLING_ASYNCIO_READ.md`.

---

## 1. Problem

Long-running agents accumulate up to **128 MB of
stderr logs** filled with thousands of identical lines:

```
WARNING: Failed to delete remote object
         ('builtins.method', 10665440, 6985923220734123).
         Possible memory leak on remote side.
```

For one observed log: **7080 occurrences of the very same
`id_pack`**, all clustered immediately after the RPyC peer
(a downstream application) had disconnected. Same shape on at least four
agents in production.

Two layers of failure compound:

1. Something on the agent side keeps **re-creating** the same
   `builtins.method` netref proxy (cache miss → new proxy → GC →
   enqueue HANDLE_DEL → repeat). That is a separate retention bug
   we will track elsewhere — **not in scope here**.
2. Each enqueued HANDLE_DEL is dispatched via
   `_async_request_with_ack` on a connection whose peer is **gone**.
   The call returns `False`, and the cleanup loop emits the
   `WARNING` **and** a second identical `logger.warning` line —
   forever, for every drain cycle, for every entry in the queue.

This document fixes layer (2) only: the spam.

## 2. Scope

**In scope.**
* Stop the WARNING storm when the connection is already closed.
* Stop wasting CPU serializing HANDLE_DEL frames that will be
  dropped at the closed stream.
* Preserve **all** existing warning behaviour while the connection
  is alive — including transient `_async_request_with_ack`
  timeouts on a live connection (those still indicate a real peer
  problem worth surfacing).

**Out of scope.**
* The root retention bug (why the same bound-method proxy is
  rematerialized thousands of times against a dead peer).
* Any change to `_process_pending_deletions_sync` (the
  close-time best-effort drain). That path is already silent on
  send failure.
* Changes to the warning text, severity, or stderr-vs-logger
  duplication policy. The "DO NOT REMOVE OR MODIFY THIS LOGGING"
  banner is respected — we are only adding a guard *before* the
  banner-protected code runs, not modifying the banner-protected
  code itself.

## 3. Constraints / Policies

* **NO POLLING.** This fix is purely guard-clauses on an existing
  event-driven path. No timers, no sleeps, no background threads
  added.
* **NO NEW THREADS.** Same — no threading additions.
* **FILE PERMISSIONS.** No new files created at runtime by the
  library; non-applicable here.
* **TDD.** Tests written first; implementation only after the
  test demonstrates the failing case.
* **NO REGRESSION** to live-connection behaviour — must be proven
  by an explicit "negative" unit test (still warns when alive).

## 4. Design

### 4.1 Change

Add **one guard clause** at the top of
`_process_pending_deletions` in
`rpyc/core/protocol.py` (currently line 1521):

```python
async def _process_pending_deletions(self) -> None:
    """..."""
    # Guard: if the peer is gone, HANDLE_DEL cannot succeed.
    # Drop the queued batch silently — the peer process no
    # longer exists, so what we fail to send cannot leak. Also
    # drain the queue so it does not grow unbounded across
    # repeated cleanup_loop iterations on a closed connection.
    # See a related internal incident analysis.
    if self.closed:
        dropped = 0
        try:
            while True:
                self._pending_deletions.get_nowait()
                dropped += 1
        except Exception:
            pass  # Queue is empty (queue.Empty) or already gone
        if dropped:
            logger = self._config.get("logger")
            if logger:
                logger.debug(
                    "Dropping %d pending HANDLE_DELs on closed "
                    "connection (peer is gone)", dropped
                )
        return

    # ... existing batch-collect-and-send logic unchanged ...
```

Note on field access: the code reads `self.closed` (the public
property at protocol.py:788, which returns `self._closed`). Tests
that pre-set the state should write `conn._closed = True` —
that is the same write `Connection.close()` performs internally
(protocol.py:769).

### 4.2 Why this is correct

`self.closed` flips to `True` exactly inside `Connection.close()`
(protocol.py:769). After that, the channel/stream is being torn
down and no RPC can succeed.

The existing **close-time** drain
`_process_pending_deletions_sync` is invoked at protocol.py:772
**before** the asyncio path is disabled and **before** the
`_async_request_with_ack` channel is closed. That sync drain is
the official last-chance flush. Our async drain has no
equivalent obligation after `closed` — by then the sync drain
has already run.

So:

* Live connection: behaviour unchanged. The guard short-circuits
  only when `self.closed`.
* Closed connection: we skip the HANDLE_DEL send and the
  warning. The peer is gone, so we cannot cause a leak we are not
  already powerless to prevent.

### 4.3 Belt-and-braces (optional, recommended)

Inside the existing `if not result:` branch (protocol.py:1588),
add a second check:

```python
if not result:
    if self.closed:
        # Connection closed mid-batch (between get_nowait and
        # await). Stay silent — see top-of-function guard.
        continue
    # ... existing warning prints unchanged ...
```

This closes a small race: the top-of-function guard catches the
common case ("conn was already closed when we entered the
function"), and this second check catches the rarer
"conn closed while we were awaiting ack". Without it, we would
emit one stale warning per id_pack in the rare race.

Both guards are tiny, allocation-free, and preserve the
"DO NOT REMOVE THIS LOGGING" banner intent — we are not
suppressing legitimate live-connection warnings.

### 4.4 What we do NOT do

* **Do NOT add a `_dead_id_packs` set / TTL cache** to dedupe
  warnings on a live connection. That would mask real
  per-id_pack leaks on a working peer and was rejected during
  scoping.
* **Do NOT change** `_process_pending_deletions_sync`. It is
  already silent on send failure (catches all exceptions from
  `sync_request`).
* **Do NOT remove or alter** the existing WARNING lines or the
  "DO NOT REMOVE OR MODIFY THIS LOGGING" banner.
* **Do NOT touch** the `__del__` / `_enqueue_deletion` /
  `cleanup_loop` paths. The queue accumulating entries is the
  upstream symptom; suppressing dispatched-but-doomed sends is
  the right place to add the guard.

## 5. Test plan (TDD)

All tests go in a new file
`tests/test_handle_del_suppress_on_closed.py`.
We do NOT add to `test_background_cleanup.py` so the regression
is named after the incident and easy to locate later.

### 5.1 Unit tests (no real socket, no peer)

These tests construct a minimal `Connection` stub (or use the
existing test harness) and drive `_process_pending_deletions`
directly.

**Test A — `test_no_warning_when_closed`:**
1. Build a `Connection` with `_pending_deletions` pre-populated
   with several synthetic `(id_pack, refcount)` tuples (use
   >1 so the drain assert is meaningful).
2. Set `conn._closed = True` (this is what `Connection.close()`
   does internally — see protocol.py:769; the public reader
   `conn.closed` is the property defined at protocol.py:788).
3. Monkey-patch `_async_request_with_ack` to a `MagicMock` so we
   can assert it was NOT called.
4. Capture `sys.stderr` and the logger.
5. `await conn._process_pending_deletions()`.
6. Assert:
   * `_async_request_with_ack.call_count == 0`.
   * No `WARNING: Failed to delete remote object` in stderr.
   * No `WARNING`-level log record matching the fail message.
   * **`conn._pending_deletions.empty()` is True** — the queue
     was drained (rec. from reviewer, prevents unbounded growth
     across cleanup_loop iterations).
   * Exactly one `DEBUG`-level log record matching `"Dropping
     %d pending HANDLE_DELs"` with the correct count.

**Test B — `test_warning_still_fires_when_open_and_ack_fails` (regression):**
1. Same setup but `_closed = False`.
2. Monkey-patch `_async_request_with_ack` to return `False`
   (simulates a real ack failure on a live conn).
3. Capture stderr + logger.
4. `await conn._process_pending_deletions()`.
5. Assert:
   * `_async_request_with_ack.call_count == 1`.
   * `WARNING: Failed to delete remote object` IS present in
     stderr.
   * `logger.warning` was called with the matching message.

**Test C (optional, covers the belt-and-braces race) —
`test_no_warning_when_closes_mid_await`:**
1. `_closed = False` at entry.
2. Monkey-patch `_async_request_with_ack` to an `async` function
   that sets `self._closed = True` then returns `False`.
3. Run `_process_pending_deletions`.
4. Assert: NO warning, because the post-await guard kicked in.

### 5.2 Integration test (real peer, no mocks)

`tests/test_handle_del_suppress_integration.py`:

1. Start a real `AsyncIOServer` in a subprocess with a service
   that exposes a method returning a bound method (so the client
   gets a netref proxy with class `builtins.method`).
2. From the client process, call the method many times so the
   proxy is created and GC'd repeatedly, queueing HANDLE_DELs.
3. Hard-kill the server subprocess (SIGKILL) so the client's
   connection transitions to closed without a clean teardown.
4. Wait until `client_conn.closed` is `True` (use
   `await client_conn.wait_closed()` per existing policy — no
   `while not closed: sleep` polling).
5. Continue cycling the proxy on the client side for a short
   bounded window (e.g. `await asyncio.sleep(0)` 100 times) to
   let cleanup loop iterations run.
6. Capture stderr.
7. Assert:
   * `"Failed to delete remote object"` appears in stderr
     **zero** times after the close. (May appear once or twice
     in the close-race window — assert `count < 5` to be
     tolerant of the documented race, **not** unbounded.)
   * `len(client_conn._pending_deletions.queue) == 0` (or
     `qsize() == 0`) after the post-close cycling window —
     verifies the drain in the guard works under a real
     workload, not just on a single synthetic tuple.

This is a no-mock test per project policy.

### 5.3 Pass criteria

* Before fix: Test A FAILS (warning emitted), Test B PASSES, Test
  C FAILS, integration FAILS (storm reproduces).
* After fix: A, B, C, and integration all PASS.

## 6. Risk / regression matrix

| Risk | Mitigation | Test |
|---|---|---|
| Hides real "peer is slow" warnings on a live conn | Guard only on `self.closed`, which is **not** flipped on timeouts | Test B |
| Hides legitimate leak on a live but flaky peer | Same — only triggers when conn fully closed | Test B |
| Breaks close-time drain | Sync drain (`_process_pending_deletions_sync`) is untouched and runs **before** `_cleanup` | Existing tests for sync drain |
| Race: conn closes between batch dequeue and await | Second guard in `if not result:` catches it | Test C |
| Affects non-asyncio connections | Async drain is asyncio-only; sync drain unchanged | N/A |

## 7. Acceptance criteria

1. All three new unit tests pass.
2. The new integration test passes (storm pattern absent post-kill).
3. Existing `tests/test_background_cleanup.py`,
   `tests/test_batch_deletion.py`,
   `tests/test_refcount_delete_timeout.py`,
   `tests/test_cleanup_loop_pin.py`,
   `tests/test_netref_cleanup_callbacks.py`,
   and `tests/test_no_polling_policy.py` continue to pass
   without modification.
4. Pre-commit hooks pass without bypass.
5. No new threads, no new timers, no new polling.

## 8. Open questions for review — RESOLVED

**Q.** Should the silent skip emit a single `logger.debug`
("dropped N pending deletions on closed connection") for
observability, or stay completely silent?

**Resolution (from review):** emit
`logger.debug` (NOT `info`, NOT silent).
* `info` would spam every graceful shutdown.
* Complete silence loses the diagnostic signal needed if the
  upstream retention bug (layer 1) is ever investigated.
* `debug` stays quiet at default WARNING level but is available
  when the operator opts in.

This is folded into §4.1 above.

## 9. Files touched

* `rpyc/core/protocol.py` — add two guard
  clauses in `_process_pending_deletions`.
* `tests/test_handle_del_suppress_on_closed.py` — new.
* `tests/test_handle_del_suppress_integration.py` — new.
* `docs/DESIGN_HANDLE_DEL_SUPPRESS_ON_CLOSED.md` — this file.

No other files in scope.
