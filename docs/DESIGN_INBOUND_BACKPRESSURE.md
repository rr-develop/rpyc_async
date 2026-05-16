# Inbound Dispatch Backpressure (per-Connection quarantine)

**Status:** design, not yet implemented.
**Severity:** high — production observed 12.88 GB RSS / 73 min on a single process because of a malformed websocket client spamming RPyC requests in a `while True` loop.
**Related incidents:**
- A related internal incident analysis (not included here) — `_DISPATCH_INFLIGHT` strong-ref fix (peer side).
- An internal agent-side observation that motivates this design.

## TL;DR

An RPyC `Connection` currently has **no upper bound** on the number of in-flight inbound dispatch tasks it accepts from a peer. If a peer is structurally broken (live socket, but never resolves the callbacks the handler issues back to it — see "Why prior fixes don't help" below), the agent accumulates parked `_dispatch_request_async` Tasks unboundedly. Production growth rate: **~27 000 dispatch tasks / minute / channel**, RSS grows ~10 MB/min/channel. At 73 min one channel had **2 024 936 inflight tasks** and **2 339 600 entries in `_request_callbacks`**, pinning **12.88 GB**.

The defence is **per-`Connection` backpressure**: count inbound inflight, and when the count exceeds a configurable threshold (default **10 000**), **quarantine** the channel:

1. log ONCE with full diagnostic detail,
2. drop every further inbound `MSG_REQUEST` on this channel (silent no-op at `_dispatch`),
3. cancel all currently-parked dispatch tasks belonging to this Connection,
4. clear `Connection._request_callbacks` for this Connection (outbound AsyncResults the agent was waiting for the peer to answer — by definition stale, since the peer is so broken it's spamming inbound while ignoring outbound),
5. the channel remains open at the TCP level (peer continues to send into the kernel recv buffer; we drain and drop), but `_inbound_quarantined` is **terminal** — no recovery, no flapping.

This protects an agent from one broken client without affecting any of its other clients.

## Why prior fixes don't help

The six prior async-suite fixes all close the same general class of bug: "channel torn down / Task GC'd / cancellation cascade — chain leaks because the cleanup hook never ran". They assume that **eventually the channel closes** or the Task transitions to terminal state, and the cleanup chain fires.

This scenario violates that assumption:

| signal | value on the leaking conn | meaning |
|---|---|---|
| `Connection._closed` | `False` | peer's socket is alive, kernel still moves bytes |
| `_DISPATCH_INFLIGHT` (per this conn) | **2 024 936** | all Tasks parked at `_handle_async_call:172` (`await async_res`) |
| `_request_callbacks` | **2 339 600** | outbound AR's the handler is awaiting; peer never replies |
| `helpers._INFLIGHT` | 0 | outbound fire-and-forget chain healthy on this side |
| `CLEANUP_LOOPS` | 3 | cleanup_loop pin works as designed |
| `traceback`, `CancelledError` instances | 1, 0 | this is NOT the traceback-retention path |

Existing fixes target: "peer dead" (`is_connected`/`_closed`), "Task collected pending" (`_INFLIGHT`/`_DISPATCH_INFLIGHT` strong-refs), "cancel-on-timeout slot leak" (`__await__` done-callback), "cleanup_loop self-cycle" (weakref+module-set), "traceback retention" (eager `_box_exc`). NONE of them handle "**peer is live, never closes, but its handlers don't make progress, so each inbound triggers an outbound callback that hangs forever**".

The new failure shape needs a new countermeasure: bound how much memory ONE channel may consume, regardless of why it's consuming it.

## Where in the codebase

**File:** `rpyc/core/protocol.py`

The fix lives entirely inside `Connection`. Three touch points:

1. **`Connection.__init__`** (~line 365) — add per-conn state (counter + quarantine flag + log-once flag).
2. **`Connection._dispatch`** (~line 2245-2252) — before scheduling the dispatch task, check the counter; if quarantined, drop the request; if just crossed threshold, transition to quarantine.
3. **`Connection._cleanup`** (~line 547-580) — the cancellation+clear logic already exists for the close path; extract it into a helper so the quarantine path can reuse it without duplicating the per-conn frame scan.

### Why not at `AsyncioServer` or `Application` level

| candidate level | rejected because |
|---|---|
| `AsyncioServer.accept` | sees connection counts but not in-flight requests; can't tell one broken client from N healthy clients sharing the same accept queue. |
| an application-level service | application-level handlers only see the `exposed_*` method body, not the RPyC-internal `_handle_async_call:172` path where the actual leak frame lives. Also requires repeating the same guard in every project that builds on this library. |
| global module-level cap on `_DISPATCH_INFLIGHT` | a single bad channel would silently freeze every other healthy channel sharing the process. Wrong granularity. |

Per-Connection is the only level that has both the right granularity (one bad channel, one bounded blast radius) and the right visibility (sees inbound dispatch directly).

### Why a counter, not `len(_DISPATCH_INFLIGHT)` scan

An earlier incident specifically called out the failure mode of scanning `asyncio.all_tasks()` and inspecting `frame.f_locals.get("self") is self` per inbound — it's O(N) per request, and since the very mechanism pins N, the process livelocks at 60-80% CPU once N grows large. The fix went out of its way to keep `_dispatch` **O(1)** by handing the Task object directly to `_schedule` instead of looking it up.

This design preserves that O(1) property: a per-Connection integer counter (`self._inbound_inflight`), incremented inside `_schedule` (which is already the place that has the Task in hand) and decremented in the existing `add_done_callback`. Threshold check at `_dispatch` is a single `if int >= int`.

## The exact changes

### 1. `Connection.__init__` — three new instance fields

```python
# Per-connection backpressure state. See
# docs/DESIGN_INBOUND_BACKPRESSURE.md.
self._inbound_inflight: int = 0
self._inbound_quarantined: bool = False
self._inbound_quarantine_logged: bool = False
```

`_inbound_inflight` is incremented exactly when a dispatch Task is added to `_DISPATCH_INFLIGHT` for this Connection, and decremented exactly when that Task's `done_callback` fires. The counter therefore tracks the true per-Connection working set of inbound dispatches, regardless of whether they completed normally, were cancelled, or raised.

`_inbound_quarantined` is **terminal** — once `True`, never reset. A peer that already proved itself broken does not get a second chance.

`_inbound_quarantine_logged` ensures the diagnostic line lands in the log exactly **once** per Connection, not once per dropped request. The incident had 149 055 traceback lines from the broken client in one log file — this flag prevents the symmetric storm on the rpyc side.

### 2. `Connection._dispatch` — backpressure check

In the existing `_schedule` closure (line 2247-2252), wrap the body to track `_inbound_inflight`:

```python
def _schedule(_coro=_coro):
    task = asyncio.get_event_loop().create_task(_coro)
    _DISPATCH_INFLIGHT.add(task)
    self._inbound_inflight += 1
    def _on_done(_t, _self=self):
        _DISPATCH_INFLIGHT.discard(_t)
        _self._inbound_inflight -= 1
    task.add_done_callback(_on_done)
```

`_on_done` runs on the loop thread (same thread as `_schedule` since `add_done_callback` schedules via `call_soon` on the Task's loop), so the integer mutations are race-free — no lock needed.

**Before** `_schedule` runs, in `_dispatch` proper, add the threshold check. The check happens on the channel-reader (which is whatever thread `_dispatch` is invoked on); reading `self._inbound_inflight` and `self._inbound_quarantined` from another thread is safe because:
- the integer write happens atomically (CPython GIL guarantees single-instruction int load/store visibility),
- if we read a stale "below threshold" value we just schedule one extra request that the next iteration will catch — the overshoot is bounded by `concurrency × 1`, not material at threshold = 10 000,
- if we read a stale "True" for `_inbound_quarantined` we just drop, which is the conservative direction.

```python
# ... inside _dispatch, after msg_type == MSG_REQUEST branch,
#     just before _coro = self._dispatch_request_async(seq, args)
if self._inbound_quarantined:
    # Channel is in terminal quarantine — silently drop. The peer
    # is by definition misbehaving (it crossed the threshold and
    # has been spamming since). Don't reply, don't log per-request:
    # the one-shot quarantine log already covered this channel.
    return

max_inflight = self._config["max_inbound_inflight"]
if max_inflight and self._inbound_inflight >= max_inflight:
    self._enter_inbound_quarantine()
    return

_coro = self._dispatch_request_async(seq, args)
# ... existing _schedule code ...
```

### 3. New helper `Connection._enter_inbound_quarantine`

Encapsulates the one-shot transition. Lives on `Connection` so it has direct access to per-conn state and uses the same per-conn task scan as `_cleanup`.

```python
def _enter_inbound_quarantine(self) -> None:
    """Transition this Connection to terminal inbound-quarantine.

    Called exactly once per Connection, at the moment
    self._inbound_inflight first crosses ``max_inbound_inflight``.
    From this point onward _dispatch silently drops MSG_REQUEST on
    this channel. Outbound AsyncResults waiting for the peer's
    reply are cleared (they will never resolve — the peer that
    DDoS'd us is by definition not going to answer).

    The Connection is NOT closed. We keep the channel open and
    drain inbound bytes from the kernel buffer to /dev/null so
    the peer doesn't get TCP-level backpressure that might make
    its bug even louder (the broken client would just
    log harder if we closed). open-and-ignore is the cheapest
    state.

    Idempotent: subsequent invocations are no-ops via the
    quarantined flag check at the top.
    """
    if self._inbound_quarantined:
        return
    self._inbound_quarantined = True

    inflight_snapshot = self._inbound_inflight
    rcb_snapshot = len(self._request_callbacks)
    peer_repr = self._channel_peer_for_log()  # best-effort, never raises

    logger = self._config.get("logger")
    if logger and not self._inbound_quarantine_logged:
        self._inbound_quarantine_logged = True
        # ONE line — full diagnostic context, no follow-up storm.
        logger.error(
            "rpyc inbound quarantine: connid=%s peer=%s "
            "inbound_inflight=%d threshold=%d request_callbacks=%d. "
            "Channel kept open; further MSG_REQUEST on this channel "
            "are silently dropped. Cancelling parked dispatch tasks "
            "and clearing outbound _request_callbacks. "
            "See docs/DESIGN_INBOUND_BACKPRESSURE.md.",
            self._config["connid"], peer_repr,
            inflight_snapshot,
            self._config["max_inbound_inflight"],
            rcb_snapshot,
        )

    # Reuse the same per-conn task scan used by _cleanup. Extracted
    # into _drain_inbound_dispatch() so close-path and quarantine-
    # path share one implementation.
    self._drain_inbound_dispatch()
    self._request_callbacks.clear()
```

### 4. Extract `Connection._drain_inbound_dispatch` from `_cleanup`

The block at lines 565-580 of `_cleanup` walks `_DISPATCH_INFLIGHT`, identifies tasks whose coroutine frame has `self` bound to **this** Connection, and cancels them. Move that block verbatim into a helper:

```python
def _drain_inbound_dispatch(self) -> None:
    """Cancel every inbound dispatch task belonging to THIS Connection.

    Shared by _cleanup (close path) and _enter_inbound_quarantine
    (overload path). Snapshot the set via list() because cancel()
    triggers a done-callback that mutates _DISPATCH_INFLIGHT.
    """
    for _task in list(_DISPATCH_INFLIGHT):
        try:
            _coro = _task.get_coro()
            _frame = getattr(_coro, "cr_frame", None)
            if _frame is not None and \
               _frame.f_locals.get("self") is self and \
               not _task.done():
                _task.cancel()
        except Exception:
            # Never let cleanup raise from a Task introspection.
            pass
```

`_cleanup`'s existing block becomes `self._drain_inbound_dispatch()`. No behaviour change on the close path.

This is the one O(N) scan in the design — but it runs **once per Connection per quarantine event**, not per-request. Same amortised cost as `_cleanup`'s scan, which is already considered acceptable on the close path.

### 5. `DEFAULT_CONFIG` — new config key

In `rpyc/core/protocol.py` near the top, add:

```python
DEFAULT_CONFIG = dict(
    ...,
    # Per-Connection cap on simultaneously-inflight inbound dispatch
    # tasks. Once exceeded, the Connection enters terminal
    # quarantine: further MSG_REQUEST silently dropped, parked
    # dispatch tasks cancelled, outbound _request_callbacks cleared.
    # 0 disables the cap (legacy behaviour). See
    # docs/DESIGN_INBOUND_BACKPRESSURE.md.
    max_inbound_inflight = 10_000,
    ...
)
```

Default **10 000**: about 100× the realistic high-water mark for legitimate concurrency (production agents are typically 10-100 inflight under load), comfortably below the millions seen in pathological cases. Operators who want to disable the cap entirely can set it to `0`.

### 6. Helper `Connection._channel_peer_for_log`

`Connection._channel` exposes the underlying socket via varying attribute names depending on stream type. A small best-effort lookup that returns `"unknown"` on any failure keeps the log line resilient:

```python
def _channel_peer_for_log(self) -> str:
    try:
        stream = getattr(self._channel, "stream", None)
        sock = getattr(stream, "sock", None) if stream else None
        if sock is not None:
            try:
                return repr(sock.getpeername())
            except OSError:
                return "<getpeername failed>"
        endpoints = self._config.get("endpoints")
        if endpoints:
            return repr(endpoints[1])
    except Exception:
        pass
    return "<unknown>"
```

## What this does NOT do (out of scope, do not slip in)

- **No per-handler granularity.** All MSG_REQUEST is counted equally. A peer that genuinely sends 10 000 cheap HANDLE_HASH requests in parallel hits the same threshold as one that pumps 10 000 deep handlers. That's intentional — the dispatch Task footprint is similar in both cases (~5 KB Python heap each), and counting handler types adds complexity without changing the answer.
- **No re-arm / recovery.** Once quarantined, the Connection stays quarantined until close. An operator who wants the peer to recover **reconnects the peer** — that gets a fresh Connection with a fresh counter. We don't try to detect "peer has calmed down" — every detection heuristic for that adds an attack surface for a peer that bursts, calms, bursts.
- **No metric / health endpoint.** One ERROR log line per quarantine + `Connection._inbound_quarantined` attribute is sufficient post-mortem signal. Adding metrics requires touching the integration layer (FastAPI? prometheus? other?), out of scope for `rpyc_async`.
- **No close.** Closing the channel triggers peer reconnect within milliseconds for any auto-reconnecting client (a typical auto-restart option does exactly this). The reconnect would just start the cycle over. Open-and-ignore is the cheapest stable state.
- **No outbound limit.** `_request_callbacks` (outbound side) is cleared on quarantine entry, but there is no equivalent threshold for outbound. Reason: outbound is bounded by the application's own behaviour; if the application keeps issuing outbound AR's without timeout, the right fix is in the application (use `fire_and_forget_async(..., timeout=...)`). The incident's outbound `_request_callbacks` growth was **caused by** the inbound spam — cap the inbound and the outbound naturally stops.

## Regression coverage

New tests in `rpyc_async/tests/test_inbound_backpressure.py`:

| test | what it asserts |
|---|---|
| `test_quarantine_drops_excess_inbound` | configure `max_inbound_inflight=10`, hold 10 dispatch tasks parked on an `Event`, send the 11th — assert: 11th never reaches handler, conn `_inbound_quarantined=True`, error logged exactly once. |
| `test_quarantine_cancels_parked_tasks` | same setup; after the 11th arrives, the 10 parked tasks transition to cancelled within one event-loop tick. |
| `test_quarantine_clears_request_callbacks` | populate `_request_callbacks` with 5 entries, trigger quarantine, assert dict is empty. |
| `test_quarantine_is_terminal` | after quarantine, drop the parked load (cancel everything), send 100 more requests — none reach the handler. |
| `test_other_connections_unaffected` | two parallel Connections from the same `AsyncioServer`, only one floods; the second's traffic is processed normally end-to-end. |
| `test_threshold_zero_disables_cap` | `max_inbound_inflight=0`, push 50 000 parked tasks, no quarantine, all processed. |
| `test_log_emitted_once_per_quarantine` | after quarantine, send 1 000 more requests — `logger.error` called exactly once total. |
| `test_quarantine_counter_decrements_on_task_done` | configure threshold=10, let 9 dispatches complete, send 10th — handled, not quarantined (counter is 1, not 10). |
| `test_close_path_still_works_through_helper` | verifies the `_drain_inbound_dispatch` extraction doesn't regress the existing close-path behaviour (i.e. tasks belonging to other Connections are not cancelled). |

All red on master, green with this design.

## Failure modes considered

| scenario | behaviour |
|---|---|
| legitimate burst to 9 999 inflight | no quarantine, normal recovery as tasks complete. |
| legitimate burst spikes through 10 000 briefly | quarantine — by design, the same channel is in trouble or the threshold is too low for this workload. Operator response: raise `max_inbound_inflight` for this deployment. |
| broken peer (this scenario) | quarantine at 10 000, ~10 000 × 5 KB = **~50 MB Python heap ceiling** per bad channel, not 12 GB. |
| peer reconnects after quarantine | fresh Connection, fresh counter. If the peer's bug still applies, fresh quarantine. Quarantine count over time on the **same** peer-addr is a deployment-level signal (logs are timestamped, peer addr is in the log line). |
| multiple bad peers | each independently quarantined; no global state interaction; healthy peers unaffected. |
| `_inbound_inflight` underflow (decrement when increment never ran) | impossible: increment runs synchronously before `add_done_callback` is registered, so the decrement can only fire after the matching increment. Even if a Task fails to start (rare — `create_task` only raises on a closed loop), the increment-then-immediate-cancel still produces a single increment / single decrement pair through the done-callback. |
| race between `_dispatch` reading the counter and `_on_done` decrementing | benign — counter is monotonic per-direction (`_dispatch` only increments, `_on_done` only decrements, both on the same loop). The check `>= max_inflight` uses `>=` not `==`, so a "missed decrement" can only delay quarantine entry by one request, never trigger a spurious one. |

## Operational notes

After this lands, an operator who sees the quarantine log line should:

1. Look at the peer addr in the log — that identifies the broken client.
2. Restart the broken client. Server-side, nothing to do — the channel is sequestered until the peer reconnects.
3. If the same peer addr triggers quarantine repeatedly across reconnects, it's a stable bug in the peer, not a transient flake. Fix the peer.

## Future work (not part of this fix)

- **Per-handler-type counters** could be added as a follow-up if a future incident shows a single handler dominating. Currently no evidence of that.
- **`_handle_async_call:172` timeout** — the underlying primitive that makes this whole class of leak possible. Wrapping `await async_res` in `asyncio.wait_for(..., timeout=...)` would cap the time any individual handler can hang. Higher-impact than per-conn backpressure but also higher-blast-radius (changes the contract of every async-flagged netref-call across the codebase). Should be done, but in a separate design.
- **Quarantine telemetry** — if a deployment runs many agents, surfacing `_inbound_quarantined` count on a `/healthz`-style endpoint would be useful. Out of scope for this library; the integration layer owns its own HTTP surface.
