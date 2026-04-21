# Design: AsyncioServer client must use `async_connect`, never `rpyc.connect`

Status: **Draft** — implementation in the same change.

## 1. Context

`rpyc.connect(host, port)` is synchronous. It performs a blocking
`socket.connect()`, blocking handshake, and returns a `Connection` whose
default `sync_request` path serves the socket with a blocking `select()`
inside `Connection.serve()`.

If a user calls `rpyc.connect()` from inside an asyncio coroutine to talk
to an `AsyncioServer`, the event loop is blocked:

1. During `socket.connect()` — tens to hundreds of milliseconds on real
   networks, enough to miss timers and starve other connections.
2. During every later `sync_request` (including the implicit
   `sync_request(HANDLE_GETROOT)` triggered by `conn.root`) — blocks the
   loop for one RTT, multiplied by every access.

The project already ships `rpyc.core.async_connect.async_connect`, which
is the correct path. It uses `loop.sock_connect()` (non-blocking) and
auto-enables asyncio serving. But it has five defects that together make
it easy for a user to fall back to the blocking path by accident:

1. Not exported at `rpyc.async_connect`. Imports are awkward, and most
   users default to `rpyc.connect`.
2. Dead code: a whole `AsyncioStream` class in `rpyc/core/async_connect.py`
   that `async_connect` does not use. It confuses readers into thinking the
   module's stream layer is async-native when it is not.
3. After `async_connect` returns, the connection uses a plain
   `SocketStream` with blocking `read()/write()`. It works only because
   `enable_asyncio_serving` re-routes all reads through `loop.add_reader`.
   Any code path that still calls `sync_request` (e.g. `conn.root`,
   `conn.ping`, third-party helpers) will block the loop. Nothing stops
   that silently.
4. Documentation (README, migration guide, examples) recommends
   `rpyc.connect()` everywhere, including in async contexts.
5. The "eager handshake" promise in `async_connect`'s docstring is not
   implemented: `_remote_root` is `None` after `async_connect` returns, so
   the first `conn.root` access does a blocking `sync_request`. Test
   `test_async_connect_root_ready_immediately` has been failing on master.

The project-wide constraint also applies: **no polling**. All waits on
the AsyncioServer/async_connect path must be event-driven.

## 2. Goal

An asyncio user connecting to an `AsyncioServer` must find one obvious,
non-blocking API, get a connection whose `root` is ready without blocking,
and be told loudly (runtime error) if any downstream code tries to slip
into a blocking `sync_request` from inside the running loop.

## 3. Non-goals

- Not changing `rpyc.connect()` semantics for sync callers. Threaded
  clients keep working as before.
- Not making `SocketStream` itself asyncio-native. That would be a much
  larger refactor and is unnecessary — `add_reader`-based dispatch and an
  async request path (`async_request` + awaitable `AsyncResult`) already
  cover all the code paths we need.

## 4. Design

### 4.1 Export `async_connect` at package top level

Add `async_connect` to `rpyc/__init__.py` so that
`rpyc.async_connect(...)` works. This is the one-liner that fixes
discoverability.

### 4.2 Runtime guard against blocking `sync_request` in the running loop

`Connection.sync_request` acquires `_recvlock` and calls
`Connection.serve()`, which does a blocking `select()`/`recv()` inside
`_channel.poll()`. Called from inside a running asyncio loop on an
asyncio-enabled connection, this deadlocks or, at best, blocks the loop.

Add a guard at the top of `sync_request`:

```python
if self._asyncio_enabled:
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if running is self._asyncio_loop:
        raise RuntimeError(
            "sync_request called from the asyncio loop that serves this "
            "connection. This would block the loop and can deadlock.\n"
            "Use an async alternative:\n"
            "  * `await conn.async_request(handler, *args)` — generic\n"
            "  * `await conn.root.method()` — async method call\n"
            "  * `conn.aclose()` instead of `conn.close()` in async code\n"
        )
```

Key points:

- Only fires when asyncio serving is enabled **and** the caller is running
  on **the same loop**. Sync callers (threaded code, `close()` invoked
  during loop teardown, `__del__` on process exit) pass the guard.
- Does not depend on heuristics (no `inspect.stack()` sniffing). The
  invariant is precise: "are we running in the loop that owns this
  connection's FD?"
- The guard is a defence-in-depth measure. The primary fix (eager root)
  removes the last in-tree blocking call from the async path, but the
  guard catches any third-party code that reaches for `sync_request`.

### 4.3 Eager handshake in `async_connect` (fixes `_remote_root is None`)

After `enable_asyncio_serving(loop=loop)`, pre-fetch the root:

```python
conn._remote_root = await conn.async_request(consts.HANDLE_GETROOT)
```

- Uses the awaitable `AsyncResult` path (already NO-POLLING, event-driven).
- If the server side raises, `async_connect` raises too, with the socket
  torn down — same error semantics as `rpyc.connect()` for a handshake
  failure.
- After this point, `conn.root` never needs `sync_request`.

### 4.4 `Connection.aclose()` — async close

`Connection.close()` calls `sync_request(HANDLE_CLOSE)`. With the guard
from §4.2, calling `close()` from the serving loop would raise. Provide
an async alternative that sends the close request via `async_request` and
awaits it:

```python
async def aclose(self) -> None:
    if self._closed:
        return
    # 1. Best-effort drain of pending deletions (async path).
    try:
        await self._process_pending_deletions()
    except Exception:
        pass
    # 2. Send HANDLE_CLOSE asynchronously.
    try:
        await asyncio.wait_for(
            self.async_request(consts.HANDLE_CLOSE),
            timeout=self._config.get("sync_request_timeout") or 5.0,
        )
    except (asyncio.TimeoutError, EOFError, Exception):
        pass
    # 3. Local cleanup (sync, no I/O).
    self._closed = True
    self.disable_asyncio_serving()
    self._cleanup(_anyway=True)
```

`close()` still works for sync callers. `aclose()` is the async path.

Callers inside `AsyncioServer._serve_connection` currently call `close()`
from `_handle_client`'s `finally`. That runs in the serving loop, so with
the new guard it would start raising. Switch that path to `aclose()`.

### 4.5 Delete dead `AsyncioStream`

`AsyncioStream` in `rpyc/core/async_connect.py` is unused by
`async_connect`. Its `read()`/`write()` are misleading: they call
`loop.run_until_complete()`, which is forbidden inside a running loop.
Remove the class. Keep only `async_connect` and its helpers.

No deprecation shim — no known users (grep confirms zero imports).

### 4.6 Documentation

- `README.rst`: replace the AsyncioServer client example with
  `await rpyc.async_connect(...)` and add an explicit warning: "do not use
  `rpyc.connect()` from async code".
- `docs/ASYNCIO_SERVER_MIGRATION.md`: new top-level section
  "Clients: use `rpyc.async_connect`, never `rpyc.connect`" with the
  blocking-loop rationale.
- `docs/EXAMPLES.md`, example scripts: switch async examples to
  `rpyc.async_connect`.

## 5. TDD plan

Failing tests go first:

1. **Export**: `assert callable(rpyc.async_connect)` and
   `inspect.iscoroutinefunction(rpyc.async_connect)`.
2. **Eager root**: after `await rpyc.async_connect(...)`,
   `conn._remote_root is not None`, and accessing `conn.root` does not
   issue any `sync_request` (spy on `Connection.sync_request`).
3. **sync_request guard**: inside a running loop on an asyncio-enabled
   connection, `conn.sync_request(...)` raises `RuntimeError` with a
   message naming the async alternative.
4. **Sync callers unchanged**: `rpyc.connect(...)` + `sync_request` still
   works in threaded/sync code. No guard false-positives.
5. **`aclose()`**: `await conn.aclose()` closes cleanly from inside the
   serving loop; `conn.closed` is `True` afterwards; no blocking.
6. **AsyncioStream gone**: `assert not hasattr(rpyc.core.async_connect,
   "AsyncioStream")`.
7. **NO-POLLING still enforced**: existing `test_no_polling_policy.py`
   keeps passing.

These live in `tests/test_async_connect_policy.py` (new) and in additions
to `tests/test_async_connect.py`. The already-failing
`test_async_connect_root_ready_immediately` flips to green as a free
bonus.

## 6. Risk & rollback

- Risk: the `sync_request` guard could false-positive in unexpected
  callers. Mitigated by the precise condition ("same running loop as the
  connection owns"). A user on a *different* loop, or in a sync thread,
  is unaffected.
- Risk: `aclose()` divergence from `close()`. Mitigated by sharing
  `_cleanup()` at the tail — the only difference is the request path.
- Rollback: the change set is self-contained in `async_connect.py`,
  `protocol.py`, `async_server.py`, `__init__.py`, and docs. Reverting
  the commit restores previous behavior.

## 7. Out of scope

- Making `SocketStream.read/write` truly async. Not needed: asyncio
  serving routes all reads via `add_reader`; writes are short buffered
  sends on the same socket. If writes ever become a latency issue, revisit
  with `loop.sock_sendall`.
- `ssl_connect` async variant. Tracked separately.
- Merging `AsyncioServer` client convenience helpers (`async with
  rpyc.async_connect(...) as conn`). Simple follow-up.
