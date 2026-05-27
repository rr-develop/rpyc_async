# DESIGN: eliminate ALL polling / busy-loops from AsyncioServer reads

**Status:** design. Branch: `async_support`.
**Author context:** a downstream application's server pegged a CPU core at 99.9% — a
half-closed inbound socket (CLOSE-WAIT, FIN pending) made the asyncio loop fire
the rpyc read callback ~34 000×/sec while it issued **zero `recv` syscalls**.
Band-aids (`poll(0)` guards, `MSG_PEEK` EOF probes, EOF/partial-frame `close()`
paths) treated symptoms. This document shows the **root cause is a polling
read design**, proves a fully event-driven design is possible, and specifies
it. The mandate: **no `poll(0)`, no `while poll(...)`, no busy/semi-busy loops
anywhere AsyncioServer touches.**

---

## 0. The non-negotiable rule

`AsyncioServer` and everything it uses must wake **only** on real readiness
events from the OS, via:

- `loop.sock_accept(sock)` — wait for an incoming connection,
- `loop.add_reader(fd, cb)` — wake when an fd is readable,
- `asyncio.Event` / `loop.call_soon` — cross-coroutine / cross-thread signalling.

Forbidden anywhere on the asyncio path: `socket/select.poll(...)`,
`select.select(...)`, `channel.poll(...)`, `stream.poll(...)`, `while cond:
await sleep(x)`, `MSG_PEEK`-probing, or "check if more is readable" calls.
`loop.add_reader` already IS the readiness event — re-asking the OS "is there
more?" with `poll(0)` is the bug.

---

## 1. Root cause — why `poll(0)` is there at all

The async read callback (`Connection.enable_asyncio_serving.on_readable`) tries
to assemble **whole rpyc frames synchronously inside one callback**:

```python
def on_readable():
    while self._channel.poll(0):          # ← POLLING the socket
        data = self._channel.recv()       # ← channel.recv → stream.read(N)
        self._dispatch(data)
```

`channel.recv()` (`channel.py:49`) does:

```python
header = self.stream.read(self.FRAME_HEADER.size)        # read EXACTLY 5 bytes
length, compressed = self.FRAME_HEADER.unpack(header)    # Struct("!LB")
data = self.stream.read(length + len(self.FLUSHER))[:-1] # read EXACTLY length+1
```

and `SocketStream.read(count)` (`stream.py:264`) is a **blocking, read-exactly-N
loop** of `sock.recv()`. So:

1. One `add_reader` wakeup may carry only a *partial* frame. `read(N)` would
   then block — which is illegal in an asyncio callback. To avoid blocking, the
   code guards every `recv()` with `poll(0)` ("is a full read available?").
2. But `poll(0)` cannot answer "is a *whole frame* available" — only "are *some*
   bytes readable". On a half-closed socket the pending **EOF** makes the fd
   permanently readable, so `poll(0)` is forever `True` (or the framing can't
   complete), and the callback either spins (`while poll(0)`) or returns having
   done nothing while `add_reader` immediately re-fires it. Either way: a CPU
   busy-loop the OS event the design was supposed to rely on.

**The polling exists only to paper over a blocking, frame-synchronous reader.**
Remove the blocking framing and the polling disappears with it.

---

## 2. Proof it is possible (it already is, elsewhere)

This is exactly the problem `asyncio.Protocol` solves and has solved for a
decade: the loop calls `data_received(chunk)` with **whatever bytes arrived**,
the protocol appends to a buffer, extracts whole frames it can, and keeps the
remainder for the next call. `eof_received()` / `connection_lost()` handle
close. No protocol implementation ever polls the socket. rpyc's own sync path
also never busy-polls: it uses `poll(timeout>0)` — a **blocking wait with a real
timeout** (one `select`/`poll` syscall that sleeps until readable or the
deadline), which is event-driven, not a spin.

The framing is **fully determinable from buffered bytes**: a complete frame is
`5 (FRAME_HEADER) + length + 1 (FLUSHER)` bytes, and `length` is in the header.
Given a byte buffer you can always decide "do I have ≥ one whole frame?"
**without touching the socket.** Therefore a poll-free async reader is not just
possible — it is the standard, and the current code is the anomaly.

---

## 3. The design — buffered, edge-driven, zero polling

### 3.1 A non-blocking single-shot socket read

Add to the stream a **non-blocking, read-what's-there** primitive (distinct from
the blocking exact-N `read`):

```python
# SocketStream
def recv_available(self) -> bytes:
    """Read whatever is in the socket buffer, right now, without blocking.
    Returns b'' ONLY on real EOF (peer closed). Raises EOFError on a hard
    socket error. Never blocks, never polls."""
    self.sock.setblocking(False)
    try:
        chunk = self.sock.recv(self.MAX_IO_CHUNK)
    except (BlockingIOError, InterruptedError):
        return None          # nothing right now (spurious/again) — sentinel
    except socket.error as ex:
        self.close(); raise EOFError(ex)
    if chunk == b"":
        return b""           # EOF: peer closed its write end
    return chunk
```

`b""` = definitive EOF (orderly shutdown). `None` = EAGAIN (benign spurious
wakeup — return, do not close, do not spin: a healthy fd only re-fires on real
data). Non-empty = data. **One syscall, no poll.**

### 3.2 A frame buffer on the Channel (or Connection)

Maintain a per-connection `bytearray` and a tiny incremental framer:

```python
self._inbuf = bytearray()

def _feed_and_extract(self, chunk: bytes) -> list[bytes]:
    """Append chunk; return every COMPLETE frame now available. Partial
    remainder stays in _inbuf for the next wakeup. No socket access."""
    self._inbuf += chunk
    frames = []
    H = Channel.FRAME_HEADER.size            # 5
    F = len(Channel.FLUSHER)                  # 1
    while len(self._inbuf) >= H:
        length, compressed = Channel.FRAME_HEADER.unpack_from(self._inbuf, 0)
        total = H + length + F
        if len(self._inbuf) < total:
            break                             # frame not fully arrived yet
        payload = bytes(self._inbuf[H:H+length])
        del self._inbuf[:total]
        if compressed:
            payload = zlib.decompress(payload)
        frames.append(payload)
    return frames
```

This is pure in-memory work — it can never block and never polls. A partial
frame simply waits in `_inbuf`; the next `add_reader` wakeup (a real OS event,
because more bytes actually arrived) resumes it.

### 3.3 The event-driven `on_readable` (no poll, no while-poll)

```python
def on_readable():
    try:
        chunk = self._stream.recv_available()
    except EOFError:
        self._close_and_remove_reader(); return
    if chunk is None:           # EAGAIN: benign spurious wakeup
        return                  #   -> just return; NOT a spin (no re-arm storm,
                                #      the loop only re-fires on real readiness)
    if chunk == b"":            # EOF: peer closed
        self._close_and_remove_reader(); return
    for frame in self._feed_and_extract(chunk):
        self._dispatch(frame)
    # loop is level/edge-managed by asyncio; we do NOT re-check the socket.
```

No `poll(0)`. No `while self._channel.poll(0)`. No `MSG_PEEK`. Exactly **one**
`recv` syscall per wakeup. EOF closes deterministically.

### 3.4 Deterministic reader removal (the other half of "no spin")

The current `disable_asyncio_serving` removes `self._registered_fd`. To
guarantee a half-closed fd can NEVER re-fire (even across `close()`
idempotency / fd reuse), removal must happen **directly when EOF is detected**,
not only via `close()`:

```python
def _close_and_remove_reader(self):
    # Remove the reader FIRST and UNCONDITIONALLY (independent of self._closed),
    # so a stale/duplicate reader on a half-closed fd cannot survive a
    # no-op close(). Then close the connection.
    if self._asyncio_loop and self._registered_fd is not None:
        try: self._asyncio_loop.remove_reader(self._registered_fd)
        except Exception: pass
        self._registered_fd = None
        self._loop_fd_registered = False
    self.close()
```

Removing the reader is the event-driven act that stops the loop firing — it must
not depend on `close()`'s idempotency guard (the live production spin was
exactly a reader that outlived a no-op `close()`).

### 3.5 EOF-during-handshake / `fileno()` on a closed stream

`stream.poll()` used to raise `EOFError` from `fileno()` on a closed socket; the
new path never calls `poll()`/`fileno()` in the hot loop. If `recv_available`
sees a closed socket it raises `EOFError` → `_close_and_remove_reader`. The
"EOF from the poll() condition" guard becomes unnecessary and is removed.

---

## 4. What about the sync `serve()` paths?

`Connection.serve()` / `poll(timeout)` (`protocol.py:2596,2829`) and the
threaded server use `channel.poll(timeout>0)` — a **blocking wait with a real
timeout** (a single `select`/`poll` that sleeps until readable or the deadline).
That is event-driven (it does not spin) and is the *sync* API for
threaded/blocking clients. It is **out of scope**: `AsyncioServer` must not call
`serve()`/`poll()` at all on its event loop. The fix is confined to the asyncio
read path (`enable_asyncio_serving` + the new buffered reader). We additionally
assert (test) that no `AsyncioServer` code path invokes `serve()`/`poll(0)`.

The only legitimate `poll` with `timeout=0` left anywhere would be a true
"non-blocking peek for the sync API" — and even that is replaced by
`recv_available` returning `None`. Goal: **`poll(0)` deleted from the async
path entirely.**

---

## 5. TDD plan (no mocks where it matters)

Framer (pure, fast):
- empty buffer → no frames; partial header (<5B) → no frames, buffer retained;
- one whole frame → exactly one payload, buffer empty;
- 1.5 frames in one chunk → first frame extracted, half retained;
- frame split across N chunks → assembled only when complete (N-1 wakeups
  yield nothing, the Nth yields it);
- compressed frame round-trips.

Reader (real socketpair, NO mocks — these are the regression tests for the
99.9%-CPU bug):
- **half-closed + partial frame** (peer writes 3 bytes then `SHUT_WR`): reader
  fires a BOUNDED number of times (≈1), connection closes, fd removed. (Today:
  34k/s forever.)
- **half-closed + whole frame + EOF**: frame dispatched, then EOF closes.
- **benign EAGAIN wakeup** (reader invoked with nothing readable): returns,
  does NOT close, does NOT spin.
- **streamed partial frames** over several real writes: each real write causes
  exactly one wakeup; no wakeup without a write (assert via an
  `add_reader`-fire counter == number of writes + close).
- **no-recv-spin assertion**: over a 1s window on a half-closed socket, count
  `recv`/`epoll` activity is bounded (e.g. < 10), proving the loop is asleep.

Policy test (extend `tests/test_no_polling_policy.py`):
- static-scan `enable_asyncio_serving` / `on_readable` source for `poll(0)`,
  `while .*poll`, `MSG_PEEK`, `select(` → assert ABSENT.

End-to-end (real AsyncioServer + async_connect, no mocks):
- a client connects, half-closes; server CPU stays ~0 (utime jiffies over 2s
  below a small bound) and the server keeps serving other connections.

---

## 6. Migration / risk

- The change is confined to `Connection.enable_asyncio_serving` + a new
  `SocketStream.recv_available` + a per-connection frame buffer. `channel.recv`
  / `stream.read` (blocking exact-N) stay for the sync path untouched.
- Compression, FLUSHER, header format are unchanged — the framer reuses
  `Channel.FRAME_HEADER` / `FLUSHER` / `zlib`, so the wire format is identical;
  only the *reading strategy* changes (buffer-and-frame vs poll-and-block).
- Backwards compatible: sync clients still use `serve()`/`poll(timeout)`.
- Removes every `poll(0)` band-aid added during the earlier incident
  (the `_peer_eof_for_busyloop_guard` MSG_PEEK probe, the `while poll(0)` drain,
  the partial-frame `except` close) — they are subsumed by the buffered reader.

---

## 7. Conclusion

It is **not** impossible — the opposite: polling is the anomaly here. rpyc
frames are self-delimiting (length-prefixed), so a buffered, `add_reader`-driven
reader that does one non-blocking `recv` per real readiness event, frames from
an in-memory buffer, and removes the reader on EOF is strictly correct and
**spends zero CPU when idle or half-closed**. The `poll(0)`/`while poll`
constructs exist only to support a blocking frame-synchronous read inside an
async callback; replacing that read removes them entirely.

---

## 8. Reader lifetime is bound to the socket (FD-reuse spin)

A second spin surfaced AFTER the buffered reader landed, with a different root
cause — see a related internal incident analysis (not included here).

**Problem.** The socket layer (`SocketStream.close()` → `sock.close()`) and the
reader registration (`disable_asyncio_serving` → `loop.remove_reader`) lived in
separate layers with no link. Any close path that bypassed
`Connection._cleanup` — a raw `stream.close()`, or simply dropping the last
external reference (the Connection can't be GC'd because the cycle
`loop → on_readable → Connection` pins it, so `__del__` never runs) — freed the
fd while the reader stayed armed. In a shared-loop process (uvicorn + rpyc on
one event loop, one fd table) the freed fd is recycled by uvicorn's own
transport; the orphaned reader then fires on a foreign fd and
`loop.remove_reader` raises `RuntimeError('fd is used by transport')` forever →
a fresh ~43k-epoll/sec, zero-recv busy-loop. (uvloop can even SIGSEGV on the
stale-reader/transport collision.)

**Rule.** *Closing the socket MUST unregister the reader, synchronously, while
the fd is still valid — no matter how the socket is closed.* Reader lifetime is
bound to the socket, not to the Connection close path.

**Mechanism.**
- `SocketStream` exposes `set_close_callback(cb)`; `close()` fires it FIRST,
  before `shutdown()`/`sock.close()`, so the listener unregisters the fd while
  it is still ours.
- `enable_asyncio_serving` registers `self._unregister_reader` as that hook.
- `_unregister_reader` is the single, idempotent removal primitive; it swallows
  EVERY `remove_reader` error including uvloop's `RuntimeError('used by
  transport')` — that error must NEVER reach the hot path.
- `on_readable` self-defends: `if self._closed or self._channel.closed:
  _unregister_reader(); return` — it never `recv`s a possibly-recycled fd.
- `async_connect`'s outer error handler tears the half-built Connection down
  via `disable_asyncio_serving` (not just `sock.close()`), closing the
  connect-time leak.

🚫 DO NOT close a SocketStream's fd on the asyncio path without going through a
reader-unregister. DO NOT narrow `_unregister_reader`'s `except` to specific
exception types — uvloop's transport-collision `RuntimeError` must stay caught.
