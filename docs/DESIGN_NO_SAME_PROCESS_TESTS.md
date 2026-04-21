# Design: AsyncioServer + rpyc client live in separate processes — always

Status: **Enforced** by `tests/test_no_same_process_server_client.py`.

## 1. The rule

Every test that starts an `AsyncioServer` and connects to it from an rpyc
client MUST put the server in a **different OS process** from the client.
Same-thread-different-loop is not an escape hatch. Cross-thread
ThreadedServer tricks that used to work on pip-rpyc are also disallowed:
this project tests only the one topology it supports.

```python
# ✅ The only supported pattern (see tests/support.py):
with mp_asyncio_server(service_factory) as port:
    async def go():
        conn = await rpyc.async_connect("127.0.0.1", port)
        ...
        await conn.aclose()
    asyncio.run(go())
```

```python
# ❌ FORBIDDEN — same loop, instant deadlock on first round-trip:
server = AsyncioServer(Svc, port=0)
await server.start()                           # server on THIS loop
conn = await rpyc.async_connect(...)           # client on THIS loop too
await conn.root.some_async_method()            # hangs forever
```

```python
# ❌ FORBIDDEN — cross-thread ThreadedServer tricks:
Thread(target=server.start, daemon=True).start()
conn = rpyc.connect(...)                       # different thread, same proc
```

## 2. Why same-loop deadlocks

Both `AsyncioServer`'s dispatch and the rpyc client register on the event
loop via `loop.add_reader(fd, cb)` and the peer-facing coroutines
(`loop.sock_accept`, `loop.sock_connect`). When they share a loop they
compete for the same single-threaded callback pump.

The first round-trip that requires the peer to answer before a local
future resolves — `HANDLE_GETROOT` (fired by the eager handshake in
`async_connect`), `HANDLE_INSPECT` (fired by `_netref_factory` while
unboxing a netref), any `HANDLE_ASYNC_CALL` — needs the server coroutine
and the client `add_reader` callback to *both* run. One blocks the other.

This is an architectural property of single-threaded cooperative
scheduling, not a bug in the dispatcher. It is **not** fixed by swapping
`asyncio.run_coroutine_threadsafe(coro, loop)` for
`loop.create_task(coro)` inside the `on_readable` callback: both
schedule the coroutine on the same loop, and the same loop still can't
run two competing things at once. (The existing
`run_coroutine_threadsafe` form is correct regardless: it is idempotent
from the loop thread, just with one extra `call_soon_threadsafe` wakeup
compared to `create_task`.)

## 3. Why cross-thread same-process is also out

1. Two loops in one process still share `asyncio` global state, CPython
   id-ranges, and the rpyc-side `_proxy_cache` / `_local_objects`
   registries if the client accidentally imports the same service class.
2. `debug_refcounting` in particular wires a logger through the
   `Connection` config; shared loggers between a "local" client and a
   "local" server that happen to live in one process produce false
   positives and false negatives that do not match how the code behaves
   in production (client and server always in different processes).
3. Putting the rule at the process boundary — not the loop boundary —
   makes it trivial to enforce with a grep / AST check.

## 4. Helper

`tests/support.py::mp_asyncio_server(service_factory, *, protocol_config=None)`:

* Starts an `AsyncioServer(service_factory())` in a fresh
  `multiprocessing.Process`.
* Waits for the child to publish `"ready"` on a `multiprocessing.Queue`.
* Yields the bound port.
* Terminates the child (SIGTERM; SIGKILL if needed) on exit.

`service_factory` must be a top-level, picklable callable that returns
the `Service` *class*. Local classes defined inside test methods do not
pickle on `spawn` start-method platforms; keep service classes at module
scope.

## 5. Enforcement

`tests/test_no_same_process_server_client.py` scans every `tests/test_*.py`
and fails if it finds either of these shapes:

* `AsyncioServer(...)` instantiated at module scope, i.e. outside a
  helper function that is later passed to `multiprocessing.Process`.
  This catches the "inline server + `await server.start()`" anti-pattern
  directly.
* `asyncio.create_task(server.start())` in the same file that also
  instantiates a real rpyc client (`rpyc.connect`, `async_connect`, or
  a netref access through `conn.root.*`). This catches same-loop tests.
* `Thread(target=... server.start ...)` or `server._start_in_thread()`
  — catches cross-thread same-process tricks.

Exceptions (files where `AsyncioServer(...)` appears at module scope
for legitimate reasons, e.g. mocking unit tests like
`test_no_polling_policy.py` that never call `.start()`) are gated by an
explicit allow-list in the enforcement test with the justification
written next to each entry.

## 6. Migration checklist

If you have an old test in this tree that runs server + client in one
process:

1. Move the `rpyc.Service` subclass to **module scope** in the test file.
2. Replace the server setup (`server = AsyncioServer(...); await
   server.start()` / threaded server) with:

   ```python
   with mp_asyncio_server(_my_service_factory) as port:
       async def go(port: int) -> None:
           conn = await rpyc.async_connect("127.0.0.1", port)
           try:
               ...
           finally:
               await conn.aclose()
       asyncio.run(go(port))
   ```

3. Drop any leftover `loop = asyncio.get_running_loop()` /
   `enable_asyncio_serving(loop=loop)` calls — `async_connect` handles
   both.
4. Drop any `conn.close()` / `conn.disable_asyncio_serving()` pair in
   favor of `await conn.aclose()`.

## 7. Why not fix same-loop instead

It is not fixable in the current dispatcher without trading one
single-threaded property for another:

* You can't run the server coroutine synchronously under an outer
  `await` because that would blow the `no-polling` policy — you can't
  busy-wait for the peer reply.
* You can't serialize the wire in a way that makes the two roles
  cooperative: the client `_on_readable` must dispatch a server reply
  *while the client is suspended awaiting the reply*; but it is
  suspended on the same loop that would also have to schedule the
  server reply.
* Running `asyncio.run(go())` inside another `asyncio.run(server_main())`
  is forbidden by asyncio.

Multi-process is the actual supported topology in production. Tests
follow production.
