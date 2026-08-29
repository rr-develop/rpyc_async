"""Regression tests for a blocking netref on the event loop.

Three production outages had the same
shape: a FastAPI handler touched ``conn.root.<attr>`` while running ON the
asyncio loop that serves that very connection. ``netref.__getattribute__``
is unconditionally synchronous::

    # rpyc_async/core/netref.py
    return syncreq(self, consts.HANDLE_GETATTR, name)

``syncreq`` → ``Connection.sync_request`` → ``AsyncResult.wait()``, whose body
is ``while self._waiting(): self._conn.serve(...)``. That loop drives the
socket from inside the coroutine, so the event loop never gets control back:
uvicorn stopped accepting connections and the whole web UI died until SIGKILL.

The peer does not have to be dead. In one outage the agent was alive and had
simply not answered yet (its in-flight task had been cancelled shortly before),
which is exactly what ``exposed_never_answers`` models here.

What these tests pin down:

1. ``async_call`` keeps the loop responsive while a request is outstanding
   against a peer that never answers (this is the outage scenario).
2. ``async_call`` returns real values for both ``async def`` and plain ``def``
   remote methods — the replacement must not be narrower than what it replaces.
3. ``async_call`` is cancellable / bounded by ``asyncio.wait_for``, unlike the
   blocking path where the timer itself cannot fire.

Liveness is measured by a ticker coroutine incrementing a counter on
``asyncio.sleep``. If the loop is blocked the ticker cannot advance. The
assertion is on the ticker advancing, never on wall-clock timing, so the test
does not poll and is not timing-flaky.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import sys
import time

import pytest

import rpyc_async as rpyc
from rpyc_async.core import consts, netref
from rpyc_async.core.async_connect import async_connect
from rpyc_async.utils.async_server import AsyncioServer


# ---------------------------------------------------------------------------
# Service under test
# ---------------------------------------------------------------------------

class _SlowPeerService(rpyc.Service):
    """A peer that is alive and reachable but does not answer some calls."""

    async def exposed_never_answers(self) -> int:
        # Alive, connected, simply never replies — the outage shape.
        await asyncio.sleep(999999)
        return 42

    async def exposed_async_echo(self, value: int) -> int:
        return value * 2

    def exposed_sync_echo(self, value: int) -> int:
        # Deliberately NOT ``async def``: async_call must handle plain
        # remote methods too, otherwise it cannot replace the sync path.
        return value * 3

    def _rpyc_getattr(self, name: str):
        # Stall inside ATTRIBUTE RESOLUTION itself. This is the exact frame
        # py-spy caught in production (``HANDLE_GETATTR`` -> ``_handle_getattr``),
        # and it is reachable because ``sync_request`` whitelists GETATTR as
        # "short, deterministic, cache-backed" -- an assumption that does not
        # hold once the peer runs real code to resolve an attribute.
        if name == "stalls_on_getattr":
            time.sleep(999999)
        return getattr(self, "exposed_" + name)


def _run_server(port: int, ready_event) -> None:
    async def server_main() -> None:
        server = AsyncioServer(
            _SlowPeerService,
            port=port,
            protocol_config={"allow_all_attrs": True, "allow_public_attrs": True},
        )
        ready_event.set()
        await server.serve_forever()

    try:
        asyncio.run(server_main())
    except KeyboardInterrupt:
        pass
    except Exception as exc:  # pragma: no cover - diagnostic only
        print(f"server error: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------

class _Ticker:
    """Event-driven liveness probe: counts turns of the event loop."""

    def __init__(self) -> None:
        self.count = 0
        self._task: asyncio.Task | None = None
        self._moved = asyncio.Event()

    async def _run(self) -> None:
        while True:
            # sleep(0) would spin; a real (small) delay keeps this
            # event-driven and cheap while still advancing promptly.
            await asyncio.sleep(0.01)
            self.count += 1
            self._moved.set()

    def start(self) -> None:
        self._task = asyncio.get_running_loop().create_task(self._run())

    async def wait_for_progress(self, timeout: float) -> bool:
        """Await the NEXT tick. False if the loop is wedged."""
        self._moved.clear()
        try:
            await asyncio.wait_for(self._moved.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


@pytest.fixture
def peer_port():
    # Let the OS pick a free port, then hand it to the child process.
    import socket

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture
def peer(peer_port):
    """Start the peer in its own process; always reap it."""
    ready = multiprocessing.Event()
    proc = multiprocessing.Process(target=_run_server, args=(peer_port, ready))
    proc.start()
    try:
        if not ready.wait(timeout=10.0):
            raise RuntimeError("peer failed to start")
        yield peer_port
    finally:
        # Guaranteed teardown: this test must never leave a stray process.
        proc.kill()
        proc.join(timeout=10)


async def _connect(port: int):
    return await asyncio.wait_for(
        async_connect("127.0.0.1", port, config={"allow_all_attrs": True}),
        timeout=10.0,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_call_keeps_loop_alive_when_peer_never_answers(peer):
    """THE regression: an unanswered call must not wedge the event loop.

    On the blocking path (``root.never_answers()``) this fails: the loop stops
    turning entirely and the ticker never advances again.
    """
    conn = await _connect(peer)
    ticker = _Ticker()
    ticker.start()
    try:
        assert await ticker.wait_for_progress(timeout=5.0), "loop dead before test"

        # async_call returns an AsyncResult (awaitable, not a coroutine),
        # so wrap it before handing it to create_task.
        async def _issue():
            return await netref.async_call(conn.root, "never_answers")

        pending = asyncio.get_running_loop().create_task(_issue())
        try:
            # The call is outstanding and will never be answered. The loop
            # must keep serving other work regardless.
            for _ in range(3):
                assert await ticker.wait_for_progress(timeout=5.0), (
                    "event loop stopped while an RPC was outstanding — "
                    "the netref call is blocking the loop"
                )
            assert not pending.done()
        finally:
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
    finally:
        await ticker.stop()
        await conn.aclose()


@pytest.mark.asyncio
async def test_async_call_returns_value_for_async_and_sync_methods(peer):
    """The non-blocking path must not be narrower than the blocking one."""
    conn = await _connect(peer)
    try:
        assert await asyncio.wait_for(
            netref.async_call(conn.root, "async_echo", 21), timeout=10.0
        ) == 42
        assert await asyncio.wait_for(
            netref.async_call(conn.root, "sync_echo", 14), timeout=10.0
        ) == 42
    finally:
        await conn.aclose()


@pytest.mark.asyncio
async def test_async_call_is_bounded_by_wait_for(peer):
    """``asyncio.wait_for`` must actually be able to fire.

    On the blocking path the timer cannot run, because the loop that would
    fire it is the loop being blocked. That is why ``timeout=5.0`` in
    ``fire_and_forget_async`` never saved the web server.
    """
    conn = await _connect(peer)
    ticker = _Ticker()
    ticker.start()
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                netref.async_call(conn.root, "never_answers"), timeout=0.5
            )
        # And the loop is still healthy afterwards.
        assert await ticker.wait_for_progress(timeout=5.0)
    finally:
        await ticker.stop()
        await conn.aclose()


@pytest.mark.asyncio
async def test_async_call_propagates_remote_errors(peer):
    """A missing attribute must surface as an error, not hang or vanish."""
    conn = await _connect(peer)
    try:
        with pytest.raises(Exception):
            await asyncio.wait_for(
                netref.async_call(conn.root, "no_such_method"), timeout=10.0
            )
    finally:
        await conn.aclose()


@pytest.mark.asyncio
async def test_async_call_uses_a_single_round_trip(peer):
    """getattr+call must go out as ONE request.

    Resolving the attribute separately would reintroduce the blocking
    ``HANDLE_GETATTR`` that the py-spy stacks caught in all three outages.
    """
    conn = await _connect(peer)
    sent: list[int] = []
    original = conn.async_request

    def _recording(handler, *args, **kwargs):
        sent.append(handler)
        return original(handler, *args, **kwargs)

    conn.async_request = _recording  # type: ignore[method-assign]
    try:
        assert await asyncio.wait_for(
            netref.async_call(conn.root, "async_echo", 21), timeout=10.0
        ) == 42
        assert sent == [consts.HANDLE_ASYNC_CALLATTR], (
            f"expected exactly one ASYNC_CALLATTR request, got {sent}"
        )
    finally:
        conn.async_request = original  # type: ignore[method-assign]
        await conn.aclose()


@pytest.mark.asyncio
async def test_blocking_attribute_access_wedges_the_loop(peer):
    """Baseline: this is the defect ``async_call`` exists to avoid.

    Touching ``conn.root.<attr>`` on the serving loop, against a peer that
    stalls while resolving that attribute, freezes the ENTIRE loop until
    ``sync_request_timeout`` expires. Nothing else runs meanwhile -- which is
    how one browser request took the whole web UI down three times.

    The test asserts the loop is starved, then that ``async_call`` on the same
    peer and the same connection keeps it alive. If a future change ever makes
    the blocking path safe, this test fails loudly and should be revisited
    rather than deleted.
    """
    conn = await _connect(peer)
    # Keep the freeze short: the default sync_request_timeout is 30 s.
    conn._config["sync_request_timeout"] = 2
    ticker = _Ticker()
    ticker.start()
    try:
        assert await ticker.wait_for_progress(timeout=5.0), "loop dead before test"

        before = ticker.count
        started = time.monotonic()
        with pytest.raises(Exception):
            # Blocking getattr, exactly as production did it.
            conn.root.stalls_on_getattr
        blocked_for = time.monotonic() - started
        starved = ticker.count - before

        assert blocked_for >= 1.0, (
            f"expected the blocking path to stall, took only {blocked_for:.2f}s"
        )
        assert starved == 0, (
            f"expected the loop to be starved while blocked, but it ticked "
            f"{starved} times in {blocked_for:.2f}s"
        )

        # Same connection, same peer: the async path stays responsive.
        assert await ticker.wait_for_progress(timeout=5.0)
        pending = asyncio.get_running_loop().create_task(
            _await_async_call(conn, "stalls_on_getattr")
        )
        try:
            for _ in range(3):
                assert await ticker.wait_for_progress(timeout=5.0), (
                    "async_call starved the loop the same way the blocking "
                    "path does -- the fix is not effective"
                )
        finally:
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
    finally:
        await ticker.stop()
        await conn.aclose()


async def _await_async_call(conn, name, *args):
    return await netref.async_call(conn.root, name, *args)


@pytest.mark.asyncio
async def test_async_call_works_on_plain_local_objects():
    """Non-netref objects fall back to a direct call.

    Keeps ``async_call`` usable with test doubles and in-process stand-ins:
    their attribute access is local and cannot block on a peer, so the
    hazard this helper guards against does not apply.
    """
    class Local:
        async def async_method(self, x):
            return x * 2

        def sync_method(self, x):
            return x * 3

    obj = Local()
    assert await netref.async_call(obj, "async_method", 21) == 42
    assert await netref.async_call(obj, "sync_method", 14) == 42


@pytest.mark.asyncio
async def test_local_fallback_is_lazy():
    """Nothing runs until the result is awaited, as with the netref path."""
    calls = []

    class Local:
        def go(self):
            calls.append(1)
            return "done"

    pending = netref.async_call(Local(), "go")
    assert calls == [], "async_call must not invoke the method eagerly"
    assert await pending == "done"
    assert calls == [1]


@pytest.mark.asyncio
async def test_local_fallback_propagates_errors():
    class Local:
        def boom(self):
            raise ValueError("nope")

    with pytest.raises(ValueError, match="nope"):
        await netref.async_call(Local(), "boom")
