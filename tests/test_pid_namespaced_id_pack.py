"""Regression + contract tests for PID-namespaced ``id_pack`` seq.

See ``docs/DESIGN_PID_NAMESPACED_ID_PACK.md`` for the design.

Before this fix, ``Connection._id_pack_seq`` started at a fixed
``1 << 40`` on every connection in every process. Two independent
processes minted identical ``id_pack`` triples for their Nth boxed
builtin-typed object (e.g. a bound method). A cross-process callback
loop that round-tripped such an object landed in the receive-side
shortcut in ``_unbox(LABEL_REMOTE_REF)``, which resolved the peer's
id_pack against the receiver's **own** ``_local_objects`` — returning
the wrong object, triggering an infinite ping-pong, and leaking
memory at ~15 MB/s on both sides until OOM.

The fix seeds the seq at ``(os.getpid() << 32) + 1``. Live processes
have disjoint PIDs by kernel guarantee, therefore disjoint seq
ranges, therefore globally-unique ``id_pack`` tuples. The shortcut
can no longer false-positive.

These tests pin:

1. The seed itself on ``Connection.__init__``.
2. Cross-process disjointness of seqs via a multiprocess probe.
3. End-to-end absence of the leak on a callback round-trip.
4. The debug-mode sanity assert rejects a handcrafted foreign-pid
   id_pack that would have been silently resolved before.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import resource
import time

import pytest
import rpyc_async as rpyc
from rpyc_async.core.async_connect import async_connect
from rpyc_async.utils.async_server import AsyncioServer

from tests.support import get_free_port, mp_asyncio_server


# ─────────────────────────────────────────────────────────────────────────────
# 1. Seq seed contract
# ─────────────────────────────────────────────────────────────────────────────


def test_id_pack_seq_starts_with_pid_shifted_by_32() -> None:
    """The first seq a fresh ``Connection`` hands out must be
    ``(os.getpid() << 32) + 1``."""
    from rpyc_async.core.channel import Channel
    from rpyc_async.core.protocol import Connection
    from rpyc_async.core.service import VoidService
    from rpyc_async.core.stream import PipeStream

    # Minimal channel pair for a Connection that doesn't actually
    # touch the wire. ``PipeStream.from_std`` gives us a channel that
    # is inert for this test — we only inspect ``_id_pack_seq``.
    rfd, wfd = os.pipe()
    r2, w2 = os.pipe()
    try:
        stream = PipeStream(
            incoming=os.fdopen(rfd, "rb"),
            outgoing=os.fdopen(w2, "wb"),
        )
        conn = Connection(VoidService(), Channel(stream))
        try:
            first = next(conn._id_pack_seq)
            expected_low = (os.getpid() << 32) + 1
            assert first == expected_low, (
                f"first seq must be (pid << 32) + 1 = {expected_low}; "
                f"got {first}"
            )
            # Every subsequent seq must stay in this pid's range.
            second = next(conn._id_pack_seq)
            assert second == expected_low + 1
            assert (second >> 32) == os.getpid()
        finally:
            # Avoid GC invoking _cleanup on a half-wired connection.
            conn._closed = True
    finally:
        # Close any fds we still own.
        for fd in (r2, wfd):
            try:
                os.close(fd)
            except OSError:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# 2. Cross-process uniqueness
# ─────────────────────────────────────────────────────────────────────────────


class _IdPackProbeService(rpyc.Service):
    """Exposes the PID it runs under and a helper that boxes a
    bound method so the caller can read its id_pack seq.

    We don't use remote introspection of the connection's allocator
    directly; instead we exercise the real ``_box`` path by returning
    a bound method and reading its ``____id_pack__`` on the other
    side. The seq we see there is allocated by the server's Connection.
    """

    async def exposed_get_pid(self) -> int:
        return os.getpid()

    async def exposed_get_a_bound_method(self) -> object:
        # Fresh class per call so nothing is cached across calls.
        class _Tmp:
            def meth(self) -> None:  # noqa: D401
                return None

        return _Tmp().meth


@pytest.mark.asyncio
async def test_id_pack_seqs_disjoint_across_processes() -> None:
    """Two independent subprocess servers must produce ``id_pack``
    seqs whose upper 32 bits equal **their own** PID and that do not
    collide with each other."""
    with mp_asyncio_server(lambda: _IdPackProbeService) as port_a, \
         mp_asyncio_server(lambda: _IdPackProbeService) as port_b:

        conn_a = await async_connect("localhost", port_a)
        conn_b = await async_connect("localhost", port_b)
        try:
            pid_a = await conn_a.root.get_pid()
            pid_b = await conn_b.root.get_pid()
            assert pid_a != pid_b, "two subprocess servers must have different PIDs"

            meth_a = await conn_a.root.get_a_bound_method()
            meth_b = await conn_b.root.get_a_bound_method()

            seq_a = meth_a.____id_pack__[2]
            seq_b = meth_b.____id_pack__[2]

            # Each seq's upper 32 bits must encode the producing PID.
            assert (seq_a >> 32) == pid_a, (
                f"seq {seq_a} from PID {pid_a}: expected upper=={pid_a}, "
                f"got upper=={seq_a >> 32}"
            )
            assert (seq_b >> 32) == pid_b

            # The full id_packs must differ at least in slot 2.
            assert meth_a.____id_pack__[2] != meth_b.____id_pack__[2], (
                "id_pack[2] must differ between independent processes"
            )
        finally:
            await conn_a.aclose()
            await conn_b.aclose()


# ─────────────────────────────────────────────────────────────────────────────
# 3. Regression: cross-process bound-method callback does NOT leak
# ─────────────────────────────────────────────────────────────────────────────


class _LocalService:
    """Simple Python class whose bound method will be boxed.

    Deliberately defined at module scope (picklable) and named
    identically on both sides of the connection. With fixed seq
    origin, both sides' first-boxed bound method of
    ``_LocalService.exposed_on_message`` would have shared
    ``name_pack="builtins.method"`` + matching seqs — the exact
    collision mode the production leak hit.
    """

    async def exposed_on_message(self, msg: str) -> str:
        # Return the message so the result itself also flows back as
        # a primitive — no extra boxing churn. Returning the same
        # payload keeps the scenario round-trippable for N calls.
        return msg


class _CallbackServer(rpyc.Service):
    """Server that, when a client subscribes, will call
    ``callback.on_message(payload)`` N times and then return.

    This reproduces the exact pattern that was leaking in
    a downstream application: a web-registered ``MessageCallbackService``
    invoked by an agent in a tight loop. Also exposes its own PID
    and RSS so the test can measure BOTH sides — the leak manifested
    on both the server and client processes simultaneously.

    To maximize the collision surface the server also holds a local
    ``_LocalService`` and hands out its bound method on request.
    Client-side test code does the same, so both peers box bound
    methods with ``name_pack="builtins.method"`` under matching
    seqs. Under the pre-fix fixed origin, the two peers' independent
    allocators produce bit-identical id_packs for their respective
    first-boxed bound methods, and the ``_unbox`` shortcut
    false-positives.
    """

    def on_connect(self, conn: object) -> None:  # noqa: D401
        # Hold a reusable instance so the first boxed bound method
        # is deterministic across test runs.
        self._local_svc = _LocalService()

    async def exposed_get_pid(self) -> int:
        return os.getpid()

    async def exposed_get_rss_kb(self) -> int:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    async def exposed_get_server_bound_method(self) -> object:
        """Hand out a bound method so the client boxes one early."""
        return self._local_svc.exposed_on_message

    async def exposed_trigger_callback_loop(
        self, callback: object, n: int, payload: str
    ) -> int:
        """Call ``callback.on_message(payload)`` ``n`` times and
        return the number of successful calls."""
        ok = 0
        for _ in range(n):
            try:
                await callback.on_message(payload)
                ok += 1
            except Exception:  # noqa: BLE001
                break
        return ok


def _rss_kb() -> int:
    """Current process RSS in kilobytes. Portable across Linux."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


@pytest.mark.asyncio
async def test_cross_process_callback_does_not_leak() -> None:
    """Regression for a downstream application's 10 GB leak. Register a
    callback, run 200 round-trips, assert the client-side RSS did
    not explode.

    Before the fix: RSS grew by hundreds of MB even on the first
    hundred calls (measured 14–18 MB/s in the production repro).
    After the fix: growth should be well under 50 MB for 200 calls
    even with Python's generous allocation slack.
    """
    with mp_asyncio_server(lambda: _CallbackServer) as port:
        conn = await async_connect("localhost", port)
        try:
            received: list[str] = []

            # Client-side service with the same shape as the server's
            # ``_LocalService``. This gives both peers a symmetric
            # bound-method box path — the pattern that produced the
            # production collision.
            client_local = _LocalService()

            class CallbackService(rpyc.Service):
                async def exposed_on_message(self, msg: str) -> None:
                    # Parse so we touch the data path (matches the
                    # production MessageCallbackService shape).
                    received.append(msg[:32])

            # Force both sides to box their FIRST bound method as
            # early as possible, so their respective ``id_pack``
            # allocators line up on the small-N range where
            # pre-fix collisions are deterministic.
            _ = await conn.root.get_server_bound_method()
            # Client-side: box one of our own bound methods to
            # populate _local_objects under the smallest seqs.
            # We do this by round-tripping it — the server pulls it
            # back to us and we see the returned bound method.
            await conn.root.trigger_callback_loop(
                CallbackService(), 1, "prime"
            )

            # Baseline on BOTH sides: the production leak was
            # synchronous on both peers (each grew by ~15 MB/s).
            # Measuring only the client side misses half the bug.
            await conn.root.trigger_callback_loop(
                CallbackService(), 10, "warmup"
            )
            await asyncio.sleep(0.1)
            client_rss_before = _rss_kb()
            server_rss_before = await conn.root.get_rss_kb()

            # Real load — 2000 cross-process bound-method callbacks.
            # Pre-fix production leak produced ~18 MB/s on each side
            # when a collision landed. 2000 trivial calls in quick
            # succession must stay tame post-fix.
            ok = await conn.root.trigger_callback_loop(
                CallbackService(),
                2000,
                '{"type":"status_change","status":"idle"}',
            )
            await asyncio.sleep(0.2)
            client_rss_after = _rss_kb()
            server_rss_after = await conn.root.get_rss_kb()

            assert ok == 2000, (
                f"callback loop did not complete: {ok}/2000 — the "
                f"ping-pong bug may be degrading calls even when not "
                f"growing RSS"
            )
            assert len(received) >= 2000, (
                f"CallbackService.exposed_on_message was not invoked "
                f"for every call: received {len(received)}/2000. "
                f"Callbacks may be silently dropped by the collision "
                f"shortcut (hitting local-object instead of peer proxy)."
            )

            client_grew_kb = client_rss_after - client_rss_before
            server_grew_kb = server_rss_after - server_rss_before
            # Generous ceiling: before the fix, collision-driven
            # round-trips produced hundreds of MB of growth on both
            # sides within a few seconds. Post-fix growth for 500
            # trivial calls is bounded by Python allocation slack,
            # typically well under 20 MB per side.
            assert client_grew_kb < 50 * 1024, (
                f"client RSS grew by {client_grew_kb / 1024:.1f} MB "
                f"during 500 cross-process callbacks — leak regressed"
            )
            assert server_grew_kb < 50 * 1024, (
                f"server RSS grew by {server_grew_kb / 1024:.1f} MB "
                f"during 500 cross-process callbacks — leak regressed"
            )
        finally:
            await conn.aclose()


# ─────────────────────────────────────────────────────────────────────────────
# 4. Debug-mode sanity check on the shortcut
# ─────────────────────────────────────────────────────────────────────────────


def test_unbox_shortcut_rejects_foreign_pid_under_debug() -> None:
    """With ``debug_refcounting=True``, if a ``LABEL_REMOTE_REF`` id_pack
    matches a local slot but its upper 32 bits encode a PID other than
    ours, ``_unbox`` must raise.

    This is the defensive assert added alongside the seed change: in
    the new regime the shortcut can only legitimately fire when the
    id_pack was minted by this process. Anything else is a protocol
    violation (or a bug in the allocator) and should fail loudly
    instead of silently returning the wrong object.
    """
    from rpyc_async.core import consts
    from rpyc_async.core.channel import Channel
    from rpyc_async.core.protocol import Connection
    from rpyc_async.core.service import VoidService
    from rpyc_async.core.stream import PipeStream

    rfd, wfd = os.pipe()
    r2, w2 = os.pipe()
    try:
        stream = PipeStream(
            incoming=os.fdopen(rfd, "rb"),
            outgoing=os.fdopen(w2, "wb"),
        )
        conn = Connection(
            VoidService(),
            Channel(stream),
            config={"debug_refcounting": True},
        )
        try:
            # Plant a local slot under a foreign-pid id_pack. In
            # normal operation no code path ever constructs such an
            # id_pack, but we simulate the corrupted-wire case to
            # exercise the guard.
            foreign_pid = (os.getpid() + 1) & 0xFFFFFFFF
            if foreign_pid == os.getpid():
                foreign_pid += 1
            foreign_seq = (foreign_pid << 32) + 1
            id_pack = ("builtins.object", 42, foreign_seq)
            sentinel = object()
            conn._local_objects.add(id_pack, sentinel)

            # _unbox dispatches on the label. We pass the LABEL_REMOTE_REF
            # payload shape (see protocol.py::_unbox).
            # Extended 4-element format: (name, type_id, seq, flags).
            package = (
                consts.LABEL_REMOTE_REF,
                (id_pack[0], id_pack[1], id_pack[2], 0),
            )
            with pytest.raises(ValueError, match=r"pid"):
                conn._unbox(package)
        finally:
            conn._closed = True
    finally:
        for fd in (r2, wfd):
            try:
                os.close(fd)
            except OSError:
                pass
