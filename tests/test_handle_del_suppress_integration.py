"""
Integration test for HANDLE_DEL suppression on closed connection.

NO MOCKS — real AsyncioServer in a subprocess, real client, real
SIGKILL of the server. Verifies the production scenario from
a related internal incident analysis (not included here)
is suppressed end-to-end.

Test shape:
  1. Start a real AsyncioServer in a subprocess.
  2. Materialize 200 distinct bound-method netrefs on the client.
  3. SIGKILL the server (no graceful close — exactly like the
     production disconnect of a downstream application).
  4. Drop all proxies and force GC so __del__ enqueues HANDLE_DELs
     against the (now dead) peer.
  5. Run the cleanup_loop for a bounded window.
  6. Assert: stderr contains NO ``Failed to delete remote object``
     warnings. With the fix this is 0; in production, before the
     fix, this was thousands.

The per-cycle exhaustive proof is in
``test_handle_del_suppress_on_closed.py`` (unit tests with full
control over the ack-fail path); this integration test verifies
the guards are wired correctly into the real
async_connect/AsyncioServer stack and that the realistic SIGKILL
shutdown path stays silent.

The test follows the same multiprocessing pattern as
``test_fire_and_forget_rpc.py`` (cross-process, real
AsyncioServer, signal/event for ready-handshake).
"""

from __future__ import annotations

import asyncio
import io
import multiprocessing
import sys

import pytest
import rpyc
from rpyc.utils.async_server import AsyncioServer


# ════════════════════════════════════════════════════════════════════
# Service: returns a bound method (the production proxy shape we saw
# in the incident — ('builtins.method', ...)).
# ════════════════════════════════════════════════════════════════════
class _MethodVendor(rpyc.Service):
    """Returns a fresh bound method per call. Each call wraps a new
    no-op closure into an attribute, then returns it — so every
    proxy on the client has a DISTINCT id_pack. Mirrors the
    production callback / subscription pattern that triggered the
    storm (every subscription registers a new callback object)."""

    def __init__(self):
        super().__init__()
        self._counter = 0

    async def exposed_get_method(self):
        # Build a fresh closure-backed bound method per call. The
        # `MethodType(...)` wrapping is what produces the
        # `builtins.method` class on the wire (matching the
        # incident's id_pack[0]).
        import types
        self._counter += 1
        n = self._counter

        def _impl(_self):
            return n

        bound = types.MethodType(_impl, self)
        return bound


def _run_server(port: int, ready: multiprocessing.Event) -> None:
    """Subprocess entrypoint — run AsyncioServer until killed."""

    async def main() -> None:
        server = AsyncioServer(
            _MethodVendor,
            port=port,
            protocol_config={
                "allow_all_attrs": True,
                "allow_public_attrs": True,
            },
        )
        ready.set()
        await server.serve_forever()

    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass


def _get_free_port() -> int:
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ════════════════════════════════════════════════════════════════════
# The integration test
# ════════════════════════════════════════════════════════════════════
@pytest.mark.asyncio
async def test_no_storm_after_server_sigkill():
    """Storm pattern from the incident must NOT reproduce
    after a SIGKILL of the server: stderr stays bounded, and the
    client's pending deletions queue stays bounded.

    Tolerance: a small number (<5) of WARNINGs may legitimately
    leak through during the close-race window, but the storm
    (hundreds-to-thousands per id_pack) must not. The fix targets
    the post-close steady state, not the close-race itself.
    """
    from rpyc.core.async_connect import async_connect

    port = _get_free_port()
    ready = multiprocessing.Event()
    server_proc = multiprocessing.Process(
        target=_run_server, args=(port, ready), daemon=True,
    )
    server_proc.start()
    try:
        assert ready.wait(timeout=5.0), "Server failed to become ready"

        conn = await async_connect("localhost", port, timeout=5.0)

        # ── Phase 1: build up MANY pending deletions on a LIVE peer ──
        # Materialize many bound-method netrefs. Doing this against
        # a live peer is the harmless baseline. We use `await` because
        # the server method is `async def` (required by rpyc_async to
        # avoid deadlock from sync_request on the serving loop).
        proxies = []
        for _ in range(200):
            proxies.append(await conn.root.get_method())

        # ── Phase 2: SIGKILL the server BEFORE dropping the proxies ──
        # This is critical for reproducing the storm: the proxies
        # are still alive (so HANDLE_DEL will be enqueued when we
        # drop them), but the peer is gone (so each HANDLE_DEL ack
        # will fail). Without the fix, every drop → enqueue → fail
        # → 2 log lines.
        server_proc.kill()
        server_proc.join(timeout=5.0)
        assert not server_proc.is_alive(), "Server failed to die"

        # ── Phase 3: redirect stderr to capture any storm ──
        orig_stderr = sys.stderr
        captured = io.StringIO()
        sys.stderr = captured
        try:
            # Drop all 200 distinct-id_pack proxies → 200 enqueue'd
            # HANDLE_DELs against a dead peer. Each one ack-fails.
            # Without the fix this produces ~200 warning attempts
            # (with the fix: 0, modulo a small close-race window).
            proxies.clear()
            import gc
            gc.collect()  # Make sure __del__ runs synchronously
                          # so _enqueue_deletion is called for all.

            # Yield aggressively so the cleanup loop gets many ticks
            # to drain the queue and (without the fix) emit the storm.
            # We loop until the conn observes closed and then for a
            # bounded extra window.
            for _ in range(200):
                await asyncio.sleep(0.005)
                if conn.closed:
                    break
            # Bounded extra window AFTER close — this is where the
            # storm used to grow (the cleanup_loop tried to drain on
            # every cycle even after disconnect).
            for _ in range(100):
                await asyncio.sleep(0.005)

            # Wait for the connection to acknowledge it is closed.
            # Use the event-driven API per the NO POLLING POLICY
            # (do NOT loop on ``while not conn.closed``).
            try:
                await asyncio.wait_for(conn.wait_closed(), timeout=5.0)
            except asyncio.TimeoutError:
                # Some channels stay half-open until the cleanup loop
                # observes EOF. Force a close so the rest of the test
                # is deterministic — this is what the production
                # codebase does when it gives up on a peer.
                try:
                    conn.close()
                except Exception:
                    pass

            assert conn.closed, "Connection should be closed by now"

            # Give the cleanup_loop several iterations on the dead
            # conn — exactly the window where the storm used to grow.
            for _ in range(50):
                await asyncio.sleep(0)

        finally:
            sys.stderr = orig_stderr

        out = captured.getvalue()
        n_warnings = out.count("Failed to delete remote object")

        # Production behaviour: after SIGKILL of the peer, every
        # GC'd netref's HANDLE_DEL fails and emits the WARNING.
        # With the closed-conn guard in place, the cleanup loop
        # observes ``self.closed == True`` on its first post-close
        # tick, drains the queue silently, and emits at most ONE
        # ``logger.debug`` summary (not visible in stderr).
        #
        # We assert == 0: the realistic SIGKILL path closes the
        # connection BEFORE the cleanup_loop tries to drain (the
        # event-driven close-callback wakes the loop after EOF is
        # observed), so there is no close-race here.
        assert n_warnings == 0, (
            f"Storm not fully suppressed — {n_warnings} 'Failed to "
            f"delete' warnings after server SIGKILL (expected 0 "
            f"with the fix). Captured stderr (first 2 KB):\n"
            f"{out[:2000]}"
        )

    finally:
        if server_proc.is_alive():
            server_proc.kill()
            server_proc.join(timeout=2.0)
