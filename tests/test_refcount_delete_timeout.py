"""
Verify that the HANDLE_DEL timeout path emits the expected warning.

When the remote peer takes longer to ACK HANDLE_DEL than
`cleanup_ack_timeout`, the calling side's `_async_request_with_ack`
returns `False` and `_process_pending_deletions` must emit
`Failed to delete remote object …` to stderr
(`protocol.py:957-985`). This is a real, supported diagnostic path.

Topology: server and client live in DIFFERENT processes (per
`docs/DESIGN_NO_SAME_PROCESS_TESTS.md`). The server monkey-patches
its own Connection's `_handle_del` to block for longer than the
client's ack timeout — this produces the exact stall shape that
motivated the warning.

Rewrite history
---------------
Original file was scaffolding the author's own comment in
`tests/REFCOUNT_ERRORS_ANALYSIS.md` tagged as `Not finished`. It:
  * overrode `_handle_del` to return a `dict` — now deadly. Per
    `protocol.py:2348-2394`, HANDLE_DEL's reply MUST be a brine
    primitive or the calling side recurses `bool(netref)` into a
    nested sync RPC and self-deadlocks (see commits 62d508e / fe14779),
  * issued sync netref ops (`conn.root.get_registry_stats()`) from
    inside the asyncio loop — blocked by `protocol.py:2129`.

The rewrite keeps the scenario (slow HANDLE_DEL → `Failed to delete`
warning) but fixes both: the patched `_handle_del` returns `bool`,
and the client uses only async RPCs / tuple replies.
"""
import asyncio
import io
import sys
import time
import unittest

import rpyc

from tests.support import mp_asyncio_server


# Server-side tunable that the patched `_handle_del` reads. Lives at
# module scope in the CHILD process (each mp_asyncio_server spawns a
# fresh process, so there is no cross-test pollution).
_DELETE_DELAY_SEC = 2.0


class _SlowDeleteService(rpyc.Service):
    """On `on_connect` we wrap the Connection's `_handle_del` with a
    sleep. Return value stays a `bool` — preserving the brine-
    primitive invariant documented in `protocol.py:2348-2394`."""

    def on_connect(self, conn):
        self._conn = conn

        original_handle_del = conn._handle_del

        def slow_handle_del(obj, count=1):
            # Intentionally blocks the server event loop — this
            # imitates a remote peer that cannot ACK within the
            # client's `cleanup_ack_timeout`. The client's
            # `_async_request_with_ack` will return False, and
            # `_process_pending_deletions` will log
            # `Failed to delete remote object`.
            time.sleep(_DELETE_DELAY_SEC)
            # Preserve the real (bool) return type — any non-primitive
            # here would recurse into bool(netref) on the caller and
            # self-deadlock the cleanup loop.
            return bool(original_handle_del(obj, count))

        conn._handle_del = slow_handle_del
        # Also replace in the handler dispatch table so incoming
        # HANDLE_DEL messages hit the wrapped version.
        from rpyc.core import consts
        conn._HANDLERS[consts.HANDLE_DEL] = (
            lambda self_conn, obj, count=1: slow_handle_del(obj, count)
        )

    async def exposed_return_temp_objects(self, count):
        """Return a list of temp dicts; each dict on the server side
        enters `_local_objects` and will be sent a HANDLE_DEL when
        the client's netref falls out of scope."""
        return [{"i": i} for i in range(count)]

    async def exposed_registry_size(self) -> int:
        return int(len(self._conn._local_objects._dict))


class TestRefcountDeleteTimeout(unittest.TestCase):

    def test_slow_handle_del_emits_failed_to_delete_warning(self):
        """Client's cleanup_ack_timeout < server's artificial delete
        delay → at least one `Failed to delete remote object` line on
        client's stderr."""

        cap = io.StringIO()
        real = sys.stderr
        nonlocal_snapshot: dict = {"text": ""}

        async def body():
            with mp_asyncio_server(_SlowDeleteService) as port:
                # Very short ack timeout — the server will hold for
                # 2s, we wait 0.5s for ack → timeout is certain.
                conn = await rpyc.async_connect(
                    "127.0.0.1", port,
                    config={"cleanup_ack_timeout": 0.5},
                )
                try:
                    result = await conn.root.return_temp_objects(5)
                    self.assertIsNotNone(result)

                    # Drop the netref — cleanup_loop enqueues
                    # HANDLE_DEL, which the server will stall on.
                    del result
                    import gc
                    gc.collect()

                    # Give the cleanup_loop time to (a) send
                    # HANDLE_DEL, (b) wait cleanup_ack_timeout=0.5s,
                    # (c) log the warning. We pad generously.
                    await asyncio.sleep(1.2)

                    nonlocal_snapshot["text"] = cap.getvalue()
                finally:
                    # Close from client side WITHOUT waiting for the
                    # server to finish its stalled _handle_del — that
                    # would take another 2s.
                    await conn.aclose()

        try:
            sys.stderr = cap
            asyncio.run(body())
        finally:
            sys.stderr = real

        steady = nonlocal_snapshot["text"]
        self.assertGreaterEqual(
            steady.count("Failed to delete remote object"), 1,
            "expected at least one 'Failed to delete remote object' "
            f"warning on client stderr when server stalls longer than "
            f"cleanup_ack_timeout; got:\n{steady}"
        )


if __name__ == "__main__":
    unittest.main()
