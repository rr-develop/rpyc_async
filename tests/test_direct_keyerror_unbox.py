"""
Verify the `_unbox(LABEL_LOCAL_REF)` contract when the referenced
object is missing from `_local_objects`.

Rewrite history
---------------
Original test built a `Connection` on top of a raw
`socket.socketpair()` + `Channel(client_sock)`. That low-level path
was incompatible with the current `Channel.send` contract (streams
must expose `MAX_IO_CHUNK`; raw sockets do not). The original test
also asserted the OLD contract — a bare `KeyError` — by calling
`pytest.fail(...)` if KeyError was NOT raised. The CURRENT contract
(`protocol.py:1446-1472`) is different and better:

  * missing LABEL_LOCAL_REF → `ValueError` with structured message,
    `.from None` to suppress the internal KeyError,
  * stderr `logger.error(...)` with id_pack diagnostics.

This rewrite exercises the new contract end-to-end via an
AsyncioServer in a child process (per
`docs/DESIGN_NO_SAME_PROCESS_TESTS.md`). To cause the "missing"
condition we evict the server-side `_local_objects` slot by hand
immediately before the next sync_request that re-dereferences the
same LABEL_LOCAL_REF. A helper method on the service performs both
the eviction and the re-dereference in the same request so the race
is deterministic.
"""
import asyncio
import unittest

import rpyc_async as rpyc

from tests.support import mp_asyncio_server


class _EvictingService(rpyc.Service):

    def on_connect(self, conn):
        self._conn = conn

    async def exposed_make_handle(self):
        """Return a dict (becomes a server-side _local_objects entry,
        with the client holding a netref to it). We cache the id_pack
        so the next call can evict deterministically."""
        obj = {"payload": "evict-me"}
        # Build the id_pack the same way `_box` would — use the
        # stable-seq allocator.
        self._stashed_id_pack = self._conn._stable_id_pack(obj)
        # Also register it in _local_objects so the eviction is
        # visible as a slot removal (mirrors what `_box` does when
        # the return value is boxed).
        self._conn._local_objects.add(self._stashed_id_pack, obj)
        return obj

    async def exposed_evict_and_unbox(self):
        """Evict the stashed id_pack from `_local_objects` and then
        re-build a LABEL_LOCAL_REF package with that id_pack and feed
        it back to `_unbox`. Under the current contract this must
        raise `ValueError` with a structured message mentioning the
        phrase `_local_objects`.

        Return the outcome as a brine-primitive tuple so the client
        can assert without any further netref dereferencing.
        """
        from rpyc_async.core import consts

        id_pack = self._stashed_id_pack
        if id_pack in self._conn._local_objects._dict:
            del self._conn._local_objects._dict[id_pack]

        pkg = (consts.LABEL_LOCAL_REF, id_pack)
        try:
            self._conn._unbox(pkg)
            return ("unexpected-success", "")
        except ValueError as exc:
            return ("raised-ValueError", str(exc))
        except KeyError as exc:
            return ("raised-KeyError-legacy", repr(exc))


class TestUnboxMissingLocalRef(unittest.TestCase):

    def test_missing_local_ref_raises_structured_valueerror(self):
        async def body():
            with mp_asyncio_server(_EvictingService) as port:
                conn = await rpyc.async_connect("127.0.0.1", port)
                try:
                    _ = await conn.root.make_handle()
                    outcome, detail = await conn.root.evict_and_unbox()
                    self.assertEqual(
                        outcome, "raised-ValueError",
                        f"expected ValueError contract, got "
                        f"{outcome!r} (detail: {detail!r})"
                    )
                    # Message must mention the locality and the
                    # missing-object framing so production logs can
                    # be grepped.
                    self.assertIn("_local_objects", detail)
                finally:
                    await conn.aclose()

        asyncio.run(body())


if __name__ == "__main__":
    unittest.main()
