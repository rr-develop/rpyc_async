"""
Tests for ``debug_refcounting`` mode on the client-side ``_local_objects``.

``debug_refcounting`` tags each ``RefCountingColl.add`` / ``decref`` with a
readable repr of the object — useful for diagnosing "premature decref"
bugs in production. The logger is attached to the connection's config.

POLICY
------
NO SAME-PROCESS SERVER+CLIENT — see ``tests/support.py``. The server is
started in a child process via ``mp_asyncio_server``. We inspect the
CLIENT-side logger here: outbound objects (``_box(obj)`` on the client)
hit the client's ``_local_objects`` and produce ``[REFCOUNT]`` log
records in the client's logger.

Anything observed server-side lives in a separate process and cannot be
inspected from here; that is by design — a previous version of these
tests tried to share a logger between server and client in one process,
which relied on the forbidden same-process topology.
"""
from __future__ import annotations

import asyncio
import logging
import unittest
from io import StringIO
from typing import Any

import rpyc_async as rpyc
from rpyc_async.core.async_connect import async_connect
from tests.support import mp_asyncio_server


# ─── Picklable service (spawn-safe) ─────────────────────────────────────────

class _EchoService(rpyc.Service):
    """Accepts a payload and returns it unchanged through an async method.

    The client passes a list and a dict as arguments. That boxing on the
    client creates ``[REFCOUNT] ADD`` records in the client's logger —
    which is exactly what these tests assert on.
    """

    async def exposed_echo(self, payload: Any) -> Any:
        return payload


def _service_factory() -> type[rpyc.Service]:
    return _EchoService


class TestDebugRefcountingClientSide(unittest.TestCase):
    """Verify ``debug_refcounting`` produces readable client-side logs when
    the client boxes outbound objects.
    """

    def test_debug_refcounting_logs_object_repr(self) -> None:
        log_stream = StringIO()
        handler = logging.StreamHandler(log_stream)
        handler.setLevel(logging.DEBUG)
        client_logger = logging.getLogger("rpyc.test.debug_refcounting.client")
        client_logger.setLevel(logging.DEBUG)
        client_logger.addHandler(handler)

        async def _go(port: int) -> None:
            conn = await async_connect(
                "127.0.0.1", port,
                config={
                    "debug_refcounting": True,
                    "logger": client_logger,
                    "allow_all_attrs": True,
                },
                timeout=5.0,
            )
            try:
                # Boxing these structures on the client registers them in
                # the client's _local_objects — one ADD record per object.
                the_list = [1, 2, 3, "hello"]
                the_dict = {"key": "value", "number": 42}
                a_echo = rpyc.async_(conn.root.echo)
                _ = await a_echo(the_list)
                _ = await a_echo(the_dict)
            finally:
                await conn.aclose()

        try:
            with mp_asyncio_server(_service_factory) as port:
                asyncio.run(_go(port))
        finally:
            client_logger.removeHandler(handler)

        log_output = log_stream.getvalue()

        self.assertIn(
            "[REFCOUNT] ADD",
            log_output,
            msg=(
                "Expected at least one [REFCOUNT] ADD line from the client's "
                "_local_objects while boxing outbound arguments. Got:\n"
                f"{log_output}"
            ),
        )
        # Verify the readable repr machinery is engaged (list or dict
        # content literal appears in the log).
        self.assertTrue(
            "[1, 2, 3, 'hello']" in log_output
            or ("key" in log_output and "value" in log_output),
            msg=(
                "Expected the list repr or dict contents in the log (proves "
                "the debug_refcounting path evaluated repr(obj)). Got:\n"
                f"{log_output}"
            ),
        )


if __name__ == "__main__":
    unittest.main()
