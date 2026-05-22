"""Tests for the optional ``on_inbound_quarantine`` callback hook.

When a Connection enters inbound quarantine, the host app may want to
surface a warning (log line, web-UI banner) WITHOUT changing the
connection's behavior. ``on_inbound_quarantine`` (config, default None) is
invoked exactly once at that moment with a small info dict.

Contract:
  * default config exposes the key (value None);
  * when set, the callback fires once on quarantine with connid/peer/
    inbound_inflight/threshold/request_callbacks;
  * a raising callback must NOT break quarantine (dispatch path is sacred);
  * no callback configured → no error (current behavior preserved).
"""

from __future__ import annotations

import asyncio
import unittest

from rpyc.core import protocol
from rpyc.core.protocol import Connection
from rpyc.core.service import VoidService


class _SilentChannel:
    def __init__(self) -> None:
        self.closed = False

    def send(self, data):
        return None

    async def asend(self, data):
        return None

    def recv(self):
        raise EOFError("stream has been closed")

    def close(self):
        self.closed = True

    def fileno(self):
        return -1

    def poll(self, timeout):
        return False


def _make_connection(test_case, *, config: dict | None = None):
    conn = Connection(VoidService(), _SilentChannel(), config=config or {})
    conn._asyncio_enabled = True
    conn._asyncio_loop = asyncio.get_event_loop()
    test_case.addCleanup(setattr, conn, "_closed", True)
    return conn


class TestQuarantineHook(unittest.IsolatedAsyncioTestCase):
    async def test_default_config_has_hook_key_none(self) -> None:
        self.assertIn("on_inbound_quarantine", protocol.DEFAULT_CONFIG)
        self.assertIsNone(protocol.DEFAULT_CONFIG["on_inbound_quarantine"])

    async def test_callback_fires_once_with_info(self) -> None:
        calls: list[dict] = []
        conn = _make_connection(
            self, config={"on_inbound_quarantine": calls.append}
        )
        # Pretend we've crossed the threshold.
        conn._inbound_inflight = 10_000

        conn._enter_inbound_quarantine()
        # Idempotent: a second call must not re-fire.
        conn._enter_inbound_quarantine()

        self.assertEqual(len(calls), 1, "hook must fire exactly once")
        info = calls[0]
        for key in ("connid", "peer", "inbound_inflight", "threshold", "request_callbacks"):
            self.assertIn(key, info)
        self.assertEqual(info["inbound_inflight"], 10_000)

    async def test_raising_callback_does_not_break_quarantine(self) -> None:
        def _boom(_info):
            raise RuntimeError("callback blew up")

        conn = _make_connection(
            self, config={"on_inbound_quarantine": _boom}
        )
        conn._inbound_inflight = 10_000

        # Must not raise — quarantine still completes.
        conn._enter_inbound_quarantine()
        self.assertTrue(conn._inbound_quarantined)

    async def test_no_callback_configured_is_fine(self) -> None:
        conn = _make_connection(self)  # default: no hook
        conn._inbound_inflight = 10_000
        conn._enter_inbound_quarantine()  # must not raise
        self.assertTrue(conn._inbound_quarantined)


if __name__ == "__main__":
    unittest.main()
