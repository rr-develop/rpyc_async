"""Regression test for: sync ``_dispatch_request`` must await coroutine results.

Production failure mode (downstream application, 2026-05-27 / 2026-06-06)
================================================================
``MessageCallbackService.exposed_on_message`` on the web side is declared
``async def`` to delegate into FastAPI's async broadcast pipeline. The
agent invokes the subscriber via::

    from rpyc_async.core.netref import asyncreq
    awaitable = asyncreq(
        subscriber, 8,            # HANDLE_CALLATTR  (sync)
        "on_message", (payload,), (),
    )
    fire_and_forget_async(awaitable, timeout=10)

That puts ``MSG_REQUEST`` + ``HANDLE_CALLATTR`` on the wire (because the
caller chose this exact handler — as seen in a downstream client's
service module).
``_needs_async_dispatch`` then returns ``False`` for the peer, because
``self._HANDLERS[HANDLE_CALLATTR]`` is a plain ``def`` — so the request
falls into the synchronous ``_dispatch_request`` path.

Synchronous ``_dispatch_request`` blindly calls ``_handle_callattr``,
gets back the user method's return value, and ``_box``-es it as the
reply payload. But the user method is ``async def`` — its return value
is an *unawaited coroutine*. The coroutine is shipped as a brine
payload, never awaited, never runs the user body, never emits the
WebSocket ``status_change`` event. Symptom in production: web's status
UI freezes; ``RuntimeWarning: Boxing coroutine object …`` floods stderr.

The fix this regression covers
==============================
``_dispatch_request`` must detect ``inspect.iscoroutine(res)`` and, when
the connection has ``_asyncio_enabled`` + ``_asyncio_loop``, hand the
coroutine off to the existing async-dispatch pinning machinery so it is
``await``-ed and its result is sent as ``MSG_REPLY``. Architecturally
identical to the routing at the top of ``_dispatch`` (line ~2645) that
already handles MSG_REQUEST when ``_needs_async_dispatch`` returns True;
the new code just covers the case where that classifier missed because
the *built-in* handler is sync but the *user* method underneath is async.

Test design
-----------
We use real ``AsyncioServer`` (in a child process) + real
``async_connect`` (in the parent / test process). On the server we
expose:

  * ``exposed_register(callback)`` — stash a client netref.
  * ``exposed_trigger()`` — drive the callback through the EXACT path
    used by production: ``asyncreq(callback, HANDLE_CALLATTR, ...)``,
    *not* through ``async_(callback.on_message)`` (which would force
    the async-flag path and miss the bug).

On the client we expose ``async def exposed_on_message(msg)`` which
sets an ``asyncio.Event``. The test fails if the event is not set
within a generous timeout — that is the bug.

Strict policy compliance: server and client live in separate processes
(see ``tests/support.py``).
"""
from __future__ import annotations

import asyncio
import unittest

import rpyc_async as rpyc
from rpyc_async.core.async_connect import async_connect
from tests.support import mp_asyncio_server


# ─── Server service (top-level — spawn-picklable) ──────────────────────────

class _SubscriberRelayService(rpyc.Service):
    """Mirrors the agent side of a downstream application.

    ``exposed_register`` stashes the client callback netref. ``exposed_trigger``
    invokes it via ``asyncreq(HANDLE_CALLATTR=8)`` — the exact code path
    a downstream client's service module uses to broadcast
    messages to subscribers. We do NOT use ``rpyc.async_(...)`` or the
    async-flagged netref ``__call__``; that would mask the bug.
    """

    _callback = None  # class-level — only one subscriber per test

    async def exposed_register(self, callback):
        type(self)._callback = callback
        return True

    async def exposed_trigger(self, payload: str) -> bool:
        cb = type(self)._callback
        if cb is None:
            return False
        # Drive the callback the SAME way a downstream client does — sync
        # HANDLE_CALLATTR + asyncreq. The returned AsyncResult is what
        # ``fire_and_forget_async`` would await in production.
        from rpyc_async.core.netref import asyncreq  # noqa: PLC0415
        from rpyc_async.core import consts  # noqa: PLC0415

        awaitable = asyncreq(cb, consts.HANDLE_CALLATTR, "on_message", (payload,), ())
        # Await so the test can observe completion deterministically.
        # In production this is wrapped in ``asyncio.wait_for`` with a
        # 10 s timeout; we use 3 s for tests.
        await asyncio.wait_for(awaitable, timeout=3.0)
        return True


def _service_factory() -> type[rpyc.Service]:
    return _SubscriberRelayService


# ─── Test ───────────────────────────────────────────────────────────────────

class TestSyncDispatchAwaitsCoroutineResult(unittest.TestCase):
    """sync ``_dispatch_request`` must await a coroutine handler result.

    With the bug present, the client-side ``async def exposed_on_message``
    returns a coroutine into ``_dispatch_request``; the coroutine is
    ``_box``-ed (RuntimeWarning) and never run — the asyncio.Event
    used as a tripwire stays clear, and the test fails by timeout.
    """

    def test_async_exposed_callback_is_awaited_under_sync_callattr(self) -> None:
        async def _go(port: int) -> None:
            # Tripwire — set inside the async exposed method on the client.
            fired = asyncio.Event()
            seen_payload: list[str] = []

            class CallbackService(rpyc.Service):
                async def exposed_on_message(self, msg: str) -> None:
                    # If we got here, the coroutine was actually awaited.
                    seen_payload.append(msg)
                    fired.set()

            conn = await async_connect("127.0.0.1", port, timeout=5.0)
            try:
                ok = await asyncio.wait_for(
                    conn.root.register(CallbackService()), timeout=5.0,
                )
                self.assertTrue(ok, "register() did not return True")

                triggered = await asyncio.wait_for(
                    conn.root.trigger("hello-from-agent"), timeout=5.0,
                )
                self.assertTrue(triggered, "trigger() did not return True")

                # The bug manifests here: fired.wait() will time out because
                # the client-side coroutine was boxed-not-awaited.
                try:
                    await asyncio.wait_for(fired.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    self.fail(
                        "async exposed_on_message was never awaited by the "
                        "client-side _dispatch_request — coroutine was boxed "
                        "as the reply payload instead. See module docstring."
                    )

                self.assertEqual(seen_payload, ["hello-from-agent"])
            finally:
                await conn.aclose()

        with mp_asyncio_server(_service_factory) as port:
            asyncio.run(_go(port))


if __name__ == "__main__":
    unittest.main(verbosity=2)
