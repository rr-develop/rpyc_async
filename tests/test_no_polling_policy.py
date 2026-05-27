"""
Polling-ban enforcement tests (TDD).

This test suite enforces a **strict prohibition on polling** in the AsyncioServer
code path and in the asyncio-native parts of Connection.

WHY POLLING IS FORBIDDEN
========================
Polling wakes the event loop on a timer to check a condition. At 10 Hz
(asyncio.sleep(0.1)) it burns ~20 wakeups/sec for just two active RPyC
connections — observed driving agent CPU from 1.2% idle to 33%+ with two
connections attached. It also masks real bugs: stale connections whose
``.closed`` flag is never flipped keep burning cycles forever.

The ONLY acceptable idle-wait primitives in async paths are:
  * ``asyncio.Event().wait()``  — wake on exactly one event
  * ``asyncio.Condition.wait()`` — wake on a predicate change
  * ``asyncio.Queue.get()``      — wake when work is enqueued
  * ``loop.add_reader(fd, cb)``  — wake when an FD becomes readable
  * ``await some_future``        — wake when the future resolves

These tests will FAIL if someone reintroduces polling. That is deliberate.
"""
import ast
import asyncio
import inspect
import re
import textwrap
import unittest
from unittest import mock

from rpyc.core import protocol
from rpyc.utils import async_server


def _find_async_function(source: str, name: str) -> ast.AsyncFunctionDef:
    """Parse source and return the AST node for an async function by name."""
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return node
    raise AssertionError(f"async def {name} not found in source")


def _contains_sleep_based_polling(func_node: ast.AST) -> bool:
    """Return True iff the function body contains a loop that awaits asyncio.sleep().

    A loop-with-sleep is the canonical polling shape we forbid:

        while <cond>:
            ...
            await asyncio.sleep(<x>)

    A bare ``await asyncio.sleep(x)`` outside a loop is fine (tests, pacing a
    send). A ``for _ in range(N): await asyncio.sleep(x)`` is also polling.
    """
    for loop_node in ast.walk(func_node):
        if not isinstance(loop_node, (ast.While, ast.For, ast.AsyncFor)):
            continue
        for inner in ast.walk(loop_node):
            if not isinstance(inner, ast.Await):
                continue
            call = inner.value
            if not isinstance(call, ast.Call):
                continue
            func = call.func
            if isinstance(func, ast.Attribute) and func.attr == "sleep":
                if isinstance(func.value, ast.Name) and func.value.id == "asyncio":
                    return True
            if isinstance(func, ast.Name) and func.id == "sleep":
                return True
    return False


class TestAsyncioServerNoPolling(unittest.TestCase):
    """Static-analysis guard: AsyncioServer must not contain polling loops."""

    def test_serve_connection_has_no_polling_loop(self):
        """_serve_connection MUST NOT busy-wait via asyncio.sleep.

        Expected shape: wait on an event-driven primitive (asyncio.Event,
        Future, conn close-callback) and return when the connection closes.
        """
        source = inspect.getsource(async_server.AsyncioServer._serve_connection)
        tree = ast.parse(source.lstrip())
        func = None
        for node in ast.walk(tree):
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "_serve_connection":
                func = node
                break
        self.assertIsNotNone(func, "_serve_connection not found")
        self.assertFalse(
            _contains_sleep_based_polling(func),
            "_serve_connection uses asyncio.sleep() inside a loop — this is "
            "forbidden polling. Replace with event-driven wait "
            "(e.g. await conn.wait_closed()).",
        )

    def test_serve_connection_waits_on_event_driven_primitive(self):
        """_serve_connection must actually AWAIT an event-driven wait primitive.

        We require at least one of these tokens to appear: wait_closed,
        Event().wait, closed_event, _closed_event, wait().
        """
        source = inspect.getsource(async_server.AsyncioServer._serve_connection)
        event_driven_tokens = (
            "wait_closed",
            "closed_event",
            "_closed_event",
            ".wait(",
        )
        self.assertTrue(
            any(tok in source for tok in event_driven_tokens),
            f"_serve_connection does not await any event-driven wait primitive "
            f"(looked for {event_driven_tokens!r}). It must block on an event, "
            f"not poll.",
        )


class TestCleanupLoopNoIdlePolling(unittest.TestCase):
    """Static-analysis guard: cleanup_loop must wake on work, not on a timer.

    The old shape was:
        while running:
            await process()
            await asyncio.sleep(cleanup_interval)   # <-- polling every 2s

    The new shape must wake only when a deletion is enqueued OR when the
    connection is closing (for final drain). No idle timer wakeups.
    """

    def test_start_cleanup_task_source_has_no_sleep_poll(self):
        """_start_cleanup_task's cleanup loop must not timer-poll.

        We walk the AST (NOT grep the source — the docstring deliberately
        mentions the banned pattern as an example). We locate every inner
        async function defined inside ``_start_cleanup_task`` (that's the
        cleanup loop coroutine) and assert it does not contain a
        ``while ...: await asyncio.sleep(...)`` shape.
        """
        source = inspect.getsource(protocol.Connection._start_cleanup_task)
        tree = ast.parse(source.lstrip())
        # Find the inner cleanup_loop async def.
        inner_async_funcs = []
        outer = None
        for node in ast.walk(tree):
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "_start_cleanup_task":
                outer = node
        self.assertIsNotNone(outer, "_start_cleanup_task not found") if False else None
        # The outer is `def` (not async) so re-collect:
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_start_cleanup_task":
                outer = node
                break
        self.assertIsNotNone(outer)
        assert outer is not None
        for sub in ast.walk(outer):
            if isinstance(sub, ast.AsyncFunctionDef) and sub is not outer:
                inner_async_funcs.append(sub)
        self.assertTrue(
            inner_async_funcs,
            "Expected an inner async cleanup_loop coroutine inside "
            "_start_cleanup_task.",
        )
        for fn in inner_async_funcs:
            self.assertFalse(
                _contains_sleep_based_polling(fn),
                f"Inner coroutine {fn.name!r} inside _start_cleanup_task "
                f"contains `while <cond>: await asyncio.sleep(...)` — forbidden "
                f"polling. Wake the loop via an asyncio.Event set from "
                f"Connection._enqueue_deletion and from the close path instead.",
            )


class TestConnectionCloseEventAPI(unittest.TestCase):
    """Connection must expose an event-driven close-notification API.

    This is the primitive that _serve_connection uses to avoid polling.
    """

    def test_connection_has_wait_closed(self):
        self.assertTrue(
            hasattr(protocol.Connection, "wait_closed"),
            "Connection.wait_closed() is missing. This coroutine must be "
            "provided so that async callers can block on close without polling.",
        )

    def test_wait_closed_is_coroutine(self):
        self.assertTrue(
            asyncio.iscoroutinefunction(protocol.Connection.wait_closed),
            "Connection.wait_closed must be an async def (coroutine function).",
        )

    def test_connection_has_add_close_callback(self):
        self.assertTrue(
            hasattr(protocol.Connection, "add_close_callback"),
            "Connection.add_close_callback(cb) is missing. Callers need it to "
            "register one-shot close notifications without polling .closed.",
        )


class TestServeConnectionDoesNotBurnCPU(unittest.IsolatedAsyncioTestCase):
    """Runtime guard: _serve_connection must not wake on a timer.

    We patch asyncio.sleep so any timer-based wakeup inside _serve_connection
    is counted. Then we drive a real connection through open -> close and
    assert the count is zero (or close to it — certainly not 10/sec).
    """

    async def test_serve_connection_does_not_sleep_while_waiting_for_close(self):
        from rpyc.core.protocol import Connection

        # Build a minimal Connection double that supports close() + event-driven wait.
        # We don't need full RPyC — we only test the waiting contract.
        class DummyConn:
            def __init__(self):
                self._closed = False
                self._waiters = []

            @property
            def closed(self):
                return self._closed

            def add_close_callback(self, cb):
                if self._closed:
                    cb()
                else:
                    self._waiters.append(cb)

            async def wait_closed(self):
                if self._closed:
                    return
                fut = asyncio.get_running_loop().create_future()
                self.add_close_callback(lambda: fut.set_result(None) if not fut.done() else None)
                await fut

            def close(self):
                if self._closed:
                    return
                self._closed = True
                for cb in self._waiters:
                    try:
                        cb()
                    except Exception:
                        pass
                self._waiters.clear()

            def disable_asyncio_serving(self):
                pass

        conn = DummyConn()
        server = async_server.AsyncioServer.__new__(async_server.AsyncioServer)
        import logging
        server.logger = logging.getLogger("test")

        sleep_calls = []
        real_sleep = asyncio.sleep

        async def counting_sleep(delay, *a, **kw):
            # Record every sleep the serve path performs.
            sleep_calls.append(delay)
            return await real_sleep(delay, *a, **kw)

        with mock.patch("rpyc.utils.async_server.asyncio.sleep", counting_sleep):
            task = asyncio.create_task(server._serve_connection(conn, sock=None))
            # Give the serve task a few real ticks to establish its wait.
            for _ in range(5):
                await real_sleep(0)
            # Close and expect the serve task to return promptly.
            conn.close()
            await asyncio.wait_for(task, timeout=2.0)

        # Zero sleeps is the standard. One at most would be tolerable only as
        # a transient but we want to be strict — the whole point of this work.
        self.assertEqual(
            sleep_calls,
            [],
            f"_serve_connection called asyncio.sleep {len(sleep_calls)} time(s) "
            f"while waiting for close ({sleep_calls!r}). Polling is forbidden.",
        )


class TestAsyncReaderHasNoPolling(unittest.TestCase):
    """Static guard for the asyncio READ path (``enable_asyncio_serving`` and
    its nested ``on_readable``). These MUST be purely event-driven: one
    non-blocking ``recv_available()`` per ``add_reader`` wakeup + in-memory
    framing. Re-introducing a socket poll here brings back the 99.9%-CPU
    half-closed-socket busy-loop (observed in a downstream application).
    See docs/DESIGN_NO_POLLING_ASYNCIO_READ.md.
    """

    def _read_path_ast(self) -> ast.AST:
        # enable_asyncio_serving defines on_readable nested inside it, so its
        # AST covers the whole async read callback. We scan the AST (not text)
        # so that DOCSTRING/COMMENT mentions of "poll"/"MSG_PEEK" — which the
        # banner intentionally contains — are NOT flagged; only real
        # calls/names are.
        src = textwrap.dedent(
            inspect.getsource(protocol.Connection.enable_asyncio_serving)
        )
        return ast.parse(src)

    def _called_attrs(self, tree: ast.AST) -> list[str]:
        """Names of attribute-style calls in the AST, e.g. 'poll' for x.poll()."""
        out = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                out.append(node.func.attr)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                out.append(node.func.id)
        return out

    def _names_used(self, tree: ast.AST) -> set[str]:
        names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                names.add(node.id)
            if isinstance(node, ast.Attribute):
                names.add(node.attr)
        return names

    def test_no_poll_call_in_async_reader(self) -> None:
        calls = self._called_attrs(self._read_path_ast())
        self.assertNotIn(
            "poll", calls,
            "the asyncio read path CALLS poll(...) — POLLING is forbidden. "
            "Read once via recv_available() and frame from the buffer "
            "(_extract_frames); never poll the socket. "
            "See docs/DESIGN_NO_POLLING_ASYNCIO_READ.md.",
        )
        self.assertNotIn(
            "select", calls,
            "select(...) is polling — forbidden on the async read path.",
        )

    def test_no_while_poll_drain_loop(self) -> None:
        """No ``while``/``for`` loop in the reader whose test/body calls
        poll() — that is the busy-loop shape."""
        tree = self._read_path_ast()
        for loop in ast.walk(tree):
            if isinstance(loop, (ast.While, ast.For, ast.AsyncFor)):
                self.assertNotIn(
                    "poll", self._called_attrs(loop),
                    "the asyncio read path has a loop that calls poll(...) — "
                    "that is the drain busy-loop. Use one recv_available() + "
                    "in-memory framing.",
                )

    def test_no_msg_peek_name_in_async_reader(self) -> None:
        names = self._names_used(self._read_path_ast())
        self.assertNotIn(
            "MSG_PEEK", names,
            "MSG_PEEK probing is a polling band-aid — forbidden on the async "
            "read path; recv_available() distinguishes EOF/EAGAIN cleanly.",
        )

    def test_reader_uses_recv_available(self) -> None:
        """Positive assertion: the event-driven primitive IS called."""
        self.assertIn(
            "recv_available", self._called_attrs(self._read_path_ast()),
            "the asyncio read path must read via the non-blocking, "
            "single-shot recv_available() (the event-driven primitive).",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
