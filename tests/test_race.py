import rpyc
import rpyc.core.async_ as rc_async_
import rpyc.core.protocol as rc_protocol
import contextlib
import signal
import threading
import time
import unittest


# ═══════════════════════════════════════════════════════════════════════════
# LEGACY TEST — skipped under current project policies.
#
# This file exercises an AsyncResult race in the *sync* ThreadedServer
# pathway via `rpyc.classic.connect_thread()`, which puts the server and
# the client in the SAME process. That topology is forbidden by
# `docs/DESIGN_NO_SAME_PROCESS_TESTS.md` and enforced by
# `tests/test_no_same_process_server_client.py` — AsyncioServer and
# rpyc clients MUST live in different processes (see
# `tests/support.py::mp_asyncio_server`).
#
# The race this tries to reproduce (KeyboardInterrupt during
# AsyncResult.wait() on a ThreadedServer serve_all loop) has no
# analogue in the event-driven AsyncioServer code path. Current
# bidirectional-async race coverage is in
# `tests/test_critical_bidirectional_async.py` and
# `tests/test_e2e_recursive_async.py`.
# ═══════════════════════════════════════════════════════════════════════════
@unittest.skip(
    "Legacy — CANNOT be ported to the AsyncioServer topology. "
    "The race under test is a signal/thread interaction specific to "
    "the synchronous ThreadedServer `serve_all` loop: `AsyncResult.wait()` "
    "calls `conn.serve()` on a dedicated thread, and a SIGINT "
    "delivered to that thread must NOT raise KeyboardInterrupt back "
    "to the caller. There is no analogous code path under "
    "AsyncioServer — event-loop wait uses `asyncio.Event.wait()` and "
    "signal handling goes through `loop.add_signal_handler()`, which "
    "do not exhibit this race. Re-implementing the test under the "
    "supported topology would be testing a different bug. Current "
    "bidirectional-async race coverage: "
    "tests/test_critical_bidirectional_async.py, "
    "tests/test_e2e_recursive_async.py."
)
class TestRace(unittest.TestCase):
    def setUp(self):
        self.connection = rpyc.classic.connect_thread()

        self.a_str = rpyc.async_(self.connection.builtin.str)

    def tearDown(self):
        self.connection.close()

    def test_asyncresult_race(self):
        with _patch():
            def hook():
                time.sleep(0.2)  # loose race

            _AsyncResult._HOOK = hook

            threading.Thread(target=self.connection.serve_all).start()
            time.sleep(0.1)  # wait for thread to serve

            # schedule KeyboardInterrupt
            thread_id = threading.get_ident()
            _ = lambda: signal.pthread_kill(thread_id, signal.SIGINT)
            timer = threading.Timer(1, _)
            timer.start()

            a_result = self.a_str("")  # request
            time.sleep(0.1)  # wait for race to start
            try:
                a_result.wait()
            except KeyboardInterrupt:
                raise Exception("deadlock")

            timer.cancel()


class _AsyncResult(rc_async_.AsyncResult):
    _HOOK = None

    def __call__(self, *args, **kwargs):
        hook = type(self)._HOOK
        if hook is not None:
            hook()
        return super().__call__(*args, **kwargs)


@contextlib.contextmanager
def _patch():
    AsyncResult = rc_async_.AsyncResult
    try:
        rc_async_.AsyncResult = _AsyncResult
        rc_protocol.AsyncResult = _AsyncResult  # from import
        yield

    finally:
        rc_async_.AsyncResult = AsyncResult
        rc_protocol.AsyncResult = AsyncResult


if __name__ == "__main__":
    unittest.main()
