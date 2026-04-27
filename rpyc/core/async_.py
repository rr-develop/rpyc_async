import time  # noqa: F401
from threading import Event
from rpyc.lib import Timeout
from rpyc.lib.compat import TimeoutError as AsyncResultTimeout


class AsyncResult(object):
    """*AsyncResult* represents a computation that occurs in the background and
    will eventually have a result. Use the :attr:`value` property to access the
    result (which will block if the result has not yet arrived).
    """
    # ``_seq`` is set by ``Connection.async_request`` after the request
    # is registered in ``Connection._request_callbacks``. It enables
    # ``__await__`` to release the slot on cancel — see the cancel-leak
    # cleanup in ``__await__`` below and the regression tests in
    # ``tests/test_asyncresult_cancel_leak.py``.
    __slots__ = ["_conn", "_is_ready", "_is_exc", "_callbacks", "_obj",
                 "_ttl", "_seq"]

    def __init__(self, conn):
        self._conn = conn
        self._is_ready = False
        self._is_exc = None
        self._obj = None
        self._callbacks = []
        self._ttl = Timeout(None)
        self._seq = None

    def __repr__(self):
        if self._is_ready:
            state = "ready"
        elif self._is_exc:
            state = "error"
        elif self.expired:
            state = "expired"
        else:
            state = "pending"
        return f"<AsyncResult object ({state}) at 0x{id(self):08x}>"

    def __call__(self, is_exc, obj):
        if self.expired:
            return
        self._is_exc = is_exc
        self._obj = obj
        self._is_ready = True
        for cb in self._callbacks:
            cb(self)
        del self._callbacks[:]

    def wait(self):
        """Waits for the result to arrive. If the AsyncResult object has an
        expiry set, and the result did not arrive within that timeout,
        an :class:`AsyncResultTimeout` exception is raised"""
        while self._waiting():
            # Serve the connection since we are not ready. Suppose
            # the reply for our seq is served. The callback is this class
            # so __call__ sets our obj and _is_ready to true.
            self._conn.serve(self._ttl, waiting=self._waiting)

        # Check if we timed out before result was ready
        if not self._is_ready:
            raise AsyncResultTimeout("result expired")

    def _waiting(self):
        return not (self._is_ready or self.expired)

    def add_callback(self, func):
        """Adds a callback to be invoked when the result arrives. The callback
        function takes a single argument, which is the current AsyncResult
        (``self``). If the result has already arrived, the function is invoked
        immediately.

        :param func: the callback function to add
        """
        if self._is_ready:
            func(self)
        else:
            self._callbacks.append(func)

    def set_expiry(self, timeout):
        """Sets the expiry time (in seconds, relative to now) or ``None`` for
        unlimited time

        :param timeout: the expiry time in seconds or ``None``
        """
        self._ttl = Timeout(timeout)

    @property
    def ready(self):
        """Indicates whether the result has arrived"""
        if self._is_ready:
            return True
        if self.expired:
            return False
        self._conn.poll_all()
        return self._is_ready

    @property
    def error(self):
        """Indicates whether the returned result is an exception"""
        return self.ready and self._is_exc

    @property
    def expired(self):
        """Indicates whether the AsyncResult has expired"""
        return not self._is_ready and self._ttl.expired()

    @property
    def value(self):
        """Returns the result of the operation. If the result has not yet
        arrived, accessing this property will wait for it. If the result does
        not arrive before the expiry time elapses, :class:`AsyncResultTimeout`
        is raised. If the returned result is an exception, it will be raised
        here. Otherwise, the result is returned directly.
        """
        self.wait()
        if self._is_exc:
            raise self._obj
        else:
            return self._obj

    # ═══════════════════════════════════════════════════════════════
    # Asyncio Support (v5.1)
    # ═══════════════════════════════════════════════════════════════

    def __await__(self):
        """
        Make AsyncResult awaitable in async context.

        This allows using AsyncResult with await syntax:
            result = await conn.root.async_method()

        Returns:
            Result value if ready, otherwise waits asynchronously.

        Raises:
            Exception: If remote call raised exception

        Example:
            async def main():
                conn = rpyc.connect("localhost", 18861)
                conn.enable_asyncio_serving()

                # Can now await!
                result = await conn.root.async_method()
                print(result)
        """
        import asyncio

        # Fast path: result already ready
        if self._is_ready:
            if self._is_exc:
                # Exception ready - raise it
                async def _raise_exc():
                    raise self._obj
                return _raise_exc().__await__()
            else:
                # Value ready - return it
                async def _return_value():
                    return self._obj
                return _return_value().__await__()

        # Slow path: result not ready yet
        # Need to serve connection until result arrives
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def on_result(async_res):
            """Callback when result becomes ready."""
            if future.done():
                return  # Already resolved (timeout/cancel)

            if async_res._is_exc:
                # Exception - set exception on future
                loop.call_soon_threadsafe(
                    future.set_exception,
                    async_res._obj
                )
            else:
                # Success - set result on future
                loop.call_soon_threadsafe(
                    future.set_result,
                    async_res._obj
                )

        # Register callback
        self.add_callback(on_result)

        # ─── cancel-leak cleanup ─────────────────────────────────────
        # When the awaiter is cancelled (asyncio.wait_for timeout, an
        # outer task.cancel(), the loop shutting down, …) the future
        # is marked done with CancelledError but the AsyncResult still
        # sits in ``Connection._request_callbacks[seq]`` waiting for
        # a reply that may never come — and ``self._callbacks`` still
        # holds ``on_result``, which closes over ``future``. Both keep
        # an entire chain (Task / Future / Context / cells / bound
        # methods) alive indefinitely. On a long-lived process talking
        # to a flapping peer that grows into multi-GB RSS — see
        # a related internal incident analysis (not included here).
        #
        # Hooking ``future.add_done_callback`` runs once per await
        # regardless of how the future finishes:
        #   * Reply arrives normally → ``_seq_request_callback`` has
        #     already popped our seq from the dict; our pop is a no-op
        #     thanks to the ``None`` default.
        #   * Cancel / timeout → we pop the seq ourselves and clear
        #     ``self._callbacks`` so ``on_result``'s closure releases
        #     ``future``.
        #
        # The cleanup is intentionally tolerant: ``Connection`` may
        # already have been torn down (``_request_callbacks`` is
        # ``None`` or replaced), in which case there is nothing to do.
        async_result_self = self

        def _on_future_done(_fut):
            try:
                seq = async_result_self._seq
                if seq is not None:
                    cbs = getattr(async_result_self._conn,
                                  "_request_callbacks", None)
                    if cbs is not None:
                        cbs.pop(seq, None)
                # Drop the closure over ``future`` so the chain is GC'able
                # whether or not the slot was still in the dict.
                async_result_self._callbacks = []
            except Exception:
                # The cleanup must never raise back into asyncio's
                # done-callback loop — that would mark the future as
                # having an unhandled exception and trip the loop's
                # error handler for what is, by definition, a teardown
                # path.
                pass

        future.add_done_callback(_on_future_done)

        # ═══════════════════════════════════════════════════════════════
        # CRITICAL: NO POLLING FALLBACK!
        # ═══════════════════════════════════════════════════════════════
        # The previous implementation had a polling fallback that caused
        # SEVERE CPU performance issues (1000+ polls/sec per async request).
        #
        # HIGH-CPU POLLING IS ABSOLUTELY PROHIBITED!
        #
        # Instead, we REQUIRE enable_asyncio_serving() to be called.
        # This registers the socket with event loop for event-driven I/O.
        #
        # DO NOT re-add polling fallback under ANY circumstances!
        # If you think polling is needed, you are wrong.
        # Fix the real issue instead (missing enable_asyncio_serving call).
        #
        # Why no fallback?
        # 1. Polling = 1000 wakeups/sec = unacceptable CPU usage
        # 2. Users MUST use proper async patterns (enable_asyncio_serving)
        # 3. Fail-fast is better than silent performance degradation
        # ═══════════════════════════════════════════════════════════════

        # Check if asyncio serving is enabled
        if not self._conn._asyncio_enabled:
            # Fail immediately with clear instructions
            raise RuntimeError(
                "AsyncResult.__await__() requires asyncio serving to be enabled!\n"
                "\n"
                "Async RPC calls require event-driven I/O to avoid high-CPU polling.\n"
                "\n"
                "Solution:\n"
                "  If using async_connect():\n"
                "    - async_connect() auto-enables asyncio serving (v5.3.1+)\n"
                "    - Upgrade rpyc_async if you see this error\n"
                "\n"
                "  If using rpyc.connect() in async context:\n"
                "    conn = rpyc.connect('localhost', 18861)\n"
                "    loop = asyncio.get_running_loop()\n"
                "    conn.enable_asyncio_serving(loop)  # ← ADD THIS!\n"
                "    result = await conn.root.async_method()\n"
                "\n"
                "  Server-side (use AsyncioServer, not ThreadedServer):\n"
                "    from rpyc.utils.async_server import AsyncioServer\n"
                "    server = AsyncioServer(MyService, port=18861)\n"
                "    await server.start()\n"
                "\n"
                "See: docs/ASYNCIO_SERVER_MIGRATION.md for details"
            )

        # With asyncio serving enabled, the event loop will call on_readable()
        # when data arrives, which calls _dispatch(), which calls on_result(),
        # which sets future result. No polling needed!

        # Return future's awaitable
        return future.__await__()
