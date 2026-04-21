"""Supporting functions for unit tests

The core logic of the functions `_ignore_deprecated_imports` and `import_module` is from the cpython code base:
- https://github.com/python/cpython/blob/da576e08296490e94924421af71001bcfbccb317/Lib/test/support/import_helper.py

═══════════════════════════════════════════════════════════════════════════════
NO SAME-PROCESS SERVER+CLIENT POLICY — READ BEFORE WRITING RPYC TESTS
═══════════════════════════════════════════════════════════════════════════════
In this project, an ``AsyncioServer`` and an rpyc client that talks to it
MUST live in DIFFERENT processes. There are no exceptions.

WHY
  ``AsyncioServer`` dispatch and the rpyc client both work by registering
  themselves on the event loop via ``loop.add_reader(fd, cb)`` and/or
  ``loop.sock_accept`` / ``loop.sock_connect``. When both run on the same
  loop in the same process they compete for the same callback-dispatch
  pump and deadlock on the very first round-trip that requires the peer
  to answer before a local future resolves (GETROOT, INSPECT, any async
  RPC). This is an architectural property of single-threaded cooperative
  scheduling, not a bug.

HOW
  Use ``mp_asyncio_server(service_cls, ...)`` below. It starts the server
  in a fresh ``multiprocessing.Process``, waits for it to report ready,
  yields the bound port, and tears the process down on exit.

  ```python
  from tests.support import mp_asyncio_server

  class TestFoo(unittest.TestCase):
      def test_bar(self):
          with mp_asyncio_server(MyService) as port:
              async def go():
                  conn = await rpyc.async_connect("localhost", port)
                  assert await conn.root.some_async_method() == "ok"
                  await conn.aclose()
              asyncio.run(go())
  ```

FORBIDDEN
  * Running ``AsyncioServer.start()`` and an rpyc client in the same
    Python process (same process — even in different threads — is
    tolerated by ThreadedServer for legacy reasons but is NOT allowed
    for tests going forward; use ``mp_asyncio_server``).
  * Starting the server via ``asyncio.create_task(server.start())`` and
    then connecting from the same ``asyncio.run(...)`` call. This is the
    exact shape that caused deadlock reports; it will hang.
  * Bypassing this rule with ``# noqa``, skipping tests, or hand-rolled
    ``Thread`` hacks. If you think you need an exception, you don't.
═══════════════════════════════════════════════════════════════════════════════
"""
import warnings
import sys
import contextlib
import unittest
import socket
import asyncio
import multiprocessing
from multiprocessing.queues import Queue as MPQueue
from typing import Any, Callable, Iterator, Optional


@contextlib.contextmanager
def _ignore_deprecated_imports(ignore=True):
    """Context manager to suppress package and module deprecation
    warnings when importing them.
    If ignore is False, this context manager has no effect.
    """
    if ignore:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", ".+ (module|package)",
                                    DeprecationWarning)
            yield
    else:
        yield


def import_module(name, deprecated=False, *, required_on=(), fromlist=()):
    """Import and return the module to be tested, raising SkipTest if
    it is not available.
    If deprecated is True, any module or package deprecation messages
    will be suppressed. If a module is required on a platform but optional for
    others, set required_on to an iterable of platform prefixes which will be
    compared against sys.platform.
    """
    with _ignore_deprecated_imports(deprecated):
        try:
            module = __import__(name, fromlist=fromlist)
            for a in fromlist:
                if not hasattr(module, a):
                    raise ImportError(f"cannot import name '{a}' from '{name}'")
            return module
        except ImportError as msg:
            if sys.platform.startswith(tuple(required_on)):
                raise
            raise unittest.SkipTest(str(msg))


def get_free_port():
    """
    Get a free port by binding to port 0 and letting the OS assign one.

    Returns:
        int: An available port number

    Usage:
        ALWAYS use this function instead of hardcoded ports in tests to prevent:
        - Port conflicts when tests run in parallel
        - Port conflicts with other processes
        - Race conditions between test executions

        Correct usage pattern:
        ```python
        def setUp(self):
            # Get unique port for THIS test instance
            self.port = get_free_port()
            self.server = AsyncioServer(MyService, port=self.port)
            await self.server.start()

        def test_something(self):
            # Use self.port to connect
            conn = rpyc.connect("localhost", self.port)
        ```

        WRONG - DO NOT DO THIS:
        ```python
        # ❌ WRONG: Hardcoded port causes conflicts
        server = AsyncioServer(MyService, port=18870)

        # ❌ WRONG: setUpClass with shared port causes race conditions
        @classmethod
        def setUpClass(cls):
            cls.port = get_free_port()  # Shared across tests = BAD
        ```

    Important:
        - Call get_free_port() in setUp() (per-test), NOT in setUpClass() (per-class)
        - Each test should get its own unique port
        - Use instance variables (self.port), not class variables (cls.port)
        - This ensures test isolation and prevents race conditions

    Note:
        There is a small race condition where the port could be taken between
        when we release it and when it's used, but this is unlikely in practice.
        The benefits of avoiding hardcoded ports far outweigh this minimal risk.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


# ═════════════════════════════════════════════════════════════════════════════
# Multi-process AsyncioServer helper — see policy block at top of this file.
# ═════════════════════════════════════════════════════════════════════════════


def _mp_server_entrypoint(
    service_factory: Callable[[], Any],
    port: int,
    ready_queue: "MPQueue[str]",
    protocol_config: Optional[dict[str, Any]],
) -> None:
    """Run an ``AsyncioServer`` for ``service_factory()`` inside this process.

    Called by ``mp_asyncio_server`` via ``multiprocessing.Process``. Lives at
    module level (not a closure) so it is picklable on spawn-start-method
    platforms.
    """
    # Imported inside the child so that the parent is not forced to import
    # heavy rpyc pieces just to start a server process.
    from rpyc.utils.async_server import AsyncioServer

    service_cls = service_factory()

    async def _main() -> None:
        server = AsyncioServer(
            service_cls,
            hostname="localhost",
            port=port,
            protocol_config=protocol_config or {},
        )
        await server.start()
        try:
            ready_queue.put("ready")
            # Sleep until the parent kills us.
            await asyncio.Event().wait()
        finally:
            await server.close()

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass


@contextlib.contextmanager
def mp_asyncio_server(
    service_factory: Callable[[], Any],
    *,
    protocol_config: Optional[dict[str, Any]] = None,
    ready_timeout: float = 5.0,
) -> Iterator[int]:
    """Start ``AsyncioServer(service_factory())`` in a child process.

    The child process binds a free port, runs the server, and reports
    ``"ready"`` on a shared queue. The parent yields that port. On
    ``__exit__`` the child is terminated (and SIGKILL'd if it refuses).

    Parameters
    ----------
    service_factory:
        A zero-arg **picklable** callable that returns the rpyc
        ``Service`` class (NOT an instance — ``AsyncioServer`` expects a
        class). A top-level class reference works fine; a local class
        defined inside a test method does NOT pickle on spawn.
    protocol_config:
        Optional dict passed to ``AsyncioServer(protocol_config=...)``.
    ready_timeout:
        How long to wait for the child to report ``"ready"``. Raises
        ``RuntimeError`` on timeout (and best-effort kills the child).

    Yields
    ------
    int: the port the child process bound to.
    """
    port = get_free_port()
    ready_queue: "MPQueue[str]" = multiprocessing.Queue()
    proc = multiprocessing.Process(
        target=_mp_server_entrypoint,
        args=(service_factory, port, ready_queue, protocol_config),
        daemon=True,
    )
    proc.start()
    try:
        try:
            signal = ready_queue.get(timeout=ready_timeout)
        except Exception as exc:
            raise RuntimeError(
                f"AsyncioServer child process failed to report ready within "
                f"{ready_timeout}s: {exc!r}",
            ) from exc
        if signal != "ready":
            raise RuntimeError(
                f"Unexpected ready-queue signal from child: {signal!r}",
            )
        yield port
    finally:
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=2.0)
            if proc.is_alive():
                proc.kill()
                proc.join(timeout=1.0)
