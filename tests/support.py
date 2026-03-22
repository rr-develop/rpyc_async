"""Supporting functions for unit tests

The core logic of the functions `_ignore_deprecated_imports` and `import_module` is from the cpython code base:
- https://github.com/python/cpython/blob/da576e08296490e94924421af71001bcfbccb317/Lib/test/support/import_helper.py
"""
import warnings
import sys
import contextlib
import unittest
import socket


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
