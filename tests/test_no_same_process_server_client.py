"""
Enforcement: no test may run ``AsyncioServer`` and its rpyc client in the
same OS process.

See ``docs/DESIGN_NO_SAME_PROCESS_TESTS.md`` and the policy block at the
top of ``tests/support.py``.

The scanner is a static AST pass: fast and deterministic, runs on every
pytest invocation. It catches the three anti-patterns we keep seeing:

1. ``AsyncioServer(...)`` instantiated at module scope (i.e. outside a
   function that is the target of ``multiprocessing.Process``). That
   shape means the server lives in the test's own process.

2. ``asyncio.create_task(server.start())`` — the classic "start the
   server on the current loop" move. Always a deadlock with rpyc.

3. ``Thread(target=... server.start ...)`` or
   ``server._start_in_thread()`` — cross-thread same-process tricks.

Allow-list: files where ``AsyncioServer(...)`` appears at module scope
for legitimate reasons (mocks, unit tests that never call ``.start()``).
Every entry has a written justification.
"""
from __future__ import annotations

import ast
import unittest
from pathlib import Path
from typing import List, Tuple

_TESTS_DIR: Path = Path(__file__).resolve().parent
_POLICY_DOC: str = "docs/DESIGN_NO_SAME_PROCESS_TESTS.md"

# ─── Allow-list ─────────────────────────────────────────────────────────────
# Each entry is ``(filename, justification)``. Keep justifications short
# and concrete. If in doubt, do NOT add to the allow-list — migrate the
# test to ``mp_asyncio_server`` instead.

_ALLOW_MODULE_SCOPE_INSTANTIATION: dict[str, str] = {
    # Enforcement test itself: scans other test files, does not run
    # servers.
    "test_no_same_process_server_client.py":
        "Enforcement test; imports ast only, never instantiates a server.",
    # Static / unit test on the no-polling signal path. Mocks a
    # Connection, never calls AsyncioServer.start(); the ``AsyncioServer``
    # import is exercised through ``AsyncioServer.__new__(AsyncioServer)``
    # on a mock harness and through source inspection only.
    "test_no_polling_policy.py":
        "Mock-based; does not start a real server.",
}


# ─── AST visitors ───────────────────────────────────────────────────────────


def _is_name_call(node: ast.AST, name: str) -> bool:
    """True for ``Name(name)(...)``, e.g. ``AsyncioServer(...)``."""
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == name
    )


def _is_attr_call(node: ast.AST, attr: str) -> bool:
    """True for ``something.ATTR(...)``, e.g. ``asyncio.create_task(...)``."""
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == attr
    )


def _calls_server_start(call: ast.Call) -> bool:
    """Match calls of the shape ``<something>.start()`` where the callee
    looks like a server object.

    We can't do full type inference, so we approximate: the receiver
    variable/attribute name contains ``server`` or ``srv``, case-insensitive.
    """
    if not isinstance(call.func, ast.Attribute):
        return False
    if call.func.attr != "start":
        return False
    recv = call.func.value
    recv_name: str | None = None
    if isinstance(recv, ast.Name):
        recv_name = recv.id
    elif isinstance(recv, ast.Attribute):
        recv_name = recv.attr
    if recv_name is None:
        return False
    low = recv_name.lower()
    return "server" in low or low == "srv"


def _find_module_scope_asyncio_server(tree: ast.Module) -> List[int]:
    """Return line numbers of ``AsyncioServer(...)`` calls at module scope.

    "Module scope" means: not nested inside any ``FunctionDef`` or
    ``AsyncFunctionDef``. Class bodies count as module-scope here — a
    ``AsyncioServer`` created in a class body is just as bad.
    """
    hits: List[int] = []

    def walk(node: ast.AST, inside_function: bool) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                walk(child, True)
                continue
            if not inside_function and _is_name_call(child, "AsyncioServer"):
                hits.append(getattr(child, "lineno", -1))
            walk(child, inside_function)

    walk(tree, False)
    return hits


def _find_create_task_server_start(tree: ast.Module) -> List[int]:
    """Return line numbers for ``asyncio.create_task(<server>.start())``."""
    hits: List[int] = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "create_task"
        ):
            continue
        if not node.args:
            continue
        inner = node.args[0]
        if isinstance(inner, ast.Call) and _calls_server_start(inner):
            hits.append(getattr(node, "lineno", -1))
    return hits


def _find_threaded_server_start(tree: ast.Module) -> List[int]:
    """Return line numbers where a thread is spawned to run ``server.start()``
    in the same process.
    """
    hits: List[int] = []
    for node in ast.walk(tree):
        # Thread(target=<server>.start)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "Thread"
        ):
            for kw in node.keywords:
                if kw.arg == "target" and isinstance(kw.value, ast.Attribute):
                    if kw.value.attr == "start":
                        recv = kw.value.value
                        recv_name = (
                            recv.id if isinstance(recv, ast.Name)
                            else getattr(recv, "attr", None)
                        )
                        if recv_name and (
                            "server" in recv_name.lower() or recv_name.lower() == "srv"
                        ):
                            hits.append(getattr(node, "lineno", -1))
        # server._start_in_thread()
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_start_in_thread"
        ):
            hits.append(getattr(node, "lineno", -1))
    return hits


# ─── The test ───────────────────────────────────────────────────────────────


class TestNoSameProcessServerClient(unittest.TestCase):
    """Scan every ``tests/test_*.py`` and reject same-process server+client
    patterns.
    """

    def _iter_test_files(self) -> List[Path]:
        return sorted(_TESTS_DIR.glob("test_*.py"))

    def test_no_module_scope_asyncio_server(self) -> None:
        violations: List[Tuple[str, int]] = []
        for path in self._iter_test_files():
            if path.name in _ALLOW_MODULE_SCOPE_INSTANTIATION:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for lineno in _find_module_scope_asyncio_server(tree):
                violations.append((path.name, lineno))

        if violations:
            lines = "\n".join(f"  {name}:{lineno}" for name, lineno in violations)
            raise AssertionError(
                "Found AsyncioServer(...) at module scope — forbidden "
                "by NO SAME-PROCESS SERVER+CLIENT policy. The server must "
                "run in a child process via "
                "``tests.support.mp_asyncio_server``.\n"
                f"Violations:\n{lines}\n\n"
                f"Policy: {_POLICY_DOC}. If you believe the violation is a "
                "false positive (e.g. mock-only code that never calls "
                ".start()), add the filename to "
                "_ALLOW_MODULE_SCOPE_INSTANTIATION with a written "
                "justification."
            )

    def test_no_create_task_server_start(self) -> None:
        violations: List[Tuple[str, int]] = []
        for path in self._iter_test_files():
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for lineno in _find_create_task_server_start(tree):
                violations.append((path.name, lineno))

        if violations:
            lines = "\n".join(f"  {name}:{lineno}" for name, lineno in violations)
            raise AssertionError(
                "Found `asyncio.create_task(<server>.start())` in test "
                "files — this puts the server on the SAME loop as the "
                "client and deadlocks on the first round-trip. Use "
                "``tests.support.mp_asyncio_server`` instead.\n"
                f"Violations:\n{lines}\n\nPolicy: {_POLICY_DOC}"
            )

    def test_no_thread_wrapped_server_start_in_asyncio_tests(self) -> None:
        """Thread-wrapped server starts are forbidden in files that also
        use ``AsyncioServer`` or the async client (``async_connect`` /
        awaiting netref calls).

        We deliberately do NOT scan legacy ThreadedServer-only tests
        (upstream rpyc test modules like ``test_threaded_server.py``,
        ``test_ssl.py``, ``test_netref_hierachy.py``): those exercise
        ThreadedServer semantics, which is by design a server-thread +
        client-thread model in one process. Migrating them to
        multiprocess would change *what* is tested, not how.
        """
        violations: List[Tuple[str, int]] = []
        for path in self._iter_test_files():
            src = path.read_text(encoding="utf-8")
            # Only scan files that touch the AsyncioServer / async_connect
            # surface — those are the ones this policy is about.
            touches_asyncio = (
                "AsyncioServer" in src
                or "async_connect" in src
                or "rpyc.aio_connect" in src
            )
            if not touches_asyncio:
                continue
            tree = ast.parse(src, filename=str(path))
            for lineno in _find_threaded_server_start(tree):
                violations.append((path.name, lineno))

        if violations:
            lines = "\n".join(f"  {name}:{lineno}" for name, lineno in violations)
            raise AssertionError(
                "Found a server started in a background Thread inside a "
                "file that also uses AsyncioServer or async_connect. "
                "Forbidden by NO SAME-PROCESS SERVER+CLIENT policy. The "
                "server must run in a child process via "
                "``tests.support.mp_asyncio_server``.\n"
                f"Violations:\n{lines}\n\nPolicy: {_POLICY_DOC}"
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
