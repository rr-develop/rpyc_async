"""Guard: peer-resolved module-name string literals must name rpyc_async.

conn.modules["X"] causes importlib.import_module("X") on the PEER.
Renaming the import package makes any literal "rpyc.*" unresolvable there.
Neither the AST gate (these are strings) nor sed (word-boundary on identifiers)
catches them. deliver() has no other test coverage at all.
"""
import ast
import pathlib
import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCAN_DIRS = ("rpyc_async", "tests", "bin", "demos", "examples")


def _peer_module_literals():
    """All string literals passed to peer/dotted-path resolvers, repo-wide.

    Covers:
      * ``<expr>.modules[<str>]``  (getitem on .modules)
      * ``<expr>.getmodule(<str>)`` (rpyc classic slave)
      * ``mock.patch(<str>, ...)`` (dotted patch targets — resolvable modules)
    """
    out = []
    for d in SCAN_DIRS:
        for src in sorted((ROOT / d).rglob("*.py")):
            tree = ast.parse(src.read_text(), filename=str(src))
            for node in ast.walk(tree):
                # <expr>.modules[<str>]
                if isinstance(node, ast.Subscript):
                    val = node.value
                    if isinstance(val, ast.Attribute) and val.attr == "modules":
                        sl = node.slice
                        if isinstance(sl, ast.Constant) and isinstance(sl.value, str):
                            out.append((f"{src.relative_to(ROOT)}:{node.lineno}", sl.value))
                # <expr>.getmodule(<str>) and mock.patch(<str>, ...)
                if isinstance(node, ast.Call) and node.args:
                    first = node.args[0]
                    if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
                        continue
                    func = node.func
                    if isinstance(func, ast.Attribute):
                        if func.attr == "getmodule":
                            out.append((f"{src.relative_to(ROOT)}:{node.lineno}", first.value))
                        elif func.attr == "patch":
                            # mock.patch("dotted.path", ...) target must resolve
                            out.append((f"{src.relative_to(ROOT)}:{node.lineno}", first.value))
    return out


def test_call_sites_are_discovered():
    assert _peer_module_literals(), "scanner found nothing; it is broken"


@pytest.mark.parametrize("lineno,name", _peer_module_literals())
def test_peer_module_literal_is_renamed(lineno, name):
    head = name.split(".", 1)[0]
    assert head != "rpyc", (
        f"{lineno} passes {name!r} to the peer; "
        f"importlib.import_module({name!r}) fails after the rename"
    )
