#!/usr/bin/env python3
"""Static gate: reads of the name `rpyc` that no import in the module binds.

After the rpyc -> rpyc_async rename, `zerodeploy.py` kept four calls on a name
nothing bound any more. `compileall` passes them (syntax is valid) and `import`
passes them (NameError fires only at call time), so this AST pass is the only
gate that sees them.

Usage:  python3 tools/namecheck.py rpyc_async tests bin demos examples
Exit 1 if any unbound read is found.
"""
import ast
import pathlib
import sys

TARGET = "rpyc"


def bound_names(tree):
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                names.add(a.asname or a.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                names.add(a.asname or a.name)
    return names


def unbound_reads(path):
    src = path.read_text(encoding="utf-8", errors="replace")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return []
    if TARGET in bound_names(tree):
        return []
    lines = src.splitlines()
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == TARGET and isinstance(node.ctx, ast.Load):
            hits.append((node.lineno, lines[node.lineno - 1].strip()))
    return hits


def main(roots):
    total = 0
    for root in roots:
        for path in sorted(pathlib.Path(root).rglob("*.py")):
            for lineno, text in unbound_reads(path):
                print(f"{path}:{lineno}: {text}")
                total += 1
    print(f"\n{total} unbound read(s) of name {TARGET!r}")
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or ["."]))
