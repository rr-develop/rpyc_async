"""Resolver semantics for connect_subproc()'s default server discovery.

Unit-level only: `shutil.which` is injected, so no subprocess is spawned, no
socket is opened and no thread is started.

The contract (see docs, "connect_subproc resolves a console script by name"):

  * default discovery resolves ONLY the installed entry point
    ``rpyc-async-classic``;
  * it NEVER considers an upstream name (``rpyc_classic.py`` / ``rpyc_classic``),
    even when one is on PATH -- silently launching the upstream server against an
    rpyc-async client binds two peers that are explicitly incompatible;
  * a miss fails loudly rather than falling back to anything;
  * an explicit ``server_file`` bypasses PATH entirely.
"""
import unittest

from rpyc_async.utils import classic


FORK_CMD = "rpyc-async-classic"
UPSTREAM_NAMES = ("rpyc_classic.py", "rpyc_classic")


def fake_which(available):
    """Build a `shutil.which` stand-in backed by an explicit name -> path map."""
    def _which(cmd, *args, **kwargs):
        return available.get(cmd)
    return _which


ONLY_UPSTREAM = fake_which({
    "rpyc_classic.py": "/usr/local/bin/rpyc_classic.py",
    "rpyc_classic": "/usr/local/bin/rpyc_classic",
})
ONLY_FORK = fake_which({FORK_CMD: "/usr/local/bin/rpyc-async-classic"})
BOTH = fake_which({
    FORK_CMD: "/usr/local/bin/rpyc-async-classic",
    "rpyc_classic.py": "/usr/local/bin/rpyc_classic.py",
    "rpyc_classic": "/usr/local/bin/rpyc_classic",
})
EMPTY = fake_which({})


class ConnectSubprocDiscoveryTest(unittest.TestCase):
    def test_only_upstream_on_path_refuses_loudly(self):
        """Upstream present, fork absent -> raise; never select the foreign script."""
        with self.assertRaises(ValueError) as ctx:
            classic._resolve_server_file(None, _which=ONLY_UPSTREAM)
        msg = str(ctx.exception)
        self.assertIn(FORK_CMD, msg)
        # The error must not point the user at the upstream script.
        for name in UPSTREAM_NAMES:
            self.assertNotIn(f"/{name}", msg)

    def test_only_fork_on_path_is_selected(self):
        got = classic._resolve_server_file(None, _which=ONLY_FORK)
        self.assertEqual(got, "/usr/local/bin/rpyc-async-classic")

    def test_empty_path_refuses_loudly(self):
        with self.assertRaises(ValueError):
            classic._resolve_server_file(None, _which=EMPTY)

    def test_coexistence_selects_fork_never_upstream(self):
        """The scenario this rename exists for: both installed side by side."""
        got = classic._resolve_server_file(None, _which=BOTH)
        self.assertEqual(got, "/usr/local/bin/rpyc-async-classic")
        for name in UPSTREAM_NAMES:
            self.assertNotIn(name, got)

    def test_explicit_server_file_bypasses_path(self):
        """An explicit path wins, even when PATH would resolve to something else."""
        explicit = "/somewhere/bin/rpyc_async_classic.py"
        self.assertEqual(
            classic._resolve_server_file(explicit, _which=EMPTY), explicit)
        self.assertEqual(
            classic._resolve_server_file(explicit, _which=BOTH), explicit)


if __name__ == "__main__":
    unittest.main()
