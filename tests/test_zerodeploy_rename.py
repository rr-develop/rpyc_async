"""Structural invariants of zerodeploy after the rpyc -> rpyc_async rename.

Runs entirely against fakes: no SSH, no socket, no thread, no polling.

Two things must hold, and they are coupled:

  * the package directory copied to the remote host is named ``rpyc_async``;
  * the rendered SERVER_SCRIPT imports ``rpyc_async.utils.server`` and
    ``rpyc_async.core.service``.

The remote script does ``sys.path.insert(0, here)`` and then imports the module
path from the rendered template, so the copied directory's basename and the
module root in the script MUST agree, or the remote import fails.

The fake process returns a valid port from ``readline()`` immediately. That is
deliberate: the tunnel setup (and its use of the module-level ``rpyc`` name) is
only reachable *after* the port is parsed. A fake that fails the port read would
silently skip that code path.
"""
import re
import unittest

from rpyc_async.utils import zerodeploy


PKG = "rpyc_async"


class FakePath:
    """Minimal stand-in for a plumbum remote path."""

    def __init__(self, name, sink=None):
        self.name = name
        self.sink = sink if sink is not None else []

    def __truediv__(self, other):
        return FakePath(f"{self.name}/{other}", self.sink)

    def write(self, data):
        self.sink.append(data)

    def __str__(self):
        return self.name


class FakeTmpCtx:
    def __init__(self, sink):
        self.sink = sink

    def __enter__(self):
        return FakePath("/remote/tmp", self.sink)

    def __exit__(self, *exc):
        return False


class FakeProc:
    """Yields a valid port at once, so tunnel setup is actually reached."""

    argv = ["fake-python", "deployed-rpyc.py"]
    returncode = 0

    def __init__(self):
        self.stdout = self

    def readline(self):
        return b"18861\n"

    def terminate(self):
        pass

    def communicate(self):
        # Reached only if the port read raises; present so a failure surfaces as
        # the real error rather than AttributeError on the fake.
        return b"", b""


class FakeCmd:
    def __init__(self, popen_calls):
        self.popen_calls = popen_calls

    def popen(self, script, new_session=False):
        self.popen_calls.append(str(script))
        return FakeProc()


class FakeMachine:
    def __init__(self, sink, popen_calls, tunnels):
        self.sink = sink
        self.python = FakeCmd(popen_calls)
        self._popen_calls = popen_calls
        self.tunnels = tunnels

    def tempdir(self):
        return FakeTmpCtx(self.sink)

    def __getitem__(self, key):
        return FakeCmd(self._popen_calls)

    def tunnel(self, lport, rport):
        self.tunnels.append((lport, rport))
        return object()


class _FakeLocal:
    """Stands in for plumbum's `local`; resolves the package root."""

    @staticmethod
    def path(p):
        class _P:
            def up(self):
                return f"/local/site-packages/{PKG}"
        return _P()


class ZerodeployRenameTest(unittest.TestCase):
    def setUp(self):
        self.rendered = []
        self.popen_calls = []
        self.tunnels = []
        self.copied = []

        self._orig_copy = zerodeploy.copy
        self._orig_local = zerodeploy.local
        zerodeploy.copy = lambda src, dst: self.copied.append((str(src), str(dst)))
        zerodeploy.local = _FakeLocal()

    def tearDown(self):
        zerodeploy.copy = self._orig_copy
        zerodeploy.local = self._orig_local

    def _deploy(self):
        machine = FakeMachine(self.rendered, self.popen_calls, self.tunnels)
        return zerodeploy.DeployedServer(machine)

    def test_construction_has_no_unbound_name(self):
        """After the rename, `rpyc` is no longer bound in zerodeploy's globals.

        Any surviving `rpyc.foo()` call raises NameError -- and only at call
        time, which is why neither compileall nor import catches it.
        """
        try:
            self._deploy()
        except NameError as exc:  # pragma: no cover - the failure we guard against
            self.fail(f"unbound name reached at runtime: {exc}")

    def test_copied_package_directory_is_renamed(self):
        self._deploy()
        self.assertTrue(self.copied, "the package was never copied")
        _src, dst = self.copied[0]
        basename = dst.rstrip("/").rsplit("/", 1)[-1]
        self.assertEqual(basename, PKG)

    def test_rendered_script_imports_renamed_modules(self):
        self._deploy()
        self.assertTrue(self.rendered, "SERVER_SCRIPT was never written")
        script = self.rendered[0]
        self.assertIn(f"from {PKG}.utils.server import ThreadedServer", script)
        self.assertIn(f"from {PKG}.core.service import SlaveService", script)

    def test_copied_dir_and_module_root_agree(self):
        """The three-way coupling: server_class / service_class / copied dir."""
        self._deploy()
        script = self.rendered[0]
        roots = set(re.findall(r"^from (\w+)\.", script, re.M))
        self.assertEqual(roots, {PKG})

        _src, dst = self.copied[0]
        basename = dst.rstrip("/").rsplit("/", 1)[-1]
        self.assertEqual(
            roots, {basename},
            "copied directory name must match the module root the remote "
            "SERVER_SCRIPT imports, or the remote import fails",
        )


if __name__ == "__main__":
    unittest.main()
