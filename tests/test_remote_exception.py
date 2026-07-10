from __future__ import with_statement
import rpyc
import unittest


# A version whose *major* differs from ours, so that vinegar appends the
# mismatch warning. Must not be derived from rpyc.version, or the test would
# silently stop exercising the warning whenever our own major changes.
MISMATCHED_VERSION = '0.0.0'


class MyService(rpyc.Service):

    def exposed_set_version(self):
        rpyc.version.__version__ = MISMATCHED_VERSION

    def exposed_remote_assert(self, val):
        assert val


class TestRemoteException(unittest.TestCase):
    def setUp(self):
        self.server = rpyc.utils.server.OneShotServer(MyService, port=0)
        self.server.logger.quiet = False
        self.server._start_in_thread()
        self.original_version_string = rpyc.version.__version__
        self.conn = rpyc.connect("localhost", port=self.server.port)

    def tearDown(self):
        rpyc.version.__version__ = self.original_version_string
        self.conn.close()

    def test_remote_exception(self):
        # Since the server/client share the same namespace, the version will change for both.
        # Even so, this should suffice for unit testing
        warn_msg = 'WARNING: Remote is on RPyC {} and local is on RPyC {}.'.format(
            MISMATCHED_VERSION, MISMATCHED_VERSION)
        try:
            self.conn.root.remote_assert(False)
        except Exception as exc:
            exc_rpyc_version = exc._remote_version
            exc_remote_tb = exc._remote_tb
        else:
            exc_rpyc_version = None
            exc_remote_tb = ''
        self.assertEqual(self.original_version_string, exc_rpyc_version)
        self.assertFalse(warn_msg in exc_remote_tb)
        try:
            self.conn.root.set_version()
            self.conn.root.remote_assert(False)
        except Exception as exc:
            exc_rpyc_version = exc._remote_version
            exc_remote_tb = exc._remote_tb
        else:
            exc_rpyc_version = None
            exc_remote_tb = ''
        self.assertEqual(MISMATCHED_VERSION, exc_rpyc_version)
        self.assertTrue(warn_msg in exc_remote_tb)


class TestExclusionsRemoteException(unittest.TestCase):
    def setUp(self):
        config = {'include_local_traceback': False, 'include_local_version': False}
        self.server = rpyc.utils.server.OneShotServer(MyService, port=0, protocol_config=config)
        self.server.logger.quiet = False
        self.server._start_in_thread()
        self.conn = rpyc.connect("localhost", port=self.server.port)

    def tearDown(self):
        self.conn.close()

    def test_remote_exception(self):
        try:
            self.conn.root.remote_assert(False)
        except Exception as exc:
            exc_rpyc_version = exc._remote_version
            exc_remote_tb = exc._remote_tb
        else:
            exc_rpyc_version = None
            exc_remote_tb = ''
        self.assertEqual("<traceback denied>", exc_remote_tb)
        self.assertEqual("<version denied>", exc_rpyc_version)


if __name__ == "__main__":
    unittest.main()
