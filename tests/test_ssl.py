import rpyc_async as rpyc
import os
import unittest
from rpyc_async.utils.authenticators import SSLAuthenticator
from rpyc_async.utils.server import ThreadedServer
from rpyc_async import SlaveService

try:
    import ssl  # noqa
    _ssl_import_failed = False
except ImportError:
    _ssl_import_failed = True


def _server_cert_valid():
    """True if the bundled server test certificate is currently valid.

    These tests authenticate against tests/server.crt. That self-signed
    fixture has a fixed validity window, so once it expires every TLS
    handshake fails with 'certificate expired' before any tested logic runs.
    Skip (rather than fail) when the cert is missing/expired/not-yet-valid;
    regenerate the fixtures to re-enable the tests.
    """
    if _ssl_import_failed:
        return False
    cert = os.path.join(os.path.dirname(__file__), "server.crt")
    try:
        not_after = ssl.cert_time_to_seconds(
            ssl._ssl._test_decode_cert(cert)["notAfter"]
        )
        not_before = ssl.cert_time_to_seconds(
            ssl._ssl._test_decode_cert(cert)["notBefore"]
        )
    except Exception:
        return False
    import time
    now = time.time()
    return not_before <= now <= not_after


_certs_valid = _server_cert_valid()
_certs_skip_reason = "SSL test certificates are missing or expired"


@unittest.skipIf(_ssl_import_failed, "Ssl not available")
@unittest.skipUnless(_certs_valid, _certs_skip_reason)
class Test_SSL(unittest.TestCase):
    '''Created keys/certs like https://gist.github.com/soarez/9688998
    # Server key
    openssl genrsa -out server.key 4096
    openssl req -new -x509 -key server.key -out server.crt
    # Client key
    openssl genrsa -out client.key 4096
    openssl req -new -key client.key -out client.csr
    openssl x509 -req -in  client.csr  -CA server.crt -CAkey server.key -out client.crt
    # Client2 key
    openssl genrsa -out client2.key 4096
    openssl req -new -key client2.key -out client2.csr
    openssl x509 -req -in  client2.csr  -CA client.crt -CAkey client.key -out client2.crt
    # Create bundle
    cat client.crt server.crt > client-server.bundle.crt
    '''

    def setUp(self):
        self.key = os.path.join(os.path.dirname(__file__), "server.key")
        self.cert = os.path.join(os.path.dirname(__file__), "server.crt")
        self.client_key = os.path.join(os.path.dirname(__file__), "client.key")
        self.client_cert = os.path.join(os.path.dirname(__file__), "client.crt")
        self.client2_key = os.path.join(os.path.dirname(__file__), "client2.key")
        self.client2_cert = os.path.join(os.path.dirname(__file__), "client2.crt")
        self.ca_certs = os.path.join(os.path.dirname(__file__), "client-server.bundle.crt")
        print(self.cert, self.key)

        authenticator = SSLAuthenticator(self.key, self.cert, self.ca_certs)
        self.server = ThreadedServer(SlaveService, port=18812,
                                     auto_register=False, authenticator=authenticator)
        self.server.logger.quiet = False
        self.server._start_in_thread()

    def tearDown(self):
        while self.server.clients:
            pass
        self.server.close()

    def test_client(self):
        c = rpyc.classic.ssl_connect("localhost", port=18812,
                                     keyfile=self.client_key, certfile=self.client_cert)
        print(repr(c))
        print(c.modules.sys)
        print(c.modules["xml.dom.minidom"].parseString("<a/>"))
        c.execute("x = 5")
        self.assertEqual(c.namespace["x"], 5)
        self.assertEqual(c.eval("1+x"), 6)
        c.close()

    def test_client2(self):
        '''Assert exception client signed client2, but being in ca bundle is not server signature'''
        with self.assertRaisesRegex(EOFError, 'tlsv[0-9]* alert unknown ca'):
            c = rpyc.classic.ssl_connect("localhost", port=18812,
                                         keyfile=self.client2_key, certfile=self.client2_cert)
            c.close()

    def test_nokey(self):
        '''Assert exception when cert not provided'''
        with self.assertRaisesRegex(EOFError, 'tlsv[0-9]* alert certificate required'):
            c = rpyc.classic.ssl_connect("localhost", port=18812)
            c.close()


@unittest.skipIf(_ssl_import_failed, "Ssl not available")
@unittest.skipUnless(_certs_valid, _certs_skip_reason)
class Test_SSL_CERT_REQUIRED(unittest.TestCase):
    '''It may be nonobvious and easy to misconfigure, but not specify'''
    def setUp(self):
        self.key = os.path.join(os.path.dirname(__file__), "server.key")
        self.cert = os.path.join(os.path.dirname(__file__), "server.crt")
        print(self.cert, self.key)

        authenticator = SSLAuthenticator(self.key, self.cert, cert_reqs=ssl.CERT_REQUIRED)
        self.server = ThreadedServer(SlaveService, port=18812,
                                     auto_register=False, authenticator=authenticator)
        self.server.logger.quiet = False
        self.server._start_in_thread()

    def tearDown(self):
        while self.server.clients:
            pass
        self.server.close()

    def test_nokey(self):
        '''Assert exception when cert not provided'''
        with self.assertRaisesRegex(EOFError, 'tlsv[0-9]* alert certificate required'):
            c = rpyc.classic.ssl_connect("localhost", port=18812)
            c.close()


@unittest.skipIf(_ssl_import_failed, "Ssl not available")
@unittest.skipUnless(_certs_valid, _certs_skip_reason)
class Test_SSL_CERT_NONE(unittest.TestCase):
    '''It may be nonobvious and easy to misconfigure, but not specify'''
    def setUp(self):
        self.key = os.path.join(os.path.dirname(__file__), "server.key")
        self.cert = os.path.join(os.path.dirname(__file__), "server.crt")
        print(self.cert, self.key)

        authenticator = SSLAuthenticator(self.key, self.cert)
        self.server = ThreadedServer(SlaveService, port=18812,
                                     auto_register=False, authenticator=authenticator)
        self.server.logger.quiet = False
        self.server._start_in_thread()

    def tearDown(self):
        while self.server.clients:
            pass
        self.server.close()

    def test_nokey_noexc(self):
        c = rpyc.classic.ssl_connect("localhost", port=18812)
        c.close()


if __name__ == "__main__":
    unittest.main()
