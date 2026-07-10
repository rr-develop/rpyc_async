.. _ssl:

SSL
===
Using external tools, you can generate client and server certificates, and a certificate
authority. After going through this setup stage, you can easily establish an SSL-enabled
connection.

Server side::

    from rpyc_async.utils.authenticators import SSLAuthenticator
    from rpyc_async.utils.server import ThreadedServer

    # ...

    authenticator = SSLAuthenticator("myserver.key", "myserver.cert")
    server = ThreadedServer(SlaveService, port = 12345, authenticator = authenticator)
    server.start()

Client side::

    import rpyc_async as rpyc

    conn = rpyc.ssl_connect("hostname", port = 12345, keyfile="client.key",
                            certfile="client.cert")

For more info, see the documentation of `ssl module <https://docs.python.org/3/library/ssl.html>`_.
