"""
RPyC AsyncioServer - Native asyncio server with persistent event loops

This server implementation provides full bidirectional async support by:
1. Using asyncio.start_server() instead of threading
2. Maintaining persistent event loops for each connection
3. Enabling server ↔ client async callbacks to work properly
4. No thread creation - all async operations use existing event loops

Critical difference from ThreadedServer:
- ThreadedServer: Uses asyncio.run() fallback → temporary loop per request → no bidirectional async
- AsyncioServer: Uses persistent loop → bidirectional async works!

═══════════════════════════════════════════════════════════════════════════
    NO POLLING POLICY — STRICT BAN (file-wide)
═══════════════════════════════════════════════════════════════════════════
DO NOT use polling (``while <cond>: await asyncio.sleep(x)``) anywhere in
this file. An earlier version of ``_serve_connection`` polled every 100 ms
to check ``conn.closed``. Measured impact:
  * 1 connection  → ~10 wakeups/sec
  * 2 connections → ~33% CPU sustained
  * N connections → linear scaling of wakeups

Correct async primitives for this file:
  * ``await conn.wait_closed()``        — for "wait until connection closes"
  * ``loop.sock_accept(sock)``          — for "wait for incoming connection"
  * ``loop.add_reader(fd, cb)``         — for "wake when fd is readable"
  * ``asyncio.Event().wait()``          — for "wait for a one-shot signal"
  * ``asyncio.Queue.get()``             — for "wait for next item"

If you think you need a timer here, you're wrong. Ask yourself what event
you're actually waiting for and register for it directly. Reviewers must
reject any PR that adds a polling loop to this file.

Enforced by ``tests/test_no_polling_policy.py``.
═══════════════════════════════════════════════════════════════════════════
"""
import asyncio
import socket
import logging
from rpyc.core import SocketStream, Connection, Channel
from rpyc.utils.authenticators import AuthenticationError


class AsyncioServer:
    """
    Asyncio-native RPyC server with persistent event loops.

    This server implementation enables full bidirectional async support:
    - Server can call client async methods
    - Client can call server async methods
    - Recursive async callbacks work
    - No thread creation (all async operations in event loop)

    Example:
        >>> class MyService(rpyc.Service):
        ...     async def exposed_hello(self):
        ...         return "Hello!"
        ...
        >>> async def main():
        ...     server = AsyncioServer(MyService, port=18861)
        ...     await server.start()
        ...
        >>> asyncio.run(main())
    """

    def __init__(self, service, hostname='localhost', port=18861,
                 backlog=socket.SOMAXCONN, reuse_addr=True,
                 authenticator=None, protocol_config=None, logger=None):
        """
        Initialize AsyncioServer.

        Args:
            service: The rpyc.Service class to expose
            hostname: Host to bind to (default: localhost)
            port: Port to bind to (default: 18861)
            backlog: Socket backlog for listen()
            reuse_addr: Whether to set SO_REUSEADDR
            authenticator: Authentication handler (optional)
            protocol_config: RPyC protocol configuration dict
            logger: Logger instance (optional)
        """
        self.service = service
        self.hostname = hostname
        self.port = port
        self.backlog = backlog
        self.reuse_addr = reuse_addr
        self.authenticator = authenticator
        self.protocol_config = protocol_config or {}

        if logger is None:
            logger = logging.getLogger(f"AsyncioServer/{port}")
        self.logger = logger

        self.server = None
        self.connections = set()  # Track active connections
        self._closed = False

    async def start(self):
        """
        Start the asyncio server.

        This creates a listening socket and uses loop.sock_accept() to accept
        connections asynchronously. This gives us full control over the socket
        so we can use it with RPyC's SocketStream.

        Returns:
            None
        """
        self.logger.info(f"Starting AsyncioServer on {self.hostname}:{self.port}")

        # Create listening socket manually (not using asyncio.start_server)
        # This way we have full control over accepted sockets
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if self.reuse_addr:
            self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.listener.bind((self.hostname, self.port))
        self.listener.listen(self.backlog)
        self.listener.setblocking(False)  # Non-blocking for asyncio

        # Get actual port if using port=0
        if self.port == 0:
            self.port = self.listener.getsockname()[1]

        self.logger.info(f"AsyncioServer listening on {self.hostname}:{self.port}")

        # Start accepting connections in background task
        loop = asyncio.get_running_loop()
        self._accept_task = loop.create_task(self._accept_loop())

        return None

    async def serve_forever(self):
        """
        Start server and serve forever.

        This is the main entry point for running the server.

        Example:
            >>> server = AsyncioServer(MyService, port=18861)
            >>> asyncio.run(server.serve_forever())
        """
        await self.start()

        # Wait forever (accept loop runs in background)
        try:
            await asyncio.Future()  # Wait indefinitely
        except asyncio.CancelledError:
            await self.close()
            raise

    async def _accept_loop(self):
        """
        Accept connections in a loop.

        This runs as a background task and accepts connections using
        loop.sock_accept() which is async-friendly.
        """
        loop = asyncio.get_running_loop()
        self.logger.info(f"Accept loop started (closed={self._closed})")

        while not self._closed:
            try:
                self.logger.info(f"Waiting for connection (closed={self._closed})...")
                # Accept connection asynchronously
                sock, addr = await loop.sock_accept(self.listener)
                self.logger.info(f"Accepted connection from {addr}")

                # Handle client in separate task
                task = loop.create_task(self._handle_client(sock, addr))
                # Store task to prevent garbage collection
                self.connections.add(task)
                task.add_done_callback(self.connections.discard)

            except asyncio.CancelledError:
                self.logger.info("Accept loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error accepting connection: {e}", exc_info=True)

        self.logger.info(f"Accept loop exited (closed={self._closed})")

    async def _handle_client(self, sock, addr):
        """
        Handle a single client connection.

        CRITICAL: This runs as a coroutine in the main event loop,
        NOT in a separate thread. This enables persistent event loop
        for bidirectional async communication.

        Args:
            sock: socket.socket - the accepted client socket
            addr: tuple - client address
        """
        self.logger.info(f"New connection from {addr}")

        try:
            # Set socket to blocking mode for RPyC (RPyC expects blocking socket)
            sock.setblocking(True)

            # Create SocketStream from the socket
            stream = SocketStream(sock)

            # Authenticate if needed
            if self.authenticator:
                try:
                    sock, credentials = self.authenticator(sock)
                    # Update stream if socket changed
                    stream = SocketStream(sock)
                except AuthenticationError as e:
                    self.logger.warning(f"Authentication failed for {addr}: {e}")
                    sock.close()
                    return
                # Update protocol config with credentials
                config = self.protocol_config.copy()
                config['credentials'] = credentials
                config['endpoints'] = (sock.getsockname(), addr)
                config['logger'] = self.logger
            else:
                config = self.protocol_config.copy()
                config['endpoints'] = (sock.getsockname(), addr)
                config['logger'] = self.logger
                credentials = None

            # Create RPyC connection using service._connect
            # CRITICAL: This connection will use the CURRENT event loop
            # (the one running this coroutine), which is persistent!
            conn = self.service._connect(
                Channel(stream),
                config=config
            )

            # CRITICAL: Enable asyncio serving immediately
            # This registers the connection's file descriptor with the event loop
            # so all incoming messages are processed asynchronously
            loop = asyncio.get_running_loop()
            self.logger.info(f"Enabling asyncio serving for connection from {addr}")
            conn.enable_asyncio_serving(loop=loop)
            self.logger.info(f"Asyncio serving enabled for {addr}")

            self.logger.info(f"Connection established with {addr}")

            # Serve connection until it closes
            # The connection will process messages asynchronously via the event loop
            try:
                await self._serve_connection(conn, sock)
            finally:
                # Cleanup. Use aclose() — the sync close() path would hit the
                # sync_request guard (we are running on the connection's own
                # serving loop) and raise. aclose() is the event-driven equivalent.
                self.logger.info(f"Connection closed from {addr}")
                try:
                    await conn.aclose()
                except Exception:
                    # Connection may already be mid-close; log and continue.
                    self.logger.debug(
                        "aclose() raised during cleanup — ignoring",
                        exc_info=True,
                    )

        except Exception as e:
            self.logger.error(f"Error handling client {addr}: {e}", exc_info=True)

    async def _serve_connection(self, conn, sock):
        """
        Serve a connection until it closes.

        CRITICAL: The connection is already registered with the event loop
        via enable_asyncio_serving(), so incoming messages are processed
        automatically via add_reader() callback. We just wait for connection to close.

        ═══════════════════════════════════════════════════════════════════
        NO POLLING POLICY — STRICT BAN
        ═══════════════════════════════════════════════════════════════════
        DO NOT reintroduce ``while not conn.closed: await asyncio.sleep(x)``.
        That pattern was previously here and caused ~33% CPU with just two
        open RPyC connections (10 wakeups/sec per connection at sleep(0.1);
        scales linearly with connection count). It also masks stale
        connections whose ``.closed`` flag is never flipped by the peer —
        they keep burning cycles forever.

        The ONLY correct way to wait for a connection to close in this path:

            await conn.wait_closed()

        This suspends the coroutine on a Future resolved by ``close()`` from
        inside ``Connection._fire_close_notifications``. Zero wakeups while
        idle.

        If you think you need a timer here, STOP and ask yourself:
          * Is there a real event you should register for instead?
          * Would `loop.add_reader(fd, cb)` fit the job?
          * Would `asyncio.Event` / `asyncio.Queue` fit the job?
        The answer in async_server.py has always been yes.
        ═══════════════════════════════════════════════════════════════════

        Args:
            conn: RPyC Connection instance
            sock: socket.socket
        """
        try:
            # Event-driven wait: zero CPU while the connection is healthy.
            # Resolves the instant close() fires, not on the next tick boundary.
            await conn.wait_closed()

        except asyncio.CancelledError:
            # Server is shutting down
            self.logger.info("Connection task cancelled")
            raise

        except Exception as e:
            self.logger.error(f"Error serving connection: {e}", exc_info=True)

        finally:
            # Disable asyncio serving
            try:
                conn.disable_asyncio_serving()
            except Exception:
                pass

    async def close(self):
        """Close the server and all connections."""
        if self._closed:
            return

        self._closed = True
        self.logger.info("Closing AsyncioServer")

        # Cancel accept task
        if hasattr(self, '_accept_task'):
            self._accept_task.cancel()
            try:
                await self._accept_task
            except asyncio.CancelledError:
                pass

        # Cancel all connection tasks
        for task in list(self.connections):
            if isinstance(task, asyncio.Task):
                task.cancel()

        # Close listener
        if hasattr(self, 'listener'):
            self.listener.close()

        self.logger.info("AsyncioServer closed")

    def __enter__(self):
        """Context manager support (not recommended - use async with instead)."""
        raise RuntimeError("Use 'async with AsyncioServer(...)' instead of 'with'")

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
        return False


# Convenience function for running server
def run_async_server(service, hostname='localhost', port=18861, **kwargs):
    """
    Run AsyncioServer with asyncio.run().

    This is a convenience function for simple use cases.

    Example:
        >>> run_async_server(MyService, port=18861)

    Args:
        service: The rpyc.Service class to expose
        hostname: Host to bind to
        port: Port to bind to
        **kwargs: Additional arguments for AsyncioServer
    """
    async def _run():
        server = AsyncioServer(service, hostname, port, **kwargs)
        await server.serve_forever()

    asyncio.run(_run())
