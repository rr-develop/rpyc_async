"""
Async RPyC connection module.

Provides ``async_connect(host, port, ...)`` — the canonical entry point
for talking to an ``AsyncioServer`` from asyncio code:

    import rpyc_async as rpyc
    conn = await rpyc.async_connect("localhost", 18861)
    result = await conn.root.some_async_method()
    await conn.aclose()

Why this exists (vs the synchronous ``rpyc.connect``):

* ``rpyc.connect`` does a blocking ``socket.connect()`` — it stalls the
  event loop for the full duration of the TCP/TLS handshake.
* After connect, the sync path serves requests via blocking ``select()``
  inside ``Connection.serve``. From async code that is either a deadlock
  or — since the ``sync_request`` guard in ``Connection.sync_request`` —
  a loud ``RuntimeError``.

``async_connect`` avoids both:

1. ``loop.sock_connect(sock, (host, port))`` — event-driven TCP connect.
2. ``conn.enable_asyncio_serving(loop=loop)`` — re-routes reads through
   ``loop.add_reader``; no blocking I/O afterwards.
3. Eager handshake — pre-fetches ``_remote_root`` via the awaitable
   ``async_request`` + ``AsyncResult`` path so the first ``conn.root``
   access does not need a ``sync_request`` at all.

NO threads, NO blocking ``socket.connect()``, NO polling.
"""
import asyncio
import socket as socket_module
from typing import Optional, Dict, Any

from rpyc_async.core.stream import SocketStream
from rpyc_async.core.channel import Channel
from rpyc_async.core.service import VoidService
from rpyc_async.core import consts


async def async_connect(
    host: str,
    port: int,
    *,
    service: Any = VoidService,
    config: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
    loop: Optional[asyncio.AbstractEventLoop] = None
) -> Any:  # Returns Connection, but avoid circular import
    """
    Establish async RPyC connection using asyncio.

    CRITICAL: This is FULLY ASYNC - NO blocking socket operations!

    This replaces rpyc.connect() which uses blocking socket.connect().
    Uses loop.sock_connect() for non-blocking TCP connection with a real socket.

    NEW (v5.3): Performs eager handshake to fetch remote root object during connection.
    This prevents blocking sync_request() on first access to conn.root in async context.

    Architecture:
        1. Create non-blocking socket
        2. loop.sock_connect(sock, (host, port)) - async connect
        3. SocketStream(sock) - standard RPyC socket stream
        4. Channel(stream) - RPyC packet layer
        5. service._connect(channel, config) - RPyC connection
        6. Eager handshake: fetch root object asynchronously

    Args:
        host: hostname or IP address to connect to
        port: TCP port number
        service: RPyC service class (default: VoidService)
        config: RPyC configuration dict
        timeout: connection timeout in seconds (None = no timeout)
        loop: event loop to use (None = get current loop)

    Returns:
        RPyC Connection object with _remote_root already fetched

    Raises:
        asyncio.TimeoutError: if connection times out
        ConnectionError: if connection fails

    Example:
        >>> loop = asyncio.get_running_loop()
        >>> conn = await async_connect("127.0.0.1", 18812, timeout=5.0)
        >>> result = conn.root.some_method()  # NO blocking - root already fetched!
    """
    if loop is None:
        loop = asyncio.get_running_loop()

    if config is None:
        config = {}

    # Create non-blocking socket
    sock = socket_module.socket(socket_module.AF_INET, socket_module.SOCK_STREAM)
    sock.setblocking(False)

    # Sentinel so the outer error handler can tear down a half-built Connection
    # (and, critically, UNREGISTER its asyncio reader) instead of just closing
    # the raw socket — which would leak the add_reader and let the freed fd be
    # recycled into a 100%-CPU spin. See a related internal incident analysis
    # (not included here).
    conn = None

    try:
        # Async TCP connection - NO BLOCKING!
        try:
            if timeout:
                await asyncio.wait_for(
                    loop.sock_connect(sock, (host, port)),
                    timeout=timeout
                )
            else:
                await loop.sock_connect(sock, (host, port))
        except asyncio.TimeoutError as e:
            sock.close()
            raise ConnectionError(f"Connection to {host}:{port} timed out after {timeout}s") from e
        except Exception as e:
            sock.close()
            raise ConnectionError(f"Failed to connect to {host}:{port}: {e}") from e

        # Restore blocking mode AFTER connect.
        # SocketStream.write() uses plain sock.send(); on a non-blocking socket
        # that raises BlockingIOError (EAGAIN) instead of sending all bytes,
        # which breaks the handshake. Reads are safe because asyncio serving
        # re-routes them through loop.add_reader (the FD is checked readable
        # before any recv() is issued).
        sock.setblocking(True)

        # Wrap socket in standard RPyC SocketStream
        stream = SocketStream(sock)

        # Create RPyC channel and connection
        channel = Channel(stream)
        conn = service._connect(channel, config)

        # ═══════════════════════════════════════════════════════════════
        # AUTO-ENABLE ASYNCIO SERVING
        # ═══════════════════════════════════════════════════════════════
        # CRITICAL: Automatically enable asyncio serving to prevent
        # high-CPU polling fallback in AsyncResult.__await__().
        #
        # This makes async_connect() fully ready for async operations:
        # - Socket FD registered with event loop (event-driven I/O)
        # - Incoming messages processed via on_readable() callback
        # - Background cleanup task started
        #
        # Users no longer need to manually call enable_asyncio_serving()!
        # ═══════════════════════════════════════════════════════════════
        conn.enable_asyncio_serving(loop=loop)

        # ═══════════════════════════════════════════════════════════════
        # EAGER HANDSHAKE — pre-fetch the remote root object
        # ═══════════════════════════════════════════════════════════════
        # Without this, the first access to `conn.root` would trigger
        # `sync_request(HANDLE_GETROOT)`, which BLOCKS the event loop while
        # doing a blocking select()/recv(). After the sync_request guard
        # installed in Connection.sync_request, that first access would
        # also raise a RuntimeError because we are on the serving loop.
        #
        # We use the event-driven async_request + awaitable AsyncResult
        # path (which itself refuses to run without asyncio serving — see
        # rpyc/core/async_.py), honoring the NO-POLLING policy.
        # ═══════════════════════════════════════════════════════════════
        try:
            conn._remote_root = await conn.async_request(consts.HANDLE_GETROOT)
        except Exception:
            # Roll back on any handshake failure so we don't leak a
            # half-open connection.
            try:
                conn.disable_asyncio_serving()
            except Exception:
                pass
            try:
                conn._cleanup(_anyway=True)
            except Exception:
                pass
            raise

        return conn

    except Exception:
        # On ANY failure after enable_asyncio_serving(), the connection may have
        # an armed add_reader. Tear it down through the Connection so the reader
        # is unregistered (disable_asyncio_serving) BEFORE the fd is released —
        # otherwise the reader leaks and the freed fd can be recycled into a
        # 100%-CPU spin. Fall back to closing the raw socket only if no conn was
        # built yet. See a related internal incident analysis (not included here).
        if conn is not None:
            try:
                conn.disable_asyncio_serving()
            except Exception:
                pass
            try:
                conn._cleanup(_anyway=True)
            except Exception:
                pass
        try:
            sock.close()
        except Exception:
            pass
        raise
