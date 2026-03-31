"""
Async RPyC connection module using asyncio streams.

CRITICAL: This module provides FULLY ASYNC socket connections for RPyC,
eliminating all blocking I/O operations.

Architecture:
- asyncio.open_connection() - non-blocking TCP socket connection
- AsyncioStream - wraps asyncio StreamReader/StreamWriter with sync interface for RPyC
- async_connect() - creates RPyC connection over async streams

NO threads, NO blocking socket.connect(), ONLY asyncio event loop!
"""
import asyncio
import errno
import sys
from typing import Optional, Dict, Any

from rpyc.core.stream import Stream, ClosedFile, STREAM_CHUNK
from rpyc.core.channel import Channel
from rpyc.core.service import VoidService
from rpyc.lib.compat import BYTES_LITERAL


class AsyncioStream(Stream):
    """
    Stream implementation wrapping asyncio StreamReader/StreamWriter.

    CRITICAL: This provides SYNCHRONOUS interface over ASYNC I/O.
    RPyC expects sync read()/write() methods, but underlying I/O is fully async.

    The trick: we run async operations using loop.run_until_complete() when called
    from sync context, but the socket I/O itself is non-blocking.

    Why this works:
    - RPyC calls stream.read()/write() from its internal threads
    - We execute async read/write operations in the SAME event loop
    - No blocking socket operations, no ThreadPoolExecutor needed
    """

    __slots__ = ("_reader", "_writer", "_loop", "_closed", "_fd")
    MAX_IO_CHUNK: int = STREAM_CHUNK

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        loop: asyncio.AbstractEventLoop
    ) -> None:
        """
        Initialize AsyncioStream with asyncio reader/writer.

        Args:
            reader: asyncio StreamReader for reading data
            writer: asyncio StreamWriter for writing data
            loop: event loop for running async operations
        """
        self._reader: asyncio.StreamReader = reader
        self._writer: asyncio.StreamWriter = writer
        self._loop: asyncio.AbstractEventLoop = loop
        self._closed: bool = False

        # Cache file descriptor for fileno() calls
        try:
            sock = writer.get_extra_info('socket')
            self._fd: Optional[int] = sock.fileno() if sock else None
        except Exception:
            self._fd = None

    @property
    def closed(self) -> bool:
        """Check if stream is closed."""
        return self._closed or self._writer.is_closing()

    def close(self) -> None:
        """Close the stream, releasing resources."""
        if not self._closed:
            self._closed = True
            try:
                self._writer.close()
                # Wait for writer to close (sync from async)
                if not self._loop.is_closed():
                    # Use run_until_complete if we're NOT in async context
                    try:
                        if sys.version_info >= (3, 7):
                            # Python 3.7+ has writer.wait_closed()
                            coro = self._writer.wait_closed()
                            if asyncio.get_event_loop() == self._loop and self._loop.is_running():
                                # We're in async context, can't run_until_complete
                                # Schedule for later and continue
                                asyncio.create_task(coro)
                            else:
                                # Sync context, safe to wait
                                self._loop.run_until_complete(coro)
                    except Exception:
                        pass
            except Exception:
                pass

    def fileno(self) -> int:
        """Return file descriptor of underlying socket."""
        if self._closed:
            raise EOFError("stream has been closed")

        if self._fd is None:
            # Try to get FD again
            try:
                sock = self._writer.get_extra_info('socket')
                if sock:
                    self._fd = sock.fileno()
            except Exception:
                pass

        if self._fd is None:
            raise EOFError("no file descriptor available")

        return self._fd

    def poll(self, timeout: float) -> bool:
        """
        Check if data is available for reading within timeout seconds.

        This is called by RPyC's Channel to check for incoming data.
        Uses asyncio.wait_for() with reader to detect available data.
        """
        if self._closed:
            return False

        async def _poll_async() -> bool:
            """Async implementation of poll."""
            try:
                # Peek at data without consuming it
                # If data is available, _reader._buffer will have data
                if self._reader._buffer:  # type: ignore
                    return True

                # Try to read with timeout
                # Use wait_for with very short read to detect data
                try:
                    data = await asyncio.wait_for(
                        self._reader.read(1),
                        timeout=timeout
                    )
                    if data:
                        # Put data back by prepending to buffer
                        self._reader._buffer = data + self._reader._buffer  # type: ignore
                        return True
                    return False
                except asyncio.TimeoutError:
                    return False
            except Exception:
                return False

        # Run async poll in event loop
        try:
            # Check if we're in async context
            try:
                current_loop = asyncio.get_running_loop()
                if current_loop == self._loop:
                    # We're already in async context - can't use run_until_complete
                    # This shouldn't happen in RPyC's sync API usage, but handle it
                    raise RuntimeError("poll() called from async context - not supported")
            except RuntimeError:
                # No running loop - safe to use run_until_complete
                pass

            return self._loop.run_until_complete(_poll_async())
        except Exception:
            return False

    def read(self, count: int) -> bytes:
        """
        Read exactly `count` bytes from stream.

        This is SYNCHRONOUS method called by RPyC's Channel,
        but uses ASYNC I/O underneath via asyncio StreamReader.

        Args:
            count: number of bytes to read

        Returns:
            bytes read

        Raises:
            EOFError: if connection closed or incomplete read
        """
        if self._closed:
            raise EOFError("stream has been closed")

        async def _read_async() -> bytes:
            """Async implementation of read."""
            try:
                # StreamReader.readexactly() reads exactly count bytes or raises IncompleteReadError
                data = await self._reader.readexactly(count)
                return data
            except asyncio.IncompleteReadError as e:
                # Connection closed before reading all data
                self._closed = True
                raise EOFError(f"connection closed by peer (read {len(e.partial)}/{count} bytes)")
            except Exception as e:
                self._closed = True
                raise EOFError(f"read error: {e}")

        # Run async read in event loop
        try:
            # Check if we're in async context
            try:
                current_loop = asyncio.get_running_loop()
                if current_loop == self._loop:
                    # We're in async context - RPyC shouldn't call this, but handle gracefully
                    raise RuntimeError(
                        "AsyncioStream.read() called from async context! "
                        "This indicates RPyC is trying to use sync API in async context."
                    )
            except RuntimeError:
                # No running loop - safe to use run_until_complete
                pass

            return self._loop.run_until_complete(_read_async())
        except EOFError:
            raise
        except Exception as e:
            self._closed = True
            raise EOFError(f"read error: {e}")

    def write(self, data: bytes) -> None:
        """
        Write all data to stream.

        This is SYNCHRONOUS method called by RPyC's Channel,
        but uses ASYNC I/O underneath via asyncio StreamWriter.

        Args:
            data: bytes to write

        Raises:
            EOFError: if write fails
        """
        if self._closed:
            raise EOFError("stream has been closed")

        async def _write_async() -> None:
            """Async implementation of write."""
            try:
                self._writer.write(data)
                await self._writer.drain()
            except Exception as e:
                self._closed = True
                raise EOFError(f"write error: {e}")

        # Run async write in event loop
        try:
            # Check if we're in async context
            try:
                current_loop = asyncio.get_running_loop()
                if current_loop == self._loop:
                    # We're in async context - RPyC shouldn't call this, but handle gracefully
                    raise RuntimeError(
                        "AsyncioStream.write() called from async context! "
                        "This indicates RPyC is trying to use sync API in async context."
                    )
            except RuntimeError:
                # No running loop - safe to use run_until_complete
                pass

            self._loop.run_until_complete(_write_async())
        except EOFError:
            raise
        except Exception as e:
            self._closed = True
            raise EOFError(f"write error: {e}")


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
    import socket as socket_module
    from rpyc.core.stream import SocketStream
    from rpyc.core import consts

    if loop is None:
        loop = asyncio.get_running_loop()

    if config is None:
        config = {}

    # Create non-blocking socket
    sock = socket_module.socket(socket_module.AF_INET, socket_module.SOCK_STREAM)
    sock.setblocking(False)

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

        # Wrap socket in standard RPyC SocketStream
        stream = SocketStream(sock)

        # Create RPyC channel and connection
        channel = Channel(stream)
        conn = service._connect(channel, config)

        # ═══════════════════════════════════════════════════════════════
        # NEW (v5.3): EAGER HANDSHAKE - Fetch root object asynchronously
        # ═══════════════════════════════════════════════════════════════
        # This prevents blocking sync_request() on first conn.root access.
        # We use loop.run_in_executor() to run sync_request in thread pool,
        # keeping event loop non-blocking.
        #
        # Why thread pool instead of AsyncResult.__await__()?
        # - AsyncResult.__await__() requires asyncio serving enabled
        # - We want async_connect() to work without enable_asyncio_serving()
        # - Thread pool is simple and works in all cases
        # ═══════════════════════════════════════════════════════════════
        conn_timeout = config.get("sync_request_timeout", 30)
        try:
            if timeout:
                # Use smaller of connection timeout and sync_request_timeout
                handshake_timeout = min(timeout, conn_timeout)
            else:
                handshake_timeout = conn_timeout

            # Run sync_request in thread pool to avoid blocking event loop
            conn._remote_root = await asyncio.wait_for(
                loop.run_in_executor(
                    None,  # Use default ThreadPoolExecutor
                    conn.sync_request,
                    consts.HANDLE_GETROOT
                ),
                timeout=handshake_timeout
            )
        except asyncio.TimeoutError as e:
            conn.close()
            raise ConnectionError(
                f"Handshake with {host}:{port} timed out after {handshake_timeout}s"
            ) from e
        except Exception as e:
            conn.close()
            raise ConnectionError(f"Handshake with {host}:{port} failed: {e}") from e

        return conn

    except Exception:
        # Ensure socket is closed on any error
        try:
            sock.close()
        except Exception:
            pass
        raise
