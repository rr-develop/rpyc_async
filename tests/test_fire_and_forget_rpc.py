"""
Integration tests for fire_and_forget() with cross-process async RPC calls.

These tests use real multiprocessing with AsyncioServer to test:
- Cross-process async RPC calls
- Hung/dead connection handling
- Killed process handling
- Bidirectional async calls

CRITICAL: All tests use AsyncioServer exclusively. ThreadedServer is not supported.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import signal
import sys
import time
from typing import Any

import pytest
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer


# ============================================================================
# Test Service Implementations
# ============================================================================

class TestService(rpyc.Service):
    """Test service with various async methods for testing."""

    async def exposed_fast(self, value: int) -> int:
        """Returns immediately."""
        return value * 2

    async def exposed_slow(self, value: int, delay: float) -> int:
        """Returns after delay."""
        await asyncio.sleep(delay)
        return value * 2

    async def exposed_hang_forever(self) -> int:
        """Never returns (for timeout tests)."""
        await asyncio.sleep(999999)
        return 42

    async def exposed_error(self) -> None:
        """Raises an exception."""
        raise ValueError("Intentional RPC error")

    async def exposed_with_callback(self, callback, value: int) -> int:
        """Calls client's async callback (bidirectional async test)."""
        result = await callback(value)
        return result * 2


# ============================================================================
# Server Process Functions
# ============================================================================

def run_async_server(port: int, ready_event: multiprocessing.Event):
    """Run AsyncioServer in a separate process."""

    async def server_main():
        server = AsyncioServer(
            TestService,
            port=port,
            protocol_config={"allow_all_attrs": True, "allow_public_attrs": True},
        )

        # Signal that server is ready
        ready_event.set()

        # Run server
        await server.serve_forever()

    try:
        asyncio.run(server_main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)


def run_hanging_server(port: int, ready_event: multiprocessing.Event, hang_event: multiprocessing.Event):
    """Run AsyncioServer that can be made to hang."""

    class HangingService(rpyc.Service):
        """Service that can hang on command."""

        async def exposed_normal_call(self, value: int) -> int:
            """Normal call."""
            return value * 2

        async def exposed_hang_on_signal(self) -> int:
            """Hangs when hang_event is set."""
            # Wait for hang signal
            hang_event.set()
            # Now hang forever
            await asyncio.sleep(999999)
            return 42

    async def server_main():
        server = AsyncioServer(
            HangingService,
            port=port,
            protocol_config={"allow_all_attrs": True, "allow_public_attrs": True},
        )

        ready_event.set()
        await server.serve_forever()

    try:
        asyncio.run(server_main())
    except KeyboardInterrupt:
        pass


# ============================================================================
# Helper Functions
# ============================================================================

class CallbackTracker:
    """Track callback invocations across processes."""

    def __init__(self):
        self.success_results: list[Any] = []
        self.error_results: list[BaseException] = []
        self.success_event = asyncio.Event()
        self.error_event = asyncio.Event()

    def on_success(self, result: Any) -> None:
        self.success_results.append(result)
        self.success_event.set()

    def on_error(self, exc: BaseException) -> None:
        self.error_results.append(exc)
        self.error_event.set()

    async def on_success_async(self, result: Any) -> None:
        await asyncio.sleep(0)
        self.success_results.append(result)
        self.success_event.set()

    async def on_error_async(self, exc: BaseException) -> None:
        await asyncio.sleep(0)
        self.error_results.append(exc)
        self.error_event.set()

    async def wait_success(self, timeout: float = 5.0) -> None:
        await asyncio.wait_for(self.success_event.wait(), timeout=timeout)

    async def wait_error(self, timeout: float = 5.0) -> None:
        await asyncio.wait_for(self.error_event.wait(), timeout=timeout)


async def start_server(port: int) -> multiprocessing.Process:
    """Start AsyncioServer in background process."""
    ready_event = multiprocessing.Event()
    server_proc = multiprocessing.Process(
        target=run_async_server,
        args=(port, ready_event),
    )
    server_proc.start()

    # Wait for server to be ready
    if not ready_event.wait(timeout=5.0):
        server_proc.kill()
        server_proc.join()
        raise RuntimeError("Server failed to start")

    # Give server a moment to fully initialize
    await asyncio.sleep(0.2)

    return server_proc


# ============================================================================
# Tests
# ============================================================================

class TestBasicRPC:
    """Tests for basic cross-process RPC with fire_and_forget."""

    @pytest.mark.asyncio
    async def test_simple_rpc_call(self):
        """Test fire_and_forget with simple cross-process RPC call."""
        from rpyc_async.core.async_connect import async_connect
        from rpyc_async.utils.helpers import fire_and_forget

        port = 18861
        server_proc = await start_server(port)

        try:
            # Connect to server
            conn = await async_connect("localhost", port, timeout=5.0)

            tracker = CallbackTracker()

            # Fire and forget RPC call
            task = fire_and_forget(
                conn.root.fast(10),
                success_callback=tracker.on_success,
                error_callback=tracker.on_error,
            )

            # Wait for completion
            await tracker.wait_success(timeout=5.0)

            # Verify
            assert len(tracker.success_results) == 1
            assert tracker.success_results[0] == 20
            assert len(tracker.error_results) == 0
            assert task.done()

            conn.close()

        finally:
            server_proc.terminate()
            server_proc.join(timeout=2.0)
            if server_proc.is_alive():
                server_proc.kill()
                server_proc.join()

    @pytest.mark.asyncio
    async def test_rpc_with_timeout_success(self):
        """Test fire_and_forget RPC with timeout that doesn't expire."""
        from rpyc_async.core.async_connect import async_connect
        from rpyc_async.utils.helpers import fire_and_forget

        port = 18862
        server_proc = await start_server(port)

        try:
            conn = await async_connect("localhost", port, timeout=5.0)
            tracker = CallbackTracker()

            task = fire_and_forget(
                conn.root.slow(10, 0.1),
                timeout=2.0,  # Longer than delay
                success_callback=tracker.on_success,
                error_callback=tracker.on_error,
            )

            await tracker.wait_success(timeout=5.0)

            assert len(tracker.success_results) == 1
            assert tracker.success_results[0] == 20
            assert len(tracker.error_results) == 0

            conn.close()

        finally:
            server_proc.terminate()
            server_proc.join(timeout=2.0)
            if server_proc.is_alive():
                server_proc.kill()
                server_proc.join()

    @pytest.mark.asyncio
    async def test_rpc_with_exception(self):
        """Test fire_and_forget RPC when remote raises exception."""
        from rpyc_async.core.async_connect import async_connect
        from rpyc_async.utils.helpers import fire_and_forget

        port = 18863
        server_proc = await start_server(port)

        try:
            conn = await async_connect("localhost", port, timeout=5.0)
            tracker = CallbackTracker()

            task = fire_and_forget(
                conn.root.error(),
                success_callback=tracker.on_success,
                error_callback=tracker.on_error,
            )

            await tracker.wait_error(timeout=5.0)

            assert len(tracker.error_results) == 1
            assert "Intentional RPC error" in str(tracker.error_results[0])
            assert len(tracker.success_results) == 0

            conn.close()

        finally:
            server_proc.terminate()
            server_proc.join(timeout=2.0)
            if server_proc.is_alive():
                server_proc.kill()
                server_proc.join()

    @pytest.mark.asyncio
    async def test_async_callbacks(self):
        """Test fire_and_forget_async with cross-process RPC."""
        from rpyc_async.core.async_connect import async_connect
        from rpyc_async.utils.helpers import fire_and_forget_async

        port = 18864
        server_proc = await start_server(port)

        try:
            conn = await async_connect("localhost", port, timeout=5.0)
            tracker = CallbackTracker()

            task = fire_and_forget_async(
                conn.root.fast(10),
                success_callback=tracker.on_success_async,
                error_callback=tracker.on_error_async,
            )

            await tracker.wait_success(timeout=5.0)

            assert len(tracker.success_results) == 1
            assert tracker.success_results[0] == 20

            conn.close()

        finally:
            server_proc.terminate()
            server_proc.join(timeout=2.0)
            if server_proc.is_alive():
                server_proc.kill()
                server_proc.join()


class TestHungConnection:
    """CRITICAL: Tests for hung/dead connection handling."""

    @pytest.mark.asyncio
    async def test_timeout_with_hung_call(self):
        """
        CRITICAL TEST: Verify timeout fires instantly even when remote hangs.

        This is the key requirement - fire_and_forget must complete quickly
        via local timeout, not wait for hung remote process.
        """
        from rpyc_async.core.async_connect import async_connect
        from rpyc_async.utils.helpers import fire_and_forget

        port = 18865
        server_proc = await start_server(port)

        try:
            conn = await async_connect("localhost", port, timeout=5.0)
            tracker = CallbackTracker()

            start_time = time.time()

            # Call that hangs forever on server side
            task = fire_and_forget(
                conn.root.hang_forever(),
                timeout=0.5,  # Should fire after 0.5s
                success_callback=tracker.on_success,
                error_callback=tracker.on_error,
            )

            # Wait for error callback
            await tracker.wait_error(timeout=2.0)

            elapsed = time.time() - start_time

            # Verify timeout fired locally
            assert len(tracker.error_results) == 1
            assert isinstance(tracker.error_results[0], asyncio.TimeoutError)
            assert len(tracker.success_results) == 0

            # CRITICAL: Timeout should fire in ~0.5s, not hang forever
            assert elapsed < 1.5, f"Timeout should fire quickly, took {elapsed}s"
            assert elapsed >= 0.4, f"Timeout fired too early: {elapsed}s"

            conn.close()

        finally:
            server_proc.terminate()
            server_proc.join(timeout=2.0)
            if server_proc.is_alive():
                server_proc.kill()
                server_proc.join()

    @pytest.mark.asyncio
    async def test_killed_process_during_call(self):
        """
        Test fire_and_forget when server process is killed during call.

        With a short timeout, should get TimeoutError (not hang forever).
        The key requirement is that timeout fires locally, regardless of remote state.
        """
        from rpyc_async.core.async_connect import async_connect
        from rpyc_async.utils.helpers import fire_and_forget

        port = 18866
        server_proc = await start_server(port)

        try:
            conn = await async_connect("localhost", port, timeout=5.0)
            tracker = CallbackTracker()

            # Start slow call with short timeout
            task = fire_and_forget(
                conn.root.slow(10, 10.0),  # 10 second delay
                timeout=1.0,  # Short timeout - will fire before we kill process
                success_callback=tracker.on_success,
                error_callback=tracker.on_error,
            )

            # Let call start
            await asyncio.sleep(0.2)

            # Kill server
            server_proc.kill()
            server_proc.join()

            # Wait for error callback (should fire due to timeout)
            start_time = time.time()
            await tracker.wait_error(timeout=3.0)
            elapsed = time.time() - start_time

            # Verify timeout error callback fired
            assert len(tracker.error_results) == 1
            assert isinstance(tracker.error_results[0], asyncio.TimeoutError)
            assert len(tracker.success_results) == 0

            # Should fire around 1 second (the timeout), not hang
            assert elapsed < 2.5, f"Timeout should fire quickly, took {elapsed}s"

        finally:
            if server_proc.is_alive():
                server_proc.kill()
                server_proc.join()

    @pytest.mark.asyncio
    async def test_multiple_concurrent_with_mixed_timeouts(self):
        """
        Test many concurrent fire_and_forget calls, some timing out.

        Verifies that timeouts don't interfere with each other.
        """
        from rpyc_async.core.async_connect import async_connect
        from rpyc_async.utils.helpers import fire_and_forget

        port = 18867
        server_proc = await start_server(port)

        try:
            conn = await async_connect("localhost", port, timeout=5.0)

            success_count = 0
            timeout_count = 0

            def on_success(result):
                nonlocal success_count
                success_count += 1

            def on_timeout(exc):
                nonlocal timeout_count
                if isinstance(exc, asyncio.TimeoutError):
                    timeout_count += 1

            tasks = []

            # Mix of fast calls (will succeed) and hanging calls (will timeout)
            for i in range(10):
                if i % 2 == 0:
                    # Fast call - should succeed
                    task = fire_and_forget(
                        conn.root.fast(i),
                        timeout=2.0,
                        success_callback=on_success,
                        error_callback=on_timeout,
                    )
                else:
                    # Hanging call - should timeout
                    task = fire_and_forget(
                        conn.root.hang_forever(),
                        timeout=0.3,
                        success_callback=on_success,
                        error_callback=on_timeout,
                    )
                tasks.append(task)

            # Wait for all tasks
            await asyncio.gather(*tasks, return_exceptions=True)

            # Verify correct counts
            assert success_count == 5, f"Expected 5 successes, got {success_count}"
            assert timeout_count == 5, f"Expected 5 timeouts, got {timeout_count}"

            conn.close()

        finally:
            server_proc.terminate()
            server_proc.join(timeout=2.0)
            if server_proc.is_alive():
                server_proc.kill()
                server_proc.join()


class TestBidirectionalAsync:
    """Tests for bidirectional async calls (server calls client)."""

    @pytest.mark.asyncio
    async def test_bidirectional_with_fire_and_forget(self):
        """
        Test fire_and_forget when server calls back to client.

        This requires AsyncioServer with proper event loop setup.
        """
        from rpyc_async.core.async_connect import async_connect
        from rpyc_async.utils.helpers import fire_and_forget

        port = 18868
        server_proc = await start_server(port)

        try:
            conn = await async_connect("localhost", port, timeout=5.0)

            # Define async callback on client side
            async def client_callback(value: int) -> int:
                await asyncio.sleep(0.1)
                return value + 10

            tracker = CallbackTracker()

            # Server will call our callback
            task = fire_and_forget(
                conn.root.with_callback(client_callback, 5),
                timeout=5.0,
                success_callback=tracker.on_success,
                error_callback=tracker.on_error,
            )

            await tracker.wait_success(timeout=10.0)

            # Server should have called callback(5) -> 15, then returned 15 * 2 = 30
            assert len(tracker.success_results) == 1
            assert tracker.success_results[0] == 30

            conn.close()

        finally:
            server_proc.terminate()
            server_proc.join(timeout=2.0)
            if server_proc.is_alive():
                server_proc.kill()
                server_proc.join()


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
