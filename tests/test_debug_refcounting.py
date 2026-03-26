"""
Test debug_refcounting mode for tracking object lifecycle.
"""
import pytest
import rpyc
import logging
import asyncio
from io import StringIO


def test_debug_refcounting_logs_object_repr():
    """Test that debug_refcounting mode logs readable object representations."""

    # Setup logger to capture debug output
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.DEBUG)
    logger = logging.getLogger("rpyc.test")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    # Create service with debug_refcounting enabled
    class DebugService(rpyc.Service):
        def exposed_get_list(self):
            """Return a list object."""
            return [1, 2, 3, "hello"]

        def exposed_get_dict(self):
            """Return a dict object."""
            return {"key": "value", "number": 42}

    # Create server with debug config
    from rpyc.utils.server import ThreadedServer
    config = {
        "debug_refcounting": True,
        "logger": logger,
        "allow_all_attrs": True,
    }

    server = ThreadedServer(
        DebugService,
        port=0,  # random port
        protocol_config=config
    )

    # Start server in background
    import threading
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()

    # Wait for server to start
    import time
    time.sleep(0.5)

    try:
        # Connect client
        conn = rpyc.connect("localhost", server.port, config=config)

        # Call exposed methods to create objects
        result_list = conn.root.get_list()
        result_dict = conn.root.get_dict()

        # Access elements to ensure objects are used
        _ = result_list[0]
        _ = result_dict["key"]

        # Close connection (should trigger decref)
        conn.close()

        # Get log output
        log_output = log_stream.getvalue()

        # Verify debug logs contain readable object representations
        assert "[REFCOUNT] ADD" in log_output, "Should log ADD operations"
        assert "[1, 2, 3, 'hello']" in log_output, "Should log list repr"
        assert "key" in log_output or "value" in log_output, "Should log dict content"

        # Verify DECREF/DELETE operations are logged
        assert "[REFCOUNT] DECREF" in log_output or "[REFCOUNT] DELETE" in log_output, \
            "Should log DECREF/DELETE operations"

        print("\n=== Debug Refcounting Log Output ===")
        print(log_output)
        print("=" * 40)

    finally:
        server.close()
        logger.removeHandler(handler)


@pytest.mark.asyncio
async def test_debug_refcounting_async():
    """Test debug_refcounting with async server."""

    # Setup logger
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.DEBUG)
    logger = logging.getLogger("rpyc.test_async")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    class AsyncDebugService(rpyc.Service):
        async def exposed_async_get_data(self):
            """Return async data."""
            await asyncio.sleep(0.01)
            return {"async": True, "data": [1, 2, 3]}

    from rpyc.utils.server import AsyncioServer
    config = {
        "debug_refcounting": True,
        "logger": logger,
        "allow_all_attrs": True,
    }

    server = AsyncioServer(
        AsyncDebugService,
        port=0,
        protocol_config=config
    )

    # Start server
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.2)

    try:
        # Connect and test
        conn = await rpyc.aio_connect("localhost", server.port, config=config)

        result = await conn.root.async_get_data()
        assert result["async"] is True

        conn.close()
        await asyncio.sleep(0.1)

        # Verify logs
        log_output = log_stream.getvalue()
        assert "[REFCOUNT]" in log_output, "Should have refcount debug logs"

        print("\n=== Async Debug Refcounting Log ===")
        print(log_output)
        print("=" * 40)

    finally:
        server.close()
        await server_task
        logger.removeHandler(handler)


if __name__ == "__main__":
    # Run basic test
    test_debug_refcounting_logs_object_repr()
    print("\nBasic test passed!")

    # Run async test
    asyncio.run(test_debug_refcounting_async())
    print("\nAsync test passed!")
