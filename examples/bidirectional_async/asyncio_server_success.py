#!/usr/bin/env python3
"""
Comparison Example: AsyncioServer SUCCESS with Bidirectional Async

This example demonstrates the CORRECT way to implement bidirectional async.

✅ Uses AsyncioServer with persistent event loops
✅ Bidirectional async works perfectly
✅ No deadlocks or timeouts

Compare this with threaded_server_failure.py to see the difference.

Usage:
    # Run both server and client in same process
    python3 asyncio_server_success.py
"""
import asyncio
import rpyc
from rpyc.utils.async_server import AsyncioServer


# ═══════════════════════════════════════════════════════════════
# Server Service (identical to ThreadedServer example)
# ═══════════════════════════════════════════════════════════════

class ProcessService(rpyc.Service):
    """Server service that calls client callback."""

    async def exposed_process_with_callback(self, callback, value):
        """
        ✅ This WORKS with AsyncioServer!

        Flow:
            1. Server receives request in persistent event loop
            2. Server calls: await callback(value)
            3. Callback request sent to client
            4. Client processes in its persistent loop
            5. Reply received by server's persistent loop
            6. ✅ SUCCESS!
        """
        print(f"[SERVER] Received value: {value}")
        print(f"[SERVER] Calling client callback...")
        print(f"[SERVER] ✅ Persistent event loop can receive reply!")

        # ✅ This works perfectly with AsyncioServer
        result = await callback(value * 2)

        # ✅ Reaches here successfully!
        print(f"[SERVER] Callback returned: {result}")
        return f"Server processed: {result}"


# ═══════════════════════════════════════════════════════════════
# Client Service (identical)
# ═══════════════════════════════════════════════════════════════

class ClientService(rpyc.Service):
    """Client callback service."""

    async def exposed_callback(self, value):
        """Client async callback."""
        print(f"  [CLIENT CALLBACK] Received: {value}")
        await asyncio.sleep(0.05)
        result = value + 100
        print(f"  [CLIENT CALLBACK] Returning: {result}")
        return result


# ═══════════════════════════════════════════════════════════════
# Main Application
# ═══════════════════════════════════════════════════════════════

async def main():
    """Run server and client with bidirectional async."""
    print("=" * 70)
    print("✅ AsyncioServer - Bidirectional Async SUCCESS")
    print("=" * 70)

    # Start server
    print("\n[SETUP] Starting AsyncioServer...")
    server = AsyncioServer(
        ProcessService,
        hostname='localhost',
        port=18864
    )
    await server.start()
    print(f"[SETUP] ✅ Server started with persistent event loop")

    # Small delay
    await asyncio.sleep(0.2)

    # Connect client
    print(f"[SETUP] Connecting client...")
    conn = rpyc.connect("localhost", 18864)
    print(f"[SETUP] ✅ Client connected")

    try:
        # Enable asyncio serving (persistent loop)
        conn.enable_asyncio_serving()
        print(f"[SETUP] ✅ Client persistent event loop enabled\n")

        # Create client service
        client_service = ClientService()

        print("=" * 70)
        print("✅ BIDIRECTIONAL ASYNC CALL (WILL SUCCEED)")
        print("=" * 70)
        print("\nFlow:")
        print("  1. Client calls server.process_with_callback(callback, 5)")
        print("  2. Server receives in persistent loop")
        print("  3. Server calls await callback(10)")
        print("  4. Client callback processes in persistent loop")
        print("  5. Client returns result to server")
        print("  6. Server receives reply in persistent loop")
        print("  7. Server returns final result to client")
        print("  8. ✅ SUCCESS!\n")

        # ✅ This works perfectly!
        result = await conn.root.process_with_callback(
            client_service.exposed_callback,
            5
        )

        print("\n" + "=" * 70)
        print(f"✅ SUCCESS! Result: {result}")
        print("=" * 70)
        print("\nExplanation:")
        print("  ✅ AsyncioServer uses persistent event loop")
        print("  ✅ Server can receive callback replies")
        print("  ✅ Client has persistent loop via enable_asyncio_serving()")
        print("  ✅ Both sides process messages asynchronously")
        print("  ✅ No deadlocks or timeouts!")
        print("\nKey Difference from ThreadedServer:")
        print("  ❌ ThreadedServer: Temporary loop per request")
        print("  ✅ AsyncioServer: Persistent loop for all requests")
        print("\n" + "=" * 70)

    finally:
        conn.disable_asyncio_serving()
        conn.close()
        await server.close()
        print("\n[CLEANUP] ✅ Server and client shut down cleanly")


if __name__ == '__main__':
    asyncio.run(main())
