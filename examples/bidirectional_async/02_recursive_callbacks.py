#!/usr/bin/env python3
"""
Example 2: Recursive Bidirectional Async Callbacks

Demonstrates recursive async communication between server and client:
- Server calls client callback
- Client callback calls server method
- Server calls client callback again
- ... continues recursively until depth=0

This example runs both server and client in the same process for simplicity.

Usage:
    python3 02_recursive_callbacks.py
"""
import asyncio
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer


# ═══════════════════════════════════════════════════════════════
# Server Service
# ═══════════════════════════════════════════════════════════════

class RecursiveService(rpyc.Service):
    """Server service with recursive async methods."""

    async def exposed_countdown(self, callback, n):
        """
        Recursive countdown that calls client callback.

        Flow:
            Server(n) → Client(n-1) → Server(n-2) → ... → Base case

        Args:
            callback: Client async callback
            n: Current countdown value

        Returns:
            Result chain showing recursion path
        """
        print(f"[SERVER] countdown({n})")

        # Base case
        if n <= 0:
            print(f"[SERVER] Base case reached!")
            return "Server: Done!"

        # Simulate async work
        await asyncio.sleep(0.05)

        # ✅ Recursive call to client callback
        print(f"[SERVER] Calling client callback with n={n-1}")
        result = await callback(n - 1)

        final_result = f"Server({n}) → {result}"
        print(f"[SERVER] Returning: {final_result}")

        return final_result


# ═══════════════════════════════════════════════════════════════
# Client Service
# ═══════════════════════════════════════════════════════════════

class ClientCallbackService(rpyc.Service):
    """Client service that recursively calls server."""

    def __init__(self, server_conn):
        super().__init__()
        self.server_conn = server_conn

    async def exposed_client_callback(self, n):
        """
        Client callback that recursively calls server.

        Flow:
            Client(n) → Server(n-1) → Client(n-2) → ... → Base case

        Args:
            n: Current countdown value

        Returns:
            Result chain showing recursion path
        """
        print(f"  [CLIENT] client_callback({n})")

        # Base case
        if n <= 0:
            print(f"  [CLIENT] Base case reached!")
            return "Client: Done!"

        # Simulate async work
        await asyncio.sleep(0.05)

        # ✅ Recursive call back to server
        print(f"  [CLIENT] Calling server.countdown with n={n-1}")
        result = await self.server_conn.root.countdown(
            self.exposed_client_callback,
            n - 1
        )

        final_result = f"Client({n}) → {result}"
        print(f"  [CLIENT] Returning: {final_result}")

        return final_result


# ═══════════════════════════════════════════════════════════════
# Main Application
# ═══════════════════════════════════════════════════════════════

async def main():
    """Run server and client in same process."""
    print("=" * 70)
    print("Recursive Bidirectional Async Example")
    print("=" * 70)

    # Start server
    print("\n[SETUP] Starting AsyncioServer...")
    server = AsyncioServer(
        RecursiveService,
        hostname='localhost',
        port=18862
    )
    await server.start()
    print(f"[SETUP] ✅ Server started on port {server.port}")

    # Small delay to ensure server is ready
    await asyncio.sleep(0.2)

    # Connect client
    print(f"[SETUP] Connecting client...")
    conn = rpyc.connect("localhost", 18862)
    print(f"[SETUP] ✅ Client connected")

    try:
        # Enable asyncio serving
        conn.enable_asyncio_serving()
        print(f"[SETUP] ✅ Asyncio serving enabled\n")

        # Create client service
        client_service = ClientCallbackService(conn)

        # Test different recursion depths
        for depth in [3, 5]:
            print("\n" + "=" * 70)
            print(f"Starting Recursive Call Chain (depth={depth})")
            print("=" * 70)
            print(f"\nFlow: Server({depth}) → Client({depth-1}) → Server({depth-2}) → ...\n")

            # Start recursive chain
            result = await conn.root.countdown(
                client_service.exposed_client_callback,
                depth
            )

            print("\n" + "-" * 70)
            print(f"Final Result (depth={depth}):")
            print(f"  {result}")
            print("-" * 70)

            # Small delay between tests
            await asyncio.sleep(0.5)

        print("\n" + "=" * 70)
        print("✅ All recursive tests completed successfully!")
        print("=" * 70)

    finally:
        # Cleanup
        conn.disable_asyncio_serving()
        conn.close()
        await server.close()
        print("\n[CLEANUP] ✅ Server and client shut down")


if __name__ == '__main__':
    asyncio.run(main())
