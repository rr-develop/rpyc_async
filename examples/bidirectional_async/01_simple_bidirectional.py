#!/usr/bin/env python3
"""
Example 1: Simple Bidirectional Async

Demonstrates basic bidirectional async communication:
- Server calls client async callback
- Client processes and returns result
- Uses AsyncioServer (required for bidirectional async)

Usage:
    # Terminal 1
    python3 01_simple_bidirectional.py server

    # Terminal 2
    python3 01_simple_bidirectional.py client
"""
import sys
import asyncio
import rpyc_async as rpyc
from rpyc_async.utils.async_server import AsyncioServer


# ═══════════════════════════════════════════════════════════════
# Server Service
# ═══════════════════════════════════════════════════════════════

class DataProcessingService(rpyc.Service):
    """Server service that processes data using client callbacks."""

    async def exposed_process_with_callback(self, callback, value):
        """
        Server method that calls client callback.

        Args:
            callback: Client async callback function
            value: Value to process

        Returns:
            Processed result from client
        """
        print(f"[SERVER] Received value: {value}")
        print(f"[SERVER] Calling client callback...")

        # Simulate some async work
        await asyncio.sleep(0.1)

        # ✅ Call client async callback - WORKS with AsyncioServer!
        result = await callback(value * 2)

        print(f"[SERVER] Client callback returned: {result}")

        return f"Server processed: {result}"

    async def exposed_ping(self):
        """Simple ping method for testing."""
        return "pong"


# ═══════════════════════════════════════════════════════════════
# Client Service
# ═══════════════════════════════════════════════════════════════

class ClientCallbackService(rpyc.Service):
    """Client service that provides callbacks to server."""

    async def exposed_process_data(self, value):
        """
        Client async callback called by server.

        Args:
            value: Value received from server

        Returns:
            Processed value
        """
        print(f"  [CLIENT CALLBACK] Received value: {value}")

        # Simulate async processing
        await asyncio.sleep(0.05)

        result = value + 100
        print(f"  [CLIENT CALLBACK] Returning: {result}")

        return result


# ═══════════════════════════════════════════════════════════════
# Server Application
# ═══════════════════════════════════════════════════════════════

async def run_server():
    """Run AsyncioServer with bidirectional async support."""
    print("=" * 60)
    print("Starting AsyncioServer (Bidirectional Async Support)")
    print("=" * 60)

    server = AsyncioServer(
        DataProcessingService,
        hostname='localhost',
        port=18861,
        protocol_config={
            'allow_all_attrs': True,
        }
    )

    print(f"✅ Server listening on localhost:18861")
    print(f"✅ Persistent event loop ready for bidirectional async")
    print(f"\nWaiting for client connections...\n")

    await server.serve_forever()


# ═══════════════════════════════════════════════════════════════
# Client Application
# ═══════════════════════════════════════════════════════════════

async def run_client():
    """Run client with bidirectional async support."""
    print("=" * 60)
    print("Client: Connecting to AsyncioServer")
    print("=" * 60)

    # Connect to server
    conn = rpyc.connect("localhost", 18861)
    print("✅ Connected to server")

    try:
        # ✅ CRITICAL: Enable asyncio serving for persistent event loop
        conn.enable_asyncio_serving()
        print("✅ Asyncio serving enabled (persistent event loop)")

        # Test simple ping
        pong = await conn.root.ping()
        print(f"✅ Ping test: {pong}\n")

        # Create client service with callback
        client_service = ClientCallbackService()

        print("=" * 60)
        print("Starting Bidirectional Async Call")
        print("=" * 60)

        # Call server method with client callback
        print("\n[CLIENT] Calling server.process_with_callback(callback, 5)")
        result = await conn.root.process_with_callback(
            client_service.exposed_process_data,
            5
        )

        print(f"\n[CLIENT] Final result: {result}")
        print("\n" + "=" * 60)
        print("✅ Bidirectional async call completed successfully!")
        print("=" * 60)

    finally:
        conn.disable_asyncio_serving()
        conn.close()
        print("\n✅ Client disconnected")


# ═══════════════════════════════════════════════════════════════
# Main Entry Point
# ═══════════════════════════════════════════════════════════════

def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 01_simple_bidirectional.py server")
        print("  python3 01_simple_bidirectional.py client")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "server":
        asyncio.run(run_server())
    elif mode == "client":
        asyncio.run(run_client())
    else:
        print(f"Unknown mode: {mode}")
        print("Use 'server' or 'client'")
        sys.exit(1)


if __name__ == '__main__':
    main()
