#!/usr/bin/env python3
"""
Comparison Example: ThreadedServer FAILURE with Bidirectional Async

This example demonstrates WHY ThreadedServer CANNOT support bidirectional async.

⚠️  WARNING: This example will HANG/TIMEOUT!

This is intentional to show the problem. Use Ctrl+C to stop.

Problem:
    - ThreadedServer creates temporary event loop (asyncio.run())
    - Temporary loop blocks until request completes
    - Server cannot receive client callback reply
    - DEADLOCK!

Solution:
    - Use AsyncioServer (see asyncio_server_success.py)
"""
import sys
import asyncio
import rpyc
from rpyc import ThreadedServer
import threading


# ═══════════════════════════════════════════════════════════════
# Server Service (same as AsyncioServer example)
# ═══════════════════════════════════════════════════════════════

class ProcessService(rpyc.Service):
    """Server service that tries to call client callback."""

    async def exposed_process_with_callback(self, callback, value):
        """
        ⚠️  This will DEADLOCK with ThreadedServer!

        Flow:
            1. Server receives request
            2. asyncio.run() creates temporary loop
            3. Server tries: await callback(value)
            4. ❌ DEADLOCK: Cannot receive reply!
        """
        print(f"[SERVER] Received value: {value}")
        print(f"[SERVER] Calling client callback...")
        print(f"[SERVER] ⚠️  This will HANG because ThreadedServer uses temporary loop!")

        # This await will HANG FOREVER
        result = await callback(value * 2)

        # Never reaches here with ThreadedServer
        print(f"[SERVER] Result: {result}")
        return result


# ═══════════════════════════════════════════════════════════════
# Client Service
# ═══════════════════════════════════════════════════════════════

class ClientService(rpyc.Service):
    """Client callback service."""

    async def exposed_callback(self, value):
        """Client async callback."""
        print(f"  [CLIENT CALLBACK] Received: {value}")
        await asyncio.sleep(0.05)
        return value + 100


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def run_server():
    """Run ThreadedServer (will deadlock on bidirectional async)."""
    print("=" * 70)
    print("⚠️  ThreadedServer - WILL DEADLOCK!")
    print("=" * 70)
    print("\nStarting ThreadedServer...")
    print("⚠️  This server CANNOT handle bidirectional async!")
    print("⚠️  It will HANG when client tries to call with callback.\n")

    server = ThreadedServer(
        ProcessService,
        hostname='localhost',
        port=18863,
        protocol_config={'allow_all_attrs': True}
    )

    server.start()


async def run_client():
    """Run client (will hang waiting for callback reply)."""
    print("=" * 70)
    print("Client: Connecting to ThreadedServer")
    print("=" * 70)

    # Wait for server to start
    await asyncio.sleep(1)

    conn = rpyc.connect("localhost", 18863)
    print("✅ Connected to server")

    try:
        # Enable asyncio serving
        conn.enable_asyncio_serving()
        print("✅ Asyncio serving enabled")

        # Create client service
        client_service = ClientService()

        print("\n" + "=" * 70)
        print("⚠️  ATTEMPTING BIDIRECTIONAL ASYNC (WILL HANG)")
        print("=" * 70)
        print("\nCalling server.process_with_callback(callback, 5)...")
        print("⚠️  This will HANG because ThreadedServer cannot handle it!")
        print("⚠️  Press Ctrl+C to stop.\n")

        # This will HANG FOREVER
        result = await asyncio.wait_for(
            conn.root.process_with_callback(
                client_service.exposed_callback,
                5
            ),
            timeout=10.0  # 10 second timeout
        )

        # Never reaches here
        print(f"Result: {result}")

    except asyncio.TimeoutError:
        print("\n" + "=" * 70)
        print("❌ TIMEOUT! Bidirectional async FAILED!")
        print("=" * 70)
        print("\n⚠️  As expected, ThreadedServer CANNOT handle bidirectional async.")
        print("⚠️  The server is stuck in asyncio.run() waiting for callback reply,")
        print("⚠️  but it CANNOT receive the reply because the loop is temporary!\n")
        print("✅ Solution: Use AsyncioServer instead!")
        print("   See: asyncio_server_success.py\n")

    finally:
        conn.close()


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("⚠️  WARNING: This example demonstrates ThreadedServer FAILURE!")
        print("⚠️  It will HANG to show the problem. Use Ctrl+C to stop.\n")
        print("Usage:")
        print("  python3 threaded_server_failure.py server  # Terminal 1")
        print("  python3 threaded_server_failure.py client  # Terminal 2")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "server":
        run_server()
    elif mode == "client":
        asyncio.run(run_client())
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)


if __name__ == '__main__':
    main()
