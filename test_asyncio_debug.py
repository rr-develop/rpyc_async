"""
Debug AsyncioServer - check if accept works
"""
import asyncio
from rpyc.utils.async_server import AsyncioServer
import rpyc


class TestService(rpyc.Service):
    async def exposed_hello(self):
        print("[SERVER exposed_hello] Called!")
        return "Hello!"


async def test_server():
    print("[TEST] Creating server...")

    server = AsyncioServer(
        TestService,
        hostname='localhost',
        port=19998
    )

    print("[TEST] Starting server...")
    await server.start()
    print(f"[TEST] Server started on port {server.port}")

    # Keep server running
    print("[TEST] Server running, waiting 30 seconds...")
    await asyncio.sleep(30)

    print("[TEST] Closing server...")
    await server.close()
    print("[TEST] Done!")


if __name__ == '__main__':
    asyncio.run(test_server())
