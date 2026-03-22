"""
Simple test to debug AsyncioServer
"""
import asyncio
import rpyc
from rpyc.utils.async_server import AsyncioServer


class TestService(rpyc.Service):
    async def exposed_hello(self):
        print("[SERVER] hello() called")
        await asyncio.sleep(0.01)
        return "Hello from async server!"


async def main():
    print("Starting AsyncioServer...")

    server = AsyncioServer(
        TestService,
        hostname='localhost',
        port=19999,
        protocol_config={'allow_all_attrs': True}
    )

    await server.start()
    print(f"Server started on port {server.port}")

    # Give server time to start
    await asyncio.sleep(1)

    print("Connecting client...")
    # This is the problem - rpyc.connect() is synchronous
    # and creates a blocking connection
    conn = rpyc.connect("localhost", 19999)
    print("Client connected!")

    print("Calling async method...")
    result = await conn.root.hello()
    print(f"Result: {result}")

    conn.close()
    await server.close()
    print("Done!")


if __name__ == '__main__':
    asyncio.run(main())
