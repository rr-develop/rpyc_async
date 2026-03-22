"""Start AsyncioServer and wait"""
import asyncio
from rpyc.utils.async_server import AsyncioServer
import rpyc


class TestService(rpyc.Service):
    def exposed_sync_hello(self):
        print("[SERVER] sync_hello called")
        return "Sync Hello!"

    async def exposed_async_hello(self):
        print("[SERVER] async_hello called")
        await asyncio.sleep(0.01)
        return "Async Hello!"


async def main():
    server = AsyncioServer(
        TestService,
        hostname='localhost',
        port=19997,
        protocol_config={'allow_all_attrs': True}
    )

    print("Starting server on port 19997...")
    await server.start()
    print("Server started! Listening...")

    # Run for 30 seconds
    try:
        await asyncio.sleep(30)
    finally:
        await server.close()


if __name__ == '__main__':
    asyncio.run(main())
