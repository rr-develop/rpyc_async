"""Check if port is actually listening"""
import asyncio
from rpyc.utils.async_server import AsyncioServer
import rpyc
import socket


class TestService(rpyc.Service):
    def exposed_hello(self):
        return "Hello!"


async def main():
    server = AsyncioServer(TestService, hostname='127.0.0.1', port=19991)
    await server.start()

    print("Server started")

    # Check if port is listening
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        result = sock.connect_ex(('127.0.0.1', 19991))
        if result == 0:
            print("Port 19991 is open and listening!")
        else:
            print(f"Port 19991 is not listening (error code: {result})")
        sock.close()
    except Exception as e:
        print(f"Error checking port: {e}")

    # Wait a bit
    await asyncio.sleep(2)

    await server.close()
    print("Server closed")


if __name__ == '__main__':
    asyncio.run(main())
