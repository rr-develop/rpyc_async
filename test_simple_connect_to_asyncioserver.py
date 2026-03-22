"""Test simple socket connect to AsyncioServer"""
import asyncio
import socket
import threading
import time
import rpyc
from rpyc.utils.async_server import AsyncioServer
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')


class TestService(rpyc.Service):
    def exposed_hello(self):
        return "Hello!"


async def test():
    server = AsyncioServer(TestService, hostname='127.0.0.1', port=19991)
    await server.start()
    print("Server started")

    await asyncio.sleep(1)

    def simple_connect():
        print("[THREAD] Raw socket connecting...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', 19991))
        print("[THREAD] Raw socket connected!")
        time.sleep(2)
        sock.close()
        print("[THREAD] Raw socket closed")

    thread = threading.Thread(target=simple_connect)
    thread.start()

    await asyncio.sleep(5)

    await server.close()
    thread.join()
    print("Done!")


if __name__ == '__main__':
    asyncio.run(test())
