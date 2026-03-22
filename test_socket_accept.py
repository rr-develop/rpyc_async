"""Test basic socket accept with asyncio"""
import asyncio
import socket
import threading
import time


async def accept_one():
    # Create listening socket
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(('127.0.0.1', 29999))
    listener.listen(5)
    listener.setblocking(False)

    print(f"Listening on port 29999")

    loop = asyncio.get_running_loop()

    print("Waiting for connection...")
    sock, addr = await loop.sock_accept(listener)
    print(f"Accepted connection from {addr}!")

    sock.close()
    listener.close()
    return True


def connect_client():
    time.sleep(1)  # Give server time to start
    print("[CLIENT] Connecting...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 29999))
    print("[CLIENT] Connected!")
    time.sleep(0.5)
    sock.close()
    print("[CLIENT] Closed")


async def main():
    # Start client thread
    thread = threading.Thread(target=connect_client)
    thread.start()

    # Accept connection
    result = await accept_one()
    print(f"Result: {result}")

    thread.join()
    print("Done!")


if __name__ == '__main__':
    asyncio.run(main())
