"""Simple AsyncioServer test with extended wait time"""
import asyncio
import rpyc
from rpyc.utils.async_server import AsyncioServer
import logging
import threading
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')


class TestService(rpyc.Service):
    def exposed_sync_hello(self):
        print("[SERVER] sync_hello called!")
        return "Sync Hello!"


async def test():
    print("Starting AsyncioServer...")
    server = AsyncioServer(
        TestService,
        hostname='127.0.0.1',
        port=19990,
        protocol_config={'allow_all_attrs': True}
    )

    await server.start()
    print(f"Server started on port {server.port}")

    # Wait LONGER to give accept loop time
    print("Waiting 2 seconds before connecting client...")
    await asyncio.sleep(2)

    result = []
    error = []

    def connect_thread():
        try:
            print("[THREAD] Connecting...")
            conn = rpyc.connect('127.0.0.1', 19990)
            print("[THREAD] Connected!")

            print("[THREAD] Calling sync_hello...")
            res = conn.root.sync_hello()
            print(f"[THREAD] Got: {res}")
            result.append(res)

            conn.close()
            print("[THREAD] Done!")
        except Exception as e:
            print(f"[THREAD] Error: {e}")
            import traceback
            traceback.print_exc()
            error.append(e)

    thread = threading.Thread(target=connect_thread)
    thread.start()

    # Wait for thread with LONGER timeout
    print("Waiting for client thread (15 second timeout)...")
    thread.join(timeout=15)

    if thread.is_alive():
        print("\nERROR: Thread timed out!")
    elif error:
        print(f"\nERROR: {error[0]}")
    elif result:
        print(f"\nSUCCESS! Got result: {result[0]}")
    else:
        print("\nERROR: No result")

    print("\nClosing server...")
    await server.close()
    print("Server closed")


if __name__ == '__main__':
    asyncio.run(test())
