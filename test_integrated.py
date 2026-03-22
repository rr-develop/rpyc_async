"""
Integrated test - server and client in same event loop
"""
import asyncio
import rpyc
from rpyc.utils.async_server import AsyncioServer


class TestService(rpyc.Service):
    def exposed_sync_hello(self):
        print("[SERVER] sync_hello called!")
        return "Sync Hello!"

    async def exposed_async_hello(self):
        print("[SERVER] async_hello called!")
        await asyncio.sleep(0.01)
        return "Async Hello!"


async def test():
    print("Starting AsyncioServer...")
    server = AsyncioServer(
        TestService,
        hostname='127.0.0.1',  # Use IPv4 explicitly
        port=19990,
        protocol_config={'allow_all_attrs': True}
    )

    await server.start()
    print(f"Server started on port {server.port}")

    # Small delay
    await asyncio.sleep(1)

    print("\nConnecting sync client in thread...")
    # We need to connect from separate thread since rpyc.connect() is blocking
    import threading
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
    thread.join(timeout=5)

    if thread.is_alive():
        print("ERROR: Thread timed out!")
    elif error:
        print(f"ERROR: {error[0]}")
    elif result:
        print(f"\nSUCCESS! Got result: {result[0]}")
    else:
        print("ERROR: No result")

    await server.close()
    print("\nServer closed")


if __name__ == '__main__':
    asyncio.run(test())
