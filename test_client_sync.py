"""Test sync client connecting to AsyncioServer"""
import rpyc
import time

print("Connecting to server on port 19997...")
conn = rpyc.connect('localhost', 19997)
print("Connected!")

print("Calling sync method...")
result = conn.root.sync_hello()
print(f"Sync result: {result}")

conn.close()
print("Done!")
