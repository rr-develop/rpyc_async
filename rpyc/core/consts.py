"""Constants used by the protocol
"""

# ============================================================================
# Protocol Version
# ============================================================================
PROTOCOL_VERSION = (5, 1)  # Bumped from (5, 0) for async support

# ============================================================================
# Sync Messages (existing)
# ============================================================================
MSG_REQUEST = 1
MSG_REPLY = 2
MSG_EXCEPTION = 3

# ============================================================================
# Async Messages (new in v5.1)
# ============================================================================
MSG_ASYNC_REQUEST = 10      # Async RPC request
MSG_ASYNC_REPLY = 11        # Async RPC reply
MSG_ASYNC_EXCEPTION = 12    # Async RPC exception

# boxing
LABEL_VALUE = 1
LABEL_TUPLE = 2
LABEL_LOCAL_REF = 3
LABEL_REMOTE_REF = 4

# ============================================================================
# Sync Action Handlers (existing)
# ============================================================================
HANDLE_PING = 1
HANDLE_CLOSE = 2
HANDLE_GETROOT = 3
HANDLE_GETATTR = 4
HANDLE_DELATTR = 5
HANDLE_SETATTR = 6
HANDLE_CALL = 7
HANDLE_CALLATTR = 8
HANDLE_REPR = 9
HANDLE_STR = 10
HANDLE_CMP = 11
HANDLE_HASH = 12
HANDLE_DIR = 13
HANDLE_PICKLE = 14
HANDLE_DEL = 15
HANDLE_INSPECT = 16
HANDLE_BUFFITER = 17
HANDLE_OLDSLICING = 18
HANDLE_CTXEXIT = 19
HANDLE_INSTANCECHECK = 20

# ============================================================================
# Async Action Handlers (new in v5.1)
# ============================================================================
HANDLE_ASYNC_CALL = 100          # Call async function
HANDLE_ASYNC_CALLATTR = 101      # Call async method/attribute

# ============================================================================
# Object Flags (new in v5.1)
# Used in extended id_pack format: (class, id, ver, flags)
# ============================================================================
FLAGS_SYNC = 0x00          # Default: sync object
FLAGS_ASYNC = 0x01         # Bit 0: async function/coroutine
# Reserved for future use:
# FLAGS_GENERATOR = 0x02   # Bit 1: generator
# FLAGS_CONTEXT = 0x04     # Bit 2: context manager

# ============================================================================
# Optimized Exceptions
# ============================================================================
EXC_STOP_ITERATION = 1

# ============================================================================
# IO Values
# ============================================================================
STREAM_CHUNK = 64000  # read/write chunk is 64KB, too large of a value will degrade response for other clients

# DEBUG
# for k in globals().keys():
#    globals()[k] = k
