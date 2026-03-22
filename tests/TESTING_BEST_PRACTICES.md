# RPyC Testing Best Practices

## Dynamic Port Allocation - REQUIRED

### ⚠️ Critical Rule

**NEVER use hardcoded ports in tests. ALWAYS use `get_free_port()` from `tests/support.py`.**

### Why Dynamic Ports Are Required

1. **Parallel Test Execution**: Hardcoded ports cause conflicts when tests run in parallel
2. **Sequential Test Conflicts**: Even sequential tests can conflict if ports aren't released properly
3. **System Port Conflicts**: Hardcoded ports may already be in use by other processes
4. **CI/CD Reliability**: Tests must be reliable in any environment

### ✅ CORRECT Pattern

```python
from tests.support import get_free_port
import unittest

class TestMyService(unittest.TestCase):
    def setUp(self):
        """Create isolated server for THIS test."""
        # Get unique port for this test instance
        self.server_port = get_free_port()

        # Start server with dynamic port
        self.server = AsyncioServer(
            MyService,
            hostname='localhost',
            port=self.server_port  # ✅ Dynamic port
        )
        await self.server.start()

    def tearDown(self):
        """Clean up after THIS test."""
        # Stop server and release port
        await self.server.close()

    def test_something(self):
        """Test uses isolated server."""
        # Connect to THIS test's server
        conn = rpyc.connect("localhost", self.server_port)
        # ... test logic ...
        conn.close()
```

### ❌ WRONG Patterns - DO NOT USE

#### 1. Hardcoded Ports ❌

```python
# ❌ WRONG: Hardcoded port
server = AsyncioServer(MyService, port=18870)

# ❌ WRONG: Hardcoded in multiple places
def test_a():
    conn = rpyc.connect("localhost", 18870)

def test_b():
    conn = rpyc.connect("localhost", 18870)  # Conflicts with test_a!
```

**Problem**: Tests will conflict and fail randomly depending on execution order.

#### 2. Shared Ports with setUpClass ❌

```python
class TestMyService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # ❌ WRONG: Port shared across ALL tests
        cls.server_port = get_free_port()
        cls.server = AsyncioServer(MyService, port=cls.server_port)

    def test_a(self):
        conn = rpyc.connect("localhost", self.server_port)

    def test_b(self):
        # Race condition! May fail if test_a didn't clean up
        conn = rpyc.connect("localhost", self.server_port)
```

**Problems**:
- Tests share state and are not isolated
- Race conditions between sequential tests
- One test failure can cascade to others
- Harder to debug failures

#### 3. Class Variables Instead of Instance Variables ❌

```python
class TestMyService(unittest.TestCase):
    server_port = 18870  # ❌ WRONG: Class variable, hardcoded

    def setUp(self):
        self.server = AsyncioServer(MyService, port=TestMyService.server_port)
```

**Problem**: Class variables are shared across test instances.

### Key Principles

1. **One Port Per Test**: Each test gets its own unique port via `setUp()`
2. **Instance Variables**: Use `self.port`, NOT `cls.port` or class attributes
3. **setUp/tearDown**: Create and destroy servers per-test, NOT per-class
4. **Isolation**: Tests should never share servers or ports
5. **No Hardcoding**: Use `get_free_port()` ALWAYS

### Example: Converting Bad Test to Good Test

#### Before (BAD) ❌

```python
class TestBidirectional(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = AsyncioServer(MyService, port=18870)
        cls.server.start()

    def test_call_1(self):
        conn = rpyc.connect("localhost", 18870)
        # ...

    def test_call_2(self):
        conn = rpyc.connect("localhost", 18870)
        # May fail due to race condition!
```

#### After (GOOD) ✅

```python
from tests.support import get_free_port

class TestBidirectional(unittest.TestCase):
    def setUp(self):
        # Each test gets isolated server
        self.port = get_free_port()
        self.server = AsyncioServer(MyService, port=self.port)
        self.server.start()

    def tearDown(self):
        # Clean up after each test
        self.server.close()

    def test_call_1(self):
        conn = rpyc.connect("localhost", self.port)
        # ... test is isolated ...
        conn.close()

    def test_call_2(self):
        conn = rpyc.connect("localhost", self.port)
        # ... independent of test_call_1 ...
        conn.close()
```

### Testing Async Servers

For `AsyncioServer`, the pattern is slightly different due to async nature:

```python
class TestAsyncServer(unittest.TestCase):
    def setUp(self):
        """Start server in background thread with event loop."""
        self.port = get_free_port()
        self.server_loop = asyncio.new_event_loop()

        async def run_server():
            self.server = AsyncioServer(MyService, port=self.port)
            await self.server.start()

        def start_server():
            asyncio.set_event_loop(self.server_loop)
            self.server_loop.run_until_complete(run_server())
            self.server_loop.run_forever()

        self.server_thread = Thread(target=start_server, daemon=True)
        self.server_thread.start()
        time.sleep(0.5)  # Wait for server to start

    def tearDown(self):
        """Stop server cleanly."""
        async def stop():
            await self.server.close()

        future = asyncio.run_coroutine_threadsafe(stop(), self.server_loop)
        future.result(timeout=2.0)
        self.server_loop.call_soon_threadsafe(self.server_loop.stop)
```

### Reference Implementation

See `tests/test_critical_bidirectional_async.py` for a complete working example of:
- Dynamic port allocation per test
- Isolated AsyncioServer instances
- Proper setUp/tearDown lifecycle
- Clean resource cleanup

### Quick Checklist

Before writing or modifying tests, verify:

- [ ] No hardcoded port numbers (18870, 18861, etc.)
- [ ] Using `get_free_port()` from `tests/support.py`
- [ ] Port assigned in `setUp()`, NOT `setUpClass()`
- [ ] Port stored in `self.port`, NOT `cls.port`
- [ ] Server created and destroyed per-test
- [ ] Clean tearDown that stops server
- [ ] No shared state between tests

### Common Mistakes to Avoid

1. **Copy-pasting tests with hardcoded ports** - Always replace with `get_free_port()`
2. **Forgetting to update connection calls** - Use `self.port`, not hardcoded values
3. **Using setUpClass for "efficiency"** - Isolation > efficiency for tests
4. **Not cleaning up in tearDown** - Causes resource leaks

### Getting Help

If you see test failures related to:
- "Address already in use"
- "Connection refused"
- "Timeout" (especially in sequential tests)
- Random failures that pass when run individually

→ Check if you're following these best practices!

---

**Remember**: Good tests are isolated, repeatable, and don't depend on execution order. Dynamic port allocation is ESSENTIAL for achieving this.
