# RPyC Tests

## Running Tests

```bash
# Run all tests
python3 -m pytest tests/

# Run specific test file
python3 -m pytest tests/test_critical_bidirectional_async.py -v

# Run specific test
python3 -m pytest tests/test_critical_bidirectional_async.py::TestCriticalBidirectionalAsync::test_bidirectional_async_depth_5 -v
```

## ⚠️ IMPORTANT: Dynamic Port Allocation

**When writing or modifying tests, you MUST use dynamic port allocation.**

See **[TESTING_BEST_PRACTICES.md](./TESTING_BEST_PRACTICES.md)** for complete guidelines.

### Quick Reference

✅ **DO THIS:**
```python
from tests.support import get_free_port

class TestMyService(unittest.TestCase):
    def setUp(self):
        self.port = get_free_port()  # ✅ Dynamic port per test
        self.server = AsyncioServer(MyService, port=self.port)
```

❌ **DON'T DO THIS:**
```python
# ❌ Hardcoded port
server = AsyncioServer(MyService, port=18870)

# ❌ Shared port in setUpClass
@classmethod
def setUpClass(cls):
    cls.port = get_free_port()  # Race conditions!
```

### Why This Matters

- Prevents port conflicts between tests
- Enables parallel test execution
- Ensures tests work in any environment
- Eliminates random "Address already in use" failures

### Reference Implementation

See `tests/test_critical_bidirectional_async.py` for a complete example of proper test isolation.

## Test Utilities

- `tests/support.py::get_free_port()` - Get OS-assigned free port (REQUIRED for all tests)
- `tests/support.py::import_module()` - Safely import modules with deprecation handling

## More Information

- [TESTING_BEST_PRACTICES.md](./TESTING_BEST_PRACTICES.md) - Comprehensive testing guidelines
- `tests/test_critical_bidirectional_async.py` - Reference implementation
