"""
Integration Test: RPyC Refcount Error Reproduction (No External Dependencies)

This test reproduces the refcount errors that appear in production:
- "[REFCOUNT] DECREF on missing key" errors
- "Failed to delete remote object" warnings

The test performs intensive operations that trigger race conditions in:
1. Netref creation/deletion lifecycle
2. Proxy cache weak reference management
3. Background cleanup task processing
4. Remote object registry synchronization

Expected behavior: Test should FAIL and detect refcount errors in logs/stderr.
"""
import unittest
import asyncio
import time
import gc
import sys
import io
import re
import logging
from contextlib import redirect_stderr
from multiprocessing import Process, Queue

import rpyc
from rpyc.core.async_connect import async_connect
from rpyc.utils.async_server import AsyncioServer
from tests.support import get_free_port


def run_refcount_error_server(port, ready_queue):
    """Server process that exposes methods likely to trigger refcount issues"""

    class RefcountTestService(rpyc.Service):
        """Service designed to trigger refcount race conditions"""

        def __init__(self):
            super().__init__()
            self._conn = None
            self.temp_storage = {}
            self.call_counter = 0

        def on_connect(self, conn):
            """Store connection reference"""
            self._conn = conn

        async def exposed_rapid_object_creation(self, count):
            """
            Create and return many temporary objects rapidly.
            This tests netref creation/deletion race conditions.
            """
            results = []
            for i in range(count):
                # Create different types of objects
                temp_dict = {"index": i, "data": f"value_{i}"}
                temp_list = [i, f"item_{i}", {"nested": i}]
                temp_tuple = (i, f"tuple_{i}")

                results.append({
                    "dict": temp_dict,
                    "list": temp_list,
                    "tuple": temp_tuple
                })

            return results

        async def exposed_return_object_with_methods(self, count):
            """
            CRITICAL: Return objects with bound methods.
            This is what triggers the refcount errors!

            When methods are passed as netrefs and then deleted,
            we get "[REFCOUNT] DECREF on missing key" for bound methods.
            """
            results = []
            for i in range(count):
                # Create object with methods
                obj = {"index": i, "data": f"value_{i}"}

                # Add dict methods (these become netrefs)
                results.append({
                    "obj": obj,
                    "get_method": obj.get,  # Bound method
                    "keys_method": obj.keys,  # Bound method
                    "values_method": obj.values,  # Bound method
                })

            return results

        async def exposed_pass_methods_to_callback(self, client_callback, iterations):
            """
            Pass objects with methods to client callback.
            This creates heavy method netref traffic.
            """
            for i in range(iterations):
                obj = {"id": i, "data": [1, 2, 3]}

                # Pass object WITH its methods to client
                await client_callback({
                    "obj": obj,
                    "get": obj.get,
                    "keys": obj.keys,
                    "items": obj.items,
                })

                # Force immediate cleanup
                if i % 5 == 0:
                    import gc
                    gc.collect()

            return iterations

        async def exposed_return_same_method_multiple_times(self, count):
            """
            CRITICAL: Return THE SAME bound method multiple times.

            This might trigger double-deletion:
            1. Client gets same method multiple times
            2. Creates multiple netrefs to same method
            3. All netrefs deleted
            4. Multiple DECREF calls for same object
            5. First DECREF removes from registry
            6. Subsequent DECREFs fail with "DECREF on missing key"
            """
            obj = {"shared": "data", "value": 123}

            # Return same method many times
            results = []
            for i in range(count):
                results.append({
                    "iteration": i,
                    "get_method": obj.get,  # SAME METHOD EVERY TIME
                    "keys_method": obj.keys,  # SAME METHOD EVERY TIME
                })

            return results

        async def exposed_create_and_pass_back(self, client_callback, iterations):
            """
            Create objects and pass them to client callback, which passes them back.
            This creates HEAVY netref traffic in both directions.
            """
            for i in range(iterations):
                # Create object on server
                server_obj = {"server_id": i, "data": [1, 2, 3]}

                # Pass to client, client processes and returns
                client_result = await client_callback(server_obj)

                # Immediately discard both objects
                del server_obj
                del client_result

                # Force immediate GC attempt (aggressive)
                if i % 10 == 0:
                    import gc
                    gc.collect()

            return iterations

        async def exposed_nested_callback_chain(self, client_callback, depth):
            """
            Create deep callback chain with nested netrefs.
            This tests cleanup of nested proxy references.
            """
            if depth <= 0:
                return {"depth": 0, "value": "leaf"}

            # Create temporary object and pass to client
            temp_obj = {"depth": depth, "data": f"level_{depth}"}

            # Call client callback with temp object (creates netref on client)
            client_result = await client_callback(temp_obj)

            # Recurse
            nested_result = await self.exposed_nested_callback_chain(
                client_callback,
                depth - 1
            )

            return {
                "depth": depth,
                "client_result": client_result,
                "nested": nested_result
            }

        async def exposed_rapid_store_release(self, key_prefix, iterations):
            """
            Rapidly store and release objects.
            Tests race between storage and cleanup.
            """
            for i in range(iterations):
                key = f"{key_prefix}_{i}"
                obj = {"key": key, "iteration": i, "data": [1, 2, 3]}

                # Store
                self.temp_storage[key] = obj

                # Small delay to allow netref creation
                await asyncio.sleep(0.001)

                # Immediate release
                del self.temp_storage[key]

            return iterations

        async def exposed_callback_burst(self, client_callback, burst_size):
            """
            Call client callback in rapid succession.
            Tests concurrent netref creation/deletion.

            FIXED: Netref calls return AsyncResult, not coroutines.
            We need to wrap them in async functions for create_task().
            """
            async def call_wrapper(i):
                """Wrapper to convert AsyncResult to coroutine"""
                # client_callback() returns AsyncResult (netref call)
                # await converts it to actual result
                return await client_callback({"burst_id": i, "data": f"burst_{i}"})

            tasks = []
            for i in range(burst_size):
                # Create task with wrapper coroutine
                task = asyncio.create_task(call_wrapper(i))
                tasks.append(task)

            # Wait for all callbacks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)

            return {"completed": len(results), "errors": sum(1 for r in results if isinstance(r, Exception))}

        def exposed_get_registry_stats(self):
            """Get current registry statistics"""
            return {
                "local_objects_count": len(self._conn._local_objects._dict),
                "proxy_cache_count": len(self._conn._proxy_cache._dict),
                "pending_deletions": self._conn._pending_deletions.qsize(),
                "cleanup_running": self._conn._cleanup_running
            }

        def exposed_force_cleanup_cycle(self):
            """Force a cleanup cycle (for debugging)"""
            gc.collect()
            return True

    async def server_main():
        server = AsyncioServer(
            RefcountTestService,
            port=port,
            protocol_config={
                "allow_public_attrs": True,
                "sync_request_timeout": 30,
                "cleanup_interval": 0.5,  # Faster cleanup for testing
                "cleanup_ack_timeout": 2.0,
                "logger": logging.getLogger("rpyc.server"),
                "debug_refcounting": True  # Enable refcount debug logging
            }
        )

        try:
            await server.start()
            ready_queue.put("ready")
            await asyncio.Event().wait()  # Wait forever until cancelled
        except asyncio.CancelledError:
            pass
        finally:
            await server.close()

    try:
        asyncio.run(server_main())
    except KeyboardInterrupt:
        pass


class TestRefcountErrorReproduction(unittest.TestCase):
    """Test case to reproduce refcount errors"""

    @classmethod
    def setUpClass(cls):
        """Start test server"""
        cls.port = get_free_port()
        cls.ready_queue = Queue()

        cls.server_process = Process(
            target=run_refcount_error_server,
            args=(cls.port, cls.ready_queue),
            daemon=True
        )
        cls.server_process.start()

        # Wait for server ready
        try:
            signal = cls.ready_queue.get(timeout=10)
            if signal != "ready":
                raise RuntimeError(f"Unexpected signal: {signal}")
        except Exception as e:
            cls.server_process.terminate()
            cls.server_process.join(timeout=2)
            raise RuntimeError(f"Server startup timeout: {e}")

        time.sleep(0.5)  # Additional stabilization

    @classmethod
    def tearDownClass(cls):
        """Stop test server"""
        if cls.server_process.is_alive():
            cls.server_process.terminate()
            cls.server_process.join(timeout=5)
            if cls.server_process.is_alive():
                cls.server_process.kill()

    def _capture_stderr_and_logs(self):
        """Capture stderr and logging output"""
        stderr_capture = io.StringIO()
        log_capture = io.StringIO()

        # Capture stderr
        stderr_redirect = redirect_stderr(stderr_capture)

        # Capture logging
        log_handler = logging.StreamHandler(log_capture)
        log_handler.setLevel(logging.DEBUG)
        log_formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s')
        log_handler.setFormatter(log_formatter)

        rpyc_logger = logging.getLogger("rpyc")
        rpyc_logger.addHandler(log_handler)
        rpyc_logger.setLevel(logging.DEBUG)

        return stderr_redirect, stderr_capture, log_handler, log_capture

    def _check_for_refcount_errors(self, stderr_output, log_output):
        """
        Check for refcount error patterns.

        Returns:
            dict with error details
        """
        # Combine stderr and log output
        combined_output = stderr_output + "\n" + log_output

        # Patterns to search for
        patterns = {
            "decref_missing_key": r'\[REFCOUNT\]\s+DECREF on missing key',
            "failed_delete": r'Failed to delete remote object',
            "refcount_missing": r'REFCOUNT.*missing',
            "delete_failed": r'delete.*remote.*object.*failed',
        }

        errors = []
        for pattern_name, pattern in patterns.items():
            matches = re.finditer(pattern, combined_output, re.IGNORECASE)
            for match in matches:
                # Get context around match
                start = max(0, match.start() - 100)
                end = min(len(combined_output), match.end() + 100)
                context = combined_output[start:end]

                errors.append({
                    "pattern": pattern_name,
                    "match": match.group(0),
                    "context": context
                })

        return {
            "has_errors": len(errors) > 0,
            "error_count": len(errors),
            "errors": errors,
            "full_output": combined_output
        }

    def test_rapid_object_creation_triggers_refcount_errors(self):
        """
        Test: Rapid object creation/deletion should trigger refcount errors.

        This test creates many objects rapidly and immediately releases them,
        causing race conditions in the cleanup mechanism.
        """
        # Set up capture
        stderr_redirect, stderr_capture, log_handler, log_capture = self._capture_stderr_and_logs()

        try:
            with stderr_redirect:
                async def run_test():
                    # Connect with asyncio enabled
                    conn = await async_connect(
                        "localhost",
                        self.port,
                        config={
                            "sync_request_timeout": 30,
                            "cleanup_interval": 0.5,
                            "cleanup_ack_timeout": 2.0,
                            "logger": logging.getLogger("rpyc.client"),
                            "debug_refcounting": True
                        }
                    )

                    try:
                        # Enable asyncio serving to start cleanup task
                        loop = asyncio.get_running_loop()
                        conn.enable_asyncio_serving(loop=loop)

                        print(f"Connected to server on port {self.port}")

                        # Perform rapid object creation (creates many netrefs)
                        print("Performing rapid object creation (100 objects)...")
                        results = await conn.root.rapid_object_creation(100)

                        # Verify we got results
                        self.assertEqual(len(results), 100)
                        print(f"Created {len(results)} temporary objects")

                        # Clear results immediately (triggers cleanup)
                        del results
                        gc.collect()

                        # Get stats before cleanup
                        stats_before = conn.root.get_registry_stats()
                        print(f"Registry stats before cleanup: {stats_before}")

                        # Wait for cleanup cycles
                        print("Waiting for cleanup cycles (3 seconds)...")
                        await asyncio.sleep(3.0)

                        # Force garbage collection
                        gc.collect()
                        await asyncio.sleep(1.0)

                        # Get stats after cleanup
                        stats_after = conn.root.get_registry_stats()
                        print(f"Registry stats after cleanup: {stats_after}")

                    finally:
                        # Close connection
                        print("Closing connection...")
                        conn.close()
                        await asyncio.sleep(1.0)

                # Run the test
                asyncio.run(run_test())

        finally:
            # Remove log handler
            rpyc_logger = logging.getLogger("rpyc")
            rpyc_logger.removeHandler(log_handler)

        # Wait for any delayed log writes
        time.sleep(1.0)

        # Check for errors
        stderr_output = stderr_capture.getvalue()
        log_output = log_capture.getvalue()

        result = self._check_for_refcount_errors(stderr_output, log_output)

        # Print analysis
        print("\n" + "="*80)
        print("REFCOUNT ERROR DETECTION RESULTS")
        print("="*80)
        print(f"Errors found: {result['error_count']}")

        if result['has_errors']:
            print("\nDETECTED ERRORS:")
            for i, error in enumerate(result['errors'], 1):
                print(f"\nError #{i}:")
                print(f"  Pattern: {error['pattern']}")
                print(f"  Match: {error['match']}")
                print(f"  Context: {error['context']}")

        print("\n" + "="*80)

        # INTENTIONALLY FAIL if errors found (this is expected!)
        if result['has_errors']:
            self.fail(
                f"✅ SUCCESS: Reproduced refcount errors! Found {result['error_count']} error(s).\n"
                f"Patterns detected: {', '.join(e['pattern'] for e in result['errors'])}\n"
                f"This test successfully reproduces the issue."
            )
        else:
            # This means the bug might be fixed or we need a more aggressive test
            print("⚠️  WARNING: No refcount errors detected. Bug may be fixed or test needs adjustment.")

    def test_nested_callbacks_trigger_refcount_errors(self):
        """
        Test: Nested callbacks with complex object passing trigger refcount errors.

        This test creates deeply nested callback chains that pass objects back and forth.
        """
        stderr_redirect, stderr_capture, log_handler, log_capture = self._capture_stderr_and_logs()

        try:
            with stderr_redirect:
                async def run_test():
                    conn = await async_connect(
                        "localhost",
                        self.port,
                        config={
                            "sync_request_timeout": 30,
                            "cleanup_interval": 0.5,
                            "cleanup_ack_timeout": 2.0,
                            "logger": logging.getLogger("rpyc.client"),
                            "debug_refcounting": True
                        }
                    )

                    try:
                        loop = asyncio.get_running_loop()
                        conn.enable_asyncio_serving(loop=loop)

                        print(f"Testing nested callbacks...")

                        # Client callback that processes server objects
                        async def client_callback(server_obj):
                            # Process and return new object
                            return {
                                "received": server_obj,
                                "processed": True,
                                "timestamp": time.time()
                            }

                        # Call nested callback chain
                        result = await conn.root.nested_callback_chain(
                            client_callback,
                            depth=10
                        )

                        print(f"Nested callback completed: depth={result.get('depth')}")

                        # Clear everything
                        del result
                        gc.collect()

                        # Wait for cleanup
                        await asyncio.sleep(3.0)
                        gc.collect()

                    finally:
                        conn.close()
                        await asyncio.sleep(1.0)

                asyncio.run(run_test())

        finally:
            rpyc_logger = logging.getLogger("rpyc")
            rpyc_logger.removeHandler(log_handler)

        time.sleep(1.0)

        # Check for errors
        result = self._check_for_refcount_errors(
            stderr_capture.getvalue(),
            log_capture.getvalue()
        )

        print(f"\nNested callbacks test - Errors found: {result['error_count']}")

        if result['has_errors']:
            self.fail(
                f"✅ SUCCESS: Nested callbacks reproduced refcount errors! "
                f"Found {result['error_count']} error(s)."
            )

    def test_rapid_store_release_cycle(self):
        """
        Test: Rapid store/release cycles trigger refcount errors.

        This test rapidly stores and releases objects to trigger race conditions
        between object registration and cleanup.
        """
        stderr_redirect, stderr_capture, log_handler, log_capture = self._capture_stderr_and_logs()

        try:
            with stderr_redirect:
                async def run_test():
                    conn = await async_connect(
                        "localhost",
                        self.port,
                        config={
                            "sync_request_timeout": 30,
                            "cleanup_interval": 0.3,  # Very fast cleanup
                            "cleanup_ack_timeout": 1.0,
                            "debug_refcounting": True
                        }
                    )

                    try:
                        loop = asyncio.get_running_loop()
                        conn.enable_asyncio_serving(loop=loop)

                        print(f"Testing rapid store/release cycles...")

                        # Perform rapid store/release
                        iterations = await conn.root.rapid_store_release("test_key", 50)
                        print(f"Completed {iterations} rapid store/release cycles")

                        # Wait for cleanup
                        await asyncio.sleep(2.0)
                        gc.collect()

                        stats = conn.root.get_registry_stats()
                        print(f"Final stats: {stats}")

                    finally:
                        conn.close()
                        await asyncio.sleep(1.0)

                asyncio.run(run_test())

        finally:
            rpyc_logger = logging.getLogger("rpyc")
            rpyc_logger.removeHandler(log_handler)

        time.sleep(1.0)

        # Check for errors
        result = self._check_for_refcount_errors(
            stderr_capture.getvalue(),
            log_capture.getvalue()
        )

        print(f"\nRapid store/release test - Errors found: {result['error_count']}")

        if result['has_errors']:
            self.fail(
                f"✅ SUCCESS: Rapid store/release reproduced refcount errors! "
                f"Found {result['error_count']} error(s)."
            )

    def test_bidirectional_object_passing_stress(self):
        """
        Test: Bidirectional object passing under stress triggers refcount errors.

        This test passes objects back and forth between client and server rapidly,
        creating heavy netref traffic in both directions with immediate cleanup.
        """
        stderr_redirect, stderr_capture, log_handler, log_capture = self._capture_stderr_and_logs()

        try:
            with stderr_redirect:
                async def run_test():
                    conn = await async_connect(
                        "localhost",
                        self.port,
                        config={
                            "sync_request_timeout": 30,
                            "cleanup_interval": 0.2,  # Very fast cleanup
                            "cleanup_ack_timeout": 1.0,
                            "debug_refcounting": True
                        }
                    )

                    try:
                        loop = asyncio.get_running_loop()
                        conn.enable_asyncio_serving(loop=loop)

                        print(f"Testing bidirectional object passing stress...")

                        # Client callback that creates new objects and passes them back
                        async def client_callback(server_obj):
                            # Process server object
                            client_obj = {
                                "received": server_obj,
                                "client_data": [10, 20, 30],
                                "processed": True
                            }
                            # Small delay to increase race condition chance
                            await asyncio.sleep(0.001)
                            return client_obj

                        # Run multiple iterations with rapid object passing
                        iterations = await conn.root.create_and_pass_back(
                            client_callback,
                            iterations=100
                        )
                        print(f"Completed {iterations} bidirectional object passes")

                        # Force GC
                        gc.collect()
                        await asyncio.sleep(1.0)

                        # Wait for cleanup cycles
                        await asyncio.sleep(2.0)
                        gc.collect()

                        stats = conn.root.get_registry_stats()
                        print(f"Final stats: {stats}")

                    finally:
                        conn.close()
                        await asyncio.sleep(1.0)

                asyncio.run(run_test())

        finally:
            rpyc_logger = logging.getLogger("rpyc")
            rpyc_logger.removeHandler(log_handler)

        time.sleep(1.0)

        # Check for errors
        result = self._check_for_refcount_errors(
            stderr_capture.getvalue(),
            log_capture.getvalue()
        )

        print(f"\nBidirectional stress test - Errors found: {result['error_count']}")

        if result['has_errors']:
            self.fail(
                f"✅ SUCCESS: Bidirectional stress test reproduced refcount errors! "
                f"Found {result['error_count']} error(s)."
            )

    def test_concurrent_callback_burst(self):
        """
        Test: Concurrent callback burst triggers refcount errors.

        This test fires many callbacks concurrently to trigger race conditions
        in netref creation and cleanup.
        """
        stderr_redirect, stderr_capture, log_handler, log_capture = self._capture_stderr_and_logs()

        try:
            with stderr_redirect:
                async def run_test():
                    conn = await async_connect(
                        "localhost",
                        self.port,
                        config={
                            "sync_request_timeout": 30,
                            "cleanup_interval": 0.5,
                            "cleanup_ack_timeout": 2.0,
                            "logger": logging.getLogger("rpyc.client"),
                            "debug_refcounting": True
                        }
                    )

                    try:
                        loop = asyncio.get_running_loop()
                        conn.enable_asyncio_serving(loop=loop)

                        print(f"Testing concurrent callback burst...")

                        # Client callback
                        async def client_callback(data):
                            # Small delay to increase chance of race condition
                            await asyncio.sleep(0.01)
                            return {"received": data, "processed": True}

                        # Fire burst of concurrent callbacks
                        result = await conn.root.callback_burst(
                            client_callback,
                            burst_size=30
                        )

                        print(f"Burst completed: {result}")

                        # Wait for cleanup
                        await asyncio.sleep(3.0)
                        gc.collect()

                    finally:
                        conn.close()
                        await asyncio.sleep(1.0)

                asyncio.run(run_test())

        finally:
            rpyc_logger = logging.getLogger("rpyc")
            rpyc_logger.removeHandler(log_handler)

        time.sleep(1.0)

        # Check for errors
        result = self._check_for_refcount_errors(
            stderr_capture.getvalue(),
            log_capture.getvalue()
        )

        print(f"\nConcurrent burst test - Errors found: {result['error_count']}")

        if result['has_errors']:
            self.fail(
                f"✅ SUCCESS: Concurrent callbacks reproduced refcount errors! "
                f"Found {result['error_count']} error(s)."
            )


    def test_extreme_concurrent_operations_with_forced_cleanup(self):
        """
        Test: EXTREME concurrent operations with forced cleanup cycles.

        This test creates maximum chaos by:
        1. Running many concurrent operations
        2. Forcing GC during operations
        3. Creating/deleting objects rapidly
        4. Testing cleanup during high netref traffic
        """
        stderr_redirect, stderr_capture, log_handler, log_capture = self._capture_stderr_and_logs()

        try:
            with stderr_redirect:
                async def run_test():
                    conn = await async_connect(
                        "localhost",
                        self.port,
                        config={
                            "sync_request_timeout": 30,
                            "cleanup_interval": 0.1,  # VERY aggressive cleanup
                            "cleanup_ack_timeout": 0.5,
                            "debug_refcounting": True
                        }
                    )

                    try:
                        loop = asyncio.get_running_loop()
                        conn.enable_asyncio_serving(loop=loop)

                        print(f"Testing EXTREME concurrent operations...")

                        # Create multiple concurrent tasks
                        tasks = []

                        # Task 1: Rapid object creation
                        async def task1():
                            for _ in range(5):
                                results = await conn.root.rapid_object_creation(50)
                                del results
                                gc.collect()
                                await asyncio.sleep(0.05)

                        # Task 2: Bidirectional passing
                        async def task2():
                            async def callback(obj):
                                return {"processed": obj}
                            for _ in range(3):
                                await conn.root.create_and_pass_back(callback, 20)
                                gc.collect()
                                await asyncio.sleep(0.05)

                        # Task 3: Rapid store/release
                        async def task3():
                            for _ in range(3):
                                await conn.root.rapid_store_release("key", 20)
                                gc.collect()
                                await asyncio.sleep(0.05)

                        # Run all tasks concurrently
                        tasks = [task1(), task2(), task3()]
                        await asyncio.gather(*tasks, return_exceptions=True)

                        print("Concurrent tasks completed")

                        # Force GC
                        gc.collect()
                        await asyncio.sleep(1.0)

                        # Check stats
                        stats = conn.root.get_registry_stats()
                        print(f"Stats after concurrent operations: {stats}")

                        # Wait for cleanup
                        await asyncio.sleep(3.0)
                        gc.collect()

                        final_stats = conn.root.get_registry_stats()
                        print(f"Final stats: {final_stats}")

                    finally:
                        conn.close()
                        await asyncio.sleep(1.0)

                asyncio.run(run_test())

        finally:
            rpyc_logger = logging.getLogger("rpyc")
            rpyc_logger.removeHandler(log_handler)

        time.sleep(1.0)

        # Check for errors
        result = self._check_for_refcount_errors(
            stderr_capture.getvalue(),
            log_capture.getvalue()
        )

        print(f"\nExtreme concurrent test - Errors found: {result['error_count']}")

        if result['has_errors']:
            print("\n" + "="*80)
            print("ERROR DETAILS:")
            for i, error in enumerate(result['errors'], 1):
                print(f"\n{i}. {error['match']}")
                print(f"   Context: {error['context'][:200]}")
            print("="*80)

            self.fail(
                f"✅ SUCCESS: Extreme concurrent operations reproduced refcount errors! "
                f"Found {result['error_count']} error(s)."
            )
        else:
            # Print output for debugging
            print("\nCaptured stderr:")
            print(stderr_capture.getvalue()[:1000])
            print("\nCaptured logs:")
            print(log_capture.getvalue()[:1000])


    def test_method_netrefs_trigger_refcount_errors(self):
        """
        Test: Passing bound methods as netrefs triggers DECREF errors.

        THIS IS THE KEY TEST that reproduces the real-world issue!

        The external test shows errors like:
        "[REFCOUNT] DECREF on missing key <bound method ...>"

        This happens when:
        1. Server returns objects with bound methods
        2. Client receives methods as netrefs
        3. Client deletes netrefs
        4. Cleanup tries to decref methods that are already gone
        """
        stderr_redirect, stderr_capture, log_handler, log_capture = self._capture_stderr_and_logs()

        try:
            with stderr_redirect:
                async def run_test():
                    conn = await async_connect(
                        "localhost",
                        self.port,
                        config={
                            "sync_request_timeout": 30,
                            "cleanup_interval": 0.3,
                            "cleanup_ack_timeout": 1.0,
                            "debug_refcounting": True
                        }
                    )

                    try:
                        loop = asyncio.get_running_loop()
                        conn.enable_asyncio_serving(loop=loop)

                        print(f"\n{'='*60}")
                        print("TEST: Method Netrefs (THIS SHOULD TRIGGER ERRORS!)")
                        print('='*60)

                        # Get objects with methods
                        print("Requesting objects with bound methods...")
                        results = await conn.root.return_object_with_methods(50)

                        print(f"Received {len(results)} objects with methods")

                        # Access some methods to ensure they're netrefs
                        for i in range(min(5, len(results))):
                            obj_data = results[i]
                            # These are netrefs to bound methods
                            get_method = obj_data["get_method"]
                            keys_method = obj_data["keys_method"]

                        print("Accessed method netrefs")

                        # Delete immediately - triggers cleanup
                        del results
                        gc.collect()

                        print("Deleted results, running GC...")

                        # Wait for cleanup cycles
                        await asyncio.sleep(2.0)
                        gc.collect()
                        await asyncio.sleep(1.0)

                        stats = conn.root.get_registry_stats()
                        print(f"Registry stats: {stats}")

                    finally:
                        conn.close()
                        await asyncio.sleep(1.0)

                asyncio.run(run_test())

        finally:
            rpyc_logger = logging.getLogger("rpyc")
            rpyc_logger.removeHandler(log_handler)

        time.sleep(1.0)

        # Check for errors
        result = self._check_for_refcount_errors(
            stderr_capture.getvalue(),
            log_capture.getvalue()
        )

        print(f"\n{'='*60}")
        print(f"RESULTS: Method netref test - Errors found: {result['error_count']}")
        print('='*60)

        if result['has_errors']:
            print("\n✅ SUCCESS! Found refcount errors:")
            for i, error in enumerate(result['errors'][:10], 1):
                print(f"\n{i}. Pattern: {error['pattern']}")
                print(f"   Match: {error['match']}")

            self.fail(
                f"✅ SUCCESS: Method netrefs reproduced refcount errors! "
                f"Found {result['error_count']} error(s).\n"
                f"This confirms the test reproduces the real-world issue."
            )
        else:
            print("\n⚠️  No errors detected in method netref test")
            print("\nCaptured output (first 500 chars):")
            print(stderr_capture.getvalue()[:500])
            print(log_capture.getvalue()[:500])

    def test_same_method_multiple_netrefs_CRITICAL(self):
        """
        CRITICAL TEST: Pass same method multiple times.

        This is THE KEY scenario that should trigger:
        "[REFCOUNT] DECREF on missing key"

        Scenario:
        1. Server returns SAME bound method 100 times
        2. Client creates 100 netrefs to the SAME method
        3. Client deletes all 100 netrefs
        4. First cleanup removes method from registry
        5. Subsequent 99 cleanups try to DECREF missing key
        6. ERROR: "[REFCOUNT] DECREF on missing key <bound method...>"
        """
        stderr_redirect, stderr_capture, log_handler, log_capture = self._capture_stderr_and_logs()

        # Also capture to file for debugging
        import sys
        original_stderr = sys.stderr

        try:
            with stderr_redirect:
                async def run_test():
                    conn = await async_connect(
                        "localhost",
                        self.port,
                        config={
                            "sync_request_timeout": 30,
                            "cleanup_interval": 0.1,  # Very fast
                            "cleanup_ack_timeout": 0.5,
                            "debug_refcounting": True
                        }
                    )

                    try:
                        loop = asyncio.get_running_loop()
                        conn.enable_asyncio_serving(loop=loop)

                        print(f"\n{'='*70}")
                        print("CRITICAL TEST: Same Method Multiple Times")
                        print("THIS SHOULD DEFINITELY TRIGGER DECREF ERRORS!")
                        print('='*70)

                        # Get same method 100 times
                        print("Requesting same method 100 times...")
                        results = await conn.root.return_same_method_multiple_times(100)

                        print(f"Received {len(results)} items (all with SAME methods)")

                        # Access a few to confirm they're netrefs
                        print(f"First method: {results[0]['get_method']}")
                        print(f"Last method: {results[-1]['get_method']}")

                        # Get stats before deletion
                        stats_before = conn.root.get_registry_stats()
                        print(f"\nStats BEFORE deletion: {stats_before}")

                        # DELETE ALL - should trigger 100 DECREF calls for same methods
                        print("\nDeleting all results...")
                        del results
                        gc.collect()

                        print("Waiting for cleanup (first cycle)...")
                        await asyncio.sleep(0.5)

                        stats_mid = conn.root.get_registry_stats()
                        print(f"Stats AFTER first cleanup: {stats_mid}")

                        # More cleanup cycles
                        for i in range(3):
                            await asyncio.sleep(0.5)
                            gc.collect()
                            print(f"Cleanup cycle {i+2}...")

                        stats_final = conn.root.get_registry_stats()
                        print(f"\nFinal stats: {stats_final}")

                    finally:
                        print("\nClosing connection...")
                        conn.close()
                        await asyncio.sleep(1.0)

                asyncio.run(run_test())

        finally:
            sys.stderr = original_stderr
            rpyc_logger = logging.getLogger("rpyc")
            rpyc_logger.removeHandler(log_handler)

        time.sleep(1.0)

        # Check for errors
        stderr_text = stderr_capture.getvalue()
        log_text = log_capture.getvalue()

        result = self._check_for_refcount_errors(stderr_text, log_text)

        print(f"\n{'='*70}")
        print(f"RESULTS: Same method test - Errors found: {result['error_count']}")
        print('='*70)

        if result['has_errors']:
            print("\n✅✅✅ SUCCESS! REPRODUCED THE BUG! ✅✅✅")
            print(f"\nFound {result['error_count']} refcount errors!")
            print("\nFirst 10 errors:")
            for i, error in enumerate(result['errors'][:10], 1):
                print(f"\n{i}. Pattern: {error['pattern']}")
                print(f"   Match: {error['match']}")

            self.fail(
                f"✅ SUCCESS: Same-method test reproduced refcount errors!\n"
                f"Found {result['error_count']} '[REFCOUNT] DECREF on missing key' error(s).\n"
                f"This confirms the test successfully reproduces the real-world issue.\n"
                f"\n"
                f"The bug occurs when the same bound method is passed multiple times,\n"
                f"creating multiple netrefs to the same object, which then triggers\n"
                f"multiple DECREF calls during cleanup - but only the first one succeeds."
            )
        else:
            print("\n⚠️  WARNING: No errors detected")
            print("\nThis might mean:")
            print("  1. The refcount system correctly handles duplicate methods")
            print("  2. The errors occur only in specific conditions not reproduced here")
            print("  3. The logging is not being captured properly")
            print(f"\nStderr length: {len(stderr_text)}")
            print(f"Log length: {len(log_text)}")
            if stderr_text:
                print(f"\nStderr sample:\n{stderr_text[:500]}")
            if log_text:
                print(f"\nLog sample:\n{log_text[:500]}")

    def test_method_callbacks_trigger_refcount_errors(self):
        """
        Test: Passing methods through callbacks triggers DECREF errors.

        This simulates the real usage pattern where objects with methods
        are passed back and forth between client and server.
        """
        stderr_redirect, stderr_capture, log_handler, log_capture = self._capture_stderr_and_logs()

        try:
            with stderr_redirect:
                async def run_test():
                    conn = await async_connect(
                        "localhost",
                        self.port,
                        config={
                            "sync_request_timeout": 30,
                            "cleanup_interval": 0.2,
                            "cleanup_ack_timeout": 1.0,
                            "debug_refcounting": True
                        }
                    )

                    try:
                        loop = asyncio.get_running_loop()
                        conn.enable_asyncio_serving(loop=loop)

                        print(f"\n{'='*60}")
                        print("TEST: Method Callbacks (AGGRESSIVE)")
                        print('='*60)

                        # Client callback that receives objects with methods
                        async def client_callback(data):
                            # Receive object with method netrefs
                            obj = data.get("obj")
                            get_method = data.get("get")

                            # Return new object with methods
                            result = {"status": "processed"}
                            return {
                                "result": result,
                                "get": result.get,
                                "keys": result.keys
                            }

                        # Pass methods through callbacks
                        print("Running method callback iterations...")
                        iterations = await conn.root.pass_methods_to_callback(
                            client_callback,
                            iterations=50
                        )

                        print(f"Completed {iterations} callback iterations")

                        # Force cleanup
                        gc.collect()
                        await asyncio.sleep(2.0)
                        gc.collect()

                        stats = conn.root.get_registry_stats()
                        print(f"Final stats: {stats}")

                    finally:
                        conn.close()
                        await asyncio.sleep(1.0)

                asyncio.run(run_test())

        finally:
            rpyc_logger = logging.getLogger("rpyc")
            rpyc_logger.removeHandler(log_handler)

        time.sleep(1.0)

        # Check for errors
        result = self._check_for_refcount_errors(
            stderr_capture.getvalue(),
            log_capture.getvalue()
        )

        print(f"\n{'='*60}")
        print(f"RESULTS: Method callbacks - Errors found: {result['error_count']}")
        print('='*60)

        if result['has_errors']:
            print("\n✅ SUCCESS! Found refcount errors:")
            for i, error in enumerate(result['errors'][:10], 1):
                print(f"\n{i}. {error['match']}")

            self.fail(
                f"✅ SUCCESS: Method callbacks reproduced refcount errors! "
                f"Found {result['error_count']} error(s)."
            )


if __name__ == "__main__":
    # Run with verbose output
    unittest.main(verbosity=2)
