"""
Async handler implementations for RPyC protocol.

This module provides async-aware handlers that can execute
async functions and coroutines without blocking.

Handlers:
    - _handle_async_call: Execute async function calls and await coroutines
    - _handle_async_callattr: Get attribute and call it asynchronously

Usage:
    # Register handlers in Connection
    from rpyc.core.async_handlers import register_async_handlers
    register_async_handlers(conn)
"""
import inspect
from typing import Any, Tuple, List
from rpyc.core import consts


async def _handle_async_call(
    conn: Any,
    obj: Any,
    args: Tuple,
    kwargs: List[Tuple[str, Any]] = ()
) -> Any:
    """
    Handler for HANDLE_ASYNC_CALL.

    Executes async function calls and awaits coroutines.

    Args:
        conn: Connection instance
        obj: Callable or coroutine to execute
        args: Positional arguments tuple
        kwargs: Keyword arguments list of (key, value) tuples

    Returns:
        Result of function call

    Raises:
        Exception: Any exception raised by the function

    Examples:
        # Async function
        async def add(x, y):
            return x + y
        result = await _handle_async_call(conn, add, (3, 4), ())

        # Pre-created coroutine
        coro = add(3, 4)
        result = await _handle_async_call(conn, coro, (), ())

        # Sync function (fallback)
        def mul(x, y):
            return x * y
        result = await _handle_async_call(conn, mul, (3, 4), ())
    """
    # Convert kwargs list to dict
    kwargs_dict = dict(kwargs)

    # Case 1: obj is already a coroutine (pre-created)
    if inspect.iscoroutine(obj):
        result = await obj
        return result

    # Case 2: obj is async function - call and await
    if inspect.iscoroutinefunction(obj):
        coro = obj(*args, **kwargs_dict)
        result = await coro
        return result

    # Case 3: obj is sync function - call normally
    # (This handles sync callbacks passed to async exposed methods)
    result = obj(*args, **kwargs_dict)

    # If sync function returned coroutine, await it
    if inspect.iscoroutine(result):
        result = await result

    return result


async def _handle_async_callattr(
    conn: Any,
    obj: Any,
    name: str,
    args: Tuple,
    kwargs: List[Tuple[str, Any]] = ()
) -> Any:
    """
    Handler for HANDLE_ASYNC_CALLATTR.

    Gets attribute and calls it asynchronously if needed.

    Args:
        conn: Connection instance
        obj: Object to get attribute from
        name: Attribute name
        args: Positional arguments tuple
        kwargs: Keyword arguments list of (key, value) tuples

    Returns:
        Result of method call

    Raises:
        AttributeError: If attribute doesn't exist
        Exception: Any exception raised by the method

    Examples:
        class Calc:
            async def add(self, x, y):
                return x + y

        calc = Calc()
        result = await _handle_async_callattr(conn, calc, 'add', (3, 4), ())
    """
    # Get attribute
    attr = getattr(obj, name)

    # Call via _handle_async_call
    return await _handle_async_call(conn, attr, args, kwargs)


def register_async_handlers(conn: Any) -> None:
    """
    Register async handlers in connection.

    Args:
        conn: Connection instance to register handlers on

    Example:
        conn = Connection(...)
        register_async_handlers(conn)
    """
    conn._HANDLERS[consts.HANDLE_ASYNC_CALL] = _handle_async_call
    conn._HANDLERS[consts.HANDLE_ASYNC_CALLATTR] = _handle_async_callattr
