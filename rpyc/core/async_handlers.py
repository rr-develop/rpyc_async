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

    # ═══════════════════════════════════════════════════════════════════
    # Netref fast-path — MUST run before ``inspect.iscoroutinefunction``.
    #
    # If ``obj`` is a netref, probe its local ``____id_pack__`` /
    # ``____is_async__`` hints via ``object.__getattribute__`` (no RPC).
    # Two reasons this pre-check exists:
    #
    # 1. **Correctness (nested-AsyncResult leak).** For netrefs with
    #    ``____is_async__ == True``, ``obj(*args)`` goes through
    #    ``asyncreq`` and returns an ``AsyncResult``, not a coroutine.
    #    ``inspect.iscoroutine(result)`` is False on ``AsyncResult``,
    #    so without an explicit await we would ship the un-awaited
    #    ``AsyncResult`` back over the wire — symptom:
    #    ``<AsyncResult object (ready) at …> != <expected int>`` in
    #    ``test_netref_identity_preserved``.
    #
    # 2. **Liveness (bidirectional-recursion deadlock).** In
    #    server↔client recursive async call chains, both peers can end
    #    up entering ``inspect.iscoroutinefunction(netref_obj)`` at the
    #    same time. That call reaches into the netref's ``__func__`` /
    #    ``__code__`` via ``inspect._has_code_flag``, which issues a
    #    blocking ``syncreq(HANDLE_GETATTR)``. Both peers block in
    #    ``stream.write`` (mutual TCP flow-control stall) or in
    #    ``channel.poll`` waiting for a reply the peer can't produce.
    #    See ``docs/DESIGN_NESTED_ASYNC_RESULT.md`` §5.2 / §6 for the
    #    stack-snapshot evidence.
    #
    # Detection: ``____id_pack__`` is set only on netrefs; its presence
    # unambiguously identifies one. ``object.__getattribute__`` bypasses
    # the netref's RPC-based ``__getattribute__``.
    # ═══════════════════════════════════════════════════════════════════
    try:
        object.__getattribute__(obj, "____id_pack__")
        is_netref = True
    except (AttributeError, TypeError):
        is_netref = False

    if is_netref:
        try:
            netref_is_async = bool(
                object.__getattribute__(obj, "____is_async__")
            )
        except (AttributeError, TypeError):
            netref_is_async = False

        if netref_is_async:
            # Async-flagged netref — call goes through asyncreq and
            # yields an ``AsyncResult``; awaiting it is event-driven.
            async_res = obj(*args, **kwargs_dict)
            return await async_res

        # Sync netref — call via the normal syncreq path. No
        # ``iscoroutinefunction`` probe (would deadlock — see above).
        result = obj(*args, **kwargs_dict)
        if inspect.iscoroutine(result):
            result = await result
        return result

    # Not a netref — safe to probe with ``inspect.iscoroutinefunction``.
    try:
        is_coro_func = inspect.iscoroutinefunction(obj)
    except (AttributeError, TypeError):
        # Rare: local object that doesn't expose ``__code__`` cleanly.
        is_coro_func = False

    if is_coro_func:
        # Case 2: real async function.
        coro = obj(*args, **kwargs_dict)
        result = await coro
        return result

    # Case 3: real sync function — sync callbacks passed to async
    # exposed methods end up here.
    result = obj(*args, **kwargs_dict)
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
