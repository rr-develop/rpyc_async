"""
Async handler implementations for RPyC protocol.

This module provides async-aware handlers that can execute
async functions and coroutines without blocking.

Handlers:
    - _handle_async_call: Execute async function calls and await coroutines
    - _handle_async_callattr: Get attribute and call it asynchronously

Usage:
    # Register handlers in Connection
    from rpyc_async.core.async_handlers import register_async_handlers
    register_async_handlers(conn)

═══════════════════════════════════════════════════════════════════════════════
CRITICAL: READ ``docs/DESIGN_BIDIRECTIONAL_ASYNC_FIXES.md`` BEFORE EDITING.
═══════════════════════════════════════════════════════════════════════════════
This file was the first of three sites fixed in a cascade that closed a
bidirectional-recursion deadlock and a nested-``AsyncResult`` correctness
leak. The ordering of the branches in ``_handle_async_call`` — **netref
fast-path BEFORE ``inspect.iscoroutinefunction``** — is load-bearing:
any future refactor that reorders these checks will re-open the
deadlock. Do not "simplify" by merging the netref and non-netref paths
into a single ``inspect`` call. See the design doc.
═══════════════════════════════════════════════════════════════════════════════
"""
import inspect
from typing import Any, Tuple, List
from rpyc_async.core import consts


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
    # ═══════════════════════════════════════════════════════════════════
    # ⚠  REGRESSION WARNING — DO NOT REORDER OR REMOVE  ⚠
    # ═══════════════════════════════════════════════════════════════════
    # This block closes TWO bugs that took hours of stack-snapshot
    # forensics to diagnose. Full post-mortem:
    # ``docs/DESIGN_BIDIRECTIONAL_ASYNC_FIXES.md``.
    #
    # If ``obj`` is a netref, probe its local ``____id_pack__`` /
    # ``____is_async__`` hints via ``object.__getattribute__`` (no RPC).
    # Two independent reasons this pre-check exists — both required.
    #
    # 1. **Correctness (nested-AsyncResult leak).** For netrefs with
    #    ``____is_async__ == True``, ``obj(*args)`` goes through
    #    ``asyncreq`` and returns an ``AsyncResult``, not a coroutine.
    #    ``inspect.iscoroutine(result)`` is False on ``AsyncResult``,
    #    so without an explicit ``await`` we would ship the un-awaited
    #    ``AsyncResult`` back over the wire — symptom:
    #    ``<AsyncResult object (ready) at …> != <expected int>`` in
    #    ``tests/test_e2e_netref_identity.py::test_netref_identity_preserved``.
    #
    # 2. **Liveness (bidirectional-recursion deadlock).** In
    #    server↔client recursive async call chains, BOTH peers can end
    #    up simultaneously entering ``inspect.iscoroutinefunction(
    #    netref_obj)``. That call reaches into the netref's
    #    ``__func__`` / ``__code__`` via ``inspect._has_code_flag``,
    #    each of which issues a **blocking**
    #    ``syncreq(HANDLE_GETATTR)`` back to the peer. If both peers
    #    do this at once, both are parked in ``stream.write`` (TCP
    #    flow-control stall) or ``channel.poll`` — stack-snapshot
    #    verified, see design doc §2.1.
    #    Reproducer:
    #    ``tests/test_e2e_netref_async_callback.py::test_netref_recursive_async_calls``.
    #
    # Why ``object.__getattribute__``:
    # The netref class overrides ``__getattribute__`` to dispatch to
    # the peer via ``HANDLE_GETATTR`` for non-LOCAL_ATTRS names.
    # Calling ``obj.____id_pack__`` or even ``getattr(obj, ...)`` on
    # LOCAL_ATTRS works, but ``object.__getattribute__`` bypasses the
    # netref machinery entirely and reads the slot directly — no RPC,
    # no failure modes, no re-entry into the deadlock.
    #
    # Why ``____id_pack__`` as the probe:
    # It's present on every netref (set in ``__init__``, see
    # ``rpyc/core/netref.py``) and absent on every non-netref Python
    # object. A fresh local coroutine function / bound method / user
    # callable never has it. So ``object.__getattribute__(obj,
    # "____id_pack__")`` either succeeds (netref) or raises
    # ``AttributeError`` (not a netref) — binary signal, no RPC.
    #
    # Do NOT attempt to "simplify" this by:
    #   - Calling ``hasattr(obj, "____id_pack__")``  ← fires
    #     ``__getattribute__``, re-enters the deadlock.
    #   - Folding the netref branch into the ``iscoroutinefunction``
    #     branch  ← re-opens both bugs.
    #   - Using ``isinstance(obj, BaseNetref)``  ← works but circular
    #     import; the slot-probe is cleaner and matches the duck-typed
    #     style of the rest of the netref layer.
    # ═══════════════════════════════════════════════════════════════════
    try:
        object.__getattribute__(obj, "____id_pack__")
        is_netref = True
    except (AttributeError, TypeError):
        is_netref = False

    if is_netref:
        # ``____is_async__`` is another LOCAL slot set by ``_unbox``
        # when the peer's boxing marked the callable with
        # ``FLAGS_ASYNC`` (i.e. the peer called
        # ``inspect.iscoroutinefunction(real_obj)`` at box-time and
        # stored the verdict). Probed via ``object.__getattribute__``
        # for the same reason as above: no RPC, no deadlock.
        try:
            netref_is_async = bool(
                object.__getattribute__(obj, "____is_async__")
            )
        except (AttributeError, TypeError):
            netref_is_async = False

        if netref_is_async:
            # Async-flagged netref. Calling ``obj(*args)`` invokes
            # the netref's ``__call__``, which — because
            # ``____is_async__`` is True — routes to ``asyncreq(
            # HANDLE_ASYNC_CALL, ...)`` and returns an
            # ``AsyncResult`` (NOT a coroutine). ``AsyncResult.__await__``
            # is event-driven (``loop.create_future`` +
            # ``add_callback`` resolved by the peer's reply via
            # ``on_readable``), so awaiting it does not block the
            # loop, does not poll, and does not re-enter the
            # deadlock. This path REPLACES what Case 2 would have
            # done if ``iscoroutinefunction(netref)`` had been safe.
            async_res = obj(*args, **kwargs_dict)
            return await async_res

        # Sync netref. Calling ``obj(*args)`` invokes the netref's
        # ``__call__`` which routes through ``syncreq(HANDLE_CALL, ...)``
        # and returns a concrete value (or a coroutine object from
        # the peer, if the peer happens to have handed us an
        # async-producing sync function — rare, but preserved for
        # backward compatibility).
        #
        # We deliberately do NOT run ``inspect.iscoroutinefunction``
        # here for the same deadlock reason documented above. The
        # syncreq path is already blocking-but-not-deadlocking on
        # localhost (the peer's handler dispatches promptly on its
        # own loop) — we don't need the probe to pick an async path,
        # because if the result is a coroutine we'll ``await`` it
        # explicitly below.
        result = obj(*args, **kwargs_dict)
        if inspect.iscoroutine(result):
            result = await result
        return result

    # ═══════════════════════════════════════════════════════════════════
    # Non-netref path — safe to run ``inspect.iscoroutinefunction``
    # because we're calling it on a local Python object (real bound
    # method, real function, real callable) where attribute access is
    # synchronous and local — no RPC, no deadlock risk.
    # ═══════════════════════════════════════════════════════════════════
    try:
        is_coro_func = inspect.iscoroutinefunction(obj)
    except (AttributeError, TypeError):
        # Rare: some callable whose ``__code__`` isn't accessible in
        # the usual way (e.g. certain C-implemented callables,
        # ``functools.partial`` wrapping non-standard callables).
        # Safe fallback: treat as sync; if it turns out to return a
        # coroutine we ``await`` it below.
        is_coro_func = False

    if is_coro_func:
        # Case 2: real async function — call produces a coroutine
        # object; await it directly.
        coro = obj(*args, **kwargs_dict)
        result = await coro
        return result

    # Case 3: real sync function. Covers plain functions and the
    # sync-callback-passed-to-async-exposed-method pattern. If the
    # sync function happens to return a coroutine (uncommon but
    # legal), await it.
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
