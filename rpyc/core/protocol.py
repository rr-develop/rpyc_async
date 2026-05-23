"""The RPyC protocol
"""
import sys
import itertools
import socket
import time  # noqa: F401
import gc  # noqa: F401

import collections
import concurrent.futures as c_futures
import os
import threading
import asyncio
import weakref
from queue import Queue
from typing import Optional, Tuple, Any, Dict

from threading import Lock, Condition, RLock
from rpyc.lib import spawn, Timeout, get_methods, get_id_pack, hasattr_static
from rpyc.lib.compat import pickle, next, maxint, select_error, acquire_lock  # noqa: F401
from rpyc.lib.colls import WeakValueDict, RefCountingColl
from rpyc.core import consts, brine, vinegar, netref
from rpyc.core.async_ import AsyncResult


class PingError(Exception):
    """The exception raised should :func:`Connection.ping` fail"""
    pass


UNBOUND_THREAD_ID = 0  # Used when the message is being sent but the thread is not bound yet.
DEFAULT_CONFIG = dict(
    # ATTRIBUTES
    allow_safe_attrs=True,
    allow_exposed_attrs=True,
    # allow_public_attrs: Allow access to public attributes (not starting with '_').
    # This is enabled by default to allow natural usage of regular Python objects
    # passed as netref arguments. Service classes can still use 'exposed_' prefix
    # to explicitly mark their API methods, but regular objects work as expected.
    # Changed from False to True to enable intuitive netref behavior.
    allow_public_attrs=True,
    allow_all_attrs=False,
    safe_attrs=set(['__abs__', '__add__', '__and__', '__bool__', '__cmp__', '__contains__',
                    '__delitem__', '__delslice__', '__div__', '__divmod__', '__doc__',
                    '__eq__', '__float__', '__floordiv__', '__ge__', '__getitem__',
                    '__getslice__', '__gt__', '__hash__', '__hex__', '__iadd__', '__iand__',
                    '__idiv__', '__ifloordiv__', '__ilshift__', '__imod__', '__imul__',
                    '__index__', '__int__', '__invert__', '__ior__', '__ipow__', '__irshift__',
                    '__isub__', '__iter__', '__itruediv__', '__ixor__', '__le__', '__len__',
                    '__long__', '__lshift__', '__lt__', '__mod__', '__mul__', '__ne__',
                    '__neg__', '__new__', '__nonzero__', '__oct__', '__or__', '__pos__',
                    '__pow__', '__radd__', '__rand__', '__rdiv__', '__rdivmod__', '__repr__',
                    '__rfloordiv__', '__rlshift__', '__rmod__', '__rmul__', '__ror__',
                    '__rpow__', '__rrshift__', '__rshift__', '__rsub__', '__rtruediv__',
                    '__rxor__', '__setitem__', '__setslice__', '__str__', '__sub__',
                    '__truediv__', '__xor__', 'next', '__length_hint__', '__enter__',
                    '__exit__', '__next__', '__format__']),
    exposed_prefix="exposed_",
    allow_getattr=True,
    allow_setattr=False,
    allow_delattr=False,
    # EXCEPTIONS
    include_local_traceback=True,
    include_local_version=True,
    instantiate_custom_exceptions=False,
    import_custom_exceptions=False,
    instantiate_oldstyle_exceptions=False,  # which don't derive from Exception
    propagate_SystemExit_locally=False,  # whether to propagate SystemExit locally or to the other party
    propagate_KeyboardInterrupt_locally=True,  # whether to propagate KeyboardInterrupt locally or to the other party
    log_exceptions=True,
    # MISC
    allow_pickle=False,
    connid=None,
    credentials=None,
    endpoints=None,
    logger=None,
    sync_request_timeout=30,
    before_closed=None,
    close_catchall=False,
    bind_threads=os.environ.get('RPYC_BIND_THREADS') == 'true',
    # NETREF LIFECYCLE (v5.2)
    cleanup_interval=2.0,  # Background cleanup runs every N seconds
    cleanup_ack_timeout=5.0,  # Timeout for HANDLE_DEL acknowledgment
    debug_refcounting=False,  # Enable debug logging for refcount operations
    # NETREF REFCOUNT RACE FIX (v5.3)
    # See docs/DESIGN_REFCOUNT_RACE_FIX.md (variant D) and
    # docs/DESIGN_REFCOUNT_RACE_FIX_A.md (variant A — the real fix).
    #
    # Variant A (stable monotonic id_pack[2]) closes the id() reuse race
    # at its source. The debounce (variant D) is retained as an opt-in
    # throttle for pathological HANDLE_DEL fan-out, but the default is
    # 0.0 — fire immediately — because there is no correctness reason to
    # delay cleanup any more.
    #
    # Implementation: _signal_deletion_available schedules the "work
    # available" event via `loop.call_later(cleanup_debounce, event.set)`
    # — a ONE-SHOT timer, not a polling loop. A positive value will
    # coalesce deletion bursts into a single cleanup wake-up; users on
    # churn-heavy workloads can tune it up.
    cleanup_debounce=0.0,
    # Per-Connection cap on simultaneously-inflight inbound dispatch
    # tasks. Once exceeded, the Connection enters terminal quarantine:
    # further MSG_REQUEST silently dropped, parked dispatch tasks
    # cancelled, outbound ``_request_callbacks`` cleared, one ERROR
    # logged. 0 disables the cap (legacy behaviour). See
    # docs/DESIGN_INBOUND_BACKPRESSURE.md.
    max_inbound_inflight=10_000,
    # Optional callback invoked once, when a Connection first enters inbound
    # quarantine (crosses ``max_inbound_inflight``). Signature:
    # ``on_inbound_quarantine(info: dict) -> None`` where info has keys
    # ``connid``, ``peer``, ``inbound_inflight``, ``threshold``,
    # ``request_callbacks``. Lets the host app surface a warning (log/UI)
    # WITHOUT changing the connection's behavior. Default None (no-op).
    # Must never raise — exceptions are swallowed so the dispatch path is
    # never broken. See docs/DESIGN_INBOUND_BACKPRESSURE.md.
    on_inbound_quarantine=None,
)
"""
The default configuration dictionary of the protocol. You can override these parameters
by passing a different configuration dict to the :class:`Connection` class.

.. note::
   You only need to override the parameters you want to change. There's no need
   to repeat parameters whose values remain unchanged.

=======================================  ================  =====================================================
Parameter                                Default value     Description
=======================================  ================  =====================================================
``allow_safe_attrs``                     ``True``          Whether to allow the use of *safe* attributes
                                                           (only those listed as ``safe_attrs``)
``allow_exposed_attrs``                  ``True``          Whether to allow exposed attributes
                                                           (attributes that start with the ``exposed_prefix``)
``allow_public_attrs``                   ``False``         Whether to allow public attributes
                                                           (attributes that don't start with ``_``)
``allow_all_attrs``                      ``False``         Whether to allow all attributes (including private)
``safe_attrs``                           ``set([...])``    The set of attributes considered safe
``exposed_prefix``                       ``"exposed_"``    The prefix of exposed attributes
``allow_getattr``                        ``True``          Whether to allow getting of attributes (``getattr``)
``allow_setattr``                        ``False``         Whether to allow setting of attributes (``setattr``)
``allow_delattr``                        ``False``         Whether to allow deletion of attributes (``delattr``)
``allow_pickle``                         ``False``         Whether to allow the use of ``pickle``

``include_local_traceback``              ``True``          Whether to include the local traceback
                                                           in the remote exception
``instantiate_custom_exceptions``        ``False``         Whether to allow instantiation of
                                                           custom exceptions (not the built in ones)
``import_custom_exceptions``             ``False``         Whether to allow importing of
                                                           exceptions from not-yet-imported modules
``instantiate_oldstyle_exceptions``      ``False``         Whether to allow instantiation of exceptions
                                                           which don't derive from ``Exception``. This
                                                           is not applicable for Python 3 and later.
``propagate_SystemExit_locally``         ``False``         Whether to propagate ``SystemExit``
                                                           locally (kill the server) or to the other
                                                           party (kill the client)
``propagate_KeyboardInterrupt_locally``  ``False``         Whether to propagate ``KeyboardInterrupt``
                                                           locally (kill the server) or to the other
                                                           party (kill the client)
``logger``                               ``None``          The logger instance to use to log exceptions
                                                           (before they are sent to the other party)
                                                           and other events. If ``None``, no logging takes place.

``connid``                               ``None``          **Runtime**: the RPyC connection ID (used
                                                           mainly for debugging purposes)
``credentials``                          ``None``          **Runtime**: the credentials object that was returned
                                                           by the server's :ref:`authenticator <api-authenticators>`
                                                           or ``None``
``endpoints``                            ``None``          **Runtime**: The connection's endpoints. This is a tuple
                                                           made of the local socket endpoint (``getsockname``) and the
                                                           remote one (``getpeername``). This is set by the server
                                                           upon accepting a connection; client side connections
                                                           do no have this configuration option set.

``sync_request_timeout``                 ``30``            Default timeout for waiting results
``bind_threads``                         ``False``         Whether to restrict request/reply by thread (experimental).
                                                           The default value is False. Setting the environment variable
                                                           `RPYC_BIND_THREADS` to `"true"` will enable this feature.
=======================================  ================  =====================================================
"""


_connection_id_generator = itertools.count(1)


# ─── strong-ref pin for inbound dispatch tasks ─────────────────────────────
#
# WHY THIS SET EXISTS — DO NOT REMOVE IT.
#
# ``_dispatch`` schedules each inbound MSG_REQUEST via
# ``asyncio.run_coroutine_threadsafe(self._dispatch_request_async(...),
# self._asyncio_loop)`` (look for the call site below). The returned
# ``concurrent.futures.Future`` is intentionally not awaited — we want
# the dispatch to be fire-and-forget so the channel reader can keep
# pulling the next frame off the wire without blocking.
#
# Problem: asyncio holds only WEAK references to running tasks.
# ``asyncio._all_tasks`` is a ``WeakSet`` — explicitly documented at
# https://docs.python.org/3/library/asyncio-task.html#asyncio.Task with
# the warning "Save a reference to the result of this function, to avoid
# a task disappearing mid-execution". If nobody holds a strong reference
# to the Task that ``run_coroutine_threadsafe`` schedules, a GC cycle
# can collect that Task while it is still parked on an inner ``await``.
#
# Production failure mode (observed in a downstream application, 12.1 GB
# RSS in 3 days):
#
#   1. Inbound MSG_REQUEST arrives. ``_dispatch`` schedules
#      ``_dispatch_request_async(seq, args)`` via
#      ``run_coroutine_threadsafe`` and throws the future away.
#   2. The handler the dispatch is calling is itself an async function
#      that issues an outbound RPC on the same connection (this is the
#      whole point of bidirectional async — handler can call peer back
#      mid-request). The handler awaits an ``AsyncResult`` returned by
#      ``conn.root.method()``. That AsyncResult registers in
#      ``self._request_callbacks[outbound_seq]`` and stays there while
#      the handler waits.
#   3. CPython's GC runs (it always does, sooner or later). The inbound
#      dispatch Task is unreachable in strong-ref terms — only the
#      WeakSet entry survives. GC collects it. Its coroutine frame is
#      destroyed mid-await.
#   4. The outbound AsyncResult is now ORPHAN: nothing is awaiting its
#      future. ``Connection._seq_request_callback`` (which would have
#      popped the slot on reply) cannot resolve it because no reply
#      will arrive (the peer is waiting for the inbound request that
#      just died). The earlier cancel-aware fix in
#      ``AsyncResult.__await__`` cannot help because the future never
#      reaches a done state (nobody calls set_result / set_exception /
#      cancel on it). The AR stays pinned via a cycle that is held
#      alive externally by the half-collected dispatch task's
#      bookkeeping inside asyncio.
#   5. Steady-state bidirectional traffic accumulates ~1 AR chain per
#      lost dispatch task. The affected production process had
#      1 941 735 leaked chains pinning ~10.7 GB of pymalloc heap.
#
# The fix:
#
#   * Every Task that ``_dispatch`` creates is added to
#     ``_DISPATCH_INFLIGHT`` BEFORE control returns to the channel
#     reader.
#   * A single done-callback removes the Task from the set the moment
#     it finishes (success / handler-exception / cancellation —
#     Task.done() is True for all three). After that the set no longer
#     holds the Task; Python's GC is then free to collect it normally.
#
# This guarantees the Task lives until its coroutine completes, which
# in turn guarantees the surrounding outbound-AsyncResult chain runs
# its own cleanup (``add_done_callback`` from an earlier fix, or
# ``__del__`` from a later fix). The auto-discard makes sure
# the set never grows beyond the natural working set of in-flight
# inbound RPCs — no new unbounded-set failure mode is introduced.
#
# Companion fixes — the full leak-prevention picture, in chronological
# order:
#
#   * ``AsyncResult.__await__`` cancel-aware cleanup
#     (commit c40fb00) — pops the seq when the *future* finishes.
#     Catches the timeout / cancel case.
#   * ``rpyc.utils.helpers._INFLIGHT`` (commit 079e80e) —
#     same shape as this fix but for ``fire_and_forget_async`` (the
#     outbound side). Catches the GC-of-pending-task case for any
#     user-issued outbound RPC.
#   * ``AsyncResult.__del__`` (same commit) — defence-in-
#     depth that pops the seq when the AR itself is collected.
#   * ``_DISPATCH_INFLIGHT`` (this set) — last known
#     instance of the discarded-Task pattern. Catches the
#     GC-of-pending-dispatch-task case on the inbound side. With this
#     in place, every Task that participates in an RPyC RPC chain
#     (inbound or outbound) is strong-ref-pinned for its lifetime.
#
# DO NOT remove ``_DISPATCH_INFLIGHT`` and DO NOT remove the
# ``add_done_callback(_DISPATCH_INFLIGHT.discard)`` line at the
# scheduling site below. The two together are the entire fix; either
# one alone is broken (no set → leak; set without auto-discard →
# unbounded set growth).
#
# Regression tests:
#   * tests/test_dispatch_strong_ref.py — asserts the set exists,
#     a parked dispatch task is in it, a finished dispatch task is
#     not.
#   * tests/test_dispatch_task_leak_on_disconnect.py — the
#     earlier regression test that the same bug had been
#     diagnosed at originally; this fix is what makes it green.
#
# Investigation report: a related internal incident analysis
# (not included here).
_DISPATCH_INFLIGHT: "set[asyncio.Task]" = set()


# ─── strong-ref pin for the per-Connection cleanup_loop task ───────────────
#
# WHY THIS SET EXISTS — DO NOT REMOVE IT.
#
# Each ``Connection._start_cleanup_task()`` schedules a long-lived
# ``cleanup_loop()`` coroutine via ``loop.create_task(...)``. The
# resulting Task is stored on ``self._cleanup_task`` for ``close()``-time
# cancellation. The coroutine itself closes over ``self`` (it touches
# ``self._deletion_available``, ``self._cleanup_running``,
# ``self.closed``, ``self._process_pending_deletions``).
#
# That makes a strong-reference CYCLE:
#
#   Connection
#     → _cleanup_task         (strong; stored on the Connection)
#     → asyncio.Task
#     → coroutine             (the cleanup_loop() generator)
#     → coroutine.cr_frame.f_locals['self']
#     → Connection            (back to the start)
#
# Python's cycle collector breaks cycles as soon as NO EXTERNAL
# strong reference holds either end. ``asyncio._all_tasks`` is a
# ``WeakSet`` and does NOT provide one. So when the last
# application-level reference to the Connection goes away — for
# example when a downstream application's connection registry
# evicts a torn-down connection through the ``is_connected``
# liveness probe — the entire cycle becomes
# collectible. The cleanup_loop Task is destroyed while still
# parked on ``await self._deletion_available.wait()``. asyncio
# emits ``Task was destroyed but it is pending!`` and any
# HANDLE_DEL entries still queued in ``self._pending_deletions``
# are silently dropped → remote netrefs LEAK on the peer.
#
# Production failure (observed in a downstream application): the log
# captured the canonical signature:
#
#   asyncio - ERROR - Task was destroyed but it is pending!
#   task: <Task pending name='Task-3309'
#          coro=<Connection._start_cleanup_task.<locals>.cleanup_loop()
#                  running at rpyc/core/protocol.py:939>
#          wait_for=<Future pending cb=[Task.task_wakeup()]>>
#   WARNING: Failed to delete remote object
#            (a remote service netref). Possible memory leak on
#            remote side.
#
# The fix has TWO halves and either half alone is broken:
#
#   1. ``_CLEANUP_LOOPS`` (this set) holds a strong reference to
#      every cleanup_loop Task at module scope. That reference is
#      independent of the Connection, so GC of the Connection
#      does not destroy the Task. ``Task.add_done_callback(
#      _CLEANUP_LOOPS.discard)`` releases the strong-ref once the
#      Task is genuinely done, so the set never grows beyond the
#      live-Connection working set.
#
#   2. The cleanup_loop coroutine must NOT close over
#      ``self`` directly — that would re-introduce a strong-ref
#      cycle on the OTHER side (Task → coroutine → self), making
#      the Connection live forever (because _CLEANUP_LOOPS keeps
#      the Task alive). Instead the coroutine takes a
#      ``weakref.ref(self)`` and dereferences on each iteration.
#      When the Connection is collected, the weakref returns
#      ``None`` and the loop drains any final deletions then
#      exits cleanly.
#
# Connection's GC is wired to signal the loop via
# ``weakref.finalize`` — the finalize callback sets
# ``_deletion_available`` so the loop's ``event.wait()`` unblocks
# and the ``finally:`` drain runs.
#
# Regression tests: ``tests/test_cleanup_loop_pin.py``.
# DO NOT remove ``_CLEANUP_LOOPS`` and DO NOT change cleanup_loop
# to close over ``self`` directly — see a related internal incident
# analysis (not included here).
_CLEANUP_LOOPS: "set[asyncio.Task]" = set()


class Connection(object):
    """The RPyC *connection* (AKA *protocol*).

    Objects referenced over the connection are either local or remote. This class retains a strong reference to
    local objects that is deleted when the reference count is zero. Remote/proxied objects have a life-cycle
    controlled by a different address space. Since garbage collection is handled on the remote end, a weak reference
    is used for netrefs.

    :param root: the :class:`~rpyc.core.service.Service` object to expose
    :param channel: the :class:`~rpyc.core.channel.Channel` over which messages are passed
    :param config: the connection's configuration dict (overriding parameters
                   from the :data:`default configuration <DEFAULT_CONFIG>`)
    """

    def __init__(self, root, channel, config={}):
        self._closed = True
        self._config = DEFAULT_CONFIG.copy()
        self._config.update(config)
        if self._config["connid"] is None:
            self._config["connid"] = f"conn{next(_connection_id_generator)}"

        self._HANDLERS = self._request_handlers()
        self._channel = channel
        self._seqcounter = itertools.count()
        self._recvlock = RLock()  # AsyncResult implementation means that synchronous requests have multiple acquires
        self._sendlock = Lock()
        self._recv_event = Condition()  # TODO: why not simply timeout? why not associate w/ recvlock? explain/redesign
        self._request_callbacks = {}
        # ─── Per-connection inbound dispatch backpressure ─────────
        # See docs/DESIGN_INBOUND_BACKPRESSURE.md.
        # ``_inbound_inflight`` is incremented in the ``_schedule``
        # closure inside ``_dispatch`` (exactly when a dispatch Task
        # is added to ``_DISPATCH_INFLIGHT`` for this Connection)
        # and decremented in the matching ``add_done_callback``.
        # Once ``_inbound_inflight`` first reaches
        # ``self._config["max_inbound_inflight"]`` (default 10_000;
        # 0 disables), ``_inbound_quarantined`` flips to True and
        # stays there for the rest of the Connection's life —
        # subsequent inbound MSG_REQUEST is silently dropped. The
        # one-shot quarantine log is gated by
        # ``_inbound_quarantine_logged`` to keep the log clean.
        self._inbound_inflight: int = 0
        self._inbound_quarantined: bool = False
        self._inbound_quarantine_logged: bool = False
        # Initialize _local_objects with debug refcounting if enabled
        debug_refcount = self._config.get("debug_refcounting", False)
        logger = self._config.get("logger")
        self._local_objects = RefCountingColl(logger=logger, debug=debug_refcount)

        # ═══════════════════════════════════════════════════════════════
        # REFCOUNT RACE FIX — variant A (full)
        # See docs/DESIGN_REFCOUNT_RACE_FIX_A.md.
        #
        # Instead of id_pack[2] = id(obj) (which CPython recycles after
        # GC), each _box of a Python instance gets a stable
        # connection-local monotonic sequence number. The mapping lives
        # in two data structures:
        #
        #  * _obj_to_seq_weak — WeakKeyDictionary for weakref-able
        #    objects. Python GC evicts entries automatically.
        #  * _obj_to_seq_by_id — id()-keyed fallback for un-weakref-able
        #    types (bound methods, some built-ins). Safe because
        #    _local_objects holds a strong ref while the seq is
        #    meaningful; see design doc §3 "Why the id-fallback is safe".
        # ═══════════════════════════════════════════════════════════════
        # PID-namespaced seq — see docs/DESIGN_PID_NAMESPACED_ID_PACK.md.
        #
        # The old seed ``1 << 40`` was identical on every connection in
        # every process. Two independent peers minted bit-identical
        # ``id_pack`` tuples for their Nth boxed builtin-typed object
        # (the receive-side shortcut in ``_unbox(LABEL_REMOTE_REF)``
        # then resolved peer id_packs to the receiver's own local,
        # producing infinite ping-pong callbacks — the 10 GB/6 min leak
        # reported in a downstream application).
        #
        # Starting the seq at ``(pid << 32) + 1`` makes two LIVE peers'
        # seq ranges disjoint by kernel guarantee: the kernel never
        # assigns the same PID to two live processes simultaneously,
        # therefore their id_pack triples are globally unique while they
        # both run.  Each process gets a private range of 2**32 seqs,
        # plenty for any realistic connection lifetime.
        self._id_pack_seq = itertools.count((os.getpid() << 32) + 1)
        self._obj_to_seq_weak: "weakref.WeakKeyDictionary[Any, int]" = (
            weakref.WeakKeyDictionary()
        )
        self._obj_to_seq_by_id: Dict[int, Tuple[Any, int]] = {}

        # ═══════════════════════════════════════════════════════════════
        # CRITICAL: DO NOT REMOVE THIS DIAGNOSTIC MESSAGE!
        # ═══════════════════════════════════════════════════════════════
        # This INFO message serves multiple purposes:
        # 1. Confirms that refcount error monitoring is active
        # 2. Helps diagnose "logging not working" issues
        # 3. Tracks connection lifecycle in logs
        #
        # Users NEED to see this to verify that error logging works.
        # Without this, they can't distinguish between:
        # - "No errors" (good)
        # - "Errors silently suppressed" (bad)
        #
        # This is NOT spam. Each message = one connection established.
        # If you see too many, reduce connection churn, don't hide the message.
        # ═══════════════════════════════════════════════════════════════
        import sys
        connid = self._config.get("connid", "unknown")
        print(
            f"INFO: RPyC Connection {connid} initialized. "
            f"Refcount error monitoring: ENABLED (errors always logged to stderr)",
            file=sys.stderr
        )
        # ``_last_traceback`` is kept as an attribute for binary-
        # compatibility with ``rpyc.utils.classic.pdb_post_mortem``,
        # which reads it across an RPC. PRODUCTION CODE MUST NEVER
        # WRITE TO IT. Storing a live TracebackType here pins every
        # ``tb_frame``'s ``f_locals`` for the lifetime of the
        # Connection, which on a busy bidirectional-async
        # deployment cascades into a multi-GB AsyncResult retention
        # leak — see a related internal incident analysis (not
        # included here)
        # for the production failure and the regression tests in
        # ``tests/test_traceback_no_retention.py`` that guard this
        # invariant. If you find yourself reaching for
        # ``self._last_traceback = tb``: capture the text via
        # ``traceback.format_exception(...)``, store the string,
        # and drop the live ``tb`` immediately.
        self._last_traceback = None
        self._proxy_cache = WeakValueDict()
        self._netref_classes_cache = {}
        self._remote_root = None
        self._send_queue = []
        self._local_root = root
        self._closed = False
        # Settings for bind_threads
        self._bind_threads = self._config['bind_threads']
        self._threads = None
        if self._bind_threads:
            self._lock = threading.Lock()
            self._threads = {}
            self._receiving = False
            self._thread_pool = []
            self._thread_pool_executor = c_futures.ThreadPoolExecutor()
        # ═══════════════════════════════════════════════════════════════
        # Asyncio Support (v5.1)
        # ═══════════════════════════════════════════════════════════════
        self._asyncio_loop = None           # Event loop reference
        self._asyncio_enabled = False       # Async serving enabled?
        self._loop_fd_registered = False    # FD registered in loop?
        self._registered_fd = None          # Saved FD for cleanup
        # ═══════════════════════════════════════════════════════════════
        # Event-driven close notification (v5.3 - NO POLLING POLICY)
        # ═══════════════════════════════════════════════════════════════
        # STRICT POLICY: Callers MUST NOT poll `self.closed` via
        # `while not conn.closed: await asyncio.sleep(...)`. That pattern
        # burns CPU (10 wakeups/sec per connection at sleep(0.1); ~33% CPU
        # measured with two connections) and silently breaks when .closed
        # is never flipped. Use `await conn.wait_closed()` or register a
        # callback via `conn.add_close_callback(cb)` instead.
        self._close_callbacks: list = []    # Fired once, in close order
        self._close_lock = threading.Lock() # Protects callbacks list
        self._close_waiters: list = []      # list[(loop, future)] — async waiters
        # ═══════════════════════════════════════════════════════════════
        # Netref Lifecycle Management (v5.2)
        # ═══════════════════════════════════════════════════════════════
        # Queue for pending netref deletions: (id_pack, refcount) tuples
        self._pending_deletions: Queue[Tuple[Tuple[str, int, int], int]] = Queue()
        # Background cleanup task
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_running: bool = False
        # Signal fired when a deletion is enqueued — replaces the old
        # asyncio.sleep(cleanup_interval) polling loop. The cleanup task
        # waits on this event (with a bounded timeout as a safety fallback,
        # not as the primary wake source) and wakes only when there is
        # actual work to do OR when the connection is closing.
        self._deletion_available: Optional[asyncio.Event] = None
        # Cleanup configuration
        self._cleanup_interval: float = self._config.get("cleanup_interval", 2.0)
        self._cleanup_ack_timeout: float = self._config.get("cleanup_ack_timeout", 5.0)
        # Debounce window for deletion-available signal. A tight GC burst
        # on the peer coalesces into one cleanup wake-up over this window,
        # avoiding HANDLE_DEL fan-out racing against in-flight RPC replies.
        # See docs/DESIGN_REFCOUNT_RACE_FIX.md (variant D).
        self._cleanup_debounce: float = self._config.get("cleanup_debounce", 0.050)
        # Pending-flag for the debounce: True means a call_later is already
        # armed and further _signal_deletion_available calls must coalesce
        # with it instead of arming another timer.
        self._signal_pending: bool = False

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self.close()

    def __repr__(self):
        a, b = object.__repr__(self).split(" object ")
        return f"{a} {self._config['connid']!r} object {b}"

    # ─────────────────────────────────────────────────────────────
    # Inbound dispatch backpressure (quarantine path).
    # See docs/DESIGN_INBOUND_BACKPRESSURE.md.
    # ─────────────────────────────────────────────────────────────

    def _drain_inbound_dispatch(self) -> None:
        """Cancel every inbound dispatch task belonging to THIS
        Connection.

        Shared by ``_cleanup`` (close path) and
        ``_enter_inbound_quarantine`` (overload path). Each
        dispatch coroutine's frame has ``self`` bound to its
        owning Connection, so we walk ``_DISPATCH_INFLIGHT`` and
        cancel only matches. We snapshot the set via ``list(...)``
        because ``cancel()`` triggers a done-callback that mutates
        the set.

        Never raises — Task introspection errors are swallowed; the
        worst case is one missed cancel, which the strong-ref pin
        still bounds (the Task can still finish on its own).
        """
        for _task in list(_DISPATCH_INFLIGHT):
            try:
                _coro = _task.get_coro()
                _frame = getattr(_coro, "cr_frame", None)
                if _frame is not None and \
                   _frame.f_locals.get("self") is self and \
                   not _task.done():
                    _task.cancel()
            except Exception:
                pass

    def _channel_peer_for_log(self) -> str:
        """Best-effort peer-addr lookup for the quarantine log line.

        Returns a printable string. Never raises. Returns
        ``"<unknown>"`` when the underlying stream / socket is not
        exposed by the channel implementation.
        """
        try:
            stream = getattr(self._channel, "stream", None)
            sock = getattr(stream, "sock", None) if stream else None
            if sock is not None:
                try:
                    return repr(sock.getpeername())
                except OSError:
                    return "<getpeername failed>"
            endpoints = self._config.get("endpoints")
            if endpoints:
                return repr(endpoints[1])
        except Exception:
            pass
        return "<unknown>"

    def _enter_inbound_quarantine(self) -> None:
        """Transition this Connection to terminal inbound-quarantine.

        Called exactly once per Connection, at the moment
        ``self._inbound_inflight`` first crosses
        ``max_inbound_inflight``. From this point onward
        ``_dispatch`` silently drops MSG_REQUEST on this channel.
        Outbound AsyncResults waiting for the peer's reply are
        cleared — by the time we accept the peer has stopped
        making progress, no future reply is going to reconcile
        them.

        The Connection is NOT closed. We keep the channel open and
        drain inbound bytes from the kernel buffer to /dev/null so
        the peer doesn't get TCP-level backpressure that might
        make its bug even louder (the broken client
        would just log harder if we closed). open-and-ignore is
        the cheapest stable state.

        Idempotent: subsequent invocations are no-ops via the
        quarantined flag check at the top.
        """
        if self._inbound_quarantined:
            return
        self._inbound_quarantined = True

        inflight_snapshot = self._inbound_inflight
        rcb_snapshot = len(self._request_callbacks)
        peer_repr = self._channel_peer_for_log()

        logger = self._config.get("logger")
        if logger is not None and not self._inbound_quarantine_logged:
            self._inbound_quarantine_logged = True
            try:
                logger.error(
                    "rpyc inbound quarantine: connid=%s peer=%s "
                    "inbound_inflight=%d threshold=%d "
                    "request_callbacks=%d. Channel kept open; further "
                    "MSG_REQUEST on this channel are silently dropped. "
                    "Cancelling parked dispatch tasks and clearing "
                    "outbound _request_callbacks. See "
                    "docs/DESIGN_INBOUND_BACKPRESSURE.md.",
                    self._config.get("connid"),
                    peer_repr,
                    inflight_snapshot,
                    self._config.get("max_inbound_inflight", 0),
                    rcb_snapshot,
                )
            except Exception:
                # Logging must never break the dispatch path.
                pass

        # Optional host-app notification hook (e.g. surface a warning in a
        # web UI). Fire-and-forget: must never break the dispatch path, so
        # all exceptions are swallowed. The connection's behavior is
        # unchanged whether or not a callback is configured.
        on_quarantine = self._config.get("on_inbound_quarantine")
        if on_quarantine is not None:
            try:
                on_quarantine({
                    "connid": self._config.get("connid"),
                    "peer": peer_repr,
                    "inbound_inflight": inflight_snapshot,
                    "threshold": self._config.get("max_inbound_inflight", 0),
                    "request_callbacks": rcb_snapshot,
                })
            except Exception:
                pass

        self._drain_inbound_dispatch()
        self._request_callbacks.clear()

    def _cleanup(self, _anyway=True):  # IO
        if self._closed and not _anyway:
            return
        # Idempotency: _cleanup may be entered twice on the async close
        # path (once from on_readable's EOF-driven close(), once from
        # aclose()'s finally). After the first pass _local_root is None
        # and the rest of the teardown has already run — just mark closed
        # and return without repeating side-effects. The second path still
        # needs to fire close notifications (they are reset to [] on first
        # pass, so firing again is a no-op).
        if self._local_root is None:
            self._closed = True
            self._fire_close_notifications()
            return
        self._closed = True
        self._channel.close()
        self._local_root.on_disconnect(self)
        self._request_callbacks.clear()
        # Cancel every inbound dispatch task belonging to THIS
        # connection. The strong-ref pin in ``_DISPATCH_INFLIGHT``
        # (see module-level docstring) keeps these Tasks alive past
        # the channel teardown so they cannot be silently GC'd in
        # pending state — but on close we WANT them gone, since
        # there is no longer a peer to reply to them. ``cancel()``
        # propagates CancelledError into the parked handler, which
        # unwinds its await chain and lets the per-AsyncResult
        # cleanup (the ``__await__`` cancel-aware fix /
        # ``AsyncResult.__del__``) reclaim the chain.
        # Identification: each dispatch coroutine's frame has
        # ``self`` bound to THIS Connection. The actual per-conn
        # scan was extracted into ``_drain_inbound_dispatch`` so
        # the quarantine path (see
        # docs/DESIGN_INBOUND_BACKPRESSURE.md) can reuse the same
        # implementation.
        self._drain_inbound_dispatch()
        self._local_objects.clear()
        self._proxy_cache.clear()
        self._netref_classes_cache.clear()
        self._last_traceback = None
        self._remote_root = None
        self._local_root = None
        # self._seqcounter = None
        # self._config.clear()
        del self._HANDLERS
        if self._bind_threads:
            self._thread_pool_executor.shutdown(wait=True)  # TODO where?
        if _anyway:
            try:
                self._recvlock.release()
            except Exception:
                pass
            try:
                self._sendlock.release()
            except Exception:
                pass
        # Fire close notifications AFTER all internal state is torn down so
        # callbacks observe a fully-closed connection. See NO-POLLING policy
        # note in __init__ — these notifications are why callers no longer
        # need to poll `.closed`.
        self._fire_close_notifications()

    def close(self) -> None:  # IO
        """
        Closes the connection, releasing all held resources.

        NEW (v5.2): Processes any pending netref deletions before closing.
        This ensures deletions are sent even if background cleanup task
        wasn't running (e.g., non-asyncio connections).
        """
        if self._closed:
            return
        try:
            self._closed = True

            # NEW (v5.2 - Phase 4): Process pending deletions before close
            self._process_pending_deletions_sync()

            # NEW (v5.1): Cleanup asyncio resources first
            self.disable_asyncio_serving()
            if self._config.get("before_closed"):
                self._config["before_closed"](self.root)
            # TODO: define invariants/expectations around close sequence and timing
            self.sync_request(consts.HANDLE_CLOSE)
        except (EOFError, TimeoutError):
            pass
        except Exception:
            if not self._config["close_catchall"]:
                raise
        finally:
            self._cleanup(_anyway=True)

    @property
    def closed(self):  # IO
        """Indicates whether the connection has been closed or not"""
        return self._closed

    # ═══════════════════════════════════════════════════════════════
    # Event-driven close notification (v5.3 - NO POLLING POLICY)
    # ═══════════════════════════════════════════════════════════════
    # STRICT POLICY — DO NOT POLL `.closed`.
    #
    # Anywhere an async caller wants to block until the connection closes,
    # use one of:
    #   * `await conn.wait_closed()`                 — coroutine
    #   * `conn.add_close_callback(cb)`              — sync callback
    #
    # NEVER write `while not conn.closed: await asyncio.sleep(x)`. That is
    # polling, burns CPU (measured: +30% CPU for 2 connections at sleep(0.1)),
    # and is brittle when `.closed` is not flipped promptly by the peer.

    def add_close_callback(self, callback) -> None:
        """Register a one-shot callback fired exactly once when the connection
        closes.

        Thread-safe. If the connection is already closed at registration time,
        the callback is invoked immediately (on the calling thread).

        The callback takes no arguments. It must not raise; any exception is
        swallowed and logged.

        Use this instead of polling ``conn.closed`` from sync code.
        """
        with self._close_lock:
            if not self._closed:
                self._close_callbacks.append(callback)
                return
        # Already closed — fire synchronously outside the lock.
        try:
            callback()
        except Exception:
            logger = self._config.get("logger")
            if logger:
                logger.exception("close callback raised")

    async def wait_closed(self) -> None:
        """Block (in asyncio) until this connection is closed.

        Returns immediately if already closed. This is the event-driven
        replacement for ``while not conn.closed: await asyncio.sleep(...)``.
        Zero CPU while waiting — the coroutine is suspended on a Future that
        is resolved by ``close()``.
        """
        if self._closed:
            return
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        with self._close_lock:
            if self._closed:
                # Raced with close() — return immediately.
                return
            self._close_waiters.append((loop, fut))
        try:
            await fut
        finally:
            # Best-effort removal; close path also clears the list.
            with self._close_lock:
                try:
                    self._close_waiters.remove((loop, fut))
                except ValueError:
                    pass

    def _fire_close_notifications(self) -> None:
        """Invoke all registered close callbacks and resolve async waiters.

        Called from ``_cleanup()`` once. Safe to call from any thread.
        """
        with self._close_lock:
            callbacks = self._close_callbacks
            waiters = self._close_waiters
            self._close_callbacks = []
            self._close_waiters = []

        for cb in callbacks:
            try:
                cb()
            except Exception:
                logger = self._config.get("logger")
                if logger:
                    logger.exception("close callback raised")
                else:
                    import sys, traceback
                    traceback.print_exc(file=sys.stderr)

        for loop, fut in waiters:
            # Resolving the future must happen on its own loop.
            try:
                if loop.is_closed():
                    continue
                if loop.is_running():
                    loop.call_soon_threadsafe(
                        self._resolve_close_future, fut
                    )
                else:
                    # Loop is not running — set result directly; any awaiter
                    # will observe the result when the loop next runs.
                    self._resolve_close_future(fut)
            except Exception:
                logger = self._config.get("logger")
                if logger:
                    logger.exception("failed to resolve close waiter")

    @staticmethod
    def _resolve_close_future(fut) -> None:
        if not fut.done():
            try:
                fut.set_result(None)
            except asyncio.InvalidStateError:
                pass

    def fileno(self):  # IO
        """Returns the connectin's underlying file descriptor"""
        return self._channel.fileno()

    # ═══════════════════════════════════════════════════════════════
    # Asyncio Integration Methods (v5.1)
    # ═══════════════════════════════════════════════════════════════
    #
    # NO POLLING POLICY — STRICT BAN (applies to every asyncio code
    # path in this class: enable/disable_asyncio_serving, the cleanup
    # task started by _start_cleanup_task, wait_closed, and anything
    # else awaited inside the event loop).
    #
    # Forbidden shape:
    #     while <cond>:
    #         await asyncio.sleep(<x>)
    #
    # Why: at sleep(0.1) this burns ~10 wakeups/sec per connection; with
    # two connections observed CPU rose from 1.2% idle to 33%. It also
    # masks stale state — a condition that never flips = forever loop.
    #
    # Required primitives (pick the one that matches your event):
    #   * ``await conn.wait_closed()``       — wait for close
    #   * ``conn.add_close_callback(cb)``    — sync close notification
    #   * ``asyncio.Event().wait()``         — wait for work-available
    #   * ``loop.add_reader(fd, cb)``        — wake when FD is readable
    #   * ``loop.call_soon_threadsafe(...)`` — cross-thread signalling
    #
    # Enforced by ``tests/test_no_polling_policy.py``. Reviewers: reject
    # any PR that adds ``await asyncio.sleep(...)`` inside a loop here.
    # ═══════════════════════════════════════════════════════════════

    def enable_asyncio_serving(self, loop=None):
        """
        Enable asyncio-native serving for this connection.

        Registers the connection's file descriptor with the event loop
        for non-blocking, event-driven I/O. Must be called from within
        a running event loop.

        Args:
            loop: asyncio event loop to use (default: get_running_loop())

        Raises:
            RuntimeError: If no event loop is running

        Example:
            conn = rpyc.connect("localhost", 18861)
            conn.enable_asyncio_serving()  # Use current loop
            await conn.root.async_method()

        Note:
            This method is idempotent - calling multiple times is safe.
        """
        import asyncio

        if self._asyncio_enabled:
            return  # Already enabled

        # Get event loop
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                raise RuntimeError(
                    "enable_asyncio_serving() must be called from within "
                    "a running event loop, or pass loop explicitly"
                )

        self._asyncio_loop = loop
        self._asyncio_enabled = True

        # Register file descriptor for read events
        fd = self._channel.fileno()

        # CRITICAL: Remove any existing registration for this FD first
        # This handles the case where FD is reused before old connection cleanup
        try:
            loop.remove_reader(fd)
        except (ValueError, OSError):
            # FD not registered or already removed - this is fine
            pass

        def on_readable():
            """Called when socket has data to read."""
            # ── EOF / closed-stream guard (CRITICAL — DO NOT REMOVE) ──────
            # The ``while self._channel.poll(0)`` condition below calls
            # ``stream.poll() → stream.fileno()``, and on an ALREADY-CLOSED
            # stream ``fileno()`` raises ``EOFError`` ("stream has been
            # closed"). That exception is raised by the *while condition*,
            # i.e. OUTSIDE the inner try, so it used to escape ``on_readable``
            # entirely: asyncio logged "Exception in callback", ``self.close()``
            # was NEVER reached, the reader stayed armed on the loop, and the
            # loop immediately re-fired ``on_readable`` → instant re-raise → a
            # tight log-spamming livelock that wrote ~1.7 GB in minutes
            # (observed in a downstream application log storm).
            #
            # So the WHOLE poll/recv loop is wrapped: any EOFError — whether
            # from poll() (the condition) or recv() (the body) — closes the
            # connection. ``self.close()`` → ``disable_asyncio_serving`` →
            # ``loop.remove_reader``, so the reader comes OFF the loop and can
            # never re-fire. This is the only thing that stops the storm.
            try:
                # Read all available data (edge-triggered behavior)
                while self._channel.poll(0):
                    try:
                        data = self._channel.recv()
                        if self._config.get("logger"):
                            self._config["logger"].debug(f"[enable_asyncio_serving] Received data, dispatching...")
                        self._dispatch(data)
                        # Notify any threads waiting in serve()
                        with self._recv_event:
                            self._recv_event.notify_all()
                    except EOFError:
                        if self._config.get("logger"):
                            self._config["logger"].debug(f"[enable_asyncio_serving] EOF, closing connection")
                        self.close()
                        break
                    except Exception:
                        # Log and continue
                        if self._config.get("logger"):
                            self._config["logger"].exception(
                                "Error in async dispatch"
                            )
            except EOFError:
                # EOF from the poll() condition itself: the stream is closed.
                # Close once (idempotent) to remove the reader, then return —
                # NEVER let this escape, or the reader stays armed and storms.
                if self._config.get("logger"):
                    self._config["logger"].debug(
                        "[enable_asyncio_serving] EOF from poll(); closing connection"
                    )
                try:
                    self.close()
                except Exception:
                    # close() must not turn a benign EOF into an escaping
                    # exception (which would re-arm the storm).
                    pass

        loop.add_reader(fd, on_readable)
        self._loop_fd_registered = True
        self._registered_fd = fd  # Save FD for cleanup

        # ═══════════════════════════════════════════════════════════════
        # NEW (v5.2): Start background cleanup task
        # ═══════════════════════════════════════════════════════════════
        self._start_cleanup_task()

    def disable_asyncio_serving(self):
        """
        Disable asyncio-native serving.

        Removes FD from event loop and disables async dispatch.
        Safe to call multiple times.

        Example:
            conn.disable_asyncio_serving()
        """
        if not self._asyncio_enabled:
            return

        if self._loop_fd_registered and self._asyncio_loop and self._registered_fd is not None:
            # Use saved FD instead of fileno() which may fail if stream is closed
            try:
                self._asyncio_loop.remove_reader(self._registered_fd)
            except Exception:
                # Ignore errors - FD may already be removed or closed
                pass
            self._loop_fd_registered = False
            self._registered_fd = None

        self._asyncio_enabled = False
        self._asyncio_loop = None

    # ═══════════════════════════════════════════════════════════════
    # NEW (v5.2): Background Cleanup Task
    # ═══════════════════════════════════════════════════════════════

    def _signal_deletion_available(self) -> None:
        """Wake the background cleanup task when a new deletion is enqueued.

        Called from ``netref.__del__`` via ``_enqueue_deletion``. Safe to call
        from any thread.

        ═══════════════════════════════════════════════════════════════════
        NO POLLING POLICY — why this exists:
        The old cleanup loop polled every 2s via ``asyncio.sleep(interval)``.
        Even with nothing to clean up, it woke the loop 30 times/minute per
        connection. This signal replaces that timer: the cleanup loop sleeps
        on an ``asyncio.Event`` and wakes only when there IS work.

        DEBOUNCE (variant D in docs/DESIGN_REFCOUNT_RACE_FIX.md):
        Instead of firing the event immediately on every GC'd netref, we
        arm a ONE-SHOT ``loop.call_later(debounce, fire)`` the first time
        a burst starts. Subsequent calls within the window coalesce into
        that same scheduled fire. This gives in-flight RPC replies a
        chance to settle before the HANDLE_DEL fan-out starts, which
        avoids racing reused ``id()`` slots (see §2.1 of the design doc).

        This is NOT polling — it is one scheduled callback per burst.
        ═══════════════════════════════════════════════════════════════════
        """
        event = self._deletion_available
        loop = self._asyncio_loop
        if event is None or loop is None:
            return
        # Debounce guard: if a fire is already pending, coalesce.
        if self._signal_pending:
            return
        self._signal_pending = True

        def _fire():
            # Clear the pending-flag FIRST so that any signals arriving
            # after this point start a new debounce window; then wake the
            # cleanup task.
            self._signal_pending = False
            if self._deletion_available is not None:
                self._deletion_available.set()

        try:
            if loop.is_closed():
                self._signal_pending = False
                return
            debounce = self._cleanup_debounce
            if debounce <= 0:
                # Legacy / opt-out: fire immediately.
                loop.call_soon_threadsafe(_fire)
            else:
                # One-shot timer. Must be scheduled via call_soon_threadsafe
                # because call_later itself is not thread-safe.
                loop.call_soon_threadsafe(loop.call_later, debounce, _fire)
        except RuntimeError:
            # Loop is shutting down — cleanup will drain via
            # _process_pending_deletions_sync() in close().
            self._signal_pending = False

    def _enqueue_deletion(self, id_pack, refcount) -> None:
        """Queue a netref deletion and notify the background cleanup task.

        Thread-safe. Callers (netref ``__del__``) use this instead of touching
        ``_pending_deletions`` directly so the wake-up signal never gets lost.
        """
        self._pending_deletions.put((id_pack, refcount))
        self._signal_deletion_available()

    def _start_cleanup_task(self) -> None:
        """
        Start background cleanup task for netref garbage collection.

        ═══════════════════════════════════════════════════════════════════
        NO POLLING POLICY — STRICT BAN
        ═══════════════════════════════════════════════════════════════════
        This task MUST NOT wake on a timer. Previously it used
        ``await asyncio.sleep(self._cleanup_interval)`` between cycles,
        which wasted 30 wakeups/minute per connection even when idle.

        Correct design (implemented here):
          1. Block on ``self._deletion_available.wait()``  — event set by
             ``_enqueue_deletion`` when a netref is garbage-collected.
          2. Process the batch.
          3. Clear the event; loop.

        If you add a ``await asyncio.sleep(...)`` here as "just a small delay"
        you reintroduce the polling bug. DON'T. If you need debounce/coalesce,
        drain the queue after a short ``asyncio.sleep(0)`` that yields to the
        scheduler (not a fixed timer) — but note the current code already
        drains the entire queue per cycle via ``get_nowait``.
        ═══════════════════════════════════════════════════════════════════

        Note: Should only be called when asyncio serving is enabled.
        """
        if self._cleanup_running:
            return  # Already running

        if not self._asyncio_enabled or self._asyncio_loop is None:
            return  # Can't start without event loop

        self._cleanup_running = True

        # Create the wake-up event ON the event loop that will await it.
        # Creating from a different loop raises RuntimeError on .wait().
        self._deletion_available = asyncio.Event()

        # ── strong-ref cycle prevention — see _CLEANUP_LOOPS docstring ──
        # We intentionally DO NOT close over ``self`` inside
        # ``cleanup_loop``. Instead the coroutine takes a weakref and
        # dereferences it on every iteration. This way:
        #
        #   * _CLEANUP_LOOPS holds the Task (so the Task survives GC of
        #     the Connection), and
        #   * the Task does NOT hold the Connection (so the Connection
        #     stays collectible when application drops its last ref).
        #
        # Together they let GC reclaim the Connection without
        # destroying the cleanup_loop coroutine pending. The
        # ``weakref.finalize`` registered below signals the loop to
        # wake up the moment the Connection is collected, so its
        # ``finally:``-block drain runs and any final HANDLE_DELs go
        # out before the Task exits.
        self_wref = weakref.ref(self)
        # Local capture of the wake-up event — it lives on the
        # Connection, so we resolve it once here and keep a strong
        # ref for the lifetime of the loop. The Event by itself does
        # not own the Connection.
        event = self._deletion_available
        # Logger comes from config (a plain dict) so referencing it
        # by closure does NOT pin the Connection.
        logger = self._config.get("logger")

        async def cleanup_loop() -> None:
            """Main cleanup loop — event-driven, no polling.

            Wakes only when (a) a deletion is enqueued, or (b) the
            connection is closing, or (c) the Connection itself has
            been GC'd (signalled via the weakref.finalize hook below).
            Zero wakeups while idle.
            """
            try:
                while True:
                    conn = self_wref()
                    if conn is None:
                        break  # Connection collected; just run finally drain
                    if not conn._cleanup_running or conn.closed:
                        break
                    # Release the strong ref BEFORE we await; we only
                    # want to hold the Connection during the active
                    # tick, never across an await boundary.
                    conn = None
                    try:
                        # Event-driven wait. Zero CPU while nothing to do.
                        await event.wait()
                        event.clear()
                        conn = self_wref()
                        if conn is None or not conn._cleanup_running or conn.closed:
                            break
                        # Drain: a burst of deletions may have arrived;
                        # process them all in one batch.
                        await conn._process_pending_deletions()
                        conn = None
                    except asyncio.CancelledError:
                        # Task was cancelled — exit cleanly
                        break
                    except Exception as e:
                        # Log error but continue running. DO NOT add a sleep
                        # here as "backoff" on every error — re-arm by waiting
                        # for the next signal instead.
                        if logger:
                            logger.error(
                                f"Error in cleanup loop: {e}", exc_info=True
                            )
                        continue
            finally:
                # Final drain of anything queued after our last wake-up.
                # The Connection may already be gone; if so, there is
                # nothing to drain (its _pending_deletions queue went
                # with it) and we just exit.
                conn = self_wref()
                if conn is not None:
                    try:
                        await conn._process_pending_deletions()
                    except Exception:
                        pass

        # Create and start task in event loop.
        task = self._asyncio_loop.create_task(cleanup_loop())
        self._cleanup_task = task
        # Pin the Task in the module-level set so that GC of the
        # Connection cannot destroy it pending. See _CLEANUP_LOOPS
        # docstring for the full failure-mode chain.
        _CLEANUP_LOOPS.add(task)
        task.add_done_callback(_CLEANUP_LOOPS.discard)

        # Register a weakref.finalize callback on THIS Connection.
        # When the Connection is GC'd, the callback fires and sets
        # the wake-up event so the cleanup_loop's ``await
        # event.wait()`` unblocks. The loop then sees ``self_wref()
        # is None`` and runs its ``finally:`` block, draining any
        # final HANDLE_DELs and exiting cleanly. Without this, the
        # loop would stay parked on event.wait() forever after
        # Connection GC — defeating the whole pin.
        #
        # The finalize callback captures only ``event`` (the
        # asyncio.Event, not the Connection) so it does not itself
        # create a strong-ref cycle.
        def _on_connection_collected(_event=event):
            try:
                _event.set()
            except Exception:
                pass
        weakref.finalize(self, _on_connection_collected)

    def _stop_cleanup_task(self) -> None:
        """
        Stop background cleanup task.

        Cancels the cleanup task and waits for it to finish.
        Safe to call multiple times.
        """
        self._cleanup_running = False

        # Wake the cleanup loop so it can observe _cleanup_running=False and
        # exit without waiting for a deletion to arrive. Otherwise it would
        # stay suspended on event.wait() forever (no polling fallback).
        self._signal_deletion_available()

        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            self._cleanup_task = None

    def _process_pending_deletions_sync(self) -> None:
        """
        Process pending netref deletions synchronously (for connection close).

        This is a synchronous version of _process_pending_deletions() used
        during connection close to ensure deletions are sent even if background
        cleanup task wasn't running.

        Note: Does not wait for acknowledgment (fire-and-forget) since connection
        is closing anyway. This is acceptable as it's a best-effort attempt.
        """
        if self._pending_deletions.empty():
            return  # Nothing to process

        logger = self._config.get("logger")
        if logger:
            pending_count = self._pending_deletions.qsize()
            logger.debug(
                f"[CLEANUP] Processing {pending_count} pending deletions on close"
            )

        # Collect all pending deletions from queue
        batch: List[Tuple[Tuple[str, int, int], int]] = []
        while not self._pending_deletions.empty():
            try:
                item = self._pending_deletions.get_nowait()
                batch.append(item)
            except:
                break

        if not batch:
            return

        # Group deletions by id_pack and sum refcounts
        deletions: Dict[Tuple[str, int, int], int] = {}
        for id_pack, refcount in batch:
            deletions[id_pack] = deletions.get(id_pack, 0) + refcount

        # Send deletions (fire-and-forget, no acknowledgment on close)
        for id_pack, total_refcount in deletions.items():
            try:
                # Resurrection check
                proxy = self._proxy_cache.get(id_pack)
                if proxy is not None:
                    continue  # Netref resurrected - skip deletion

                # Send HANDLE_DEL synchronously (no acknowledgment)
                # Use sync_request which will fail silently if connection dead
                try:
                    self.sync_request(consts.HANDLE_DEL, id_pack, total_refcount)
                except:
                    # Connection may already be closed - ignore errors
                    pass

            except Exception as e:
                if logger:
                    logger.warning(
                        f"[CLEANUP] Failed to send deletion for {id_pack}: {e}"
                    )

    async def _process_pending_deletions(self) -> None:
        """
        Process all pending netref deletions in batch.

        Collects all pending deletions from queue, groups them by id_pack
        (to batch multiple deletions of same object), checks for resurrection,
        and sends HANDLE_DEL with acknowledgment.
        """
        # Collect all pending deletions from queue (non-blocking)
        batch: List[Tuple[Tuple[str, int, int], int]] = []
        while not self._pending_deletions.empty():
            try:
                item = self._pending_deletions.get_nowait()
                batch.append(item)
            except:
                break

        if not batch:
            return  # Nothing to process

        # Group deletions by id_pack and sum refcounts
        deletions: Dict[Tuple[str, int, int], int] = {}
        for id_pack, refcount in batch:
            deletions[id_pack] = deletions.get(id_pack, 0) + refcount

        # Process each unique id_pack
        for id_pack, total_refcount in deletions.items():
            try:
                # ═══════════════════════════════════════════════════
                # Phase 3: Resurrection check (race condition prevention)
                # ═══════════════════════════════════════════════════
                # Check if netref was resurrected (re-created) after deletion was queued
                proxy = self._proxy_cache.get(id_pack)
                if proxy is not None:
                    # Netref resurrected - cancel deletion
                    logger = self._config.get("logger")
                    if logger:
                        logger.debug(
                            f"[CLEANUP] Netref {id_pack} resurrected - "
                            f"cancelling deletion"
                        )
                    continue

                # Send HANDLE_DEL with acknowledgment.
                #
                # ⚠  The reply MUST be a brine-dumpable primitive  ⚠
                # (currently ``bool`` — see ``_handle_del`` and
                # ``docs/DESIGN_BIDIRECTIONAL_ASYNC_FIXES.md`` §3).
                # If the reply is a netref, the ``if not result:``
                # below fires a synchronous RPC on this same event
                # loop and self-deadlocks under recursive async
                # traffic. Do not change ``_handle_del``'s return
                # type without re-reading that design doc.
                result = await self._async_request_with_ack(
                    consts.HANDLE_DEL,
                    id_pack,
                    total_refcount,
                    timeout=self._cleanup_ack_timeout
                )

                # Local truth-test of a primitive — NO RPC.
                # ``result`` is either ``False`` (timeout/error path
                # inside ``_async_request_with_ack``) or a bool from
                # ``_handle_del``. If it's ever anything else, fix
                # THAT caller — do not add a ``getattr``/``bool()``
                # workaround here; the invariant is "reply is a
                # primitive".
                if not result:
                    # ═══════════════════════════════════════════════════════════════
                    # CRITICAL: DO NOT REMOVE OR MODIFY THIS LOGGING!
                    # ═══════════════════════════════════════════════════════════════
                    # This warning indicates cleanup failure:
                    # - HANDLE_DEL timeout (remote side slow/unresponsive)
                    # - Network issues causing ack failure
                    # - Remote process crashed during deletion
                    #
                    # This MUST be logged to stderr ALWAYS, regardless of logger config.
                    # Consequences of hiding this error:
                    # - Memory leaks on remote side accumulate silently
                    # - Difficult to diagnose production performance degradation
                    # - "Works on my machine" syndrome (local tests pass, prod fails)
                    #
                    # DO NOT "optimize away" this logging. It's critical diagnostics.
                    # ═══════════════════════════════════════════════════════════════
                    import sys
                    print(
                        f"WARNING: Failed to delete remote object {id_pack}. "
                        f"Possible memory leak on remote side.",
                        file=sys.stderr
                    )
                    logger = self._config.get("logger")
                    if logger:
                        logger.warning(
                            f"Failed to delete remote object {id_pack}. "
                            f"Possible memory leak on remote side."
                        )
            except Exception as e:
                # ═══════════════════════════════════════════════════════════════
                # CRITICAL: DO NOT REMOVE OR MODIFY THIS LOGGING!
                # ═══════════════════════════════════════════════════════════════
                # This error in cleanup path is ALWAYS a bug that needs attention.
                # Common causes:
                # - Protocol desynchronization
                # - Unexpected exception in HANDLE_DEL
                # - Connection state corruption
                #
                # Full traceback MUST be logged to stderr for debugging.
                # This is not "noise" - each exception here represents:
                # - Potential memory leak
                # - Broken cleanup mechanism
                # - Production stability issue
                #
                # If you see this frequently, FIX THE BUG, don't hide the error.
                # ═══════════════════════════════════════════════════════════════
                import sys
                import traceback
                print(
                    f"ERROR: Error deleting remote object {id_pack}: {e}",
                    file=sys.stderr
                )
                traceback.print_exc(file=sys.stderr)
                logger = self._config.get("logger")
                if logger:
                    logger.error(
                        f"Error deleting remote object {id_pack}: {e}",
                        exc_info=True
                    )

    async def _async_request_with_ack(
        self,
        handler: int,
        *args: Any,
        timeout: float = 5.0
    ) -> Any:
        """
        Send async request and wait for acknowledgment with timeout.

        Args:
            handler: Request handler constant (e.g., HANDLE_DEL)
            *args: Arguments to pass to handler
            timeout: Timeout in seconds (default from config)

        Returns:
            Response from remote side, or False on timeout/error
        """
        try:
            # Create AsyncResult for tracking response
            res = AsyncResult(self)

            # Send async request
            self._async_request(handler, args, res)

            # Wait for response with timeout
            result = await asyncio.wait_for(res, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            return False  # Timeout treated as failure
        except Exception:
            return False  # Any error treated as failure

    # ═══════════════════════════════════════════════════════════════
    # End Asyncio Integration
    # ═══════════════════════════════════════════════════════════════

    def ping(self, data=None, timeout=3):  # IO
        """Asserts that the other party is functioning properly, by making sure
        the *data* is echoed back before the *timeout* expires

        :param data: the data to send (leave ``None`` for the default buffer)
        :param timeout: the maximal time to wait for echo

        :raises: :class:`PingError` if the echoed data does not match
        :raises: :class:`EOFError` if the remote host closes the connection
        """
        if data is None:
            data = "abcdefghijklmnopqrstuvwxyz" * 20
        res = self.async_request(consts.HANDLE_PING, data, timeout=timeout)
        if res.value != data:
            raise PingError("echo mismatches sent data")

    def _get_seq_id(self):  # IO
        return next(self._seqcounter)

    # ═══════════════════════════════════════════════════════════════════
    # REFCOUNT RACE FIX — variant A (full): stable id_pack[2] allocator
    # See docs/DESIGN_REFCOUNT_RACE_FIX_A.md.
    # ═══════════════════════════════════════════════════════════════════

    def _alloc_stable_obj_id(self, obj) -> int:
        """Return a connection-local, monotonic, stable integer id for ``obj``.

        Contract:
        * For classes (``inspect.isclass(obj)`` True) — returns 0.
          Preserves the legacy ``id_pack[2] == 0 iff class`` convention.
        * For every other object — returns a monotonically-allocated
          integer that is **stable for the lifetime of ``obj`` on this
          connection**.

        The allocator replaces raw ``id(obj)`` in ``_box`` to close
        variant §2.1 of the refcount race: CPython recycles ``id()``
        after an object is collected, so two different short-lived
        Python objects could end up sharing an ``id_pack`` and driving
        stale ``HANDLE_DEL`` ops against the wrong slot. The stable
        seq is never reused (``itertools.count`` is monotonic across
        the connection's whole lifetime), so that class of bug is
        structurally impossible.

        Weakref-able objects route through ``_obj_to_seq_weak`` (a
        ``WeakKeyDictionary``): GC evicts the entry automatically when
        the object dies, freeing the seq for nothing in particular
        (we never reuse seqs). Un-weakref-able objects (bound methods,
        some built-ins) go through ``_obj_to_seq_by_id`` keyed by
        ``id()``; that's safe because ``_local_objects`` holds a strong
        ref while the seq is meaningful — ``id()`` cannot be reused
        until after the registry slot is removed, at which point
        ``_forget_stable_obj_id`` has already cleared the mapping.
        """
        import inspect
        if inspect.isclass(obj):
            return 0

        # Fast path (weakref-able): already seen.
        try:
            return self._obj_to_seq_weak[obj]
        except (KeyError, TypeError):
            # KeyError: not yet seen.
            # TypeError: object is not weakref-able (falls through to fallback).
            pass

        # Try weakref-able storage.
        try:
            weakref.ref(obj)
            weakrefable = True
        except TypeError:
            weakrefable = False

        if weakrefable:
            seq = next(self._id_pack_seq)
            try:
                self._obj_to_seq_weak[obj] = seq
            except TypeError:
                # Rare: object says weakref.ref works but
                # WeakKeyDictionary rejects it (e.g. unhashable).
                # Route to id-fallback instead.
                pass
            else:
                return seq

        # id-fallback for un-weakref-able / unhashable objects.
        py_id = id(obj)
        entry = self._obj_to_seq_by_id.get(py_id)
        if entry is not None and entry[0] is obj:
            return entry[1]
        # Either new or an id() collision. Both cases: assign a fresh
        # seq. The registry's strong ref guarantees we don't overwrite
        # a live entry — see design doc §3.
        seq = next(self._id_pack_seq)
        self._obj_to_seq_by_id[py_id] = (obj, seq)
        return seq

    def _stable_id_pack(self, obj) -> Tuple[str, int, int]:
        """Build the id_pack for a local ``obj`` using the stable allocator.

        This is the variant-A replacement for the ``get_id_pack(obj)``
        call in ``_box``: the third slot comes from
        ``_alloc_stable_obj_id`` instead of ``id(obj)``, eliminating
        the CPython ``id()`` reuse race at its source.

        Name-pack and type-id computation mirror ``rpyc.lib.get_id_pack``
        so the rest of the stack (netref class factory, `_handle_inspect`)
        sees exactly the shape it expects.
        """
        import inspect as _inspect
        undef = object()
        # Netrefs self-identify via ____id_pack__; respect that.
        name_pack = getattr(obj, '____id_pack__', undef)
        if name_pack is not undef:
            return name_pack  # type: ignore[return-value]

        obj_name = getattr(obj, '__name__', None)

        if _inspect.ismodule(obj):
            # Module objects. Module name becomes the name pack.
            import sys as _sys
            if obj_name and obj_name != 'module' and obj_name in _sys.modules:
                name_pack = obj_name
            else:
                obj_cls = getattr(obj, '__class__', type(obj))
                name_pack = f'{obj_cls.__module__}.{obj_name}'
            return (name_pack, id(type(obj)), self._alloc_stable_obj_id(obj))

        if not _inspect.isclass(obj):
            obj_cls = getattr(obj, '__class__', type(obj))
            name_pack = f'{obj_cls.__module__}.{obj_cls.__name__}'
            return (name_pack, id(type(obj)), self._alloc_stable_obj_id(obj))

        # Class path: third slot is 0 (unchanged contract).
        name_pack = f'{obj.__module__}.{obj_name}'
        return (name_pack, id(obj), 0)

    def _forget_stable_obj_id(self, id_pack) -> None:
        """Drop the stable-id mapping for ``id_pack`` once its
        ``_local_objects`` slot is deleted (``decref`` reached 0).

        Called from ``_handle_del`` after ``_local_objects.decref``
        returns True. Only the id-fallback map needs explicit cleanup;
        the ``WeakKeyDictionary`` prunes itself when the object dies.

        We don't know which Python object the id_pack belonged to any
        more — it may have been evicted from ``_local_objects`` already
        — but we can scan ``_obj_to_seq_by_id`` for the matching seq
        (``id_pack[2]``) and drop it. The map is bounded by the number
        of live un-weakref-able boxed objects per connection, so the
        scan is cheap.
        """
        if not isinstance(id_pack, tuple) or len(id_pack) < 3:
            return
        target_seq = id_pack[2]
        if target_seq == 0:
            return  # classes are never stored here
        stale_key = None
        for py_id, (_obj, seq) in self._obj_to_seq_by_id.items():
            if seq == target_seq:
                stale_key = py_id
                break
        if stale_key is not None:
            self._obj_to_seq_by_id.pop(stale_key, None)

    def _send(self, msg, seq, args):  # IO
        data = brine.I1.pack(msg) + brine.dump((seq, args))  # see _dispatch
        if self._bind_threads:
            this_thread = self._get_thread()
            data = brine.I8I8.pack(this_thread.id, this_thread._remote_thread_id) + data
            if msg == consts.MSG_REQUEST:
                this_thread._occupation_count += 1
            else:
                this_thread._occupation_count -= 1
                if this_thread._occupation_count == 0:
                    this_thread._remote_thread_id = UNBOUND_THREAD_ID
        # GC might run while sending data
        # if so, a BaseNetref.__del__ might be called
        # BaseNetref.__del__ must call asyncreq,
        # which will cause a deadlock
        # Solution:
        # Add the current request to a queue and let the thread that currently
        # holds the sendlock send it when it's done with its current job.
        # NOTE: Atomic list operations should be thread safe,
        # please call me out if they are not on all implementations!
        self._send_queue.append(data)
        # It is crucial to check the queue each time AFTER releasing the lock:
        while self._send_queue:
            if not self._sendlock.acquire(False):
                # Another thread holds the lock. It will send the data after
                # it's done with its current job. We can safely return.
                return
            try:
                # Can happen if another consumer was scheduled in between
                # `while` and `acquire`:
                if not self._send_queue:
                    # Must `continue` to ensure that `send_queue` is checked
                    # after releasing the lock! (in case another producer is
                    # scheduled before `release`)
                    continue
                data = self._send_queue.pop(0)
                self._channel.send(data)
            finally:
                self._sendlock.release()

    def _box(self, obj):  # boxing
        """store a local object in such a way that it could be recreated on
        the remote party either by-value or by-reference"""
        import inspect

        if brine.dumpable(obj):
            return consts.LABEL_VALUE, obj
        if type(obj) is tuple:
            return consts.LABEL_TUPLE, tuple(self._box(item) for item in obj)
        elif isinstance(obj, netref.BaseNetref) and obj.____conn__ is self:
            # ═══════════════════════════════════════════════════════════
            # Netref round-trip boxing — DIRECTION DISAMBIGUATOR.
            # ═══════════════════════════════════════════════════════════
            # ⚠  REGRESSION WARNING — DO NOT "SIMPLIFY" THIS BRANCH  ⚠
            # Post-mortem: ``docs/DESIGN_BIDIRECTIONAL_ASYNC_FIXES.md``.
            # ═══════════════════════════════════════════════════════════
            #
            # When ``obj.____conn__ is self``, the netref was created
            # on THIS side of THIS connection. But that alone doesn't
            # tell us who OWNS the underlying Python object. Two
            # scenarios produce such a netref:
            #
            #   (a) We received a LABEL_LOCAL_REF from the peer:
            #       "here's YOUR own object back". In this case the
            #       real object lives in ``self._local_objects`` and
            #       ``_unbox`` returned the real object directly (so
            #       we never actually see ``obj`` as a netref in case
            #       (a); this branch is unreachable for that path —
            #       kept as a safety fallback below). Round-tripping
            #       it back to the peer → LABEL_LOCAL_REF.
            #
            #   (b) We received a LABEL_REMOTE_REF from the peer:
            #       "here's MY object, track it". ``_unbox`` created a
            #       proxy, stored it in ``self._proxy_cache[id_pack]``,
            #       and returned it. The real object lives on the
            #       peer. Round-tripping it back to the peer → the
            #       peer needs LABEL_REMOTE_REF with the SAME id_pack
            #       so the peer can look it up in its own
            #       ``_local_objects``.
            #
            # The pre-existing disambiguator
            # ``if id_pack in self._local_objects._dict`` is
            # **INSUFFICIENT** because id_pack can collide across two
            # independent processes:
            #
            #   id_pack = (name_pack, id(type(obj)), seq)
            #
            #   * ``name_pack`` — module-qualified class name.
            #     Identical across processes by definition.
            #   * ``id(type(obj))`` — address of the type object in
            #     RAM. For built-in types (``builtins.method``,
            #     ``builtins.dict``, ``builtins.list``, ...) this is
            #     IDENTICAL across processes because CPython resolves
            #     built-in type objects to deterministic addresses
            #     in the executable's data segment. Verified:
            #       $ python3 -c 'class C: pass
            #                      print(id(type(C().__init__)))'
            #       10665440
            #       $ python3 -c 'class C: pass
            #                      print(id(type(C().__init__)))'
            #       10665440      ← same address in both processes
            #   * ``seq`` — per-connection monotonic counter
            #     (``itertools.count(1 << 40)``). Each new
            #     ``Connection`` starts at the SAME origin. Two peers
            #     both independently allocate seq 1099511627776,
            #     1099511627777, ... for their Nth boxed object.
            #
            # So two independent processes EACH mint
            # ``('builtins.method', 10665440, 1099511627777)``
            # for their own 2nd boxed built-in-method-typed object.
            # These are completely unrelated Python objects, but the
            # id_pack tuples are bit-identical.
            #
            # Concrete failure reproduced in
            # ``tests/test_e2e_netref_async_callback.py::test_netref_recursive_async_calls``:
            # server holds a proxy (scenario b) to client's
            # ``async_chain`` bound method. Server boxes it for
            # outbound RPC. With the old check
            # ``id_pack in self._local_objects._dict``: server's own
            # 2nd-boxed bound method ``exposed_async_chain_calls``
            # ALSO has id_pack ``('builtins.method', 10665440,
            # 1099511627777)``, so server mistakenly took the
            # LABEL_LOCAL_REF branch and told the client "this is
            # YOUR object". Client looked it up in client's
            # ``_local_objects`` — found client's own ``async_chain``
            # under the same colliding id_pack — and dispatched
            # there. BUT client's entry was correct; the BUG is that
            # when the call returned through the same path the SERVER
            # got a LABEL_REMOTE_REF back with the same id_pack,
            # resolved it against its own colliding slot, fed it to
            # ``_handle_async_call`` as a netref, which called it
            # again → peer received yet another LABEL_LOCAL_REF …
            # infinite ping-pong between peers with identical
            # ``args`` and ``id_pack``. Stack-snapshot verified; see
            # design doc §2.2, §2.3.
            #
            # THE FIX: ``_proxy_cache`` is populated **exclusively**
            # by ``_unbox(LABEL_REMOTE_REF)``. If the very ``obj``
            # object-identity matches what's cached at ``id_pack`` in
            # ``_proxy_cache``, then ``obj`` is DEFINITIVELY a
            # peer-owned proxy (scenario b). No collision risk:
            # ``_proxy_cache`` is keyed AND filtered by the proxy's
            # own identity (``is obj``), not just the id_pack.
            #
            # Priority order (load-bearing, do not reorder):
            #   1. ``_proxy_cache.get(id_pack) is obj`` → peer proxy,
            #      send LABEL_REMOTE_REF.
            #   2. else ``id_pack in self._local_objects._dict`` →
            #      our own object genuinely, send LABEL_LOCAL_REF.
            #   3. else → unknown (evicted slot, hand-crafted netref)
            #      → fall back to LABEL_REMOTE_REF; peer will either
            #      find it or raise a clear error.
            #
            # DO NOT replace the ``_proxy_cache`` check with
            # ``id_pack in self._proxy_cache``. WeakValueDict keys
            # can remain after the value was GC'd; the ``is obj``
            # identity check is what guarantees correctness.
            # ═══════════════════════════════════════════════════════════
            id_pack = obj.____id_pack__
            is_peer_proxy = self._proxy_cache.get(id_pack) is obj
            if is_peer_proxy:
                # Scenario (b): peer-owned proxy round-tripping back.
                # Peer resolves the id_pack in ITS ``_local_objects``.
                is_async = getattr(obj, "____is_async__", False)
                flags = consts.FLAGS_ASYNC if is_async else consts.FLAGS_SYNC
                return consts.LABEL_REMOTE_REF, (*id_pack, flags)
            elif id_pack in self._local_objects._dict:
                # Scenario (a): genuinely our local object (rarely
                # reached because ``_unbox(LABEL_LOCAL_REF)`` returns
                # the real object, not a netref — but covered for
                # safety and for pathological hand-crafted netrefs).
                return consts.LABEL_LOCAL_REF, id_pack
            else:
                # Unknown: the slot was evicted (premature decref
                # race) or someone constructed a netref manually.
                # Safer to send LABEL_REMOTE_REF and let the peer
                # report "id_pack not found" with a clear message
                # than to send LABEL_LOCAL_REF and silently hit a
                # collision on the other side.
                logger = self._config.get("logger")
                if logger:
                    logger.debug(
                        f"Netref {id_pack} not in _local_objects and not "
                        f"a known proxy; using LABEL_REMOTE_REF."
                    )
                is_async = getattr(obj, "____is_async__", False)
                flags = consts.FLAGS_ASYNC if is_async else consts.FLAGS_SYNC
                return consts.LABEL_REMOTE_REF, (*id_pack, flags)
        else:
            # REFCOUNT RACE FIX — variant A (full).
            # Build id_pack via the stable-seq allocator instead of
            # ``get_id_pack(obj)`` (which uses id(obj) and exposes us
            # to CPython id() reuse — see
            # docs/DESIGN_REFCOUNT_RACE_FIX_A.md).
            id_pack = self._stable_id_pack(obj)
            self._local_objects.add(id_pack, obj)

            # ═══════════════════════════════════════════════════
            # NEW (v5.1): Extended id_pack for async functions
            # ═══════════════════════════════════════════════════
            if inspect.iscoroutinefunction(obj):
                # Async function - add FLAGS_ASYNC metadata
                id_pack_with_flags = (*id_pack, consts.FLAGS_ASYNC)
                return consts.LABEL_REMOTE_REF, id_pack_with_flags
            elif inspect.iscoroutine(obj):
                # Coroutine instance (unusual - likely bug)
                import warnings
                warnings.warn(
                    f"Boxing coroutine object {obj!r}. "
                    f"Did you forget to await?",
                    RuntimeWarning,
                    stacklevel=2
                )
                id_pack_with_flags = (*id_pack, consts.FLAGS_ASYNC)
                return consts.LABEL_REMOTE_REF, id_pack_with_flags
            else:
                # Sync object - use standard 3-tuple id_pack
                return consts.LABEL_REMOTE_REF, id_pack

    def _unbox(self, package):  # boxing
        """recreate a local object representation of the remote object: if the
        object is passed by value, just return it; if the object is passed by
        reference, create a netref to it"""
        label, value = package
        if label == consts.LABEL_VALUE:
            return value
        if label == consts.LABEL_TUPLE:
            return tuple(self._unbox(item) for item in value)
        if label == consts.LABEL_LOCAL_REF:
            # Try to retrieve object from _local_objects
            # Add defensive error handling for missing objects (e.g., premature decref)
            try:
                return self._local_objects[value]
            except KeyError:
                # Object missing from _local_objects - likely removed via decref
                # while still being referenced by remote side
                logger = self._config.get("logger")

                # Try to provide helpful information about what was expected
                # value is id_pack: (name_pack, type_id, object_id)
                name_pack = value[0] if len(value) > 0 else "unknown"
                type_id = value[1] if len(value) > 1 else 0
                obj_id = value[2] if len(value) > 2 else 0

                if logger:
                    logger.error(
                        f"LABEL_LOCAL_REF points to missing object {value}. "
                        f"Expected: {name_pack} (type_id={type_id:#x}, obj_id={obj_id:#x}). "
                        f"Object may have been garbage collected or improperly reference counted."
                    )
                raise ValueError(
                    f"Local object {value} ({name_pack}) not found in _local_objects. "
                    "Object may have been garbage collected or removed via premature decref. "
                    "This indicates a race condition in reference counting."
                ) from None
        if label == consts.LABEL_REMOTE_REF:
            # ═══════════════════════════════════════════════════
            # NEW (v5.1): Handle extended id_pack format
            # ═══════════════════════════════════════════════════
            # Check if extended format (4 elements) or old format (3)
            if len(value) == 4:
                # New format: (class, id, ver, flags)
                id_pack = (str(value[0]), value[1], value[2])
                flags = value[3]
            elif len(value) == 3:
                # Old format: (class, id, ver)
                id_pack = (str(value[0]), value[1], value[2])
                flags = consts.FLAGS_SYNC  # Default: sync object
            else:
                raise ValueError(f"Invalid id_pack length: {len(value)}")

            # ═══════════════════════════════════════════════════
            # IMPORTANT FIX: Check if object is actually LOCAL
            # ═══════════════════════════════════════════════════
            # If LABEL_REMOTE_REF points to object in OUR _local_objects,
            # return the local object directly instead of creating a proxy.
            # This handles the case where a proxy to remote object is sent back.
            if id_pack in self._local_objects._dict:
                # With PID-namespaced seqs (see
                # docs/DESIGN_PID_NAMESPACED_ID_PACK.md) a membership
                # hit here is only legitimate if slot 2 encodes OUR
                # pid.  Anything else means a peer handed us an
                # id_pack that happens to collide with ours — this
                # was impossible by construction after the fix.
                # Guard it in debug mode to catch regressions in the
                # allocator; in production we still fall through to
                # return the local object (keeping wire-compat with
                # pre-fix peers).
                if self._config.get("debug_refcounting", False):
                    owner_pid = id_pack[2] >> 32
                    my_pid = os.getpid()
                    if owner_pid != my_pid:
                        raise ValueError(
                            f"id_pack {id_pack} matches a local slot "
                            f"but slot[2] encodes pid={owner_pid}, "
                            f"not our pid={my_pid}. This indicates "
                            f"either a peer running pre-fix rpyc_async "
                            f"(old fixed 1<<40 seed) or a genuine bug "
                            f"in _alloc_stable_obj_id."
                        )
                # Object is actually local! Return it directly.
                return self._local_objects[id_pack]

            proxy = self._proxy_cache.get(id_pack)  # Ensure referents exist until we increment refcount issue #558
            if proxy is not None:
                proxy.____refcount__ += 1  # if cached then remote incremented refcount, so sync refcount
            else:
                proxy = self._netref_factory(id_pack)
                self._proxy_cache[id_pack] = proxy

                # ═══════════════════════════════════════════════════
                # NEW (v5.2): Register cleanup callback for netref
                # ═══════════════════════════════════════════════════
                # Store the proxy's refcount in a mutable container.
                # This will be read by netref.__del__ and passed to callback.
                # We store it as a list so we can update it when refcount changes.
                refcount_holder = {"id_pack": id_pack, "refcount": proxy.____refcount__}

                # Attach refcount holder to proxy so __del__ can update it
                object.__setattr__(proxy, "_refcount_holder", refcount_holder)
                object.__setattr__(proxy, "_cleanup_connection", self)

                # No finalizer registration here - we'll use __del__ instead
                # (but __del__ won't do I/O, just queue deletion)

            # ═══════════════════════════════════════════════════
            # NEW (v5.1): Attach async metadata to proxy
            # ═══════════════════════════════════════════════════
            # Use object.__setattr__ to avoid triggering netref's __setattr__
            if flags & consts.FLAGS_ASYNC:
                # Mark proxy as async
                # This metadata is used by netref for handler selection
                object.__setattr__(proxy, "____is_async__", True)
            else:
                object.__setattr__(proxy, "____is_async__", False)

            return proxy
        raise ValueError(f"invalid label {label!r}")

    def _netref_factory(self, id_pack):  # boxing
        """id_pack is for remote, so when class id fails to directly match """
        cls = None
        if id_pack[2] == 0 and id_pack in self._netref_classes_cache:
            cls = self._netref_classes_cache[id_pack]
        elif id_pack[0] in netref.builtin_classes_cache:
            cls = netref.builtin_classes_cache[id_pack[0]]
        if cls is None:
            # in the future, it could see if a sys.module cache/lookup hits first
            cls_methods = self.sync_request(consts.HANDLE_INSPECT, id_pack)
            cls = netref.class_factory(id_pack, cls_methods)
            if id_pack[2] == 0:
                # only use cached netrefs for classes
                # ... instance caching after gc of a proxy will take some mental gymnastics
                self._netref_classes_cache[id_pack] = cls
        return cls(self, id_pack)

    # ═══════════════════════════════════════════════════════════════
    # Async Dispatch Pipeline (v5.1)
    # ═══════════════════════════════════════════════════════════════

    def _is_async_handler(self, handler_id):
        """
        Check if handler requires async dispatch.

        Args:
            handler_id: Handler constant (HANDLE_CALL, etc.)

        Returns:
            bool: True if handler is async
        """
        # Explicit async handlers
        if handler_id in (
            consts.HANDLE_ASYNC_CALL,
            consts.HANDLE_ASYNC_CALLATTR
        ):
            return True

        # Check handler function signature
        handler_func = self._HANDLERS.get(handler_id)
        if handler_func is None:
            return False

        import inspect
        return inspect.iscoroutinefunction(handler_func)

    def _needs_async_dispatch(self, msg_type, args):
        """
        Determine if message requires async dispatch pipeline.

        Args:
            msg_type: Message type constant
            args: Message arguments (handler_id, ...)

        Returns:
            bool: True if async dispatch needed
        """
        # Explicit async messages
        if msg_type in (
            consts.MSG_ASYNC_REQUEST,
            consts.MSG_ASYNC_REPLY,
            consts.MSG_ASYNC_EXCEPTION
        ):
            return True

        # Check handler type for MSG_REQUEST
        if msg_type == consts.MSG_REQUEST:
            handler_id, _ = args
            return self._is_async_handler(handler_id)

        return False

    def _send_async_result_safe(self, msg, seq, args_factory):
        """Send an async REPLY/EXCEPTION, tolerating a dead channel.

        If the peer has closed the underlying stream, ``self._send`` will
        raise ``EOFError`` (or a related ``OSError`` subclass). There is
        nothing to reply to at that point — the request is already lost
        — so we swallow the error and mark the connection closed. If we
        re-raised, the error would escape ``_dispatch_request_async``,
        which is scheduled via ``asyncio.run_coroutine_threadsafe`` with
        no one awaiting its Future; Python would then emit
        ``Exception ignored in: <coroutine ...>`` on every subsequent
        dispatch. Under a hot retry loop (e.g. a periodic status-poll
        watcher) that produces hundreds of thousands of log lines and
        unbounded memory growth. See a related internal incident
        analysis (not included here).

        ``args_factory`` is a zero-arg callable that produces the payload
        lazily — this lets us skip the potentially-expensive ``_box`` /
        ``_box_exc`` work if we can cheaply short-circuit in future.

        Args:
            msg: rpyc message type constant.
            seq: sequence number.
            args_factory: callable returning the boxed payload.
        """
        # Skip the send entirely if the connection is already torn down.
        # _send would observe the closed channel and raise anyway, but
        # short-circuiting is clearer (and avoids boxing work that will
        # be discarded).
        if self._closed:
            return
        try:
            self._send(msg, seq, args_factory())
        except (EOFError, OSError) as exc:
            # OSError covers BrokenPipeError, ConnectionResetError, and
            # the generic "Bad file descriptor" case that shows up when
            # a reloader tears the socket down mid-dispatch.
            logger = self._config.get("logger")
            if logger is not None:
                logger.debug(
                    "async dispatch: channel closed before reply could be "
                    "sent (msg=%s seq=%s): %s",
                    msg, seq, exc,
                )
            # Mark the connection closed so the next dispatch short-
            # circuits immediately instead of repeating the failure.
            # Avoid self.close(): that path does sync_request(HANDLE_CLOSE)
            # which would itself try to write to the dead channel.
            self._closed = True

    async def _dispatch_request_async(self, seq, raw_args):
        """
        Async version of _dispatch_request() - can await handlers!

        Args:
            seq: Request sequence number
            raw_args: Raw (handler_id, args) tuple

        This method runs in the event loop and can safely await.
        """
        import inspect

        try:
            handler_id, args = raw_args

            # Unbox arguments
            args = self._unbox(args)

            # Get handler function
            handler_func = self._HANDLERS.get(handler_id)
            if handler_func is None:
                raise AttributeError(f"Unknown handler: {handler_id}")

            # ═══════════════════════════════════════════════════
            # EXECUTE HANDLER (async or sync)
            # ═══════════════════════════════════════════════════
            if inspect.iscoroutinefunction(handler_func):
                # Handler is async - await it!
                res = await handler_func(self, *args)
            else:
                # Handler is sync - call normally
                res = handler_func(self, *args)

                # Check if result is coroutine (from async function call)
                if inspect.iscoroutine(res):
                    res = await res

        except:  # TODO: revisit exception handling
            # Exception during execution.
            #
            # ─── traceback retention safety ─────────────────────
            # CRITICAL: we MUST NOT store the live ``tb`` object on
            # any long-lived attribute. A TracebackType keeps every
            # ``tb_frame`` alive, and each frame's ``f_locals``
            # keeps every local variable it had at the moment of
            # the exception — including any ``AsyncResult`` the
            # handler was awaiting on. On a busy bidirectional-
            # async deployment, one cancellation cascade through
            # ``Connection._cleanup`` raises ``CancelledError`` in
            # every in-flight dispatch task. If we hang on to those
            # tracebacks, EACH one pins its handler frame's locals
            # = a full AsyncResult chain = 50 KB+ leaked per
            # cancelled request. The production incident
            # accumulated 4 038 032 AR chains and 17.8 GB RSS this
            # way. See a related internal incident analysis (not
            # included here).
            #
            # The fix:
            #   1. Eagerly box the exception NOW, while ``tb`` is
            #      on the local stack and about to disappear. This
            #      produces a brine-dumpable tuple (string text
            #      for the traceback, no live frames).
            #   2. Pass the *result* of boxing to
            #      ``_send_async_result_safe``, NOT a lambda that
            #      closes over ``t, v, tb``. A closure over those
            #      would re-create the retention bug if the lambda
            #      ever outlives this frame.
            #   3. Do not store ``tb`` on ``self`` under any name.
            #      The legacy ``self._last_traceback`` was used by
            #      ``rpyc.utils.classic.pdb_post_mortem`` — a
            #      developer-debug path that has no business
            #      shipping in production. If post-mortem ever
            #      needs to come back, it should grab the
            #      traceback at the point of debugging via
            #      ``sys.last_traceback`` or by re-raising, not by
            #      having every Connection carry one around.
            #   4. ``del t, v, tb`` at the end of the ``except``
            #      block. CPython auto-deletes only the bound name
            #      in ``except X as e:`` — ``sys.exc_info()`` tuple
            #      members are regular locals and stay until the
            #      coroutine frame itself is collected. For a hot
            #      dispatch loop "until the frame is collected" is
            #      "until the next await suspends and resumes" —
            #      long enough for GC to see the chain.
            t, v, tb = sys.exc_info()

            logger = self._config["logger"]
            if logger and t is not StopIteration:
                logger.debug("Exception caught in async dispatch", exc_info=True)

            if t is SystemExit and self._config["propagate_SystemExit_locally"]:
                raise
            if t is KeyboardInterrupt and self._config["propagate_KeyboardInterrupt_locally"]:
                raise

            # Eagerly box; do NOT capture (t, v, tb) in a closure.
            try:
                boxed = self._box_exc(t, v, tb)
            except Exception:
                # If boxing itself fails (rare — should always be
                # safe for primitive errors), fall back to a
                # plain string repr to avoid retaining tb.
                boxed = self._box_exc(
                    RuntimeError,
                    RuntimeError(f"unboxable {t!r}: {v!r}"),
                    None,
                )
            # Send async exception reply. If the channel is already dead
            # (peer disconnected), there is nothing to reply to — swallow
            # the I/O error and mark the connection closed. Letting it
            # escape here turns every subsequent dispatch into an
            # unawaited-coroutine log storm (observed in a downstream application).
            self._send_async_result_safe(
                consts.MSG_ASYNC_EXCEPTION, seq, lambda boxed=boxed: boxed
            )
            # Drop strong refs to traceback and exception value
            # before this coroutine frame can be inspected by any
            # other path (cancellation, debugger, gc walk). See
            # the "traceback retention safety" comment above for
            # why this matters.
            del t, v, tb, boxed
        else:
            # Success - send async reply
            self._send_async_result_safe(
                consts.MSG_ASYNC_REPLY, seq, lambda: self._box(res)
            )

    # ═══════════════════════════════════════════════════════════════
    # End Async Dispatch Pipeline
    # ═══════════════════════════════════════════════════════════════

    def _dispatch_request(self, seq, raw_args):  # dispatch
        try:
            handler, args = raw_args
            args = self._unbox(args)
            res = self._HANDLERS[handler](self, *args)
        except:  # TODO: revisit how to catch handle locally, this should simplify when py2 is dropped
            # need to catch old style exceptions too.
            #
            # SAME traceback-retention safety as the async catch-
            # all above (search for "traceback retention safety"
            # in this file). We DO NOT store ``tb`` on ``self``
            # because a long-lived attribute holding a TracebackType
            # pins every frame's f_locals — which on a busy
            # connection means every AsyncResult the handler was
            # awaiting on. The production incident
            # accumulated 4 M leaked AsyncResult chains and
            # 17.8 GB RSS through this exact pattern; see a related
            # internal incident analysis (not included here).
            t, v, tb = sys.exc_info()
            logger = self._config["logger"]
            if logger and t is not StopIteration:
                logger.debug("Exception caught", exc_info=True)
            if t is SystemExit and self._config["propagate_SystemExit_locally"]:
                raise
            if t is KeyboardInterrupt and self._config["propagate_KeyboardInterrupt_locally"]:
                raise
            # Box now (returns brine-dumpable tuple with text-only
            # traceback) — release ``tb`` before send.
            boxed = self._box_exc(t, v, tb)
            self._send(consts.MSG_EXCEPTION, seq, boxed)
            del t, v, tb, boxed
        else:
            self._send(consts.MSG_REPLY, seq, self._box(res))

    def _box_exc(self, typ, val, tb):  # dispatch?
        return vinegar.dump(typ, val, tb,
                            include_local_traceback=self._config["include_local_traceback"],
                            include_local_version=self._config["include_local_version"])

    def _unbox_exc(self, raw):  # dispatch?
        return vinegar.load(raw,
                            import_custom_exceptions=self._config["import_custom_exceptions"],
                            instantiate_custom_exceptions=self._config["instantiate_custom_exceptions"],
                            instantiate_oldstyle_exceptions=self._config["instantiate_oldstyle_exceptions"])

    def _seq_request_callback(self, msg, seq, is_exc, obj):
        _callback = self._request_callbacks.pop(seq, None)
        if _callback is not None:
            _callback(is_exc, obj)
        elif self._config["logger"] is not None:
            debug_msg = 'Recieved {} seq {} and a related request callback did not exist'
            self._config["logger"].debug(debug_msg.format(msg, seq))

    def _dispatch(self, data):  # serving---dispatch?
        msg, = brine.I1.unpack(data[:1])  # unpack just msg to minimize time to release
        if msg == consts.MSG_REQUEST:
            if self._bind_threads:
                self._get_thread()._occupation_count += 1
            elif not self._asyncio_enabled:
                # Only release lock if NOT using asyncio serving
                # (asyncio serving doesn't use the lock - event loop serializes)
                self._recvlock.release()
            seq, args = brine.load(data[1:])

            # ═══════════════════════════════════════════════════════════
            # NEW (v5.1): Async Dispatch Routing
            # ═══════════════════════════════════════════════════════════
            needs_async = self._needs_async_dispatch(msg, args)

            if needs_async and self._asyncio_enabled and self._asyncio_loop:
                # ASYNC DISPATCH PIPELINE (with event loop)
                import asyncio
                # Schedule the dispatch coroutine on the connection's
                # event loop. We then immediately STRONG-REF-PIN the
                # resulting asyncio.Task in ``_DISPATCH_INFLIGHT`` so
                # GC cannot collect it mid-flight. See the extensive
                # ``_DISPATCH_INFLIGHT`` docstring at module scope for
                # the full production-failure-mode chain and the
                # reason this pin is mandatory. The auto-discard
                # done-callback releases the pin the moment the Task
                # finishes (success / handler-exception / cancel),
                # so the set never grows beyond the in-flight working
                # set.
                # ── O(1) schedule + pin ────────────────────────────
                # We need TWO things for every inbound dispatch:
                #   1. The coroutine must start running on
                #      ``self._asyncio_loop`` (which may be on a
                #      different thread from the channel reader).
                #   2. The resulting ``asyncio.Task`` must be added
                #      to ``_DISPATCH_INFLIGHT`` (see module-level
                #      docstring on why this strong-ref pin is
                #      mandatory).
                #
                # The OBVIOUSLY-WRONG way (used in the first cut of
                # this fix, commit 0858bd3, caused a production
                # livelock) is to call
                # ``asyncio.run_coroutine_threadsafe(...)`` and then,
                # in a separate ``call_soon_threadsafe`` callback,
                # scan ``asyncio.all_tasks()`` to find the Task we
                # just scheduled by matching its coroutine's
                # qualname + frame.f_locals. That's O(N) where N is
                # the number of in-flight tasks — and since this
                # very mechanism pins every dispatch, N grows with
                # the number of dispatches processed. End state:
                # O(N²), livelock, 60–80 % CPU on
                # ``frame.f_locals.get("self") is self`` scans, zero
                # forward progress.
                #
                # The CORRECT way (this code) is to schedule the
                # task via a tiny bridge that runs ON the loop
                # thread and ALREADY has the Task object in its
                # hands — no lookup needed:
                #
                #   def _schedule():
                #       task = loop.create_task(coro)
                #       _DISPATCH_INFLIGHT.add(task)
                #       task.add_done_callback(_DISPATCH_INFLIGHT.discard)
                #
                #   loop.call_soon_threadsafe(_schedule)
                #
                # This is O(1) per dispatch — three dict-set ops,
                # one ``add_done_callback``, no traversal of any
                # collection whose size scales with the number of
                # live tasks. Regression test:
                # ``tests/test_dispatch_strong_ref.py::
                #  test_dispatch_does_not_call_asyncio_all_tasks``.
                #
                # The coroutine is built HERE on the channel-reader
                # thread (it doesn't start running until create_task
                # picks it up), then passed by closure into
                # ``_schedule``. Calling
                # ``self._dispatch_request_async(seq, args)`` from
                # the wrong thread is safe — it just constructs a
                # coroutine object; no awaits happen until the loop
                # actually starts driving it.
                # ─── Per-Connection inbound backpressure ─────────
                # See docs/DESIGN_INBOUND_BACKPRESSURE.md. Once a
                # Connection has crossed ``max_inbound_inflight``
                # parked dispatch tasks, it enters terminal
                # quarantine and we silently drop every further
                # MSG_REQUEST on this channel. This protects an
                # agent from a malformed client that keeps pumping
                # requests while ignoring our callbacks (observed in
                # a downstream application, 12.88 GB
                # RSS in 73 min before manual kill).
                if self._inbound_quarantined:
                    return
                _max_inflight = self._config.get("max_inbound_inflight", 0)
                if _max_inflight and self._inbound_inflight >= _max_inflight:
                    self._enter_inbound_quarantine()
                    return

                _coro = self._dispatch_request_async(seq, args)

                def _schedule(_coro=_coro, _self=self):
                    task = asyncio.get_event_loop().create_task(_coro)
                    _DISPATCH_INFLIGHT.add(task)
                    _self._inbound_inflight += 1

                    def _on_done(_t, _self=_self):
                        _DISPATCH_INFLIGHT.discard(_t)
                        _self._inbound_inflight -= 1

                    task.add_done_callback(_on_done)

                self._asyncio_loop.call_soon_threadsafe(_schedule)
                # Returns immediately - does NOT block!
            elif needs_async:
                # ASYNC DISPATCH (no event loop available - ERROR)
                # Cannot execute async method without persistent event loop
                raise RuntimeError(
                    "Async method requires persistent event loop. "
                    "Either:\n"
                    "1. Use AsyncioServer for server-side: from rpyc.utils.async_server import AsyncioServer\n"
                    "2. Enable asyncio serving for client-side: conn.enable_asyncio_serving()\n"
                    "\n"
                    "ThreadedServer cannot support bidirectional async due to architectural limitations.\n"
                    "See: docs/LIMITATIONS.md"
                )
            else:
                # SYNC DISPATCH (existing behavior)
                self._dispatch_request(seq, args)
        else:
            if self._bind_threads:
                this_thread = self._get_thread()
                this_thread._occupation_count -= 1
                if this_thread._occupation_count == 0:
                    this_thread._remote_thread_id = UNBOUND_THREAD_ID
            if msg == consts.MSG_REPLY:
                seq, args = brine.load(data[1:])
                obj = self._unbox(args)
                self._seq_request_callback(msg, seq, False, obj)
                if not self._bind_threads and not self._asyncio_enabled:
                    self._recvlock.release()  # releasing here fixes race condition with AsyncResult.wait
            elif msg == consts.MSG_EXCEPTION:
                if not self._bind_threads and not self._asyncio_enabled:
                    self._recvlock.release()
                seq, args = brine.load(data[1:])
                obj = self._unbox_exc(args)
                self._seq_request_callback(msg, seq, True, obj)
            # ═══════════════════════════════════════════════════════════
            # NEW (v5.1): Async Reply/Exception handling
            # ═══════════════════════════════════════════════════════════
            elif msg == consts.MSG_ASYNC_REPLY:
                seq, args = brine.load(data[1:])
                obj = self._unbox(args)
                self._seq_request_callback(msg, seq, False, obj)
                if not self._bind_threads and not self._asyncio_enabled:
                    self._recvlock.release()
            elif msg == consts.MSG_ASYNC_EXCEPTION:
                if not self._bind_threads and not self._asyncio_enabled:
                    self._recvlock.release()
                seq, args = brine.load(data[1:])
                obj = self._unbox_exc(args)
                self._seq_request_callback(msg, seq, True, obj)
            else:
                raise ValueError(f"invalid message type: {msg!r}")

    def serve(self, timeout=1, wait_for_lock=True, waiting=lambda: True):  # serving
        """Serves a single request or reply that arrives within the given
        time frame (default is 1 sec). Note that the dispatching of a request
        might trigger multiple (nested) requests, thus this function may be
        reentrant.

        :returns: ``True`` if a request or reply were received, ``False`` otherwise.
        """
        timeout = Timeout(timeout)
        if self._bind_threads:
            return self._serve_bound(timeout, wait_for_lock)
        with self._recv_event:
            # Exit early if we cannot acquire the recvlock
            if not self._recvlock.acquire(False):
                if wait_for_lock:
                    if not waiting():  # unlikely, but the result could've arrived and another thread could've won the race to acquire
                        return False
                    # Wait condition for recvlock release; recvlock is not underlying lock for condition
                    return self._recv_event.wait(timeout.timeleft())
                else:
                    return False
        if not waiting():  # the result arrived and we won the race to acquire, unlucky
            self._recvlock.release()
            with self._recv_event:
                self._recv_event.notify_all()
            return False
        # Assume the receive rlock is acquired and incremented
        # We must release once BEFORE dispatch, dispatch any data, and THEN notify all (see issue #527 and #449)
        try:
            data = None  # Ensure data is initialized
            data = self._channel.poll(timeout) and self._channel.recv()
        except Exception as exc:
            self._recvlock.release()
            if isinstance(exc, EOFError):
                self.close()  # sends close async request
            raise
        else:
            if data:
                self._dispatch(data)  # Dispatch will unbox, invoke callbacks, etc.
                return True
            else:
                self._recvlock.release()
                return False
        finally:
            with self._recv_event:
                self._recv_event.notify_all()

    def _serve_bound(self, timeout, wait_for_lock):
        """Serves messages like `serve` with the added benefit of making request/reply thread bound.
        - Experimental functionality `RPYC_BIND_THREADS`

        The first 8 bytes indicate the sending thread ID and intended recipient ID. When the recipient
        thread ID is not the thread that received the data, the remote thread ID and message are appended
        to the intended threads `_deque` and `_event` is set.

        :returns: ``True`` if a request or reply were received, ``False`` otherwise.
        """
        this_thread = self._get_thread()
        wait = False

        with self._lock:
            message_available = this_thread._event.is_set() and len(this_thread._deque) != 0

            if message_available:
                remote_thread_id, message = this_thread._deque.popleft()
                if len(this_thread._deque) == 0:
                    this_thread._event.clear()

            else:
                if self._receiving:  # enter pool
                    self._thread_pool.append(this_thread)
                    wait = True

                else:
                    self._receiving = True

        if message_available:  # just process
            this_thread._remote_thread_id = remote_thread_id
            self._dispatch(message)
            return True

        if wait:
            while True:
                if wait_for_lock:
                    this_thread._event.wait(timeout.timeleft())

                with self._lock:
                    if this_thread._event.is_set():
                        message_available = len(this_thread._deque) != 0

                        if message_available:
                            remote_thread_id, message = this_thread._deque.popleft()
                            if len(this_thread._deque) == 0:
                                this_thread._event.clear()

                        else:
                            this_thread._event.clear()

                            if self._receiving:  # another thread was faster
                                continue

                            self._receiving = True

                        self._thread_pool.remove(this_thread)  # leave pool
                        break

                    else:  # timeout
                        return False

            if message_available:
                this_thread._remote_thread_id = remote_thread_id
                self._dispatch(message)
                return True

        while True:
            # from upstream
            try:
                message = self._channel.poll(timeout) and self._channel.recv()

            except Exception as exception:
                if isinstance(exception, EOFError):
                    self.close()  # sends close async request

                with self._lock:
                    self._receiving = False

                    for thread in self._thread_pool:
                        thread._event.set()
                        break

                raise

            if not message:  # timeout; from upstream
                with self._lock:
                    for thread in self._thread_pool:
                        if not thread._event.is_set():
                            self._receiving = False
                            thread._event.set()
                            break

                    else:  # stop receiving
                        self._receiving = False

                return False

            remote_thread_id, local_thread_id = brine.I8I8.unpack(message[:16])
            message = message[16:]

            this = False

            if local_thread_id == UNBOUND_THREAD_ID and this_thread._occupation_count != 0:
                # Message is not meant for this thread. Use a thread that is not occupied or have the pool create a new one.
                # TODO: reusing threads may be problematic if occupation being zero is wrong...
                new = False
                with self._lock:
                    for thread in self._thread_pool:
                        if thread._occupation_count == 0 and not thread._event.is_set():
                            thread._deque.append((remote_thread_id, message))
                            thread._event.set()
                            break

                    else:
                        new = True

                if new:
                    self._thread_pool_executor.submit(self._serve_temporary, remote_thread_id, message)

            elif local_thread_id in {UNBOUND_THREAD_ID, this_thread.id}:
                # Of course, the message is for this thread if equal. When id is UNBOUND_THREAD_ID,
                # we deduce that occupation count is 0 from the previous if condition. So, set this True.
                this = True
            else:
                # Otherwise, message was meant for another thread.
                thread = self._get_thread(id=local_thread_id)
                with self._lock:
                    thread._deque.append((remote_thread_id, message))
                    thread._event.set()

            if this:
                with self._lock:
                    for thread in self._thread_pool:
                        if not thread._event.is_set():
                            self._receiving = False
                            thread._event.set()
                            break

                    else:  # stop receiving
                        self._receiving = False

                this_thread._remote_thread_id = remote_thread_id
                self._dispatch(message)
                return True

    def _serve_temporary(self, remote_thread_id, message):
        """Callable that is used to schedule serve as a new thread
        - Experimental functionality `RPYC_BIND_THREADS`

        :returns: None
        """
        thread = self._get_thread()
        thread._deque.append((remote_thread_id, message))
        thread._event.set()

        # from upstream
        try:
            while not self.closed:
                self.serve(None)

                if thread._occupation_count == 0:
                    break

        except (socket.error, select_error, IOError):
            if not self.closed:
                raise
        except EOFError:
            pass

    def _get_thread(self, id=None):
        """Get internal thread information for current thread for ID, when None use current thread.
        - Experimental functionality `RPYC_BIND_THREADS`

        :returns: _Thread
        """
        if id is None:
            id = threading.get_ident()

        thread = self._threads.get(id)
        if thread is None:
            thread = _Thread(id)
            self._threads[id] = thread

        return thread

    def poll(self, timeout=0):  # serving
        """Serves a single transaction, should one arrives in the given
        interval. Note that handling a request/reply may trigger nested
        requests, which are all part of a single transaction.

        :returns: ``True`` if a transaction was served, ``False`` otherwise"""
        return self.serve(timeout, False)

    def serve_all(self):  # serving
        """Serves all requests and replies for as long as the connection is
        alive."""
        try:
            while not self.closed:
                self.serve(None)
        except (socket.error, select_error, IOError):
            if not self.closed:
                raise
        except EOFError:
            pass
        finally:
            self.close()

    def serve_threaded(self, thread_count=10):  # serving
        """Serves all requests and replies for as long as the connection is alive.

        CAVEAT: using non-immutable types that require a netref to be constructed to serve a request,
        or invoking anything else that performs a sync_request, may timeout due to the sync_request reply being
        received by another thread serving the connection. A more conventional approach where each client thread
        opens a new connection would allow `ThreadedServer` to naturally avoid such multiplexing issues and
        is the preferred approach for threading procedures that invoke sync_request. See issue #345
        """
        def _thread_target():
            try:
                while True:
                    self.serve(None)
            except (socket.error, select_error, IOError):
                if not self.closed:
                    raise
            except EOFError:
                pass

        try:
            threads = [spawn(_thread_target)
                       for _ in range(thread_count)]

            for thread in threads:
                thread.join()
        finally:
            self.close()

    def poll_all(self, timeout=0):  # serving
        """Serves all requests and replies that arrive within the given interval.

        :returns: ``True`` if at least a single transaction was served, ``False`` otherwise
        """
        at_least_once = False
        timeout = Timeout(timeout)
        try:
            while True:
                if self.poll(timeout):
                    at_least_once = True
                if timeout.expired():
                    break
        except EOFError:
            pass
        return at_least_once

    def sync_request(self, handler, *args):
        """requests, sends a synchronous request (waits for the reply to arrive)

        :raises: any exception that the requests may be generated
        :returns: the result of the request
        """
        # ═══════════════════════════════════════════════════════════════
        # GUARD: forbid sync_request from the loop that serves this conn
        # ═══════════════════════════════════════════════════════════════
        # If this connection has asyncio serving enabled AND we are
        # currently running on the same event loop, a blocking wait
        # inside AsyncResult.wait()/Connection.serve() would stall — or
        # outright deadlock — the loop. That is exactly the class of bug
        # this project has already spent effort to eliminate
        # (NO POLLING POLICY + event-driven serving). Fail loudly with
        # instructions instead.
        #
        # Sync callers (threaded code, or the close path during process
        # teardown with no running loop) are unaffected — `get_running_loop`
        # raises RuntimeError in that case and we fall through.
        #
        # NOTE: intentionally NOT a heuristic. The invariant we check is
        # precise: "are we on the loop that owns this connection's FD?".
        # User-facing RPC (HANDLE_CALL / HANDLE_ASYNC_CALL) from the loop
        # that serves this connection is always wrong: the handler runs
        # remotely, round-trip time is unbounded, and a blocking wait here
        # stalls the whole event loop for arbitrary duration. Refuse with
        # a clear pointer to the async alternative.
        #
        # Protocol-level fast-path handlers (HANDLE_INSPECT, HANDLE_GETATTR,
        # HANDLE_SETATTR, HANDLE_DEL, HANDLE_CLOSE, ...) are NOT refused:
        # they are short, deterministic, cache-backed, and netref/proxy
        # construction fundamentally needs them during reply unboxing
        # (Connection._netref_factory issues HANDLE_INSPECT when creating
        # a proxy class the first time). Blocking loops for <1 ms on a
        # localhost protocol hop is a lesser evil than fragmenting the
        # whole netref layer into sync/async variants.
        _USER_RPC_HANDLERS = (consts.HANDLE_CALL, consts.HANDLE_ASYNC_CALL)
        if (
            self._asyncio_enabled
            and handler in _USER_RPC_HANDLERS
        ):
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            if running is not None and running is self._asyncio_loop:
                raise RuntimeError(
                    "sync_request() was called from the asyncio loop that "
                    "serves this connection for a user-level RPC (handler="
                    f"{handler!r}). This would block the loop for the full "
                    "remote round-trip and can deadlock.\n"
                    "Use an async alternative:\n"
                    "  * `await rpyc.async_(conn.root.method)(args)` "
                    "— async wrapper for a sync netref method\n"
                    "  * `await conn.root.async_method(args)` "
                    "— if the remote method is `async def`\n"
                    "  * `await conn.async_request(handler, *args)` "
                    "— generic async RPC\n"
                    "  * `await conn.aclose()` "
                    "— async close (instead of conn.close())\n"
                    "If you called `rpyc.connect()` from async code, "
                    "switch to `await rpyc.async_connect(...)`."
                )
        timeout = self._config["sync_request_timeout"]
        _async_res = self.async_request(handler, *args, timeout=timeout)
        # _async_res is an instance of AsyncResult, the value property invokes Connection.serve via AsyncResult.wait
        # So, the _recvlock can be acquired multiple times by the owning thread and warrants the use of RLock
        return _async_res.value

    async def aclose(self) -> None:
        """
        Asynchronously close this connection.

        The synchronous ``close()`` calls ``sync_request(HANDLE_CLOSE)``
        which blocks the loop — and, with the sync_request guard above,
        would actually raise when called from the loop that serves this
        connection. ``aclose()`` is the correct path from async code: it
        drains pending netref deletions asynchronously, sends HANDLE_CLOSE
        via ``async_request`` (awaitable, event-driven), and then runs
        local cleanup.

        Safe to call multiple times. Never blocks the event loop.
        """
        if self._closed:
            return
        # 1. Best-effort drain of pending netref deletions via the
        #    event-driven path before the connection goes down.
        try:
            await self._process_pending_deletions()
        except Exception:
            pass
        # 2. Send HANDLE_CLOSE as fire-and-forget.
        #    Why not await the reply: the server handler is `_handle_close`,
        #    which calls `_cleanup()` *before* the MSG_REPLY is emitted —
        #    so the reply typically never arrives. The sync `close()` path
        #    has always tolerated this (EOFError/TimeoutError suppressed).
        #    We replicate that behavior without ever blocking the loop.
        try:
            self._async_request(consts.HANDLE_CLOSE)
        except Exception:
            # If even the send fails (peer gone), just proceed to cleanup.
            if not self._config["close_catchall"]:
                raise
        # 3. Yield once so the MSG_REQUEST actually goes out on the wire
        #    before we tear the socket down.
        await asyncio.sleep(0)
        # 4. Local cleanup: disable asyncio serving (removes add_reader,
        #    stops cleanup task), mark closed, run the standard cleanup
        #    sequence. No I/O here.
        self._closed = True
        try:
            self.disable_asyncio_serving()
        except Exception:
            pass
        self._cleanup(_anyway=True)

    def _async_request(self, handler, args=(), callback=(lambda a, b: None)):  # serving
        seq = self._get_seq_id()
        self._request_callbacks[seq] = callback
        # Tell the AsyncResult its slot id so that ``__await__`` can
        # release the slot itself when the awaiter is cancelled. See
        # ``AsyncResult.__await__`` and the cancel-leak regression
        # test ``tests/test_asyncresult_cancel_leak.py``.
        if isinstance(callback, AsyncResult):
            callback._seq = seq
        try:
            self._send(consts.MSG_REQUEST, seq, (handler, self._box(args)))
        except Exception:
            # TODO: review test_remote_exception, logging exceptions show attempt to write on closed stream
            # depending on the case, the MSG_REQUEST may or may not have been sent completely
            # so, pop the callback and raise to keep response integrity is consistent
            self._request_callbacks.pop(seq, None)
            raise

    def async_request(self, handler, *args, **kwargs):  # serving
        """Send an asynchronous request (does not wait for it to finish)

        :returns: an :class:`rpyc.core.async_.AsyncResult` object, which will
                  eventually hold the result (or exception)
        """
        timeout = kwargs.pop("timeout", None)
        if kwargs:
            raise TypeError("got unexpected keyword argument(s) {list(kwargs.keys()}")
        res = AsyncResult(self)
        self._async_request(handler, args, res)
        if timeout is not None:
            res.set_expiry(timeout)
        return res

    @property
    def root(self):  # serving
        """Fetches the root object (service) of the other party"""
        if self._remote_root is None:
            self._remote_root = self.sync_request(consts.HANDLE_GETROOT)
        return self._remote_root

    def _check_attr(self, obj, name, perm):  # attribute access
        """
        Check if attribute access is allowed based on security configuration.

        Access is granted if ANY of these conditions are met:
        1. allow_all_attrs=True - allows all attributes
        2. allow_exposed_attrs=True and name starts with exposed_prefix (default: "exposed_")
        3. allow_safe_attrs=True and name is in safe_attrs list (magic methods)
        4. allow_public_attrs=True (default) and name doesn't start with "_"

        This allows natural usage of regular Python objects passed as netref arguments
        while still requiring Service classes to use 'exposed_' prefix for API methods.
        """
        config = self._config
        if not config[perm]:
            raise AttributeError(f"cannot access {name!r}")
        prefix = config["allow_exposed_attrs"] and config["exposed_prefix"]
        plain = config["allow_all_attrs"]
        plain |= config["allow_exposed_attrs"] and name.startswith(prefix)
        plain |= config["allow_safe_attrs"] and name in config["safe_attrs"]
        plain |= config["allow_public_attrs"] and not name.startswith("_")

        # ═══════════════════════════════════════════════════════════════════════════
        # CRITICAL FIX: Avoid hasattr() on netref objects to prevent recursion
        # ═══════════════════════════════════════════════════════════════════════════
        # When obj is a netref, hasattr(obj, attr) triggers __getattribute__() which
        # makes a remote call back to the server. If the server's _check_attr() also
        # uses hasattr(), this creates infinite recursion (observed in a downstream application).
        #
        # Solution: For netref objects, only use hasattr_static() which doesn't trigger
        # __getattribute__(). This breaks the recursion cycle.
        #
        # For regular (non-netref) objects, use hasattr() for exposed check and plain check.
        is_netref = isinstance(obj, netref.BaseNetref)

        if is_netref:
            # For netref: only use hasattr_static (no remote calls)
            has_exposed = prefix and hasattr_static(obj, prefix + name)
            # For netref: skip hasattr() check in plain validation (would cause recursion)
            # Instead, rely on the remote side to validate when the attribute is accessed
            if plain and not has_exposed:
                return name
        else:
            # For regular objects: use hasattr() as before (safe, no recursion)
            has_exposed = prefix and (hasattr(obj, prefix + name) or hasattr_static(obj, prefix + name))
            if plain and (not has_exposed or hasattr(obj, name)):
                return name

        if has_exposed:
            return prefix + name
        if plain:
            return name  # chance for better traceback
        raise AttributeError(f"cannot access {name!r}")

    def _access_attr(self, obj, name, args, overrider, param, default):  # attribute access
        if type(name) is bytes:
            name = str(name, "utf8")
        elif type(name) is not str:
            raise TypeError("name must be a string")
        accessor = getattr(type(obj), overrider, None)
        if accessor is None:
            accessor = default
            name = self._check_attr(obj, name, param)
        return accessor(obj, name, *args)

    @classmethod
    def _request_handlers(cls):  # request handlers
        from rpyc.core import async_handlers

        handlers = {
            consts.HANDLE_PING: cls._handle_ping,
            consts.HANDLE_CLOSE: cls._handle_close,
            consts.HANDLE_GETROOT: cls._handle_getroot,
            consts.HANDLE_GETATTR: cls._handle_getattr,
            consts.HANDLE_DELATTR: cls._handle_delattr,
            consts.HANDLE_SETATTR: cls._handle_setattr,
            consts.HANDLE_CALL: cls._handle_call,
            consts.HANDLE_CALLATTR: cls._handle_callattr,
            consts.HANDLE_REPR: cls._handle_repr,
            consts.HANDLE_STR: cls._handle_str,
            consts.HANDLE_CMP: cls._handle_cmp,
            consts.HANDLE_HASH: cls._handle_hash,
            consts.HANDLE_INSTANCECHECK: cls._handle_instancecheck,
            consts.HANDLE_DIR: cls._handle_dir,
            consts.HANDLE_PICKLE: cls._handle_pickle,
            consts.HANDLE_DEL: cls._handle_del,
            consts.HANDLE_INSPECT: cls._handle_inspect,
            consts.HANDLE_BUFFITER: cls._handle_buffiter,
            consts.HANDLE_OLDSLICING: cls._handle_oldslicing,
            consts.HANDLE_CTXEXIT: cls._handle_ctxexit,
            # ═══════════════════════════════════════════════════
            # Async Handlers (v5.1)
            # ═══════════════════════════════════════════════════
            consts.HANDLE_ASYNC_CALL: async_handlers._handle_async_call,
            consts.HANDLE_ASYNC_CALLATTR: async_handlers._handle_async_callattr,
        }
        return handlers

    def _handle_ping(self, data):  # request handler
        return data

    def _handle_close(self):  # request handler
        self._cleanup()

    def _handle_getroot(self):  # request handler
        return self._local_root

    def _handle_del(self, obj, count=1):  # request handler
        """
        Handle netref deletion request.

        Returns:
            bool: ``True`` if the object was fully deleted (refcount
            reached zero), ``False`` otherwise.

        ═══════════════════════════════════════════════════════════════
        ⚠  REGRESSION WARNING — RETURN VALUE MUST BE BRINE-PRIMITIVE  ⚠
        ═══════════════════════════════════════════════════════════════
        Post-mortem: ``docs/DESIGN_BIDIRECTIONAL_ASYNC_FIXES.md`` §3
        (cleanup-loop self-deadlock).

        The return value is sent back over the wire as the reply to
        ``HANDLE_DEL``. The CALLER is
        ``_async_request_with_ack`` inside the background
        ``cleanup_loop`` task, which does:

            result = await self._async_request_with_ack(
                consts.HANDLE_DEL, id_pack, total_refcount, ...
            )
            if not result:
                # log cleanup failure
                ...

        If this handler returns anything that is NOT
        ``brine.dumpable`` (e.g. a ``dict`` or ``list``), the boxing
        layer will wrap it as ``LABEL_REMOTE_REF`` — i.e. the caller
        receives a **netref**, not a plain value. Then the innocent-
        looking ``if not result:`` fires ``bool(netref)`` which
        triggers a ``HANDLE_CALLATTR('__bool__')`` **synchronous**
        RPC back to this side — on the SAME event loop that is
        currently draining another netref deletion and has no room
        to service its own inbound reply.

        This is the cleanup-loop self-deadlock (stack-snapshot
        verified, see design doc §3): under recursive bidirectional
        async traffic, both peers end up parked in
        ``cleanup_loop → _process_pending_deletions → syncreq →
        stream.write``, waiting for each other to drain. Nothing
        moves. Pytest hits its timeout.

        ``bool`` is ``brine.dumpable`` (brine supports primitives:
        ``int``, ``float``, ``str``, ``bytes``, ``bool``, ``None``,
        ``tuple``-of-dumpable). The reply goes as ``LABEL_VALUE``,
        caller receives a plain Python bool, ``if not result:`` is a
        local C-level check — no RPC, no deadlock.

        DO NOT "enhance" this return type with a dict, a
        dataclass, or a named tuple. If you need to return multiple
        values, use a ``tuple`` of primitives (brine-dumpable) OR
        define a new separate handler. The caller's truth-test MUST
        remain local.
        ═══════════════════════════════════════════════════════════════
        """
        # obj is already an id_pack tuple, don't call get_id_pack() on it.
        id_pack = obj
        deleted = self._local_objects.decref(id_pack, count)

        if deleted:
            # variant A: free the stable-seq entry too. WeakKeyDictionary
            # cleans itself up; the id-fallback dict needs the explicit
            # scan. See docs/DESIGN_REFCOUNT_RACE_FIX_A.md §3.
            self._forget_stable_obj_id(id_pack)

        # Explicit ``bool(...)`` cast to guarantee the return type
        # stays primitive even if ``RefCountingColl.decref`` ever
        # evolves to return something truthy-but-non-bool.
        return bool(deleted)

    def _handle_repr(self, obj):  # request handler
        return repr(obj)

    def _handle_str(self, obj):  # request handler
        return str(obj)

    def _handle_cmp(self, obj, other, op='__cmp__'):  # request handler
        # cmp() might enter recursive resonance... so use the underlying type and return cmp(obj, other)
        try:
            return self._access_attr(type(obj), op, (), "_rpyc_getattr", "allow_getattr", getattr)(obj, other)
        except Exception:
            raise

    def _handle_hash(self, obj):  # request handler
        return hash(obj)

    def _handle_call(self, obj, args, kwargs=()):  # request handler
        return obj(*args, **dict(kwargs))

    def _handle_dir(self, obj):  # request handler
        return tuple(dir(obj))

    def _handle_inspect(self, id_pack):  # request handler
        # Check if object exists in _local_objects
        try:
            obj = self._local_objects[id_pack]
        except KeyError:
            raise ValueError(
                f"Cannot inspect object {id_pack}: not found in _local_objects. "
                "Object may have been garbage collected or removed via premature decref."
            ) from None

        if hasattr(obj, '____conn__'):
            # When RPyC is chained (RPyC over RPyC), id_pack is cached in local objects as a netref
            # since __mro__ is not a safe attribute the request is forwarded using the proxy connection
            # see issue #346 or tests.test_rpyc_over_rpyc.Test_rpyc_over_rpyc
            conn = obj.____conn__
            return conn.sync_request(consts.HANDLE_INSPECT, id_pack)
        else:
            return tuple(get_methods(netref.LOCAL_ATTRS, obj))

    def _handle_getattr(self, obj, name):  # request handler
        return self._access_attr(obj, name, (), "_rpyc_getattr", "allow_getattr", getattr)

    def _handle_delattr(self, obj, name):  # request handler
        return self._access_attr(obj, name, (), "_rpyc_delattr", "allow_delattr", delattr)

    def _handle_setattr(self, obj, name, value):  # request handler
        return self._access_attr(obj, name, (value,), "_rpyc_setattr", "allow_setattr", setattr)

    def _handle_callattr(self, obj, name, args, kwargs=()):  # request handler
        obj = self._handle_getattr(obj, name)
        return self._handle_call(obj, args, kwargs)

    def _handle_ctxexit(self, obj, exc):  # request handler
        if exc:
            try:
                raise exc
            except Exception:
                exc, typ, tb = sys.exc_info()
        else:
            typ = tb = None
        return self._handle_getattr(obj, "__exit__")(exc, typ, tb)

    def _handle_instancecheck(self, obj, other_id_pack):
        # TODOs:
        #  + refactor cache instancecheck/inspect/class_factory
        #  + improve cache docs

        if hasattr(obj, '____conn__'):  # keep unwrapping!
            # When RPyC is chained (RPyC over RPyC), id_pack is cached in local objects as a netref
            # since __mro__ is not a safe attribute the request is forwarded using the proxy connection
            # relates to issue #346 or tests.test_netref_hierachy.Test_Netref_Hierarchy.test_StandardError
            conn = obj.____conn__
            return conn.sync_request(consts.HANDLE_INSPECT, other_id_pack)
        # Create a name pack which would be familiar here and see if there is a hit
        other_id_pack2 = (other_id_pack[0], other_id_pack[1], 0)
        if other_id_pack[0] in netref.builtin_classes_cache:
            cls = netref.builtin_classes_cache[other_id_pack[0]]
            other = cls(self, other_id_pack)
        elif other_id_pack2 in self._netref_classes_cache:
            cls = self._netref_classes_cache[other_id_pack2]
            other = cls(self, other_id_pack2)
        else:  # might just have missed cache, FIX ME
            return False
        return isinstance(other, obj)

    def _handle_pickle(self, obj, proto):  # request handler
        if not self._config["allow_pickle"]:
            raise ValueError("pickling is disabled")
        return bytes(pickle.dumps(obj, proto))

    def _handle_buffiter(self, obj, count):  # request handler
        return tuple(itertools.islice(obj, count))

    def _handle_oldslicing(self, obj, attempt, fallback, start, stop, args):  # request handler
        try:
            # first try __xxxitem__
            getitem = self._handle_getattr(obj, attempt)
            return getitem(slice(start, stop), *args)
        except Exception:
            # fallback to __xxxslice__. see issue #41
            if stop is None:
                stop = maxint
            getslice = self._handle_getattr(obj, fallback)
            return getslice(start, stop, *args)


class _Thread:
    """Internal thread information for the RPYC protocol used for thread binding."""

    def __init__(self, id):
        super().__init__()

        self.id = id

        self._remote_thread_id = UNBOUND_THREAD_ID
        self._occupation_count = 0
        self._event = threading.Event()
        self._deque = collections.deque()
