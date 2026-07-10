"""*NetRef*: a transparent *network reference*. This module contains quite a lot
of *magic*, so beware.
"""
import sys
import types
from rpyc_async.lib import get_methods, get_id_pack
from rpyc_async.lib.compat import pickle, maxint
from rpyc_async.core import consts


builtin_id_pack_cache = {}  # name_pack -> id_pack
builtin_classes_cache = {}  # id_pack -> class
# If these can be accessed, numpy will try to load the array from local memory,
# resulting in exceptions and/or segfaults, see #236:
DELETED_ATTRS = frozenset([
    '__array_struct__', '__array_interface__',
])

"""the set of attributes that are local to the netref object"""
LOCAL_ATTRS = frozenset([
    '____conn__', '____id_pack__', '____refcount__', '____is_async__', '__class__', '__cmp__', '__del__', '__delattr__',
    '__dir__', '__doc__', '__getattr__', '__getattribute__', '__hash__', '__instancecheck__',
    '__init__', '__metaclass__', '__module__', '__new__', '__reduce__',
    '__reduce_ex__', '__repr__', '__setattr__', '__slots__', '__str__',
    '__weakref__', '__dict__', '__methods__', '__exit__',
    '__eq__', '__ne__', '__lt__', '__gt__', '__le__', '__ge__',
]) | DELETED_ATTRS

"""a list of types considered built-in (shared between connections)
this is needed because iterating the members of the builtins module is not enough,
some types (e.g NoneType) are not members of the builtins module.
TODO: this list is not complete.
"""
_builtin_types = [
    type, object, bool, complex, dict, float, int, list, slice, str, tuple, set,
    frozenset, BaseException, Exception, type(None), types.BuiltinFunctionType, types.GeneratorType,
    types.MethodType, types.CodeType, types.FrameType, types.TracebackType,
    types.ModuleType, types.FunctionType, types.MappingProxyType,

    type(int.__add__),      # wrapper_descriptor
    type((1).__add__),      # method-wrapper
    type(iter([])),         # listiterator
    type(iter(())),         # tupleiterator
    type(iter(set())),      # setiterator
    bytes, bytearray, type(iter(range(10))), memoryview
]
_normalized_builtin_types = {}


def syncreq(proxy, handler, *args):
    """Performs a synchronous request on the given proxy object.
    Not intended to be invoked directly.

    :param proxy: the proxy on which to issue the request
    :param handler: the request handler (one of the ``HANDLE_XXX`` members of
                    ``rpyc.protocol.consts``)
    :param args: arguments to the handler

    :raises: any exception raised by the operation will be raised
    :returns: the result of the operation
    """
    conn = object.__getattribute__(proxy, "____conn__")
    return conn.sync_request(handler, proxy, *args)


def asyncreq(proxy, handler, *args):
    """Performs an asynchronous request on the given proxy object.
    Not intended to be invoked directly.

    :param proxy: the proxy on which to issue the request
    :param handler: the request handler (one of the ``HANDLE_XXX`` members of
                    ``rpyc.protocol.consts``)
    :param args: arguments to the handler

    :returns: an :class:`~rpyc_async.core.async_.AsyncResult` representing
              the operation
    """
    conn = object.__getattribute__(proxy, "____conn__")
    return conn.async_request(handler, proxy, *args)


class NetrefMetaclass(type):
    """A *metaclass* used to customize the ``__repr__`` of ``netref`` classes.
    It is quite useless, but it makes debugging and interactive programming
    easier"""
    __slots__ = ()

    def __repr__(self):
        if self.__module__:
            return f"<netref class '{self.__module__}.{self.__name__}'>"
        else:
            return f"<netref class '{self.__name__}'>"


class BaseNetref(object, metaclass=NetrefMetaclass):
    """The base netref class, from which all netref classes derive. Some netref
    classes are "pre-generated" and cached upon importing this module (those
    defined in the :data:`_builtin_types`), and they are shared between all
    connections.

    The rest of the netref classes are created by :meth:`rpyc_async.core.protocol.Connection._unbox`,
    and are private to the connection.

    Do not use this class directly; use :func:`class_factory` instead.

    :param conn: the :class:`rpyc_async.core.protocol.Connection` instance
    :param id_pack: id tuple for an object ~ (name_pack, remote-class-id, remote-instance-id)
        (cont.) name_pack := __module__.__name__ (hits or misses on builtin cache and sys.module)
                remote-class-id := id of object class (hits or misses on netref classes cache and instance checks)
                remote-instance-id := id object instance (hits or misses on proxy cache)
        id_pack is usually created by rpyc_async.lib.get_id_pack
    """
    __slots__ = [
        "____conn__",
        "____id_pack__",
        "__weakref__",
        "____refcount__",
        "____is_async__",
        "_refcount_holder",     # NEW (v5.2): For cleanup callback
        "_cleanup_connection"   # NEW (v5.2): Reference to connection
    ]

    def __init__(self, conn, id_pack):
        object.__setattr__(self, "____conn__", conn)
        object.__setattr__(self, "____id_pack__", id_pack)
        object.__setattr__(self, "____refcount__", 1)
        object.__setattr__(self, "____is_async__", False)  # NEW (v5.1): Set by _unbox() if FLAGS_ASYNC
        # NEW (v5.2): These will be set by _unbox() if cleanup callback is used
        object.__setattr__(self, "_refcount_holder", None)
        object.__setattr__(self, "_cleanup_connection", None)

    def __del__(self) -> None:
        """
        Netref destructor (v5.2 - Phase 3: Single Mechanism).

        Queue deletion for background cleanup. Cleanup callback is ALWAYS registered
        by _unbox() for all netrefs. No fallback mechanism exists.

        The deletion is queued in _pending_deletions and processed by background
        cleanup task (if asyncio enabled) or during connection close.
        """
        try:
            # Get cleanup callback (MUST be present - registered by _unbox)
            cleanup_conn = object.__getattribute__(self, "_cleanup_connection")
            refcount_holder = object.__getattribute__(self, "_refcount_holder")

            if cleanup_conn is not None and refcount_holder is not None:
                # Queue deletion for background processing (ONLY mechanism).
                # Uses _enqueue_deletion (not bare _pending_deletions.put) so
                # the background cleanup task is woken via its asyncio.Event.
                # The task does NOT poll — it sleeps on the event until we
                # signal. See NO-POLLING policy in protocol.py.
                refcount_holder["refcount"] = self.____refcount__
                cleanup_conn._enqueue_deletion(
                    refcount_holder["id_pack"],
                    self.____refcount__,
                )
            else:
                # This should NEVER happen - cleanup callback always registered
                # If this occurs, it's a critical bug in _unbox()
                import sys
                try:
                    id_pack = object.__getattribute__(self, "____id_pack__")
                    print(
                        f"[NETREF_CRITICAL] Netref {id_pack} missing cleanup callback! "
                        f"This is a BUG in _unbox(). Object will LEAK on remote side.",
                        file=sys.stderr
                    )
                except:
                    print(
                        "[NETREF_CRITICAL] Netref missing cleanup callback and id_pack! "
                        "Object will LEAK on remote side.",
                        file=sys.stderr
                    )
        except Exception:
            # Errors in __del__ cannot be raised (Python limitation)
            # This catches exceptions during queue.put() or attribute access
            # Most likely during program termination when connection is closed
            pass

    def __getattribute__(self, name):
        if name in LOCAL_ATTRS:
            if name == "__class__":
                cls = object.__getattribute__(self, "__class__")
                if cls is None:
                    cls = self.__getattr__("__class__")
                return cls
            elif name == "__doc__":
                return self.__getattr__("__doc__")
            elif name in DELETED_ATTRS:
                raise AttributeError()
            else:
                return object.__getattribute__(self, name)
        elif name == "__call__":                          # IronPython issue #10
            return object.__getattribute__(self, "__call__")
        elif name == "__array__":
            return object.__getattribute__(self, "__array__")
        else:
            return syncreq(self, consts.HANDLE_GETATTR, name)

    def __getattr__(self, name):
        if name in DELETED_ATTRS:
            raise AttributeError()
        return syncreq(self, consts.HANDLE_GETATTR, name)

    def __delattr__(self, name):
        if name in LOCAL_ATTRS:
            object.__delattr__(self, name)
        else:
            syncreq(self, consts.HANDLE_DELATTR, name)

    def __setattr__(self, name, value):
        if name in LOCAL_ATTRS:
            object.__setattr__(self, name, value)
        else:
            syncreq(self, consts.HANDLE_SETATTR, name, value)

    def __dir__(self):
        return list(syncreq(self, consts.HANDLE_DIR))

    # support for metaclasses
    def __hash__(self):
        return syncreq(self, consts.HANDLE_HASH)

    def __cmp__(self, other):
        return syncreq(self, consts.HANDLE_CMP, other, '__cmp__')

    def __eq__(self, other):
        return syncreq(self, consts.HANDLE_CMP, other, '__eq__')

    def __ne__(self, other):
        return syncreq(self, consts.HANDLE_CMP, other, '__ne__')

    def __lt__(self, other):
        return syncreq(self, consts.HANDLE_CMP, other, '__lt__')

    def __gt__(self, other):
        return syncreq(self, consts.HANDLE_CMP, other, '__gt__')

    def __le__(self, other):
        return syncreq(self, consts.HANDLE_CMP, other, '__le__')

    def __ge__(self, other):
        return syncreq(self, consts.HANDLE_CMP, other, '__ge__')

    def __repr__(self):
        return syncreq(self, consts.HANDLE_REPR)

    def __str__(self):
        return syncreq(self, consts.HANDLE_STR)

    def __exit__(self, exc, typ, tb):
        return syncreq(self, consts.HANDLE_CTXEXIT, exc)  # can't pass type nor traceback

    def __reduce_ex__(self, proto):
        # support for pickling netrefs
        return pickle.loads, (syncreq(self, consts.HANDLE_PICKLE, proto),)

    def __instancecheck__(self, other):
        # support for checking cached instances across connections
        if isinstance(other, BaseNetref):
            if self.____id_pack__[2] != 0:
                raise TypeError("isinstance() arg 2 must be a class, type, or tuple of classes and types")
            elif self.____id_pack__[1] == other.____id_pack__[1]:
                if other.____id_pack__[2] == 0:
                    return False
                elif other.____id_pack__[2] != 0:
                    return True
            else:
                # seems dubious if each netref proxies to a different address spaces
                return syncreq(self, consts.HANDLE_INSTANCECHECK, other.____id_pack__)
        else:
            if self.____id_pack__[2] == 0:
                # outside the context of `__instancecheck__`, `__class__` is expected to be type(self)
                # within the context of `__instancecheck__`, `other` should be compared to the proxied class
                return isinstance(other, type(self).__dict__['__class__'].instance)
            else:
                raise TypeError("isinstance() arg 2 must be a class, type, or tuple of classes and types")


def _make_method(name, doc):
    """creates a method with the given name and docstring that invokes
    :func:`syncreq` on its `self` argument"""

    slicers = {"__getslice__": "__getitem__", "__delslice__": "__delitem__", "__setslice__": "__setitem__"}

    name = str(name)                                      # IronPython issue #10
    if name == "__call__":
        def __call__(_self, *args, **kwargs):
            kwargs = tuple(kwargs.items())

            # ═══════════════════════════════════════════════════════════
            # NEW (v5.1): Async Detection
            # ═══════════════════════════════════════════════════════════
            # Check if this is an async function (set by _unbox)
            is_async = getattr(_self, '____is_async__', False)

            if is_async:
                # Use async request - returns AsyncResult (awaitable!)
                return asyncreq(_self, consts.HANDLE_ASYNC_CALL, args, kwargs)
            else:
                # Use sync handler (existing behavior)
                return syncreq(_self, consts.HANDLE_CALL, args, kwargs)

        __call__.__doc__ = doc
        return __call__
    elif name in slicers:                                 # 32/64 bit issue #41
        def method(self, start, stop, *args):
            if stop == maxint:
                stop = None
            return syncreq(self, consts.HANDLE_OLDSLICING, slicers[name], name, start, stop, args)
        method.__name__ = name
        method.__doc__ = doc
        return method
    elif name == "__array__":
        def __array__(self, *args, **kwargs):
            # Note that protocol=-1 will only work between python
            # interpreters of the same version.
            if not object.__getattribute__(self,'____conn__')._config["allow_pickle"]:
                # Security check that server side allows pickling per #551
                raise ValueError("pickling is disabled")
            array = pickle.loads(syncreq(self, consts.HANDLE_PICKLE, -1))
            return array.__array__(*args, **kwargs)
        __array__.__doc__ = doc
        return __array__
    else:
        def method(_self, *args, **kwargs):
            kwargs = tuple(kwargs.items())
            return syncreq(_self, consts.HANDLE_CALLATTR, name, args, kwargs)
        method.__name__ = name
        method.__doc__ = doc
        return method


class NetrefClass(object):
    """a descriptor of the class being proxied

    Future considerations:
     + there may be a cleaner alternative but lib.compat.with_metaclass prevented using __new__
     + consider using __slot__ for this class
     + revisit the design choice to use properties here
    """

    def __init__(self, class_obj):
        self._class_obj = class_obj

    @property
    def instance(self):
        """accessor to class object for the instance being proxied"""
        return self._class_obj

    @property
    def owner(self):
        """accessor to the class object for the instance owner being proxied"""
        return self._class_obj.__class__

    def __get__(self, netref_instance, netref_owner):
        """the value returned when accessing the netref class is dictated by whether or not an instance is proxied"""
        return self.owner if netref_instance.____id_pack__[2] == 0 else self.instance


def class_factory(id_pack, methods):
    """Creates a netref class proxying the given class

    :param id_pack: the id pack used for proxy communication
    :param methods: a list of ``(method name, docstring)`` tuples, of the methods that the class defines

    :returns: a netref class
    """
    ns = {"__slots__": (), "__class__": None}
    name_pack = id_pack[0]
    class_descriptor = None
    if name_pack is not None:
        # attempt to resolve __class__ using normalized builtins first
        _builtin_class = _normalized_builtin_types.get(name_pack)
        if _builtin_class is not None:
            class_descriptor = NetrefClass(_builtin_class)
        # then by imported modules (this also tries all builtins under "builtins")
        else:
            _module = None
            cursor = len(name_pack)
            while cursor != -1:
                _module = sys.modules.get(name_pack[:cursor])
                if _module is None:
                    cursor = name_pack[:cursor].rfind('.')
                    continue
                _class_name = name_pack[cursor + 1:]
                _class = getattr(_module, _class_name, None)
                if _class is not None and hasattr(_class, '__class__'):
                    class_descriptor = NetrefClass(_class)
                elif _class is None:
                    class_descriptor = NetrefClass(type(_module))
                break
    ns['__class__'] = class_descriptor
    # create methods that must perform a syncreq
    for name, doc in methods:
        name = str(name)  # IronPython issue #10
        # only create methods that won't shadow BaseNetref during merge for mro
        if name not in LOCAL_ATTRS:  # i.e. `name != __class__`
            ns[name] = _make_method(name, doc)
    netref_cls = type(name_pack, (BaseNetref, ), ns)
    return netref_cls


for _builtin in _builtin_types:
    _id_pack = get_id_pack(_builtin)
    _name_pack = _id_pack[0]
    _normalized_builtin_types[_name_pack] = _builtin
    _builtin_methods = get_methods(LOCAL_ATTRS, _builtin)
    # assume all normalized builtins are classes
    builtin_classes_cache[_name_pack] = class_factory(_id_pack, _builtin_methods)
