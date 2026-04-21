from __future__ import with_statement
import weakref
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import logging


class WeakValueDict(object):
    """a light-weight version of weakref.WeakValueDictionary"""
    __slots__ = ("_dict",)

    def __init__(self):
        self._dict = {}

    def __repr__(self):
        return repr(self._dict)

    def __iter__(self):
        return self.iterkeys()

    def __len__(self):
        return len(self._dict)

    def __contains__(self, key):
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __getitem__(self, key):
        obj = self._dict[key]()
        if obj is None:
            raise KeyError(key)
        return obj

    def __setitem__(self, key, value):
        def remover(wr, _dict=self._dict, key=key):
            _dict.pop(key, None)
        self._dict[key] = weakref.ref(value, remover)

    def __delitem__(self, key):
        del self._dict[key]

    def iterkeys(self):
        return self._dict.keys()

    def keys(self):
        return self._dict.keys()

    def itervalues(self):
        for k in self:
            yield self[k]

    def values(self):
        return list(self.itervalues())

    def iteritems(self):
        for k in self:
            yield k, self[k]

    def items(self):
        return list(self.iteritems())

    def clear(self):
        self._dict.clear()


class RefCountingColl(object):
    """
    A set-like object that implements refcounting on its contained objects.

    New behavior (v5.2):
    - Initial refcount is 1 (registry acts as strong reference)
    - decref() returns deletion status (True if deleted, False if still alive)
    - Defensive checks prevent KeyError on missing keys
    """
    __slots__ = ("_lock", "_dict", "_logger", "_debug")

    def __init__(self, logger: Optional["logging.Logger"] = None, debug: bool = False) -> None:
        self._lock: Lock = Lock()
        # Dict structure: {id_pack: [object, refcount]}
        self._dict: Dict[Tuple[str, int, int], List[Any]] = {}
        self._logger: Optional["logging.Logger"] = logger
        self._debug: bool = debug

    def __repr__(self):
        return repr(self._dict)

    def add(self, key: Tuple[str, int, int], obj: Any) -> None:
        """
        Add object to refcounting collection.

        New behavior (v5.2):
        - Initial refcount is 1 (registry acts as strong reference)
        - Subsequent adds increment refcount

        v5.3 (variant A-lite, see docs/DESIGN_REFCOUNT_RACE_FIX.md):
        - If the slot already exists but stores a DIFFERENT Python object
          (``slot[0] is not obj``), treat this as an ``id()`` collision
          — CPython recycled the id() after the original object was
          evicted. Replace the slot fresh instead of incrementing the
          refcount of the wrong object. The refcount for the stale
          id_pack is intentionally dropped; nothing can reach the old
          object through id_pack any more, so its prior refcount is
          already meaningless.

        Args:
            key: Object identifier (class_name, class_id, object_id)
            obj: Object to store
        """
        with self._lock:
            slot = self._dict.get(key, None)
            if slot is None:
                # NEW: Initial refcount is 1 (registry counts as reference)
                slot = [obj, 1]
                if self._debug and self._logger:
                    try:
                        obj_repr = repr(obj)
                        # Truncate very long reprs
                        if len(obj_repr) > 200:
                            obj_repr = obj_repr[:200] + "..."
                    except Exception:
                        obj_repr = f"<{type(obj).__name__} at {id(obj):#x}>"
                    self._logger.debug(f"[REFCOUNT] ADD {key} -> {obj_repr} (refcount=1)")
            elif slot[0] is not obj:
                # id() COLLISION — different Python object landed at the
                # same address after the original was evicted. Reset the
                # slot to the new object with a fresh refcount of 1.
                # See docs/DESIGN_REFCOUNT_RACE_FIX.md §A-lite.
                if self._logger:
                    self._logger.warning(
                        "[REFCOUNT] id() COLLISION on %s: slot was bound "
                        "to %r (refcount=%d), rebinding to %r.",
                        key, type(slot[0]).__name__, slot[1],
                        type(obj).__name__,
                    )
                slot = [obj, 1]
            else:
                slot[1] += 1
                if self._debug and self._logger:
                    try:
                        obj_repr = repr(obj)
                        if len(obj_repr) > 200:
                            obj_repr = obj_repr[:200] + "..."
                    except Exception:
                        obj_repr = f"<{type(obj).__name__} at {id(obj):#x}>"
                    self._logger.debug(f"[REFCOUNT] INCREF {key} -> {obj_repr} (refcount={slot[1]})")
            self._dict[key] = slot

    def clear(self):
        with self._lock:
            self._dict.clear()

    def decref(self, key: Tuple[str, int, int], count: int = 1) -> bool:
        """
        Decrement refcount for object.

        New behavior (v5.2):
        - Returns True if object was deleted (refcount reached 0)
        - Returns False if object still alive (refcount > 0)
        - Returns False if key not found (defensive, no KeyError)

        Args:
            key: Object identifier
            count: Amount to decrement (default 1)

        Returns:
            True if object deleted, False otherwise
        """
        with self._lock:
            # NEW: Defensive check - return False if key not found
            if key not in self._dict:
                # ═══════════════════════════════════════════════════════════════
                # CRITICAL: DO NOT REMOVE OR MODIFY THIS LOGGING!
                # ═══════════════════════════════════════════════════════════════
                # This error indicates a serious bug in refcount management:
                # - Race condition in netref lifecycle
                # - Double deletion attempt
                # - Cleanup desynchronization between client/server
                #
                # This MUST be logged to stderr ALWAYS, regardless of logger config.
                # Silently ignoring this error leads to:
                # - Hidden memory leaks
                # - Difficult-to-debug production issues
                # - Accumulated technical debt
                #
                # If you think this logging is "too verbose", you are wrong.
                # Fix the underlying bug instead of hiding the symptom.
                # ═══════════════════════════════════════════════════════════════
                import sys
                print(f"WARNING: [REFCOUNT] DECREF on missing key {key}", file=sys.stderr)
                if self._logger:
                    self._logger.warning(f"[REFCOUNT] DECREF on missing key {key}")
                return False

            slot = self._dict[key]
            slot[1] -= count

            # Check if refcount reached 0 or below
            if slot[1] <= 0:
                if self._debug and self._logger:
                    try:
                        obj_repr = repr(slot[0])
                        if len(obj_repr) > 200:
                            obj_repr = obj_repr[:200] + "..."
                    except Exception:
                        obj_repr = f"<{type(slot[0]).__name__} at {id(slot[0]):#x}>"
                    self._logger.debug(
                        f"[REFCOUNT] DELETE {key} -> {obj_repr} "
                        f"(refcount was {slot[1] + count}, decref by {count})"
                    )
                del self._dict[key]
                return True  # Object deleted
            else:
                if self._debug and self._logger:
                    try:
                        obj_repr = repr(slot[0])
                        if len(obj_repr) > 200:
                            obj_repr = obj_repr[:200] + "..."
                    except Exception:
                        obj_repr = f"<{type(slot[0]).__name__} at {id(slot[0]):#x}>"
                    self._logger.debug(f"[REFCOUNT] DECREF {key} -> {obj_repr} (refcount={slot[1]})")
                self._dict[key] = slot
                return False  # Object still alive

    def __getitem__(self, key: Tuple[str, int, int]) -> Any:
        """Get object by key"""
        with self._lock:
            return self._dict[key][0]
