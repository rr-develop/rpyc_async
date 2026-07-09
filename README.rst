|Tests| |Python| |License|

rpyc-async
==========

**rpyc-async** is an asyncio-native fork of RPyC_ (*Remote Python Call*), a
**transparent** library for **symmetrical** `remote procedure calls`_,
clustering_, and distributed-computing_.  RPyC makes use of object-proxying_,
a technique that employs python's dynamic nature, to overcome the physical
boundaries between processes and computers, so that remote objects can be
manipulated as if they were local.

This fork adds native ``async``/``await`` support: async services, async
clients, and bidirectional async callbacks driven by a persistent event loop.

.. warning::

   **rpyc-async is not a drop-in replacement for classic synchronous RPyC.**
   It is a separate distribution with its own version line, built around
   ``async_connect()`` and ``AsyncioServer``. Compatibility with upstream RPyC
   is **not guaranteed**, at neither the API nor the wire-protocol level.
   Both peers must run ``rpyc-async``.

Relationship to upstream RPyC
-----------------------------

====================  ===================================================
Upstream project      RPyC_ by Tomer Filiba and contributors
Forked at             RPyC 6.0.1
This fork             ``rpyc-async`` 1.0.0 (versioned independently)
Distribution name     ``rpyc-async``
Import name           ``rpyc`` (unchanged)
Licence               MIT (both upstream and this fork)
====================  ===================================================

The upstream commit history is preserved in this repository, so authorship and
provenance of the original RPyC code remain visible in ``git log``. See LICENSE_
and CONTRIBUTORS_ for attribution.

Installation
------------

::

    pip install rpyc-async

Requires **Python 3.10+**. Because both distributions provide the ``rpyc``
import name, install only one of them per environment. If you need the classic
synchronous behaviour, install upstream RPyC instead (``pip install rpyc``).

Server Selection Guide
======================

``rpyc-async`` provides two main server implementations. Choose based on your
async requirements:

+-------------------+---------------------------+------------------------------------+
| Server Type       | Use When                  | Limitations                        |
+===================+===========================+====================================+
| **ThreadedServer**| - Synchronous methods     | - **Cannot** support bidirectional |
|                   | - Unidirectional async    |   async (async callbacks)          |
|                   |   (client→server only)    | - Async methods without persistent |
|                   | - Simple use cases        |   event loop will raise error      |
+-------------------+---------------------------+------------------------------------+
| **AsyncioServer** | - Bidirectional async     | - Requires asyncio event loop      |
|                   | - Async callbacks         | - More complex setup               |
|                   | - Server calling client   |                                    |
|                   |   async methods           |                                    |
+-------------------+---------------------------+------------------------------------+

Quick Examples
--------------

**ThreadedServer** (for simple sync/unidirectional async)::

    from rpyc import ThreadedServer, Service

    class MyService(Service):
        def exposed_sync_method(self, x):
            return x * 2

    server = ThreadedServer(MyService, port=18861)
    server.start()

**AsyncioServer** (for bidirectional async with callbacks)::

    from rpyc import AsyncioServer, Service
    import asyncio

    class MyService(Service):
        async def exposed_async_with_callback(self, callback, value):
            # Server can call client's async callback
            result = await callback(value * 2)
            return f"Got: {result}"

    async def main():
        server = AsyncioServer(MyService, port=18861)
        await server.start()
        # Server runs in background, handle other tasks
        await asyncio.sleep(3600)  # or other async work

    asyncio.run(main())

**Client for AsyncioServer** — use ``await rpyc.async_connect(...)``, NOT
``rpyc.connect()``::

    import rpyc, asyncio

    async def main():
        conn = await rpyc.async_connect("localhost", 18861)
        try:
            # Native async method — just await.
            result = await conn.root.some_async_method(42)
            # Sync remote method? Wrap with rpyc.async_() to stay event-driven.
            sync_result = await rpyc.async_(conn.root.some_sync_method)(42)
        finally:
            await conn.aclose()

    asyncio.run(main())

``rpyc.connect()`` is **synchronous** — it blocks ``socket.connect`` and
every later ``sync_request`` on the calling thread. From inside an asyncio
event loop it now raises ``RuntimeError`` pointing at
``rpyc.async_connect``. See ``docs/DESIGN_ASYNC_CONNECT_POLICY.md``.

For more details on async support, see ``docs/LIMITATIONS.md``.

.. note::

   **NO POLLING POLICY (AsyncioServer).** The asyncio server and the
   asyncio-serving code path inside ``Connection`` must never poll via
   ``while not conn.closed: await asyncio.sleep(x)``. The old 100 ms poll
   burned ~33% CPU with just two active connections. Use event-driven
   primitives instead: ``await conn.wait_closed()``,
   ``conn.add_close_callback(cb)``, ``asyncio.Event``, or
   ``loop.add_reader(fd, cb)``. See ``docs/ASYNCIO_SERVER_MIGRATION.md``
   for the full policy and ``tests/test_no_polling_policy.py`` for
   enforcement.

.. figure:: docs/_static/screenshot.png
   :align: center

   A screenshot of a Windows client connecting to a Linux server.

   Note that text written to the server's ``stdout`` is actually printed on
   the server's console.

Documentation
-------------

- `Migration Guide <docs/MIGRATION_GUIDE.md>`_ — moving to the asyncio-native API
- `API Reference <docs/API_REFERENCE.md>`_
- `Examples <docs/EXAMPLES.md>`_
- `Limitations <docs/LIMITATIONS.md>`_

Documentation for *classic synchronous* RPyC lives at https://rpyc.readthedocs.io


.. References:

.. _RPyC:                   https://github.com/tomerfiliba-org/rpyc
.. _LICENSE:                LICENSE
.. _CONTRIBUTORS:           CONTRIBUTORS.rst
.. _remote procedure calls: http://en.wikipedia.org/wiki/Remote_procedure_calls
.. _clustering:             http://en.wikipedia.org/wiki/Clustering
.. _distributed-computing:  http://en.wikipedia.org/wiki/Distributed_computing
.. _object-proxying:        http://en.wikipedia.org/wiki/Proxy_pattern

.. Badges:

.. |Tests| image::     https://github.com/rr-develop/rpyc_async/actions/workflows/python-app.yml/badge.svg
   :target:            https://github.com/rr-develop/rpyc_async/actions/workflows/python-app.yml
   :alt:               Build Status

.. |Python| image::    https://img.shields.io/badge/python-3.10%2B-blue.svg?style=flat
   :target:            https://www.python.org/downloads/
   :alt:               Python Versions

.. |License| image::   https://img.shields.io/badge/license-MIT-green.svg?style=flat
   :target:            LICENSE
   :alt:               License: MIT
