|Version| |Python| |Tests| |License|

RPyC_ (pronounced like *are-pie-see*), or *Remote Python Call*, is a
**transparent** library for **symmetrical** `remote procedure calls`_,
clustering_, and distributed-computing_.  RPyC makes use of object-proxying_,
a technique that employs python's dynamic nature, to overcome the physical
boundaries between processes and computers, so that remote objects can be
manipulated as if they were local.

Documentation can be found at https://rpyc.readthedocs.io

Server Selection Guide
======================

RPyC provides two main server implementations. Choose based on your async requirements:

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

.. figure:: http://rpyc.readthedocs.org/en/latest/_images/screenshot.png
   :align: center

   A screenshot of a Windows client connecting to a Linux server.

   Note that text written to the server's ``stdout`` is actually printed on
   the server's console.


.. References:

.. _RPyC:                   https://github.com/tomerfiliba-org/rpyc
.. _remote procedure calls: http://en.wikipedia.org/wiki/Remote_procedure_calls
.. _clustering:             http://en.wikipedia.org/wiki/Clustering
.. _distributed-computing:  http://en.wikipedia.org/wiki/Distributed_computing
.. _object-proxying:        http://en.wikipedia.org/wiki/Proxy_pattern

.. Badges:

.. |Version| image::   https://img.shields.io/pypi/v/rpyc.svg?style=flat
   :target:            https://pypi.python.org/pypi/rpyc
   :alt:               Version

.. |Python| image::    https://img.shields.io/pypi/pyversions/rpyc.svg?style=flat
   :target:            https://pypi.python.org/pypi/rpyc#downloads
   :alt:               Python Versions

.. |Tests| image::     https://github.com/tomerfiliba-org/rpyc/actions/workflows/python-app.yml/badge.svg
   :target:            https://github.com/tomerfiliba-org/rpyc/actions/workflows/python-app.yml
   :alt:               Build Status

.. |License| image::   https://img.shields.io/pypi/l/rpyc.svg?style=flat
   :target:            https://github.com/tomerfiliba-org/rpyc/blob/master/LICENSE
   :alt:               License: MIT
