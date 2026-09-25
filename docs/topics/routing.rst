Routing
=======

While consumers are valid :doc:`ASGI </asgi>` applications, you don't want
to just write one and have that be the only thing you can give to protocol
servers like Daphne. Channels provides routing classes that allow you to
combine and stack your consumers (and any other valid ASGI application) to
dispatch based on what the connection is.

.. important::

    Channels routers only work on the *scope* level, not on the level of
    individual *events*, which means you can only have one consumer for any
    given connection. Routing is to work out what single consumer to give a
    connection, not how to spread events from one connection across
    multiple consumers.

Routers are themselves valid ASGI applications, and it's possible to nest them.
We suggest that you have a ``ProtocolTypeRouter`` as the root application of
your project - the one that you pass to protocol servers - and nest other,
more protocol-specific routing underneath there.

Channels expects you to be able to define a single *root application*, and
provide the path to it as the ``ASGI_APPLICATION`` setting (think of this as
being analogous to the ``ROOT_URLCONF`` setting in Django). There's no fixed
rule as to where you need to put the routing and the root application, but we
recommend following Django's conventions and putting them in a project-level
file called ``asgi.py``, next to ``urls.py``. You can read more about deploying
Channels projects and settings in :doc:`/deploying`.

Here's an example of what that ``asgi.py`` might look like:

.. include:: ../includes/asgi_example.rst


.. note::
  We call the ``as_asgi()`` classmethod when routing our consumers. This
  returns an ASGI wrapper application that will instantiate a new consumer
  instance for each connection or scope. This is similar to Django's
  ``as_view()``, which plays the same role for per-request instances of
  class-based views.

It's possible to have routers from third-party apps, too, or write your own,
but we'll go over the built-in Channels ones here.


ProtocolTypeRouter
------------------

``channels.routing.ProtocolTypeRouter``

This should be the top level of your ASGI application stack and the main entry
in your routing file.

It lets you dispatch to one of a number of other ASGI applications based on the
``type`` value present in the ``scope``. Protocols will define a fixed type
value that their scope contains, so you can use this to distinguish between
incoming connection types.

It takes a single argument - a dictionary mapping type names to ASGI
applications that serve them:

.. code-block:: python

    ProtocolTypeRouter({
        "http": some_app,
        "websocket": some_other_app,
    })

If you want to split HTTP handling between long-poll handlers and Django views,
use a URLRouter using Django's ``get_asgi_application()`` specified as the last
entry with a match-everything pattern.

.. _urlrouter:

URLRouter
---------

``channels.routing.URLRouter``

Routes ``http`` or ``websocket`` type connections via their HTTP path. Takes a
single argument, a list of Django URL objects (either ``path()`` or
``re_path()``):

.. code-block:: python

    URLRouter([
        re_path(r"^longpoll/$", LongPollConsumer.as_asgi()),
        re_path(r"^notifications/(?P<stream>\w+)/$", LongPollConsumer.as_asgi()),
        re_path(r"", get_asgi_application()),
    ])

Any captured groups will be provided in ``scope`` as the key ``url_route``, a
dict with a ``kwargs`` key containing a dict of the named regex groups and
an ``args`` key with a list of positional regex groups. Note that named
and unnamed groups cannot be mixed: Positional groups are discarded as soon
as a single named group is matched.

For example, to pull out the named group ``stream`` in the example above, you
would do this:

.. code-block:: python

    stream = self.scope["url_route"]["kwargs"]["stream"]

Please note that ``URLRouter`` nesting will not work properly with
``path()`` routes if inner routers are wrapped by additional middleware.
See `Issue #1428 <https://github.com/django/channels/issues/1428>`__.


SubprotocolRouter
-----------------

``channels.routing.SubprotocolRouter``

Routes ``websocket`` connections based on the subprotocols the client offers
during the opening handshake (``scope["subprotocols"]``). This is useful when
a single URL has to serve more than one version or flavour of an application
and clients select one with a WebSocket subprotocol. It only routes
``websocket`` scopes; ``http`` scopes passed to it raise ``ValueError``, so
place it underneath a ``ProtocolTypeRouter`` (or inside a ``URLRouter``).

It takes a single argument - a list of ``(subprotocols, application)`` pairs.
``subprotocols`` is a single subprotocol string or a list of strings the
application is willing to speak:

.. code-block:: python

    SubprotocolRouter([
        ("chat.v2", ChatV2Consumer.as_asgi()),
        (["chat.v1"], ChatV1Consumer.as_asgi()),
    ])

The matching rules are:

* The first subprotocol offered by the client (client preference order) that
  is also configured on one of the pairs selects the application.
* If that protocol is configured on more than one pair, the first pair in the
  list wins - the order of your server configuration sets the priority.

The selected application is called with an independent child scope in which
``subprotocols`` contains only the single negotiated protocol; the chosen
value is also available directly as ``scope["subprotocol"]``. When the
application sends ``websocket.accept`` it may only confirm that exact
subprotocol - accepting with a different one (or with no subprotocol) raises
``ValueError``. It may still explicitly refuse the handshake by sending
``websocket.close``. If none of the offered subprotocols match any pair, the
handshake is rejected with close code ``1002``.

Like the other routers it holds no per-connection state, so the same
``SubprotocolRouter`` instance can safely serve many connections and
applications concurrently; scope changes made by the selected application or
wrapping middleware are confined to that connection's child scope.


ChannelNameRouter
-----------------

``channels.routing.ChannelNameRouter``

Routes ``channel`` type scopes based on the value of the ``channel`` key in
their scope. Intended for use with the :doc:`/topics/worker`.

It takes a single argument - a dictionary mapping channel names to ASGI
applications that serve them:

.. code-block:: python

    ChannelNameRouter({
        "thumbnails-generate": some_app,
        "thumbnails-delete": some_other_app,
    })
