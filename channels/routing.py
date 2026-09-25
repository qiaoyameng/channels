import importlib

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.urls.exceptions import Resolver404
from django.urls.resolvers import RegexPattern, RoutePattern, URLResolver

"""
All Routing instances inside this file are also valid ASGI applications - with
new Channels routing, whatever you end up with as the top level object is just
served up as the "ASGI application".
"""


def get_default_application():
    """
    Gets the default application, set in the ASGI_APPLICATION setting.
    """
    try:
        path, name = settings.ASGI_APPLICATION.rsplit(".", 1)
    except (ValueError, AttributeError):
        raise ImproperlyConfigured("Cannot find ASGI_APPLICATION setting.")
    try:
        module = importlib.import_module(path)
    except ImportError:
        raise ImproperlyConfigured("Cannot import ASGI_APPLICATION module %r" % path)
    try:
        value = getattr(module, name)
    except AttributeError:
        raise ImproperlyConfigured(
            "Cannot find %r in ASGI_APPLICATION module %s" % (name, path)
        )
    return value


class ProtocolTypeRouter:
    """
    Takes a mapping of protocol type names to other Application instances,
    and dispatches to the right one based on protocol name (or raises an error)
    """

    def __init__(self, application_mapping):
        self.application_mapping = application_mapping

    async def __call__(self, scope, receive, send):
        if scope["type"] in self.application_mapping:
            application = self.application_mapping[scope["type"]]
            return await application(scope, receive, send)
        else:
            raise ValueError(
                "No application configured for scope type %r" % scope["type"]
            )


class URLRouter:
    """
    Routes to different applications/consumers based on the URL path.

    Works with anything that has a ``path`` key, but intended for WebSocket
    and HTTP. Uses Django's django.urls objects for resolution -
    path() or re_path().
    """

    #: This router wants to do routing based on scope[path] or
    #: scope[path_remaining]. ``path()`` entries in URLRouter should not be
    #: treated as endpoints (ended with ``$``), but similar to ``include()``.
    _path_routing = True

    def __init__(self, routes):
        self.routes = routes

        for route in self.routes:
            # The inner ASGI app wants to do additional routing, route
            # must not be an endpoint
            if getattr(route.callback, "_path_routing", False) is True:
                pattern = route.pattern
                if isinstance(pattern, RegexPattern):
                    arg = pattern._regex
                elif isinstance(pattern, RoutePattern):
                    arg = pattern._route
                else:
                    raise ValueError(f"Unsupported pattern type: {type(pattern)}")
                route.pattern = pattern.__class__(arg, pattern.name, is_endpoint=False)

            if not route.callback and isinstance(route, URLResolver):
                raise ImproperlyConfigured(
                    "%s: include() is not supported in URLRouter. Use nested"
                    " URLRouter instances instead." % (route,)
                )

    async def __call__(self, scope, receive, send):
        # Get the path
        path = scope.get("path_remaining", scope.get("path", None))
        if path is None:
            raise ValueError("No 'path' key in connection scope, cannot route URLs")

        if "path_remaining" not in scope:
            # We are the outermost URLRouter, so handle root_path if present.
            root_path = scope.get("root_path", "")
            if root_path and not path.startswith(root_path):
                # If root_path is present, path must start with it.
                raise ValueError("No route found for path %r." % path)
            path = path[len(root_path) :]

        # Remove leading / to match Django's handling
        path = path.lstrip("/")
        # Run through the routes we have until one matches
        for route in self.routes:
            try:
                match = route.pattern.match(path)
                if match:
                    new_path, args, kwargs = match
                    # Add defaults to kwargs from the URL pattern.
                    kwargs.update(route.default_args)
                    # Add args or kwargs into the scope
                    outer = scope.get("url_route", {})
                    application = route.callback
                    return await application(
                        dict(
                            scope,
                            path_remaining=new_path,
                            url_route={
                                "args": outer.get("args", ()) + args,
                                "kwargs": {**outer.get("kwargs", {}), **kwargs},
                            },
                        ),
                        receive,
                        send,
                    )
            except Resolver404:
                pass
        else:
            if "path_remaining" in scope:
                raise Resolver404("No route found for path %r." % path)
            # We are the outermost URLRouter
            raise ValueError("No route found for path %r." % path)


class ChannelNameRouter:
    """
    Maps to different applications based on a "channel" key in the scope
    (intended for the Channels worker mode)
    """

    def __init__(self, application_mapping):
        self.application_mapping = application_mapping

    async def __call__(self, scope, receive, send):
        if "channel" not in scope:
            raise ValueError(
                "ChannelNameRouter got a scope without a 'channel' key. "
                + "Did you make sure it's only being used for 'channel' type messages?"
            )
        if scope["channel"] in self.application_mapping:
            application = self.application_mapping[scope["channel"]]
            return await application(scope, receive, send)
        else:
            raise ValueError(
                "No application configured for channel name %r" % scope["channel"]
            )


class SubprotocolRouter:
    """
    Routes ``websocket`` connections to different applications based on the
    subprotocols offered by the client in ``scope["subprotocols"]``.

    It takes a single argument - a list of ``(subprotocols, application)``
    pairs. ``subprotocols`` is a single subprotocol string or a list/tuple of
    strings the application is willing to speak. The order of the pairs is
    significant: when a client offers several protocols that more than one
    configured application could serve, the first pair in the list wins:

    .. code-block:: python

        SubprotocolRouter([
            (["chat.v2"], ChatV2Consumer.as_asgi()),
            (["chat.v1"], ChatV1Consumer.as_asgi()),
        ])

    For each connection the first subprotocol offered by the client that is
    also configured on one of the pairs (client preference order) selects the
    application. The selected application receives an independent child scope
    in which ``subprotocols`` contains only the single negotiated protocol
    (also exposed as ``scope["subprotocol"]``); the router additionally
    enforces that the application only accepts that exact protocol, or
    explicitly rejects the connection with ``websocket.close``.

    If none of the offered subprotocols match, the connection is rejected.
    """

    #: Close code sent back to the client when no configured subprotocol
    #: intersects with the subprotocols it offered (a protocol error).
    reject_close_code = 1002

    def __init__(self, routes):
        self.routes = []
        for entry in routes:
            try:
                subprotocols, application = entry
            except (TypeError, ValueError):
                raise ValueError(
                    "SubprotocolRouter entries must be (subprotocols, "
                    "application) pairs, got %r." % (entry,)
                )
            if isinstance(subprotocols, str):
                subprotocols = (subprotocols,)
            normalized = []
            entry_seen = set()
            for subprotocol in subprotocols:
                if not isinstance(subprotocol, str) or not subprotocol:
                    raise ValueError(
                        "SubprotocolRouter subprotocols must be non-empty "
                        "strings, got %r." % (subprotocol,)
                    )
                if subprotocol in entry_seen:
                    raise ValueError(
                        "Subprotocol %r is configured more than once on the"
                        " same router entry." % (subprotocol,)
                    )
                entry_seen.add(subprotocol)
                normalized.append(subprotocol)
            if not normalized:
                raise ValueError(
                    "SubprotocolRouter entry %r has no subprotocols." % (entry,)
                )
            self.routes.append((normalized, application))

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "websocket":
            raise ValueError(
                "SubprotocolRouter can only route 'websocket' scopes, got "
                "type %r." % scope.get("type")
            )

        offered = scope.get("subprotocols")
        if offered is None:
            offered = []

        # Normalise the client list (de-duplicate while keeping the client's
        # preference order) without mutating the caller's scope.
        client_subprotocols = []
        for subprotocol in offered:
            if (
                isinstance(subprotocol, str)
                and subprotocol not in client_subprotocols
            ):
                client_subprotocols.append(subprotocol)

        # Client preference decides which of the offered protocols is picked;
        # configuration order breaks ties between candidate applications.
        selected = None
        application = None
        for subprotocol in client_subprotocols:
            for configured, candidate in self.routes:
                if subprotocol in configured:
                    selected = subprotocol
                    application = candidate
                    break
            if selected is not None:
                break

        if selected is None:
            # No intersection: explicitly reject rather than handing the
            # connection to an application that cannot serve it.
            await send({"type": "websocket.close", "code": self.reject_close_code})
            return

        # Hand the selected application an independent child scope so changes
        # made by middleware or the application cannot leak back upstream or
        # into other connections handled concurrently by the same router.
        child_scope = dict(scope)
        child_scope["subprotocols"] = [selected]
        child_scope["subprotocol"] = selected

        async def guarded_send(message):
            # The application may confirm the negotiated protocol, send data
            # or explicitly reject/close the connection; it must not confirm
            # a different (or no) subprotocol.
            if message["type"] == "websocket.accept":
                confirmed = message.get("subprotocol")
                if confirmed != selected:
                    raise ValueError(
                        "SubprotocolRouter selected %r but the application "
                        "tried to accept with subprotocol %r."
                        % (selected, confirmed)
                    )
            await send(message)

        await application(child_scope, receive, guarded_send)
