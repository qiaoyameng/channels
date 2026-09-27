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


class SubprotocolRouter:
    """
    Routes WebSocket connections to different applications based on the
    subprotocols offered by the client in ``scope["subprotocols"]``.

    Takes an ordered mapping of subprotocol names to ASGI applications. When
    several configured subprotocols are offered by the client at once, the
    one that appears first in the mapping wins - server configuration order
    decides priority, not the client's ordering.

    The negotiated subprotocol is placed into a child scope (a copy, so the
    outer scope is never mutated) as ``scope["subprotocol"]``. The routed
    application may then only confirm that protocol when it accepts the
    connection, or reject the connection outright:

    * ``websocket.accept`` without an explicit subprotocol is completed with
      the negotiated one, so the client always learns the result.
    * ``websocket.accept`` naming a different subprotocol raises ValueError.
    * ``websocket.close`` (rejection) is passed through untouched.

    Non-websocket scopes are rejected with ValueError; use ProtocolTypeRouter
    above this router to keep HTTP traffic away from it.
    """

    def __init__(self, application_mapping):
        self.application_mapping = dict(application_mapping)

    async def __call__(self, scope, receive, send):
        if scope["type"] != "websocket":
            raise ValueError(
                "SubprotocolRouter can only route 'websocket' scopes, got "
                "%r. Use ProtocolTypeRouter to dispatch other protocol "
                "types." % scope["type"]
            )
        # Deduplicate the offered subprotocols (preserving order) so repeated
        # entries in the client list behave predictably.
        offered = list(dict.fromkeys(scope.get("subprotocols") or []))
        for subprotocol, application in self.application_mapping.items():
            if subprotocol in offered:
                child_scope = dict(scope, subprotocol=subprotocol)
                return await application(
                    child_scope, receive, self._send_wrapper(send, subprotocol)
                )
        raise ValueError(
            "No application configured for any of the offered subprotocols "
            "%r" % (offered,)
        )

    @staticmethod
    def _send_wrapper(send, subprotocol):
        """
        Wraps the ASGI send callable to enforce the negotiation contract for
        a single connection. Created per connection, so concurrent
        connections through the same router never share state.
        """

        async def sender(message):
            if message["type"] == "websocket.accept":
                accepted = message.get("subprotocol")
                if accepted is None:
                    message = dict(message, subprotocol=subprotocol)
                elif accepted != subprotocol:
                    raise ValueError(
                        "SubprotocolRouter negotiated %r but the application "
                        "tried to accept %r. Applications may only accept "
                        "the negotiated subprotocol or close the connection."
                        % (subprotocol, accepted)
                    )
            await send(message)

        return sender


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
