import asyncio

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.urls import path, re_path

from channels.routing import (
    ChannelNameRouter,
    ProtocolTypeRouter,
    SubprotocolRouter,
    URLRouter,
)
from channels.testing import WebsocketCommunicator


class MockApplication:
    call_args = None

    def __init__(self, return_value):
        self.return_value = return_value
        super().__init__()

    async def __call__(self, scope, receive, send):
        self.call_args = ((scope, receive, send), None)
        return self.return_value


@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
async def test_protocol_type_router():
    """
    Tests the ProtocolTypeRouter
    """
    # Test basic operation
    router = ProtocolTypeRouter(
        {
            "websocket": MockApplication(return_value="ws"),
            "http": MockApplication(return_value="http"),
        }
    )
    assert await router({"type": "websocket"}, None, None) == "ws"
    assert await router({"type": "http"}, None, None) == "http"
    # Test an unmatched type
    with pytest.raises(ValueError):
        await router({"type": "aprs"}, None, None)
    # Test a scope with no type
    with pytest.raises(KeyError):
        await router({"tyyyype": "http"}, None, None)


@pytest.mark.asyncio
async def test_channel_name_router():
    """
    Tests the ChannelNameRouter
    """
    # Test basic operation
    router = ChannelNameRouter(
        {
            "test": MockApplication(return_value=1),
            "other_test": MockApplication(return_value=2),
        }
    )
    assert await router({"channel": "test"}, None, None) == 1
    assert await router({"channel": "other_test"}, None, None) == 2
    # Test an unmatched channel
    with pytest.raises(ValueError):
        await router({"channel": "chat"}, None, None)
    # Test a scope with no channel
    with pytest.raises(ValueError):
        await router({"type": "http"}, None, None)


@pytest.mark.asyncio
async def test_url_router():
    """
    Tests the URLRouter
    """
    posarg_app = MockApplication(return_value=4)
    kwarg_app = MockApplication(return_value=5)
    defaultkwarg_app = MockApplication(return_value=6)
    router = URLRouter(
        [
            path("", MockApplication(return_value=1)),
            path("foo/", MockApplication(return_value=2)),
            re_path(r"bar", MockApplication(return_value=3)),
            re_path(r"^posarg/(\d+)/$", posarg_app),
            path("kwarg/<str:name>/", kwarg_app),
            path("defaultkwargs/", defaultkwarg_app, kwargs={"default": 42}),
        ]
    )
    # Valid basic matches
    assert await router({"type": "http", "path": "/"}, None, None) == 1
    assert await router({"type": "http", "path": "/foo/"}, None, None) == 2
    assert await router({"type": "http", "path": "/bar/"}, None, None) == 3
    assert await router({"type": "http", "path": "/bar/baz/"}, None, None) == 3
    # Valid positional matches
    assert await router({"type": "http", "path": "/posarg/123/"}, None, None) == 4
    assert posarg_app.call_args[0][0]["url_route"] == {"args": ("123",), "kwargs": {}}
    assert await router({"type": "http", "path": "/posarg/456/"}, None, None) == 4
    assert posarg_app.call_args[0][0]["url_route"] == {"args": ("456",), "kwargs": {}}
    # Valid keyword argument matches
    assert await router({"type": "http", "path": "/kwarg/hello/"}, None, None) == 5
    assert kwarg_app.call_args[0][0]["url_route"] == {
        "args": tuple(),
        "kwargs": {"name": "hello"},
    }
    assert await router({"type": "http", "path": "/kwarg/hellothere/"}, None, None) == 5
    assert kwarg_app.call_args[0][0]["url_route"] == {
        "args": tuple(),
        "kwargs": {"name": "hellothere"},
    }
    # Valid default keyword arguments
    assert await router({"type": "http", "path": "/defaultkwargs/"}, None, None) == 6
    assert defaultkwarg_app.call_args[0][0]["url_route"] == {
        "args": tuple(),
        "kwargs": {"default": 42},
    }
    # Valid root_path in scope
    assert (
        await router(
            {"type": "http", "path": "/root/", "root_path": "/root"}, None, None
        )
        == 1
    )
    assert (
        await router(
            {"type": "http", "path": "/root/foo/", "root_path": "/root"}, None, None
        )
        == 2
    )

    # Unmatched root_path in scope
    with pytest.raises(ValueError):
        await router({"type": "http", "path": "/", "root_path": "/root"}, None, None)

    # Invalid matches
    with pytest.raises(ValueError):
        await router({"type": "http", "path": "/nonexistent/"}, None, None)


@pytest.mark.asyncio
async def test_url_router_nesting():
    """
    Tests that nested URLRouters add their keyword captures together.
    """
    test_app = MockApplication(return_value=1)
    inner_router = URLRouter(
        [
            re_path(r"^book/(?P<book>[\w\-]+)/page/(?P<page>\d+)/$", test_app),
            re_path(r"^test/(\d+)/$", test_app),
        ]
    )
    outer_router = URLRouter(
        [
            re_path(
                r"^universe/(?P<universe>\d+)/author/(?P<author>\w+)/", inner_router
            ),
            re_path(r"^positional/(\w+)/", inner_router),
        ]
    )
    assert (
        await outer_router(
            {
                "type": "http",
                "path": "/universe/42/author/andrewgodwin/book/channels-guide/page/10/",
            },
            None,
            None,
        )
        == 1
    )
    assert test_app.call_args[0][0]["url_route"] == {
        "args": (),
        "kwargs": {
            "book": "channels-guide",
            "author": "andrewgodwin",
            "page": "10",
            "universe": "42",
        },
    }

    assert (
        await outer_router(
            {"type": "http", "path": "/positional/foo/test/3/"}, None, None
        )
        == 1
    )
    assert test_app.call_args[0][0]["url_route"] == {"args": ("foo", "3"), "kwargs": {}}


@pytest.mark.asyncio
async def test_url_router_nesting_path():
    """
    Tests that nested URLRouters add their keyword captures together when used
    with path().
    """
    from django.urls import path

    test_app = MockApplication(return_value=1)
    inner_router = URLRouter([path("test/<int:page>/", test_app)])

    def asgi_middleware(inner):
        # Some middleware which hides the fact that we have an inner URLRouter
        async def app(scope, receive, send):
            return await inner(scope, receive, send)

        app._path_routing = True
        return app

    outer_router = URLRouter(
        [path("number/<int:number>/", asgi_middleware(inner_router))]
    )

    assert await inner_router({"type": "http", "path": "/test/3/"}, None, None) == 1
    assert (
        await outer_router({"type": "http", "path": "/number/42/test/3/"}, None, None)
        == 1
    )
    assert test_app.call_args[0][0]["url_route"] == {
        "args": (),
        "kwargs": {"number": 42, "page": 3},
    }
    with pytest.raises(ValueError):
        assert await outer_router(
            {"type": "http", "path": "/number/42/test/3/bla/"}, None, None
        )
    with pytest.raises(ValueError):
        assert await outer_router(
            {"type": "http", "path": "/number/42/blub/"}, None, None
        )


@pytest.mark.asyncio
async def test_url_router_path():
    """
    Tests that URLRouter also works with path()
    """
    from django.urls import path

    kwarg_app = MockApplication(return_value=3)
    router = URLRouter(
        [
            path("", MockApplication(return_value=1)),
            path("foo/", MockApplication(return_value=2)),
            path("author/<name>/", kwarg_app),
            path("year/<int:year>/", kwarg_app),
        ]
    )
    # Valid basic matches
    assert await router({"type": "http", "path": "/"}, None, None) == 1
    assert await router({"type": "http", "path": "/foo/"}, None, None) == 2
    # Named without typecasting
    assert (
        await router({"type": "http", "path": "/author/andrewgodwin/"}, None, None) == 3
    )
    assert kwarg_app.call_args[0][0]["url_route"] == {
        "args": tuple(),
        "kwargs": {"name": "andrewgodwin"},
    }
    # Named with typecasting
    assert await router({"type": "http", "path": "/year/2012/"}, None, None) == 3
    assert kwarg_app.call_args[0][0]["url_route"] == {
        "args": tuple(),
        "kwargs": {"year": 2012},
    }
    # Invalid matches
    with pytest.raises(ValueError):
        await router({"type": "http", "path": "/nonexistent/"}, None, None)


@pytest.mark.asyncio
async def test_path_remaining():
    """
    Resolving continues in outer router if an inner router has no matching
    routes
    """
    inner_router = URLRouter([path("no-match/", MockApplication(return_value=1))])
    test_app = MockApplication(return_value=2)
    outer_router = URLRouter(
        [path("prefix/", inner_router), path("prefix/stuff/", test_app)]
    )
    outermost_router = URLRouter([path("", outer_router)])

    assert (
        await outermost_router({"type": "http", "path": "/prefix/stuff/"}, None, None)
        == 2
    )

    assert (
        await outermost_router(
            {"type": "http", "path": "/root/prefix/stuff/", "root_path": "/root"},
            None,
            None,
        )
        == 2
    )

    with pytest.raises(ValueError):
        await outermost_router(
            {"type": "http", "path": "/root/root/prefix/stuff/", "root_path": "/root"},
            None,
            None,
        )

    with pytest.raises(ValueError):
        await outermost_router(
            {"type": "http", "path": "/root/prefix/root/stuff/", "root_path": "/root"},
            None,
            None,
        )


def test_invalid_routes():
    """
    Test URLRouter route validation
    """
    from django.urls import include

    with pytest.raises(ImproperlyConfigured) as exc:
        URLRouter([path("", include([]))])

    assert "include() is not supported in URLRouter." in str(exc)


@pytest.mark.asyncio
async def test_subprotocol_router():
    """
    Tests the SubprotocolRouter
    """
    v1_app = MockApplication(return_value="v1")
    v2_app = MockApplication(return_value="v2")
    router = SubprotocolRouter({"chat.v1": v1_app, "chat.v2": v2_app})

    # Basic routing on the offered subprotocol
    scope = {"type": "websocket", "subprotocols": ["chat.v1"]}
    assert await router(scope, None, None) == "v1"
    # The chosen subprotocol is placed into the child scope...
    assert v1_app.call_args[0][0]["subprotocol"] == "chat.v1"
    # ...while the outer scope is left untouched
    assert "subprotocol" not in scope

    # Server configuration order decides between multiple matches, not the
    # client's ordering.
    assert (
        await router(
            {"type": "websocket", "subprotocols": ["chat.v2", "chat.v1"]},
            None,
            None,
        )
        == "v1"
    )
    assert (
        await router(
            {"type": "websocket", "subprotocols": ["chat.v1", "chat.v2"]},
            None,
            None,
        )
        == "v1"
    )

    # Unconfigured subprotocols in the offer are skipped
    assert (
        await router(
            {"type": "websocket", "subprotocols": ["chat.v0", "chat.v2"]},
            None,
            None,
        )
        == "v2"
    )

    # Duplicate offers are deduplicated and route predictably
    assert (
        await router(
            {"type": "websocket", "subprotocols": ["chat.v2", "chat.v2"]},
            None,
            None,
        )
        == "v2"
    )

    # No intersection
    with pytest.raises(ValueError):
        await router({"type": "websocket", "subprotocols": ["chat.v3"]}, None, None)
    # No subprotocols offered at all
    with pytest.raises(ValueError):
        await router({"type": "websocket", "subprotocols": []}, None, None)
    with pytest.raises(ValueError):
        await router({"type": "websocket"}, None, None)
    # HTTP scopes must not enter WebSocket routing
    with pytest.raises(ValueError):
        await router({"type": "http", "subprotocols": ["chat.v1"]}, None, None)


@pytest.mark.asyncio
async def test_subprotocol_router_accept_contract():
    """
    Tests that SubprotocolRouter enforces the accept contract: applications
    may confirm the negotiated subprotocol (explicitly or by omission) or
    reject the connection, but never accept a different subprotocol.
    """
    sent_messages = []

    async def send(message):
        sent_messages.append(message)

    def accept_app(subprotocol):
        async def app(scope, receive, send):
            await send({"type": "websocket.accept", "subprotocol": subprotocol})

        return app

    async def close_app(scope, receive, send):
        await send({"type": "websocket.close", "code": 4404})

    scope = {"type": "websocket", "subprotocols": ["chat.v1"]}

    # Accepting without a subprotocol confirms the negotiated one
    router = SubprotocolRouter({"chat.v1": accept_app(None)})
    await router(scope, None, send)
    assert sent_messages[-1]["subprotocol"] == "chat.v1"

    # Accepting the negotiated subprotocol explicitly is fine
    router = SubprotocolRouter({"chat.v1": accept_app("chat.v1")})
    await router(scope, None, send)
    assert sent_messages[-1]["subprotocol"] == "chat.v1"

    # Accepting a different subprotocol is an error
    router = SubprotocolRouter({"chat.v1": accept_app("chat.v2")})
    with pytest.raises(ValueError):
        await router(scope, None, send)

    # Rejection passes through untouched
    router = SubprotocolRouter({"chat.v1": close_app})
    await router(scope, None, send)
    assert sent_messages[-1] == {"type": "websocket.close", "code": 4404}


@pytest.mark.asyncio
async def test_subprotocol_router_nested_url_router():
    """
    Tests that SubprotocolRouter nests with URLRouter in both directions.
    """
    inner_app = MockApplication(return_value=1)

    # URLRouter inside SubprotocolRouter
    router = SubprotocolRouter(
        {"chat.v1": URLRouter([path("ws/v1/", inner_app)])}
    )
    scope = {"type": "websocket", "path": "/ws/v1/", "subprotocols": ["chat.v1"]}
    assert await router(scope, None, None) == 1
    assert inner_app.call_args[0][0]["subprotocol"] == "chat.v1"
    assert inner_app.call_args[0][0]["url_route"] == {"args": (), "kwargs": {}}

    # SubprotocolRouter inside URLRouter
    router = URLRouter(
        [path("ws/", SubprotocolRouter({"chat.v2": inner_app}))]
    )
    scope = {"type": "websocket", "path": "/ws/", "subprotocols": ["chat.v2"]}
    assert await router(scope, None, None) == 1
    assert inner_app.call_args[0][0]["subprotocol"] == "chat.v2"


@pytest.mark.asyncio
async def test_subprotocol_router_middleware_scope_changes():
    """
    Tests that middleware modifying the scope around a SubprotocolRouter
    behaves predictably.
    """
    inner_app = MockApplication(return_value=1)

    def additive_middleware(inner):
        async def app(scope, receive, send):
            return await inner(dict(scope, user="anne"), receive, send)

        return app

    router = additive_middleware(SubprotocolRouter({"chat.v1": inner_app}))
    scope = {"type": "websocket", "subprotocols": ["chat.v1"]}
    assert await router(scope, None, None) == 1
    # Keys added by middleware are visible to the routed application
    assert inner_app.call_args[0][0]["user"] == "anne"
    assert inner_app.call_args[0][0]["subprotocol"] == "chat.v1"
    # The original scope is not polluted by the router
    assert "subprotocol" not in scope
    assert "user" not in scope

    # Middleware that strips the offered subprotocols leads to a
    # predictable no-match error
    def stripping_middleware(inner):
        async def app(scope, receive, send):
            return await inner(dict(scope, subprotocols=[]), receive, send)

        return app

    router = stripping_middleware(SubprotocolRouter({"chat.v1": inner_app}))
    with pytest.raises(ValueError):
        await router({"type": "websocket", "subprotocols": ["chat.v1"]}, None, None)


def version_echo_app():
    """
    An ASGI app that accepts (letting the router confirm the negotiated
    subprotocol) and echoes frames back prefixed with the subprotocol it
    was routed under.
    """

    async def app(scope, receive, send):
        await send({"type": "websocket.accept"})
        while True:
            message = await receive()
            if message["type"] == "websocket.receive":
                await send(
                    {
                        "type": "websocket.send",
                        "text": "%s:%s" % (scope["subprotocol"], message["text"]),
                    }
                )
            elif message["type"] == "websocket.disconnect":
                return

    return app


@pytest.mark.asyncio
async def test_subprotocol_router_concurrent_versions():
    """
    Tests that a single SubprotocolRouter instance can serve two protocol
    versions concurrently without shared state crosstalk.
    """
    router = SubprotocolRouter(
        {
            "chat.v1": version_echo_app(),
            "chat.v2": version_echo_app(),
        }
    )
    communicator_v1 = WebsocketCommunicator(router, "/ws/", subprotocols=["chat.v1"])
    # The client prefers v2 here; either way only one protocol is negotiated
    # per connection.
    communicator_v2 = WebsocketCommunicator(router, "/ws/", subprotocols=["chat.v2"])

    # Connect both concurrently through the same router instance
    (connected_v1, subprotocol_v1), (connected_v2, subprotocol_v2) = (
        await asyncio.gather(communicator_v1.connect(), communicator_v2.connect())
    )
    assert (connected_v1, subprotocol_v1) == (True, "chat.v1")
    assert (connected_v2, subprotocol_v2) == (True, "chat.v2")
    # The communicator records the actual negotiation result for assertions
    assert communicator_v1.response_subprotocol == "chat.v1"
    assert communicator_v2.response_subprotocol == "chat.v2"

    # Interleave frames; each connection must only see its own version
    await communicator_v1.send_to(text_data="hello")
    await communicator_v2.send_to(text_data="world")
    await communicator_v1.send_to(text_data="again")
    assert await communicator_v1.receive_from() == "chat.v1:hello"
    assert await communicator_v2.receive_from() == "chat.v2:world"
    assert await communicator_v1.receive_from() == "chat.v1:again"

    await asyncio.gather(communicator_v1.disconnect(), communicator_v2.disconnect())
