import asyncio

import pytest
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.middleware import BaseMiddleware
from channels.routing import (
    ProtocolTypeRouter,
    SubprotocolRouter,
    URLRouter,
)
from channels.testing import WebsocketCommunicator
from django.urls import path


class NoDBWebsocketConsumer(AsyncWebsocketConsumer):
    """
    AsyncWebsocketConsumer variant for these tests that skips the database
    connection cleanup on every dispatch. None of these consumers touch the
    database, and this keeps the tests independent of the sync->async thread
    offload (which relies on the event loop being woken from a worker thread).
    """

    __test__ = False

    async def dispatch(self, message):
        handler = getattr(self, message["type"].replace(".", "_"), None)
        if handler is None:
            raise ValueError("No handler for message type %s" % message["type"])
        await handler(message)


class ChatV1Consumer(NoDBWebsocketConsumer):
    async def connect(self):
        await self.accept("chat.v1")

    async def receive(self, text_data=None, bytes_data=None):
        await self.send(text_data="v1-echo:%s" % text_data)


class ChatV2Consumer(NoDBWebsocketConsumer):
    async def connect(self):
        await self.accept("chat.v2")

    async def receive(self, text_data=None, bytes_data=None):
        await self.send(text_data="v2-echo:%s" % text_data)


class StrictChatV1Consumer(NoDBWebsocketConsumer):
    """Accepts whatever the router put into scope (must be chat.v1)."""

    async def connect(self):
        await self.accept(self.scope["subprotocol"])


class EchoConsumer(NoDBWebsocketConsumer):
    """Confirms the negotiated protocol and tags replies with a prefix."""

    def __init__(self, tag):
        super().__init__()
        self.tag = tag

    async def connect(self):
        await self.accept(self.scope["subprotocol"])

    async def receive(self, text_data=None, bytes_data=None):
        await self.send(text_data="%s:%s" % (self.tag, text_data))


class RogueConsumer(NoDBWebsocketConsumer):
    """Tries to confirm a protocol the router did not negotiate."""

    def __init__(self, subprotocol=None):
        super().__init__()
        self._subprotocol = subprotocol

    async def connect(self):
        await self.accept(self._subprotocol)


class CloserConsumer(NoDBWebsocketConsumer):
    """Explicitly refuses the handshake."""

    async def connect(self):
        await self.close(code=4401)


def build_router():
    # v2 listed first: when both match, server configuration order wins.
    return SubprotocolRouter(
        [
            ("chat.v2", ChatV2Consumer.as_asgi()),
            (("chat.v1",), ChatV1Consumer.as_asgi()),
        ]
    )


@pytest.mark.asyncio
async def test_routes_by_client_subprotocol():
    router = build_router()

    v1 = WebsocketCommunicator(router, "/chat/", subprotocols=["chat.v1"])
    connected, subprotocol = await v1.connect()
    assert connected is True
    assert subprotocol == "chat.v1"
    assert v1.subprotocol == "chat.v1"
    assert v1.connected is True
    await v1.send_to("hello")
    assert await v1.receive_from() == "v1-echo:hello"
    await v1.disconnect()

    v2 = WebsocketCommunicator(router, "/chat/", subprotocols=["chat.v2"])
    connected, subprotocol = await v2.connect()
    assert connected is True
    assert subprotocol == "chat.v2"
    assert v2.subprotocol == "chat.v2"
    await v2.send_to("hello")
    assert await v2.receive_from() == "v2-echo:hello"
    await v2.disconnect()


@pytest.mark.asyncio
async def test_configuration_order_decides_overlapping_candidates():
    """
    When the client offers protocols that multiple entries could serve, the
    first entry in the server configuration list takes priority.
    """
    router = SubprotocolRouter(
        [
            (["chat.v2", "shared"], EchoConsumer.as_asgi(tag="v2")),
            (["chat.v1", "shared"], EchoConsumer.as_asgi(tag="v1")),
        ]
    )
    communicator = WebsocketCommunicator(
        router, "/chat/", subprotocols=["shared"]
    )
    connected, subprotocol = await communicator.connect()
    assert (connected, subprotocol) == (True, "shared")
    # The first configured application (v2) handled the connection.
    await communicator.send_to("ping")
    assert await communicator.receive_from() == "v2:ping"
    await communicator.disconnect()


@pytest.mark.asyncio
async def test_client_preference_order_within_entry():
    """
    Among protocols served by the selected application, the client's first
    offered match is negotiated.
    """
    picked = []

    class Recorder(NoDBWebsocketConsumer):
        async def connect(self):
            picked.append(self.scope["subprotocol"])
            await self.accept(self.scope["subprotocol"])

    router = SubprotocolRouter(
        [(["chat.v1", "chat.v2"], Recorder.as_asgi())]
    )
    communicator = WebsocketCommunicator(
        router, "/chat/", subprotocols=["other", "chat.v2", "chat.v1"]
    )
    connected, subprotocol = await communicator.connect()
    assert (connected, subprotocol) == (True, "chat.v2")
    assert picked == ["chat.v2"]
    await communicator.disconnect()


@pytest.mark.asyncio
async def test_no_intersection_is_rejected():
    router = build_router()
    communicator = WebsocketCommunicator(
        router, "/chat/", subprotocols=["unknown.protocol"]
    )
    connected, code = await communicator.connect()
    assert connected is False
    assert code == SubprotocolRouter.reject_close_code
    assert communicator.close_code == SubprotocolRouter.reject_close_code
    assert communicator.connected is False


@pytest.mark.asyncio
async def test_no_subprotocols_is_rejected():
    router = build_router()
    communicator = WebsocketCommunicator(router, "/chat/")
    connected, code = await communicator.connect()
    assert (connected, code) == (False, SubprotocolRouter.reject_close_code)


@pytest.mark.asyncio
async def test_selected_scope_is_isolated_child_scope():
    original_subprotocols = ["chat.v1"]
    scope = {
        "type": "websocket",
        "path": "/chat/",
        "headers": [],
        "subprotocols": list(original_subprotocols),
    }
    seen = {}

    async def app(inner_scope, receive, send):
        seen["scope"] = inner_scope
        seen["is_same_object"] = inner_scope is scope
        # Mutating the child must not touch the caller's scope.
        inner_scope["subprotocols"].append("tampered")
        inner_scope["extra"] = True
        await send({"type": "websocket.accept", "subprotocol": "chat.v1"})

    router = SubprotocolRouter([("chat.v1", app)])

    async def receive():
        return {"type": "websocket.disconnect", "code": 1000}

    sent = []

    async def send(message):
        sent.append(message)

    await router(scope, receive, send)

    assert seen["is_same_object"] is False
    assert seen["scope"]["subprotocols"] == ["chat.v1", "tampered"]
    assert seen["scope"]["subprotocol"] == "chat.v1"
    # The caller's scope is untouched.
    assert scope["subprotocols"] == original_subprotocols
    assert "subprotocol" not in scope
    assert "extra" not in scope
    assert sent == [{"type": "websocket.accept", "subprotocol": "chat.v1"}]


@pytest.mark.asyncio
async def test_child_scope_only_contains_negotiated_protocol():
    router = SubprotocolRouter(
        [("chat.v1", StrictChatV1Consumer.as_asgi())]
    )
    communicator = WebsocketCommunicator(
        router,
        "/chat/",
        subprotocols=["other", "chat.v1", "chat.v1"],
    )
    connected, subprotocol = await communicator.connect()
    assert (connected, subprotocol) == (True, "chat.v1")
    await communicator.disconnect()


def test_repeated_subprotocol_within_entry_raises():
    with pytest.raises(ValueError) as excinfo:
        SubprotocolRouter(
            [(["chat.v2", "chat.v2"], ChatV2Consumer.as_asgi())]
        )
    assert "chat.v2" in str(excinfo.value)


@pytest.mark.asyncio
async def test_duplicate_client_offers_are_deduplicated():
    """
    A client repeating a protocol must not make the negotiation ambiguous;
    the first occurrence is used and the connection is still negotiated once.
    """
    router = SubprotocolRouter(
        [(["chat.v1", "chat.v2"], StrictChatV1Consumer.as_asgi())]
    )
    communicator = WebsocketCommunicator(
        router, "/chat/", subprotocols=["chat.v1", "chat.v2", "chat.v1"]
    )
    connected, subprotocol = await communicator.connect()
    assert (connected, subprotocol) == (True, "chat.v1")
    await communicator.disconnect()


def test_invalid_entries_raise():
    with pytest.raises(ValueError):
        SubprotocolRouter([("chat.v1",)])
    with pytest.raises(ValueError):
        SubprotocolRouter([("", ChatV1Consumer.as_asgi())])
    with pytest.raises(ValueError):
        SubprotocolRouter([([], ChatV1Consumer.as_asgi())])


@pytest.mark.asyncio
async def test_http_scope_does_not_enter_websocket_router():
    router = build_router()
    with pytest.raises(ValueError) as excinfo:
        await router({"type": "http", "subprotocols": ["chat.v1"]}, None, None)
    assert "websocket" in str(excinfo.value)

    # When stacked under a ProtocolTypeRouter, HTTP is dispatched to the HTTP
    # application and never reaches the WebSocket subprotocol router.
    http_hits = []

    async def http_app(scope, receive, send):
        http_hits.append(scope["type"])

    root = ProtocolTypeRouter(
        {
            "http": http_app,
            "websocket": build_router(),
        }
    )
    await root({"type": "http"}, None, None)
    assert http_hits == ["http"]


@pytest.mark.asyncio
async def test_nested_in_urlrouter():
    router = URLRouter(
        [
            path(
                "chat/",
                SubprotocolRouter(
                    [
                        ("chat.v2", ChatV2Consumer.as_asgi()),
                        ("chat.v1", ChatV1Consumer.as_asgi()),
                    ]
                ),
            )
        ]
    )
    communicator = WebsocketCommunicator(
        router, "/chat/", subprotocols=["chat.v1"]
    )
    connected, subprotocol = await communicator.connect()
    assert (connected, subprotocol) == (True, "chat.v1")
    await communicator.send_to("x")
    assert await communicator.receive_from() == "v1-echo:x"
    await communicator.disconnect()


@pytest.mark.asyncio
async def test_middleware_scope_changes_are_contained():
    """
    Middleware wrapping a selected app may mutate its own scope copy; that
    must not affect the router or other concurrent connections.
    """

    class TaggingMiddleware(BaseMiddleware):
        async def __call__(self, scope, receive, send):
            scope = dict(scope)
            scope["subprotocols"] = ["modified"]
            scope["tag"] = id(scope)
            return await super().__call__(scope, receive, send)

    router = SubprotocolRouter(
        [
            (
                "chat.v2",
                TaggingMiddleware(ChatV2Consumer.as_asgi()),
            ),
            ("chat.v1", ChatV1Consumer.as_asgi()),
        ]
    )
    v2 = WebsocketCommunicator(router, "/chat/", subprotocols=["chat.v2"])
    connected, subprotocol = await v2.connect()
    assert (connected, subprotocol) == (True, "chat.v2")
    await v2.disconnect()

    # A second connection still sees a pristine, independently built scope.
    v1 = WebsocketCommunicator(router, "/chat/", subprotocols=["chat.v1"])
    connected, subprotocol = await v1.connect()
    assert (connected, subprotocol) == (True, "chat.v1")
    await v1.send_to("y")
    assert await v1.receive_from() == "v1-echo:y"
    await v1.disconnect()


@pytest.mark.asyncio
async def test_application_must_confirm_selected_subprotocol():
    # Accepting a different protocol is an error, not a silent override.
    router = SubprotocolRouter(
        [("chat.v1", RogueConsumer.as_asgi(subprotocol="chat.v2"))]
    )
    communicator = WebsocketCommunicator(
        router, "/chat/", subprotocols=["chat.v1"]
    )
    with pytest.raises(ValueError) as excinfo:
        await communicator.connect()
    message = str(excinfo.value)
    assert "chat.v1" in message and "chat.v2" in message

    # Accepting with no subprotocol when one was negotiated is also an error.
    router = SubprotocolRouter(
        [("chat.v1", RogueConsumer.as_asgi(subprotocol=None))]
    )
    communicator = WebsocketCommunicator(
        router, "/chat/", subprotocols=["chat.v1"]
    )
    with pytest.raises(ValueError):
        await communicator.connect()


@pytest.mark.asyncio
async def test_application_may_explicitly_reject():
    router = SubprotocolRouter([("chat.v1", CloserConsumer.as_asgi())])
    communicator = WebsocketCommunicator(
        router, "/chat/", subprotocols=["chat.v1"]
    )
    connected, code = await communicator.connect()
    assert (connected, code) == (False, 4401)


@pytest.mark.asyncio
async def test_concurrent_connections_have_no_shared_state():
    """
    The same router instance interleaves a v1 and v2 handshake/traffic; the
    routing decision and scopes must not bleed between the connections.
    """
    router = build_router()
    v1 = WebsocketCommunicator(router, "/chat/", subprotocols=["chat.v1"])
    v2 = WebsocketCommunicator(router, "/chat/", subprotocols=["chat.v2"])

    accepted_v1, accepted_v2 = await asyncio.gather(
        v1.connect(), v2.connect()
    )
    assert accepted_v1 == (True, "chat.v1")
    assert accepted_v2 == (True, "chat.v2")

    await asyncio.gather(v1.send_to("one"), v2.send_to("two"))
    reply_v1, reply_v2 = await asyncio.gather(
        v1.receive_from(), v2.receive_from()
    )
    assert reply_v1 == "v1-echo:one"
    assert reply_v2 == "v2-echo:two"

    await asyncio.gather(v1.disconnect(), v2.disconnect())


@pytest.mark.asyncio
async def test_communicator_rejects_unoffered_negotiation():
    """
    The test communicator itself asserts that a confirmed subprotocol was
    actually offered by the client.
    """

    async def liar(scope, receive, send):
        await send(
            {"type": "websocket.accept", "subprotocol": "never.offered"}
        )

    communicator = WebsocketCommunicator(
        liar, "/chat/", subprotocols=["chat.v1"]
    )
    with pytest.raises(AssertionError) as excinfo:
        await communicator.connect()
    assert "never.offered" in str(excinfo.value)
