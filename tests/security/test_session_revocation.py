import asyncio
import hashlib
from importlib import import_module

import pytest
from django.conf import settings
from django.contrib.auth import (
    BACKEND_SESSION_KEY,
    HASH_SESSION_KEY,
    SESSION_KEY,
    get_user_model,
)
from django.contrib.auth import logout as django_logout
from django.test import RequestFactory

from channels.auth import (
    SESSION_REVOKED_MESSAGE_TYPE,
    AuthMiddleware,
    SessionRevocationMiddleware,
    arevoke_session,
    logout,
)
from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.layers import get_channel_layer
from channels.sessions import SessionMiddlewareStack
from channels.testing import WebsocketCommunicator


class EchoConsumer(AsyncWebsocketConsumer):
    """
    Accepts the connection, echoes messages back and captures its scope so
    tests can manipulate the session server-side.
    """

    captured_scopes = []

    async def connect(self):
        self.captured_scopes.append(self.scope)
        await self.accept()

    async def receive(self, text_data=None, bytes_data=None):
        await self.send(text_data=text_data)


class SessionDeletingMiddleware:
    """
    Deletes the scope session from the session store before the inner
    application (and thus SessionRevocationMiddleware) runs, simulating a
    revocation that races connection setup.
    """

    def __init__(self, inner):
        self.inner = inner

    async def __call__(self, scope, receive, send):
        await database_sync_to_async(scope["session"].delete)()
        return await self.inner(scope, receive, send)


@pytest.fixture(autouse=True)
async def event_loop_wakeup_pump():
    """
    Keeps the event loop ticking so callbacks posted from other threads
    (e.g. database_sync_to_async results) are flushed promptly, even on
    event loops whose cross-thread wakeup mechanism is unreliable.
    """
    loop = asyncio.get_running_loop()
    pumping = True

    def tick():
        if pumping:
            loop.call_later(0.01, tick)

    tick()
    yield
    pumping = False


@pytest.fixture(autouse=True)
async def clean_channel_layer():
    layer = get_channel_layer()
    await layer.flush()
    yield
    await get_channel_layer().flush()


@database_sync_to_async
def create_user(username):
    return get_user_model().objects.create(username=username)


@database_sync_to_async
def create_session(user=None):
    SessionStore = import_module(settings.SESSION_ENGINE).SessionStore
    session = SessionStore()
    if user is not None:
        session[SESSION_KEY] = str(user.pk)
        session[BACKEND_SESSION_KEY] = "django.contrib.auth.backends.ModelBackend"
        session[HASH_SESSION_KEY] = user.get_session_auth_hash()
    session.create()
    return session


def make_application(extra_middleware=None):
    app = SessionRevocationMiddleware(EchoConsumer())
    if extra_middleware is not None:
        app = extra_middleware(app)
    return SessionMiddlewareStack(AuthMiddleware(app))


async def connect(session=None, extra_middleware=None):
    headers = []
    if session is not None:
        headers = [
            (
                b"cookie",
                b"%s=%s"
                % (
                    settings.SESSION_COOKIE_NAME.encode("ascii"),
                    session.session_key.encode("ascii"),
                ),
            )
        ]
    communicator = WebsocketCommunicator(
        make_application(extra_middleware), "/", headers=headers
    )
    connected, _ = await communicator.connect()
    return communicator, connected


async def assert_closed_by_server(communicator):
    response = await communicator.receive_output()
    assert response["type"] == "websocket.close"
    await communicator.wait()


async def wait_for_condition(condition, timeout=1):
    for _ in range(int(timeout * 100)):
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("Condition was not met within %s seconds" % timeout)


@pytest.mark.django_db(transaction=True)
async def test_revoked_session_closes_connection():
    """
    Revoking the session a connection was established with closes it, and
    the subscription is cleaned up afterwards.
    """
    user = await create_user("bob")
    session = await create_session(user)
    communicator, connected = await connect(session)
    assert connected
    assert len(get_channel_layer().groups) == 1
    await arevoke_session(session.session_key)
    await assert_closed_by_server(communicator)
    assert get_channel_layer().groups == {}


@pytest.mark.django_db(transaction=True)
async def test_revocation_only_closes_matching_session():
    """
    Revoking one session does not close connections holding a different
    session - not even a newer session of the same user.
    """
    user = await create_user("bob")
    old_session = await create_session(user)
    # The same user logging in again gets a separate session.
    new_session = await create_session(user)
    old_communicator, connected = await connect(old_session)
    assert connected
    new_communicator, connected = await connect(new_session)
    assert connected
    await arevoke_session(old_session.session_key)
    await assert_closed_by_server(old_communicator)
    # The connection of the user's new session is unaffected.
    await new_communicator.send_to(text_data="hello")
    assert await new_communicator.receive_from() == "hello"
    await new_communicator.disconnect()


@pytest.mark.django_db(transaction=True)
async def test_revocation_message_does_not_contain_session_key():
    """
    Neither the group name nor the revocation message contain the raw
    session cookie value.
    """
    user = await create_user("bob")
    session = await create_session(user)
    communicator, connected = await connect(session)
    assert connected
    layer = get_channel_layer()
    (group,) = list(layer.groups)
    assert session.session_key not in group
    # Listen in on the group to inspect the revocation message.
    probe = await layer.new_channel()
    await layer.group_add(group, probe)
    await arevoke_session(session.session_key)
    message = await layer.receive(probe)
    assert message["type"] == SESSION_REVOKED_MESSAGE_TYPE
    assert session.session_key not in str(message)
    await layer.group_discard(group, probe)
    await assert_closed_by_server(communicator)


@pytest.mark.django_db(transaction=True)
async def test_session_revoked_before_subscribe_is_not_missed():
    """
    A session revoked between the session being read and the connection
    subscribing is caught by a second check, and the connection is refused.
    """
    user = await create_user("bob")
    session = await create_session(user)
    communicator, connected = await connect(
        session, extra_middleware=SessionDeletingMiddleware
    )
    assert not connected
    await communicator.wait()
    assert get_channel_layer().groups == {}


@pytest.mark.django_db(transaction=True)
async def test_session_rotation_moves_subscription():
    """
    When the session key rotates (e.g. through login), the connection
    follows the new session. A late revocation of the old session does not
    close it; revoking the new session does.
    """
    user = await create_user("bob")
    session = await create_session(user)
    EchoConsumer.captured_scopes.clear()
    communicator, connected = await connect(session)
    assert connected
    layer = get_channel_layer()
    (old_group,) = list(layer.groups)
    old_session_key = session.session_key
    # Rotate the session key server-side, as login() would.
    scope_session = EchoConsumer.captured_scopes[0]["session"]
    await database_sync_to_async(scope_session.cycle_key)()
    new_session_key = scope_session.session_key
    assert new_session_key != old_session_key
    # The middleware follows the rotation once the connection is active.
    await communicator.send_to(text_data="ping")
    assert await communicator.receive_from() == "ping"
    await wait_for_condition(
        lambda: len(layer.groups) == 1 and old_group not in layer.groups
    )
    (new_group,) = list(layer.groups)
    # A late revocation of the old session key must not close the connection.
    await arevoke_session(old_session_key)
    # Neither must a revocation message carrying the old session's hash.
    old_session_key_hash = hashlib.sha256(old_session_key.encode("utf-8")).hexdigest()
    await layer.group_send(
        new_group,
        {
            "type": SESSION_REVOKED_MESSAGE_TYPE,
            "session_key_hash": old_session_key_hash,
        },
    )
    await communicator.send_to(text_data="still here")
    assert await communicator.receive_from() == "still here"
    # Revoking the rotated (current) session closes the connection.
    await arevoke_session(new_session_key)
    await assert_closed_by_server(communicator)
    assert layer.groups == {}


@pytest.mark.django_db(transaction=True)
async def test_anonymous_connection_is_not_tracked():
    """
    Connections without an authenticated session are never subscribed, and
    revoking their (anonymous) session does not affect them.
    """
    session = await create_session()
    communicator, connected = await connect(session)
    assert connected
    assert get_channel_layer().groups == {}
    await arevoke_session(session.session_key)
    await communicator.send_to(text_data="hello")
    assert await communicator.receive_from() == "hello"
    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
async def test_connection_without_session_cookie_is_not_tracked():
    """
    Connections without any session cookie are passed through and never
    subscribed.
    """
    communicator, connected = await connect()
    assert connected
    assert get_channel_layer().groups == {}
    await communicator.send_to(text_data="hello")
    assert await communicator.receive_from() == "hello"
    await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
async def test_disconnect_cleans_up_subscription():
    """
    Once the connection goes away, its group subscription is removed.
    """
    user = await create_user("bob")
    session = await create_session(user)
    communicator, connected = await connect(session)
    assert connected
    layer = get_channel_layer()
    assert len(layer.groups) == 1
    await communicator.disconnect()
    assert layer.groups == {}


@pytest.mark.django_db(transaction=True)
async def test_logout_in_other_page_closes_connection():
    """
    A regular Django logout (e.g. in another browser tab) revokes the
    session via the user_logged_out signal and closes the connection.
    """
    user = await create_user("bob")
    session = await create_session(user)
    communicator, connected = await connect(session)
    assert connected

    @database_sync_to_async
    def logout_elsewhere():
        SessionStore = import_module(settings.SESSION_ENGINE).SessionStore
        request = RequestFactory().get("/")
        request.session = SessionStore(session.session_key)
        request.user = user
        django_logout(request)

    await logout_elsewhere()
    await assert_closed_by_server(communicator)
    assert get_channel_layer().groups == {}


@pytest.mark.django_db(transaction=True)
async def test_channels_logout_closes_connection():
    """
    channels.auth.logout() revokes the flushed session, closing connections
    that still hold it.
    """
    user = await create_user("bob")
    session = await create_session(user)
    EchoConsumer.captured_scopes.clear()
    communicator, connected = await connect(session)
    assert connected
    scope = EchoConsumer.captured_scopes[0]
    await logout(scope)
    await assert_closed_by_server(communicator)
    assert get_channel_layer().groups == {}


@pytest.mark.django_db(transaction=True)
async def test_no_channel_layer_passes_connections_through(settings):
    """
    Without a configured channel layer the middleware is inert.
    """
    settings.CHANNEL_LAYERS = {}
    user = await create_user("bob")
    session = await create_session(user)
    communicator, connected = await connect(session)
    assert connected
    await communicator.send_to(text_data="hello")
    assert await communicator.receive_from() == "hello"
    await communicator.disconnect()
