import hashlib
import json
from importlib import import_module
from types import SimpleNamespace

import pytest
from django.conf import settings
from django.contrib.auth import get_user_model, user_logged_out
from django.test import override_settings

from channels.auth import (
    AsyncSessionRevocationMixin,
    SessionRevocationMixin,
    arevoke_session,
    get_session_revocation_group,
    login,
    logout,
    revoke_session,
)
from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer, WebsocketConsumer
from channels.layers import get_channel_layer
from channels.sessions import SessionMiddlewareStack
from channels.testing import WebsocketCommunicator

CHANNEL_LAYERS_SETTING = {
    "default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}
}


class AsyncRevocationConsumer(AsyncSessionRevocationMixin, AsyncWebsocketConsumer):
    async def receive(self, text_data=None, bytes_data=None):
        if text_data == "rotate":
            # Simulate a re-login cycling the session key.
            await database_sync_to_async(self.scope["session"].cycle_key)()
            await self.send(text_data=self.scope["session"].session_key)
        elif text_data is not None:
            await self.send(text_data="echo:" + text_data)


class SyncRevocationConsumer(SessionRevocationMixin, WebsocketConsumer):
    def receive(self, text_data=None, bytes_data=None):
        if text_data is not None:
            self.send(text_data="echo:" + text_data)


def get_session_store():
    return import_module(settings.SESSION_ENGINE).SessionStore


def create_session(data=None):
    session = get_session_store()()
    for key, value in (data or {}).items():
        session[key] = value
    session.save()
    return session


def make_headers(session_key):
    return [(b"cookie", b"sessionid=" + session_key.encode("ascii"))]


def make_communicator(consumer, session_key=None):
    app = SessionMiddlewareStack(consumer.as_asgi())
    headers = make_headers(session_key) if session_key else []
    return WebsocketCommunicator(app, "/ws/", headers=headers)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_connection_subscribes_to_hashed_session_group():
    """
    Each connection subscribes to a group derived from its own session key;
    the raw session key never appears in the group name.
    """
    session = await database_sync_to_async(create_session)({"user_id": 1})
    with override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS_SETTING):
        channel_layer = get_channel_layer()
        communicator = make_communicator(AsyncRevocationConsumer, session.session_key)
        connected, _ = await communicator.connect()
        assert connected
        group = get_session_revocation_group(session.session_key)
        assert list(channel_layer.groups) == [group]
        assert session.session_key not in group
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_revocation_closes_connection():
    """
    Revoking the session closes connections holding it and cleans up their
    group subscription.
    """
    session = await database_sync_to_async(create_session)({"user_id": 1})
    with override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS_SETTING):
        channel_layer = get_channel_layer()
        communicator = make_communicator(AsyncRevocationConsumer, session.session_key)
        connected, _ = await communicator.connect()
        assert connected

        await arevoke_session(session.session_key)
        response = await communicator.receive_output()
        assert response["type"] == "websocket.close"
        assert channel_layer.groups == {}
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_revocation_close_code_is_configurable():
    """
    The close code used on revocation can be customized.
    """

    class CodedConsumer(AsyncRevocationConsumer):
        session_revocation_close_code = 4001

    session = await database_sync_to_async(create_session)({"user_id": 1})
    with override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS_SETTING):
        communicator = make_communicator(CodedConsumer, session.session_key)
        connected, _ = await communicator.connect()
        assert connected

        await arevoke_session(session.session_key)
        response = await communicator.receive_output()
        assert response["type"] == "websocket.close"
        assert response["code"] == 4001
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_revocation_message_does_not_contain_session_key():
    """
    The revocation message identifies the session by hash only.
    """
    session_key = "secret-session-key"
    with override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS_SETTING):
        channel_layer = get_channel_layer()
        channel = await channel_layer.new_channel()
        await channel_layer.group_add(
            get_session_revocation_group(session_key), channel
        )
        await arevoke_session(session_key)
        message = await channel_layer.receive(channel)
        assert message["type"] == "session.revoked"
        assert message["session"] == hashlib.sha256(
            session_key.encode("utf-8")
        ).hexdigest()
        assert session_key not in json.dumps(message)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_revocation_between_session_read_and_subscribe_closes_connection():
    """
    If the session is revoked after the middleware read it but before the
    connection subscribed, the re-check on subscribe catches it and the
    connection is refused.
    """
    session = await database_sync_to_async(create_session)({"user_id": 1})
    # Snapshot of the session as read by the middleware, while still valid.
    def read_session():
        stale_session = get_session_store()(session.session_key)
        assert stale_session.keys()
        return stale_session

    stale_session = await database_sync_to_async(read_session)()
    # The session is revoked before the connection subscribes.
    await database_sync_to_async(session.delete)()

    consumer = AsyncRevocationConsumer.as_asgi()

    async def app(scope, receive, send):
        await consumer(dict(scope, session=stale_session), receive, send)

    with override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS_SETTING):
        communicator = WebsocketCommunicator(app, "/ws/")
        connected, _ = await communicator.connect()
        assert not connected
        assert get_channel_layer().groups == {}
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_late_revocation_for_old_session_is_ignored_after_rotation():
    """
    When the session key rotates (e.g. the user logs in again), the
    subscription follows the new key and a late revocation for the old
    session does not close the connection.
    """
    session = await database_sync_to_async(create_session)({"user_id": 1})
    old_key = session.session_key
    with override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS_SETTING):
        channel_layer = get_channel_layer()
        communicator = make_communicator(AsyncRevocationConsumer, old_key)
        connected, _ = await communicator.connect()
        assert connected
        assert get_session_revocation_group(old_key) in channel_layer.groups

        # Rotate the session key, as a fresh login would.
        await communicator.send_to(text_data="rotate")
        new_key = await communicator.receive_from()
        assert new_key != old_key

        # A late revocation for the old session must not close the connection.
        await arevoke_session(old_key)
        await communicator.send_to(text_data="ping")
        assert await communicator.receive_from() == "echo:ping"

        # The subscription now follows the new session key only.
        assert get_session_revocation_group(new_key) in channel_layer.groups
        assert get_session_revocation_group(old_key) not in channel_layer.groups

        # Revoking the current session still closes the connection.
        await arevoke_session(new_key)
        response = await communicator.receive_output()
        assert response["type"] == "websocket.close"
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_anonymous_connection_is_not_subscribed():
    """
    Connections without a session key subscribe to nothing and are not
    affected by revocation broadcasts.
    """
    with override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS_SETTING):
        channel_layer = get_channel_layer()
        communicator = make_communicator(AsyncRevocationConsumer)
        connected, _ = await communicator.connect()
        assert connected
        assert channel_layer.groups == {}

        await arevoke_session("some-other-session")
        await communicator.send_to(text_data="ping")
        assert await communicator.receive_from() == "echo:ping"

        await communicator.disconnect()
        assert channel_layer.groups == {}


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_subscription_is_cleaned_up_on_disconnect():
    """
    Disconnecting removes the connection from its session revocation group.
    """
    session = await database_sync_to_async(create_session)({"user_id": 1})
    with override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS_SETTING):
        channel_layer = get_channel_layer()
        communicator = make_communicator(AsyncRevocationConsumer, session.session_key)
        connected, _ = await communicator.connect()
        assert connected
        group = get_session_revocation_group(session.session_key)
        assert len(channel_layer.groups[group]) == 1

        await communicator.disconnect()
        assert channel_layer.groups == {}


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_logout_revokes_session():
    """
    channels.auth.logout() broadcasts a revocation for the flushed session.
    """
    user = await database_sync_to_async(get_user_model().objects.create_user)("bob")
    scope = {"session": await database_sync_to_async(create_session)()}
    await login(scope, user)
    session_key = scope["session"].session_key

    with override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS_SETTING):
        communicator = make_communicator(AsyncRevocationConsumer, session_key)
        connected, _ = await communicator.connect()
        assert connected

        await logout(scope)
        response = await communicator.receive_output()
        assert response["type"] == "websocket.close"
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_django_logout_signal_revokes_session():
    """
    Logging out through Django's auth (e.g. an HTTP view in another tab)
    closes connections holding that session.
    """
    session = await database_sync_to_async(create_session)({"user_id": 1})
    with override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS_SETTING):
        communicator = make_communicator(AsyncRevocationConsumer, session.session_key)
        connected, _ = await communicator.connect()
        assert connected

        request = SimpleNamespace(session=get_session_store()(session.session_key))
        user_logged_out.send(sender=get_user_model(), request=request, user=None)
        response = await communicator.receive_output()
        assert response["type"] == "websocket.close"
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_sync_revoke_session_closes_connection():
    """
    The synchronous revoke_session() wrapper works from async contexts.
    """
    session = await database_sync_to_async(create_session)({"user_id": 1})
    with override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS_SETTING):
        communicator = make_communicator(AsyncRevocationConsumer, session.session_key)
        connected, _ = await communicator.connect()
        assert connected

        revoke_session(session.session_key)
        response = await communicator.receive_output()
        assert response["type"] == "websocket.close"
        await communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_sync_consumer_is_closed_on_revocation():
    """
    SessionRevocationMixin brings the same behavior to WebsocketConsumer.
    """
    session = await database_sync_to_async(create_session)({"user_id": 1})
    with override_settings(CHANNEL_LAYERS=CHANNEL_LAYERS_SETTING):
        channel_layer = get_channel_layer()
        communicator = make_communicator(SyncRevocationConsumer, session.session_key)
        connected, _ = await communicator.connect()
        assert connected
        group = get_session_revocation_group(session.session_key)
        assert group in channel_layer.groups

        await communicator.send_to(text_data="ping")
        assert await communicator.receive_from() == "echo:ping"

        await arevoke_session(session.session_key)
        response = await communicator.receive_output()
        assert response["type"] == "websocket.close"
        await communicator.disconnect()
        assert channel_layer.groups == {}


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_revocation_without_channel_layer_is_noop():
    """
    Revocation and logout degrade gracefully when no channel layer is
    configured.
    """
    with override_settings(CHANNEL_LAYERS={}):
        await arevoke_session("some-session")
        revoke_session("some-session")
        scope = {"session": await database_sync_to_async(create_session)()}
        await logout(scope)
