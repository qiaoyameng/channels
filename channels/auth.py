import asyncio
import hashlib
from importlib import import_module

from asgiref.sync import async_to_sync
from django.conf import settings
from django.contrib.auth import (
    BACKEND_SESSION_KEY,
    HASH_SESSION_KEY,
    SESSION_KEY,
    _get_backends,
    get_user_model,
    load_backend,
    user_logged_in,
    user_logged_out,
)
from django.utils.crypto import constant_time_compare
from django.utils.functional import LazyObject

from channels.consumer import get_handler_name
from channels.db import database_sync_to_async
from channels.layers import get_channel_layer
from channels.middleware import BaseMiddleware
from channels.sessions import CookieMiddleware, SessionMiddleware


@database_sync_to_async
def get_user(scope):
    """
    Return the user model instance associated with the given scope.
    If no user is retrieved, return an instance of `AnonymousUser`.
    """
    # postpone model import to avoid ImproperlyConfigured error before Django
    # setup is complete.
    from django.contrib.auth.models import AnonymousUser

    if "session" not in scope:
        raise ValueError(
            "Cannot find session in scope. You should wrap your consumer in "
            "SessionMiddleware."
        )
    session = scope["session"]
    user = None
    try:
        user_id = _get_user_session_key(session)
        backend_path = session[BACKEND_SESSION_KEY]
    except KeyError:
        pass
    else:
        if backend_path in settings.AUTHENTICATION_BACKENDS:
            backend = load_backend(backend_path)
            user = backend.get_user(user_id)
            # Verify the session
            if hasattr(user, "get_session_auth_hash"):
                session_hash = session.get(HASH_SESSION_KEY)
                session_hash_verified = session_hash and constant_time_compare(
                    session_hash, user.get_session_auth_hash()
                )
                if not session_hash_verified:
                    session.flush()
                    user = None
    return user or AnonymousUser()


@database_sync_to_async
def login(scope, user, backend=None):
    """
    Persist a user id and a backend in the request.
    This way a user doesn't have to re-authenticate on every request.
    Note that data set during the anonymous session is retained when the user
    logs in.
    """
    if "session" not in scope:
        raise ValueError(
            "Cannot find session in scope. You should wrap your consumer in "
            "SessionMiddleware."
        )
    session = scope["session"]
    session_auth_hash = ""
    if user is None:
        user = scope.get("user", None)
    if user is None:
        raise ValueError(
            "User must be passed as an argument or must be present in the scope."
        )
    if hasattr(user, "get_session_auth_hash"):
        session_auth_hash = user.get_session_auth_hash()
    if SESSION_KEY in session:
        if _get_user_session_key(session) != user.pk or (
            session_auth_hash
            and not constant_time_compare(
                session.get(HASH_SESSION_KEY, ""), session_auth_hash
            )
        ):
            # To avoid reusing another user's session, create a new, empty
            # session if the existing session corresponds to a different
            # authenticated user.
            session.flush()
    else:
        session.cycle_key()
    try:
        backend = backend or user.backend
    except AttributeError:
        backends = _get_backends(return_tuples=True)
        if len(backends) == 1:
            _, backend = backends[0]
        else:
            raise ValueError(
                "You have multiple authentication backends configured and "
                "therefore must provide the `backend` "
                "argument or set the `backend` attribute on the user."
            )
    session[SESSION_KEY] = user._meta.pk.value_to_string(user)
    session[BACKEND_SESSION_KEY] = backend
    session[HASH_SESSION_KEY] = session_auth_hash
    scope["user"] = user
    # note this does not reset the CSRF_COOKIE/Token
    user_logged_in.send(sender=user.__class__, request=None, user=user)


@database_sync_to_async
def _logout(scope):
    """
    Synchronous part of logout(). Returns the key of the session that was
    flushed, or None if there was none.
    """
    # postpone model import to avoid ImproperlyConfigured error before Django
    # setup is complete.
    from django.contrib.auth.models import AnonymousUser

    if "session" not in scope:
        raise ValueError(
            "Login cannot find session in scope. You should wrap your "
            "consumer in SessionMiddleware."
        )
    session = scope["session"]
    # Dispatch the signal before the user is logged out so the receivers have a
    # chance to find out *who* logged out.
    user = scope.get("user", None)
    if hasattr(user, "is_authenticated") and not user.is_authenticated:
        user = None
    if user is not None:
        user_logged_out.send(sender=user.__class__, request=None, user=user)
    session_key = session.session_key
    session.flush()
    if "user" in scope:
        scope["user"] = AnonymousUser()
    return session_key


async def logout(scope):
    """
    Remove the authenticated user's ID from the request and flush their session
    data.

    Also broadcasts a session revocation notice for the flushed session, so
    that any connections using SessionRevocationMixin (or
    AsyncSessionRevocationMixin) with that session are closed.
    """
    session_key = await _logout(scope)
    if session_key:
        await arevoke_session(session_key)


def _get_user_session_key(session):
    # This value in the session is always serialized to a string, so we need
    # to convert it back to Python whenever we access it.
    return get_user_model()._meta.pk.to_python(session[SESSION_KEY])


class UserLazyObject(LazyObject):
    """
    Throw a more useful error message when scope['user'] is accessed before
    it's resolved
    """

    def _setup(self):
        raise ValueError("Accessing scope user before it is ready.")


class AuthMiddleware(BaseMiddleware):
    """
    Middleware which populates scope["user"] from a Django session.
    Requires SessionMiddleware to function.
    """

    def populate_scope(self, scope):
        # Make sure we have a session
        if "session" not in scope:
            raise ValueError(
                "AuthMiddleware cannot find session in scope. "
                "SessionMiddleware must be above it."
            )
        # Add it to the scope if it's not there already
        if "user" not in scope:
            scope["user"] = UserLazyObject()

    async def resolve_scope(self, scope):
        scope["user"]._wrapped = await get_user(scope)

    async def __call__(self, scope, receive, send):
        scope = dict(scope)
        # Scope injection/mutation per this middleware's needs.
        self.populate_scope(scope)
        # Grab the finalized/resolved scope
        await self.resolve_scope(scope)

        return await super().__call__(scope, receive, send)


# Handy shortcut for applying all three layers at once
def AuthMiddlewareStack(inner):
    return CookieMiddleware(SessionMiddleware(AuthMiddleware(inner)))


# Session revocation
# ------------------
#
# An opt-in mechanism that closes connections when the session they were
# opened with is revoked (for example by the user logging out elsewhere, or
# by an administrator deleting the session). Connections subscribe to a
# channel layer group derived from a hash of their session key, so the raw
# session key (the cookie value) never appears in channel layer messages.

SESSION_REVOCATION_GROUP_PREFIX = "channels.session."


def _hash_session_key(session_key):
    """
    Returns a stable, non-reversible identifier for a session key. Used both
    for group names and inside revocation messages so the raw session key is
    never exposed to the channel layer.
    """
    return hashlib.sha256(session_key.encode("utf-8")).hexdigest()


def get_session_revocation_group(session_key):
    """
    Returns the channel layer group name that carries revocation notices for
    the given session key.
    """
    return SESSION_REVOCATION_GROUP_PREFIX + _hash_session_key(session_key)


def _groups_supported(channel_layer):
    return channel_layer is not None and "groups" in getattr(
        channel_layer, "extensions", []
    )


async def arevoke_session(session_key, *, channel_layer=None):
    """
    Broadcasts a revocation notice for the given session key. Any connection
    using SessionRevocationMixin (or AsyncSessionRevocationMixin) that still
    holds this session is closed. Does nothing if there is no channel layer,
    or the channel layer does not support groups.
    """
    if not session_key:
        return
    if channel_layer is None:
        channel_layer = get_channel_layer()
    if not _groups_supported(channel_layer):
        return
    await channel_layer.group_send(
        get_session_revocation_group(session_key),
        {"type": "session.revoked", "session": _hash_session_key(session_key)},
    )


def revoke_session(session_key, *, channel_layer=None):
    """
    Synchronous version of arevoke_session(), for use from sync code such as
    admin actions, management commands or signal receivers. When called from
    within a running event loop the broadcast is scheduled onto it.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        async_to_sync(arevoke_session)(session_key, channel_layer=channel_layer)
    else:
        asyncio.ensure_future(
            arevoke_session(session_key, channel_layer=channel_layer)
        )


def revoke_session_on_logout(sender, request=None, **kwargs):
    """
    Receiver for Django's ``user_logged_out`` signal that revokes the session
    being logged out of, so connections holding it are closed. Connected in
    ChannelsConfig.ready().
    """
    if request is None:
        # Logouts via channels.auth.logout() pass no request; that function
        # broadcasts the revocation itself.
        return
    session = getattr(request, "session", None)
    session_key = getattr(session, "session_key", None)
    if session_key:
        revoke_session(session_key)


def _session_data_vanished(session, session_key):
    """
    Returns True if the given session held data when it was read, but its key
    is no longer present in the session store - meaning it was revoked in
    between the read and the connection subscribing to revocation notices.
    """
    if not session_key or not session.keys():
        # Sessions that were empty when read have nothing to revoke, and may
        # legitimately not exist in the store (e.g. never-saved sessions).
        return False
    session_store = import_module(settings.SESSION_ENGINE).SessionStore
    try:
        return not session_store(session_key).exists(session_key)
    except NotImplementedError:
        # Stateless session backends (e.g. signed cookies) cannot be revoked.
        return False


def _session_revocation_scope_key(scope):
    if "session" not in scope:
        raise ValueError(
            "Cannot find session in scope. You should wrap your consumer in "
            "SessionMiddleware to use session revocation."
        )
    return scope["session"].session_key or None


class AsyncSessionRevocationMixin:
    """
    Mixin for AsyncWebsocketConsumer that closes the connection when the
    session it was opened with is revoked - for example when the user logs
    out in another tab, or an administrator deletes the session.

    Requires SessionMiddleware (or AuthMiddlewareStack) and a channel layer
    that supports groups. Anonymous connections without a session key are
    not subscribed to anything and are never closed by this mechanism. If
    the session key rotates (e.g. the user logs in again) the subscription
    transparently follows the new key, and late revocation notices for the
    old session are ignored.
    """

    session_revocation_close_code = None

    async def dispatch(self, message):
        if message["type"] != "websocket.disconnect":
            if await self._session_revocation_subscribe():
                await self._session_revocation_close()
                return
        await super().dispatch(message)

    async def websocket_disconnect(self, message):
        await self._session_revocation_unsubscribe()
        await super().websocket_disconnect(message)

    async def session_revoked(self, event):
        """
        Handles a revocation notice from the channel layer.
        """
        session_key = _session_revocation_scope_key(self.scope)
        if session_key is None:
            return
        if event.get("session") != _hash_session_key(session_key):
            # Late revocation for a session this connection no longer holds
            # (the key rotated, e.g. the user logged in again).
            return
        await self._session_revocation_close()

    async def _session_revocation_subscribe(self):
        """
        Keeps the connection's group membership in sync with the current
        session key. Returns True if the session turns out to have been
        revoked already.
        """
        session_key = _session_revocation_scope_key(self.scope)
        if not _groups_supported(self.channel_layer):
            return False
        group = get_session_revocation_group(session_key) if session_key else None
        if group == getattr(self, "_session_revocation_group", None):
            return False
        await self._session_revocation_unsubscribe()
        self._session_revocation_group = group
        if group is None:
            return False
        await self.channel_layer.group_add(group, self.channel_name)
        # The session was read before we subscribed; re-check the store so a
        # revocation that fell in between is not missed.
        return await database_sync_to_async(_session_data_vanished)(
            self.scope["session"], session_key
        )

    async def _session_revocation_unsubscribe(self):
        group = getattr(self, "_session_revocation_group", None)
        self._session_revocation_group = None
        if group is not None and _groups_supported(self.channel_layer):
            await self.channel_layer.group_discard(group, self.channel_name)

    async def _session_revocation_close(self):
        await self._session_revocation_unsubscribe()
        await self.close(code=self.session_revocation_close_code)


class SessionRevocationMixin:
    """
    Synchronous version of AsyncSessionRevocationMixin, for use with
    WebsocketConsumer.
    """

    session_revocation_close_code = None

    @database_sync_to_async
    def dispatch(self, message):
        if message["type"] != "websocket.disconnect":
            if self._session_revocation_subscribe():
                self._session_revocation_close()
                return
        # Mirror SyncConsumer.dispatch().
        handler = getattr(self, get_handler_name(message), None)
        if handler:
            handler(message)
        else:
            raise ValueError("No handler for message type %s" % message["type"])

    def websocket_disconnect(self, message):
        self._session_revocation_unsubscribe()
        super().websocket_disconnect(message)

    def session_revoked(self, event):
        """
        Handles a revocation notice from the channel layer.
        """
        session_key = _session_revocation_scope_key(self.scope)
        if session_key is None:
            return
        if event.get("session") != _hash_session_key(session_key):
            # Late revocation for a session this connection no longer holds
            # (the key rotated, e.g. the user logged in again).
            return
        self._session_revocation_close()

    def _session_revocation_subscribe(self):
        """
        Keeps the connection's group membership in sync with the current
        session key. Returns True if the session turns out to have been
        revoked already.
        """
        session_key = _session_revocation_scope_key(self.scope)
        if not _groups_supported(self.channel_layer):
            return False
        group = get_session_revocation_group(session_key) if session_key else None
        if group == getattr(self, "_session_revocation_group", None):
            return False
        self._session_revocation_unsubscribe()
        self._session_revocation_group = group
        if group is None:
            return False
        async_to_sync(self.channel_layer.group_add)(group, self.channel_name)
        # The session was read before we subscribed; re-check the store so a
        # revocation that fell in between is not missed.
        return _session_data_vanished(self.scope["session"], session_key)

    def _session_revocation_unsubscribe(self):
        group = getattr(self, "_session_revocation_group", None)
        self._session_revocation_group = None
        if group is not None and _groups_supported(self.channel_layer):
            async_to_sync(self.channel_layer.group_discard)(group, self.channel_name)

    def _session_revocation_close(self):
        self._session_revocation_unsubscribe()
        self.close(code=self.session_revocation_close_code)
