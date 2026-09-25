import asyncio
import contextlib
import hashlib

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

from channels.db import database_sync_to_async
from channels.exceptions import InvalidChannelLayerError
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


async def logout(scope):
    """
    Remove the authenticated user's ID from the request and flush their session
    data.

    The flushed session key is also revoked, so any other connections
    subscribed to it via ``SessionRevocationMiddleware`` are closed.
    """
    session_key = await _logout_session(scope)
    if session_key:
        await arevoke_session(session_key)


@database_sync_to_async
def _logout_session(scope):
    """
    Remove the authenticated user's ID from the request and flush their session
    data. Returns the key the session had before it was flushed.
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


def _get_user_session_key(session):
    # This value in the session is always serialized to a string, so we need
    # to convert it back to Python whenever we access it.
    return get_user_model()._meta.pk.to_python(session[SESSION_KEY])


# Message type used to tell connections that their session was revoked.
SESSION_REVOKED_MESSAGE_TYPE = "channels.session.revoked"

# Prefix for the per-session channel layer groups used for revocation.
SESSION_REVOCATION_GROUP_PREFIX = "session-revoke"


def _session_key_hash(session_key):
    """
    Hashes a session key, so neither group names nor channel layer messages
    ever contain the raw session cookie value.
    """
    return hashlib.sha256(session_key.encode("utf-8")).hexdigest()


def _session_revocation_group(session_key_hash):
    return "%s-%s" % (SESSION_REVOCATION_GROUP_PREFIX, session_key_hash)


async def arevoke_session(session_key, channel_layer=None):
    """
    Revokes the session with the given key: any WebSocket connection that was
    established with it (and is wrapped in ``SessionRevocationMiddleware``)
    is closed promptly.

    This is a no-op if no channel layer is configured or the layer does not
    support groups, so it is always safe to call.
    """
    if not session_key:
        return
    if channel_layer is None:
        channel_layer = get_channel_layer()
    if channel_layer is None or not hasattr(channel_layer, "group_send"):
        return
    session_key_hash = _session_key_hash(session_key)
    await channel_layer.group_send(
        _session_revocation_group(session_key_hash),
        {
            "type": SESSION_REVOKED_MESSAGE_TYPE,
            "session_key_hash": session_key_hash,
        },
    )


def revoke_session(session_key, channel_layer=None):
    """
    Sync version of ``arevoke_session``. Suitable for admin actions,
    management commands and other synchronous code that deletes sessions.
    """
    async_to_sync(arevoke_session)(session_key, channel_layer=channel_layer)


def revoke_session_on_logout(sender, request, user, **kwargs):
    """
    ``user_logged_out`` signal receiver that revokes the session being
    logged out of, so connections holding it (e.g. WebSockets open in other
    tabs) are closed. Connected below; the signal is sent by Django before
    the session is flushed, so the session key is still available.
    """
    if request is None:
        # The logout did not happen through an HTTP request (e.g. it came
        # from channels.auth.logout, which revokes the session itself).
        return
    session = getattr(request, "session", None)
    session_key = getattr(session, "session_key", None)
    if session_key:
        revoke_session(session_key)


user_logged_out.connect(
    revoke_session_on_logout,
    dispatch_uid="channels.auth.revoke_session_on_logout",
)


class _SessionRevocationConnection:
    """
    Tracks the session revocation subscription of a single WebSocket
    connection. Created per connection by SessionRevocationMiddleware.
    """

    def __init__(self, scope, channel_layer, inner):
        self.scope = scope
        self.session = scope["session"]
        self.channel_layer = channel_layer
        self.inner = inner
        self.channel_name = None
        # Hash of the session key we are currently subscribed to, if any.
        self.session_key_hash = None
        # Set once a matching revocation message has arrived.
        self.revoked = asyncio.Event()

    async def __call__(self, receive, send):
        self.channel_name = await self.channel_layer.new_channel()
        try:
            await self.sync_subscription()
            # The session may have been revoked between SessionMiddleware
            # loading it and the subscription above; check the session store
            # again so that a revocation in that window is not missed.
            if (
                self.session_key_hash is not None
                and not await self.session_is_valid()
            ):
                await send({"type": "websocket.close"})
                return
            watcher = asyncio.ensure_future(self.watch_for_revocation(send))
            try:
                return await self.inner(
                    self.scope,
                    self.wrap_receive(receive),
                    self.wrap_send(send),
                )
            finally:
                watcher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await watcher
        finally:
            await self.unsubscribe()

    def current_session_key_hash(self):
        """
        Returns the hash of the session key to subscribe to right now, or
        None if the connection should not be tracked. Only authenticated
        sessions are tracked; anonymous connections are left alone.
        """
        if SESSION_KEY not in self.session:
            return None
        session_key = self.session.session_key
        if not session_key:
            return None
        return _session_key_hash(session_key)

    async def sync_subscription(self):
        """
        Aligns the group subscription with the scope session, following
        session key rotation (e.g. after channels.auth.login).
        """
        desired = self.current_session_key_hash()
        if desired == self.session_key_hash:
            return
        await self.unsubscribe()
        self.session_key_hash = desired
        if desired is not None:
            await self.channel_layer.group_add(
                _session_revocation_group(desired), self.channel_name
            )

    async def unsubscribe(self):
        if self.session_key_hash is not None:
            await self.channel_layer.group_discard(
                _session_revocation_group(self.session_key_hash),
                self.channel_name,
            )
            self.session_key_hash = None

    async def session_is_valid(self):
        """
        Re-reads the session from the store and checks it still authenticates
        the same user, to catch revocations that raced the subscription.
        """
        session_key = self.session.session_key
        user_id = self.session.get(SESSION_KEY)
        session_auth_hash = self.session.get(HASH_SESSION_KEY)

        @database_sync_to_async
        def check():
            fresh_session = self.session.__class__(session_key)
            return (
                fresh_session.get(SESSION_KEY) == user_id
                and fresh_session.get(HASH_SESSION_KEY) == session_auth_hash
            )

        return await check()

    async def watch_for_revocation(self, send):
        """
        Waits for a revocation message for the subscribed session, then
        closes the connection.
        """
        while True:
            message = await self.channel_layer.receive(self.channel_name)
            if message.get("type") != SESSION_REVOKED_MESSAGE_TYPE:
                continue
            # Ignore revocations for other sessions - in particular a late
            # revocation of a previous (rotated-away) session key must not
            # close the connection of the user's new session.
            if message.get("session_key_hash") != self.session_key_hash:
                continue
            await send({"type": "websocket.close"})
            self.revoked.set()
            return

    def wrap_receive(self, receive):
        """
        Wraps the application's receive callable to keep the subscription in
        sync with the session, and to turn a revocation into a clean
        disconnect so the inner application shuts down normally.
        """

        async def revocation_aware_receive():
            await self.sync_subscription()
            receive_task = asyncio.ensure_future(receive())
            revoked_task = asyncio.ensure_future(self.revoked.wait())
            try:
                done, pending = await asyncio.wait(
                    {receive_task, revoked_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in (receive_task, revoked_task):
                    if not task.done():
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
            if revoked_task in done:
                return {"type": "websocket.disconnect", "code": 1000}
            return receive_task.result()

        return revocation_aware_receive

    def wrap_send(self, send):
        """
        Wraps the application's send callable to follow session key rotation
        (e.g. login during the connection) as soon as the application
        responds.
        """

        async def send_and_sync(message):
            await send(message)
            if message["type"] == "websocket.send":
                await self.sync_subscription()

        return send_and_sync


class SessionRevocationMiddleware:
    """
    WebSocket middleware that closes the connection as soon as the Django
    session it was established with is revoked - for example because the
    user logged out in another browser tab, or an administrator deleted the
    session server-side.

    Each connection subscribes to a channel layer group derived from a hash
    of its own session key only, and revocation messages never contain the
    raw session cookie. Requires ``SessionMiddleware`` above it and a
    channel layer with groups support. Anonymous connections and non-
    WebSocket scopes are passed through untouched.
    """

    def __init__(self, inner):
        self.inner = inner

    async def __call__(self, scope, receive, send):
        # Only WebSocket connections are long-lived enough to need tracking.
        if scope["type"] != "websocket":
            return await self.inner(scope, receive, send)
        if "session" not in scope:
            raise ValueError(
                "SessionRevocationMiddleware cannot find session in scope. "
                "SessionMiddleware must be above it."
            )
        channel_layer = get_channel_layer()
        if channel_layer is None:
            # Without a channel layer revocations cannot be delivered.
            return await self.inner(scope, receive, send)
        if not hasattr(channel_layer, "group_add"):
            raise InvalidChannelLayerError(
                "SessionRevocationMiddleware requires a channel layer that "
                "supports groups."
            )
        connection = _SessionRevocationConnection(scope, channel_layer, self.inner)
        return await connection(receive, send)


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
