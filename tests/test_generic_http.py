import asyncio
import json
import time

import pytest

import channels.consumer
from channels.exceptions import RequestAborted
from channels.generic.http import AsyncHttpConsumer
from channels.testing import HttpCommunicator


@pytest.fixture
def noop_aclose_old_connections(monkeypatch):
    """
    Replaces ``aclose_old_connections`` (invoked on every dispatch) with a
    no-op. It goes through ``sync_to_async``, whose executor completion
    cannot wake the event loop in some sandboxed environments, which would
    otherwise hang any test that dispatches more than one message. It is
    orthogonal to the streaming behavior under test.
    """

    async def noop():
        pass

    monkeypatch.setattr(channels.consumer, "aclose_old_connections", noop)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_http_consumer():
    """
    Tests that AsyncHttpConsumer is implemented correctly.
    """

    class TestConsumer(AsyncHttpConsumer):
        async def handle(self, body):
            data = json.loads(body.decode("utf-8"))
            await self.send_response(
                200,
                json.dumps({"value": data["value"]}).encode("utf-8"),
                headers={b"Content-Type": b"application/json"},
            )

    app = TestConsumer()

    # Open a connection
    communicator = HttpCommunicator(
        app,
        method="POST",
        path="/test/",
        body=json.dumps({"value": 42, "anything": False}).encode("utf-8"),
    )
    response = await communicator.get_response()
    assert response["body"] == b'{"value": 42}'
    assert response["status"] == 200
    assert response["headers"] == [(b"Content-Type", b"application/json")]


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_error():
    class TestConsumer(AsyncHttpConsumer):
        async def handle(self, body):
            raise AssertionError("Error correctly raised")

    communicator = HttpCommunicator(TestConsumer(), "GET", "/")
    with pytest.raises(AssertionError) as excinfo:
        await communicator.get_response(timeout=0.05)

    assert str(excinfo.value) == "Error correctly raised"


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_per_scope_consumers():
    """
    Tests that a distinct consumer is used per scope, with AsyncHttpConsumer as
    the example consumer class.
    """

    class TestConsumer(AsyncHttpConsumer):
        def __init__(self):
            super().__init__()
            self.time = time.time()

        async def handle(self, body):
            body = f"{self.__class__.__name__} {id(self)} {self.time}"

            await self.send_response(
                200,
                body.encode("utf-8"),
                headers={b"Content-Type": b"text/plain"},
            )

    app = TestConsumer.as_asgi()

    # Open a connection
    communicator = HttpCommunicator(app, method="GET", path="/test/")
    response = await communicator.get_response()
    assert response["status"] == 200

    # And another one.
    communicator = HttpCommunicator(app, method="GET", path="/test2/")
    second_response = await communicator.get_response()
    assert second_response["status"] == 200

    assert response["body"] != second_response["body"]


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_http_consumer_future():
    """
    Regression test for channels accepting only coroutines. The ASGI specification
    states that the `receive` and `send` arguments to an ASGI application should be
    "awaitable callable" objects. That includes non-coroutine functions that return
    Futures.
    """

    class TestConsumer(AsyncHttpConsumer):
        async def handle(self, body):
            await self.send_response(
                200,
                b"42",
                headers={b"Content-Type": b"text/plain"},
            )

    app = TestConsumer()

    # Ensure the passed functions are specifically coroutines.
    async def coroutine_app(scope, receive, send):
        async def receive_coroutine():
            return await asyncio.ensure_future(receive())

        async def send_coroutine(*args, **kwargs):
            return await asyncio.ensure_future(send(*args, **kwargs))

        await app(scope, receive_coroutine, send_coroutine)

    communicator = HttpCommunicator(coroutine_app, method="GET", path="/")
    response = await communicator.get_response()
    assert response["body"] == b"42"
    assert response["status"] == 200
    assert response["headers"] == [(b"Content-Type", b"text/plain")]

    # Ensure the passed functions are "Awaitable Callables" and NOT coroutines.
    async def awaitable_callable_app(scope, receive, send):
        def receive_awaitable_callable():
            return asyncio.ensure_future(receive())

        def send_awaitable_callable(*args, **kwargs):
            return asyncio.ensure_future(send(*args, **kwargs))

        await app(scope, receive_awaitable_callable, send_awaitable_callable)

    # Open a connection
    communicator = HttpCommunicator(awaitable_callable_app, method="GET", path="/")
    response = await communicator.get_response()
    assert response["body"] == b"42"
    assert response["status"] == 200
    assert response["headers"] == [(b"Content-Type", b"text/plain")]


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_http_consumer_streaming(noop_aclose_old_connections):
    """
    Tests that handle_stream processes the first body chunk and responds
    before the last chunk is even sent, and that the consumer then exits
    without waiting for the rest of the body.
    """
    processed = []

    class TestConsumer(AsyncHttpConsumer):
        async def handle_stream(self, stream):
            async for chunk in stream:
                processed.append(chunk)
                # Respond straight away, without reading the rest of the body.
                await self.send_response(200, b"first:" + chunk)
                return

    communicator = HttpCommunicator(TestConsumer(), "POST", "/")
    communicator.sent_request = True  # Chunks are pushed manually below.
    await communicator.send_input(
        {"type": "http.request", "body": b"one", "more_body": True}
    )
    # The response arrives before the final chunk has been sent.
    response = await communicator.get_response()
    assert response["status"] == 200
    assert response["body"] == b"first:one"
    assert processed == [b"one"]
    # The consumer exited rather than staying parked on receive().
    await communicator.wait()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_http_consumer_streaming_empty_chunks(
    noop_aclose_old_connections,
):
    """
    Tests that empty chunks are delivered to handle_stream without being
    confused with the end-of-body signal.
    """
    collected = []

    class TestConsumer(AsyncHttpConsumer):
        async def handle_stream(self, stream):
            async for chunk in stream:
                collected.append(chunk)
            await self.send_response(200, b"".join(collected))

    communicator = HttpCommunicator(TestConsumer(), "POST", "/")
    communicator.sent_request = True  # Chunks are pushed manually below.
    await communicator.send_input(
        {"type": "http.request", "body": b"a", "more_body": True}
    )
    await communicator.send_input(
        {"type": "http.request", "body": b"", "more_body": True}
    )
    await communicator.send_input(
        {"type": "http.request", "body": b"", "more_body": True}
    )
    await communicator.send_input({"type": "http.request", "body": b"b"})
    response = await communicator.get_response()
    assert response["status"] == 200
    assert response["body"] == b"ab"
    assert collected == [b"a", b"", b"", b"b"]
    await communicator.wait()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_http_consumer_streaming_empty_body(
    noop_aclose_old_connections,
):
    """
    Tests that an empty request body terminates the stream immediately
    instead of hanging handle_stream.
    """

    class TestConsumer(AsyncHttpConsumer):
        async def handle_stream(self, stream):
            chunks = [chunk async for chunk in stream]
            await self.send_response(200, b"".join(chunks) or b"empty")

    communicator = HttpCommunicator(TestConsumer(), "GET", "/")
    response = await communicator.get_response()
    assert response["status"] == 200
    assert response["body"] == b"empty"
    await communicator.wait()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_http_consumer_streaming_disconnect(
    noop_aclose_old_connections,
):
    """
    Tests that a disconnect mid-upload is distinguishable from a normal end
    of the body via RequestAborted.
    """
    results = []

    class TestConsumer(AsyncHttpConsumer):
        async def handle_stream(self, stream):
            try:
                async for chunk in stream:
                    pass
            except RequestAborted:
                results.append("aborted")
                await self.send_response(200, b"aborted")

    communicator = HttpCommunicator(TestConsumer(), "POST", "/")
    communicator.sent_request = True  # Chunks are pushed manually below.
    await communicator.send_input(
        {"type": "http.request", "body": b"a", "more_body": True}
    )
    await communicator.send_input({"type": "http.disconnect"})
    response = await communicator.get_response()
    assert response["status"] == 200
    assert response["body"] == b"aborted"
    assert results == ["aborted"]
    await communicator.wait()
