import asyncio
import json
import time

import pytest

from channels.exceptions import RequestAborted
from channels.generic.http import AsyncHttpConsumer
from channels.testing import ApplicationCommunicator, HttpCommunicator

HTTP_SCOPE = {"type": "http", "http_version": "1.1", "method": "POST", "path": "/"}


@pytest.fixture
def noop_connection_cleanup(monkeypatch):
    """
    The streaming tests below do not touch the database, so replace the
    per-message old-connection cleanup (which hops to a worker thread via
    sync_to_async) with a no-op to keep them self-contained.
    """

    async def aclose_old_connections():
        pass

    monkeypatch.setattr(
        "channels.consumer.aclose_old_connections", aclose_old_connections
    )
    monkeypatch.setattr(
        "channels.generic.http.aclose_old_connections", aclose_old_connections
    )


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


@pytest.mark.asyncio
async def test_async_http_consumer_streaming(noop_connection_cleanup):
    """
    Tests that handle_stream() receives body chunks as they arrive, before
    the final chunk has been sent.
    """
    received = []
    first_chunk_processed = asyncio.Event()

    class TestConsumer(AsyncHttpConsumer):
        async def handle_stream(self):
            async for chunk in self.stream_body():
                received.append(chunk)
                if len(received) == 1:
                    first_chunk_processed.set()
            await self.send_response(200, b"".join(received))

    communicator = ApplicationCommunicator(TestConsumer(), HTTP_SCOPE)
    await communicator.send_input(
        {"type": "http.request", "body": b"first", "more_body": True}
    )
    # The first chunk is processed before the last chunk has been sent.
    await asyncio.wait_for(first_chunk_processed.wait(), timeout=1)
    assert received == [b"first"]

    await communicator.send_input({"type": "http.request", "body": b"second"})
    response_start = await communicator.receive_output()
    assert response_start["type"] == "http.response.start"
    assert response_start["status"] == 200
    response_body = await communicator.receive_output()
    assert response_body["type"] == "http.response.body"
    assert response_body["body"] == b"firstsecond"
    await communicator.wait()


@pytest.mark.asyncio
async def test_async_http_consumer_streaming_empty_chunks(noop_connection_cleanup):
    """
    Tests that empty chunks and the more_body end-of-body signal are not
    confused: iteration only stops after a message without more_body.
    """
    received = []

    class TestConsumer(AsyncHttpConsumer):
        async def handle_stream(self):
            async for chunk in self.stream_body():
                received.append(chunk)
            await self.send_response(200, b"done")

    communicator = ApplicationCommunicator(TestConsumer(), HTTP_SCOPE)
    await communicator.send_input(
        {"type": "http.request", "body": b"", "more_body": True}
    )
    await communicator.send_input(
        {"type": "http.request", "body": b"", "more_body": True}
    )
    await communicator.send_input(
        {"type": "http.request", "body": b"data", "more_body": True}
    )
    await communicator.send_input({"type": "http.request", "body": b""})
    response_body = await communicator.receive_output()
    assert response_body["type"] == "http.response.start"
    response_body = await communicator.receive_output()
    assert response_body["body"] == b"done"
    assert received == [b"", b"", b"data", b""]
    await communicator.wait()


@pytest.mark.asyncio
async def test_async_http_consumer_streaming_empty_body(noop_connection_cleanup):
    """
    Tests that a streaming consumer handles a request with an empty body.
    """
    received = []

    class TestConsumer(AsyncHttpConsumer):
        async def handle_stream(self):
            async for chunk in self.stream_body():
                received.append(chunk)
            await self.send_response(200, b"done")

    communicator = ApplicationCommunicator(TestConsumer(), HTTP_SCOPE)
    await communicator.send_input({"type": "http.request", "body": b""})
    response_start = await communicator.receive_output()
    assert response_start["type"] == "http.response.start"
    response_body = await communicator.receive_output()
    assert response_body["body"] == b"done"
    assert received == [b""]
    await communicator.wait()


@pytest.mark.asyncio
async def test_async_http_consumer_streaming_disconnect(noop_connection_cleanup):
    """
    Tests that a client disconnect mid-upload raises RequestAborted in
    stream_body(), distinguishing it from a cleanly finished body.
    """
    outcome = []

    class TestConsumer(AsyncHttpConsumer):
        def __init__(self):
            super().__init__()
            self.disconnect_called = False

        async def handle_stream(self):
            try:
                async for chunk in self.stream_body():
                    pass
                outcome.append("completed")
            except RequestAborted:
                outcome.append("aborted")

        async def disconnect(self):
            self.disconnect_called = True

    consumer = TestConsumer()
    communicator = ApplicationCommunicator(consumer, HTTP_SCOPE)
    await communicator.send_input(
        {"type": "http.request", "body": b"partial", "more_body": True}
    )
    await communicator.send_input({"type": "http.disconnect"})
    # The consumer exits cleanly on its own.
    await communicator.wait()
    assert outcome == ["aborted"]
    assert consumer.disconnect_called


@pytest.mark.asyncio
async def test_async_http_consumer_streaming_early_response(noop_connection_cleanup):
    """
    Tests that if handle_stream() responds without reading the whole body,
    the remaining consumption is cancelled and the consumer exits without
    waiting for the rest of the upload.
    """
    chunks_read = []

    class TestConsumer(AsyncHttpConsumer):
        def __init__(self):
            super().__init__()
            self.disconnect_called = False

        async def handle_stream(self):
            async for chunk in self.stream_body():
                chunks_read.append(chunk)
                await self.send_response(200, b"early")
                return

        async def disconnect(self):
            self.disconnect_called = True

    consumer = TestConsumer()
    communicator = ApplicationCommunicator(consumer, HTTP_SCOPE)
    await communicator.send_input(
        {"type": "http.request", "body": b"one", "more_body": True}
    )
    response_start = await communicator.receive_output()
    assert response_start["type"] == "http.response.start"
    response_body = await communicator.receive_output()
    assert response_body["body"] == b"early"
    # The consumer exits on its own, without waiting for more body messages
    # and without leaving a receive hanging.
    await communicator.wait()
    assert chunks_read == [b"one"]
    assert consumer.disconnect_called


@pytest.mark.asyncio
async def test_async_http_consumer_streaming_backpressure(noop_connection_cleanup):
    """
    Tests that stream_body() only pulls further messages as the caller
    iterates; at most one chunk is buffered inside the consumer.
    """
    first_chunk_read = asyncio.Event()
    keep_reading = asyncio.Event()

    class TestConsumer(AsyncHttpConsumer):
        async def handle_stream(self):
            async for chunk in self.stream_body():
                first_chunk_read.set()
                await keep_reading.wait()
            await self.send_response(200, b"done")

    consumer = TestConsumer()
    communicator = ApplicationCommunicator(consumer, HTTP_SCOPE)
    await communicator.send_input(
        {"type": "http.request", "body": b"1", "more_body": True}
    )
    await asyncio.wait_for(first_chunk_read.wait(), timeout=1)
    # The handler is now paused and not reading further chunks.
    await communicator.send_input(
        {"type": "http.request", "body": b"2", "more_body": True}
    )
    await communicator.send_input(
        {"type": "http.request", "body": b"3", "more_body": True}
    )
    await communicator.send_input({"type": "http.request", "body": b"4"})
    # Give the consumer a chance to (not) consume the queued input.
    await asyncio.sleep(0.1)
    # At most one chunk is buffered inside the consumer; the rest is still
    # waiting in the communicator's input queue.
    assert consumer._stream_queue.qsize() <= 1
    assert communicator.input_queue.qsize() == 1

    keep_reading.set()
    response_start = await communicator.receive_output()
    assert response_start["type"] == "http.response.start"
    response_body = await communicator.receive_output()
    assert response_body["body"] == b"done"
    await communicator.wait()


@pytest.mark.asyncio
async def test_async_http_consumer_streaming_error(noop_connection_cleanup):
    """
    Tests that exceptions raised by handle_stream() propagate out of the
    application.
    """

    class TestConsumer(AsyncHttpConsumer):
        async def handle_stream(self):
            raise AssertionError("Stream error correctly raised")

    communicator = ApplicationCommunicator(TestConsumer(), HTTP_SCOPE)
    await communicator.send_input({"type": "http.request", "body": b""})
    with pytest.raises(AssertionError) as excinfo:
        await communicator.wait()

    assert str(excinfo.value) == "Stream error correctly raised"
