import asyncio

from channels.consumer import AsyncConsumer

from ..db import aclose_old_connections
from ..exceptions import RequestAborted, StopConsumer


class AsyncHttpConsumer(AsyncConsumer):
    """
    Async HTTP consumer. Provides basic primitives for building asynchronous
    HTTP endpoints.
    """

    def __init__(self, *args, **kwargs):
        self.body = []
        # Streaming state, only used when handle_stream() is overridden.
        self._stream_queue = None
        self._stream_task = None
        self._dispatch_task = None

    async def __call__(self, scope, receive, send):
        """
        Runs the consumer, supervising the optional streaming body handler so
        the consumer exits as soon as that handler returns - even if the
        client has not finished (or never finishes) sending the request body.
        """
        self._dispatch_task = asyncio.ensure_future(
            super().__call__(scope, receive, send)
        )
        try:
            try:
                await self._dispatch_task
            except asyncio.CancelledError:
                # The stream handler stops the dispatch loop when it
                # finishes; any other cancellation is external and must
                # propagate after stopping the dispatch loop as well.
                if not self._dispatch_task.cancelled():
                    self._dispatch_task.cancel()
                    raise
        finally:
            if self._stream_task is not None and not self._stream_task.done():
                self._stream_task.cancel()
                try:
                    await self._stream_task
                except asyncio.CancelledError:
                    pass
        if self._stream_task is not None and not self._stream_task.cancelled():
            exception = self._stream_task.exception()
            if exception is not None:
                raise exception

    async def send_headers(self, *, status=200, headers=None):
        """
        Sets the HTTP response status and headers. Headers may be provided as
        a list of tuples or as a dictionary.

        Note that the ASGI spec requires that the protocol server only starts
        sending the response to the client after ``self.send_body`` has been
        called the first time.
        """
        if headers is None:
            headers = []
        elif isinstance(headers, dict):
            headers = list(headers.items())

        await self.send(
            {"type": "http.response.start", "status": status, "headers": headers}
        )

    async def send_body(self, body, *, more_body=False):
        """
        Sends a response body to the client. The method expects a bytestring.

        Set ``more_body=True`` if you want to send more body content later.
        The default behavior closes the response, and further messages on
        the channel will be ignored.
        """
        assert isinstance(body, bytes), "Body is not bytes"
        await self.send(
            {"type": "http.response.body", "body": body, "more_body": more_body}
        )

    async def send_response(self, status, body, **kwargs):
        """
        Sends a response to the client. This is a thin wrapper over
        ``self.send_headers`` and ``self.send_body``, and everything said
        above applies here as well. This method may only be called once.
        """
        await self.send_headers(status=status, **kwargs)
        await self.send_body(body)

    async def handle(self, body):
        """
        Receives the request body as a bytestring. Response may be composed
        using the ``self.send*`` methods; the return value of this method is
        thrown away.
        """
        raise NotImplementedError(
            "Subclasses of AsyncHttpConsumer must provide a handle() "
            "or handle_stream() method."
        )

    async def handle_stream(self, stream):
        """
        Receives the request body as an async iterator of bytestrings, which
        only pulls further chunks from the client as they are consumed, so
        large uploads do not need to be buffered whole. Override this instead
        of ``handle`` to process the body incrementally. Iterating raises
        ``RequestAborted`` if the client disconnects mid-upload.
        """
        raise NotImplementedError(
            "Subclasses of AsyncHttpConsumer must provide a handle() "
            "or handle_stream() method."
        )

    async def disconnect(self):
        """
        Overrideable place to run disconnect handling. Do not send anything
        from here.
        """
        pass

    async def http_request(self, message):
        """
        Async entrypoint - concatenates body fragments and hands off control
        to ``self.handle`` when the body has been completely received. If
        ``self.handle_stream`` is overridden, instead feeds the fragments to
        it one by one as they arrive.
        """
        if self._stream_queue is not None:
            # Streaming already in progress; forward the fragment.
            await self._stream_put(message)
        elif type(self).handle_stream is not AsyncHttpConsumer.handle_stream:
            # First fragment of a streamed request body.
            self._stream_queue = asyncio.Queue(maxsize=1)
            self._stream_task = asyncio.ensure_future(self._run_handle_stream())
            await self._stream_put(message)
        else:
            if "body" in message:
                self.body.append(message["body"])
            if not message.get("more_body"):
                try:
                    await self.handle(b"".join(self.body))
                finally:
                    await self.disconnect()
                raise StopConsumer()

    async def http_disconnect(self, message):
        """
        Let the user do their cleanup and close the consumer.
        """
        if self._stream_queue is not None:
            # Streaming in progress: notify the iterator, which raises
            # RequestAborted inside the handler, and let the handler unwind.
            await self._stream_put(message)
            return
        await self.disconnect()
        await aclose_old_connections()
        raise StopConsumer()

    async def _run_handle_stream(self):
        """
        Runs the user's ``handle_stream`` and shuts the consumer down once it
        returns, cancelling any remaining body consumption.
        """
        try:
            await self.handle_stream(self._body_stream())
        finally:
            await self.disconnect()
            if self._dispatch_task is not None:
                self._dispatch_task.cancel()

    async def _stream_put(self, message):
        """
        Passes an incoming message to the stream handler. Applies
        backpressure so further messages are only pulled from the client once
        the previous ones were consumed, and stops the consumer if the
        handler finished without reading the whole body.
        """
        put = asyncio.ensure_future(self._stream_queue.put(message))
        done, _ = await asyncio.wait(
            {put, self._stream_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if put not in done:
            put.cancel()
            try:
                await put
            except asyncio.CancelledError:
                pass
            # The handler returned without consuming the rest of the body;
            # stop the consumer rather than buffering unread messages.
            raise StopConsumer()

    async def _body_stream(self):
        """
        Async iterator over the request body chunks. Empty chunks are yielded
        as-is; the end of the body is signalled by ``more_body`` only, and an
        ``http.disconnect`` raises ``RequestAborted``.
        """
        while True:
            message = await self._stream_queue.get()
            if message["type"] == "http.disconnect":
                raise RequestAborted()
            if "body" in message:
                yield message["body"]
            if not message.get("more_body"):
                return
