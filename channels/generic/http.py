import asyncio

from channels.consumer import AsyncConsumer

from ..db import aclose_old_connections
from ..exceptions import RequestAborted, StopConsumer

#: Queued in place of an ``http.request`` message to tell a streaming
#: handler that the client disconnected before the body fully arrived.
_DISCONNECT = object()


class AsyncHttpConsumer(AsyncConsumer):
    """
    Async HTTP consumer. Provides basic primitives for building asynchronous
    HTTP endpoints.
    """

    def __init__(self, *args, **kwargs):
        self.body = []
        # Streaming state. Only set up by __call__ when the subclass
        # overrides handle_stream().
        self._stream_queue = None
        self._stream_done = None
        self._stream_task = None

    async def __call__(self, scope, receive, send):
        """
        Dispatches incoming messages. When the subclass implements
        ``handle_stream``, the request body is instead fed to it lazily,
        chunk by chunk, and the consumer stops as soon as the handler
        returns - even if the client never sends another message.
        """
        if type(self).handle_stream is AsyncHttpConsumer.handle_stream:
            # No streaming handler: classic buffered behavior.
            return await super().__call__(scope, receive, send)

        # A queue of size one means the dispatch loop only pulls the next
        # message off receive once the handler consumed the previous one.
        self._stream_queue = asyncio.Queue(maxsize=1)
        self._stream_done = asyncio.Event()

        async def receive_or_stream_done():
            receive_task = asyncio.ensure_future(receive())
            done_task = asyncio.ensure_future(self._stream_done.wait())
            try:
                await asyncio.wait(
                    [receive_task, done_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if done_task.done():
                    # handle_stream() returned; stop the consumer without
                    # waiting for further client messages.
                    raise StopConsumer()
                return receive_task.result()
            finally:
                # Never leave a receive hanging once we stop.
                for task in (receive_task, done_task):
                    if not task.done():
                        task.cancel()

        try:
            await super().__call__(scope, receive_or_stream_done, send)
        finally:
            if self._stream_task is not None and not self._stream_task.done():
                self._stream_task.cancel()
                try:
                    await self._stream_task
                except asyncio.CancelledError:
                    pass
        if self._stream_task is not None and not self._stream_task.cancelled():
            # Surface any exception raised by handle_stream().
            self._stream_task.result()

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
            "Subclasses of AsyncHttpConsumer must provide a handle() method."
        )

    async def handle_stream(self):
        """
        Streaming alternative to ``handle``. Override it (instead of
        ``handle``) to consume the request body chunk by chunk via
        ``self.stream_body()`` while it is still being received. Response
        may be composed using the ``self.send*`` methods; the return value
        of this method is thrown away.
        """
        raise NotImplementedError(
            "Subclasses of AsyncHttpConsumer must provide a handle() or "
            "handle_stream() method."
        )

    async def stream_body(self):
        """
        Async iterator over the request body, yielding one chunk per
        incoming ``http.request`` message.

        Empty chunks are yielded as-is; iteration only ends once a message
        without ``more_body`` set has been yielded. Further messages are
        only pulled as you iterate, so the body is never buffered in its
        entirety. Raises ``channels.exceptions.RequestAborted`` if the
        client disconnects before the body has been fully received.
        """
        if self._stream_queue is None:
            raise RuntimeError(
                "stream_body() is only available while handle_stream() "
                "is running."
            )
        while True:
            message = await self._stream_queue.get()
            if message is _DISCONNECT:
                raise RequestAborted(
                    "Client disconnected before the request body was "
                    "fully received."
                )
            yield message.get("body", b"")
            if not message.get("more_body"):
                return

    async def disconnect(self):
        """
        Overrideable place to run disconnect handling. Do not send anything
        from here.
        """
        pass

    async def http_request(self, message):
        """
        Async entrypoint - concatenates body fragments and hands off control
        to ``self.handle`` when the body has been completely received.
        If the subclass implements ``handle_stream``, body fragments are
        instead streamed to it chunk by chunk.
        """
        if self._stream_queue is not None:
            await self._stream_request(message)
            return
        if "body" in message:
            self.body.append(message["body"])
        if not message.get("more_body"):
            try:
                await self.handle(b"".join(self.body))
            finally:
                await self.disconnect()
            raise StopConsumer()

    async def _stream_request(self, message):
        """
        Feeds one ``http.request`` message to the streaming handler,
        blocking while the internal queue is full so further messages are
        only pulled once the handler asks for them.
        """
        if self._stream_task is None:
            self._stream_task = asyncio.ensure_future(self._run_handle_stream())
        if self._stream_task.done():
            # The handler already responded without reading the whole body.
            self._stream_task.result()
            raise StopConsumer()
        put_task = asyncio.ensure_future(self._stream_queue.put(message))
        done, _ = await asyncio.wait(
            [put_task, self._stream_task], return_when=asyncio.FIRST_COMPLETED
        )
        if self._stream_task in done:
            # The handler finished while this chunk was still waiting to be
            # consumed: discard it and stop the consumer.
            put_task.cancel()
            try:
                await put_task
            except asyncio.CancelledError:
                pass
            self._stream_task.result()
            raise StopConsumer()
        if not message.get("more_body"):
            # Final chunk: wait for the handler to finish consuming it.
            await self._stream_task
            raise StopConsumer()

    async def _run_handle_stream(self):
        """
        Runs ``handle_stream`` and guarantees cleanup: ``disconnect()`` runs
        afterwards, and the consumer is signalled to stop even if the client
        never sends another message.
        """
        try:
            try:
                await self.handle_stream()
            except RequestAborted:
                # The client disconnected mid-body; not an application error.
                pass
            await self.disconnect()
        finally:
            self._stream_done.set()

    async def http_disconnect(self, message):
        """
        Let the user do their cleanup and close the consumer.
        """
        if self._stream_task is not None:
            if not self._stream_task.done():
                # Wake a handler blocked in stream_body() so it can tell a
                # disconnect apart from a cleanly finished upload.
                put_task = asyncio.ensure_future(
                    self._stream_queue.put(_DISCONNECT)
                )
                done, _ = await asyncio.wait(
                    [put_task, self._stream_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not put_task.done():
                    put_task.cancel()
                # Wait for the handler to wind down; _run_handle_stream runs
                # disconnect() and flags the consumer to stop.
                await self._stream_task
            raise StopConsumer()
        await self.disconnect()
        await aclose_old_connections()
        raise StopConsumer()
