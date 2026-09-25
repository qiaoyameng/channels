import asyncio

import async_timeout
import pytest

from channels.exceptions import ChannelFull
from channels.layers import InMemoryChannelLayer


@pytest.fixture()
async def channel_layer():
    """
    Channel layer fixture that flushes automatically.
    """
    channel_layer = InMemoryChannelLayer(capacity=3)
    yield channel_layer
    await channel_layer.flush()
    await channel_layer.close()


@pytest.mark.asyncio
async def test_send_receive(channel_layer):
    """
    Makes sure we can send a message to a normal channel then receive it.
    """
    await channel_layer.send(
        "test-channel-1", {"type": "test.message", "text": "Ahoy-hoy!"}
    )
    await channel_layer.send(
        "test-channel-1", {"type": "test.message", "text": "Ahoy-hoy!"}
    )
    message = await channel_layer.receive("test-channel-1")
    assert message["type"] == "test.message"
    assert message["text"] == "Ahoy-hoy!"
    # not removed because not empty
    assert "test-channel-1" in channel_layer.channels
    message = await channel_layer.receive("test-channel-1")
    assert message["type"] == "test.message"
    assert message["text"] == "Ahoy-hoy!"
    # removed because empty
    assert "test-channel-1" not in channel_layer.channels


@pytest.mark.asyncio
async def test_race_empty(channel_layer):
    """
    Makes sure the race is handled gracefully.
    """
    receive_task = asyncio.create_task(channel_layer.receive("test-channel-1"))
    await asyncio.sleep(0.1)
    await channel_layer.send(
        "test-channel-1", {"type": "test.message", "text": "Ahoy-hoy!"}
    )
    del channel_layer.channels["test-channel-1"]
    await asyncio.sleep(0.1)
    message = await receive_task
    assert message["type"] == "test.message"
    assert message["text"] == "Ahoy-hoy!"


@pytest.mark.asyncio
async def test_send_capacity(channel_layer):
    """
    Makes sure we get ChannelFull when we hit the send capacity
    """
    await channel_layer.send("test-channel-1", {"type": "test.message"})
    await channel_layer.send("test-channel-1", {"type": "test.message"})
    await channel_layer.send("test-channel-1", {"type": "test.message"})
    with pytest.raises(ChannelFull):
        await channel_layer.send("test-channel-1", {"type": "test.message"})


@pytest.mark.asyncio
async def test_process_local_send_receive(channel_layer):
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    channel_name = await channel_layer.new_channel()
    await channel_layer.send(
        channel_name, {"type": "test.message", "text": "Local only please"}
    )
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Local only please"


@pytest.mark.asyncio
async def test_process_local_shared_capacity():
    """
    Makes sure process-specific channels sharing a non-local prefix also
    share its capacity, while different prefixes are independent.
    """
    channel_layer = InMemoryChannelLayer(capacity=1)
    await channel_layer.send("proc!a", {"type": "message.1"})
    # The shared "proc!" queue is full, even for a different local part.
    with pytest.raises(ChannelFull):
        await channel_layer.send("proc!b", {"type": "message.2"})
    # A different prefix has its own capacity.
    await channel_layer.send("other!a", {"type": "message.3"})
    # Receiving by full channel name drains the shared prefix queue.
    assert (await channel_layer.receive("proc!a"))["type"] == "message.1"
    # Capacity freed by the receive, so sending now works again.
    await channel_layer.send("proc!b", {"type": "message.2"})
    assert (await channel_layer.receive("proc!b"))["type"] == "message.2"
    assert (await channel_layer.receive("other!a"))["type"] == "message.3"
    # Queues are cleaned up once empty.
    assert len(channel_layer.channels) == 0


@pytest.mark.asyncio
async def test_process_local_shared_capacity_expiry():
    """
    Makes sure expiry frees shared prefix capacity, interleaved with
    capacity-one sends and receives.
    """
    channel_layer = InMemoryChannelLayer(capacity=1, expiry=0.1)
    await channel_layer.send("proc!a", {"type": "message.1"})
    with pytest.raises(ChannelFull):
        await channel_layer.send("proc!b", {"type": "message.2"})

    await asyncio.sleep(0.15)

    # The expired message is dropped on receive, which then blocks.
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(0.5):
            await channel_layer.receive("proc!a")
    # Expiry freed the shared capacity.
    await channel_layer.send("proc!b", {"type": "message.2"})
    assert (await channel_layer.receive("proc!b"))["type"] == "message.2"
    assert len(channel_layer.channels) == 0


@pytest.mark.asyncio
async def test_process_local_channel_capacity():
    """
    Makes sure channel_capacity patterns apply to the shared prefix queue.
    """
    channel_layer = InMemoryChannelLayer(capacity=1, channel_capacity={"proc!*": 2})
    await channel_layer.send("proc!a", {"type": "message.1"})
    await channel_layer.send("proc!b", {"type": "message.2"})
    with pytest.raises(ChannelFull):
        await channel_layer.send("proc!c", {"type": "message.3"})
    assert (await channel_layer.receive("proc!a"))["type"] == "message.1"
    assert (await channel_layer.receive("proc!b"))["type"] == "message.2"


@pytest.mark.asyncio
async def test_process_local_group_send_capacity():
    """
    Makes sure group_send drops messages when the shared prefix queue is
    full, while a direct send reports ChannelFull.
    """
    channel_layer = InMemoryChannelLayer(capacity=1)
    await channel_layer.group_add("test-group", "proc!a")
    await channel_layer.group_send("test-group", {"type": "message.1"})
    # Direct send to the same prefix is full and raises.
    with pytest.raises(ChannelFull):
        await channel_layer.send("proc!b", {"type": "message.2"})
    # group_send discards silently instead of raising.
    await channel_layer.group_send("test-group", {"type": "message.3"})
    assert (await channel_layer.receive("proc!a"))["type"] == "message.1"
    # Flush resets the shared prefix queues.
    await channel_layer.send("proc!a", {"type": "message.4"})
    await channel_layer.flush()
    assert len(channel_layer.channels) == 0
    await channel_layer.send("proc!a", {"type": "message.5"})
    assert (await channel_layer.receive("proc!a"))["type"] == "message.5"


@pytest.mark.asyncio
async def test_multi_send_receive(channel_layer):
    """
    Tests overlapping sends and receives, and ordering.
    """
    await channel_layer.send("test-channel-3", {"type": "message.1"})
    await channel_layer.send("test-channel-3", {"type": "message.2"})
    await channel_layer.send("test-channel-3", {"type": "message.3"})
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.1"
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.2"
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.3"


@pytest.mark.asyncio
async def test_groups_basic(channel_layer):
    """
    Tests basic group operation.
    """
    await channel_layer.group_add("test-group", "test-gr-chan-1")
    await channel_layer.group_add("test-group", "test-gr-chan-2")
    await channel_layer.group_add("test-group", "test-gr-chan-3")
    await channel_layer.group_discard("test-group", "test-gr-chan-2")
    await channel_layer.group_send("test-group", {"type": "message.1"})
    # Make sure we get the message on the two channels that were in
    async with async_timeout.timeout(1):
        assert (await channel_layer.receive("test-gr-chan-1"))["type"] == "message.1"
        assert (await channel_layer.receive("test-gr-chan-3"))["type"] == "message.1"
    # Make sure the removed channel did not get the message
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive("test-gr-chan-2")


@pytest.mark.asyncio
async def test_groups_channel_full(channel_layer):
    """
    Tests that group_send ignores ChannelFull
    """
    await channel_layer.group_add("test-group", "test-gr-chan-1")
    await channel_layer.group_send("test-group", {"type": "message.1"})
    await channel_layer.group_send("test-group", {"type": "message.1"})
    await channel_layer.group_send("test-group", {"type": "message.1"})
    await channel_layer.group_send("test-group", {"type": "message.1"})
    await channel_layer.group_send("test-group", {"type": "message.1"})


@pytest.mark.asyncio
async def test_expiry_single():
    """
    Tests that a message can expire.
    """
    channel_layer = InMemoryChannelLayer(expiry=0.1)
    await channel_layer.send("test-channel-1", {"type": "message.1"})
    assert len(channel_layer.channels) == 1

    await asyncio.sleep(0.1)

    # Message should have expired and been dropped.
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(0.5):
            await channel_layer.receive("test-channel-1")

    # Channel should be cleaned up.
    assert len(channel_layer.channels) == 0


@pytest.mark.asyncio
async def test_expiry_unread():
    """
    Tests that a message on a channel can expire and be cleaned up even if
    the channel is not read from again.
    """
    channel_layer = InMemoryChannelLayer(expiry=0.1)
    await channel_layer.send("test-channel-1", {"type": "message.1"})

    await asyncio.sleep(0.1)

    await channel_layer.send("test-channel-2", {"type": "message.2"})
    assert len(channel_layer.channels) == 2
    assert (await channel_layer.receive("test-channel-2"))["type"] == "message.2"
    # Both channels should be cleaned up.
    assert len(channel_layer.channels) == 0


@pytest.mark.asyncio
async def test_expiry_multi():
    """
    Tests that multiple messages can expire.
    """
    channel_layer = InMemoryChannelLayer(expiry=0.1)
    await channel_layer.send("test-channel-1", {"type": "message.1"})
    await channel_layer.send("test-channel-1", {"type": "message.2"})
    await channel_layer.send("test-channel-1", {"type": "message.3"})
    assert (await channel_layer.receive("test-channel-1"))["type"] == "message.1"

    await asyncio.sleep(0.1)
    await channel_layer.send("test-channel-1", {"type": "message.4"})
    assert (await channel_layer.receive("test-channel-1"))["type"] == "message.4"

    # The second and third message should have expired and been dropped.
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(0.5):
            await channel_layer.receive("test-channel-1")

    # Channel should be cleaned up.
    assert len(channel_layer.channels) == 0
