"""B4: live tail — Consume(follow=true) waits instead of ending.

Two layers of test: the Tail notifier on its own (pure asyncio, no server),
then the streaming behaviour end to end over a real gRPC channel.

Every wait here is bounded by asyncio.wait_for. A live-tail bug is usually a
hang, not a wrong answer, so a test that could hang forever is useless.
"""

import asyncio

import grpc
import pytest

from pyka.broker.server import BrokerServer
from pyka.broker.store import Store
from pyka.broker.tail import Tail
from pyka.v1 import broker_pb2, broker_pb2_grpc

KEY = b"user-1"  # one key, one partition
TIMEOUT = 5.0  # generous; a healthy notification arrives in microseconds


# --------------------------------------------------------------------------
# Tail on its own
# --------------------------------------------------------------------------


async def test_notify_wakes_a_waiter():
    tail = Tail()
    event = tail.subscribe("orders", 0)
    tail.notify("orders", 0)
    await asyncio.wait_for(event.wait(), TIMEOUT)


async def test_notify_wakes_every_waiter_on_that_partition():
    # A broadcast, not a queue: several consumers may follow one partition.
    tail = Tail()
    events = [tail.subscribe("orders", 0) for _ in range(3)]
    assert len({id(e) for e in events}) == 1, "one event shared per partition"

    tail.notify("orders", 0)
    await asyncio.wait_for(asyncio.gather(*(e.wait() for e in events)), TIMEOUT)


async def test_partitions_are_independent():
    tail = Tail()
    zero, one = tail.subscribe("orders", 0), tail.subscribe("orders", 1)
    tail.notify("orders", 1)
    assert one.is_set()
    assert not zero.is_set()


def test_topics_are_independent():
    tail = Tail()
    orders, clicks = tail.subscribe("orders", 0), tail.subscribe("clicks", 0)
    tail.notify("clicks", 0)
    assert clicks.is_set() and not orders.is_set()


def test_a_fresh_event_is_handed_out_after_a_notify():
    # Otherwise the second wait would return instantly on a stale set flag,
    # and a consumer would spin instead of waiting.
    tail = Tail()
    first = tail.subscribe("orders", 0)
    tail.notify("orders", 0)
    second = tail.subscribe("orders", 0)

    assert first is not second
    assert not second.is_set()


def test_notifying_with_nobody_waiting_is_harmless():
    # The common case: almost every append has no follower parked on it.
    Tail().notify("orders", 0)


def test_close_releases_every_waiter():
    tail = Tail()
    events = [tail.subscribe("orders", p) for p in range(3)]
    tail.close()

    assert tail.closed
    assert all(e.is_set() for e in events)


# --------------------------------------------------------------------------
# following, end to end
# --------------------------------------------------------------------------


@pytest.fixture
async def store(tmp_path):
    store = Store(tmp_path / "data", partitions=1)
    await store.open()
    yield store
    await store.close()


@pytest.fixture
async def stub(store):
    server = BrokerServer(store, port=0)
    await server.start()
    async with grpc.aio.insecure_channel(server.address) as channel:
        yield broker_pb2_grpc.BrokerStub(channel)
    await server.stop(grace=0)


def follow(stub, offset: int = 0, topic: str = "orders"):
    return stub.Consume(
        broker_pb2.ConsumeRequest(
            topic=topic, partition=0, offset=offset, follow=True
        )
    )


async def produce(stub, value: bytes, topic: str = "orders") -> int:
    response = await stub.Produce(
        broker_pb2.ProduceRequest(topic=topic, key=KEY, value=value)
    )
    return response.offset


async def test_a_following_stream_does_not_end_at_the_last_record(stub, store):
    """The whole point: a plain read would return nothing and close. This one
    stays open and delivers a record produced afterwards."""
    await store.create("orders")
    stream = follow(stub)

    await produce(stub, b"arrived-later")
    record = await asyncio.wait_for(stream.read(), TIMEOUT)

    assert record.value == b"arrived-later"
    stream.cancel()


async def test_existing_records_come_first_then_new_ones(stub):
    for n in range(3):
        await produce(stub, f"old-{n}".encode())

    stream = follow(stub)
    caught_up = [await asyncio.wait_for(stream.read(), TIMEOUT) for _ in range(3)]
    assert [r.value for r in caught_up] == [b"old-0", b"old-1", b"old-2"]

    await produce(stub, b"new")
    fresh = await asyncio.wait_for(stream.read(), TIMEOUT)
    assert (fresh.value, fresh.offset) == (b"new", 3)
    stream.cancel()


async def test_a_follower_starting_mid_log_sees_the_rest_then_waits(stub):
    for n in range(5):
        await produce(stub, f"v{n}".encode())

    stream = follow(stub, offset=3)
    assert [
        (await asyncio.wait_for(stream.read(), TIMEOUT)).offset for _ in range(2)
    ] == [3, 4]

    await produce(stub, b"v5")
    assert (await asyncio.wait_for(stream.read(), TIMEOUT)).offset == 5
    stream.cancel()


async def test_several_records_in_a_row_all_arrive(stub, store):
    await store.create("orders")
    stream = follow(stub)

    for n in range(10):
        await produce(stub, f"v{n}".encode())

    received = [await asyncio.wait_for(stream.read(), TIMEOUT) for _ in range(10)]
    assert [r.offset for r in received] == list(range(10))
    stream.cancel()


async def test_two_followers_both_receive_every_record(stub, store):
    # Pub/sub: the log is not consumed destructively.
    await store.create("orders")
    first, second = follow(stub), follow(stub)

    await produce(stub, b"broadcast")

    a = await asyncio.wait_for(first.read(), TIMEOUT)
    b = await asyncio.wait_for(second.read(), TIMEOUT)
    assert a.value == b.value == b"broadcast"
    first.cancel()
    second.cancel()


async def test_a_tombstone_survives_the_live_path_too(stub, store):
    await store.create("orders")
    stream = follow(stub)

    await stub.Produce(
        broker_pb2.ProduceRequest(topic="orders", key=KEY, value=None)
    )
    record = await asyncio.wait_for(stream.read(), TIMEOUT)
    assert not record.HasField("value")
    stream.cancel()


async def test_max_records_is_ignored_while_following(stub, store):
    """The contract says so, and it is the right call: a live tail that stops
    after N records is just a bounded read with extra steps."""
    await store.create("orders")
    stream = stub.Consume(
        broker_pb2.ConsumeRequest(
            topic="orders", partition=0, offset=0, follow=True, max_records=1
        )
    )
    await produce(stub, b"first")
    await produce(stub, b"second")

    assert (await asyncio.wait_for(stream.read(), TIMEOUT)).value == b"first"
    assert (await asyncio.wait_for(stream.read(), TIMEOUT)).value == b"second"
    stream.cancel()


async def test_follow_reads_across_a_segment_roll(tmp_path):
    store = Store(tmp_path / "data", partitions=1, max_segment_bytes=2000)
    await store.open()
    server = BrokerServer(store, port=0)
    await server.start()
    async with grpc.aio.insecure_channel(server.address) as channel:
        stub = broker_pb2_grpc.BrokerStub(channel)
        await store.create("orders")
        stream = follow(stub)

        for _ in range(40):
            await stub.Produce(
                broker_pb2.ProduceRequest(topic="orders", key=KEY, value=b"x" * 100)
            )
        received = [
            await asyncio.wait_for(stream.read(), TIMEOUT) for _ in range(40)
        ]
        assert len(store.topic.get("orders", 0).segments) > 1, "needs a roll"
        assert [r.offset for r in received] == list(range(40))
        stream.cancel()
    await server.stop(grace=0)
    await store.close()


# --------------------------------------------------------------------------
# shutdown — a parked stream must not hold the process open
# --------------------------------------------------------------------------


async def test_closing_the_store_ends_a_parked_stream(stub, store):
    """Without this, server.stop(grace) waits out the full grace period on
    consumers that are, by design, waiting forever — which in Kubernetes is
    the difference between a clean rolling update and a SIGKILL mid-append.

    Note grpc.aio ends a stream with the EOF *sentinel*, not an exception:
    `read()` returns `grpc.aio.EOF` rather than raising StopAsyncIteration.
    """
    await store.create("orders")
    stream = follow(stub)
    await produce(stub, b"one")
    await asyncio.wait_for(stream.read(), TIMEOUT)  # parked after this

    await store.close()  # the fixture closes again; close is idempotent

    assert await asyncio.wait_for(stream.read(), TIMEOUT) is grpc.aio.EOF


async def test_a_stream_that_arrives_during_shutdown_never_parks(stub, store):
    # The other shutdown check: not "wake the parked", but "do not park at
    # all". A consumer connecting while the broker drains must be told the
    # log ended, not left holding an event nobody will ever set.
    await store.create("orders")
    await produce(stub, b"one")
    await store.close()

    records = [record async for record in follow(stub)]
    assert [r.value for r in records] == [b"one"]  # ended, did not hang


async def test_a_cancelled_stream_does_not_break_the_broker(stub, store):
    # A consumer disconnecting mid-wait is routine, not exceptional.
    await store.create("orders")
    stream = follow(stub)
    await produce(stub, b"one")
    await asyncio.wait_for(stream.read(), TIMEOUT)
    stream.cancel()

    # the broker keeps serving
    assert (await produce(stub, b"two")) == 1
