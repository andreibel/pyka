"""B3: Consume — server-streaming reads over gRPC.

Real server, real channel, real files. The produce side is used to set up
state, so these also check that a record survives the whole round trip: gRPC
in, segment on disk, gRPC out.
"""

import grpc
import pytest

from pyka.broker.handler import BATCH
from pyka.broker.server import BrokerServer
from pyka.broker.store import Store
from pyka.v1 import broker_pb2, broker_pb2_grpc

KEY = b"user-1"  # one key, so everything lands in one partition


@pytest.fixture
async def store(tmp_path):
    store = Store(tmp_path / "data", partitions=2)
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


async def fill(stub, count: int, topic: str = "orders", key: bytes = KEY) -> int:
    """Produce ``count`` records under one key; returns their partition."""
    partition = 0
    for n in range(count):
        response = await stub.Produce(
            broker_pb2.ProduceRequest(topic=topic, key=key, value=f"v{n}".encode())
        )
        partition = response.partition
    return partition


async def consume(stub, partition: int, offset: int = 0, **kwargs) -> list:
    request = broker_pb2.ConsumeRequest(
        topic=kwargs.pop("topic", "orders"),
        partition=partition,
        offset=offset,
        **kwargs,
    )
    return [record async for record in stub.Consume(request)]


# --------------------------------------------------------------------------
# reading
# --------------------------------------------------------------------------


async def test_consume_returns_every_record_in_order(stub):
    partition = await fill(stub, 10)
    records = await consume(stub, partition)

    assert [r.offset for r in records] == list(range(10))
    assert [r.value for r in records] == [f"v{n}".encode() for n in range(10)]


async def test_consume_starts_at_the_requested_offset(stub):
    partition = await fill(stub, 10)
    records = await consume(stub, partition, offset=6)

    assert [r.offset for r in records] == [6, 7, 8, 9]


async def test_consume_from_the_head_yields_nothing_and_does_not_error(stub):
    # A consumer polling at the head is normal, not a failure.
    partition = await fill(stub, 5)
    assert await consume(stub, partition, offset=5) == []


async def test_consume_far_past_the_end_yields_nothing(stub):
    partition = await fill(stub, 5)
    assert await consume(stub, partition, offset=9999) == []


async def test_consume_of_an_empty_topic_yields_nothing(stub, store):
    await store.create("orders")
    assert await consume(stub, 0) == []


async def test_max_records_bounds_the_stream(stub):
    partition = await fill(stub, 20)
    records = await consume(stub, partition, max_records=5)

    assert [r.offset for r in records] == [0, 1, 2, 3, 4]


async def test_max_records_larger_than_the_log_is_harmless(stub):
    partition = await fill(stub, 3)
    assert len(await consume(stub, partition, max_records=100)) == 3


async def test_zero_max_records_means_unlimited(stub):
    partition = await fill(stub, 7)
    assert len(await consume(stub, partition, max_records=0)) == 7


async def test_records_carry_their_timestamps(stub):
    await stub.Produce(
        broker_pb2.ProduceRequest(topic="orders", key=KEY, value=b"v", timestamp=12345)
    )
    partition = (
        await stub.Produce(
            broker_pb2.ProduceRequest(topic="orders", key=KEY, value=b"v2")
        )
    ).partition

    records = await consume(stub, partition)
    assert records[0].timestamp == 12345


# --------------------------------------------------------------------------
# field presence survives the round trip
# --------------------------------------------------------------------------


async def test_a_tombstone_comes_back_as_an_absent_value(stub):
    """The full loop for the distinction the record format draws on disk:
    absent (deletion marker) vs present-but-empty. Flattening either into the
    other would corrupt a compacted topic."""
    await stub.Produce(broker_pb2.ProduceRequest(topic="orders", key=KEY, value=None))
    partition = (
        await stub.Produce(
            broker_pb2.ProduceRequest(topic="orders", key=KEY, value=b"")
        )
    ).partition

    records = await consume(stub, partition)
    assert not records[0].HasField("value")  # tombstone
    assert records[1].HasField("value") and records[1].value == b""  # empty


async def test_a_null_key_comes_back_absent(stub):
    await stub.Produce(broker_pb2.ProduceRequest(topic="orders", value=b"v"))
    for partition in (0, 1):
        records = await consume(stub, partition)
        if records:
            assert not records[0].HasField("key")
            return
    pytest.fail("the record landed in neither partition")


# --------------------------------------------------------------------------
# batching — the stream must not slurp the whole log into memory
# --------------------------------------------------------------------------


async def test_a_log_longer_than_one_batch_streams_completely(stub):
    """Crosses the BATCH boundary, where the paging loop advances its offset.

    An off-by-one there drops or repeats exactly one record per batch — a bug
    that a log shorter than BATCH can never reveal.
    """
    count = BATCH + 25
    partition = await fill(stub, count)

    records = await consume(stub, partition)
    assert [r.offset for r in records] == list(range(count))


async def test_a_log_of_exactly_one_batch_streams_completely(stub):
    # The boundary itself: a full batch followed by an empty one.
    partition = await fill(stub, BATCH)
    assert len(await consume(stub, partition)) == BATCH


async def test_max_records_across_a_batch_boundary(stub):
    partition = await fill(stub, BATCH + 50)
    records = await consume(stub, partition, max_records=BATCH + 10)
    assert [r.offset for r in records] == list(range(BATCH + 10))


# --------------------------------------------------------------------------
# errors — three different conditions, three different status codes
# --------------------------------------------------------------------------


async def test_an_unknown_topic_is_not_found(stub):
    # Reads raise where produce would create: a consumer naming a missing
    # topic has typo'd, and an empty auto-created topic would never say so.
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await consume(stub, 0, topic="nope")
    assert err.value.code() == grpc.StatusCode.NOT_FOUND


async def test_a_partition_the_topic_does_not_have_is_invalid_argument(stub):
    await fill(stub, 1)
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await consume(stub, 9)
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


async def test_an_illegal_topic_name_is_invalid_argument(stub):
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await consume(stub, 0, topic="../escape")
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


async def test_an_offset_before_the_log_starts_is_out_of_range(stub):
    """OUT_OF_RANGE, not INVALID_ARGUMENT — the request is well formed, the
    records simply are not here. A consumer resuming from a stale committed
    offset lands here and should reset, not fix its arguments."""
    partition = await fill(stub, 3)
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await consume(stub, partition, offset=-1)
    assert err.value.code() == grpc.StatusCode.OUT_OF_RANGE


async def test_the_channel_survives_a_failed_consume(stub):
    partition = await fill(stub, 3)
    with pytest.raises(grpc.aio.AioRpcError):
        await consume(stub, 0, topic="nope")
    assert len(await consume(stub, partition)) == 3


# --------------------------------------------------------------------------
# produce -> disk -> consume, and across a restart
# --------------------------------------------------------------------------


async def test_records_survive_a_broker_restart(stub, store, tmp_path):
    """What `kubectl delete pod` does: the volume outlives the process."""
    partition = await fill(stub, 10)
    await store.close()

    reborn = Store(tmp_path / "data", partitions=2)
    await reborn.open()
    server = BrokerServer(reborn, port=0)
    await server.start()
    async with grpc.aio.insecure_channel(server.address) as channel:
        records = await consume(broker_pb2_grpc.BrokerStub(channel), partition)
    await server.stop(grace=0)
    await reborn.close()

    assert [r.offset for r in records] == list(range(10))
    assert [r.value for r in records] == [f"v{n}".encode() for n in range(10)]


async def test_consume_reads_across_a_segment_roll(tmp_path):
    # Segment boundaries must be invisible to a consumer.
    store = Store(tmp_path / "data", partitions=1, max_segment_bytes=2000)
    await store.open()
    server = BrokerServer(store, port=0)
    await server.start()
    async with grpc.aio.insecure_channel(server.address) as channel:
        stub = broker_pb2_grpc.BrokerStub(channel)
        for n in range(60):
            await stub.Produce(
                broker_pb2.ProduceRequest(topic="orders", key=KEY, value=b"x" * 100)
            )
        assert len(store.topic.get("orders", 0).segments) > 1, "needs a roll"
        records = await consume(stub, 0)
    await server.stop(grace=0)
    await store.close()

    assert [r.offset for r in records] == list(range(60))
