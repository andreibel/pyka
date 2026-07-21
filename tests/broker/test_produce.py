"""B2: Produce over gRPC, against a real server and real files on disk.

Nothing is mocked. Each test starts a grpc.aio server on an OS-chosen port
with a Store rooted in tmp_path, so a passing test means bytes actually
reached a segment.
"""

import grpc
import pytest

from pyka.broker.server import BrokerServer
from pyka.broker.store import Store
from pyka.storage.record import Record
from pyka.storage.types import Offset
from pyka.v1 import broker_pb2, broker_pb2_grpc


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


def request(**kwargs) -> broker_pb2.ProduceRequest:
    kwargs.setdefault("topic", "orders")
    return broker_pb2.ProduceRequest(**kwargs)


# --------------------------------------------------------------------------
# the happy path
# --------------------------------------------------------------------------


async def test_produce_returns_partition_and_offset(stub):
    response = await stub.Produce(request(key=b"user-1", value=b"hello"))
    assert 0 <= response.partition < 2
    assert response.offset == 0  # each partition numbers from zero


async def test_produce_auto_creates_the_topic(stub, store):
    assert await store.names() == []
    await stub.Produce(request(key=b"k", value=b"v"))
    assert await store.names() == ["orders"]


async def test_the_record_really_lands_on_disk(stub, store):
    response = await stub.Produce(request(key=b"user-1", value=b"payload"))

    records = await store.read("orders", Offset(response.offset), response.partition)
    assert [(r.key, r.value) for r in records] == [(b"user-1", b"payload")]


async def test_offsets_increase_within_a_partition(stub):
    offsets = []
    for _ in range(5):
        response = await stub.Produce(request(key=b"same-key", value=b"v"))
        offsets.append((response.partition, response.offset))

    partitions = {p for p, _ in offsets}
    assert len(partitions) == 1, "one key must stick to one partition"
    assert [o for _, o in offsets] == [0, 1, 2, 3, 4]


async def test_the_broker_stamps_a_timestamp_when_none_is_given(stub, store):
    await stub.Produce(request(key=b"k", value=b"v"))
    response = await stub.Produce(request(key=b"k", value=b"v", timestamp=99))

    records = await store.read("orders", Offset(0), response.partition)
    assert records[0].timestamp > 1_700_000_000_000  # broker's clock, in ms
    assert records[1].timestamp == 99                # the client's, respected


async def test_correlation_id_is_echoed(stub):
    response = await stub.Produce(request(key=b"k", value=b"v", correlation_id=4242))
    assert response.correlation_id == 4242


async def test_no_correlation_id_means_none_comes_back(stub):
    response = await stub.Produce(request(key=b"k", value=b"v"))
    assert not response.HasField("correlation_id")


# --------------------------------------------------------------------------
# field presence — where an empty check would corrupt data
# --------------------------------------------------------------------------


async def test_an_absent_value_is_a_tombstone_not_an_empty_value(stub, store):
    """The bug field presence exists to prevent.

    An unset `optional bytes` reads back as b"", so `request.value or None`
    would turn a present-but-empty value into a tombstone — the same
    klen == -1 vs klen == 0 distinction the record format draws on disk. These
    two records must survive the wire as different things.
    """
    tombstone = await stub.Produce(request(key=b"same", value=None))
    empty = await stub.Produce(request(key=b"same", value=b""))
    assert tombstone.partition == empty.partition  # same key, same partition

    records = await store.read("orders", Offset(0), tombstone.partition)
    assert records[0].value is None   # absent  → deletion marker
    assert records[1].value == b""    # present → empty payload


async def test_an_absent_key_is_a_null_key_not_an_empty_key(stub, store):
    await stub.Produce(request(key=None, value=b"v"))
    records = await store.read("orders", Offset(0), 0) + await store.read(
        "orders", Offset(0), 1
    )
    assert records[0].key is None


async def test_null_keyed_records_round_robin_across_partitions(stub):
    # No key means no home: they spread rather than sticking.
    partitions = {(await stub.Produce(request(value=b"v"))).partition for _ in range(4)}
    assert len(partitions) == 2  # the topic has 2


# --------------------------------------------------------------------------
# errors — client bugs get INVALID_ARGUMENT, not a dropped connection
# --------------------------------------------------------------------------


@pytest.mark.parametrize("topic", ["../escape", "a/b", "", "."])
async def test_an_illegal_topic_name_is_invalid_argument(stub, topic):
    # The security boundary, now reachable from the network.
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await stub.Produce(broker_pb2.ProduceRequest(topic=topic, value=b"v"))
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


async def test_a_record_over_max_size_is_rejected_not_written(stub, store):
    """The audit's critical finding, from the network side.

    Before the fix this was written successfully and left the segment
    permanently unreadable — one oversized value from any client would brick
    a partition.
    """
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await stub.Produce(request(value=b"x" * (Record.MAX_SIZE + 1)))
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "over the" in err.value.details()

    # and the topic is still perfectly usable afterwards
    ok = await stub.Produce(request(key=b"k", value=b"v"))
    assert ok.offset == 0


async def test_the_connection_survives_a_rejected_request(stub):
    with pytest.raises(grpc.aio.AioRpcError):
        await stub.Produce(broker_pb2.ProduceRequest(topic="../bad", value=b"v"))
    assert (await stub.Produce(request(key=b"k", value=b"v"))).offset == 0


# --------------------------------------------------------------------------
# ProduceStream
# --------------------------------------------------------------------------


async def test_produce_stream_appends_every_record(stub, store):
    requests = [request(key=b"same-key", value=f"v{n}".encode()) for n in range(10)]

    responses = [r async for r in stub.ProduceStream(iter(requests))]
    assert [r.offset for r in responses] == list(range(10))

    records = await store.read("orders", Offset(0), responses[0].partition)
    assert [r.value for r in records] == [f"v{n}".encode() for n in range(10)]


async def test_produce_stream_echoes_correlation_ids(stub):
    requests = [
        request(key=b"k", value=b"v", correlation_id=100 + n) for n in range(5)
    ]
    responses = [r async for r in stub.ProduceStream(iter(requests))]
    assert [r.correlation_id for r in responses] == [100, 101, 102, 103, 104]


async def test_produce_stream_can_span_several_topics(stub, store):
    requests = [
        request(topic="orders", key=b"k", value=b"a"),
        request(topic="clicks", key=b"k", value=b"b"),
        request(topic="orders", key=b"k", value=b"c"),
    ]
    responses = [r async for r in stub.ProduceStream(iter(requests))]
    assert [r.offset for r in responses] == [0, 0, 1]
    assert sorted(await store.names()) == ["clicks", "orders"]


async def test_a_bad_record_aborts_the_stream_but_keeps_earlier_ones(stub, store):
    """The documented limitation: ProduceResponse has no per-record error, so
    one bad record ends the call. It is recoverable — every record that landed
    was already acknowledged, so the client knows exactly how far it got."""
    requests = [
        request(key=b"k", value=b"good-1"),
        request(key=b"k", value=b"good-2"),
        broker_pb2.ProduceRequest(topic="../bad", value=b"boom"),
        request(key=b"k", value=b"never-sent"),
    ]

    seen = []
    with pytest.raises(grpc.aio.AioRpcError) as err:
        async for response in stub.ProduceStream(iter(requests)):
            seen.append(response.offset)
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert seen == [0, 1]  # acknowledged before the failure

    records = await store.read("orders", Offset(0), 0) + await store.read(
        "orders", Offset(0), 1
    )
    assert [r.value for r in records] == [b"good-1", b"good-2"]


async def test_an_empty_stream_is_fine(stub):
    assert [r async for r in stub.ProduceStream(iter([]))] == []
