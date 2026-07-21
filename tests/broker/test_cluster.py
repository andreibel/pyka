"""D4: two brokers that genuinely shard.

Each broker gets its own data directory and its own Ring ordinal — exactly
what a StatefulSet gives two pods. Then the questions worth answering:

  * where does a record actually live?
  * how does a client find out?
  * what happens when it asks the wrong broker?

The answers here are the whole cluster design, and none of them involve the
brokers talking to each other. They never do.
"""

import grpc
import pytest

from pyka.broker.server import BrokerServer
from pyka.broker.store import Store
from pyka.cluster.ring import Ring
from pyka.v1 import broker_pb2, broker_pb2_grpc

PARTITIONS = 4
BROKERS = 2


class Cluster:
    """Two brokers on one machine: separate disks, separate ordinals."""

    def __init__(self):
        self.stores: list[Store] = []
        self.servers: list[BrokerServer] = []
        self.channels: list[grpc.aio.Channel] = []
        self.stubs: list[broker_pb2_grpc.BrokerStub] = []

    def stub_for(self, partition: int) -> broker_pb2_grpc.BrokerStub:
        """The broker that owns this partition — what a routing client does."""
        return self.stubs[partition % BROKERS]

    def wrong_stub_for(self, partition: int) -> broker_pb2_grpc.BrokerStub:
        return self.stubs[(partition + 1) % BROKERS]


@pytest.fixture
async def cluster(tmp_path):
    c = Cluster()
    for ordinal in range(BROKERS):
        # Same code, same config — the ONLY difference between two brokers is
        # `me` and their data directory. That is what "a cluster is N copies
        # of one process" means in practice.
        store = Store(
            tmp_path / f"broker-{ordinal}",
            partitions=PARTITIONS,
            ring=Ring(
                brokers=BROKERS,
                me=ordinal,
                address_template="localhost:909{ordinal}",
            ),
        )
        await store.open()
        server = BrokerServer(store, port=0)
        await server.start()
        channel = grpc.aio.insecure_channel(server.address)

        c.stores.append(store)
        c.servers.append(server)
        c.channels.append(channel)
        c.stubs.append(broker_pb2_grpc.BrokerStub(channel))

    yield c

    for channel in c.channels:
        await channel.close()
    for server in c.servers:
        await server.stop(grace=0)
    for store in c.stores:
        await store.close()


async def create_everywhere(cluster: Cluster, name: str = "orders") -> None:
    """Create the topic on every broker.

    Needed because nothing propagates a create: with no controller, a broker
    learns a topic exists when someone tells it. This IS the gap a real
    cluster controller fills, made visible.
    """
    for store in cluster.stores:
        await store.create(name)


# --------------------------------------------------------------------------
# where records live
# --------------------------------------------------------------------------


async def test_each_broker_stores_only_its_own_partitions(cluster):
    await create_everywhere(cluster)

    assert await cluster.stores[0].local_partitions("orders") == [0, 2]
    assert await cluster.stores[1].local_partitions("orders") == [1, 3]


async def test_only_the_owner_creates_the_directory_on_disk(cluster, tmp_path):
    await create_everywhere(cluster)

    assert sorted(p.name for p in (tmp_path / "broker-0" / "orders").iterdir()) == [
        "0", "2", "topic.json",
    ]
    assert sorted(p.name for p in (tmp_path / "broker-1" / "orders").iterdir()) == [
        "1", "3", "topic.json",
    ]


async def test_both_brokers_agree_on_the_total_partition_count(cluster):
    """The reason topic.json exists.

    Broker 0 holds 2 directories of a 4-partition topic. Counting them would
    answer 2, the partitioner would compute `crc32(key) % 2`, and every key
    would route somewhere different from where broker 1 sends it. The total
    cannot be derived from local disk once partitions are shared out.
    """
    await create_everywhere(cluster)

    assert await cluster.stores[0].partitions_of("orders") == PARTITIONS
    assert await cluster.stores[1].partitions_of("orders") == PARTITIONS


async def test_a_record_lands_on_one_broker_and_not_the_other(cluster):
    await create_everywhere(cluster)
    key = b"user-42"

    partition = await cluster.stores[0].route("orders", key)
    owner, other = partition % BROKERS, (partition + 1) % BROKERS

    response = await cluster.stubs[owner].Produce(
        broker_pb2.ProduceRequest(topic="orders", key=key, value=b"payload")
    )
    assert response.partition == partition

    # present on the owner...
    on_owner = await cluster.stores[owner].read("orders", 0, partition)
    assert [r.value for r in on_owner] == [b"payload"]

    # ...and the other broker does not even have the partition
    assert partition not in await cluster.stores[other].local_partitions("orders")


# --------------------------------------------------------------------------
# how a client finds out — Metadata
# --------------------------------------------------------------------------


async def test_every_broker_returns_the_same_routing_table(cluster):
    """Agreement by construction: no controller, no election, no messages
    between brokers. Both derive the identical answer from `partition % 2`."""
    await create_everywhere(cluster)

    tables = []
    for stub in cluster.stubs:
        response = await stub.Metadata(broker_pb2.MetadataRequest(topics=["orders"]))
        tables.append(
            {p.partition: (p.broker, p.address) for p in response.topics[0].partitions}
        )

    assert tables[0] == tables[1]
    assert tables[0] == {
        0: (0, "localhost:9090"),
        1: (1, "localhost:9091"),
        2: (0, "localhost:9090"),
        3: (1, "localhost:9091"),
    }


async def test_metadata_names_the_answering_broker(cluster):
    for ordinal, stub in enumerate(cluster.stubs):
        response = await stub.Metadata(broker_pb2.MetadataRequest())
        assert (response.broker_id, response.broker_count) == (ordinal, BROKERS)


async def test_metadata_for_a_topic_this_broker_has_never_seen_is_not_found(cluster):
    """The honest gap in a cluster with no controller.

    Nothing propagates a create. A broker answers only about topics it has
    been told about, so two brokers can genuinely disagree about whether a
    topic exists. This is exactly the hole a real controller fills, and it is
    why create_everywhere() exists in these tests.
    """
    await cluster.stores[0].create("orders")  # broker 0 only

    await cluster.stubs[0].Metadata(broker_pb2.MetadataRequest(topics=["orders"]))
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await cluster.stubs[1].Metadata(broker_pb2.MetadataRequest(topics=["orders"]))
    assert err.value.code() == grpc.StatusCode.NOT_FOUND


# --------------------------------------------------------------------------
# asking the wrong broker
# --------------------------------------------------------------------------


async def test_producing_to_the_wrong_broker_is_redirected_not_written(cluster):
    """The check that makes split-brain impossible.

    Accepting this write would create a second copy of partition 1 — on the
    wrong disk, numbering its offsets from zero, with no way to reconcile.
    Kafka answers NOT_LEADER_FOR_PARTITION for the same reason.
    """
    await create_everywhere(cluster)
    partition = 1  # owned by broker 1

    with pytest.raises(grpc.aio.AioRpcError) as err:
        await cluster.stubs[0].Produce(
            broker_pb2.ProduceRequest(
                topic="orders", key=b"k", value=b"v", partition=partition
            )
        )

    assert err.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "broker 1" in err.value.details()
    assert "localhost:9091" in err.value.details()  # where to go instead
    assert partition not in await cluster.stores[0].local_partitions("orders")


async def test_the_redirect_names_the_right_address_for_every_partition(cluster):
    await create_everywhere(cluster)
    for partition in range(PARTITIONS):
        wrong = cluster.wrong_stub_for(partition)
        with pytest.raises(grpc.aio.AioRpcError) as err:
            await wrong.Produce(
                broker_pb2.ProduceRequest(
                    topic="orders", key=b"k", value=b"v", partition=partition
                )
            )
        assert f"localhost:909{partition % BROKERS}" in err.value.details()


async def test_an_unrouted_produce_is_redirected_when_the_key_belongs_elsewhere(
    cluster,
):
    """A client that skips metadata and lets the broker route.

    Fine on one broker — every partition is local. With two, the broker
    computes the right partition, finds it is not its own, and says so. This
    is why client-side partitioning stops being optional in a cluster.
    """
    await create_everywhere(cluster)

    redirects = 0
    for n in range(20):
        key = f"user-{n}".encode()
        partition = await cluster.stores[0].route("orders", key)
        try:
            await cluster.stubs[0].Produce(
                broker_pb2.ProduceRequest(topic="orders", key=key, value=b"v")
            )
            assert partition % BROKERS == 0, "accepted a partition it does not own"
        except grpc.aio.AioRpcError as err:
            assert err.code() == grpc.StatusCode.FAILED_PRECONDITION
            assert partition % BROKERS == 1
            redirects += 1
    assert redirects > 0, "test needs some keys to belong to the other broker"


async def test_consuming_from_the_wrong_broker_is_redirected(cluster):
    await create_everywhere(cluster)
    await cluster.stubs[1].Produce(
        broker_pb2.ProduceRequest(topic="orders", key=b"k", value=b"v", partition=1)
    )

    stream = cluster.stubs[0].Consume(
        broker_pb2.ConsumeRequest(topic="orders", partition=1, offset=0)
    )
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await stream.read()
    assert err.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "broker 1" in err.value.details()


# --------------------------------------------------------------------------
# a client that routes properly — the whole loop
# --------------------------------------------------------------------------


async def test_a_routing_client_can_write_and_read_every_partition(cluster):
    """What a real client library does: fetch metadata once, then send each
    record straight to the broker that owns its key."""
    await create_everywhere(cluster)

    # 1. ask any broker for the routing table
    metadata = await cluster.stubs[0].Metadata(
        broker_pb2.MetadataRequest(topics=["orders"])
    )
    owner_of = {p.partition: p.broker for p in metadata.topics[0].partitions}

    # 2. route each key and send to its owner
    written: dict[int, list[bytes]] = {}
    for n in range(20):
        key = f"user-{n}".encode()
        partition = await cluster.stores[0].route("orders", key)
        value = f"v{n}".encode()
        response = await cluster.stubs[owner_of[partition]].Produce(
            broker_pb2.ProduceRequest(
                topic="orders", key=key, value=value, partition=partition
            )
        )
        assert response.partition == partition
        written.setdefault(partition, []).append(value)

    assert len(written) > 1, "test needs records in more than one partition"

    # 3. read each partition back from its owner
    for partition, values in written.items():
        stream = cluster.stubs[owner_of[partition]].Consume(
            broker_pb2.ConsumeRequest(topic="orders", partition=partition, offset=0)
        )
        got = [record.value async for record in stream]
        assert got == values


async def test_offsets_are_per_partition_across_brokers(cluster):
    # Both brokers number their own partitions from zero — an offset is
    # meaningless without the partition it belongs to, and doubly so here.
    await create_everywhere(cluster)

    zero = await cluster.stubs[0].Produce(
        broker_pb2.ProduceRequest(topic="orders", key=b"k", value=b"a", partition=0)
    )
    one = await cluster.stubs[1].Produce(
        broker_pb2.ProduceRequest(topic="orders", key=b"k", value=b"b", partition=1)
    )
    assert (zero.partition, zero.offset) == (0, 0)
    assert (one.partition, one.offset) == (1, 0)  # also zero, different log


async def test_a_broker_going_away_makes_only_its_partitions_unreachable(cluster):
    """Sharding buys capacity, not availability.

    This is the experiment worth running in Kubernetes: kill a pod and watch
    exactly its partitions go dark while the rest keep serving. Replication is
    what would fix it, and we have none.
    """
    await create_everywhere(cluster)
    await cluster.stubs[0].Produce(
        broker_pb2.ProduceRequest(topic="orders", key=b"k", value=b"survives", partition=0)
    )

    await cluster.servers[1].stop(grace=0)  # broker 1 "dies"

    # its partitions are gone...
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await cluster.stubs[1].Produce(
            broker_pb2.ProduceRequest(topic="orders", key=b"k", value=b"v", partition=1)
        )
    assert err.value.code() == grpc.StatusCode.UNAVAILABLE

    # ...while broker 0 serves its own, unaffected
    stream = cluster.stubs[0].Consume(
        broker_pb2.ConsumeRequest(topic="orders", partition=0, offset=0)
    )
    assert [r.value async for r in stream] == [b"survives"]
