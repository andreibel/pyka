"""The client library, against real brokers.

Two brokers on real sockets with real files, because everything interesting
about a client is what it does with a cluster: routing, discovery, and what
happens when a broker is not there.

The brokers run on their own event loop in a background thread. That is not
test scaffolding for its own sake — it is the actual shape of the system. The
broker is async because it serves many connections; the client is blocking
because it serves one caller. A sync test calling into a grpc.aio server whose
loop is not running simply hangs, which is exactly what happened before this
fixture existed.
"""

import asyncio
import threading

import pytest

from pyka.broker.server import BrokerServer
from pyka.broker.store import Store
from pyka.client import (
    Admin,
    Consumer,
    DeliveryFailed,
    Producer,
    PykaError,
    UnknownTopic,
)
from pyka.cluster.ring import Ring

BROKERS = 2
PARTITIONS = 4


class _FixedAddressRing(Ring):
    """A ring whose addresses are handed in rather than templated.

    Tests bind ephemeral ports (port=0, so the OS chooses), which no
    `localhost:909{ordinal}` template can predict. Everything else about
    routing is the real Ring.
    """

    def __init__(self, brokers: int, me: int, addresses: list[str]) -> None:
        super().__init__(brokers=brokers, me=me)
        object.__setattr__(self, "_addresses", tuple(addresses))

    def address_of(self, broker: int) -> str:
        if not 0 <= broker < self.brokers:
            raise ValueError(f"no broker {broker} in a {self.brokers}-broker cluster")
        return self._addresses[broker]


class Cluster:
    """Brokers on a background event loop, driven from blocking test code."""

    def __init__(self, tmp_path):
        self._tmp = tmp_path
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self.stores: list[Store] = []
        self.servers: list[BrokerServer] = []
        self.addresses: list[str] = []

    def run(self, coro, timeout: float = 15.0):
        """Run a coroutine on the broker loop and block for its result."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def start(self) -> None:
        for ordinal in range(BROKERS):
            # Constructed INSIDE the loop, not merely started there:
            # grpc.aio.server() binds to whatever loop is current when it is
            # built, so building it on the test thread and starting it on the
            # broker loop gives "Future attached to a different loop".
            async def _build(ordinal=ordinal):
                store = Store(self._tmp / f"broker-{ordinal}", partitions=PARTITIONS)
                await store.open()
                server = BrokerServer(store, port=0)
                await server.start()
                return store, server

            store, server = self.run(_build())
            self.stores.append(store)
            self.servers.append(server)

        # Ports are known only after binding, so the rings are built now and
        # swapped in. Every broker gets the same address list — that is the
        # whole point: identical inputs, identical routing table.
        self.addresses = [f"localhost:{s.port}" for s in self.servers]
        for ordinal, store in enumerate(self.stores):
            store._ring = _FixedAddressRing(BROKERS, ordinal, self.addresses)

    def create(self, topic: str) -> None:
        """Create on every broker — nothing propagates a create without a
        controller, which is a real gap in the design, not in this helper."""
        for store in self.stores:
            self.run(store.create(topic))

    def kill(self, ordinal: int) -> None:
        self.run(self.servers[ordinal].stop(grace=0))

    def stop(self) -> None:
        # Stores first: closing one releases parked live-tail streams, and
        # server.stop() would otherwise wait for them. Same ordering the real
        # broker uses on SIGTERM.
        for store in self.stores:
            self.run(store.close())
        for server in self.servers:
            self.run(server.stop(grace=0))
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


@pytest.fixture
def cluster(tmp_path):
    c = Cluster(tmp_path)
    c.start()
    yield c
    c.stop()


@pytest.fixture
def ready(cluster):
    """A cluster with `orders` created, and the bootstrap addresses."""
    cluster.create("orders")
    return cluster.addresses


# --------------------------------------------------------------------------
# producing
# --------------------------------------------------------------------------


def test_send_returns_where_the_record_landed(ready):
    with Producer(ready) as producer:
        partition, offset = producer.send("orders", b"hello", key=b"user-1")
    assert 0 <= partition < PARTITIONS
    assert offset == 0


def test_the_client_routes_by_key_not_the_broker(ready):
    # The client computes the partition and connects to its owner. A broker
    # answers FAILED_PRECONDITION for anything it does not own, so these
    # sends succeeding IS the proof that routing happened client-side.
    with Producer(ready) as producer:
        seen = {
            producer.send("orders", b"v", key=f"user-{n}".encode())[0]
            for n in range(20)
        }
    assert len(seen) > 1, "20 keys should not all land in one partition"


def test_one_key_always_reaches_the_same_partition(ready):
    # The ordering guarantee, from the client's side: same key, same log.
    with Producer(ready) as producer:
        landed = {
            producer.send("orders", f"v{n}".encode(), key=b"steady")[0]
            for n in range(10)
        }
    assert len(landed) == 1


def test_offsets_increase_for_one_key(ready):
    with Producer(ready) as producer:
        offsets = [
            producer.send("orders", f"v{n}".encode(), key=b"k")[1] for n in range(5)
        ]
    assert offsets == [0, 1, 2, 3, 4]


def test_send_batch_groups_by_broker(ready):
    # One stream per broker rather than a round trip per record.
    with Producer(ready) as producer:
        results = producer.send_batch(
            "orders", [(f"user-{n}".encode(), f"v{n}".encode()) for n in range(30)]
        )
    assert len(results) == 30
    assert len({partition for partition, _ in results}) > 1


def test_producing_to_an_unknown_topic_raises(ready):
    with Producer(ready) as producer, pytest.raises(UnknownTopic):
        producer.send("nope", b"v", key=b"k")


def test_an_illegal_topic_name_is_not_retried(ready):
    # INVALID_ARGUMENT is the caller's fault and would fail identically
    # forever; retrying until the delivery timeout wastes a minute per record.
    with Producer(ready, delivery_timeout=30) as producer:
        with pytest.raises(PykaError, match="INVALID_ARGUMENT"):
            producer.send("../escape", b"v", key=b"k")


# --------------------------------------------------------------------------
# consuming
# --------------------------------------------------------------------------


def test_consume_reads_back_what_was_produced(ready):
    with Producer(ready) as producer:
        partition, _ = producer.send("orders", b"payload", key=b"user-1")

    with Consumer(ready) as consumer:
        records = list(consumer.consume("orders", partition))
    assert [r.value for r in records] == [b"payload"]
    assert (records[0].key, records[0].topic) == (b"user-1", "orders")


def test_consume_resumes_from_an_offset(ready):
    # How a restarted consumer catches up: records carry their own offsets.
    with Producer(ready) as producer:
        for n in range(5):
            partition, _ = producer.send("orders", f"v{n}".encode(), key=b"k")

    with Consumer(ready) as consumer:
        records = list(consumer.consume("orders", partition, offset=3))
    assert [r.value for r in records] == [b"v3", b"v4"]


def test_consume_respects_a_limit(ready):
    with Producer(ready) as producer:
        for n in range(10):
            partition, _ = producer.send("orders", f"v{n}".encode(), key=b"k")

    with Consumer(ready) as consumer:
        assert len(list(consumer.consume("orders", partition, limit=3))) == 3


def test_a_tombstone_survives_the_round_trip(ready):
    # None is absent, b"" is present-and-empty. The library must not collapse
    # them: a compacted topic reads a tombstone as "this key is deleted".
    with Producer(ready) as producer:
        partition, _ = producer.send("orders", None, key=b"gone")
        producer.send("orders", b"", key=b"gone")

    with Consumer(ready) as consumer:
        records = list(consumer.consume("orders", partition))
    assert records[0].value is None and records[0].is_tombstone
    assert records[1].value == b"" and not records[1].is_tombstone


def test_consuming_an_unknown_topic_raises(ready):
    with Consumer(ready) as consumer, pytest.raises(UnknownTopic):
        list(consumer.consume("nope", 0))


def test_follow_delivers_records_produced_later(ready):
    """The live path through the library: the stream stays open and a record
    written afterwards arrives without polling."""
    with Producer(ready) as producer:
        # Pick a key whose partition we will then follow, so the test does not
        # depend on which partition a fixed key happens to hash to.
        key = b"user-7"
        partition = producer.partition_for("orders", key)

        received: list[bytes] = []
        subscribed = threading.Event()

        def follower():
            with Consumer(ready) as consumer:
                stream = consumer.consume("orders", partition, follow=True)
                subscribed.set()
                for record in stream:
                    received.append(record.value)
                    return

        thread = threading.Thread(target=follower, daemon=True)
        thread.start()
        assert subscribed.wait(timeout=5)

        producer.send("orders", b"live", key=key)
        thread.join(timeout=10)

    assert received == [b"live"]


# --------------------------------------------------------------------------
# discovery and failure
# --------------------------------------------------------------------------


def test_any_bootstrap_broker_can_answer(ready):
    # Bootstrap addresses are redundancy, not a peer list: every broker holds
    # the same routing table, so reversing the order changes nothing.
    with Consumer(ready) as first, Consumer(list(reversed(ready))) as second:
        assert first.routing("orders") == second.routing("orders")


def test_a_dead_bootstrap_is_skipped(ready):
    # Why more than one address is worth listing: the first may be down.
    with Consumer(["localhost:1", *ready]) as consumer:
        assert len(consumer.routing("orders")) == PARTITIONS


def test_no_reachable_broker_raises(ready):
    with Consumer(["localhost:1", "localhost:2"]) as consumer:
        with pytest.raises(PykaError, match="no bootstrap broker answered"):
            consumer.routing("orders")


def test_delivery_fails_loudly_when_the_owner_never_returns(cluster):
    """After the deadline the library raises rather than pretending, and hands
    back the records that never landed.

    Waiting in the client is the only safe place for them: a broker holding
    another broker's writes would create a second log for that partition,
    numbering from zero, with no way to reconcile the two.
    """
    cluster.create("orders")
    producer = Producer(cluster.addresses, delivery_timeout=1.0, retry_backoff=0.05)

    key = next(
        k
        for k in (f"user-{n}".encode() for n in range(50))
        if producer.partition_for("orders", k) % BROKERS == 1
    )
    cluster.kill(1)

    with pytest.raises(DeliveryFailed) as err:
        producer.send("orders", b"never", key=key)
    assert err.value.undelivered == [("orders", key, b"never")]
    producer.close()


def test_records_for_a_live_broker_still_flow_while_another_is_down(cluster):
    # Sharding's actual promise: an outage is partial, not total.
    cluster.create("orders")
    producer = Producer(cluster.addresses, delivery_timeout=1.0, retry_backoff=0.05)
    alive = next(
        k
        for k in (f"user-{n}".encode() for n in range(50))
        if producer.partition_for("orders", k) % BROKERS == 0
    )
    cluster.kill(1)

    partition, offset = producer.send("orders", b"fine", key=alive)
    assert offset == 0
    producer.close()


# --------------------------------------------------------------------------
# admin
# --------------------------------------------------------------------------


def test_admin_reports_an_unreachable_api_clearly():
    # A connection error should say what could not be reached, not surface a
    # raw urllib traceback from three frames down.
    admin = Admin("http://localhost:1", timeout=1.0)
    with pytest.raises(PykaError, match="unreachable"):
        admin.topics()
    assert admin.ready() is False


# --------------------------------------------------------------------------
# admin, against a real control plane
# --------------------------------------------------------------------------


@pytest.fixture
def admin(cluster):
    """The admin API of broker 0, on its own port."""
    import uvicorn

    from pyka.broker.admin import create_app

    server = uvicorn.Server(
        uvicorn.Config(create_app(cluster.stores[0]), host="127.0.0.1", port=0,
                       log_config=None)
    )
    server.install_signal_handlers = lambda: None
    task = cluster.run(_spawn(server))
    while not server.started:
        pass
    port = server.servers[0].sockets[0].getsockname()[1]
    yield Admin(f"http://127.0.0.1:{port}")
    server.should_exit = True
    cluster.run(_await(task))


async def _spawn(server):
    return asyncio.ensure_future(server.serve())


async def _await(task):
    await task


def test_admin_creates_and_lists_topics(admin):
    assert admin.topics() == []
    assert admin.create_topic("orders", 3) == {"name": "orders", "partitions": 3}
    assert admin.topics() == ["orders"]


def test_admin_create_is_idempotent_and_never_repartitions(admin):
    # Changing the count would move every key to a different partition and
    # break the per-key ordering the partitioner exists to provide.
    admin.create_topic("orders", 3)
    assert admin.create_topic("orders", 9)["partitions"] == 3


def test_admin_describes_a_topic_and_its_segments(admin):
    admin.create_topic("orders", 2)
    assert admin.describe("orders") == {"name": "orders", "partitions": 2}

    info = admin.partition_info("orders", 0)
    assert info["next_offset"] == 0
    assert len(info["segments"]) == 1
    assert info["segments"][0]["sealed"] is False


def test_admin_reports_broker_identity_and_readiness(admin):
    assert admin.ready() is True
    assert admin.broker()["broker_id"] == 0


def test_admin_raises_on_an_unknown_topic(admin):
    with pytest.raises(PykaError, match="404"):
        admin.describe("nope")


def test_admin_raises_on_an_illegal_name(admin):
    # The security boundary is on the server; the client just reports it.
    with pytest.raises(PykaError):
        admin.create_topic("../escape", 1)
