"""pyKA client: Producer, Consumer, Admin.

    from pyka.client import Producer, Consumer

    with Producer("localhost:9090") as producer:
        partition, offset = producer.send("orders", b"hello", key=b"user-1")

    with Consumer("localhost:9090") as consumer:
        for record in consumer.consume("orders", partition=0, follow=True):
            print(record.offset, record.value)

Why a client library is not just a thin wrapper: in a cluster it has to do
real work that a single-broker client never did.

* **Route.** It computes the partition itself, with the same hash the broker
  uses, because it cannot know which broker to open a socket to until it knows
  the partition. A broker cannot do this for it without an extra network hop.
* **Discover.** It asks any broker for metadata and caches the routing table.
* **Recover.** It retries a dead broker with backoff and refetches metadata
  when a broker says the routing has moved. Records wait in memory HERE — a
  broker holding another broker's writes would be split-brain.

Everything is blocking and synchronous. The broker is async because it serves
thousands of connections; a client serves one caller and gains nothing from an
event loop.
"""
import json
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Self

import grpc

from pyka.topic.partitioner import Partitioner
from pyka.v1 import broker_pb2 as pb
from pyka.v1 import broker_pb2_grpc as pbg

DEFAULT_TIMEOUT = 10.0
DEFAULT_DELIVERY_TIMEOUT = 60.0


class PykaError(Exception):
    """Base for every error this library raises."""


class DeliveryFailed(PykaError):
    """A record could not be delivered before the deadline.

    Carries the records that never landed, so the caller can decide what to do
    with them — the library will not silently drop data it accepted.
    """

    def __init__(self, message: str, undelivered: list[tuple]) -> None:
        super().__init__(message)
        self.undelivered = undelivered


class UnknownTopic(PykaError):
    """No such topic on the broker that was asked."""


@dataclass(frozen=True)
class Record:
    """A record as a consumer sees it.

    ``key``/``value`` are None when absent, never b"": a null value is a
    tombstone (this key is deleted) and an empty value is an empty payload,
    and the two must not collapse into each other.
    """

    topic: str
    partition: int
    offset: int
    timestamp: int
    key: bytes | None
    value: bytes | None

    @property
    def is_tombstone(self) -> bool:
        return self.value is None


def _record(topic: str, partition: int, message: pb.Record) -> Record:
    return Record(
        topic=topic,
        partition=partition,
        offset=message.offset,
        timestamp=message.timestamp,
        key=message.key if message.HasField("key") else None,
        value=message.value if message.HasField("value") else None,
    )


class _Connection:
    """Channels and routing, shared by Producer and Consumer."""

    def __init__(
        self,
        bootstrap: str | list[str] = "localhost:9092",
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        # A list only for redundancy at startup: any broker can answer
        # metadata, so this is "somewhere to begin", not a set of peers.
        self._bootstrap = [bootstrap] if isinstance(bootstrap, str) else list(bootstrap)
        if not self._bootstrap:
            raise ValueError("at least one bootstrap address is required")
        self._timeout = timeout
        self._channels: dict[str, grpc.Channel] = {}
        self._routing: dict[str, dict[int, str]] = {}  # topic -> partition -> address
        self._partitioner = Partitioner()

    # ------------------------------------------------------------ plumbing

    def _stub(self, address: str) -> pbg.BrokerStub:
        # One channel per broker, reused. gRPC channels are expensive to build
        # and are designed to be long-lived; a channel per request would spend
        # more time on TCP and HTTP/2 handshakes than on records.
        if address not in self._channels:
            self._channels[address] = grpc.insecure_channel(address)
        return pbg.BrokerStub(self._channels[address])

    def routing(self, topic: str, refresh: bool = False) -> dict[int, str]:
        """partition -> broker address, cached.

        Refetched only when a broker says the map is stale. Fetching it per
        request would double the round trips for no benefit: the routing
        changes when a cluster is resized, which is approximately never.
        """
        if refresh or topic not in self._routing:
            self._routing[topic] = self._fetch_routing(topic)
        return self._routing[topic]

    def _fetch_routing(self, topic: str) -> dict[int, str]:
        last: grpc.RpcError | None = None
        for address in self._bootstrap:
            try:
                response = self._stub(address).Metadata(
                    pb.MetadataRequest(topics=[topic]), timeout=self._timeout
                )
                return {
                    p.partition: p.address for p in response.topics[0].partitions
                }
            except grpc.RpcError as err:
                if err.code() == grpc.StatusCode.NOT_FOUND:
                    raise UnknownTopic(f"no topic {topic!r}") from err
                last = err  # this broker is down; try the next one
        raise PykaError(f"no bootstrap broker answered: {last}") from last

    def partition_for(self, topic: str, key: bytes | None) -> int:
        """The same crc32(key) % n the broker computes.

        It must be the same function. If the two disagreed, every record would
        be rejected by the broker it was sent to — and a hash that changed
        between runs would split one key's history across two partitions.
        """
        return self._partitioner.partition_for(key, len(self.routing(topic)))

    def close(self) -> None:
        for channel in self._channels.values():
            channel.close()
        self._channels.clear()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class Producer(_Connection):
    """Sends records, routing each one to the broker that owns its key."""

    def __init__(
        self,
        bootstrap: str | list[str] = "localhost:9092",
        timeout: float = DEFAULT_TIMEOUT,
        delivery_timeout: float = DEFAULT_DELIVERY_TIMEOUT,
        retry_backoff: float = 0.25,
    ) -> None:
        super().__init__(bootstrap, timeout)
        self._delivery_timeout = delivery_timeout
        self._retry_backoff = retry_backoff

    def send(
        self,
        topic: str,
        value: bytes | None,
        key: bytes | None = None,
        timestamp: int | None = None,
    ) -> tuple[int, int]:
        """Send one record; returns (partition, offset).

        Retries while the owning broker is unreachable, up to
        ``delivery_timeout``. Raises DeliveryFailed after that rather than
        pretending — the record is the caller's problem again.

        ``value=None`` writes a tombstone: the marker meaning "this key is
        deleted", which is what a compacted topic reads.
        """
        deadline = time.monotonic() + self._delivery_timeout
        backoff = self._retry_backoff

        while True:
            partition = self.partition_for(topic, key)
            request = pb.ProduceRequest(
                topic=topic, key=key, value=value, partition=partition
            )
            if timestamp is not None:
                request.timestamp = timestamp

            try:
                address = self.routing(topic)[partition]
                response = self._stub(address).Produce(request, timeout=self._timeout)
                return response.partition, response.offset
            except grpc.RpcError as err:
                self._handle(err, topic)
                if time.monotonic() >= deadline:
                    raise DeliveryFailed(
                        f"could not deliver to {topic}/{partition} within "
                        f"{self._delivery_timeout}s: {err.code().name}",
                        [(topic, key, value)],
                    ) from err
                time.sleep(backoff)
                backoff = min(backoff * 2, 4.0)  # exponential, capped

    def send_batch(
        self, topic: str, records: list[tuple[bytes | None, bytes | None]]
    ) -> list[tuple[int, int]]:
        """Send many (key, value) pairs, one stream per broker.

        Grouping by broker is the point: a stream keeps many records in flight
        over one call, instead of paying a round trip each. Records for one key
        stay in order because they share a partition and a stream.
        """
        routing = self.routing(topic)
        grouped: dict[str, list[pb.ProduceRequest]] = {}
        for key, value in records:
            partition = self.partition_for(topic, key)
            grouped.setdefault(routing[partition], []).append(
                pb.ProduceRequest(
                    topic=topic, key=key, value=value, partition=partition
                )
            )

        results = []
        for address, requests in grouped.items():
            stream = self._stub(address).ProduceStream(iter(requests))
            results.extend((r.partition, r.offset) for r in stream)
        return results

    def _handle(self, err: grpc.RpcError, topic: str) -> None:
        """Decide whether an error is worth retrying, and fix what we can."""
        code = err.code()
        if code == grpc.StatusCode.FAILED_PRECONDITION:
            # "You have the wrong broker" — our map is stale, so replace it
            # and try again rather than guessing.
            self.routing(topic, refresh=True)
        elif code == grpc.StatusCode.UNAVAILABLE:
            pass  # the owner is down; wait and retry
        elif code == grpc.StatusCode.NOT_FOUND:
            raise UnknownTopic(f"no topic {topic!r}") from err
        else:
            # INVALID_ARGUMENT and friends are our fault: a bad topic name, an
            # oversized record. Retrying would fail identically forever.
            raise PykaError(f"{code.name}: {err.details()}") from err


class Consumer(_Connection):
    """Reads records from one partition at a time.

    One partition, deliberately. Merging several would require inventing an
    order across them, and there is none: each partition numbers its records
    from zero and nothing relates one to another. To read a whole topic, run
    one consumer per partition — which is exactly how consumer groups
    parallelise.
    """

    def consume(
        self,
        topic: str,
        partition: int = 0,
        offset: int = 0,
        follow: bool = False,
        limit: int = 0,
    ) -> Iterator[Record]:
        """Yield records from ``offset`` onward.

        ``follow=False`` ends at the last record currently written.
        ``follow=True`` never ends: it blocks and yields new records as they
        are appended. ``limit`` is ignored while following.

        Each record carries its own offset, which is how a caller knows where
        to resume after a restart.
        """
        address = self.routing(topic)[partition]
        request = pb.ConsumeRequest(
            topic=topic,
            partition=partition,
            offset=offset,
            follow=follow,
            max_records=limit,
        )
        try:
            # No timeout: a following stream is meant to block indefinitely,
            # and a bounded read is as long as the data it returns.
            for message in self._stub(address).Consume(request):
                yield _record(topic, partition, message)
        except grpc.RpcError as err:
            code = err.code()
            if code == grpc.StatusCode.NOT_FOUND:
                raise UnknownTopic(f"no topic {topic!r}") from err
            if code == grpc.StatusCode.FAILED_PRECONDITION:
                self.routing(topic, refresh=True)
            if code == grpc.StatusCode.CANCELLED:
                return  # the caller stopped iterating; not an error
            raise PykaError(f"{code.name}: {err.details()}") from err

    def partitions(self, topic: str) -> list[int]:
        return sorted(self.routing(topic))


class Admin:
    """Control plane: create and inspect topics over the REST API.

    A separate class on a separate port because it is a separate concern —
    rare calls made by people and scripts, versus constant calls made by
    client libraries. Uses urllib so the library needs no HTTP dependency.
    """

    def __init__(self, url: str = "http://localhost:8080", timeout: float = 10.0) -> None:
        self._url = url.rstrip("/")
        self._timeout = timeout

    def create_topic(self, name: str, partitions: int = 1) -> dict:
        """Create a topic. Idempotent; never re-partitions an existing one.

        In a cluster this must be called against EVERY broker: nothing
        propagates a create, because there is no controller. That is a real
        gap in the design, not an oversight in this method.
        """
        return self._request("/topics", "POST", {"name": name, "partitions": partitions})

    def topics(self) -> list[str]:
        return self._request("/topics")

    def describe(self, name: str) -> dict:
        return self._request(f"/topics/{name}")

    def partition_info(self, name: str, partition: int = 0) -> dict:
        """Segments, sizes and index entries — the storage layer, visible."""
        return self._request(f"/topics/{name}/partitions/{partition}")

    def broker(self) -> dict:
        return self._request("/")

    def ready(self) -> bool:
        try:
            return bool(self._request("/readyz")["ready"])
        except PykaError:
            return False

    def _request(self, path: str, method: str = "GET", body: dict | None = None):
        request = urllib.request.Request(
            self._url + path,
            method=method,
            data=json.dumps(body).encode() if body else None,
            headers={"content-type": "application/json"} if body else {},
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as err:
            raise PykaError(f"{err.code} from {path}: {err.read().decode()}") from err
        except OSError as err:
            raise PykaError(f"admin API unreachable at {self._url}: {err}") from err
