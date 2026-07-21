# pyKA

**A lightweight message broker — publish and subscribe over gRPC, backed by a
durable append-only log.**

Runs as a single container, a Compose cluster, or a Kubernetes StatefulSet.
No JVM, no ZooKeeper, no external services to stand up first.

[![CI](https://github.com/andreibel/pyKA/actions/workflows/ci.yml/badge.svg)](https://github.com/andreibel/pyKA/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pyka-log)](https://pypi.org/project/pyka-log/)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

`Python 3.12` · `gRPC` · `Protobuf` · `asyncio` · `FastAPI` · `Docker` · `pytest` · `uv`

---

## Why pyKA

**Easy to deploy.** One 284 MB container with two ports and a volume. Scale to
a cluster by changing one number. Health and readiness endpoints that
Kubernetes understands, and a shutdown that drains cleanly instead of being
killed mid-write.

**Easy to use.** `pip install pyka-log` and three classes: `Producer`,
`Consumer`, `Admin`. The client finds the right broker for you, retries when
one is down, and hands back anything it could not deliver rather than dropping
it quietly.

**Easy to learn.** About 1,700 lines of commented Python across four small
layers. Every design decision is written down with the reasoning behind it, and
several were changed after being measured. If you have ever wanted to know how
a log broker actually works, this one is short enough to read in an evening.

---

## Features

- **Durable log** — records are CRC-checked, written to segments that roll at a
  size limit, and survive a crash: torn writes are truncated on restart and
  damaged files are refused rather than silently served
- **Topics and partitions** — records sharing a key always land together and
  keep their order; partitions give you parallelism
- **Live tail** — consumers block on a stream and receive records the moment
  they are written, with no polling
- **Fast reads anywhere in the log** — a sparse index makes reading from the
  middle of a large log as cheap as reading from the end
- **Scales across brokers** — partitions spread over a cluster with consistent
  hashing; clients route themselves and are redirected if they guess wrong
- **Two planes** — gRPC on `:9092` for data, an HTTP control plane on `:8080`
  for topics, inspection, and probes

---

## Quick start

```sh
docker run --rm -p 9092:9092 -p 8080:8080 \
    -v pyka-data:/var/lib/pyka \
    ghcr.io/andreibel/pyka:main
```

Tags: `:main` tracks the latest green build, `:sha-<commit>` pins one exactly,
and `:0.1.0` / `:latest` appear once a version is released.

Then, from Python:

```python
from pyka.client import Admin, Producer, Consumer

Admin("http://localhost:8080").create_topic("orders", partitions=3)

with Producer("localhost:9092") as producer:
    partition, offset = producer.send("orders", b"hello", key=b"user-1")

with Consumer("localhost:9092") as consumer:
    for record in consumer.consume("orders", partition, follow=True):
        print(record.offset, record.key, record.value)
```

Interactive API docs at <http://localhost:8080/docs>.

---

## Install

```sh
pip install pyka-log             # the client: Producer, Consumer, Admin
pip install 'pyka-log[broker]'   # plus everything needed to run a broker
```

The client depends only on `grpcio` and `protobuf` — a client has no business
installing a web framework. The distribution is `pyka-log` because `pyka` is
taken on PyPI; the import name is still `pyka`.

---

## Using the client

### Producing

```python
from pyka.client import Producer, DeliveryFailed

with Producer(["localhost:9090", "localhost:9091"]) as producer:
    partition, offset = producer.send("orders", b"payload", key=b"user-1")

    # many records at once, grouped into one stream per broker
    results = producer.send_batch("orders", [(b"user-1", b"a"), (b"user-2", b"b")])

    # a tombstone: value=None means "this key is deleted"
    producer.send("orders", None, key=b"user-1")
```

`send` returns **both** partition and offset, because every partition numbers
its records from zero — an offset alone identifies nothing.

The client routes: it computes the partition itself and connects to the broker
that owns it, retrying with backoff if that broker is down and refetching
metadata if the routing moved. Records that never land come back to you rather
than disappearing:

```python
try:
    producer.send("orders", value, key=key)
except DeliveryFailed as err:
    for topic, key, value in err.undelivered:
        spool_somewhere(topic, key, value)
```

### Consuming

```python
from pyka.client import Consumer

with Consumer("localhost:9092") as consumer:
    # read what exists, then stop
    for record in consumer.consume("orders", partition=0, limit=100):
        handle(record)

    # or block and receive records as they are written
    for record in consumer.consume("orders", partition=0, offset=42, follow=True):
        handle(record)
        save_my_offset(record.offset)
```

One partition per consumer, deliberately: there is no correct order in which to
merge two partitions, since each numbers from zero. Read a whole topic with one
consumer per partition.

### Administering

```python
from pyka.client import Admin

admin = Admin("http://localhost:8080")
admin.create_topic("orders", partitions=6)
admin.topics()
admin.partition_info("orders", 0)   # segments, sizes, index entries
```

Full reference: **[docs/client-api.md](docs/client-api.md)**.

---

## Running a broker

### One broker

```sh
docker run -p 9092:9092 -p 8080:8080 -v pyka-data:/var/lib/pyka \
    ghcr.io/andreibel/pyka:latest
```

Or from a checkout, without Docker:

```sh
uv sync --all-extras
PYKA_DATA_DIR=/tmp/pyka uv run pyka-broker
```

### A cluster, with Docker Compose

```sh
docker compose up -d --build
docker compose ps
docker compose down -v
```

Three brokers on `localhost:9090`, `9091` and `9092`, each with its own volume.

### A cluster, as individual containers

Each broker needs its own identity, volume and host ports. The ordinal comes
from the hostname, exactly as it will from a Kubernetes StatefulSet:

```sh
docker network create pyka

for n in 0 1 2; do
  docker run -d --name pyka-$n --network pyka --hostname pyka-$n \
    -e PYKA_BROKERS=3 \
    -e PYKA_ADDRESS_TEMPLATE='localhost:909{ordinal}' \
    -e PYKA_PARTITIONS=6 \
    -p 909$n:9092 -p 808$n:8080 \
    -v pyka-$n-data:/var/lib/pyka \
    ghcr.io/andreibel/pyka:latest
done
```

`PYKA_ADDRESS_TEMPLATE` is what brokers advertise to clients, so it must be
reachable from wherever the client runs: `localhost:909N` for a client on your
machine, service names for one inside the network. This is the same
distinction Kafka draws between `listeners` and `advertised.listeners`.

### Live, in two terminals

```sh
uv run python examples/consumer.py orders 0    # blocks, prints as records arrive
uv run python examples/producer.py orders      # type a line, press enter
```

See [examples/](examples/).

---

## Configuration

| variable | default | |
|---|---|---|
| `PYKA_DATA_DIR` | `/var/lib/pyka` | data root; a mounted volume in Kubernetes |
| `PYKA_PORT` | `9092` | gRPC, the data plane |
| `PYKA_ADMIN_PORT` | `8080` | HTTP, the control plane |
| `PYKA_PARTITIONS` | `1` | partitions for **new** topics |
| `PYKA_SEGMENT_BYTES` | 1 GiB | roll threshold; 4 GiB maximum |
| `PYKA_SYNC_RECORDS` | unset | fsync every N records |
| `PYKA_SYNC_MILLIS` | unset | fsync every N milliseconds |
| `PYKA_BROKERS` | `1` | cluster size; immutable while data exists |
| `PYKA_ADDRESS_TEMPLATE` | `pyka-{ordinal}.pyka-hl:9092` | what clients are told to dial |
| `PYKA_RING` | consistent | `modulo` for the naive `p % n` |
| `PYKA_GRACE` | `30` | shutdown drain, in seconds |

---

## Documentation

| | |
|---|---|
| [Client API](docs/client-api.md) | Producer, Consumer, Admin, and the semantics that matter |
| [Design](docs/design.md) | how the log, index, segments and topics work, and why |
| [Architecture](docs/architecture.md) | call paths and cluster topology, with diagrams |
| [Operations](docs/operations.md) | clusters, broker failure, resizing, migration |
| [Roadmap](docs/roadmap.md) | what is built and what is not |

---

## Status

Working: storage, partitioning, the gRPC broker, live tail, sharding across
brokers, the client library, and Docker.

**Not implemented, deliberately:** replication — so a broker's partitions are
unreachable while it is down, and lost if its disk is — along with consumer
groups, committed offsets, retention and compaction. Delivery is at-least-once.

`PYKA_BROKERS` cannot change while a cluster holds data: a broker that finds
partitions it no longer owns refuses readiness rather than serving a split log.
See [Operations](docs/operations.md).

This is a learning project built to understand how a log broker works, not a
production message bus.

---

## Development

```sh
uv sync --all-extras
uv run pytest                       # 479 tests
./scripts/gen_proto.sh              # regenerate stubs after editing the .proto
uv run python bench/bench_seek.py   # the index benchmark
./scripts/cluster.sh start 3        # a local three-broker cluster
```

## License

MIT — see [LICENSE](LICENSE).
