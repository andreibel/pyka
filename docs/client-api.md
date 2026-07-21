# Client API

```python
from pyka.client import Producer, Consumer, Admin, Record
from pyka.client import PykaError, DeliveryFailed, UnknownTopic
```

Everything is **blocking**. The broker is async because it serves thousands of
connections; a client serves one caller and gains nothing from an event loop.

---

## Producer

```python
Producer(
    bootstrap: str | list[str] = "localhost:9092",
    timeout: float = 10.0,            # per-RPC deadline
    delivery_timeout: float = 60.0,   # how long to keep retrying one record
    retry_backoff: float = 0.25,      # first retry delay; doubles, capped at 4s
)
```

`bootstrap` is **somewhere to begin**, not a peer list — every broker can
answer metadata, so several addresses are redundancy for startup only.

### `send(topic, value, key=None, timestamp=None) -> (partition, offset)`

```python
partition, offset = producer.send("orders", b"payload", key=b"user-1")
```

Blocks until the record is written. Returns **both** numbers, because each
partition numbers from zero — an offset alone identifies nothing.

- `key=None` → round-robin. Such records have no ordering relationship with
  anything and cannot be compacted.
- `value=None` → **tombstone**: the marker meaning "this key is deleted".
  `b""` is a *different thing* — a present-but-empty payload.
- `timestamp=None` → the broker stamps epoch milliseconds.

### `send_batch(topic, [(key, value), ...]) -> [(partition, offset), ...]`

Groups by owning broker and opens one stream per broker, so many records are
in flight over one call instead of a round trip each. Results are **not** in
input order — they arrive grouped by broker.

### `partition_for(topic, key) -> int`

The same `crc32(key) % n` the broker computes. Useful for deciding which
partition to consume from after producing.

---

## Consumer

```python
Consumer(bootstrap="localhost:9092", timeout=10.0)
```

### `consume(topic, partition=0, offset=0, follow=False, limit=0) -> Iterator[Record]`

```python
for record in consumer.consume("orders", partition=2, offset=100, follow=True):
    print(record.offset, record.value)
```

- `follow=False` — ends at the last record currently written.
- `follow=True` — **never ends**. Blocks and yields records as they are
  appended. `limit` is ignored.
- `limit=0` — no limit.

**One partition per call, deliberately.** There is no correct order in which to
merge two partitions: each numbers from zero and nothing relates them. To read
a whole topic, run one consumer per partition — which is exactly how consumer
groups parallelise.

### `partitions(topic) -> list[int]`

---

## Record

```python
@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    timestamp: int          # epoch milliseconds
    key: bytes | None       # None = absent, b"" = present and empty
    value: bytes | None     # None = TOMBSTONE
    is_tombstone: bool      # property: value is None
```

---

## Admin

Control plane, on the REST port (8080), not gRPC (9092). Separate because it
is a separate concern: rare calls made by people and scripts.

```python
admin = Admin("http://localhost:8080")

admin.create_topic("orders", partitions=3)   # -> {"name": ..., "partitions": 3}
admin.topics()                               # -> ["orders"]
admin.describe("orders")                     # -> {"name":..., "partitions":...}
admin.partition_info("orders", 0)            # -> segments, sizes, index entries
admin.broker()                               # -> broker_id, brokers, ready, topics
admin.ready()                                # -> bool, never raises
```

`create_topic` is idempotent and **never re-partitions**: changing the count
would move every key to a different partition and break per-key ordering. Ask
for a different count on an existing topic and you get the real one back.

**In a cluster you must call `create_topic` against every broker.** Nothing
propagates a create, because there is no controller. That is a real gap in the
design, not an oversight in this method.

`partition_info` is the interesting one — it exposes the storage layer:

```json
{"partition": 0, "next_offset": 309, "size_bytes": 80101,
 "segments": [{"base_offset": 0,   "size_bytes": 59827, "index_entries": 14, "sealed": true},
              {"base_offset": 231, "size_bytes": 20274, "index_entries": 4,  "sealed": false}]}
```

---

## Errors

| exception | when | retry? |
|---|---|---|
| `UnknownTopic` | no such topic on the broker asked | no — create it |
| `DeliveryFailed` | owner unreachable past `delivery_timeout` | your call; `.undelivered` holds the records |
| `PykaError` | everything else (bad name, oversized record, no broker reachable) | no |

`Producer.send` handles two cases internally and never surfaces them:

- **`UNAVAILABLE`** — the owner is down. Retries with exponential backoff until
  `delivery_timeout`, then raises `DeliveryFailed`.
- **`FAILED_PRECONDITION`** — the routing moved. Refetches metadata and retries
  immediately.

`INVALID_ARGUMENT` is *not* retried: an illegal topic name or an oversized
record will fail identically forever, so retrying wastes the whole timeout.

```python
try:
    producer.send("orders", value, key=key)
except DeliveryFailed as err:
    for topic, key, value in err.undelivered:
        spool_to_disk(topic, key, value)   # never silently dropped
```

---

## Semantics — read this part

**Ordering is per key, not per topic.** Same key → same partition → same log →
records come back in exactly the order they were sent. Across partitions there
is *no* order at all. If you need total ordering, create the topic with **one**
partition and accept one consumer's worth of throughput.

**Delivery is at-least-once.** If a record is written but the response is lost,
the retry writes it again. There are no producer ids or sequence numbers, so
there is no deduplication. **Make your consumers idempotent**, or key by
something you can deduplicate on.

**You track your own offsets.** There is no committed-offset storage yet
(that's phase C). A consumer that restarts resumes wherever you tell it to:

```python
last = load_my_offset()               # your database, your file, your problem
for record in consumer.consume("orders", partition, offset=last + 1, follow=True):
    handle(record)
    save_my_offset(record.offset)     # after handling, for at-least-once
```

Saving *before* handling gives at-most-once instead. Neither is exactly-once;
that needs transactions we do not have.

**Metadata is cached.** Fetched on first use per topic, refetched only when a
broker says the routing moved. If you resize a cluster, long-lived clients
pick it up on their next redirect, not immediately.

**Connections are pooled** — one gRPC channel per broker, reused. Channels are
designed to be long-lived; `close()` (or the context manager) releases them.

---

## What is not here yet

- **consumer groups** — no automatic partition assignment or rebalancing.
  Assign partitions yourself, or run one consumer per partition.
- **committed offsets** — see above.
- **compression, transactions, exactly-once, schemas** — not planned.
- **`async` client** — the broker is async; this is not. Wrap calls in
  `asyncio.to_thread` if you need it from an event loop.
