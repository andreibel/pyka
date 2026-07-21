# Operations

Running pyKA beyond a single process: clusters, failure, resizing, and
every configuration knob.

---

## Running a cluster

```sh
./scripts/cluster.sh start 3      # broker N on gRPC 909N, admin 808N
./scripts/demo.py create orders 6
./scripts/demo.py layout          # every topic, every broker
./scripts/demo.py map    orders   # who holds what
./scripts/demo.py wrong  orders 1 # ask the wrong broker on purpose
./scripts/cluster.sh kill 1       # watch its partitions go dark
./scripts/cluster.sh clean
```

`scripts/demo.py` is a miniature client library: it fetches metadata, computes
the partition **itself** (with the same `Partitioner` the broker uses), and
connects to the owner. That is not optional once there is more than one
broker — you cannot know which broker to open a socket to until you know the
partition.

## What happens when a broker dies, or the cluster resizes

| event | what happens | safe? |
|---|---|---|
| **pod restarts** (`kubectl delete pod`) | same ordinal, same PVC, same ring → ownership unchanged. Its partitions are unavailable while it recovers, then return intact. | safe |
| **rolling update** | the above, one pod at a time | safe |
| **broker down** | *only its* partitions are unreachable. Nothing is lost, and nothing is served in its place — see below. | partial outage |
| **broker's disk destroyed** | those partitions are **gone permanently**. No replication means no second copy. | **data loss** |
| **scale up / down** | ownership moves, but **data does not**. Segments stay on the old broker while the new owner starts an empty log at offset 0. | **needs migration** |

**`PYKA_BROKERS` is effectively immutable while the cluster holds data.**
Changing it without moving files is the worst failure this system has: not an
error, but two logs for one partition, neither aware of the other.

So a broker that finds partition directories it no longer owns **refuses
readiness**: the process stays up (exec in and look), Kubernetes routes it no
traffic, and `kubectl get pods` shows `0/1`. Loud stop over silent divergence.

```
ERROR pyka.broker.store: REFUSING TO SERVE: orders: partitions [1]. The broker
count changed under existing data, so these partitions are orphaned — their
segments are here but their owner is elsewhere.
```

### While a partition's owner is down

Produce and consume for **its** partitions fail with `UNAVAILABLE`; everything
else keeps working. The data is not lost, only unreachable, and it comes back
intact when the broker does.

**No other broker may take those writes.** It would create a second log for the
same partition, starting at offset 0, while the real one is somewhere else at
offset 7 — two logs, both claiming the same offsets, holding different records,
with no correct way to merge them. That is why the ownership check answers
`FAILED_PRECONDITION` even during an outage: refusing the write *is* the
feature.

So the buffering belongs in the **producer**, never on another broker. That is
what a Kafka producer does (`retries`, `delivery.timeout.ms`, an in-memory
accumulator) and what `./scripts/demo.py resilient` demonstrates:

```
$ ./scripts/cluster.sh kill 2
$ ./scripts/demo.py resilient orders 40 40
  delivered 17/40; 23 buffered, waiting on broker(s) [2] — retrying in 0.2s
  delivered 17/40; 23 buffered, waiting on broker(s) [2] — retrying in 4.0s
restarting broker 2
  delivered all 40 records in 7.9s
```

Consumers need nothing special: they reconnect and resume from their last
offset, because offsets are durable on disk.

**Only replication removes the outage** — a follower already holds the records,
so a leader election continues service in seconds. Nothing else gets you there.

### Resizing a cluster (offline)

1. stop every broker
2. for each orphaned partition, move `.../<topic>/<partition>/` to the
   directory of its new owner — ask any broker's `Metadata`, or compute
   `HashRing(brokers=NEW, me=0).broker_for(topic, partition)`
3. start with the new `PYKA_BROKERS`

`PYKA_ALLOW_ORPHANS=1` starts anyway and accepts the split — an escape hatch,
not a fix.

**What would make this online is replication**: copy the partition to its new
owner *before* flipping ownership. That is Kafka's partition reassignment, and
it is the large project this design deliberately defers.

## Environment

| variable | default | |
|---|---|---|
| `PYKA_DATA_DIR` | `/var/lib/pyka` | a mounted volume in Kubernetes |
| `PYKA_PARTITIONS` | `1` | for **new** topics only |
| `PYKA_SEGMENT_BYTES` | 1 GiB | max 4 GiB — the index's u32 position |
| `PYKA_SYNC_RECORDS` / `PYKA_SYNC_MILLIS` | unset | unset = never fsync explicitly |
| `PYKA_PORT` / `PYKA_ADMIN_PORT` | 9092 / 8080 | |
| `PYKA_BROKERS` | 1 | cluster size; ordinal from `$HOSTNAME`. **Immutable while data exists** |
| `PYKA_ADDRESS_TEMPLATE` | `pyka-{ordinal}.pyka-hl:9092` | what Metadata hands clients |
| `PYKA_RING` | consistent | `modulo` for the naive `p % n` |
| `PYKA_ALLOW_ORPHANS` | unset | `1` to serve despite misplaced partitions |
| `PYKA_GRACE` | 30 | shutdown drain seconds |

