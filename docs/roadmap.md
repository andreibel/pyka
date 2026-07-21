# Roadmap

What is built, what is next, and what is deliberately out of scope.

### Phase A — the log (files only, no network)
- [x] A1: `Record` — framing, crc, encode/decode/read_one, torn-tail handling
- [x] A2: `Segment` — one `.log` file per base offset, roll at a size limit
- [x] A3: `Index` — sparse offset→position map, rebuildable
- [x] A4: `Log` — many segments, logical offsets, recovery on open
- [x] A5: `Topic` — a directory of logs, one per topic name

### Phase B — the broker (gRPC on asyncio)
- [x] B1: `.proto` defined, stubs generated, `grpc.aio` server with health
- [x] B1b: FastAPI control plane on :8080, same process, shared `Topic`
- [x] B2: `Produce` + `ProduceStream` append to a topic's log
- [x] B3: `Consume` server-streams records from an offset
- [x] B4: live tail — `follow=true` keeps the stream open (`asyncio.Event` per partition)

### Phase D — Kubernetes (the deployment is the lesson)
- [ ] D1: Dockerfile, non-root, `PYKA_DATA_DIR` as a volume
- [ ] D2: StatefulSet `replicas: 1` + headless Service + PVC + gRPC probes
- [ ] D3: `kubectl delete pod` → watch recovery rebuild state from the PVC
- [x] D4: `Metadata` RPC + client-side partitioning + ownership redirects
- [x] D4b: consistent hashing (`HashRing`), orphaned-partition detection
- [ ] D5: `replicas: 3` in Kubernetes — then **kill a pod and watch a partition go dark**

### Phase C — consumer offsets
- [ ] C1: broker tracks each consumer group's committed offset
- [ ] C2: offsets survive a restart — stored in a log, keyed `group/topic/partition`,
      which is exactly what tombstones and compaction were reserved for in A1

### Stretch — pick by appetite, none are required
- **retention** — delete sealed segments past an age/size bound. Small, and the
  reason `roll()` exists. Also the answer to "the PVC filled up".
- **replication** — leader/follower, ISR, high-water-mark, election. The big one:
  larger than everything above combined, and the only thing that buys
  *availability*. A follower is just a consumer, so B3 is its prerequisite.
- consistent hashing (only if `p % N` rebalancing actually hurts) · compaction ·
  Prometheus metrics on :8080 · Textual TUI

> **Sharding vs replication.** Everything built so far is *sharding*: a
> partition exists exactly once, and `Ring` says where. That buys capacity, not
> availability — a broker being down makes its partitions unreachable.
> *Replication* (copies of each partition) is what buys availability, and it is
> also what lets Kafka be durable without fsync: `acks=all` means the record is
> in three machines' page cache, which is both cheaper and stronger than one
> local fsync. That is why our `SYNC_NEVER` default is copied from Kafka and
> means something different here — same default, no replication behind it.

