# pyKA

A mini Kafka, built from scratch to learn Python: an append-only log storage
engine first, a gRPC broker on asyncio on top of it second.

Rules of the project: single node, no replication, our own gRPC/protobuf API
(not Kafka's wire protocol). Max UI: a TUI, later.

## Architecture

Four layers. Each one only knows about the layer below it.

See **[docs/architecture.md](docs/architecture.md)** for the call paths —
producing and consuming end to end, with diagrams.

```
┌─────────────────────────────────────────────────────────────┐
│ Layer 3  broker/     gRPC :9092 (data) + FastAPI :8080      │
│                      (control). server / handler / admin /  │
│                      store. The only async code, and the    │
│                      only place with dependencies.          │
├─────────────────────────────────────────────────────────────┤
│ Layer 2  topic/      policy: which Log a record goes to,    │
│                      when to fsync. topic / partitioner /   │
│                      policy. Owns topic names — the point   │
│                      where untrusted input meets the disk.  │
├─────────────────────────────────────────────────────────────┤
│ Layer 1  storage/    mechanism: bytes on disk.              │
│                      record / segment / index / log.        │
│                      Imports nothing from the rest of pyka. │
└─────────────────────────────────────────────────────────────┘

  cluster/ring.py  is NOT a layer above these. It imports nothing and is
  imported BY the broker: a leaf lookup table, (topic, partition) -> broker.
  A cluster is N copies of this same process, not a tier on top of one.
  Ring = p % n (naive, kept for comparison); HashRing = consistent hashing
  with virtual nodes, and the default.
```

### Sync or async?

**Layers 1–2 are plain synchronous code. Layer 3 is asyncio.**

There is no asynchronous file I/O in the stdlib — `read`, `write` and `fsync`
are blocking syscalls, and libraries like `aiofiles` only run them on a thread
pool. For a log the blocking is often the point: `fsync()` not returning until
the write is durable *is* the guarantee.

The seam is `await asyncio.to_thread(log.append, record)`. The event loop stays
free for sockets, where concurrency actually pays.

Consequence, now handled: `to_thread` means several connections can reach the
same `Log` at once, so `Log.append` holds a `threading.RLock` around
claim-offset / choose-segment / write. Reentrant because `append` calls
`roll`. Without it, 4 threads x 50 appends produced 2 usable records and a log
that would not reopen — measured, not theorised.

## Layer 1 — classes

```
┌────────────────────────────────────────────────────────────────────┐
│ Record                          «frozen dataclass, slots=True»     │
├────────────────────────────────────────────────────────────────────┤
│ + offset:    int          logical position in partition (0,1,2…)   │
│ + timestamp: int          epoch milliseconds                       │
│ + key:       bytes | None None = no key (round-robin, no compact)  │
│ + value:     bytes | None None = tombstone (deletion marker)       │
├────────────────────────────────────────────────────────────────────┤
│ + encode() -> bytes                     frame for disk             │
│ + decode(buf, pos=0) -> Record                       @classmethod  │
│ + read_one(f: BinaryIO) -> Record | None   None = clean/torn EOF   │
│ + size() -> int                         total framed byte length   │
└────────────────────────────────────────────────────────────────────┘
         │ raises CorruptRecord(ValueError) when the bytes are wrong
         ▼
┌────────────────────────────────────────────────────────────────────┐
│ Segment                                                            │
├────────────────────────────────────────────────────────────────────┤
│ - base_offset: Offset     first logical offset; also the filename  │
│ - next_offset: Offset     what the next appended record gets       │
│ - position:    Position   current end of file                      │
│ - max_bytes:   int        roll threshold, SOFT (default 1 GiB)     │
├────────────────────────────────────────────────────────────────────┤
│ + append(record) -> Position     byte position it starts at        │
│ + read_from(offset) -> Iterator[Record]      absolute offsets only │
│ + has_room_for(record) -> bool   ask BEFORE appending              │
│ + base_offset / next_offset / size_bytes / sealed / index_entries  │
│ + sync() / close() / __enter__ / __exit__                          │
│ - _recover() -> (Offset, Position)   runs in __init__, truncates   │
└────────────────────────────────────────────────────────────────────┘
                     │ owns one
                     ▼
┌────────────────────────────────────────────────────────────────────┐
│ Index                                       «private to Segment»   │
├────────────────────────────────────────────────────────────────────┤
│ An array of 8-byte entries: (rel_off u32, position u32), big-      │
│ endian, no header. Entry n is at byte n*8 — so lookup is a binary  │
│ search, not a scan. One entry per ~4 KiB of log.                   │
│ A HINT: always verified against the log, never trusted. Derived    │
│ and rebuildable, therefore no crc — repair is rebuild, not raise.  │
├────────────────────────────────────────────────────────────────────┤
│ - base_offset:    Offset  from the filename; rel_off is vs. this   │
│ - interval_bytes: int     sparsity threshold, default 4096         │
├────────────────────────────────────────────────────────────────────┤
│ + maybe_append(offset, position) -> None  no-op until 4 KiB passed │
│ + lookup(offset) -> (Offset, Position)    greatest entry <= offset │
│ + clear() -> None                         before a rebuild scan    │
│ + last_entry() -> (Offset, Position) | None                        │
│ + sync() / close() / __len__                                       │
└────────────────────────────────────────────────────────────────────┘
                     ▲ many Segments (each with its own private Index —
                     │ Log never sees or touches an Index)
┌────────────────────────────────────────────────────────────────────┐
│ Log                                                                │
├────────────────────────────────────────────────────────────────────┤
│ One topic-partition = an ordered chain of Segments in one dir.     │
│ Owns the next logical offset, so callers pass payloads and never   │
│ offsets. Only the tail is open for writing; the rest are closed    │
│ and still serve reads. Raises CorruptLog if the chain has a gap.   │
├────────────────────────────────────────────────────────────────────┤
│ - directory:         Path  Log creates it; Segment never does      │
│ - max_segment_bytes: int   roll threshold, passed down (1 GiB)     │
│ + next_offset -> Offset                  delegated to the tail     │
│ + append(key, value, timestamp=None) -> Offset   None = now, in ms │
│ + read_from(offset) -> Iterator[Record]  crosses segments          │
│ + roll() -> Segment        seal the tail; no-op when it is empty   │
│ + segments -> tuple[Segment, ...]                                  │
│ + sync() / close() / __enter__ / __exit__                          │
└────────────────────────────────────────────────────────────────────┘
```

## Layer 2 — classes

```
┌────────────────────────────────────────────────────────────────────┐
│ Topic                                     «a registry, not one     │
│                                            topic — see decisions»  │
├────────────────────────────────────────────────────────────────────┤
│ root/<name>/<partition>/00000000000000000000.log + .index          │
│ Composes rather than implements: Log does the storing, Partitioner │
│ the routing, SyncPolicy the durability. Owns name validation.      │
├────────────────────────────────────────────────────────────────────┤
│ - root:         Path                                               │
│ - partitions:   int        default for NEW topics only (1)         │
│ - sync_policy:  SyncPolicy shared, frozen; counters live per       │
│                            partition in Topic                      │
├────────────────────────────────────────────────────────────────────┤
│ + create(name, partitions=None) -> int    idempotent, never        │
│                                           re-partitions            │
│ + get(name, partition=0) -> Log           raises UnknownTopic      │
│ + append(name, key, value, timestamp=None) -> (partition, Offset)  │
│ + read_from(name, offset, partition=0) -> Iterator[Record]         │
│ + names() / exists(name) / partitions_of(name)                     │
│ + sync() / close() / __enter__ / __exit__                          │
└────────────────────────────────────────────────────────────────────┘
      │ routes with                    │ asks
      ▼                                ▼
┌──────────────────────────────┐  ┌──────────────────────────────────┐
│ Partitioner                  │  │ SyncPolicy  «frozen dataclass»   │
├──────────────────────────────┤  ├──────────────────────────────────┤
│ Append-side only: a consumer │  │ How much a power cut may lose.   │
│ names its own partition.     │  │ records / millis, both optional, │
│ Stateful only for round-     │  │ OR'd. Pure — the caller keeps    │
│ robin, so one per Topic.     │  │ the counters, so it is shared.   │
├──────────────────────────────┤  ├──────────────────────────────────┤
│ + partition_for(key, n)      │  │ + should_sync(appends, millis)   │
│     key  -> crc32(key) % n   │  │ SYNC_EVERY_RECORD / SYNC_NEVER   │
│     None -> round-robin      │  │                                  │
└──────────────────────────────┘  └──────────────────────────────────┘
```

## Record format

Kafka's v1 message format, minus the fields we have no use for.

```
┌──────────────┬────────────┬───────────┬───────────────┬───────────┬───────────┬─────┬───────┐
│ offset  i64  │ size  u32  │ crc  u32  │ timestamp i64 │ klen  i32 │ vlen  i32 │ key │ value │
└──────────────┴────────────┴───────────┴───────────────┴───────────┴───────────┴─────┴───────┘
  ↑ 8 bytes      ↑ 4          ↑ 4         ↑ 8             ↑ 4         ↑ 4
                 │            └────────────── crc covers from here to the end ──────────────┘
                 └── counts every byte AFTER this field
```

Fixed header is 32 bytes. `size` = `4 + 16 + len(key) + len(value)`, and the
invariant `PREFIX_SIZE + size == Record.size()` must always hold.

### Decisions

- **Kafka v1, not v2.** v2 is batch-oriented: records carry *deltas* from a
  batch header and cannot be decoded standalone. Batching buys throughput and
  exactly-once producer semantics, neither of which we have a broker for yet.
- **`magic` and `attributes` omitted.** A format-version byte only pays off
  when a second format exists, and `attributes` is compression flags we don't
  have. Adding them as always-zero is cargo cult.
- **Fixed-width `int32` lengths, not varints.** Varints save real bytes at
  Kafka's scale and cost a hand-rolled encoder plus a class of parsing bugs.
  Fixed-width is the right call for an implementation we're learning from.
- **`offset` sits outside the length-delimited region**, before `size`. After
  seeking to a byte position from the `Index`, the first thing read is the
  offset — so the seek can be *verified* rather than trusted.
- **`-1` means absent, `0` means present-but-empty**, for both key and value.
  That is why `klen`/`vlen` are signed. A null value is a **tombstone**: the
  record meaning "this key is deleted". Compaction is *not implemented* — the
  format only reserves the ability.
- **CRC-32 (`zlib.crc32`) over timestamp..value.** Torn-tail recovery catches
  *truncation*; it cannot catch *corruption* — bit rot, bad RAM, a partial
  overwrite landing the right number of wrong bytes. Without a checksum,
  silent corruption is indistinguishable from correct data. CRC is not
  cryptographic and does not detect deliberate tampering; that is not the
  threat model.
- **Logical offsets, not byte offsets.** `append()` returns a record number
  (0, 1, 2…), not a file position. Byte positions are an implementation detail
  of `Segment` and are reachable through the `Index`.
- **`None` vs. exception.** `read_one` returning `None` means the stream ended
  — clean EOF or a torn tail after a crash, both *expected*. `CorruptRecord`
  means the bytes are wrong. Recovery treats these completely differently.
- **`MAX_SIZE` (1 MiB) is checked before allocating.** On a file a corrupt
  length is merely wasteful; when the same framing reads from a socket in
  phase B, an unchecked length is a remote OOM with four bytes of input.

## Segment decisions

- **`_recover()` runs inside `__init__`.** A segment that has not recovered
  cannot be appended to safely, so the invalid state is made unconstructible
  rather than left to a method the caller must remember to call.
- **A torn tail is truncated, not merely skipped.** If the garbage stayed, the
  next append would land *after* it and permanently embed an unreadable hole
  in the middle of the file. This is why recovery must precede any append.
- **`read_one` does not rewind.** It leaves the file position mid-record on a
  torn tail; the caller captures `f.tell()` after each success instead. Kafka
  leaves this to the recovery logic too. *(Resolves the earlier open question.)*
- **Offsets are verified for continuity while scanning.** `offset` sits outside
  the crc-covered region, so a bit flip there is invisible to the checksum.
  Offsets within a segment are strictly sequential, so `rec.offset ==
  next_offset` is the check that catches it.
- **Mid-file corruption is fatal.** `CorruptRecord` during recovery makes the
  segment unopenable. Kafka instead truncates at the corruption point and
  loses the tail; we prefer loud over silent data loss. Revisit if it proves
  too brittle.
- **`sync()` flushes before `fsync`.** `os.fsync` pushes OS→disk and cannot see
  Python's buffer. Calling it alone syncs nothing — a durability bug that only
  appears after a power cut.
- **`max_bytes` is a SOFT limit.** An empty segment accepts any record, however
  large; otherwise a record bigger than `max_bytes` would roll forever,
  creating empty segments until the disk fills. A segment can therefore reach
  `max_bytes + (largest record) - 1`. Do not size buffers off `max_bytes`.
- **`Log` asks `has_room_for()` before appending**, rather than `Segment`
  rejecting an oversized write. Keeps `append` a plain write and puts the
  rolling decision in the layer that can act on it.
- **`read_from` takes absolute offsets only.** No relative/absolute flag: one
  `int` with two meanings turns a loud error into a silent wrong answer.
  Relative offsets belong *inside* `Index`'s file format, where storing
  `offset - base_offset` as u32 halves the entry size — never in a public API.
- **`read_from` validates eagerly.** A function containing `yield` executes
  nothing until the first `next()`, so the bounds check lives in a plain
  wrapper that returns a private generator.
- **`Offset` and `Position` are distinct `NewType`s.** Both are `int` at
  runtime; the distinction exists so `read_from(segment.append(r))` — a byte
  position used as a record number — is a type error rather than wrong data.
- **`Segment` does not create its parent directory.** `Log`/`Topic` owns
  directory creation; a segment silently `mkdir -p`-ing puts log files in
  surprising places.
- **A new segment's `base_offset` must equal the next offset**, since `append`
  rejects any record whose offset is not sequential. `Log` rolls with
  `Segment(dir, self._next_offset)`.

## Index decisions

- **Fixed-width 8-byte entries, so the file *is* an array.** No framing, no
  header, no length field: entry *n* lives at byte `n * 8`, and
  `entry_count == file_size // 8`. That arithmetic is what makes `lookup` a
  binary search instead of a scan — an index you had to scan would cost the
  same as scanning the log, i.e. nothing.
- **`(rel_off u32, position u32)`, relative to `base_offset`.** Absolute
  offsets would need `i64` and double the entry to 16 bytes. `base_offset`
  is already recoverable from the filename, so relative is free. The
  encoding stays internal: `lookup()` takes and returns absolute `Offset`s.
- **u32 caps a segment at 4 GiB and ~4 billion records.** The entry format
  and `max_bytes` are one decision, not two — which is exactly why Kafka's
  `segment.bytes` defaults to 1 GB. Our default is 1 GiB for the same reason.
- **Big-endian (`>`) like everything else here.** Readable in a hex dump,
  machine-independent, and the `>` prefix also disables struct's alignment
  padding so `calcsize` is exactly the sum of the fields.
- **No crc — the index is a hint, never an authority.** Every `lookup` result
  is verified against the offset field in the `.log` before use, and a
  mismatch falls back to a full scan. The index therefore *cannot* cause a
  wrong answer; the worst it can do is fail to make things fast. This is the
  property that makes corruption a rebuild rather than an error, and it is
  why `offset` sits outside the crc-covered region in the record format.
- **Three self-checks, all arithmetic.** `file_size % 8 != 0` means a torn
  entry (truncate down to the nearest multiple of 8); `rel_off` must be
  strictly increasing; and the hint must verify against the log. None of
  these needs a checksum.
- **`Segment` owns the `Index`; `Log` must not know indexes exist.**
  Ownership follows the invariant: *every entry points at a real record
  boundary in this `.log`*. Only `Segment` writes both files and can scan the
  log to repair them. Any split makes `Log` a co-guarantor of an invariant it
  cannot check without doing `Segment`'s job — and forces the
  verify-and-fall-back logic to straddle the boundary.
- **`Segment`'s public API does not change.** The index is pure internal
  acceleration: `read_from` gets faster, no signature moves. A correctly
  placed optimization is invisible from outside.
- **`Index` owns the sparsity rule, not `Segment`.** `maybe_append` is called
  after every append and decides for itself, using the last position it
  recorded. `Segment` tracking a `_bytes_since_index` counter would leak index
  policy into the class that shouldn't care. The name is honest: most calls
  do nothing.
- **`Index` never imports `Record` or `Segment`.** It rebuilds by being *fed*
  from the scan `_recover()` already runs (`clear()`, then `maybe_append` per
  record) rather than reading the log itself. Keeps it independently testable
  and ignorant of the log format. If `Index` can't be tested without building
  a `Segment`, the delegation has degenerated into coupling.
- **Rebuilt on every open, for now.** `_recover()` already walks the whole
  file, so rebuilding rides along for free. Trusting the index to *seed* the
  scan needs a clean-shutdown marker we don't have yet — that's the
  optimization to measure later, not now.
- **`CorruptRecord` is swallowed only at the hint position.** A hint can lie
  three ways — EOF, a wrong offset, or bytes that don't decode at all — and
  all three fall back to the scan from byte 0, because the log hasn't been
  convicted, the hint has. Once scanning from a *verified* boundary,
  corruption is the log's own and propagates loudly.
- **Sync order: log first, index second.** An index made durable ahead of its
  log can point past the end of the file; a stale index merely rebuilds.
- **Measured (bench/bench_seek.py):** time-to-first-record went from linear —
  5.8/23/93/377 ms at 1/4/16/64 MiB, a clean 4.0x per 4x of file — to a flat
  ~0.03 ms at every size. The index turns O(file) reads into O(interval).

## Log decisions

- **`append` takes a key and a value, not a `Record`.** `Log` owns the offset
  sequence, so it stamps the offset itself: a wrong offset is not rejected,
  it is unconstructible. `Segment.append` still checks, but that check now
  guards `Log`'s bookkeeping rather than the caller's arithmetic.
- **Sealing a segment means *closing* it.** `Segment.close()` drops only the
  write handles — `read_from` opens its own handle per call and `lookup`
  bisects an in-memory list, so a closed segment still serves reads. "Only
  the tail is writable" is therefore physical rather than an `if`: a sealed
  segment has no handle to append with. Open fds stay at ~2 for the whole
  log instead of 2 per segment.
- **The chain is checked for continuity on open, and a gap is fatal.**
  `segments[i+1].base_offset == segments[i].next_offset`, else `CorruptLog`.
  A hole means records are silently missing from every read; loud over
  silent, the same verdict as mid-file corruption.
- **Deleting a segment has three different outcomes**, and only one is an
  error. From the *middle* → `CorruptLog`, because the next file disagrees.
  From the *front* → opens fine and shorter: still continuous, and
  indistinguishable from retention, which will do exactly this on purpose.
  From the *back* → opens fine and **reissues the lost offsets**, because
  there is no file after it to disagree with. That last one is undetectable
  without a recovery checkpoint we don't have; it is pinned by a test rather
  than defended.
- **Rolling builds the record first, then asks.** `has_room_for` needs the
  record's size, and the roll target is `Segment(dir, offset)` — so the new
  base equals the next offset by construction, and the chain invariant is
  maintained by the same line that rolls. An empty segment accepts anything,
  so a roll can never happen twice for one record.
- **`next_offset` is delegated to the tail, not duplicated.** Two copies of
  one truth drift; the tail segment already maintains it.
- **`read_from` past the end yields nothing; before the start raises.** A
  consumer polling at the head is normal, not an error. Asking for offsets
  that never existed here is a caller bug — and once retention lands, the
  same check will mean "already deleted".
- **Reads are a floor-search, exactly like `Index.lookup`.** The segment that
  can hold an offset is the last one whose base is `<=` it; from there on,
  each later segment is read from *its own* base. Two levels of the same
  find-the-floor-then-scan-forward shape.
- **One `Log` is one topic-partition.** Partitioning by key hash belongs to
  layer 2 (`topic/partitioner.py`), never here: splitting one partition
  across files would destroy the total order that offsets, recovery and the
  index all depend on, and would turn sequential writes into scattered ones.
  Parallelism comes from many independent logs, not from striping one.

## Topic decisions

- **`Topic` is a registry of topics, not one topic.** The name is inherited
  from the roadmap; Kafka calls the equivalent `LogManager`. Worth knowing
  when reading `topic.log(name)`-shaped code.
- **Names are validated with a whitelist, and this is a security boundary.**
  In phase B these arrive from a socket, so `../../etc` must not escape the
  data root. Allowed: letters, digits, `.`, `_`, `-`, up to 200 chars. `.`
  and `..` pass the charset check but *are* traversal, so they are rejected
  by name — the same two-step check Kafka does.
- **Appends auto-create; reads raise `UnknownTopic`.** A producer writing to
  a new name is how topics come into existence (Kafka's
  `auto.create.topics.enable`). A consumer naming a topic that isn't there
  has almost certainly typo'd, and silently returning zero records from a
  freshly conjured empty topic would never tell it so.
- **`append` returns `(partition, offset)`.** Every partition numbers its
  records from zero, so an offset without its partition is meaningless.
- **Partition count belongs to the topic on disk, not to the registry.**
  Reopening a 4-partition topic finds 4 whatever `Topic(partitions=…)` says,
  and `create` never re-partitions an existing topic: changing the count
  moves every key to a different partition and silently breaks the ordering
  the partitioner exists to provide.
- **Partitions nest (`root/<name>/<partition>/`)** rather than Kafka's flat
  `orders-0/`, so listing topic names is an `iterdir` instead of parsing and
  de-duplicating suffixes.
- **`names()` reads the disk, not the cache.** A topic exists because its
  directory does; otherwise a restart reports none until something touches
  them.
- **`crc32(key) % n`, never `hash(key)`.** Python randomizes `hash()` for
  `str`/`bytes` per process (`PYTHONHASHSEED`), so a restarted broker would
  reroute every key and split its history across two logs. A partitioner
  must be stable across processes and hosts; Kafka uses murmur2 for the same
  reason. Pinned by a test that runs three subprocesses under different
  seeds.
- **Keys route by hash, null keys round-robin.** Same key → same partition →
  same log → relative order preserved. That is *why* Kafka's ordering
  guarantee is per-partition, and why compaction needs a key to compact by.
  Keyless records have no ordering relationship with anything.
- **The partitioner is called on append only.** A consumer is *assigned* a
  partition and asks for it by number, so reads never route. Merging
  partitions is not offered: there is no correct order to merge them into.
- **`SyncPolicy` is frozen and pure; the counters live in `Topic`.** One
  policy instance is therefore shared by every partition of every topic.
  Layer 2 decides *when* to fsync, layer 1 provides the mechanism
  (`Log.sync()`) and never chooses.
- **`SYNC_NEVER` is the default, and that is a real tradeoff.** Kafka
  defaults the same way, but it can afford to: there, a record is durable
  once *replicated*. We are single-node, so this default genuinely means a
  power cut can lose the tail. Phase B's broker is where a deliberate choice
  belongs.
- **The time threshold is only checked on append.** An idle log never syncs
  itself, because there is no background thread. Kafka runs a scheduler for
  exactly this; ours would need one before `millis` means what it says.

## Roadmap

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

## Layout

```
src/pyka/
  storage/   layer 1 — record.py  index.py  segment.py  log.py
  topic/     layer 2 — topic.py  partitioner.py  policy.py
  broker/    layer 3 — server.py  handler.py  admin.py  store.py
  cluster/   leaf    — ring.py     imported BY broker, depends on nothing
  v1/        generated protobuf/gRPC stubs — do not edit, see scripts/
  cli/       entry points — broker.py  (the server's own bootstrap)
proto/
  pyka/v1/broker.proto   the wire contract, hand-written
docs/
  architecture.md        call paths and topology, with diagrams
tests/
  storage/  topic/  cluster/  broker/
bench/
  bench_seek.py   time-to-first-record vs log size; results/ holds both runs
```

`src/` layout on purpose: it stops an import resolving against the working
directory instead of the installed package.

## Running it

```sh
uv sync

PYKA_DATA_DIR=/tmp/pyka PYKA_PARTITIONS=3 PYKA_SEGMENT_BYTES=60000 uv run pyka-broker
```

Two ports: **gRPC on 9092** (produce/consume) and **REST on 8080** (topics,
segments, probes). Interactive docs at <http://localhost:8080/docs>.

`scripts/demo.py` is a hand client for poking it:

```sh
./scripts/demo.py create  orders 3          # REST
./scripts/demo.py produce orders user-1 hi  # gRPC
./scripts/demo.py bulk    orders 500        # gRPC streaming
./scripts/demo.py consume orders 0 0 20     # topic partition offset limit
./scripts/demo.py tail    orders 0          # live: blocks, prints as records arrive
./scripts/demo.py topics
./scripts/demo.py show    orders 2          # the segment chain
```

`show` is the one worth watching — it prints the storage layer directly:

```
partition 2: next_offset=309 total=80,101 bytes
  base        0        59,827 bytes     14 index entries  SEALED
  base      231        20,274 bytes      4 index entries  active
```

Produce past `PYKA_SEGMENT_BYTES` and a segment seals and a new one appears.
Kill the broker (`Ctrl-C`) and restart it on the same `PYKA_DATA_DIR`: the log
recovers, offsets continue, nothing is lost — which is the whole point, and
the same thing that will happen on `kubectl delete pod`.

Or with plain `curl` / `grpcurl` (reflection is enabled, so no `.proto` needed):

```sh
curl -s localhost:8080/topics
curl -s localhost:8080/topics/orders/partitions/0 | python3 -m json.tool
grpcurl -plaintext localhost:9092 list
```

### Running a cluster

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

### What happens when a broker dies, or the cluster resizes

| event | what happens | safe? |
|---|---|---|
| **pod restarts** (`kubectl delete pod`) | same ordinal, same PVC, same ring → ownership unchanged. Its partitions are unavailable while it recovers, then return intact. | ✅ |
| **rolling update** | the above, one pod at a time | ✅ |
| **broker down** | *only its* partitions are unreachable. Nothing is lost; nothing is served in its place, because there is no replication. | ⚠️ partial outage |
| **scale up / down** | ownership moves, but **data does not**. Segments stay on the old broker while the new owner starts an empty log at offset 0. | 🔴 **needs migration** |

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

#### Resizing a cluster (offline)

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

### Environment

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

## Dev

```sh
uv sync                    # create the venv
uv run pytest              # 451 tests
./scripts/gen_proto.sh     # regenerate stubs after editing the .proto
uv run python bench/bench_seek.py
```