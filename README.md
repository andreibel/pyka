# pyKA

A mini Kafka, built from scratch to learn Python: an append-only log storage
engine first, an asyncio TCP broker on top of it second.

Rules of the project: single node, no replication, our own simple JSON-lines
protocol (not Kafka's wire protocol). Max UI: a TUI, later.

## Architecture

Four layers. Each one only knows about the layer below it.

```
┌─────────────────────────────────────────────────────────────┐
│ Layer 4  cluster/    ring.py — consistent hashing,          │
│                      partition -> node. NOT IMPLEMENTED,    │
│                      placeholder for the stretch phase.     │
├─────────────────────────────────────────────────────────────┤
│ Layer 3  broker/     asyncio TCP. server / protocol /       │
│                      handler / store. The only async code.  │
├─────────────────────────────────────────────────────────────┤
│ Layer 2  topic/      policy: which Log a record goes to,    │
│                      when to fsync. topic / partitioner /   │
│                      policy.                                │
├─────────────────────────────────────────────────────────────┤
│ Layer 1  storage/    mechanism: bytes on disk.              │
│                      record / segment / index / log.        │
│                      Imports nothing from the rest of pyka. │
└─────────────────────────────────────────────────────────────┘
```

### Sync or async?

**Layers 1–2 are plain synchronous code. Layer 3 is asyncio.**

There is no asynchronous file I/O in the stdlib — `read`, `write` and `fsync`
are blocking syscalls, and libraries like `aiofiles` only run them on a thread
pool. For a log the blocking is often the point: `fsync()` not returning until
the write is durable *is* the guarantee.

The seam is `await asyncio.to_thread(log.append, record)`. The event loop stays
free for sockets, where concurrency actually pays.

Consequence to remember: `to_thread` means several connections can reach the
same `Log` at once, so `Log.append` will need a `threading.Lock` around
seek/write/offset-increment before B2 lands.

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
│ Segment                                              NOT WRITTEN   │
├────────────────────────────────────────────────────────────────────┤
│ + base_offset: int        first logical offset in this segment     │
│ + path:        Path       <base_offset:020d>.log + .index          │
├────────────────────────────────────────────────────────────────────┤
│ + append(record) -> int                 byte position written at   │
│ + read_from(offset) -> Iterator[Record]                            │
│ + is_full(limit) -> bool                time to roll?              │
│ + recover() -> int                      truncate a torn tail       │
└────────────────────────────────────────────────────────────────────┘
                     │ owns one
                     ▼
┌────────────────────────────────────────────────────────────────────┐
│ Index                                                NOT WRITTEN   │
├────────────────────────────────────────────────────────────────────┤
│ sparse map: logical offset -> byte position. One entry per ~4KB.   │
│ Derived and rebuildable — NEVER the source of truth.               │
├────────────────────────────────────────────────────────────────────┤
│ + append(offset, position) -> None                                 │
│ + lookup(offset) -> int      greatest entry <= offset; scan onward  │
│ + rebuild(segment) -> None              after an unclean shutdown  │
└────────────────────────────────────────────────────────────────────┘
                     ▲ many
                     │
┌────────────────────────────────────────────────────────────────────┐
│ Log                                                  NOT WRITTEN   │
├────────────────────────────────────────────────────────────────────┤
│ One topic-partition = an ordered list of Segments.                 │
│ Owns the next logical offset. Only the last segment is writable.   │
├────────────────────────────────────────────────────────────────────┤
│ + append(key, value) -> int             returns logical offset     │
│ + read_from(offset) -> Iterator[Record]                            │
│ + close() -> None                                                  │
└────────────────────────────────────────────────────────────────────┘
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

### Not decided yet

`read_one` returning `None` after a partial body read leaves the file position
*mid-record*. The caller must capture `f.tell()` before each call to recover.
Whether `read_one` should instead seek back and be non-destructive is open —
Kafka leaves it to the recovery logic.

## Roadmap

### Phase A — the log (files only, no network)
- [x] A1: `Record` — framing, crc, encode/decode/read_one, torn-tail handling
- [ ] A2: `Segment` — one `.log` file per base offset, roll at a size limit
- [ ] A3: `Index` — sparse offset→position map, rebuildable
- [ ] A4: `Log` — many segments, logical offsets, recovery on open
- [ ] A5: `Topic` — a directory of logs, one per topic name

### Phase B — the broker (asyncio TCP)
- [ ] B1: server accepts connections, speaks JSON-lines
- [ ] B2: `produce` command appends to a topic's log
- [ ] B3: `consume` command streams records from an offset
- [ ] B4: live tail — consumers get new records as they arrive

### Phase C — consumer offsets
- [ ] C1: broker tracks each consumer group's committed offset
- [ ] C2: offsets survive a broker restart (stored in a log, naturally)

### Stretch
- partitions · retention/compaction · consistent hashing · Textual TUI

> Partitions are *modelled* in the layout (`Log` = one topic-partition) but not
> implemented. Until the stretch phase there is exactly one log per topic.

## Layout

```
src/pyka/
  storage/   layer 1 — record.py  index.py  segment.py  log.py
  topic/     layer 2 — topic.py  partitioner.py  policy.py
  broker/    layer 3 — server.py  protocol.py  handler.py  store.py
  cluster/   layer 4 — ring.py                    (placeholder)
  cli/       entry points — broker.py
tests/
  storage/  topic/  broker/
```

`src/` layout on purpose: it stops an import resolving against the working
directory instead of the installed package.

## Dev

```sh
uv sync          # create .venv with pytest
uv run pytest    # run the tests
```