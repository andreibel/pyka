# pyKA

A mini Kafka, built from scratch to learn Python: an append-only log storage
engine first, an asyncio TCP broker on top of it second.

Rules of the project: single node, no replication, one log per topic, our own
simple JSON-lines protocol (not Kafka's wire protocol). Max UI: a TUI, later.

## Roadmap

### Phase A — the log (files only, no network)
- [ ] A1: `Log` class — append length-prefixed records to a file, iterate them back
- [ ] A2: offsets — `append()` returns a byte offset; `read_from(offset)` seeks and streams
- [ ] A3: durability — crash-safe flushing; replay a half-written tail without dying
- [ ] A4: `Topic` — one directory, one log per topic name

### Phase B — the broker (asyncio TCP)
- [ ] B1: server accepts connections, speaks JSON-lines
- [ ] B2: `produce` command appends to a topic's log
- [ ] B3: `consume` command streams records from an offset
- [ ] B4: live tail — consumers get new records as they arrive

### Phase C — consumer offsets
- [ ] C1: broker tracks each consumer group's committed offset
- [ ] C2: offsets survive a broker restart (stored in a log, naturally)

### Stretch
- partitions · retention/compaction · Textual TUI topic browser

## Record format (phase A)

```
┌─────────────┬──────────────────┐
│ length: u32 │ payload: <bytes> │   repeated until EOF
└─────────────┴──────────────────┘
```

## Dev

```sh
uv sync          # create .venv with pytest
uv run pytest    # run the tests
```
