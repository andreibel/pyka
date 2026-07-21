# Examples

Two terminals, a producer and a consumer, watching records flow live.

## Start a cluster

Either three local processes:

```sh
./scripts/cluster.sh start 3
```

or three containers:

```sh
docker compose up -d --build
```

Both give you brokers on `localhost:9090`, `9091`, `9092`.

## Terminal 1 — the consumer

```sh
uv run python examples/consumer.py orders 0
```

It blocks. That is the live tail: the stream stays open and prints records as
they are appended, rather than ending at the last one.

## Terminal 2 — the producer

```sh
uv run python examples/producer.py orders
```

Type a message, press enter, watch it appear in terminal 1. Type `key=value`
to set a key — records with the same key always land in the same partition, so
they keep their order relative to each other.

If the consumer is watching partition 0 and your key routes elsewhere, nothing
appears — that is not a bug, it is the partitioning. The producer prints where
each record went, so run a consumer on that partition too.

## Things worth trying

```sh
# where does everything live?
./scripts/demo.py map orders

# kill the broker your consumer is reading from
./scripts/cluster.sh kill 1

# the consumer stops; produce anyway and watch the producer buffer and retry
./scripts/cluster.sh restart 1

# the segments on disk, as they fill and roll
./scripts/demo.py show orders 0
```
