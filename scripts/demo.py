#!/usr/bin/env python
"""A cluster-aware client for poking pyKA by hand.

    ./scripts/cluster.sh start 3

    ./scripts/demo.py create  orders 4       # on every broker
    ./scripts/demo.py map     orders         # who holds what
    ./scripts/demo.py layout                 # every topic, every broker
    ./scripts/demo.py produce orders user-1 "hello"
    ./scripts/demo.py bulk    orders 200
    ./scripts/demo.py consume orders 0 0 10  # topic partition offset limit
    ./scripts/demo.py tail    orders 1       # live; blocks
    ./scripts/demo.py show    orders 0       # segments on disk
    ./scripts/demo.py wrong   orders 1       # ask the WRONG broker on purpose
    ./scripts/demo.py resilient orders 60    # keeps producing while you kill a broker

This is a miniature client library. Note what it does that a single-broker
client never had to: fetch metadata, compute the partition itself, and open a
connection to the broker that owns it. Partitioning has to happen here,
because you cannot know which broker to talk to until you know the partition.
"""
import json
import sys
import time
import urllib.error
import urllib.request

import grpc

# The SAME function the broker uses. It has to be: if client and server
# disagreed about where a key belongs, the client would send records to a
# broker that would reject them — and a restart with a different hash would
# split one key's history across two partitions.
from pyka.topic.partitioner import Partitioner
from pyka.v1 import broker_pb2 as pb
from pyka.v1 import broker_pb2_grpc as pbg

BOOTSTRAP = "localhost:9090"  # any broker will do — that is the point


def _grpc(broker: int) -> pbg.BrokerStub:
    return pbg.BrokerStub(grpc.insecure_channel(f"localhost:909{broker}"))


def _admin(broker: int, path: str, method: str = "GET", body: dict | None = None):
    request = urllib.request.Request(
        f"http://localhost:808{broker}{path}",
        method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"content-type": "application/json"} if body else {},
    )
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read())


def _broker_count() -> int:
    """Ask any broker how big the cluster is. Works even with no topics."""
    return _grpc(0).Metadata(pb.MetadataRequest()).broker_count


def _routing(topic: str) -> dict[int, int]:
    """partition -> broker, straight from a Metadata call.

    Every broker returns the same table, so it does not matter which one is
    asked. This is the entire cluster protocol.
    """
    response = _grpc(0).Metadata(pb.MetadataRequest(topics=[topic]))
    return {p.partition: p.broker for p in response.topics[0].partitions}


# ---------------------------------------------------------------- commands


def create(topic: str, partitions: str = "1") -> None:
    """Create on EVERY broker.

    Needed because nothing propagates a create: with no controller, a broker
    only knows about topics it has been told about. A real cluster has a
    controller precisely so this loop is not the client's job.
    """
    for broker in range(_broker_count()):
        info = _admin(broker, "/topics", "POST",
                      {"name": topic, "partitions": int(partitions)})
        print(f"  broker {broker}: {info}")


def map(topic: str) -> None:  # noqa: A001 — a demo command, not a builtin
    """Who holds what, and where the records physically are."""
    routing = _routing(topic)
    count = _broker_count()

    print(f"{topic}: {len(routing)} partitions across {count} brokers")
    for partition, broker in sorted(routing.items()):
        print(f"  partition {partition} -> broker {broker} (localhost:909{broker})")

    print()
    for broker in range(count):
        mine = [p for p, b in sorted(routing.items()) if b == broker]
        held = f"partitions {mine}" if mine else "NOTHING from this topic"
        print(f"  broker {broker}: {held}")


def layout() -> None:
    """Every topic on every broker — the whole cluster at a glance."""
    count = _broker_count()
    names: set[str] = set()
    for broker in range(count):
        names.update(_admin(broker, "/topics"))

    print(f"{len(names)} topic(s), {count} brokers\n")
    header = f"{'topic':<12} {'parts':>5}  " + "".join(
        f"{'broker ' + str(b):<18}" for b in range(count)
    )
    print(header)
    print("-" * len(header))
    for name in sorted(names):
        routing = _routing(name)
        row = f"{name:<12} {len(routing):>5}  "
        for broker in range(count):
            mine = [p for p, b in sorted(routing.items()) if b == broker]
            row += f"{str(mine) if mine else '-':<18}"
        print(row)


def produce(topic: str, key: str, value: str) -> None:
    """Route on the CLIENT, then send to the owner."""
    routing = _routing(topic)
    key_bytes = key.encode() if key != "-" else None
    partition = Partitioner().partition_for(key_bytes, len(routing))
    broker = routing[partition]

    response = _grpc(broker).Produce(
        pb.ProduceRequest(topic=topic, key=key_bytes, value=value.encode(),
                          partition=partition)
    )
    print(f"  key {key!r} -> partition {response.partition} -> broker {broker}"
          f" -> offset {response.offset}")


def bulk(topic: str, count: str) -> None:
    """Many records, each to its owner. One stream per broker."""
    routing = _routing(topic)
    partitioner = Partitioner()

    batches: dict[int, list] = {}
    for n in range(int(count)):
        key = f"user-{n % 50}".encode()
        partition = partitioner.partition_for(key, len(routing))
        batches.setdefault(routing[partition], []).append(
            pb.ProduceRequest(topic=topic, key=key, partition=partition,
                              value=f"record {n:05d} ".encode() + b"x" * 180)
        )

    for broker, requests in sorted(batches.items()):
        sent = sum(1 for _ in _grpc(broker).ProduceStream(iter(requests)))
        print(f"  broker {broker}: {sent} records")


def consume(topic: str, partition: str = "0", offset: str = "0",
            limit: str = "10") -> None:
    partition = int(partition)
    broker = _routing(topic)[partition]
    print(f"  reading partition {partition} from broker {broker}")
    stream = _grpc(broker).Consume(
        pb.ConsumeRequest(topic=topic, partition=partition,
                          offset=int(offset), max_records=int(limit))
    )
    for record in stream:
        _print_record(record)


def tail(topic: str, partition: str = "0", offset: str = "0") -> None:
    """Live: catch up, then block until new records arrive. Ctrl-C to stop."""
    partition = int(partition)
    broker = _routing(topic)[partition]
    print(f"following {topic}/{partition} on broker {broker} — Ctrl-C to stop")
    stream = _grpc(broker).Consume(
        pb.ConsumeRequest(topic=topic, partition=partition,
                          offset=int(offset), follow=True)
    )
    try:
        for record in stream:
            _print_record(record)
    except KeyboardInterrupt:
        print("\nstopped")


def wrong(topic: str, partition: str = "0") -> None:
    """Deliberately ask a broker that does not own this partition.

    The answer is a redirect, not a write — which is what keeps the same
    partition from existing twice with conflicting offsets.
    """
    partition = int(partition)
    owner = _routing(topic)[partition]
    victim = (owner + 1) % _broker_count()
    print(f"  partition {partition} belongs to broker {owner};"
          f" asking broker {victim} anyway")
    try:
        _grpc(victim).Produce(
            pb.ProduceRequest(topic=topic, key=b"k", value=b"v", partition=partition)
        )
        print("  !! accepted — that would be split-brain")
    except grpc.RpcError as err:
        print(f"  {err.code().name}: {err.details()}")


def resilient(topic: str, count: str = "60", deadline: str = "30") -> None:
    """Produce with retry and buffering, the way a real producer does.

    Run this, then `./scripts/cluster.sh kill 2` in another terminal, then
    start broker 2 again. Records for the dead broker's partitions pile up in
    the buffer HERE and are delivered when it returns; everything else keeps
    flowing the whole time.

    The buffer is in the client and nowhere else. A broker holding another
    broker's writes would be split-brain — two logs for one partition, both
    numbering from zero, no correct way to merge them. That is why the broker
    answers FAILED_PRECONDITION instead of being helpful.
    """
    total, limit = int(count), float(deadline)
    partitioner = Partitioner()
    routing = _routing(topic)

    pending = [
        (partitioner.partition_for(f"user-{n}".encode(), len(routing)),
         f"user-{n}".encode(), f"record-{n:04d}".encode())
        for n in range(total)
    ]
    delivered, backoff, started = 0, 0.25, time.monotonic()

    while pending and time.monotonic() - started < limit:
        still_waiting, failures = [], 0
        for partition, key, value in pending:
            broker = routing.get(partition)
            try:
                _grpc(broker).Produce(
                    pb.ProduceRequest(topic=topic, key=key, value=value,
                                      partition=partition),
                    timeout=2,
                )
                delivered += 1
            except grpc.RpcError as err:
                if err.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    # Our routing is stale — the cluster moved. Refetch and
                    # retry rather than guessing.
                    routing = _routing(topic)
                still_waiting.append((partition, key, value))
                failures += 1

        pending = still_waiting
        if pending:
            waiting_on = sorted({routing.get(p) for p, _, _ in pending})
            print(f"  delivered {delivered}/{total}; {len(pending)} buffered, "
                  f"waiting on broker(s) {waiting_on} — retrying in {backoff:.1f}s",
                  flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 4.0)  # exponential, capped

    if pending:
        # A real producer raises here rather than pretending: after the
        # deadline the records are the application's problem again.
        print(f"  GAVE UP after {limit:.0f}s with {len(pending)} undelivered "
              f"— the owner never came back")
    else:
        print(f"  delivered all {total} records in "
              f"{time.monotonic() - started:.1f}s")


def show(topic: str, partition: str = "0") -> None:
    """The segment chain of one partition, on whichever broker holds it."""
    partition = int(partition)
    broker = _routing(topic)[partition]
    info = _admin(broker, f"/topics/{topic}/partitions/{partition}")
    print(f"broker {broker}, partition {partition}: "
          f"next_offset={info['next_offset']} total={info['size_bytes']:,} bytes")
    for segment in info["segments"]:
        state = "SEALED" if segment["sealed"] else "active"
        print(f"  base {segment['base_offset']:>8}  {segment['size_bytes']:>10,} bytes"
              f"  {segment['index_entries']:>4} index entries  {state}")


def _print_record(record) -> None:
    key = record.key.decode() if record.HasField("key") else "<none>"
    value = (record.value.decode(errors="replace")[:40]
             if record.HasField("value") else "<TOMBSTONE>")
    print(f"  offset {record.offset:>6}  key {key:<10} {value}", flush=True)


COMMANDS = {"create": create, "map": map, "layout": layout, "produce": produce,
            "bulk": bulk, "consume": consume, "tail": tail, "wrong": wrong,
            "resilient": resilient, "show": show}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__)
        sys.exit(1)
    try:
        COMMANDS[sys.argv[1]](*sys.argv[2:])
    except grpc.RpcError as err:
        print(f"gRPC {err.code().name}: {err.details()}")
        sys.exit(1)
    except urllib.error.URLError as err:
        print(f"admin API unreachable: {err}\nIs the cluster running? "
              f"./scripts/cluster.sh start 3")
        sys.exit(1)
