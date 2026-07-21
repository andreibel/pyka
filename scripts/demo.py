#!/usr/bin/env python
"""A client for poking a running broker by hand.

    ./scripts/demo.py produce orders user-1 "hello world"
    ./scripts/demo.py bulk    orders 500
    ./scripts/demo.py consume orders 0 0 20      # topic partition offset limit
    ./scripts/demo.py topics
    ./scripts/demo.py show    orders 0

The broker must be running (see README "Running it"). This talks gRPC on 9092
for data and REST on 8080 for control — the same split any real client uses.
"""
import json
import sys
import urllib.request

import grpc

from pyka.v1 import broker_pb2, broker_pb2_grpc

GRPC = "localhost:9092"
REST = "http://localhost:8080"


def _rest(path: str, method: str = "GET", body: dict | None = None):
    request = urllib.request.Request(
        REST + path,
        method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"content-type": "application/json"} if body else {},
    )
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read())


def produce(topic: str, key: str, value: str) -> None:
    with grpc.insecure_channel(GRPC) as channel:
        response = broker_pb2_grpc.BrokerStub(channel).Produce(
            broker_pb2.ProduceRequest(
                topic=topic,
                key=key.encode() if key != "-" else None,  # "-" = no key
                value=value.encode(),
            )
        )
    print(f"partition {response.partition}  offset {response.offset}")


def bulk(topic: str, count: str) -> None:
    """Stream many records down one call — this is what fills segments."""
    total = int(count)

    def requests():
        for n in range(total):
            yield broker_pb2.ProduceRequest(
                topic=topic,
                key=f"user-{n % 50}".encode(),
                value=f"record number {n:06d} ".encode() + b"x" * 200,
            )

    counts: dict[int, int] = {}
    with grpc.insecure_channel(GRPC) as channel:
        for response in broker_pb2_grpc.BrokerStub(channel).ProduceStream(requests()):
            counts[response.partition] = counts.get(response.partition, 0) + 1
    print(f"produced {total} records")
    for partition in sorted(counts):
        print(f"  partition {partition}: {counts[partition]}")


def consume(topic: str, partition: str = "0", offset: str = "0",
            limit: str = "10") -> None:
    """Server-streaming read: records arrive one at a time, not as a blob."""
    with grpc.insecure_channel(GRPC) as channel:
        stream = broker_pb2_grpc.BrokerStub(channel).Consume(
            broker_pb2.ConsumeRequest(
                topic=topic,
                partition=int(partition),
                offset=int(offset),
                max_records=int(limit),
            )
        )
        for record in stream:
            key = record.key.decode() if record.HasField("key") else "<none>"
            if not record.HasField("value"):
                value = "<TOMBSTONE>"
            else:
                value = record.value.decode(errors="replace")[:40]
            print(f"  offset {record.offset:>6}  key {key:<12} {value}")


def topics() -> None:
    print(json.dumps(_rest("/topics"), indent=2))


def create(topic: str, partitions: str = "1") -> None:
    print(json.dumps(_rest("/topics", "POST",
                           {"name": topic, "partitions": int(partitions)}), indent=2))


def show(topic: str, partition: str = "0") -> None:
    """The segment chain — watch it grow as you produce."""
    info = _rest(f"/topics/{topic}/partitions/{partition}")
    print(f"partition {info['partition']}: next_offset={info['next_offset']} "
          f"total={info['size_bytes']:,} bytes")
    for segment in info["segments"]:
        state = "SEALED" if segment["sealed"] else "active"
        print(f"  base {segment['base_offset']:>8}  {segment['size_bytes']:>12,} bytes"
              f"  {segment['index_entries']:>5} index entries  {state}")


COMMANDS = {"produce": produce, "bulk": bulk, "consume": consume,
            "topics": topics, "create": create, "show": show}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__)
        sys.exit(1)
    COMMANDS[sys.argv[1]](*sys.argv[2:])
