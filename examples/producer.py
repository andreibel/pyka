#!/usr/bin/env python
"""Interactive producer. Type a line, press enter, it is a record.

    uv run python examples/producer.py orders

Type `key=value` to set a key. Records sharing a key land in the same
partition and therefore keep their order; keyless records round-robin.
"""
import sys

from pyka.client import DeliveryFailed, Producer

BOOTSTRAP = ["localhost:9090", "localhost:9091", "localhost:9092"]


def main() -> None:
    topic = sys.argv[1] if len(sys.argv) > 1 else "orders"

    with Producer(BOOTSTRAP, delivery_timeout=30) as producer:
        partitions = len(producer.routing(topic))
        print(f"producing to {topic!r} ({partitions} partitions) — Ctrl-D to stop")
        print("type `key=value` to set a key\n")

        while True:
            try:
                line = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                return
            if not line:
                continue

            key, _, value = line.partition("=")
            if not value:
                key, value = None, line

            try:
                partition, offset = producer.send(
                    topic,
                    value.encode(),
                    key=key.encode() if key else None,
                )
                print(f"    -> partition {partition}  offset {offset}")
            except DeliveryFailed as err:
                # The broker that owns this key never came back. The record is
                # handed back rather than dropped.
                print(f"    !! {err}")


if __name__ == "__main__":
    main()
