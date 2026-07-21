#!/usr/bin/env python
"""Live consumer. Blocks and prints records as they arrive.

    uv run python examples/consumer.py orders 0

One partition per consumer, deliberately: there is no correct order in which
to merge two partitions, since each numbers its records from zero. Reading a
whole topic means one consumer per partition — which is exactly how consumer
groups parallelise.
"""
import sys

from pyka.client import Consumer, PykaError

BOOTSTRAP = ["localhost:9090", "localhost:9091", "localhost:9092"]


def main() -> None:
    topic = sys.argv[1] if len(sys.argv) > 1 else "orders"
    partition = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    offset = int(sys.argv[3]) if len(sys.argv) > 3 else 0

    with Consumer(BOOTSTRAP) as consumer:
        address = consumer.routing(topic)[partition]
        print(f"following {topic}/{partition} on {address} from offset {offset}")
        print("blocking — Ctrl-C to stop\n")

        try:
            for record in consumer.consume(topic, partition, offset, follow=True):
                key = record.key.decode() if record.key else "<none>"
                value = "<TOMBSTONE>" if record.is_tombstone else record.value.decode()
                print(f"  offset {record.offset:>5}  key {key:<12} {value}", flush=True)
        except KeyboardInterrupt:
            print("\nstopped")
        except PykaError as err:
            # The owning broker went away mid-stream. A real consumer would
            # refresh metadata and resume from record.offset + 1.
            print(f"\nstream ended: {err}")


if __name__ == "__main__":
    main()
