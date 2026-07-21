"""Partitioner: key -> partition index (hash % n, or round-robin)."""
import zlib


class Partitioner:
    """Chooses which partition a record goes to. Called on append only.

    Reads never need this: a consumer is *assigned* a partition and asks for
    it by number. That asymmetry is why partitioning touches one code path.

    Stateful only for the round-robin counter, so: one instance per Topic.
    """

    def __init__(self) -> None:
        self._next = 0

    def partition_for(self, key: bytes | None, partitions: int) -> int:
        """Partition index within ``range(partitions)``.

        A key always lands in the same partition, forever — that is the whole
        contract. Records sharing a key therefore share one log and keep their
        relative order, which is why Kafka's ordering guarantee is *per
        partition*, and why compaction needs a key to compact by.

        No key means no home: round-robin, spreading load evenly. Such records
        have no ordering relationship with anything and cannot be compacted.
        """
        if partitions < 1:
            raise ValueError(f"partitions must be >= 1, got {partitions}")

        if key is None:
            partition = self._next % partitions
            self._next += 1
            return partition

        # zlib.crc32, NOT hash(): Python randomizes hash() for str and bytes
        # per process (PYTHONHASHSEED), so a restart would reroute every key
        # and silently break per-key ordering — the one thing this function
        # promises. A partitioner must be stable across processes and hosts.
        # Kafka uses murmur2 for the same reason; crc32 is already imported
        # elsewhere here and is not being used as a checksum.
        return zlib.crc32(key) % partitions
