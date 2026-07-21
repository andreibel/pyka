"""Topic: named logs on disk — validation, routing, and when to fsync."""
import string
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Self

from pyka.storage.log import Log
from pyka.storage.record import Record
from pyka.storage.types import Offset
from pyka.topic.partitioner import Partitioner
from pyka.topic.policy import SYNC_NEVER, SyncPolicy

# Topic names become directory names, so only characters that are safe in a
# path are allowed. "." and ".." pass the character check but ARE traversal,
# so they are rejected by name (Kafka does the same).
_ALLOWED_NAME_CHARS = frozenset(string.ascii_letters + string.digits + "._-")
_MAX_NAME_LENGTH = 200


class UnknownTopic(KeyError):
    """Asked for a topic that does not exist.

    Reads raise this; appends create instead. A consumer naming a topic that
    isn't there has almost certainly typo'd, and silently returning zero
    records from a freshly conjured empty topic would never tell it so.
    """


def validate_name(name: str) -> None:
    """Raise ``ValueError`` unless ``name`` is usable as a directory name.

    This is layer 2's security boundary: in phase B these names arrive from a
    socket, and a name like ``../../etc`` must not escape the data root. The
    charset is a whitelist on purpose — a blacklist of "dangerous" characters
    is a losing game across filesystems.
    """
    if not name:
        raise ValueError("topic name must not be empty")
    if len(name) > _MAX_NAME_LENGTH:
        raise ValueError(f"topic name longer than {_MAX_NAME_LENGTH}: {name!r}")
    if name in (".", ".."):
        raise ValueError(f"reserved topic name: {name!r}")
    if not set(name) <= _ALLOWED_NAME_CHARS:
        raise ValueError(f"invalid topic name: {name!r}")


def _is_valid_name(name: str) -> bool:
    """validate_name as a predicate, for filtering rather than guarding."""
    try:
        validate_name(name)
    except ValueError:
        return False
    return True


@dataclass
class _Partition:
    """One open Log plus the counters its sync policy reads.

    The counters live here rather than in SyncPolicy so the policy can stay a
    frozen value shared by every partition.
    """

    log: Log
    appends: int = 0
    synced_at: float = field(default_factory=time.monotonic)

    def millis_since_sync(self) -> float:
        # monotonic, not time(): a clock adjustment must not make a log look
        # overdue (or never due) for an fsync.
        return (time.monotonic() - self.synced_at) * 1000

    def mark_synced(self) -> None:
        self.appends = 0
        self.synced_at = time.monotonic()


class Topic:
    """A directory of topics, each a directory of partitions, each a Log.

        root/orders/0/00000000000000000000.log + .index
        root/orders/1/...
        root/clicks/0/...

    Partitions are nested rather than flat (Kafka writes ``orders-0/``) so
    listing topic names is an ``iterdir`` instead of parsing and de-duplicating
    suffixes. A topic's partition count is a property of the topic recorded on
    disk, NOT of this registry: reopening a 4-partition topic finds 4, whatever
    ``partitions`` says here.
    """

    def __init__(
        self,
        root: Path,
        partitions: int = 1,
        sync_policy: SyncPolicy = SYNC_NEVER,
        max_segment_bytes: int = 1 << 30,
    ) -> None:
        if partitions < 1:
            raise ValueError(f"partitions must be >= 1, got {partitions}")
        self._root = root
        self._default_partitions = partitions
        self._sync_policy = sync_policy
        self._max_segment_bytes = max_segment_bytes
        self._partitioner = Partitioner()
        self._open: dict[str, list[_Partition]] = {}
        root.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------- names

    def names(self) -> list[str]:
        """Every topic on disk, whether or not this process has opened it.

        Read from the filesystem, not the cache: a topic exists because its
        directory does. Otherwise a fresh restart would report none until
        something happened to touch them.

        A topic is a directory *containing partitions* — a structural test,
        not a lexical one. The data root is not always ours alone: a freshly
        formatted ext4 volume (every cloud PersistentVolumeClaim) arrives with
        a ``lost+found``, and NFS volumes grow a ``.snapshot``. The second of
        those is a perfectly legal topic name, so checking the name is not
        enough — but neither directory holds partitions, and listing a name
        that ``get()`` then rejects with UnknownTopic would make the API
        disagree with itself.
        """
        return sorted(
            p.name
            for p in self._root.iterdir()
            if p.is_dir() and _is_valid_name(p.name) and self._partition_dirs(p.name)
        )

    def exists(self, name: str) -> bool:
        validate_name(name)
        return (self._root / name).is_dir()

    def partitions_of(self, name: str) -> int:
        return len(self._partitions(name))

    # ------------------------------------------------------- create and get

    def create(self, name: str, partitions: int | None = None) -> int:
        """Create ``name`` if absent; return its partition count either way.

        Idempotent, and it never re-partitions an existing topic: changing the
        count would move every key to a different partition and silently break
        the ordering guarantee the partitioner exists to provide.
        """
        validate_name(name)
        if name in self._open:
            return len(self._open[name])

        existing = self._partition_dirs(name)
        count = len(existing) or (partitions or self._default_partitions)
        self._open[name] = [
            _Partition(Log(self._root / name / str(p), self._max_segment_bytes))
            for p in range(count)
        ]
        return count

    def get(self, name: str, partition: int = 0) -> Log:
        """The Log for one partition. Raises rather than creating — see
        UnknownTopic."""
        parts = self._partitions(name)
        if not 0 <= partition < len(parts):
            raise ValueError(
                f"topic {name!r} has {len(parts)} partition(s), asked for {partition}"
            )
        return parts[partition].log

    # -------------------------------------------------------- read and write

    def append(
        self,
        name: str,
        key: bytes | None,
        value: bytes | None,
        timestamp: int | None = None,
    ) -> tuple[int, Offset]:
        """Route (key, value) to a partition, append it, maybe fsync.

        Returns (partition, offset) — the offset alone is meaningless without
        the partition, since each one numbers its records from zero.

        Auto-creates the topic: a producer writing to a new name is the normal
        way topics come into existence (Kafka's auto.create.topics.enable).
        """
        validate_name(name)
        if name not in self._open:
            self.create(name)
        parts = self._open[name]

        partition = self._partitioner.partition_for(key, len(parts))
        entry = parts[partition]
        offset = entry.log.append(key, value, timestamp)

        # Durability is decided HERE, not in storage: Log.sync() is the
        # mechanism, this layer owns the policy.
        entry.appends += 1
        if self._sync_policy.should_sync(entry.appends, entry.millis_since_sync()):
            entry.log.sync()
            entry.mark_synced()
        return partition, offset

    def read_from(
        self, name: str, offset: Offset, partition: int = 0
    ) -> Iterator[Record]:
        """Records from ``offset`` onward in one partition.

        No partitioner involved: a consumer names the partition it was
        assigned. Merging partitions is not offered, because there is no
        correct order to merge them into.
        """
        return self.get(name, partition).read_from(offset)

    # ------------------------------------------------------------ lifecycle

    def sync(self) -> None:
        for parts in self._open.values():
            for entry in parts:
                entry.log.sync()
                entry.mark_synced()

    def close(self) -> None:
        for parts in self._open.values():
            for entry in parts:
                entry.log.close()  # closing already fsyncs
        self._open.clear()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # -------------------------------------------------------------- private

    def _partition_dirs(self, name: str) -> list[int]:
        directory = self._root / name
        if not directory.is_dir():
            return []
        return sorted(int(p.name) for p in directory.iterdir() if p.name.isdigit())

    def _partitions(self, name: str) -> list[_Partition]:
        """Open partitions for an EXISTING topic, or raise."""
        validate_name(name)
        if name in self._open:
            return self._open[name]
        if not self._partition_dirs(name):
            raise UnknownTopic(name)
        self.create(name)  # adopts the count from disk
        return self._open[name]
