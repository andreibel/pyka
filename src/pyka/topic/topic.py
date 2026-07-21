"""Topic: named logs on disk — validation, routing, and when to fsync."""
import json
import string
import threading
import time
from collections.abc import Callable, Iterator
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

META_FILE = "topic.json"
"""Topic-level metadata: how many partitions the topic has, in TOTAL.

Load-bearing the moment partitions are spread across brokers. This instance
stores only the partitions it owns, so counting local directories would answer
2 for a 6-partition topic — and the partitioner would then compute
`crc32(key) % 2` and send every key to the wrong place.

The total cannot be derived from local disk, so it is written down. This is
the smallest possible piece of cluster metadata, and it is exactly why real
systems centralise theirs: every broker keeps a copy, and they agree only
because they were told the same number.
"""


class UnknownTopic(KeyError):
    """Asked for a topic that does not exist.

    Reads raise this; appends create instead. A consumer naming a topic that
    isn't there has almost certainly typo'd, and silently returning zero
    records from a freshly conjured empty topic would never tell it so.
    """


class PartitionNotLocal(KeyError):
    """The partition exists, but this instance does not store it.

    Different from a partition that does not exist: the caller is not wrong
    about the topic, it is talking to the wrong holder. Layer 3 turns this
    into "try broker N at this address"; the wording here stays generic
    because layer 2 has never heard of brokers.
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


@dataclass
class _OpenTopic:
    """A topic as this instance sees it: the whole, and its own share."""

    partitions: int               # total, across every holder
    local: dict[int, _Partition]  # only the ones stored here


class Topic:
    """A directory of topics, each a directory of partitions, each a Log.

        root/orders/topic.json      {"partitions": 6}
        root/orders/0/00000...log + .index
        root/orders/3/...           only the partitions THIS instance owns
        root/clicks/0/...

    Partitions are nested rather than flat (Kafka writes ``orders-0/``) so
    listing topic names is an ``iterdir`` instead of parsing and
    de-duplicating suffixes.

    ``owns(topic, partition)`` decides which partitions live here, and defaults
    to all of them — the single-node case, and the only one anything below
    layer 3 has ever needed. Layer 3 passes ``ring.owns``, as a plain
    predicate, so this module still knows nothing about brokers or clusters.

    The topic NAME is part of the question, not just the index: a router that
    ignored it would send partition 0 of every topic to the same place.
    """

    def __init__(
        self,
        root: Path,
        partitions: int = 1,
        sync_policy: SyncPolicy = SYNC_NEVER,
        max_segment_bytes: int = 1 << 30,
        owns: Callable[[str, int], bool] | None = None,
    ) -> None:
        if partitions < 1:
            raise ValueError(f"partitions must be >= 1, got {partitions}")
        self._root = root
        self._default_partitions = partitions
        self._sync_policy = sync_policy
        self._max_segment_bytes = max_segment_bytes
        self._owns = owns if owns is not None else (lambda _topic, _p: True)
        self._partitioner = Partitioner()
        self._open: dict[str, _OpenTopic] = {}
        # Layer 3 reaches this object from two servers (gRPC and the admin
        # API), each dispatching blocking calls through asyncio.to_thread — so
        # several threads really can call create() for the same name at once.
        # Without this, both would win the "not in cache" check and build two
        # Log objects on one directory, each holding its own write handle.
        # Reentrant because append() and _topic() both call _open_locked().
        #
        # Scope is deliberately the CACHE, not the whole class: appends to
        # different topics must not serialise. Concurrent appends to ONE log
        # are made safe by Log.append's own lock.
        self._lock = threading.RLock()
        root.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------- names

    def names(self) -> list[str]:
        """Every topic on disk, whether or not this process has opened it.

        Read from the filesystem, not the cache: a topic exists because its
        directory does. Otherwise a fresh restart would report none until
        something happened to touch them.

        A topic is a directory holding a ``topic.json`` — a structural test,
        not a lexical one. The data root is not always ours alone: a freshly
        formatted ext4 volume (every cloud PersistentVolumeClaim) arrives with
        a ``lost+found``, and NFS volumes grow a ``.snapshot``; the second is a
        perfectly legal topic name, so filtering by name is not enough. The
        metadata file is also the only marker that works for an instance
        holding *none* of a topic's partitions — no partition directory, but
        the topic still exists.
        """
        return sorted(
            p.name
            for p in self._root.iterdir()
            if p.is_dir() and _is_valid_name(p.name) and (p / META_FILE).is_file()
        )

    def exists(self, name: str) -> bool:
        validate_name(name)
        return (self._root / name / META_FILE).is_file()

    def partitions_of(self, name: str) -> int:
        """Total partitions in this topic, across every holder."""
        return self._topic(name).partitions

    def local_partitions(self, name: str) -> list[int]:
        """The subset stored here — all of them unless ``owns`` says otherwise."""
        return sorted(self._topic(name).local)

    def foreign_partitions(self, name: str) -> list[int]:
        """Partition directories on disk that ``owns`` says are NOT ours.

        This should always be empty. If it is not, the routing changed under a
        directory that already had data — someone resized the cluster — and
        those segments are now orphaned: unreachable here, while whoever owns
        them now starts an empty log and renumbers from zero. Two logs for one
        partition, neither aware of the other.

        Detecting it is the difference between a loud operational stop and
        silent divergence, which is the worst failure mode this system has.
        """
        directory = self._root / name
        if not directory.is_dir():
            return []
        return sorted(
            int(p.name)
            for p in directory.iterdir()
            if p.name.isdigit() and not self._owns(name, int(p.name))
        )

    # ------------------------------------------------------- create and get

    def create(self, name: str, partitions: int | None = None) -> int:
        """Create ``name`` if absent; return its partition count either way.

        Idempotent, and it never re-partitions an existing topic: changing the
        count would move every key to a different partition and silently break
        the ordering guarantee the partitioner exists to provide.
        """
        validate_name(name)
        with self._lock:
            if name in self._open:
                return self._open[name].partitions
            return self._open_locked(name, partitions).partitions

    def _open_locked(self, name: str, partitions: int | None) -> _OpenTopic:
        count = self._read_meta(name)
        if count is None:
            count = partitions or self._default_partitions
            self._write_meta(name, count)

        opened = _OpenTopic(
            partitions=count,
            local={
                p: _Partition(Log(self._root / name / str(p), self._max_segment_bytes))
                for p in range(count)
                if self._owns(name, p)
            },
        )
        self._open[name] = opened
        return opened

    def get(self, name: str, partition: int = 0) -> Log:
        """The Log for one partition. Raises rather than creating — see
        UnknownTopic."""
        topic = self._topic(name)
        self._check_partition(name, topic, partition)
        return topic.local[partition].log

    # -------------------------------------------------------- read and write

    def route(self, name: str, key: bytes | None) -> int:
        """Which partition this key belongs to. Creates nothing.

        Exposed so a caller can find out where a record *should* go before
        discovering it cannot accept it — a holder that does not own the
        answer must redirect rather than write.
        """
        return self._partitioner.partition_for(key, self._topic(name).partitions)

    def append(
        self,
        name: str,
        key: bytes | None,
        value: bytes | None,
        timestamp: int | None = None,
        partition: int | None = None,
    ) -> tuple[int, Offset]:
        """Route (key, value) to a partition, append it, maybe fsync.

        Returns (partition, offset) — the offset alone is meaningless without
        the partition, since each one numbers its records from zero.

        ``partition`` overrides routing, for a caller that has already routed.
        Either way the answer must be a partition stored here, or
        PartitionNotLocal.

        Auto-creates the topic: a producer writing to a new name is the normal
        way topics come into existence (Kafka's auto.create.topics.enable).
        """
        validate_name(name)
        with self._lock:
            # Only the cache lookup is guarded. The append itself is outside
            # the lock so two topics never block each other; concurrent
            # appends to ONE log are Log.append's problem.
            if name not in self._open:
                self._open_locked(name, None)
            topic = self._open[name]

        if partition is None:
            partition = self._partitioner.partition_for(key, topic.partitions)
        self._check_partition(name, topic, partition)

        entry = topic.local[partition]
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
        for entry in self._every_partition():
            entry.log.sync()
            entry.mark_synced()

    def close(self) -> None:
        """Close every log, even if one of them fails.

        This runs on SIGTERM — the moment when an unflushed tail costs most —
        so one bad partition must not stop the other twenty from reaching disk.
        Errors are collected and raised together rather than swallowed.
        """
        errors = []
        for entry in self._every_partition():
            try:
                entry.log.close()  # closing already fsyncs
            except Exception as err:  # noqa: BLE001 — re-raised below
                errors.append(err)
        with self._lock:
            self._open.clear()
        if errors:
            raise ExceptionGroup(f"failed to close {self._root}", errors)

    def _every_partition(self) -> list[_Partition]:
        # Snapshot under the lock: create() may add topics while we iterate.
        with self._lock:
            return [
                entry
                for topic in self._open.values()
                for entry in topic.local.values()
            ]

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # -------------------------------------------------------------- private

    def _check_partition(self, name: str, topic: _OpenTopic, partition: int) -> None:
        """Two different wrongs, deliberately two different errors.

        Out of range means the caller is confused about the topic; not local
        means the caller is confused about *who holds it*, which is a
        recoverable mistake — refresh your routing and try elsewhere.
        """
        if not 0 <= partition < topic.partitions:
            raise ValueError(
                f"topic {name!r} has {topic.partitions} partition(s), "
                f"asked for {partition}"
            )
        if partition not in topic.local:
            raise PartitionNotLocal(
                f"partition {partition} of {name!r} is not stored here"
            )

    def _meta_path(self, name: str) -> Path:
        return self._root / name / META_FILE

    def _read_meta(self, name: str) -> int | None:
        path = self._meta_path(name)
        if not path.is_file():
            return None
        return int(json.loads(path.read_text())["partitions"])

    def _write_meta(self, name: str, partitions: int) -> None:
        path = self._meta_path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"partitions": partitions}))

    def _topic(self, name: str) -> _OpenTopic:
        """An EXISTING topic, opened here if it was not already, or raise."""
        validate_name(name)
        with self._lock:
            if name in self._open:
                return self._open[name]
            if self._read_meta(name) is None:
                raise UnknownTopic(name)
            return self._open_locked(name, None)
