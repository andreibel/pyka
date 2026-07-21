"""Store: the one Topic this process owns, and the async/blocking seam.

Both servers — gRPC on 9092, the admin API on 8080 — share this single object.
They must: Segment holds an exclusive write handle and Log caches next_offset
in memory, so a second process on the same directory would believe it also
owned the tail. Co-location is a correctness requirement here, not a
convenience.

Every storage call goes through asyncio.to_thread. Layers 1-2 are ordinary
blocking code and stay that way: there is no async file I/O in the stdlib, and
for a log the blocking is often the point — fsync not returning until the write
is durable IS the guarantee. The event loop stays free for sockets, where
concurrency actually pays.
"""
import asyncio
import logging
from pathlib import Path

from pyka.broker.tail import Tail
from pyka.cluster.ring import  Ring
from pyka.storage.record import Record
from pyka.storage.types import Offset
from pyka.topic.policy import SYNC_NEVER, SyncPolicy
from pyka.topic.topic import Topic

log = logging.getLogger(__name__)


class Store:
    def __init__(
        self,
        root: Path,
        partitions: int = 1,
        sync_policy: SyncPolicy = SYNC_NEVER,
        max_segment_bytes: int = 1 << 30,
        ring: Ring | None = None,
        allow_orphans: bool = False,
    ) -> None:
        self._ring = ring or Ring(brokers=1, me=0)
        # The ring reaches layer 2 as a bare predicate, never as a Ring: the
        # topic layer decides where a key goes, but has no idea that machines
        # exist. With one broker `owns` is always True and nothing changes.
        self._topic = Topic(
            root,
            partitions,
            sync_policy,
            max_segment_bytes,
            owns=self._ring.owns,
        )
        self._tail = Tail()
        self._ready = False
        self._allow_orphans = allow_orphans
        self._orphans: dict[str, list[int]] = {}

    @property
    def topic(self) -> Topic:
        return self._topic

    @property
    def tail(self) -> Tail:
        return self._tail

    @property
    def ring(self) -> Ring:
        return self._ring

    @property
    def ready(self) -> bool:
        """False until every log on disk has been recovered — and False for
        good if this broker holds data it does not own."""
        return self._ready

    @property
    def orphans(self) -> dict[str, list[int]]:
        """topic -> partitions whose segments are here but whose owner is not.

        Always empty in a healthy cluster. Non-empty means someone changed the
        broker count without migrating the data.
        """
        return dict(self._orphans)

    async def open(self) -> None:
        """Recover every topic found on disk, then report ready.

        Eager rather than lazy on purpose. Recovery scans each segment at
        roughly 15 s/GiB (bench/), and doing it lazily would push that cost
        onto the first unlucky request — a produce that mysteriously takes a
        minute. Doing it here means the readiness probe covers it: Kubernetes
        holds traffic back until this returns, which is the entire reason the
        health service starts NOT_SERVING.
        """
        names = await asyncio.to_thread(self._topic.names)
        for name in names:
            log.info("recovering topic %s", name)
            await asyncio.to_thread(self._topic.create, name)

        self._orphans = await asyncio.to_thread(self._find_orphans, names)
        if self._orphans and not self._allow_orphans:
            # Stay alive but never become ready: the pod keeps running so an
            # operator can exec in and look, while Kubernetes routes no traffic
            # to it and `kubectl get pods` shows 0/1. Serving anyway would mean
            # this broker's peers write a fresh empty log for partitions whose
            # data is sitting right here, unreachable.
            log.error(
                "REFUSING TO SERVE: %s. The broker count changed under existing "
                "data, so these partitions are orphaned — their segments are "
                "here but their owner is elsewhere. Move the directories to "
                "their new owners (see README 'Resizing a cluster'), or set "
                "PYKA_ALLOW_ORPHANS=1 to serve anyway and accept the split.",
                "; ".join(f"{n}: partitions {ps}" for n, ps in self._orphans.items()),
            )
            return

        if self._orphans:
            log.warning("serving with orphaned partitions: %s", self._orphans)
        self._ready = True
        log.info("store ready: %d topic(s)", len(names))

    def _find_orphans(self, names: list[str]) -> dict[str, list[int]]:
        found = {}
        for name in names:
            foreign = self._topic.foreign_partitions(name)
            if foreign:
                found[name] = foreign
        return found

    async def close(self) -> None:
        self._ready = False
        # Release parked live-tail streams first: otherwise server.stop(grace)
        # sits out the whole grace period waiting for consumers that are, by
        # design, waiting forever.
        self._tail.close()
        await asyncio.to_thread(self._topic.close)

    # ------------------------------------------------------------ operations

    async def names(self) -> list[str]:
        return await asyncio.to_thread(self._topic.names)

    async def create(self, name: str, partitions: int | None = None) -> int:
        return await asyncio.to_thread(self._topic.create, name, partitions)

    async def partitions_of(self, name: str) -> int:
        return await asyncio.to_thread(self._topic.partitions_of, name)

    async def local_partitions(self, name: str) -> list[int]:
        return await asyncio.to_thread(self._topic.local_partitions, name)

    async def route(self, name: str, key: bytes | None) -> int:
        """Where a key belongs — asked before appending, so a broker that does
        not own the answer can redirect instead of writing."""
        return await asyncio.to_thread(self._topic.route, name, key)

    async def append(
        self,
        name: str,
        key: bytes | None,
        value: bytes | None,
        timestamp: int | None = None,
        partition: int | None = None,
    ) -> tuple[int, Offset]:
        partition, offset = await asyncio.to_thread(
            self._topic.append, name, key, value, timestamp, partition
        )
        # After the await, so back on the event loop — which is what makes a
        # plain asyncio.Event safe. Every append must come through here or a
        # live tail will miss it.
        self._tail.notify(name, partition)
        return partition, offset

    async def read(
        self, name: str, offset: Offset, partition: int = 0, limit: int = 0
    ) -> list[Record]:
        """Read a bounded batch. Returns a list, not an iterator.

        The generator would otherwise be consumed on the event loop, doing
        blocking reads one record at a time between awaits — the exact thing
        to_thread exists to prevent. Batching moves the whole read into the
        worker thread; ``limit`` keeps it from pulling a gigabyte into memory.
        """

        def _read() -> list[Record]:
            records = self._topic.read_from(name, offset, partition)
            if limit <= 0:
                return list(records)
            out = []
            for record in records:
                out.append(record)
                if len(out) >= limit:
                    break
            return out

        return await asyncio.to_thread(_read)
