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

from pyka.cluster.ring import Ring
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
    ) -> None:
        self._topic = Topic(root, partitions, sync_policy, max_segment_bytes)
        self._ring = ring or Ring(brokers=1, me=0)
        self._ready = False

    @property
    def topic(self) -> Topic:
        return self._topic

    @property
    def ring(self) -> Ring:
        return self._ring

    @property
    def ready(self) -> bool:
        """False until every log on disk has been recovered."""
        return self._ready

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
        self._ready = True
        log.info("store ready: %d topic(s)", len(names))

    async def close(self) -> None:
        self._ready = False
        await asyncio.to_thread(self._topic.close)

    # ------------------------------------------------------------ operations

    async def names(self) -> list[str]:
        return await asyncio.to_thread(self._topic.names)

    async def create(self, name: str, partitions: int | None = None) -> int:
        return await asyncio.to_thread(self._topic.create, name, partitions)

    async def append(
        self,
        name: str,
        key: bytes | None,
        value: bytes | None,
        timestamp: int | None = None,
    ) -> tuple[int, Offset]:
        return await asyncio.to_thread(self._topic.append, name, key, value, timestamp)

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
