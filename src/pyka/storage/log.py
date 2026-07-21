"""Log: one topic-partition — an ordered chain of Segments, only the tail writable."""
import bisect
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Self

from pyka.storage.record import Record
from pyka.storage.segment import Segment
from pyka.storage.types import Offset


class CorruptLog(ValueError):
    """The chain of segment files is broken — a gap means records are gone.

    Sibling of CorruptRecord one level down: that one means bytes inside a
    file are wrong, this one means a whole file is missing from the middle.
    """


class Log:
    def __init__(self, directory: Path, max_segment_bytes: int = 1 << 30) -> None:
        self._directory = directory
        self._max_segment_bytes = max_segment_bytes
        # Layer 3 dispatches every storage call through asyncio.to_thread, and
        # two servers share one Topic — so several threads really do reach one
        # Log at once. Without this, "read next_offset / write / advance" is a
        # read-modify-write race: two appends claim the same offset, both write
        # to the same byte position, and the segment is left with an offset gap
        # that recovery refuses to open. Demonstrated, not theorised: 4 threads
        # x 50 appends produced 2 usable records and a corrupt log.
        #
        # Reentrant because append() calls roll() and both need the lock.
        self._lock = threading.RLock()
        # Log owns directory creation — a Segment silently mkdir-ing would put
        # log files in surprising places.
        directory.mkdir(parents=True, exist_ok=True)

        # The filename IS the base offset (that's what the :020d padding is
        # for). A .log file whose stem isn't a number is not ours to tolerate.
        bases = sorted(Offset(int(p.stem)) for p in directory.glob("*.log"))
        if not bases:
            bases = [Offset(0)]
        self._segments = [Segment(directory, base, max_segment_bytes) for base in bases]

        # Offsets must be continuous ACROSS segments, not just within them.
        # A hole in the chain means a deleted or lost file: records silently
        # missing from every read. Loud over silent, same verdict as mid-file
        # corruption.
        for prev, cur in zip(self._segments, self._segments[1:]):
            if cur.base_offset != prev.next_offset:
                raise CorruptLog(
                    f"segment chain broken in {directory}: "
                    f"{prev.base_offset:020d}.log ends at {prev.next_offset}, "
                    f"next file starts at {cur.base_offset}"
                )

        # Seal everything but the tail. close() only drops the WRITE handles —
        # reads open their own handle per call and lookup bisects in memory,
        # so a closed Segment still serves read_from. "Only the tail is
        # writable" is therefore physical, not an if-check: a sealed segment
        # has no file handle to append with. Also keeps open fds at ~2 total
        # instead of 2 per segment.
        for seg in self._segments[:-1]:
            seg.close()

    @property
    def _active(self) -> Segment:
        return self._segments[-1]

    @property
    def next_offset(self) -> Offset:
        # Delegated, not duplicated: the tail already maintains this, and two
        # copies of one truth drift.
        return self._active.next_offset

    def append(self, key: bytes | None, value: bytes | None,
               timestamp: int | None = None) -> Offset:
        """Stamp the next offset on (key, value), write it, return the offset.

        Callers hand over payloads, never offsets — Log owns the sequence, so
        a wrong offset is not rejected, it is unconstructible. (Segment.append
        still checks; that check now guards Log's own bookkeeping.)
        """
        # The whole body is one critical section: claiming the offset, choosing
        # the segment and writing the bytes must be indivisible, or two threads
        # claim the same offset and write over each other.
        with self._lock:
            offset = self._active.next_offset
            if timestamp is None:
                timestamp = time.time_ns() // 1_000_000  # epoch ms, like Kafka
            record = Record(offset, timestamp, key, value)

            # Built before the roll check on purpose: has_room_for needs the
            # record's size. The rolled-to segment starts empty, and an empty
            # segment accepts anything — so this cannot roll twice, and the new
            # base equals the next offset by construction (the chain invariant
            # is maintained by the same line that rolls).
            if not self._active.has_room_for(record):
                self.roll()
            self._active.append(record)
            return offset

    def roll(self) -> Segment:
        """Seal the active segment and start a new one; returns the new tail.

        Called by append when the tail is full, and by an operator who wants
        to force it — sealing is what makes a segment eligible for retention,
        so "roll now" is a real operational request and not just a test hook.

        The new base offset is the current next offset, which is what keeps
        the chain continuous: the invariant is maintained by the same line
        that breaks the old segment off.

        Rolling an EMPTY segment does nothing, and must not. Its base already
        equals the next offset, so the "new" segment would take the same name
        and therefore the same file — leaving two Segment objects writing one
        path, and a chain listing segments that do not exist. An empty segment
        is already a fresh one; there is nothing to seal.
        """
        with self._lock:
            if self._active.size_bytes == 0:
                return self._active

            self._active.close()  # seal: write handles drop, reads keep working
            self._segments.append(
                Segment(self._directory, self.next_offset, self._max_segment_bytes)
            )
            return self._active

    @property
    def segments(self) -> tuple[Segment, ...]:
        """The chain, oldest first. A tuple so a caller cannot splice it —
        only append and roll may change which segments exist."""
        with self._lock:
            return tuple(self._segments)

    def read_from(self, offset: Offset) -> Iterator[Record]:
        """Records from ``offset`` onward, crossing segment boundaries.

        Past the end yields nothing — a consumer polling at the head is
        normal, not an error. Before the start raises: those records never
        existed here, so asking is a caller bug (and once retention exists it
        will mean "already deleted").
        """
        if offset < self._segments[0].base_offset:
            raise ValueError(
                f"offset {offset} is before this log starts "
                f"({self._segments[0].base_offset})"
            )
        return self._iter_from(offset)

    def _iter_from(self, offset: Offset) -> Iterator[Record]:
        # Snapshot under the lock: a concurrent roll appends to _segments, and
        # iterating a list while another thread mutates it is how readers see
        # a segment twice or not at all. The snapshot may go stale — a roll
        # during a long read is invisible to it — which is fine: those records
        # did not exist when the read started.
        with self._lock:
            segments = tuple(self._segments)

        # The same floor-search as Index.lookup, one level up: the segment
        # that can contain `offset` is the last one whose base is <= it.
        # From the next segment on, scan from each segment's own base —
        # passing the original offset would trip their below-base check,
        # which is doing its job: within those segments it IS out of range.
        bases = [seg.base_offset for seg in segments]
        start = bisect.bisect_right(bases, offset) - 1
        for seg in segments[start:]:
            yield from seg.read_from(Offset(max(offset, seg.base_offset)))

    def sync(self) -> None:
        # Sealed segments were synced by close(); only the tail ever moves.
        # Locked so a concurrent roll cannot swap the tail mid-fsync.
        with self._lock:
            self._active.sync()

    def close(self) -> None:
        """Close every segment, even if one of them fails.

        A single failing close must not leave the rest unflushed: this runs on
        SIGTERM, which is exactly when losing a tail matters most. Errors are
        collected and raised together at the end, so nothing is hidden either.
        """
        with self._lock:
            errors = []
            for seg in self._segments:
                try:
                    seg.close()  # idempotent — sealed ones are already closed
                except Exception as err:  # noqa: BLE001 — re-raised below
                    errors.append(err)
            if errors:
                raise ExceptionGroup(f"failed to close {self._directory}", errors)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
