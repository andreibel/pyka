"""Index: sparse logical-offset -> byte-position map. Derived, rebuildable."""
import bisect
import os
import struct
from pathlib import Path

from pyka.storage.types import Offset, Position

ENTRY = ">II"
ENTRY_SIZE = struct.calcsize(ENTRY)

MAX_U32 = 0xFFFFFFFF
"""Both fields of an entry are u32, which caps a segment at 4 GiB and ~4
billion records. Segment imports this rather than hard-coding 4 GiB: the
entry format and the segment size limit are one decision, not two."""


class Index:

    def __init__(self, path: Path, base_offset: Offset, interval_bytes: int = 4096) -> None:
        self._index_path = path
        self._base_offset = base_offset
        self._interval_bytes = interval_bytes
        self._index_path.touch(exist_ok=True)

        index_bytes = self._index_path.read_bytes()
        if len(index_bytes) % ENTRY_SIZE:
            end = len(index_bytes) - len(index_bytes) % ENTRY_SIZE
            os.truncate(self._index_path, end)
            index_bytes = index_bytes[:end]

        self._entries = list(struct.iter_unpack(ENTRY, index_bytes))
        self._file = open(self._index_path, "ab")

    def maybe_append(self, offset: Offset, position: Position) -> None:
        """Record (offset, position) once interval_bytes have passed since the
        last entry. Called after every append; most calls do nothing.

        ``position`` is where the record STARTS, so a seek there lands on a
        record boundary.
        """
        # An empty index already means "start from byte 0" — that is what
        # lookup falls back to — so the record at position 0 is indexed
        # implicitly and never needs an entry of its own.
        last_position = self._entries[-1][1] if self._entries else 0
        if position - last_position < self._interval_bytes:
            return

        relative = offset - self._base_offset
        if not 0 <= relative <= MAX_U32:
            raise ValueError(
                f"offset {offset} is {relative} from base {self._base_offset}, "
                f"outside the u32 an entry can hold"
            )
        # Position is u32 too. Unchecked, struct.pack raises a bare struct.error
        # mid-append, from inside the index, about a segment that grew too big —
        # an error message pointing nowhere near the cause.
        if not 0 <= position <= MAX_U32:
            raise ValueError(
                f"position {position} is outside the u32 an entry can hold; "
                f"a segment cannot exceed {MAX_U32 + 1} bytes"
            )
        if self._entries and relative <= self._entries[-1][0]:
            raise ValueError(
                f"offsets must increase: {offset} follows "
                f"{self._base_offset + self._entries[-1][0]}"
            )

        # No flush: the index is derived and rebuilt on every open, so a lost
        # tail costs a slower first read, never a wrong answer. sync() exists
        # for the caller that wants it. (Segment.append flushes the log every
        # time because the log IS the source of truth.)
        self._file.write(struct.pack(ENTRY, relative, position))
        self._entries.append((relative, position))

    def lookup(self, offset: Offset) -> tuple[Offset, Position]:
        """Best place to start reading for ``offset``: the greatest entry at or
        before it, or the start of the segment when there is none.

        A hint, never an authority. The caller seeks here, checks the offset it
        actually finds, and rescans from 0 if they disagree. Total function —
        it cannot fail, only be less helpful.
        """
        relative = offset - self._base_offset
        # bisect_right returns the insertion point AFTER any equal entry, so
        # i-1 is "the last entry <= relative", exact hits included. bisect_left
        # would land ON an exact match and i-1 would step one entry too far
        # back: still correct, just a needlessly longer scan.
        i = bisect.bisect_right(self._entries, relative, key=lambda e: e[0]) - 1
        if i < 0:
            # No entry at or before this offset — including offsets below
            # base_offset, which is why this is a fallback and not a raise.
            return self._base_offset, Position(0)

        rel_off, position = self._entries[i]
        return Offset(self._base_offset + rel_off), Position(position)

    def clear(self) -> None:
        self._entries = []
        self._file.flush()
        os.truncate(self._index_path, 0)

    def last_entry(self) -> tuple[Offset, Position] | None:
        if not self._entries:
            return None
        rel_off, position = self._entries[-1]
        return Offset(self._base_offset + rel_off), Position(position)

    def sync(self) -> None:
        self._file.flush()
        os.fsync(self._file.fileno())

    def close(self) -> None:
        if self._file.closed:
            return
        self.sync()
        self._file.close()

    def __len__(self) -> int:
        return len(self._entries)
