"""Segment: one .log file plus its .index, named by base offset."""
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Self

from pyka.storage.record import CorruptRecord, Record
from pyka.storage.types import Offset, Position


class Segment:
    def __init__(self, base_path: Path, base_offset: Offset, max_bytes: int = 1 << 30) -> None:
        self._base_offset = base_offset
        self._max_bytes = max_bytes
        self._log_path: Path = base_path / f'{base_offset:020d}.log'
        # written in A3; the sparse offset -> position map lives beside the log
        self._index_path = base_path / f'{base_offset:020d}.index'
        self._next_offset, self._position = self._recover()
        self._file = open(self._log_path, "ab")

    def _recover(self) -> tuple[Offset, Position]:
        """Scan the log, returning (next_offset, end of the last good record).

        Truncates a torn tail: without it the next append would land *after*
        the garbage and embed an unreadable hole in the middle of the file.

        Runs a full scan every open. The .index will later give a starting
        point so only the tail needs scanning — this same loop becomes
        Index.rebuild().
        """
        self._log_path.touch(exist_ok=True)

        next_offset, position = self._base_offset, Position(0)
        with open(self._log_path, "rb") as f:
            while (rec := Record.read_one(f)) is not None:
                # offset lives outside the crc-covered region, so a bit flip
                # there is invisible to the checksum. Offsets in a segment are
                # strictly sequential, so continuity is the check that catches it.
                if rec.offset != next_offset:
                    raise CorruptRecord(
                        f"offset gap at byte {position} in {self._log_path.name}: "
                        f"expected {next_offset}, got {rec.offset}"
                    )
                next_offset = Offset(rec.offset + 1)
                position = Position(f.tell())

        if position < self._log_path.stat().st_size:
            os.truncate(self._log_path, position)
        return next_offset, position

    def append(self, record: Record) -> Position:
        """Write one record; returns the byte position it starts at."""
        if record.offset != self._next_offset:
            raise ValueError(f"expected offset {self._next_offset}, got {record.offset}")

        position = self._position
        expected_end = position + record.size()
        self._file.write(record.encode())
        self._file.flush()

        end = self._file.tell()
        if end != expected_end:
            raise RuntimeError(
                f"{self._log_path.name}: expected to end at {expected_end}, "
                f"file is at {end} — another writer?"
            )
        self._next_offset = Offset(self._next_offset + 1)
        self._position = Position(end)
        return position

    def sync(self) -> None:
        self._file.flush()
        os.fsync(self._file.fileno())

    def close(self) -> None:
        if self._file.closed:
            return
        self.sync()
        self._file.close()

    def read_from(self, offset: Offset) -> Iterator[Record]:
        """Records from ``offset`` onward. Validates eagerly — see _iter_from."""
        if offset < self._base_offset:
            raise ValueError(f"offset {offset} is out of range in this segment")
        return self._iter_from(offset)

    def _iter_from(self, offset: Offset) -> Iterator[Record]:
        # Split from read_from so the bounds check runs at call time: a function
        # containing `yield` executes nothing until the first next().
        #
        # Scans from byte 0, decoding every record before the one asked for.
        # Index.lookup(offset) will supply a starting position in A3.
        with open(self._log_path, "rb") as f:
            while (rec := Record.read_one(f)) is not None:
                if rec.offset >= offset:
                    yield rec


    def has_room_for(self, record: Record) -> bool:
        # An empty segment always accepts: otherwise a record bigger than
        # max_bytes would roll forever, creating empty segments until the disk
        # fills. Consequence: max_bytes is a SOFT limit, and a segment can
        # reach max_bytes + (largest record) - 1 bytes.
        if self._position == 0:
            return True
        return self._position + record.size() <= self._max_bytes

    def is_full(self) -> bool:
        return self._position >= self._max_bytes

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # Returning None (falsy) lets any exception propagate. Returning True
        # would swallow it — a silent way to lose errors.
        self.close()