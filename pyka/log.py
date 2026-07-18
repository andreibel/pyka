"""Append-only record log backed by a single file."""

from pathlib import Path
import struct
from collections.abc import Iterator


class Log():
    """An append-only log of opaque byte records.

    Records are stored back to back, each one framed by a 4-byte
    big-endian length header::

        ┌─────────────┬──────────────────┐
        │ length: u32 │ payload: <bytes> │   repeated until EOF
        └─────────────┴──────────────────┘

    The header is what makes the file self-describing: a reader learns
    each payload's size before reading it, so records need no delimiter
    and a payload may contain any byte sequence.

    The log holds an open write handle and is therefore a resource —
    call :meth:`close` when done with it.
    """

    def __init__(self, path: Path) -> None:
        """Open ``path`` for appending, creating it if it does not exist.

        Existing records are kept; writes always land at the end.

        :param path: file backing this log
        """
        self._path = path
        self._file = open(path, "ab")

    def append(self, value: bytes) -> int:
        """Append a single record to the end of the log

        The record is written as a 4-byte big-endian length header
        followed by the payload itself
        :param value: raw payload bytes; may be empty
        :return: the offset from the start of the log to the current log that append
        """
        header = struct.pack(">I", len(value))
        offset = self._file.tell()
        self._file.write(header + value)
        return offset

    def read_from(self, offset: int) -> Iterator[bytes]:
        with open(self._path, "rb") as f:
            f.seek(offset)
            while True:
                header = f.read(4)
                if not header:
                    break
                (length,) = struct.unpack(">I", header)
                yield f.read(length)

    def close(self) -> None:
        """Close the write handle, flushing buffered records to disk.

        :return: None
        """
        self._file.close()

    def __iter__(self) -> Iterator[bytes]:
        """Yield every record in the log, oldest first.

        Each call opens its own read handle, so the same log may be
        iterated more than once, and by several readers at a time,
        without disturbing the append position.

        :return: an iterator over payloads, with header bytes stripped
        """
        return self.read_from(0)
