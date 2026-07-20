"""Record: framing — offset, timestamp, key, value; encode/decode."""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass

from typing import BinaryIO, ClassVar

from pyka.storage.types import Offset, Position


# On-disk layout (Kafka v1, minus magic/attributes/compression).
#
#   offset     int64   >q    logical position in the partition (0, 1, 2, ...)
#   size       int32   >I    number of bytes AFTER this field
#   -- crc covers everything below this line --
#   crc        uint32  >I    zlib.crc32 over timestamp..value
#   timestamp  int64   >q    epoch milliseconds
#   klen       int32   >i    -1 = null key, 0 = present-but-empty
#   vlen       int32   >i    -1 = null value
#   key        bytes         klen bytes, absent when klen == -1
#   value      bytes         vlen bytes, absent when vlen == -1
#
# offset sits OUTSIDE the length-delimited region on purpose: after seeking to
# a byte position from the Index, the first thing read is the offset, so the
# seek can be verified instead of trusted.


class CorruptRecord(ValueError):
    """Bytes on disk are not a valid record.

    Distinct from a torn tail (``read_one`` returning ``None``), which is the
    expected result of a crash mid-append and is not an error.
    """


@dataclass(frozen=True, slots=True)
class Record:
    offset: Offset
    timestamp: int
    key: bytes | None
    value: bytes | None  # None = tombstone (deletion marker for a compacted log) | None

    # struct formats
    PREFIX: ClassVar[str] = ">qI"  # offset, size
    HEADER: ClassVar[str] = ">qIIqii"  # offset, size, crc, timestamp, klen, vlen
    CRC_BODY: ClassVar[str] = ">qii"  # timestamp, klen, vlen — the crc-covered fixed part

    PREFIX_SIZE: ClassVar[int] = 12  # struct.calcsize(PREFIX)
    HEADER_SIZE: ClassVar[int] = 32  # struct.calcsize(HEADER)

    NULL: ClassVar[int] = -1  # klen/vlen sentinel for "absent"
    MAX_SIZE: ClassVar[int] = 1 << 20  # reject any claimed `size` above this

    def encode(self) -> bytes:
        key_bytes = self.key if self.key is not None else b""
        value_bytes = self.value if self.value is not None else b""
        klen = len(self.key) if self.key is not None else self.NULL
        vlen = len(self.value) if self.value is not None else self.NULL

        crc_body = struct.pack(self.CRC_BODY, self.timestamp, klen, vlen)
        crc = zlib.crc32(value_bytes, zlib.crc32(key_bytes, zlib.crc32(crc_body)))
        crc_pack = struct.pack(">I", crc)

        size = 4 + struct.calcsize(self.CRC_BODY) + len(key_bytes) + len(value_bytes)
        return struct.pack(self.PREFIX, self.offset, size) + crc_pack + crc_body + key_bytes + value_bytes

    def size(self) -> int:
        return (
                self.HEADER_SIZE
                + (len(self.key) if self.key is not None else 0)
                + (len(self.value) if self.value is not None else 0)
        )

    @classmethod
    def decode(cls, buf: bytes, pos: Position = Position(0)) -> Record:
        (offset, size) = struct.unpack_from(cls.PREFIX, buf, pos)
        if size > cls.MAX_SIZE:
            raise CorruptRecord(f"size {size} exceeds MAX_SIZE at pos {pos}")
        if pos + cls.PREFIX_SIZE + size > len(buf):
            raise CorruptRecord(
                f"record at pos {pos} claims {size} bytes, buffer has {len(buf) - pos - cls.PREFIX_SIZE}")

        (crc_expected,) = struct.unpack_from(">I", buf, pos + cls.PREFIX_SIZE)
        body_start = pos + cls.PREFIX_SIZE + 4
        body_len = struct.calcsize(cls.CRC_BODY)
        crc_body = buf[body_start:body_start + body_len]
        timestamp, klen, vlen = struct.unpack(cls.CRC_BODY, crc_body)

        if klen < cls.NULL or vlen < cls.NULL:
            raise CorruptRecord(f"negative length at pos {pos}: klen={klen}, vlen={vlen}")

        key_len = max(klen, 0)
        value_len = max(vlen, 0)

        # (A) the declared size and the field lengths must agree
        expected = 4 + body_len + key_len + value_len
        if expected != size:
            raise CorruptRecord(
                f"record at pos {pos}: size field says {size}, fields total {expected}"
            )

        key_start = pos + cls.HEADER_SIZE
        key_bytes = buf[key_start:key_start + key_len]
        value_bytes = buf[key_start + key_len:key_start + key_len + value_len]

        crc_actual = zlib.crc32(value_bytes, zlib.crc32(key_bytes, zlib.crc32(crc_body)))
        if crc_actual != crc_expected:
            raise CorruptRecord(f"crc mismatch at pos {pos}: {crc_actual:#x} != {crc_expected:#x}")

        # (B) -1 means absent, not empty — branch on the declared length, never the bytes
        key = key_bytes if klen != cls.NULL else None
        value = value_bytes if vlen != cls.NULL else None
        return cls(Offset(offset), timestamp, key, value)

    @classmethod
    def read_one(cls, f: BinaryIO) -> Record | None:
        prefix = f.read(cls.PREFIX_SIZE)
        if len(prefix) < cls.PREFIX_SIZE:
            return None
        _, size = struct.unpack(cls.PREFIX, prefix)
        if size > cls.MAX_SIZE:
            raise CorruptRecord(f"size {size} exceeds MAX_SIZE")
        rest = f.read(size)
        if len(rest) < size:
            return None
        return cls.decode(prefix + rest)
