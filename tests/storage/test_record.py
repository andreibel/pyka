import io
import struct

import pytest

from pyka.storage.record import CorruptRecord, Record
from pyka.storage.types import Offset


def a_record() -> Record:
    return Record(7, 1234, b"key", b"value")

@pytest.mark.parametrize("key", [None, b"", b"k"])
@pytest.mark.parametrize("value", [None, b"", b"v"])
def test_size(key, value):
    r = Record(0, 1234,key,value)
    assert r.size() == len(r.encode())


@pytest.mark.parametrize("key", [None, b"", b"k"])
@pytest.mark.parametrize("value", [None, b"", b"v"])
def test_round_trip(key, value):
  r = Record(7, 1234, key, value)
  assert Record.decode(r.encode()) == r


# --- corruption: every raise in decode() should have a test that fires it ---


@pytest.mark.parametrize("pos", [12, 16, 28, 33])
def test_bit_flip_anywhere_after_the_prefix_is_caught(pos):
    """crc + size cross-check together cover the whole record body.

    pos 12 = crc itself, 16 = timestamp, 28 = vlen, 33 = inside the value.
    """
    buf = bytearray(a_record().encode())
    buf[pos] ^= 0x01  # flip the lowest bit
    with pytest.raises(CorruptRecord):
        Record.decode(bytes(buf))


def test_size_field_disagreeing_with_lengths_is_caught():
    buf = bytearray(a_record().encode())
    # 27 instead of 28: still inside the buffer, so this gets past the bounds
    # check and reaches the cross-check rather than short-circuiting earlier.
    struct.pack_into(">I", buf, 8, len(buf) - Record.PREFIX_SIZE - 1)
    with pytest.raises(CorruptRecord, match="size field says"):
        Record.decode(bytes(buf))


def test_size_over_max_is_caught_before_allocating():
    buf = bytearray(a_record().encode())
    struct.pack_into(">I", buf, 8, Record.MAX_SIZE + 1)
    with pytest.raises(CorruptRecord, match="MAX_SIZE"):
        Record.decode(bytes(buf))


def test_negative_klen_is_caught():
    buf = bytearray(a_record().encode())
    struct.pack_into(">i", buf, 24, -5)  # klen lives at byte 24
    with pytest.raises(CorruptRecord, match="negative length"):
        Record.decode(bytes(buf))


def test_truncated_buffer_is_caught():
    buf = a_record().encode()
    with pytest.raises(CorruptRecord):
        Record.decode(buf[:-1])


def test_decode_at_offset():
    a, b = Record(0, 1, b"a", b"a"), Record(1, 2, b"b", b"b")
    buf = a.encode() + b.encode()
    assert Record.decode(buf, a.size()) == b


# --- read_one: None means "stream ended", CorruptRecord means "bytes are wrong" ---


def test_reads_records_in_order_then_stops():
    records = [Record(i, 1000 + i, b"k", b"v") for i in range(3)]
    f = io.BytesIO(b"".join(r.encode() for r in records))
    assert [Record.read_one(f) for _ in range(3)] == records
    # the stream must end exactly on the boundary, not one byte either side
    assert Record.read_one(f) is None


def test_empty_stream_is_none_not_an_error():
    assert Record.read_one(io.BytesIO(b"")) is None


@pytest.mark.parametrize("keep", [5, 12, 20])
def test_torn_tail_is_none(keep):
    """Crash mid-append: partial prefix (5), prefix only (12), partial body (20)."""
    f = io.BytesIO(a_record().encode()[:keep])
    assert Record.read_one(f) is None


def test_torn_tail_after_a_good_record_still_yields_the_good_one():
    good = a_record()
    f = io.BytesIO(good.encode() + good.encode()[:20])
    assert Record.read_one(f) == good
    assert Record.read_one(f) is None


def test_absurd_size_raises_before_allocating():
    """The socket defense: a 4-byte lie must not become a huge read()."""
    buf = bytearray(a_record().encode())
    struct.pack_into(">I", buf, 8, Record.MAX_SIZE + 1)
    with pytest.raises(CorruptRecord, match="MAX_SIZE"):
        Record.read_one(io.BytesIO(bytes(buf)))

# --------------------------------------------------------------------------
# the size limit — encode and read_one must be total inverses
# --------------------------------------------------------------------------


def test_a_record_over_max_size_cannot_be_encoded():
    """Found by audit, not by a test.

    read_one refuses any record claiming more than MAX_SIZE, so encoding one
    produced bytes nothing could read back — and since recovery reads too, a
    single oversized record made the whole segment unopenable. Silent, total,
    unrecoverable data loss from one oversized value.
    """
    too_big = b"x" * (Record.MAX_SIZE + 1)
    with pytest.raises(ValueError, match="over the .* limit"):
        Record(Offset(0), 1, None, too_big).encode()


def test_the_largest_encodable_record_still_round_trips():
    # The boundary itself must work, or the limit is off by the header.
    overhead = Record.HEADER_SIZE - Record.PREFIX_SIZE  # counted by `size`
    value = b"x" * (Record.MAX_SIZE - overhead)
    record = Record(Offset(0), 1, None, value)

    buf = record.encode()
    assert Record.decode(buf) == record
    assert Record.read_one(io.BytesIO(buf)) == record


def test_the_limit_counts_key_and_value_together():
    half = (Record.MAX_SIZE // 2) + 100
    with pytest.raises(ValueError, match="over the .* limit"):
        Record(Offset(0), 1, b"k" * half, b"v" * half).encode()
