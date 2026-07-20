import struct
from pathlib import Path

import pytest

from pyka.storage.record import CorruptRecord, Record
from pyka.storage.segment import Segment

BASE = 417  # deliberately not 0 — catches code that assumes segments start at zero


def rec(offset: int) -> Record:
    return Record(offset, 1000 + offset, b"key", b"value")


def log_path(d: Path, base: int = BASE) -> Path:
    return d / f"{base:020d}.log"


def write_log(d: Path, records: list[Record], tail: bytes = b"", base: int = BASE) -> Path:
    """Lay bytes down directly, bypassing Segment — this is 'what a crash left'."""
    p = log_path(d, base)
    p.write_bytes(b"".join(r.encode() for r in records) + tail)
    return p


REC_SIZE = rec(0).size()


# --- naming ------------------------------------------------------------------


def test_filename_is_zero_padded_to_20(tmp_path):
    Segment(tmp_path, 42)
    assert (tmp_path / "00000000000000000042.log").exists()


def test_filenames_sort_in_numeric_order(tmp_path):
    """Log finds a segment by sorting filenames, so lexical order must equal numeric."""
    for base in (0, 9, 10, 100, 1000):
        Segment(tmp_path, base)
    names = sorted(p.name for p in tmp_path.glob("*.log"))
    assert names == [f"{b:020d}.log" for b in (0, 9, 10, 100, 1000)]


# --- _recover: nothing to recover --------------------------------------------


def test_fresh_directory_creates_the_file(tmp_path):
    s = Segment(tmp_path, BASE)
    assert log_path(tmp_path).exists()
    assert (s._next_offset, s._position) == (BASE, 0)


def test_empty_file_recovers_to_base(tmp_path):
    log_path(tmp_path).touch()
    s = Segment(tmp_path, BASE)
    assert (s._next_offset, s._position) == (BASE, 0)


@pytest.mark.parametrize("base", [0, 1, 417, 999999])
def test_next_offset_starts_at_base_offset_not_zero(tmp_path, base):
    s = Segment(tmp_path, base)
    assert s._next_offset == base


# --- _recover: clean files ----------------------------------------------------


@pytest.mark.parametrize("n", [1, 2, 3, 10])
def test_clean_file_recovers_full_state(tmp_path, n):
    write_log(tmp_path, [rec(BASE + i) for i in range(n)])
    s = Segment(tmp_path, BASE)
    assert (s._next_offset, s._position) == (BASE + n, n * REC_SIZE)


def test_clean_file_is_not_truncated(tmp_path):
    p = write_log(tmp_path, [rec(BASE + i) for i in range(3)])
    before = p.stat().st_size
    Segment(tmp_path, BASE)
    assert p.stat().st_size == before


# --- _recover: torn tails (a crash mid-append) --------------------------------


@pytest.mark.parametrize("keep", [1, 5, 11, 12, 13, 20, 33])
def test_torn_tail_is_truncated_away(tmp_path, keep):
    """Every truncation point of a 4th record: partial prefix, full prefix, partial body."""
    good = [rec(BASE + i) for i in range(3)]
    p = write_log(tmp_path, good, tail=rec(BASE + 3).encode()[:keep])
    s = Segment(tmp_path, BASE)
    assert (s._next_offset, s._position) == (BASE + 3, 3 * REC_SIZE)
    assert p.stat().st_size == 3 * REC_SIZE  # garbage is gone from disk


def test_torn_first_record_leaves_an_empty_segment(tmp_path):
    write_log(tmp_path, [], tail=rec(BASE).encode()[:20])
    s = Segment(tmp_path, BASE)
    assert (s._next_offset, s._position) == (BASE, 0)
    assert log_path(tmp_path).stat().st_size == 0


def test_truncation_is_required_or_the_next_append_embeds_a_hole(tmp_path):
    """The reason _recover truncates rather than just stopping."""
    write_log(tmp_path, [rec(BASE)], tail=rec(BASE + 1).encode()[:20])
    s = Segment(tmp_path, BASE)
    s.append(rec(BASE + 1))

    reopened = Segment(tmp_path, BASE)
    assert reopened._next_offset == BASE + 2  # both records readable, no hole


# --- _recover: corruption is not a torn tail ----------------------------------


@pytest.mark.parametrize("flip", [16, 20, 24, 28, 32])
def test_bit_flip_in_the_crc_covered_region_raises(tmp_path, flip):
    """crc, timestamp, klen, vlen and payload all sit under the checksum."""
    blob = bytearray(rec(BASE).encode() + rec(BASE + 1).encode())
    blob[flip] ^= 0x01
    log_path(tmp_path).write_bytes(bytes(blob))
    with pytest.raises(CorruptRecord):
        Segment(tmp_path, BASE)


def test_bit_flip_in_an_offset_field_raises_via_continuity(tmp_path):
    """offset sits OUTSIDE the crc, so only the sequence check catches this."""
    blob = bytearray(b"".join(rec(BASE + i).encode() for i in range(3)))
    blob[REC_SIZE + 6] ^= 0x01  # offset field of record #2
    log_path(tmp_path).write_bytes(bytes(blob))
    with pytest.raises(CorruptRecord, match="offset gap"):
        Segment(tmp_path, BASE)


def test_first_record_not_matching_base_offset_raises(tmp_path):
    """A segment file whose contents belong to a different segment."""
    write_log(tmp_path, [rec(BASE + 50)])
    with pytest.raises(CorruptRecord, match="offset gap"):
        Segment(tmp_path, BASE)


def test_absurd_size_field_raises_rather_than_allocating(tmp_path):
    blob = bytearray(rec(BASE).encode())
    struct.pack_into(">I", blob, 8, Record.MAX_SIZE + 1)
    log_path(tmp_path).write_bytes(bytes(blob))
    with pytest.raises(CorruptRecord, match="MAX_SIZE"):
        Segment(tmp_path, BASE)


# --- append -------------------------------------------------------------------


def test_append_returns_the_start_position_of_each_record(tmp_path):
    s = Segment(tmp_path, BASE)
    positions = [s.append(rec(BASE + i)) for i in range(3)]
    assert positions == [0, REC_SIZE, 2 * REC_SIZE]


def test_append_advances_state(tmp_path):
    s = Segment(tmp_path, BASE)
    s.append(rec(BASE))
    assert (s._next_offset, s._position) == (BASE + 1, REC_SIZE)


@pytest.mark.parametrize("bad", [BASE - 1, BASE + 1, 0, 999999])
def test_append_rejects_a_non_sequential_offset(tmp_path, bad):
    s = Segment(tmp_path, BASE)
    with pytest.raises(ValueError, match="expected offset"):
        s.append(rec(bad))


def test_rejected_append_does_not_advance_state(tmp_path):
    s = Segment(tmp_path, BASE)
    with pytest.raises(ValueError):
        s.append(rec(BASE + 5))
    assert (s._next_offset, s._position) == (BASE, 0)
    assert log_path(tmp_path).stat().st_size == 0


def test_append_flushes_so_a_separate_reader_can_see_it(tmp_path):
    """Without flush() the record sits in Python's buffer and readers miss it."""
    s = Segment(tmp_path, BASE)
    s.append(rec(BASE))
    with open(log_path(tmp_path), "rb") as f:
        assert Record.read_one(f) == rec(BASE)


@pytest.mark.parametrize(
    "key,value",
    [(None, b"v"), (b"", b"v"), (b"k", None), (b"k", b""), (None, None)],
)
def test_append_handles_null_keys_and_tombstones(tmp_path, key, value):
    s = Segment(tmp_path, BASE)
    r = Record(BASE, 1234, key, value)
    s.append(r)
    with open(log_path(tmp_path), "rb") as f:
        assert Record.read_one(f) == r


# --- append + recover together ------------------------------------------------


@pytest.mark.parametrize("n", [1, 5, 20])
def test_append_then_reopen_recovers_identical_state(tmp_path, n):
    s = Segment(tmp_path, BASE)
    for i in range(n):
        s.append(rec(BASE + i))
    before = (s._next_offset, s._position)

    reopened = Segment(tmp_path, BASE)
    assert (reopened._next_offset, reopened._position) == before


def test_appends_survive_across_two_reopens(tmp_path):
    Segment(tmp_path, BASE).append(rec(BASE))
    Segment(tmp_path, BASE).append(rec(BASE + 1))
    s = Segment(tmp_path, BASE)
    assert s._next_offset == BASE + 2


def test_every_appended_record_reads_back_in_order(tmp_path):
    s = Segment(tmp_path, BASE)
    written = [rec(BASE + i) for i in range(5)]
    for r in written:
        s.append(r)

    read_back = []
    with open(log_path(tmp_path), "rb") as f:
        while (r := Record.read_one(f)) is not None:
            read_back.append(r)
    assert read_back == written


# --- read_from ----------------------------------------------------------------


def filled(tmp_path, n=5) -> Segment:
    s = Segment(tmp_path, BASE)
    for i in range(n):
        s.append(rec(BASE + i))
    return s


@pytest.mark.parametrize("start", range(5))
def test_read_from_yields_the_tail_beginning_at_offset(tmp_path, start):
    s = filled(tmp_path)
    got = [r.offset for r in s.read_from(BASE + start)]
    assert got == list(range(BASE + start, BASE + 5))


def test_read_from_base_yields_everything(tmp_path):
    s = filled(tmp_path)
    assert [r.offset for r in s.read_from(BASE)] == [BASE + i for i in range(5)]


def test_read_from_yields_whole_records_not_just_offsets(tmp_path):
    s = filled(tmp_path)
    assert list(s.read_from(BASE)) == [rec(BASE + i) for i in range(5)]


def test_read_from_next_offset_is_empty_not_an_error(tmp_path):
    """The caught-up consumer — hit constantly in phase B, must be cheap and quiet."""
    s = filled(tmp_path)
    assert list(s.read_from(s._next_offset)) == []


@pytest.mark.parametrize("beyond", [1, 10, 10_000])
def test_read_from_past_the_end_is_empty(tmp_path, beyond):
    s = filled(tmp_path)
    assert list(s.read_from(s._next_offset + beyond)) == []


def test_read_from_an_empty_segment_is_empty(tmp_path):
    s = Segment(tmp_path, BASE)
    assert list(s.read_from(BASE)) == []


@pytest.mark.parametrize("below", [0, 1, BASE - 1])
def test_read_from_below_base_offset_raises(tmp_path, below):
    s = filled(tmp_path)
    with pytest.raises(ValueError, match="out of range"):
        s.read_from(below)


def test_read_from_validates_eagerly_not_on_first_next(tmp_path):
    """read_from must not be a generator function, or the check fires late.

    A generator body does not run until iteration, so a bad offset would
    surface somewhere unrelated to the caller that got it wrong.
    """
    s = filled(tmp_path)
    with pytest.raises(ValueError):
        s.read_from(0)  # no iteration at all


def test_read_from_stops_at_a_torn_tail(tmp_path):
    good = [rec(BASE + i) for i in range(3)]
    write_log(tmp_path, good, tail=rec(BASE + 3).encode()[:20])
    s = Segment(tmp_path, BASE)
    assert list(s.read_from(BASE)) == good


def test_read_from_sees_records_appended_after_the_iterator_was_made(tmp_path):
    """Each reader opens its own handle, lazily — this is what live tail needs."""
    s = filled(tmp_path, n=2)
    it = s.read_from(BASE)
    s.append(rec(BASE + 2))
    assert [r.offset for r in it] == [BASE, BASE + 1, BASE + 2]


def test_readers_do_not_disturb_the_write_position(tmp_path):
    s = filled(tmp_path, n=3)
    before = s._position
    list(s.read_from(BASE))
    s.append(rec(BASE + 3))
    assert s._position == before + REC_SIZE


def test_two_readers_are_independent(tmp_path):
    s = filled(tmp_path)
    a, b = s.read_from(BASE), s.read_from(BASE + 3)
    next(a)
    assert [r.offset for r in b] == [BASE + 3, BASE + 4]
    assert [r.offset for r in a] == [BASE + i for i in range(1, 5)]


# --- has_room_for / is_full ---------------------------------------------------


def test_empty_segment_has_room(tmp_path):
    s = Segment(tmp_path, BASE, max_bytes=1000)
    assert s.has_room_for(rec(BASE))


@pytest.mark.parametrize("n_before", [1, 2, 3])
def test_has_room_while_under_the_limit(tmp_path, n_before):
    s = Segment(tmp_path, BASE, max_bytes=10 * REC_SIZE)
    for i in range(n_before):
        s.append(rec(BASE + i))
    assert s.has_room_for(rec(BASE + n_before))


def test_a_record_that_exactly_fills_the_segment_fits(tmp_path):
    """<= not <: filling to exactly max_bytes must be allowed, or we roll early forever."""
    s = Segment(tmp_path, BASE, max_bytes=3 * REC_SIZE)
    for i in range(2):
        s.append(rec(BASE + i))
    assert s.has_room_for(rec(BASE + 2))  # lands on exactly max_bytes


def test_one_byte_over_the_limit_does_not_fit(tmp_path):
    s = Segment(tmp_path, BASE, max_bytes=3 * REC_SIZE - 1)
    for i in range(2):
        s.append(rec(BASE + i))
    assert not s.has_room_for(rec(BASE + 2))


def test_empty_segment_accepts_a_record_larger_than_max_bytes(tmp_path):
    """Otherwise Log rolls forever and never stores the record."""
    s = Segment(tmp_path, BASE, max_bytes=1)
    assert s.has_room_for(rec(BASE))
    s.append(rec(BASE))
    assert s._position == REC_SIZE  # over the limit, and that is correct


def test_rolling_terminates_when_max_bytes_is_smaller_than_a_record(tmp_path):
    """The infinite-loop guard, exercised the way Log will drive it."""
    written = 0
    for i in range(5):
        d = tmp_path / f"seg{i}"
        d.mkdir()
        # a real roll names the new segment after the offset it starts at
        s = Segment(d, BASE + i, max_bytes=1)
        assert s.has_room_for(rec(BASE + i))  # never refuses on an empty segment
        s.append(rec(BASE + i))
        written += 1
    assert written == 5


def test_max_bytes_is_a_soft_limit(tmp_path):
    """A segment may exceed max_bytes by up to one record."""
    s = Segment(tmp_path, BASE, max_bytes=2 * REC_SIZE)
    for i in range(2):
        s.append(rec(BASE + i))
    assert not s.has_room_for(rec(BASE + 2))
    assert s._position <= 2 * REC_SIZE + REC_SIZE - 1


def test_is_full_tracks_the_threshold(tmp_path):
    s = Segment(tmp_path, BASE, max_bytes=2 * REC_SIZE)
    assert not s.is_full()
    s.append(rec(BASE))
    assert not s.is_full()
    s.append(rec(BASE + 1))
    assert s.is_full()


def test_has_room_survives_a_reopen(tmp_path):
    s = Segment(tmp_path, BASE, max_bytes=2 * REC_SIZE)
    s.append(rec(BASE))
    s.close()

    reopened = Segment(tmp_path, BASE, max_bytes=2 * REC_SIZE)
    assert reopened.has_room_for(rec(BASE + 1))
    reopened.append(rec(BASE + 1))
    assert not reopened.has_room_for(rec(BASE + 2))


# --- context manager ----------------------------------------------------------


def test_with_block_closes_on_normal_exit(tmp_path):
    with Segment(tmp_path, BASE) as s:
        s.append(rec(BASE))
    assert s._file.closed


def test_with_block_closes_when_the_body_raises(tmp_path):
    s = None
    with pytest.raises(RuntimeError, match="boom"):
        with Segment(tmp_path, BASE) as s:
            s.append(rec(BASE))
            raise RuntimeError("boom")
    assert s._file.closed


def test_exit_does_not_swallow_exceptions(tmp_path):
    """__exit__ must return falsy — returning True would silently eat errors."""
    with pytest.raises(ValueError):
        with Segment(tmp_path, BASE) as s:
            s.append(rec(BASE + 99))  # wrong offset


def test_enter_returns_the_segment_itself(tmp_path):
    seg = Segment(tmp_path, BASE)
    with seg as bound:
        assert bound is seg


def test_data_written_inside_a_with_block_is_durable(tmp_path):
    with Segment(tmp_path, BASE) as s:
        for i in range(3):
            s.append(rec(BASE + i))

    reopened = Segment(tmp_path, BASE)
    assert reopened._next_offset == BASE + 3


# --- close / sync -------------------------------------------------------------


def test_close_releases_the_handle(tmp_path):
    s = Segment(tmp_path, BASE)
    s.close()
    assert s._file.closed


def test_close_is_idempotent(tmp_path):
    """Log closes segments on roll and again on shutdown — double close must be safe."""
    s = Segment(tmp_path, BASE)
    s.close()
    s.close()  # must not raise


def test_close_preserves_appended_data(tmp_path):
    s = Segment(tmp_path, BASE)
    written = [rec(BASE + i) for i in range(3)]
    for r in written:
        s.append(r)
    s.close()

    reopened = Segment(tmp_path, BASE)
    assert (reopened._next_offset, reopened._position) == (BASE + 3, 3 * REC_SIZE)


def test_append_after_close_raises(tmp_path):
    s = Segment(tmp_path, BASE)
    s.close()
    with pytest.raises(ValueError):  # "I/O operation on closed file"
        s.append(rec(BASE))


def test_sync_makes_bytes_visible_on_disk(tmp_path):
    """sync() must flush Python's buffer first — fsync alone syncs nothing."""
    s = Segment(tmp_path, BASE)
    s.append(rec(BASE))
    s.sync()
    assert log_path(tmp_path).stat().st_size == REC_SIZE


def test_sync_flushes_pythons_buffer_before_fsync(tmp_path):
    """os.fsync only pushes OS -> disk; it cannot see Python's buffer.

    Writes past append() on purpose, because append() flushes and would mask
    the bug. If the per-append flush is ever dropped as an optimisation, this
    is the test that stops sync() from silently becoming a no-op.
    """
    s = Segment(tmp_path, BASE)
    s._file.write(rec(BASE).encode())  # buffered, not flushed
    s.sync()
    assert log_path(tmp_path).stat().st_size == REC_SIZE


def test_sync_is_safe_to_call_repeatedly(tmp_path):
    s = Segment(tmp_path, BASE)
    s.append(rec(BASE))
    s.sync()
    s.sync()
    assert log_path(tmp_path).stat().st_size == REC_SIZE


def test_sync_on_an_empty_segment_does_not_raise(tmp_path):
    Segment(tmp_path, BASE).sync()


def test_position_returned_by_append_is_where_the_record_actually_starts(tmp_path):
    """This is the number Index will store — it must be seekable."""
    s = Segment(tmp_path, BASE)
    positions = [s.append(rec(BASE + i)) for i in range(4)]

    with open(log_path(tmp_path), "rb") as f:
        for i, pos in enumerate(positions):
            f.seek(pos)
            assert Record.read_one(f) == rec(BASE + i)