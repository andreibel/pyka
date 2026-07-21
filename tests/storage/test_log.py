"""Log tests: appending, rolling, and reads that cross segment boundaries.

The log under test rolls every 3 records (86-byte records, 300-byte cap), so
a handful of appends exercises the chain machinery a production log would
need gigabytes to reach.
"""

import time

import pytest

from pyka.storage.log import Log
from pyka.storage.record import Record
from pyka.storage.types import Offset

MAX_BYTES = 300
VALUE = b"v" * 50
TS = 1_700_000_000_000

# key f"{n:04d}" (4 bytes) + VALUE (50) + header (32) = 86 bytes per record:
# 3 fit under MAX_BYTES (258), a 4th would hit 344 — so segments hold 3.
PER_SEGMENT = 3


def key(n: int) -> bytes:
    return f"{n:04d}".encode()


def open_log(tmp_path) -> Log:
    return Log(tmp_path / "topic-0", max_segment_bytes=MAX_BYTES)


def fill(log: Log, count: int) -> list[Record]:
    """Append ``count`` records with deterministic fields, return the expected
    Record objects — frozen dataclass equality makes whole-record asserts."""
    expected = []
    for n in range(int(log.next_offset), int(log.next_offset) + count):
        log.append(key(n), VALUE, timestamp=TS + n)
        expected.append(Record(Offset(n), TS + n, key(n), VALUE))
    return expected


def log_files(tmp_path) -> list[str]:
    return sorted(p.name for p in (tmp_path / "topic-0").glob("*.log"))


# --------------------------------------------------------------------------
# opening
# --------------------------------------------------------------------------


def test_a_new_log_creates_its_directory(tmp_path):
    nested = tmp_path / "a" / "b" / "topic-0"
    Log(nested)
    assert nested.is_dir()


def test_a_new_log_starts_empty_with_one_segment_at_zero(tmp_path):
    log = open_log(tmp_path)
    assert log.next_offset == 0
    assert list(log.read_from(Offset(0))) == []
    assert log_files(tmp_path) == ["00000000000000000000.log"]


# --------------------------------------------------------------------------
# appending
# --------------------------------------------------------------------------


def test_append_returns_sequential_offsets(tmp_path):
    log = open_log(tmp_path)
    assert [log.append(key(n), VALUE) for n in range(5)] == [0, 1, 2, 3, 4]
    assert log.next_offset == 5


def test_append_stamps_offset_timestamp_key_and_value(tmp_path):
    log = open_log(tmp_path)
    log.append(b"k", b"payload", timestamp=TS)
    assert list(log.read_from(Offset(0))) == [Record(Offset(0), TS, b"k", b"payload")]


def test_timestamp_defaults_to_now_in_epoch_millis(tmp_path):
    log = open_log(tmp_path)
    before = time.time_ns() // 1_000_000
    log.append(b"k", VALUE)
    after = time.time_ns() // 1_000_000

    (rec,) = log.read_from(Offset(0))
    assert before <= rec.timestamp <= after


def test_tombstones_and_null_keys_pass_through(tmp_path):
    log = open_log(tmp_path)
    log.append(None, VALUE, timestamp=TS)     # no key: round-robin, no compact
    log.append(b"gone", None, timestamp=TS)   # no value: tombstone
    a, b = log.read_from(Offset(0))
    assert (a.key, a.value) == (None, VALUE)
    assert (b.key, b.value) == (b"gone", None)


# --------------------------------------------------------------------------
# rolling
# --------------------------------------------------------------------------


def test_the_log_rolls_when_a_segment_fills(tmp_path):
    log = open_log(tmp_path)
    fill(log, 10)  # 3 + 3 + 3 + 1
    assert log_files(tmp_path) == [
        "00000000000000000000.log",
        "00000000000000000003.log",
        "00000000000000000006.log",
        "00000000000000000009.log",
    ]


def test_offsets_keep_counting_across_a_roll(tmp_path):
    # The whole point of the chain: segment boundaries are invisible in the
    # offset sequence. A reset here would be a new log, not a new segment.
    log = open_log(tmp_path)
    fill(log, 10)
    assert [r.offset for r in log.read_from(Offset(0))] == list(range(10))


def test_rolling_seals_the_previous_segment(tmp_path):
    log = open_log(tmp_path)
    fill(log, PER_SEGMENT + 1)  # forces one roll
    sealed, active = log._segments
    # sealed = closed: no write handle left, so "only the tail is writable"
    # is physical, not an if-check
    assert sealed._file.closed
    assert not active._file.closed


def test_rolling_an_empty_segment_does_nothing(tmp_path):
    """Found by running the broker, not by a test.

    A new segment is named for the next offset. Roll an empty one and that
    offset has not moved, so the "new" segment takes the SAME filename — two
    Segment objects writing one file, and a chain listing segments that have
    no file of their own. An empty segment is already fresh.
    """
    log = open_log(tmp_path)
    assert log.roll() is log._active
    assert len(log.segments) == 1
    assert log_files(tmp_path) == ["00000000000000000000.log"]

    fill(log, 1)
    log.roll()
    log.roll()  # second one is a no-op: the new segment is empty
    assert len(log.segments) == 2
    assert len({s.base_offset for s in log.segments}) == 2  # no duplicate bases


def test_rolling_seals_and_keeps_the_chain_continuous(tmp_path):
    log = open_log(tmp_path)
    fill(log, 2)
    sealed_at = log.next_offset

    new_tail = log.roll()
    assert new_tail.base_offset == sealed_at
    assert log.segments[-2].sealed and not new_tail.sealed
    assert [r.offset for r in log.read_from(Offset(0))] == [0, 1]

    fill(log, 1)  # keeps counting into the new segment
    assert [r.offset for r in log.read_from(Offset(0))] == [0, 1, 2]


def test_a_record_bigger_than_max_bytes_still_lands(tmp_path):
    # max_bytes is a SOFT limit: an empty segment accepts anything, otherwise
    # an oversized record would roll forever, filling the disk with empty files.
    log = open_log(tmp_path)
    fill(log, PER_SEGMENT)                      # fill segment 0 exactly
    big = b"x" * (2 * MAX_BYTES)
    log.append(key(3), big, timestamp=TS)       # rolls, then lands alone

    records = list(log.read_from(Offset(0)))
    assert records[-1].value == big
    assert len(log_files(tmp_path)) == 2


# --------------------------------------------------------------------------
# reading across the chain
# --------------------------------------------------------------------------


def test_read_from_zero_returns_every_record_in_order(tmp_path):
    log = open_log(tmp_path)
    expected = fill(log, 10)
    assert list(log.read_from(Offset(0))) == expected


def test_read_from_every_offset_matches_a_python_slice(tmp_path):
    """The floor-search differential: for every start offset, the log must
    return exactly expected[offset:] — one bisect off and some boundary
    returns a record too many or too few."""
    log = open_log(tmp_path)
    expected = fill(log, 10)
    for n in range(10):
        assert list(log.read_from(Offset(n))) == expected[n:], f"wrong tail from {n}"


def test_read_from_a_segment_base_crosses_into_later_segments(tmp_path):
    log = open_log(tmp_path)
    expected = fill(log, 10)
    # offset 3 is exactly the second segment's base
    assert list(log.read_from(Offset(PER_SEGMENT))) == expected[PER_SEGMENT:]


def test_read_past_the_end_yields_nothing_and_does_not_raise(tmp_path):
    log = open_log(tmp_path)
    fill(log, 4)
    assert list(log.read_from(Offset(4))) == []    # the head: next to arrive
    assert list(log.read_from(Offset(999))) == []


def test_read_before_the_start_raises_eagerly(tmp_path):
    log = open_log(tmp_path)
    fill(log, 4)
    with pytest.raises(ValueError, match="before this log starts"):
        log.read_from(Offset(-1))  # raises HERE, not at first next()


def test_a_closed_log_still_serves_reads(tmp_path):
    # close() drops write handles only; every read opens its own. This is the
    # same property that lets sealed segments serve reads mid-chain.
    log = open_log(tmp_path)
    expected = fill(log, 10)
    log.close()
    assert list(log.read_from(Offset(0))) == expected


def test_the_log_is_a_context_manager(tmp_path):
    with Log(tmp_path / "topic-0", max_segment_bytes=MAX_BYTES) as log:
        fill(log, 4)
    assert log._active._file.closed
