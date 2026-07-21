"""Index tests: format, sparsity, lookup.

Deliberately imports neither Record nor Segment. The index is fed offsets and
positions and knows nothing about the log they came from — if a test here needs
a Segment to set it up, the layering has leaked.
"""

import struct

import pytest

from pyka.storage.index import ENTRY, ENTRY_SIZE, MAX_U32, Index
from pyka.storage.types import Offset, Position

BASE = 417  # never 0 — catches code that forgets to subtract the base offset
INTERVAL = 4096


def idx(tmp_path, base: int = BASE, interval: int = INTERVAL) -> Index:
    return Index(tmp_path / "test.index", Offset(base), interval)


def with_entries(tmp_path, pairs: list[tuple[int, int]]) -> Index:
    """Build an index holding exactly ``pairs``.

    Positions must be interval-spaced or maybe_append will drop them — which is
    the point of the method, so the tests spell the spacing out rather than
    reaching past it.
    """
    ix = idx(tmp_path)
    for offset, position in pairs:
        ix.maybe_append(Offset(offset), Position(position))
    assert len(ix) == len(pairs), "setup itself was filtered by the sparsity rule"
    return ix


# --------------------------------------------------------------------------
# format
# --------------------------------------------------------------------------


def test_a_new_index_is_empty(tmp_path):
    ix = idx(tmp_path)
    assert len(ix) == 0
    assert ix.last_entry() is None


def test_the_file_is_created_if_it_does_not_exist(tmp_path):
    idx(tmp_path)
    assert (tmp_path / "test.index").exists()


def test_entry_size_is_eight_bytes(tmp_path):
    # Load-bearing: 4096 / 8 == 512 exactly, so no entry straddles a page.
    assert ENTRY_SIZE == 8


def test_one_entry_is_one_entry_size_on_disk(tmp_path):
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE + 10), Position(INTERVAL))
    ix.sync()
    assert (tmp_path / "test.index").stat().st_size == ENTRY_SIZE


def test_offsets_are_stored_relative_to_base_offset(tmp_path):
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE + 10), Position(INTERVAL))
    ix.sync()

    raw = (tmp_path / "test.index").read_bytes()
    rel_off, position = struct.unpack(ENTRY, raw)
    assert (rel_off, position) == (10, INTERVAL)  # 10, not BASE + 10


def test_entry_n_lands_at_byte_n_times_entry_size(tmp_path):
    ix = with_entries(tmp_path, [(BASE + 1, INTERVAL), (BASE + 2, 2 * INTERVAL)])
    ix.sync()

    raw = (tmp_path / "test.index").read_bytes()
    assert struct.unpack_from(ENTRY, raw, 0) == (1, INTERVAL)
    assert struct.unpack_from(ENTRY, raw, ENTRY_SIZE) == (2, 2 * INTERVAL)


def test_len_tracks_the_entry_count(tmp_path):
    ix = idx(tmp_path)
    for n in range(1, 4):
        ix.maybe_append(Offset(BASE + n), Position(n * INTERVAL))
        assert len(ix) == n


def test_last_entry_is_absolute_not_relative(tmp_path):
    ix = with_entries(tmp_path, [(BASE + 1, INTERVAL), (BASE + 9, 3 * INTERVAL)])
    assert ix.last_entry() == (BASE + 9, 3 * INTERVAL)


# --------------------------------------------------------------------------
# sparsity — the whole point is that most calls do nothing
# --------------------------------------------------------------------------


@pytest.mark.parametrize("position", [0, 1, 100, INTERVAL - 1])
def test_nothing_is_written_below_the_interval(tmp_path, position):
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE), Position(position))
    assert len(ix) == 0


def test_the_record_at_position_zero_is_never_indexed(tmp_path):
    # Not a special case: an empty index already means "start at byte 0", which
    # is exactly what lookup falls back to. Entry zero is implicit.
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE), Position(0))
    assert len(ix) == 0
    assert ix.lookup(Offset(BASE)) == (BASE, 0)


def test_written_at_exactly_the_interval(tmp_path):
    # The boundary: >= not >. One off here and every entry lands a record late.
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE + 5), Position(INTERVAL))
    assert len(ix) == 1


def test_one_entry_per_interval_not_per_record(tmp_path):
    ix = idx(tmp_path)
    # 40 records spanning two intervals: positions 0, 512, 1024 ... 19968
    for n in range(40):
        ix.maybe_append(Offset(BASE + n), Position(n * 512))
    assert len(ix) == 4  # at 4096, 8192, 12288, 16384


def test_a_single_jump_past_several_intervals_writes_only_one_entry(tmp_path):
    # A record bigger than the interval does not backfill the gap it skipped.
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE + 1), Position(10 * INTERVAL))
    assert len(ix) == 1


def test_the_interval_is_measured_from_the_last_entry_not_the_last_call(tmp_path):
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE + 1), Position(INTERVAL))  # written
    ix.maybe_append(Offset(BASE + 2), Position(INTERVAL + 10))  # too close
    ix.maybe_append(Offset(BASE + 3), Position(2 * INTERVAL))  # written
    assert [p for _, p in ix._entries] == [INTERVAL, 2 * INTERVAL]


def test_offsets_must_increase(tmp_path):
    ix = with_entries(tmp_path, [(BASE + 10, INTERVAL)])
    with pytest.raises(ValueError, match="must increase"):
        ix.maybe_append(Offset(BASE + 9), Position(2 * INTERVAL))


def test_an_offset_beyond_u32_is_rejected(tmp_path):
    # The 4 GiB cap the entry format implies, made explicit.
    ix = idx(tmp_path)
    with pytest.raises(ValueError, match="u32"):
        ix.maybe_append(Offset(BASE + 2**32), Position(INTERVAL))


# --------------------------------------------------------------------------
# lookup — floor, never ceiling: you can only scan forward
# --------------------------------------------------------------------------


def test_empty_index_returns_the_start_of_the_segment(tmp_path):
    assert idx(tmp_path).lookup(Offset(BASE + 999)) == (BASE, 0)


def test_an_offset_before_the_first_entry_returns_the_start(tmp_path):
    ix = with_entries(tmp_path, [(BASE + 100, INTERVAL)])
    assert ix.lookup(Offset(BASE + 50)) == (BASE, 0)


def test_an_offset_below_base_returns_the_start_and_does_not_raise(tmp_path):
    # lookup is total: it is a hint, and the caller verifies whatever it gets.
    ix = with_entries(tmp_path, [(BASE + 100, INTERVAL)])
    assert ix.lookup(Offset(BASE - 300)) == (BASE, 0)


def test_an_exact_hit_returns_that_entry(tmp_path):
    # bisect_left would return the entry BEFORE this one — still a usable
    # answer, just a needlessly longer scan, which is why it needs its own test.
    ix = with_entries(
        tmp_path, [(BASE + 100, INTERVAL), (BASE + 200, 2 * INTERVAL)]
    )
    assert ix.lookup(Offset(BASE + 200)) == (BASE + 200, 2 * INTERVAL)


def test_between_two_entries_returns_the_lower_one(tmp_path):
    ix = with_entries(
        tmp_path, [(BASE + 100, INTERVAL), (BASE + 200, 2 * INTERVAL)]
    )
    assert ix.lookup(Offset(BASE + 150)) == (BASE + 100, INTERVAL)


def test_past_the_last_entry_returns_the_last_entry(tmp_path):
    # The offset may not even exist. The index still hands back the best start
    # available and lets the caller scan and find nothing.
    ix = with_entries(
        tmp_path, [(BASE + 100, INTERVAL), (BASE + 200, 2 * INTERVAL)]
    )
    assert ix.lookup(Offset(BASE + 9999)) == (BASE + 200, 2 * INTERVAL)


def test_lookup_returns_the_true_floor_for_every_offset_in_range(tmp_path):
    """Brute force is the oracle: for each offset, the answer bisect gives must
    equal the greatest entry <= it, found by scanning.

    This is the test that actually catches an off-by-one in the search — the
    individual cases above can all pass while one boundary is wrong.
    """
    pairs = [(BASE + 100 * n, INTERVAL * n) for n in range(1, 9)]
    ix = with_entries(tmp_path, pairs)

    for offset in range(BASE - 10, BASE + 900):
        floor = [p for p in pairs if p[0] <= offset]
        expected = floor[-1] if floor else (BASE, 0)
        assert ix.lookup(Offset(offset)) == expected, f"wrong floor for {offset}"


def test_a_position_beyond_u32_is_rejected(tmp_path):
    """Found by audit. The offset was checked, the position was not — so
    struct.pack raised a bare struct.error from inside the index, about a
    segment that had grown too big, with a message pointing nowhere near the
    cause. Segment now refuses such a max_bytes up front; this is the
    backstop."""
    ix = idx(tmp_path)
    with pytest.raises(ValueError, match="outside the u32"):
        ix.maybe_append(Offset(BASE + 1), Position(MAX_U32 + 1))


def test_the_largest_legal_position_is_accepted(tmp_path):
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE + 1), Position(MAX_U32))
    assert ix.last_entry() == (BASE + 1, MAX_U32)
