"""Index tests: damage, clear, durability, round trip.

Everything here is about the index surviving a crash — or rather, about it not
needing to. It is derived data, so the bar is "loads without lying", not "loses
nothing".
"""

import struct

import pytest

from pyka.storage.index import ENTRY, ENTRY_SIZE, Index
from pyka.storage.types import Offset, Position

BASE = 417
INTERVAL = 4096


def idx(tmp_path, base: int = BASE, interval: int = INTERVAL) -> Index:
    return Index(tmp_path / "test.index", Offset(base), interval)


def write_raw(tmp_path, pairs: list[tuple[int, int]], tail: bytes = b""):
    """Lay entry bytes down directly — this is 'what a crash left behind'.

    Pairs are RELATIVE offsets, because that is what is on disk.
    """
    p = tmp_path / "test.index"
    p.write_bytes(b"".join(struct.pack(ENTRY, r, pos) for r, pos in pairs) + tail)
    return p


# --------------------------------------------------------------------------
# a ragged tail — the file is not a whole number of entries
# --------------------------------------------------------------------------


@pytest.mark.parametrize("tail", [b"\x00", b"ab", b"\xff" * 7])
def test_a_ragged_tail_is_dropped_on_open(tmp_path, tail):
    write_raw(tmp_path, [(100, INTERVAL), (200, 2 * INTERVAL)], tail=tail)
    ix = idx(tmp_path)
    assert len(ix) == 2
    assert ix.last_entry() == (BASE + 200, 2 * INTERVAL)


def test_the_ragged_tail_is_removed_from_DISK_not_just_memory(tmp_path):
    p = write_raw(tmp_path, [(100, INTERVAL)], tail=b"xyz")
    assert p.stat().st_size == ENTRY_SIZE + 3

    idx(tmp_path)
    assert p.stat().st_size == ENTRY_SIZE


def test_appending_after_a_ragged_tail_lands_on_an_entry_boundary(tmp_path):
    """The reason truncation has to hit the file and not only the list.

    The handle is opened "ab", so writes go to the end of the FILE. Leave the
    stray bytes there and the next entry starts 3 bytes late — misaligning
    every entry after it, permanently, with nothing ever raising.
    """
    p = write_raw(tmp_path, [(100, INTERVAL), (200, 2 * INTERVAL)], tail=b"xyz")

    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE + 300), Position(3 * INTERVAL))
    ix.close()

    assert p.stat().st_size == 3 * ENTRY_SIZE
    reopened = idx(tmp_path)
    assert len(reopened) == 3
    assert reopened.last_entry() == (BASE + 300, 3 * INTERVAL)


def test_a_file_too_short_for_even_one_entry_loads_empty(tmp_path):
    write_raw(tmp_path, [], tail=b"junk")
    ix = idx(tmp_path)
    assert len(ix) == 0
    assert ix.lookup(Offset(BASE + 50)) == (BASE, 0)  # falls back, does not raise


# --------------------------------------------------------------------------
# clear — must leave the object indistinguishable from a fresh empty index
# --------------------------------------------------------------------------


def test_clear_empties_memory_and_disk(tmp_path):
    p = write_raw(tmp_path, [(100, INTERVAL), (200, 2 * INTERVAL)])
    ix = idx(tmp_path)

    ix.clear()
    assert len(ix) == 0
    assert p.stat().st_size == 0
    assert ix.last_entry() is None


def test_clear_survives_a_reopen(tmp_path):
    # Without the os.truncate, memory would look empty while the file still
    # held every old entry — and the next open would load them all back.
    write_raw(tmp_path, [(100, INTERVAL)])
    ix = idx(tmp_path)
    ix.clear()
    ix.close()

    assert len(idx(tmp_path)) == 0


def test_clear_then_append_starts_from_the_beginning(tmp_path):
    p = write_raw(tmp_path, [(100, INTERVAL), (200, 2 * INTERVAL)])
    ix = idx(tmp_path)
    ix.clear()

    ix.maybe_append(Offset(BASE + 7), Position(INTERVAL))
    ix.close()

    assert p.stat().st_size == ENTRY_SIZE  # one entry, at byte 0
    assert idx(tmp_path).last_entry() == (BASE + 7, INTERVAL)


def test_clear_on_an_already_empty_index_is_harmless(tmp_path):
    ix = idx(tmp_path)
    ix.clear()
    ix.clear()
    assert len(ix) == 0


def test_clear_discards_buffered_entries_that_never_reached_disk(tmp_path):
    # maybe_append does not flush, so at this point the entry lives only in
    # Python's buffer. clear() flushes BEFORE truncating precisely so those
    # bytes cannot land after the truncate and resurrect themselves.
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE + 1), Position(INTERVAL))
    ix.clear()
    ix.close()

    assert (tmp_path / "test.index").stat().st_size == 0
    assert len(idx(tmp_path)) == 0


# --------------------------------------------------------------------------
# durability — deliberately weak, because the index is rebuildable
# --------------------------------------------------------------------------


def test_entries_are_not_flushed_per_append(tmp_path):
    """Documents a decision, not an accident.

    Segment.append flushes every record because the log IS the source of truth.
    The index is derived and rebuilt on open, so a lost tail costs a slower
    first read and nothing else. If this ever starts failing, someone added a
    flush — check they meant to pay for it.
    """
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE + 1), Position(INTERVAL))
    assert (tmp_path / "test.index").stat().st_size == 0


def test_sync_makes_entries_durable(tmp_path):
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE + 1), Position(INTERVAL))
    ix.sync()
    assert (tmp_path / "test.index").stat().st_size == ENTRY_SIZE


def test_close_syncs(tmp_path):
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE + 1), Position(INTERVAL))
    ix.close()
    assert (tmp_path / "test.index").stat().st_size == ENTRY_SIZE


def test_close_is_idempotent(tmp_path):
    # Called from Segment.close and possibly __exit__ too; a cleanup method
    # that cannot be called twice is a trap.
    ix = idx(tmp_path)
    ix.close()
    ix.close()
    ix.close()


# --------------------------------------------------------------------------
# round trip
# --------------------------------------------------------------------------


def test_every_lookup_answers_identically_after_a_reopen(tmp_path):
    pairs = [(BASE + 100 * n, INTERVAL * n) for n in range(1, 9)]
    ix = idx(tmp_path)
    for offset, position in pairs:
        ix.maybe_append(Offset(offset), Position(position))

    before = {o: ix.lookup(Offset(o)) for o in range(BASE - 10, BASE + 900)}
    ix.close()

    reopened = idx(tmp_path)
    after = {o: reopened.lookup(Offset(o)) for o in range(BASE - 10, BASE + 900)}
    assert before == after


def test_a_reopened_index_keeps_appending_where_it_left_off(tmp_path):
    ix = idx(tmp_path)
    ix.maybe_append(Offset(BASE + 100), Position(INTERVAL))
    ix.close()

    reopened = idx(tmp_path)
    reopened.maybe_append(Offset(BASE + 200), Position(2 * INTERVAL))
    assert len(reopened) == 2
    assert reopened.last_entry() == (BASE + 200, 2 * INTERVAL)


def test_the_interval_is_configurable(tmp_path):
    ix = idx(tmp_path, interval=64)
    for n in range(1, 5):
        ix.maybe_append(Offset(BASE + n), Position(n * 64))
    assert len(ix) == 4  # all of them: 64 apart clears a 64-byte interval
