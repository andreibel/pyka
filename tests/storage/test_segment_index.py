"""Segment + Index together: the seam where the hint meets the authority.

The oracle throughout is a full scan straight off the .log file using only
Record.read_one — no Segment, no Index. Whatever the indexed path returns must
be identical to that, record for record, whatever state the index is in:
fresh, reopened, deleted, garbage, or deliberately lying. An index may make
reads faster or slower, never different.
"""

import random
from pathlib import Path

from pyka.storage.index import ENTRY_SIZE
from pyka.storage.record import Record
from pyka.storage.segment import Segment
from pyka.storage.types import Offset, Position

BASE = 100  # never 0 — a relative/absolute mixup must not be invisible
COUNT = 200

# 0xFF on purpose: any 12 bytes of it, read as a record prefix, claim a size
# of 0xFFFFFFFF — so a seek into the middle of a value fails loudly and
# deterministically instead of depending on what the payload happened to be.
VALUE = b"\xff" * 200
REC_SIZE = Record.HEADER_SIZE + 4 + len(VALUE)  # 4-byte keys below


def build(tmp_path, count: int = COUNT) -> Segment:
    seg = Segment(tmp_path, Offset(BASE))
    for n in range(count):
        seg.append(
            Record(Offset(BASE + n), 1_700_000_000_000 + n, f"{n:04d}".encode(), VALUE)
        )
    return seg


def log_path(tmp_path) -> Path:
    return tmp_path / f"{BASE:020d}.log"


def index_path(tmp_path) -> Path:
    return tmp_path / f"{BASE:020d}.index"


def scan_from(tmp_path, offset: int) -> list[Record]:
    """The honest answer, computed the slow way."""
    out = []
    with open(log_path(tmp_path), "rb") as f:
        while (rec := Record.read_one(f)) is not None:
            if rec.offset >= offset:
                out.append(rec)
    return out


# --------------------------------------------------------------------------
# wiring — the index exists, and everything it claims is true of the log
# --------------------------------------------------------------------------


def test_opening_a_segment_creates_the_index_file(tmp_path):
    Segment(tmp_path, Offset(BASE))
    assert index_path(tmp_path).exists()


def test_appending_fills_the_index(tmp_path):
    seg = build(tmp_path)
    # ~236-byte records, one entry per 4096 bytes: definitely more than one
    assert len(seg._index) >= 2


def test_every_lookup_hint_points_at_a_true_record_boundary(tmp_path):
    """THE test for the position-capture trap in _recover and append.

    Feed the index a record's END instead of its START and every hint lands
    one record late: offsets still increase, nothing raises, and every read
    silently falls back to a full scan — correct answers, index never helps.
    This is the only test that notices.
    """
    seg = build(tmp_path)
    with open(log_path(tmp_path), "rb") as f:
        for offset in range(BASE, BASE + COUNT):
            hint_offset, position = seg._index.lookup(Offset(offset))
            f.seek(position)
            rec = Record.read_one(f)
            assert rec is not None, f"hint for {offset} points past the data"
            assert rec.offset == hint_offset, f"hint for {offset} is off a boundary"


def test_append_and_recovery_build_the_same_index(tmp_path):
    """Two independent code paths feed the index — append as records arrive,
    _recover by rescanning the log. They must agree entry for entry, or one
    of them is capturing the wrong position."""
    seg = build(tmp_path)
    fed_by_append = list(seg._index._entries)
    seg.close()

    reopened = Segment(tmp_path, Offset(BASE))
    assert list(reopened._index._entries) == fed_by_append


# --------------------------------------------------------------------------
# differential — indexed reads vs the oracle
# --------------------------------------------------------------------------


def test_indexed_reads_match_a_full_scan_for_every_offset(tmp_path):
    seg = build(tmp_path)
    for offset in range(BASE, BASE + COUNT):
        assert list(seg.read_from(Offset(offset))) == scan_from(tmp_path, offset), (
            f"indexed read diverged from the scan at offset {offset}"
        )


def test_indexed_reads_still_match_after_a_reopen(tmp_path):
    build(tmp_path).close()
    seg = Segment(tmp_path, Offset(BASE))  # index rebuilt by _recover this time
    for offset in range(BASE, BASE + COUNT, 7):
        assert list(seg.read_from(Offset(offset))) == scan_from(tmp_path, offset)


# --------------------------------------------------------------------------
# a lying index — the fallback must make every lie harmless
#
# In-memory poisoning is deliberate: on-disk damage never survives _recover's
# rebuild, so the only way to stage a wrong hint is to plant it after open —
# standing in for the stale-index bugs a future optimization could introduce.
# --------------------------------------------------------------------------


def test_a_hint_at_the_wrong_record_boundary_falls_back(tmp_path):
    seg = build(tmp_path)
    # Every entry now points at record 0: a valid boundary, but the offset
    # found there won't match the hint — the mismatch branch.
    seg._index._entries = [(rel, Position(0)) for rel, _ in seg._index._entries]

    for offset in range(BASE, BASE + COUNT, 7):
        assert list(seg.read_from(Offset(offset))) == scan_from(tmp_path, offset)


def test_a_hint_into_the_middle_of_a_record_falls_back(tmp_path):
    seg = build(tmp_path)
    # Point every entry into a VALUE region (header + key past the record
    # start): the bytes there are 0xFF, which decode as an impossible size.
    # A lying index must cost time, never raise — the log itself is fine.
    seg._index._entries = [
        (rel, Position(pos + Record.HEADER_SIZE + 4 + 10))
        for rel, pos in seg._index._entries
    ]

    for offset in range(BASE, BASE + COUNT, 7):
        assert list(seg.read_from(Offset(offset))) == scan_from(tmp_path, offset)


def test_a_hint_past_the_end_of_the_log_falls_back(tmp_path):
    seg = build(tmp_path)
    end = log_path(tmp_path).stat().st_size
    seg._index._entries = [(rel, Position(end + 999)) for rel, _ in seg._index._entries]

    for offset in range(BASE, BASE + COUNT, 7):
        assert list(seg.read_from(Offset(offset))) == scan_from(tmp_path, offset)


# --------------------------------------------------------------------------
# on-disk damage — _recover's rebuild makes the .index file expendable
# --------------------------------------------------------------------------


def test_a_deleted_index_is_rebuilt_on_open(tmp_path):
    build(tmp_path).close()
    index_path(tmp_path).unlink()

    seg = Segment(tmp_path, Offset(BASE))
    assert index_path(tmp_path).exists()
    assert len(seg._index) >= 2
    for offset in range(BASE, BASE + COUNT, 7):
        assert list(seg.read_from(Offset(offset))) == scan_from(tmp_path, offset)


def test_a_garbage_index_file_is_replaced_on_open(tmp_path):
    build(tmp_path).close()
    index_path(tmp_path).write_bytes(random.Random(7).randbytes(64))

    seg = Segment(tmp_path, Offset(BASE))
    # rebuilt from the log, so every hint verifies again
    with open(log_path(tmp_path), "rb") as f:
        for offset in range(BASE, BASE + COUNT, 7):
            hint_offset, position = seg._index.lookup(Offset(offset))
            f.seek(position)
            rec = Record.read_one(f)
            assert rec is not None and rec.offset == hint_offset


def test_close_makes_the_index_durable(tmp_path):
    seg = build(tmp_path)
    entries = len(seg._index)
    seg.close()
    assert index_path(tmp_path).stat().st_size == entries * ENTRY_SIZE
