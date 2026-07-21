"""Log recovery tests: reopening, torn tails, and damage to the chain itself.

Segment-level recovery (torn records, corrupt bytes) is tested in
test_segment*.py and not repeated here — this file is about the failure mode
only Log can have: the CHAIN of files being wrong, not the files themselves.
"""

import pytest

from pyka.storage.log import CorruptLog, Log
from pyka.storage.record import Record
from pyka.storage.types import Offset

MAX_BYTES = 300
VALUE = b"v" * 50
TS = 1_700_000_000_000
PER_SEGMENT = 3  # see test_log.py for the arithmetic


def key(n: int) -> bytes:
    return f"{n:04d}".encode()


def open_log(tmp_path) -> Log:
    return Log(tmp_path / "topic-0", max_segment_bytes=MAX_BYTES)


def fill(log: Log, count: int) -> list[Record]:
    expected = []
    for n in range(int(log.next_offset), int(log.next_offset) + count):
        log.append(key(n), VALUE, timestamp=TS + n)
        expected.append(Record(Offset(n), TS + n, key(n), VALUE))
    return expected


def segment_file(tmp_path, base: int):
    return tmp_path / "topic-0" / f"{base:020d}.log"


# --------------------------------------------------------------------------
# reopening — a restart must be invisible
# --------------------------------------------------------------------------


def test_reopen_restores_next_offset(tmp_path):
    log = open_log(tmp_path)
    fill(log, 10)
    log.close()

    assert open_log(tmp_path).next_offset == 10


def test_reopen_reads_identically(tmp_path):
    log = open_log(tmp_path)
    expected = fill(log, 10)
    log.close()

    reopened = open_log(tmp_path)
    for n in range(10):
        assert list(reopened.read_from(Offset(n))) == expected[n:]


def test_reopen_keeps_appending_where_it_left_off(tmp_path):
    log = open_log(tmp_path)
    fill(log, 10)
    log.close()

    reopened = open_log(tmp_path)
    assert reopened.append(key(10), VALUE, timestamp=TS + 10) == 10
    assert [r.offset for r in reopened.read_from(Offset(0))] == list(range(11))


def test_reopen_does_not_reroll_existing_segments(tmp_path):
    # Reopening must adopt the chain as it stands, not create new files.
    log = open_log(tmp_path)
    fill(log, 10)
    log.close()

    before = sorted(p.name for p in (tmp_path / "topic-0").glob("*.log"))
    open_log(tmp_path)
    after = sorted(p.name for p in (tmp_path / "topic-0").glob("*.log"))
    assert before == after


def test_a_torn_tail_in_the_active_segment_is_truncated_on_open(tmp_path):
    # A crash mid-append tears only the LAST segment — sealed ones were never
    # written again. Log inherits Segment's recovery for exactly this file.
    # A torn tail is a PREFIX OF A REAL RECORD (the write stopped early), not
    # arbitrary bytes — read_one sees a size it cannot satisfy and returns
    # None, the expected-crash signal, and recovery truncates.
    log = open_log(tmp_path)
    fill(log, 10)
    log.close()

    torn = Record(Offset(10), TS, key(10), VALUE).encode()[:30]
    with open(segment_file(tmp_path, 9), "ab") as f:
        f.write(torn)

    reopened = open_log(tmp_path)
    assert reopened.next_offset == 10                       # the tear dropped
    assert reopened.append(key(10), VALUE, timestamp=TS) == 10  # and appendable


def test_a_garbage_tail_is_corruption_and_refuses_to_open(tmp_path):
    """The other half of the None-vs-CorruptRecord split: bytes that PARSE
    but lie (here, an impossible size) are not a torn write — they are damage,
    and damage is loud. Only an unsatisfiable size is quietly truncated."""
    log = open_log(tmp_path)
    fill(log, 10)
    log.close()

    with open(segment_file(tmp_path, 9), "ab") as f:
        f.write(b"half-a-record")  # 13 bytes: parses as size ~1.7 GB

    with pytest.raises(Exception, match="exceeds MAX_SIZE"):
        open_log(tmp_path)


def test_sync_makes_the_active_segment_durable(tmp_path):
    # Only the tail can have unsynced bytes: sealed segments were synced by
    # the close() that sealed them. Log.sync is the hook the policy layer will
    # call, so it must reach the file without a close.
    log = open_log(tmp_path)
    expected = fill(log, 4)
    log.sync()

    # read through a fresh Log, no close on the writer
    assert list(open_log(tmp_path).read_from(Offset(0))) == expected


def test_close_is_idempotent(tmp_path):
    log = open_log(tmp_path)
    fill(log, 10)
    log.close()
    log.close()
    log.close()


# --------------------------------------------------------------------------
# chain damage — the failure mode only Log can detect
# --------------------------------------------------------------------------


def test_a_missing_middle_segment_raises_CorruptLog(tmp_path):
    # Records 3..5 are simply gone; every read would silently skip them.
    # Loud over silent: refuse to open at all.
    log = open_log(tmp_path)
    fill(log, 10)  # segments at 0, 3, 6, 9
    log.close()

    segment_file(tmp_path, 3).unlink()
    with pytest.raises(CorruptLog, match="chain broken"):
        open_log(tmp_path)


def test_the_gap_error_names_both_files(tmp_path):
    log = open_log(tmp_path)
    fill(log, 10)
    log.close()
    segment_file(tmp_path, 6).unlink()

    with pytest.raises(CorruptLog, match=r"0+3\.log.*starts at 9"):
        open_log(tmp_path)


def test_a_missing_OLDEST_segment_is_a_shorter_log_not_an_error(tmp_path):
    """Deleting from the front leaves a continuous chain that starts later —
    indistinguishable from retention, which will do exactly this on purpose.
    The chain check cannot and should not flag it."""
    log = open_log(tmp_path)
    expected = fill(log, 10)
    log.close()
    segment_file(tmp_path, 0).unlink()

    reopened = open_log(tmp_path)
    assert list(reopened.read_from(Offset(3))) == expected[3:]
    with pytest.raises(ValueError, match="before this log starts"):
        reopened.read_from(Offset(0))  # offsets 0..2 no longer exist here


def test_a_missing_NEWEST_segment_is_a_shorter_log_that_recounts(tmp_path):
    """Deleting the tail is silent data loss the chain cannot detect — there
    is no file after it to disagree with. Documented, not defended: the log
    reopens shorter and hands out the lost offsets again."""
    log = open_log(tmp_path)
    fill(log, 10)
    log.close()
    segment_file(tmp_path, 9).unlink()

    reopened = open_log(tmp_path)
    assert reopened.next_offset == 9  # offset 9 will be reissued
    assert reopened.append(key(9), VALUE, timestamp=TS) == 9


def test_missing_index_files_do_not_stop_a_reopen(tmp_path):
    # Indexes are derived; the chain check runs on .log files alone.
    log = open_log(tmp_path)
    expected = fill(log, 10)
    log.close()

    for p in (tmp_path / "topic-0").glob("*.index"):
        p.unlink()

    reopened = open_log(tmp_path)
    assert list(reopened.read_from(Offset(0))) == expected
