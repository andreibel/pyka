"""Concurrency and shutdown: what layer 3 does to layer 1.

Two servers dispatch through asyncio.to_thread, so several threads reach one
Log at once. These are the tests for the audit findings — every one of them
failed before the fix.
"""

import threading

import pytest

from pyka.storage.index import MAX_U32
from pyka.storage.log import Log
from pyka.storage.segment import Segment
from pyka.storage.types import Offset, Position

VALUE = b"v" * 20


def test_concurrent_appends_do_not_corrupt_the_log(tmp_path):
    """4 threads x 50 appends. Before the lock this produced 2 usable records
    and a log that would not reopen: 'offset gap at byte 108'."""
    log = Log(tmp_path / "p0")
    offsets: list[int] = []
    errors: list[Exception] = []

    def writer(n: int) -> None:
        try:
            for _ in range(50):
                offsets.append(log.append(f"k{n}".encode(), VALUE))
        except Exception as err:  # noqa: BLE001
            errors.append(err)

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert sorted(offsets) == list(range(200))  # every offset claimed once
    log.close()

    reopened = Log(tmp_path / "p0")  # would raise CorruptLog if interleaved
    assert reopened.next_offset == 200
    assert len(list(reopened.read_from(Offset(0)))) == 200


def test_concurrent_appends_survive_segment_rolls(tmp_path):
    # Rolling mutates the segment chain, so it is the riskiest moment to race.
    log = Log(tmp_path / "p0", max_segment_bytes=300)

    def writer() -> None:
        for _ in range(40):
            log.append(b"k", VALUE)

    threads = [threading.Thread(target=writer) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.close()

    reopened = Log(tmp_path / "p0", max_segment_bytes=300)
    assert len(reopened.segments) > 1, "test needs at least one roll"
    assert [r.offset for r in reopened.read_from(Offset(0))] == list(range(120))


def test_reads_are_safe_while_another_thread_appends(tmp_path):
    # _iter_from snapshots the chain under the lock; without that, a roll
    # during iteration can show a reader a segment twice or skip one.
    log = Log(tmp_path / "p0", max_segment_bytes=300)
    for _ in range(20):
        log.append(b"k", VALUE)

    stop = threading.Event()
    reads = 0

    def appender() -> None:
        for _ in range(2000):
            if stop.is_set():
                return
            log.append(b"k", VALUE)

    thread = threading.Thread(target=appender)
    thread.start()
    try:
        # Read until the writer is done rather than a fixed count: asserting
        # "the log grew between two reads" would be a timing race, and a
        # bounded read loop can finish before the writer even starts.
        while thread.is_alive() and reads < 100:
            offsets = [r.offset for r in log.read_from(Offset(0))]
            reads += 1
            # The invariant: whatever is visible is a clean prefix. A reader
            # may miss records appended after it started — it must never see
            # a gap, a duplicate, or a half-written record.
            assert offsets == list(range(len(offsets)))
    finally:
        stop.set()
        thread.join()

    assert reads > 0
    assert len(list(log.read_from(Offset(0)))) == log.next_offset


# --------------------------------------------------------------------------
# shutdown must not stop at the first failure
# --------------------------------------------------------------------------


class _Boom:
    """A segment that refuses to close, as a disk error at shutdown would."""

    def close(self) -> None:
        raise OSError("disk gone")


def test_close_flushes_every_segment_even_if_one_fails(tmp_path):
    """SIGTERM is exactly when losing a tail costs most, so one bad segment
    must not leave the others unflushed."""
    log = Log(tmp_path / "p0", max_segment_bytes=300)
    for _ in range(20):
        log.append(b"k", VALUE)
    assert len(log.segments) > 2, "test needs several segments"

    healthy = list(log.segments[1:])
    log._segments[0] = _Boom()  # type: ignore[call-overload]

    with pytest.raises(ExceptionGroup) as err:
        log.close()
    assert "disk gone" in str(err.value.exceptions[0])
    assert all(seg.sealed for seg in healthy), "the rest were never closed"


# --------------------------------------------------------------------------
# the u32 ceiling, enforced where the format defines it
# --------------------------------------------------------------------------


@pytest.mark.parametrize("max_bytes", [0, -1, MAX_U32 + 2, 8 << 30])
def test_a_segment_larger_than_the_u32_position_is_rejected(tmp_path, max_bytes):
    # Previously accepted, then struct.error deep inside maybe_append once the
    # file actually passed 4 GiB — an error pointing nowhere near the cause.
    with pytest.raises(ValueError, match="max_bytes must be between"):
        Segment(tmp_path, Offset(0), max_bytes=max_bytes)


def test_the_largest_legal_segment_size_is_accepted(tmp_path):
    Segment(tmp_path, Offset(0), max_bytes=MAX_U32 + 1)  # exactly 4 GiB
