"""How long does it take to reach one record in the middle of a segment?

Run once BEFORE wiring Index into Segment, once after:

    uv run python bench/bench_seek.py

The script notices whether Segment has an index (``_index`` attribute) and
labels its results accordingly, so the second run compares itself against the
first automatically.

What is being measured
----------------------
Time to the FIRST record from ``read_from(offset)`` — not the full iteration.
read_from is a generator, so consuming all of it would stream the rest of the
file in both variants and dilute the very difference we are looking for.

What is NOT being measured
--------------------------
Disk I/O. The bench writes the log and then immediately reads it, so it is in
the OS page cache throughout. These numbers are the CPU cost of decoding
records in Python (a struct.unpack plus a zlib.crc32 each). A cold cache would
only widen the gap, so treating this as the pessimistic case is fair.

The claim under test
--------------------
Bytes scanned without an index average file_size/2 and so grow linearly with
the log; with an index they are bounded by the sparsity interval and stay flat.
One file size cannot show that — the sweep is the point, not the headline
ratio, which is just file_size/interval and therefore a choice we made.
"""

from __future__ import annotations

import json
import random
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pyka.storage.record import Record  # noqa: E402
from pyka.storage.segment import Segment  # noqa: E402
from pyka.storage.types import Offset  # noqa: E402

MIB = 1 << 20
SIZES = [1 * MIB, 4 * MIB, 16 * MIB, 64 * MIB]
QUERIES = 20
SEED = 20260721  # fixed: both runs must ask the identical questions

KEY = b"k0000001"  # 8 bytes
VALUE = b"v" * 40
RECORD_SIZE = Record.HEADER_SIZE + len(KEY) + len(VALUE)  # 80

RESULTS = Path(__file__).resolve().parent / "results"


def build(directory: Path, target_bytes: int) -> tuple[Segment, int]:
    """Fill one segment to ~target_bytes. Returns it closed, with its count."""
    seg = Segment(directory, Offset(0))
    count = target_bytes // RECORD_SIZE
    ts = 1_700_000_000_000
    for n in range(count):
        seg.append(Record(Offset(n), ts + n, KEY, VALUE))
    seg.close()
    return seg, count


def time_first_record(seg: Segment, offset: Offset) -> float:
    """Seconds until read_from(offset) yields its first record."""
    start = time.perf_counter()
    it = seg.read_from(offset)
    next(it)
    elapsed = time.perf_counter() - start
    it.close()  # GeneratorExit unwinds the `with open`, closing the handle
    return elapsed


def run_one_size(target_bytes: int) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        seg, count = build(directory, target_bytes)

        open_start = time.perf_counter()
        seg = Segment(directory, Offset(0))  # reopen: full recovery scan
        open_seconds = time.perf_counter() - open_start

        rng = random.Random(SEED)
        offsets = [Offset(rng.randrange(count)) for _ in range(QUERIES)]

        time_first_record(seg, offsets[0])  # warm-up, discarded
        samples = [time_first_record(seg, o) for o in offsets]
        seg.close()

    return {
        "bytes": target_bytes,
        "records": count,
        "open_seconds": open_seconds,
        "min_ms": min(samples) * 1000,
        "median_ms": statistics.median(samples) * 1000,
        "mean_ms": statistics.mean(samples) * 1000,
        "max_ms": max(samples) * 1000,
    }


def main() -> None:
    indexed = hasattr(Segment(Path(tempfile.mkdtemp()), Offset(0)), "_index")
    label = "index" if indexed else "scan"

    print(f"variant: {label}   {QUERIES} random offsets per size, seed {SEED}")
    print(f"record: {RECORD_SIZE} bytes  ({len(KEY)}b key + {len(VALUE)}b value)\n")
    header = f"{'log size':>10} {'records':>10} {'open s':>9} {'min ms':>10} {'median ms':>11} {'mean ms':>10} {'max ms':>10}"
    print(header)
    print("-" * len(header))

    rows = []
    for size in SIZES:
        row = run_one_size(size)
        rows.append(row)
        print(
            f"{row['bytes'] // MIB:>7} MiB {row['records']:>10,} {row['open_seconds']:>9.2f}"
            f" {row['min_ms']:>10.3f} {row['median_ms']:>11.3f}"
            f" {row['mean_ms']:>10.3f} {row['max_ms']:>10.3f}"
        )

    # Linear or flat? Each size is 4x the last, so a scan should be ~4x slower
    # and an indexed lookup ~1x. This is the actual result.
    print("\nmedian growth per 4x log size:")
    for prev, cur in zip(rows, rows[1:]):
        factor = cur["median_ms"] / prev["median_ms"] if prev["median_ms"] else float("nan")
        print(f"  {prev['bytes'] // MIB:>3} -> {cur['bytes'] // MIB:>3} MiB: {factor:5.2f}x")

    RESULTS.mkdir(exist_ok=True)
    out = RESULTS / f"{label}.json"
    out.write_text(json.dumps(rows, indent=2))
    print(f"\nwrote {out.relative_to(Path.cwd())}")

    other = RESULTS / ("scan.json" if indexed else "index.json")
    if other.exists():
        baseline = {r["bytes"]: r for r in json.loads(other.read_text())}
        print(f"\nvs {other.stem}:")
        for row in rows:
            was = baseline.get(row["bytes"])
            if was:
                speedup = was["median_ms"] / row["median_ms"]
                print(
                    f"  {row['bytes'] // MIB:>3} MiB: {was['median_ms']:9.3f} ms"
                    f" -> {row['median_ms']:8.3f} ms   {speedup:9.1f}x"
                )
    else:
        print(f"(no {other.stem}.json yet — run the other variant to compare)")


if __name__ == "__main__":
    main()
