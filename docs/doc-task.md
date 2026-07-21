# Task: add missing comments and docstrings

You are documenting an existing, working Python project. **Every test passes
and must keep passing.** Your job is to explain code that is already correct.

## THE RULE THAT OVERRIDES EVERYTHING

**Add comments and docstrings only. Never change a line of code.**

If your change alters anything other than comments and docstrings, undo it.

## What to do, one file at a time

Work through the file list below **in order**, one file per commit.

For each file:

1. Read the whole file first.
2. Find test functions (or functions/classes in `src/`) that have **no
   docstring and no explanatory comment**.
3. Add a short docstring or a comment saying **why the test exists** — what
   bug or wrong behaviour it would catch. Not what the code does.
4. Run `uv run pytest -q`. It must report the same number of passing tests as
   before, and zero failures.
5. Run `git diff --stat` and read `git diff`. Confirm that **only** comment
   and docstring lines were added.
6. Commit that one file. Then move to the next.

Do not batch several files into one commit. Do not skip the test run.

## What a good comment looks like

Explain the *reason*, in one or two sentences. Examples already in this
codebase:

```python
def test_written_at_exactly_the_interval(tmp_path):
    # The boundary: >= not >. One off here and every entry lands a record late.

def test_a_tombstone_comes_back_as_an_absent_value(stub):
    """Absent (deletion marker) vs present-but-empty. Flattening either into
    the other would corrupt a compacted topic."""
```

Bad comments — do not write these:

```python
# This test creates a segment and appends a record.   <- restates the code
# Test for append.                                    <- says nothing
"""Tests the append method."""                        <- says nothing
```

If you cannot say why a test matters, leave it alone and move on.

## DO NOT

- **Do not modify, reformat, or reorder any code.** No renaming. No refactoring.
  No changing imports. No "while I was here" fixes.
- **Do not rewrite comments or docstrings that already exist.** If a function
  already has one, leave it exactly as it is, even if you would word it
  differently. Only add where there is nothing.
- **Do not edit `README.md`, `CLAUDE.md`, or anything else in `docs/`.** Those
  are already written.
- **Do not touch `src/pyka/v1/`** — it is generated protobuf code.
- Do not add type hints, assertions, or new tests.
- Do not add a `Co-Authored-By` line or any AI-attribution trailer to a commit
  message. This is a hard project rule.
- Do not commit if `uv run pytest` fails or if the test count changed.

## Commit messages

One line, imperative, naming the file's subject:

```
Document Segment tests: framing, recovery, roll boundaries
Document Topic tests: naming, routing, sync policy
```

Not `docs: update`, not `Added comments`, not multi-paragraph.

## File list, in order

Start at the top. The number is how many undocumented functions were counted,
so you know roughly how much is there.

| # | file | bare |
|---|---|---|
| 1 | `tests/storage/test_segment.py` | 38 |
| 2 | `tests/topic/test_topic.py` | 20 |
| 3 | `tests/broker/test_admin.py` | 16 |
| 4 | `tests/storage/test_index.py` | 14 |
| 5 | `tests/storage/test_log.py` | 14 |
| 6 | `tests/broker/test_consume.py` | 13 |
| 7 | `tests/broker/test_produce.py` | 13 |
| 8 | `tests/cluster/test_ring.py` | 13 |
| 9 | `tests/storage/test_index_recovery.py` | 11 |
| 10 | `tests/storage/test_record.py` | 10 |
| 11 | `tests/broker/test_tail.py` | 9 |
| 12 | `tests/broker/test_cluster.py` | 6 |
| 13 | `tests/storage/test_log_recovery.py` | 5 |
| 14 | `tests/broker/test_server.py` | 5 |
| 15 | `tests/topic/test_partitioner.py` | 4 |
| 16 | `tests/topic/test_policy.py` | 3 |
| 17 | `tests/storage/test_log_concurrency.py` | 1 |

Stop when the list is done. Do not look for more work.

## Context you will need

pyKA is a small Kafka-like log broker.

- `storage/` — `Record` (framing + crc), `Segment` (one file, rolls at a size
  limit), `Index` (sparse offset → byte position, a hint that is always
  verified), `Log` (a chain of segments = one topic-partition).
- `topic/` — `Topic` (named logs, validation), `Partitioner`
  (`crc32(key) % n`), `SyncPolicy` (when to fsync).
- `cluster/ring.py` — `(topic, partition) → broker`. `Ring` is `p % n`;
  `HashRing` is consistent hashing.
- `broker/` — gRPC on 9092 (produce/consume), FastAPI on 8080 (admin).

`README.md` explains every design decision. Read the relevant section before
documenting a file — the reasons are already written down there, and a good
comment often just points at the one that applies.
