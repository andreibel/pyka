"""Partitioner tests: stability is the whole contract."""

import subprocess
import sys

import pytest

from pyka.topic.partitioner import Partitioner


def test_a_key_always_lands_in_the_same_partition(tmp_path):
    p = Partitioner()
    first = p.partition_for(b"user-42", 8)
    assert all(p.partition_for(b"user-42", 8) == first for _ in range(100))


def test_different_partitioner_instances_agree(tmp_path):
    # Keyed routing must not depend on instance state — only round-robin does.
    assert Partitioner().partition_for(b"k", 16) == Partitioner().partition_for(b"k", 16)


def test_the_hash_is_stable_across_processes():
    """The reason this uses zlib.crc32 and not hash().

    Python randomizes hash() for bytes per process unless PYTHONHASHSEED is
    set, so a restarted broker would reroute every key and silently break
    per-key ordering. Two subprocesses with different seeds must still agree.
    """
    code = (
        "from pyka.topic.partitioner import Partitioner;"
        "print(Partitioner().partition_for(b'user-42', 8))"
    )
    runs = {
        subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True, text=True, check=True,
            env={"PYTHONHASHSEED": seed, "PATH": "", "PYTHONPATH": "src"},
        ).stdout.strip()
        for seed in ("0", "1", "random")
    }
    assert len(runs) == 1, f"partition moved between processes: {runs}"


def test_keys_spread_across_partitions(tmp_path):
    # Not a distribution test — just proof it isn't a constant function.
    p = Partitioner()
    seen = {p.partition_for(f"key-{n}".encode(), 8) for n in range(200)}
    assert len(seen) == 8


def test_the_result_is_always_in_range(tmp_path):
    p = Partitioner()
    for n in range(100):
        for count in (1, 2, 3, 7, 16):
            assert 0 <= p.partition_for(f"k{n}".encode(), count) < count


def test_one_partition_sends_everything_to_zero(tmp_path):
    # The default configuration: routing exists but has nothing to decide.
    p = Partitioner()
    assert {p.partition_for(f"k{n}".encode(), 1) for n in range(50)} == {0}
    assert {p.partition_for(None, 1) for n in range(50)} == {0}


def test_null_keys_round_robin(tmp_path):
    p = Partitioner()
    assert [p.partition_for(None, 4) for _ in range(9)] == [0, 1, 2, 3, 0, 1, 2, 3, 0]


def test_round_robin_is_independent_of_keyed_routing(tmp_path):
    # Keyed calls must not advance the round-robin cursor, or a keyless
    # record's partition would depend on unrelated traffic.
    p = Partitioner()
    assert p.partition_for(None, 4) == 0
    p.partition_for(b"noise", 4)
    p.partition_for(b"more noise", 4)
    assert p.partition_for(None, 4) == 1


def test_an_empty_key_is_a_key_not_a_null(tmp_path):
    # b"" is present-but-empty and routes by hash; None is absent. Same
    # distinction the record format draws with klen 0 vs -1.
    p = Partitioner()
    assert p.partition_for(b"", 8) == p.partition_for(b"", 8)


@pytest.mark.parametrize("count", [0, -1])
def test_fewer_than_one_partition_is_rejected(count):
    with pytest.raises(ValueError, match="must be >= 1"):
        Partitioner().partition_for(b"k", count)
