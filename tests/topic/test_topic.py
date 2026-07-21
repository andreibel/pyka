"""Topic tests: naming, routing, partition layout, and the sync seam.

Topic is the first class that composes rather than implements — its job is to
put Log, Partitioner and SyncPolicy together correctly, so most of these tests
are about the wiring between them.
"""

import pytest

from pyka.storage.types import Offset
from pyka.topic.policy import SYNC_EVERY_RECORD, SyncPolicy
from pyka.topic.topic import Topic, UnknownTopic, validate_name

VALUE = b"v" * 20


def topic(tmp_path, **kwargs) -> Topic:
    return Topic(tmp_path / "data", **kwargs)


# --------------------------------------------------------------------------
# names — layer 2's security boundary
# --------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["orders", "a", "A-b_c.d", "x" * 200, "0"])
def test_valid_names_are_accepted(name):
    validate_name(name)


@pytest.mark.parametrize(
    "name,reason",
    [
        ("", "must not be empty"),
        ("x" * 201, "longer than"),
        (".", "reserved"),
        ("..", "reserved"),
        ("../etc", "invalid"),
        ("a/b", "invalid"),
        ("a\\b", "invalid"),
        ("a b", "invalid"),
        ("a\x00b", "invalid"),
        ("emoji-🙂", "invalid"),
    ],
)
def test_dangerous_names_are_rejected(name, reason):
    # In phase B these arrive from a socket. A whitelist, not a blacklist:
    # "." and ".." pass the charset check but ARE traversal, hence by-name.
    with pytest.raises(ValueError, match=reason):
        validate_name(name)


def test_a_traversal_name_cannot_create_anything(tmp_path):
    t = topic(tmp_path)
    with pytest.raises(ValueError):
        t.append("../escaped", b"k", VALUE)
    assert not (tmp_path / "escaped").exists()


# --------------------------------------------------------------------------
# create / get / names
# --------------------------------------------------------------------------


def test_a_new_registry_has_no_topics(tmp_path):
    assert topic(tmp_path).names() == []


@pytest.mark.parametrize("count", [0, -1])
def test_fewer_than_one_partition_is_rejected(tmp_path, count):
    with pytest.raises(ValueError, match="must be >= 1"):
        topic(tmp_path, partitions=count)


def test_create_makes_a_directory_per_partition(tmp_path):
    t = topic(tmp_path, partitions=3)
    assert t.create("orders") == 3
    assert sorted(p.name for p in (tmp_path / "data" / "orders").iterdir()) == [
        "0", "1", "2",
    ]


def test_create_is_idempotent(tmp_path):
    t = topic(tmp_path, partitions=2)
    assert t.create("orders") == t.create("orders") == 2
    assert t.names() == ["orders"]


def test_create_never_repartitions_an_existing_topic(tmp_path):
    # Changing the count would move every key to a different partition and
    # silently break the ordering the partitioner exists to provide.
    topic(tmp_path, partitions=4).create("orders")
    assert topic(tmp_path, partitions=1).create("orders", partitions=9) == 4


def test_names_come_from_disk_not_the_cache(tmp_path):
    # A topic exists because its directory does — otherwise a restart would
    # report nothing until something touched it.
    topic(tmp_path).create("orders")
    fresh = topic(tmp_path)
    assert fresh.names() == ["orders"]      # never opened by this instance
    assert fresh.exists("orders")


def test_get_raises_for_an_unknown_topic(tmp_path):
    # Reads raise, appends create: a consumer naming a missing topic has
    # typo'd, and an empty auto-created topic would never tell it so.
    with pytest.raises(UnknownTopic):
        topic(tmp_path).get("nope")


def test_get_raises_for_a_partition_out_of_range(tmp_path):
    t = topic(tmp_path, partitions=2)
    t.create("orders")
    with pytest.raises(ValueError, match="2 partition"):
        t.get("orders", 2)


def test_get_returns_the_same_log_object_each_time(tmp_path):
    # Logs are cached: two Log objects on one directory would each think they
    # own the write handle.
    t = topic(tmp_path)
    t.create("orders")
    assert t.get("orders") is t.get("orders")


# --------------------------------------------------------------------------
# append — routing plus auto-create
# --------------------------------------------------------------------------


def test_append_auto_creates_the_topic(tmp_path):
    t = topic(tmp_path)
    assert t.append("orders", b"k", VALUE) == (0, 0)
    assert t.names() == ["orders"]


def test_append_returns_partition_and_offset(tmp_path):
    t = topic(tmp_path, partitions=4)
    partition, offset = t.append("orders", b"user-1", VALUE)
    assert 0 <= partition < 4
    assert offset == 0  # each partition numbers from zero


def test_offsets_are_per_partition_not_global(tmp_path):
    """The reason append returns a pair. Two partitions both start at 0, so an
    offset without its partition is meaningless."""
    t = topic(tmp_path, partitions=4)
    seen: dict[int, list[int]] = {}
    for n in range(20):
        p, off = t.append("orders", f"key-{n}".encode(), VALUE)
        seen.setdefault(p, []).append(off)
    assert len(seen) > 1, "test needs keys landing in different partitions"
    for offsets in seen.values():
        assert offsets == list(range(len(offsets)))  # each counts from 0


def test_records_for_one_key_stay_in_one_partition(tmp_path):
    # The ordering guarantee: same key, same log, therefore same order.
    t = topic(tmp_path, partitions=8)
    partitions = {t.append("orders", b"user-42", VALUE)[0] for _ in range(20)}
    assert len(partitions) == 1


def test_a_record_can_be_read_back_from_the_partition_it_went_to(tmp_path):
    t = topic(tmp_path, partitions=4)
    partition, offset = t.append("orders", b"user-1", b"payload")
    (rec,) = t.read_from("orders", Offset(offset), partition)
    assert (rec.key, rec.value) == (b"user-1", b"payload")


def test_read_from_an_unknown_topic_raises(tmp_path):
    with pytest.raises(UnknownTopic):
        topic(tmp_path).read_from("nope", Offset(0))


def test_tombstones_and_null_keys_survive_the_round_trip(tmp_path):
    t = topic(tmp_path)
    t.append("orders", None, VALUE)
    t.append("orders", b"gone", None)
    a, b = t.read_from("orders", Offset(0))
    assert (a.key, a.value) == (None, VALUE)
    assert (b.key, b.value) == (b"gone", None)


# --------------------------------------------------------------------------
# the sync seam — policy decides, Log performs
# --------------------------------------------------------------------------


def test_sync_never_leaves_the_bytes_unsynced(tmp_path):
    # Not a durability claim: flush() still ran, so another reader sees the
    # data. This only pins that Topic did not call fsync behind our back.
    t = topic(tmp_path, sync_policy=SyncPolicy())
    t.append("orders", b"k", VALUE)
    assert t._open["orders"][0].appends == 1  # counter never reset


def test_sync_every_record_resets_the_counter(tmp_path):
    t = topic(tmp_path, sync_policy=SYNC_EVERY_RECORD)
    for _ in range(3):
        t.append("orders", b"k", VALUE)
    assert t._open["orders"][0].appends == 0


def test_the_record_threshold_fires_on_schedule(tmp_path):
    t = topic(tmp_path, sync_policy=SyncPolicy(records=3))
    counts = []
    for _ in range(7):
        t.append("orders", b"k", VALUE)
        counts.append(t._open["orders"][0].appends)
    assert counts == [1, 2, 0, 1, 2, 0, 1]


def test_sync_counters_are_per_partition(tmp_path):
    # One frozen policy shared by all; the counters must not be.
    t = topic(tmp_path, partitions=4, sync_policy=SyncPolicy(records=100))
    t.append("orders", b"user-1", VALUE)
    appends = [p.appends for p in t._open["orders"]]
    assert sorted(appends) == [0, 0, 0, 1]


def test_explicit_sync_resets_every_partition(tmp_path):
    t = topic(tmp_path, partitions=2)
    for n in range(6):
        t.append("orders", f"k{n}".encode(), VALUE)
    t.sync()
    assert all(p.appends == 0 for p in t._open["orders"])


# --------------------------------------------------------------------------
# lifecycle and reopening
# --------------------------------------------------------------------------


def test_a_reopened_registry_finds_the_partition_count_on_disk(tmp_path):
    # Partition count belongs to the topic, not the registry that opened it.
    topic(tmp_path, partitions=4).create("orders")
    assert topic(tmp_path, partitions=1).partitions_of("orders") == 4


def test_records_survive_a_close_and_reopen(tmp_path):
    t = topic(tmp_path, partitions=4)
    written = [t.append("orders", f"key-{n}".encode(), f"v{n}".encode()) for n in range(20)]
    t.close()

    reopened = topic(tmp_path, partitions=4)
    for partition, offset in written:
        assert list(reopened.read_from("orders", Offset(offset), partition))


def test_routing_is_stable_across_a_reopen(tmp_path):
    # crc32, not hash(): the same key must find the same partition in a new
    # process, or its history is split across two logs.
    t = topic(tmp_path, partitions=8)
    first, _ = t.append("orders", b"user-42", VALUE)
    t.close()
    again, _ = topic(tmp_path, partitions=8).append("orders", b"user-42", VALUE)
    assert again == first


def test_close_then_reopen_keeps_appending(tmp_path):
    t = topic(tmp_path)
    t.append("orders", b"k", VALUE)
    t.close()
    assert topic(tmp_path).append("orders", b"k", VALUE)[1] == 1


def test_close_is_idempotent(tmp_path):
    t = topic(tmp_path)
    t.append("orders", b"k", VALUE)
    t.close()
    t.close()


def test_topic_is_a_context_manager(tmp_path):
    with topic(tmp_path) as t:
        t.append("orders", b"k", VALUE)
    assert t._open == {}


def test_several_topics_are_independent(tmp_path):
    t = topic(tmp_path)
    t.append("orders", b"k", b"one")
    t.append("clicks", b"k", b"two")
    assert t.names() == ["clicks", "orders"]
    assert [r.value for r in t.read_from("orders", Offset(0))] == [b"one"]
    assert [r.value for r in t.read_from("clicks", Offset(0))] == [b"two"]
