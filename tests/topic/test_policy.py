"""SyncPolicy tests: a pure decision, so these are just truth tables."""

import pytest

from pyka.topic.policy import SYNC_EVERY_RECORD, SYNC_NEVER, SyncPolicy


def test_never_is_the_default(tmp_path):
    assert SyncPolicy() == SYNC_NEVER
    assert SYNC_NEVER.should_sync(1_000_000, 1_000_000) is False


def test_every_record_syncs_immediately(tmp_path):
    assert SYNC_EVERY_RECORD.should_sync(1, 0) is True


@pytest.mark.parametrize(
    "appends,expected", [(1, False), (99, False), (100, True), (101, True)]
)
def test_the_record_threshold_is_inclusive(appends, expected):
    # >= not >: "sync every 100 records" must fire ON the 100th.
    assert SyncPolicy(records=100).should_sync(appends, 0) is expected


@pytest.mark.parametrize(
    "millis,expected", [(0, False), (999, False), (1000, True), (5000, True)]
)
def test_the_time_threshold_is_inclusive(millis, expected):
    assert SyncPolicy(millis=1000).should_sync(0, millis) is expected


def test_the_thresholds_are_ored_not_anded(tmp_path):
    # Either bound alone is enough — they are two ceilings on data loss, not
    # a pair of conditions.
    policy = SyncPolicy(records=100, millis=1000)
    assert policy.should_sync(100, 0) is True     # count only
    assert policy.should_sync(0, 1000) is True    # time only
    assert policy.should_sync(1, 1) is False      # neither


def test_a_policy_is_frozen_and_shareable(tmp_path):
    # Frozen on purpose: the counters live in the caller, so one instance is
    # safely shared by every partition of every topic.
    policy = SyncPolicy(records=10)
    with pytest.raises(Exception):
        policy.records = 5  # type: ignore[misc]
    assert hash(policy) == hash(SyncPolicy(records=10))
