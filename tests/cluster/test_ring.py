"""Ring tests: agreement by construction, and the flaw that motivates the next step.

Nothing here touches a disk or a socket — the ring is arithmetic. That is the
claim being tested: a cluster whose only coordination is a shared formula.
"""

import pytest

from pyka.cluster.ring import Ring


# --------------------------------------------------------------------------
# assignment
# --------------------------------------------------------------------------


def test_partitions_are_assigned_round_robin():
    ring = Ring(brokers=3, me=0)
    assert [ring.broker_for(p) for p in range(7)] == [0, 1, 2, 0, 1, 2, 0]


def test_a_single_broker_owns_everything():
    # The default configuration, and today's reality: a cluster of one.
    ring = Ring(brokers=1, me=0)
    assert all(ring.owns(p) for p in range(50))


def test_every_partition_has_exactly_one_owner():
    # The invariant the whole design exists to guarantee. Two owners is
    # split-brain; zero owners is unreachable data.
    brokers = 4
    rings = [Ring(brokers=brokers, me=n) for n in range(brokers)]
    for partition in range(40):
        owners = [r.me for r in rings if r.owns(partition)]
        assert owners == [partition % brokers], f"partition {partition}: {owners}"


def test_every_broker_computes_the_same_table():
    """Agreement by construction: no protocol, no controller, no exchange of
    messages — every node derives the identical answer from identical static
    inputs. This is what replaces consensus in this design."""
    tables = [Ring(brokers=5, me=n).routing_table(20) for n in range(5)]
    assert all(t == tables[0] for t in tables)


def test_my_partitions_partition_the_whole_range():
    # Union of every broker's share == all partitions, with no overlap.
    brokers, partitions = 3, 10
    shares = [Ring(brokers=brokers, me=n).my_partitions(partitions) for n in range(brokers)]
    flat = sorted(p for share in shares for p in share)
    assert flat == list(range(partitions))


def test_owns_is_consistent_with_broker_for():
    ring = Ring(brokers=4, me=2)
    for p in range(20):
        assert ring.owns(p) == (ring.broker_for(p) == 2)


# --------------------------------------------------------------------------
# the flaw — why consistent hashing comes next
# --------------------------------------------------------------------------


def test_growing_the_cluster_moves_almost_every_partition():
    """The known cost of `p % n`, measured rather than asserted.

    Each moved partition means its entire log migrating across the network.
    Consistent hashing exists to make this ~1/n instead of ~all — this test is
    here so that motivation is a number in the suite, not a claim in a README.
    """
    partitions = 100
    before = Ring(brokers=4, me=0)
    after = Ring(brokers=5, me=0)

    moved = sum(before.broker_for(p) != after.broker_for(p) for p in range(partitions))
    assert moved >= 75, f"only {moved}/100 moved — has the assignment changed?"


def test_a_partition_never_moves_while_the_cluster_is_the_same_size():
    # Stability under restarts: the ring is config, so a broker restarting
    # rebuilds the identical map.
    a, b = Ring(brokers=3, me=1), Ring(brokers=3, me=1)
    assert a == b
    assert a.routing_table(30) == b.routing_table(30)


# --------------------------------------------------------------------------
# validation
# --------------------------------------------------------------------------


@pytest.mark.parametrize("brokers", [0, -1])
def test_a_cluster_needs_at_least_one_broker(brokers):
    with pytest.raises(ValueError, match="brokers must be >= 1"):
        Ring(brokers=brokers, me=0)


@pytest.mark.parametrize("me", [-1, 3, 99])
def test_an_ordinal_outside_the_cluster_is_rejected(me):
    # Catches the k8s misconfiguration where PYKA_BROKERS disagrees with the
    # StatefulSet's replica count — pyka-3 existing in a 3-broker cluster.
    with pytest.raises(ValueError, match="outside a 3-broker cluster"):
        Ring(brokers=3, me=me)


def test_a_negative_partition_is_rejected():
    with pytest.raises(ValueError, match="partition must be >= 0"):
        Ring(brokers=3, me=0).broker_for(-1)


def test_the_ring_is_frozen():
    # Configuration, not runtime state: a ring that could change while running
    # would let two brokers hold different versions, which is the disagreement
    # this design has no protocol to resolve.
    ring = Ring(brokers=3, me=0)
    with pytest.raises(Exception):
        ring.brokers = 5  # type: ignore[misc]


# --------------------------------------------------------------------------
# addresses and environment — the Kubernetes seam
# --------------------------------------------------------------------------


def test_address_is_the_per_pod_dns_name():
    # A headless Service gives each pod its own record. A load-balanced
    # Service would resolve to "any broker", which is never right when a
    # specific one owns the partition.
    assert Ring(brokers=3, me=0).address_of(1) == "pyka-1.pyka-hl:9092"


def test_the_host_template_and_port_are_configurable():
    ring = Ring(brokers=2, me=0, host_template="broker{ordinal}.svc", port=7000)
    assert ring.address_of(1) == "broker1.svc:7000"


def test_the_routing_table_maps_partitions_to_addresses():
    assert Ring(brokers=2, me=0).routing_table(4) == {
        0: "pyka-0.pyka-hl:9092",
        1: "pyka-1.pyka-hl:9092",
        2: "pyka-0.pyka-hl:9092",
        3: "pyka-1.pyka-hl:9092",
    }


def test_an_unknown_broker_has_no_address():
    with pytest.raises(ValueError, match="no broker 5"):
        Ring(brokers=3, me=0).address_of(5)


def test_from_env_reads_the_ordinal_out_of_the_hostname(monkeypatch):
    # How a pod learns who it is: the StatefulSet ordinal is stable across
    # restarts, so identity comes from the hostname rather than config.
    monkeypatch.setenv("HOSTNAME", "pyka-2")
    monkeypatch.setenv("PYKA_BROKERS", "3")
    ring = Ring.from_env()
    assert (ring.me, ring.brokers) == (2, 3)


def test_from_env_defaults_to_a_cluster_of_one(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "pyka-0")
    monkeypatch.delenv("PYKA_BROKERS", raising=False)
    assert Ring.from_env() == Ring(brokers=1, me=0)


def test_from_env_honours_template_and_port(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "kafka-1")
    monkeypatch.setenv("PYKA_BROKERS", "2")
    monkeypatch.setenv("PYKA_HOST_TEMPLATE", "kafka-{ordinal}.internal")
    monkeypatch.setenv("PYKA_PORT", "9000")
    assert Ring.from_env().address_of(1) == "kafka-1.internal:9000"


def test_from_env_rejects_a_hostname_without_an_ordinal(monkeypatch):
    # A Deployment gives pods random names like pyka-7d4f-x9k2 — the failure
    # this project's README argues against, caught loudly at startup.
    monkeypatch.setenv("HOSTNAME", "pyka-7d4f-x9k2")
    with pytest.raises(ValueError, match="no StatefulSet ordinal"):
        Ring.from_env()
