"""Ring tests: agreement by construction, and the flaw that motivates the next step.

Nothing here touches a disk or a socket — the ring is arithmetic. That is the
claim being tested: a cluster whose only coordination is a shared formula.
"""

import pytest

from pyka.cluster.ring import HashRing, Ring, digest


# --------------------------------------------------------------------------
# assignment
# --------------------------------------------------------------------------


def test_partitions_are_assigned_round_robin():
    ring = Ring(brokers=3, me=0)
    assert [ring.broker_for("t", p) for p in range(7)] == [0, 1, 2, 0, 1, 2, 0]


def test_a_single_broker_owns_everything():
    # The default configuration, and today's reality: a cluster of one.
    ring = Ring(brokers=1, me=0)
    assert all(ring.owns("t", p) for p in range(50))


def test_every_partition_has_exactly_one_owner():
    # The invariant the whole design exists to guarantee. Two owners is
    # split-brain; zero owners is unreachable data.
    brokers = 4
    rings = [Ring(brokers=brokers, me=n) for n in range(brokers)]
    for partition in range(40):
        owners = [r.me for r in rings if r.owns("t", partition)]
        assert owners == [partition % brokers], f"partition {partition}: {owners}"


def test_every_broker_computes_the_same_table():
    """Agreement by construction: no protocol, no controller, no exchange of
    messages — every node derives the identical answer from identical static
    inputs. This is what replaces consensus in this design."""
    tables = [Ring(brokers=5, me=n).routing_table("t", 20) for n in range(5)]
    assert all(t == tables[0] for t in tables)


def test_my_partitions_partition_the_whole_range():
    # Union of every broker's share == all partitions, with no overlap.
    brokers, partitions = 3, 10
    shares = [Ring(brokers=brokers, me=n).my_partitions("t", partitions) for n in range(brokers)]
    flat = sorted(p for share in shares for p in share)
    assert flat == list(range(partitions))


def test_owns_is_consistent_with_broker_for():
    ring = Ring(brokers=4, me=2)
    for p in range(20):
        assert ring.owns("t", p) == (ring.broker_for("t", p) == 2)


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

    moved = sum(before.broker_for("t", p) != after.broker_for("t", p) for p in range(partitions))
    assert moved >= 75, f"only {moved}/100 moved — has the assignment changed?"


def test_a_partition_never_moves_while_the_cluster_is_the_same_size():
    # Stability under restarts: the ring is config, so a broker restarting
    # rebuilds the identical map.
    a, b = Ring(brokers=3, me=1), Ring(brokers=3, me=1)
    assert a == b
    assert a.routing_table("t", 30) == b.routing_table("t", 30)


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
        Ring(brokers=3, me=0).broker_for("t", -1)


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


def test_the_address_template_is_configurable():
    # One template with the port in it, so brokers can differ by port —
    # which is how several run on one machine.
    ring = Ring(brokers=2, me=0, address_template="localhost:909{ordinal}")
    assert ring.address_of(1) == "localhost:9091"


def test_the_routing_table_maps_partitions_to_addresses():
    assert Ring(brokers=2, me=0).routing_table("t", 4) == {
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
    monkeypatch.delenv("PYKA_ADDRESS_TEMPLATE", raising=False)
    ring = Ring.from_env()
    assert (ring.brokers, ring.me) == (1, 0)


def test_from_env_advertises_localhost_by_default(monkeypatch):
    """The default has to describe the DEFAULT deployment: one broker on a
    machine, reachable at localhost.

    Defaulting to the Kubernetes name meant a broker started on a laptop told
    every client to dial `pyka-0.pyka-hl:9092`, which resolves nowhere — so
    the README's own first example failed with a DNS error.
    """
    monkeypatch.setenv("HOSTNAME", "pyka-0")
    monkeypatch.delenv("PYKA_ADDRESS_TEMPLATE", raising=False)
    monkeypatch.delenv("PYKA_BROKERS", raising=False)
    monkeypatch.setenv("PYKA_PORT", "9095")
    assert Ring.from_env().address_of(0) == "localhost:9095"


def test_from_env_keeps_the_template_when_the_hostname_has_no_ordinal(monkeypatch):
    """The bug this pair of changes fixes.

    from_env used to raise on any hostname without an ordinal, and the caller
    caught it and rebuilt the ring from defaults — silently throwing away
    PYKA_ADDRESS_TEMPLATE. Setting the variable appeared to do nothing.
    """
    monkeypatch.setenv("HOSTNAME", "Andreis-MacBook-Pro.local")
    monkeypatch.delenv("PYKA_BROKERS", raising=False)
    monkeypatch.setenv("PYKA_ADDRESS_TEMPLATE", "my-host:7000")

    ring = Ring.from_env()
    assert (ring.brokers, ring.me) == (1, 0)
    assert ring.address_of(0) == "my-host:7000"


def test_from_env_honours_the_address_template(monkeypatch):
    monkeypatch.setenv("HOSTNAME", "kafka-1")
    monkeypatch.setenv("PYKA_BROKERS", "2")
    monkeypatch.setenv("PYKA_ADDRESS_TEMPLATE", "kafka-{ordinal}.internal:9000")
    assert Ring.from_env().address_of(1) == "kafka-1.internal:9000"


def test_a_hostname_without_an_ordinal_is_fine_for_one_broker(monkeypatch):
    # Laptops are not called pyka-0. With a single broker there is nothing to
    # identify, so demanding an ordinal only ever blocked local use.
    monkeypatch.setenv("HOSTNAME", "Andreis-MacBook-Pro.local")
    monkeypatch.delenv("PYKA_BROKERS", raising=False)
    assert Ring.from_env().me == 0


def test_a_hostname_without_an_ordinal_is_fatal_in_a_cluster(monkeypatch):
    # With more than one broker, identity decides who owns what. Guessing 0
    # would make two brokers claim the same partitions.
    monkeypatch.setenv("HOSTNAME", "pyka-7d4f-x9k2")  # a Deployment pod name
    monkeypatch.setenv("PYKA_BROKERS", "3")
    with pytest.raises(ValueError, match="cannot tell which of 3"):
        Ring.from_env()


# --------------------------------------------------------------------------
# HashRing — the two flaws of modulo, fixed and measured against it
# --------------------------------------------------------------------------


def test_the_hash_is_stable_across_processes():
    """blake2b, not hash(): Python randomises hash() for str per process, so a
    restart would rebuild the circle in a different order and move every
    partition. A router must be stable across processes and machines."""
    import subprocess
    import sys

    code = "from pyka.cluster.ring import digest; print(digest('orders:3'))"
    runs = {
        subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True,
            env={"PYTHONHASHSEED": seed, "PATH": "", "PYTHONPATH": "src"},
        ).stdout.strip()
        for seed in ("0", "1", "random")
    }
    assert len(runs) == 1, f"hash moved between processes: {runs}"


def test_consistent_hashing_moves_far_fewer_partitions_than_modulo():
    """The headline claim, measured rather than asserted.

    Adding a broker inserts new points on the circle and steals only the arc
    behind each one — about 1/N. Modulo has no such property: `p % 10` and
    `p % 11` are unrelated functions, so nearly every answer changes.
    """
    partitions = 1000
    for old, new in ((3, 4), (10, 11)):
        modulo = sum(
            Ring(brokers=old, me=0).broker_for("t", p)
            != Ring(brokers=new, me=0).broker_for("t", p)
            for p in range(partitions)
        )
        consistent = sum(
            HashRing(brokers=old, me=0).broker_for("t", p)
            != HashRing(brokers=new, me=0).broker_for("t", p)
            for p in range(partitions)
        )
        ideal = partitions / new
        assert consistent < modulo / 2, f"{old}->{new}: {consistent} vs {modulo}"
        assert consistent < ideal * 2, f"{old}->{new}: {consistent} vs ideal {ideal}"


def test_modulo_puts_every_single_partition_topic_on_broker_zero():
    """The hotspot, stated as a test so the fix has something to beat.

    `p % n` ignores the topic entirely, so partition 0 of every topic lands on
    the same machine — and a cluster of single-partition topics uses one broker
    while the rest idle.
    """
    ring = Ring(brokers=3, me=0)
    assert {ring.broker_for(f"topic-{i}", 0) for i in range(50)} == {0}


def test_consistent_hashing_spreads_single_partition_topics():
    from collections import Counter

    ring = HashRing(brokers=3, me=0)
    counts = Counter(ring.broker_for(f"topic-{i}", 0) for i in range(300))

    assert len(counts) == 3, "every broker should get some"
    assert min(counts.values()) > 300 / 3 * 0.5, f"badly skewed: {counts}"


def test_more_virtual_nodes_means_a_smoother_spread():
    """Why vnodes exist at all. One point per broker cuts the circle into a few
    arcs of wildly uneven size; ~128 points each averages them out."""
    from collections import Counter

    def skew(vnodes: int) -> float:
        ring = HashRing(brokers=4, me=0, vnodes=vnodes)
        counts = Counter(ring.broker_for("orders", p) for p in range(2000))
        return max(counts.values()) / min(counts.values())

    assert skew(256) < skew(1), "virtual nodes should reduce the imbalance"


def test_a_hash_ring_still_gives_every_partition_exactly_one_owner():
    # The invariant does not change with the algorithm.
    brokers = 4
    rings = [HashRing(brokers=brokers, me=n) for n in range(brokers)]
    for partition in range(200):
        owners = [r.me for r in rings if r.owns("orders", partition)]
        assert len(owners) == 1, f"partition {partition} has owners {owners}"


def test_every_broker_computes_the_same_hash_ring():
    # Agreement by construction survives the upgrade: still no messages.
    tables = [HashRing(brokers=5, me=n).routing_table("orders", 50) for n in range(5)]
    assert all(t == tables[0] for t in tables)


def test_a_hash_ring_is_stable_for_the_same_inputs():
    a, b = HashRing(brokers=3, me=1), HashRing(brokers=3, me=1)
    assert a.routing_table("orders", 100) == b.routing_table("orders", 100)


def test_topics_are_routed_independently():
    # Same partition index, different topics, different answers — which is
    # exactly what modulo cannot do.
    ring = HashRing(brokers=5, me=0)
    owners = {ring.broker_for(name, 0) for name in ("orders", "clicks", "events")}
    assert len(owners) > 1


@pytest.mark.parametrize("vnodes", [0, -1])
def test_a_hash_ring_needs_at_least_one_virtual_node(vnodes):
    with pytest.raises(ValueError, match="vnodes must be >= 1"):
        HashRing(brokers=3, me=0, vnodes=vnodes)


def test_a_hash_ring_validates_its_cluster_like_a_plain_ring():
    with pytest.raises(ValueError, match="outside a 3-broker cluster"):
        HashRing(brokers=3, me=5)


def test_a_hash_ring_rejects_a_negative_partition():
    with pytest.raises(ValueError, match="partition must be >= 0"):
        HashRing(brokers=3, me=0).broker_for("orders", -1)
