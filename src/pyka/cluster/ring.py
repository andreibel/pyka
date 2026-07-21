"""Ring: partition -> broker, by static modulo assignment.

The whole cluster design in one idea: every broker computes the same answer
from the same static inputs, so they agree *by construction* rather than by
protocol. No controller, no election, no heartbeats, no metadata store — and
therefore no split-brain, because there is nothing to disagree about.

The inputs come from Kubernetes. A StatefulSet with ``replicas: 3`` declares
that pyka-0, pyka-1 and pyka-2 exist, and gives each pod a stable ordinal that
survives restarts. That declaration IS the membership answer; k8s maintains it
with its own consensus (etcd) so we do not need ours.

What this buys: sharding — capacity scales with brokers, and clients route
straight to the owner. What it does NOT buy: availability. There is no
replication here, so a broker being down makes its partitions unreachable
rather than served from a copy. That is a deliberate scope choice, not an
oversight; see README "Cluster".
"""
import bisect
import hashlib
import os
import socket
from dataclasses import dataclass, field


def digest(value: str) -> int:
    """A stable 64-bit hash.

    blake2b rather than hash(): Python randomises hash() for str per process,
    so a restart would rebuild the ring in a different order and move every
    partition. Stable across processes, machines, and Python versions is not
    optional for something that decides where data lives.
    """
    return int.from_bytes(
        hashlib.blake2b(value.encode(), digest_size=8).digest(), "big"
    )


@dataclass(frozen=True)
class Ring:
    """Static partition->broker assignment for a cluster of ``brokers`` nodes.

    Frozen on purpose: the ring is configuration, never runtime state. If it
    could change while running, two brokers could hold different versions and
    agreement-by-construction would be gone — which is exactly the problem a
    real cluster needs consensus to solve.
    """

    brokers: int
    me: int

    address_template: str = "pyka-{ordinal}.pyka-hl:9092"
    """host:port of broker N — one template, port included.

    Brokers differ by port as well as host: in Kubernetes they share a port
    and differ by hostname (``pyka-1.pyka-hl:9092``), while several on one
    laptop share a host and differ by port (``localhost:909{ordinal}``). A
    separate host and port field could not express the second.
    """

    def __post_init__(self) -> None:
        if self.brokers < 1:
            raise ValueError(f"brokers must be >= 1, got {self.brokers}")
        if not 0 <= self.me < self.brokers:
            raise ValueError(
                f"broker ordinal {self.me} is outside a {self.brokers}-broker cluster"
            )

    def broker_for(self, topic: str, partition: int) -> int:
        """Which broker owns this partition. Two known flaws, both real.

        **Churn.** Changing ``brokers`` moves almost every partition, because
        ``p % 4`` and ``p % 5`` agree for hardly any p — measured at 9 of 12.
        Every moved partition means its whole log migrating.

        **Hotspots.** The answer ignores ``topic`` entirely, so partition 0 of
        EVERY topic lands on broker 0. A cluster full of single-partition
        topics puts all of them on one machine while the others idle.

        HashRing fixes both. This class is kept because the comparison is the
        lesson: its churn and its skew are measured against HashRing's in the
        test suite.
        """
        if partition < 0:
            raise ValueError(f"partition must be >= 0, got {partition}")
        return partition % self.brokers

    def owns(self, topic: str, partition: int) -> bool:
        """Is this broker the owner of this partition?

        The guard for a misrouted request. A client with stale metadata may
        send a produce for someone else's partition; accepting it would create
        a second log for the same partition on the wrong disk — the split-brain
        this design otherwise makes impossible.
        """
        return self.broker_for(topic, partition) == self.me

    def my_partitions(self, topic: str, partitions: int) -> list[int]:
        """Which of a topic's ``partitions`` this broker serves."""
        return [p for p in range(partitions) if self.owns(topic, p)]

    def address_of(self, broker: int) -> str:
        """Where to reach ``broker`` — what a metadata response hands clients.

        The default template is the per-pod DNS a headless Service creates:
        pyka-1.pyka-hl resolves to that pod alone. A normal (load-balanced)
        Service would be wrong here — "any broker" is never the right answer
        when a specific one owns the partition.
        """
        if not 0 <= broker < self.brokers:
            raise ValueError(f"no broker {broker} in a {self.brokers}-broker cluster")
        return self.address_template.format(ordinal=broker)

    def routing_table(self, topic: str, partitions: int) -> dict[int, str]:
        """partition -> address, the body of a metadata response.

        Every broker produces an identical table, which is the point: a client
        can ask any of them and route correctly.
        """
        return {
            p: self.address_of(self.broker_for(topic, p)) for p in range(partitions)
        }

    @classmethod
    def from_env(cls) -> "Ring":
        """Build from the environment a StatefulSet pod is born with.

        ``me`` comes from the ordinal suffix of the hostname (pyka-1 -> 1) —
        the identity k8s guarantees is stable across restarts, so a broker
        learns who it is by looking in the mirror rather than being told.
        ``brokers`` must be supplied, because a pod cannot see its own
        StatefulSet's replica count.
        """
        hostname = os.environ.get("HOSTNAME") or socket.gethostname()
        _, _, ordinal = hostname.rpartition("-")
        if not ordinal.isdigit():
            raise ValueError(
                f"hostname {hostname!r} has no StatefulSet ordinal suffix — "
                f"expected something like 'pyka-1'"
            )
        return cls(
            brokers=int(os.environ.get("PYKA_BROKERS", "1")),
            me=int(ordinal),
            address_template=os.environ.get(
                "PYKA_ADDRESS_TEMPLATE", "pyka-{ordinal}.pyka-hl:9092"
            ),
        )


@dataclass(frozen=True)
class HashRing(Ring):
    """Consistent hashing: partitions and brokers on one 64-bit circle.

    Every broker is hashed onto the circle at ``vnodes`` positions. A partition
    hashes to a point and belongs to the first broker position clockwise.

    **Why this beats modulo on churn.** Adding a broker inserts new points and
    steals only the arc immediately behind each one — about 1/N of the circle.
    Every other partition keeps its owner, because nothing about their position
    changed. Modulo has no such property: `p % 4` and `p % 5` are unrelated
    functions, so almost every answer changes at once.

    **Why virtual nodes.** With one point per broker, three brokers cut the
    circle into three arcs of wildly uneven size — one machine can easily own
    half the keyspace. Hashing each broker to ~128 points averages the arcs out;
    the idea comes from Chord and Dynamo. (Google's 2016 "Consistent Hashing
    with Bounded Loads" goes further and caps any node's share outright; we do
    not need that here.)

    **Why the topic name is in the hash.** Modulo ignores it, so partition 0 of
    every topic lands on broker 0 and single-partition topics all pile onto one
    machine. Hashing ``"orders:0"`` and ``"clicks:0"`` separates them.

    What it still does NOT do: move any data. When a partition's owner changes,
    its segments stay on the old broker's disk while the new owner starts an
    empty log — so resizing remains an offline operation with a migration step.
    Consistent hashing shrinks the blast radius; it does not remove it. Kafka
    avoids the problem entirely by storing an explicit assignment, which needs
    the controller we deliberately do not have.
    """

    vnodes: int = 128

    # Derived, not configured: the sorted circle and its keys for bisect.
    _points: tuple[tuple[int, int], ...] = field(default=(), repr=False, compare=False)
    _keys: tuple[int, ...] = field(default=(), repr=False, compare=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.vnodes < 1:
            raise ValueError(f"vnodes must be >= 1, got {self.vnodes}")

        points = tuple(
            sorted(
                (digest(f"broker-{broker}#{v}"), broker)
                for broker in range(self.brokers)
                for v in range(self.vnodes)
            )
        )
        # object.__setattr__ because the dataclass is frozen: the circle is
        # derived from the fields, so computing it once here keeps the object
        # immutable AND keeps broker_for from rebuilding it on every call.
        object.__setattr__(self, "_points", points)
        object.__setattr__(self, "_keys", tuple(key for key, _ in points))

    def broker_for(self, topic: str, partition: int) -> int:
        if partition < 0:
            raise ValueError(f"partition must be >= 0, got {partition}")
        # First broker position clockwise, wrapping past the top of the circle
        # — that wrap is what makes it a ring rather than a line.
        index = bisect.bisect_right(self._keys, digest(f"{topic}:{partition}"))
        return self._points[index % len(self._points)][1]
