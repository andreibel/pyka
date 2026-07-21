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
import os
import socket
from dataclasses import dataclass


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
    host_template: str = "pyka-{ordinal}.pyka-hl"
    port: int = 9092

    def __post_init__(self) -> None:
        if self.brokers < 1:
            raise ValueError(f"brokers must be >= 1, got {self.brokers}")
        if not 0 <= self.me < self.brokers:
            raise ValueError(
                f"broker ordinal {self.me} is outside a {self.brokers}-broker cluster"
            )

    def broker_for(self, partition: int) -> int:
        """Which broker owns ``partition``.

        Deliberately the simplest rule that works, and it has a known flaw:
        changing ``brokers`` moves almost every partition, because ``p % 4``
        and ``p % 5`` agree for hardly any p. Every moved partition means its
        whole log migrating across the network. Consistent hashing exists to
        fix precisely this — move ~1/n instead of ~all — and is the natural
        follow-on once the pain is felt rather than read about.
        """
        if partition < 0:
            raise ValueError(f"partition must be >= 0, got {partition}")
        return partition % self.brokers

    def owns(self, partition: int) -> bool:
        """Is this broker the owner of ``partition``?

        The guard for a misrouted request. A client with stale metadata may
        send a produce for someone else's partition; accepting it would create
        a second log for the same partition on the wrong disk — the split-brain
        this design otherwise makes impossible.
        """
        return self.broker_for(partition) == self.me

    def my_partitions(self, partitions: int) -> list[int]:
        """Which of a topic's ``partitions`` this broker serves."""
        return [p for p in range(partitions) if self.owns(p)]

    def address_of(self, broker: int) -> str:
        """Where to reach ``broker`` — what a metadata response hands clients.

        The default template is the per-pod DNS a headless Service creates:
        pyka-1.pyka-hl resolves to that pod alone. A normal (load-balanced)
        Service would be wrong here — "any broker" is never the right answer
        when a specific one owns the partition.
        """
        if not 0 <= broker < self.brokers:
            raise ValueError(f"no broker {broker} in a {self.brokers}-broker cluster")
        return f"{self.host_template.format(ordinal=broker)}:{self.port}"

    def routing_table(self, partitions: int) -> dict[int, str]:
        """partition -> address, the body of a metadata response.

        Every broker produces an identical table, which is the point: a client
        can ask any of them and route correctly.
        """
        return {p: self.address_of(self.broker_for(p)) for p in range(partitions)}

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
            host_template=os.environ.get("PYKA_HOST_TEMPLATE", "pyka-{ordinal}.pyka-hl"),
            port=int(os.environ.get("PYKA_PORT", "9092")),
        )
