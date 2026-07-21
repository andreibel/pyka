"""SyncPolicy: when a Log should fsync."""
from dataclasses import dataclass


@dataclass(frozen=True)
class SyncPolicy:
    """How much data we are willing to lose to a power cut.

    ``Segment.append`` already flushes Python's buffer to the OS every time,
    so a *process* crash loses nothing. fsync is the next step down — OS to
    physical platter — and it is the only thing that survives a power cut.
    It costs roughly a millisecond, which is why nobody does it per record
    without meaning to.

    Both thresholds are optional and OR'd; neither set means never. Frozen
    and pure — the counters live in the caller, so one policy instance is
    safely shared by every log.
    """

    records: int | None = None
    millis: int | None = None

    def should_sync(self, appends_since_sync: int, millis_since_sync: float) -> bool:
        """Called after every append, with that log's counters since its last
        fsync. Note the time bound is only *checked* on append: an idle log
        does not sync itself, because we have no background thread to do it.
        Kafka runs a scheduler for exactly this; ours would need one too.
        """
        if self.records is not None and appends_since_sync >= self.records:
            return True
        if self.millis is not None and millis_since_sync >= self.millis:
            return True
        return False


SYNC_EVERY_RECORD = SyncPolicy(records=1)
"""Durable per append, ~1 ms each. Correct, and roughly 1000x slower."""

SYNC_NEVER = SyncPolicy()
"""Rely on the OS to write back; sync() and close() remain explicit.

The default, and Kafka's too — but for a different reason. Kafka can afford
it because a record is durable once *replicated*; fsync is an optimization
there. We are single-node with no replication (a project guardrail), so this
default really does mean "a power cut can lose the tail". It is the right
default for a learning project and the wrong one for real data; phase B's
broker is where a deliberate choice belongs.
"""

