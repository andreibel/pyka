"""Tail: wakes streaming consumers when records are appended.

Live tail is `Consume(follow=true)`: the stream does not end at the last
record, it waits for the next one. Something has to tell it a record arrived,
and polling the log on a timer would trade latency against wasted reads.

Why a plain asyncio.Event is safe here, despite appends running in a worker
thread: the notification is sent from `Store.append`, which is `async def`
and has already come back from `await asyncio.to_thread(...)` — so it runs on
the event loop, not in the thread. The thread hop is inside the await. This
holds only while EVERY append goes through Store.append; something writing
straight to a Log (a replication follower, later) would have to signal with
`loop.call_soon_threadsafe`.

This class lives in layer 3 on purpose. Layers 1-2 are plain blocking code and
must not learn about asyncio — the log does not know what a consumer is.
"""
import asyncio


class Tail:
    def __init__(self) -> None:
        self._events: dict[tuple[str, int], asyncio.Event] = {}
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def subscribe(self, topic: str, partition: int) -> asyncio.Event:
        """The event to await for the next append to this partition.

        Must be called BEFORE reading the log, never after. A consumer that
        read first and subscribed second would miss any append landing in
        between and then wait for a notification that had already gone —
        hanging until some later append happened to wake it.
        """
        return self._events.setdefault((topic, partition), asyncio.Event())

    def notify(self, topic: str, partition: int) -> None:
        """Wake everyone waiting on this partition.

        The event is removed as it fires, so the next round of waiters gets a
        fresh one — a broadcast, not a queue. Nothing is buffered: a consumer
        that is busy reading when this fires does not need the signal, because
        it will see the new records in its next read.
        """
        event = self._events.pop((topic, partition), None)
        if event is not None:
            event.set()

    def close(self) -> None:
        """Release every waiter so the server can actually shut down.

        Without this, `server.stop(grace)` waits out the full grace period on
        streams that are parked indefinitely — and in Kubernetes that is the
        difference between a clean rolling update and a SIGKILL landing
        mid-append. Waiters check `closed` after waking and end their streams.
        """
        self._closed = True
        for event in self._events.values():
            event.set()
        self._events.clear()
