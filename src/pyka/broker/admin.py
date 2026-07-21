"""Admin API: the control plane, on FastAPI.

Split from gRPC by traffic profile, not by taste. Control-plane calls are rare,
made by people and probes, and want to be curl-able and self-documenting —
FastAPI gives OpenAPI docs at /docs for free. Data-plane calls are constant,
made by client libraries, and want streaming and raw bytes — that is gRPC's
job, on another port in this same process.

The interesting endpoint is the segment listing. It makes the storage layer
observable: you can watch segments roll, watch the sparse index fill, and see
recovery happen after a pod restart. For a system whose whole subject is files
on disk, that view is worth more than the CRUD around it.
"""
import asyncio

from fastapi import APIRouter, FastAPI, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from pyka.broker.store import Store
from pyka.storage.log import Log
from pyka.topic.topic import UnknownTopic

router = APIRouter()


# ---------------------------------------------------------------- schemas


class CreateTopic(BaseModel):
    name: str
    partitions: int | None = Field(default=None, ge=1)


class SegmentInfo(BaseModel):
    base_offset: int
    next_offset: int
    size_bytes: int
    index_entries: int
    sealed: bool = Field(description="closed, therefore no longer writable")


class PartitionInfo(BaseModel):
    partition: int
    base_offset: int
    next_offset: int
    size_bytes: int
    segments: list[SegmentInfo]


class TopicInfo(BaseModel):
    name: str
    partitions: int


class BrokerInfo(BaseModel):
    broker_id: int
    brokers: int
    ready: bool
    topics: int


# ---------------------------------------------------------------- helpers


def _store(request: Request) -> Store:
    return request.app.state.store


def _partition_info(partition: int, log: Log) -> PartitionInfo:
    segments = [
        SegmentInfo(
            base_offset=segment.base_offset,
            next_offset=segment.next_offset,
            size_bytes=segment.size_bytes,
            index_entries=segment.index_entries,
            sealed=segment.sealed,
        )
        for segment in log.segments
    ]
    return PartitionInfo(
        partition=partition,
        base_offset=segments[0].base_offset,
        next_offset=log.next_offset,
        size_bytes=sum(s.size_bytes for s in segments),
        segments=segments,
    )


# ---------------------------------------------------------------- probes


@router.get("/healthz", tags=["probes"])
async def healthz() -> dict[str, str]:
    """Liveness: the process is up. Deliberately says nothing about storage —
    a broker still recovering is alive, and restarting it would only make it
    start the same scan again."""
    return {"status": "ok"}


@router.get("/readyz", tags=["probes"])
async def readyz(request: Request, response: Response) -> dict[str, bool]:
    """Readiness: recovery has finished and traffic is welcome.

    503 while recovering, so Kubernetes holds requests back instead of routing
    them to a broker that would block on a segment scan.
    """
    ready = _store(request).ready
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"ready": ready}


# ---------------------------------------------------------------- topics


@router.get("/", tags=["broker"])
async def broker_info(request: Request) -> BrokerInfo:
    store = _store(request)
    return BrokerInfo(
        broker_id=store.ring.me,
        brokers=store.ring.brokers,
        ready=store.ready,
        topics=len(await store.names()),
    )


@router.get("/topics", tags=["topics"])
async def list_topics(request: Request) -> list[str]:
    return await _store(request).names()


@router.post("/topics", status_code=status.HTTP_201_CREATED, tags=["topics"])
async def create_topic(request: Request, body: CreateTopic) -> TopicInfo:
    """Create a topic, or return the existing one unchanged.

    Idempotent, and it never re-partitions: changing the count would move
    every key to a different partition and break per-key ordering. Ask for a
    different count on an existing topic and you get the real one back.
    """
    store = _store(request)
    try:
        count = await store.create(body.name, body.partitions)
    except ValueError as err:  # invalid name — the security boundary
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(err)) from err
    return TopicInfo(name=body.name, partitions=count)


@router.get("/topics/{name}", tags=["topics"])
async def describe_topic(request: Request, name: str) -> TopicInfo:
    # No try/except here: _to_thread already maps UnknownTopic to 404 and
    # ValueError to 400, in one place, for every endpoint.
    store = _store(request)
    count = await _to_thread(store, store.topic.partitions_of, name)
    return TopicInfo(name=name, partitions=count)


@router.get("/topics/{name}/partitions/{partition}", tags=["topics"])
async def describe_partition(
    request: Request, name: str, partition: int
) -> PartitionInfo:
    """The segment chain of one partition — where the storage layer shows.

    Watch `sealed` flip and a new segment appear as the log rolls, watch
    `index_entries` climb with size, and watch both survive a pod restart.
    """
    store = _store(request)
    log = await _get_log(store, name, partition)
    return await _to_thread(store, _partition_info, partition, log)


@router.post("/topics/{name}/sync", tags=["topics"])
async def sync_topic(request: Request, name: str) -> dict[str, str]:
    """Force an fsync on every partition of this topic.

    The escape hatch from SyncPolicy: with SYNC_NEVER the tail is only as
    durable as the OS decides, and this is how an operator makes it durable
    now — before a planned shutdown, say.
    """
    store = _store(request)
    logs = await _all_logs(store, name)
    for log in logs:
        await _to_thread(store, log.sync)
    return {"status": "synced"}


@router.post("/topics/{name}/partitions/{partition}/roll", tags=["topics"])
async def roll_partition(request: Request, name: str, partition: int) -> SegmentInfo:
    """Seal the active segment and start a new one.

    Sealing is what makes a segment eligible for retention, so this is a real
    operational request rather than a test hook — and the quickest way to see
    the chain grow without writing a gigabyte.
    """
    store = _store(request)
    log = await _get_log(store, name, partition)
    segment = await _to_thread(store, log.roll)
    return SegmentInfo(
        base_offset=segment.base_offset,
        next_offset=segment.next_offset,
        size_bytes=segment.size_bytes,
        index_entries=segment.index_entries,
        sealed=segment.sealed,
    )


# ------------------------------------------------------- storage plumbing


async def _to_thread(store: Store, fn, *args):
    """Run a blocking storage call off the event loop, mapping storage
    exceptions onto HTTP status codes in one place."""
    try:
        return await asyncio.to_thread(fn, *args)
    except UnknownTopic as err:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(err)) from err
    except ValueError as err:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(err)) from err


async def _get_log(store: Store, name: str, partition: int) -> Log:
    return await _to_thread(store, store.topic.get, name, partition)


async def _all_logs(store: Store, name: str) -> list[Log]:
    count = await _to_thread(store, store.topic.partitions_of, name)
    return [await _get_log(store, name, p) for p in range(count)]


# ---------------------------------------------------------------- the app


def create_app(store: Store) -> FastAPI:
    app = FastAPI(
        title="pyKA admin",
        version="0.1.0",
        description="Control plane. The data plane is gRPC on :9092.",
    )
    app.state.store = store
    app.include_router(router)
    return app
