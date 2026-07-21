"""BrokerServicer: the gRPC service implementation.

Every method is `async def` because grpc.aio drives them on the event loop, but
nothing below layer 3 becomes async: file I/O stays blocking and moves to a
worker thread inside Store. That is the whole point of the layering, and it is
why `Log.append` needed a lock before this milestone could land — several
connections now really do reach one log at once.

Methods the contract declares but no milestone has implemented yet abort with
UNIMPLEMENTED, naming the milestone. A client built against them gets a clear
standard answer instead of a connection error or an invented shape.
"""
import asyncio
import contextlib
from collections.abc import AsyncIterator

import grpc

from pyka.broker.store import Store
from pyka.storage.log import OffsetOutOfRange
from pyka.storage.record import Record
from pyka.storage.types import Offset
from pyka.topic.topic import UnknownTopic
from pyka.v1 import broker_pb2, broker_pb2_grpc

_NOT_YET = "not implemented until {milestone}"

BATCH = 500
"""Records fetched per trip to the worker thread.

Streaming must not slurp: a consumer reading a 1 GiB partition from offset 0
cannot hold it in memory. Bigger batches mean fewer thread hops and more
memory held at once; 500 records of a few hundred bytes is well under a
megabyte, and the fixed cost of asyncio.to_thread is amortised over all of it.
"""

POLL_SECONDS = 30.0
"""Backstop for a following stream that is waiting on an append.

NOT the mechanism — Tail.notify is. This only bounds the damage if a
notification is ever missed: a stream that would otherwise hang forever
instead recovers within half a minute. If you find this timeout doing real
work, there is a missing notify() to go and find.
"""


def _key(request: broker_pb2.ProduceRequest) -> bytes | None:
    # HasField, never truthiness: an unset `optional bytes` reads back as b"",
    # so `request.value or None` would turn "present but empty" into "absent"
    # and quietly forge a tombstone. This is the wire-level twin of the
    # klen == -1 vs klen == 0 distinction in the record format.
    return request.key if request.HasField("key") else None


def _value(request: broker_pb2.ProduceRequest) -> bytes | None:
    return request.value if request.HasField("value") else None


def _timestamp(request: broker_pb2.ProduceRequest) -> int | None:
    return request.timestamp if request.HasField("timestamp") else None


def _to_proto(record: Record) -> broker_pb2.Record:
    """Wire form of a stored record.

    Absent fields are left unset rather than sent as b"": protobuf field
    presence is what carries "no key" and "tombstone" across the network, and
    assigning b"" would flatten both into an empty payload.
    """
    message = broker_pb2.Record(offset=record.offset, timestamp=record.timestamp)
    if record.key is not None:
        message.key = record.key
    if record.value is not None:
        message.value = record.value
    return message


class BrokerServicer(broker_pb2_grpc.BrokerServicer):
    def __init__(self, store: Store) -> None:
        self._store = store

    async def Produce(
        self, request: broker_pb2.ProduceRequest, context: grpc.aio.ServicerContext
    ) -> broker_pb2.ProduceResponse:
        """Append one record; return where it landed.

        The topic is auto-created if new — a producer writing to a fresh name
        is how topics come into existence. Consumers get the opposite rule.
        """
        partition, offset = await self._append(request, context)
        response = broker_pb2.ProduceResponse(partition=partition, offset=offset)
        if request.HasField("correlation_id"):
            response.correlation_id = request.correlation_id
        return response

    async def ProduceStream(
        self,
        request_iterator: AsyncIterator[broker_pb2.ProduceRequest],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[broker_pb2.ProduceResponse]:
        """Many appends over one call, so a client can keep several in flight.

        Known limitation: one bad record aborts the whole stream, because
        ProduceResponse carries no per-record error. It is recoverable — the
        client has already received a response for everything that landed, so
        it knows exactly how far it got — but a real broker reports per-record
        status instead. Revisit if streaming producers become the normal path.
        """
        async for request in request_iterator:
            partition, offset = await self._append(request, context)
            response = broker_pb2.ProduceResponse(partition=partition, offset=offset)
            if request.HasField("correlation_id"):
                response.correlation_id = request.correlation_id
            yield response

    async def _append(
        self, request: broker_pb2.ProduceRequest, context: grpc.aio.ServicerContext
    ) -> tuple[int, int]:
        try:
            return await self._store.append(
                request.topic, _key(request), _value(request), _timestamp(request)
            )
        except ValueError as err:
            # An illegal topic name or an oversized record: the client sent
            # something it can never send successfully, so say so plainly
            # rather than failing the connection.
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(err))
            raise AssertionError("unreachable")  # pragma: no cover — abort raises

    async def Metadata(
        self, request: broker_pb2.MetadataRequest, context: grpc.aio.ServicerContext
    ) -> broker_pb2.MetadataResponse:
        await context.abort(
            grpc.StatusCode.UNIMPLEMENTED, _NOT_YET.format(milestone="D4")
        )
        raise AssertionError("unreachable")  # pragma: no cover — abort() raises

    async def Consume(
        self, request: broker_pb2.ConsumeRequest, context: grpc.aio.ServicerContext
    ) -> AsyncIterator[broker_pb2.Record]:
        """Stream records from an offset until the log runs out.

        No partitioner here: a consumer names the partition it was assigned,
        so reads never route by key. Records carry their own offsets, which is
        how a client knows where to resume.

        Records are fetched a batch at a time rather than all at once — see
        BATCH. Between batches the event loop is free, so one consumer reading
        a huge partition does not stall every other connection.
        """
        offset = request.offset
        # max_records is ignored while following — the contract says so, and a
        # live tail that stops after N records is just a bounded read.
        remaining = (
            request.max_records
            if request.max_records > 0 and not request.follow
            else None
        )
        tail = self._store.tail

        while True:
            limit = BATCH if remaining is None else min(BATCH, remaining)
            # Subscribe BEFORE reading. Reading first and subscribing second
            # would miss an append landing in between: the notification fires
            # while nobody holds the event, and the consumer then waits for a
            # signal that has already gone.
            waiter = tail.subscribe(request.topic, request.partition) if request.follow else None

            batch = await self._read(request, offset, limit, context)
            for record in batch:
                yield _to_proto(record)
            if batch:
                offset = batch[-1].offset + 1

            if remaining is not None:
                remaining -= len(batch)
                if remaining <= 0:
                    return
            if len(batch) == limit:
                continue  # a full batch: more may already be waiting

            # Short batch: caught up with the log.
            if not request.follow:
                return
            if tail.closed:
                return  # broker shutting down
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(waiter.wait(), POLL_SECONDS)
            if tail.closed:
                return

    async def _read(
        self,
        request: broker_pb2.ConsumeRequest,
        offset: int,
        limit: int,
        context: grpc.aio.ServicerContext,
    ) -> list[Record]:
        try:
            return await self._store.read(
                request.topic, Offset(offset), request.partition, limit
            )
        except UnknownTopic:
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"no topic {request.topic!r}"
            )
        except OffsetOutOfRange as err:
            # Well-formed but unsatisfiable — the records are not here. A
            # consumer resuming from a stale committed offset lands here, and
            # should reset to the earliest offset rather than retry.
            await context.abort(grpc.StatusCode.OUT_OF_RANGE, str(err))
        except ValueError as err:
            # Malformed: an illegal topic name, or a partition this topic
            # does not have.
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(err))
        raise AssertionError("unreachable")  # pragma: no cover — abort raises
