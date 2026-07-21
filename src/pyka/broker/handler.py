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
from collections.abc import AsyncIterator

import grpc

from pyka.broker.store import Store
from pyka.v1 import broker_pb2, broker_pb2_grpc

_NOT_YET = "not implemented until {milestone}"


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
        await context.abort(
            grpc.StatusCode.UNIMPLEMENTED, _NOT_YET.format(milestone="B3")
        )
        yield broker_pb2.Record()  # pragma: no cover
