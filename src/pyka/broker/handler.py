"""BrokerServicer: the gRPC service implementation.

B1 declares every method the contract defines and implements none of them.
That is the idiomatic gRPC shape: the .proto *is* the API, so it is written in
full up front and the bodies arrive milestone by milestone. A client built
against this today gets UNIMPLEMENTED — a clear, standard answer — instead of
a connection error or an invented shape it would later have to unlearn.

The storage seam goes here in B2:

    await asyncio.to_thread(self._topic.append, name, key, value)

Every method is `async def` because grpc.aio drives them on the event loop.
Nothing below layer 3 becomes async: file I/O stays blocking and moves to a
worker thread, which is the whole point of the layering.
"""
from collections.abc import AsyncIterator

import grpc

from pyka.v1 import broker_pb2, broker_pb2_grpc

_NOT_YET = "not implemented until {milestone}"


class BrokerServicer(broker_pb2_grpc.BrokerServicer):
    async def Metadata(
        self, request: broker_pb2.MetadataRequest, context: grpc.aio.ServicerContext
    ) -> broker_pb2.MetadataResponse:
        await context.abort(
            grpc.StatusCode.UNIMPLEMENTED, _NOT_YET.format(milestone="B2")
        )
        raise AssertionError("unreachable")  # pragma: no cover — abort() raises

    async def Produce(
        self, request: broker_pb2.ProduceRequest, context: grpc.aio.ServicerContext
    ) -> broker_pb2.ProduceResponse:
        await context.abort(
            grpc.StatusCode.UNIMPLEMENTED, _NOT_YET.format(milestone="B2")
        )
        raise AssertionError("unreachable")  # pragma: no cover

    async def ProduceStream(
        self,
        request_iterator: AsyncIterator[broker_pb2.ProduceRequest],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[broker_pb2.ProduceResponse]:
        await context.abort(
            grpc.StatusCode.UNIMPLEMENTED, _NOT_YET.format(milestone="B2")
        )
        yield broker_pb2.ProduceResponse()  # pragma: no cover — makes this a generator

    async def Consume(
        self, request: broker_pb2.ConsumeRequest, context: grpc.aio.ServicerContext
    ) -> AsyncIterator[broker_pb2.Record]:
        await context.abort(
            grpc.StatusCode.UNIMPLEMENTED, _NOT_YET.format(milestone="B3")
        )
        yield broker_pb2.Record()  # pragma: no cover
