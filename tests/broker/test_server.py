"""B1: the gRPC server stands up, reports health, and refuses cleanly.

Every test runs a real server on a real socket (port 0 = let the OS pick, so
tests never race for a fixed port) and talks to it over a real channel. There
are no mocks here: gRPC's failure modes live in the plumbing, so testing the
plumbing is the point.
"""

import grpc
import pytest
from grpc_health.v1 import health_pb2, health_pb2_grpc

from pyka.broker.server import SERVICE_NAME, BrokerServer
from pyka.v1 import broker_pb2, broker_pb2_grpc


@pytest.fixture
async def server():
    server = BrokerServer(port=0)
    await server.start()
    yield server
    await server.stop(grace=0)


@pytest.fixture
async def channel(server):
    async with grpc.aio.insecure_channel(server.address) as channel:
        yield channel


# --------------------------------------------------------------------------
# lifecycle
# --------------------------------------------------------------------------


async def test_the_server_binds_a_real_port():
    # port=0 asks the OS for any free port; the server must report back which.
    server = BrokerServer(port=0)
    await server.start()
    assert server.port > 0
    assert server.address.endswith(str(server.port))
    await server.stop(grace=0)


async def test_the_server_accepts_a_connection(channel):
    await channel.channel_ready()


async def test_stop_is_graceful_and_leaves_the_port_closed(channel, server):
    await server.stop(grace=0)
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await broker_pb2_grpc.BrokerStub(channel).Produce(broker_pb2.ProduceRequest())
    assert err.value.code() == grpc.StatusCode.UNAVAILABLE


# --------------------------------------------------------------------------
# health — what a Kubernetes probe actually calls
# --------------------------------------------------------------------------


async def test_health_starts_NOT_SERVING(channel):
    """The distinction that keeps k8s from killing a recovering broker.

    The port binds immediately, but recovery scans every segment at ~15 s/GiB
    (bench/). A readiness probe that only checks "is the port open" would send
    traffic to a broker that cannot answer — or worse, a startup probe would
    declare it dead and restart it, forever.
    """
    health = health_pb2_grpc.HealthStub(channel)
    response = await health.Check(health_pb2.HealthCheckRequest())
    assert response.status == health_pb2.HealthCheckResponse.NOT_SERVING


async def test_health_reports_SERVING_once_ready(channel, server):
    server.set_serving(True)
    health = health_pb2_grpc.HealthStub(channel)
    response = await health.Check(health_pb2.HealthCheckRequest())
    assert response.status == health_pb2.HealthCheckResponse.SERVING


async def test_health_is_reported_for_the_named_service_too(channel, server):
    # "" is the overall key a k8s probe uses by default; the named service is
    # what a client checks when it cares about this API specifically.
    server.set_serving(True)
    health = health_pb2_grpc.HealthStub(channel)
    response = await health.Check(health_pb2.HealthCheckRequest(service=SERVICE_NAME))
    assert response.status == health_pb2.HealthCheckResponse.SERVING


async def test_health_goes_NOT_SERVING_before_the_door_closes(channel, server):
    # stop() flips health first so a load balancer stops sending work before
    # connections are refused — clients get routed away, not errored.
    server.set_serving(True)
    health = health_pb2_grpc.HealthStub(channel)
    assert (await health.Check(health_pb2.HealthCheckRequest())).status == (
        health_pb2.HealthCheckResponse.SERVING
    )

    server.set_serving(False)
    assert (await health.Check(health_pb2.HealthCheckRequest())).status == (
        health_pb2.HealthCheckResponse.NOT_SERVING
    )


async def test_an_unknown_service_is_NOT_FOUND(channel):
    health = health_pb2_grpc.HealthStub(channel)
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await health.Check(health_pb2.HealthCheckRequest(service="nope.Service"))
    assert err.value.code() == grpc.StatusCode.NOT_FOUND


# --------------------------------------------------------------------------
# the contract exists, the bodies do not
# --------------------------------------------------------------------------


async def test_produce_is_unimplemented(channel):
    stub = broker_pb2_grpc.BrokerStub(channel)
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await stub.Produce(broker_pb2.ProduceRequest(topic="orders", value=b"hi"))
    assert err.value.code() == grpc.StatusCode.UNIMPLEMENTED
    assert "B2" in err.value.details()


async def test_metadata_is_unimplemented(channel):
    stub = broker_pb2_grpc.BrokerStub(channel)
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await stub.Metadata(broker_pb2.MetadataRequest())
    assert err.value.code() == grpc.StatusCode.UNIMPLEMENTED


async def test_produce_stream_is_unimplemented(channel):
    stub = broker_pb2_grpc.BrokerStub(channel)
    call = stub.ProduceStream(iter([broker_pb2.ProduceRequest(topic="orders")]))
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await call.read()
    assert err.value.code() == grpc.StatusCode.UNIMPLEMENTED


async def test_consume_is_unimplemented(channel):
    # Server-streaming: the error surfaces on the first read, not the call.
    stub = broker_pb2_grpc.BrokerStub(channel)
    call = stub.Consume(broker_pb2.ConsumeRequest(topic="orders", partition=0))
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await call.read()
    assert err.value.code() == grpc.StatusCode.UNIMPLEMENTED
    assert "B3" in err.value.details()


# --------------------------------------------------------------------------
# reflection — how grpcurl works without the .proto
# --------------------------------------------------------------------------


async def test_reflection_lists_the_broker_service(channel):
    from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

    stub = reflection_pb2_grpc.ServerReflectionStub(channel)
    call = stub.ServerReflectionInfo(
        iter([reflection_pb2.ServerReflectionRequest(list_services="")])
    )
    response = await call.read()
    services = {s.name for s in response.list_services_response.service}
    assert SERVICE_NAME in services
