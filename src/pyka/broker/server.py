"""BrokerServer: the grpc.aio server and its lifecycle.

Three things ride along with the Broker service, and each one exists for a
concrete operational reason rather than for completeness:

* the standard **health** service — Kubernetes has spoken this protocol
  natively since 1.24, so a readiness probe becomes a real RPC instead of
  "is the port open". That distinction matters here: recovery scans every
  segment at ~15 s/GiB (see bench/), so the port binds long before the broker
  can actually serve. Health is NOT_SERVING until storage is ready.
* **reflection** — lets grpcurl work without the .proto file on hand, which
  buys back the debuggability given up by not using a text protocol.
* **graceful stop** — drains in-flight RPCs on SIGTERM. Without it every
  rolling update tears the tail of an active segment.
"""
import logging
from typing import Self

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from pyka.broker.handler import BrokerServicer
from pyka.broker.store import Store
from pyka.v1 import broker_pb2, broker_pb2_grpc

log = logging.getLogger(__name__)

SERVICE_NAME = broker_pb2.DESCRIPTOR.services_by_name["Broker"].full_name
DEFAULT_PORT = 9092  # Kafka's, by convention
DEFAULT_GRACE = 30.0


class BrokerServer:
    """Owns the gRPC server. One instance per process.

    Not a context manager on purpose: startup and shutdown are awaited, and
    `async with` would hide the grace period — the one number that decides
    whether a rolling update loses data.
    """

    def __init__(
        self, store: Store, port: int = DEFAULT_PORT, host: str = "[::]"
    ) -> None:
        self._host = host
        self._port = port
        self._server = grpc.aio.server()
        self._health = health.HealthServicer()

        broker_pb2_grpc.add_BrokerServicer_to_server(
            BrokerServicer(store), self._server
        )
        health_pb2_grpc.add_HealthServicer_to_server(self._health, self._server)
        reflection.enable_server_reflection(
            (SERVICE_NAME, health.SERVICE_NAME, reflection.SERVICE_NAME), self._server
        )

        # Port 0 means "let the OS pick" — how the tests get a free port
        # without racing each other for a fixed one.
        self._bound_port = self._server.add_insecure_port(f"{host}:{port}")

    @property
    def port(self) -> int:
        """The port actually bound, which differs from the requested one when
        that was 0."""
        return self._bound_port

    @property
    def address(self) -> str:
        return f"localhost:{self._bound_port}"

    async def start(self) -> Self:
        """Bind and accept, but report NOT_SERVING until told otherwise.

        Two states on purpose: a liveness probe wants "the process is alive"
        (true from here), while a readiness probe wants "send me traffic"
        (false until set_serving). Collapsing them is what makes Kubernetes
        kill a broker that is merely still recovering.
        """
        await self._server.start()
        self.set_serving(False)
        log.info("gRPC listening on %s:%d", self._host, self._bound_port)
        return self

    def set_serving(self, serving: bool) -> None:
        status = (
            health_pb2.HealthCheckResponse.SERVING
            if serving
            else health_pb2.HealthCheckResponse.NOT_SERVING
        )
        # "" is the overall-server key that k8s probes use by default; the
        # named service is what a client checks for this API specifically.
        self._health.set("", status)
        self._health.set(SERVICE_NAME, status)

    async def wait_for_termination(self) -> None:
        await self._server.wait_for_termination()

    async def stop(self, grace: float = DEFAULT_GRACE) -> None:
        """Refuse new RPCs, let in-flight ones finish, then close.

        Health flips first so a load balancer stops sending work before the
        door closes — otherwise clients meet a refused connection instead of
        being routed elsewhere.
        """
        self.set_serving(False)
        await self._server.stop(grace)
        log.info("gRPC stopped")
