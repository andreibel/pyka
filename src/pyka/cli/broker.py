"""Entry point for the pyka-broker binary — two servers, one process.

    :9092  gRPC     data plane     produce / consume / metadata
    :8080  FastAPI  control plane  topics, segments, probes

They share one Store, and must: Segment holds an exclusive write handle, so
splitting them into two processes on one data directory would give each its
own idea of where the log ends.
"""
import asyncio
import logging
import os
import signal
from pathlib import Path

import uvicorn

from pyka.broker.admin import create_app
from pyka.broker.server import DEFAULT_GRACE, DEFAULT_PORT, BrokerServer
from pyka.broker.store import Store
from pyka.cluster.ring import Ring
from pyka.topic.policy import SyncPolicy

log = logging.getLogger(__name__)

DEFAULT_ADMIN_PORT = 8080
DEFAULT_ROOT = "/var/lib/pyka"


def _store_from_env() -> Store:
    """Build the Store from the environment a container is handed.

    The data root is a mounted volume in Kubernetes — a PersistentVolumeClaim
    that outlives the pod. Nothing here knows that; it is an ordinary path.
    """
    try:
        ring = Ring.from_env()
    except ValueError:
        # No StatefulSet ordinal in the hostname — running on a laptop, or in
        # a Deployment, which this design deliberately does not support for
        # more than one replica.
        ring = Ring(brokers=1, me=0)

    return Store(
        root=Path(os.environ.get("PYKA_DATA_DIR", DEFAULT_ROOT)),
        partitions=int(os.environ.get("PYKA_PARTITIONS", "1")),
        sync_policy=SyncPolicy(
            records=_optional_int("PYKA_SYNC_RECORDS"),
            millis=_optional_int("PYKA_SYNC_MILLIS"),
        ),
        max_segment_bytes=int(os.environ.get("PYKA_SEGMENT_BYTES", 1 << 30)),
        ring=ring,
    )


def _optional_int(name: str) -> int | None:
    value = os.environ.get(name)
    return int(value) if value else None


async def serve() -> None:
    store = _store_from_env()
    grpc_server = BrokerServer(
        store, port=int(os.environ.get("PYKA_PORT", DEFAULT_PORT))
    )
    await grpc_server.start()

    admin = uvicorn.Server(
        uvicorn.Config(
            create_app(store),
            host="0.0.0.0",
            port=int(os.environ.get("PYKA_ADMIN_PORT", DEFAULT_ADMIN_PORT)),
            log_config=None,  # keep our logging setup, not uvicorn's
        )
    )
    # We own the signal handling; uvicorn installing its own would race with
    # the shutdown sequence below and skip the gRPC drain entirely.
    admin.install_signal_handlers = lambda: None  # type: ignore[method-assign]
    admin_task = asyncio.create_task(admin.serve())

    # Both ports are listening now, but health stays NOT_SERVING and /readyz
    # returns 503 until recovery finishes — a scan of every segment on the
    # volume, ~15 s/GiB. This is the gap a naive probe turns into a crash loop.
    await store.open()
    grpc_server.set_serving(True)
    log.info("ready")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        # add_signal_handler, not signal.signal: the latter runs between
        # bytecodes on the main thread and can land in the middle of an await.
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()

    # Order matters, in four steps:
    #
    #   1. stop being routed to      (health NOT_SERVING)
    #   2. release parked live tails (they are in-flight RPCs waiting forever)
    #   3. drain the rest            (stop(grace) waits for real work)
    #   4. fsync and close files     (last: an in-flight append must not meet
    #                                 a closed segment)
    #
    # Step 2 is not optional and its position is not arbitrary. A follow=true
    # stream parked on an append is an in-flight RPC, so stop() waits for it —
    # but only store.close() releases it, and that is step 4. Put them in the
    # wrong order and shutdown blocks for the whole grace period, every time,
    # on any broker with one live consumer. In Kubernetes that is a 30-second
    # rolling update per pod, or a SIGKILL mid-append if the grace is shorter.
    log.info("shutting down")
    grpc_server.set_serving(False)
    admin.should_exit = True
    store.tail.close()
    await asyncio.gather(
        grpc_server.stop(float(os.environ.get("PYKA_GRACE", DEFAULT_GRACE))),
        admin_task,
    )
    await store.close()
    log.info("stopped")


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("PYKA_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    asyncio.run(serve())


if __name__ == "__main__":
    main()
