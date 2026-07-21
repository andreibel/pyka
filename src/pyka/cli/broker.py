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

try:
    import uvicorn
except ModuleNotFoundError as err:  # pragma: no cover — a packaging path
    # `pip install pyka-log` gets the CLIENT — Producer, Consumer, Admin — and
    # nothing else, because a client has no business pulling a web framework.
    # Running a broker is the extra. A bare ModuleNotFoundError three frames
    # deep would send someone hunting for a bug that is really a missing flag.
    raise SystemExit(
        f"pyka-broker needs the broker dependencies (missing: {err.name}).\n"
        "They are an optional extra:\n"
        "    pip install 'pyka-log[broker]'\n"
        "    uv sync --all-extras          # from a checkout"
    ) from err

from pyka.broker.admin import create_app
from pyka.broker.server import DEFAULT_GRACE, DEFAULT_PORT, BrokerServer
from pyka.broker.store import Store
from pyka.cluster.ring import HashRing, Ring
from pyka.topic.policy import SyncPolicy

log = logging.getLogger(__name__)

DEFAULT_ADMIN_PORT = 8080
DEFAULT_ROOT = "/var/lib/pyka"


def _store_from_env() -> Store:
    """Build the Store from the environment a container is handed.

    The data root is a mounted volume in Kubernetes — a PersistentVolumeClaim
    that outlives the pod. Nothing here knows that; it is an ordinary path.
    """
    # HashRing by default: modulo moves ~90% of partitions when the cluster
    # resizes and puts partition 0 of every topic on broker 0. Ring is kept
    # for the comparison, selectable for anyone who wants the simpler rule.
    ring_class = Ring if os.environ.get("PYKA_RING") == "modulo" else HashRing
    # No try/except: from_env handles a laptop hostname itself. Catching its
    # ValueError and rebuilding the ring from defaults silently discarded
    # PYKA_ADDRESS_TEMPLATE, so a broker advertised a Kubernetes DNS name to
    # clients that could not resolve it — and said nothing about why.
    ring = ring_class.from_env()

    root = Path(os.environ.get("PYKA_DATA_DIR", DEFAULT_ROOT))
    try:
        root.mkdir(parents=True, exist_ok=True)
    except PermissionError as err:
        # The default suits a container, where /var/lib/pyka is a mounted
        # volume owned by the broker. On a laptop it is root-owned, and a
        # bare PermissionError traceback is a miserable first impression.
        raise SystemExit(
            f"cannot use {root} as the data directory: {err.strerror}.\n"
            "Set PYKA_DATA_DIR to somewhere writable, for example:\n"
            "    PYKA_DATA_DIR=./data pyka-broker"
        ) from err

    return Store(
        root=root,
        partitions=int(os.environ.get("PYKA_PARTITIONS", "1")),
        sync_policy=SyncPolicy(
            records=_optional_int("PYKA_SYNC_RECORDS"),
            millis=_optional_int("PYKA_SYNC_MILLIS"),
        ),
        max_segment_bytes=int(os.environ.get("PYKA_SEGMENT_BYTES", 1 << 30)),
        ring=ring,
        allow_orphans=os.environ.get("PYKA_ALLOW_ORPHANS") == "1",
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

    # Wait for uvicorn to actually bind before going any further. Without this,
    # a port conflict on the admin side left the broker running with gRPC up,
    # "ready" in the log, and no control plane at all — a half-started process
    # that had already announced itself. Fail fast and loudly instead.
    while not admin.started and not admin_task.done():
        await asyncio.sleep(0.01)
    if admin_task.done():
        admin_task.result()  # re-raises whatever uvicorn failed with
        raise RuntimeError("admin server exited during startup")

    # Both ports are listening now, but health stays NOT_SERVING and /readyz
    # returns 503 until recovery finishes — a scan of every segment on the
    # volume, ~15 s/GiB. This is the gap a naive probe turns into a crash loop.
    await store.open()
    # Both probes must agree. store.open() can decline to become ready — it
    # refuses when this broker holds partitions it no longer owns — and gRPC
    # health saying SERVING while /readyz says 503 would let a k8s gRPC probe
    # route traffic to a broker the HTTP probe is holding back.
    grpc_server.set_serving(store.ready)
    if store.ready:
        log.info("ready")
    else:
        log.error("NOT ready — serving nothing until this is resolved")

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
