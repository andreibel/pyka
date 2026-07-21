"""Entry point for the pyka-broker binary."""
import asyncio
import logging
import os
import signal

from pyka.broker.server import DEFAULT_GRACE, DEFAULT_PORT, BrokerServer

log = logging.getLogger(__name__)


async def serve() -> None:
    """Run until SIGTERM or SIGINT, then shut down gracefully.

    SIGTERM is what Kubernetes sends first; the grace period must fit inside
    terminationGracePeriodSeconds or the pod is SIGKILLed mid-drain, tearing
    the tail of whatever segment was being appended to.

    add_signal_handler, not signal.signal: the latter runs the handler between
    bytecodes on the main thread, which can land in the middle of an await.
    The loop version schedules it as a normal callback instead.
    """
    server = BrokerServer(port=int(os.environ.get("PYKA_PORT", DEFAULT_PORT)))
    await server.start()

    # B1 has no storage to recover, so it is ready as soon as it is listening.
    # In B2 this moves to after Topic has finished opening its logs.
    server.set_serving(True)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()
    log.info("shutting down")
    await server.stop(float(os.environ.get("PYKA_GRACE", DEFAULT_GRACE)))


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("PYKA_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    asyncio.run(serve())


if __name__ == "__main__":
    main()
