"""PoC executors: consume heartbeat messages and optionally republish them to an exchange.

This demonstrates both executor roles from the PoC topology (see examples.poc.messaging):
1. fanout   — consumes poc.heartbeat.source, republishes to poc.heartbeat.fanout, and before
              publishing declares and binds both terminator queues to it
2. terminator1/terminator2 — each declares its own queue bound to the fanout exchange

Every role also declares its input queue's dead-letter exchange and queue, `<queue>.dlx` and
`<queue>.dlq`, which retain the deliveries that role rejects, and its retry queue
`<queue>.retry`, where a message that keeps failing transiently waits before it is executed
again. Each executor takes its topology from examples.poc.messaging, the single place where the
PoC's queue, exchange, and binding names are defined.

The class headers declare which heartbeat versions each role accepts and publishes, the contract
between the agents. They show an upgrade from version 1 to version 2 in progress (see
examples.poc.messages): the fanout role accepts both versions and publishes version 2, the only
one the terminators accept.

Role is selected via the POC_EXECUTOR_ROLE environment variable, so the same script can be
run three times with different roles (each with its own `.kicak` connection file).
Start the fanout role before running the injector, so that its queue is bound to the injector's
exchange; the terminator queues collect messages even before their executors start.
Ctrl+C or SIGTERM stops an executor gracefully, after the message in progress.
"""

from __future__ import annotations

import logging
import os
import signal
from typing import assert_never

from examples.poc.messages import HeartbeatMessageV1, HeartbeatMessageV2
from examples.poc.messaging.bindings import QUEUE_BINDINGS, queues_bound_to
from examples.poc.messaging.exchanges import HEARTBEAT_FANOUT_EXCHANGE
from examples.poc.messaging.queues import (
    HEARTBEAT_QUEUE_TYPE,
    HEARTBEAT_SOURCE_QUEUE,
    HEARTBEAT_TERMINATOR_QUEUE_1,
    HEARTBEAT_TERMINATOR_QUEUE_2,
)
from pykicak import KicakConfig, KicakExecutorAbstract


class HeartbeatForwarder(
    KicakExecutorAbstract[HeartbeatMessageV1 | HeartbeatMessageV2, HeartbeatMessageV2]
):
    """Fan-out role: forwards each heartbeat to the destination exchange as version 2.

    Its class header accepts both versions, so the injector can be upgraded from version 1 to
    version 2 while version 1 heartbeats are still queued, or still published by an injector
    instance that has not been upgraded yet. Each delivery is decoded into the version its
    `type` property names. It publishes version 2 only.
    """

    def execute(self, message: HeartbeatMessageV1 | HeartbeatMessageV2) -> HeartbeatMessageV2:
        """Forward a version 2 heartbeat unchanged, and convert a version 1 one into version 2.

        The result keeps the heartbeat's message ID, so executing it again publishes the same
        message.
        """
        match message:
            case HeartbeatMessageV2():
                heartbeat = message
            case HeartbeatMessageV1():
                # Version 2 requires a sender, which version 1 did not record. This PoC passes the
                # same message type through; a real executor usually publishes results of its own
                # work, of a type of its own, and needs no such value.
                heartbeat = HeartbeatMessageV2(
                    message_id=message.message_id,
                    sequence=message.sequence,
                    generated_at=message.generated_at,
                    sender="unknown",
                )
            case _:
                assert_never(message)  # a type checker reports a version left unhandled
        print(f"forwarding: {heartbeat}")
        return heartbeat


class HeartbeatTerminator(KicakExecutorAbstract[HeartbeatMessageV2, None]):
    """Terminator role: prints each heartbeat and publishes nothing (result type None).

    It accepts version 2 only, the version the fan-out executor upstream of it publishes.
    """

    def execute(self, message: HeartbeatMessageV2) -> None:
        """Print the message; printing it again on a redelivery is harmless."""
        print(f"received: {message}")


def _build_executor(config: KicakConfig, role: str) -> HeartbeatForwarder | HeartbeatTerminator:
    """Build the executor for `role`, wiring its topology from examples.poc.messaging."""
    if role == "fanout":
        return HeartbeatForwarder(
            config,
            source_queue=HEARTBEAT_SOURCE_QUEUE,
            source_exchange=QUEUE_BINDINGS[HEARTBEAT_SOURCE_QUEUE],
            destination_exchange=HEARTBEAT_FANOUT_EXCHANGE,
            destination_queues=queues_bound_to(HEARTBEAT_FANOUT_EXCHANGE),
            queue_type=HEARTBEAT_QUEUE_TYPE,
        )
    terminator_queues = {
        "terminator1": HEARTBEAT_TERMINATOR_QUEUE_1,
        "terminator2": HEARTBEAT_TERMINATOR_QUEUE_2,
    }
    if role in terminator_queues:
        source_queue = terminator_queues[role]
        return HeartbeatTerminator(
            config,
            source_queue=source_queue,
            source_exchange=QUEUE_BINDINGS[source_queue],
            queue_type=HEARTBEAT_QUEUE_TYPE,
        )
    raise ValueError(f"Unknown POC_EXECUTOR_ROLE: {role!r}")


if __name__ == "__main__":
    # pykicak only emits log records; the application decides where they go and from which level.
    # Warnings from every library, and pykicak's own INFO records (messages received and published).
    logging.basicConfig(
        level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("pykicak").setLevel(logging.INFO)
    config = KicakConfig.from_file(os.environ.get("KICAK_CONFIG_PATH", ".kicak"))
    executor = _build_executor(config, os.environ.get("POC_EXECUTOR_ROLE", "fanout"))
    # Ctrl+C or SIGTERM: finish the message in progress, then stop before taking another one
    for stop_signal in (signal.SIGINT, signal.SIGTERM):
        signal.signal(stop_signal, lambda _signum, _frame: executor.stop())
    executor.run()
