"""PoC executors: consume heartbeat messages and optionally republish them to an exchange.

This demonstrates both executor roles from the PoC topology (see examples.poc.messaging):
1. fanout   — consumes poc.heartbeat.source, republishes to poc.heartbeat.fanout
2. terminator1/terminator2 — each declares its own queue bound to the fanout exchange

Every role also declares its input queue's dead-letter exchange and queue, which retain the
deliveries that role rejects. Each executor takes its topology from examples.poc.messaging,
the single place where the PoC's queue, exchange, and binding names are defined.

Role is selected via the POC_EXECUTOR_ROLE environment variable, so the same script can be
run three times with different roles (each with its own `.kicak` connection file).
Start both terminator roles before publishing messages so they can create and bind their queues.
Ctrl+C or SIGTERM stops an executor gracefully, after the message in progress.
"""

from __future__ import annotations

import os
import signal

from examples.poc.messages import HeartbeatMessage
from examples.poc.messaging.bindings import DEAD_LETTER_EXCHANGES, QUEUE_BINDINGS
from examples.poc.messaging.exchanges import HEARTBEAT_FANOUT_EXCHANGE
from examples.poc.messaging.queues import (
    HEARTBEAT_QUEUE_TYPE,
    HEARTBEAT_SOURCE_DEAD_LETTER_QUEUE,
    HEARTBEAT_SOURCE_QUEUE,
    HEARTBEAT_TERMINATOR_DEAD_LETTER_QUEUE_1,
    HEARTBEAT_TERMINATOR_DEAD_LETTER_QUEUE_2,
    HEARTBEAT_TERMINATOR_QUEUE_1,
    HEARTBEAT_TERMINATOR_QUEUE_2,
)
from pykicak import KicakConfig, KicakExecutorAbstract


class HeartbeatForwarder(KicakExecutorAbstract[HeartbeatMessage]):
    """Fan-out role: forwards each HeartbeatMessage unchanged to the destination exchange."""

    def message_type(self) -> type[HeartbeatMessage]:
        """HeartbeatMessage is the type expected on the source queue."""
        return HeartbeatMessage

    def execute(self, message: HeartbeatMessage) -> HeartbeatMessage:
        """Forward the message unchanged, so executing it again publishes the same message ID."""
        print(f"forwarding: {message}")
        return message


class HeartbeatTerminator(KicakExecutorAbstract[HeartbeatMessage]):
    """Terminator role: prints each HeartbeatMessage and publishes nothing."""

    def message_type(self) -> type[HeartbeatMessage]:
        """HeartbeatMessage is the type expected on the source queue."""
        return HeartbeatMessage

    def execute(self, message: HeartbeatMessage) -> None:
        """Print the message; printing it again on a redelivery is harmless."""
        print(f"received: {message}")


def _build_executor(
    config: KicakConfig, role: str
) -> HeartbeatForwarder | HeartbeatTerminator:
    """Build the executor for `role`, wiring its topology from examples.poc.messaging."""
    if role == "fanout":
        return HeartbeatForwarder(
            config,
            source_queue=HEARTBEAT_SOURCE_QUEUE,
            source_exchange=QUEUE_BINDINGS[HEARTBEAT_SOURCE_QUEUE],
            dead_letter_exchange=DEAD_LETTER_EXCHANGES[HEARTBEAT_SOURCE_QUEUE],
            dead_letter_queue=HEARTBEAT_SOURCE_DEAD_LETTER_QUEUE,
            queue_type=HEARTBEAT_QUEUE_TYPE,
            destination_exchange=HEARTBEAT_FANOUT_EXCHANGE,
        )
    terminator_queues = {
        "terminator1": (HEARTBEAT_TERMINATOR_QUEUE_1, HEARTBEAT_TERMINATOR_DEAD_LETTER_QUEUE_1),
        "terminator2": (HEARTBEAT_TERMINATOR_QUEUE_2, HEARTBEAT_TERMINATOR_DEAD_LETTER_QUEUE_2),
    }
    if role in terminator_queues:
        source_queue, dead_letter_queue = terminator_queues[role]
        return HeartbeatTerminator(
            config,
            source_queue=source_queue,
            source_exchange=QUEUE_BINDINGS[source_queue],
            dead_letter_exchange=DEAD_LETTER_EXCHANGES[source_queue],
            dead_letter_queue=dead_letter_queue,
            queue_type=HEARTBEAT_QUEUE_TYPE,
        )
    raise ValueError(f"Unknown POC_EXECUTOR_ROLE: {role!r}")


if __name__ == "__main__":
    config = KicakConfig.from_file(os.environ.get("KICAK_CONFIG_PATH", ".kicak"))
    executor = _build_executor(config, os.environ.get("POC_EXECUTOR_ROLE", "fanout"))
    # Ctrl+C or SIGTERM: finish the message in progress, then stop before taking another one
    for stop_signal in (signal.SIGINT, signal.SIGTERM):
        signal.signal(stop_signal, lambda _signum, _frame: executor.stop())
    executor.run()
