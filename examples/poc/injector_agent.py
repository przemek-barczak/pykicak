"""PoC injector: generates a heartbeat message and publishes it to the exchange."""

from __future__ import annotations

import os
import sys
import uuid
from datetime import UTC, datetime

from examples.poc.messages import HeartbeatMessage
from examples.poc.messaging.exchanges import HEARTBEAT_INJECT_EXCHANGE
from pykicak import InjectionError, KicakConfig, KicakInjectorAbstract


class HeartbeatInjector(KicakInjectorAbstract):
    """Generates HeartbeatMessages with an incrementing sequence number and publishes them."""

    def __init__(self, config: KicakConfig) -> None:
        """Initialize the sequence counter alongside the base injector setup."""
        super().__init__(config, exchange_name=HEARTBEAT_INJECT_EXCHANGE)
        self._sequence = 0

    def generate(self) -> HeartbeatMessage:
        """Build the next HeartbeatMessage, with a new ID and the current UTC time.

        The ID is random because this is where the message is first created; agents that derive
        results from it use IDs computed from this one (e.g. with uuid.uuid5).
        """
        self._sequence += 1
        return HeartbeatMessage(
            message_id=str(uuid.uuid4()),
            sequence=self._sequence,
            generated_at=datetime.now(UTC).isoformat(),
        )


if __name__ == "__main__":
    config = KicakConfig.from_file(os.environ.get("KICAK_CONFIG_PATH", ".kicak"))
    injector = HeartbeatInjector(config)
    try:
        injector.run()  # one-shot; pass interval_seconds=60 to inject a heartbeat every minute
    except InjectionError as error:
        # run() has already closed the connection; exit status 1 tells the caller it failed
        sys.exit(str(error))
