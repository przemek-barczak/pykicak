"""PoC injector: generates a heartbeat message and publishes it to the exchange.

It publishes version 2 of the heartbeat. With POC_HEARTBEAT_VERSION=1 it runs as it was before
the upgrade to version 2 and publishes version 1, as an injector instance not upgraded yet would
during a rolling upgrade; the fan-out executor accepts both (see examples.poc.messages).
"""

from __future__ import annotations

import os
import socket
import sys
import uuid
from datetime import UTC, datetime

from examples.poc.messages import HeartbeatMessageV1, HeartbeatMessageV2
from examples.poc.messaging.exchanges import HEARTBEAT_INJECT_EXCHANGE
from pykicak import InjectionError, KicakConfig, KicakInjectorAbstract


class HeartbeatInjector(KicakInjectorAbstract[HeartbeatMessageV2]):
    """Generates version 2 heartbeats with an incrementing sequence number and publishes them.

    Its class header declares the version it publishes; the fan-out executor must accept it.
    """

    def __init__(self, config: KicakConfig) -> None:
        """Initialize the sequence counter alongside the base injector setup."""
        super().__init__(config, exchange_name=HEARTBEAT_INJECT_EXCHANGE)
        self._sequence = 0

    def generate(self) -> HeartbeatMessageV2:
        """Build the next heartbeat, with a new ID, the current UTC time, and this host's name.

        The ID is random because this is where the message is first created; agents that derive
        results from it use IDs computed from this one (e.g. with uuid.uuid5).
        """
        self._sequence += 1
        return HeartbeatMessageV2(
            message_id=str(uuid.uuid4()),
            sequence=self._sequence,
            generated_at=datetime.now(UTC).isoformat(),
            sender=socket.gethostname(),
        )


class HeartbeatInjectorV1(KicakInjectorAbstract[HeartbeatMessageV1]):
    """The injector as it was before the upgrade: generates version 1 heartbeats."""

    def __init__(self, config: KicakConfig) -> None:
        """Initialize the sequence counter alongside the base injector setup."""
        super().__init__(config, exchange_name=HEARTBEAT_INJECT_EXCHANGE)
        self._sequence = 0

    def generate(self) -> HeartbeatMessageV1:
        """Build the next heartbeat, with a new ID and the current UTC time."""
        self._sequence += 1
        return HeartbeatMessageV1(
            message_id=str(uuid.uuid4()),
            sequence=self._sequence,
            generated_at=datetime.now(UTC).isoformat(),
        )


def _build_injector(config: KicakConfig, version: str) -> HeartbeatInjector | HeartbeatInjectorV1:
    """Build the injector of heartbeat `version`: "2", the current one, or "1"."""
    if version == "2":
        return HeartbeatInjector(config)
    if version == "1":
        return HeartbeatInjectorV1(config)
    raise ValueError(f"Unknown POC_HEARTBEAT_VERSION: {version!r}")


if __name__ == "__main__":
    config = KicakConfig.from_file(os.environ.get("KICAK_CONFIG_PATH", ".kicak"))
    injector = _build_injector(config, os.environ.get("POC_HEARTBEAT_VERSION", "2"))
    try:
        injector.run()  # one-shot; pass interval_seconds=60 to inject a heartbeat every minute
    except InjectionError as error:
        # run() has already closed the connection; exit status 1 tells the caller it failed
        sys.exit(str(error))
