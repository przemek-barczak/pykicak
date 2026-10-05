"""Message type shared by the PoC injector and executors."""

from __future__ import annotations

import dataclasses

from pykicak import KicakMessage


@dataclasses.dataclass(frozen=True, slots=True)
class HeartbeatMessage(KicakMessage):
    """A single heartbeat: an increasing sequence number and its generation time.

    Its `message_id`, inherited from `KicakMessage`, is assigned once, when the injector creates
    the message, and never changes, so agents can process the message idempotently.
    """

    sequence: int
    generated_at: str
