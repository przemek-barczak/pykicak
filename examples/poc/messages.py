"""Message types shared by the PoC injector and executors: two versions of a heartbeat.

The PoC shows an upgrade of the heartbeat from version 1 to version 2 while the flow keeps
running (see "A new version of a message" in docs/messages.md):

1. The fan-out executor, the consumer of the injector's heartbeats, accepts both versions, so
   version 1 heartbeats still waiting in its queue are processed after the upgrade. It publishes
   version 2 only, so it converts each version 1 heartbeat, with the sender "unknown".
2. The injector publishes version 2. `POC_HEARTBEAT_VERSION=1` runs the injector as it was before
   the upgrade, publishing version 1, which the fan-out executor still accepts.
3. The terminators, downstream of the fan-out executor, receive version 2 only.

Once no version 1 heartbeat is left in the fan-out executor's queue and its retry queue, it can
stop accepting version 1, and HeartbeatMessageV1 can be deleted.
"""

from __future__ import annotations

import dataclasses

from pykicak import KicakMessage


@dataclasses.dataclass(frozen=True, slots=True)
class HeartbeatMessageV1(KicakMessage):
    """Version 1 of a heartbeat: an increasing sequence number and its generation time.

    Its `message_id`, inherited from `KicakMessage`, is assigned once, when the injector creates
    the message, and never changes, so agents can process the message idempotently.

    `kicak_type` names this version; every heartbeat is sent with it as its AMQP `type` property,
    and an executor accepts it only if it declares this class.
    """

    kicak_type = "poc.heartbeat.v1"
    sequence: int
    generated_at: str


@dataclasses.dataclass(frozen=True, slots=True)
class HeartbeatMessageV2(KicakMessage):
    """Version 2 of a heartbeat: version 1, plus the host that sent it, which is required.

    A new version is a new class with its own `kicak_type`, never a change of HeartbeatMessageV1,
    whose heartbeats may still be queued with the old fields. It deliberately does not inherit
    from HeartbeatMessageV1, so `case HeartbeatMessageV1()` never matches a version 2 heartbeat.
    """

    kicak_type = "poc.heartbeat.v2"
    sequence: int
    generated_at: str
    sender: str
