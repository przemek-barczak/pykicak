"""Binding definitions: which queue each exchange delivers to.

```text
injector ──► poc.heartbeat.inject ──► poc.heartbeat.source ──► executor ──► poc.heartbeat.fanout
                                                                            ├──► poc.heartbeat.terminator1
                                                                            └──► poc.heartbeat.terminator2
```

Each executor binds its own input queue to its source exchange, and the fan-out executor also
binds both terminator queues to its destination exchange before publishing, so a deleted
binding is restored from either side.

Each input queue's executor dead-letters the deliveries it cannot process to the queue's own
dead-letter exchange, publishing a copy that says why, and the exchange is bound to a dead-letter
queue that retains them. pykicak derives both names from the queue's, so they are not defined
here:

```text
poc.heartbeat.source      ──► poc.heartbeat.source.dlx      ──► poc.heartbeat.source.dlq
poc.heartbeat.terminator1 ──► poc.heartbeat.terminator1.dlx ──► poc.heartbeat.terminator1.dlq
poc.heartbeat.terminator2 ──► poc.heartbeat.terminator2.dlx ──► poc.heartbeat.terminator2.dlq
```
"""

from examples.poc.messaging.exchanges import HEARTBEAT_FANOUT_EXCHANGE, HEARTBEAT_INJECT_EXCHANGE
from examples.poc.messaging.queues import (
    HEARTBEAT_SOURCE_QUEUE,
    HEARTBEAT_TERMINATOR_QUEUE_1,
    HEARTBEAT_TERMINATOR_QUEUE_2,
)

QUEUE_BINDINGS: dict[str, str] = {
    HEARTBEAT_SOURCE_QUEUE: HEARTBEAT_INJECT_EXCHANGE,
    HEARTBEAT_TERMINATOR_QUEUE_1: HEARTBEAT_FANOUT_EXCHANGE,
    HEARTBEAT_TERMINATOR_QUEUE_2: HEARTBEAT_FANOUT_EXCHANGE,
}
"""Maps each executor's input queue to the exchange it is bound to, its source exchange."""


def queues_bound_to(exchange: str) -> tuple[str, ...]:
    """Return every queue bound to `exchange`: the destination queues of an executor publishing
    to it."""
    return tuple(queue for queue, bound_to in QUEUE_BINDINGS.items() if bound_to == exchange)
