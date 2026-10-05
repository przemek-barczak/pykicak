"""Binding definitions: which consuming executor binds each queue to which exchange.

```text
injector ──► poc.heartbeat.inject ──► poc.heartbeat.source ──► executor ──► poc.heartbeat.fanout
                                                                            ├──► poc.heartbeat.terminator1
                                                                            └──► poc.heartbeat.terminator2
```

Each input queue dead-letters the deliveries its executor rejects (`basic_nack(requeue=False)`)
to its own dead-letter exchange, which is bound to a dead-letter queue that retains them:

```text
poc.heartbeat.source      ──► poc.heartbeat.source.dlx      ──► poc.heartbeat.source.dlq
poc.heartbeat.terminator1 ──► poc.heartbeat.terminator1.dlx ──► poc.heartbeat.terminator1.dlq
poc.heartbeat.terminator2 ──► poc.heartbeat.terminator2.dlx ──► poc.heartbeat.terminator2.dlq
```
"""

from examples.poc.messaging.exchanges import (
    HEARTBEAT_FANOUT_EXCHANGE,
    HEARTBEAT_INJECT_EXCHANGE,
    HEARTBEAT_SOURCE_DEAD_LETTER_EXCHANGE,
    HEARTBEAT_TERMINATOR_DEAD_LETTER_EXCHANGE_1,
    HEARTBEAT_TERMINATOR_DEAD_LETTER_EXCHANGE_2,
)
from examples.poc.messaging.queues import (
    HEARTBEAT_SOURCE_DEAD_LETTER_QUEUE,
    HEARTBEAT_SOURCE_QUEUE,
    HEARTBEAT_TERMINATOR_DEAD_LETTER_QUEUE_1,
    HEARTBEAT_TERMINATOR_DEAD_LETTER_QUEUE_2,
    HEARTBEAT_TERMINATOR_QUEUE_1,
    HEARTBEAT_TERMINATOR_QUEUE_2,
)

QUEUE_BINDINGS: dict[str, str] = {
    HEARTBEAT_SOURCE_QUEUE: HEARTBEAT_INJECT_EXCHANGE,
    HEARTBEAT_TERMINATOR_QUEUE_1: HEARTBEAT_FANOUT_EXCHANGE,
    HEARTBEAT_TERMINATOR_QUEUE_2: HEARTBEAT_FANOUT_EXCHANGE,
    HEARTBEAT_SOURCE_DEAD_LETTER_QUEUE: HEARTBEAT_SOURCE_DEAD_LETTER_EXCHANGE,
    HEARTBEAT_TERMINATOR_DEAD_LETTER_QUEUE_1: HEARTBEAT_TERMINATOR_DEAD_LETTER_EXCHANGE_1,
    HEARTBEAT_TERMINATOR_DEAD_LETTER_QUEUE_2: HEARTBEAT_TERMINATOR_DEAD_LETTER_EXCHANGE_2,
}
"""Maps each consumer-owned queue, dead-letter queues included, to the exchange it is bound to."""

DEAD_LETTER_EXCHANGES: dict[str, str] = {
    HEARTBEAT_SOURCE_QUEUE: HEARTBEAT_SOURCE_DEAD_LETTER_EXCHANGE,
    HEARTBEAT_TERMINATOR_QUEUE_1: HEARTBEAT_TERMINATOR_DEAD_LETTER_EXCHANGE_1,
    HEARTBEAT_TERMINATOR_QUEUE_2: HEARTBEAT_TERMINATOR_DEAD_LETTER_EXCHANGE_2,
}
"""Maps each input queue to its dead-letter exchange (its x-dead-letter-exchange argument)."""
