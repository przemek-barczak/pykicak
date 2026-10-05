"""Exchange name definitions for the PoC heartbeat topology."""

HEARTBEAT_INJECT_EXCHANGE = "poc.heartbeat.inject"
"""Exchange the injector publishes new heartbeat messages to."""

HEARTBEAT_FANOUT_EXCHANGE = "poc.heartbeat.fanout"
"""Exchange the fan-out executor republishes to; terminators bind their own queues to it."""

HEARTBEAT_SOURCE_DEAD_LETTER_EXCHANGE = "poc.heartbeat.source.dlx"
"""Dead-letter exchange of HEARTBEAT_SOURCE_QUEUE; receives messages the fan-out executor rejects."""

HEARTBEAT_TERMINATOR_DEAD_LETTER_EXCHANGE_1 = "poc.heartbeat.terminator1.dlx"
"""Dead-letter exchange of HEARTBEAT_TERMINATOR_QUEUE_1; receives messages the first terminator rejects."""

HEARTBEAT_TERMINATOR_DEAD_LETTER_EXCHANGE_2 = "poc.heartbeat.terminator2.dlx"
"""Dead-letter exchange of HEARTBEAT_TERMINATOR_QUEUE_2; receives messages the second terminator rejects."""
