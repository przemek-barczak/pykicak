"""Queue name and type definitions for the PoC heartbeat topology."""

from pykicak import QueueType

HEARTBEAT_QUEUE_TYPE = QueueType.CLASSIC
"""Type of every PoC queue. QueueType.QUORUM replicates the queues across a RabbitMQ cluster;
RabbitMQ cannot change the type of an existing queue, so delete the PoC queues before switching."""

HEARTBEAT_SOURCE_QUEUE = "poc.heartbeat.source"
"""Queue the fan-out executor consumes from; bound to HEARTBEAT_INJECT_EXCHANGE."""

HEARTBEAT_TERMINATOR_QUEUE_1 = "poc.heartbeat.terminator1"
"""First terminator's input queue; its executor binds it to HEARTBEAT_FANOUT_EXCHANGE."""

HEARTBEAT_TERMINATOR_QUEUE_2 = "poc.heartbeat.terminator2"
"""Second terminator's input queue; its executor binds it to HEARTBEAT_FANOUT_EXCHANGE."""

HEARTBEAT_SOURCE_DEAD_LETTER_QUEUE = "poc.heartbeat.source.dlq"
"""Retains messages rejected from HEARTBEAT_SOURCE_QUEUE; bound to HEARTBEAT_SOURCE_DEAD_LETTER_EXCHANGE."""

HEARTBEAT_TERMINATOR_DEAD_LETTER_QUEUE_1 = "poc.heartbeat.terminator1.dlq"
"""Retains messages rejected from HEARTBEAT_TERMINATOR_QUEUE_1; bound to HEARTBEAT_TERMINATOR_DEAD_LETTER_EXCHANGE_1."""

HEARTBEAT_TERMINATOR_DEAD_LETTER_QUEUE_2 = "poc.heartbeat.terminator2.dlq"
"""Retains messages rejected from HEARTBEAT_TERMINATOR_QUEUE_2; bound to HEARTBEAT_TERMINATOR_DEAD_LETTER_EXCHANGE_2."""
