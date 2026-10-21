"""Exchange name definitions for the PoC heartbeat topology."""

HEARTBEAT_INJECT_EXCHANGE = "poc.heartbeat.inject"
"""Exchange the injector publishes new heartbeat messages to; only the source queue is bound to it."""

HEARTBEAT_FANOUT_EXCHANGE = "poc.heartbeat.fanout"
"""Exchange the fan-out executor republishes to; both terminator queues are bound to it."""
