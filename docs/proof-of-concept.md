# Proof of concept

[`examples/poc/`](../examples/poc) in the repository contains a runnable example (examples are not
part of the installed package): an injector that publishes a heartbeat message, an executor that
publishes it to a fan-out exchange, and two terminator executors that each declare their own queue
and bind it to that exchange. The fan-out executor names both terminator queues in its
`destination_queues`, so it declares and binds them too before publishing. Each executor also
declares the dead-letter exchange and queue of its input queue (for example
`poc.heartbeat.source.dlx` → `poc.heartbeat.source.dlq`) and its retry queue
(`poc.heartbeat.source.retry`). Start the fan-out executor before running the injector: until it has
declared its queue, the injector fails with `UnroutableError`. The terminators can start at any
time; their queues collect the messages meanwhile. The queue/exchange topology is defined once in
`examples/poc/messaging/` (`queues.py`, `exchanges.py`, `bindings.py`) and imported by every agent —
none of it lives in `.kicak`.

The PoC also shows [a new version of a message](messages.md#a-new-version-of-a-message) being
introduced while the flow keeps running. `examples/poc/messages.py` defines two versions of the
heartbeat, and each agent's class header declares which ones it publishes and accepts:

| Agent | Class header | Role in the upgrade |
| --- | --- | --- |
| `HeartbeatInjector` | `KicakInjectorAbstract[HeartbeatMessageV2]` | the upgraded producer: publishes version 2 |
| `HeartbeatInjectorV1` | `KicakInjectorAbstract[HeartbeatMessageV1]` | the producer before the upgrade: publishes version 1 |
| `HeartbeatForwarder` | `KicakExecutorAbstract[HeartbeatMessageV1 \| HeartbeatMessageV2, HeartbeatMessageV2]` | the consumer, upgraded first: accepts both versions, converts version 1 to version 2 with the sender `"unknown"` |
| `HeartbeatTerminator` | `KicakExecutorAbstract[HeartbeatMessageV2, None]` | downstream: receives version 2 only |

Run the injector with `POC_HEARTBEAT_VERSION=1` and without it: the fan-out executor processes both
versions, and the terminators print version 2 heartbeats, with `sender='unknown'` for the ones that
started as version 1. Version 2 requires a sender, which version 1 never recorded, so the fan-out
executor has to supply one. That comes from the PoC passing the same message type through; a real
Executor usually publishes results of its own work, of a type of its own.

Each process reads its `.kicak` file (connection details only) from the path in the
`KICAK_CONFIG_PATH` environment variable (defaulting to `.kicak`). The executor's role is
selected via `POC_EXECUTOR_ROLE` (`fanout`, `terminator1`, or `terminator2`), and the heartbeat
version the injector publishes via `POC_HEARTBEAT_VERSION` (`2`, the default, or `1`). Ctrl+C or
SIGTERM stops an executor gracefully through `stop()`. Run the commands from the repository root:

```bash
KICAK_CONFIG_PATH=.kicak.injector python -m examples.poc.injector_agent
KICAK_CONFIG_PATH=.kicak.injector POC_HEARTBEAT_VERSION=1 python -m examples.poc.injector_agent
KICAK_CONFIG_PATH=.kicak.executor POC_EXECUTOR_ROLE=fanout python -m examples.poc.executor_agent
KICAK_CONFIG_PATH=.kicak.terminator1 POC_EXECUTOR_ROLE=terminator1 python -m examples.poc.executor_agent
KICAK_CONFIG_PATH=.kicak.terminator2 POC_EXECUTOR_ROLE=terminator2 python -m examples.poc.executor_agent
```
