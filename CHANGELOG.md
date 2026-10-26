# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-10-26

Every Executor now needs the keyword-only `max_retry_rounds`. With classic queues, the queue
arguments are unchanged, so agents can be upgraded one at a time. Quorum input queues are now
declared with at-least-once dead-lettering, and RabbitMQ refuses to redeclare a quorum queue
declared by 0.3.0 without it (`406 PRECONDITION_FAILED`): with quorum queues, upgrade with an empty
system: stop the Injectors, let the Executors process every queue empty, stop them, delete the
quorum queues they consume from, then start the 0.4.0 agents.

### Added

- Dead-lettering publishes a confirmed copy of the message to `<queue>.dlx` with headers saying why
  (`x-kicak-dead-letter-reason`: `malformed`, `retries-exhausted`, `result-too-large`, or
  `unexpected-error`), which exception (`x-kicak-error-type`, `x-kicak-error-message`), when, and
  from which queue, and acknowledges the original only afterwards. A copy RabbitMQ returns or
  refuses requeues the message instead of losing it.
- `max_retry_rounds`, a required argument of the Executor: a message still failing transiently
  after that many rounds through the retry queue is dead-lettered as `retries-exhausted`. A message
  sent back from the dead-letter queue gets all its rounds again.
- `get_info()` reports how deliveries ended, as `counts` (`ExecutorCounts` and `DeadLetterCounts`,
  new exports, only growing, for Prometheus-style counters), and the delivery in progress:
  `attempt`, `redelivered`, and `retry_round`.
- Statistics in the logs: one `Message handled` line per delivery and one `Message injected` line
  per message an Injector hands to RabbitMQ, whatever the outcome, with durations, body sizes, and
  `queued_seconds`, the time a message waited in its queue. Every published message carries the
  time it was published in the `x-kicak-published-at` header. Leaving a recovery status logs
  `recovery_seconds`, and a graceful stop `stop_seconds`. The records carry the values, typed, as
  their `kicak` attribute.
- Client certificates (mutual TLS), through the optional keys `RABBIT_MQ_CERT_FILE` and
  `RABBIT_MQ_KEY_FILE`. Encrypted keys are refused instead of prompting on the terminal.
- `tls_options(config)` (new export): pika's TLS options with the policy agents connect with, for
  an application's own pika connections.
- `examples/replay_dead_letters.py`, an example tool that sends the dead letters of chosen reasons
  back to their queue.
- The user documentation in `docs/`: the README is an overview, and the manual has a page per
  topic, including "When a message can still be lost".
- Python 3.15 in the package classifiers.

### Changed

- **Breaking:** `KicakExecutorAbstract` requires `max_retry_rounds`; there is no default.
- **Breaking:** transient failures no longer go round the retry queue forever: after
  `max_retry_rounds` rounds the message is dead-lettered.
- **Breaking:** the Executor's dead-letter copies have no `x-death` header; read
  `x-kicak-dead-letter-reason` first, and `x-death` for messages RabbitMQ dead-letters itself.
- **Breaking:** quorum queues an Executor consumes from are declared with `x-dead-letter-strategy`
  `at-least-once` and `x-overflow` `reject-publish`, so a message RabbitMQ dead-letters itself,
  e.g. at the delivery limit, is kept until `<queue>.dlq` takes it instead of being dropped. A
  quorum queue with a length limit now refuses new messages instead of dropping its oldest.
- **Breaking:** `from_bytes()` refuses a body that lacks a field, also one the class gives a
  default, and a message class with a `field(init=False)` is refused when an agent declaring it is
  created.
- **Breaking:** `ExecutorInfo` has new required fields, for code that constructs it directly.
- The Executor and the Injector are built of private parts in `pykicak/_executor/` and
  `pykicak/_injector/`; public import paths and behavior are unchanged.
- Public docstrings state the contract and refer to `docs/` for how things work.

### Fixed

- Retry and dead-letter copies drop the `CC` and `BCC` routing headers; another publisher's `CC`
  naming the source queue delivered a retry copy at once and doubled it every round.
- A connection whose AMQP handshake never completes (`AMQPConnectorStackTimeout`) is retried,
  instead of stopping an Executor and ending an Injector after one attempt.
- `InjectionError`'s repr no longer shows the message payload.
- No connection is left open: after RabbitMQ closed only its channel, when connection setup is
  interrupted, e.g. by `KeyboardInterrupt`, or by `run()` after `connect()` and `stop()`.
- An exception whose `str()` fails, raised while decoding a message or serializing a result,
  dead-letters the message as malformed instead of stopping the Executor.
- The field names of a malformed body no longer reach the logs and the dead-letter headers.
- Durations too large for a float raise `ValueError` instead of `OverflowError`, and
  `retry_delay_seconds` just above its maximum is refused instead of rounded down to it.
- Invalid UTF-8 in a `.kicak` file that starts with a byte order mark is reported on its own line.
- The documentation no longer promises that a message is never lost: the cases RabbitMQ can still
  lose one, e.g. with classic queues, are listed. Its account of the quorum delivery limit covers
  RabbitMQ 4.3 and newer, where requeues no longer count toward it.

## [0.3.0] - 2026-10-22

This release changes the messages on the wire and the topology, so 0.2.0 and 0.3.0 agents cannot
share a system. Upgrade with an empty one: stop the Injectors, let the Executors process every
queue empty, stop them, delete the queues whose dead-letter exchange is not named `<queue>.dlx`,
then start the 0.3.0 agents. A message published by a 0.2.0 agent has no `type` property, so a
0.3.0 Executor dead-letters it. Set `RABBIT_MQ_TLS` in every configuration.

### Added

- TLS connections, configured by the required `RABBIT_MQ_TLS` key and the optional
  `RABBIT_MQ_CA_FILE`: TLS 1.2 or newer, the broker's certificate and host name verified, with the
  same strict certificate checks on every supported Python version. A certificate that fails
  verification is not retried.
- `KicakConfig.from_env()`, which reads the connection keys from the environment variables of the
  same names, and no other variable. A `KicakConfig` is now immutable, hashable, and picklable.
- Message type names: every message class declares its `kicak_type`, which every message is sent
  with as the AMQP `type` property. Agents declare their message types in their class header,
  `KicakInjectorAbstract[Message]` and `KicakExecutorAbstract[Accepted, Result]` (`Result` `None`
  for a terminator), checked when the agent is created and enforced on publishing and receiving.
  An Executor accepts several versions of a message as a union, e.g. `ChunkV1 | ChunkV2`, so
  producers can be upgraded while the flow keeps running.
- A retry queue, `<queue>.retry`, for each Executor: a message whose `execute()` still raises
  `TransientProcessingError` after `max_processing_attempts` calls waits there for
  `retry_delay_seconds` (new argument, default 10) and returns to the back of its queue, without a
  limit on the rounds. The headers `x-kicak-retry-round` and `x-kicak-retrying-since` count them.
- `get_info()`, which returns an `ExecutorInfo` (new export): one snapshot of the status, when it
  was entered and checked (as UTC `datetime`s), whether and when `stop()` was called, and the
  message being processed. New statuses tell an Executor's activity apart: `CREATED`,
  `UPSTREAM_RECOVER`, `WAITING_FOR_MESSAGE`, `PROCESSING`, `DOWNSTREAM_RECOVER`, and
  `SENDING_RESULT`.
- Topology verification from both sides: a publishing Executor declares and binds every queue of
  its new `destination_queues` argument before it publishes results; the input side is declared
  again after every message and every `input_check_seconds` (new argument, default 10) while
  waiting, and the dead-letter route before every rejection. A deleted queue, exchange, or binding
  is restored.
- Every message names its publisher in the AMQP `user_id` property, which RabbitMQ verifies, and
  the agent that created it in the `x-kicak-original-publisher` header.
- Named connections, e.g. `executor queue=classifier host=worker-3 pid=4242`, shown by RabbitMQ's
  management UI.
- The Injector's `blocked_timeout_seconds` argument (default 60, before fixed), for an Injector
  that someone waits for.
- Queue and exchange names are checked when an agent is created: a non-empty `str` of at most 255
  UTF-8 bytes (249 for a queue an Executor consumes from), not starting with `amq.`. An Executor
  publishing back to its own queue is refused.

### Changed

- **Breaking:** the Executor's `dead_letter_exchange` and `dead_letter_queue` arguments are
  removed; the dead-letter exchange and queue of a queue are always `<queue>.dlx` and
  `<queue>.dlq`. A queue declared with another dead-letter exchange fails with
  `406 PRECONDITION_FAILED`.
- **Breaking:** an Executor with a `destination_exchange` requires `destination_queues`.
- **Breaking:** `KicakInjectorAbstract` takes its message type as a type argument, and
  `KicakExecutorAbstract` two, replacing `message_type()`, which is removed. A subclass of either
  class without type arguments, or a message class without its own `kicak_type`, raises
  `TypeError` when the agent is created.
- **Breaking:** an Executor dead-letters a delivery without a `type` property, or with one it does
  not accept, as malformed.
- **Breaking:** `get_status()`, `get_timestamp()`, and `ExecutorStatus.RUNNING` are removed; use
  `get_info()`.
- **Breaking:** `RABBIT_MQ_TLS` is a required configuration key.
- A message whose processing keeps failing transiently is moved to the retry queue instead of
  being requeued with `basic_nack(requeue=True)`, which redelivered it at once and held up the
  messages behind it.
- Results that RabbitMQ returns as unroutable are held, with the input unacknowledged, and
  published again after a backoff, instead of requeueing the input and stopping the Executor.
- A result larger than RabbitMQ's `max_message_size` dead-letters its input, and the Executor
  continues.
- A value returned by `execute()` that is not a message, or a result that cannot be serialized,
  dead-letters the input as malformed, and the Executor continues instead of stopping. A delivery
  that its message class's `__post_init__` refuses, with any exception, is dead-lettered too.
- `to_bytes()` refuses NaN and infinite floats with `ValueError`, and `from_bytes()` refuses `NaN`,
  `Infinity`, `-Infinity`, numbers too large for a float, and bodies nested too deeply to decode.
- When RabbitMQ cancels the consumer, e.g. because the queue was deleted, the Executor reconnects,
  which declares the queue again, instead of stopping.
- A delivery that cannot be decoded is logged with the `message_id` property its sender set, or
  `None`, instead of `unknown`. A message that cannot be serialized or decoded is logged at `ERROR`
  once, by the agent, instead of twice.

### Fixed

- An Executor whose delivery outlasts RabbitMQ's `consumer_timeout` reconnects and executes the
  message again instead of stopping: on RabbitMQ 4.2, which closes the channel, and on 4.3 and
  newer, which cancel the consumer of a quorum queue. It publishes no results and no retry copy
  for a delivery RabbitMQ has taken back.
- A stopping Executor no longer opens a connection only to close it, and a failure to close the
  connection after a graceful stop is logged, while `run()` still returns with the status
  `STOPPED`.

## [0.2.0] - 2026-10-06

### Changed

- The Executor publishes all results of one input in a single RabbitMQ transaction, on a channel
  of its own, and acknowledges the input only after the commit, so RabbitMQ delivers all results
  or none. A lost connection no longer leaves the results published before it; they are no longer
  published twice when the input is executed again.
- Unroutable results are detected when the transaction is committed and still raise
  `UnroutableError`. `NackError` no longer occurs for results; RabbitMQ refusing the commit
  raises `ChannelClosedByBroker` instead, and the Executor stops as before.
- An Executor that loses its connection while acknowledging or rejecting a message reconnects and
  receives the message again, instead of stopping. This includes a terminator whose `execute()`
  outlasts `heartbeat_seconds`, which now executes the redelivered message again like a
  publishing Executor. After an unexpected failure or unroutable results
  the Executor still raises that error and stops.
- An Injector gives up on a connection that a RabbitMQ memory or disk alarm blocks for more than
  60 seconds (`ConnectionBlockedTimeout`, retrying it like other connection errors), so an injection
  can no longer hang during an alarm.

## [0.1.0] - 2026-10-05

First public release.

### Added

- `KicakInjectorAbstract`: short-lived publisher that injects a generated message on its own
  connection, with publisher confirms, mandatory publishing, and up to three attempts; failures
  raise `InjectionError`.
- `KicakExecutorAbstract[M]`: long-running consumer that executes one message at a time, publishes
  its results before acknowledging, dead-letters malformed messages, retries
  `TransientProcessingError`, reconnects after connection loss, and stops gracefully with `stop()`.
  Terminator mode without a destination exchange.
- Executor lifecycle reporting with `get_status()`, `get_timestamp()`, and `ExecutorStatus`.
- `QueueType` to declare classic or quorum input and dead-letter queues.
- `KicakMessage`, a frozen dataclass base with JSON serialization and a required, keyword-only
  `message_id`, also published as the AMQP `message_id` property.
- `KicakConfig`, a `.kicak` file parser that keeps values out of reprs, errors, and logs.

[Unreleased]: https://github.com/przemek-barczak/pykicak/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/przemek-barczak/pykicak/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/przemek-barczak/pykicak/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/przemek-barczak/pykicak/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/przemek-barczak/pykicak/releases/tag/v0.1.0
