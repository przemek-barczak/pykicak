# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/przemek-barczak/pykicak/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/przemek-barczak/pykicak/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/przemek-barczak/pykicak/releases/tag/v0.1.0
