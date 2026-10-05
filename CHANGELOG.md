# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0]: https://github.com/przemek-barczak/pykicak/releases/tag/v0.1.0
