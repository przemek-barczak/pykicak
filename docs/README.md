# pykicak documentation

Start with the [README](../README.md): what pykicak does, the [requirement of idempotent
processing](../README.md#requirement-idempotent-processing) every agent must meet, installation, a
quick start, and [what `execute()` raises](../README.md#what-execute-raises-is-your-decision). These
pages describe each part in detail:

- [Configuration](configuration.md): the `.kicak` file, environment variables, every key, TLS, and
  client certificates (mutual TLS).
- [RabbitMQ topology](topology.md): the constructor arguments, naming conventions, who declares
  what, topology verification, and queue types.
- [Message types](messages.md): message classes, `message_id`, type names, and introducing a new
  version of a message.
- [Running an Injector](injector.md): how an injection is retried, `InjectionError`, `publish()`,
  and repeated injections.
- [Every outcome of a delivery](outcomes.md): what happens to a delivery in every case, held
  unroutable results, and the results' transaction.
- [Retry queue](retry-queue.md): where a message waits between retries, `max_retry_rounds`, and its
  headers.
- [Dead-letter queue](dead-letter-queue.md): where failed messages are kept, the headers that say
  why, and sending them back.
- [Running and stopping an Executor](running-an-executor.md): graceful stop, long-running
  `execute()`, and scaling to several instances.
- [Monitoring](monitoring.md): `get_info()`, logging, statistics in the logs, and connection names.
- [Delivery guarantees](delivery-guarantees.md): what publishing guarantees, who published a message
  and when, and connection retries.
- [Proof of concept](proof-of-concept.md): the runnable example in `examples/poc/`, including a
  message upgrade in progress.
- [Development](development.md): running the tests and checks, and CI.
