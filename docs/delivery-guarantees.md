# Delivery guarantees

Every message is published as mandatory and persistent (`delivery_mode=2`), with the content type
`application/json`, its `message_id` as the AMQP `message_id` property, its class's `kicak_type` as
the AMQP `type` property (see [Type names: the contract between
agents](messages.md#type-names-the-contract-between-agents)), the names of its publisher and its
creator (see [Who published a message](#who-published-a-message)), and the time it was published
(see [When a message was published](#when-a-message-was-published)). The Injector publishes in
publisher-confirm mode: a publish returns only after RabbitMQ has taken responsibility for the
message, for persistent messages in durable queues after writing it to disk. The Executor publishes
its results in a transaction (see [Results are published in a
transaction](outcomes.md#results-are-published-in-a-transaction)): the commit returns only after
RabbitMQ has taken responsibility for all of them. RabbitMQ refusing a message raises one of these
errors, which are not retried:

- `pika.exceptions.UnroutableError`: no queue is bound to the exchange, so RabbitMQ would have
  discarded the message. For an Injector this means the first Executor's topology is not installed.
- `pika.exceptions.NackError` (Injector): RabbitMQ could not take responsibility for the message.
  For an Executor's results, RabbitMQ refuses the commit instead, which closes the results channel.

An Injector raises `InjectionError` to its caller, with one of these errors as its `__cause__`. An
Executor keeps the input message: after unroutable results it holds them and tries again (see
[Unroutable results are held](outcomes.md#unroutable-results-are-held)); after a refused commit,
closing the connection requeues the input and the Executor stops (see [Every outcome of a
delivery](outcomes.md)). So no message is lost between two stages of a pipeline, apart from the
rare cases in [When a message can still be lost](#when-a-message-can-still-be-lost). A result larger
than RabbitMQ's `max_message_size` is the exception: its input is dead-lettered, and the Executor
continues.

A mandatory message counts as routed if at least one queue is bound to the exchange, so RabbitMQ
does not report a fanout exchange that is missing only some of its queues. An Executor therefore
binds every one of its `destination_queues` right before it publishes, which leaves only the few
milliseconds before the commit (see [Topology verification](topology.md#topology-verification)); and
only one queue may be bound to an Injector's exchange, so a missing binding there always fails the
injection.

## When a message can still be lost

Wherever a failure leaves pykicak a choice, it processes a message again rather than risk losing it.
A few cases remain in which a message can be lost, all of them inside RabbitMQ, where no client can
confirm the outcome:

- **A classic retry queue whose input queue refuses the message.** A message waiting in a classic
  retry queue is returned to the input queue by RabbitMQ itself, at most once and without any
  confirmation (see "The way back" in [Retry queue](retry-queue.md)). If the input queue does not
  take it, because it is full and set to refuse new messages (a length limit with
  `overflow: reject-publish`, for example from a policy), or because it does not exist at that
  moment, RabbitMQ drops the message and tells nobody, while the Executor has counted it as moved to
  the retry queue. A quorum retry queue returns it at least once and keeps it until the input queue
  takes it. **Use `QueueType.QUORUM` where no message may be lost**, and with classic queues, set no
  limit that makes an input queue refuse messages.
- **A message a classic queue dead-letters itself.** RabbitMQ itself dead-letters a message from a
  classic input queue when it expires (a message TTL from a policy, or an expiration its publisher
  set) or a length limit drops it, at most once and without any confirmation. If no queue takes it
  at that moment, for example because `<queue>.dlq` or its binding was deleted, or `<queue>.dlq` is
  full and refuses new messages, RabbitMQ drops it. A quorum queue dead-letters at least once and
  keeps the message until `<queue>.dlq` takes it (see [Queue type](topology.md#queue-type)).
- **Results that miss a downstream queue.** If some, but not all, of the bindings of an Executor's
  destination exchange are deleted in the few milliseconds between its output-side verification and
  the commit, the queues that lost their binding miss the results, and RabbitMQ reports nothing as
  long as one queue is still bound (see [Topology verification](topology.md#topology-verification)).
- **A dead-letter copy taken by another queue only.** The same applies to `<queue>.dlx`: if
  `<queue>.dlq` or its binding is deleted in the milliseconds before a dead-letter copy is
  published, while another queue you bound to `<queue>.dlx` still takes the copy, RabbitMQ confirms
  it and the Executor acknowledges the original, but `<queue>.dlq` keeps nothing (see
  [Dead-letter queue](dead-letter-queue.md)).
- **A broker failure during a commit.** RabbitMQ does not guarantee that a transaction is atomic if
  the broker itself fails while committing it, so some of an input's results can be lost (see
  [Results are published in a transaction](outcomes.md#results-are-published-in-a-transaction)).

## Who published a message

Every message a pykicak agent publishes names two RabbitMQ users:

| Where | Who | Can it be trusted? |
| --- | --- | --- |
| `user_id` property | the agent that published this message to RabbitMQ | yes: RabbitMQ refuses, with `406 PRECONDITION_FAILED`, a `user_id` other than the user the publishing connection authenticated as |
| `x-kicak-original-publisher` header | the agent that created the message | only as far as the clients that may publish to your exchanges: RabbitMQ does not check headers |

An agent's user is `RABBIT_MQ_USERNAME` from its configuration. The two names differ only on a
copy that an Executor moves to its retry queue or its dead-letter queue: the copy keeps the header
of the message it copies, while its `user_id` becomes the Executor's. Every other message is created
by the agent that publishes it, so it names that agent twice: an Injector's message names the
Injector, and an Executor's result names the Executor, whoever created its input. For example:

```text
Injector "loader" injects message M        user_id=loader  x-kicak-original-publisher=loader
Executor "parser" moves M to its retry     user_id=parser  x-kicak-original-publisher=loader
Executor "parser" dead-letters M           user_id=parser  x-kicak-original-publisher=loader
Executor "parser" publishes result R of M  user_id=parser  x-kicak-original-publisher=parser
```

A message that another client published without the header was created by that client: a copy of
it names the client's `user_id` in the header, if the client set one, and has no such header
otherwise.

## When a message was published

Every message a pykicak agent publishes, an Injector's message and an Executor's result alike,
carries the time it was published in its `x-kicak-published-at` header, in ISO 8601 and UTC, e.g.
`2026-10-01T17:05:42.123456+00:00`, taken from the publishing host's clock. An Injector stamps each
attempt anew, so a retried injection carries the time of the attempt that published it, and an
Executor stamps its results when it publishes them, not when their input arrived. A copy moved to
the retry queue is published anew too, so it gets the time of the move, while a dead-letter copy
keeps the time of the message it copies (see [Dead-letter queue](dead-letter-queue.md)).

The Executor that receives the message compares that time with its own clock to tell how long the
message waited in its queue, `queued_seconds` in its log (see [Statistics in the
logs](monitoring.md#statistics-in-the-logs)). The header is not verified by RabbitMQ, like
`x-kicak-original-publisher`, and a message that another client published without it has no
`queued_seconds`.

## Retries

Injectors retry transient RabbitMQ connection and publish failures up to three attempts, with 1- and
2-second delays between attempts; each retry publishes the message again on a new connection, and
after the third failed attempt the error is raised. Executors reconnect indefinitely after transient
failures, using exponential full jitter with a 0.5-second initial delay and a 30-second maximum, but
never publish results again on a new connection: after a lost connection the input is redelivered
and executed again instead. On the same connection they publish again only results RabbitMQ returned
as unroutable, while holding the input (see
[Unroutable results are held](outcomes.md#unroutable-results-are-held)). Authentication and
access-denied failures, and a broker certificate that fails verification (see
[TLS](configuration.md#tls)), are not retried. Application processing is retried only for
`TransientProcessingError`, first in place and then through the retry queue (see
[Retry queue](retry-queue.md)). Message acknowledgements are not retried: an acknowledgement is
valid only on the connection that delivered the message, so when the connection is lost while
settling a message, RabbitMQ requeues it and the Executor reconnects and receives it again.

An Executor cannot tell a broker that is down from a connection setting that will never work, so it
also retries indefinitely when the host name does not resolve, nothing listens on the port, or the
port does not speak what `RABBIT_MQ_TLS` selects (for example the TLS port 5671 with
`RABBIT_MQ_TLS=false`, the plain port 5672 with `true`, or the management port), or the broker
refuses its [client certificate](configuration.md#client-certificates-mutual-tls). Its status is
`"UPSTREAM_RECOVER"` meanwhile (`"CREATED"` during the very first attempt), and it logs a `WARNING`
for every failed attempt; alert on a status that stays `"UPSTREAM_RECOVER"`, on those warnings, or
on the queue having no consumer.

A server that accepts the connection but never completes the AMQP handshake, such as a broker that
is still starting or overloaded, or a proxy in front of one that is down, is a connection error too:
after pika's handshake timeout (15 seconds) the attempt fails with pika's
`AMQPConnectorStackTimeout` and is retried. An Executor reconnects, and an Injector makes its three
attempts.

An Injector's connection fails with `ConnectionBlockedTimeout` after RabbitMQ has blocked it for
`blocked_timeout_seconds` (60 by default) because of a memory or disk alarm, and the attempt is
retried like other connection errors.
An Executor's connection stays blocked until the alarm clears, so it does not execute its message
again in the meantime.

Publishing is at-least-once: if the connection is lost before RabbitMQ's confirmation or the reply
to a commit arrives, RabbitMQ may already have accepted the messages. The Injector then publishes
its message again, and the Executor executes the redelivered input again. pykicak does not provide
exactly-once delivery, which is why it requires idempotent agents (see [Requirement: idempotent
processing](../README.md#requirement-idempotent-processing)).
