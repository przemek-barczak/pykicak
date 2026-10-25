# RabbitMQ topology

Queue and exchange names are declared outside the Kicak abstract classes, in your application's own
`messaging/` module (e.g. `messaging/queues.py`, `messaging/exchanges.py`, `messaging/bindings.py`),
and passed into agent constructors in code. This keeps a single owner for each queue/exchange name,
so independent agents reference the same definitions instead of inventing their own. See
[`examples/poc/messaging/`](../examples/poc/messaging) for a worked example.

| Constructor arg | Used by | Description |
| --- | --- | --- |
| `exchange_name` | `KicakInjectorAbstract` (required) | Exchange to publish messages to |
| `blocked_timeout_seconds` | `KicakInjectorAbstract` (optional, default 60) | How long an attempt waits on a connection that a RabbitMQ memory or disk alarm blocks before it fails and is retried; a finite number greater than 0 (see [Usage](../README.md#usage)) |
| `source_queue` | `KicakExecutorAbstract` (required) | Queue to consume messages from |
| `source_exchange` | `KicakExecutorAbstract` (required) | Upstream exchange `source_queue` is bound to |
| `destination_exchange` | `KicakExecutorAbstract` (optional) | Exchange that the results returned by `execute()` are published to |
| `destination_queues` | `KicakExecutorAbstract` (required with `destination_exchange`) | Every queue that must receive the results, at least one; the Executor declares and binds each of them before publishing (see [Topology verification](#topology-verification)) |
| `queue_type` | `KicakExecutorAbstract` (optional, default `QueueType.CLASSIC`) | RabbitMQ type of every queue the Executor declares: `QueueType.CLASSIC` or `QueueType.QUORUM` (see [Queue type](#queue-type)) |
| `max_processing_attempts` | `KicakExecutorAbstract` (optional, default 3) | How many times `execute()` is called for one delivery, one call after the other, while it raises `TransientProcessingError`, before the message is moved to the retry queue (see [Retry queue](retry-queue.md)) |
| `max_retry_rounds` | `KicakExecutorAbstract` (required) | How many times a message that keeps raising `TransientProcessingError` may be moved to the retry queue before it is dead-lettered; an int, 0 or more, where 0 dead-letters it after its first `max_processing_attempts` calls (see [Retry queue](retry-queue.md)) |
| `retry_delay_seconds` | `KicakExecutorAbstract` (optional, default 10) | How long a message waits in the retry queue `<queue>.retry` before it returns to the back of the input queue; from 0 to 4294967.295 (about 49.7 days) (see [Retry queue](retry-queue.md)) |
| `heartbeat_seconds` | `KicakExecutorAbstract` (optional, default 600) | AMQP heartbeat timeout of the Executor's connections, an int from 1 to 65535; set it above the longest processing of one message, retries of `execute()` included (see [Long-running `execute()`](running-an-executor.md#long-running-execute)) |
| `input_check_seconds` | `KicakExecutorAbstract` (optional, default 10) | How often the input side is verified while the Executor waits for a message; 0 verifies it on every wakeup, about once a second (see [Topology verification](#topology-verification)) |

Constructor arguments after `config` are keyword-only, for both the Injector and the Executor
(`exchange_name="..."`, `source_queue="..."`, and so on). An Executor declares its own durable input
queue of type `queue_type` and binds it to the durable fanout source exchange. Omit both
`destination_exchange` and `destination_queues` for terminator mode; giving only one of them raises
`ValueError`.

**Names are checked when an agent is created.** Every queue and exchange name must be a non-empty
`str` that fits AMQP's limit of 255 bytes in UTF-8 and does not start with `amq.`. RabbitMQ reserves
that prefix: it refuses to create such a queue or exchange (`403 ACCESS_REFUSED`), and its own
`amq.*` exchanges, such as `amq.fanout`, are shared by the whole virtual host. A queue an Executor
consumes from, its `source_queue` or one of its `destination_queues`, may have at most 249 bytes,
leaving room for the `.retry` suffix of its retry queue (see below). Any other name makes the
constructor raise `TypeError` or `ValueError`, instead of failing when the agent first connects.

**An Executor never publishes to itself.** A `destination_exchange` equal to its
`source_exchange`, or `destination_queues` that include its `source_queue`, makes the constructor
raise `ValueError`: the results would come back to its own queue and be executed again, possibly
forever. Results always go downstream. To send them back upstream, let a downstream Executor
publish them to the upstream Executor's `source_exchange`, with that queue in its
`destination_queues`.

**Dead-letter names follow a convention.** The input queue's `x-dead-letter-exchange` argument is
`<queue>.dlx`, a durable fanout exchange, and the durable dead-letter queue `<queue>.dlq` is bound
to it, so dead-lettered messages are kept for inspection or replay (see
[Dead-letter queue](dead-letter-queue.md)). For the queue `classifier`, these are
`classifier.dlx` and `classifier.dlq`. Only the queue's own Executor declares them. Because every
input queue's arguments follow from its name and `queue_type` alone, any Executor can declare a
queue exactly as the queue's own Executor does, which is what lets an upstream Executor restore a
deleted downstream queue. A dead-letter queue has no dead-letter exchange of its own, so it can be
neither an Executor's source queue nor one of its destination queues. The constructor refuses that,
a source or destination exchange that is the source queue's dead-letter exchange, and a
destination exchange that is a destination queue's.

**The retry queue follows a convention too.** Each input queue has a durable retry queue
`<queue>.retry`, for example `classifier.retry`, of the same `queue_type`, which returns its
messages to the input queue (see [Retry queue](retry-queue.md)). Only the queue's own Executor
declares it, and the constructor refuses a source or destination queue that is the retry queue of
one of them.

**Who declares what.** Each Executor verifies its topology from both sides:

- its **input side**: its own queue, that queue's binding to the source exchange, its
  dead-letter route, and its retry queue;
- its **output side**: the destination exchange, and every queue in `destination_queues`, each
  bound to it.

A queue between two Executors is therefore declared and bound by both: by its own Executor, which
consumes from it, and by the Executor upstream of it, which publishes to its exchange. An Injector
declares only its exchange and knows nothing of the queues behind it, so **exactly one queue may
be bound to an Injector's exchange**: the input queue of the first Executor. With one queue, a
missing binding always makes the injection fail with `UnroutableError` and raise `InjectionError`;
with several, a message could reach some of them and be lost for the others without any error (see
[Delivery guarantees](delivery-guarantees.md)). To fan out what an Injector sends, let the first
Executor publish it to an exchange of its own.

The library assumes this deployment model:

- **Each queue belongs to one Executor type, which can run as several instances.** To scale an
  Executor, run more processes of it with the same configuration; they consume from the same queue
  and RabbitMQ shares its messages between them (see [Scaling an
  Executor](running-an-executor.md#scaling-an-executor)).
- **One queue type for the whole system.** Every Executor declares its downstream queues with its
  own `queue_type`, so all Executors of a system must use the same type; a mismatch fails with
  `406 PRECONDITION_FAILED`.
- **Permanent topology.** Once declared, exchanges and queues stay in the broker, and agents never
  delete them. Executors verify their topology all the time (see [Topology
  verification](#topology-verification)), so anything removed by mistake is restored. The messages
  of a deleted queue are lost with it: RabbitMQ discards them. To redesign the topology, stop
  injecting, let the Executors drain every queue, stop every instance with
  [`stop()`](running-an-executor.md#graceful-stop), remove the whole structure, then start the new
  Executors, which declare the new structure.
- **The first Executor is started before its Injector.** Until the first Executor has declared and
  bound its queue, an injection fails with `UnroutableError` and raises `InjectionError`. Downstream
  Executors can start in any order: the Executor upstream of a queue declares and binds it before
  publishing, so the queue collects messages before its own Executor starts.
- **Adding a consumer** of an Executor's results: add its queue to the `destination_queues` of every
  Executor that publishes to that exchange (restarting their instances one by one is enough), then
  start the new Executor; its queue already holds the results published since.
- **Removing a consumer:** first remove its queue from those `destination_queues`. Otherwise the
  publishing Executors keep declaring and binding it, and it fills up with nobody reading it.

RabbitMQ queue arguments cannot change after a queue is created. If the source queue or a
destination queue already exists with another `x-dead-letter-exchange` argument (for example one
that does not follow the `<queue>.dlx` convention), or any queue exists with another queue type, its
declaration fails with `406 PRECONDITION_FAILED`, which is not retried: connecting raises, and a
running Executor stops. Queues declared by an earlier pykicak version with other dead-letter names
must therefore be drained and deleted before the Executors that declare them are upgraded.

## Topology verification

An Executor declares its topology again, idempotently, whenever a deletion would otherwise go
unnoticed. AMQP cannot check whether a binding exists without creating it, so the Executor simply
declares and binds again; RabbitMQ treats a declaration of something that exists as a no-op, and
restores whatever was deleted. That is logged only at `DEBUG`.

- **On every connection**, including each reconnection: the input side and the output side.
- **Before waiting for a message**: the input side, after each message. A deleted source exchange
  takes the input queue's binding with it, and RabbitMQ tells nobody, so this check is what binds
  the queue again.
- **While waiting**: the input side, every `input_check_seconds` (default 10). With `0`, on every
  wakeup, which happens about once a second. A deleted input queue is noticed at once anyway:
  RabbitMQ cancels the consumer, and the Executor logs an `ERROR` and reconnects.
- **Right before publishing the results** of each message: the output side, on the results
  channel. So a downstream queue, binding, or destination exchange deleted since is restored
  before the results are published, and none of the queues misses them.
- **Right before dead-lettering or requeueing a message**: the dead-letter route. The copy of a
  dead-lettered message is published to the dead-letter exchange, and a quorum queue can
  dead-letter a requeued message at its delivery limit; RabbitMQ drops a message it dead-letters
  into an exchange that does not exist.
- **Right before moving a message to the retry queue**: the retry queue.

The input side is the input queue, its binding to the source exchange, the dead-letter route, and
the retry queue.

A connection or channel lost during a verification makes the Executor reconnect, which verifies
everything again. RabbitMQ refusing a declaration, for example `406 PRECONDITION_FAILED` for a queue
with other arguments or `403 ACCESS_REFUSED` for missing permissions, stops the Executor.

Verification costs broker round trips: about 7 for the input side, and 1 + 2N for the output side
with N destination queues. That is paid after every message, before every publish, and about every
`input_check_seconds` while idle.

A few milliseconds remain between the verification of the output side and the commit of the results.
If every binding of the destination exchange disappears in that window, RabbitMQ returns the results
as unroutable, and the Executor holds them and tries again (see [Unroutable results are
held](outcomes.md#unroutable-results-are-held)). If only some disappear, the results reach the other
queues and are lost for the missing ones; this window is the only place left where that can happen.

## Queue type

`queue_type` selects the RabbitMQ queue type of every queue an Executor declares: its input queue,
its dead-letter queue, its retry queue, and its destination queues, so all Executors of a system use
the same type.
Pass a member of the exported `QueueType` enum:

- `QueueType.CLASSIC` (the default) is a queue stored on one broker node. Its retry queue returns
  messages at most once, so in a rare case a message can be lost (see [When a message can still be
  lost](delivery-guarantees.md#when-a-message-can-still-be-lost)).
- `QueueType.QUORUM` is a queue replicated across the nodes of a RabbitMQ cluster, which keeps its
  messages when a node fails, and whose retry queue returns messages at least once. It also works on
  a single node. Use it where no message may be lost.

For the `ClassifyingExecutor` of the [Usage](../README.md#usage) example in the README:

```python
from pykicak import QueueType

executor = ClassifyingExecutor(
    config,
    source_queue="classifier",
    source_exchange="items.inbox",
    destination_exchange="items.classified",
    destination_queues=["db-writer", "quality-checker"],
    max_retry_rounds=0,
    queue_type=QueueType.QUORUM,
)
```

`QueueType` is a string enum, so the values `"classic"` and `"quorum"` work too, for example when
the type comes from your own configuration; any other value raises `ValueError`.

The type is always declared explicitly (`x-queue-type`), so a default queue type configured for the
vhost never applies. Queues declared without a type, for example by another tool, are classic
queues and keep working with the default `QueueType.CLASSIC`. To change the type of existing
queues, follow the topology redesign steps above: drain them, stop every instance of every
Executor, delete the queues, then start the Executors with the new `queue_type`.

Quorum queues count the deliveries of each message. On RabbitMQ 4.0 and newer they apply a default
delivery limit of 20: a message delivered 20 times without success is dead-lettered with the
`x-death` reason `delivery_limit` instead of being delivered a 21st time. What counts depends on the
version of RabbitMQ:

- RabbitMQ 4.2 counts every return of a message to the queue: a requeue by the Executor
  (`basic_nack` with `requeue=True`), and a delivery left unsettled by a lost connection or a hard
  stop.
- RabbitMQ 4.3 and newer count only a delivery that fails, such as one left unsettled by a lost
  connection. A requeue by the Executor does not count (RabbitMQ counts it in the `x-acquired-count`
  header instead), so a message the Executor keeps requeueing, for example because RabbitMQ keeps
  refusing its copy for the retry queue, is never dead-lettered by the limit.

A message that comes back from the retry queue arrives as a new message, so its count starts again:
the delivery limit never ends the retries of `TransientProcessingError`, which only
`max_retry_rounds` bounds (see [Retry queue](retry-queue.md)). A classic queue has no such limit and
requeues a message indefinitely. The limit can be changed with a RabbitMQ policy (`delivery-limit`).
