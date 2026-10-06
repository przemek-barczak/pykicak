# pykicak

`pykicak` makes it easy to build RabbitMQ-connected agent nodes with idempotent message
processing. It provides abstract base classes — Injector and Executor — that handle RabbitMQ
connection setup and queue/exchange declaration, so subclasses only need to implement their
specific behavior. Delivery is at-least-once, so the processing your agents implement must be
idempotent; see [Requirement: idempotent processing](#requirement-idempotent-processing).

- **Injector** — short-lived node that creates and publishes an initial message to start a
  processing flow (e.g. a document ingestion trigger).
- **Executor** — long-running node that consumes messages from a queue, executes them, and
  optionally publishes the results to an exchange. Messages that can never be processed are
  dead-lettered instead of blocking the queue. Can operate in "terminator" mode where it
  consumes but does not publish (e.g. a database writer or quality checker).

## Requirement: idempotent processing

**pykicak is designed for idempotent agents only.** It delivers every message *at least once*, never
*exactly once*: it never loses a message, and the price is that a message can be processed, and its
results published, more than once. Every agent built on pykicak must therefore be idempotent:
processing the same message twice, or twice at the same time, must leave the system in the same
state as processing it once. Using pykicak for processing that is not idempotent can corrupt data or
repeat external actions.

A message is processed more than once when:

- an Executor is killed or stopped without `stop()` (crash, `SystemExit`, `KeyboardInterrupt`, a
  supervisor's forced kill) while it processes a message, so RabbitMQ redelivers the message, which
  is executed again, and its results are published again if they had already been published;
  a graceful [`stop()`](#graceful-stop) finishes the message first and avoids this;
- the connection is lost while an Executor commits its results, after RabbitMQ committed them but
  before the Executor learned of it, so the input is executed again and all its results are
  published again;
- `execute()` runs longer than `heartbeat_seconds`, so the input is redelivered while it is still
  being executed, and with several instances it runs concurrently on another instance;
- `execute()` raises `TransientProcessingError`, so it is called again, and requeued after
  `max_processing_attempts` failed calls, so it is executed again later;
- RabbitMQ returns an Executor's results as unroutable or refuses their commit, so the input is
  requeued and executed again;
- the acknowledgement fails, for example because the connection was lost right after the results
  were published, so RabbitMQ redelivers the input, which is executed again and its results
  published again;
- an Injector loses its connection before RabbitMQ confirms a message, so it publishes the message
  again;
- a RabbitMQ memory or disk alarm blocks an Injector's connection for more than 60 seconds, so the
  Injector gives up on that attempt and publishes the message again. RabbitMQ may still enqueue the
  earlier publish once the alarm clears, even when the injection ends with `InjectionError`.

What this requires from an application:

- **Idempotent side effects in `execute()`.** Enforce them in the database (unique constraints,
  upserts keyed by a message ID), not by checking first and writing afterwards, which fails when two
  instances run the same message concurrently. This includes partial work done by a call that then
  raises `TransientProcessingError`.
- **Stable message IDs.** Every message has a required `message_id` (see
  [Message types](#message-types)). Give each message a unique ID when it is first created, and
  derive the IDs of results from the input's ID (for example with `uuid.uuid5`), not from
  `uuid4()`, so that executing an input again produces results with the same IDs, which downstream
  agents can deduplicate.
- **Idempotent downstream agents.** Every Executor in a pipeline receives duplicates, so the
  requirement applies to all of them.
- **No at-most-once actions without a deduplication key.** Actions that must not be repeated, such as
  sending an email, charging a payment, or calling a non-idempotent API, need an idempotency key
  derived from the message ID, or must not be performed by a pykicak agent.

## Installation

```bash
pip install pykicak
```

Requires Python 3.12+. Depends on [`pika`](https://pypi.org/project/pika/) for the RabbitMQ
connection.

## Configuration: the `.kicak` file

Applications load RabbitMQ **connection** details with `KicakConfig.from_file(path)` and pass
the resulting config to an agent. The library does not select a config path or read environment
variables. A `.kicak` file is a simple `KEY=VALUE` text file, read as UTF-8 on every platform
(a leading byte order mark is allowed); blank lines and lines starting with `#` are ignored.
See [`.kicak.example`](https://github.com/przemek-barczak/pykicak/blob/main/.kicak.example) for a template.

| Key | Description |
| --- | --- |
| `RABBIT_MQ_USERNAME` | RabbitMQ username |
| `RABBIT_MQ_PASSWORD` | RabbitMQ password |
| `RABBIT_MQ_HOST` | RabbitMQ host |
| `RABBIT_MQ_PORT` | RabbitMQ port |
| `RABBIT_MQ_VIRTUAL_HOST` | RabbitMQ virtual host |

Never commit a real `.kicak` file; `.kicak` files are excluded by `.gitignore`.

Missing or unreadable files, invalid UTF-8, malformed lines (including a line with an empty key),
duplicate keys, and missing required keys raise `KicakConfigError`. The port is converted to an
integer when `rabbitmq_port` is accessed, and a value that is not an integer from 1 to 65535 also
raises `KicakConfigError`.

`KicakConfig` keeps values out of its `repr()` and its errors, so they do not reach logs or crash
reporters: the repr lists only the keys (`KicakConfig(keys=['RABBIT_MQ_HOST', ...])`), and a
malformed line or invalid value is reported by its line number or key, never its content.

The `.kicak` file does **not** hold queue, exchange, or binding names — that is application
topology, not connection configuration. See [RabbitMQ topology](#rabbitmq-topology) below.

## RabbitMQ topology

Queue and exchange names are declared outside the Kicak abstract classes, in your application's
own `messaging/` module (e.g. `messaging/queues.py`, `messaging/exchanges.py`,
`messaging/bindings.py`), and passed into agent constructors in code. This keeps a single
owner for each queue/exchange name, so independent agents reference the same definitions
instead of inventing their own. See [`examples/poc/messaging/`](https://github.com/przemek-barczak/pykicak/blob/main/examples/poc/messaging)
for a worked example.

| Constructor arg | Used by | Description |
| --- | --- | --- |
| `exchange_name` | `KicakInjectorAbstract` (required) | Exchange to publish messages to |
| `source_queue` | `KicakExecutorAbstract` (required) | Queue to consume messages from |
| `source_exchange` | `KicakExecutorAbstract` (required) | Upstream exchange `source_queue` is bound to |
| `dead_letter_exchange` | `KicakExecutorAbstract` (required) | Exchange that rejected deliveries from `source_queue` are routed to |
| `dead_letter_queue` | `KicakExecutorAbstract` (required) | Queue bound to `dead_letter_exchange` that retains rejected deliveries |
| `destination_exchange` | `KicakExecutorAbstract` (optional) | Exchange that the results returned by `execute()` are published to |
| `queue_type` | `KicakExecutorAbstract` (optional, default `QueueType.CLASSIC`) | RabbitMQ type of both `source_queue` and `dead_letter_queue`: `QueueType.CLASSIC` or `QueueType.QUORUM` (see [Queue type](#queue-type)) |
| `max_processing_attempts` | `KicakExecutorAbstract` (optional, default 3) | How many times `execute()` is called for one delivery while it raises `TransientProcessingError`, before the delivery is requeued (see [Error handling and dead-lettering](#error-handling-and-dead-lettering)) |
| `heartbeat_seconds` | `KicakExecutorAbstract` (optional, default 600) | AMQP heartbeat timeout of the Executor's connections, from 1 to 65535; set it above the longest `execute()` (see [Long-running `execute()`](#long-running-execute)) |

Constructor arguments after `config` are keyword-only, for both the Injector and the Executor
(`exchange_name="..."`, `source_queue="..."`, and so on). An Executor declares its own durable input
queue of type `queue_type` with the `x-dead-letter-exchange` argument set to `dead_letter_exchange`,
and binds that queue to the durable fanout source exchange. It also declares the durable fanout
dead-letter exchange and the durable dead-letter queue, of the same type, bound to it, so rejected
messages are kept for inspection or replay. It may also
declare a durable fanout `destination_exchange`. Executors never create or bind downstream queues.
Each downstream consumer owns its queue and binding. Omit `destination_exchange` for terminator
mode.

The dead-letter exchange must differ from the source and destination exchanges, and the
dead-letter queue must differ from the source queue.

The library assumes this deployment model:

- **Each queue belongs to one Executor type, which can run as several instances.** To scale an
  Executor, run more processes of it with the same configuration; they consume from the same queue
  and RabbitMQ shares its messages between them (see [Scaling an Executor](#scaling-an-executor)).
- **Permanent topology.** Once declared, exchanges and queues stay in the broker, and agents never
  delete them. Agents declare their topology idempotently each time they connect, including every
  reconnection, so an agent still running recreates anything removed in the meantime. To redesign
  the topology, stop injecting, let the Executors drain every queue, stop every instance with
  [`stop()`](#graceful-stop), remove the whole structure, then start the new Executors, which
  declare the new structure.
- **Topology installed before publishing.** Start each Executor at least once before anything
  publishes to its source exchange. Until its queue is declared and bound, publishing to that
  exchange fails with `UnroutableError` (see [Delivery guarantees](#delivery-guarantees)).

RabbitMQ queue arguments cannot change after a queue is created. If `source_queue` already exists
without the same `x-dead-letter-exchange` argument, or either queue exists with another queue type,
connecting fails with `406 PRECONDITION_FAILED` and is not retried. Delete the old queue (after
draining it) before deploying an Executor that declares it differently.

### Queue type

`queue_type` selects the RabbitMQ queue type of the Executor's input queue and its dead-letter
queue; both always have the same type. Pass a member of the exported `QueueType` enum:

- `QueueType.CLASSIC` (the default) is a queue stored on one broker node.
- `QueueType.QUORUM` is a queue replicated across the nodes of a RabbitMQ cluster, which keeps its
  messages when a node fails. It also works on a single node.

```python
from pykicak import QueueType

executor = ClassifyingExecutor(
    config,
    source_queue="classifier",
    source_exchange="items.inbox",
    dead_letter_exchange="classifier.dlx",
    dead_letter_queue="classifier.dlq",
    destination_exchange="items.classified",
    queue_type=QueueType.QUORUM,
)
```

`QueueType` is a string enum, so the values `"classic"` and `"quorum"` work too, for example when
the type comes from your own configuration; any other value raises `ValueError`.

The type is always declared explicitly (`x-queue-type`), so a default queue type configured for the
vhost never applies. Queues declared without a type, for example by another tool, are classic
queues and keep working with the default `QueueType.CLASSIC`. To change the type of an existing
queue, follow the topology redesign steps above: drain it, stop every instance, delete the queue,
then start the Executors with the new `queue_type`.

Quorum queues count the deliveries of each message. On RabbitMQ 4.0 and newer they apply a default
delivery limit of 20: a message delivered 21 times is dead-lettered with the `x-death` reason
`delivery_limit` instead of being delivered again. Every requeue counts, whether after
`TransientProcessingError` exhausted `max_processing_attempts`, or after a lost connection or a hard
stop. A classic queue has no such limit and requeues a message indefinitely. The limit can be
changed with a RabbitMQ policy (`delivery-limit`).

## Message types

Messages sent and received by agents are declared as `KicakMessage` subclasses, independent of
any agent class, so the same type can be imported by both the publishing side (injector/executor)
and the consuming side (executor):

```python
import dataclasses
from pykicak import KicakMessage

@dataclasses.dataclass(frozen=True, slots=True)
class CrawledItem(KicakMessage):
    url: str
    content: str

item = CrawledItem(url="https://example.com", content="...", message_id="5f0c7b1e-...")
```

Every message has a `message_id`, a field that `KicakMessage` declares for all message types, so
your classes do not declare it. Idempotent processing depends on it (see
[Requirement: idempotent processing](#requirement-idempotent-processing)):

- It is required and keyword-only, with no default. A generated default would give the results of
  a re-executed input new IDs, so each message's creator chooses it: a new ID (for example
  `str(uuid.uuid4())`) where a message is first created, and an ID derived from the input's ID for
  the results of an Executor.
- It must be a non-empty string of at most 255 bytes in UTF-8; otherwise creating the message raises
  `TypeError` or `ValueError`, and a received message is dead-lettered as malformed.
- It is sent in the message body and as the AMQP `message_id` property, and the library includes
  it in its log records.

A message class that defines `__post_init__` must call `KicakMessage.__post_init__(self)`, which
validates the ID. Zero-argument `super()` does not work in a `slots=True` dataclass on Python 3.12.

Fields must be JSON-serializable (store timestamps as ISO-8601 strings, not `datetime` objects).
Decoding checks that the body is a JSON object with exactly the class's fields; it does not convert
or check the types of their values, so a nested dataclass, for example, is decoded as a `dict`.
Validate values in `execute()` and raise `MalformedMessageError` for invalid ones.

## Usage

The examples use the `CrawledItem` message defined above and literal topology names for brevity.
In an application, define these names centrally in its `messaging/` module as described in
[RabbitMQ topology](#rabbitmq-topology).

Executor (terminator mode — no publishing):

```python
from pykicak import (
    KicakConfig,
    KicakExecutorAbstract,
    MalformedMessageError,
    TransientProcessingError,
)

class SaveToDbExecutor(KicakExecutorAbstract[CrawledItem]):
    def message_type(self) -> type[CrawledItem]:
        return CrawledItem

    def execute(self, message: CrawledItem) -> None:
        if not message.url:
            raise MalformedMessageError("url is empty")  # dead-lettered
        try:
            ...  # upsert the item into a database, keyed by message.message_id
        except ConnectionError as error:
            raise TransientProcessingError("database unavailable") from error  # retried
        return None  # a terminator publishes nothing

config = KicakConfig.from_file(".kicak")
executor = SaveToDbExecutor(
    config,
    source_queue="db-writer",
    source_exchange="items.inbox",
    dead_letter_exchange="db-writer.dlx",
    dead_letter_queue="db-writer.dlq",
)
executor.run()
```

Injector (publishes an initial message):

```python
import uuid

from pykicak import KicakConfig, KicakInjectorAbstract

class StartProcessingInjector(KicakInjectorAbstract):
    def generate(self) -> CrawledItem:
        # A new ID where the message is first created; it never changes afterwards
        return CrawledItem(message_id=str(uuid.uuid4()), url="https://example.com", content="...")

config = KicakConfig.from_file(".kicak")
injector = StartProcessingInjector(config, exchange_name="items.inbox")
injector.run()  # publishes once, then returns
```

Each injection is short-lived. `run()` calls `generate()` and, if it returns a message, opens a
connection, publishes the message, waits for RabbitMQ's confirmation, and closes the connection. If
`generate()` returns `None`, no connection is opened. Connecting and publishing are attempted up to
three times: immediately, after 1 second, and after 2 more seconds, each time on a new connection
and with the same message. An attempt waits at most 60 seconds on a connection that a RabbitMQ
memory or disk alarm blocks; it then fails with `pika.exceptions.ConnectionBlockedTimeout` and is
retried, so an Injector started by a scheduler never hangs during an alarm.

`run()` returns normally only if the message was injected. Otherwise it closes the connection and
raises `InjectionError`. Its `__cause__` is the error that ended the last attempt, for example
`pika.exceptions.UnroutableError` when no queue is bound to the exchange yet, or a pika connection
error when RabbitMQ is unreachable. Its `message` attribute is the message that was not injected,
and its text names the message type and `message_id`, never the payload. Catch it to learn that the
message was not injected:

```python
from pykicak import InjectionError

try:
    injector.run()
except InjectionError as error:  # nothing was injected; the connection is already closed
    ...  # e.g. return an error to the API caller, or exit with a non-zero status
```

`publish()`, used with `connect()` or a `with` block, raises `InjectionError` in the same way. It
raises `RuntimeError` without a connection, including after `close()` or the end of the `with`
block.
Errors from your own `generate()`, or from serializing its message, are not wrapped and propagate
unchanged, because they are application bugs rather than delivery failures.

`injector.run(interval_seconds=60)` repeats the injection every 60 seconds until the process ends;
there is no `stop()`. No connection is kept open between injections, so the interval is not limited
by RabbitMQ's heartbeat. A failed injection ends `run()` with its error, as above, so no message is
skipped silently.

Executor (with publishing to an exchange):

```python
import dataclasses
import uuid

from pykicak import KicakConfig, KicakExecutorAbstract, KicakMessage

@dataclasses.dataclass(frozen=True, slots=True)
class ClassifiedItem(KicakMessage):
    url: str
    category: str

class ClassifyingExecutor(KicakExecutorAbstract[CrawledItem]):
    def message_type(self) -> type[CrawledItem]:
        return CrawledItem

    def execute(self, message: CrawledItem) -> ClassifiedItem:
        # The result's ID is derived from the input's, so executing the input again
        # publishes a result with the same ID, which downstream agents can deduplicate
        result_id = uuid.uuid5(uuid.NAMESPACE_URL, f"classified:{message.message_id}")
        return ClassifiedItem(message_id=str(result_id), url=message.url, category="news")

config = KicakConfig.from_file(".kicak")
executor = ClassifyingExecutor(
    config,
    source_queue="classifier",
    source_exchange="items.inbox",
    dead_letter_exchange="classifier.dlx",
    dead_letter_queue="classifier.dlq",
    destination_exchange="items.classified",
)
executor.run()
```

`execute()` returns a message, a sequence of messages, or `None`. The Executor publishes the results
itself, only after `execute()` succeeds, so a failed message never produces partial output. It
publishes all results of one input in a single RabbitMQ transaction and acknowledges the input only
after the transaction is committed (see [Results are published in a transaction](#results-are-published-in-a-transaction)).

Subclassing `KicakExecutorAbstract[CrawledItem]` tells type checkers that `execute()` receives a
`CrawledItem`. A subclass of the plain `KicakExecutorAbstract` works too; its `execute()` then
receives a `KicakMessage`.

All queues/exchanges are declared automatically (idempotently) when the agent connects.

## Error handling and dead-lettering

The Executor decides the outcome of every delivery from how `execute()` ends:

| Outcome | Cause | Delivery is… | Executor |
| --- | --- | --- | --- |
| Success | `execute()` returns | results published in one transaction, then acknowledged | continues |
| Deterministic failure | missing body, undecodable body (not a JSON object, missing or unexpected fields, no valid `message_id`), or `MalformedMessageError` raised by `execute()` | rejected with `basic_nack(requeue=False)`, so RabbitMQ routes it to the dead-letter exchange | continues |
| Transient failure | `TransientProcessingError` raised by `execute()` | retried in-process; if every attempt fails, requeued with `basic_nack(requeue=True)` | continues |
| Unexpected failure | any other exception from `execute()`, or results that are not `KicakMessage`s, cannot be serialized, or are returned by a terminator | dead-lettered as above | logs the error and raises it, stopping |
| Results returned as unroutable | `UnroutableError`: no queue bound to the destination exchange when the results are committed | requeued with `basic_nack(requeue=True)`; the message is not at fault | logs the error and raises it, stopping |
| Results not committed | another error while publishing or committing the results, e.g. RabbitMQ refusing the commit | requeued by RabbitMQ when the connection closes | logs the error and raises it, stopping |
| Connection lost while publishing results | a transient connection error before or during the commit | already requeued by RabbitMQ when the connection dropped; uncommitted results are discarded by RabbitMQ, not re-sent | reconnects and receives the message again |
| Connection lost while settling | a transient connection error from the acknowledgement or rejection | requeued by RabbitMQ when the connection dropped, instead of being acknowledged, requeued, or dead-lettered | reconnects and receives the message again; after an unexpected failure or unroutable results, still raises that error and stops |

`execute()` is called up to `max_processing_attempts` times (default 3) while it raises
`TransientProcessingError`, with 0.5- and 1.0-second delays that double up to a 30-second cap. A
requeued message is delivered again later. With a classic queue, a failure that never clears keeps
being retried; a quorum queue dead-letters the message once it reaches its delivery limit (see
[Queue type](#queue-type)).

Dead-lettered messages keep their original body. RabbitMQ adds an `x-death` header recording the
source queue and the reason: `rejected` when the Executor rejected the message, which it logs at
`ERROR` level, or `delivery_limit` when a quorum queue's delivery limit was reached.

### Results are published in a transaction

All results of one input are published in a single RabbitMQ transaction, so RabbitMQ delivers all
of them or none. The input is acknowledged only after the transaction is committed, so it is
acknowledged only once every result has been delivered to the destination exchange. For example, a
chunker that splits a document into chunks never leaves its downstream agents with only some of the
chunks while its input is acknowledged or dead-lettered.

- **Before the commit, nothing is delivered.** If the connection is lost while the results are being
  published, RabbitMQ discards them and requeues the input, and the Executor executes it again.
- **The acknowledgement is not part of the transaction.** RabbitMQ routes the messages of a
  transaction only when it is committed, so a result that no queue is bound to is detected only after
  the commit. An acknowledgement committed together with it would remove the input while the result
  is dropped. The Executor therefore acknowledges after the commit; if RabbitMQ returned a result as
  unroutable, it requeues the input instead and stops.
- **A result can still be published twice.** If the process stops between the commit and the
  acknowledgement, or the connection drops after RabbitMQ committed the results but before the
  Executor learned of it, the input is executed again and all its results are published again; this
  is one of the reasons agents must be idempotent (see
  [Requirement: idempotent processing](#requirement-idempotent-processing)).
- **Two RabbitMQ cases can leave part of a transaction.** RabbitMQ does not guarantee atomicity if
  the broker itself fails during the commit. And a destination queue with a length limit and
  `overflow: reject-publish` can refuse some of the results while other queues accept them; the
  commit then fails and the Executor stops. Do not use `reject-publish` on queues that receive
  results of several messages per input.
- **Results are never re-sent on a new connection.** An acknowledgement can only be sent on the
  channel that delivered the message, and RabbitMQ requeues the delivery as soon as that connection
  is lost, so re-sending would duplicate the results. The redelivered input is executed again
  instead.

The results use a channel of their own, because a RabbitMQ channel in transaction mode cannot also be
in confirm mode; the Executor opens it, once per connection, the first time it publishes results.

## Running and stopping an Executor

The Executor takes one message at a time: it sets a prefetch count of 1, so RabbitMQ delivers the
next message only after the previous one has been acknowledged or rejected. Messages waiting in the
queue are therefore never held, unprocessed, by the Executor, and their RabbitMQ delivery timeout
does not start early.

`run()` consumes until you call `stop()`, and then returns normally.

### Graceful stop

`stop()` asks the Executor to finish its current work and then return from `run()` without taking
another message:

- **While a message is being processed,** the Executor finishes it: `execute()` completes, its
  results are published, and the message is acknowledged (or requeued or dead-lettered, as usual).
  `run()` then returns instead of waiting for the next message.
- **While waiting for a message,** `run()` returns within about a second, without taking one.
- **While reconnecting after a lost connection,** `run()` stops retrying and returns within about a
  second.
- **A message delivered after the stop request,** before the Executor notices it, is not processed;
  RabbitMQ returns it to the queue when the connection closes.

`stop()` only sets a flag, so it is safe to call from any thread and from a signal handler. Connect
it to the signals your process supervisor sends, for example:

```python
import signal

executor = ClassifyingExecutor(config, ...)  # topology arguments as above
for stop_signal in (signal.SIGINT, signal.SIGTERM):
    signal.signal(stop_signal, lambda _signum, _frame: executor.stop())
executor.run()  # returns after the message in progress, once a signal arrives
```

The stop request is permanent: calling `run()` on a stopped Executor returns immediately. Create a
new instance to consume again.

Give your supervisor's shutdown timeout (for example Kubernetes' `terminationGracePeriodSeconds`) at
least the longest `execute()` plus a few seconds; otherwise it kills the process in the middle of a
message.

### Other ways `run()` ends

Without a stop request, `run()` ends only by raising:

- a failure, as described above;
- `RuntimeError` if RabbitMQ cancels the consumer, which happens when the queue is deleted while
  the Executor runs. The Executor does not recreate the queue; under the permanent-topology model
  this is an error for an operator to investigate;
- a `BaseException` such as `SystemExit` or `KeyboardInterrupt`, for example Python's default
  reaction to Ctrl+C when no signal handler calls `stop()`. This is a hard stop: the exception
  passes through unchanged, even in the middle of `execute()`, the connection is closed, and RabbitMQ
  requeues the message being processed, which is then executed again.

### Status and timestamp

An Executor reports its lifecycle status, for example to a health check or a monitoring thread:

| `get_status()` | Meaning |
| --- | --- |
| `"RUNNING"` | Set when the Executor is created; stays while `run()` consumes, until `stop()` is called. |
| `"STOPPING"` | `stop()` was called; `run()` finishes the message in progress, then returns. |
| `"STOPPED"` | `run()` returned after a graceful stop. |
| `"CRASHED"` | `run()` ended by raising an exception: a failure or a hard stop. |

`get_status()` returns a plain string. The values are also available as the `ExecutorStatus`
string enum, so `executor.get_status() == ExecutorStatus.STOPPED` works too. `"STOPPED"` and
`"CRASHED"` are final: a later `stop()` does not change them. Calling `run()` again after
`"CRASHED"`, without a stop request, makes the Executor `"RUNNING"` again.

`get_timestamp()` returns when the status last changed or was last read by `get_status()`, as an
ISO 8601 string in UTC, for example `"2026-10-01T17:05:42.123456+00:00"`. Calling `get_timestamp()`
does not update it, so a monitor that calls `get_status()` regularly can tell when it last checked.

```python
status = executor.get_status()        # "RUNNING"; also updates the timestamp
checked_at = executor.get_timestamp() # e.g. "2026-10-01T17:05:42.123456+00:00"
```

Both methods are safe to call from any thread while `run()` is running, and neither uses the
RabbitMQ connection.

## Long-running `execute()`

RabbitMQ uses heartbeats to detect dead connections, and pika can answer them only between messages,
never while `execute()` runs. If one `execute()` call lasts longer than the connection's heartbeat
timeout, RabbitMQ closes the connection and requeues the message while it is still being processed.
The work is wasted and the message is executed again, so an `execute()` that always takes that long
never completes. An Executor that publishes results discards them, and a terminator's
acknowledgement fails on the closed connection; either way it reconnects and receives the message
again.

Only the application knows how long its processing takes, so each Executor type sets its own
timeout with `heartbeat_seconds`, in seconds. It defaults to 600 (10 minutes), sized for LLM calls
and heavy computation, and RabbitMQ uses the requested value even when its own default is lower.
Choose a value comfortably above the longest `execute()` you expect for that Executor type:

```python
executor = ClassifyingExecutor(
    config,
    source_queue="classifier",
    source_exchange="items.inbox",
    dead_letter_exchange="classifier.dlx",
    dead_letter_queue="classifier.dlq",
    destination_exchange="items.classified",
    heartbeat_seconds=1800,  # this classifier can take up to 20 minutes per item
)
```

Consider these when choosing the value:

- **A larger value detects dead connections later.** If an Executor's host or network disappears
  without closing the connection, RabbitMQ redelivers its in-flight message only after up to
  `heartbeat_seconds`.
- **RabbitMQ has a separate limit on unacknowledged messages.** The broker's `consumer_timeout`
  (30 minutes by default) closes the channel of a consumer that holds a message unacknowledged for
  longer, and requeues the message. If `execute()` can take longer than that, raise
  `consumer_timeout` on the broker as well; `heartbeat_seconds` alone is not enough.
- AMQP limits the value to 1–65535 seconds. Injectors keep the broker's default: they hold a
  connection only while publishing one message, so heartbeats do not limit them.

## Scaling an Executor

To process a queue faster, run several instances of the same Executor. Give every instance the
same configuration (`source_queue`, `source_exchange`, `dead_letter_exchange`, `dead_letter_queue`,
`destination_exchange`, `queue_type`, `max_processing_attempts`, and `heartbeat_seconds`), and
start them as independent processes:

```text
                                      ┌──► classifier instance 1 ──┐
items.inbox ──► classifier (queue) ───┼──► classifier instance 2 ──┼──► items.classified
                                      └──► classifier instance 3 ──┘
```

- All instances declare the same queue, binding, and dead-letter route; RabbitMQ treats identical
  declarations as no-ops. An instance configured differently, for example with another
  `dead_letter_exchange` or `queue_type`, fails to connect with `406 PRECONDITION_FAILED`.
- RabbitMQ delivers each message to one instance only. With the prefetch count of 1, each instance
  holds only the message it is processing, so the next message goes to whichever instance is free.
- Each instance acknowledges, requeues, and dead-letters its own deliveries; no coordination is
  needed. A requeued message may be delivered to any instance.
- Messages are processed in parallel, so they complete, and their results are published, in no
  particular order.

This is different from fan-out: Executors with their *own* queues bound to the same exchange each
receive every message.

Things to plan for when running several instances:

- **The same message can be executed by two instances at the same time.** When RabbitMQ redelivers
  a message, for example after a lost connection, another instance can receive it while the first is
  still running `execute()`. Results are still published only by the instance whose connection is
  intact, but side effects inside `execute()`, such as database writes, can happen twice
  concurrently. The idempotency that pykicak requires must therefore be enforced by the database
  (see [Requirement: idempotent processing](#requirement-idempotent-processing)).
- **An `execute()` longer than `heartbeat_seconds` causes such redeliveries.** RabbitMQ closes the
  connection and redelivers the message while it is still being processed (see
  [Long-running `execute()`](#long-running-execute)). With several instances, another instance
  picks it up immediately.
- **An unexpected error stops only the instance that hit it.** Each instance dead-letters the
  message and stops on its own, so a bug that fails every message dead-letters one message per
  instance before all have stopped.

## Delivery guarantees

Every message is published as mandatory and persistent (`delivery_mode=2`), with the content type
`application/json` and its `message_id` as the AMQP `message_id` property. The Injector publishes
in publisher-confirm mode: a publish returns only after RabbitMQ has taken responsibility for the
message, for persistent messages in durable queues after writing it to disk. The Executor publishes
its results in a transaction (see
[Results are published in a transaction](#results-are-published-in-a-transaction)): the commit
returns only after RabbitMQ has taken responsibility for all of them. RabbitMQ refusing a message
raises one of these errors, which are not retried:

- `pika.exceptions.UnroutableError`: no queue is bound to the exchange, so RabbitMQ would have
  discarded the message. For an Injector this means the first Executor's topology is not installed.
- `pika.exceptions.NackError` (Injector): RabbitMQ could not take responsibility for the message.
  For an Executor's results, RabbitMQ refuses the commit instead, which closes the results channel.

An Injector raises `InjectionError` to its caller, with one of these errors as its `__cause__`. An
Executor requeues the input message and stops (see
[Error handling and dead-lettering](#error-handling-and-dead-lettering)), so no message is lost
between two stages of a pipeline.

A mandatory message counts as routed if at least one queue is bound to the exchange. A fanout
exchange whose other bound queues are missing is not detected.

## Retries and logging

Injectors retry transient RabbitMQ connection and publish failures up to three attempts, with
1- and 2-second delays between attempts; each retry publishes the message again on a new
connection, and after the third failed attempt the error is raised. Executors reconnect indefinitely after transient failures, using exponential full jitter
with a 0.5-second initial delay and a 30-second maximum, but never retry a result publish (see
[Error handling and dead-lettering](#error-handling-and-dead-lettering)). Authentication and
access-denied failures are not retried.
Application processing is retried only for `TransientProcessingError` (see
[Error handling and dead-lettering](#error-handling-and-dead-lettering)). Message acknowledgements
are not retried: an acknowledgement is valid only on the connection that delivered the message, so
when the connection is lost while settling a message, RabbitMQ requeues it and the Executor
reconnects and receives it again.

An Injector's connection fails with `ConnectionBlockedTimeout` after RabbitMQ has blocked it for 60
seconds because of a memory or disk alarm, and the attempt is retried like other connection errors.
An Executor's connection stays blocked until the alarm clears, so it does not execute its message
again in the meantime.

Publishing is at-least-once: if the connection is lost before RabbitMQ's confirmation or the reply
to a commit arrives, RabbitMQ may already have accepted the messages. The Injector then publishes
its message again, and the Executor executes the redelivered input again. pykicak does not provide exactly-once delivery, which
is why it requires idempotent agents (see
[Requirement: idempotent processing](#requirement-idempotent-processing)).

The library emits standard Python logging records through module loggers. Applications configure
handlers and levels, for example:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

## Public API

`pykicak` exports `ExecutorStatus`, `InjectionError`, `KicakConfig`, `KicakConfigError`,
`KicakExecutorAbstract`, `KicakInjectorAbstract`, `KicakMessage`, `MalformedMessageError`,
`QueueType`, `TransientProcessingError`, and `__version__`. Build agents by subclassing
`KicakInjectorAbstract` or `KicakExecutorAbstract`; their shared base class is internal.

## Proof of concept

[`examples/poc/`](https://github.com/przemek-barczak/pykicak/blob/main/examples/poc) in the repository contains a runnable example (examples are not part of the
installed package): an injector that publishes a heartbeat message, an executor that publishes it
to a fan-out exchange, and two terminator executors that each declare their own queue and bind it
to that exchange. Each executor also declares a dead-letter
exchange and queue for its input queue (for example `poc.heartbeat.source.dlx` →
`poc.heartbeat.source.dlq`). Start all three executors (at least once) before running the
injector: until the fan-out executor has declared its queue, the injector fails with
`UnroutableError`, and until a terminator has declared its queue, the fan-out executor requeues the
message and stops. The queue/exchange topology is defined once in `examples/poc/messaging/`
(`queues.py`, `exchanges.py`, `bindings.py`) and imported by every agent — none of it lives in
`.kicak`.

Each process reads its `.kicak` file (connection details only) from the path in the
`KICAK_CONFIG_PATH` environment variable (defaulting to `.kicak`). The executor's role is
selected via `POC_EXECUTOR_ROLE` (`fanout`, `terminator1`, or `terminator2`). Ctrl+C or SIGTERM
stops an executor gracefully through `stop()`. Run the commands from the repository root:

```bash
KICAK_CONFIG_PATH=.kicak.injector python -m examples.poc.injector_agent
KICAK_CONFIG_PATH=.kicak.executor POC_EXECUTOR_ROLE=fanout python -m examples.poc.executor_agent
KICAK_CONFIG_PATH=.kicak.terminator1 POC_EXECUTOR_ROLE=terminator1 python -m examples.poc.executor_agent
KICAK_CONFIG_PATH=.kicak.terminator2 POC_EXECUTOR_ROLE=terminator2 python -m examples.poc.executor_agent
```

## Development

Install the package in editable mode with the development tools (pytest, ruff, mypy, build, and
twine), then run the test suite and the checks:

```bash
pip install -e . --group dev   # pip 25.1 or newer
pytest
ruff check .
mypy
```

This excludes integration tests, which require a live RabbitMQ broker (e.g.
`docker run -p 5672:5672 rabbitmq`). They connect with the `.kicak` file in the working directory,
or the one whose path is in the `KICAK_CONFIG_PATH` environment variable; with the container above,
a copy of `.kicak.example` works as it is. Run them explicitly with:

```bash
pytest -m integration
```

## License

`pykicak` is released under the [MIT License](https://github.com/przemek-barczak/pykicak/blob/main/LICENSE).
