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
  dead-lettered instead of blocking the queue. It keeps its part of the RabbitMQ topology in
  place, restoring deleted queues, exchanges, and bindings on both its input and its output side.
  Can operate in "terminator" mode where it consumes but does not publish (e.g. a database writer
  or quality checker).

This README covers what every agent must meet, installation, and a quick start; the full
documentation is in [`docs/`][docs].

## Requirement: idempotent processing

**pykicak is designed for idempotent agents only.** It delivers every message *at least once*, never
*exactly once*: wherever a failure leaves a choice, it processes a message again rather than risk
losing it, and the price is that a message can be processed, and its results published, more than
once. The few cases in which RabbitMQ itself can still lose a message, one of them with classic
retry queues, are listed in [When a message can still be lost][when-a-message-can-still-be-lost].
Every agent built on pykicak must therefore be idempotent: processing the same message twice, or
twice at the same time, must leave the system in the same state as processing it once. Using pykicak
for processing that is not idempotent can corrupt data or repeat external actions.

A message is processed more than once when:

- an Executor is killed or stopped without `stop()` (crash, `SystemExit`, `KeyboardInterrupt`, a
  supervisor's forced kill) while it processes a message, so RabbitMQ redelivers the message, which
  is executed again, and its results are published again if they had already been published;
  a graceful [`stop()`][graceful-stop] finishes the message first and avoids this;
- the connection is lost while an Executor commits its results, after RabbitMQ committed them but
  before the Executor learned of it, so the input is executed again and all its results are
  published again;
- processing a message, retries of `execute()` included, takes longer than `heartbeat_seconds` or
  than RabbitMQ's `consumer_timeout`, so the input is redelivered while it is still being executed,
  and with several instances it runs concurrently on another instance (see
  [Long-running `execute()`][long-running-execute]);
- `execute()` raises `TransientProcessingError`, so it is called again, and after
  `max_processing_attempts` failed calls the message waits in the retry queue and is executed again,
  up to `max_retry_rounds` times; if the Executor loses its connection between putting the message
  into the retry queue and acknowledging it, it is executed once more on top (see
  [Retry queue][retry-queue]);
- a dead-lettered message is sent back from the dead-letter queue, for example once a service that
  was down is back, so it is executed again; if the connection is lost while it is sent back, it can
  be sent back, and executed, twice (see [Sending dead letters back][sending-dead-letters-back]);
- an Executor dead-letters a message, and loses its connection, or `consumer_timeout` takes the
  message back, between putting a copy of it into the dead-letter queue and acknowledging it, so
  the message is executed again: it may be dead-lettered a second time, or succeed and be in the
  dead-letter queue and processed (see [Dead-letter queue][dead-letter-queue]); and if RabbitMQ
  does not take the copy, the Executor requeues the message, which is executed again;
- an Executor holds results that RabbitMQ returned as unroutable (see
  [Unroutable results are held][unroutable-results-are-held]), and `stop()` is called, so it
  requeues the input, or the connection is lost or `consumer_timeout` expires meanwhile, so
  RabbitMQ requeues it; either way the input is executed again;
- RabbitMQ refuses the commit of an Executor's results, or one of the declarations that verify its
  output side, so the input is requeued and executed again;
- the acknowledgement fails, for example because the connection was lost, or RabbitMQ's
  `consumer_timeout` took the input back right after the results were published, so RabbitMQ
  redelivers the input, which is executed again and its results published again;
- an Injector loses its connection before RabbitMQ confirms a message, so it publishes the message
  again;
- a RabbitMQ memory or disk alarm blocks an Injector's connection for longer than its
  `blocked_timeout_seconds` (60 by default), so the Injector gives up on that attempt and publishes
  the message again. RabbitMQ may still enqueue the earlier publish once the alarm clears, even
  when the injection ends with `InjectionError`.

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

## Configuration

Agents take their RabbitMQ connection details from a `KicakConfig`, which reads a `.kicak` file of
`KEY=VALUE` lines, the environment variables of the same names, or a mapping built in code:

```text
RABBIT_MQ_USERNAME=guest
RABBIT_MQ_PASSWORD=guest
RABBIT_MQ_HOST=localhost
RABBIT_MQ_PORT=5672
RABBIT_MQ_VIRTUAL_HOST=/
RABBIT_MQ_TLS=false
```

```python
from pykicak import KicakConfig

config = KicakConfig.from_file(".kicak")  # or KicakConfig.from_env(), or KicakConfig(values={...})
```

`RABBIT_MQ_TLS` is required, so an unencrypted connection is always an explicit choice: use `false`
only on a network you fully trust. Never commit a real `.kicak` file. See
[Configuration][configuration] for every key, environment variables, TLS, and client certificates.

## Message types

Messages sent and received by agents are declared as `KicakMessage` subclasses, independent of
any agent class, so the same type can be imported by both the publishing side (injector/executor)
and the consuming side (executor). Each class names its type with `kicak_type` (see
[Type names: the contract between agents][type-names-the-contract-between-agents]):

```python
import dataclasses
from pykicak import KicakMessage


@dataclasses.dataclass(frozen=True, slots=True)
class CrawledItem(KicakMessage):
    kicak_type = "crawled-item.v1"
    url: str
    content: str


@dataclasses.dataclass(frozen=True, slots=True)
class ClassifiedItem(KicakMessage):
    kicak_type = "classified-item.v1"
    url: str
    category: str


item = CrawledItem(url="https://example.com", content="...", message_id="5f0c7b1e-...")
```

Every message has a required, keyword-only `message_id`, declared by `KicakMessage`, so your classes
do not declare it. See [Message types][messages] for the rules for fields and IDs, how type names
form the contract between agents, and how to introduce a new version of a message without stopping
the flow.

## Usage

The examples use the messages defined in [Message types](#message-types) and literal topology names
for brevity. In an application, define these names centrally in its `messaging/` module as described
in [RabbitMQ topology][topology].

Executor (terminator mode — no publishing):

```python
from pykicak import (
    KicakConfig,
    KicakExecutorAbstract,
    MalformedMessageError,
    TransientProcessingError,
)


class SaveToDbExecutor(KicakExecutorAbstract[ClassifiedItem, None]):  # None: publishes nothing
    def execute(self, message: ClassifiedItem) -> None:
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
    source_queue="db-writer",  # dead-letters to db-writer.dlx, retained in db-writer.dlq
    source_exchange="items.classified",
    max_retry_rounds=360,  # 360 rounds of 10 seconds: waits out a database down for an hour
)
executor.run()
```

The exception `execute()` raises decides what happens to the message and the Executor: any
exception other than these two stops the Executor. See
[What `execute()` raises is your decision](#what-execute-raises-is-your-decision).

Injector (publishes an initial message):

```python
import uuid

from pykicak import KicakConfig, KicakInjectorAbstract


class StartProcessingInjector(KicakInjectorAbstract[CrawledItem]):  # the type it publishes
    def generate(self) -> CrawledItem:
        # A new ID where the message is first created; it never changes afterwards
        return CrawledItem(message_id=str(uuid.uuid4()), url="https://example.com", content="...")


config = KicakConfig.from_file(".kicak")
injector = StartProcessingInjector(config, exchange_name="items.inbox")
injector.run()  # publishes once, then returns
```

Exactly one queue may be bound to an Injector's exchange: the input queue of the first Executor,
here `classifier`, whose Executor binds it (see [RabbitMQ topology][topology]).

[Running an Injector][injector] describes how an injection is retried, what `run()`
raises when a message was not injected, `publish()`, and repeated injections.

Executor (with publishing to an exchange):

```python
import uuid

from pykicak import KicakConfig, KicakExecutorAbstract


class ClassifyingExecutor(KicakExecutorAbstract[CrawledItem, ClassifiedItem]):
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
    destination_exchange="items.classified",
    destination_queues=["db-writer", "quality-checker"],  # every queue bound to items.classified
    max_retry_rounds=0,  # its execute() never raises TransientProcessingError
)
executor.run()
```

`execute()` returns a message, a sequence of messages, or `None`. The Executor publishes the results
itself, only after `execute()` succeeds, so a failed message never produces partial output. It
publishes all results of one input in a single RabbitMQ transaction and acknowledges the input only
after the transaction is committed (see [Results are published in a
transaction][results-are-published-in-a-transaction]).

The class header `KicakExecutorAbstract[CrawledItem, ClassifiedItem]` declares that this Executor
accepts `CrawledItem` and publishes `ClassifiedItem`. Type checkers check `execute()` against it,
and the Executor refuses any other type in both directions (see
[Type names: the contract between agents][type-names-the-contract-between-agents]). Here the
queues `db-writer` and `quality-checker` must belong to Executors that accept `ClassifiedItem`, as
`SaveToDbExecutor` does.

Agents declare their queues and exchanges automatically (idempotently) when they connect, and
Executors declare them again while they run (see [Topology verification][topology-verification]).

`run()` consumes until `stop()` is called, from any thread or a signal handler. See
[Running and stopping an Executor][running-an-executor] for a graceful stop,
`execute()` calls that take long, and running several instances of an Executor.

## Error handling and dead-lettering

### What `execute()` raises is your decision

The library cannot tell whether a failure inside `execute()` will clear by itself, is caused by the
message, or is a bug in your code. Your `execute()` tells it, through the exception it raises, and
that decides what happens to the message and to the Executor:

| `execute()` | The message | The Executor |
| --- | --- | --- |
| returns | its results are published, then it is acknowledged | continues |
| raises `TransientProcessingError`: the failure may clear, e.g. a service is down, times out, or limits your rate | `execute()` is called again, up to `max_processing_attempts` times; if it still fails, the message is moved to the **retry queue** and executed again after `retry_delay_seconds` (10 by default), up to `max_retry_rounds` times; still failing after that, it is **dead-lettered** with the reason `retries-exhausted` (see [Retry queue][retry-queue]) | continues |
| raises `MalformedMessageError`: the message can never succeed, e.g. its content is invalid | **dead-lettered** to `<queue>.dlq` at once, without another attempt, with headers saying why (see [Dead-letter queue][dead-letter-queue]) | continues |
| raises **any other exception**: treated as a bug | dead-lettered, so it cannot block the queue | **stops**: `run()` raises the exception, and the status is `"CRASHED"` |
| raises a `BaseException` such as `KeyboardInterrupt` or `SystemExit` | not settled; RabbitMQ requeues it | stops at once, in the middle of `execute()`: `run()` raises it, and the status is `"CRASHED"` |

An exception never leads to `"STOPPED"`: that status follows only a graceful `stop()`.

"Any other exception" includes everything raised by the libraries `execute()` calls: a client
library's timeout, a `ConnectionError`, a `KeyError` for a missing field. Left uncaught, such an
error dead-letters the message and stops the Executor, even if it would have cleared a moment later.
So it is your job to catch the errors you expect, where they happen, and raise one of the two
classified exceptions from them:

```python
from pykicak import MalformedMessageError, TransientProcessingError


def execute(self, message: Document) -> Summary:
    if not message.text.strip():
        raise MalformedMessageError("document has no text")  # dead-lettered; the Executor continues
    try:
        text = self.summarizer.summarize(message.text)  # your client of an external service
    except (TimeoutError, ConnectionError) as error:
        raise TransientProcessingError("summarizer unavailable") from error  # retried later
    # Any other exception, e.g. a bug, dead-letters the message and stops the Executor
    return Summary(message_id=f"{message.message_id}.summary", text=text)
```

Stopping on an unexpected exception is deliberate: a bug would otherwise fail, and dead-letter,
every message in the queue. Stopping limits the damage to one message per instance (see [Scaling an
Executor][scaling-an-executor]) and makes the failure visible. Once the bug is fixed, start the
Executor again and, if needed, send the dead-lettered message back from `<queue>.dlq` (see
[Sending dead letters back][sending-dead-letters-back]).

Raise `TransientProcessingError` only for failures that can clear. A failure that never clears keeps
the message going round through the retry queue, calling `execute()` again every round, until
`max_retry_rounds` are used up and it is dead-lettered (see [Retry queue][retry-queue]). Every call
may have done part of the work before failing, which is one more reason `execute()` must be
idempotent (see [Requirement: idempotent processing](#requirement-idempotent-processing)).

### Where failed messages go

A message that keeps raising `TransientProcessingError` waits in the Executor's retry queue
`<queue>.retry` between rounds (see [Retry queue][retry-queue]). A dead-lettered message is kept in
`<queue>.dlq` with headers saying why (see [Dead-letter queue][dead-letter-queue]), and can be sent
back once the cause is fixed (see [Sending dead letters back][sending-dead-letters-back]).
[Every outcome of a delivery][outcomes] lists what happens to a delivery in
every case, connection failures included.

## Documentation

- [Configuration][configuration]: the `.kicak` file, environment variables, every key, TLS, and
  client certificates (mutual TLS).
- [RabbitMQ topology][topology]: the constructor arguments, naming conventions, who declares what,
  topology verification, and queue types.
- [Message types][messages]: message classes, `message_id`, type names, and introducing a new
  version of a message.
- [Running an Injector][injector]: how an injection is retried, `InjectionError`, `publish()`, and
  repeated injections.
- [Every outcome of a delivery][outcomes]: what happens to a delivery in every case, held unroutable
  results, and the results' transaction.
- [Retry queue][retry-queue]: where a message waits between retries, `max_retry_rounds`, and its
  headers.
- [Dead-letter queue][dead-letter-queue]: where failed messages are kept, the headers that say why,
  and sending them back.
- [Running and stopping an Executor][running-an-executor]: graceful stop, long-running `execute()`,
  and scaling to several instances.
- [Monitoring][monitoring]: `get_info()`, logging, statistics in the logs, and connection names.
- [Delivery guarantees][delivery-guarantees]: what publishing guarantees, who published a message
  and when, and connection retries.
- [Proof of concept][proof-of-concept]: the runnable example in `examples/poc/`, including a message
  upgrade in progress.
- [Development][development]: running the tests and checks, and CI.

## Public API

`pykicak` exports `DeadLetterCounts`, `ExecutorCounts`, `ExecutorInfo`, `ExecutorStatus`,
`InjectionError`, `KicakConfig`, `KicakConfigError`, `KicakExecutorAbstract`,
`KicakInjectorAbstract`, `KicakMessage`, `MalformedMessageError`, `QueueType`,
`TransientProcessingError`, `tls_options`, and `__version__`. Build agents by subclassing
`KicakInjectorAbstract` or `KicakExecutorAbstract`; their shared base class is internal.
`tls_options(config)` returns the TLS settings agents connect with, for a pika connection of your
own (see [TLS for a connection of your own][tls-for-a-connection-of-your-own]).

## Development

See [Development][development] for running the tests and the checks CI runs, and
[`RELEASING.md`](https://github.com/przemek-barczak/pykicak/blob/main/RELEASING.md) for how a
version is released.

## License

`pykicak` is released under the [MIT License](https://github.com/przemek-barczak/pykicak/blob/main/LICENSE).

[docs]: https://github.com/przemek-barczak/pykicak/blob/main/docs/README.md
[graceful-stop]: https://github.com/przemek-barczak/pykicak/blob/main/docs/running-an-executor.md#graceful-stop
[long-running-execute]: https://github.com/przemek-barczak/pykicak/blob/main/docs/running-an-executor.md#long-running-execute
[retry-queue]: https://github.com/przemek-barczak/pykicak/blob/main/docs/retry-queue.md
[sending-dead-letters-back]: https://github.com/przemek-barczak/pykicak/blob/main/docs/dead-letter-queue.md#sending-dead-letters-back
[dead-letter-queue]: https://github.com/przemek-barczak/pykicak/blob/main/docs/dead-letter-queue.md
[unroutable-results-are-held]: https://github.com/przemek-barczak/pykicak/blob/main/docs/outcomes.md#unroutable-results-are-held
[configuration]: https://github.com/przemek-barczak/pykicak/blob/main/docs/configuration.md
[tls-for-a-connection-of-your-own]: https://github.com/przemek-barczak/pykicak/blob/main/docs/configuration.md#tls-for-a-connection-of-your-own
[type-names-the-contract-between-agents]: https://github.com/przemek-barczak/pykicak/blob/main/docs/messages.md#type-names-the-contract-between-agents
[messages]: https://github.com/przemek-barczak/pykicak/blob/main/docs/messages.md
[topology]: https://github.com/przemek-barczak/pykicak/blob/main/docs/topology.md
[injector]: https://github.com/przemek-barczak/pykicak/blob/main/docs/injector.md
[results-are-published-in-a-transaction]: https://github.com/przemek-barczak/pykicak/blob/main/docs/outcomes.md#results-are-published-in-a-transaction
[topology-verification]: https://github.com/przemek-barczak/pykicak/blob/main/docs/topology.md#topology-verification
[running-an-executor]: https://github.com/przemek-barczak/pykicak/blob/main/docs/running-an-executor.md
[scaling-an-executor]: https://github.com/przemek-barczak/pykicak/blob/main/docs/running-an-executor.md#scaling-an-executor
[outcomes]: https://github.com/przemek-barczak/pykicak/blob/main/docs/outcomes.md
[monitoring]: https://github.com/przemek-barczak/pykicak/blob/main/docs/monitoring.md
[delivery-guarantees]: https://github.com/przemek-barczak/pykicak/blob/main/docs/delivery-guarantees.md
[when-a-message-can-still-be-lost]: https://github.com/przemek-barczak/pykicak/blob/main/docs/delivery-guarantees.md#when-a-message-can-still-be-lost
[proof-of-concept]: https://github.com/przemek-barczak/pykicak/blob/main/docs/proof-of-concept.md
[development]: https://github.com/przemek-barczak/pykicak/blob/main/docs/development.md
