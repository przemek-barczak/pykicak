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
- processing a message, retries of `execute()` included, takes longer than `heartbeat_seconds` or
  than RabbitMQ's `consumer_timeout`, so the input is redelivered while it is still being executed,
  and with several instances it runs concurrently on another instance (see
  [Long-running `execute()`](#long-running-execute));
- `execute()` raises `TransientProcessingError`, so it is called again, and after
  `max_processing_attempts` failed calls the message waits in the retry queue and is executed again,
  as often as the failure lasts; if the Executor loses its connection between putting the message
  into the retry queue and acknowledging it, it is executed once more on top (see
  [Retry queue](#retry-queue));
- an Executor dead-letters a message, and loses its connection, or `consumer_timeout` takes the
  message back, between putting a copy of it into the dead-letter queue and acknowledging it, so
  the message is executed again: it may be dead-lettered a second time, or succeed and be in the
  dead-letter queue and processed (see [Dead-letter queue](#dead-letter-queue)); and if RabbitMQ
  does not take the copy, the Executor requeues the message, which is executed again;
- an Executor holds results that RabbitMQ returned as unroutable (see
  [Unroutable results are held](#unroutable-results-are-held)), and `stop()` is called, so it
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

Applications load RabbitMQ **connection** details into a `KicakConfig` and pass it to an agent. A
config comes from one of three sources, all with the same keys:

- `KicakConfig.from_file(path)` reads a [`.kicak` file](#the-kicak-file).
- `KicakConfig.from_env()` reads the [environment variables](#environment-variables) of the same
  names, for example in a container.
- `KicakConfig(values={...})` takes a mapping built in code, for example with values from a secrets
  manager. The values are used exactly as given.

The library never picks a source itself: it reads no file and no environment variable unless the
application calls `from_file()` or `from_env()`.

| Key | Description |
| --- | --- |
| `RABBIT_MQ_USERNAME` | RabbitMQ username |
| `RABBIT_MQ_PASSWORD` | RabbitMQ password |
| `RABBIT_MQ_HOST` | RabbitMQ host |
| `RABBIT_MQ_PORT` | RabbitMQ port |
| `RABBIT_MQ_VIRTUAL_HOST` | RabbitMQ virtual host |
| `RABBIT_MQ_TLS` | `true` to connect with TLS (usually port 5671), `false` for plain, unencrypted AMQP (see [TLS](#tls)) |
| `RABBIT_MQ_CA_FILE` | Optional, TLS only: a file of PEM CA certificates, the only CAs trusted to have issued the broker's certificate (see [TLS](#tls)) |

Every key except `RABBIT_MQ_CA_FILE` is required. Whatever the source, keys are checked when they
are read, that is when an agent connects: a missing required key raises `KicakConfigError` (from an
Injector's `run()` or `publish()`, as the cause of `InjectionError`), and so does a port that is
not an integer from 1 to 65535, a `RABBIT_MQ_TLS` other than `true` or `false` (in any letter
case), and an empty `RABBIT_MQ_CA_FILE`.

`KicakConfig` keeps values out of its `repr()` and its errors, so they do not reach logs or crash
reporters: the repr lists only the keys (`KicakConfig(keys=['RABBIT_MQ_HOST', ...])`), and a
malformed line or invalid value is reported by its line number or key, never its content.

A `KicakConfig` is immutable: its `values` is a read-only copy of the mapping it was created with.
It is hashable, and it can be pickled, for example to pass it to a `multiprocessing` worker.

A config does **not** hold queue, exchange, or binding names — that is application topology, not
connection configuration. See [RabbitMQ topology](#rabbitmq-topology) below.

### The `.kicak` file

A `.kicak` file is a simple `KEY=VALUE` text file, read as UTF-8 on every platform (a leading byte
order mark is allowed). Blank lines and lines starting with `#` are ignored, keys and values are
stripped of surrounding whitespace, and only the first `=` separates the key from the value. See
[`.kicak.example`](https://github.com/przemek-barczak/pykicak/blob/main/.kicak.example) for a
template.

Never commit a real `.kicak` file; `.kicak` files are excluded by `.gitignore`.

Missing or unreadable files, invalid UTF-8, malformed lines (including a line with an empty key),
and duplicate keys raise `KicakConfigError` from `from_file()`.

### Environment variables

`KicakConfig.from_env()` reads the keys from the environment variables of the same names, such as
`RABBIT_MQ_HOST`, for example as set by a Compose file or a Kubernetes secret:

```python
config = KicakConfig.from_env()
```

It takes only the keys in the table above, never other variables, so the config does not collect
unrelated secrets of the process. As in a `.kicak` file, values are stripped of surrounding
whitespace, which also removes the trailing newline of a secret created from a file; a variable
that is not set leaves its key missing, and one set to an empty value keeps it empty. A value that
is not valid UTF-8 raises `KicakConfigError`, naming only the variable. The environment is read
once, when `from_env()` is called.

Sources can be combined through `values`. Here every key set in the environment, such as a password
from a Kubernetes secret, overrides the file:

```python
file_config = KicakConfig.from_file("/etc/orders/.kicak")
config = KicakConfig(values={**file_config.values, **KicakConfig.from_env().values})
```

### TLS

Without TLS, the connection to RabbitMQ is not encrypted: the username, the password, and every
message cross the network in clear text, and anyone on the path can read or change them. Use
`RABBIT_MQ_TLS=false` only for a broker on the same host or on a network you fully trust. Managed
brokers offer TLS, and some, such as Amazon MQ, accept nothing else. `RABBIT_MQ_TLS` is required,
so an unencrypted connection is always an explicit choice:

```text
RABBIT_MQ_HOST=broker.example.com
RABBIT_MQ_PORT=5671
RABBIT_MQ_TLS=true
```

With `RABBIT_MQ_TLS=true`, agents use Python's recommended TLS client settings
(`ssl.create_default_context()`) with TLS 1.2 or newer, and they connect only if the broker's
certificate is valid and:

- is issued by a trusted CA. Without `RABBIT_MQ_CA_FILE` these are the CAs the operating system
  trusts, which is enough for brokers with a publicly trusted certificate, such as managed ones.
  With it, they are only the CA certificates in that file: set it for a broker whose certificate
  comes from a private CA. The file may also hold an intermediate CA, which is then trusted on its
  own. A relative path is resolved against the working directory of the process.
- names `RABBIT_MQ_HOST`, as a DNS name or an IP address. Connect with the name the certificate was
  issued for.

There is deliberately no setting that turns these checks off; for a self-hosted broker, give its CA
certificate in `RABBIT_MQ_CA_FILE` instead. Certificates are also checked strictly
(`ssl.VERIFY_X509_STRICT`), as Python 3.13 and newer do by default, and pykicak applies the same
checks on Python 3.12, so a certificate is accepted or refused alike on every supported version. A
private CA made with an old tool may therefore be refused, for example one whose Basic Constraints
extension is not marked critical; issue it again with a current tool rather than relaxing the
check. Client certificates (mutual TLS) are not supported: agents log in with the
username and password, protected by the encryption.

When TLS fails:

- A broker certificate that fails verification (an untrusted CA, an expired certificate, or another
  host name) raises `ssl.SSLCertVerificationError` and is never retried: either the configuration
  is wrong or the server is not the intended broker. An Executor stops, with the status
  `"CRASHED"`; an Injector raises `InjectionError` with it as the cause, after one attempt. An
  expired broker certificate therefore stops every Executor until it is renewed, so monitor its
  expiry date.
- A `RABBIT_MQ_CA_FILE` that is missing, unreadable, or contains no PEM certificate raises
  `KicakConfigError` when connecting, as does one given with `RABBIT_MQ_TLS=false`, because it
  shows that TLS was intended. The error names the key, never the path.
- Other TLS errors, for example a handshake cut off, are retried like other connection errors. That
  includes a port that does not speak TLS, such as 5672 with `RABBIT_MQ_TLS=true` (see
  [Retries and logging](#retries-and-logging)).

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
| `blocked_timeout_seconds` | `KicakInjectorAbstract` (optional, default 60) | How long an attempt waits on a connection that a RabbitMQ memory or disk alarm blocks before it fails and is retried; a finite number greater than 0 (see [Usage](#usage)) |
| `source_queue` | `KicakExecutorAbstract` (required) | Queue to consume messages from |
| `source_exchange` | `KicakExecutorAbstract` (required) | Upstream exchange `source_queue` is bound to |
| `destination_exchange` | `KicakExecutorAbstract` (optional) | Exchange that the results returned by `execute()` are published to |
| `destination_queues` | `KicakExecutorAbstract` (required with `destination_exchange`) | Every queue that must receive the results, at least one; the Executor declares and binds each of them before publishing (see [Topology verification](#topology-verification)) |
| `queue_type` | `KicakExecutorAbstract` (optional, default `QueueType.CLASSIC`) | RabbitMQ type of every queue the Executor declares: `QueueType.CLASSIC` or `QueueType.QUORUM` (see [Queue type](#queue-type)) |
| `max_processing_attempts` | `KicakExecutorAbstract` (optional, default 3) | How many times `execute()` is called for one delivery, one call after the other, while it raises `TransientProcessingError`, before the message is moved to the retry queue (see [Retry queue](#retry-queue)) |
| `retry_delay_seconds` | `KicakExecutorAbstract` (optional, default 10) | How long a message waits in the retry queue `<queue>.retry` before it returns to the back of the input queue; from 0 to 4294967.295 (about 49.7 days) (see [Retry queue](#retry-queue)) |
| `heartbeat_seconds` | `KicakExecutorAbstract` (optional, default 600) | AMQP heartbeat timeout of the Executor's connections, an int from 1 to 65535; set it above the longest processing of one message, retries of `execute()` included (see [Long-running `execute()`](#long-running-execute)) |
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
[Dead-letter queue](#dead-letter-queue)). For the queue `classifier`, these are
`classifier.dlx` and `classifier.dlq`. Only the queue's own Executor declares them. Because every
input queue's arguments follow from its name and `queue_type` alone, any Executor can declare a
queue exactly as the queue's own Executor does, which is what lets an upstream Executor restore a
deleted downstream queue. A dead-letter queue has no dead-letter exchange of its own, so it can be
neither an Executor's source queue nor one of its destination queues. The constructor refuses that,
a source or destination exchange that is the source queue's dead-letter exchange, and a
destination exchange that is a destination queue's.

**The retry queue follows a convention too.** Each input queue has a durable retry queue
`<queue>.retry`, for example `classifier.retry`, of the same `queue_type`, which returns its
messages to the input queue (see [Retry queue](#retry-queue)). Only the queue's own Executor
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
[Delivery guarantees](#delivery-guarantees)). To fan out what an Injector sends, let the first
Executor publish it to an exchange of its own.

The library assumes this deployment model:

- **Each queue belongs to one Executor type, which can run as several instances.** To scale an
  Executor, run more processes of it with the same configuration; they consume from the same queue
  and RabbitMQ shares its messages between them (see [Scaling an Executor](#scaling-an-executor)).
- **One queue type for the whole system.** Every Executor declares its downstream queues with its
  own `queue_type`, so all Executors of a system must use the same type; a mismatch fails with
  `406 PRECONDITION_FAILED`.
- **Permanent topology.** Once declared, exchanges and queues stay in the broker, and agents never
  delete them. Executors verify their topology all the time (see
  [Topology verification](#topology-verification)), so anything removed by mistake is restored. The
  messages of a deleted queue are lost with it: RabbitMQ discards them. To redesign the topology,
  stop injecting, let the Executors drain every queue, stop every instance with
  [`stop()`](#graceful-stop), remove the whole structure, then start the new Executors, which
  declare the new structure.
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

### Topology verification

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
  dead-lettered message is published to the dead-letter exchange, and a quorum queue dead-letters
  a requeued message once it reaches its delivery limit; RabbitMQ drops a message it dead-letters
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

A few milliseconds remain between the verification of the output side and the commit of the
results. If every binding of the destination exchange disappears in that window, RabbitMQ returns
the results as unroutable, and the Executor holds them and tries again (see
[Unroutable results are held](#unroutable-results-are-held)). If only some disappear, the results
reach the other queues and are lost for the missing ones; this window is the only place left where
that can happen.

### Queue type

`queue_type` selects the RabbitMQ queue type of every queue an Executor declares: its input queue,
its dead-letter queue, its retry queue, and its destination queues, so all Executors of a system use
the same type.
Pass a member of the exported `QueueType` enum:

- `QueueType.CLASSIC` (the default) is a queue stored on one broker node.
- `QueueType.QUORUM` is a queue replicated across the nodes of a RabbitMQ cluster, which keeps its
  messages when a node fails. It also works on a single node.

```python
from pykicak import QueueType

executor = ClassifyingExecutor(
    config,
    source_queue="classifier",
    source_exchange="items.inbox",
    destination_exchange="items.classified",
    destination_queues=["db-writer", "quality-checker"],
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
delivery limit of 20: a message delivered 21 times is dead-lettered with the `x-death` reason
`delivery_limit` instead of being delivered again. Every requeue counts, for example after a lost
connection or a hard stop. A message that comes back from the retry queue arrives as a new message,
so its count starts again: the limit never ends the retries of `TransientProcessingError` (see
[Retry queue](#retry-queue)). A classic queue has no such limit and requeues a message indefinitely.
The limit can be changed with a RabbitMQ policy (`delivery-limit`).

## Message types

Messages sent and received by agents are declared as `KicakMessage` subclasses, independent of
any agent class, so the same type can be imported by both the publishing side (injector/executor)
and the consuming side (executor). Each class names its type with `kicak_type` (see
[Type names: the contract between agents](#type-names-the-contract-between-agents)):

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
  it in its log records. For a delivery whose body cannot be decoded, the logs show the
  `message_id` property its sender set (`None` if it set none), so a dead-lettered message can
  still be found by its ID.

A message class that defines `__post_init__` must call `KicakMessage.__post_init__(self)`, which
validates the ID. Zero-argument `super()` does not work in a `slots=True` dataclass on Python 3.12.
Its own validation may raise any exception: an Executor dead-letters a received message that its
class refuses, and carries on with the next one.

Fields must be JSON-serializable (store timestamps as ISO-8601 strings, not `datetime` objects).
Floats must be finite: JSON has no NaN or infinity, and other languages' JSON parsers refuse the
`NaN` and `Infinity` that Python writes by default. Serializing a message that holds one raises
`ValueError` (an Executor dead-letters the input whose results hold one), and decoding refuses
`NaN`, `Infinity`, `-Infinity`, and a number too large for a float.
Decoding checks that the body is a JSON object with exactly the fields of the class that the
delivery's `type` property names; it does not convert or check the types of their values, so a
nested dataclass, for example, is decoded as a `dict`.
Validate values in `execute()` and raise `MalformedMessageError` for invalid ones. A dataclass you
nest in a message, also inside a list, tuple, or dict, is serialized as a JSON object of its
fields.

A message that `from_bytes()` decoded can be serialized again with `to_bytes()`, with two limits:

- **Your message contract.** The class must not turn decoded values into ones JSON cannot hold, for
  example a `__post_init__` that replaces an ISO-8601 string with a `datetime`.
- **Python's `json` module.** It sets the limits: decoding refuses a body nested too deeply for it,
  and it encodes nesting about as deep as it decodes, though not always exactly as deep (about
  10,000 levels on CPython 3.12 and 3.13, more on 3.14).

Every string decoding accepts is written again, even one JSON can only escape, such as a lone
surrogate `\ud800`: `to_bytes()` writes every character outside ASCII as a `\u` escape. That makes
non-ASCII text larger (a Polish letter takes 6 bytes instead of 2) but keeps every body ASCII. The
bytes can differ from the body received, in whitespace or escapes, but they hold the same JSON
value.

### Type names: the contract between agents

`kicak_type` names a message type on the wire. It is the contract between the agents that publish
the type and the Executors that consume it:

- **You choose it, once.** It is a fixed string, not derived from the class name, so renaming or
  moving the class does not break the contract, and agents written in other languages can use the
  same name. It must be a non-empty string of at most 255 bytes in UTF-8. Put the version in it,
  for example `.v1`.
- **It is a class attribute, not a field.** Assign it without an annotation, or annotate it as
  `ClassVar[str]`; it is not part of the body. With `kicak_type: str` it would be a field, and
  agents refuse the class.
- **Each class declares its own.** An agent refuses a class that only inherits `kicak_type` from
  its base class, because its messages would be sent under the name of another type.
- **It travels as the AMQP `type` property** of every message, next to the `message_id` property.
  RabbitMQ's management UI shows it; the body holds only the fields.

Each agent declares its message types once, in its class header, where both type checkers and the
library read them:

| Agent | Class header | Declares |
| --- | --- | --- |
| Injector | `KicakInjectorAbstract[CrawledItem]` | the type it publishes |
| Executor | `KicakExecutorAbstract[CrawledItem, ClassifiedItem]` | the types it accepts, and the type of its results |
| Terminator | `KicakExecutorAbstract[ClassifiedItem, None]` | the types it accepts, and `None`: it publishes nothing |

The library enforces the declarations at both ends:

- **Publishing.** Every message an agent publishes must be exactly of its declared class, not a
  subclass. Anything else is a bug in the agent: an Injector raises `TypeError` before publishing,
  and an Executor dead-letters the input and stops (see
  [Every outcome of a delivery](#every-outcome-of-a-delivery)).
- **Receiving.** An Executor reads the `type` property of each delivery and decodes the body into
  the accepted class of that name. A delivery without a `type`, or with one the Executor does not
  accept, is dead-lettered as malformed, and the Executor continues; the log line names the type
  received and the types accepted. The Executor never compares fields to guess a type, so a message
  of another type is refused even if its fields happen to match.
- **Creating an agent.** The declaration is checked when an agent is created, before it connects.
  A missing class header (a subclass of the plain `KicakExecutorAbstract` or
  `KicakInjectorAbstract`), a class without its own `kicak_type`, or two accepted classes with the
  same `kicak_type` raise `TypeError`. An Executor that declares a result type needs
  `destination_exchange` and `destination_queues`, and a terminator must not have them; otherwise
  `ValueError`. A generic class of your own in between, such as
  `class Base[M: KicakMessage](KicakExecutorAbstract[M, Summary])`, works as long as the class you
  create gives concrete types.

So every consumer of an exchange must accept what its producer declares: the first Executor accepts
its Injector's type, and every Executor whose queue is in another Executor's `destination_queues`
accepts that Executor's result type.

### A new version of a message

Never change the fields of a message class in place. Decoding requires exactly the class's fields,
so the messages already waiting in the queues, with the old fields, would be dead-lettered, and
under one type name nothing would tell old messages from new ones. A change of the fields, or of
what a value means, is a new class with a new `kicak_type`, next to the old one:

```python
@dataclasses.dataclass(frozen=True, slots=True)
class ChunkV1(KicakMessage):
    kicak_type = "chunk.v1"
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class ChunkV2(KicakMessage):
    kicak_type = "chunk.v2"
    text: str
    language: str
```

Do not derive `ChunkV2` from `ChunkV1`: a `case ChunkV1()` would then match V2 messages too.

An Executor accepts several versions with a union in its class header, and `execute()` receives
each message as the class it was sent as:

```python
from typing import assert_never


class Summarizer(KicakExecutorAbstract[ChunkV1 | ChunkV2, Summary]):
    def execute(self, message: ChunkV1 | ChunkV2) -> Summary:
        match message:
            case ChunkV2():
                language = message.language
            case ChunkV1():
                language = "und"  # V1 messages predate language detection
            case _:
                assert_never(message)  # a type checker reports a version left unhandled
        ...
```

A union used in several places can be named with the `type` statement, e.g.
`type AnyChunk = ChunkV1 | ChunkV2` and then `KicakExecutorAbstract[AnyChunk, Summary]`.

That lets you upgrade from V1 to V2 without stopping the flow and without touching the messages
already queued:

1. **Consumers first.** Deploy every Executor that consumes the producer's messages, all of its
   instances, accepting both versions: `KicakExecutorAbstract[ChunkV1 | ChunkV2, ...]`.
2. **Then the producer.** Deploy the Injector or Executor that creates the messages, publishing V2:
   `KicakInjectorAbstract[ChunkV2]` or `KicakExecutorAbstract[..., ChunkV2]`. While its instances
   restart one by one, both versions are published, and the consumers accept both.
3. **Then drop V1**, once no V1 message is left: none in the consumers' queues, and none in their
   retry queues `<queue>.retry`, from which a message comes back later (see
   [Retry queue](#retry-queue)). Deploy the consumers accepting only V2.

The [proof of concept](#proof-of-concept) shows this upgrade in progress, as code you can run.

In the other order, the consumers dead-letter every message of a version they do not accept yet.
The messages are kept in `<queue>.dlq` with their `type` property, so they can be replayed once the
consumers accept them. A V1 message replayed after step 3 is dead-lettered again, so keep V1
accepted for as long as you may replay such messages.

## Usage

The examples use the messages defined in [Message types](#message-types) and literal topology names
for brevity. In an application, define these names centrally in its `messaging/` module as described in
[RabbitMQ topology](#rabbitmq-topology).

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
here `classifier`, whose Executor binds it (see [RabbitMQ topology](#rabbitmq-topology)).

Each injection is short-lived. `run()` calls `generate()` and, if it returns a message, opens a
connection, publishes the message, waits for RabbitMQ's confirmation, and closes the connection. If
`generate()` returns `None`, no connection is opened. Connecting and publishing are attempted up to
three times: immediately, after 1 second, and after 2 more seconds, each time on a new connection
and with the same message. An attempt waits at most `blocked_timeout_seconds` (60 by default) on a
connection that a RabbitMQ memory or disk alarm blocks; it then fails with
`pika.exceptions.ConnectionBlockedTimeout` and is retried, so an Injector started by a scheduler
never hangs during an alarm. Lower it for an Injector that someone waits for, for example one
answering an HTTP request: `StartProcessingInjector(config, exchange_name="items.inbox",
blocked_timeout_seconds=5)` gives up within about 20 seconds, three attempts of 5 seconds plus the
1- and 2-second waits between them. Every attempt that timed out may still be enqueued once the
alarm clears, so the message can arrive more than once.

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
Errors from your own `generate()`, or from serializing its message, and the `TypeError` for a
message of another class than the declared one, are logged at `ERROR` and propagate unchanged, not
wrapped, because they are application bugs rather than delivery failures.

`injector.run(interval_seconds=60)` repeats the injection every 60 seconds until the process ends;
there is no `stop()`. No connection is kept open between injections, so the interval is not limited
by RabbitMQ's heartbeat. A failed injection ends `run()` with its error, as above, so no message is
skipped silently. `interval_seconds` is checked before anything is generated: NaN or infinity
raises `ValueError`, and a value that is not an `int` or `float`, `bool` included, `TypeError`.

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
)
executor.run()
```

`execute()` returns a message, a sequence of messages, or `None`. The Executor publishes the results
itself, only after `execute()` succeeds, so a failed message never produces partial output. It
publishes all results of one input in a single RabbitMQ transaction and acknowledges the input only
after the transaction is committed (see [Results are published in a transaction](#results-are-published-in-a-transaction)).

The class header `KicakExecutorAbstract[CrawledItem, ClassifiedItem]` declares that this Executor
accepts `CrawledItem` and publishes `ClassifiedItem`. Type checkers check `execute()` against it,
and the Executor refuses any other type in both directions (see
[Type names: the contract between agents](#type-names-the-contract-between-agents)). Here the
queues `db-writer` and `quality-checker` must belong to Executors that accept `ClassifiedItem`, as
`SaveToDbExecutor` does.

Agents declare their queues and exchanges automatically (idempotently) when they connect, and
Executors declare them again while they run (see [Topology verification](#topology-verification)).

## Error handling and dead-lettering

### What `execute()` raises is your decision

The library cannot tell whether a failure inside `execute()` will clear by itself, is caused by the
message, or is a bug in your code. Your `execute()` tells it, through the exception it raises, and
that decides what happens to the message and to the Executor:

| `execute()` | The message | The Executor |
| --- | --- | --- |
| returns | its results are published, then it is acknowledged | continues |
| raises `TransientProcessingError`: the failure may clear, e.g. a service is down, times out, or limits your rate | `execute()` is called again, up to `max_processing_attempts` times; if it still fails, the message is moved to the **retry queue**, not dead-lettered, and executed again after `retry_delay_seconds` (10 by default), as often as needed (see [Retry queue](#retry-queue)) | continues |
| raises `MalformedMessageError`: the message can never succeed, e.g. its content is invalid | **dead-lettered** to `<queue>.dlq` at once, without another attempt, with headers saying why (see [Dead-letter queue](#dead-letter-queue)) | continues |
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
Executor](#scaling-an-executor)) and makes the failure visible. Once the bug is fixed, start the
Executor again and, if needed, replay the dead-lettered message from `<queue>.dlq`.

Raise `TransientProcessingError` only for failures that can clear. It never dead-letters a message,
whatever the queue type: a failure that never clears keeps the message going round through the
retry queue until you fix the cause or take the message out (see [Retry queue](#retry-queue)). Every
call may have done part of the work before failing, which is one more reason `execute()` must be
idempotent (see [Requirement: idempotent processing](#requirement-idempotent-processing)).

### Retry queue

When `execute()` still raises `TransientProcessingError` after `max_processing_attempts` calls, the
message does not go back to the front of its queue, where RabbitMQ would hand it out again at once
and hold up the messages behind it. It waits in the Executor's retry queue instead:

```text
classifier ──► Executor ── still failing after max_processing_attempts ──► classifier.retry
    ▲                                                                            │
    └─────────── after retry_delay_seconds, RabbitMQ returns it to the back ─────┘
```

1. The Executor publishes a copy of the message to `<queue>.retry`, and acknowledges the original
   only after RabbitMQ has confirmed the copy. If the connection is lost in between, RabbitMQ
   redelivers the original as well, so the message is executed once more: never lost, possibly
   repeated.
2. Nothing consumes the retry queue. Each copy expires there after `retry_delay_seconds` (10 by
   default), and RabbitMQ dead-letters it, through its default exchange, which routes by queue
   name, to the back of the input queue. RabbitMQ removes the expiry on the way, so the message does
   not expire again there.
3. Whichever instance is free executes it again, with another `max_processing_attempts` calls.
   Messages that arrived meanwhile are processed while it waits.

There is no limit on the number of rounds: a failure that never clears keeps the message going
round every `retry_delay_seconds`. The copy keeps the body and the message's properties, its `type`
included, so it comes back as the same version of the message, and its headers, except the headers
RabbitMQ manages itself (`x-death`, `x-delivery-count`, `x-first-death-*`, and `x-last-death-*`).
Its `user_id` becomes the Executor's own user, because RabbitMQ refuses any other, while its
`x-kicak-original-publisher` header keeps naming the agent that created the message (see
[Who published a message](#who-published-a-message)), and its `x-kicak-published-at` header
becomes the time of the move, because the copy is published anew (see
[When a message was published](#when-a-message-was-published)). It also gets two headers that count
the rounds:

| Header | Value |
| --- | --- |
| `x-kicak-retry-round` | 1 for the first move to the retry queue, then 2, 3, and so on |
| `x-kicak-retrying-since` | when the message was first moved there, in ISO 8601 and UTC |

Every move is logged at `WARNING`, for example:

```text
Message moved to retry queue after 3 failed processing attempts id=doc-42 queue=classifier
retry_queue=classifier.retry retry_round=37 retrying_since=2026-10-19T08:00:00+00:00
retry_in=10.0s: summarizer unavailable
```

**Finding a message that never succeeds.** Alert on these warnings: the same `id` with a high
`retry_round`, or a `retrying_since` long ago, is a message that keeps failing. A growing number of
messages in `<queue>.retry` shows a failure that affects many. RabbitMQ's management UI shows the
headers of the messages waiting there; use "Get messages" with "Nack message requeue true" to look
at them without removing them.

**Taking such a message out.** RabbitMQ can only take messages from the front of a queue, not one
chosen by its ID, and the message spends part of its time elsewhere. Taking messages from the retry
queue by hand also removes the ones in front of it, and rejecting one there sends it back to the
input queue. The safe way is to make `execute()` raise `MalformedMessageError` for it, for example
for the IDs on a list your application reads: the next time the message comes round, it is
dead-lettered to `<queue>.dlq`, where it is kept for inspection or replay. Fixing the cause, such as
a service that is down, ends the rounds by itself.

**The way back.** The default exchange cannot be deleted, so the way back depends only on the input
queue. RabbitMQ dead-letters the expired messages of a classic queue at most once: a message that
expires while the input queue does not exist is dropped. That takes someone deleting the input
queue, which destroys every message waiting in it anyway, and the Executor declares the queue again
within about a second. A quorum retry queue is declared to dead-letter at least once
(`x-dead-letter-strategy` `at-least-once`, which requires `x-overflow` `reject-publish`), so it
keeps such a message until the input queue is back.

**Changing the delay.** The delay is set on each message, not as an argument of the queue, so
changing `retry_delay_seconds` takes only a restart. RabbitMQ expires only the message at the front
of a queue, so while messages with the old delay are still waiting, one with a shorter new delay can
wait behind them, for at most the old delay.

**When the copy cannot be placed.** If RabbitMQ returns the copy as unroutable, because the retry
queue was deleted right after the Executor declared it, or refuses it, the Executor requeues the
message instead and logs an `ERROR`; RabbitMQ then delivers it again at once. RabbitMQ refusing to
declare the retry queue, for example with `406 PRECONDITION_FAILED` because a queue of that name
exists with other arguments, stops the Executor.

### Dead-letter queue

A dead-lettered message is kept in `<queue>.dlq` together with the reason it is there, so it can be
looked into without searching the logs:

```text
classifier ──► Executor ── malformed, a bug, or a result too large ──► classifier.dlx ──► classifier.dlq
```

1. The Executor publishes a copy of the message to the dead-letter exchange `<queue>.dlx`, right
   after declaring the dead-letter route again, and acknowledges the original only after RabbitMQ
   has confirmed the copy.
2. Every queue bound to `<queue>.dlx` receives the copy: `<queue>.dlq`, and any queue you bind to
   it yourself, for example one that raises alerts.

The copy keeps the body, the `message_id` property exactly as the message arrived, so it is found by
the same ID, and the other properties, its `type` included, so it can be replayed as the same version
of the message. It keeps the message's headers, its retry headers `x-kicak-retry-round` and
`x-kicak-retrying-since` and its `x-kicak-published-at` included, as its history, except the
headers RabbitMQ manages itself. It never expires. Its `user_id` becomes the Executor's own user, while its `x-kicak-original-publisher`
header keeps naming the agent that created the message (see
[Who published a message](#who-published-a-message)). It also gets headers that say why, when, and
from where:

| Header | Value |
| --- | --- |
| `x-kicak-dead-letter-reason` | `malformed`: a `MalformedMessageError`, raised by `execute()`, or by the Executor for a message it cannot decode or whose results are invalid; `unexpected-error`: any other exception, treated as a bug, which also stopped the Executor; `result-too-large`: RabbitMQ refused one of its results as larger than its `max_message_size` |
| `x-kicak-error-type` | the class of the exception with its module, e.g. `pykicak.executor.MalformedMessageError` or `myapp.errors.InvoiceError`; a built-in one without it, e.g. `KeyError` |
| `x-kicak-error-message` | the text of the exception, at most 1024 bytes of UTF-8; a longer one is cut and ends with `…` |
| `x-kicak-dead-lettered-at` | when the message was dead-lettered, in ISO 8601 and UTC |
| `x-kicak-dead-lettered-from` | the queue it was taken from |

The error text travels with the message: whoever can read the dead-letter queue, for example in
RabbitMQ's management UI, can read it. The library's own texts hold no values from the message, but
the texts of your exceptions, and of the libraries you call, hold whatever they were given. Keep
secrets out of them, as out of your logs, where the Executor writes the same error.

**Messages RabbitMQ dead-letters itself.** A quorum queue dead-letters a message that reached its
delivery limit (see [Queue type](#queue-type)), and a queue with a length limit or a message TTL set
by a policy dead-letters the messages it drops. RabbitMQ routes them through the same
`<queue>.dlx`, with its own `x-death` header recording the queue and the reason, for example
`delivery_limit`, and without the headers above. The Executor's copies have no `x-death`, because
RabbitMQ did not dead-letter them. So read `x-kicak-dead-letter-reason` first, and `x-death` when
it is missing.

**A message can be both in the dead-letter queue and processed.** If the connection is lost, or
RabbitMQ's `consumer_timeout` takes the message back, after RabbitMQ has confirmed the copy but
before the Executor acknowledges the original, RabbitMQ redelivers the original, which is executed
again. It may then be dead-lettered again, leaving two copies, or succeed, for example once a bug is
fixed. Replaying a message from the dead-letter queue is safe because agents are idempotent (see
[Requirement: idempotent processing](#requirement-idempotent-processing)). A replayed message still
carries the headers above; the Executor drops them from every copy it makes, so a later move to the
retry queue or the dead-letter queue never shows a stale reason.

**When the copy cannot be placed.** If RabbitMQ returns the copy as unroutable, because the
dead-letter queue or its binding was deleted right after the Executor declared them, or refuses it,
for example because the dead-letter queue is full and refuses new messages, the Executor requeues the
message instead and logs an `ERROR`; RabbitMQ then delivers it again at once. Rejecting it for
RabbitMQ to dead-letter would not help: RabbitMQ dead-letters a rejected message at most once and
drops it, telling nobody, when no queue takes it. RabbitMQ refusing to declare the dead-letter route,
or refusing the copy in another way, for example with `403 ACCESS_REFUSED`, stops the Executor.
After an unexpected failure, the Executor stops either way, and raises that failure, not the error
of the copy.

### Every outcome of a delivery

The Executor decides the outcome of every delivery from how `execute()` ends, and from what RabbitMQ
reports while the results are published and the message is settled:

| Outcome | Cause | Delivery is… | Executor |
| --- | --- | --- | --- |
| Success | `execute()` returns | results published in one transaction, then acknowledged | continues |
| Deterministic failure | a `type` property that is missing or names a type the Executor does not accept, missing body, undecodable body (not JSON, `NaN` and `Infinity` included, a number too large for a float, nested too deeply to decode, not a JSON object, missing or unexpected fields, no valid `message_id`, refused by the message class's `__post_init__` with any exception), `MalformedMessageError` raised by `execute()`, or results returned by `execute()` that are not `KicakMessage`s or cannot be serialized | dead-lettered: copied to the dead-letter exchange with headers saying why, and acknowledged once RabbitMQ has confirmed the copy; if RabbitMQ does not take the copy (returns it as unroutable, or refuses it), requeued with `basic_nack(requeue=True)` and logged at `ERROR` (see [Dead-letter queue](#dead-letter-queue)) | continues; RabbitMQ refusing to declare the dead-letter route stops it |
| Transient failure | `TransientProcessingError` raised by `execute()` | retried in-process; if every attempt fails, copied to the retry queue and acknowledged once RabbitMQ has confirmed the copy; if RabbitMQ does not take the copy (returns it as unroutable, or refuses it), requeued with `basic_nack(requeue=True)` and logged at `ERROR` | continues; RabbitMQ refusing to declare the retry queue stops it |
| Unexpected failure | any other exception from `execute()`, results of another class than the declared result type, or results returned by a terminator | dead-lettered as above | logs the error and raises it, stopping |
| Results returned as unroutable | `UnroutableError`: no queue was bound to the destination exchange when the results were committed, although the output side was just verified | held, unacknowledged, with its results; the message is not at fault | logs an `ERROR`, waits (0.5 seconds, doubling up to 30, with jitter), verifies the output side, and publishes the results again, without executing the message again (see [Unroutable results are held](#unroutable-results-are-held)) |
| Result too large | RabbitMQ refuses a result larger than its `max_message_size` (16 MiB by default in RabbitMQ 4) | dead-lettered as above; RabbitMQ discards the whole transaction, so none of its results is published | continues |
| Results not committed | another error while verifying the output side, publishing, or committing the results, e.g. RabbitMQ refusing a declaration (`406`, `403`) or the commit | requeued by RabbitMQ when the connection closes | logs the error and raises it, stopping |
| Connection lost while publishing results | a transient connection error while verifying the output side, before or during the commit, or while the results are held, or RabbitMQ having already taken the message back because it outlasted `consumer_timeout` (closing its channel, or cancelling the consumer on RabbitMQ 4.3 and newer) | already requeued by RabbitMQ when the connection or channel closed; uncommitted results are discarded by RabbitMQ, not re-sent | reconnects and receives the message again |
| Connection lost while settling | a transient connection error from the acknowledgement or a requeue, or from moving the message to the dead-letter queue or the retry queue, the declarations before them included, or RabbitMQ taking the message back because it outlasted `consumer_timeout` (closing its channel, or cancelling the consumer on RabbitMQ 4.3 and newer, which then ignores the settlement) | requeued by RabbitMQ when the connection or channel closed, instead of being acknowledged, requeued, or dead-lettered | reconnects and receives the message again; after an unexpected failure, still raises that error and stops |
| Input side not verified | between messages: a transient connection error, or RabbitMQ refusing a declaration of the input side | no message is held | reconnects; after a refusal, logs the error and raises it, stopping |

Results returned by a terminator, whose `execute()` must return `None`, are an unexpected failure
too, and so are results of another class than the Executor declares in its class header: the code
contradicts its own declaration, so the message is dead-lettered and the Executor stops.

Decoding is different: a received message of a type the Executor does not accept, or that its
class's `__post_init__` refuses with whatever exception, is at fault itself, whoever produced it, so
it is dead-lettered as malformed and the Executor continues. The declared types themselves are
checked when the Executor is created, so a broken declaration raises before `run()` takes any
message.

The same goes for what `execute()` returns, which can depend on data from outside: a value that is
not a message or a sequence of messages, or a result that cannot be serialized (for example a field
holding a `datetime` or `Decimal` from an external service), dead-letters the input as malformed,
and the Executor continues. None of that input's results is published.

`execute()` is called up to `max_processing_attempts` times (default 3) while it raises
`TransientProcessingError`, with 0.5- and 1.0-second delays that double up to a 30-second cap, and
the message then goes to the retry queue (see [Retry queue](#retry-queue)).

Every dead-lettered message is logged at `ERROR`, and its copy in `<queue>.dlq` keeps its original
body and says why it is there (see [Dead-letter queue](#dead-letter-queue)).

### Unroutable results are held

The Executor verifies its output side right before publishing, so RabbitMQ returns its results as
unroutable only if every binding of the destination exchange disappeared in the few milliseconds
before the commit. None of the results was delivered then, and the Executor holds them:

- The input stays unacknowledged, and its results stay in memory; `execute()` is not called again.
- After a wait of 0.5 seconds, doubling up to 30, with jitter, the Executor verifies the output side
  again, which binds the queues again, and publishes the results. The wait starts over for the next
  message. During it, the connection keeps answering heartbeats.
- The status is `"DOWNSTREAM_RECOVER"` until the results are committed (see
  [Status information](#status-information)).
- `stop()` ends the hold within about a second: the Executor requeues the input with
  `basic_nack(requeue=True)`, so it is executed again later, and `run()` returns.
- A lost connection, or RabbitMQ's `consumer_timeout` (30 minutes by default) expiring, also ends
  it: RabbitMQ requeues the input, and the Executor reconnects and executes it again.

On a quorum queue, every requeue counts toward the delivery limit (see [Queue type](#queue-type)).

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
  is dropped. The Executor therefore acknowledges after the commit; if RabbitMQ returned the results
  as unroutable, it holds the input and its results and tries again after a wait (see
  [Unroutable results are held](#unroutable-results-are-held)).
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
  results are published, and the message is acknowledged (or dead-lettered, or moved to the retry
  queue, as usual).
  `run()` then returns instead of waiting for the next message.
- **While results are held** because RabbitMQ returned them as unroutable, the Executor requeues the
  message instead of waiting for the downstream side, within about a second, and returns; the
  message is executed again later.
- **While waiting for a message,** `run()` returns within about a second, without taking one.
- **While reconnecting after a lost connection,** `run()` stops retrying and returns within about a
  second.
- **A message delivered after the stop request,** before the Executor notices it, is not processed;
  RabbitMQ returns it to the queue when the connection closes.

`run()` then closes the connection and returns, with the status `"STOPPED"`. If closing the
connection fails, the failure is logged at `ERROR`, and `run()` still returns normally: no message
depends on it, because RabbitMQ requeues an unsettled message once the connection is gone.

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
least the longest processing of one message, retries of `execute()` included, plus a few seconds;
otherwise it kills the process in the middle of a message.

### Other ways `run()` ends

Without a stop request, `run()` ends only by raising:

- a failure, as described above;
- a `BaseException` such as `SystemExit` or `KeyboardInterrupt`, for example Python's default
  reaction to Ctrl+C when no signal handler calls `stop()`. This is a hard stop: the exception
  passes through unchanged, even in the middle of `execute()`, the connection is closed, and RabbitMQ
  requeues the message being processed, which is then executed again.

### Status information

An Executor reports what it is doing, for example to a health check or a monitoring thread.
`get_info()` returns an `ExecutorInfo`, a frozen snapshot whose fields all describe the same
moment:

| Field | Meaning |
| --- | --- |
| `status` | What the Executor is doing: an `ExecutorStatus` member (see the table below). |
| `status_changed_at` | When the status was entered. |
| `status_checked_at` | When this snapshot was taken. |
| `stop_requested` | Whether `stop()` has been called; it stays `True` after the stop. |
| `stop_requested_at` | When `stop()` was first called, or `None`. |
| `message` | The input message being processed, or `None`. |
| `attempt` | Which call of `execute()` for `message` is under way, from 1 to `max_processing_attempts`, or `None`. |
| `redelivered` | Whether RabbitMQ delivered `message` before without it being settled, or `None`. |
| `retry_round` | How often `message` was moved to the retry queue before, 0 if never, or `None`. |
| `counts` | How the deliveries the Executor took so far have ended: an `ExecutorCounts`. |

The times are timezone-aware `datetime` objects in UTC, so a monitor can compare and subtract them
directly, for example to alert when a recovery lasts too long. For text, such as a REST endpoint's
JSON, use `.isoformat()`, which gives for example `"2026-10-01T17:05:42.123456+00:00"`.

| `status` | Meaning |
| --- | --- |
| `"CREATED"` | The Executor was created; `run()` has not consumed yet. It stays so during the first connection attempt. |
| `"UPSTREAM_RECOVER"` | The connection was lost, connecting failed, or the input side could not be verified; the Executor is reconnecting. Also after RabbitMQ cancelled the consumer, and from a `run()` called again after `"CRASHED"`. |
| `"WAITING_FOR_MESSAGE"` | The input side is verified; the Executor waits for the next message. |
| `"PROCESSING"` | A message is decoded and executed, retries of `execute()` included. A message without results is also acknowledged, dead-lettered, or moved to the retry queue in this status. |
| `"DOWNSTREAM_RECOVER"` | RabbitMQ returned the results as unroutable; the Executor holds the message and its results and tries again (see [Unroutable results are held](#unroutable-results-are-held)). |
| `"SENDING_RESULT"` | The output side is verified; the results are published and committed, then the message is acknowledged. |
| `"STOPPING"` | `stop()` was called; `run()` finishes the message in progress, then returns. It overrides the statuses above. |
| `"STOPPED"` | `run()` returned after a graceful stop. |
| `"CRASHED"` | `run()` ended by raising an exception: a failure or a hard stop. |

For each message the status goes from `"WAITING_FOR_MESSAGE"` to `"PROCESSING"`, then
`"SENDING_RESULT"` if it has results, and back. The first attempt to connect or to verify the
topology keeps the current status; only a failed attempt enters a recovery status, and the
Executor logs a `WARNING` when it enters one and an `INFO` when it leaves it, which tells how long
it lasted (see [Durations in the logs](#durations-in-the-logs)). A lost connection is
`"UPSTREAM_RECOVER"` even when it is noticed while sending results, because the message is executed
again after reconnecting. The recovery statuses last however long the recovery takes (see
[Retries and logging](#retries-and-logging)).

`ExecutorStatus` is a string enum, so `info.status == "STOPPED"` and
`info.status == ExecutorStatus.STOPPED` both work, and `json.dumps()` writes the plain string.
`"STOPPED"` and `"CRASHED"` are final: a later `stop()` does not change them. Calling `run()` again
after `"CRASHED"`, without a stop request, starts again in `"UPSTREAM_RECOVER"`.

`message` is set from the moment a delivery is decoded until it is acknowledged, moved to the retry
queue, requeued, or dead-lettered, or the connection is lost. It is `None` while the Executor waits
or recovers, for a delivery that cannot be decoded, and once `run()` has ended. During `"STOPPING"`
it shows the message being finished, if any. It is the same object `execute()` receives, so treat it
as read-only: lists and dicts inside it are shared with `execute()`. The snapshot's `repr()` shows
the message only by its type and `message_id`, never its contents, so the snapshot can be logged.

`attempt`, `redelivered`, and `retry_round` describe the delivery of `message`, and are `None`
exactly when it is:

- `attempt` counts the calls of `execute()` for this delivery: it grows while `execute()` raises
  `TransientProcessingError`, and starts at 1 again for every delivery, also when the message is
  delivered again or comes back from the retry queue. A long `"PROCESSING"` with `attempt` 3 of 3
  is a message whose last attempt is under way.
- `redelivered` is RabbitMQ's redelivered flag: `True` when the message was delivered before and
  not settled, after a lost connection, a crash, a requeue, or an expired `consumer_timeout`. When
  instances keep crashing or losing their connection, a message that keeps arriving redelivered
  may be the cause. A message back from the retry queue is a new delivery, so it is not
  redelivered.
- `retry_round` is the message's `x-kicak-retry-round` header (see [Retry queue](#retry-queue)):
  how often it was moved to the retry queue before, 0 for a message that never was.

`counts` tells how every delivery the Executor started to process has ended. Each one counts
once, when it is settled or left to RabbitMQ:

| `counts.` | Deliveries that were… |
| --- | --- |
| `completed` | executed, their results published, if any, and acknowledged |
| `dead_lettered.malformed` | dead-lettered for a `MalformedMessageError` (see [Dead-letter queue](#dead-letter-queue)) |
| `dead_lettered.result_too_large` | dead-lettered because RabbitMQ refused one of their results as too large |
| `dead_lettered.unexpected_error` | dead-lettered for any other exception, each of which also stopped the Executor |
| `dead_lettered.total` | dead-lettered, for any of the three reasons |
| `moved_to_retry` | moved to the retry queue after `TransientProcessingError` persisted |
| `requeued` | returned to the queue by the Executor: RabbitMQ did not take their copy for the retry queue or the dead-letter queue, or `stop()` ended a hold of their unroutable results |
| `abandoned` | left to RabbitMQ, which delivers them again: the connection was lost, `consumer_timeout` expired, or `run()` ended by raising an exception while processing them |

The counts start at zero when the Executor is created and only grow, also across a `run()` called
again after `"CRASHED"`, so they map directly onto monitoring counters, such as Prometheus counters
read on every scrape; like those, they start again with the process. They count deliveries, not
messages: a message delivered twice counts twice, and every count describes this instance only.
A growing `abandoned` usually means that processing outlasts `heartbeat_seconds` or
`consumer_timeout`, so messages are executed again (see
[Long-running `execute()`](#long-running-execute)). A delivery received after `stop()` is not
processed, and not counted. How long the deliveries took is in the logs: see
[Durations in the logs](#durations-in-the-logs).

```python
from datetime import timedelta

info = executor.get_info()
info.status  # e.g. "PROCESSING"
info.status_changed_at  # e.g. datetime(2026, 10, 1, 17, 5, 42, 123456, tzinfo=UTC)
info.status_checked_at  # e.g. datetime(2026, 10, 1, 17, 5, 44, 987654, tzinfo=UTC)
info.stop_requested  # False
info.message  # e.g. MyMessage(message_id="...", ...), or None
info.attempt  # e.g. 2: the second call of execute() for it
info.counts.completed  # e.g. 1250
info.counts.dead_lettered.total  # e.g. 3

recovering = info.status in ("UPSTREAM_RECOVER", "DOWNSTREAM_RECOVER")
if recovering and info.status_checked_at - info.status_changed_at > timedelta(minutes=5):
    ...  # alert: the Executor has been recovering for more than 5 minutes
```

`get_info()` is safe to call from any thread while `run()` is running. It neither locks nor changes
any state, and it does not use the RabbitMQ connection.

## Long-running `execute()`

RabbitMQ uses heartbeats to detect dead connections, and pika can answer them only between messages,
never while one is being processed: not while `execute()` runs, and not during the waits between the
calls that `TransientProcessingError` retries. If processing one message lasts longer than the
connection's heartbeat timeout, RabbitMQ closes the connection and requeues the message while it is
still being processed. The work is wasted and the message is executed again, so processing that
always takes that long never completes. An Executor that publishes results discards them, and a
terminator's acknowledgement fails on the closed connection; either way it reconnects and receives
the message again.

Only the application knows how long its processing takes, so each Executor type sets its own
timeout with `heartbeat_seconds`, in seconds. It defaults to 600 (10 minutes), sized for LLM calls
and heavy computation, and RabbitMQ uses the requested value even when its own default is lower.
Choose a value comfortably above the longest processing of one message you expect for that Executor
type. If `execute()` can raise `TransientProcessingError` late in its work, that is up to
`max_processing_attempts` calls of `execute()`, plus the waits between them (0.5 seconds, then
doubling up to 30 seconds):

```python
executor = ClassifyingExecutor(
    config,
    source_queue="classifier",
    source_exchange="items.inbox",
    destination_exchange="items.classified",
    destination_queues=["db-writer", "quality-checker"],
    heartbeat_seconds=1800,  # this classifier can take up to 20 minutes per item, retries included
)
```

Consider these when choosing the value:

- **A larger value detects dead connections later.** If an Executor's host or network disappears
  without closing the connection, RabbitMQ redelivers its in-flight message only after up to
  `heartbeat_seconds`.
- **RabbitMQ has a separate limit on unacknowledged messages.** The broker's `consumer_timeout`
  (30 minutes by default) takes back a message that a consumer holds unacknowledged for longer, and
  requeues it. RabbitMQ 4.2 closes the consumer's channel, with `406 PRECONDITION_FAILED`, for
  classic and quorum queues alike. RabbitMQ 4.3 and newer evaluate the timeout for quorum queues
  only, and cancel the consumer, keeping its channel open; classic queues no longer have a consumer
  timeout. Either way the Executor reacts as to an expired heartbeat: it does not commit the
  results of that message or move it to the retry queue or the dead-letter queue, logs a
  `WARNING`, reconnects, and executes the message again. Only if RabbitMQ takes the message back
  while the results are being committed are they published, and then published again after the
  redelivery. If processing can
  take longer than `consumer_timeout`, raise `consumer_timeout` on the broker as well;
  `heartbeat_seconds` alone is not enough.
- **On RabbitMQ 4.3 and newer, the Executor relies on an internal pika hook** to notice the
  cancellation while it processes the message, because pika reports a cancelled consumer of its
  `consume()` generator only later. If a pika version lacks the hook, the Executor logs a
  `WARNING` whenever it starts consuming; a timed-out message is then still executed again, but
  its results or its copy for the retry queue or the dead-letter queue may be published twice.
- AMQP limits the value to 1–65535 seconds, and it must be an `int`. `None` raises `TypeError`:
  it would accept the broker's proposal, 60 seconds by default, which a long `execute()` outlasts.
  Injectors keep the broker's default: they hold a connection only while publishing one message, so
  heartbeats do not limit them.

## Scaling an Executor

To process a queue faster, run several instances of the same Executor. Give every instance the
same configuration (`source_queue`, `source_exchange`, `destination_exchange`,
`destination_queues`, `queue_type`, `max_processing_attempts`, `heartbeat_seconds`,
`input_check_seconds`, and `retry_delay_seconds`), and start them as independent processes:

```text
                                      ┌──► classifier instance 1 ──┐
items.inbox ──► classifier (queue) ───┼──► classifier instance 2 ──┼──► items.classified
                                      └──► classifier instance 3 ──┘
```

- All instances declare the same queues, bindings, dead-letter route, and retry queue; RabbitMQ
  treats identical declarations as no-ops. An instance configured differently, for example with
  another `queue_type`, fails to connect with `406 PRECONDITION_FAILED`.
- RabbitMQ delivers each message to one instance only. With the prefetch count of 1, each instance
  holds only the message it is processing, so the next message goes to whichever instance is free.
- Each instance acknowledges, dead-letters, and moves to the retry queue its own deliveries; no
  coordination is needed. A message coming back from the retry queue, or requeued, may be delivered
  to any instance.
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
- **Processing longer than `heartbeat_seconds` or `consumer_timeout` causes such redeliveries.**
  RabbitMQ closes the connection or channel, or cancels the consumer, and redelivers the message
  while it is still being processed (see [Long-running `execute()`](#long-running-execute)).
  With several instances, another instance picks it up immediately.
- **An unexpected error stops only the instance that hit it.** Each instance dead-letters the
  message and stops on its own, so a bug that fails every message dead-letters one message per
  instance before all have stopped.
- **A failure that requeues the message and stops reaches every instance.** A refused commit, or a
  refused declaration of the output side (see
  [Error handling and dead-lettering](#error-handling-and-dead-lettering)), returns the input to
  the queue before the instance stops, so the next instance receives the same message and stops
  too, until none is left running. With a classic queue, a commit that RabbitMQ always refuses,
  for example because the Executor may not write to its destination exchange, also stops every
  instance started later; a quorum queue dead-letters the message once it reaches its delivery
  limit (see [Queue type](#queue-type)). A result that is too large is the exception: only its input
  is dead-lettered, and the Executor continues. A refused declaration of the input side holds no
  message, and stops each instance when it verifies its input side.

## Delivery guarantees

Every message is published as mandatory and persistent (`delivery_mode=2`), with the content type
`application/json`, its `message_id` as the AMQP `message_id` property, its class's `kicak_type` as
the AMQP `type` property (see
[Type names: the contract between agents](#type-names-the-contract-between-agents)), the names
of its publisher and its creator (see [Who published a message](#who-published-a-message)), and
the time it was published (see [When a message was published](#when-a-message-was-published)). The
Injector publishes in publisher-confirm mode: a publish returns only after RabbitMQ has taken
responsibility for the message, for persistent messages in durable queues after writing it to disk.
The Executor publishes its results in a transaction (see
[Results are published in a transaction](#results-are-published-in-a-transaction)): the commit
returns only after RabbitMQ has taken responsibility for all of them. RabbitMQ refusing a message
raises one of these errors, which are not retried:

- `pika.exceptions.UnroutableError`: no queue is bound to the exchange, so RabbitMQ would have
  discarded the message. For an Injector this means the first Executor's topology is not installed.
- `pika.exceptions.NackError` (Injector): RabbitMQ could not take responsibility for the message.
  For an Executor's results, RabbitMQ refuses the commit instead, which closes the results channel.

An Injector raises `InjectionError` to its caller, with one of these errors as its `__cause__`. An
Executor keeps the input message: after unroutable results it holds them and tries again (see
[Unroutable results are held](#unroutable-results-are-held)); after a refused commit, closing the
connection requeues the input and the Executor stops (see
[Error handling and dead-lettering](#error-handling-and-dead-lettering)). So no message is lost
between two stages of a pipeline. A result larger than RabbitMQ's `max_message_size` is the
exception: its input is dead-lettered, and the Executor continues.

A mandatory message counts as routed if at least one queue is bound to the exchange, so RabbitMQ
does not report a fanout exchange that is missing only some of its queues. An Executor therefore
binds every one of its `destination_queues` right before it publishes, which leaves only the few
milliseconds before the commit (see [Topology verification](#topology-verification)); and only one
queue may be bound to an Injector's exchange, so a missing binding there always fails the
injection.

### Who published a message

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

### When a message was published

Every message a pykicak agent publishes, an Injector's message and an Executor's result alike,
carries the time it was published in its `x-kicak-published-at` header, in ISO 8601 and UTC, e.g.
`2026-10-01T17:05:42.123456+00:00`, taken from the publishing host's clock. An Injector stamps each
attempt anew, so a retried injection carries the time of the attempt that published it, and an
Executor stamps its results when it publishes them, not when their input arrived. A copy moved to
the retry queue is published anew too, so it gets the time of the move, while a dead-letter copy
keeps the time of the message it copies (see [Dead-letter queue](#dead-letter-queue)).

The Executor that receives the message compares that time with its own clock to tell how long the
message waited in its queue, `queued_seconds` in its log (see
[Durations in the logs](#durations-in-the-logs)). The header is not verified by RabbitMQ, like
`x-kicak-original-publisher`, and a message that another client published without it has no
`queued_seconds`.

## Retries and logging

Injectors retry transient RabbitMQ connection and publish failures up to three attempts, with
1- and 2-second delays between attempts; each retry publishes the message again on a new
connection, and after the third failed attempt the error is raised. Executors reconnect
indefinitely after transient failures, using exponential full jitter with a 0.5-second initial
delay and a 30-second maximum, but never retry a result publish (see
[Error handling and dead-lettering](#error-handling-and-dead-lettering)). Authentication and
access-denied failures, and a broker certificate that fails verification (see [TLS](#tls)), are not
retried.
Application processing is retried only for `TransientProcessingError`, first in place and then
through the retry queue (see [Retry queue](#retry-queue)). Message acknowledgements
are not retried: an acknowledgement is valid only on the connection that delivered the message, so
when the connection is lost while settling a message, RabbitMQ requeues it and the Executor
reconnects and receives it again.

An Executor cannot tell a broker that is down from a connection setting that will never work, so it
also retries indefinitely when the host name does not resolve, nothing listens on the port, or the
port does not speak what `RABBIT_MQ_TLS` selects (for example the TLS port 5671 with
`RABBIT_MQ_TLS=false`, the plain port 5672 with `true`, or the management port). Its status is
`"UPSTREAM_RECOVER"` meanwhile (`"CREATED"` during the very first attempt), and it logs a `WARNING`
for every failed attempt; alert on a status that stays `"UPSTREAM_RECOVER"`, on those warnings, or
on the queue having no consumer.

An Injector's connection fails with `ConnectionBlockedTimeout` after RabbitMQ has blocked it for
`blocked_timeout_seconds` (60 by default) because of a memory or disk alarm, and the attempt is
retried like other connection errors.
An Executor's connection stays blocked until the alarm clears, so it does not execute its message
again in the meantime.

Publishing is at-least-once: if the connection is lost before RabbitMQ's confirmation or the reply
to a commit arrives, RabbitMQ may already have accepted the messages. The Injector then publishes
its message again, and the Executor executes the redelivered input again. pykicak does not provide
exactly-once delivery, which is why it requires idempotent agents (see
[Requirement: idempotent processing](#requirement-idempotent-processing)).

The library emits standard Python logging records through module loggers. Applications configure
handlers and levels, for example:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

### Durations in the logs

Every delivery an Executor takes ends with one `INFO` line, whatever its outcome, telling how long
each part of its handling took, so that statistics such as averages and percentiles can be
computed outside the library:

```text
Message handled outcome=completed reason=None type=Chunk id=doc-17 queue=chunker delivery_tag=42 redelivered=False retry_round=0 attempts=1 results=3 total_seconds=1.284 execute_seconds=1.201 backoff_seconds=0.000 publish_seconds=0.071 held_seconds=0.000 settle_seconds=0.001 queued_seconds=0.350
```

| Key | Value |
| --- | --- |
| `outcome` | how the delivery ended, named like the field of [`counts`](#status-information) that counts it: `completed`, `dead_lettered`, `moved_to_retry`, `requeued`, or `abandoned` |
| `reason` | for `dead_lettered`, its `x-kicak-dead-letter-reason` (`malformed`, `result-too-large`, or `unexpected-error`); otherwise `None` |
| `type`, `id` | the message's class and `message_id`; for a delivery that could not be decoded, the accepted class its `type` property names and its sender's `message_id` property, each `None` if there is none |
| `queue`, `delivery_tag` | the source queue, and the delivery's tag on the consuming channel |
| `redelivered`, `retry_round` | RabbitMQ's redelivered flag, and how often the message was moved to the retry queue before (see [Status information](#status-information)) |
| `attempts` | the calls of `execute()`; 0 for a delivery that could not be decoded |
| `results` | the results `execute()` returned, once validated and serialized; they are published only when the outcome is `completed` |
| `total_seconds` | from taking the delivery until it was settled or left to RabbitMQ |
| `execute_seconds` | in `execute()`, all its calls together |
| `backoff_seconds` | waiting between the calls retried after `TransientProcessingError` |
| `publish_seconds` | verifying the output side, publishing, and committing the results, all attempts together |
| `held_seconds` | holding results RabbitMQ returned as unroutable (see [Unroutable results are held](#unroutable-results-are-held)) |
| `settle_seconds` | acknowledging or requeueing the delivery, including publishing its copy for the retry queue or the dead-letter queue and waiting for RabbitMQ's confirmation |
| `queued_seconds` | how long the delivery waited since it was published, before the Executor took it: from its `x-kicak-published-at` header (see [When a message was published](#when-a-message-was-published)) to the Executor's clock when it took the delivery; `None` without a valid header |

The parts do not overlap, and what `total_seconds` has beyond them is decoding the delivery,
serializing the results, and logging. Durations are seconds, three decimals in the text, measured
with a monotonic clock (`time.perf_counter()`), so setting the system clock does not affect them.

`queued_seconds` is the exception: it comes before the delivery was taken, outside
`total_seconds`, and compares the clocks of two hosts, the publisher's and the Executor's. Keep them
synchronized, e.g. with NTP; a difference between them shows in it as it is, not corrected, and can
make it negative, so do not clamp it, which would bias averages. For a message back from the retry
queue (`retry_round` above 0), it includes the time it spent there, `retry_delay_seconds`, because
the copy was published when it was moved; leave such deliveries out to measure queue waits alone. A
message replayed from the dead-letter queue keeps the time it was published before it was
dead-lettered, so its `queued_seconds` includes its time in the dead-letter queue.

The record also carries the same values, typed (numbers, booleans, and `None`), as its `kicak`
attribute: a dict with one more key, `event`, here `"message_handled"`. Log handlers that write
records as JSON, such as python-json-logger's, include it, so nothing has to parse the text, and a
handler of your own can feed a monitoring system:

```python
import logging

from prometheus_client import Histogram

EXECUTE_SECONDS = Histogram("kicak_execute_seconds", "Time in execute()", ["queue"])


class ExecuteTimes(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        values = getattr(record, "kicak", None)
        if values and values["event"] == "message_handled" and values["outcome"] == "completed":
            EXECUTE_SECONDS.labels(queue=values["queue"]).observe(values["execute_seconds"])


executor_logger = logging.getLogger("pykicak.executor")
executor_logger.setLevel(logging.INFO)  # the lines are logged at INFO
executor_logger.addHandler(ExecuteTimes())
```

The lines describe deliveries, not messages. Delivery is at-least-once, so a message executed twice
has two lines, the second usually `redelivered=True`; group them by `id` for figures per message.
There is exactly one line for every delivery `get_info().counts` counts, so a delivery received
after `stop()`, which is not processed, has none. The `total_seconds` of an `abandoned` delivery is
work that is done again; for how long processing takes, average over `outcome=completed`, because
malformed messages usually fail fast.

Two more `INFO` lines carry a duration, also in their `kicak` attribute:

```text
Executor for queue chunker recovered from UPSTREAM_RECOVER: WAITING_FOR_MESSAGE recovery_seconds=12.345
Executor for queue chunker stopped on request stop_seconds=4.210
```

- `recovery_seconds` is how long the recovery status lasted, from the failure that entered it to
  the recovery (`event` `"recovered"`, with `queue`, `recovered_from`, `status`, and
  `recovery_seconds`).
- `stop_seconds` is how long a graceful stop took, from the first `stop()` until the connection was
  closed, finishing the message in progress included; the line says `stopped on request while
  reconnecting` when the stop ended a reconnection (`event` `"stopped"`, with `queue` and
  `stop_seconds`). It helps size the time a platform gives the process to stop, such as Kubernetes'
  `terminationGracePeriodSeconds`.

### Connection names

Every connection an agent opens is named after the agent's role, the queue or exchange it serves,
its host, and its process ID. RabbitMQ shows the name in its management UI and HTTP API
(`user_provided_name`), next to the connection's address:

```text
injector exchange=items.inbox host=loader-1 pid=812
executor queue=classifier host=worker-3 pid=4242
terminator queue=db-writer host=worker-5 pid=97
```

A terminator is an Executor declared with the result type `None`, which has no
`destination_exchange`. With several instances of an Executor, the host and process ID tell them
apart, for example to find the instance that holds a message unacknowledged or keeps reconnecting.

## Public API

`pykicak` exports `DeadLetterCounts`, `ExecutorCounts`, `ExecutorInfo`, `ExecutorStatus`,
`InjectionError`, `KicakConfig`, `KicakConfigError`, `KicakExecutorAbstract`,
`KicakInjectorAbstract`, `KicakMessage`, `MalformedMessageError`, `QueueType`,
`TransientProcessingError`, and `__version__`. Build agents by
subclassing `KicakInjectorAbstract` or `KicakExecutorAbstract`; their shared base class is internal.

## Proof of concept

[`examples/poc/`](https://github.com/przemek-barczak/pykicak/blob/main/examples/poc) in the
repository contains a runnable example (examples are not part of the installed package): an injector
that publishes a heartbeat message, an executor that publishes it to a fan-out exchange, and two
terminator executors that each declare their own queue and bind it to that exchange. The fan-out
executor names both terminator queues in its `destination_queues`, so it declares and binds them too
before publishing. Each executor also declares the dead-letter exchange and queue of its input queue
(for example `poc.heartbeat.source.dlx` → `poc.heartbeat.source.dlq`) and its retry queue
(`poc.heartbeat.source.retry`). Start the fan-out executor before running the injector: until it has
declared its queue, the injector fails with `UnroutableError`. The terminators can start at any
time; their queues collect the messages meanwhile. The queue/exchange topology is defined once in
`examples/poc/messaging/` (`queues.py`, `exchanges.py`, `bindings.py`) and imported by every agent —
none of it lives in `.kicak`.

The PoC also shows [a new version of a message](#a-new-version-of-a-message) being introduced
while the flow keeps running. `examples/poc/messages.py` defines two versions of the heartbeat,
and each agent's class header declares which ones it publishes and accepts:

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

## Development

Install the package in editable mode with the development tools (pytest, pytest-cov, trustme,
cryptography, ruff, mypy, build, and twine), then run the test suite and the checks that CI runs:

```bash
pip install -e . --group dev   # pip 25.1 or newer
pytest --cov                   # fails below 85% statement coverage
ruff check .
ruff format --check .
mypy
```

The tools are pinned to exact versions in `[dependency-groups]` of `pyproject.toml`, so that every
run checks with the same ones; Dependabot proposes updates. `mypy` checks `pykicak` and `examples`
in strict mode, and the code in `tests` and `.github/scripts` too, without requiring annotations on
test functions and fixtures.

`pytest` excludes integration tests, which require a live RabbitMQ broker (e.g.
`docker run -p 5672:5672 rabbitmq`). They connect with the `.kicak` file in the working directory,
or the one whose path is in the `KICAK_CONFIG_PATH` environment variable; with the container above,
a copy of `.kicak.example` works as it is. Run them explicitly with:

```bash
pytest -m integration
```

Two of them check how an Executor handles RabbitMQ's `consumer_timeout` ending a delivery: on a
classic queue with RabbitMQ 4.2, and on a quorum queue with RabbitMQ 4.3 and newer, which evaluate
the timeout for quorum queues only. RabbitMQ 4.2 checks that timeout only once a minute by default,
so each of them takes about a minute there. A broker started with
`[{rabbit, [{channel_tick_interval, 1000}]}].` in `/etc/rabbitmq/advanced.config` checks every
second, and they take a few seconds; the CI integration job starts its brokers that way, with
RabbitMQ 4.2 and 4.4. `rabbitmq.conf` has no setting for this interval. On RabbitMQ 4.4 they take
about 15 seconds whatever the interval.

The TLS integration tests also need a broker with a TLS listener, and the path of a `.kicak` file
for it, with `RABBIT_MQ_TLS=true` and `RABBIT_MQ_CA_FILE`, in the `KICAK_TLS_CONFIG_PATH`
environment variable; without it they are skipped. `python -m tests.integration.tls_broker_files
<directory>` writes a test CA, a broker certificate for `localhost`, and the broker's TLS
configuration; the integration job in `.github/workflows/ci.yml` shows how to start RabbitMQ with
them.

### CI and releases

GitHub Actions runs these checks, the unit tests on every supported Python version and with the
oldest supported pika, and the integration tests against RabbitMQ, on every push to `main` and
every pull request.
Releases are published to PyPI from GitHub Actions alone, when a version tag is pushed and a
maintainer approves it. [`RELEASING.md`](https://github.com/przemek-barczak/pykicak/blob/main/RELEASING.md)
describes the workflows, how to release a version, and how to recover from a failed release.

## License

`pykicak` is released under the [MIT License](https://github.com/przemek-barczak/pykicak/blob/main/LICENSE).
