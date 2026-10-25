# Dead-letter queue

A dead-lettered message is kept in `<queue>.dlq` together with the reason it is there, so it can be
looked into without searching the logs:

```text
classifier ──► Executor ── malformed, retries used up, a bug, or a result too large ──► classifier.dlx ──► classifier.dlq
```

1. The Executor publishes a copy of the message to the dead-letter exchange `<queue>.dlx`, right
   after declaring the dead-letter route again, and acknowledges the original only after RabbitMQ
   has confirmed the copy.
2. Every queue bound to `<queue>.dlx` receives the copy: `<queue>.dlq`, and any queue you bind to
   it yourself, for example one that raises alerts.

The copy keeps the body, the `message_id` property exactly as the message arrived, so it is found by
the same ID, and the other properties, its `type` included, so it can be replayed as the same
version of the message. It keeps the message's headers, its retry headers `x-kicak-retry-round` and
`x-kicak-retrying-since` and its `x-kicak-published-at` included, as its history, except the headers
RabbitMQ manages itself and the routing headers `CC` and `BCC`, with which RabbitMQ would also
deliver the copy to the queues they name. It never expires. Its `user_id` becomes the Executor's own
user, while its `x-kicak-original-publisher` header keeps naming the agent that created the message
(see [Who published a message](delivery-guarantees.md#who-published-a-message)). It also gets
headers that say why, when, and from where:

| Header | Value |
| --- | --- |
| `x-kicak-dead-letter-reason` | `malformed`: a `MalformedMessageError`, raised by `execute()`, or by the Executor for a message it cannot decode or whose results are invalid; `retries-exhausted`: a `TransientProcessingError` that persisted through `max_retry_rounds` rounds in the retry queue; `unexpected-error`: any other exception, treated as a bug, which also stopped the Executor; `result-too-large`: RabbitMQ refused one of its results as larger than its `max_message_size` |
| `x-kicak-error-type` | the class of the exception with its module, e.g. `pykicak.executor.MalformedMessageError` or `myapp.errors.InvoiceError`; a built-in one without it, e.g. `KeyError`; for `retries-exhausted`, the `TransientProcessingError` of the last attempt |
| `x-kicak-error-message` | the text of the exception, at most 1024 bytes of UTF-8; a longer one is cut and ends with `…` |
| `x-kicak-dead-lettered-at` | when the message was dead-lettered, in ISO 8601 and UTC |
| `x-kicak-dead-lettered-from` | the queue it was taken from |

The error text travels with the message: whoever can read the dead-letter queue, for example in
RabbitMQ's management UI, can read it. The library's own texts hold no values from the message, but
the texts of your exceptions, and of the libraries you call, hold whatever they were given. Keep
secrets out of them, as out of your logs, where the Executor writes the same error.

**Messages RabbitMQ dead-letters itself.** A quorum queue dead-letters a message that reached its
delivery limit (see [Queue type](topology.md#queue-type)), and a queue with a length limit or a
message TTL set by a policy dead-letters the messages it drops. RabbitMQ routes them through the
same `<queue>.dlx`, with its own `x-death` header recording the queue and the reason, for example
`delivery_limit`, and without the headers above. The Executor's copies have no `x-death`, because
RabbitMQ did not dead-letter them. So read `x-kicak-dead-letter-reason` first, and `x-death` when it
is missing.

**A message can be both in the dead-letter queue and processed.** If the connection is lost, or
RabbitMQ's `consumer_timeout` takes the message back, after RabbitMQ has confirmed the copy but
before the Executor acknowledges the original, RabbitMQ redelivers the original, which is executed
again. It may then be dead-lettered again, leaving two copies, or succeed, for example once a bug is
fixed. Sending a message back from the dead-letter queue is safe because agents are idempotent (see
[Requirement: idempotent processing](../README.md#requirement-idempotent-processing)).

**When the copy cannot be placed.** If RabbitMQ returns the copy as unroutable, because the
dead-letter queue or its binding was deleted right after the Executor declared them and no other
queue is bound to `<queue>.dlx`, or refuses it, for example because the dead-letter queue is full
and refuses new messages, the Executor requeues the message instead and logs an `ERROR`; RabbitMQ
then delivers it again at once. Rejecting it for RabbitMQ to dead-letter would not help: RabbitMQ
dead-letters a rejected message at most once and drops it, telling nobody, when no queue takes it.
RabbitMQ refusing to declare the dead-letter route, or refusing the copy in another way, for example
with `403 ACCESS_REFUSED`, stops the Executor. After an unexpected failure, the Executor stops
either way, and raises that failure, not the error of the copy.

## Sending dead letters back

Once the cause is fixed, for example when a service that was down longer than `max_retry_rounds`
allowed is back, or a bug in `execute()` is fixed, a dead-lettered message can be sent back to its
queue to be executed again. Send it to the queue itself, through RabbitMQ's default exchange, which
routes by queue name, not to the Executor's `source_exchange`, which may deliver it to other
consumers' queues too.

pykicak itself has no tool for this; operating the broker is the application's. The repository has
an example, [`examples/replay_dead_letters.py`](../examples/replay_dead_letters.py), built on pika
and `KicakConfig`, which sends back the messages dead-lettered for the reasons you choose and leaves
the others in the dead-letter queue:

```bash
python -m examples.replay_dead_letters classifier --reason retries-exhausted --dry-run
python -m examples.replay_dead_letters classifier --reason retries-exhausted
```

It removes a message from `<queue>.dlq` only after RabbitMQ has confirmed its copy in `<queue>`,
so a lost connection in between can send a message back twice. Choose the reasons with care: a
message dead-lettered as `unexpected-error` stops an Executor instance again until the bug is fixed.
A RabbitMQ shovel, such as "Move messages" in the management UI, moves every message of the queue,
whatever its reason.

A message sent back keeps the headers of its dead-lettering, and from those the Executor recognizes
it:

- Its retry rounds start again: it gets `max_retry_rounds` rounds once more, and `get_info()` and
  the `Message handled` line report its `retry_round` as 0.
- Every copy the Executor makes of it, for the retry queue or the dead-letter queue, drops those
  headers and its old retry headers, so it never shows a stale reason or round.
