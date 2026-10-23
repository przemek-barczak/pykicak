# Every outcome of a delivery

How `execute()` tells the Executor what kind of failure it met is described in [What `execute()`
raises is your decision](../README.md#what-execute-raises-is-your-decision).

The Executor decides the outcome of every delivery from how `execute()` ends, and from what RabbitMQ
reports while the results are published and the message is settled:

| Outcome | Cause | Delivery is… | Executor |
| --- | --- | --- | --- |
| Success | `execute()` returns | results published in one transaction, then acknowledged | continues |
| Deterministic failure | a `type` property that is missing or names a type the Executor does not accept, missing body, undecodable body (not JSON, `NaN` and `Infinity` included, a number too large for a float, nested too deeply to decode, not a JSON object, missing or unexpected fields, no valid `message_id`, refused by the message class's `__post_init__` with any exception), `MalformedMessageError` raised by `execute()`, or results returned by `execute()` that are not `KicakMessage`s or cannot be serialized | dead-lettered: copied to the dead-letter exchange with headers saying why, and acknowledged once RabbitMQ has confirmed the copy; if RabbitMQ does not take the copy (returns it as unroutable, or refuses it), requeued with `basic_nack(requeue=True)` and logged at `ERROR` (see [Dead-letter queue](dead-letter-queue.md)) | continues; RabbitMQ refusing to declare the dead-letter route stops it |
| Transient failure | `TransientProcessingError` raised by `execute()` | retried in-process; if every attempt fails, copied to the retry queue and acknowledged once RabbitMQ has confirmed the copy; if RabbitMQ does not take the copy (returns it as unroutable, or refuses it), requeued with `basic_nack(requeue=True)` and logged at `ERROR` | continues; RabbitMQ refusing to declare the retry queue stops it |
| Retries used up | `TransientProcessingError` raised by every attempt of the last round, after `max_retry_rounds` moves to the retry queue (at once with `max_retry_rounds=0`) | dead-lettered as above, with the reason `retries-exhausted` (see [Retry queue](retry-queue.md)) | continues |
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
the message then goes to the retry queue, or, once it has had `max_retry_rounds` rounds there, to
the dead-letter queue (see [Retry queue](retry-queue.md)).

Every dead-lettered message is logged at `ERROR`, and its copy in `<queue>.dlq` keeps its original
body and says why it is there (see [Dead-letter queue](dead-letter-queue.md)).

## Unroutable results are held

The Executor verifies its output side right before publishing, so RabbitMQ returns its results as
unroutable only if every binding of the destination exchange disappeared in the few milliseconds
before the commit. None of the results was delivered then, and the Executor holds them:

- The input stays unacknowledged, and its results stay in memory; `execute()` is not called again.
- After a wait of 0.5 seconds, doubling up to 30, with jitter, the Executor verifies the output side
  again, which binds the queues again, and publishes the results. The wait starts over for the next
  message. During it, the connection keeps answering heartbeats.
- The status is `"DOWNSTREAM_RECOVER"` until the results are committed (see
  [Status information](monitoring.md#status-information)).
- `stop()` ends the hold within about a second: the Executor requeues the input with
  `basic_nack(requeue=True)`, so it is executed again later, and `run()` returns.
- A lost connection, or RabbitMQ's `consumer_timeout` (30 minutes by default) expiring, also ends
  it: RabbitMQ requeues the input, and the Executor reconnects and executes it again.

On a quorum queue, every requeue counts toward the delivery limit (see [Queue
type](topology.md#queue-type)).

## Results are published in a transaction

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
  is one of the reasons agents must be idempotent (see [Requirement: idempotent
  processing](../README.md#requirement-idempotent-processing)).
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
