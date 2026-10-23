# Monitoring

An Executor reports what it is doing through `get_info()`, and every agent logs through Python's
standard `logging`, including a line with durations and sizes for every message it handles.

## Status information

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
| `retry_round` | How often `message` was moved to the retry queue before, 0 if never or if it was sent back from the dead-letter queue, or `None`. |
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
| `"DOWNSTREAM_RECOVER"` | RabbitMQ returned the results as unroutable; the Executor holds the message and its results and tries again (see [Unroutable results are held](outcomes.md#unroutable-results-are-held)). |
| `"SENDING_RESULT"` | The output side is verified; the results are published and committed, then the message is acknowledged. |
| `"STOPPING"` | `stop()` was called; `run()` finishes the message in progress, then returns. It overrides the statuses above. |
| `"STOPPED"` | `run()` returned after a graceful stop. |
| `"CRASHED"` | `run()` ended by raising an exception: a failure or a hard stop. |

For each message the status goes from `"WAITING_FOR_MESSAGE"` to `"PROCESSING"`, then
`"SENDING_RESULT"` if it has results, and back. The first attempt to connect or to verify the
topology keeps the current status; only a failed attempt enters a recovery status, and the
Executor logs a `WARNING` when it enters one and an `INFO` when it leaves it, which tells how long
it lasted (see [Statistics in the logs](#statistics-in-the-logs)). A lost connection is
`"UPSTREAM_RECOVER"` even when it is noticed while sending results, because the message is executed
again after reconnecting. The recovery statuses last however long the recovery takes (see
[Retries and logging](delivery-guarantees.md#retries)).

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
- `retry_round` is the message's `x-kicak-retry-round` header (see [Retry queue](retry-queue.md)):
  how often it was moved to the retry queue before, 0 for a message that never was. A message whose
  `retry_round` equals `max_retry_rounds` is dead-lettered if this round fails too. A message sent
  back from the dead-letter queue starts again at 0 (see
  [Sending dead letters back](dead-letter-queue.md#sending-dead-letters-back)).

`counts` tells how every delivery the Executor started to process has ended. Each one counts
once, when it is settled or left to RabbitMQ:

| `counts.` | Deliveries that were… |
| --- | --- |
| `completed` | executed, their results published, if any, and acknowledged |
| `dead_lettered.malformed` | dead-lettered for a `MalformedMessageError` (see [Dead-letter queue](dead-letter-queue.md)) |
| `dead_lettered.retries_exhausted` | dead-lettered because `TransientProcessingError` persisted through `max_retry_rounds` rounds in the retry queue (see [Retry queue](retry-queue.md)) |
| `dead_lettered.result_too_large` | dead-lettered because RabbitMQ refused one of their results as too large |
| `dead_lettered.unexpected_error` | dead-lettered for any other exception, each of which also stopped the Executor |
| `dead_lettered.total` | dead-lettered, for any of the four reasons |
| `moved_to_retry` | moved to the retry queue after `TransientProcessingError` persisted |
| `requeued` | returned to the queue by the Executor: RabbitMQ did not take their copy for the retry queue or the dead-letter queue, or `stop()` ended a hold of their unroutable results |
| `abandoned` | left to RabbitMQ, which delivers them again: the connection was lost, `consumer_timeout` expired, or `run()` ended by raising an exception while processing them |

The counts start at zero when the Executor is created and only grow, also across a `run()` called
again after `"CRASHED"`, so they map directly onto monitoring counters, such as Prometheus counters
read on every scrape; like those, they start again with the process. They count deliveries, not
messages: a message delivered twice counts twice, and every count describes this instance only. A
growing `abandoned` usually means that processing outlasts `heartbeat_seconds` or
`consumer_timeout`, so messages are executed again (see [Long-running
`execute()`](running-an-executor.md#long-running-execute)). A delivery received after `stop()` is
not processed, and not counted. How long the deliveries took is in the logs: see [Statistics in the
logs](#statistics-in-the-logs).

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

## Logging

The library emits standard Python logging records through module loggers. Applications configure
handlers and levels, for example:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

## Statistics in the logs

Every delivery an Executor takes ends with one `INFO` line, whatever its outcome, telling how long
each part of its handling took and how large its message and results were, so that statistics
such as averages and percentiles can be computed outside the library:

```text
Message handled outcome=completed reason=None type=Chunk id=doc-17 queue=chunker delivery_tag=42 redelivered=False retry_round=0 attempts=1 results=3 input_bytes=2048 result_bytes=6144 largest_result_bytes=2560 total_seconds=1.284 execute_seconds=1.201 backoff_seconds=0.000 publish_seconds=0.071 held_seconds=0.000 settle_seconds=0.001 queued_seconds=0.350
```

| Key | Value |
| --- | --- |
| `outcome` | how the delivery ended, named like the field of [`counts`](#status-information) that counts it: `completed`, `dead_lettered`, `moved_to_retry`, `requeued`, or `abandoned` |
| `reason` | for `dead_lettered`, its `x-kicak-dead-letter-reason` (`malformed`, `retries-exhausted`, `result-too-large`, or `unexpected-error`); otherwise `None` |
| `type`, `id` | the message's class and `message_id`; for a delivery that could not be decoded, the accepted class its `type` property names and its sender's `message_id` property, each `None` if there is none |
| `queue`, `delivery_tag` | the source queue, and the delivery's tag on the consuming channel |
| `redelivered`, `retry_round` | RabbitMQ's redelivered flag, and how often the message was moved to the retry queue before (see [Status information](#status-information)) |
| `attempts` | the calls of `execute()`; 0 for a delivery that could not be decoded |
| `results` | the results `execute()` returned, once validated and serialized; they are published only when the outcome is `completed` |
| `input_bytes` | the size of the delivery's body, also of one that could not be decoded; `None` for a delivery without a body |
| `result_bytes` | the size of all the results' bodies together; 0 without results |
| `largest_result_bytes` | the size of the largest result's body; 0 without results |
| `total_seconds` | from taking the delivery until it was settled or left to RabbitMQ |
| `execute_seconds` | in `execute()`, all its calls together |
| `backoff_seconds` | waiting between the calls retried after `TransientProcessingError` |
| `publish_seconds` | verifying the output side, publishing, and committing the results, all attempts together |
| `held_seconds` | holding results RabbitMQ returned as unroutable (see [Unroutable results are held](outcomes.md#unroutable-results-are-held)) |
| `settle_seconds` | acknowledging or requeueing the delivery, including publishing its copy for the retry queue or the dead-letter queue and waiting for RabbitMQ's confirmation |
| `queued_seconds` | how long the delivery waited since it was published, before the Executor took it: from its `x-kicak-published-at` header (see [When a message was published](delivery-guarantees.md#when-a-message-was-published)) to the Executor's clock when it took the delivery; `None` without a valid header |

The parts do not overlap, and what `total_seconds` has beyond them is decoding the delivery,
serializing the results, and logging. Durations are seconds, three decimals in the text, measured
with a monotonic clock (`time.perf_counter()`), so setting the system clock does not affect them.

Sizes are bytes of the bodies, the messages as JSON, without headers and properties. RabbitMQ's
`max_message_size` (16 MiB by default) limits the body of each message, so a
`largest_result_bytes` growing towards it warns before inputs are dead-lettered as
`result-too-large`; the line of such an input carries the sizes of the results it could not
publish.

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

An Injector logs one `INFO` line for every message it tries to publish, from `run()` and from
`publish()`, whether or not the message was injected:

```text
Message injected outcome=injected error=None type=Order id=order-17 exchange=orders attempts=1 message_bytes=412 total_seconds=0.083 generate_seconds=0.012 connect_seconds=0.061 publish_seconds=0.009 backoff_seconds=0.000
```

| Key | Value |
| --- | --- |
| `outcome` | `injected`, or `failed`: the Injector raised `InjectionError`, or was interrupted, e.g. by `KeyboardInterrupt` |
| `error` | for `failed`, the class of the error that ended the last attempt, e.g. `UnroutableError`, `NackError`, or `StreamLostError`, the `__cause__` of the `InjectionError`; otherwise `None` |
| `type`, `id`, `exchange` | the message's class and `message_id`, and the exchange |
| `attempts` | the attempts to connect and publish, at most 3 |
| `message_bytes` | the size of the message's body |
| `total_seconds` | for `run()`, from calling `generate()` until the connection was closed; for `publish()`, the call |
| `generate_seconds` | in `generate()`; `None` for `publish()`, whose caller made the message |
| `connect_seconds` | opening a connection and declaring the exchange, all attempts together; 0 for a `publish()` on an open connection |
| `publish_seconds` | publishing the message and waiting for RabbitMQ's confirmation, all attempts together |
| `backoff_seconds` | between the attempts: discarding the failed connection and waiting before retrying (1 second, then 2) |

A message that never reaches RabbitMQ has no line: when `generate()` returns `None` or raises, or
the message cannot be serialized or is of another class than the declared one. What
`total_seconds` has beyond the parts is serializing the message, closing the connection of `run()`,
and logging, and the `event` of the line's `kicak` attribute is `"message_injected"`. With `run()`,
every injection opens a connection of its own: a `connect_seconds` much larger than
`publish_seconds` shows an Injector that would gain from keeping one connection open, with
`connect()` and `publish()`.

## Connection names

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
