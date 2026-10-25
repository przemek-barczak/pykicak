# Running and stopping an Executor

The examples use the `ClassifyingExecutor` of the [Usage](../README.md#usage) example in the README.

The Executor takes one message at a time: it sets a prefetch count of 1, so RabbitMQ delivers the
next message only after the previous one has been acknowledged or rejected. Messages waiting in the
queue are therefore never held, unprocessed, by the Executor, and their RabbitMQ delivery timeout
does not start early.

`run()` consumes until you call `stop()`, and then returns normally.

## Graceful stop

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
  second of a wait between two attempts. An attempt already under way is not interrupted: it ends
  when the broker answers or pika's connection timeouts expire, about 15 seconds for a server that
  accepts the connection but never answers. The same holds for a call to RabbitMQ already under
  way, so give the process time to stop accordingly.
- **A message delivered after the stop request,** before the Executor notices it, is not processed;
  RabbitMQ returns it to the queue when the connection closes.

`run()` then closes the connection and returns, with the status `"STOPPED"`. If closing the
connection fails, the failure is logged at `ERROR`, and `run()` still returns normally: no message
depends on it, because RabbitMQ requeues an unsettled message once the connection is gone.

`stop()` only sets a flag, so it is safe to call from any thread and from a signal handler. Connect
it to the signals your process supervisor sends, for example:

```python
import signal

executor = ClassifyingExecutor(config, ...)  # topology arguments as in the Usage example
for stop_signal in (signal.SIGINT, signal.SIGTERM):
    signal.signal(stop_signal, lambda _signum, _frame: executor.stop())
executor.run()  # returns after the message in progress, once a signal arrives
```

The stop request is permanent: calling `run()` on a stopped Executor returns immediately. Create a
new instance to consume again.

Give your supervisor's shutdown timeout (for example Kubernetes' `terminationGracePeriodSeconds`) at
least the longest processing of one message, retries of `execute()` included, plus a few seconds;
otherwise it kills the process in the middle of a message.

## Other ways `run()` ends

Without a stop request, `run()` ends only by raising:

- a failure (see [Every outcome of a delivery](outcomes.md));
- a `BaseException` such as `SystemExit` or `KeyboardInterrupt`, for example Python's default
  reaction to Ctrl+C when no signal handler calls `stop()`. This is a hard stop: the exception
  passes through unchanged, even in the middle of `execute()`, the connection is closed, and RabbitMQ
  requeues the message being processed, which is then executed again.

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
    max_retry_rounds=0,
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
`destination_queues`, `queue_type`, `max_processing_attempts`, `max_retry_rounds`,
`heartbeat_seconds`, `input_check_seconds`, and `retry_delay_seconds`), and start them as
independent processes:

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

- **The same message can be executed by two instances at the same time.** When RabbitMQ redelivers a
  message, for example after a lost connection, another instance can receive it while the first is
  still running `execute()`. Results are still published only by the instance whose connection is
  intact, but side effects inside `execute()`, such as database writes, can happen twice
  concurrently. The idempotency that pykicak requires must therefore be enforced by the database
  (see [Requirement: idempotent processing](../README.md#requirement-idempotent-processing)).
- **Processing longer than `heartbeat_seconds` or `consumer_timeout` causes such redeliveries.**
  RabbitMQ closes the connection or channel, or cancels the consumer, and redelivers the message
  while it is still being processed (see [Long-running `execute()`](#long-running-execute)).
  With several instances, another instance picks it up immediately.
- **An unexpected error stops only the instance that hit it.** Each instance dead-letters the
  message and stops on its own, so a bug that fails every message dead-letters one message per
  instance before all have stopped.
- **A failure that requeues the message and stops reaches every instance.** A refused commit, or a
  refused declaration of the output side (see [Every outcome of a
  delivery](outcomes.md)), returns the input to the queue
  before the instance stops, so the next instance receives the same message and stops too, until
  none is left running. With a classic queue, a commit that RabbitMQ always refuses, for example
  because the Executor may not write to its destination exchange, also stops every instance started
  later; a quorum queue dead-letters the message once it reaches its delivery limit (see [Queue
  type](topology.md#queue-type)). A result that is too large is the exception: only its input is
  dead-lettered, and the Executor continues. A refused declaration of the input side holds no
  message, and stops each instance when it verifies its input side.
