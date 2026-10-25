# Running an Injector

An Injector is created and run as in the [Usage](../README.md#usage) example of the README.

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
and its text and its `repr()` name the message type and `message_id`, never the payload. Catch it to
learn that the message was not injected:

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
