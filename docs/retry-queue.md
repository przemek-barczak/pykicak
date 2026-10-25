# Retry queue

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
4. After `max_retry_rounds` moves, a message whose last round fails too is dead-lettered instead,
   with the reason `retries-exhausted` (see [Dead-letter queue](dead-letter-queue.md)), and the
   Executor continues.

**Choosing `max_retry_rounds`.** It is required: how long a failing service may stay down, and how
much calling it again may cost, depends on the agent, so every Executor states it. Two numbers follow
from it:

- A message is dead-lettered no sooner than `max_retry_rounds × retry_delay_seconds` after its first
  move, because it waits in the retry queue at least that long in every round. It counts tries, not
  time: waiting in a long input queue, or while the Executor is stopped, uses up no rounds. Size it
  to the longest outage the agent should wait out, for example 360 rounds of 10 seconds for an
  hour.
- The retry rounds call `execute()` at most `(max_retry_rounds + 1) × max_processing_attempts`
  times for one message, which bounds what a message that never succeeds costs, for example in
  calls to a paid service. A longer `retry_delay_seconds` with fewer rounds waits out the same
  outage with fewer calls: 6 rounds of 10 minutes instead of 360 of 10 seconds.
- That bound counts the rounds only. Every repeat that does not move the message to the retry
  queue adds calls on top: a redelivery after a lost connection, a crash, or an expired
  `consumer_timeout`, a requeue when RabbitMQ does not take the copy for the retry queue, and
  sending the message back from the dead-letter queue (see [Requirement: idempotent
  processing](../README.md#requirement-idempotent-processing)). With at-least-once delivery no
  bound on the calls can be absolute.

With `max_retry_rounds=0` a message is never moved to the retry queue: one still failing after its
first `max_processing_attempts` calls is dead-lettered. The rounds are counted in the message's own
headers (below), not in the Executor, so every instance continues the count of another, and changing
`max_retry_rounds` takes only a restart; give every instance of an Executor the same value. After an
outage that outlasted the limit, send the dead-lettered messages back (see [Sending dead letters
back](dead-letter-queue.md#sending-dead-letters-back)): they get all their rounds again.

The copy in the retry queue keeps the body and the message's properties, its `type` included, so it
comes back as the same version of the message, and its headers, except the headers RabbitMQ manages
itself (`x-death`, `x-delivery-count`, `x-first-death-*`, and `x-last-death-*`) and the routing
headers `CC` and `BCC`: another publisher's `CC` naming the source queue would otherwise deliver the
copy there at once, skipping the retry delay, besides putting it in the retry queue. Its `user_id`
becomes the Executor's own user, because RabbitMQ refuses any other, while its
`x-kicak-original-publisher` header keeps naming the agent that created the message (see [Who
published a message](delivery-guarantees.md#who-published-a-message)), and its
`x-kicak-published-at` header becomes the time of the move, because the copy is published anew (see
[When a message was published](delivery-guarantees.md#when-a-message-was-published)). It also gets
two headers that count the rounds:

| Header | Value |
| --- | --- |
| `x-kicak-retry-round` | 1 for the first move to the retry queue, then 2, 3, and so on, up to `max_retry_rounds`; 1 again for the first move of a message sent back from the dead-letter queue |
| `x-kicak-retrying-since` | when the message was first moved there, in ISO 8601 and UTC |

Every move is logged at `WARNING`, for example:

```text
Message moved to retry queue after 3 failed processing attempts id=doc-42 queue=classifier
retry_queue=classifier.retry retry_round=37 max_retry_rounds=360
retrying_since=2026-10-19T08:00:00+00:00 retry_in=10.0s: summarizer unavailable
```

**Finding a message that keeps failing.** Alert on these warnings: the same `id` with a
`retry_round` approaching `max_retry_rounds`, or a `retrying_since` long ago, is a message that keeps
failing. A growing number of messages in `<queue>.retry` shows a failure that affects many.
RabbitMQ's management UI shows the headers of the messages waiting there; use "Get messages" with
"Nack message requeue true" to look at them without removing them.

**Taking such a message out sooner.** `max_retry_rounds` takes a message that never succeeds out by
itself. To take one out before that, note that RabbitMQ can only take messages from the front of a
queue, not one chosen by its ID, and the message spends part of its time elsewhere: taking messages
from the retry queue by hand also removes the ones in front of it, and rejecting one there sends it
back to the input queue. The safe way is to make `execute()` raise `MalformedMessageError` for it,
for example for the IDs on a list your application reads: the next time the message comes round, it
is dead-lettered to `<queue>.dlq`, where it is kept for inspection or replay. Fixing the cause, such
as a service that is down, ends the rounds by itself.

**The way back.** The default exchange cannot be deleted, so the way back depends only on the input
queue. RabbitMQ dead-letters the expired messages of a classic queue at most once, without any
confirmation: a message that expires while the input queue refuses it is dropped, and nobody is
told. That happens when the input queue is full and set to refuse new messages (a length limit with
`overflow: reject-publish`, for example from a policy), or does not exist, because someone deleted
it; the Executor declares it again within about a second. The Executor has already counted the
message as moved to the retry queue. A quorum retry queue is declared to dead-letter at least once
(`x-dead-letter-strategy` `at-least-once`, which requires `x-overflow` `reject-publish`), so it
keeps such a message until the input queue takes it. Use quorum queues where no message may be lost
(see [When a message can still be lost](delivery-guarantees.md#when-a-message-can-still-be-lost)).

**Changing the delay.** The delay is set on each message, not as an argument of the queue, so
changing `retry_delay_seconds` takes only a restart. RabbitMQ expires only the message at the front
of a queue, so while messages with the old delay are still waiting, one with a shorter new delay can
wait behind them, for at most the old delay.

**When the copy cannot be placed.** If RabbitMQ returns the copy as unroutable, because the retry
queue was deleted right after the Executor declared it, or refuses it, the Executor requeues the
message instead and logs an `ERROR`; RabbitMQ then delivers it again at once. RabbitMQ refusing to
declare the retry queue, for example with `406 PRECONDITION_FAILED` because a queue of that name
exists with other arguments, stops the Executor.
