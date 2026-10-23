# Message types

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
your classes do not declare it. Idempotent processing depends on it (see [Requirement: idempotent
processing](../README.md#requirement-idempotent-processing)):

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

## Type names: the contract between agents

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
  [Every outcome of a delivery](outcomes.md)).
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

## A new version of a message

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
   [Retry queue](retry-queue.md)). Deploy the consumers accepting only V2.

The [proof of concept](proof-of-concept.md) shows this upgrade in progress, as code you can run.

In the other order, the consumers dead-letter every message of a version they do not accept yet.
The messages are kept in `<queue>.dlq` with their `type` property, so they can be replayed once the
consumers accept them. A V1 message replayed after step 3 is dead-lettered again, so keep V1
accepted for as long as you may replay such messages.
