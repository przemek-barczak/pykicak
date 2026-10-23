"""Message types: declared in the class header, sent as `type`."""

import dataclasses
from typing import Any, TypedDict

import pika
import pika.channel
import pytest

from pykicak.executor import (
    ExecutorStatus,
    KicakExecutorAbstract,
    MalformedMessageError,
)
from pykicak.messages import KicakMessage
from tests.executor.helpers import (
    DESTINATION_QUEUES,
    TOPOLOGY,
    ProcessingExecutor,
    SampleExecutor,
    SampleMessage,
    SampleTerminator,
    dead_letter_copies,
    dead_lettered,
    make_executor,
    run_until_stopped,
)
from tests.fakes import (
    SAMPLE_TYPE,
    FakeChannel,
)


@dataclasses.dataclass(frozen=True, slots=True)
class ChunkV1(KicakMessage):
    kicak_type = "chunk.v1"
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class ChunkV2(KicakMessage):
    kicak_type = "chunk.v2"
    text: str  # the same fields as ChunkV1: only the type property tells them apart


@dataclasses.dataclass(frozen=True, slots=True)
class UnnamedSample(SampleMessage):
    """Inherits SampleMessage's kicak_type without declaring its own."""


@dataclasses.dataclass(frozen=True, slots=True)
class RenamedSample(SampleMessage):
    kicak_type = "renamed-sample.v1"


@dataclasses.dataclass(frozen=True, slots=True)
class SameNameAsChunkV1(KicakMessage):
    kicak_type = "chunk.v1"
    text: str


class DeliveryArguments(TypedDict):
    properties: pika.BasicProperties


def delivery_of(message: KicakMessage) -> DeliveryArguments:
    """Keyword arguments of queue_deliveries() for `message`, sent as a pykicak agent sends it."""
    return {
        "properties": pika.BasicProperties(
            type=type(message).kicak_type, message_id=message.message_id
        )
    }


class Open[M: KicakMessage](ProcessingExecutor[M, None]):
    """A generic executor class that leaves its input type open."""


class Unparameterized(KicakExecutorAbstract):  # type: ignore[type-arg]
    """An application's base class that forgot the message types; its subclasses inherit that."""

    def execute(self, message):
        return None


type AnyChunk = ChunkV1 | ChunkV2


type OnlyV1 = ChunkV1


type WithSample[T] = T | SampleMessage


type NoResult = None


class ChunkTerminator(ProcessingExecutor[ChunkV1 | ChunkV2, None]):
    """Accepts both versions of a chunk, e.g. during an upgrade of its producer."""


@pytest.mark.parametrize(
    ("declaration", "error", "match"),
    [
        (lambda: KicakExecutorAbstract, TypeError, "must declare its message types"),
        (lambda: ProcessingExecutor, TypeError, "must declare its message types"),
        (lambda: Open, TypeError, "must declare its message types"),
        (lambda: Unparameterized, TypeError, "must declare its message types"),
        (lambda: ProcessingExecutor[UnnamedSample, None], TypeError, "its own kicak_type"),
        (lambda: ProcessingExecutor[KicakMessage, None], TypeError, "KicakMessage subclass"),
        (
            lambda: ProcessingExecutor[SampleMessage | None, None],  # type: ignore[type-var]
            TypeError,
            "not None",
        ),
        (
            lambda: ProcessingExecutor[ChunkV1 | SameNameAsChunkV1, None],
            TypeError,
            "ChunkV1 and SameNameAsChunkV1, which declare the same kicak_type 'chunk.v1'",
        ),
        (lambda: ProcessingExecutor[SampleMessage, ChunkV1 | ChunkV2], TypeError, "one result"),
        (lambda: ProcessingExecutor[SampleMessage, UnnamedSample], TypeError, "own kicak_type"),
    ],
    ids=[
        "unparameterized",
        "unparameterized-generic-subclass",
        "open-type-parameter",
        "subclass-of-an-unparameterized-subclass",
        "inherited-type-name",
        "base-message-class",
        "none-accepted",
        "duplicate-type-name",
        "union-result",
        "result-without-type-name",
    ],
)
def test_init_rejects_an_invalid_declaration_of_message_types(config, declaration, error, match):
    base = declaration()

    class Declared(base):  # type: ignore[valid-type, misc]
        def execute(self, message):
            return None

    arguments = (config, FakeChannel()) if issubclass(Declared, ProcessingExecutor) else (config,)
    with pytest.raises(error, match=match):
        Declared(*arguments, **TOPOLOGY)


def test_init_requires_a_destination_for_a_declared_result_type(config):
    with pytest.raises(ValueError, match="declares its results as SampleMessage"):
        SampleExecutor(config, FakeChannel(), **TOPOLOGY)


def test_init_rejects_a_destination_for_a_terminator(config):
    with pytest.raises(ValueError, match="declares the result type None, so it is a terminator"):
        SampleTerminator(
            config,
            FakeChannel(),
            **TOPOLOGY,
            destination_exchange="output-exchange",
            destination_queues=DESTINATION_QUEUES,
        )


def test_message_types_are_resolved_through_subclasses_and_generic_classes(config):
    class Swapped[R: KicakMessage | None, M: KicakMessage](ProcessingExecutor[M, R]):
        pass

    class Concrete(Swapped[None, ChunkV1 | ChunkV2]):
        pass

    class WithOpenMember[M: KicakMessage](ProcessingExecutor[M | ChunkV1, None]):
        pass

    class Closed(WithOpenMember[ChunkV2]):
        pass

    class Inherited(SampleTerminator):
        pass

    class LoggingMixin:
        """A base that is no agent, listed before the agent base."""

    class Mixed(LoggingMixin, ProcessingExecutor[ChunkV1, None]):
        pass

    assert Concrete(config, FakeChannel(), **TOPOLOGY)._accepted_types == {
        "chunk.v1": ChunkV1,
        "chunk.v2": ChunkV2,
    }
    assert set(Closed(config, FakeChannel(), **TOPOLOGY)._accepted_types.values()) == {
        ChunkV1,
        ChunkV2,
    }
    inherited = Inherited(config, FakeChannel(), **TOPOLOGY)
    assert inherited._accepted_types == {SAMPLE_TYPE: SampleMessage}
    assert inherited._result_type is None
    mixed = Mixed(config, FakeChannel(), **TOPOLOGY)
    assert mixed._accepted_types == {"chunk.v1": ChunkV1}
    assert mixed._result_type is None


def test_message_types_are_resolved_through_type_statement_aliases(config):
    class AliasedUnion(ProcessingExecutor[AnyChunk, SampleMessage]):
        pass

    class AliasAsUnionMember(ProcessingExecutor[OnlyV1 | ChunkV2, None]):
        pass

    class GenericAlias(ProcessingExecutor[WithSample[ChunkV1], None]):
        pass

    class AliasedResults(ProcessingExecutor[SampleMessage, OnlyV1]):
        pass

    class AliasedTerminator(ProcessingExecutor[SampleMessage, NoResult]):
        pass

    publishing: dict[str, Any] = {
        "destination_exchange": "output-exchange",
        "destination_queues": ["output"],
    }
    aliased = AliasedUnion(config, FakeChannel(), **TOPOLOGY, **publishing)
    assert aliased._accepted_types == {"chunk.v1": ChunkV1, "chunk.v2": ChunkV2}
    assert aliased._result_type is SampleMessage
    member = AliasAsUnionMember(config, FakeChannel(), **TOPOLOGY)
    assert member._accepted_types == {"chunk.v1": ChunkV1, "chunk.v2": ChunkV2}
    generic = GenericAlias(config, FakeChannel(), **TOPOLOGY)
    assert generic._accepted_types == {"chunk.v1": ChunkV1, SAMPLE_TYPE: SampleMessage}
    results = AliasedResults(config, FakeChannel(), **TOPOLOGY, **publishing)
    assert results._result_type is ChunkV1
    assert AliasedTerminator(config, FakeChannel(), **TOPOLOGY)._result_type is None


def test_run_decodes_each_delivery_into_the_version_its_type_property_names(config):
    channel = FakeChannel()
    executor = ChunkTerminator(config, channel, **TOPOLOGY)
    old = ChunkV1(message_id="old", text="same fields")
    new = ChunkV2(message_id="new", text="same fields")
    channel.queue_deliveries(old.to_bytes(), **delivery_of(old))
    channel.queue_deliveries(new.to_bytes(), **delivery_of(new))

    run_until_stopped(executor)

    # The bodies are alike; the type property alone decides the class
    assert old.to_bytes().replace(b"old", b"new") == new.to_bytes()
    assert [type(message) for message in executor.processed_messages] == [ChunkV1, ChunkV2]
    assert executor.processed_messages == [old, new]
    assert channel.acked_delivery_tags == [1, 2]


def test_run_decodes_a_type_property_delivered_as_bytes(config):
    channel = FakeChannel()
    executor = ChunkTerminator(config, channel, **TOPOLOGY)
    chunk = ChunkV2(message_id="bytes-type", text="text")
    channel.queue_deliveries(chunk.to_bytes(), properties=pika.BasicProperties(type=b"chunk.v2"))

    run_until_stopped(executor)

    assert executor.processed_messages == [chunk]


@pytest.mark.parametrize(
    ("properties", "reason"),
    [
        (
            pika.BasicProperties(type="chunk.v3", message_id="unaccepted"),
            "Received a message of type 'chunk.v3', which is not accepted; "
            "accepted types: 'chunk.v1', 'chunk.v2'",
        ),
        (
            pika.BasicProperties(type=SAMPLE_TYPE, message_id="unaccepted"),
            f"Received a message of type '{SAMPLE_TYPE}', which is not accepted",
        ),
        (
            pika.BasicProperties(message_id="unaccepted"),
            "Received a message without a type property; accepted types: 'chunk.v1', 'chunk.v2'",
        ),
        (
            pika.BasicProperties(type="", message_id="unaccepted"),
            "Received a message without a type property",
        ),
    ],
    ids=["unknown-version", "another-type", "no-type", "empty-type"],
)
def test_run_dead_letters_a_delivery_whose_type_is_not_accepted_and_continues(
    config, caplog, properties, reason
):
    channel = FakeChannel()
    executor = ChunkTerminator(config, channel, **TOPOLOGY)
    accepted = ChunkV1(message_id="accepted", text="text")
    # The body would decode into either version: without an accepted type it is still refused
    channel.queue_deliveries(
        ChunkV1(message_id="unaccepted", text="text").to_bytes(), properties=properties
    )
    channel.queue_deliveries(accepted.to_bytes(), **delivery_of(accepted))

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert channel.acked_delivery_tags == [1, 2]
    assert executor.processed_messages == [accepted]
    [record] = [r for r in caplog.records if "Message dead-lettered" in r.message]
    assert record.levelname == "ERROR"
    assert "type=None id=unaccepted queue=input-queue" in record.message
    assert reason in record.message


def test_results_are_published_with_the_type_name_of_their_class(config):
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda message: SampleMessage(message_id=f"{message.message_id}.result", text=""),
        destination_exchange="output-exchange",
    )
    channel.queue_deliveries(SampleMessage(message_id="input", text="input").to_bytes())

    run_until_stopped(executor)

    [published] = channel.published_messages
    assert published.properties.type == SAMPLE_TYPE
    assert published.properties.message_id == "input.result"


@pytest.mark.parametrize(
    "result",
    [
        RenamedSample(message_id="subclass", text="a subclass of the declared class"),
        ChunkV1(message_id="other", text="another class"),
    ],
    ids=["subclass", "other-class"],
)
def test_run_dead_letters_and_raises_when_a_result_is_not_of_the_declared_type(
    config, caplog, result
):
    channel = FakeChannel()
    executor = make_executor(
        config,
        channel,
        handler=lambda _message: [SampleMessage(message_id="valid", text="valid"), result],
        destination_exchange="output-exchange",
    )
    channel.queue_deliveries(
        SampleMessage(message_id="input", text="input").to_bytes(),
        SampleMessage(message_id="never-processed", text="never processed").to_bytes(),
    )

    with pytest.raises(TypeError, match="declares its results as SampleMessage"):
        executor.run()

    # A bug in the executor's own code: nothing of the input's results is published
    assert channel.published_messages == dead_letter_copies(channel)
    assert dead_lettered(channel) == [1]
    assert len(executor.processed_messages) == 1
    assert executor.get_info().status == ExecutorStatus.CRASHED
    assert "failed unexpectedly type=SampleMessage id=input" in caplog.text
    assert f"result of type {type(result).__name__}" in caplog.text


def test_run_dead_letters_message_with_unexpected_fields(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(b'{"message_id": "hello", "text": "hello", "extra": true}')

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert executor.processed_messages == []


@pytest.mark.parametrize(
    "body",
    [
        b'{"text": "hello"}',
        b'{"message_id": "", "text": "hello"}',
        b'{"message_id": 7, "text": "hello"}',
    ],
    ids=["missing", "empty", "not-a-string"],
)
def test_run_dead_letters_message_without_valid_message_id_and_continues(config, body):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(body, SampleMessage(message_id="valid", text="valid").to_bytes())

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert channel.acked_delivery_tags == [1, 2]
    assert executor.processed_messages == [SampleMessage(message_id="valid", text="valid")]


def test_run_dead_letters_message_without_body(config):
    channel = FakeChannel()
    executor = make_executor(config, channel)
    channel.queue_deliveries(None)

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert dead_letter_copies(channel)[0].body == b""


def test_run_dead_letters_message_rejected_by_execute_without_publishing(config):
    def reject(_message):
        raise MalformedMessageError("fails application validation")

    channel = FakeChannel()
    executor = make_executor(
        config, channel, handler=reject, destination_exchange="output-exchange"
    )
    channel.queue_deliveries(SampleMessage(message_id="invalid", text="invalid").to_bytes())

    run_until_stopped(executor)

    assert dead_lettered(channel) == [1]
    assert channel.published_messages == dead_letter_copies(channel)
