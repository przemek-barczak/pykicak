import dataclasses

import pytest

from pykicak.messages import KicakMessage


@dataclasses.dataclass(frozen=True, slots=True)
class SampleMessage(KicakMessage):
    name: str
    count: int


def test_to_bytes_from_bytes_round_trip():
    message = SampleMessage(message_id="m-1", name="hello", count=3)

    restored = SampleMessage.from_bytes(message.to_bytes())

    assert restored == message


def test_to_bytes_produces_json_bytes():
    message = SampleMessage(message_id="m-1", name="hello", count=3)

    assert message.to_bytes() == b'{"message_id": "m-1", "name": "hello", "count": 3}'


def test_serialization_and_deserialization_emit_debug_logs(caplog):
    message = SampleMessage(message_id="m-1", name="hello", count=3)
    caplog.set_level("DEBUG", logger="pykicak.messages")

    restored = SampleMessage.from_bytes(message.to_bytes())

    assert restored == message
    assert any(
        record.levelname == "DEBUG" and "Serialized message type=SampleMessage" in record.message
        for record in caplog.records
    )
    assert any(
        record.levelname == "DEBUG" and "Deserialized message type=SampleMessage" in record.message
        for record in caplog.records
    )


def test_from_bytes_raises_on_invalid_json():
    with pytest.raises(ValueError):
        SampleMessage.from_bytes(b"not json")


def test_from_bytes_raises_on_unexpected_fields():
    with pytest.raises(TypeError):
        SampleMessage.from_bytes(
            b'{"message_id": "m-1", "name": "hello", "count": 3, "extra": true}'
        )


def test_message_id_is_required():
    with pytest.raises(TypeError, match="message_id"):
        SampleMessage(name="hello", count=3)


def test_message_id_is_keyword_only():
    with pytest.raises(TypeError):
        SampleMessage("hello", 3, "m-1")


@pytest.mark.parametrize(
    ("message_id", "error"),
    [
        (7, TypeError),
        (None, TypeError),
        ("", ValueError),
        ("x" * 256, ValueError),
        ("ż" * 128, ValueError),  # 256 bytes in UTF-8
    ],
    ids=["int", "none", "empty", "256-ascii", "256-utf8-bytes"],
)
def test_message_id_must_be_a_non_empty_string_of_at_most_255_bytes(message_id, error):
    with pytest.raises(error, match="message_id"):
        SampleMessage(message_id=message_id, name="hello", count=3)


@pytest.mark.parametrize("message_id", ["x" * 255, "ż" * 127 + "x"], ids=["ascii", "utf8"])
def test_message_id_accepts_255_bytes(message_id):
    message = SampleMessage(message_id=message_id, name="hello", count=3)

    assert SampleMessage.from_bytes(message.to_bytes()) == message


def test_from_bytes_rejects_body_without_message_id():
    with pytest.raises(TypeError, match="message_id"):
        SampleMessage.from_bytes(b'{"name": "hello", "count": 3}')


@pytest.mark.parametrize(
    ("body", "error"),
    [
        (b'{"message_id": 7, "name": "hello", "count": 3}', TypeError),
        (b'{"message_id": "", "name": "hello", "count": 3}', ValueError),
    ],
    ids=["not-a-string", "empty"],
)
def test_from_bytes_rejects_invalid_message_id(body, error):
    with pytest.raises(error, match="message_id"):
        SampleMessage.from_bytes(body)


def test_subclass_post_init_can_call_the_base_validation():
    @dataclasses.dataclass(frozen=True, slots=True)
    class ValidatedMessage(KicakMessage):
        count: int

        def __post_init__(self) -> None:
            KicakMessage.__post_init__(self)
            if self.count < 0:
                raise ValueError("count must not be negative")

    assert ValidatedMessage(message_id="m-1", count=1).count == 1
    with pytest.raises(ValueError, match="message_id"):
        ValidatedMessage(message_id="", count=1)


def test_to_bytes_validates_message_id_when_subclass_skips_base_validation():
    @dataclasses.dataclass(frozen=True, slots=True)
    class SkippingMessage(KicakMessage):
        def __post_init__(self) -> None:
            pass  # forgets KicakMessage.__post_init__(self)

    message = SkippingMessage(message_id="")

    with pytest.raises(ValueError, match="message_id"):
        message.to_bytes()
