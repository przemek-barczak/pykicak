import dataclasses
import enum
import json
from typing import ClassVar

import pytest

from pykicak.messages import KicakMessage, _type_name_of


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


@dataclasses.dataclass(frozen=True, slots=True)
class MeasurementMessage(KicakMessage):
    value: object


@pytest.mark.parametrize(
    "value",
    [float("nan"), float("inf"), float("-inf"), [1.0, float("nan")], {"nested": float("inf")}],
    ids=["nan", "inf", "-inf", "in-list", "in-dict"],
)
def test_to_bytes_refuses_nan_and_infinity_which_json_does_not_allow(value):
    message = MeasurementMessage(message_id="m-1", value=value)

    with pytest.raises(ValueError, match="not JSON compliant"):
        message.to_bytes()


def test_serialization_failures_are_logged_only_at_debug(caplog):
    # The caller logs the ERROR with its context, so a failure is not reported twice
    caplog.set_level("DEBUG", logger="pykicak.messages")

    with pytest.raises(TypeError):
        MeasurementMessage(message_id="m-1", value=object()).to_bytes()
    with pytest.raises(ValueError):
        SampleMessage.from_bytes(b"not json")

    records = [record for record in caplog.records if record.name == "pykicak.messages"]
    assert [(r.levelname, r.getMessage(), r.exc_info is not None) for r in records] == [
        ("DEBUG", "Failed to serialize message type=MeasurementMessage", True),
        ("DEBUG", "Invalid message for type=SampleMessage", True),
    ]


def test_to_bytes_accepts_finite_floats():
    message = MeasurementMessage(message_id="m-1", value=[0.5, -1e308, 1e308])

    assert MeasurementMessage.from_bytes(message.to_bytes()) == message


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_from_bytes_refuses_nan_and_infinity_which_json_does_not_allow(constant):
    body = f'{{"message_id": "m-1", "value": {constant}}}'.encode()

    with pytest.raises(ValueError, match=f"{constant} is not valid JSON"):
        MeasurementMessage.from_bytes(body)


def test_from_bytes_refuses_a_number_too_large_for_a_float():
    with pytest.raises(ValueError, match="out of range for a float") as error:
        MeasurementMessage.from_bytes(b'{"message_id": "m-1", "value": [1.5, 1e999]}')

    assert "1e999" not in str(error.value)  # the payload stays out of the error


def test_from_bytes_raises_value_error_on_json_nested_too_deeply():
    depth = 1_000_000  # beyond the decoder's recursion limit on every supported Python version

    with pytest.raises(ValueError, match="nested too deeply"):
        SampleMessage.from_bytes(b"[" * depth + b"]" * depth)


def test_from_bytes_raises_on_unexpected_fields():
    with pytest.raises(TypeError):
        SampleMessage.from_bytes(
            b'{"message_id": "m-1", "name": "hello", "count": 3, "extra": true}'
        )


@dataclasses.dataclass(frozen=True, slots=True)
class DefaultedMessage(KicakMessage):
    kicak_type = "defaulted.v1"
    text: str = "default"


def test_from_bytes_refuses_a_body_without_a_field_that_has_a_default():
    # Filling in the default would accept a body that never carried the field, e.g. from an
    # older version of the producer, which a new version of a message must not be mistaken for
    with pytest.raises(TypeError, match="^JSON body does not match DefaultedMessage: missing"):
        DefaultedMessage.from_bytes(b'{"message_id": "m-1"}')


def test_from_bytes_names_missing_fields_but_only_counts_unexpected_ones():
    body = b'{"message_id": "m-1", "count": 3, "SECRET_FIELD": 1, "another": 2}'

    with pytest.raises(TypeError) as raised:
        SampleMessage.from_bytes(body)

    # A field name in the body is the sender's, and may hold anything, so it is not repeated
    assert str(raised.value) == (
        "JSON body does not match SampleMessage: missing field(s) name and 2 unexpected field(s)"
    )
    assert raised.value.__context__ is None


@pytest.mark.parametrize("body", [b"[1, 2]", b'"text"', b"3", b"null"])
def test_from_bytes_refuses_a_body_that_is_not_a_json_object(body):
    with pytest.raises(TypeError, match="^JSON body must be an object, got "):
        SampleMessage.from_bytes(body)


def test_message_id_is_required():
    with pytest.raises(TypeError, match="message_id"):
        SampleMessage(name="hello", count=3)  # type: ignore[call-arg]


def test_message_id_is_keyword_only():
    with pytest.raises(TypeError):
        SampleMessage("hello", 3, "m-1")  # type: ignore[call-arg]


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


# --- serialization by the json module, not dataclasses.asdict() -----------------------------


class Color(enum.StrEnum):
    RED = "red"


class Priority(enum.IntEnum):
    HIGH = 2


@dataclasses.dataclass(frozen=True, slots=True)
class DocumentChunk(KicakMessage):
    document_id: str
    index: int
    score: float
    final: bool
    parent: str | None
    text: str
    tags: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True, slots=True)
class Point:
    x: int
    y: int


@dataclasses.dataclass
class Label:  # neither frozen nor slotted
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class Shape:
    name: str
    points: tuple[Point, ...]
    style: dict[str, object]


@dataclasses.dataclass(frozen=True, slots=True)
class Drawing(KicakMessage):
    shape: Shape
    color: Color
    priority: Priority


# dataclasses.asdict(), which to_bytes() used before, recursed in Python and stopped at about 500
# levels on Python 3.12 and 1000 on 3.14; the json module decodes and encodes about 10,000
DEEP_NESTING = 5000


@pytest.mark.parametrize(
    ("opening", "innermost", "closing"),
    [(b"[", b"", b"]"), (b'{"a": ', b"1", b"}")],
    ids=["lists", "dicts"],
)
def test_to_bytes_serializes_nesting_far_deeper_than_asdict_could(opening, innermost, closing):
    value = opening * DEEP_NESTING + innermost + closing * DEEP_NESTING
    body = b'{"message_id": "m-1", "value": ' + value + b"}"

    message = MeasurementMessage.from_bytes(body)

    assert message.to_bytes() == body


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        pytest.param(
            SampleMessage(message_id="m-1", name="hello", count=3),
            b'{"message_id": "m-1", "name": "hello", "count": 3}',
            id="flat",
        ),
        pytest.param(
            DocumentChunk(
                message_id="chunk-1",
                document_id="doc-7",
                index=-7,
                score=0.1,
                final=True,
                parent=None,
                text='zażółć "quoted" \\ back\nslash\t\x00 \U0001f600 \ud800',
                tags=("a", "b"),
            ),
            b'{"message_id": "chunk-1", "document_id": "doc-7", "index": -7, "score": 0.1, '
            b'"final": true, "parent": null, "text": "za\\u017c\\u00f3\\u0142\\u0107 '
            b'\\"quoted\\" \\\\ back\\nslash\\t\\u0000 \\ud83d'
            b'\\ude00 \\ud800", "tags": ["a", "b"]}',
            id="text-and-escapes",
        ),
        pytest.param(
            MeasurementMessage(
                message_id="numbers",
                value=[1e-07, 1e300, -0.0, 0.1, 12345678901234567890, -1, 0],
            ),
            b'{"message_id": "numbers", "value": '
            b"[1e-07, 1e+300, -0.0, 0.1, 12345678901234567890, -1, 0]}",
            id="numbers",
        ),
        pytest.param(
            MeasurementMessage(
                message_id="containers",
                value={
                    "nested": [1, [2, {"k": None}]],
                    "empty_list": [],
                    "empty_dict": {},
                    "tuple": (1, 2),
                    7: "int key",
                    2.5: "float key",
                    None: "none key",
                    True: "bool key",
                },
            ),
            b'{"message_id": "containers", "value": {"nested": [1, [2, {"k": null}]], '
            b'"empty_list": [], "empty_dict": {}, "tuple": [1, 2], "7": "int key", '
            b'"2.5": "float key", "null": "none key", "true": "bool key"}}',
            id="containers",
        ),
        pytest.param(
            Drawing(
                message_id="drawing-1",
                shape=Shape(
                    name="triangle", points=(Point(x=0, y=0), Point(x=1, y=0)), style={"width": 2}
                ),
                color=Color.RED,
                priority=Priority.HIGH,
            ),
            b'{"message_id": "drawing-1", "shape": {"name": "triangle", "points": '
            b'[{"x": 0, "y": 0}, {"x": 1, "y": 0}], "style": {"width": 2}}, "color": "red", '
            b'"priority": 2}',
            id="nested-dataclass-and-enums",
        ),
    ],
)
def test_to_bytes_produces_the_same_bytes_as_before(message, expected):
    # Recorded with the dataclasses.asdict() implementation that to_bytes() used before
    assert message.to_bytes() == expected


def test_to_bytes_serializes_dataclasses_nested_in_lists_tuples_and_dicts():
    message = MeasurementMessage(
        message_id="m-1",
        value={
            "list": [Point(x=1, y=2), [Point(x=3, y=4)]],
            "tuple": (Point(x=5, y=6), (Label(text="deep"),)),
            "dict": {"point": Point(x=7, y=8), "labels": {"first": Label(text="a")}},
            "shape": Shape(
                name="line",
                points=(Point(x=0, y=0),),
                style={"label": Label(text="b"), "dashes": [Label(text="c")]},
            ),
        },
    )

    body = message.to_bytes()

    assert json.loads(body) == {
        "message_id": "m-1",
        "value": {
            "list": [{"x": 1, "y": 2}, [{"x": 3, "y": 4}]],
            "tuple": [{"x": 5, "y": 6}, [{"text": "deep"}]],
            "dict": {"point": {"x": 7, "y": 8}, "labels": {"first": {"text": "a"}}},
            "shape": {
                "name": "line",
                "points": [{"x": 0, "y": 0}],
                "style": {"label": {"text": "b"}, "dashes": [{"text": "c"}]},
            },
        },
    }
    # The same bytes as the dataclasses.asdict() implementation that to_bytes() used before
    assert body == json.dumps(dataclasses.asdict(message)).encode("utf-8")


@pytest.mark.parametrize(
    "value",
    [object(), Point, {1, 2}, [Label], {"key": object()}, {Point(x=1, y=2): "key"}],
    ids=["object", "dataclass-class", "set", "class-in-list", "object-in-dict", "dataclass-key"],
)
def test_to_bytes_still_refuses_values_json_cannot_hold(value):
    with pytest.raises(TypeError):
        MeasurementMessage(message_id="m-1", value=value).to_bytes()


@pytest.mark.parametrize(
    "value",
    [
        b'"' + "zażółć \U0001f600".encode() + b'"',
        b'"\\ud83d' + b'\\ude00"',
        b'"\\ud800"',
        b'"\\udfff"',
        b'"\\u0000\\u001f\\n"',
    ],
    ids=["raw-utf8", "surrogate-pair", "lone-high-surrogate", "lone-low-surrogate", "control"],
)
def test_every_decoded_string_is_serialized_again_in_ascii(value):
    message = MeasurementMessage.from_bytes(b'{"message_id": "m-1", "value": ' + value + b"}")

    body = message.to_bytes()

    assert body.isascii()
    assert MeasurementMessage.from_bytes(body) == message


# --- type names: kicak_type ---------------------------------------------------------------


@dataclasses.dataclass(frozen=True, slots=True)
class OrderV1(KicakMessage):
    kicak_type = "order.v1"
    item: str


@dataclasses.dataclass(frozen=True, slots=True)
class AnnotatedOrder(KicakMessage):
    kicak_type: ClassVar[str] = "order.annotated"
    item: str


@dataclasses.dataclass(frozen=True, slots=True)
class OrderV2(OrderV1):
    kicak_type = "order.v2"
    quantity: int


@dataclasses.dataclass(frozen=True, slots=True)
class UnnamedOrder(OrderV1):
    quantity: int


@dataclasses.dataclass(frozen=True, slots=True)
class OrderWithTypeField(KicakMessage):
    # An annotation makes it a field, sent in the body
    kicak_type: str = "order.field"  # type: ignore[misc]


class PlainOrder(OrderV1):
    """A subclass without its own @dataclass decorator still names its own type."""

    kicak_type = "order.plain"


def test_type_name_is_the_class_attribute_of_the_class_itself():
    assert _type_name_of(OrderV1) == "order.v1"
    assert _type_name_of(AnnotatedOrder) == "order.annotated"
    assert _type_name_of(OrderV2) == "order.v2"
    assert _type_name_of(PlainOrder) == "order.plain"


def test_type_name_is_not_a_field_and_not_in_the_body():
    order = OrderV2(message_id="o-1", item="book", quantity=2)

    assert [field.name for field in dataclasses.fields(order)] == ["message_id", "item", "quantity"]
    assert order.to_bytes() == b'{"message_id": "o-1", "item": "book", "quantity": 2}'
    assert OrderV2.from_bytes(order.to_bytes()) == order


def test_type_name_must_be_declared_by_the_class_itself():
    with pytest.raises(TypeError, match="UnnamedOrder must declare its own kicak_type") as raised:
        _type_name_of(UnnamedOrder)

    assert "the inherited 'order.v1' names another type" in str(raised.value)


@dataclasses.dataclass(frozen=True, slots=True)
class OrderWithDerivedField(KicakMessage):
    kicak_type = "order.derived"
    item: str
    priority: int = dataclasses.field(init=False, default=1)


def test_type_name_refuses_a_class_with_an_init_false_field():
    # to_bytes() sends the field, and from_bytes() could not give it to the constructor, so the
    # Executor receiving it would dead-letter every message of the class
    with pytest.raises(TypeError, match="OrderWithDerivedField declares priority with init=False"):
        _type_name_of(OrderWithDerivedField)


def test_type_name_must_not_be_a_field():
    with pytest.raises(TypeError, match="declares kicak_type as a field"):
        _type_name_of(OrderWithTypeField)


@pytest.mark.parametrize(
    "candidate",
    [KicakMessage, OrderV1(message_id="o-1", item="book"), dict, None, "order.v1"],
    ids=["base-class", "instance", "other-class", "none", "string"],
)
def test_type_name_requires_a_message_subclass(candidate):
    with pytest.raises(TypeError, match="Expected a KicakMessage subclass"):
        _type_name_of(candidate)


@pytest.mark.parametrize(
    ("type_name", "error", "match"),
    [
        (42, TypeError, "must be a str, got int"),
        (b"order.v1", TypeError, "must be a str, got bytes"),
        ("", ValueError, "must not be empty"),
        ("x" * 256, ValueError, "at most 255 bytes"),
        ("ą" * 128, ValueError, "at most 255 bytes"),
    ],
    ids=["int", "bytes", "empty", "256-ascii", "256-utf8-bytes"],
)
def test_type_name_must_be_a_non_empty_string_of_at_most_255_bytes(type_name, error, match):
    named = type("Named", (OrderV1,), {"kicak_type": type_name})

    with pytest.raises(error, match=match):
        _type_name_of(named)


def test_type_name_accepts_255_bytes():
    named = type("Named", (OrderV1,), {"kicak_type": "ą" * 127 + "x"})

    assert _type_name_of(named) == "ą" * 127 + "x"
