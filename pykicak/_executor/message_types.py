"""The message types an Executor class declares in its class header, and what `execute()` returns."""

from __future__ import annotations

from collections.abc import Sequence
from types import NoneType
from typing import cast

from pykicak.abstracts import _type_arguments, _union_members
from pykicak.messages import KicakMessage, _type_name_of

_EXECUTOR_TYPES_USAGE = (
    "KicakExecutorAbstract[InputMessage, ResultMessage], with a union such as "
    "InputV1 | InputV2 to accept several input types, or KicakExecutorAbstract[InputMessage, None] "
    "for a terminator"
)


def _declared_message_types(
    executor_class: type, generic_class: type
) -> tuple[dict[str, type[KicakMessage]], type[KicakMessage] | None]:
    """Return the input types an Executor class accepts, by type name, and its result type.

    Both come from its class header, `KicakExecutorAbstract[Accepted, Result]`, where
    `generic_class` is `KicakExecutorAbstract`: `Accepted` is a message class or a union of
    several, and `Result` one message class, or None for a terminator. Raises `TypeError` for
    anything else, and for two accepted classes that declare the same type name.
    """
    input_argument, result_argument = _type_arguments(
        executor_class, generic_class, usage=_EXECUTOR_TYPES_USAGE
    )
    name = executor_class.__name__
    accepted: dict[str, type[KicakMessage]] = {}
    for member in _union_members(input_argument):
        if member is NoneType:
            raise TypeError(f"{name} must accept message types only, not None")
        type_name = _type_name_of(member)
        message_class = cast(type[KicakMessage], member)
        if type_name in accepted:
            raise TypeError(
                f"{name} accepts {accepted[type_name].__name__} and {message_class.__name__}, "
                f"which declare the same kicak_type {type_name!r}"
            )
        accepted[type_name] = message_class
    results = _union_members(result_argument)
    if len(results) > 1:
        raise TypeError(
            f"{name} must declare one result type, or None for a terminator, "
            f"got {result_argument!r}"
        )
    if results[0] is NoneType:
        return accepted, None
    _type_name_of(results[0])
    return accepted, cast(type[KicakMessage], results[0])


def _as_messages(results: object) -> tuple[KicakMessage, ...]:
    """Normalize the value returned by execute() to a tuple of messages."""
    if results is None:
        return ()
    if isinstance(results, KicakMessage):
        return (results,)
    messages = tuple(cast(Sequence[object], results))
    for message in messages:
        if not isinstance(message, KicakMessage):
            raise TypeError(
                f"execute() must return KicakMessage instances, got {type(message).__name__}"
            )
    return cast(tuple[KicakMessage, ...], messages)
