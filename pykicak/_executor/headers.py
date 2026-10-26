"""What an Executor reads from a delivery's properties, and writes on the copies it publishes."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

import pika

from pykicak._executor.status import _DeadLetterReason
from pykicak.abstracts import _ORIGINAL_PUBLISHER_HEADER, _PUBLISHED_AT_HEADER

_RETRY_ROUND_HEADER = "x-kicak-retry-round"
_RETRYING_SINCE_HEADER = "x-kicak-retrying-since"
_RETRY_HEADERS = frozenset({_RETRY_ROUND_HEADER, _RETRYING_SINCE_HEADER})
# Why and when a copy in the dead-letter queue was dead-lettered, and from which queue
_DEAD_LETTER_REASON_HEADER = "x-kicak-dead-letter-reason"
_ERROR_TYPE_HEADER = "x-kicak-error-type"
_ERROR_MESSAGE_HEADER = "x-kicak-error-message"
_DEAD_LETTERED_AT_HEADER = "x-kicak-dead-lettered-at"
_DEAD_LETTERED_FROM_HEADER = "x-kicak-dead-lettered-from"
_DEAD_LETTER_HEADERS = frozenset(
    {
        _DEAD_LETTER_REASON_HEADER,
        _ERROR_TYPE_HEADER,
        _ERROR_MESSAGE_HEADER,
        _DEAD_LETTERED_AT_HEADER,
        _DEAD_LETTERED_FROM_HEADER,
    }
)
# Keeps the message properties well within one frame (frame_max is 128 KiB by default)
_MAX_ERROR_MESSAGE_BYTES = 1024
_TRUNCATION_MARK = "…"
# Headers RabbitMQ sets itself; a copy must not carry stale ones, e.g. a quorum delivery count, or
# the count of requeues that quorum queues keep since RabbitMQ 4.3
_BROKER_HEADERS = frozenset({"x-death", "x-delivery-count", "x-acquired-count"})
_BROKER_HEADER_PREFIXES = ("x-first-death-", "x-last-death-")
# Sender-selected distribution: RabbitMQ also routes a message to the routing keys these name, so
# a copy keeping them, e.g. `CC` naming the source queue, would also reach queues it is not for
_ROUTING_HEADERS = frozenset({"CC", "BCC"})


def _was_dead_lettered(headers: Mapping[str, object] | None) -> bool:
    """Return whether a delivery carries the headers of a dead-lettering by an Executor.

    Only a message sent back from a dead-letter queue does: every copy an Executor makes drops
    them (see `_copied_headers()`).
    """
    return not _DEAD_LETTER_HEADERS.isdisjoint(headers or {})


def _copied_headers(headers: Mapping[str, object] | None) -> dict[str, object]:
    """Return the headers of a delivery that a copy the executor publishes of it keeps.

    RabbitMQ's own headers are dropped, and so are `CC` and `BCC`, which would route the copy to
    other queues too, and the reasons of an earlier dead-lettering, which a message sent back from
    a dead-letter queue still carries, together with its retry headers, because its retry rounds
    start again (see `_retry_round_of()`).
    """
    dropped = _DEAD_LETTER_HEADERS | _ROUTING_HEADERS
    if _was_dead_lettered(headers):
        dropped |= _RETRY_HEADERS
    return {
        name: value
        for name, value in (headers or {}).items()
        if name not in _BROKER_HEADERS
        and not name.startswith(_BROKER_HEADER_PREFIXES)
        and name not in dropped
    }


def _retry_round_of(headers: Mapping[str, object] | None) -> int:
    """Return how often a delivery was moved to the retry queue: its retry round, or 0.

    A missing or invalid `x-kicak-retry-round` header counts as 0, so the rounds start again. So
    does a message sent back from a dead-letter queue, which still carries the retry headers of
    its earlier rounds, so that it gets `max_retry_rounds` rounds again.
    """
    if _was_dead_lettered(headers):
        return 0
    retry_round = (headers or {}).get(_RETRY_ROUND_HEADER)
    if isinstance(retry_round, int) and not isinstance(retry_round, bool) and retry_round >= 0:
        return retry_round
    return 0


def _retrying_since_of(headers: Mapping[str, object] | None) -> str | None:
    """Return when a delivery was first moved to the retry queue, in ISO 8601, or None.

    None for a missing or invalid `x-kicak-retrying-since` header, and for a message sent back
    from a dead-letter queue, whose rounds start again (see `_retry_round_of()`).
    """
    if _was_dead_lettered(headers):
        return None
    since = (headers or {}).get(_RETRYING_SINCE_HEADER)
    if isinstance(since, bytes):
        since = since.decode("utf-8", errors="replace")
    if isinstance(since, str) and _is_iso_datetime(since):
        return since
    return None


def _retry_headers(headers: Mapping[str, object] | None, now: datetime) -> dict[str, object]:
    """Return the headers of a message's copy for the retry queue: its next retry round.

    The application's headers are kept, and RabbitMQ's own ones dropped (see `_copied_headers()`).
    The round counts the moves into the retry queue, and `x-kicak-retrying-since` keeps the time
    of the first one; a missing or invalid value starts them again, and so does a message sent
    back from a dead-letter queue. The copy is published anew, so `x-kicak-published-at` becomes
    `now`, and the `queued_seconds` of its return to the source queue includes the retry delay.
    """
    kept = _copied_headers(headers)
    kept[_PUBLISHED_AT_HEADER] = now.isoformat()
    kept[_RETRY_ROUND_HEADER] = _retry_round_of(headers) + 1
    kept[_RETRYING_SINCE_HEADER] = _retrying_since_of(headers) or now.isoformat()
    return kept


def _dead_letter_headers(
    headers: Mapping[str, object] | None,
    reason: _DeadLetterReason,
    error: BaseException,
    source_queue: str,
    now: datetime,
) -> dict[str, object]:
    """Return the headers of a message's copy for the dead-letter queue: why it is there.

    The application's headers and the retry headers, its history, are kept, and RabbitMQ's own
    ones dropped, and so are the retry headers of a message sent back from a dead-letter queue,
    whose rounds started again (see `_copied_headers()`).
    """
    kept = _copied_headers(headers)
    kept[_DEAD_LETTER_REASON_HEADER] = reason.value
    kept[_ERROR_TYPE_HEADER] = _error_type_name(error)
    kept[_ERROR_MESSAGE_HEADER] = _error_description(error)
    kept[_DEAD_LETTERED_AT_HEADER] = now.isoformat()
    kept[_DEAD_LETTERED_FROM_HEADER] = source_queue
    return kept


def _error_type_name(error: BaseException) -> str:
    """Return the name of the class of `error` with its module, e.g. `json.JSONDecodeError`.

    A built-in exception is named without its module, e.g. `KeyError`.
    """
    error_class = type(error)
    module = error_class.__module__
    if module == "builtins":
        return error_class.__qualname__
    return f"{module}.{error_class.__qualname__}"


def _error_text(error: BaseException) -> str:
    """Return `str(error)`, or, if that raises, a placeholder, as the traceback module does.

    An exception from outside, e.g. raised by a message class's `__post_init__`, may fail in
    `__str__` too, and describing it must not raise another exception in its place.
    """
    try:
        return str(error)
    except Exception:
        return "<exception str() failed>"


def _error_description(error: BaseException) -> str:
    """Return the text of `error` that a header can carry: at most 1024 bytes of valid UTF-8.

    A lone surrogate, which UTF-8 cannot encode and pika would fail on, becomes `?`, and a longer
    text is cut at a character boundary and ends with `…`. An exception whose `str()` raises is
    described as by `_error_text()`.
    """
    encoded = _error_text(error).encode("utf-8", errors="replace")
    if len(encoded) <= _MAX_ERROR_MESSAGE_BYTES:
        return encoded.decode("utf-8")
    kept = encoded[: _MAX_ERROR_MESSAGE_BYTES - len(_TRUNCATION_MARK.encode("utf-8"))]
    # Dropping the bytes of a character the cut split leaves valid UTF-8
    return kept.decode("utf-8", errors="ignore") + _TRUNCATION_MARK


def _text_property(value: object) -> str | None:
    """Return the value of a delivery's text property, or None if it is missing or empty.

    pika delivers a property as bytes if it is not valid UTF-8; it is decoded with replacements.
    """
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return value if isinstance(value, str) and value else None


def _sent_message_id(properties: pika.BasicProperties | None) -> str | None:
    """Return the `message_id` property of a delivery, the ID its sender gave, or None.

    Every pykicak agent sets it to the `message_id` in the body, so it names a delivery whose
    body cannot be decoded; once the body is decoded, its `message_id` is the one used.
    """
    return None if properties is None else _text_property(properties.message_id)


def _sent_type(properties: pika.BasicProperties | None) -> str | None:
    """Return the `type` property of a delivery, the type name its sender gave, or None."""
    return None if properties is None else _text_property(properties.type)


def _original_publisher_of(properties: pika.BasicProperties) -> str | None:
    """Return the user of the agent that created a delivered message, if known.

    The `x-kicak-original-publisher` header, which every pykicak agent sets on the messages it
    creates, comes first: a copy from a retry queue has another `user_id`. A message without the
    header, published by another client, was created by its publisher, so its `user_id` names
    it when the client set one. Otherwise the creator is unknown: None.
    """
    header = (properties.headers or {}).get(_ORIGINAL_PUBLISHER_HEADER)
    if isinstance(header, bytes):
        header = header.decode("utf-8", errors="replace")
    if isinstance(header, str) and header:
        return header
    user_id = properties.user_id
    return user_id if isinstance(user_id, str) and user_id else None


def _copy_properties(
    original: pika.BasicProperties,
    headers: Mapping[str, object],
    *,
    user_id: str | None,
    message_id: object,
    expiration: str | None,
) -> pika.BasicProperties:
    """Return the properties of a copy an executor publishes of a delivery.

    The copy keeps the delivery's application properties, with `headers`, `message_id`, and
    `expiration` (never the delivery's own) as given, and is persistent. Its `user_id` is
    `user_id`, the executor's user, because RabbitMQ refuses any other, so the user who created
    the message is kept in the `x-kicak-original-publisher` header.
    """
    copy_headers = dict(headers)
    original_publisher = _original_publisher_of(original)
    if original_publisher is None:
        copy_headers.pop(_ORIGINAL_PUBLISHER_HEADER, None)  # an invalid value
    else:
        copy_headers[_ORIGINAL_PUBLISHER_HEADER] = original_publisher
    return pika.BasicProperties(
        content_type=original.content_type,
        content_encoding=original.content_encoding,
        headers=copy_headers,
        delivery_mode=2,
        priority=original.priority,
        correlation_id=original.correlation_id,
        reply_to=original.reply_to,
        expiration=expiration,
        message_id=message_id,
        timestamp=original.timestamp,
        type=original.type,
        app_id=original.app_id,
        user_id=user_id,
    )


def _is_iso_datetime(value: str) -> bool:
    """Return whether `value` is an ISO 8601 date and time with a time zone."""
    try:
        return datetime.fromisoformat(value).tzinfo is not None
    except ValueError:
        return False


def _queued_seconds(properties: pika.BasicProperties | None, received_at: datetime) -> float | None:
    """Return how long a delivery waited since it was published, or None if that is unknown.

    From its `x-kicak-published-at` header, which every pykicak agent sets, to `received_at`.
    The two times come from the clocks of two hosts, so a difference between them shows, and
    the result can even be negative; it is not corrected. None for a delivery without the
    header, e.g. from another client, or whose header is not an ISO 8601 time with a time zone.
    """
    published = None if properties is None else (properties.headers or {}).get(_PUBLISHED_AT_HEADER)
    if isinstance(published, bytes):
        published = published.decode("utf-8", errors="replace")
    if not isinstance(published, str):
        return None
    try:
        published_at = datetime.fromisoformat(published)
    except ValueError:
        return None
    if published_at.utcoffset() is None:
        return None
    return (received_at - published_at).total_seconds()
