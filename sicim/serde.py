"""Serialization helpers.

Everything that crosses a durability boundary (workflow inputs, step results,
signal payloads, journal event payloads) must round-trip through JSON. To keep
replay honest, live executions also see the *round-tripped* value — what you
get on the first run is exactly what you will get on replay.
"""

from __future__ import annotations

import json
from typing import Any

from .errors import SerializationError


def encode(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise SerializationError(
            f"value of type {type(value).__name__!r} is not JSON-serializable: {exc}. "
            "Durable values (workflow inputs, step results, signal payloads) must be "
            "JSON-native (dict/list/str/int/float/bool/None)."
        ) from exc


def decode(text: str) -> Any:
    return json.loads(text)


def roundtrip(value: Any) -> Any:
    """Encode+decode so the live path returns exactly what replay will return."""
    if value is None:
        return None
    return decode(encode(value))


def preview(value: Any, limit: int = 300) -> str:
    """Best-effort repr for observability. Never raises, never used for replay."""
    try:
        text = repr(value)
    except Exception:  # noqa: BLE001 - repr of arbitrary objects may do anything
        text = f"<unrepresentable {type(value).__name__}>"
    if len(text) > limit:
        return text[: limit - 1] + "…"
    return text


def error_info(exc: BaseException) -> dict[str, str]:
    return {"type": type(exc).__name__, "message": str(exc)}
