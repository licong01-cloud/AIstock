from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any, Mapping

from .canonical import canonical_json_bytes
from .errors import DatasetReleaseError


CURSOR_SCHEMA_VERSION = "dataset_release_cursor_v1"
MAX_CURSOR_BYTES = 4096


class CursorInvalid(DatasetReleaseError):
    code = "DATASET_RELEASE_CURSOR_INVALID"


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    if not value or any(character.isspace() for character in value):
        raise CursorInvalid("cursor encoding is invalid")
    padding = "=" * (-len(value) % 4)
    try:
        return base64.b64decode(value + padding, altchars=b"-_", validate=True)
    except (ValueError, TypeError) as exc:
        raise CursorInvalid("cursor encoding is invalid") from exc


@dataclass(frozen=True)
class CursorBinding:
    endpoint: str
    principal: str
    filters: Mapping[str, Any]
    order: str
    generation: str | None = None

    def __post_init__(self) -> None:
        if not self.endpoint.strip() or not self.principal.strip() or not self.order.strip():
            raise CursorInvalid("cursor binding fields must be non-empty")


class CursorCodec:
    """Opaque authenticated cursor bound to one principal and query contract."""

    def __init__(self, signing_key: bytes) -> None:
        key = bytes(signing_key)
        if len(key) < 16:
            raise ValueError("cursor signing key must contain at least 16 bytes")
        self._key = key

    def encode(self, *, binding: CursorBinding, position: Mapping[str, Any]) -> str:
        payload = canonical_json_bytes(
            {
                "schema_version": CURSOR_SCHEMA_VERSION,
                "endpoint": binding.endpoint,
                "principal": binding.principal,
                "filters": dict(binding.filters),
                "order": binding.order,
                "generation": binding.generation,
                "position": dict(position),
            }
        )
        signature = hmac.new(self._key, payload, hashlib.sha256).digest()
        cursor = f"{_b64encode(payload)}.{_b64encode(signature)}"
        if len(cursor.encode("ascii")) > MAX_CURSOR_BYTES:
            raise CursorInvalid("cursor exceeds the bounded size")
        return cursor

    def decode(self, cursor: str, *, binding: CursorBinding) -> dict[str, Any]:
        if len(cursor.encode("utf-8")) > MAX_CURSOR_BYTES or cursor.count(".") != 1:
            raise CursorInvalid("cursor encoding is invalid")
        payload_value, signature_value = cursor.split(".", 1)
        payload = _b64decode(payload_value)
        signature = _b64decode(signature_value)
        expected = hmac.new(self._key, payload, hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            raise CursorInvalid("cursor signature is invalid")
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CursorInvalid("cursor payload is invalid") from exc
        if not isinstance(decoded, dict):
            raise CursorInvalid("cursor payload is invalid")
        expected_binding = json.loads(
            canonical_json_bytes(
                {
                    "schema_version": CURSOR_SCHEMA_VERSION,
                    "endpoint": binding.endpoint,
                    "principal": binding.principal,
                    "filters": dict(binding.filters),
                    "order": binding.order,
                    "generation": binding.generation,
                }
            ).decode("utf-8")
        )
        actual_binding = {name: decoded.get(name) for name in expected_binding}
        if actual_binding != expected_binding:
            raise CursorInvalid("cursor does not match this query")
        position = decoded.get("position")
        if not isinstance(position, dict):
            raise CursorInvalid("cursor position is invalid")
        return dict(position)
