"""Safe, bounded presentation policy for read-only QE asset inspection."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from .errors import (
    QEAssetContentInvalidError,
    QEAssetContentUnsupportedError,
    redact_public_text,
)

TEXT_EXTENSIONS = frozenset({".txt", ".log", ".md", ".csv", ".json", ".yaml", ".yml"})
JSON_EXTENSIONS = frozenset({".json"})
_SECRET_MARKERS = (
    "password",
    "token",
    "secret",
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "credential",
)


@dataclass(frozen=True)
class SanitizedAssetText:
    text: str
    schema_kind: str
    redaction_count: int


def require_text_asset(*, relative_path: str, content_type: str) -> str:
    suffix = PurePosixPath(relative_path).suffix.lower()
    normalized_type = str(content_type or "").split(";", 1)[0].strip().lower()
    if (
        normalized_type.startswith("text/")
        or normalized_type
        in {
            "application/json",
            "application/yaml",
            "application/x-yaml",
        }
        or suffix in TEXT_EXTENSIONS
    ):
        return suffix
    raise QEAssetContentUnsupportedError(
        "QE asset content inspection supports bounded text assets only",
        context={
            "relative_path": relative_path,
            "content_type": normalized_type or "application/octet-stream",
            "allowed_extensions": sorted(TEXT_EXTENSIONS),
        },
    )


def sanitize_asset_text(
    data: bytes,
    *,
    relative_path: str,
    content_type: str,
    partial: bool = False,
) -> SanitizedAssetText:
    suffix = require_text_asset(relative_path=relative_path, content_type=content_type)
    try:
        raw_text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise QEAssetContentInvalidError(
            "QE text asset is not valid UTF-8",
            context={"relative_path": relative_path},
        ) from exc

    if not partial and (
        suffix in JSON_EXTENSIONS or str(content_type or "").split(";", 1)[0].strip().lower() == "application/json"
    ):
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise QEAssetContentInvalidError(
                "QE JSON asset is invalid",
                context={"relative_path": relative_path, "line": exc.lineno, "column": exc.colno},
            ) from exc
        sanitized, count = _sanitize_value(payload)
        return SanitizedAssetText(
            text=json.dumps(sanitized, ensure_ascii=False, indent=2),
            schema_kind="json",
            redaction_count=count,
        )

    text, count = redact_public_text(raw_text)
    return SanitizedAssetText(
        text=text,
        schema_kind="partial_text" if partial else "text",
        redaction_count=count,
    )


def _sanitize_value(value: Any) -> tuple[Any, int]:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        redactions = 0
        for raw_key, nested in value.items():
            key = str(raw_key)
            if any(marker in key.lower() for marker in _SECRET_MARKERS):
                result[key] = "<redacted>"
                redactions += 1
            else:
                result[key], nested_count = _sanitize_value(nested)
                redactions += nested_count
        return result, redactions
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        items: list[Any] = []
        redactions = 0
        for item in value:
            sanitized, nested_count = _sanitize_value(item)
            items.append(sanitized)
            redactions += nested_count
        return items, redactions
    if isinstance(value, str):
        return redact_public_text(value)
    return value, 0
