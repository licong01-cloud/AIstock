from __future__ import annotations

import dataclasses
import hashlib
import json
import math
import unicodedata
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping

from .errors import CanonicalizationError


CANONICAL_JSON_VERSION = "dataset_release_canonical_json_v1"
CANONICAL_FIELDS_VERSION = "dataset_release_length_prefixed_fields_v1"


def _decimal_text(value: Decimal) -> str:
    if not value.is_finite():
        raise CanonicalizationError("non-finite Decimal is forbidden")
    if value == 0:
        return "0"
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _normalize(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        value = dataclasses.asdict(value)
    if isinstance(value, Enum):
        return _normalize(value.value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalizationError("non-finite float is forbidden")
        return value
    if isinstance(value, Decimal):
        return {"$decimal": _decimal_text(value)}
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise CanonicalizationError("naive datetime is forbidden")
        utc = value.astimezone(timezone.utc).replace(microsecond=value.microsecond)
        return {"$datetime_utc": utc.isoformat().replace("+00:00", "Z")}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, bytes):
        return {"$bytes_hex": value.hex()}
    if isinstance(value, Path):
        return {"$path": value.as_posix()}
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key)
            if key in normalized:
                raise CanonicalizationError(f"mapping key collision after string conversion: {key}")
            normalized[key] = _normalize(raw_value)
        return {key: normalized[key] for key in sorted(normalized)}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized_items = [_normalize(item) for item in value]
        return sorted(normalized_items, key=lambda item: canonical_json_bytes(item))
    raise CanonicalizationError(f"unsupported canonical value type: {type(value).__name__}")


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        _normalize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(value: bytes | str) -> str:
    payload = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(payload).hexdigest()


def ensure_sha256(value: str, *, field: str) -> str:
    text = str(value).strip().lower()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise CanonicalizationError(f"{field} must be a lowercase SHA-256 hex digest")
    return text


def _length_prefix(payload: bytes) -> bytes:
    return len(payload).to_bytes(8, byteorder="big", signed=False) + payload


def encode_named_fields(
    schema: str,
    fields: Mapping[str, Any] | Iterable[tuple[str, Any]],
) -> bytes:
    """Encode named identity fields without delimiter or concatenation ambiguity."""

    items = list(fields.items()) if isinstance(fields, Mapping) else list(fields)
    names = [str(name) for name, _ in items]
    if len(names) != len(set(names)):
        raise CanonicalizationError("duplicate canonical field name")
    items = sorted(((str(name), value) for name, value in items), key=lambda item: item[0])
    output = bytearray()
    output.extend(_length_prefix(CANONICAL_FIELDS_VERSION.encode("utf-8")))
    output.extend(_length_prefix(str(schema).encode("utf-8")))
    output.extend(len(items).to_bytes(4, byteorder="big", signed=False))
    for name, value in items:
        normalized_name = unicodedata.normalize("NFC", name).encode("utf-8")
        output.extend(_length_prefix(normalized_name))
        output.extend(_length_prefix(canonical_json_bytes(value)))
    return bytes(output)


def digest_named_fields(
    schema: str,
    fields: Mapping[str, Any] | Iterable[tuple[str, Any]],
) -> str:
    return sha256_hex(encode_named_fields(schema, fields))


def normalize_root_relative_path(value: str | Path) -> str:
    raw = unicodedata.normalize("NFC", str(value).replace("\\", "/")).strip()
    path = PurePosixPath(raw)
    drive_like = bool(path.parts and len(path.parts[0]) == 2 and path.parts[0][1] == ":")
    if (
        not raw
        or not path.parts
        or path.is_absolute()
        or drive_like
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise CanonicalizationError(f"path must be a normalized root-relative path: {value!r}")
    # Candidate roots are restricted to fixed Windows volumes, whose path identity is case-insensitive.
    return "/".join(part.casefold() for part in path.parts)


def merkle_root_from_named_digests(
    schema: str,
    leaves: Iterable[tuple[str, str]],
) -> str:
    ordered = sorted((str(name), ensure_sha256(digest, field=name)) for name, digest in leaves)
    return digest_named_fields(
        schema,
        {"leaves": [{"name": name, "digest": digest} for name, digest in ordered]},
    )
