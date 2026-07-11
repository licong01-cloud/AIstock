"""Fail-loud configuration, pseudonym, and cursor primitives for TCA reads.

This module deliberately owns no database mutation and no broker interaction.
Read APIs, evidence export, and the default-off EOD observation hook share its
version/configuration and HMAC identity contracts.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Mapping

from backend.services.trading_core.errors import TradingCoreError

from .tca_models import canonical_json_sha256


TCA_READ_SCHEMA_VERSION = "miniqmt_execution_tca_read_v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ACTIVE_VERSION_FIELDS = (
    "calculator_version",
    "formula_version",
    "schema_version",
    "query_version",
    "benchmark_policy_version",
    "mark_policy_version",
    "fee_policy_version",
    "trade_provenance_policy_version",
)


class TcaReadError(TradingCoreError):
    """Stable read/export error with an explicit HTTP mapping for adapters."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        http_status: int,
        stage: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        details = {"reason_code": reason_code, "stage": stage, **dict(context or {})}
        super().__init__(message, context=details)
        self.error_code = reason_code
        self.http_status = http_status
        self.reason_code = reason_code
        self.stage = stage


@dataclass(frozen=True, slots=True)
class TcaActiveReadVersion:
    """Content-addressed version tuple required for implicit TCA result reads."""

    calculator_version: str
    formula_version: str
    schema_version: str
    query_version: str
    benchmark_policy_version: str
    mark_policy_version: str
    fee_policy_version: str
    trade_provenance_policy_version: str
    config_sha256: str

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "TcaActiveReadVersion":
        values: dict[str, str] = {}
        missing: list[str] = []
        for field_name in _ACTIVE_VERSION_FIELDS:
            value = str(raw.get(field_name) or "").strip()
            if not value:
                missing.append(field_name)
            else:
                values[field_name] = value
        supplied_hash = str(raw.get("config_sha256") or "").strip().lower()
        if missing or not _SHA256_RE.fullmatch(supplied_hash):
            raise TcaReadError(
                "ADAPTIVE_IS_TCA_ACTIVE_READ_VERSION_INVALID",
                "MINIQMT_TCA_ACTIVE_READ_VERSION must contain a complete version tuple and config_sha256",
                http_status=503,
                stage="TCA_READ_CONFIG",
                context={"missing_fields": missing, "has_valid_config_sha256": bool(_SHA256_RE.fullmatch(supplied_hash))},
            )
        expected_hash = canonical_json_sha256(values)
        if not hmac.compare_digest(supplied_hash, expected_hash):
            raise TcaReadError(
                "ADAPTIVE_IS_TCA_ACTIVE_READ_VERSION_HASH_MISMATCH",
                "MINIQMT_TCA_ACTIVE_READ_VERSION config_sha256 does not match its version tuple",
                http_status=503,
                stage="TCA_READ_CONFIG",
            )
        return cls(config_sha256=supplied_hash, **values)

    def as_mapping(self) -> dict[str, str]:
        return {field_name: str(getattr(self, field_name)) for field_name in _ACTIVE_VERSION_FIELDS}


@dataclass(frozen=True, slots=True)
class AccountPseudonymizer:
    """Stable, key-versioned HMAC account pseudonymizer with no raw fallback."""

    key: bytes
    key_version: str

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("HMAC key must not be empty")
        if not self.key_version.strip():
            raise ValueError("HMAC key version must not be empty")

    def pseudonymize(self, account_id: str) -> str:
        normalized = str(account_id or "").strip()
        if not normalized:
            raise TcaReadError(
                "ADAPTIVE_IS_TCA_ACCOUNT_ID_MISSING",
                "account identity is required before it can be pseudonymized",
                http_status=500,
                stage="TCA_READ_PSEUDONYM",
            )
        digest = hmac.new(self.key, normalized.encode("utf-8"), hashlib.sha256).hexdigest()
        return f"acct_{self.key_version}_{digest[:32]}"


@dataclass(frozen=True, slots=True)
class TcaReadRuntimeConfig:
    """Environment-backed read configuration; no implicit version or key fallback."""

    active_read_version: TcaActiveReadVersion | None
    pseudonymizer: AccountPseudonymizer | None
    eod_observation_enabled: bool

    @classmethod
    def from_environ(cls, environ: Mapping[str, str] | None = None) -> "TcaReadRuntimeConfig":
        values = os.environ if environ is None else environ
        active_raw = str(values.get("MINIQMT_TCA_ACTIVE_READ_VERSION") or "").strip()
        if active_raw:
            try:
                decoded = json.loads(active_raw)
            except json.JSONDecodeError as exc:
                raise TcaReadError(
                    "ADAPTIVE_IS_TCA_ACTIVE_READ_VERSION_INVALID",
                    "MINIQMT_TCA_ACTIVE_READ_VERSION must be a JSON object",
                    http_status=503,
                    stage="TCA_READ_CONFIG",
                ) from exc
            if not isinstance(decoded, Mapping):
                raise TcaReadError(
                    "ADAPTIVE_IS_TCA_ACTIVE_READ_VERSION_INVALID",
                    "MINIQMT_TCA_ACTIVE_READ_VERSION must be a JSON object",
                    http_status=503,
                    stage="TCA_READ_CONFIG",
                )
            active_version = TcaActiveReadVersion.from_mapping(decoded)
        else:
            active_version = None

        raw_key = str(values.get("AISTOCK_TCA_EXPORT_HMAC_KEY") or "")
        key_version = str(values.get("AISTOCK_TCA_EXPORT_HMAC_KEY_VERSION") or "").strip()
        if raw_key and key_version:
            pseudonymizer: AccountPseudonymizer | None = AccountPseudonymizer(
                key=raw_key.encode("utf-8"), key_version=key_version
            )
        elif raw_key or key_version:
            raise TcaReadError(
                "ADAPTIVE_IS_TCA_EXPORT_IDENTITY_CONFIG_INVALID",
                "AISTOCK_TCA_EXPORT_HMAC_KEY and AISTOCK_TCA_EXPORT_HMAC_KEY_VERSION must be configured together",
                http_status=503,
                stage="TCA_READ_CONFIG",
                context={"has_key": bool(raw_key), "has_key_version": bool(key_version)},
            )
        else:
            pseudonymizer = None

        raw_enabled = str(values.get("MINIQMT_TCA_EOD_OBSERVATION_ENABLED") or "false").strip().lower()
        if raw_enabled not in {"true", "false"}:
            raise TcaReadError(
                "ADAPTIVE_IS_TCA_EOD_OBSERVATION_CONFIG_INVALID",
                "MINIQMT_TCA_EOD_OBSERVATION_ENABLED must be true or false",
                http_status=503,
                stage="TCA_EOD_CONFIG",
            )
        return cls(
            active_read_version=active_version,
            pseudonymizer=pseudonymizer,
            eod_observation_enabled=raw_enabled == "true",
        )

    def require_active_read_version(self) -> TcaActiveReadVersion:
        if self.active_read_version is None:
            raise TcaReadError(
                "ADAPTIVE_IS_TCA_ACTIVE_READ_VERSION_MISSING",
                "implicit TCA reads require MINIQMT_TCA_ACTIVE_READ_VERSION",
                http_status=503,
                stage="TCA_READ_CONFIG",
            )
        return self.active_read_version

    def require_pseudonymizer(self) -> AccountPseudonymizer:
        if self.pseudonymizer is None:
            raise TcaReadError(
                "ADAPTIVE_IS_TCA_EXPORT_IDENTITY_UNAVAILABLE",
                "TCA reads and evidence export require the configured HMAC key and key version",
                http_status=503,
                stage="TCA_READ_PSEUDONYM",
            )
        return self.pseudonymizer


@dataclass(frozen=True, slots=True)
class TcaKeysetCursorCodec:
    """HMAC-signed opaque cursor bound to a schema and normalized filter hash."""

    pseudonymizer: AccountPseudonymizer
    schema_version: str = TCA_READ_SCHEMA_VERSION

    def encode(self, *, last_key: tuple[str, str, int], filter_sha256: str) -> str:
        if not _SHA256_RE.fullmatch(filter_sha256):
            raise ValueError("filter_sha256 must be lowercase sha256")
        payload = {
            "schema_version": self.schema_version,
            "filter_sha256": filter_sha256,
            "last_key": [last_key[0], last_key[1], int(last_key[2])],
        }
        encoded_payload = _base64url(_canonical_bytes(payload))
        signature = hmac.new(self.pseudonymizer.key, encoded_payload.encode("ascii"), hashlib.sha256).digest()
        return f"{encoded_payload}.{_base64url(signature)}"

    def decode(self, *, cursor: str, expected_filter_sha256: str) -> tuple[str, str, int]:
        parts = str(cursor or "").split(".")
        if len(parts) != 2:
            raise _invalid_cursor("cursor must have payload and signature")
        encoded_payload, encoded_signature = parts
        try:
            supplied_signature = _base64url_decode(encoded_signature)
        except ValueError as exc:
            raise _invalid_cursor("cursor signature is not base64url") from exc
        expected_signature = hmac.new(self.pseudonymizer.key, encoded_payload.encode("ascii"), hashlib.sha256).digest()
        if not hmac.compare_digest(supplied_signature, expected_signature):
            raise _invalid_cursor("cursor signature does not match")
        try:
            decoded = json.loads(_base64url_decode(encoded_payload).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise _invalid_cursor("cursor payload is invalid") from exc
        if not isinstance(decoded, Mapping):
            raise _invalid_cursor("cursor payload must be an object")
        if decoded.get("schema_version") != self.schema_version:
            raise _invalid_cursor("cursor schema version does not match")
        if not hmac.compare_digest(str(decoded.get("filter_sha256") or ""), expected_filter_sha256):
            raise _invalid_cursor("cursor filter does not match")
        raw_key = decoded.get("last_key")
        if not isinstance(raw_key, list) or len(raw_key) != 3:
            raise _invalid_cursor("cursor last key is invalid")
        trade_date, parent_intent_id, parent_revision = raw_key
        if not isinstance(trade_date, str) or not isinstance(parent_intent_id, str):
            raise _invalid_cursor("cursor last key is invalid")
        try:
            revision = int(parent_revision)
        except (TypeError, ValueError) as exc:
            raise _invalid_cursor("cursor parent revision is invalid") from exc
        if revision <= 0:
            raise _invalid_cursor("cursor parent revision is invalid")
        return trade_date, parent_intent_id, revision


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _base64url_decode(value: str) -> bytes:
    if not value or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("invalid base64url")
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(f"{value}{padding}")


def _invalid_cursor(message: str) -> TcaReadError:
    return TcaReadError(
        "ADAPTIVE_IS_TCA_CURSOR_INVALID",
        message,
        http_status=400,
        stage="TCA_READ_CURSOR",
    )
