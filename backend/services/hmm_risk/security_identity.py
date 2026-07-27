"""Immutable source-specific security identity resolution for HMM stock facts."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .state_model_set import StateModelSetError, canonical_sha256

SECURITY_IDENTITY_SCHEMA = "hmm_risk_security_source_identity_manifest_v1"
DEFAULT_RESOLUTION = "canonical_same_code"
SUPPORTED_SOURCE_DATASETS = frozenset(
    {
        "market.daily_basic",
        "market.kline_daily_raw",
        "market.moneyflow_ts",
        "market.stk_limit",
    }
)
_TS_CODE_RE = re.compile(r"^[0-9]{6}\.(?:BJ|SH|SZ)$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class SecuritySourceResolution:
    security_identity_id: str
    canonical_ts_code: str
    source_dataset: str
    source_ts_code: str
    effective_start: date | None
    effective_end: date | None
    authority_ref: str
    authority_hash: str
    row_hash: str
    resolution_kind: str

    def evidence(self) -> dict[str, Any]:
        return {
            "security_identity_id": self.security_identity_id,
            "canonical_ts_code": self.canonical_ts_code,
            "source_dataset": self.source_dataset,
            "source_ts_code": self.source_ts_code,
            "effective_start": None if self.effective_start is None else self.effective_start.isoformat(),
            "effective_end": None if self.effective_end is None else self.effective_end.isoformat(),
            "authority_ref": self.authority_ref,
            "authority_hash": self.authority_hash,
            "row_hash": self.row_hash,
            "resolution_kind": self.resolution_kind,
        }


@dataclass(frozen=True)
class SecuritySourceIdentityManifest:
    manifest_version: str
    default_resolution: str
    rows: tuple[SecuritySourceResolution, ...]
    manifest_sha256: str
    rows_sha256: str

    def resolve(self, canonical_ts_code: str, trade_date: date, source_dataset: str) -> SecuritySourceResolution:
        _validate_ts_code(canonical_ts_code, "canonical_ts_code")
        if not isinstance(trade_date, date):
            raise StateModelSetError("hmm_risk_stock_fact_source_identity_unresolved: trade_date is invalid")
        if source_dataset not in SUPPORTED_SOURCE_DATASETS:
            raise StateModelSetError(
                f"hmm_risk_stock_fact_source_identity_unresolved: unsupported source_dataset={source_dataset}"
            )
        matches = [
            row
            for row in self.rows
            if row.canonical_ts_code == canonical_ts_code
            and row.source_dataset == source_dataset
            and row.effective_start is not None
            and row.effective_end is not None
            and row.effective_start <= trade_date <= row.effective_end
        ]
        if len(matches) > 1:
            raise StateModelSetError(
                "hmm_risk_stock_fact_source_identity_ambiguous: "
                f"{canonical_ts_code}/{trade_date}/{source_dataset} resolves to {len(matches)} rows"
            )
        if matches:
            return matches[0]
        if self.default_resolution != DEFAULT_RESOLUTION:
            raise StateModelSetError(
                "hmm_risk_stock_fact_source_identity_unresolved: "
                f"{canonical_ts_code}/{trade_date}/{source_dataset} has no explicit resolution"
            )
        default_body = {
            "default_resolution": self.default_resolution,
            "security_identity_id": f"canonical:{canonical_ts_code}",
            "canonical_ts_code": canonical_ts_code,
            "source_dataset": source_dataset,
            "source_ts_code": canonical_ts_code,
        }
        return SecuritySourceResolution(
            security_identity_id=default_body["security_identity_id"],
            canonical_ts_code=canonical_ts_code,
            source_dataset=source_dataset,
            source_ts_code=canonical_ts_code,
            effective_start=None,
            effective_end=None,
            authority_ref=f"manifest-default:{self.manifest_version}",
            authority_hash=self.manifest_sha256,
            row_hash=canonical_sha256(default_body),
            resolution_kind=DEFAULT_RESOLUTION,
        )

    def alias_rows(self, source_dataset: str) -> list[dict[str, str]]:
        if source_dataset not in SUPPORTED_SOURCE_DATASETS:
            raise StateModelSetError(f"unsupported security identity source dataset: {source_dataset}")
        return [
            {
                "canonical_ts_code": row.canonical_ts_code,
                "source_ts_code": row.source_ts_code,
                "effective_start": row.effective_start.isoformat(),
                "effective_end": row.effective_end.isoformat(),
                "security_identity_id": row.security_identity_id,
                "row_hash": row.row_hash,
            }
            for row in self.rows
            if row.source_dataset == source_dataset
            and row.effective_start is not None
            and row.effective_end is not None
        ]

    def evidence(self) -> dict[str, Any]:
        return {
            "schema_version": SECURITY_IDENTITY_SCHEMA,
            "manifest_version": self.manifest_version,
            "default_resolution": self.default_resolution,
            "row_count": len(self.rows),
            "rows_sha256": self.rows_sha256,
            "manifest_sha256": self.manifest_sha256,
        }


def _validate_ts_code(value: str, field: str) -> None:
    if not isinstance(value, str) or not _TS_CODE_RE.fullmatch(value):
        raise StateModelSetError(f"hmm_risk_stock_fact_source_identity_unresolved: {field} is invalid")


def _parse_date(value: Any, field: str) -> date:
    if not isinstance(value, str):
        raise StateModelSetError(f"hmm_risk_stock_fact_source_identity_unresolved: {field} is invalid")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise StateModelSetError(f"hmm_risk_stock_fact_source_identity_unresolved: {field} is invalid") from exc


def _parse_row(value: Any) -> SecuritySourceResolution:
    if not isinstance(value, dict):
        raise StateModelSetError("hmm_risk_stock_fact_source_identity_unresolved: manifest row must be an object")
    required = {
        "security_identity_id",
        "canonical_ts_code",
        "source_dataset",
        "source_ts_code",
        "effective_start",
        "effective_end",
        "authority_ref",
        "authority_hash",
        "row_hash",
    }
    if set(value) != required:
        raise StateModelSetError("hmm_risk_stock_fact_source_identity_unresolved: manifest row schema is invalid")
    canonical_ts_code = str(value["canonical_ts_code"])
    source_ts_code = str(value["source_ts_code"])
    _validate_ts_code(canonical_ts_code, "canonical_ts_code")
    _validate_ts_code(source_ts_code, "source_ts_code")
    if canonical_ts_code == source_ts_code:
        raise StateModelSetError(
            "hmm_risk_stock_fact_source_identity_ambiguous: explicit alias must change the source code"
        )
    source_dataset = str(value["source_dataset"])
    if source_dataset not in SUPPORTED_SOURCE_DATASETS:
        raise StateModelSetError(
            f"hmm_risk_stock_fact_source_identity_unresolved: unsupported source_dataset={source_dataset}"
        )
    effective_start = _parse_date(value["effective_start"], "effective_start")
    effective_end = _parse_date(value["effective_end"], "effective_end")
    if effective_start > effective_end:
        raise StateModelSetError("hmm_risk_stock_fact_source_identity_unresolved: effective interval is invalid")
    security_identity_id = str(value["security_identity_id"] or "").strip()
    authority_ref = str(value["authority_ref"] or "").strip()
    authority_hash = str(value["authority_hash"] or "").lower()
    row_hash = str(value["row_hash"] or "").lower()
    if not security_identity_id or not authority_ref or not _SHA256_RE.fullmatch(authority_hash):
        raise StateModelSetError("hmm_risk_stock_fact_source_identity_unresolved: authority evidence is incomplete")
    body = {key: value[key] for key in sorted(required - {"row_hash"})}
    if not _SHA256_RE.fullmatch(row_hash) or canonical_sha256(body) != row_hash:
        raise StateModelSetError("hmm_risk_stock_fact_source_identity_unresolved: manifest row hash mismatch")
    return SecuritySourceResolution(
        security_identity_id=security_identity_id,
        canonical_ts_code=canonical_ts_code,
        source_dataset=source_dataset,
        source_ts_code=source_ts_code,
        effective_start=effective_start,
        effective_end=effective_end,
        authority_ref=authority_ref,
        authority_hash=authority_hash,
        row_hash=row_hash,
        resolution_kind="explicit_effective_alias",
    )


def load_security_source_identity_manifest(
    path: Path,
    *,
    expected_sha256: str,
) -> SecuritySourceIdentityManifest:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StateModelSetError(f"security source identity manifest cannot be read: {exc}") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "manifest_version",
        "default_resolution",
        "rows",
    }:
        raise StateModelSetError("security source identity manifest schema is invalid")
    if payload["schema_version"] != SECURITY_IDENTITY_SCHEMA:
        raise StateModelSetError(f"security source identity manifest must use {SECURITY_IDENTITY_SCHEMA}")
    manifest_version = str(payload["manifest_version"] or "").strip()
    if not manifest_version or payload["default_resolution"] != DEFAULT_RESOLUTION:
        raise StateModelSetError("security source identity manifest default contract is invalid")
    rows_value = payload["rows"]
    if not isinstance(rows_value, list):
        raise StateModelSetError("security source identity manifest rows must be a list")
    rows = tuple(_parse_row(item) for item in rows_value)
    ordering = [(row.source_dataset, row.canonical_ts_code, row.effective_start, row.effective_end) for row in rows]
    if ordering != sorted(ordering) or len(ordering) != len(set(ordering)):
        raise StateModelSetError("security source identity manifest rows are not uniquely canonical ordered")
    grouped: dict[tuple[str, str], list[SecuritySourceResolution]] = {}
    for row in rows:
        grouped.setdefault((row.source_dataset, row.canonical_ts_code), []).append(row)
    for identity, items in grouped.items():
        for previous, current in zip(items, items[1:], strict=False):
            if previous.effective_end is not None and current.effective_start is not None:
                if current.effective_start <= previous.effective_end:
                    raise StateModelSetError(
                        f"hmm_risk_stock_fact_source_identity_ambiguous: overlapping effective intervals for {identity}"
                    )
    actual_sha256 = canonical_sha256(payload)
    if not _SHA256_RE.fullmatch(str(expected_sha256).lower()) or actual_sha256 != str(expected_sha256).lower():
        raise StateModelSetError(
            f"security source identity manifest hash mismatch expected={expected_sha256} actual={actual_sha256}"
        )
    return SecuritySourceIdentityManifest(
        manifest_version=manifest_version,
        default_resolution=DEFAULT_RESOLUTION,
        rows=rows,
        manifest_sha256=actual_sha256,
        rows_sha256=canonical_sha256(rows_value),
    )
