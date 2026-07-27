"""Immutable provider-absence authority for C-009 stock-fact NA evidence."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .state_model_set import StateModelSetError, canonical_sha256

PROVIDER_ABSENCE_SCHEMA = "hmm_risk_provider_absence_manifest_v1"
PROVIDER_AUDIT_SCHEMA = "hmm_risk_provider_absence_audit_receipt_v1"
MONEYFLOW_DATASET = "market.moneyflow_ts"
MONEYFLOW_MISSING_FIELDS = (
    "buy_elg_amount_cny",
    "buy_sm_amount_cny",
    "net_mf_amount_cny",
    "sell_elg_amount_cny",
    "sell_sm_amount_cny",
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TS_CODE_RE = re.compile(r"^[0-9]{6}\.(?:BJ|SH|SZ)$")


@dataclass(frozen=True)
class ProviderAbsenceEvidence:
    canonical_ts_code: str
    source_dataset: str
    source_ts_code: str
    trade_date: date
    missing_fields: tuple[str, ...]
    provider_audit_receipt_sha256: str
    row_hash: str

    def evidence(self) -> dict[str, Any]:
        return {
            "fact_status": "provider_absence",
            "canonical_ts_code": self.canonical_ts_code,
            "source_dataset": self.source_dataset,
            "source_ts_code": self.source_ts_code,
            "trade_date": self.trade_date.isoformat(),
            "missing_fields": list(self.missing_fields),
            "provider_audit_receipt_sha256": self.provider_audit_receipt_sha256,
            "row_hash": self.row_hash,
        }


@dataclass(frozen=True)
class ProviderAbsenceManifest:
    manifest_version: str
    provider_audit_receipt: dict[str, Any]
    rows: tuple[ProviderAbsenceEvidence, ...]
    manifest_sha256: str
    rows_sha256: str
    by_key: dict[tuple[str, str, str, date], ProviderAbsenceEvidence]

    def resolve(
        self,
        *,
        canonical_ts_code: str,
        source_dataset: str,
        source_ts_code: str,
        trade_date: date,
    ) -> ProviderAbsenceEvidence:
        key = (canonical_ts_code, source_dataset, source_ts_code, trade_date)
        match = self.by_key.get(key)
        if match is None:
            raise StateModelSetError(
                "hmm_risk_stock_fact_provider_absence_unverified: "
                f"{canonical_ts_code}/{trade_date}/{source_dataset}/{source_ts_code} is not an exact audited key"
            )
        return match

    def evidence(self) -> dict[str, Any]:
        return {
            "schema_version": PROVIDER_ABSENCE_SCHEMA,
            "manifest_version": self.manifest_version,
            "provider_audit_receipt_sha256": canonical_sha256(self.provider_audit_receipt),
            "row_count": len(self.rows),
            "rows_sha256": self.rows_sha256,
            "manifest_sha256": self.manifest_sha256,
        }


def _parse_row(value: Any, audit_sha256: str) -> ProviderAbsenceEvidence:
    required = {
        "canonical_ts_code",
        "source_dataset",
        "source_ts_code",
        "trade_date",
        "missing_fields",
        "provider_audit_receipt_sha256",
        "row_hash",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise StateModelSetError("provider absence manifest row schema is invalid")
    canonical_ts_code = str(value["canonical_ts_code"])
    source_ts_code = str(value["source_ts_code"])
    if not _TS_CODE_RE.fullmatch(canonical_ts_code) or not _TS_CODE_RE.fullmatch(source_ts_code):
        raise StateModelSetError("provider absence manifest security identity is invalid")
    if value["source_dataset"] != MONEYFLOW_DATASET:
        raise StateModelSetError("provider absence manifest source dataset is unsupported")
    try:
        trade_date = date.fromisoformat(str(value["trade_date"]))
    except ValueError as exc:
        raise StateModelSetError("provider absence manifest trade_date is invalid") from exc
    missing_fields = tuple(str(item) for item in value["missing_fields"])
    if missing_fields != MONEYFLOW_MISSING_FIELDS:
        raise StateModelSetError("provider absence manifest missing-fields contract is invalid")
    receipt_sha256 = str(value["provider_audit_receipt_sha256"]).lower()
    row_hash = str(value["row_hash"]).lower()
    if receipt_sha256 != audit_sha256:
        raise StateModelSetError("provider absence manifest row audit identity mismatch")
    body = {key: value[key] for key in sorted(required - {"row_hash"})}
    if not _SHA256_RE.fullmatch(row_hash) or canonical_sha256(body) != row_hash:
        raise StateModelSetError("provider absence manifest row hash mismatch")
    return ProviderAbsenceEvidence(
        canonical_ts_code=canonical_ts_code,
        source_dataset=MONEYFLOW_DATASET,
        source_ts_code=source_ts_code,
        trade_date=trade_date,
        missing_fields=missing_fields,
        provider_audit_receipt_sha256=receipt_sha256,
        row_hash=row_hash,
    )


def load_provider_absence_manifest(path: Path, *, expected_sha256: str) -> ProviderAbsenceManifest:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StateModelSetError(f"provider absence manifest cannot be read: {exc}") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "manifest_version",
        "provider_audit_receipt",
        "rows",
    }:
        raise StateModelSetError("provider absence manifest schema is invalid")
    if payload["schema_version"] != PROVIDER_ABSENCE_SCHEMA:
        raise StateModelSetError(f"provider absence manifest must use {PROVIDER_ABSENCE_SCHEMA}")
    manifest_version = str(payload["manifest_version"] or "").strip()
    receipt = payload["provider_audit_receipt"]
    if not manifest_version or not isinstance(receipt, dict) or receipt.get("schema_version") != PROVIDER_AUDIT_SCHEMA:
        raise StateModelSetError("provider absence audit receipt is invalid")
    receipt_sha256 = canonical_sha256(receipt)
    if receipt.get("provider") != "tushare" or receipt.get("query_authority") != "trade_date_full_market":
        raise StateModelSetError("provider absence audit authority is invalid")
    rows_value = payload["rows"]
    if not isinstance(rows_value, list):
        raise StateModelSetError("provider absence manifest rows must be a list")
    rows = tuple(_parse_row(item, receipt_sha256) for item in rows_value)
    ordering = [(row.trade_date, row.canonical_ts_code, row.source_dataset, row.source_ts_code) for row in rows]
    if ordering != sorted(ordering) or len(ordering) != len(set(ordering)):
        raise StateModelSetError("provider absence manifest rows are not uniquely canonical ordered")
    absent_keys = [
        {
            "trade_date": row.trade_date.isoformat(),
            "canonical_ts_code": row.canonical_ts_code,
            "source_dataset": row.source_dataset,
            "source_ts_code": row.source_ts_code,
        }
        for row in rows
    ]
    if int(receipt.get("absent_key_count") or -1) != len(rows):
        raise StateModelSetError("provider absence audit key count mismatch")
    if receipt.get("absent_key_sha256") != canonical_sha256(absent_keys):
        raise StateModelSetError("provider absence audit key hash mismatch")
    actual_sha256 = canonical_sha256(payload)
    normalized_expected = str(expected_sha256).lower()
    if not _SHA256_RE.fullmatch(normalized_expected) or actual_sha256 != normalized_expected:
        raise StateModelSetError(
            f"provider absence manifest hash mismatch expected={expected_sha256} actual={actual_sha256}"
        )
    return ProviderAbsenceManifest(
        manifest_version=manifest_version,
        provider_audit_receipt=dict(receipt),
        rows=rows,
        manifest_sha256=actual_sha256,
        rows_sha256=canonical_sha256(rows_value),
        by_key={(row.canonical_ts_code, row.source_dataset, row.source_ts_code, row.trade_date): row for row in rows},
    )
