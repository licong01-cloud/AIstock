"""Fail-closed consumer for the frozen HMM QE-assistance artifact."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "hmm_risk_qe_assistance_coefficients_v1"
ADJUSTMENT_MODE = "sign_safe_magnitude_v1"
FORMULA_TEXT = "adjusted_score=raw_score+(coefficient-1.0)*abs(raw_score)"
FORMULA_SHA256 = hashlib.sha256(FORMULA_TEXT.encode("utf-8")).hexdigest()
APPLIED = "applied"
NOT_APPLICABLE = "not_applicable_authority_unavailable"
UNAVAILABLE_REASON = "classification:classification_authority_unavailable"
EXPECTED_MODEL_CONTRACT = "hmm_risk_rotation_l1_g2a_v1_6"
EXPECTED_MODEL_HASH = "3956107600a3aef4b51ac1da0c56f7940ce49a34777c836e974d14a5b45fbee6"
EXPECTED_MAPPING_HASH = "e478722f700535ac4e37744a651291bc6d179cb899dccd28cdb957ed4491b82f"
EXPECTED_SOURCE_HASH = "0957ae8a6527fb28ba337a449ce0f72dfe9f43513003492329d7e770aa9da8e2"
EXPECTED_AUTHORITY_HASH = "203effb611d00edde4c0ee9c40f205759097628b8c5eb249907f3b33e6932ddf"
EXPECTED_ARTIFACT_CANONICAL_SHA256 = "de92f166c06112771c0d1385043f8599d22fbed44e5ed1c43fb8b138e86bc971"
EXPECTED_WINDOW = ("2024-07-02", "2026-03-31")
EXPECTED_DATE_COUNT = 423
EXPECTED_PREDICTION_ROWS = 1_951_448
EXPECTED_APPLIED_ROWS = 1_780_359
EXPECTED_NOT_APPLICABLE_ROWS = 171_089
ALLOWED_COEFFICIENTS = {0.98, 1.0, 1.02}
ENTRY_FIELDS = {
    "status",
    "sector_code",
    "reason_code",
    "adjustment_applied",
    "classification_receipt_hash",
    "index_membership_receipt_hash",
    "classification_row_hashes",
    "index_membership_row_hashes",
}

REASON_INPUT = "hmm_risk_qe_assistance_input_invalid"
REASON_AUTHORITY = "hmm_risk_qe_assistance_authority_identity_mismatch"
REASON_MAPPING = "hmm_risk_qe_assistance_pit_mapping_missing"
REASON_STATE = "hmm_risk_qe_assistance_state_missing"
REASON_FORMULA = "hmm_risk_qe_assistance_formula_invalid"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SECTOR = re.compile(r"^801[0-9]{3}[.]SI$")


class HMMQEAssistanceContractError(RuntimeError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def json_object_without_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise HMMQEAssistanceContractError(REASON_INPUT, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_sha256(value: object, *, context: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise HMMQEAssistanceContractError(REASON_AUTHORITY, f"invalid SHA-256: {context}")


def _validate_entry(date_key: str, stock_id: str, entry: object, day_coeffs: Mapping[str, object]) -> str:
    if not isinstance(entry, dict) or set(entry) != ENTRY_FIELDS:
        raise HMMQEAssistanceContractError(REASON_MAPPING, f"applicability shape differs: {date_key}/{stock_id}")
    for field in ("classification_receipt_hash", "index_membership_receipt_hash"):
        _require_sha256(entry[field], context=f"{date_key}/{stock_id}/{field}")
    for field in ("classification_row_hashes", "index_membership_row_hashes"):
        values = entry[field]
        if not isinstance(values, list):
            raise HMMQEAssistanceContractError(REASON_AUTHORITY, f"row lineage differs: {date_key}/{stock_id}/{field}")
        for index, value in enumerate(values):
            _require_sha256(value, context=f"{date_key}/{stock_id}/{field}[{index}]")

    status = entry["status"]
    if status == APPLIED:
        if (
            entry["adjustment_applied"] is not True
            or entry["reason_code"] is not None
            or entry["sector_code"] not in day_coeffs
            or not entry["classification_row_hashes"]
        ):
            raise HMMQEAssistanceContractError(REASON_MAPPING, f"applied entry differs: {date_key}/{stock_id}")
    elif status == NOT_APPLICABLE:
        if (
            entry["adjustment_applied"] is not False
            or entry["reason_code"] != UNAVAILABLE_REASON
            or entry["sector_code"] is not None
        ):
            raise HMMQEAssistanceContractError(REASON_MAPPING, f"non-applicable entry differs: {date_key}/{stock_id}")
    else:
        raise HMMQEAssistanceContractError(REASON_MAPPING, f"unknown applicability status: {date_key}/{stock_id}")
    return str(status)


def validate_payload(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise HMMQEAssistanceContractError(REASON_INPUT, "unsupported QE-assistance artifact schema")
    body = dict(payload)
    observed_hash = body.pop("artifact_sha256", None)
    _require_sha256(observed_hash, context="artifact_sha256")
    if observed_hash != EXPECTED_ARTIFACT_CANONICAL_SHA256 or observed_hash != _canonical_sha256(body):
        raise HMMQEAssistanceContractError(REASON_AUTHORITY, "artifact canonical hash differs")
    if payload.get("adjustment_mode") != ADJUSTMENT_MODE or payload.get("adapter_formula") != {
        "text": FORMULA_TEXT,
        "sha256": FORMULA_SHA256,
    }:
        raise HMMQEAssistanceContractError(REASON_FORMULA, "formula identity differs")
    if payload.get("tail_accessed") is not False:
        raise HMMQEAssistanceContractError(REASON_INPUT, "sealed tail must not be accessed")
    authority = payload.get("authority_identity")
    if (
        payload.get("source_model_contract") != EXPECTED_MODEL_CONTRACT
        or payload.get("model_hash") != EXPECTED_MODEL_HASH
        or payload.get("source_mapping_sha256") != EXPECTED_MAPPING_HASH
        or payload.get("source_prediction_file_sha256") != EXPECTED_SOURCE_HASH
        or not isinstance(authority, dict)
        or authority.get("bundle_hash") != EXPECTED_AUTHORITY_HASH
        or (payload.get("window_start"), payload.get("window_end")) != EXPECTED_WINDOW
    ):
        raise HMMQEAssistanceContractError(REASON_AUTHORITY, "fixed model/source/authority identity differs")
    _require_sha256(payload.get("source_prediction_row_sha256"), context="source_prediction_row_sha256")

    daily = payload.get("daily_coefficients")
    applicability = payload.get("stock_sector_applicability_by_date")
    sectors = payload.get("canonical_l1_codes")
    if not isinstance(daily, dict) or not daily or list(daily) != sorted(daily):
        raise HMMQEAssistanceContractError(REASON_STATE, "daily coefficients are missing or unsorted")
    if not isinstance(applicability, dict) or set(applicability) != set(daily):
        raise HMMQEAssistanceContractError(REASON_MAPPING, "applicability dates differ")
    if (
        not isinstance(sectors, list)
        or len(sectors) != 31
        or len(set(sectors)) != 31
        or any(not isinstance(sector, str) or _SECTOR.fullmatch(sector) is None for sector in sectors)
    ):
        raise HMMQEAssistanceContractError(REASON_AUTHORITY, "canonical L1 authority differs")

    sector_set = set(sectors)
    applied_count = 0
    not_applicable_count = 0
    for date_key, day_coeffs in daily.items():
        if not isinstance(day_coeffs, dict) or set(day_coeffs) != sector_set:
            raise HMMQEAssistanceContractError(REASON_STATE, f"coefficient denominator differs: {date_key}")
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not np.isfinite(value)
            or float(value) not in ALLOWED_COEFFICIENTS
            for value in day_coeffs.values()
        ):
            raise HMMQEAssistanceContractError(REASON_STATE, f"coefficient differs: {date_key}")
        day_applicability = applicability[date_key]
        if not isinstance(day_applicability, dict) or not day_applicability:
            raise HMMQEAssistanceContractError(REASON_MAPPING, f"applicability is empty: {date_key}")
        for stock_id, entry in day_applicability.items():
            if not isinstance(stock_id, str) or not stock_id:
                raise HMMQEAssistanceContractError(REASON_MAPPING, f"stock identity differs: {date_key}")
            status = _validate_entry(date_key, stock_id, entry, day_coeffs)
            applied_count += int(status == APPLIED)
            not_applicable_count += int(status == NOT_APPLICABLE)

    if (
        len(daily) != EXPECTED_DATE_COUNT
        or payload.get("date_count") != EXPECTED_DATE_COUNT
        or payload.get("sector_denominator") != 31
        or payload.get("prediction_row_count") != EXPECTED_PREDICTION_ROWS
        or payload.get("applied_row_count") != EXPECTED_APPLIED_ROWS
        or payload.get("not_applicable_row_count") != EXPECTED_NOT_APPLICABLE_ROWS
        or applied_count != EXPECTED_APPLIED_ROWS
        or not_applicable_count != EXPECTED_NOT_APPLICABLE_ROWS
    ):
        raise HMMQEAssistanceContractError(REASON_MAPPING, "artifact cardinality differs")
    payload["_detected_mapping_mode"] = "qe_assistance_by_trade_date_v1"
    return payload


def validate_payload_schema(payload: object) -> dict[str, Any]:
    """Dispatch only the two approved schema families.

    Historical coefficient files predate an explicit ``schema_version`` and
    remain on the legacy multiplier path.  Any explicit schema is therefore a
    versioned contract and must either be the approved QE-assistance schema or
    fail closed; it must never be guessed to be legacy.
    """

    if not isinstance(payload, dict):
        raise HMMQEAssistanceContractError(REASON_INPUT, "HMM coefficient payload must be an object")
    schema_version = payload.get("schema_version")
    if schema_version is None:
        return payload
    if schema_version != SCHEMA_VERSION:
        raise HMMQEAssistanceContractError(REASON_INPUT, "unsupported explicit HMM coefficient schema")
    return validate_payload(payload)


def apply_adjustment(payload: Mapping[str, Any], scores: pd.Series, trade_date: str) -> tuple[pd.Series, dict[str, Any]]:
    if scores is None or scores.empty:
        raise HMMQEAssistanceContractError(REASON_INPUT, f"input score is empty: date={trade_date}")
    if not scores.index.is_unique or not np.isfinite(scores.to_numpy(dtype=float)).all():
        raise HMMQEAssistanceContractError(REASON_INPUT, f"input score identity/value differs: date={trade_date}")
    day_coeffs = payload["daily_coefficients"].get(trade_date)
    mapping = payload["stock_sector_applicability_by_date"].get(trade_date)
    if not isinstance(day_coeffs, dict):
        raise HMMQEAssistanceContractError(REASON_STATE, f"coefficient date is missing: {trade_date}")
    if not isinstance(mapping, dict) or set(scores.index) != set(mapping):
        raise HMMQEAssistanceContractError(REASON_MAPPING, f"applicability denominator differs: {trade_date}")

    adjusted = scores.copy()
    rows: list[dict[str, Any]] = []
    for stock_id in adjusted.index:
        entry = mapping[stock_id]
        raw_score = float(adjusted[stock_id])
        if entry["status"] == NOT_APPLICABLE:
            coefficient = 1.0
            adjusted_score = raw_score
            reason = NOT_APPLICABLE
        elif entry["status"] == APPLIED:
            sector_code = entry["sector_code"]
            if sector_code not in day_coeffs:
                raise HMMQEAssistanceContractError(REASON_STATE, f"coefficient is missing: {trade_date}/{stock_id}")
            coefficient = float(day_coeffs[sector_code])
            adjusted_score = raw_score + (coefficient - 1.0) * abs(raw_score)
            reason = "hmm_qe_assistance_applied"
        else:
            raise HMMQEAssistanceContractError(REASON_MAPPING, f"unknown applicability status: {trade_date}/{stock_id}")
        if not np.isfinite(adjusted_score):
            raise HMMQEAssistanceContractError(REASON_FORMULA, f"adjusted score is non-finite: {trade_date}/{stock_id}")
        adjusted[stock_id] = adjusted_score
        rows.append(
            {
                "stock_id": stock_id,
                "sector_code": entry["sector_code"],
                "coefficient": coefficient,
                "raw_score": raw_score,
                "adjusted_score": adjusted_score,
                "reason": reason,
            }
        )
    return adjusted, {"date": trade_date, "mapping_mode": "qe_assistance_by_trade_date_v1", "rows": rows}
