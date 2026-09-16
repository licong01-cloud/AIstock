"""Canonical QE assistance artifact for the approved D1-D4 contract.

This module is intentionally pure.  It receives an already-bound C-013
``HMMIndustryPitAdapter`` and frozen in-memory rows, performs no filesystem,
database, network, or runtime I/O, and never invents a sector for an
authority-unavailable stock.
"""

from __future__ import annotations

import hashlib
import math
import re
import struct
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any, Protocol

from backend.services.dataset_release.canonical import canonical_json_bytes

SCHEMA_VERSION = "hmm_risk_qe_assistance_coefficients_v1"
ADJUSTMENT_MODE = "sign_safe_magnitude_v1"
FORMULA_TEXT = "adjusted_score=raw_score+(coefficient-1.0)*abs(raw_score)"
FORMULA_SHA256 = hashlib.sha256(FORMULA_TEXT.encode("utf-8")).hexdigest()
FORMAL_WINDOW_START = date(2024, 7, 2)
FORMAL_WINDOW_END = date(2026, 3, 31)
SEALED_TAIL_START = date(2026, 4, 1)
EXPECTED_SOURCE_FILE_SHA256 = "0957ae8a6527fb28ba337a449ce0f72dfe9f43513003492329d7e770aa9da8e2"
EXPECTED_MAPPING_SHA256 = "e478722f700535ac4e37744a651291bc6d179cb899dccd28cdb957ed4491b82f"
EXPECTED_AUTHORITY_BUNDLE_HASH = "203effb611d00edde4c0ee9c40f205759097628b8c5eb249907f3b33e6932ddf"
EXPECTED_APPLIED_ROWS = 1_780_359
EXPECTED_NOT_APPLICABLE_ROWS = 171_089
EXPECTED_EXECUTABLE_ROWS = 1_951_448
EXPECTED_DATE_COUNT = 423
EXPECTED_SECTOR_COUNT = 31

STATE_COEFFICIENTS = {"fading": 0.98, "neutral": 1.0, "trending": 1.02}
APPLIED = "applied"
NOT_APPLICABLE = "not_applicable_authority_unavailable"
APPROVED_UNAVAILABLE_REASON = "classification:classification_authority_unavailable"

REASON_INPUT = "hmm_risk_qe_assistance_input_invalid"
REASON_AUTHORITY = "hmm_risk_qe_assistance_authority_identity_mismatch"
REASON_MAPPING = "hmm_risk_qe_assistance_pit_mapping_missing"
REASON_STATE = "hmm_risk_qe_assistance_state_missing"
REASON_FORMULA = "hmm_risk_qe_assistance_formula_invalid"
REASON_REPLAY = "hmm_risk_qe_assistance_prediction_replay_identity_mismatch"
REASON_CALENDAR = "hmm_risk_qe_assistance_signal_calendar_mismatch"

_SYMBOL = re.compile(r"^[0-9]{6}[.](?:SH|SZ|BJ)$")
_SECTOR = re.compile(r"^801[0-9]{3}[.]SI$")


class QEAssistanceContractError(RuntimeError):
    """Typed fail-closed contract error."""

    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


class IndustryProjection(Protocol):
    status: str
    canonical_symbol: str
    trade_date: date
    l1_code: str | None
    reason_code: str | None
    classification_receipt_hash: str
    index_membership_receipt_hash: str
    classification_row_hashes: tuple[str, ...]
    index_membership_row_hashes: tuple[str, ...]


class IndustryAdapter(Protocol):
    def resolve(self, symbol: str, trade_date: date) -> IndustryProjection: ...


def _sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _require_sha256(value: Any, field: str, reason: str = REASON_INPUT) -> str:
    text = str(value or "").strip().lower()
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise QEAssistanceContractError(reason, f"{field} must be a lowercase SHA-256")
    return text


def _parse_date(value: Any, field: str, reason: str = REASON_INPUT) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise QEAssistanceContractError(reason, f"{field} must be an ISO date") from exc


def _float64_bits(value: float) -> bytes:
    return struct.pack(">d", float(value))


def apply_sign_safe_adjustment(raw_score: float, coefficient: float) -> float:
    if isinstance(raw_score, bool) or isinstance(coefficient, bool):
        raise QEAssistanceContractError(REASON_FORMULA, "score and coefficient must be numeric")
    score = float(raw_score)
    multiplier = float(coefficient)
    if not math.isfinite(score) or not math.isfinite(multiplier) or multiplier not in STATE_COEFFICIENTS.values():
        raise QEAssistanceContractError(REASON_FORMULA, "score or coefficient is outside the approved contract")
    adjusted = score + (multiplier - 1.0) * abs(score)
    if not math.isfinite(adjusted):
        raise QEAssistanceContractError(REASON_FORMULA, "adjusted score is non-finite")
    return adjusted


def apply_artifact_entry(
    raw_score: float,
    entry: Mapping[str, Any],
    daily_coefficients: Mapping[str, float],
) -> float:
    """Apply one explicit row; non-applicable preserves the float64 bits."""

    if isinstance(raw_score, bool):
        raise QEAssistanceContractError(REASON_FORMULA, "raw score must be numeric")
    score = float(raw_score)
    if not math.isfinite(score) or not isinstance(entry, Mapping):
        raise QEAssistanceContractError(REASON_FORMULA, "raw score or applicability entry is invalid")
    if entry.get("status") == NOT_APPLICABLE:
        if (
            entry.get("adjustment_applied") is not False
            or entry.get("reason_code") != APPROVED_UNAVAILABLE_REASON
            or entry.get("sector_code") is not None
        ):
            raise QEAssistanceContractError(REASON_MAPPING, "non-applicable entry differs from D3-B")
        preserved = float(score)
        if _float64_bits(preserved) != _float64_bits(score):
            raise QEAssistanceContractError(REASON_FORMULA, "non-applicable score was not preserved bitwise")
        return preserved
    if entry.get("status") != APPLIED or entry.get("adjustment_applied") is not True:
        raise QEAssistanceContractError(REASON_MAPPING, "applicability status is missing or unknown")
    sector = str(entry.get("sector_code") or "")
    coefficient = daily_coefficients.get(sector)
    if coefficient is None:
        raise QEAssistanceContractError(REASON_STATE, "applied entry lacks a daily sector coefficient")
    return apply_sign_safe_adjustment(score, float(coefficient))


def _calendar_successors(calendar: Sequence[date]) -> dict[date, date]:
    normalized = tuple(_parse_date(item, "calendar_date", REASON_CALENDAR) for item in calendar)
    if len(normalized) < 2 or tuple(sorted(set(normalized))) != normalized:
        raise QEAssistanceContractError(REASON_CALENDAR, "frozen QE calendar must be unique and increasing")
    return dict(zip(normalized, normalized[1:], strict=False))


def _state_coefficients(
    state_rows: Sequence[Mapping[str, Any]],
    *,
    expected_dates: set[date],
    source_model_contract: str,
    model_hash: str,
    canonical_l1_codes: frozenset[str],
) -> dict[str, dict[str, float]]:
    by_date: dict[date, dict[str, float]] = defaultdict(dict)
    seen: set[tuple[date, str]] = set()
    for row in state_rows:
        if not isinstance(row, Mapping):
            raise QEAssistanceContractError(REASON_STATE, "state row must be an object")
        trade_date = _parse_date(row.get("trade_date"), "state.trade_date", REASON_STATE)
        sector = str(row.get("sector_code") or "").strip()
        key = (trade_date, sector)
        if key in seen or trade_date not in expected_dates or _SECTOR.fullmatch(sector) is None:
            raise QEAssistanceContractError(REASON_STATE, "state row identity is invalid or duplicated")
        seen.add(key)
        if (
            row.get("availability") != "available"
            or row.get("reason_code") is not None
            or row.get("forecast_state") not in STATE_COEFFICIENTS
            or row.get("model_hash") != model_hash
            or row.get("model_contract") != source_model_contract
        ):
            raise QEAssistanceContractError(REASON_STATE, "state row does not match the frozen available model")
        by_date[trade_date][sector] = STATE_COEFFICIENTS[str(row["forecast_state"])]
    if set(by_date) != expected_dates or any(set(values) != canonical_l1_codes for values in by_date.values()):
        raise QEAssistanceContractError(REASON_STATE, "every executable date must contain exactly 31 L1 states")
    return {day.isoformat(): dict(sorted(values.items())) for day, values in sorted(by_date.items())}


def _projection_entry(projection: IndustryProjection, coefficients: Mapping[str, float]) -> dict[str, Any]:
    common = {
        "classification_receipt_hash": _require_sha256(
            projection.classification_receipt_hash, "classification_receipt_hash", REASON_AUTHORITY
        ),
        "index_membership_receipt_hash": _require_sha256(
            projection.index_membership_receipt_hash, "index_membership_receipt_hash", REASON_AUTHORITY
        ),
        "classification_row_hashes": [
            _require_sha256(value, "classification_row_hash", REASON_AUTHORITY)
            for value in projection.classification_row_hashes
        ],
        "index_membership_row_hashes": [
            _require_sha256(value, "index_membership_row_hash", REASON_AUTHORITY)
            for value in projection.index_membership_row_hashes
        ],
    }
    if projection.status == "resolved":
        sector = str(projection.l1_code or "")
        if _SECTOR.fullmatch(sector) is None or sector not in coefficients:
            raise QEAssistanceContractError(REASON_MAPPING, "resolved PIT identity escapes daily 31-sector state")
        if not common["classification_row_hashes"] or not common["index_membership_row_hashes"]:
            raise QEAssistanceContractError(REASON_AUTHORITY, "resolved PIT identity lacks source row lineage")
        return {
            "status": APPLIED,
            "sector_code": sector,
            "reason_code": None,
            "adjustment_applied": True,
            **common,
        }
    if projection.status == "unavailable" and projection.reason_code == APPROVED_UNAVAILABLE_REASON:
        return {
            "status": NOT_APPLICABLE,
            "sector_code": None,
            "reason_code": APPROVED_UNAVAILABLE_REASON,
            "adjustment_applied": False,
            **common,
        }
    raise QEAssistanceContractError(
        REASON_MAPPING,
        "PIT projection is neither resolved nor the approved explicit non-applicable outcome",
        context={"status": projection.status, "reason_code": projection.reason_code},
    )


def _build_qe_assistance_artifact(
    *,
    raw_prediction_rows: Sequence[Mapping[str, Any]],
    state_rows: Sequence[Mapping[str, Any]],
    calendar: Sequence[date],
    industry_adapter: IndustryAdapter,
    source_model_contract: str,
    model_hash: str,
    source_mapping_sha256: str,
    source_prediction_file_sha256: str,
    authority_identity: Mapping[str, Any],
    canonical_l1_codes: Sequence[str],
    window_start: date = FORMAL_WINDOW_START,
    window_end: date = FORMAL_WINDOW_END,
    formal_cardinality: tuple[int, int, int, int] | None,
) -> dict[str, Any]:
    """Shared builder; production callers use the fixed public wrapper."""

    model_contract = str(source_model_contract or "").strip()
    if not model_contract:
        raise QEAssistanceContractError(REASON_INPUT, "source_model_contract must be non-empty")
    normalized_model_hash = _require_sha256(model_hash, "model_hash")
    mapping_hash = _require_sha256(source_mapping_sha256, "source_mapping_sha256", REASON_AUTHORITY)
    source_file_hash = _require_sha256(source_prediction_file_sha256, "source_prediction_file_sha256", REASON_REPLAY)
    if mapping_hash != EXPECTED_MAPPING_SHA256:
        raise QEAssistanceContractError(REASON_AUTHORITY, "source mapping identity differs from v1.6")
    if source_file_hash != EXPECTED_SOURCE_FILE_SHA256:
        raise QEAssistanceContractError(REASON_REPLAY, "Loop2 prediction file identity differs")
    if (
        not isinstance(authority_identity, Mapping)
        or authority_identity.get("bundle_hash") != EXPECTED_AUTHORITY_BUNDLE_HASH
    ):
        raise QEAssistanceContractError(REASON_AUTHORITY, "authority identity differs from the frozen C-013 bundle")
    canonical_sectors = frozenset(str(value or "").strip() for value in canonical_l1_codes)
    if len(canonical_sectors) != EXPECTED_SECTOR_COUNT or any(
        _SECTOR.fullmatch(value) is None for value in canonical_sectors
    ):
        raise QEAssistanceContractError(REASON_AUTHORITY, "canonical L1 authority must contain 31 unique sectors")
    if window_start > window_end or window_end >= SEALED_TAIL_START:
        raise QEAssistanceContractError(REASON_INPUT, "requested window is invalid or enters sealed tail")

    successors = _calendar_successors(calendar)
    normalized_rows: list[tuple[date, date, str, float]] = []
    seen: set[tuple[date, str]] = set()
    dates: set[date] = set()
    stock_keys: dict[date, set[str]] = defaultdict(set)
    for row in raw_prediction_rows:
        if not isinstance(row, Mapping):
            raise QEAssistanceContractError(REASON_INPUT, "prediction row must be an object")
        source_date = _parse_date(row.get("source_date"), "prediction.source_date")
        trade_date = _parse_date(row.get("trade_date"), "prediction.trade_date")
        symbol = str(row.get("instrument") or "").strip().upper()
        score = row.get("score")
        if _SYMBOL.fullmatch(symbol) is None or isinstance(score, bool):
            raise QEAssistanceContractError(REASON_INPUT, "prediction symbol or score is invalid")
        try:
            numeric_score = float(score)
        except (TypeError, ValueError) as exc:
            raise QEAssistanceContractError(REASON_INPUT, "prediction score must be numeric") from exc
        if not math.isfinite(numeric_score):
            raise QEAssistanceContractError(REASON_INPUT, "prediction score must be finite")
        if trade_date < window_start or trade_date > window_end or trade_date >= SEALED_TAIL_START:
            raise QEAssistanceContractError(REASON_INPUT, "prediction row is outside the approved window")
        if successors.get(source_date) != trade_date:
            raise QEAssistanceContractError(REASON_CALENDAR, "source date is not the previous frozen trade date")
        key = (trade_date, symbol)
        if key in seen:
            raise QEAssistanceContractError(REASON_INPUT, "prediction execution key is duplicated")
        seen.add(key)
        dates.add(trade_date)
        stock_keys[trade_date].add(symbol)
        normalized_rows.append((source_date, trade_date, symbol, numeric_score))
    if not normalized_rows:
        raise QEAssistanceContractError(REASON_INPUT, "prediction panel is empty")

    daily_coefficients = _state_coefficients(
        state_rows,
        expected_dates=dates,
        source_model_contract=model_contract,
        model_hash=normalized_model_hash,
        canonical_l1_codes=canonical_sectors,
    )
    applicability: dict[str, dict[str, Any]] = {}
    counts: Counter[str] = Counter()
    for trade_date in sorted(dates):
        daily: dict[str, Any] = {}
        coefficients = daily_coefficients[trade_date.isoformat()]
        for symbol in sorted(stock_keys[trade_date]):
            projection = industry_adapter.resolve(symbol, trade_date)
            if projection.canonical_symbol != symbol or projection.trade_date != trade_date:
                raise QEAssistanceContractError(REASON_AUTHORITY, "PIT resolver returned a different key identity")
            entry = _projection_entry(projection, coefficients)
            daily[symbol] = entry
            counts[entry["status"]] += 1
        applicability[trade_date.isoformat()] = daily

    if sum(counts.values()) != len(normalized_rows):
        raise QEAssistanceContractError(REASON_MAPPING, "applicability denominator differs from prediction panel")
    if formal_cardinality is not None:
        expected_dates, expected_rows, expected_applied, expected_not_applicable = formal_cardinality
        if (
            len(dates) != expected_dates
            or len(normalized_rows) != expected_rows
            or counts[APPLIED] != expected_applied
            or counts[NOT_APPLICABLE] != expected_not_applicable
            or window_start != FORMAL_WINDOW_START
            or window_end != FORMAL_WINDOW_END
        ):
            raise QEAssistanceContractError(REASON_MAPPING, "formal D3-B cardinality differs")

    source_rows = [
        [source.isoformat(), trade.isoformat(), symbol, score]
        for source, trade, symbol, score in sorted(normalized_rows)
    ]
    body = {
        "schema_version": SCHEMA_VERSION,
        "source_model_contract": model_contract,
        "model_hash": normalized_model_hash,
        "source_mapping_sha256": mapping_hash,
        "source_prediction_file_sha256": source_file_hash,
        "source_prediction_row_sha256": _sha256(source_rows),
        "authority_identity": dict(authority_identity),
        "canonical_l1_codes": sorted(canonical_sectors),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "daily_coefficients": daily_coefficients,
        "stock_sector_applicability_by_date": applicability,
        "adapter_formula": {"text": FORMULA_TEXT, "sha256": FORMULA_SHA256},
        "adjustment_mode": ADJUSTMENT_MODE,
        "tail_accessed": False,
        "prediction_row_count": len(normalized_rows),
        "date_count": len(dates),
        "sector_denominator": EXPECTED_SECTOR_COUNT,
        "applied_row_count": counts[APPLIED],
        "not_applicable_row_count": counts[NOT_APPLICABLE],
    }
    return {**body, "artifact_sha256": _sha256(body)}


def build_qe_assistance_artifact(
    *,
    raw_prediction_rows: Sequence[Mapping[str, Any]],
    state_rows: Sequence[Mapping[str, Any]],
    calendar: Sequence[date],
    industry_adapter: IndustryAdapter,
    source_model_contract: str,
    model_hash: str,
    source_mapping_sha256: str,
    source_prediction_file_sha256: str,
    authority_identity: Mapping[str, Any],
    canonical_l1_codes: Sequence[str],
) -> dict[str, Any]:
    """Build the fixed formal D3-B artifact with no cardinality bypass."""

    return _build_qe_assistance_artifact(
        raw_prediction_rows=raw_prediction_rows,
        state_rows=state_rows,
        calendar=calendar,
        industry_adapter=industry_adapter,
        source_model_contract=source_model_contract,
        model_hash=model_hash,
        source_mapping_sha256=source_mapping_sha256,
        source_prediction_file_sha256=source_prediction_file_sha256,
        authority_identity=authority_identity,
        canonical_l1_codes=canonical_l1_codes,
        window_start=FORMAL_WINDOW_START,
        window_end=FORMAL_WINDOW_END,
        formal_cardinality=(
            EXPECTED_DATE_COUNT,
            EXPECTED_EXECUTABLE_ROWS,
            EXPECTED_APPLIED_ROWS,
            EXPECTED_NOT_APPLICABLE_ROWS,
        ),
    )


def validate_qe_assistance_artifact(artifact: Mapping[str, Any]) -> None:
    """Readback the self-contained schema without consulting mutable sources."""

    if not isinstance(artifact, Mapping):
        raise QEAssistanceContractError(REASON_INPUT, "artifact must be an object")
    body = dict(artifact)
    observed_hash = body.pop("artifact_sha256", None)
    if observed_hash != _sha256(body):
        raise QEAssistanceContractError(REASON_AUTHORITY, "artifact canonical hash differs")
    if (
        body.get("schema_version") != SCHEMA_VERSION
        or body.get("adjustment_mode") != ADJUSTMENT_MODE
        or body.get("adapter_formula") != {"text": FORMULA_TEXT, "sha256": FORMULA_SHA256}
        or body.get("tail_accessed") is not False
        or body.get("source_mapping_sha256") != EXPECTED_MAPPING_SHA256
        or body.get("source_prediction_file_sha256") != EXPECTED_SOURCE_FILE_SHA256
        or not isinstance(body.get("authority_identity"), Mapping)
        or body["authority_identity"].get("bundle_hash") != EXPECTED_AUTHORITY_BUNDLE_HASH
    ):
        raise QEAssistanceContractError(REASON_AUTHORITY, "artifact fixed identity differs")
    coefficients = body.get("daily_coefficients")
    applicability = body.get("stock_sector_applicability_by_date")
    if not isinstance(coefficients, Mapping) or not isinstance(applicability, Mapping):
        raise QEAssistanceContractError(REASON_INPUT, "artifact coefficient/applicability maps are missing")
    if set(coefficients) != set(applicability) or len(coefficients) != body.get("date_count"):
        raise QEAssistanceContractError(REASON_MAPPING, "artifact daily denominators differ")
    canonical_sectors = body.get("canonical_l1_codes")
    if (
        not isinstance(canonical_sectors, list)
        or len(canonical_sectors) != EXPECTED_SECTOR_COUNT
        or len(set(canonical_sectors)) != EXPECTED_SECTOR_COUNT
        or any(_SECTOR.fullmatch(str(value)) is None for value in canonical_sectors)
    ):
        raise QEAssistanceContractError(REASON_AUTHORITY, "artifact canonical L1 authority differs")
    expected_sectors = set(canonical_sectors)
    applied = 0
    not_applicable = 0
    for day, daily in applicability.items():
        day_coefficients = coefficients.get(day)
        if not isinstance(day_coefficients, Mapping) or set(day_coefficients) != expected_sectors:
            raise QEAssistanceContractError(REASON_STATE, "artifact daily coefficient denominator differs")
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) not in STATE_COEFFICIENTS.values()
            for value in day_coefficients.values()
        ):
            raise QEAssistanceContractError(REASON_STATE, "artifact daily coefficient is invalid")
        if not isinstance(daily, Mapping):
            raise QEAssistanceContractError(REASON_MAPPING, "artifact daily applicability is invalid")
        for symbol, entry in daily.items():
            if _SYMBOL.fullmatch(str(symbol)) is None or not isinstance(entry, Mapping):
                raise QEAssistanceContractError(REASON_MAPPING, "artifact applicability entry is invalid")
            if set(entry) != {
                "status",
                "sector_code",
                "reason_code",
                "adjustment_applied",
                "classification_receipt_hash",
                "index_membership_receipt_hash",
                "classification_row_hashes",
                "index_membership_row_hashes",
            }:
                raise QEAssistanceContractError(REASON_MAPPING, "artifact applicability shape differs")
            _require_sha256(entry["classification_receipt_hash"], "classification_receipt_hash", REASON_AUTHORITY)
            _require_sha256(entry["index_membership_receipt_hash"], "index_membership_receipt_hash", REASON_AUTHORITY)
            for field in ("classification_row_hashes", "index_membership_row_hashes"):
                hashes = entry[field]
                if not isinstance(hashes, list):
                    raise QEAssistanceContractError(REASON_AUTHORITY, "artifact row lineage differs")
                for value in hashes:
                    _require_sha256(value, field, REASON_AUTHORITY)
            status = entry.get("status")
            if status == APPLIED:
                if (
                    entry.get("adjustment_applied") is not True
                    or entry.get("reason_code") is not None
                    or entry.get("sector_code") not in day_coefficients
                ):
                    raise QEAssistanceContractError(REASON_MAPPING, "artifact applied entry differs")
                applied += 1
            elif status == NOT_APPLICABLE:
                if (
                    entry.get("adjustment_applied") is not False
                    or entry.get("reason_code") != APPROVED_UNAVAILABLE_REASON
                    or entry.get("sector_code") is not None
                ):
                    raise QEAssistanceContractError(REASON_MAPPING, "artifact non-applicable entry differs")
                not_applicable += 1
            else:
                raise QEAssistanceContractError(REASON_MAPPING, "artifact applicability status is unknown")
    if (
        applied != body.get("applied_row_count")
        or not_applicable != body.get("not_applicable_row_count")
        or applied + not_applicable != body.get("prediction_row_count")
    ):
        raise QEAssistanceContractError(REASON_MAPPING, "artifact applicability counts differ")


__all__ = [
    "ADJUSTMENT_MODE",
    "APPLIED",
    "APPROVED_UNAVAILABLE_REASON",
    "FORMULA_SHA256",
    "FORMULA_TEXT",
    "NOT_APPLICABLE",
    "QEAssistanceContractError",
    "SCHEMA_VERSION",
    "apply_artifact_entry",
    "apply_sign_safe_adjustment",
    "build_qe_assistance_artifact",
    "validate_qe_assistance_artifact",
]
