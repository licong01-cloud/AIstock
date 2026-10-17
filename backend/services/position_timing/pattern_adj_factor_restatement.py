"""Fail-closed, file-only adjustment-factor restatement authority.

This module verifies a dataset-owned source restatement used by PT-NEXT-018.
It never treats an adjustment-factor change as an account or corporate-action
event and performs no database, provider, dataset, registry, or runtime writes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
from typing import Any, Mapping

import numpy as np

from .action_value import ActionValueError
from .action_value_data import DailyCandidate, file_reference
from .contracts import canonical_sha256


CANDIDATE_MANIFEST_SCHEMA = "qe_dataset_manifest_v1"
AUTHORITY_SCHEMA = "dataset_release_adj_factor_restatement_authority_v1"
AUTHORITY_COMPONENT = "adj_factor_restatement_authority"
AUTHORITY_RELATIVE_PATH = (
    Path("components")
    / "position_timing_source_authority_v1"
    / "adj_factor_restatement_authority.json"
)
REQUEST_ID = "PT-NEXT-018"
PROVIDER = "tushare"
NORMALIZED_FACTOR_ABS_TOLERANCE = Decimal("0.000001")
EXPECTED_STABLE_SEAMS: Mapping[str, tuple[date, date]] = {
    "300506.SZ": (date(2026, 7, 3), date(2026, 7, 6)),
    "688109.SH": (date(2026, 7, 8), date(2026, 7, 9)),
}
_SYMBOL = re.compile(r"^\d{6}\.(SH|SZ)$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFETY_KEYS = {
    "candidate_writes",
    "database_writes",
    "production_deletes",
    "production_pointer_changes",
    "production_writes",
    "provider_database_writes",
    "service_process_controls",
}


def _validated_sha256(value: Any, *, code: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _SHA256.fullmatch(normalized):
        raise ActionValueError(code)
    return normalized


def _parsed_date(value: Any, *, code: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise ActionValueError(code) from exc


def _positive_decimal(value: Any, *, code: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ActionValueError(code) from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ActionValueError(code)
    return parsed


@dataclass(frozen=True)
class AdjFactorSeries:
    symbol: str
    start: date
    end: date
    row_count: int
    ordered_rows_sha256: str
    rows: tuple[tuple[date, Decimal], ...]
    _by_date: Mapping[date, Decimal] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        dates = tuple(day for day, _ in self.rows)
        if (
            not self.rows
            or dates != tuple(sorted(dates))
            or len(dates) != len(set(dates))
            or self.start != dates[0]
            or self.end != dates[-1]
            or self.row_count != len(self.rows)
        ):
            raise ActionValueError(
                "ADJ_FACTOR_RESTATEMENT_SERIES_ORDER_INVALID", symbol=self.symbol
            )
        object.__setattr__(self, "_by_date", dict(self.rows))

    def normalized(self, day: date) -> Decimal:
        try:
            return self._by_date[day] / self.rows[-1][1]
        except KeyError as exc:
            raise ActionValueError(
                "ADJ_FACTOR_RESTATEMENT_DATE_UNAVAILABLE",
                symbol=self.symbol,
                trade_date=day.isoformat(),
            ) from exc


@dataclass(frozen=True)
class AdjFactorRestatementAuthority:
    candidate_root: Path
    candidate_manifest_reference: Mapping[str, Any]
    candidate_dataset_manifest_sha256: str
    candidate_revision: str
    cutoff_trade_date: date
    authority_path: Path
    authority_reference: Mapping[str, Any]
    authority_canonical_sha256: str
    diagnosis_sha256: str
    series: tuple[AdjFactorSeries, ...]
    _by_symbol: Mapping[str, AdjFactorSeries] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        symbols = tuple(item.symbol for item in self.series)
        if (
            not symbols
            or symbols != tuple(sorted(symbols))
            or len(symbols) != len(set(symbols))
        ):
            raise ActionValueError("ADJ_FACTOR_RESTATEMENT_SERIES_ORDER_INVALID")
        if symbols != tuple(sorted(EXPECTED_STABLE_SEAMS)):
            raise ActionValueError(
                "ADJ_FACTOR_RESTATEMENT_SERIES_SET_INVALID",
                expected=sorted(EXPECTED_STABLE_SEAMS),
                actual=list(symbols),
            )
        object.__setattr__(
            self, "_by_symbol", {item.symbol: item for item in self.series}
        )

    def for_symbol(self, symbol: str) -> AdjFactorSeries:
        try:
            return self._by_symbol[str(symbol).upper()]
        except KeyError as exc:
            raise ActionValueError(
                "ADJ_FACTOR_RESTATEMENT_SYMBOL_UNAVAILABLE", symbol=str(symbol).upper()
            ) from exc


def open_adj_factor_restatement_authority(
    *,
    candidate_root: Path,
    expected_candidate_manifest_sha256: str,
    expected_authority_canonical_sha256: str,
) -> AdjFactorRestatementAuthority:
    """Open only the restatement authority pinned by one immutable candidate."""

    root = candidate_root.resolve()
    expected_manifest = _validated_sha256(
        expected_candidate_manifest_sha256,
        code="PATTERN_CANDIDATE_MANIFEST_EXPECTED_IDENTITY_INVALID",
    )
    expected_authority = _validated_sha256(
        expected_authority_canonical_sha256,
        code="ADJ_FACTOR_RESTATEMENT_EXPECTED_IDENTITY_INVALID",
    )
    manifest_path = (root / "qe_dataset_manifest.json").resolve()
    if not manifest_path.is_relative_to(root):
        raise ActionValueError("PATTERN_CANDIDATE_MANIFEST_PATH_INVALID")
    manifest_reference = file_reference(manifest_path)
    if manifest_reference["sha256"] != expected_manifest:
        raise ActionValueError(
            "PATTERN_CANDIDATE_MANIFEST_IDENTITY_MISMATCH",
            expected=expected_manifest,
            actual=manifest_reference["sha256"],
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("PATTERN_CANDIDATE_MANIFEST_UNAVAILABLE") from exc
    manifest_identity = {
        key: value for key, value in manifest.items() if key != "dataset_manifest_sha256"
    }
    dataset_manifest_sha256 = _validated_sha256(
        manifest.get("dataset_manifest_sha256"),
        code="PATTERN_CANDIDATE_DATASET_IDENTITY_INVALID",
    )
    component = (manifest.get("components") or {}).get(AUTHORITY_COMPONENT)
    cutoff = _parsed_date(
        manifest.get("cutoff_trade_date"), code="PATTERN_CANDIDATE_CUTOFF_INVALID"
    )
    if (
        manifest.get("schema_version") != CANDIDATE_MANIFEST_SCHEMA
        or manifest.get("availability_status") != "CANDIDATE_READY"
        or dataset_manifest_sha256 != canonical_sha256(manifest_identity)
        or not isinstance(manifest.get("revision"), str)
        or not manifest.get("revision")
        or not isinstance(component, Mapping)
    ):
        raise ActionValueError("PATTERN_CANDIDATE_MANIFEST_CONTRACT_MISMATCH")

    relative_text = str(component.get("path") or "")
    relative = Path(relative_text)
    if relative.is_absolute() or relative.as_posix() != AUTHORITY_RELATIVE_PATH.as_posix():
        raise ActionValueError("ADJ_FACTOR_RESTATEMENT_MANIFEST_PATH_INVALID")
    authority_path = (root / relative).resolve()
    if not authority_path.is_relative_to(root):
        raise ActionValueError("ADJ_FACTOR_RESTATEMENT_PATH_OUTSIDE_CANDIDATE")
    authority_reference = file_reference(authority_path)
    try:
        expected_size = int(component.get("size", -1))
    except (TypeError, ValueError) as exc:
        raise ActionValueError("ADJ_FACTOR_RESTATEMENT_MANIFEST_IDENTITY_INVALID") from exc
    if (
        authority_reference["sha256"]
        != _validated_sha256(
            component.get("sha256"),
            code="ADJ_FACTOR_RESTATEMENT_MANIFEST_IDENTITY_INVALID",
        )
        or authority_reference["size_bytes"] != expected_size
    ):
        raise ActionValueError("ADJ_FACTOR_RESTATEMENT_MANIFEST_IDENTITY_MISMATCH")
    try:
        payload = json.loads(authority_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("ADJ_FACTOR_RESTATEMENT_AUTHORITY_UNAVAILABLE") from exc
    canonical_identity = {
        key: value for key, value in payload.items() if key != "canonical_sha256"
    }
    declared_authority = _validated_sha256(
        payload.get("canonical_sha256"),
        code="ADJ_FACTOR_RESTATEMENT_CANONICAL_IDENTITY_INVALID",
    )
    safety = payload.get("safety")
    raw_series = payload.get("series")
    if (
        payload.get("schema_version") != AUTHORITY_SCHEMA
        or declared_authority != canonical_sha256(canonical_identity)
        or declared_authority != expected_authority
        or payload.get("provider") != PROVIDER
        or payload.get("request_id") != REQUEST_ID
        or _parsed_date(
            payload.get("cutoff_trade_date"),
            code="ADJ_FACTOR_RESTATEMENT_CUTOFF_INVALID",
        )
        != cutoff
        or not isinstance(safety, Mapping)
        or set(safety) != _SAFETY_KEYS
        or any(value != 0 for value in safety.values())
        or not isinstance(raw_series, list)
        or not raw_series
    ):
        raise ActionValueError("ADJ_FACTOR_RESTATEMENT_AUTHORITY_CONTRACT_MISMATCH")

    series = tuple(_parse_series(item, cutoff=cutoff) for item in raw_series)
    if file_reference(authority_path) != authority_reference:
        raise ActionValueError("ADJ_FACTOR_RESTATEMENT_CHANGED_WHILE_READING")
    return AdjFactorRestatementAuthority(
        candidate_root=root,
        candidate_manifest_reference=manifest_reference,
        candidate_dataset_manifest_sha256=dataset_manifest_sha256,
        candidate_revision=str(manifest["revision"]),
        cutoff_trade_date=cutoff,
        authority_path=authority_path,
        authority_reference=authority_reference,
        authority_canonical_sha256=declared_authority,
        diagnosis_sha256=_validated_sha256(
            payload.get("diagnosis_sha256"),
            code="ADJ_FACTOR_RESTATEMENT_DIAGNOSIS_IDENTITY_INVALID",
        ),
        series=series,
    )


def _parse_series(item: Any, *, cutoff: date) -> AdjFactorSeries:
    if not isinstance(item, Mapping):
        raise ActionValueError("ADJ_FACTOR_RESTATEMENT_SERIES_SCHEMA_INVALID")
    symbol = str(item.get("ts_code") or "").upper()
    raw_rows = item.get("rows")
    try:
        row_count = int(item.get("row_count"))
    except (TypeError, ValueError) as exc:
        raise ActionValueError("ADJ_FACTOR_RESTATEMENT_SERIES_SCHEMA_INVALID") from exc
    if not _SYMBOL.fullmatch(symbol) or not isinstance(raw_rows, list) or not raw_rows:
        raise ActionValueError("ADJ_FACTOR_RESTATEMENT_SERIES_SCHEMA_INVALID")
    ordered_rows_sha256 = _validated_sha256(
        item.get("ordered_rows_sha256"),
        code="ADJ_FACTOR_RESTATEMENT_ROWS_IDENTITY_INVALID",
    )
    if ordered_rows_sha256 != canonical_sha256(raw_rows):
        raise ActionValueError(
            "ADJ_FACTOR_RESTATEMENT_ROWS_IDENTITY_MISMATCH", symbol=symbol
        )
    rows: list[tuple[date, Decimal]] = []
    for row in raw_rows:
        if (
            not isinstance(row, Mapping)
            or set(row) != {"ts_code", "trade_date", "adj_factor"}
            or str(row.get("ts_code") or "").upper() != symbol
        ):
            raise ActionValueError(
                "ADJ_FACTOR_RESTATEMENT_ROW_SCHEMA_INVALID", symbol=symbol
            )
        rows.append(
            (
                _parsed_date(
                    row.get("trade_date"), code="ADJ_FACTOR_RESTATEMENT_ROW_DATE_INVALID"
                ),
                _positive_decimal(
                    row.get("adj_factor"), code="ADJ_FACTOR_RESTATEMENT_FACTOR_INVALID"
                ),
            )
        )
    start = _parsed_date(item.get("start"), code="ADJ_FACTOR_RESTATEMENT_RANGE_INVALID")
    end = _parsed_date(item.get("end"), code="ADJ_FACTOR_RESTATEMENT_RANGE_INVALID")
    if end != cutoff:
        raise ActionValueError(
            "ADJ_FACTOR_RESTATEMENT_CUTOFF_MISMATCH", symbol=symbol
        )
    return AdjFactorSeries(
        symbol=symbol,
        start=start,
        end=end,
        row_count=row_count,
        ordered_rows_sha256=ordered_rows_sha256,
        rows=tuple(rows),
    )


def audit_candidate_adj_factor_restatement(
    candidate: DailyCandidate,
    authority: AdjFactorRestatementAuthority,
) -> dict[str, Any]:
    """Prove candidate factor parity and the two repaired stable seams."""

    if candidate.root.resolve() != authority.candidate_root:
        raise ActionValueError("ADJ_FACTOR_RESTATEMENT_CANDIDATE_ROOT_MISMATCH")
    calendar_dates = {stamp.date() for stamp in candidate.calendar}
    calendar_start = candidate.calendar[0].date()
    calendar_end = candidate.calendar[-1].date()
    reports: list[dict[str, Any]] = []
    mismatch_count = 0
    for series in authority.series:
        authority_dates = {day for day, _ in series.rows}
        scoped_authority_dates = {
            day for day in authority_dates if calendar_start <= day <= calendar_end
        }
        if not scoped_authority_dates.issubset(calendar_dates):
            raise ActionValueError(
                "ADJ_FACTOR_RESTATEMENT_NON_TRADING_DATE_IN_CANDIDATE_SCOPE",
                symbol=series.symbol,
            )
        bars = candidate.bars(series.symbol)
        factor = bars["factor"]
        finite_rows: list[tuple[date, Decimal]] = []
        for stamp, value in factor.items():
            if stamp.date() < series.start or stamp.date() > series.end:
                continue
            if value is not None and np.isfinite(value):
                finite_rows.append((stamp.date(), Decimal(str(float(value)))))
        candidate_dates = {day for day, _ in finite_rows}
        missing_authority_dates = sorted(candidate_dates - authority_dates)
        max_abs_error = Decimal("0")
        for day, actual in finite_rows:
            if day not in authority_dates:
                continue
            expected = series.normalized(day)
            error = abs(actual - expected)
            max_abs_error = max(max_abs_error, error)
            if error > NORMALIZED_FACTOR_ABS_TOLERANCE:
                mismatch_count += 1
        seam = EXPECTED_STABLE_SEAMS.get(series.symbol)
        if seam is None or not set(seam).issubset(authority_dates & candidate_dates):
            raise ActionValueError(
                "ADJ_FACTOR_RESTATEMENT_REQUIRED_SEAM_UNAVAILABLE", symbol=series.symbol
            )
        prior, current = seam
        authority_ratio = series.normalized(current) / series.normalized(prior)
        candidate_by_date = dict(finite_rows)
        candidate_ratio = candidate_by_date[current] / candidate_by_date[prior]
        if (
            abs(authority_ratio - Decimal("1")) > NORMALIZED_FACTOR_ABS_TOLERANCE
            or abs(candidate_ratio - Decimal("1")) > NORMALIZED_FACTOR_ABS_TOLERANCE
        ):
            raise ActionValueError(
                "ADJ_FACTOR_RESTATEMENT_FALSE_STITCH_REMAINS", symbol=series.symbol
            )
        reports.append(
            {
                "symbol": series.symbol,
                "start": series.start.isoformat(),
                "end": series.end.isoformat(),
                "authority_row_count": series.row_count,
                "candidate_finite_factor_count": len(finite_rows),
                "matched_candidate_factor_count": len(finite_rows)
                - len(missing_authority_dates),
                "candidate_factor_dates_without_authority": [
                    day.isoformat() for day in missing_authority_dates
                ],
                "authority_dates_without_candidate_factor_count": len(
                    scoped_authority_dates - candidate_dates
                ),
                "max_normalized_factor_abs_error": format(max_abs_error, "f"),
                "ordered_rows_sha256": series.ordered_rows_sha256,
                "stable_seam": {
                    "previous_trade_date": prior.isoformat(),
                    "current_trade_date": current.isoformat(),
                    "authority_normalized_ratio": format(authority_ratio, "f"),
                    "candidate_normalized_ratio": format(candidate_ratio, "f"),
                },
            }
        )
    audit: dict[str, Any] = {
        "schema_version": "position_timing_adj_factor_restatement_application_audit_v1",
        "candidate_manifest_sha256": authority.candidate_manifest_reference["sha256"],
        "candidate_dataset_manifest_sha256": authority.candidate_dataset_manifest_sha256,
        "candidate_revision": authority.candidate_revision,
        "authority_file_sha256": authority.authority_reference["sha256"],
        "authority_canonical_sha256": authority.authority_canonical_sha256,
        "diagnosis_sha256": authority.diagnosis_sha256,
        "normalized_factor_abs_tolerance": format(
            NORMALIZED_FACTOR_ABS_TOLERANCE, "f"
        ),
        "series": reports,
        "series_count": len(reports),
        "candidate_factor_dates_without_authority_count": sum(
            len(report["candidate_factor_dates_without_authority"]) for report in reports
        ),
        "normalized_factor_mismatch_count": mismatch_count,
        "required_stable_seam_count": len(EXPECTED_STABLE_SEAMS),
        "coverage_complete": mismatch_count == 0
        and all(not report["candidate_factor_dates_without_authority"] for report in reports),
        "factor_account_participation_inference": False,
        "outcomes_read": False,
        "database_read": False,
        "database_write": False,
        "candidate_write": False,
        "runtime_action_performed": False,
    }
    audit["audit_sha256"] = canonical_sha256(audit)
    if not audit["coverage_complete"]:
        raise ActionValueError("ADJ_FACTOR_RESTATEMENT_COVERAGE_INCOMPLETE")
    return audit
