from __future__ import annotations

import bisect
import json
import os
import shutil
import subprocess
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd
import psutil
import psycopg2.extras

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import (
    ConsumedWindowV1,
    DecisionUse,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
)
from backend.services.event_signal.financial_event_adapter import (
    FINANCIAL_RULE_VERSION,
    classify_financial_row,
)
from backend.services.event_signal.tushare_event_raw_sync import source_row_hash
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


EXPERIMENT_ID = "ADVISORY-N3-FINANCIAL-EVENT-SOURCE-READINESS-V1"
HYPOTHESIS_FAMILY_ID = "ADVISORY-N3-UPSTREAM-NEW-SOURCE-V1"
RESEARCH_STAGE = "N3_FINANCIAL_EVENT_SOURCE_READINESS"
PARENT_ARM_ID = "CURRENT_IC_PARENT"
PARENT_COLUMNS = ("arm_id", "decision_as_of_trade_date", "instrument", "score")
PARENT_SHA256 = "48598f1afe893c1718098f258a69cc579d831c5e4bea6d54b290c7ac0bd3b039"
PARENT_SIZE_BYTES = 119_953_459
PARENT_ROW_COUNT = 1_710_301
PARENT_DECISION_DAY_COUNT = 386
PARENT_INSTRUMENT_COUNT = 4_503
PARENT_DATE_START = date(2024, 7, 4)
PARENT_DATE_END = date(2026, 2, 2)
MARGIN_ROUTE_RECEIPT_SHA256 = "0b566e6f65c8e5b2bb01cb702aa98695a427621b2637a279bc92dc1f54e86a57"
SOURCE_LOOKBACK_TRADING_DAYS = 252
LOOKBACKS = (0, 20, 60, 120, 252)
SOURCE_TIME_QUALITY = "DATE_ONLY_BACKFILLED_NON_VINTAGE"
EFFECTIVE_RULE = "announcement_date_only_next_trading_day"
BUNDLE_SCHEMA = "advisory_n3_financial_event_source_readiness_bundle_v1"
MAX_RSS_BYTES = 8 * 1024**3
MAX_TEMP_BYTES = 8 * 1024**3
READY_STATE = "SOURCE_READY_NAVIGATION_ONLY_NON_VINTAGE"
NOT_READY_STATE = "SOURCE_NOT_READY"
INVALID_STATE = "INVALID"
READY_NEXT_TASK = "N3_FINANCIAL_EVENT_INFORMATION_SET_MVE_DESIGN"
NOT_READY_NEXT_TASK = "N3_FINANCIAL_EVENT_SOURCE_GAP_DECISION"
INVALID_NEXT_TASK = "N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN"
SOURCE_TABLES = {
    "tushare_forecast": "market.tushare_forecast_raw",
    "tushare_express": "market.tushare_express_raw",
    "tushare_fina_indicator": "market.tushare_fina_indicator_raw",
}
EXPECTED_EVENT_TYPES = {
    "tushare_forecast": {
        "financial_forecast_loss",
        "financial_forecast_large_decline",
        "financial_forecast_turnaround",
        "financial_forecast_large_growth",
        "financial_forecast_neutral",
    },
    "tushare_express": {
        "financial_express_loss",
        "financial_express_large_decline",
        "financial_express_large_growth",
        "financial_express_neutral",
    },
    "tushare_fina_indicator": {
        "financial_indicator_loss",
        "financial_indicator_large_decline",
        "financial_indicator_large_growth",
        "financial_indicator_neutral",
    },
}
EXPECTED_REPORT_PERIODS = tuple(
    date(year, month, day)
    for year, periods in (
        (2023, ((6, 30), (9, 30), (12, 31))),
        (2024, ((3, 31), (6, 30), (9, 30), (12, 31))),
        (2025, ((3, 31), (6, 30), (9, 30), (12, 31))),
    )
    for month, day in periods
)
BUNDLE_MEMBERS = frozenset(
    {
        "source_request.json",
        "parent_identity.json",
        "event_source_projection.parquet",
        "source_support_daily.parquet",
        "source_revision_report.json",
        "source_readiness_receipt.json",
        "resource_report.json",
        "registry_record.json",
        "manifest.json",
    }
)


@dataclass(frozen=True)
class ParentExpectation:
    sha256: str = PARENT_SHA256
    size_bytes: int = PARENT_SIZE_BYTES
    row_count: int = PARENT_ROW_COUNT
    decision_day_count: int = PARENT_DECISION_DAY_COUNT
    instrument_count: int = PARENT_INSTRUMENT_COUNT
    date_start: date = PARENT_DATE_START
    date_end: date = PARENT_DATE_END


@dataclass(frozen=True)
class SourceThresholds:
    min_projection_rows: int = 20_000
    min_qualifying_rows: int = 5_000
    min_top20_disclosure_fraction_120d: float = 0.70
    min_top20_supported_days: int = 380
    min_top20_supported_count: int = 5
    min_top50_mixed_qualifying_days: int = 300
    max_event_type_drift_fraction: float = 0.02


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, default=_json_default) + "\n",
        encoding="utf-8",
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _raise(
            "financial-event source bundle JSON cannot be read",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )
    if not isinstance(payload, dict):
        _raise(
            "financial-event source bundle JSON is not an object",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
            path=path.as_posix(),
        )
    return payload


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(f"cannot JSON encode {type(value).__name__}")


def _git_output(repository_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repository_root), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def assert_clean_main(repository_root: str | Path) -> str:
    root = Path(repository_root).resolve()
    try:
        dirty = _git_output(root, "status", "--porcelain")
        commit = _git_output(root, "rev-parse", "HEAD")
        origin_main = _git_output(root, "rev-parse", "origin/main")
    except (OSError, subprocess.CalledProcessError) as exc:
        _raise(
            "repository identity cannot be read",
            "ADVISORY_N3_FINANCIAL_EVENT_SOURCE_IDENTITY_INVALID",
            error_type=type(exc).__name__,
        )
    if dirty:
        _raise(
            "formal source readiness requires a clean repository",
            "ADVISORY_N3_FINANCIAL_EVENT_SOURCE_IDENTITY_INVALID",
            dirty_paths=dirty.splitlines()[:20],
        )
    if commit != origin_main:
        _raise(
            "formal source readiness requires HEAD to equal origin/main",
            "ADVISORY_N3_FINANCIAL_EVENT_SOURCE_IDENTITY_INVALID",
            repository_commit=commit,
            origin_main_commit=origin_main,
        )
    return commit


def load_parent_projection(
    path: str | Path,
    *,
    expectation: ParentExpectation | None = ParentExpectation(),
) -> tuple[pd.DataFrame, dict[str, Any]]:
    target = Path(path).resolve()
    if not target.is_file():
        _raise(
            "frozen parent parquet is missing",
            "ADVISORY_N3_FINANCIAL_EVENT_PARENT_INVALID",
            path=target.as_posix(),
        )
    size = target.stat().st_size
    digest = sha256_file(target)
    if expectation is not None and (size != expectation.size_bytes or digest != expectation.sha256):
        _raise(
            "frozen parent parquet identity drifted",
            "ADVISORY_N3_FINANCIAL_EVENT_PARENT_INVALID",
            expected_size=expectation.size_bytes,
            actual_size=size,
            expected_sha256=expectation.sha256,
            actual_sha256=digest,
        )
    try:
        parent = pd.read_parquet(target, columns=list(PARENT_COLUMNS))
    except Exception as exc:
        _raise(
            "frozen parent key/score projection cannot be read",
            "ADVISORY_N3_FINANCIAL_EVENT_PARENT_INVALID",
            error_type=type(exc).__name__,
        )
    parent = parent.loc[parent["arm_id"].eq(PARENT_ARM_ID), list(PARENT_COLUMNS[1:])].copy()
    parent["decision_as_of_trade_date"] = pd.to_datetime(parent["decision_as_of_trade_date"], errors="raise").dt.date
    parent["instrument"] = parent["instrument"].astype(str)
    parent["score"] = pd.to_numeric(parent["score"], errors="coerce")
    duplicate_count = int(parent.duplicated(["decision_as_of_trade_date", "instrument"]).sum())
    nonfinite_count = int((~np.isfinite(parent["score"].to_numpy(dtype=float))).sum())
    identity = {
        "artifact_uri": target.as_posix(),
        "sha256": digest,
        "size_bytes": size,
        "row_count": int(len(parent)),
        "decision_day_count": int(parent["decision_as_of_trade_date"].nunique()),
        "instrument_count": int(parent["instrument"].nunique()),
        "date_start": min(parent["decision_as_of_trade_date"]).isoformat() if len(parent) else None,
        "date_end": max(parent["decision_as_of_trade_date"]).isoformat() if len(parent) else None,
        "duplicate_key_count": duplicate_count,
        "nonfinite_score_count": nonfinite_count,
        "read_columns": list(PARENT_COLUMNS),
        "target_columns_read": [],
    }
    if expectation is not None:
        observed = (
            identity["row_count"],
            identity["decision_day_count"],
            identity["instrument_count"],
            identity["date_start"],
            identity["date_end"],
        )
        expected = (
            expectation.row_count,
            expectation.decision_day_count,
            expectation.instrument_count,
            expectation.date_start.isoformat(),
            expectation.date_end.isoformat(),
        )
        if observed != expected:
            _raise(
                "frozen parent projection statistics drifted",
                "ADVISORY_N3_FINANCIAL_EVENT_PARENT_INVALID",
                expected=expected,
                observed=observed,
            )
    if duplicate_count or nonfinite_count or parent.empty:
        _raise(
            "frozen parent projection contains invalid keys or scores",
            "ADVISORY_N3_FINANCIAL_EVENT_PARENT_INVALID",
            duplicate_key_count=duplicate_count,
            nonfinite_score_count=nonfinite_count,
        )
    ranked = parent.sort_values(
        ["decision_as_of_trade_date", "score", "instrument"],
        ascending=[True, False, True],
        kind="mergesort",
    )
    ranked["parent_rank"] = ranked.groupby("decision_as_of_trade_date", sort=False).cumcount() + 1
    return ranked.reset_index(drop=True), identity


def verify_margin_route_receipt(
    path: str | Path,
    *,
    expected_sha256: str | None = MARGIN_ROUTE_RECEIPT_SHA256,
) -> dict[str, Any]:
    target = Path(path).resolve()
    if not target.is_file():
        _raise(
            "margin route receipt is missing",
            "ADVISORY_N3_FINANCIAL_EVENT_ROUTE_INVALID",
            path=target.as_posix(),
        )
    digest = sha256_file(target)
    if expected_sha256 is not None and digest != expected_sha256:
        _raise(
            "margin route receipt identity drifted",
            "ADVISORY_N3_FINANCIAL_EVENT_ROUTE_INVALID",
            expected_sha256=expected_sha256,
            actual_sha256=digest,
        )
    payload = _read_json(target)
    expected = {
        "status": "COMPLETE",
        "selected_trial_count": 0,
        "next_task": "N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN",
        "sealed_holdout_accessed": False,
        "runtime_eligible": False,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "final_model_written": False,
        "position_weight_output": False,
    }
    drift = {
        key: {"expected": value, "actual": payload.get(key)}
        for key, value in expected.items()
        if payload.get(key) != value
    }
    if drift:
        _raise(
            "margin receipt does not authorize financial-event source readiness",
            "ADVISORY_N3_FINANCIAL_EVENT_ROUTE_INVALID",
            drift=drift,
        )
    return {
        "artifact_uri": target.as_posix(),
        "sha256": digest,
        "size_bytes": target.stat().st_size,
        "receipt_id": payload.get("receipt_id"),
        "request_sha256": payload.get("request_sha256"),
        "source_identity_sha256": payload.get("source_identity_sha256"),
    }


def _normalize_raw_row(row: Mapping[str, Any], source_type: str) -> dict[str, Any]:
    payload = row.get("raw_payload")
    if not isinstance(payload, dict):
        _raise(
            "financial-event raw payload is not an object",
            "ADVISORY_N3_FINANCIAL_EVENT_SOURCE_INVALID",
            source_type=source_type,
            source_record_key=str(row.get("source_record_key")),
        )
    expected_hash = str(row["source_row_hash"])
    actual_hash = source_row_hash(payload)
    if expected_hash != actual_hash:
        _raise(
            "financial-event raw payload hash does not match its source identity",
            "ADVISORY_N3_FINANCIAL_EVENT_SOURCE_INVALID",
            source_type=source_type,
            source_record_key=str(row.get("source_record_key")),
            expected_source_row_hash=expected_hash,
            actual_source_row_hash=actual_hash,
        )
    return {
        "source_type": source_type,
        "source_record_key": str(row["source_record_key"]),
        "raw_observation_id": int(row["raw_observation_id"]),
        "source_row_hash": str(row["source_row_hash"]),
        "instrument": str(row["ts_code"]),
        "source_event_date": row["ann_date"],
        "report_period": row["report_period"],
        "first_seen_at": row["first_seen_at"],
        "raw_payload": payload,
    }


def project_earliest_raw_versions(
    raw_rows_by_source: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    trading_calendar: Sequence[date],
    source_start: date,
    source_end: date,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    calendar = sorted(set(trading_calendar))
    if not calendar:
        _raise(
            "trading calendar is empty",
            "ADVISORY_N3_FINANCIAL_EVENT_PIT_INVALID",
        )
    projected: list[dict[str, Any]] = []
    revision_report: dict[str, Any] = {
        "schema_version": "advisory_n3_financial_event_revision_report_v1",
        "financial_rule_version": FINANCIAL_RULE_VERSION,
        "sources": {},
    }
    for source_type in SOURCE_TABLES:
        raw_rows = [_normalize_raw_row(row, source_type) for row in raw_rows_by_source.get(source_type, ())]
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in raw_rows:
            grouped[row["source_record_key"]].append(row)
        multi_count = 0
        event_type_drift = 0
        classification_drift = 0
        for source_record_key, versions in grouped.items():
            versions.sort(
                key=lambda item: (
                    item["first_seen_at"],
                    item["raw_observation_id"],
                )
            )
            classifications = [classify_financial_row(source_type, item["raw_payload"]) for item in versions]
            if len(versions) > 1:
                multi_count += 1
                if len({item.event_type for item in classifications}) > 1:
                    event_type_drift += 1
                tuples = {
                    (
                        item.event_type,
                        str(item.severity_score),
                        str(item.confidence),
                        item.risk_level,
                        item.action,
                        item.should_signal,
                    )
                    for item in classifications
                }
                if len(tuples) > 1:
                    classification_drift += 1
            selected = versions[0]
            classification = classifications[0]
            event_date = selected["source_event_date"]
            if not isinstance(event_date, date):
                _raise(
                    "financial-event source_event_date is invalid",
                    "ADVISORY_N3_FINANCIAL_EVENT_PIT_INVALID",
                    source_record_key=source_record_key,
                )
            calendar_index = bisect.bisect_right(calendar, event_date)
            if calendar_index >= len(calendar):
                continue
            effective = calendar[calendar_index]
            if not source_start <= effective <= source_end:
                continue
            if classification.event_type not in EXPECTED_EVENT_TYPES[source_type]:
                _raise(
                    "financial-event type roster drifted",
                    "ADVISORY_N3_FINANCIAL_EVENT_SOURCE_INVALID",
                    source_type=source_type,
                    event_type=classification.event_type,
                )
            projected.append(
                {
                    "source_type": source_type,
                    "source_record_key": source_record_key,
                    "raw_observation_id": selected["raw_observation_id"],
                    "source_row_hash": selected["source_row_hash"],
                    "instrument": selected["instrument"],
                    "source_event_date": event_date,
                    "report_period": selected["report_period"],
                    "event_family": classification.event_family,
                    "event_type": classification.event_type,
                    "should_signal": bool(classification.should_signal),
                    "severity_score": float(classification.severity_score),
                    "confidence": float(classification.confidence),
                    "effective_trade_date": effective,
                    "source_time_quality": SOURCE_TIME_QUALITY,
                    "effective_rule": EFFECTIVE_RULE,
                }
            )
        denominator = multi_count or 1
        revision_report["sources"][source_type] = {
            "raw_row_count": len(raw_rows),
            "source_record_key_count": len(grouped),
            "multi_version_key_count": multi_count,
            "event_type_drift_key_count": event_type_drift,
            "classification_drift_key_count": classification_drift,
            "event_type_drift_fraction": event_type_drift / denominator,
            "classification_drift_fraction": classification_drift / denominator,
            "selected_version_rule": "EARLIEST_LOCAL_OBSERVATION",
        }
    projection = pd.DataFrame(projected)
    if projection.empty:
        projection = pd.DataFrame(
            columns=[
                "source_type",
                "source_record_key",
                "raw_observation_id",
                "source_row_hash",
                "instrument",
                "source_event_date",
                "report_period",
                "event_family",
                "event_type",
                "should_signal",
                "severity_score",
                "confidence",
                "effective_trade_date",
                "source_time_quality",
                "effective_rule",
            ]
        )
    projection = projection.sort_values(
        ["effective_trade_date", "instrument", "source_type", "source_record_key"],
        kind="mergesort",
    ).reset_index(drop=True)
    duplicate_count = int(projection.duplicated(["source_type", "source_record_key"]).sum())
    if duplicate_count:
        _raise(
            "financial-event projection contains duplicate source keys",
            "ADVISORY_N3_FINANCIAL_EVENT_SOURCE_INVALID",
            duplicate_count=duplicate_count,
        )
    return projection, revision_report


def _latest_gap_by_instrument(
    parent_instruments: np.ndarray,
    parent_trade_index: np.ndarray,
    event_instruments: np.ndarray,
    event_trade_index: np.ndarray,
) -> np.ndarray:
    sentinel = np.iinfo(np.int32).max
    gaps = np.full(len(parent_instruments), sentinel, dtype=np.int32)
    parent_groups: dict[str, list[int]] = defaultdict(list)
    event_groups: dict[str, list[int]] = defaultdict(list)
    for index, instrument in enumerate(parent_instruments):
        parent_groups[str(instrument)].append(index)
    for instrument, trade_index in zip(event_instruments, event_trade_index, strict=True):
        event_groups[str(instrument)].append(int(trade_index))
    for instrument, parent_positions in parent_groups.items():
        source_indices = event_groups.get(instrument)
        if not source_indices:
            continue
        source = np.asarray(sorted(source_indices), dtype=np.int32)
        positions = np.asarray(parent_positions, dtype=np.int64)
        target = parent_trade_index[positions]
        prior = np.searchsorted(source, target, side="right") - 1
        valid = prior >= 0
        gaps[positions[valid]] = target[valid] - source[prior[valid]]
    return gaps


def calculate_source_support(
    parent: pd.DataFrame,
    projection: pd.DataFrame,
    *,
    trading_calendar: Sequence[date],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    calendar = sorted(set(trading_calendar))
    index_by_date = {item: index for index, item in enumerate(calendar)}
    if any(item not in index_by_date for item in parent["decision_as_of_trade_date"]):
        _raise(
            "parent decision date is absent from the frozen calendar",
            "ADVISORY_N3_FINANCIAL_EVENT_PIT_INVALID",
        )
    parent_work = parent.copy()
    parent_work["trade_index"] = parent_work["decision_as_of_trade_date"].map(index_by_date).astype(np.int32)
    projection_work = projection.copy()
    if not projection_work.empty:
        if any(item not in index_by_date for item in projection_work["effective_trade_date"]):
            _raise(
                "event effective date is absent from the frozen calendar",
                "ADVISORY_N3_FINANCIAL_EVENT_PIT_INVALID",
            )
        projection_work["trade_index"] = projection_work["effective_trade_date"].map(index_by_date).astype(np.int32)
    parent_instruments = parent_work["instrument"].to_numpy(dtype=object)
    parent_indices = parent_work["trade_index"].to_numpy(dtype=np.int32)
    support_specs: dict[str, pd.Series] = {
        "disclosure": pd.Series(True, index=projection_work.index),
        "qualifying": projection_work.get("should_signal", pd.Series(False, index=projection_work.index)).astype(bool),
    }
    for source_type in SOURCE_TABLES:
        support_specs[f"disclosure_{source_type}"] = projection_work.get(
            "source_type", pd.Series("", index=projection_work.index)
        ).eq(source_type)
    gap_columns: dict[str, np.ndarray] = {}
    for support_name, source_mask in support_specs.items():
        selected = projection_work.loc[source_mask]
        gap_columns[support_name] = _latest_gap_by_instrument(
            parent_instruments,
            parent_indices,
            selected.get("instrument", pd.Series(dtype=str)).to_numpy(dtype=object),
            selected.get("trade_index", pd.Series(dtype=np.int32)).to_numpy(dtype=np.int32),
        )
    daily_rows: list[dict[str, Any]] = []
    summary: dict[str, Any] = {"lookbacks": {}}
    for support_name, gaps in gap_columns.items():
        support_summary: dict[str, Any] = {}
        for lookback in LOOKBACKS:
            supported = gaps <= lookback
            top20 = parent_work["parent_rank"].to_numpy() <= 20
            top50 = parent_work["parent_rank"].to_numpy() <= 50
            support_summary[str(lookback)] = {
                "all_fraction": float(supported.mean()),
                "top20_fraction": float(supported[top20].mean()),
                "top50_fraction": float(supported[top50].mean()),
            }
        summary["lookbacks"][support_name] = support_summary
    for decision_date, group in parent_work.groupby("decision_as_of_trade_date", sort=True):
        row: dict[str, Any] = {
            "decision_as_of_trade_date": decision_date,
            "parent_count": int(len(group)),
        }
        positions = group.index.to_numpy(dtype=np.int64)
        ranks = group["parent_rank"].to_numpy(dtype=np.int32)
        for support_name in ("disclosure", "qualifying"):
            gaps = gap_columns[support_name][positions]
            for lookback in LOOKBACKS:
                supported = gaps <= lookback
                row[f"{support_name}_{lookback}d_all_count"] = int(supported.sum())
                row[f"{support_name}_{lookback}d_top20_count"] = int(supported[ranks <= 20].sum())
                row[f"{support_name}_{lookback}d_top50_count"] = int(supported[ranks <= 50].sum())
        daily_rows.append(row)
    daily = pd.DataFrame(daily_rows)
    qualifying_top50 = daily["qualifying_120d_top50_count"]
    summary["daily"] = {
        "decision_day_count": int(len(daily)),
        "top20_disclosure_120d_min": int(daily["disclosure_120d_top20_count"].min()),
        "top20_disclosure_120d_median": float(daily["disclosure_120d_top20_count"].median()),
        "top20_disclosure_120d_max": int(daily["disclosure_120d_top20_count"].max()),
        "top20_supported_days_ge_min": int((daily["disclosure_120d_top20_count"] >= 5).sum()),
        "top50_mixed_qualifying_days": int(((qualifying_top50 > 0) & (qualifying_top50 < 50)).sum()),
    }
    return daily, summary


def evaluate_readiness(
    *,
    parent_identity: Mapping[str, Any],
    projection: pd.DataFrame,
    support: Mapping[str, Any],
    revision_report: Mapping[str, Any],
    diagnostic_pit_mismatch_count: int,
    thresholds: SourceThresholds = SourceThresholds(),
) -> tuple[str, tuple[str, ...]]:
    failures: list[str] = []
    if int(parent_identity["row_count"]) != PARENT_ROW_COUNT:
        failures.append("PARENT_ROW_COUNT_MISMATCH")
    if int(parent_identity["decision_day_count"]) != PARENT_DECISION_DAY_COUNT:
        failures.append("PARENT_DECISION_DAY_COUNT_MISMATCH")
    if int(parent_identity["instrument_count"]) != PARENT_INSTRUMENT_COUNT:
        failures.append("PARENT_INSTRUMENT_COUNT_MISMATCH")
    source_counts = projection.groupby("source_type").size().to_dict() if not projection.empty else {}
    # ALGO-COMPLEXITY-001: this loop is fixed at three source types and eleven
    # report periods; it neither joins market panels nor grows with row count.
    for source_type in SOURCE_TABLES:
        if int(source_counts.get(source_type, 0)) <= 0:
            failures.append(f"SOURCE_EMPTY:{source_type}")
        source_periods = set(projection.loc[projection["source_type"].eq(source_type), "report_period"].tolist())
        missing_periods = [item.isoformat() for item in EXPECTED_REPORT_PERIODS if item not in source_periods]
        if missing_periods:
            failures.append(f"REPORT_PERIOD_COVERAGE_GAP:{source_type}:{missing_periods!r}")
    if len(projection) < thresholds.min_projection_rows:
        failures.append("PROJECTION_SUPPORT_BELOW_MINIMUM")
    qualifying_count = int(projection.get("should_signal", pd.Series(dtype=bool)).astype(bool).sum())
    if qualifying_count < thresholds.min_qualifying_rows:
        failures.append("QUALIFYING_EVENT_SUPPORT_BELOW_MINIMUM")
    top20_fraction = float(support["lookbacks"]["disclosure"]["120"]["top20_fraction"])
    if top20_fraction < thresholds.min_top20_disclosure_fraction_120d:
        failures.append("TOP20_DISCLOSURE_SUPPORT_BELOW_MINIMUM")
    daily = support["daily"]
    if int(daily["top20_supported_days_ge_min"]) < thresholds.min_top20_supported_days:
        failures.append("TOP20_SUPPORTED_DAY_COUNT_BELOW_MINIMUM")
    if int(daily["top20_disclosure_120d_min"]) < thresholds.min_top20_supported_count:
        failures.append("TOP20_DAILY_SUPPORT_BELOW_MINIMUM")
    if int(daily["top50_mixed_qualifying_days"]) < thresholds.min_top50_mixed_qualifying_days:
        failures.append("QUALIFYING_INTERVENTION_DAY_COUNT_BELOW_MINIMUM")
    if diagnostic_pit_mismatch_count:
        failures.append("DATE_ONLY_NEXT_TRADING_DAY_MISMATCH")
    for source_type in SOURCE_TABLES:
        source_report = revision_report["sources"].get(source_type, {})
        if float(source_report.get("event_type_drift_fraction", 1.0)) > thresholds.max_event_type_drift_fraction:
            failures.append(f"EVENT_TYPE_REVISION_DRIFT:{source_type}")
    numeric = (
        projection[["severity_score", "confidence"]].to_numpy(dtype=float) if not projection.empty else np.empty((0, 2))
    )
    if numeric.size and (not np.isfinite(numeric).all() or (numeric < 0).any() or (numeric > 1).any()):
        failures.append("CLASSIFICATION_NUMERIC_INVALID")
    state = READY_STATE if not failures else NOT_READY_STATE
    return state, tuple(sorted(failures))


def read_database_snapshot(
    connection: Any,
    *,
    parent_date_start: date,
    parent_date_end: date,
) -> dict[str, Any]:
    query_count = 0
    raw_rows_by_source: dict[str, list[dict[str, Any]]] = {}
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute("SHOW transaction_read_only")
        query_count += 1
        read_only = str(cursor.fetchone()["transaction_read_only"]).lower()
        cursor.execute("SHOW transaction_isolation")
        query_count += 1
        isolation = str(cursor.fetchone()["transaction_isolation"]).lower()
        if read_only != "on" or isolation != "repeatable read":
            _raise(
                "database transaction is not repeatable-read/read-only",
                "ADVISORY_N3_FINANCIAL_EVENT_DATABASE_MODE_INVALID",
                transaction_read_only=read_only,
                transaction_isolation=isolation,
            )
        cursor.execute("SELECT current_database() AS database_name, txid_current_snapshot()::text AS snapshot_id")
        query_count += 1
        db_identity = dict(cursor.fetchone())
        cursor.execute(
            """
            SELECT cal_date
              FROM market.trading_calendar
             WHERE is_trading = TRUE AND cal_date <= %s
             ORDER BY cal_date
            """,
            (parent_date_end + timedelta(days=10),),
        )
        query_count += 1
        calendar = [item["cal_date"] for item in cursor.fetchall()]
        try:
            parent_start_index = calendar.index(parent_date_start)
        except ValueError:
            _raise(
                "parent start date is missing from database trading calendar",
                "ADVISORY_N3_FINANCIAL_EVENT_PIT_INVALID",
                parent_date_start=parent_date_start.isoformat(),
            )
        if parent_start_index < SOURCE_LOOKBACK_TRADING_DAYS:
            _raise(
                "database trading calendar has insufficient source lookback",
                "ADVISORY_N3_FINANCIAL_EVENT_PIT_INVALID",
                available_prior_days=parent_start_index,
            )
        source_start = calendar[parent_start_index - SOURCE_LOOKBACK_TRADING_DAYS]
        raw_query_start = source_start - timedelta(days=10)
        for source_type, table_name in SOURCE_TABLES.items():
            cursor.execute(
                f"""
                SELECT raw_observation_id, source_record_key, source_row_hash,
                       ts_code, ann_date, report_period, first_seen_at, raw_payload
                  FROM {table_name}
                 WHERE ann_date >= %s AND ann_date <= %s
                 ORDER BY source_record_key, first_seen_at, raw_observation_id
                """,
                (raw_query_start, parent_date_end),
            )
            query_count += 1
            raw_rows_by_source[source_type] = [dict(item) for item in cursor.fetchall()]
        cursor.execute(
            """
            SELECT source_event_date, effective_trade_date
              FROM market.event_signal
             WHERE time_mode = 'backtest'
               AND signal_status = 'ACTIVE'
               AND effective_trade_date >= %s
               AND effective_trade_date <= %s
               AND (source_type LIKE 'tushare_%%' OR source_type = 'financial_relation')
            """,
            (source_start, parent_date_end),
        )
        query_count += 1
        diagnostic_rows = [dict(item) for item in cursor.fetchall()]
    calendar_set = set(calendar)
    mismatch_count = 0
    for row in diagnostic_rows:
        pos = bisect.bisect_right(calendar, row["source_event_date"])
        expected = calendar[pos] if pos < len(calendar) else None
        if expected != row["effective_trade_date"] or expected not in calendar_set:
            mismatch_count += 1
    return {
        "database_identity": {
            "database_name": str(db_identity["database_name"]),
            "snapshot_id": str(db_identity["snapshot_id"]),
            "transaction_read_only": read_only,
            "transaction_isolation": isolation,
        },
        "trading_calendar": calendar,
        "source_start": source_start,
        "raw_rows_by_source": raw_rows_by_source,
        "diagnostic_event_signal_row_count": len(diagnostic_rows),
        "diagnostic_pit_mismatch_count": mismatch_count,
        "database_query_count": query_count,
        "database_write_count": 0,
    }


def _file_descriptor(path: Path) -> dict[str, Any]:
    descriptor: dict[str, Any] = {
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }
    if path.suffix == ".parquet":
        descriptor["row_count"] = int(len(pd.read_parquet(path)))
    return descriptor


def _build_manifest(directory: Path, bundle_id: str) -> dict[str, Any]:
    files = {
        path.name: _file_descriptor(path)
        for path in sorted(directory.iterdir(), key=lambda item: item.name)
        if path.is_file() and path.name != "manifest.json"
    }
    return {
        "schema_version": BUNDLE_SCHEMA,
        "bundle_id": bundle_id,
        "files": files,
        "sealed_holdout_accessed": False,
        "database_write_count": 0,
        "network_call_count": 0,
        "tushare_call_count": 0,
        "runtime_eligible": False,
        "activated": False,
    }


def inspect_financial_event_source_bundle(bundle_path: str | Path) -> dict[str, Any]:
    path = Path(bundle_path).resolve()
    if not path.is_dir() or {item.name for item in path.iterdir() if item.is_file()} != BUNDLE_MEMBERS:
        _raise(
            "financial-event source bundle member closure is invalid",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
            bundle_path=path.as_posix(),
        )
    manifest = _read_json(path / "manifest.json")
    if manifest.get("schema_version") != BUNDLE_SCHEMA or manifest.get("bundle_id") != path.name:
        _raise(
            "financial-event source bundle identity is invalid",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
            bundle_path=path.as_posix(),
        )
    expected_files = BUNDLE_MEMBERS - {"manifest.json"}
    if set(manifest.get("files", {})) != expected_files:
        _raise(
            "financial-event source manifest closure is invalid",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
        )
    for name, expected in manifest["files"].items():
        actual = _file_descriptor(path / name)
        if actual != expected:
            _raise(
                "financial-event source bundle member mutated",
                "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
                member=name,
            )
    receipt = _read_json(path / "source_readiness_receipt.json")
    if receipt.get("bundle_id") != path.name:
        _raise(
            "financial-event source receipt belongs to another bundle",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
        )
    if receipt.get("source_state") not in {READY_STATE, NOT_READY_STATE}:
        _raise(
            "financial-event source receipt has invalid state",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
        )
    expected_next = READY_NEXT_TASK if receipt["source_state"] == READY_STATE else NOT_READY_NEXT_TASK
    if receipt.get("next_task") != expected_next:
        _raise(
            "financial-event source state and next task disagree",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
        )
    false_fields = (
        "sealed_holdout_accessed",
        "factor_catalog_write",
        "strategy_package_write",
        "runtime_activation",
        "position_or_order_write",
    )
    if any(receipt.get(field) is not False for field in false_fields):
        _raise(
            "financial-event source receipt has a forbidden side effect",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
        )
    if any(
        int(receipt.get(field, -1)) != 0
        for field in (
            "planned_model_trial_count",
            "generated_model_trial_count",
            "evaluated_model_trial_count",
            "selected_model_trial_count",
            "database_write_count",
            "network_call_count",
            "tushare_call_count",
        )
    ):
        _raise(
            "financial-event source receipt trial/write counters are invalid",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
        )
    request = _read_json(path / "source_request.json")
    functional_request = {key: value for key, value in request.items() if key not in {"request_id", "request_sha256"}}
    request_sha256 = canonical_json_sha256(functional_request)
    if (
        request.get("request_sha256") != request_sha256
        or request.get("request_id") != f"advfevsrc_{request_sha256[:24]}"
    ):
        _raise(
            "financial-event source request identity is invalid",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
        )
    try:
        record = build_trial_record(
            **{
                key: value
                for key, value in _read_json(path / "registry_record.json").items()
                if key not in {"registry_entry_id", "record_sha256"}
            }
        )
    except Exception as exc:
        _raise(
            "financial-event source registry record is invalid",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
            error_type=type(exc).__name__,
        )
    if (
        record.study_type != ResearchStudyType.ORACLE_DIAGNOSTIC
        or record.decision_use != DecisionUse.NAVIGATION_ONLY
        or any(
            (
                record.planned_trial_count,
                record.generated_trial_count,
                record.evaluated_trial_count,
                record.selected_trial_count,
            )
        )
    ):
        _raise(
            "financial-event source registry semantics are invalid",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
        )
    return {
        "status": "VALID",
        "bundle_id": path.name,
        "source_state": receipt["source_state"],
        "next_task": receipt["next_task"],
        "failure_reasons": receipt.get("failure_reasons", []),
    }


def _request_payload(
    *,
    parent_identity: Mapping[str, Any],
    margin_route_identity: Mapping[str, Any],
    repository_commit: str,
    rule_source_sha256: str,
    database_name: str,
    source_start: date,
    output_root: Path,
    registry_path: Path,
    route_path: Path,
) -> dict[str, Any]:
    functional = {
        "schema_version": "advisory_n3_financial_event_source_request_v1",
        "experiment_id": EXPERIMENT_ID,
        "research_stage": RESEARCH_STAGE,
        "objective_contract": ObjectiveContract.ALPHA_RANKING.value,
        "study_type": ResearchStudyType.ORACLE_DIAGNOSTIC.value,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "parent_identity": dict(parent_identity),
        "margin_route_identity": dict(margin_route_identity),
        "repository_commit": repository_commit,
        "rule_source_sha256": rule_source_sha256,
        "query_contract_sha256": canonical_json_sha256(
            {
                "source_tables": SOURCE_TABLES,
                "version_rule": "EARLIEST_LOCAL_OBSERVATION",
                "calendar_table": "market.trading_calendar",
                "diagnostic_tables": ["market.event_fact", "market.event_signal"],
                "query_version": "financial_event_source_readiness_select_v1",
            }
        ),
        "database_name": database_name,
        "source_types": list(SOURCE_TABLES),
        "source_start": source_start.isoformat(),
        "source_end": PARENT_DATE_END.isoformat(),
        "lookbacks": list(LOOKBACKS),
        "source_version_rule": "EARLIEST_LOCAL_OBSERVATION",
        "source_time_quality": SOURCE_TIME_QUALITY,
        "financial_rule_version": FINANCIAL_RULE_VERSION,
        "thresholds": SourceThresholds().__dict__,
        "output_root": output_root.resolve().as_posix(),
        "registry_path": registry_path.resolve().as_posix(),
        "route_path": route_path.resolve().as_posix(),
        "planned_model_trial_count": 0,
        "database_write_allowed": False,
        "network_allowed": False,
        "tushare_allowed": False,
        "sealed_holdout_accessed": False,
        "runtime_eligible": False,
    }
    digest = canonical_json_sha256(functional)
    return {
        **functional,
        "request_id": f"advfevsrc_{digest[:24]}",
        "request_sha256": digest,
    }


def build_financial_event_source_bundle(
    *,
    parent_path: str | Path,
    margin_receipt_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    registry_path: str | Path,
    route_path: str | Path,
    connection_factory: Callable[[], Any],
    expectation: ParentExpectation | None = ParentExpectation(),
    expected_margin_receipt_sha256: str | None = MARGIN_ROUTE_RECEIPT_SHA256,
    expected_database_name: str = "aistock",
    require_clean_main: bool = True,
) -> Path:
    started = time.monotonic()
    rss_samples = [int(psutil.Process(os.getpid()).memory_info().rss)]
    repository = Path(repository_root).resolve()
    repository_commit = assert_clean_main(repository) if require_clean_main else "TEST_FIXTURE_COMMIT"
    margin_route_identity = verify_margin_route_receipt(
        margin_receipt_path,
        expected_sha256=expected_margin_receipt_sha256,
    )
    parent, parent_identity = load_parent_projection(parent_path, expectation=expectation)
    rss_samples.append(int(psutil.Process(os.getpid()).memory_info().rss))
    connection = connection_factory()
    try:
        connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        snapshot = read_database_snapshot(
            connection,
            parent_date_start=min(parent["decision_as_of_trade_date"]),
            parent_date_end=max(parent["decision_as_of_trade_date"]),
        )
        rss_samples.append(int(psutil.Process(os.getpid()).memory_info().rss))
        connection.rollback()
    finally:
        connection.close()
    if snapshot["database_identity"]["database_name"] != expected_database_name:
        _raise(
            "financial-event source probe connected to an unexpected database",
            "ADVISORY_N3_FINANCIAL_EVENT_DATABASE_MODE_INVALID",
            expected_database_name=expected_database_name,
            actual_database_name=snapshot["database_identity"]["database_name"],
        )
    projection, revision_report = project_earliest_raw_versions(
        snapshot["raw_rows_by_source"],
        trading_calendar=snapshot["trading_calendar"],
        source_start=snapshot["source_start"],
        source_end=max(parent["decision_as_of_trade_date"]),
    )
    rss_samples.append(int(psutil.Process(os.getpid()).memory_info().rss))
    daily, support = calculate_source_support(
        parent,
        projection,
        trading_calendar=snapshot["trading_calendar"],
    )
    rss_samples.append(int(psutil.Process(os.getpid()).memory_info().rss))
    source_state, failures = evaluate_readiness(
        parent_identity=parent_identity,
        projection=projection,
        support=support,
        revision_report=revision_report,
        diagnostic_pit_mismatch_count=snapshot["diagnostic_pit_mismatch_count"],
    )
    output = Path(output_root).resolve()
    registry = Path(registry_path).resolve()
    route = Path(route_path).resolve()
    request = _request_payload(
        parent_identity=parent_identity,
        margin_route_identity=margin_route_identity,
        repository_commit=repository_commit,
        rule_source_sha256=sha256_file(
            Path(__file__).resolve().parents[1] / "event_signal" / "financial_event_adapter.py"
        ),
        database_name=snapshot["database_identity"]["database_name"],
        source_start=snapshot["source_start"],
        output_root=output,
        registry_path=registry,
        route_path=route,
    )
    output.mkdir(parents=True, exist_ok=True)
    bundle_root = output / "financial_event_source_bundles"
    bundle_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".financial-event-source-", dir=bundle_root))
    try:
        _write_json(temporary / "source_request.json", request)
        _write_json(temporary / "parent_identity.json", parent_identity)
        projection.to_parquet(temporary / "event_source_projection.parquet", index=False)
        daily.to_parquet(temporary / "source_support_daily.parquet", index=False)
        _write_json(temporary / "source_revision_report.json", revision_report)
        sampled_rss = max(rss_samples, default=0)
        semantic_temp_bytes = sum(item.stat().st_size for item in temporary.iterdir() if item.is_file())
        if sampled_rss > MAX_RSS_BYTES or semantic_temp_bytes > MAX_TEMP_BYTES:
            _raise(
                "financial-event source readiness exceeded its resource contract",
                "ADVISORY_N3_FINANCIAL_EVENT_RESOURCE_LIMIT",
                max_sampled_rss_bytes=sampled_rss,
                temporary_bytes=semantic_temp_bytes,
            )
        semantic_descriptors = {
            name: _file_descriptor(temporary / name)
            for name in (
                "source_request.json",
                "parent_identity.json",
                "event_source_projection.parquet",
                "source_support_daily.parquet",
                "source_revision_report.json",
            )
        }
        bundle_id = canonical_json_sha256(
            {
                "schema_version": BUNDLE_SCHEMA,
                "request_sha256": request["request_sha256"],
                "semantic_files": semantic_descriptors,
            }
        )
        final = bundle_root / bundle_id
        if final.exists():
            inspected = inspect_financial_event_source_bundle(final)
            if inspected["source_state"] != source_state:
                _raise(
                    "existing semantic bundle has a different source state",
                    "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
                )
            shutil.rmtree(temporary)
            return final
        for prior_bundle in bundle_root.iterdir():
            if not prior_bundle.is_dir() or prior_bundle.name.startswith(".financial-event-source-"):
                continue
            prior_request_path = prior_bundle / "source_request.json"
            if not prior_request_path.is_file():
                continue
            prior_request = _read_json(prior_request_path)
            if prior_request.get("request_id") == request["request_id"]:
                _raise(
                    "the live source changed after this request identity was first published",
                    "ADVISORY_N3_FINANCIAL_EVENT_SOURCE_SNAPSHOT_DRIFT",
                    request_id=request["request_id"],
                    prior_bundle_id=prior_bundle.name,
                    current_bundle_id=bundle_id,
                )
        next_task = READY_NEXT_TASK if source_state == READY_STATE else NOT_READY_NEXT_TASK
        receipt = {
            "schema_version": "advisory_n3_financial_event_source_readiness_receipt_v1",
            "bundle_id": bundle_id,
            "request_id": request["request_id"],
            "source_state": source_state,
            "next_task": next_task,
            "failure_reasons": list(failures),
            "projection_row_count": int(len(projection)),
            "qualifying_event_row_count": int(projection["should_signal"].sum()) if len(projection) else 0,
            "neutral_disclosure_row_count": int((~projection["should_signal"]).sum()) if len(projection) else 0,
            "support": support,
            "diagnostic_event_signal_row_count": snapshot["diagnostic_event_signal_row_count"],
            "diagnostic_pit_mismatch_count": snapshot["diagnostic_pit_mismatch_count"],
            "source_time_quality": SOURCE_TIME_QUALITY,
            "evidence_class": "EXPLORATORY_NON_VINTAGE",
            "planned_model_trial_count": 0,
            "generated_model_trial_count": 0,
            "evaluated_model_trial_count": 0,
            "selected_model_trial_count": 0,
            "sealed_holdout_accessed": False,
            "database_write_count": 0,
            "network_call_count": 0,
            "tushare_call_count": 0,
            "factor_catalog_write": False,
            "strategy_package_write": False,
            "runtime_activation": False,
            "position_or_order_write": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_json(temporary / "source_readiness_receipt.json", receipt)
        resource = {
            "schema_version": "advisory_n3_financial_event_source_resource_report_v1",
            "elapsed_seconds": round(time.monotonic() - started, 6),
            "database_name": snapshot["database_identity"]["database_name"],
            "database_snapshot_id": snapshot["database_identity"]["snapshot_id"],
            "transaction_read_only": snapshot["database_identity"]["transaction_read_only"],
            "transaction_isolation": snapshot["database_identity"]["transaction_isolation"],
            "database_query_count": snapshot["database_query_count"],
            "database_write_count": 0,
            "network_call_count": 0,
            "tushare_call_count": 0,
            "temporary_bytes": sum(item.stat().st_size for item in temporary.iterdir() if item.is_file()),
            "max_sampled_rss_bytes": max(
                sampled_rss,
                int(psutil.Process(os.getpid()).memory_info().rss),
            ),
            "max_rss_bytes": MAX_RSS_BYTES,
            "max_temp_bytes": MAX_TEMP_BYTES,
        }
        _write_json(temporary / "resource_report.json", resource)
        final_receipt_path = final / "source_readiness_receipt.json"
        evidence = evidence_reference_for_file(
            temporary / "source_readiness_receipt.json",
            role="n3_financial_event_source_readiness_receipt",
        ).model_copy(update={"artifact_uri": final_receipt_path.as_posix()})
        record = build_trial_record(
            experiment_id=EXPERIMENT_ID,
            attempt_id=request["request_id"],
            research_stage=RESEARCH_STAGE,
            study_type=ResearchStudyType.ORACLE_DIAGNOSTIC,
            hypothesis_family_id=HYPOTHESIS_FAMILY_ID,
            parent_lineage=("N3_MARGIN_SELECTED_ZERO", "N2B_CURRENT_IC_PARENT"),
            unique_variable="DATE_ONLY_FINANCIAL_EVENT_SOURCE_READINESS_V1",
            objective_contract=ObjectiveContract.ALPHA_RANKING,
            dataset_identity=canonical_json_sha256(
                {
                    "parent_sha256": parent_identity["sha256"],
                    "projection_sha256": semantic_descriptors["event_source_projection.parquet"]["sha256"],
                    "source_time_quality": SOURCE_TIME_QUALITY,
                }
            ),
            schema_identity=canonical_json_sha256(
                {
                    "columns": list(projection.columns),
                    "financial_rule_version": FINANCIAL_RULE_VERSION,
                }
            ),
            policy_identity="N2B_CURRENT_IC_PARENT_SOURCE_READINESS_NO_ECONOMIC_POLICY",
            planned_trial_count=0,
            generated_trial_count=0,
            evaluated_trial_count=0,
            selected_trial_count=0,
            consumed_windows=(
                ConsumedWindowV1(
                    window_id="P0_C_DEVELOPMENT_CONSUMED",
                    dataset_identity=parent_identity["sha256"],
                    start_date=PARENT_DATE_START,
                    end_date=PARENT_DATE_END,
                ),
            ),
            result_class=ResearchResultClass.EXPLORATORY,
            decision_use=DecisionUse.NAVIGATION_ONLY,
            evidence_refs=(evidence,),
            recorded_at=datetime.now(timezone.utc),
        )
        _write_json(temporary / "registry_record.json", record.model_dump(mode="json"))
        manifest = _build_manifest(temporary, bundle_id)
        _write_json(temporary / "manifest.json", manifest)
        temporary.replace(final)
        inspect_financial_event_source_bundle(final)
        return final
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise


def deliver_financial_event_source_bundle(
    *,
    bundle_path: str | Path,
    registry_path: str | Path,
    route_path: str | Path,
) -> dict[str, Any]:
    bundle = Path(bundle_path).resolve()
    inspected = inspect_financial_event_source_bundle(bundle)
    request = _read_json(bundle / "source_request.json")
    requested_registry = Path(str(request["registry_path"])).resolve()
    requested_route = Path(str(request["route_path"])).resolve()
    if requested_registry != Path(registry_path).resolve() or requested_route != Path(route_path).resolve():
        _raise(
            "delivery paths differ from the frozen source request",
            "ADVISORY_N3_FINANCIAL_EVENT_BUNDLE_INVALID",
            requested_registry=requested_registry.as_posix(),
            requested_route=requested_route.as_posix(),
        )
    record = build_trial_record(
        **{
            key: value
            for key, value in _read_json(bundle / "registry_record.json").items()
            if key not in {"registry_entry_id", "record_sha256"}
        }
    )
    registry_summary = AdvisoryResearchTrialRegistryV1(registry_path).append_batch((record,))
    route = Path(route_path).resolve()
    route.parent.mkdir(parents=True, exist_ok=True)
    route_payload = (
        "# Advisory model-first current route\n\n"
        f"- experiment_id: `{EXPERIMENT_ID}`\n"
        f"- source_state: `{inspected['source_state']}`\n"
        f"- next_task: `{inspected['next_task']}`\n"
        f"- bundle_id: `{inspected['bundle_id']}`\n"
        "- decision_use: `NAVIGATION_ONLY`\n"
        "- evidence_class: `EXPLORATORY_NON_VINTAGE`\n"
        "- sealed_holdout_accessed: `false`\n"
        "- runtime_eligible: `false`\n"
    )
    encoded = route_payload.encode("utf-8")
    if route.exists() and route.read_bytes() == encoded:
        route_write = "exact_noop"
    else:
        temporary_route = route.with_name(f".{route.name}.{os.getpid()}.tmp")
        temporary_route.write_bytes(encoded)
        os.replace(temporary_route, route)
        route_write = "updated"
    return {
        "status": "DELIVERED",
        "bundle": inspected,
        "registry": registry_summary,
        "route_path": route.as_posix(),
        "route_write": route_write,
    }


def connect_readonly_from_env() -> Any:
    config = {
        "host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("TDX_DB_PORT", "5432")),
        "dbname": os.getenv("TDX_DB_NAME", "aistock"),
        "user": os.getenv("TDX_DB_USER", "postgres"),
        "password": os.getenv("TDX_DB_PASSWORD", ""),
        "connect_timeout": 10,
        "options": "-c default_transaction_read_only=on -c statement_timeout=120000",
    }
    return psycopg2.connect(**config)
