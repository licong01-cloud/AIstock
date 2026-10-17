from __future__ import annotations

import json
import math
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, NoReturn, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.alpha_signal_audit_pipeline import (
    _git_command_for_worktree,
    _read_bundle as _read_n2a_bundle,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.leg_disagreement_pipeline import _read_leg_bundle
from backend.services.advisory_model_first.minute_information_set_contracts import (
    MINUTE_MVE_EXPERIMENT_ID,
    MINUTE_MVE_FEATURE_SCHEMA_HASH,
    MINUTE_MVE_FEATURE_SCHEMA_VERSION,
    MINUTE_MVE_HYPOTHESIS_FAMILY_ID,
    MINUTE_MVE_RAW_ECONOMIC_FEATURES,
    MINUTE_MVE_SESSION_WIDE_SINGLE_BAR_DEFICIT_DATES,
    MINUTE_MVE_SOURCE_FIELDS,
    FrozenMinuteInformationSetRequestV1,
    MinuteInformationSetReceiptV1,
    build_minute_information_set_receipt,
    build_minute_information_set_request,
)
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.qe_alpha_mve_pipeline import (
    _cross_os_git_commit,
    _cross_os_git_dirty_paths,
    _deflated_sharpe_diagnostic,
    _file_descriptors,
    _moving_block_interval,
    _parquet_row_count,
    _peak_rss_bytes,
    _safe_correlation,
    _verify_ref,
)
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import (
    AdvisoryResearchTrialRecordV1,
    ConsumedWindowV1,
    DecisionUse,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
)
from backend.services.advisory_model_first.tier1_oracle_pipeline import _read_n1_bundle
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SOURCE_REQUIRED_COLUMNS = (
    "decision_as_of_trade_date",
    "instrument",
    "score__IC_WEIGHTED_PARENT",
    "economic_net_excess_bps",
    "outcome_known",
)
MODEL_SCORE_COLUMNS = {
    "N3_MINUTE_PARENT_RIDGE_COMPARATOR_V1": "comparator_oof_score",
    "N3_MINUTE_INFORMATION_EXPANDED_V1": "candidate_oof_score",
}
QLIB_FIELDS = tuple(f"${field}" for field in MINUTE_MVE_SOURCE_FIELDS)
MINUTE_MVE_BUNDLE_SCHEMA = "advisory_n3_minute_information_set_bundle_v1"
RESULT_IDENTITY_MEMBERS = frozenset(
    {
        "feature_schema.json",
        "minute_source_inventory.parquet",
        "minute_coverage_daily.parquet",
        "minute_feature_panel.parquet",
        "oof_score_panel.parquet",
        "fold_diagnostics.parquet",
        "daily_metrics.parquet",
        "model_summary.json",
        "frontier_receipt.json",
    }
)
BUNDLE_MEMBERS = RESULT_IDENTITY_MEMBERS | {
    "request.json",
    "source_identity_receipt.json",
    "resource_report.json",
    "learnability_receipt.json",
    "registry_record.json",
}
MinuteLoader = Callable[[pd.Timestamp, Sequence[str], Sequence[str]], pd.DataFrame]


def prepare_minute_information_set_request(
    *,
    leg_bundle_path: str | Path,
    n2a_bundle_path: str | Path,
    n1_bundle_path: str | Path,
    source_spike_receipt_path: str | Path,
    minute_provider_uri: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
) -> FrozenMinuteInformationSetRequestV1:
    """Freeze one development-only N3 minute information-set request."""

    _require_formal_environment()
    leg_path = Path(leg_bundle_path).resolve()
    n2a_path = Path(n2a_bundle_path).resolve()
    n1_path = Path(n1_bundle_path).resolve()
    spike_path = Path(source_spike_receipt_path).resolve()
    provider_path = Path(minute_provider_uri).resolve()
    repo = Path(repository_root).resolve()
    leg = _read_leg_bundle(leg_path)
    n2a = _read_n2a_bundle(n2a_path)
    n1 = _read_n1_bundle(n1_path)
    spike = _read_source_spike_receipt(spike_path)
    _validate_bound_sources(leg=leg, n2a=n2a, n1=n1, spike=spike)
    dirty = _cross_os_git_dirty_paths(repo)
    if dirty:
        _raise(
            "minute information-set request requires a clean repository",
            "ADVISORY_N3_MINUTE_MVE_REQUEST_INVALID",
            dirty_paths=dirty[:20],
        )
    commit = _cross_os_git_commit(repo)
    origin_main_commit = _git_origin_main_commit(repo)
    if commit != origin_main_commit:
        _raise(
            "minute information-set request requires HEAD to equal origin/main",
            "ADVISORY_N3_MINUTE_MVE_REQUEST_INVALID",
            repository_commit=commit,
            origin_main_commit=origin_main_commit,
        )
    evidence_refs = tuple(
        evidence_reference_for_file(path, role=role)
        for role, path in (
            ("n3_minute_leg_manifest", leg_path / "manifest.json"),
            ("n3_minute_leg_receipt", leg_path / "learnability_receipt.json"),
            ("n3_minute_n2a_manifest", n2a_path / "manifest.json"),
            ("n3_minute_n2a_request", n2a_path / "request.json"),
            ("n3_minute_n2a_full_universe", n2a_path / "full_universe_signal_outcomes.parquet"),
            ("n3_minute_n1_manifest", n1_path / "manifest.json"),
            ("n3_minute_n1_cpcv", n1_path / "n1_label_interval_cpcv.json"),
            ("n3_minute_n1_regime_daily", n1_path / "learnability_daily.parquet"),
            ("n3_minute_source_spike_receipt", spike_path),
            ("n3_minute_source_meta", provider_path / "meta_export.json"),
            ("n3_minute_source_calendar", provider_path / "calendars" / "1min.txt"),
            ("n3_minute_source_instruments", provider_path / "instruments" / "all.txt"),
        )
    )
    _validate_source_control_refs(spike=spike, evidence_refs=evidence_refs)
    key_source = _read_parquet(
        n2a_path / "full_universe_signal_outcomes.parquet",
        columns=("decision_as_of_trade_date", "instrument"),
        reason_code="ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
    )
    inventory, minute_source_content_sha256 = fingerprint_minute_source(
        provider_path=provider_path,
        instruments=tuple(sorted(key_source["instrument"].astype(str).unique())),
    )
    source_dataset_identity = n2a["record"].dataset_identity
    route_dataset_identity = leg["record"].dataset_identity
    split_identity = n1["request"].split_policy_sha256
    dataset_identity = canonical_json_sha256(
        {
            "source_dataset_identity": source_dataset_identity,
            "route_dataset_identity": route_dataset_identity,
            "n1_split_policy_sha256": split_identity,
            "minute_source_content_sha256": minute_source_content_sha256,
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }
    )
    request = build_minute_information_set_request(
        evidence_refs=evidence_refs,
        leg_bundle_path=leg_path.as_posix(),
        leg_bundle_id=leg_path.name,
        leg_request_sha256=leg["request"].request_sha256,
        leg_receipt_sha256=leg["receipt"].receipt_sha256,
        n2a_bundle_path=n2a_path.as_posix(),
        n2a_bundle_id=n2a_path.name,
        n2a_request_sha256=n2a["request"].request_sha256,
        n2a_receipt_sha256=n2a["receipt"].receipt_sha256,
        n1_bundle_path=n1_path.as_posix(),
        n1_bundle_id=n1_path.name,
        n1_request_sha256=n1["request"].request_sha256,
        n1_split_policy_sha256=split_identity,
        source_spike_receipt_path=spike_path.as_posix(),
        source_spike_receipt_sha256=sha256_file(spike_path),
        source_dataset_identity=source_dataset_identity,
        route_dataset_identity=route_dataset_identity,
        minute_source_content_sha256=minute_source_content_sha256,
        minute_source_file_count=int(len(inventory)),
        dataset_identity=dataset_identity,
        policy_identity=leg["record"].policy_identity,
        minute_provider_uri=provider_path.as_posix(),
        registry_path=_resolve_bound_path(leg["request"].registry_path).as_posix(),
        route_path=_resolve_bound_path(leg["request"].route_path).as_posix(),
        repository_root=repo.as_posix(),
        repository_commit=commit,
        output_root=Path(output_root).resolve().as_posix(),
    )
    _write_immutable_request(Path(output_path).resolve(), request)
    return request


def fingerprint_minute_source(*, provider_path: Path, instruments: Sequence[str]) -> tuple[pd.DataFrame, str]:
    """Hash the exact required minute Bin files and provider control files."""

    meta_path = provider_path / "meta_export.json"
    calendar_path = provider_path / "calendars" / "1min.txt"
    manifest_path = provider_path / "instruments" / "all.txt"
    meta = _read_json(meta_path, "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH")
    if meta.get("snapshot_id") != "qlib_minute_authoritative_full_candidate_20240102_20260630":
        _raise(
            "minute source snapshot identity drift",
            "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
            snapshot_id=meta.get("snapshot_id"),
        )
    if tuple(meta.get("required_minute_fields", ())) != (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "factor",
        "up_limit_price",
        "down_limit_price",
        "prev_close",
        "limit_up",
        "limit_down",
    ):
        _raise(
            "minute source advertised field roster drift",
            "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    records: list[dict[str, Any]] = []
    for instrument in sorted(set(str(value) for value in instruments)):
        if instrument != instrument.upper():
            _raise(
                "minute source instrument is not canonical uppercase",
                "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
                instrument=instrument,
            )
        feature_root = provider_path / "features" / instrument.lower()
        for field in MINUTE_MVE_SOURCE_FIELDS:
            path = feature_root / f"{field}.1min.bin"
            if not path.is_file():
                _raise(
                    "minute source required Bin is missing",
                    "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
                    instrument=instrument,
                    field=field,
                    path=path.as_posix(),
                )
            stat = path.stat()
            records.append(
                {
                    "instrument": instrument,
                    "field": field,
                    "relative_path": path.relative_to(provider_path).as_posix(),
                    "size_bytes": int(stat.st_size),
                    "mtime_ns_telemetry": int(stat.st_mtime_ns),
                    "sha256": sha256_file(path),
                }
            )
    inventory = pd.DataFrame(records).sort_values(["instrument", "field"]).reset_index(drop=True)
    identity_rows = inventory.drop(columns=["mtime_ns_telemetry"]).to_dict(orient="records")
    digest = canonical_json_sha256(
        {
            "meta_export_sha256": sha256_file(meta_path),
            "calendar_sha256": sha256_file(calendar_path),
            "instrument_manifest_sha256": sha256_file(manifest_path),
            "required_files": identity_rows,
        }
    )
    return inventory, digest


class _QlibMinuteLoader:
    def __init__(self, provider_uri: str) -> None:
        try:
            import qlib
            from qlib.constant import REG_CN
            from qlib.data import D
        except ImportError as exc:  # pragma: no cover - formal environment contract
            _raise(
                "Qlib minute dependency is unavailable",
                "ADVISORY_N3_MINUTE_MVE_SOURCE_SCHEMA_INVALID",
                error_type=type(exc).__name__,
            )
        if getattr(qlib, "__version__", None) != "0.9.6.99":
            _raise(
                "Qlib version drift",
                "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
                qlib_version=getattr(qlib, "__version__", None),
            )
        qlib.init(provider_uri=provider_uri, region=REG_CN)
        self._data = D

    def __call__(self, decision_date: pd.Timestamp, instruments: Sequence[str], fields: Sequence[str]) -> pd.DataFrame:
        day = decision_date.strftime("%Y-%m-%d")
        return self._data.features(
            list(instruments),
            list(fields),
            start_time=f"{day} 09:30:00",
            end_time=f"{day} 15:00:00",
            freq="1min",
        )


def build_minute_feature_panel(
    *,
    source: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    loader: MinuteLoader,
    request: FrozenMinuteInformationSetRequestV1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Stream T-day minute bars and build the exact frozen feature panel."""

    missing = set(SOURCE_REQUIRED_COLUMNS) - set(source.columns)
    if missing:
        _raise(
            "minute information-set source omits required columns",
            "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
            missing_columns=sorted(missing),
        )
    frame = source.loc[:, SOURCE_REQUIRED_COLUMNS].copy()
    frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    frame["instrument"] = frame["instrument"].astype(str)
    if frame.empty or frame.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise("minute source keys are empty or duplicated", "ADVISORY_N3_MINUTE_MVE_PIT_LEAKAGE")
    if not frame["instrument"].eq(frame["instrument"].str.upper()).all():
        _raise("minute source instruments are not canonical", "ADVISORY_N3_MINUTE_MVE_PIT_LEAKAGE")
    parent = pd.to_numeric(frame["score__IC_WEIGHTED_PARENT"], errors="coerce")
    if not np.isfinite(parent.to_numpy(dtype=float)).all():
        _raise("parent score is non-finite", "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH")
    frame["score__IC_WEIGHTED_PARENT"] = parent
    calendar = pd.DatetimeIndex(pd.to_datetime(calendar)).sort_values()
    if calendar.empty or calendar.duplicated().any():
        _raise("minute calendar is empty or duplicated", "ADVISORY_N3_MINUTE_MVE_SOURCE_SCHEMA_INVALID")

    feature_days: list[pd.DataFrame] = []
    coverage_rows: list[dict[str, Any]] = []
    observed_session_deficit_dates: set[str] = set()
    for decision_date, day_source in frame.groupby("decision_as_of_trade_date", sort=True):
        decision_date = pd.Timestamp(decision_date).normalize()
        slots = calendar[calendar.normalize() == decision_date]
        if not len(slots) or slots.max() > decision_date + pd.Timedelta(hours=15):
            _raise(
                "minute calendar violates the T-day cutoff",
                "ADVISORY_N3_MINUTE_MVE_PIT_LEAKAGE",
                decision_date=decision_date.date().isoformat(),
            )
        day_source = day_source.sort_values("instrument").reset_index(drop=True)
        instruments = tuple(day_source["instrument"].astype(str))
        raw = loader(decision_date, instruments, QLIB_FIELDS)
        day_features, coverage = aggregate_minute_day(
            decision_date=decision_date,
            instruments=instruments,
            calendar_slots=slots,
            raw=raw,
            minimum_feature_coverage=request.minimum_feature_coverage,
        )
        if coverage["session_wide_single_bar_deficit"]:
            observed_session_deficit_dates.add(decision_date.date().isoformat())
        ranked_parent = day_source["score__IC_WEIGHTED_PARENT"].rank(method="average", pct=True, ascending=True)
        day_features["parent_rank_pct"] = ranked_parent.to_numpy(dtype="float32")
        for raw_feature in MINUTE_MVE_RAW_ECONOMIC_FEATURES:
            day_features[f"{raw_feature}_rank_pct"] = (
                day_features[raw_feature].rank(method="average", pct=True, ascending=True).astype("float32")
            )
        day_features["score__IC_WEIGHTED_PARENT"] = day_source["score__IC_WEIGHTED_PARENT"].to_numpy(dtype=float)
        day_features["economic_net_excess_bps"] = day_source["economic_net_excess_bps"].to_numpy()
        day_features["outcome_known"] = day_source["outcome_known"].to_numpy()
        feature_days.append(day_features)
        coverage_rows.append(coverage)
    if tuple(sorted(observed_session_deficit_dates)) != request.expected_session_wide_single_bar_deficit_dates:
        _raise(
            "session-wide single-bar deficit date identity drift",
            "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
            expected=list(request.expected_session_wide_single_bar_deficit_dates),
            actual=sorted(observed_session_deficit_dates),
        )
    features = (
        pd.concat(feature_days, ignore_index=True)
        .sort_values(["decision_as_of_trade_date", "instrument"])
        .reset_index(drop=True)
    )
    if len(features) != len(frame) or features.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise("minute feature key coverage drift", "ADVISORY_N3_MINUTE_MVE_PIT_LEAKAGE")
    if not np.isfinite(features[["parent_rank_pct", "minute_available", "minute_coverage_fraction"]]).all().all():
        _raise("minute mandatory features are non-finite", "ADVISORY_N3_MINUTE_MVE_SOURCE_SCHEMA_INVALID")
    return features, pd.DataFrame(coverage_rows).sort_values("decision_as_of_trade_date").reset_index(drop=True)


def aggregate_minute_day(
    *,
    decision_date: pd.Timestamp,
    instruments: Sequence[str],
    calendar_slots: pd.DatetimeIndex,
    raw: pd.DataFrame,
    minimum_feature_coverage: float,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Vectorize one day of raw Qlib minute bars without dropping normal missing keys."""

    if not isinstance(raw.index, pd.MultiIndex) or raw.index.nlevels != 2 or raw.index.duplicated().any():
        _raise("minute frame index is invalid", "ADVISORY_N3_MINUTE_MVE_SOURCE_SCHEMA_INVALID")
    missing_fields = set(QLIB_FIELDS) - set(raw.columns)
    if missing_fields:
        _raise(
            "minute frame omits required fields",
            "ADVISORY_N3_MINUTE_MVE_SOURCE_SCHEMA_INVALID",
            missing_fields=sorted(missing_fields),
        )
    decision_date = pd.Timestamp(decision_date).normalize()
    slots = pd.DatetimeIndex(calendar_slots).sort_values()
    if any(value.normalize() != decision_date for value in slots) or slots.max() > decision_date + pd.Timedelta(
        hours=15
    ):
        _raise("minute frame reads beyond T-day", "ADVISORY_N3_MINUTE_MVE_PIT_LEAKAGE")
    expected_index = pd.MultiIndex.from_product(
        [list(instruments), slots], names=[raw.index.names[0] or "instrument", raw.index.names[1] or "datetime"]
    )
    extra = raw.index.difference(expected_index)
    if len(extra):
        _raise("minute frame contains unexpected keys", "ADVISORY_N3_MINUTE_MVE_PIT_LEAKAGE", extra_count=len(extra))
    ordered = raw.loc[:, QLIB_FIELDS].reindex(expected_index)
    n_instruments, n_slots = len(instruments), len(slots)
    arrays = {
        field: pd.to_numeric(ordered[f"${field}"], errors="coerce")
        .to_numpy(dtype=float)
        .reshape(n_instruments, n_slots)
        for field in MINUTE_MVE_SOURCE_FIELDS
    }
    for field in ("open", "high", "low", "close"):
        values = arrays[field]
        if (np.isfinite(values) & (values <= 0)).any():
            _raise(
                "minute price contains non-positive finite values",
                "ADVISORY_N3_MINUTE_MVE_SOURCE_SCHEMA_INVALID",
                field=field,
            )
    for field in ("volume", "amount"):
        values = arrays[field]
        if (np.isfinite(values) & (values < 0)).any():
            _raise(
                "minute activity contains negative finite values",
                "ADVISORY_N3_MINUTE_MVE_SOURCE_SCHEMA_INVALID",
                field=field,
            )
    for field in ("limit_up", "limit_down"):
        values = arrays[field]
        if (np.isfinite(values) & ~np.isin(values, (0.0, 1.0))).any():
            _raise(
                "minute limit flag is not binary",
                "ADVISORY_N3_MINUTE_MVE_SOURCE_SCHEMA_INVALID",
                field=field,
            )
    raw_valid_bar = np.ones((n_instruments, n_slots), dtype=bool)
    for field in ("open", "high", "low", "close"):
        raw_valid_bar &= np.isfinite(arrays[field]) & (arrays[field] > 0)
    market_empty = ~raw_valid_bar.any(axis=0)
    session_wide_single_bar_deficit = bool(
        n_slots > 0
        and n_instruments > 0
        and not market_empty.any()
        and np.equal(raw_valid_bar.sum(axis=1), n_slots - 1).all()
    )
    market_empty_slots = tuple(value.strftime("%Y-%m-%d %H:%M:%S") for value in slots[market_empty])
    effective = ~market_empty
    effective_slots = slots[effective]
    if not len(effective_slots):
        effective_count = 0
        sliced = {field: values[:, :0] for field, values in arrays.items()}
    else:
        effective_count = len(effective_slots)
        sliced = {field: values[:, effective] for field, values in arrays.items()}
    valid = np.ones((n_instruments, effective_count), dtype=bool)
    for field in ("open", "high", "low", "close"):
        valid &= np.isfinite(sliced[field]) & (sliced[field] > 0)
    valid_count = valid.sum(axis=1)
    available = valid_count > 0
    coverage = valid_count / effective_count if effective_count else np.zeros(n_instruments, dtype=float)
    eligible = coverage >= minimum_feature_coverage
    open_times = np.array([value.time() for value in effective_slots], dtype=object)
    opening_mask = np.array([value.strftime("%H:%M:%S") <= "10:00:00" for value in effective_slots], dtype=bool)
    closing_mask = np.array([value.strftime("%H:%M:%S") >= "14:30:00" for value in effective_slots], dtype=bool)
    del open_times

    opening_return = _window_return(sliced["open"][:, opening_mask], sliced["close"][:, opening_mask])
    closing_return = _window_return(sliced["open"][:, closing_mask], sliced["close"][:, closing_mask])
    close = sliced["close"]
    adjacent_calendar_slots = np.diff(effective_slots.asi8) == pd.Timedelta(minutes=1).value
    adjacent_valid = (
        np.isfinite(close[:, 1:])
        & np.isfinite(close[:, :-1])
        & (close[:, 1:] > 0)
        & (close[:, :-1] > 0)
        & adjacent_calendar_slots[None, :]
    )
    adjacent_returns = np.full_like(close[:, 1:], np.nan, dtype=float)
    adjacent_returns[adjacent_valid] = np.log(close[:, 1:][adjacent_valid] / close[:, :-1][adjacent_valid])
    realized_volatility = np.sqrt(np.nansum(np.square(adjacent_returns), axis=1))
    adjacent_count = np.isfinite(adjacent_returns).sum(axis=1)
    realized_volatility[adjacent_count == 0] = np.nan
    first_close = _first_finite(close)
    last_close = _last_finite(close)
    path_abs = np.nansum(np.abs(adjacent_returns), axis=1)
    directional_efficiency = np.full(n_instruments, np.nan, dtype=float)
    direction_valid = np.isfinite(first_close) & np.isfinite(last_close) & (first_close > 0) & (last_close > 0)
    positive_path = direction_valid & (path_abs > 0)
    directional_efficiency[positive_path] = (
        np.log(last_close[positive_path] / first_close[positive_path]) / path_abs[positive_path]
    )
    flat_path = direction_valid & (path_abs == 0)
    directional_efficiency[flat_path] = 0.0
    amount = sliced["amount"]
    volume = sliced["volume"]
    amount_total = np.nansum(amount, axis=1)
    volume_total = np.nansum(volume, axis=1)
    vwap = np.divide(amount_total, volume_total, out=np.full(n_instruments, np.nan), where=volume_total > 0)
    close_to_vwap = np.divide(last_close, vwap, out=np.full(n_instruments, np.nan), where=vwap > 0) - 1.0
    opening_amount = np.nansum(amount[:, opening_mask], axis=1)
    closing_amount = np.nansum(amount[:, closing_mask], axis=1)
    opening_share = np.divide(opening_amount, amount_total, out=np.full(n_instruments, np.nan), where=amount_total > 0)
    closing_share = np.divide(closing_amount, amount_total, out=np.full(n_instruments, np.nan), where=amount_total > 0)
    limit_up_mean = _nanmean_rows(sliced["limit_up"])
    limit_down_mean = _nanmean_rows(sliced["limit_down"])
    limit_pressure = limit_up_mean - limit_down_mean
    raw_features = {
        "opening_30m_return_bps": opening_return * 10_000.0,
        "closing_30m_return_bps": closing_return * 10_000.0,
        "realized_volatility_bps": realized_volatility * 10_000.0,
        "directional_efficiency": directional_efficiency,
        "close_to_vwap_bps": close_to_vwap * 10_000.0,
        "opening_30m_amount_share": opening_share,
        "closing_30m_amount_share": closing_share,
        "limit_pressure": limit_pressure,
    }
    for values in raw_features.values():
        values[~eligible] = np.nan
    result = pd.DataFrame(
        {
            "decision_as_of_trade_date": decision_date,
            "instrument": list(instruments),
            "minute_available": available.astype("int8"),
            "minute_coverage_fraction": coverage.astype("float32"),
            **{name: values.astype("float32") for name, values in raw_features.items()},
        }
    )
    complete_count = int((valid_count == effective_count).sum()) if effective_count else 0
    partial_count = int(((valid_count > 0) & (valid_count < effective_count)).sum()) if effective_count else 0
    whole_day_missing_count = int((valid_count == 0).sum())
    normalized_complete_count = n_instruments if session_wide_single_bar_deficit else complete_count
    normalized_partial_count = 0 if session_wide_single_bar_deficit else partial_count
    coverage_row = {
        "decision_as_of_trade_date": decision_date,
        "instrument_count": n_instruments,
        "raw_calendar_slot_count": n_slots,
        "effective_calendar_slot_count": effective_count,
        "market_wide_empty_slot_count": int(market_empty.sum()),
        "market_wide_empty_slots": list(market_empty_slots),
        "session_wide_single_bar_deficit": session_wide_single_bar_deficit,
        "complete_instrument_count": complete_count,
        "partial_instrument_count": partial_count,
        "whole_day_missing_instrument_count": whole_day_missing_count,
        "normalized_complete_instrument_count": normalized_complete_count,
        "normalized_partial_instrument_count": normalized_partial_count,
        "available_fraction": float(available.mean()) if len(available) else 0.0,
        "mean_coverage_fraction": float(coverage.mean()) if len(coverage) else 0.0,
    }
    return result, coverage_row


def run_minute_crossfit(
    *,
    features: pd.DataFrame,
    paths: Sequence[Mapping[str, Any]],
    request: FrozenMinuteInformationSetRequestV1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fit two frozen Ridge trials and average exactly seven OOF predictions per row."""

    try:
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import Ridge
        from sklearn.preprocessing import StandardScaler
    except ImportError as exc:  # pragma: no cover - formal environment contract
        _raise(
            "minute MVE sklearn dependency is unavailable",
            "ADVISORY_N3_MINUTE_MVE_OOF_INVALID",
            error_type=type(exc).__name__,
        )
    frame = features.copy().reset_index(drop=True)
    labels = pd.to_numeric(frame["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    evaluable = frame["outcome_known"].fillna(False).astype(bool).to_numpy() & np.isfinite(labels)
    if len(paths) != request.expected_ready_path_count:
        _raise("CPCV path count drift", "ADVISORY_N3_MINUTE_MVE_CPCV_INVALID", actual=len(paths))
    dates = pd.DatetimeIndex(frame["decision_as_of_trade_date"]).normalize()
    source_dates = set(dates.unique())
    score_output = frame.loc[
        :,
        [
            "decision_as_of_trade_date",
            "instrument",
            "parent_rank_pct",
            "economic_net_excess_bps",
            "outcome_known",
        ],
    ].copy()
    diagnostics: list[dict[str, Any]] = []
    for trial in request.model_trials:
        sums = np.zeros(len(frame), dtype=np.float64)
        counts = np.zeros(len(frame), dtype=np.uint8)
        columns = list(trial.feature_columns)
        for path in paths:
            if path.get("status") != "READY":
                _raise("CPCV path is not READY", "ADVISORY_N3_MINUTE_MVE_CPCV_INVALID")
            train_dates = pd.DatetimeIndex(pd.to_datetime(path.get("train_dates", ()))).normalize()
            validation_dates = pd.DatetimeIndex(pd.to_datetime(path.get("validation_dates", ()))).normalize()
            if not len(train_dates) or not len(validation_dates) or set(train_dates) & set(validation_dates):
                _raise("CPCV date identity invalid", "ADVISORY_N3_MINUTE_MVE_CPCV_INVALID")
            if not set(train_dates).issubset(source_dates) or not set(validation_dates).issubset(source_dates):
                _raise("CPCV date outside source", "ADVISORY_N3_MINUTE_MVE_CPCV_INVALID")
            train_index = np.flatnonzero(dates.isin(train_dates) & evaluable)
            validation_index = np.flatnonzero(dates.isin(validation_dates))
            x_train_raw = frame.loc[train_index, columns].to_numpy(dtype=float)
            x_validation_raw = frame.loc[validation_index, columns].to_numpy(dtype=float)
            if not len(train_index) or not len(validation_index) or not np.isfinite(x_train_raw).any(axis=0).all():
                _raise(
                    "CPCV fold has no rows or an all-missing feature",
                    "ADVISORY_N3_MINUTE_MVE_OOF_INVALID",
                    path_id=path.get("path_id"),
                    trial_id=trial.trial_id,
                )
            imputer = SimpleImputer(strategy="median")
            x_train_imputed = imputer.fit_transform(x_train_raw)
            x_validation_imputed = imputer.transform(x_validation_raw)
            scaler = StandardScaler()
            x_train = scaler.fit_transform(x_train_imputed)
            x_validation = scaler.transform(x_validation_imputed)
            model = Ridge(alpha=trial.alpha, solver=trial.solver, fit_intercept=trial.fit_intercept)
            model.fit(x_train, labels[train_index])
            predicted = np.asarray(model.predict(x_validation), dtype=float)
            if not np.isfinite(predicted).all():
                _raise("Ridge produced non-finite OOF", "ADVISORY_N3_MINUTE_MVE_OOF_INVALID")
            sums[validation_index] += predicted
            counts[validation_index] += 1
            diagnostics.append(
                {
                    "trial_id": trial.trial_id,
                    "path_id": str(path.get("path_id")),
                    "train_row_count": int(len(train_index)),
                    "validation_row_count": int(len(validation_index)),
                    "imputer_statistics_json": json.dumps(
                        [float(value) for value in np.asarray(imputer.statistics_)], separators=(",", ":")
                    ),
                    "coefficient_json": json.dumps(
                        [float(value) for value in np.asarray(model.coef_).reshape(-1)], separators=(",", ":")
                    ),
                    "intercept": float(model.intercept_),
                }
            )
        if not np.equal(counts, request.expected_oof_predictions_per_row).all():
            unique, frequencies = np.unique(counts, return_counts=True)
            _raise(
                "OOF prediction multiplicity drift",
                "ADVISORY_N3_MINUTE_MVE_OOF_INVALID",
                counts={str(int(key)): int(value) for key, value in zip(unique, frequencies, strict=True)},
            )
        column = MODEL_SCORE_COLUMNS[trial.trial_id]
        score_output[column] = (sums / counts).astype("float32")
        score_output[f"{column}_count"] = counts
    return score_output, pd.DataFrame(diagnostics).sort_values(["trial_id", "path_id"]).reset_index(drop=True)


def evaluate_minute_models(
    *,
    oof_scores: pd.DataFrame,
    regime_daily: pd.DataFrame,
    minute_coverage_daily: pd.DataFrame,
    request: FrozenMinuteInformationSetRequestV1,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    """Evaluate the minute candidate against current parent and parent-only Ridge."""

    required = {
        "decision_as_of_trade_date",
        "instrument",
        "parent_rank_pct",
        "economic_net_excess_bps",
        "outcome_known",
        "comparator_oof_score",
        "candidate_oof_score",
    }
    if not required.issubset(oof_scores.columns):
        _raise("OOF schema drift", "ADVISORY_N3_MINUTE_MVE_OOF_INVALID")
    scores = oof_scores.copy()
    scores["decision_as_of_trade_date"] = pd.to_datetime(scores["decision_as_of_trade_date"]).dt.normalize()
    if scores.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise("OOF keys duplicated", "ADVISORY_N3_MINUTE_MVE_OOF_INVALID")
    if not np.isfinite(scores[["parent_rank_pct", "comparator_oof_score", "candidate_oof_score"]]).all().all():
        _raise("OOF score non-finite", "ADVISORY_N3_MINUTE_MVE_OOF_INVALID")
    regimes = regime_daily.loc[:, ["decision_as_of_trade_date", "regime"]].copy()
    regimes["decision_as_of_trade_date"] = pd.to_datetime(regimes["decision_as_of_trade_date"]).dt.normalize()
    if regimes.duplicated("decision_as_of_trade_date").any():
        _raise("regime dates duplicated", "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH")
    regime_map = regimes.set_index("decision_as_of_trade_date")["regime"].astype(str).to_dict()
    rows: list[dict[str, Any]] = []
    previous: dict[str, set[str] | None] = {"parent": None, "comparator": None, "candidate": None}
    for decision_date, day in scores.groupby("decision_as_of_trade_date", sort=True):
        ids = {
            "parent": _top_ids(day, "parent_rank_pct"),
            "comparator": _top_ids(day, "comparator_oof_score"),
            "candidate": _top_ids(day, "candidate_oof_score"),
        }
        finite_label = day["outcome_known"].fillna(False).astype(bool) & np.isfinite(
            pd.to_numeric(day["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
        )
        labeled = day.loc[finite_label]
        row: dict[str, Any] = {
            "decision_as_of_trade_date": decision_date,
            "regime": regime_map.get(decision_date),
            "row_count": int(len(day)),
            "finite_label_row_count": int(finite_label.sum()),
            "parent_rank_ic": _safe_correlation(
                labeled["parent_rank_pct"], labeled["economic_net_excess_bps"], method="spearman"
            ),
            "comparator_rank_ic": _safe_correlation(
                labeled["comparator_oof_score"], labeled["economic_net_excess_bps"], method="spearman"
            ),
            "candidate_rank_ic": _safe_correlation(
                labeled["candidate_oof_score"], labeled["economic_net_excess_bps"], method="spearman"
            ),
            "candidate_parent_replacement_count": int(5 - len(ids["candidate"] & ids["parent"])),
            "candidate_comparator_replacement_count": int(5 - len(ids["candidate"] & ids["comparator"])),
            "candidate_parent_intervened": ids["candidate"] != ids["parent"],
            "candidate_comparator_intervened": ids["candidate"] != ids["comparator"],
        }
        for name in ("parent", "comparator", "candidate"):
            row[f"{name}_top5_evaluable"] = _top5_outcome_evaluable(day, ids[name])
            row[f"{name}_top5_net_excess_bps"] = _top5_net_value(day, ids[name])
            row[f"{name}_instruments"] = ",".join(sorted(ids[name]))
            prior = previous[name]
            row[f"{name}_top5_churn"] = None if prior is None else float(1.0 - len(prior & ids[name]) / 5.0)
            previous[name] = ids[name]
        rows.append(row)
    daily = pd.DataFrame(rows)
    daily["candidate_rank_ic_delta_parent"] = daily["candidate_rank_ic"] - daily["parent_rank_ic"]
    daily["candidate_top5_lift_parent_bps"] = (
        daily["candidate_top5_net_excess_bps"] - daily["parent_top5_net_excess_bps"]
    )
    daily["candidate_rank_ic_delta_comparator"] = daily["candidate_rank_ic"] - daily["comparator_rank_ic"]
    daily["candidate_top5_lift_comparator_bps"] = (
        daily["candidate_top5_net_excess_bps"] - daily["comparator_top5_net_excess_bps"]
    )
    paired_columns = (
        "candidate_rank_ic_delta_parent",
        "candidate_top5_lift_parent_bps",
        "candidate_rank_ic_delta_comparator",
        "candidate_top5_lift_comparator_bps",
    )
    daily["evaluable"] = np.isfinite(daily.loc[:, paired_columns].to_numpy(dtype=float)).all(axis=1)
    coverage = minute_coverage_daily.copy()
    coverage["decision_as_of_trade_date"] = pd.to_datetime(coverage["decision_as_of_trade_date"]).dt.normalize()
    daily = daily.merge(coverage, on="decision_as_of_trade_date", how="left", validate="one_to_one")
    if daily["instrument_count"].isna().any():
        _raise("minute daily coverage join drift", "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH")
    alpha = 0.05 / request.familywise_hypothesis_count
    inference = {
        "candidate_parent_rank_ic_delta": _metric_inference(
            daily["candidate_rank_ic_delta_parent"], request=request, alpha=alpha, threshold=0.0, seed_offset=0
        ),
        "candidate_parent_top5_lift_bps": _metric_inference(
            daily["candidate_top5_lift_parent_bps"],
            request=request,
            alpha=alpha,
            threshold=request.minimum_parent_lift_bps,
            seed_offset=1,
        ),
        "candidate_comparator_rank_ic_delta": _metric_inference(
            daily["candidate_rank_ic_delta_comparator"],
            request=request,
            alpha=alpha,
            threshold=0.0,
            seed_offset=2,
        ),
        "candidate_comparator_top5_lift_bps": _metric_inference(
            daily["candidate_top5_lift_comparator_bps"],
            request=request,
            alpha=alpha,
            threshold=0.0,
            seed_offset=3,
        ),
    }
    support_parent = _intervention_support(daily, "candidate_parent_intervened", request)
    support_comparator = _intervention_support(daily, "candidate_comparator_intervened", request)
    reasons = [*support_parent["reason_codes"], *support_comparator["reason_codes"]]
    required_inference = (
        ("candidate_parent_rank_ic_delta", 0.0),
        ("candidate_parent_top5_lift_bps", request.minimum_parent_lift_bps),
        ("candidate_comparator_rank_ic_delta", 0.0),
        ("candidate_comparator_top5_lift_bps", 0.0),
    )
    for name, threshold in required_inference:
        lower = inference[name]["familywise_confidence_lower"]
        if lower is None or float(lower) <= threshold:
            reasons.append(f"{name.upper()}_LOWER_NOT_ABOVE_THRESHOLD")
    reasons = sorted(set(reasons))
    eligible = not reasons
    selected = "N3_MINUTE_INFORMATION_EXPANDED_V1" if eligible else None
    summary = {
        "schema_version": "advisory_n3_minute_information_set_model_summary_v1",
        "request_sha256": request.request_sha256,
        "trial_count": 2,
        "familywise_hypothesis_count": request.familywise_hypothesis_count,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "support": {"parent": support_parent, "comparator": support_comparator},
        "inference": inference,
        "parent_rank_ic_mean": _mean(daily["parent_rank_ic"]),
        "comparator_rank_ic_mean": _mean(daily["comparator_rank_ic"]),
        "candidate_rank_ic_mean": _mean(daily["candidate_rank_ic"]),
        "parent_top5_mean_net_excess_bps": _mean(daily["parent_top5_net_excess_bps"]),
        "comparator_top5_mean_net_excess_bps": _mean(daily["comparator_top5_net_excess_bps"]),
        "candidate_top5_mean_net_excess_bps": _mean(daily["candidate_top5_net_excess_bps"]),
        "candidate_parent_lift_dsr": _deflated_sharpe_diagnostic(
            daily["candidate_top5_lift_parent_bps"].tolist(), trial_count=2
        ),
        "candidate_parent_score_spearman_mean": _mean_by_day_score_correlation(
            scores, "candidate_oof_score", "parent_rank_pct"
        ),
        "eligible": eligible,
        "reason_codes": reasons,
        "selected_trial_id": selected,
    }
    frontier = {
        "schema_version": "advisory_n3_minute_information_set_frontier_v1",
        "request_sha256": request.request_sha256,
        "eligible_trial_ids": (["N3_MINUTE_INFORMATION_EXPANDED_V1"] if eligible else []),
        "selected_trial_id": selected,
        "selected_trial_count": 1 if selected else 0,
        "candidate_reselection_allowed": False,
        "exact_retry_allowed": True,
        "selection_rule": "ALL_FOUR_FAMILYWISE_LOWERS_AND_DUAL_BASELINE_SUPPORT__SELECT_ONCE",
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    frontier["frontier_sha256"] = canonical_json_sha256(frontier)
    return daily, summary, frontier


def run_minute_information_set_mve(request_path: str | Path) -> dict[str, Any]:
    started = time.monotonic()
    path = Path(request_path)
    try:
        request_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        _raise(
            "minute information-set request cannot be read",
            "ADVISORY_N3_MINUTE_MVE_REQUEST_INVALID",
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )
    try:
        request = FrozenMinuteInformationSetRequestV1.model_validate_json(request_text)
    except Exception as exc:
        _raise(
            "minute information-set request contract is invalid",
            "ADVISORY_N3_MINUTE_MVE_REQUEST_INVALID",
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )
    existing = _find_existing_bundle(request)
    _verify_environment(request)
    if existing is not None:
        delivery = _deliver_bundle(request=request, bundle_path=existing)
        return _run_response(request, existing, delivery, exact_retry=True)
    sources = _load_verified_sources(request)
    _check_resource_limits(request, "sources_loaded")
    loader = _QlibMinuteLoader(request.minute_provider_uri)
    features, coverage_daily = build_minute_feature_panel(
        source=sources["n2a_source"],
        calendar=sources["calendar"],
        loader=loader,
        request=request,
    )
    _validate_parent_daily_parity_from_source(features=features, parent_daily=sources["parent_daily"])
    _check_resource_limits(request, "minute_features_built")
    oof, fold_diagnostics = run_minute_crossfit(
        features=features,
        paths=sources["cpcv"]["paths"],
        request=request,
    )
    _check_resource_limits(request, "crossfit_complete")
    daily, summary, frontier = evaluate_minute_models(
        oof_scores=oof,
        regime_daily=sources["regime_daily"],
        minute_coverage_daily=coverage_daily,
        request=request,
    )
    _validate_parent_daily_parity(daily=daily, parent_daily=sources["parent_daily"])
    _check_resource_limits(request, "evaluation_complete")
    bundle = _publish_bundle(
        request=request,
        inventory=sources["inventory"],
        coverage_daily=coverage_daily,
        features=features,
        oof_scores=oof,
        fold_diagnostics=fold_diagnostics,
        daily_metrics=daily,
        model_summary=summary,
        frontier=frontier,
        elapsed_seconds=time.monotonic() - started,
    )
    delivery = _deliver_bundle(request=request, bundle_path=bundle)
    return _run_response(request, bundle, delivery, exact_retry=False)


def inspect_minute_information_set_bundle(bundle_path: str | Path) -> dict[str, Any]:
    loaded = _read_minute_bundle(Path(bundle_path).resolve())
    receipt = loaded["receipt"]
    frontier = _read_json(Path(bundle_path) / "frontier_receipt.json", "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID")
    return {
        "status": "VALID",
        "bundle_id": loaded["manifest"]["bundle_id"],
        "request_id": loaded["request"].request_id,
        "receipt_id": receipt.receipt_id,
        "selected_trial_id": receipt.selected_trial_id,
        "eligible_trial_ids": list(receipt.eligible_trial_ids),
        "next_task": receipt.next_task,
        "frontier_sha256": frontier["frontier_sha256"],
        "planned_trial_count": 2,
        "generated_trial_count": 2,
        "evaluated_trial_count": 2,
        "selected_trial_count": receipt.selected_trial_count,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "final_model_written": False,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "position_weight_output": False,
    }


def _validate_bound_sources(
    *,
    leg: Mapping[str, Any],
    n2a: Mapping[str, Any],
    n1: Mapping[str, Any],
    spike: Mapping[str, Any],
) -> None:
    receipt = leg["receipt"]
    invalid = (
        leg["record"].experiment_id != "ADVISORY-N3-LEG-DISAGREEMENT-LEARNABILITY-V1"
        or receipt.selected_trial_count != 0
        or receipt.selected_trial_id is not None
        or receipt.next_task != "N3_MINUTE_INFORMATION_SET_MVE"
        or receipt.decision_use != DecisionUse.NAVIGATION_ONLY
        or receipt.sealed_holdout_accessed is not False
        or receipt.deployable is not False
        or n2a["record"].experiment_id != "ADVISORY-N2A-THREE-ARM-ALPHA-AUDIT"
        or n2a["record"].evaluated_trial_count != 0
        or n2a["record"].decision_use != DecisionUse.NAVIGATION_ONLY
        or n2a["record"].policy_identity != leg["record"].policy_identity
        or n1["request"].decision_date_start.isoformat() != "2024-07-04"
        or n1["request"].decision_date_end.isoformat() != "2026-02-02"
        or n1["learnability"].sealed_holdout_accessed is not False
        or spike.get("source_ready") is not True
        or spike.get("model_training_performed") is not False
        or spike.get("target_or_label_columns_read") is not False
        or spike.get("sealed_holdout_accessed") is not False
        or spike.get("database_accessed") is not False
        or spike.get("network_accessed") is not False
        or spike.get("runtime_mutated") is not False
        or spike.get("provider_uri") != "/home/lc999/data/qlib_minute_bin"
        or spike.get("snapshot_id") != "qlib_minute_authoritative_full_candidate_20240102_20260630"
        or spike.get("qlib_version") != "0.9.6.99"
        or spike.get("n2a_key_scope", {}).get("row_count") != 1_710_301
        or spike.get("n2a_key_scope", {}).get("manifest_interval_covered_rows") != 1_710_301
        or tuple(str(value)[:10] for value in spike.get("normalized_coverage", {}).get("market_wide_empty_slots", ()))
        != MINUTE_MVE_SESSION_WIDE_SINGLE_BAR_DEFICIT_DATES
        or spike.get("normalized_coverage", {}).get("complete_keys") != 1_708_614
        or spike.get("normalized_coverage", {}).get("partial_keys") != 12
        or spike.get("normalized_coverage", {}).get("whole_day_missing_keys") != 1_675
    )
    if invalid:
        _raise("bound source relation is invalid", "ADVISORY_N3_MINUTE_MVE_REQUEST_INVALID")


def _validate_source_control_refs(
    *,
    spike: Mapping[str, Any],
    evidence_refs: Sequence[Any],
) -> None:
    refs = {item.role: item for item in evidence_refs}
    source_hashes = spike.get("source_hashes")
    expected = {
        "n3_minute_source_meta": "meta_export_sha256",
        "n3_minute_source_calendar": "calendar_sha256",
        "n3_minute_source_instruments": "instrument_manifest_sha256",
    }
    if not isinstance(source_hashes, dict) or any(
        role not in refs or refs[role].sha256 != source_hashes.get(source_key) for role, source_key in expected.items()
    ):
        _raise(
            "minute source control files differ from the source-readiness receipt",
            "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
        )


def _load_verified_sources(request: FrozenMinuteInformationSetRequestV1) -> dict[str, Any]:
    leg_path = Path(request.leg_bundle_path).resolve()
    n2a_path = Path(request.n2a_bundle_path).resolve()
    n1_path = Path(request.n1_bundle_path).resolve()
    spike_path = Path(request.source_spike_receipt_path).resolve()
    provider_path = Path(request.minute_provider_uri).resolve()
    leg = _read_leg_bundle(leg_path)
    n2a = _read_n2a_bundle(n2a_path)
    n1 = _read_n1_bundle(n1_path)
    spike = _read_source_spike_receipt(spike_path)
    _validate_bound_sources(leg=leg, n2a=n2a, n1=n1, spike=spike)
    _validate_request_source_identities(request=request, leg=leg, n2a=n2a, n1=n1)
    source = _read_parquet(
        n2a_path / "full_universe_signal_outcomes.parquet",
        columns=SOURCE_REQUIRED_COLUMNS,
        reason_code="ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
    )
    dates = pd.to_datetime(source["decision_as_of_trade_date"]).dt.normalize()
    known = source["outcome_known"].fillna(False).astype(bool)
    finite_label = np.isfinite(pd.to_numeric(source["economic_net_excess_bps"], errors="coerce"))
    evaluable = known & finite_label
    if (
        len(source) != request.expected_source_row_count
        or int(known.sum()) != request.expected_known_row_count
        or int(evaluable.sum()) != request.expected_evaluable_row_count
        or int((known & ~finite_label).sum()) != request.expected_nonfinite_known_row_count
        or int((~known).sum()) != request.expected_unknown_row_count
        or dates.nunique() != request.expected_decision_date_count
        or dates.min().date() != request.signal_start
        or dates.max().date() != request.signal_end
    ):
        _raise("N2-A source coverage drift", "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH")
    inventory, source_content_sha256 = fingerprint_minute_source(
        provider_path=provider_path,
        instruments=tuple(sorted(source["instrument"].astype(str).unique())),
    )
    if (
        source_content_sha256 != request.minute_source_content_sha256
        or len(inventory) != request.minute_source_file_count
    ):
        _raise("minute Bin content identity drift", "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH")
    calendar = pd.DatetimeIndex(pd.to_datetime(pd.read_csv(provider_path / "calendars" / "1min.txt", header=None)[0]))
    cpcv = _read_json(n1_path / "n1_label_interval_cpcv.json", "ADVISORY_N3_MINUTE_MVE_CPCV_INVALID")
    if (
        cpcv.get("request_sha256") != request.n1_request_sha256
        or len(cpcv.get("paths", ())) != request.expected_ready_path_count
        or any(item.get("status") != "READY" for item in cpcv.get("paths", ()))
    ):
        _raise("N1 CPCV source drift", "ADVISORY_N3_MINUTE_MVE_CPCV_INVALID")
    regime_daily = _read_parquet(
        n1_path / "learnability_daily.parquet",
        columns=("decision_as_of_trade_date", "regime"),
        reason_code="ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
    )
    parent_daily_raw = _read_parquet(
        leg_path / "daily_metrics.parquet",
        columns=(
            "decision_as_of_trade_date",
            "parent_rank_ic",
            "parent_top5_net_excess_bps",
            "parent_top5_evaluable",
            "parent_top5_churn",
        ),
        reason_code="ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
    )
    parent_daily = _normalize_parent_daily(parent_daily_raw)
    return {
        "n2a_source": source,
        "inventory": inventory,
        "calendar": calendar,
        "cpcv": cpcv,
        "regime_daily": regime_daily,
        "parent_daily": parent_daily,
    }


def _normalize_parent_daily(parent_daily_raw: pd.DataFrame) -> pd.DataFrame:
    frame = parent_daily_raw.copy()
    frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    if frame.duplicated("decision_as_of_trade_date").any():
        _raise(
            "leg parent daily source contains duplicate dates",
            "ADVISORY_N3_MINUTE_MVE_BASELINE_PARITY_FAILED",
        )
    return frame.sort_values("decision_as_of_trade_date").reset_index(drop=True)


def _validate_request_source_identities(
    *,
    request: FrozenMinuteInformationSetRequestV1,
    leg: Mapping[str, Any],
    n2a: Mapping[str, Any],
    n1: Mapping[str, Any],
) -> None:
    if (
        Path(request.leg_bundle_path).resolve().name != request.leg_bundle_id
        or leg["request"].request_sha256 != request.leg_request_sha256
        or leg["receipt"].receipt_sha256 != request.leg_receipt_sha256
        or Path(request.n2a_bundle_path).resolve().name != request.n2a_bundle_id
        or n2a["request"].request_sha256 != request.n2a_request_sha256
        or n2a["receipt"].receipt_sha256 != request.n2a_receipt_sha256
        or Path(request.n1_bundle_path).resolve().name != request.n1_bundle_id
        or n1["request"].request_sha256 != request.n1_request_sha256
        or n1["request"].split_policy_sha256 != request.n1_split_policy_sha256
        or n2a["record"].dataset_identity != request.source_dataset_identity
        or leg["record"].dataset_identity != request.route_dataset_identity
        or leg["record"].policy_identity != request.policy_identity
        or sha256_file(Path(request.source_spike_receipt_path)) != request.source_spike_receipt_sha256
    ):
        _raise("request/source identity drift", "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH")


def _validate_parent_daily_parity_from_source(*, features: pd.DataFrame, parent_daily: pd.DataFrame) -> None:
    rows: list[dict[str, Any]] = []
    for decision_date, day in features.groupby("decision_as_of_trade_date", sort=True):
        ids = _top_ids(day, "parent_rank_pct")
        finite_label = day["outcome_known"].fillna(False).astype(bool) & np.isfinite(
            pd.to_numeric(day["economic_net_excess_bps"], errors="coerce")
        )
        labeled = day.loc[finite_label]
        rows.append(
            {
                "decision_as_of_trade_date": decision_date,
                "parent_rank_ic": _safe_correlation(
                    labeled["parent_rank_pct"], labeled["economic_net_excess_bps"], method="spearman"
                ),
                "parent_top5_net_excess_bps": _top5_net_value(day, ids),
                "parent_top5_evaluable": _top5_outcome_evaluable(day, ids),
            }
        )
    generated = pd.DataFrame(rows)
    _validate_parent_daily_parity(daily=generated, parent_daily=parent_daily, compare_churn=False)


def _validate_parent_daily_parity(
    *, daily: pd.DataFrame, parent_daily: pd.DataFrame, compare_churn: bool = True
) -> None:
    columns = [
        "decision_as_of_trade_date",
        "parent_rank_ic",
        "parent_top5_net_excess_bps",
        "parent_top5_evaluable",
    ]
    if compare_churn:
        columns.append("parent_top5_churn")
    merged = daily.loc[:, columns].merge(
        parent_daily.loc[:, columns],
        on="decision_as_of_trade_date",
        how="outer",
        validate="one_to_one",
        suffixes=("_new", "_frozen"),
        indicator=True,
    )
    if not merged["_merge"].eq("both").all():
        _raise("parent parity date drift", "ADVISORY_N3_MINUTE_MVE_BASELINE_PARITY_FAILED")
    numeric = ["parent_rank_ic"] + (["parent_top5_churn"] if compare_churn else [])
    for column in numeric:
        if not np.allclose(merged[f"{column}_new"], merged[f"{column}_frozen"], rtol=0.0, atol=1e-12, equal_nan=True):
            _raise("parent daily metric drift", "ADVISORY_N3_MINUTE_MVE_BASELINE_PARITY_FAILED", metric=column)
    new_evaluable = merged["parent_top5_evaluable_new"].fillna(False).astype(bool)
    frozen_evaluable = merged["parent_top5_evaluable_frozen"].fillna(False).astype(bool)
    if not new_evaluable.equals(frozen_evaluable):
        _raise("parent evaluability drift", "ADVISORY_N3_MINUTE_MVE_BASELINE_PARITY_FAILED")
    new_value = merged["parent_top5_net_excess_bps_new"].to_numpy(dtype=float)
    frozen_value = merged["parent_top5_net_excess_bps_frozen"].to_numpy(dtype=float)
    mask = new_evaluable.to_numpy()
    if np.isfinite(new_value[~mask]).any() or not np.allclose(
        new_value[mask], frozen_value[mask], rtol=0.0, atol=1e-12, equal_nan=False
    ):
        _raise("parent Top5 value drift", "ADVISORY_N3_MINUTE_MVE_BASELINE_PARITY_FAILED")


def _intervention_support(
    daily: pd.DataFrame, column: str, request: FrozenMinuteInformationSetRequestV1
) -> dict[str, Any]:
    evaluable = daily["evaluable"].astype(bool)
    intervention = daily[column].astype(bool) & evaluable
    mapped = daily["regime"].notna() & evaluable
    by_regime = daily.loc[intervention & mapped].groupby("regime", sort=True).size().astype(int).to_dict()
    observed = sorted(daily.loc[mapped, "regime"].astype(str).unique())
    evaluable_count = int(evaluable.sum())
    intervention_count = int(intervention.sum())
    fraction = float(intervention_count / evaluable_count) if evaluable_count else 0.0
    reasons: list[str] = []
    prefix = "PARENT" if column.endswith("parent_intervened") else "COMPARATOR"
    if evaluable_count < request.minimum_evaluable_days:
        reasons.append(f"{prefix}_EVALUABLE_DAY_COUNT_BELOW_MINIMUM")
    if intervention_count < request.minimum_intervention_days:
        reasons.append(f"{prefix}_INTERVENTION_DAY_COUNT_BELOW_MINIMUM")
    if fraction < request.minimum_intervention_fraction:
        reasons.append(f"{prefix}_INTERVENTION_FRACTION_BELOW_MINIMUM")
    if any(by_regime.get(regime, 0) < request.minimum_intervention_days_per_regime for regime in observed):
        reasons.append(f"{prefix}_INTERVENTION_REGIME_SUPPORT_BELOW_MINIMUM")
    return {
        "evaluable_day_count": evaluable_count,
        "total_decision_day_count": int(len(daily)),
        "intervention_day_count": intervention_count,
        "intervention_fraction": fraction,
        "intervention_days_by_regime": {str(key): int(value) for key, value in by_regime.items()},
        "regime_mapped_day_count": int(mapped.sum()),
        "support_sufficient": not reasons,
        "reason_codes": reasons,
    }


def _first_finite(values: np.ndarray) -> np.ndarray:
    if values.shape[1] == 0:
        return np.full(values.shape[0], np.nan)
    valid = np.isfinite(values)
    indices = valid.argmax(axis=1)
    result = values[np.arange(values.shape[0]), indices].astype(float)
    result[~valid.any(axis=1)] = np.nan
    return result


def _last_finite(values: np.ndarray) -> np.ndarray:
    if values.shape[1] == 0:
        return np.full(values.shape[0], np.nan)
    valid = np.isfinite(values)
    reverse = valid[:, ::-1].argmax(axis=1)
    indices = values.shape[1] - 1 - reverse
    result = values[np.arange(values.shape[0]), indices].astype(float)
    result[~valid.any(axis=1)] = np.nan
    return result


def _window_return(open_values: np.ndarray, close_values: np.ndarray) -> np.ndarray:
    first = _first_finite(open_values)
    last = _last_finite(close_values)
    return np.divide(last, first, out=np.full(len(first), np.nan), where=(first > 0) & np.isfinite(last)) - 1.0


def _nanmean_rows(values: np.ndarray) -> np.ndarray:
    finite = np.isfinite(values)
    counts = finite.sum(axis=1)
    totals = np.nansum(values, axis=1)
    return np.divide(totals, counts, out=np.full(values.shape[0], np.nan), where=counts > 0)


def _publish_bundle(
    *,
    request: FrozenMinuteInformationSetRequestV1,
    inventory: pd.DataFrame,
    coverage_daily: pd.DataFrame,
    features: pd.DataFrame,
    oof_scores: pd.DataFrame,
    fold_diagnostics: pd.DataFrame,
    daily_metrics: pd.DataFrame,
    model_summary: Mapping[str, Any],
    frontier: Mapping[str, Any],
    elapsed_seconds: float,
) -> Path:
    root = Path(request.output_root) / "minute_information_set_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{request.request_id}.", dir=root))
    _write_json(temporary / "request.json", request.model_dump(mode="json"))
    _write_json(
        temporary / "feature_schema.json",
        {
            "schema_version": MINUTE_MVE_FEATURE_SCHEMA_VERSION,
            "feature_schema_hash": MINUTE_MVE_FEATURE_SCHEMA_HASH,
            "source_fields": list(MINUTE_MVE_SOURCE_FIELDS),
            "raw_economic_features": list(MINUTE_MVE_RAW_ECONOMIC_FEATURES),
            "comparator_features": list(request.model_trials[0].feature_columns),
            "expanded_features": list(request.model_trials[1].feature_columns),
            "decision_clock": "T_DAY_ONLY_THROUGH_15_00_AFTER_CLOSE_RANKING",
            "normal_missing_policy": "KEEP_ALL_KEYS_TRAIN_FOLD_MEDIAN_WITH_AVAILABILITY_COVERAGE",
            "sealed_holdout_accessed": False,
        },
    )
    inventory.to_parquet(temporary / "minute_source_inventory.parquet", index=False)
    coverage_daily.to_parquet(temporary / "minute_coverage_daily.parquet", index=False)
    features.to_parquet(temporary / "minute_feature_panel.parquet", index=False)
    oof_scores.to_parquet(temporary / "oof_score_panel.parquet", index=False)
    fold_diagnostics.to_parquet(temporary / "fold_diagnostics.parquet", index=False)
    daily_metrics.to_parquet(temporary / "daily_metrics.parquet", index=False)
    _write_json(temporary / "model_summary.json", model_summary)
    _write_json(temporary / "frontier_receipt.json", frontier)
    known = features["outcome_known"].fillna(False).astype(bool)
    finite_label = np.isfinite(pd.to_numeric(features["economic_net_excess_bps"], errors="coerce"))
    source_payload = {
        "schema_version": "advisory_n3_minute_information_set_source_identity_v1",
        "request_sha256": request.request_sha256,
        "evidence_refs": [item.model_dump(mode="json") for item in request.evidence_refs],
        "leg_bundle_id": request.leg_bundle_id,
        "n2a_bundle_id": request.n2a_bundle_id,
        "n1_bundle_id": request.n1_bundle_id,
        "source_dataset_identity": request.source_dataset_identity,
        "route_dataset_identity": request.route_dataset_identity,
        "minute_source_content_sha256": request.minute_source_content_sha256,
        "minute_source_file_count": request.minute_source_file_count,
        "dataset_identity": request.dataset_identity,
        "policy_identity": request.policy_identity,
        "source_row_count": int(len(features)),
        "known_row_count": int(known.sum()),
        "evaluable_row_count": int((known & finite_label).sum()),
        "nonfinite_known_row_count": int((known & ~finite_label).sum()),
        "unknown_row_count": int((~known).sum()),
        "decision_date_count": int(features["decision_as_of_trade_date"].nunique()),
        "market_wide_empty_slots": sorted(
            {
                value
                for values in coverage_daily["market_wide_empty_slots"]
                for value in (values if isinstance(values, list) else [])
            }
        ),
        "session_wide_single_bar_deficit_dates": sorted(
            pd.to_datetime(
                coverage_daily.loc[
                    coverage_daily["session_wide_single_bar_deficit"].astype(bool),
                    "decision_as_of_trade_date",
                ]
            )
            .dt.date.astype(str)
            .tolist()
        ),
        "complete_instrument_date_count": int(coverage_daily["complete_instrument_count"].sum()),
        "partial_instrument_date_count": int(coverage_daily["partial_instrument_count"].sum()),
        "whole_day_missing_instrument_date_count": int(coverage_daily["whole_day_missing_instrument_count"].sum()),
        "normalized_complete_instrument_date_count": int(coverage_daily["normalized_complete_instrument_count"].sum()),
        "normalized_partial_instrument_date_count": int(coverage_daily["normalized_partial_instrument_count"].sum()),
        "raw_feature_finite_fraction": {
            name: float(np.isfinite(pd.to_numeric(features[name], errors="coerce")).mean())
            for name in MINUTE_MVE_RAW_ECONOMIC_FEATURES
        },
        "minute_available_fraction": float(pd.to_numeric(features["minute_available"], errors="coerce").mean()),
        "minute_coverage_fraction_mean": float(
            pd.to_numeric(features["minute_coverage_fraction"], errors="coerce").mean()
        ),
        "minute_coverage_fraction_min_by_day": float(
            pd.to_numeric(coverage_daily["available_fraction"], errors="coerce").min()
        ),
        "repository_commit": request.repository_commit,
        "database_read_performed": False,
        "network_read_performed": False,
        "qlib_minute_read_performed": True,
        "qlib_daily_read_performed": False,
        "sealed_holdout_accessed": False,
    }
    _write_json(temporary / "source_identity_receipt.json", source_payload)
    temporary_bytes = sum(path.stat().st_size for path in temporary.iterdir() if path.is_file())
    if temporary_bytes > request.resource_max_temp_bytes:
        _raise(
            "minute MVE temporary output exceeds frozen limit",
            "ADVISORY_N3_MINUTE_MVE_RESOURCE_LIMIT_EXCEEDED",
            temporary_bytes=temporary_bytes,
        )
    resource_payload = {
        "schema_version": "advisory_n3_minute_information_set_resource_report_v1",
        "elapsed_seconds": float(elapsed_seconds),
        "peak_rss_bytes": _peak_rss_bytes(),
        "temporary_bytes": temporary_bytes,
        "resource_max_rss_bytes": request.resource_max_rss_bytes,
        "resource_max_temp_bytes": request.resource_max_temp_bytes,
        "wall_time_limit_seconds": None,
        "wall_time_is_telemetry_only": True,
    }
    _write_json(temporary / "resource_report.json", resource_payload)
    result_descriptors = {
        name: descriptor for name, descriptor in _file_descriptors(temporary).items() if name in RESULT_IDENTITY_MEMBERS
    }
    selected = model_summary.get("selected_trial_id")
    eligible = ("N3_MINUTE_INFORMATION_EXPANDED_V1",) if selected else ()
    receipt = build_minute_information_set_receipt(
        request_sha256=request.request_sha256,
        selected_trial_count=1 if selected else 0,
        selected_trial_id=selected,
        eligible_trial_ids=eligible,
        next_task=("N3_MINUTE_INFORMATION_SET_CONFIRMATION_DESIGN" if selected else "N3_QE_ALPHA_GENERATOR_MVE_DESIGN"),
        source_identity_sha256=sha256_file(temporary / "source_identity_receipt.json"),
        result_files_sha256=canonical_json_sha256(result_descriptors),
        resource_report_sha256=sha256_file(temporary / "resource_report.json"),
    )
    _write_json(temporary / "learnability_receipt.json", receipt.model_dump(mode="json"))
    bundle_id = canonical_json_sha256(
        {
            "schema_version": MINUTE_MVE_BUNDLE_SCHEMA,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
        }
    )
    destination = root / bundle_id
    record = _build_registry_record(
        request=request,
        receipt_path=temporary / "learnability_receipt.json",
        receipt_artifact_uri=(destination / "learnability_receipt.json").as_posix(),
        receipt=receipt,
    )
    _write_json(temporary / "registry_record.json", record.model_dump(mode="json"))
    descriptors = _file_descriptors(temporary)
    if set(descriptors) != BUNDLE_MEMBERS:
        _raise(
            "minute bundle member roster drift",
            "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID",
            members=sorted(descriptors),
        )
    manifest = {
        "schema_version": MINUTE_MVE_BUNDLE_SCHEMA,
        "bundle_id": bundle_id,
        "request_sha256": request.request_sha256,
        "receipt_sha256": receipt.receipt_sha256,
        "leg_bundle_id": request.leg_bundle_id,
        "n2a_bundle_id": request.n2a_bundle_id,
        "n1_bundle_id": request.n1_bundle_id,
        "minute_source_content_sha256": request.minute_source_content_sha256,
        "objective_contract": ObjectiveContract.ALPHA_RANKING.value,
        "study_type": ResearchStudyType.LEARNABILITY_AUDIT.value,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "result_class": ResearchResultClass.EXPLORATORY.value,
        "planned_trial_count": 2,
        "generated_trial_count": 2,
        "evaluated_trial_count": 2,
        "selected_trial_count": receipt.selected_trial_count,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "final_model_written": False,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "position_weight_output": False,
        "files": descriptors,
    }
    _write_json(temporary / "manifest.json", manifest)
    if destination.exists():
        _read_minute_bundle(destination)
        _raise(
            "minute bundle destination appeared concurrently",
            "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID",
            bundle_id=bundle_id,
        )
    temporary.replace(destination)
    _read_minute_bundle(destination)
    return destination


def _build_registry_record(
    *,
    request: FrozenMinuteInformationSetRequestV1,
    receipt_path: Path,
    receipt_artifact_uri: str,
    receipt: MinuteInformationSetReceiptV1,
) -> AdvisoryResearchTrialRecordV1:
    return build_trial_record(
        experiment_id=MINUTE_MVE_EXPERIMENT_ID,
        attempt_id=request.request_id,
        research_stage="N3_MINUTE_INFORMATION_SET_MVE",
        study_type=ResearchStudyType.LEARNABILITY_AUDIT,
        hypothesis_family_id=MINUTE_MVE_HYPOTHESIS_FAMILY_ID,
        parent_lineage=(
            "ADVISORY-N1-TIER1-LEARNABILITY",
            "ADVISORY-N2A-THREE-ARM-ALPHA-AUDIT",
            "ADVISORY-N3-LEG-DISAGREEMENT-LEARNABILITY-V1",
        ),
        unique_variable="FIXED_PARENT_ONLY_VS_FIXED_T_DAY_MINUTE_PATH_INFORMATION",
        objective_contract=ObjectiveContract.ALPHA_RANKING,
        dataset_identity=request.dataset_identity,
        schema_identity=request.feature_schema_hash,
        policy_identity=request.policy_identity,
        planned_trial_count=2,
        generated_trial_count=2,
        evaluated_trial_count=2,
        selected_trial_count=receipt.selected_trial_count,
        consumed_windows=(
            ConsumedWindowV1(
                window_id="P0C_DEVELOPMENT_CONSUMED_20240704_20260202",
                dataset_identity=request.dataset_identity,
                start_date=request.signal_start,
                end_date=request.signal_end,
            ),
        ),
        result_class=ResearchResultClass.EXPLORATORY,
        decision_use=DecisionUse.NAVIGATION_ONLY,
        evidence_refs=(
            evidence_reference_for_file(receipt_path, role="n3_minute_information_set_learnability_receipt").model_copy(
                update={"artifact_uri": receipt_artifact_uri}
            ),
        ),
        recorded_at=datetime.now(timezone.utc),
    )


def _read_minute_bundle(path: Path) -> dict[str, Any]:
    manifest = _read_json(path / "manifest.json", "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID")
    descriptors = manifest.get("files")
    if not isinstance(descriptors, dict) or set(descriptors) != BUNDLE_MEMBERS:
        _raise("minute descriptor roster invalid", "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID")
    for name, descriptor in descriptors.items():
        member = path / name
        if not isinstance(descriptor, dict) or not member.is_file():
            _raise("minute member missing", "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID", member=name)
        actual_rows = _parquet_row_count(member) if member.suffix == ".parquet" else None
        if (
            sha256_file(member) != descriptor.get("sha256")
            or member.stat().st_size != descriptor.get("size_bytes")
            or (actual_rows is not None and actual_rows != descriptor.get("row_count"))
        ):
            _raise("minute member identity drift", "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID", member=name)
    try:
        request = FrozenMinuteInformationSetRequestV1.model_validate_json(
            (path / "request.json").read_text(encoding="utf-8")
        )
        receipt = MinuteInformationSetReceiptV1.model_validate_json(
            (path / "learnability_receipt.json").read_text(encoding="utf-8")
        )
        record = AdvisoryResearchTrialRecordV1.model_validate_json(
            (path / "registry_record.json").read_text(encoding="utf-8")
        )
    except Exception as exc:
        _raise(
            "minute bundle contract invalid",
            "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID",
            error_type=type(exc).__name__,
        )
    expected_bundle_id = canonical_json_sha256(
        {
            "schema_version": MINUTE_MVE_BUNDLE_SCHEMA,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
        }
    )
    result_descriptors = {name: descriptors[name] for name in sorted(RESULT_IDENTITY_MEMBERS)}
    frontier = _read_json(path / "frontier_receipt.json", "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID")
    frontier_functional = {key: value for key, value in frontier.items() if key != "frontier_sha256"}
    resource = _read_json(path / "resource_report.json", "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID")
    summary = _read_json(path / "model_summary.json", "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID")
    feature_schema = _read_json(path / "feature_schema.json", "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID")
    source_identity = _read_json(
        path / "source_identity_receipt.json",
        "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID",
    )
    raw_feature_finite_fraction = source_identity.get("raw_feature_finite_fraction")
    receipt_descriptor = descriptors["learnability_receipt.json"]
    invalid = (
        manifest.get("schema_version") != MINUTE_MVE_BUNDLE_SCHEMA
        or path.name != expected_bundle_id
        or manifest.get("bundle_id") != expected_bundle_id
        or manifest.get("request_sha256") != request.request_sha256
        or manifest.get("receipt_sha256") != receipt.receipt_sha256
        or receipt.request_sha256 != request.request_sha256
        or receipt.source_identity_sha256 != descriptors["source_identity_receipt.json"]["sha256"]
        or receipt.result_files_sha256 != canonical_json_sha256(result_descriptors)
        or receipt.resource_report_sha256 != descriptors["resource_report.json"]["sha256"]
        or record.experiment_id != MINUTE_MVE_EXPERIMENT_ID
        or record.attempt_id != request.request_id
        or record.study_type != ResearchStudyType.LEARNABILITY_AUDIT
        or record.decision_use != DecisionUse.NAVIGATION_ONLY
        or record.result_class != ResearchResultClass.EXPLORATORY
        or record.planned_trial_count != 2
        or record.generated_trial_count != 2
        or record.evaluated_trial_count != 2
        or record.selected_trial_count != receipt.selected_trial_count
        or len(record.evidence_refs) != 1
        or record.evidence_refs[0].role != "n3_minute_information_set_learnability_receipt"
        or record.evidence_refs[0].sha256 != receipt_descriptor["sha256"]
        or record.evidence_refs[0].size_bytes != receipt_descriptor["size_bytes"]
        or feature_schema.get("schema_version") != MINUTE_MVE_FEATURE_SCHEMA_VERSION
        or feature_schema.get("feature_schema_hash") != request.feature_schema_hash
        or tuple(feature_schema.get("source_fields", ())) != MINUTE_MVE_SOURCE_FIELDS
        or tuple(feature_schema.get("raw_economic_features", ())) != MINUTE_MVE_RAW_ECONOMIC_FEATURES
        or tuple(feature_schema.get("comparator_features", ())) != request.model_trials[0].feature_columns
        or tuple(feature_schema.get("expanded_features", ())) != request.model_trials[1].feature_columns
        or feature_schema.get("sealed_holdout_accessed") is not False
        or source_identity.get("request_sha256") != request.request_sha256
        or source_identity.get("leg_bundle_id") != request.leg_bundle_id
        or source_identity.get("n2a_bundle_id") != request.n2a_bundle_id
        or source_identity.get("n1_bundle_id") != request.n1_bundle_id
        or source_identity.get("source_dataset_identity") != request.source_dataset_identity
        or source_identity.get("route_dataset_identity") != request.route_dataset_identity
        or source_identity.get("minute_source_content_sha256") != request.minute_source_content_sha256
        or source_identity.get("minute_source_file_count") != request.minute_source_file_count
        or source_identity.get("dataset_identity") != request.dataset_identity
        or source_identity.get("policy_identity") != request.policy_identity
        or source_identity.get("database_read_performed") is not False
        or source_identity.get("network_read_performed") is not False
        or source_identity.get("qlib_minute_read_performed") is not True
        or source_identity.get("qlib_daily_read_performed") is not False
        or source_identity.get("sealed_holdout_accessed") is not False
        or not isinstance(raw_feature_finite_fraction, dict)
        or set(raw_feature_finite_fraction) != set(MINUTE_MVE_RAW_ECONOMIC_FEATURES)
        or any(
            not isinstance(value, (int, float)) or not math.isfinite(value) or not 0.0 <= value <= 1.0
            for value in raw_feature_finite_fraction.values()
        )
        or frontier.get("frontier_sha256") != canonical_json_sha256(frontier_functional)
        or frontier.get("schema_version") != "advisory_n3_minute_information_set_frontier_v1"
        or frontier.get("request_sha256") != request.request_sha256
        or frontier.get("selected_trial_id") != receipt.selected_trial_id
        or tuple(frontier.get("eligible_trial_ids", ())) != receipt.eligible_trial_ids
        or frontier.get("selected_trial_count") != receipt.selected_trial_count
        or frontier.get("candidate_reselection_allowed") is not False
        or frontier.get("exact_retry_allowed") is not True
        or frontier.get("decision_use") != DecisionUse.NAVIGATION_ONLY.value
        or frontier.get("sealed_holdout_accessed") is not False
        or frontier.get("deployable") is not False
        or summary.get("schema_version") != "advisory_n3_minute_information_set_model_summary_v1"
        or summary.get("request_sha256") != request.request_sha256
        or summary.get("trial_count") != 2
        or summary.get("familywise_hypothesis_count") != request.familywise_hypothesis_count
        or summary.get("decision_use") != DecisionUse.NAVIGATION_ONLY.value
        or summary.get("sealed_holdout_accessed") is not False
        or summary.get("deployable") is not False
        or summary.get("selected_trial_id") != receipt.selected_trial_id
        or summary.get("eligible") is not bool(receipt.selected_trial_count)
        or manifest.get("leg_bundle_id") != request.leg_bundle_id
        or manifest.get("n2a_bundle_id") != request.n2a_bundle_id
        or manifest.get("n1_bundle_id") != request.n1_bundle_id
        or manifest.get("minute_source_content_sha256") != request.minute_source_content_sha256
        or manifest.get("planned_trial_count") != 2
        or manifest.get("generated_trial_count") != 2
        or manifest.get("evaluated_trial_count") != 2
        or manifest.get("selected_trial_count") != receipt.selected_trial_count
        or manifest.get("objective_contract") != ObjectiveContract.ALPHA_RANKING.value
        or manifest.get("study_type") != ResearchStudyType.LEARNABILITY_AUDIT.value
        or manifest.get("decision_use") != DecisionUse.NAVIGATION_ONLY.value
        or manifest.get("result_class") != ResearchResultClass.EXPLORATORY.value
        or any(
            manifest.get(name) is not False
            for name in (
                "sealed_holdout_accessed",
                "deployable",
                "runtime_eligible",
                "final_model_written",
                "factor_catalog_written",
                "strategy_package_written",
                "position_weight_output",
            )
        )
        or resource.get("wall_time_limit_seconds") is not None
        or resource.get("wall_time_is_telemetry_only") is not True
        or not isinstance(resource.get("peak_rss_bytes"), int)
        or int(resource.get("peak_rss_bytes", -1)) > request.resource_max_rss_bytes
        or not isinstance(resource.get("temporary_bytes"), int)
        or int(resource.get("temporary_bytes", -1)) > request.resource_max_temp_bytes
    )
    if invalid:
        _raise("minute bundle relational identity invalid", "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID")
    return {"manifest": manifest, "request": request, "receipt": receipt, "record": record}


def _find_existing_bundle(request: FrozenMinuteInformationSetRequestV1) -> Path | None:
    root = Path(request.output_root) / "minute_information_set_bundles"
    if not root.exists():
        return None
    matches: list[Path] = []
    for path in root.iterdir():
        if not path.is_dir() or path.name.startswith(".") or not (path / "manifest.json").is_file():
            continue
        try:
            manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if manifest.get("request_sha256") == request.request_sha256:
            matches.append(path)
    if len(matches) > 1:
        _raise("request maps to multiple bundles", "ADVISORY_N3_MINUTE_MVE_BUNDLE_INVALID")
    if matches:
        _read_minute_bundle(matches[0])
        return matches[0]
    return None


def _deliver_bundle(*, request: FrozenMinuteInformationSetRequestV1, bundle_path: Path) -> dict[str, Any]:
    loaded = _read_minute_bundle(bundle_path)
    registry = AdvisoryResearchTrialRegistryV1(request.registry_path).append_batch((loaded["record"],))
    route = _write_route_page(
        path=Path(request.route_path),
        request=request,
        receipt=loaded["receipt"],
        bundle_id=loaded["manifest"]["bundle_id"],
        registry_sha256=str(registry["registry_sha256"]),
    )
    return {"registry": registry, "route": route}


def _write_route_page(
    *,
    path: Path,
    request: FrozenMinuteInformationSetRequestV1,
    receipt: MinuteInformationSetReceiptV1,
    bundle_id: str,
    registry_sha256: str,
) -> dict[str, Any]:
    selected = receipt.selected_trial_id or "NONE"
    content = "\n".join(
        (
            "# Advisory 当前研究路线",
            "",
            "- active_main_line: `N3_MINUTE_INFORMATION_SET_MVE`",
            "- active_auxiliary_line: `NONE`",
            f"- next_task: `{receipt.next_task}`",
            f"- exploratory_candidate: `{selected}`",
            f"- leg_bundle_id: `{request.leg_bundle_id}`",
            f"- minute_information_set_bundle_id: `{bundle_id}`",
            f"- trial_registry_sha256: `{registry_sha256}`",
            "- objective_contract: `ALPHA_RANKING`",
            "- study_type: `LEARNABILITY_AUDIT`",
            "- decision_use: `NAVIGATION_ONLY`",
            "- sealed_holdout_accessed: `false`",
            "- deployable/runtime/model/factor/strategy_package/position_weight: `false/false/false/false/false/false`",
            "",
            "该页面只记录开发窗口learnability导航，不构成确认、激活、资金仓位或交易输入。",
            "",
        )
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = content.encode("utf-8")
    if path.exists() and path.read_bytes() == encoded:
        return {
            "status": "EXACT_NOOP",
            "route_path": path.as_posix(),
            "route_sha256": sha256_file(path),
            "next_task": receipt.next_task,
        }
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return {
        "status": "UPDATED",
        "route_path": path.as_posix(),
        "route_sha256": sha256_file(path),
        "next_task": receipt.next_task,
    }


def _verify_environment(request: FrozenMinuteInformationSetRequestV1) -> None:
    _require_formal_environment()
    repo = Path(request.repository_root)
    if _cross_os_git_commit(repo) != request.repository_commit:
        _raise("repository commit drift", "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH")
    dirty = _cross_os_git_dirty_paths(repo)
    if dirty:
        _raise(
            "repository became dirty",
            "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH",
            dirty_paths=dirty[:20],
        )
    for reference in request.evidence_refs:
        _verify_ref(reference)
    leg = _read_leg_bundle(Path(request.leg_bundle_path))
    n2a = _read_n2a_bundle(Path(request.n2a_bundle_path))
    n1 = _read_n1_bundle(Path(request.n1_bundle_path))
    spike = _read_source_spike_receipt(Path(request.source_spike_receipt_path))
    _validate_bound_sources(leg=leg, n2a=n2a, n1=n1, spike=spike)
    _validate_request_source_identities(request=request, leg=leg, n2a=n2a, n1=n1)


def _require_formal_environment() -> None:
    if os.name == "nt" or os.environ.get("CONDA_DEFAULT_ENV") != "rdagent-gpu":
        _raise(
            "minute information-set prepare/run requires WSL rdagent-gpu",
            "ADVISORY_N3_MINUTE_MVE_SOURCE_SCHEMA_INVALID",
            os_name=os.name,
            conda_default_env=os.environ.get("CONDA_DEFAULT_ENV"),
        )


def _run_response(
    request: FrozenMinuteInformationSetRequestV1,
    bundle: Path,
    delivery: Mapping[str, Any],
    *,
    exact_retry: bool,
) -> dict[str, Any]:
    return {
        **inspect_minute_information_set_bundle(bundle),
        "request_id": request.request_id,
        "bundle_path": bundle.as_posix(),
        "exact_retry": exact_retry,
        "registry": dict(delivery["registry"]),
        "route": dict(delivery["route"]),
    }


def _metric_inference(
    values: Sequence[float] | pd.Series,
    *,
    request: FrozenMinuteInformationSetRequestV1,
    alpha: float,
    threshold: float,
    seed_offset: int,
) -> dict[str, Any]:
    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if len(array) < 2:
        return {
            "point_estimate": float(array.mean()) if len(array) else None,
            "confidence_lower": None,
            "confidence_upper": None,
            "familywise_confidence_lower": None,
            "familywise_confidence_upper": None,
            "bootstrap_standard_error": None,
            "mde": None,
            "threshold": float(threshold),
            "observation_count": int(len(array)),
        }
    ordinary = _moving_block_interval(
        array,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=request.bootstrap_seed + seed_offset,
        alpha=0.05,
    )
    familywise = _moving_block_interval(
        array,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=request.bootstrap_seed + seed_offset,
        alpha=alpha,
    )
    rng = np.random.default_rng(request.bootstrap_seed + seed_offset)
    block = min(request.block_length_trading_days, len(array))
    blocks_needed = math.ceil(len(array) / block)
    starts = rng.integers(0, len(array), size=(request.bootstrap_repetitions, blocks_needed))
    offsets = np.arange(block)
    indexes = (starts[:, :, None] + offsets[None, None, :]) % len(array)
    samples = array[indexes.reshape(request.bootstrap_repetitions, -1)[:, : len(array)]]
    standard_error = float(samples.mean(axis=1).std(ddof=1))
    mde = float((1.959963984540054 + 0.8416212335729143) * standard_error)
    return {
        "point_estimate": float(array.mean()),
        "confidence_lower": ordinary[0],
        "confidence_upper": ordinary[1],
        "familywise_confidence_lower": familywise[0],
        "familywise_confidence_upper": familywise[1],
        "bootstrap_standard_error": standard_error,
        "mde": mde,
        "threshold": float(threshold),
        "observation_count": int(len(array)),
    }


def _top_ids(frame: pd.DataFrame, score_column: str) -> set[str]:
    ranked = frame.loc[:, ["instrument", score_column]].copy()
    ranked["instrument"] = ranked["instrument"].astype(str)
    ranked = ranked.sort_values([score_column, "instrument"], ascending=[False, True], kind="mergesort")
    if len(ranked) < 5:
        _raise("daily panel has fewer than five rows", "ADVISORY_N3_MINUTE_MVE_OOF_INVALID")
    return set(ranked.head(5)["instrument"])


def _top5_net_value(frame: pd.DataFrame, instruments: set[str]) -> float:
    top = frame.loc[frame["instrument"].astype(str).isin(instruments)]
    labels = pd.to_numeric(top["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    return float(labels.mean()) if _top5_outcome_evaluable(frame, instruments) else float("nan")


def _top5_outcome_evaluable(frame: pd.DataFrame, instruments: set[str]) -> bool:
    top = frame.loc[frame["instrument"].astype(str).isin(instruments)]
    labels = pd.to_numeric(top["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    return bool(len(top) == 5 and top["outcome_known"].fillna(False).astype(bool).all() and np.isfinite(labels).all())


def _mean(values: pd.Series) -> float | None:
    array = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    array = array[np.isfinite(array)]
    return float(array.mean()) if len(array) else None


def _mean_by_day_score_correlation(frame: pd.DataFrame, left: str, right: str) -> float | None:
    values = [
        _safe_correlation(group[left], group[right], method="spearman")
        for _, group in frame.groupby("decision_as_of_trade_date", sort=True)
    ]
    finite = np.asarray([value for value in values if value is not None], dtype=float)
    return float(finite.mean()) if len(finite) else None


def _check_resource_limits(request: FrozenMinuteInformationSetRequestV1, stage: str) -> None:
    rss = _peak_rss_bytes()
    if rss > request.resource_max_rss_bytes:
        _raise(
            "minute MVE memory exceeds frozen limit",
            "ADVISORY_N3_MINUTE_MVE_RESOURCE_LIMIT_EXCEEDED",
            stage=stage,
            peak_rss_bytes=rss,
        )


def _git_origin_main_commit(repository_root: Path) -> str:
    command, root = _git_command_for_worktree(repository_root)
    try:
        commit = (
            subprocess.run(
                [*command, "rev-parse", "origin/main"],
                cwd=root,
                check=True,
                capture_output=True,
                text=True,
            )
            .stdout.strip()
            .lower()
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        _raise(
            "origin/main commit cannot be read",
            "ADVISORY_N3_MINUTE_MVE_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    if len(commit) != 40 or any(value not in "0123456789abcdef" for value in commit):
        _raise("origin/main commit is invalid", "ADVISORY_N3_MINUTE_MVE_REQUEST_INVALID")
    return commit


def _write_immutable_request(path: Path, request: FrozenMinuteInformationSetRequestV1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(
            request.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    if path.exists():
        try:
            existing = FrozenMinuteInformationSetRequestV1.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception as exc:
            _raise(
                "existing minute request is invalid",
                "ADVISORY_N3_MINUTE_MVE_REQUEST_INVALID",
                error_type=type(exc).__name__,
            )
        if existing.request_sha256 != request.request_sha256 or path.read_bytes() != encoded:
            _raise("request path contains different content", "ADVISORY_N3_MINUTE_MVE_REQUEST_INVALID")
        return
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _read_parquet(
    path: Path,
    *,
    columns: Sequence[str] | None = None,
    reason_code: str,
) -> pd.DataFrame:
    try:
        return pd.read_parquet(path, columns=list(columns) if columns is not None else None)
    except Exception as exc:
        _raise(
            "minute parquet cannot be read",
            reason_code,
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )


def _read_source_spike_receipt(path: Path) -> dict[str, Any]:
    payload = _read_json(path, "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH")
    if payload.get("schema_version") != "advisory_n3_minute_source_spike_receipt_v1":
        _raise("source spike schema drift", "ADVISORY_N3_MINUTE_MVE_SOURCE_IDENTITY_MISMATCH")
    return payload


def _read_json(path: Path, reason_code: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _raise(
            "minute JSON cannot be read",
            reason_code,
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )
    if not isinstance(payload, dict):
        _raise("minute JSON root is not an object", reason_code, path=path.as_posix())
    return payload


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _resolve_bound_path(value: str | Path) -> Path:
    text = str(value).replace("\\", "/")
    if os.name == "nt" and text.startswith("/mnt/") and len(text) > 6 and text[5].isalpha() and text[6] == "/":
        return Path(f"{text[5].upper()}:/{text[7:]}").resolve()
    if os.name != "nt" and len(text) > 2 and text[0].isalpha() and text[1:3] == ":/":
        return Path(f"/mnt/{text[0].lower()}/{text[3:]}").resolve()
    return Path(value).resolve()


def _raise(message: str, reason_code: str, **context: Any) -> NoReturn:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "aggregate_minute_day",
    "build_minute_feature_panel",
    "evaluate_minute_models",
    "fingerprint_minute_source",
    "inspect_minute_information_set_bundle",
    "prepare_minute_information_set_request",
    "run_minute_crossfit",
    "run_minute_information_set_mve",
]
