from __future__ import annotations

import json
import math
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, NoReturn, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.alpha_signal_audit_pipeline import _git_command_for_worktree
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.financial_event_information_set_contracts import (
    EVENT_DIRECTION_BY_TYPE,
    EVENT_DISCLOSURE_FEATURES,
    EVENT_DISCLOSURE_SCHEMA_FEATURES,
    EVENT_MVE_CUMULATIVE_CANDIDATE_INDEX,
    EVENT_MVE_EXPERIMENT_ID,
    EVENT_MVE_FEATURE_SCHEMA_HASH,
    EVENT_MVE_FEATURE_SCHEMA_VERSION,
    EVENT_MVE_HYPOTHESIS_FAMILY_ID,
    EVENT_MVE_SOURCE_PROJECTION_SHA256,
    EVENT_MVE_SOURCE_QUALITY,
    EVENT_PARENT_FEATURES,
    EVENT_SIGNED_FEATURES,
    EVENT_SIGNED_SCHEMA_FEATURES,
    EVENT_SOURCE_TYPES,
    FinancialEventInformationSetReceiptV1,
    FrozenFinancialEventInformationSetRequestV1,
    build_financial_event_receipt,
    build_financial_event_request,
)
from backend.services.advisory_model_first.financial_event_source_readiness import (
    BUNDLE_MEMBERS as SOURCE_BUNDLE_MEMBERS,
    BUNDLE_SCHEMA as SOURCE_BUNDLE_SCHEMA,
    inspect_financial_event_source_bundle,
)
from backend.services.advisory_model_first.independent_package_alpha_audit_pipeline import (
    _read_bundle as _read_n2b_bundle,
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
from backend.services.advisory_model_first.qe_file_source import initialize_qlib, load_trading_calendar
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


PARENT_ARM_ID = "CURRENT_IC_PARENT"
PARENT_COLUMNS = (
    "arm_id",
    "decision_as_of_trade_date",
    "instrument",
    "score",
    "target_trade_date",
    "economic_net_excess_bps",
    "outcome_known",
)
EVENT_COLUMNS = (
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
)
MODEL_SCORE_COLUMNS = {
    "EVENT_PARENT_COMPARATOR_V1": "parent_comparator_oof_score",
    "EVENT_DISCLOSURE_CONTROL_V1": "disclosure_control_oof_score",
    "EVENT_SIGNED_CONTENT_V1": "signed_candidate_oof_score",
}
MVE_BUNDLE_SCHEMA = "advisory_n3_financial_event_information_set_bundle_v1"
RESULT_IDENTITY_MEMBERS = frozenset(
    {
        "source_identity_receipt.json",
        "feature_schema.json",
        "event_feature_panel.parquet",
        "feature_coverage_daily.parquet",
        "oof_scores.parquet",
        "fold_diagnostics.parquet",
        "model_daily.parquet",
        "model_summary.json",
        "stability_report.json",
        "frontier_receipt.json",
    }
)
MVE_BUNDLE_MEMBERS = RESULT_IDENTITY_MEMBERS | {
    "request.json",
    "resource_report.json",
    "registry_records.json",
    "learnability_receipt.json",
}


def prepare_financial_event_information_set_request(
    *,
    source_bundle_path: str | Path,
    n2b_bundle_path: str | Path,
    n1_bundle_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
) -> FrozenFinancialEventInformationSetRequestV1:
    """Freeze one clean-main, development-only financial-event learnability request."""

    _require_formal_environment()
    source_path = _resolve_bound_path(source_bundle_path)
    n2b_path = _resolve_bound_path(n2b_bundle_path)
    n1_path = _resolve_bound_path(n1_bundle_path)
    repository = _resolve_bound_path(repository_root)
    output = _resolve_bound_path(output_root)
    source = _read_source_bundle(source_path)
    n2b = _read_n2b_bundle(n2b_path)
    n1 = _read_n1_bundle(n1_path)
    _validate_bound_sources(source=source, n2b=n2b, n1=n1)
    dirty = _cross_os_git_dirty_paths(repository)
    if dirty:
        _raise(
            "financial-event request requires a clean repository",
            "ADVISORY_N3_EVENT_MVE_REQUEST_INVALID",
            dirty_paths=dirty[:20],
        )
    commit = _cross_os_git_commit(repository)
    origin_main = _git_origin_main_commit(repository)
    if commit != origin_main:
        _raise(
            "financial-event request requires HEAD to equal origin/main",
            "ADVISORY_N3_EVENT_MVE_REQUEST_INVALID",
            repository_commit=commit,
            origin_main_commit=origin_main,
        )
    calendar = _load_and_verify_calendar(n1["request"])
    evidence_refs = tuple(
        evidence_reference_for_file(path, role=role)
        for role, path in (
            ("n3_event_source_manifest", source_path / "manifest.json"),
            ("n3_event_source_request", source_path / "source_request.json"),
            ("n3_event_source_receipt", source_path / "source_readiness_receipt.json"),
            ("n3_event_source_projection", source_path / "event_source_projection.parquet"),
            ("n3_event_source_support", source_path / "source_support_daily.parquet"),
            ("n3_event_n2b_manifest", n2b_path / "manifest.json"),
            ("n3_event_n2b_request", n2b_path / "request.json"),
            ("n3_event_n2b_receipt", n2b_path / "audit_receipt.json"),
            ("n3_event_n2b_outcomes", n2b_path / "arm_signal_outcomes.parquet"),
            ("n3_event_n2b_top5", n2b_path / "arm_top5_daily.parquet"),
            ("n3_event_n2b_signal_daily", n2b_path / "signal_metrics_daily.parquet"),
            ("n3_event_n1_manifest", n1_path / "manifest.json"),
            ("n3_event_n1_request", n1_path / "request.json"),
            ("n3_event_n1_cpcv", n1_path / "n1_label_interval_cpcv.json"),
            ("n3_event_n1_regime_daily", n1_path / "learnability_daily.parquet"),
        )
    )
    calendar_dates = tuple(value.date() for value in calendar)
    calendar_sha = canonical_json_sha256({"market_sessions": [value.isoformat() for value in calendar_dates]})
    source_request = source["request"]
    source_receipt = source["receipt"]
    source_dataset_identity = n2b["record"].dataset_identity
    policy_identity = n2b["record"].policy_identity
    split_identity = n1["request"].split_policy_sha256
    dataset_identity = canonical_json_sha256(
        {
            "source_dataset_identity": source_dataset_identity,
            "source_bundle_id": source_path.name,
            "source_projection_sha256": EVENT_MVE_SOURCE_PROJECTION_SHA256,
            "n1_split_policy_sha256": split_identity,
            "trading_calendar_sha256": calendar_sha,
            "feature_schema_hash": EVENT_MVE_FEATURE_SCHEMA_HASH,
            "policy_identity": policy_identity,
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }
    )
    request = build_financial_event_request(
        evidence_refs=evidence_refs,
        source_bundle_path=source_path.as_posix(),
        source_bundle_id=source_path.name,
        source_request_sha256=str(source_request["request_sha256"]),
        source_receipt_sha256=sha256_file(source_path / "source_readiness_receipt.json"),
        source_projection_sha256=EVENT_MVE_SOURCE_PROJECTION_SHA256,
        n2b_bundle_path=n2b_path.as_posix(),
        n2b_bundle_id=n2b_path.name,
        n2b_request_sha256=n2b["request"].request_sha256,
        n2b_receipt_sha256=n2b["receipt"].receipt_sha256,
        n1_bundle_path=n1_path.as_posix(),
        n1_bundle_id=n1_path.name,
        n1_request_sha256=n1["request"].request_sha256,
        n1_split_policy_sha256=split_identity,
        qlib_daily_root=n1["request"].qlib_daily_root,
        n1_market_calendar_sha256=n1["request"].market_calendar_identity.sha256,
        n1_market_calendar_row_count=n1["request"].market_calendar_identity.row_count,
        n1_market_calendar_cutoff=n1["request"].market_calendar_identity.cutoff_trade_date,
        n1_calendar_data_cutoff=n1["request"].data_cutoff,
        trading_calendar=calendar_dates,
        trading_calendar_sha256=calendar_sha,
        source_dataset_identity=source_dataset_identity,
        dataset_identity=dataset_identity,
        policy_identity=policy_identity,
        registry_path=str(source_request["registry_path"]),
        route_path=str(source_request["route_path"]),
        repository_root=repository.as_posix(),
        repository_commit=commit,
        output_root=output.as_posix(),
    )
    _write_immutable_request(_resolve_bound_path(output_path), request)
    if source_receipt.get("next_task") != "N3_FINANCIAL_EVENT_INFORMATION_SET_MVE_DESIGN":
        _raise("financial-event source route drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    return request


def build_event_feature_panel(
    *,
    parent: pd.DataFrame,
    events: pd.DataFrame,
    trading_calendar: Sequence[Any],
) -> pd.DataFrame:
    """Build frozen event features without accepting outcomes or future columns."""

    required_parent = {"decision_as_of_trade_date", "instrument", "score"}
    if not required_parent.issubset(parent.columns):
        _raise("parent feature keys are missing", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    base = parent.loc[:, ["decision_as_of_trade_date", "instrument", "score"]].copy()
    base["decision_as_of_trade_date"] = pd.to_datetime(base["decision_as_of_trade_date"], errors="raise").dt.normalize()
    base["instrument"] = base["instrument"].astype(str)
    base["score"] = pd.to_numeric(base["score"], errors="coerce")
    if (
        base.empty
        or base.duplicated(["decision_as_of_trade_date", "instrument"]).any()
        or not base["instrument"].eq(base["instrument"].str.upper()).all()
        or not np.isfinite(base["score"].to_numpy(dtype=float)).all()
    ):
        _raise("parent feature keys are invalid", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    calendar = pd.DatetimeIndex(pd.to_datetime(tuple(trading_calendar), errors="raise")).normalize()
    if calendar.empty or calendar.has_duplicates or not calendar.is_monotonic_increasing:
        _raise("trading calendar is invalid", "ADVISORY_N3_EVENT_MVE_PIT_VIOLATION")
    calendar_positions = {value: index for index, value in enumerate(calendar)}
    decision_positions = base["decision_as_of_trade_date"].map(calendar_positions)
    if decision_positions.isna().any():
        _raise("parent date is outside calendar", "ADVISORY_N3_EVENT_MVE_PIT_VIOLATION")
    event = _normalize_events(events, calendar_positions)
    base = base.reset_index(drop=True)
    base["parent_rank_pct"] = (
        base.groupby("decision_as_of_trade_date", sort=False)["score"]
        .rank(method="average", pct=True)
        .astype("float32")
    )
    output_arrays = {
        name: np.zeros(len(base), dtype=np.float32) for name in (*EVENT_DISCLOSURE_FEATURES, *EVENT_SIGNED_FEATURES)
    }
    output_arrays["event_latest_disclosure_age_120"].fill(121.0)
    parent_groups = base.groupby("instrument", sort=False).indices
    event_groups = event.groupby("instrument", sort=False).indices
    decision_index_values = decision_positions.to_numpy(dtype=np.int32)
    # ALGO-COMPLEXITY-001: the outer loop is one pass over at most 4,503
    # instruments; each group uses prefix sums/searchsorted, not a row-by-row
    # market-panel join.
    for instrument, parent_indexes_raw in parent_groups.items():
        parent_indexes = np.asarray(parent_indexes_raw, dtype=np.int64)
        event_indexes_raw = event_groups.get(instrument)
        if event_indexes_raw is None:
            continue
        group = event.loc[np.asarray(event_indexes_raw, dtype=np.int64)].sort_values(
            ["trade_index", "source_type", "event_type", "source_record_key"],
            ascending=[True, True, True, True],
            kind="mergesort",
        )
        event_pos = group["trade_index"].to_numpy(dtype=np.int32)
        target_pos = decision_index_values[parent_indexes]
        signed = group["signed_value"].to_numpy(dtype=float)
        direction = group["direction"].to_numpy(dtype=np.int8)
        qualifying = direction != 0
        neutral = direction == 0
        for lookback in (20, 60, 120, 252):
            output_arrays[f"event_signed_value_sum_{lookback}"][parent_indexes] = _window_sum(
                event_pos, signed, target_pos, lookback
            )
        output_arrays["event_disclosure_count_120_log1p"][parent_indexes] = np.log1p(
            _window_sum(event_pos, np.ones(len(group)), target_pos, 120)
        )
        output_arrays["event_neutral_count_120_log1p"][parent_indexes] = np.log1p(
            _window_sum(event_pos, neutral.astype(float), target_pos, 120)
        )
        positive20 = _window_sum(event_pos, (direction > 0).astype(float), target_pos, 20)
        negative20 = _window_sum(event_pos, (direction < 0).astype(float), target_pos, 20)
        positive120 = _window_sum(event_pos, (direction > 0).astype(float), target_pos, 120)
        negative120 = _window_sum(event_pos, (direction < 0).astype(float), target_pos, 120)
        output_arrays["event_positive_count_20_log1p"][parent_indexes] = np.log1p(positive20)
        output_arrays["event_negative_count_20_log1p"][parent_indexes] = np.log1p(negative20)
        output_arrays["event_positive_count_120_log1p"][parent_indexes] = np.log1p(positive120)
        output_arrays["event_negative_count_120_log1p"][parent_indexes] = np.log1p(negative120)
        disclosure_count = _window_sum(event_pos, np.ones(len(group)), target_pos, 120)
        qualifying_count = _window_sum(event_pos, qualifying.astype(float), target_pos, 120)
        output_arrays["event_disclosure_seen_120"][parent_indexes] = (disclosure_count > 0).astype(np.float32)
        output_arrays["event_qualifying_seen_120"][parent_indexes] = (qualifying_count > 0).astype(np.float32)
        source_presence = np.zeros(len(parent_indexes), dtype=np.float32)
        for source_type in EVENT_SOURCE_TYPES:
            source_presence += (
                _window_sum(event_pos, group["source_type"].eq(source_type).to_numpy(dtype=float), target_pos, 120) > 0
            )
        output_arrays["event_source_type_count_120"][parent_indexes] = source_presence
        for source_type, feature_name in (
            ("tushare_forecast", "event_forecast_signed_value_sum_120"),
            ("tushare_express", "event_express_signed_value_sum_120"),
            ("tushare_fina_indicator", "event_fina_indicator_signed_value_sum_120"),
        ):
            output_arrays[feature_name][parent_indexes] = _window_sum(
                event_pos,
                signed * group["source_type"].eq(source_type).to_numpy(dtype=float),
                target_pos,
                120,
            )
        latest = np.searchsorted(event_pos, target_pos, side="right") - 1
        valid_latest = latest >= 0
        latest_age = np.full(len(target_pos), 121.0, dtype=np.float32)
        latest_age[valid_latest] = target_pos[valid_latest] - event_pos[latest[valid_latest]]
        latest_age[latest_age > 120] = 121.0
        output_arrays["event_latest_disclosure_age_120"][parent_indexes] = latest_age
        qualifying_group = group.loc[qualifying].drop_duplicates("trade_index", keep="first")
        if not qualifying_group.empty:
            qpos = qualifying_group["trade_index"].to_numpy(dtype=np.int32)
            qvalues = qualifying_group["signed_value"].to_numpy(dtype=float)
            qlatest = np.searchsorted(qpos, target_pos, side="right") - 1
            qvalid = qlatest >= 0
            qage = np.full(len(target_pos), 121, dtype=np.int32)
            qage[qvalid] = target_pos[qvalid] - qpos[qlatest[qvalid]]
            qvalid &= qage <= 120
            latest_values = np.zeros(len(target_pos), dtype=np.float32)
            latest_values[qvalid] = qvalues[qlatest[qvalid]]
            output_arrays["event_latest_qualifying_signed_value_120"][parent_indexes] = latest_values
    for name, values in output_arrays.items():
        base[name] = values
    feature_values = base.loc[:, [*EVENT_PARENT_FEATURES, *EVENT_DISCLOSURE_FEATURES, *EVENT_SIGNED_FEATURES]].to_numpy(
        dtype=float
    )
    if not np.isfinite(feature_values).all():
        _raise("event feature panel contains non-finite values", "ADVISORY_N3_EVENT_MVE_FEATURE_INVALID")
    return base.sort_values(["decision_as_of_trade_date", "instrument"], kind="mergesort").reset_index(drop=True)


def _normalize_events(events: pd.DataFrame, calendar_positions: Mapping[pd.Timestamp, int]) -> pd.DataFrame:
    if set(events.columns) != set(EVENT_COLUMNS):
        _raise(
            "event projection schema drift",
            "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH",
            columns=sorted(str(value) for value in events.columns),
        )
    event = events.loc[:, EVENT_COLUMNS].copy()
    if event.empty or event["source_record_key"].astype(str).duplicated().any():
        _raise("event projection key drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    event["instrument"] = event["instrument"].astype(str)
    event["source_type"] = event["source_type"].astype(str)
    event["event_type"] = event["event_type"].astype(str)
    event["source_record_key"] = event["source_record_key"].astype(str)
    event["effective_trade_date"] = pd.to_datetime(event["effective_trade_date"], errors="raise").dt.normalize()
    if (
        not event["instrument"].eq(event["instrument"].str.upper()).all()
        or set(event["source_type"].unique()) != set(EVENT_SOURCE_TYPES)
        or set(event["event_type"].unique()) != set(EVENT_DIRECTION_BY_TYPE)
        or not event["source_time_quality"].astype(str).eq(EVENT_MVE_SOURCE_QUALITY).all()
        or not event["effective_rule"].astype(str).eq("announcement_date_only_next_trading_day").all()
    ):
        _raise("event projection categorical identity drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    event["direction"] = event["event_type"].map(EVENT_DIRECTION_BY_TYPE)
    should_signal = event["should_signal"].astype(bool)
    if not should_signal.eq(event["direction"].ne(0)).all():
        _raise("event signal/direction relation drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    severity = pd.to_numeric(event["severity_score"], errors="coerce").to_numpy(dtype=float)
    confidence = pd.to_numeric(event["confidence"], errors="coerce").to_numpy(dtype=float)
    if (
        not np.isfinite(severity).all()
        or not np.isfinite(confidence).all()
        or (severity < 0).any()
        or (severity > 1).any()
        or (confidence < 0).any()
        or (confidence > 1).any()
    ):
        _raise("event numeric identity drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    event["signed_value"] = event["direction"].to_numpy(dtype=float) * severity * confidence
    event["trade_index"] = event["effective_trade_date"].map(calendar_positions)
    if event["trade_index"].isna().any():
        _raise("event date is outside frozen calendar", "ADVISORY_N3_EVENT_MVE_PIT_VIOLATION")
    event["trade_index"] = event["trade_index"].astype(np.int32)
    return event


def _window_sum(
    event_positions: np.ndarray,
    values: np.ndarray,
    target_positions: np.ndarray,
    lookback: int,
) -> np.ndarray:
    prefix = np.concatenate(([0.0], np.cumsum(np.asarray(values, dtype=float))))
    left = np.searchsorted(event_positions, target_positions - lookback, side="left")
    right = np.searchsorted(event_positions, target_positions, side="right")
    return prefix[right] - prefix[left]


def attach_event_outcomes(*, features: pd.DataFrame, parent_outcomes: pd.DataFrame) -> pd.DataFrame:
    """Attach labels only after target-free features have been built."""

    keys = ["decision_as_of_trade_date", "instrument"]
    outcomes = parent_outcomes.loc[:, [*keys, "score", "economic_net_excess_bps", "outcome_known"]].copy()
    outcomes["decision_as_of_trade_date"] = pd.to_datetime(outcomes["decision_as_of_trade_date"]).dt.normalize()
    outcomes["instrument"] = outcomes["instrument"].astype(str)
    if outcomes.duplicated(keys).any() or len(outcomes) != len(features):
        _raise("parent outcome key parity failed", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    # ALGO-COMPLEXITY-001: this is one validated one-to-one merge over the
    # frozen 1,710,301 parent keys; it does not form a cartesian join.
    merged = features.merge(
        outcomes,
        on=keys,
        how="left",
        validate="one_to_one",
        suffixes=("", "_outcome"),
    )
    if (
        len(merged) != len(features)
        or merged["outcome_known"].isna().any()
        or not np.allclose(
            merged["score"].to_numpy(dtype=float),
            merged["score_outcome"].to_numpy(dtype=float),
            atol=0.0,
            rtol=0.0,
        )
    ):
        _raise("parent outcome relation drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    return merged.drop(columns=["score_outcome"])


def validate_event_feature_support(
    *,
    features: pd.DataFrame,
    events: pd.DataFrame,
    request: FrozenFinancialEventInformationSetRequestV1,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    required = {
        "decision_as_of_trade_date",
        "instrument",
        "score",
        *EVENT_PARENT_FEATURES,
        *EVENT_DISCLOSURE_FEATURES,
        *EVENT_SIGNED_FEATURES,
    }
    if not required.issubset(features.columns):
        _raise("event feature schema is incomplete", "ADVISORY_N3_EVENT_MVE_FEATURE_INVALID")
    values = features.loc[:, [*EVENT_PARENT_FEATURES, *EVENT_DISCLOSURE_FEATURES, *EVENT_SIGNED_FEATURES]].to_numpy(
        dtype=float
    )
    if not np.isfinite(values).all():
        _raise("event feature support contains non-finite values", "ADVISORY_N3_EVENT_MVE_FEATURE_INVALID")
    ranked = features.sort_values(
        ["decision_as_of_trade_date", "score", "instrument"],
        ascending=[True, False, True],
        kind="mergesort",
    ).copy()
    ranked["parent_position"] = ranked.groupby("decision_as_of_trade_date", sort=False).cumcount() + 1
    rows: list[dict[str, Any]] = []
    for decision_date, day in ranked.groupby("decision_as_of_trade_date", sort=True):
        top20 = day["parent_position"] <= 20
        top50 = day["parent_position"] <= 50
        top50_qualifying = int(day.loc[top50, "event_qualifying_seen_120"].sum())
        rows.append(
            {
                "decision_as_of_trade_date": decision_date,
                "instrument_count": int(len(day)),
                "disclosure_count": int(day["event_disclosure_seen_120"].sum()),
                "qualifying_count": int(day["event_qualifying_seen_120"].sum()),
                "top20_disclosure_count": int(day.loc[top20, "event_disclosure_seen_120"].sum()),
                "top20_qualifying_count": int(day.loc[top20, "event_qualifying_seen_120"].sum()),
                "top50_qualifying_count": top50_qualifying,
                "top50_qualifying_mixed": bool(0 < top50_qualifying < min(50, int(top50.sum()))),
            }
        )
    daily = pd.DataFrame(rows)
    top20_slots = int(sum(min(20, value) for value in daily["instrument_count"]))
    top20_disclosure_fraction = float(daily["top20_disclosure_count"].sum() / top20_slots)
    top20_qualifying_fraction = float(daily["top20_qualifying_count"].sum() / top20_slots)
    signed_unique = {name: int(features[name].nunique(dropna=False)) for name in EVENT_SIGNED_FEATURES}
    direction = events["event_type"].astype(str).map(EVENT_DIRECTION_BY_TYPE)
    source_counts = events.groupby("source_type", sort=True).size().astype(int).to_dict()
    reasons: list[str] = []
    if len(features) != request.expected_parent_row_count:
        reasons.append("PARENT_ROW_COUNT_MISMATCH")
    if len(events) != request.expected_source_row_count:
        reasons.append("SOURCE_ROW_COUNT_MISMATCH")
    if len(daily) != request.expected_decision_date_count:
        reasons.append("DECISION_DATE_COUNT_MISMATCH")
    if top20_disclosure_fraction < request.minimum_top20_disclosure_fraction_120:
        reasons.append("TOP20_DISCLOSURE_FRACTION_BELOW_MINIMUM")
    if top20_qualifying_fraction < request.minimum_top20_qualifying_fraction_120:
        reasons.append("TOP20_QUALIFYING_FRACTION_BELOW_MINIMUM")
    if int((daily["top20_disclosure_count"] >= request.minimum_top20_disclosure_count).sum()) < (
        request.minimum_top20_supported_days
    ):
        reasons.append("TOP20_DISCLOSURE_SUPPORTED_DAYS_BELOW_MINIMUM")
    if int(daily["top50_qualifying_mixed"].sum()) < request.minimum_top50_mixed_qualifying_days:
        reasons.append("TOP50_MIXED_QUALIFYING_DAYS_BELOW_MINIMUM")
    if set(source_counts) != set(EVENT_SOURCE_TYPES) or any(
        source_counts.get(item, 0) <= 0 for item in EVENT_SOURCE_TYPES
    ):
        reasons.append("SOURCE_TYPE_SUPPORT_INCOMPLETE")
    if not all(bool((direction == value).any()) for value in (-1, 0, 1)):
        reasons.append("DIRECTION_SUPPORT_INCOMPLETE")
    if any(value < 2 for value in signed_unique.values()):
        reasons.append("SIGNED_FEATURE_CONSTANT")
    summary = {
        "schema_version": "advisory_n3_financial_event_feature_support_v1",
        "parent_row_count": int(len(features)),
        "decision_date_count": int(len(daily)),
        "source_row_count": int(len(events)),
        "source_counts": {str(key): int(value) for key, value in source_counts.items()},
        "direction_counts": {str(value): int((direction == value).sum()) for value in (-1, 0, 1)},
        "top20_disclosure_fraction_120": top20_disclosure_fraction,
        "top20_qualifying_fraction_120": top20_qualifying_fraction,
        "top20_supported_day_count": int(
            (daily["top20_disclosure_count"] >= request.minimum_top20_disclosure_count).sum()
        ),
        "top50_mixed_qualifying_day_count": int(daily["top50_qualifying_mixed"].sum()),
        "signed_feature_unique_counts": signed_unique,
        "support_sufficient": not reasons,
        "reason_codes": sorted(reasons),
    }
    return daily, summary


def run_event_crossfit(
    *,
    panel: pd.DataFrame,
    paths: Sequence[Mapping[str, Any]],
    request: FrozenFinancialEventInformationSetRequestV1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import Ridge
        from sklearn.preprocessing import StandardScaler
    except Exception as exc:
        _raise(
            "scikit-learn is unavailable",
            "ADVISORY_N3_EVENT_MVE_MODEL_INVALID",
            error_type=type(exc).__name__,
        )
    if len(paths) != request.expected_ready_path_count or any(path.get("status") != "READY" for path in paths):
        _raise("event CPCV path roster drift", "ADVISORY_N3_EVENT_MVE_CPCV_INVALID")
    frame = panel.sort_values(["decision_as_of_trade_date", "instrument"], kind="mergesort").reset_index(drop=True)
    dates = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    source_dates = set(dates.unique())
    labels = pd.to_numeric(frame["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    known = frame["outcome_known"].fillna(False).astype(bool).to_numpy()
    evaluable = known & np.isfinite(labels)
    scores = frame.loc[
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
        columns = list(trial.feature_columns)
        sums = np.zeros(len(frame), dtype=np.float64)
        counts = np.zeros(len(frame), dtype=np.int16)
        for path in paths:
            train_dates = pd.DatetimeIndex(pd.to_datetime(path.get("train_dates", ()))).normalize()
            validation_dates = pd.DatetimeIndex(pd.to_datetime(path.get("validation_dates", ()))).normalize()
            if (
                not len(train_dates)
                or not len(validation_dates)
                or set(train_dates) & set(validation_dates)
                or not set(train_dates).issubset(source_dates)
                or not set(validation_dates).issubset(source_dates)
            ):
                _raise("event CPCV date identity invalid", "ADVISORY_N3_EVENT_MVE_CPCV_INVALID")
            train_index = np.flatnonzero(dates.isin(train_dates).to_numpy() & evaluable)
            validation_index = np.flatnonzero(dates.isin(validation_dates).to_numpy())
            x_train_raw = frame.loc[train_index, columns].to_numpy(dtype=float)
            x_validation_raw = frame.loc[validation_index, columns].to_numpy(dtype=float)
            if not len(train_index) or not len(validation_index) or not np.isfinite(x_train_raw).any(axis=0).all():
                _raise(
                    "event CPCV fold has empty rows or all-missing feature",
                    "ADVISORY_N3_EVENT_MVE_MODEL_INVALID",
                    path_id=path.get("path_id"),
                    trial_id=trial.trial_id,
                )
            if trial.trial_id == "EVENT_SIGNED_CONTENT_V1" and any(
                np.unique(x_train_raw[:, columns.index(name)]).size < 2 for name in EVENT_SIGNED_FEATURES
            ):
                _raise(
                    "event signed feature is constant in an outer train fold",
                    "ADVISORY_N3_EVENT_MVE_MODEL_INVALID",
                    path_id=path.get("path_id"),
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
                _raise("event Ridge produced non-finite OOF", "ADVISORY_N3_EVENT_MVE_MODEL_INVALID")
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
                "event OOF prediction multiplicity drift",
                "ADVISORY_N3_EVENT_MVE_CPCV_INVALID",
                trial_id=trial.trial_id,
                counts={str(int(key)): int(value) for key, value in zip(unique, frequencies, strict=True)},
            )
        column = MODEL_SCORE_COLUMNS[trial.trial_id]
        scores[column] = (sums / counts).astype("float32")
        scores[f"{column}_count"] = counts
    return scores, pd.DataFrame(diagnostics).sort_values(["trial_id", "path_id"]).reset_index(drop=True)


def evaluate_event_models(
    *,
    oof_scores: pd.DataFrame,
    regime_daily: pd.DataFrame,
    feature_coverage_daily: pd.DataFrame,
    feature_support: Mapping[str, Any],
    request: FrozenFinancialEventInformationSetRequestV1,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    score_columns = {
        "parent": "parent_rank_pct",
        "parent_comparator": "parent_comparator_oof_score",
        "disclosure_control": "disclosure_control_oof_score",
        "candidate": "signed_candidate_oof_score",
    }
    required = {
        "decision_as_of_trade_date",
        "instrument",
        "economic_net_excess_bps",
        "outcome_known",
        *score_columns.values(),
    }
    if not required.issubset(oof_scores.columns):
        _raise("event OOF schema drift", "ADVISORY_N3_EVENT_MVE_MODEL_INVALID")
    scores = oof_scores.copy()
    scores["decision_as_of_trade_date"] = pd.to_datetime(scores["decision_as_of_trade_date"]).dt.normalize()
    if (
        scores.duplicated(["decision_as_of_trade_date", "instrument"]).any()
        or not np.isfinite(scores[list(score_columns.values())].to_numpy(dtype=float)).all()
    ):
        _raise("event OOF key/value drift", "ADVISORY_N3_EVENT_MVE_MODEL_INVALID")
    regimes = regime_daily.loc[:, ["decision_as_of_trade_date", "regime"]].copy()
    regimes["decision_as_of_trade_date"] = pd.to_datetime(regimes["decision_as_of_trade_date"]).dt.normalize()
    if regimes.duplicated("decision_as_of_trade_date").any():
        _raise("event regime date duplicated", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    regime_map = regimes.set_index("decision_as_of_trade_date")["regime"].astype(str).to_dict()
    previous: dict[str, set[str] | None] = {name: None for name in score_columns}
    rows: list[dict[str, Any]] = []
    for decision_date, day in scores.groupby("decision_as_of_trade_date", sort=True):
        ids = {name: _top_ids(day, column) for name, column in score_columns.items()}
        finite_label = day["outcome_known"].fillna(False).astype(bool) & np.isfinite(
            pd.to_numeric(day["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
        )
        labeled = day.loc[finite_label]
        row: dict[str, Any] = {
            "decision_as_of_trade_date": decision_date,
            "regime": regime_map.get(decision_date),
            "row_count": int(len(day)),
            "finite_label_row_count": int(finite_label.sum()),
        }
        for name, column in score_columns.items():
            row[f"{name}_rank_ic"] = _safe_correlation(
                labeled[column], labeled["economic_net_excess_bps"], method="spearman"
            )
            row[f"{name}_top5_evaluable"] = _top5_outcome_evaluable(day, ids[name])
            row[f"{name}_top5_net_excess_bps"] = _top5_net_value(day, ids[name])
            # ALGO-COMPLEXITY-001: `ids[name]` is exactly five instruments, so
            # this deterministic formatting is constant-size per decision day.
            row[f"{name}_instruments"] = ",".join(sorted(ids[name]))
            prior = previous[name]
            row[f"{name}_top5_churn"] = None if prior is None else float(1.0 - len(prior & ids[name]) / 5.0)
            previous[name] = ids[name]
        for baseline in ("parent", "parent_comparator", "disclosure_control"):
            row[f"candidate_{baseline}_replacement_count"] = int(5 - len(ids["candidate"] & ids[baseline]))
            row[f"candidate_{baseline}_intervened"] = ids["candidate"] != ids[baseline]
        rows.append(row)
    daily = pd.DataFrame(rows)
    for baseline in ("parent", "parent_comparator", "disclosure_control"):
        daily[f"candidate_rank_ic_delta_{baseline}"] = daily["candidate_rank_ic"] - daily[f"{baseline}_rank_ic"]
        daily[f"candidate_top5_lift_{baseline}_bps"] = (
            daily["candidate_top5_net_excess_bps"] - daily[f"{baseline}_top5_net_excess_bps"]
        )
    paired_columns = [
        column
        for baseline in ("parent", "parent_comparator", "disclosure_control")
        for column in (
            f"candidate_rank_ic_delta_{baseline}",
            f"candidate_top5_lift_{baseline}_bps",
        )
    ]
    daily["evaluable"] = np.isfinite(daily[paired_columns].to_numpy(dtype=float)).all(axis=1)
    coverage = feature_coverage_daily.copy()
    coverage["decision_as_of_trade_date"] = pd.to_datetime(coverage["decision_as_of_trade_date"]).dt.normalize()
    # ALGO-COMPLEXITY-001: both sides are unique and bounded to the frozen 386
    # decision dates; validate=one_to_one prevents accidental expansion.
    daily = daily.merge(coverage, on="decision_as_of_trade_date", how="left", validate="one_to_one")
    if daily["instrument_count"].isna().any():
        _raise("event daily coverage join drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    cumulative_alpha = 0.05 / request.cumulative_primary_comparison_count
    current_alpha = 0.05 / request.current_familywise_hypothesis_count
    inference = {
        "candidate_parent_rank_ic_delta": _metric_inference(
            daily["candidate_rank_ic_delta_parent"],
            request=request,
            alpha=cumulative_alpha,
            threshold=0.0,
            seed_offset=0,
        ),
        "candidate_parent_top5_lift_bps": _metric_inference(
            daily["candidate_top5_lift_parent_bps"],
            request=request,
            alpha=cumulative_alpha,
            threshold=request.minimum_parent_lift_bps,
            seed_offset=1,
        ),
        "candidate_parent_comparator_rank_ic_delta": _metric_inference(
            daily["candidate_rank_ic_delta_parent_comparator"],
            request=request,
            alpha=current_alpha,
            threshold=0.0,
            seed_offset=2,
        ),
        "candidate_parent_comparator_top5_lift_bps": _metric_inference(
            daily["candidate_top5_lift_parent_comparator_bps"],
            request=request,
            alpha=current_alpha,
            threshold=0.0,
            seed_offset=3,
        ),
        "candidate_disclosure_control_rank_ic_delta": _metric_inference(
            daily["candidate_rank_ic_delta_disclosure_control"],
            request=request,
            alpha=current_alpha,
            threshold=0.0,
            seed_offset=4,
        ),
        "candidate_disclosure_control_top5_lift_bps": _metric_inference(
            daily["candidate_top5_lift_disclosure_control_bps"],
            request=request,
            alpha=current_alpha,
            threshold=0.0,
            seed_offset=5,
        ),
    }
    intervention = {
        baseline: _intervention_support(daily, f"candidate_{baseline}_intervened", request)
        for baseline in ("parent", "parent_comparator", "disclosure_control")
    }
    support_sufficient = bool(feature_support.get("support_sufficient")) and all(
        value["support_sufficient"] for value in intervention.values()
    )
    stability = _stability_report(daily, request=request)
    reasons = list(feature_support.get("reason_codes", ()))
    reasons.extend(
        f"{baseline.upper()}__{reason}" for baseline, value in intervention.items() for reason in value["reason_codes"]
    )
    thresholds = {
        "candidate_parent_rank_ic_delta": 0.0,
        "candidate_parent_top5_lift_bps": request.minimum_parent_lift_bps,
        "candidate_parent_comparator_rank_ic_delta": 0.0,
        "candidate_parent_comparator_top5_lift_bps": 0.0,
        "candidate_disclosure_control_rank_ic_delta": 0.0,
        "candidate_disclosure_control_top5_lift_bps": 0.0,
    }
    for name, threshold in thresholds.items():
        lower = inference[name]["familywise_confidence_lower"]
        if lower is None or float(lower) <= threshold:
            reasons.append(f"{name.upper()}_LOWER_NOT_ABOVE_THRESHOLD")
    if stability["late_half_rank_ic_delta_mean"] is None or stability["late_half_rank_ic_delta_mean"] <= 0:
        reasons.append("LATE_HALF_RANK_IC_DELTA_NOT_POSITIVE")
    if stability["late_half_top5_lift_mean_bps"] is None or stability["late_half_top5_lift_mean_bps"] <= 0:
        reasons.append("LATE_HALF_TOP5_LIFT_NOT_POSITIVE")
    if stability["positive_joint_time_block_count"] < 3:
        reasons.append("FOUR_BLOCK_STABILITY_BELOW_MINIMUM")
    reasons = sorted(set(reasons))
    eligible = not reasons
    selected = "EVENT_SIGNED_CONTENT_V1" if eligible else None
    evidence_class = (
        "EXPLORATORY_CANDIDATE_SELECTED_NON_VINTAGE"
        if eligible
        else "EXPLORATORY_NOT_SELECTED_NON_VINTAGE"
        if support_sufficient
        else "EXPLORATORY_INSUFFICIENT_SUPPORT_NON_VINTAGE"
    )
    summary = {
        "schema_version": "advisory_n3_financial_event_model_summary_v1",
        "request_sha256": request.request_sha256,
        "planned_trial_count": 3,
        "generated_trial_count": 3,
        "evaluated_trial_count": 3,
        "selectable_trial_count": 1,
        "cumulative_candidate_index_prior": request.cumulative_candidate_index_prior,
        "cumulative_candidate_index": request.cumulative_candidate_index,
        "current_familywise_hypothesis_count": request.current_familywise_hypothesis_count,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "source_time_quality": EVENT_MVE_SOURCE_QUALITY,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "feature_support": dict(feature_support),
        "intervention_support": intervention,
        "support_sufficient": support_sufficient,
        "evidence_class": evidence_class,
        "inference": inference,
        "mean_rank_ic": {name: _mean(daily[f"{name}_rank_ic"]) for name in score_columns},
        "mean_top5_net_excess_bps": {name: _mean(daily[f"{name}_top5_net_excess_bps"]) for name in score_columns},
        "top5_evaluable_day_count": {
            name: int(daily[f"{name}_top5_evaluable"].astype(bool).sum()) for name in score_columns
        },
        "candidate_parent_lift_dsr": _deflated_sharpe_diagnostic(
            daily["candidate_top5_lift_parent_bps"].tolist(), trial_count=request.cumulative_candidate_index
        ),
        "candidate_score_spearman_mean": {
            baseline: _mean_by_day_score_correlation(scores, "signed_candidate_oof_score", score_columns[baseline])
            for baseline in ("parent", "parent_comparator", "disclosure_control")
        },
        "eligible": eligible,
        "reason_codes": reasons,
        "selected_trial_id": selected,
    }
    frontier = {
        "schema_version": "advisory_n3_financial_event_frontier_v1",
        "request_sha256": request.request_sha256,
        "eligible_trial_ids": (["EVENT_SIGNED_CONTENT_V1"] if eligible else []),
        "selected_trial_id": selected,
        "selected_trial_count": int(eligible),
        "support_sufficient": support_sufficient,
        "evidence_class": evidence_class,
        "source_time_quality": EVENT_MVE_SOURCE_QUALITY,
        "candidate_reselection_allowed": False,
        "exact_retry_allowed": True,
        "selection_rule": "CUMULATIVE_PARENT_AND_CURRENT_DISCLOSURE_CONTROL_LOWERS_SUPPORT_STABILITY_SELECT_ONCE",
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    frontier["frontier_sha256"] = canonical_json_sha256(frontier)
    return daily, summary, stability, frontier


def _stability_report(
    daily: pd.DataFrame,
    *,
    request: FrozenFinancialEventInformationSetRequestV1,
) -> dict[str, Any]:
    ordered = daily.sort_values("decision_as_of_trade_date").reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    positive_joint = 0
    for index, positions in enumerate(np.array_split(np.arange(len(ordered)), 4), 1):
        block = ordered.iloc[positions]
        rank_mean = _mean(block["candidate_rank_ic_delta_parent"])
        lift_mean = _mean(block["candidate_top5_lift_parent_bps"])
        positive = rank_mean is not None and rank_mean > 0 and lift_mean is not None and lift_mean > 0
        positive_joint += int(positive)
        rows.append(
            {
                "block_index": index,
                "start_date": pd.Timestamp(block["decision_as_of_trade_date"].min()).date().isoformat(),
                "end_date": pd.Timestamp(block["decision_as_of_trade_date"].max()).date().isoformat(),
                "day_count": int(len(block)),
                "rank_ic_delta_mean": rank_mean,
                "top5_lift_mean_bps": lift_mean,
                "positive_joint": positive,
            }
        )
    late = ordered.iloc[len(ordered) // 2 :]
    payload = {
        "schema_version": "advisory_n3_financial_event_stability_v1",
        "request_sha256": request.request_sha256,
        "rows": rows,
        "late_half_start_date": pd.Timestamp(late["decision_as_of_trade_date"].min()).date().isoformat(),
        "late_half_rank_ic_delta_mean": _mean(late["candidate_rank_ic_delta_parent"]),
        "late_half_top5_lift_mean_bps": _mean(late["candidate_top5_lift_parent_bps"]),
        "positive_joint_time_block_count": positive_joint,
        "four_block_rule": "AT_LEAST_THREE_BLOCKS_HAVE_POSITIVE_RANKIC_DELTA_AND_TOP5_LIFT",
        "sealed_holdout_accessed": False,
    }
    payload["stability_sha256"] = canonical_json_sha256(payload)
    return payload


def _intervention_support(
    daily: pd.DataFrame,
    column: str,
    request: FrozenFinancialEventInformationSetRequestV1,
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
    if evaluable_count < request.minimum_evaluable_days:
        reasons.append("EVALUABLE_DAY_COUNT_BELOW_MINIMUM")
    if intervention_count < request.minimum_intervention_days:
        reasons.append("INTERVENTION_DAY_COUNT_BELOW_MINIMUM")
    if fraction < request.minimum_intervention_fraction:
        reasons.append("INTERVENTION_FRACTION_BELOW_MINIMUM")
    if any(by_regime.get(regime, 0) < request.minimum_intervention_days_per_regime for regime in observed):
        reasons.append("INTERVENTION_REGIME_SUPPORT_BELOW_MINIMUM")
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


def _metric_inference(
    values: Sequence[float] | pd.Series,
    *,
    request: FrozenFinancialEventInformationSetRequestV1,
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
            "familywise_alpha": float(alpha),
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
        alpha=min(2.0 * alpha, 1.0),
    )
    rng = np.random.default_rng(request.bootstrap_seed + seed_offset)
    block = min(request.block_length_trading_days, len(array))
    blocks_needed = math.ceil(len(array) / block)
    starts = rng.integers(0, len(array), size=(request.bootstrap_repetitions, blocks_needed))
    offsets = np.arange(block)
    indexes = (starts[:, :, None] + offsets[None, None, :]) % len(array)
    samples = array[indexes.reshape(request.bootstrap_repetitions, -1)[:, : len(array)]]
    standard_error = float(samples.mean(axis=1).std(ddof=1))
    return {
        "point_estimate": float(array.mean()),
        "confidence_lower": ordinary[0],
        "confidence_upper": ordinary[1],
        "familywise_confidence_lower": familywise[0],
        "familywise_confidence_upper": familywise[1],
        "familywise_alpha": float(alpha),
        "bootstrap_standard_error": standard_error,
        "mde": float((1.959963984540054 + 0.8416212335729143) * standard_error),
        "threshold": float(threshold),
        "observation_count": int(len(array)),
    }


def _top_ids(frame: pd.DataFrame, score_column: str) -> set[str]:
    ranked = frame.loc[:, ["instrument", score_column]].sort_values(
        [score_column, "instrument"], ascending=[False, True], kind="mergesort"
    )
    if len(ranked) < 5:
        _raise("event daily panel has fewer than five rows", "ADVISORY_N3_EVENT_MVE_MODEL_INVALID")
    return set(ranked.head(5)["instrument"].astype(str))


def _top5_outcome_evaluable(frame: pd.DataFrame, instruments: set[str]) -> bool:
    top = frame.loc[frame["instrument"].astype(str).isin(instruments)]
    labels = pd.to_numeric(top["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    return bool(len(top) == 5 and top["outcome_known"].fillna(False).astype(bool).all() and np.isfinite(labels).all())


def _top5_net_value(frame: pd.DataFrame, instruments: set[str]) -> float:
    top = frame.loc[frame["instrument"].astype(str).isin(instruments)]
    labels = pd.to_numeric(top["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    return float(labels.mean()) if _top5_outcome_evaluable(frame, instruments) else float("nan")


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


def run_financial_event_information_set_mve(request_path: str | Path) -> dict[str, Any]:
    started = time.monotonic()
    request_file = _resolve_bound_path(request_path)
    try:
        request = FrozenFinancialEventInformationSetRequestV1.model_validate_json(
            request_file.read_text(encoding="utf-8")
        )
    except Exception as exc:
        _raise(
            "financial-event request cannot be loaded",
            "ADVISORY_N3_EVENT_MVE_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    existing = _find_existing_bundle(request)
    _verify_environment(request)
    if existing is not None:
        delivery = _deliver_bundle(request=request, bundle_path=existing)
        return _run_response(request, existing, delivery, exact_retry=True)
    sources = _load_verified_sources(request)
    _check_resource_limits(request, "sources_loaded")
    target_free_parent = sources["parent_outcomes"].loc[:, ["decision_as_of_trade_date", "instrument", "score"]]
    features = build_event_feature_panel(
        parent=target_free_parent,
        events=sources["events"],
        trading_calendar=request.trading_calendar,
    )
    coverage_daily, feature_support = validate_event_feature_support(
        features=features,
        events=sources["events"],
        request=request,
    )
    panel = attach_event_outcomes(features=features, parent_outcomes=sources["parent_outcomes"])
    _check_resource_limits(request, "feature_panel_built")
    oof, fold_diagnostics = run_event_crossfit(
        panel=panel,
        paths=sources["cpcv"]["paths"],
        request=request,
    )
    _check_resource_limits(request, "crossfit_complete")
    daily, summary, stability, frontier = evaluate_event_models(
        oof_scores=oof,
        regime_daily=sources["regime_daily"],
        feature_coverage_daily=coverage_daily,
        feature_support=feature_support,
        request=request,
    )
    _validate_parent_daily_parity(
        daily=daily,
        parent_top5_daily=sources["parent_top5_daily"],
        parent_signal_daily=sources["parent_signal_daily"],
    )
    _check_resource_limits(request, "evaluation_complete")
    bundle = _publish_bundle(
        request=request,
        features=features,
        coverage_daily=coverage_daily,
        oof_scores=oof,
        fold_diagnostics=fold_diagnostics,
        daily_metrics=daily,
        model_summary=summary,
        stability=stability,
        frontier=frontier,
        elapsed_seconds=time.monotonic() - started,
    )
    delivery = _deliver_bundle(request=request, bundle_path=bundle)
    return _run_response(request, bundle, delivery, exact_retry=False)


def inspect_financial_event_information_set_bundle(bundle_path: str | Path) -> dict[str, Any]:
    loaded = _read_mve_bundle(_resolve_bound_path(bundle_path))
    receipt = loaded["receipt"]
    return {
        "status": "VALID",
        "bundle_id": loaded["manifest"]["bundle_id"],
        "request_id": loaded["request"].request_id,
        "receipt_id": receipt.receipt_id,
        "selected_trial_id": receipt.selected_trial_id,
        "eligible_trial_ids": list(receipt.eligible_trial_ids),
        "evidence_class": receipt.evidence_class,
        "next_task": receipt.next_task,
        "planned_trial_count": 3,
        "generated_trial_count": 3,
        "evaluated_trial_count": 3,
        "selected_trial_count": receipt.selected_trial_count,
        "selectable_trial_count": 1,
        "cumulative_candidate_index": EVENT_MVE_CUMULATIVE_CANDIDATE_INDEX,
        "source_time_quality": EVENT_MVE_SOURCE_QUALITY,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "final_model_written": False,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "position_weight_output": False,
    }


def _read_source_bundle(path: Path) -> dict[str, Any]:
    inspected = inspect_financial_event_source_bundle(path)
    manifest = _read_json(path / "manifest.json", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    request = _read_json(path / "source_request.json", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    receipt = _read_json(path / "source_readiness_receipt.json", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    descriptors = manifest.get("files")
    projection_descriptor = (
        descriptors.get("event_source_projection.parquet") if isinstance(descriptors, dict) else None
    )
    if (
        inspected.get("status") != "VALID"
        or manifest.get("schema_version") != SOURCE_BUNDLE_SCHEMA
        or manifest.get("bundle_id") != path.name
        or set(descriptors or {}) != (set(SOURCE_BUNDLE_MEMBERS) - {"manifest.json"})
        or not isinstance(projection_descriptor, dict)
        or projection_descriptor.get("sha256") != EVENT_MVE_SOURCE_PROJECTION_SHA256
        or projection_descriptor.get("row_count") != 84_272
        or receipt.get("source_state") != "SOURCE_READY_NAVIGATION_ONLY_NON_VINTAGE"
        or receipt.get("source_time_quality") != EVENT_MVE_SOURCE_QUALITY
        or receipt.get("next_task") != "N3_FINANCIAL_EVENT_INFORMATION_SET_MVE_DESIGN"
        or receipt.get("sealed_holdout_accessed") is not False
        or manifest.get("database_write_count") != 0
        or manifest.get("network_call_count") != 0
        or manifest.get("tushare_call_count") != 0
    ):
        _raise("financial-event source bundle relation drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    return {"manifest": manifest, "request": request, "receipt": receipt}


def _validate_bound_sources(*, source: Mapping[str, Any], n2b: Mapping[str, Any], n1: Mapping[str, Any]) -> None:
    n2b_receipt = n2b["receipt"]
    n2b_record = n2b["record"]
    cpcv_path = _resolve_bound_path(n1["request"].output_root) / "tier1_bundles" / n1["manifest"]["bundle_id"]
    cpcv = _read_json(cpcv_path / "n1_label_interval_cpcv.json", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    split_policy = cpcv.get("split_policy", {})
    if (
        source["receipt"].get("source_state") != "SOURCE_READY_NAVIGATION_ONLY_NON_VINTAGE"
        or source["receipt"].get("evaluated_model_trial_count") != 0
        or n2b_record.experiment_id != "ADVISORY-N2B-INDEPENDENT-PACKAGE-ALPHA-AUDIT-V2"
        or n2b_receipt.decision_date_count != 386
        or n2b_receipt.signal_row_count_by_arm.get(PARENT_ARM_ID) != 1_710_301
        or n2b_receipt.sealed_holdout_accessed
        or n1["oracle"].sealed_holdout_accessed
        or n1["learnability"].sealed_holdout_accessed
        or n1["quadrant"].sealed_holdout_accessed
        or len(cpcv.get("paths", ())) != 28
        or any(path.get("status") != "READY" for path in cpcv["paths"])
        or not isinstance(split_policy, dict)
        or split_policy.get("group_count") != 8
        or split_policy.get("validation_group_count") != 2
        or split_policy.get("embargo_trading_days") != 20
    ):
        _raise("financial-event bound source lineage drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")


def _load_and_verify_calendar(n1_request: Any) -> pd.DatetimeIndex:
    initialize_qlib(n1_request.qlib_daily_root)
    # N1 intentionally hashes the feature calendar through `data_cutoff`; the
    # identity object's declared cutoff tracks the broader asset release and
    # must not be substituted for this semantic end date.
    full = load_trading_calendar("2023-01-01", n1_request.data_cutoff.isoformat())
    identity_window = full[full >= pd.Timestamp("2023-09-01")]
    identity_hash = canonical_json_sha256({"market_sessions": [item.date().isoformat() for item in identity_window]})
    if (
        len(identity_window) != n1_request.market_calendar_identity.row_count
        or identity_hash != n1_request.market_calendar_identity.sha256
    ):
        _raise("N1 calendar identity drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    return full


def _load_verified_sources(request: FrozenFinancialEventInformationSetRequestV1) -> dict[str, Any]:
    source_path = _resolve_bound_path(request.source_bundle_path)
    n2b_path = _resolve_bound_path(request.n2b_bundle_path)
    n1_path = _resolve_bound_path(request.n1_bundle_path)
    source = _read_source_bundle(source_path)
    n2b = _read_n2b_bundle(n2b_path)
    n1 = _read_n1_bundle(n1_path)
    _validate_bound_sources(source=source, n2b=n2b, n1=n1)
    for reference in request.evidence_refs:
        _verify_ref(reference)
    if (
        source_path.name != request.source_bundle_id
        or source["request"].get("request_sha256") != request.source_request_sha256
        or sha256_file(source_path / "source_readiness_receipt.json") != request.source_receipt_sha256
        or sha256_file(source_path / "event_source_projection.parquet") != request.source_projection_sha256
        or n2b_path.name != request.n2b_bundle_id
        or n2b["request"].request_sha256 != request.n2b_request_sha256
        or n2b["receipt"].receipt_sha256 != request.n2b_receipt_sha256
        or n1_path.name != request.n1_bundle_id
        or n1["request"].request_sha256 != request.n1_request_sha256
        or n1["request"].split_policy_sha256 != request.n1_split_policy_sha256
        or n2b["record"].dataset_identity != request.source_dataset_identity
        or n2b["record"].policy_identity != request.policy_identity
    ):
        _raise("financial-event request/source relation drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    parent = _read_parent_outcomes(n2b_path / "arm_signal_outcomes.parquet", request=request)
    events = _read_parquet(
        source_path / "event_source_projection.parquet",
        "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH",
    )
    if len(events) != request.expected_source_row_count:
        _raise("financial-event projection row count drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    cpcv = _read_json(n1_path / "n1_label_interval_cpcv.json", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    regime_daily = _read_parquet(
        n1_path / "learnability_daily.parquet",
        "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH",
        columns=("decision_as_of_trade_date", "regime"),
    )
    parent_top5 = _read_parquet(n2b_path / "arm_top5_daily.parquet", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    parent_top5 = parent_top5.loc[parent_top5["arm_id"].astype(str) == PARENT_ARM_ID].copy()
    parent_signal = _read_parquet(
        n2b_path / "signal_metrics_daily.parquet", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH"
    )
    parent_signal = parent_signal.loc[parent_signal["arm_id"].astype(str) == PARENT_ARM_ID].copy()
    if len(parent_top5) != 386 or len(parent_signal) != 386:
        _raise("financial-event parent daily row count drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    return {
        "source": source,
        "n2b": n2b,
        "n1": n1,
        "parent_outcomes": parent,
        "events": events,
        "cpcv": cpcv,
        "regime_daily": regime_daily,
        "parent_top5_daily": parent_top5,
        "parent_signal_daily": parent_signal,
    }


def _read_parent_outcomes(
    path: Path,
    *,
    request: FrozenFinancialEventInformationSetRequestV1,
) -> pd.DataFrame:
    frame = _read_parquet(path, "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH", columns=PARENT_COLUMNS)
    frame = frame.loc[frame["arm_id"].astype(str) == PARENT_ARM_ID].copy()
    frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    frame["target_trade_date"] = pd.to_datetime(frame["target_trade_date"]).dt.normalize()
    frame["instrument"] = frame["instrument"].astype(str)
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    known = frame["outcome_known"].fillna(False).astype(bool).to_numpy()
    labels = pd.to_numeric(frame["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    finite = np.isfinite(labels)
    counts = {
        "parent": int(len(frame)),
        "known": int(known.sum()),
        "evaluable": int((known & finite).sum()),
        "nonfinite_known": int((known & ~finite).sum()),
        "unknown": int((~known).sum()),
        "dates": int(frame["decision_as_of_trade_date"].nunique()),
    }
    expected = {
        "parent": request.expected_parent_row_count,
        "known": request.expected_known_row_count,
        "evaluable": request.expected_evaluable_row_count,
        "nonfinite_known": request.expected_nonfinite_known_row_count,
        "unknown": request.expected_unknown_row_count,
        "dates": request.expected_decision_date_count,
    }
    if (
        counts != expected
        or frame.duplicated(["decision_as_of_trade_date", "instrument"]).any()
        or not frame["instrument"].eq(frame["instrument"].str.upper()).all()
        or not np.isfinite(frame["score"].to_numpy(dtype=float)).all()
        or frame["decision_as_of_trade_date"].min() != pd.Timestamp(request.signal_start)
        or frame["decision_as_of_trade_date"].max() != pd.Timestamp(request.signal_end)
        or not (frame["target_trade_date"] > frame["decision_as_of_trade_date"]).all()
    ):
        _raise(
            "financial-event parent outcome identity drift",
            "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH",
            expected=expected,
            actual=counts,
        )
    return frame.reset_index(drop=True)


def _validate_parent_daily_parity(
    *,
    daily: pd.DataFrame,
    parent_top5_daily: pd.DataFrame,
    parent_signal_daily: pd.DataFrame,
) -> None:
    top5 = parent_top5_daily.loc[:, ["decision_as_of_trade_date", "top5_net_excess_bps", "instruments"]].copy()
    signal = parent_signal_daily.loc[:, ["decision_as_of_trade_date", "matured_rank_ic"]].copy()
    for frame in (top5, signal):
        frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    # ALGO-COMPLEXITY-001: these are two 386-row one-to-one identity joins, not
    # market-panel joins; their sole purpose is frozen baseline parity.
    expected = daily.loc[
        :,
        ["decision_as_of_trade_date", "parent_rank_ic", "parent_top5_net_excess_bps", "parent_instruments"],
    ].merge(top5, on="decision_as_of_trade_date", how="left", validate="one_to_one")
    expected = expected.merge(signal, on="decision_as_of_trade_date", how="left", validate="one_to_one")
    rank_left = pd.to_numeric(expected["parent_rank_ic"], errors="coerce").to_numpy(dtype=float)
    rank_right = pd.to_numeric(expected["matured_rank_ic"], errors="coerce").to_numpy(dtype=float)
    top_left = pd.to_numeric(expected["parent_top5_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    top_right = pd.to_numeric(expected["top5_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    comparable = np.isfinite(top_left)
    instrument_match: list[bool] = []
    for current, source in zip(expected["parent_instruments"], expected["instruments"], strict=True):
        try:
            source_set = set(json.loads(str(source)))
        except (TypeError, json.JSONDecodeError):
            source_set = set()
        instrument_match.append(set(str(current).split(",")) == source_set)
    if (
        len(expected) != len(daily)
        or len(expected) != len(top5)
        or len(expected) != len(signal)
        or not np.allclose(rank_left, rank_right, atol=1e-12, rtol=0.0, equal_nan=True)
        or not comparable.any()
        or not np.allclose(top_left[comparable], top_right[comparable], atol=1e-9, rtol=0.0)
        or not all(instrument_match)
    ):
        _raise("financial-event current-parent parity failed", "ADVISORY_N3_EVENT_MVE_BASELINE_PARITY_FAILED")


def _publish_bundle(
    *,
    request: FrozenFinancialEventInformationSetRequestV1,
    features: pd.DataFrame,
    coverage_daily: pd.DataFrame,
    oof_scores: pd.DataFrame,
    fold_diagnostics: pd.DataFrame,
    daily_metrics: pd.DataFrame,
    model_summary: Mapping[str, Any],
    stability: Mapping[str, Any],
    frontier: Mapping[str, Any],
    elapsed_seconds: float,
) -> Path:
    root = _resolve_bound_path(request.output_root) / "financial_event_information_set_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{request.request_id}.", dir=root))
    _write_json(temporary / "request.json", request.model_dump(mode="json"))
    _write_json(
        temporary / "source_identity_receipt.json",
        {
            "schema_version": "advisory_n3_financial_event_mve_source_identity_v1",
            "source_bundle_id": request.source_bundle_id,
            "source_request_sha256": request.source_request_sha256,
            "source_receipt_sha256": request.source_receipt_sha256,
            "source_projection_sha256": request.source_projection_sha256,
            "source_time_quality": EVENT_MVE_SOURCE_QUALITY,
            "trading_calendar_sha256": request.trading_calendar_sha256,
            "target_columns_read_during_feature_build": False,
            "database_reads": 0,
            "network_reads": 0,
            "tushare_reads": 0,
            "qlib_calendar_reads": 1,
            "qlib_feature_reads": 0,
            "sealed_holdout_accessed": False,
        },
    )
    _write_json(
        temporary / "feature_schema.json",
        {
            "schema_version": EVENT_MVE_FEATURE_SCHEMA_VERSION,
            "feature_schema_hash": EVENT_MVE_FEATURE_SCHEMA_HASH,
            "direction_by_type": EVENT_DIRECTION_BY_TYPE,
            "parent_features": list(EVENT_PARENT_FEATURES),
            "disclosure_features": list(EVENT_DISCLOSURE_FEATURES),
            "signed_features": list(EVENT_SIGNED_FEATURES),
            "disclosure_schema_features": list(EVENT_DISCLOSURE_SCHEMA_FEATURES),
            "signed_schema_features": list(EVENT_SIGNED_SCHEMA_FEATURES),
            "source_time_quality": EVENT_MVE_SOURCE_QUALITY,
            "missing_semantics": "ZERO_COUNTS_AND_SUMS_AGE_121_WITH_SEEN_FLAGS_KEEP_ALL_PARENT_KEYS",
            "sealed_holdout_accessed": False,
        },
    )
    features.to_parquet(temporary / "event_feature_panel.parquet", index=False)
    coverage_daily.to_parquet(temporary / "feature_coverage_daily.parquet", index=False)
    oof_scores.to_parquet(temporary / "oof_scores.parquet", index=False)
    fold_diagnostics.to_parquet(temporary / "fold_diagnostics.parquet", index=False)
    daily_metrics.to_parquet(temporary / "model_daily.parquet", index=False)
    _write_json(temporary / "model_summary.json", model_summary)
    _write_json(temporary / "stability_report.json", stability)
    _write_json(temporary / "frontier_receipt.json", frontier)
    payload_bytes = sum(path.stat().st_size for path in temporary.iterdir() if path.is_file())
    if payload_bytes > request.resource_max_temp_bytes:
        _raise(
            "financial-event output exceeds frozen temp limit",
            "ADVISORY_N3_EVENT_MVE_RESOURCE_LIMIT_EXCEEDED",
            temporary_bytes=payload_bytes,
        )
    resource = {
        "schema_version": "advisory_n3_financial_event_resource_report_v1",
        "elapsed_seconds": float(elapsed_seconds),
        "peak_rss_bytes": _peak_rss_bytes(),
        "temporary_bytes": int(payload_bytes),
        "resource_max_rss_bytes": request.resource_max_rss_bytes,
        "resource_max_temp_bytes": request.resource_max_temp_bytes,
        "wall_time_limit_seconds": None,
        "wall_time_is_telemetry_only": True,
        "database_reads": 0,
        "database_writes": 0,
        "network_reads": 0,
        "tushare_reads": 0,
        "qlib_calendar_reads": 1,
        "qlib_feature_reads": 0,
        "sealed_holdout_accessed": False,
    }
    if resource["peak_rss_bytes"] > request.resource_max_rss_bytes:
        _raise(
            "financial-event publish exceeds frozen RSS limit",
            "ADVISORY_N3_EVENT_MVE_RESOURCE_LIMIT_EXCEEDED",
            peak_rss_bytes=resource["peak_rss_bytes"],
        )
    _write_json(temporary / "resource_report.json", resource)
    result_descriptors = {
        name: descriptor for name, descriptor in _file_descriptors(temporary).items() if name in RESULT_IDENTITY_MEMBERS
    }
    result_files_sha256 = canonical_json_sha256(result_descriptors)
    selected = model_summary.get("selected_trial_id")
    evidence_class = str(model_summary["evidence_class"])
    receipt = build_financial_event_receipt(
        request_sha256=request.request_sha256,
        selected_trial_count=1 if selected else 0,
        selected_trial_id=selected,
        eligible_trial_ids=(("EVENT_SIGNED_CONTENT_V1",) if selected else ()),
        evidence_class=evidence_class,
        next_task=(
            "N3_FINANCIAL_EVENT_VINTAGE_SOURCE_DECISION" if selected else "N3_SCORE_HMM_ADMISSION_MVE_IMPLEMENTATION"
        ),
        result_files_sha256=result_files_sha256,
        resource_report_sha256=sha256_file(temporary / "resource_report.json"),
    )
    _write_json(temporary / "learnability_receipt.json", receipt.model_dump(mode="json"))
    bundle_id = canonical_json_sha256(
        {
            "schema_version": MVE_BUNDLE_SCHEMA,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
        }
    )
    destination = root / bundle_id
    records = _build_registry_records(
        request=request,
        receipt=receipt,
        receipt_path=temporary / "learnability_receipt.json",
        receipt_artifact_uri=(destination / "learnability_receipt.json").as_posix(),
    )
    _write_json(temporary / "registry_records.json", [item.model_dump(mode="json") for item in records])
    descriptors = _file_descriptors(temporary)
    if set(descriptors) != MVE_BUNDLE_MEMBERS:
        _raise(
            "financial-event bundle member roster drift",
            "ADVISORY_N3_EVENT_MVE_BUNDLE_INVALID",
            members=sorted(descriptors),
        )
    final_bytes = sum(path.stat().st_size for path in temporary.iterdir() if path.is_file())
    if final_bytes > request.resource_max_temp_bytes:
        _raise(
            "financial-event final bundle exceeds frozen temp limit",
            "ADVISORY_N3_EVENT_MVE_RESOURCE_LIMIT_EXCEEDED",
            temporary_bytes=final_bytes,
        )
    manifest = {
        "schema_version": MVE_BUNDLE_SCHEMA,
        "bundle_id": bundle_id,
        "request_sha256": request.request_sha256,
        "receipt_sha256": receipt.receipt_sha256,
        "source_bundle_id": request.source_bundle_id,
        "n2b_bundle_id": request.n2b_bundle_id,
        "n1_bundle_id": request.n1_bundle_id,
        "objective_contract": ObjectiveContract.ALPHA_RANKING.value,
        "study_type": ResearchStudyType.LEARNABILITY_AUDIT.value,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "result_class": ResearchResultClass.EXPLORATORY.value,
        "evidence_class": evidence_class,
        "planned_trial_count": 3,
        "generated_trial_count": 3,
        "evaluated_trial_count": 3,
        "selectable_trial_count": 1,
        "selected_trial_count": receipt.selected_trial_count,
        "cumulative_candidate_index": request.cumulative_candidate_index,
        "source_time_quality": EVENT_MVE_SOURCE_QUALITY,
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
        _read_mve_bundle(destination)
        _raise(
            "financial-event bundle destination appeared concurrently",
            "ADVISORY_N3_EVENT_MVE_BUNDLE_INVALID",
            bundle_id=bundle_id,
        )
    temporary.replace(destination)
    _read_mve_bundle(destination)
    return destination


def _build_registry_records(
    *,
    request: FrozenFinancialEventInformationSetRequestV1,
    receipt: FinancialEventInformationSetReceiptV1,
    receipt_path: Path,
    receipt_artifact_uri: str,
) -> tuple[AdvisoryResearchTrialRecordV1, ...]:
    evidence = evidence_reference_for_file(
        receipt_path, role="n3_financial_event_information_set_learnability_receipt"
    ).model_copy(update={"artifact_uri": receipt_artifact_uri})
    unique_variables = {
        "EVENT_PARENT_COMPARATOR_V1": "PARENT_RANK_RIDGE_COMPARATOR",
        "EVENT_DISCLOSURE_CONTROL_V1": "PARENT_PLUS_FINANCIAL_EVENT_DISCLOSURE_EXISTENCE",
        "EVENT_SIGNED_CONTENT_V1": "PARENT_PLUS_FROZEN_SIGNED_FINANCIAL_EVENT_CONTENT",
    }
    records: list[AdvisoryResearchTrialRecordV1] = []
    for trial in request.model_trials:
        selected = int(trial.trial_id == receipt.selected_trial_id)
        records.append(
            build_trial_record(
                experiment_id=f"{EVENT_MVE_EXPERIMENT_ID}:{trial.trial_id}",
                attempt_id=f"{request.request_id}:{trial.trial_id}",
                research_stage="N3_FINANCIAL_EVENT_INFORMATION_SET_MVE",
                study_type=ResearchStudyType.LEARNABILITY_AUDIT,
                hypothesis_family_id=EVENT_MVE_HYPOTHESIS_FAMILY_ID,
                parent_lineage=(
                    "ADVISORY-N1-TIER1-LEARNABILITY",
                    "ADVISORY-N2B-INDEPENDENT-PACKAGE-ALPHA-AUDIT-V2",
                    "ADVISORY-N3-FINANCIAL-EVENT-SOURCE-READINESS-V1",
                ),
                unique_variable=unique_variables[trial.trial_id],
                objective_contract=ObjectiveContract.ALPHA_RANKING,
                dataset_identity=request.dataset_identity,
                schema_identity=canonical_json_sha256(
                    {
                        "feature_schema_hash": request.feature_schema_hash,
                        "trial_id": trial.trial_id,
                        "feature_columns": list(trial.feature_columns),
                    }
                ),
                policy_identity=request.policy_identity,
                planned_trial_count=1,
                generated_trial_count=1,
                evaluated_trial_count=1,
                selected_trial_count=selected,
                consumed_windows=(
                    ConsumedWindowV1(
                        window_id="P0_C_DEVELOPMENT_CONSUMED",
                        dataset_identity=request.dataset_identity,
                        start_date=request.signal_start,
                        end_date=request.signal_end,
                    ),
                ),
                result_class=ResearchResultClass.EXPLORATORY,
                decision_use=DecisionUse.NAVIGATION_ONLY,
                evidence_refs=(evidence,),
                recorded_at=datetime.now(timezone.utc),
            )
        )
    return tuple(records)


def _read_mve_bundle(path: Path) -> dict[str, Any]:
    manifest = _read_json(path / "manifest.json", "ADVISORY_N3_EVENT_MVE_BUNDLE_INVALID")
    descriptors = manifest.get("files")
    _verify_bundle_member_descriptors(
        path=path,
        descriptors=descriptors,
        expected_members=MVE_BUNDLE_MEMBERS,
        reason_code="ADVISORY_N3_EVENT_MVE_BUNDLE_INVALID",
    )
    try:
        request = FrozenFinancialEventInformationSetRequestV1.model_validate_json(
            (path / "request.json").read_text(encoding="utf-8")
        )
        receipt = FinancialEventInformationSetReceiptV1.model_validate_json(
            (path / "learnability_receipt.json").read_text(encoding="utf-8")
        )
        raw_records = json.loads((path / "registry_records.json").read_text(encoding="utf-8"))
        records = tuple(AdvisoryResearchTrialRecordV1.model_validate(item) for item in raw_records)
    except Exception as exc:
        _raise(
            "financial-event bundle contract member invalid",
            "ADVISORY_N3_EVENT_MVE_BUNDLE_INVALID",
            error_type=type(exc).__name__,
        )
    assert isinstance(descriptors, dict)
    expected_bundle_id = canonical_json_sha256(
        {
            "schema_version": MVE_BUNDLE_SCHEMA,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
        }
    )
    result_descriptors = {name: descriptors[name] for name in sorted(RESULT_IDENTITY_MEMBERS)}
    frontier = _read_json(path / "frontier_receipt.json", "ADVISORY_N3_EVENT_MVE_BUNDLE_INVALID")
    frontier_functional = {key: value for key, value in frontier.items() if key != "frontier_sha256"}
    stability = _read_json(path / "stability_report.json", "ADVISORY_N3_EVENT_MVE_BUNDLE_INVALID")
    stability_functional = {key: value for key, value in stability.items() if key != "stability_sha256"}
    summary = _read_json(path / "model_summary.json", "ADVISORY_N3_EVENT_MVE_BUNDLE_INVALID")
    resource = _read_json(path / "resource_report.json", "ADVISORY_N3_EVENT_MVE_BUNDLE_INVALID")
    receipt_descriptor = descriptors["learnability_receipt.json"]
    records_valid = (
        len(records) == 3
        and {item.unique_variable for item in records}
        == {
            "PARENT_RANK_RIDGE_COMPARATOR",
            "PARENT_PLUS_FINANCIAL_EVENT_DISCLOSURE_EXISTENCE",
            "PARENT_PLUS_FROZEN_SIGNED_FINANCIAL_EVENT_CONTENT",
        }
        and all(
            item.hypothesis_family_id == EVENT_MVE_HYPOTHESIS_FAMILY_ID
            and item.study_type == ResearchStudyType.LEARNABILITY_AUDIT
            and item.decision_use == DecisionUse.NAVIGATION_ONLY
            and item.result_class == ResearchResultClass.EXPLORATORY
            and item.planned_trial_count == 1
            and item.generated_trial_count == 1
            and item.evaluated_trial_count == 1
            and len(item.evidence_refs) == 1
            and item.evidence_refs[0].sha256 == receipt_descriptor["sha256"]
            and item.evidence_refs[0].size_bytes == receipt_descriptor["size_bytes"]
            for item in records
        )
        and sum(item.selected_trial_count for item in records) == receipt.selected_trial_count
        and {item.unique_variable: item.selected_trial_count for item in records}.get(
            "PARENT_PLUS_FROZEN_SIGNED_FINANCIAL_EVENT_CONTENT"
        )
        == receipt.selected_trial_count
    )
    resource_numbers_valid = (
        isinstance(resource.get("peak_rss_bytes"), int)
        and isinstance(resource.get("temporary_bytes"), int)
        and 0 <= resource["peak_rss_bytes"] <= request.resource_max_rss_bytes
        and 0 <= resource["temporary_bytes"] <= request.resource_max_temp_bytes
    )
    invalid = (
        manifest.get("schema_version") != MVE_BUNDLE_SCHEMA
        or path.name != expected_bundle_id
        or manifest.get("bundle_id") != expected_bundle_id
        or manifest.get("request_sha256") != request.request_sha256
        or manifest.get("receipt_sha256") != receipt.receipt_sha256
        or receipt.request_sha256 != request.request_sha256
        or receipt.result_files_sha256 != canonical_json_sha256(result_descriptors)
        or receipt.resource_report_sha256 != descriptors["resource_report.json"]["sha256"]
        or not records_valid
        or frontier.get("frontier_sha256") != canonical_json_sha256(frontier_functional)
        or frontier.get("selected_trial_id") != receipt.selected_trial_id
        or tuple(frontier.get("eligible_trial_ids", ())) != receipt.eligible_trial_ids
        or frontier.get("evidence_class") != receipt.evidence_class
        or stability.get("stability_sha256") != canonical_json_sha256(stability_functional)
        or summary.get("selected_trial_id") != receipt.selected_trial_id
        or summary.get("evidence_class") != receipt.evidence_class
        or manifest.get("source_bundle_id") != request.source_bundle_id
        or manifest.get("n2b_bundle_id") != request.n2b_bundle_id
        or manifest.get("n1_bundle_id") != request.n1_bundle_id
        or manifest.get("planned_trial_count") != 3
        or manifest.get("generated_trial_count") != 3
        or manifest.get("evaluated_trial_count") != 3
        or manifest.get("selectable_trial_count") != 1
        or manifest.get("selected_trial_count") != receipt.selected_trial_count
        or manifest.get("cumulative_candidate_index") != EVENT_MVE_CUMULATIVE_CANDIDATE_INDEX
        or manifest.get("source_time_quality") != EVENT_MVE_SOURCE_QUALITY
        or manifest.get("sealed_holdout_accessed") is not False
        or manifest.get("deployable") is not False
        or manifest.get("runtime_eligible") is not False
        or resource.get("wall_time_limit_seconds") is not None
        or resource.get("wall_time_is_telemetry_only") is not True
        or not resource_numbers_valid
        or resource.get("database_reads") != 0
        or resource.get("database_writes") != 0
        or resource.get("network_reads") != 0
        or resource.get("tushare_reads") != 0
        or resource.get("qlib_calendar_reads") != 1
        or resource.get("qlib_feature_reads") != 0
        or resource.get("sealed_holdout_accessed") is not False
    )
    if invalid:
        _raise("financial-event bundle relational identity invalid", "ADVISORY_N3_EVENT_MVE_BUNDLE_INVALID")
    return {"manifest": manifest, "request": request, "receipt": receipt, "records": records}


def _find_existing_bundle(request: FrozenFinancialEventInformationSetRequestV1) -> Path | None:
    root = _resolve_bound_path(request.output_root) / "financial_event_information_set_bundles"
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
        _raise("one event request maps to multiple bundles", "ADVISORY_N3_EVENT_MVE_BUNDLE_INVALID")
    if matches:
        _read_mve_bundle(matches[0])
        return matches[0]
    return None


def _deliver_bundle(
    *,
    request: FrozenFinancialEventInformationSetRequestV1,
    bundle_path: Path,
) -> dict[str, Any]:
    loaded = _read_mve_bundle(bundle_path)
    registry = AdvisoryResearchTrialRegistryV1(_resolve_bound_path(request.registry_path)).append_batch(
        loaded["records"]
    )
    route = _write_route_page(
        path=_resolve_bound_path(request.route_path),
        request=request,
        receipt=loaded["receipt"],
        bundle_id=loaded["manifest"]["bundle_id"],
        registry_sha256=str(registry["registry_sha256"]),
    )
    return {"registry": registry, "route": route}


def _write_route_page(
    *,
    path: Path,
    request: FrozenFinancialEventInformationSetRequestV1,
    receipt: FinancialEventInformationSetReceiptV1,
    bundle_id: str,
    registry_sha256: str,
) -> dict[str, Any]:
    selected = receipt.selected_trial_id or "NONE"
    # ALGO-COMPLEXITY-001: the route page has a fixed, bounded line roster;
    # this join is O(1) formatting and never iterates over market observations.
    content = "\n".join(
        (
            "# Advisory 当前研究路线",
            "",
            "- active_main_line: N3_FINANCIAL_EVENT_INFORMATION_SET_MVE",
            "- active_auxiliary_line: NONE",
            f"- next_task: {receipt.next_task}",
            f"- exploratory_candidate: {selected}",
            f"- source_bundle_id: {request.source_bundle_id}",
            f"- financial_event_information_set_bundle_id: {bundle_id}",
            f"- cumulative_candidate_index: {request.cumulative_candidate_index}",
            f"- trial_registry_sha256: {registry_sha256}",
            "- objective_contract: ALPHA_RANKING",
            "- study_type: LEARNABILITY_AUDIT",
            "- decision_use: NAVIGATION_ONLY",
            f"- evidence_class: {receipt.evidence_class}",
            f"- source_time_quality: {EVENT_MVE_SOURCE_QUALITY}",
            "- sealed_holdout_accessed: false",
            "- deployable/runtime/model/factor/strategy_package/position_weight: false/false/false/false/false/false",
            "",
            "该页面只记录开发窗口、非vintage财务事件learnability导航。selected=1只允许进入真实vintage source决策；",
            "selected=0或支持不足只关闭本次固定方向、窗口、特征和Ridge frontier，并转入score/HMM辅助准入实现。",
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


def _verify_environment(request: FrozenFinancialEventInformationSetRequestV1) -> None:
    _require_formal_environment()
    repository = _resolve_bound_path(request.repository_root)
    if _cross_os_git_commit(repository) != request.repository_commit:
        _raise("event repository commit drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")
    dirty = _cross_os_git_dirty_paths(repository)
    if dirty:
        _raise(
            "event repository became dirty",
            "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH",
            dirty_paths=dirty[:20],
        )
    for reference in request.evidence_refs:
        _verify_ref(reference)
    source = _read_source_bundle(_resolve_bound_path(request.source_bundle_path))
    n2b = _read_n2b_bundle(_resolve_bound_path(request.n2b_bundle_path))
    n1 = _read_n1_bundle(_resolve_bound_path(request.n1_bundle_path))
    _validate_bound_sources(source=source, n2b=n2b, n1=n1)
    actual_calendar = _load_and_verify_calendar(n1["request"])
    actual_dates = tuple(item.date() for item in actual_calendar)
    if actual_dates != request.trading_calendar:
        _raise("event frozen calendar content drift", "ADVISORY_N3_EVENT_MVE_SOURCE_IDENTITY_MISMATCH")


def _run_response(
    request: FrozenFinancialEventInformationSetRequestV1,
    bundle: Path,
    delivery: Mapping[str, Any],
    *,
    exact_retry: bool,
) -> dict[str, Any]:
    return {
        **inspect_financial_event_information_set_bundle(bundle),
        "request_id": request.request_id,
        "bundle_path": bundle.as_posix(),
        "source_bundle_path": request.source_bundle_path,
        "exact_retry": exact_retry,
        "registry": dict(delivery["registry"]),
        "route": dict(delivery["route"]),
    }


def _check_resource_limits(request: FrozenFinancialEventInformationSetRequestV1, stage: str) -> None:
    rss = _peak_rss_bytes()
    if rss > request.resource_max_rss_bytes:
        _raise(
            "financial-event resident memory exceeds frozen limit",
            "ADVISORY_N3_EVENT_MVE_RESOURCE_LIMIT_EXCEEDED",
            stage=stage,
            peak_rss_bytes=rss,
        )


def _verify_bundle_member_descriptors(
    *,
    path: Path,
    descriptors: Any,
    expected_members: frozenset[str],
    reason_code: str,
) -> None:
    if not isinstance(descriptors, dict) or set(descriptors) != expected_members:
        _raise("bundle descriptor roster invalid", reason_code)
    actual_files = {item.name for item in path.iterdir() if item.is_file()} - {"manifest.json"}
    if actual_files != expected_members:
        _raise("bundle physical member roster invalid", reason_code, members=sorted(actual_files))
    for name, descriptor in descriptors.items():
        if not isinstance(descriptor, dict):
            _raise("bundle member descriptor invalid", reason_code, member=name)
        member = path / name
        rows = _parquet_row_count(member) if member.suffix == ".parquet" and member.is_file() else None
        if (
            not member.is_file()
            or sha256_file(member) != descriptor.get("sha256")
            or member.stat().st_size != descriptor.get("size_bytes")
            or (rows is not None and rows != descriptor.get("row_count"))
        ):
            _raise("bundle member identity drift", reason_code, member=name)


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
            "ADVISORY_N3_EVENT_MVE_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    if len(commit) != 40 or any(value not in "0123456789abcdef" for value in commit):
        _raise("origin/main commit invalid", "ADVISORY_N3_EVENT_MVE_REQUEST_INVALID")
    return commit


def _write_immutable_request(
    path: Path,
    request: FrozenFinancialEventInformationSetRequestV1,
) -> None:
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
            existing = FrozenFinancialEventInformationSetRequestV1.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception as exc:
            _raise(
                "existing event request invalid",
                "ADVISORY_N3_EVENT_MVE_REQUEST_INVALID",
                error_type=type(exc).__name__,
            )
        if existing.request_sha256 != request.request_sha256:
            _raise("event request path contains different identity", "ADVISORY_N3_EVENT_MVE_REQUEST_INVALID")
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
    reason_code: str,
    *,
    columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    try:
        return pd.read_parquet(path, columns=list(columns) if columns is not None else None)
    except Exception as exc:
        _raise(
            "financial-event parquet cannot be read",
            reason_code,
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )


def _read_json(path: Path, reason_code: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _raise(
            "financial-event JSON cannot be read",
            reason_code,
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )
    if not isinstance(payload, dict):
        _raise("financial-event JSON root is not object", reason_code, path=path.as_posix())
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


def _require_formal_environment() -> None:
    if (
        os.name == "nt"
        or os.environ.get("CONDA_DEFAULT_ENV") != "rdagent-gpu"
        or os.environ.get("AISTOCK_ADVISORY_N3_EVENT_FORMAL_RUN") != "1"
    ):
        _raise(
            "financial-event formal prepare/run requires WSL rdagent-gpu and explicit flag",
            "ADVISORY_N3_EVENT_MVE_REQUEST_INVALID",
            os_name=os.name,
            conda_default_env=os.environ.get("CONDA_DEFAULT_ENV"),
        )


def _raise(message: str, reason_code: str, **context: Any) -> NoReturn:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "attach_event_outcomes",
    "build_event_feature_panel",
    "evaluate_event_models",
    "inspect_financial_event_information_set_bundle",
    "prepare_financial_event_information_set_request",
    "run_event_crossfit",
    "run_financial_event_information_set_mve",
    "validate_event_feature_support",
]
