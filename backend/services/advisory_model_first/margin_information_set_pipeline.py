from __future__ import annotations

import hashlib
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
from backend.services.advisory_model_first.independent_package_alpha_audit_pipeline import (
    _read_bundle as _read_n2b_bundle,
)
from backend.services.advisory_model_first.margin_information_set_contracts import (
    MARGIN_MVE_CALENDAR_SHA256,
    MARGIN_MVE_CALENDAR_SIZE,
    MARGIN_MVE_CURRENT_MARGIN_SHA256,
    MARGIN_MVE_CURRENT_MARGIN_SIZE,
    MARGIN_MVE_CUMULATIVE_CANDIDATE_INDEX,
    MARGIN_MVE_EXPANDED_FEATURES,
    MARGIN_MVE_EXPERIMENT_ID,
    MARGIN_MVE_FEATURE_SCHEMA_HASH,
    MARGIN_MVE_FEATURE_SCHEMA_VERSION,
    MARGIN_MVE_HYPOTHESIS_FAMILY_ID,
    MARGIN_MVE_MEMBERSHIP_FEATURES,
    MARGIN_MVE_MIN_DYNAMICS_FINITE_FRACTION,
    MARGIN_MVE_MIN_DYNAMICS_FINITE_PER_DAY,
    MARGIN_MVE_MIN_RAW_FIELD_FINITE_FRACTION,
    MARGIN_MVE_MIN_SOURCE_FRACTION,
    MARGIN_MVE_MIN_TOP20_SOURCE_FRACTION,
    MARGIN_MVE_MIN_TOP20_SUPPORTED_COUNT,
    MARGIN_MVE_MIN_TOP20_SUPPORTED_DAYS,
    MARGIN_MVE_MIN_TOP50_SOURCE_FRACTION,
    MARGIN_MVE_PARENT_FEATURES,
    MARGIN_MVE_RANKED_DYNAMICS_FEATURES,
    MARGIN_MVE_RAW_DYNAMICS_FEATURES,
    MARGIN_MVE_SECONDARY_MARGIN_SHA256,
    MARGIN_MVE_SECONDARY_MARGIN_SIZE,
    MARGIN_MVE_SOURCE_FIELDS,
    MARGIN_MVE_SOURCE_QUALITY,
    FrozenMarginInformationSetRequestV1,
    FrozenMarginSourceRequestV1,
    MarginInformationSetReceiptV1,
    MarginSourceIdentityReceiptV1,
    build_margin_information_set_receipt,
    build_margin_information_set_request,
    build_margin_source_receipt,
    build_margin_source_request,
)
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.qe_alpha_generator_pipeline import (
    _read_result_bundle as _read_generator_bundle,
)
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


PARENT_ARM_ID = "CURRENT_IC_PARENT"
PARENT_KEY_COLUMNS = ("decision_as_of_trade_date", "instrument", "score")
PARENT_OUTCOME_COLUMNS = (
    "arm_id",
    "decision_as_of_trade_date",
    "instrument",
    "score",
    "economic_net_excess_bps",
    "outcome_known",
)
MODEL_SCORE_COLUMNS = {
    "N3_MARGIN_PARENT_RIDGE_COMPARATOR_V1": "parent_comparator_oof_score",
    "N3_MARGIN_MEMBERSHIP_CONTROL_V1": "membership_oof_score",
    "N3_MARGIN_DYNAMICS_EXPANDED_V1": "candidate_oof_score",
}
SOURCE_BUNDLE_SCHEMA = "advisory_n3_margin_source_bundle_v1"
SOURCE_IDENTITY_MEMBERS = frozenset(
    {
        "candidate_state_snapshot.json",
        "margin_source_projection.parquet",
        "source_coverage_daily.parquet",
        "cross_snapshot_parity.json",
    }
)
SOURCE_BUNDLE_MEMBERS = SOURCE_IDENTITY_MEMBERS | {
    "source_request.json",
    "source_identity_receipt.json",
}
MVE_BUNDLE_SCHEMA = "advisory_n3_margin_information_set_bundle_v1"
RESULT_IDENTITY_MEMBERS = frozenset(
    {
        "source_reference.json",
        "feature_schema.json",
        "margin_feature_panel.parquet",
        "oof_score_panel.parquet",
        "fold_diagnostics.parquet",
        "daily_metrics.parquet",
        "model_summary.json",
        "stability_report.json",
        "frontier_receipt.json",
    }
)
MVE_BUNDLE_MEMBERS = RESULT_IDENTITY_MEMBERS | {
    "request.json",
    "resource_report.json",
    "learnability_receipt.json",
    "registry_record.json",
}


def prepare_margin_information_set_request(
    *,
    generator_bundle_path: str | Path,
    n2b_bundle_path: str | Path,
    n1_bundle_path: str | Path,
    candidate_root: str | Path,
    secondary_margin_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
) -> FrozenMarginInformationSetRequestV1:
    """Freeze the source projection and one development-only margin MVE request."""

    _require_formal_environment()
    started = time.monotonic()
    generator_path = _resolve_bound_path(generator_bundle_path)
    n2b_path = _resolve_bound_path(n2b_bundle_path)
    n1_path = _resolve_bound_path(n1_bundle_path)
    candidate = _resolve_bound_path(candidate_root)
    secondary = _resolve_bound_path(secondary_margin_path)
    repository = _resolve_bound_path(repository_root)
    output = _resolve_bound_path(output_root)
    generator = _read_generator_bundle(generator_path)
    n2b = _read_n2b_bundle(n2b_path)
    n1 = _read_n1_bundle(n1_path)
    _validate_bound_sources(generator=generator, n2b=n2b, n1=n1)
    dirty = _cross_os_git_dirty_paths(repository)
    if dirty:
        _raise(
            "margin MVE request requires a clean repository",
            "ADVISORY_N3_MARGIN_MVE_REQUEST_INVALID",
            dirty_paths=dirty[:20],
        )
    commit = _cross_os_git_commit(repository)
    origin_main = _git_origin_main_commit(repository)
    if commit != origin_main:
        _raise(
            "margin MVE request requires HEAD to equal origin/main",
            "ADVISORY_N3_MARGIN_MVE_REQUEST_INVALID",
            repository_commit=commit,
            origin_main_commit=origin_main,
        )
    source_bundle = _prepare_margin_source_bundle(
        n2b_path=n2b_path,
        n2b=n2b,
        candidate_root=candidate,
        secondary_margin_path=secondary,
        repository_root=repository,
        repository_commit=commit,
        output_root=output,
        started=started,
    )
    source = _read_margin_source_bundle(source_bundle)
    evidence_refs = tuple(
        evidence_reference_for_file(path, role=role)
        for role, path in (
            ("n3_margin_generator_manifest", generator_path / "manifest.json"),
            ("n3_margin_generator_receipt", generator_path / "receipt.json"),
            ("n3_margin_n2b_manifest", n2b_path / "manifest.json"),
            ("n3_margin_n2b_request", n2b_path / "request.json"),
            ("n3_margin_n2b_outcomes", n2b_path / "arm_signal_outcomes.parquet"),
            ("n3_margin_n1_manifest", n1_path / "manifest.json"),
            ("n3_margin_n1_cpcv", n1_path / "n1_label_interval_cpcv.json"),
            ("n3_margin_n1_regime_daily", n1_path / "learnability_daily.parquet"),
            ("n3_margin_source_manifest", source_bundle / "manifest.json"),
            ("n3_margin_source_receipt", source_bundle / "source_identity_receipt.json"),
            ("n3_margin_source_projection", source_bundle / "margin_source_projection.parquet"),
            ("n3_margin_source_coverage", source_bundle / "source_coverage_daily.parquet"),
            ("n3_margin_cross_snapshot_parity", source_bundle / "cross_snapshot_parity.json"),
            ("n3_margin_candidate_state_snapshot", source_bundle / "candidate_state_snapshot.json"),
        )
    )
    source_dataset_identity = n2b["record"].dataset_identity
    route_dataset_identity = generator["registry_record"].dataset_identity
    split_identity = n1["request"].split_policy_sha256
    policy_identity = n2b["record"].policy_identity
    dataset_identity = canonical_json_sha256(
        {
            "source_dataset_identity": source_dataset_identity,
            "route_dataset_identity": route_dataset_identity,
            "n1_split_policy_sha256": split_identity,
            "source_identity_sha256": source["receipt"].source_identity_sha256,
            "feature_schema_hash": MARGIN_MVE_FEATURE_SCHEMA_HASH,
            "policy_identity": policy_identity,
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }
    )
    request = build_margin_information_set_request(
        evidence_refs=evidence_refs,
        generator_bundle_path=generator_path.as_posix(),
        generator_bundle_id=generator_path.name,
        generator_request_sha256=generator["request"].request_sha256,
        generator_receipt_sha256=generator["receipt"].receipt_sha256,
        n2b_bundle_path=n2b_path.as_posix(),
        n2b_bundle_id=n2b_path.name,
        n2b_request_sha256=n2b["request"].request_sha256,
        n2b_receipt_sha256=n2b["receipt"].receipt_sha256,
        n1_bundle_path=n1_path.as_posix(),
        n1_bundle_id=n1_path.name,
        n1_request_sha256=n1["request"].request_sha256,
        n1_split_policy_sha256=split_identity,
        source_bundle_path=source_bundle.as_posix(),
        source_bundle_id=source_bundle.name,
        source_request_sha256=source["request"].source_request_sha256,
        source_receipt_sha256=source["receipt"].source_receipt_sha256,
        source_identity_sha256=source["receipt"].source_identity_sha256,
        source_dataset_identity=source_dataset_identity,
        route_dataset_identity=route_dataset_identity,
        dataset_identity=dataset_identity,
        policy_identity=policy_identity,
        registry_path=_resolve_bound_path(generator["request"].registry_path).as_posix(),
        route_path=_resolve_bound_path(generator["request"].route_path).as_posix(),
        repository_root=repository.as_posix(),
        repository_commit=commit,
        output_root=output.as_posix(),
    )
    _write_immutable_request(_resolve_bound_path(output_path), request)
    return request


def _prepare_margin_source_bundle(
    *,
    n2b_path: Path,
    n2b: Mapping[str, Any],
    candidate_root: Path,
    secondary_margin_path: Path,
    repository_root: Path,
    repository_commit: str,
    output_root: Path,
    started: float,
) -> Path:
    state_path = candidate_root / "direct_monthly_state.json"
    current_margin_path = candidate_root / "components" / "factor_h5_static_candidate_v2" / "margin_detail.h5"
    calendar_path = candidate_root / "components" / "daily_bin_candidate" / "calendars" / "day.txt"
    state_before = _read_stable_candidate_state(state_path, candidate_root)
    _verify_file_identity(
        current_margin_path,
        expected_size=MARGIN_MVE_CURRENT_MARGIN_SIZE,
        expected_sha256=MARGIN_MVE_CURRENT_MARGIN_SHA256,
    )
    _verify_file_identity(
        secondary_margin_path,
        expected_size=MARGIN_MVE_SECONDARY_MARGIN_SIZE,
        expected_sha256=MARGIN_MVE_SECONDARY_MARGIN_SHA256,
    )
    _verify_file_identity(
        calendar_path,
        expected_size=MARGIN_MVE_CALENDAR_SIZE,
        expected_sha256=MARGIN_MVE_CALENDAR_SHA256,
    )
    outcomes_path = n2b_path / "arm_signal_outcomes.parquet"
    source_request = build_margin_source_request(
        repository_root=repository_root.as_posix(),
        repository_commit=repository_commit,
        n2b_bundle_path=n2b_path.as_posix(),
        n2b_bundle_id=n2b_path.name,
        n2b_outcomes_sha256=sha256_file(outcomes_path),
        candidate_root=candidate_root.as_posix(),
        candidate_state_path=state_path.as_posix(),
        candidate_state_sha256=state_before["sha256"],
        candidate_state_size=state_before["size_bytes"],
        candidate_state_updated_at=state_before["payload"]["updated_at"],
        current_margin_path=current_margin_path.as_posix(),
        current_margin_sha256=MARGIN_MVE_CURRENT_MARGIN_SHA256,
        current_margin_size=MARGIN_MVE_CURRENT_MARGIN_SIZE,
        secondary_margin_path=secondary_margin_path.as_posix(),
        secondary_margin_sha256=MARGIN_MVE_SECONDARY_MARGIN_SHA256,
        secondary_margin_size=MARGIN_MVE_SECONDARY_MARGIN_SIZE,
        calendar_path=calendar_path.as_posix(),
        calendar_sha256=MARGIN_MVE_CALENDAR_SHA256,
        calendar_size=MARGIN_MVE_CALENDAR_SIZE,
    )
    existing = _find_existing_source_bundle(output_root, source_request.source_request_sha256)
    if existing is not None:
        state_after = _read_stable_candidate_state(state_path, candidate_root)
        _assert_state_unchanged(state_before, state_after)
        return existing
    parent = _read_parent_panel(outcomes_path, include_outcomes=False)
    calendar = _read_daily_calendar(calendar_path)
    date_map = _build_source_date_map(parent, calendar)
    required_dates = set(
        pd.to_datetime(
            date_map[["source_date_d", "source_date_d1", "source_date_d5"]].to_numpy().reshape(-1)
        ).normalize()
    )
    instruments = set(parent["instrument"].astype(str))
    current, current_rows, current_invalid = _read_margin_h5(
        current_margin_path,
        expected_format="table",
        required_dates=required_dates,
        instruments=instruments,
        calendar=calendar,
        chunk_rows=source_request.chunk_rows,
    )
    secondary, secondary_rows, secondary_invalid = _read_margin_h5(
        secondary_margin_path,
        expected_format="fixed",
        required_dates=required_dates,
        instruments=instruments,
        calendar=calendar,
        chunk_rows=source_request.chunk_rows,
    )
    projection, parity = build_margin_source_projection(
        current=current,
        secondary=secondary,
        source_window_start=pd.Timestamp(source_request.source_start),
        source_window_end=pd.Timestamp(source_request.source_end),
    )
    coverage, raw_quality = build_margin_source_coverage(
        parent=parent,
        date_map=date_map,
        projection=projection,
    )
    _validate_source_support(coverage=coverage, raw_quality=raw_quality)
    state_after = _read_stable_candidate_state(state_path, candidate_root)
    _assert_state_unchanged(state_before, state_after)
    _verify_file_identity(
        current_margin_path,
        expected_size=MARGIN_MVE_CURRENT_MARGIN_SIZE,
        expected_sha256=MARGIN_MVE_CURRENT_MARGIN_SHA256,
    )
    _verify_file_identity(
        secondary_margin_path,
        expected_size=MARGIN_MVE_SECONDARY_MARGIN_SIZE,
        expected_sha256=MARGIN_MVE_SECONDARY_MARGIN_SHA256,
    )
    _verify_file_identity(
        calendar_path,
        expected_size=MARGIN_MVE_CALENDAR_SIZE,
        expected_sha256=MARGIN_MVE_CALENDAR_SHA256,
    )
    return _publish_margin_source_bundle(
        output_root=output_root,
        request=source_request,
        candidate_state_bytes=state_before["bytes"],
        candidate_state_after_sha256=state_after["sha256"],
        projection=projection,
        coverage=coverage,
        parity={
            **parity,
            "current_invalid_value_count": current_invalid,
            "secondary_invalid_value_count": secondary_invalid,
        },
        raw_quality=raw_quality,
        current_rows_read=current_rows,
        secondary_rows_read=secondary_rows,
        source_bytes_read=(
            state_path.stat().st_size
            + current_margin_path.stat().st_size
            + secondary_margin_path.stat().st_size
            + calendar_path.stat().st_size
        ),
        elapsed_seconds=time.monotonic() - started,
    )


def build_margin_source_projection(
    *,
    current: pd.DataFrame,
    secondary: pd.DataFrame,
    source_window_start: pd.Timestamp,
    source_window_end: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Keep only exact common-key rows whose eight float32 values agree."""

    left = _normalize_margin_projection_frame(current, label="current")
    right = _normalize_margin_projection_frame(secondary, label="secondary")
    left_index = left.set_index(["datetime", "instrument"]).sort_index()
    right_index = right.set_index(["datetime", "instrument"]).sort_index()
    common = left_index.index.intersection(right_index.index).sort_values()
    if not len(common):
        _raise(
            "margin snapshots have no common keys",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    left_common = left_index.loc[common, list(MARGIN_MVE_SOURCE_FIELDS)]
    right_common = right_index.loc[common, list(MARGIN_MVE_SOURCE_FIELDS)]
    left_values = left_common.to_numpy(dtype=np.float32)
    right_values = right_common.to_numpy(dtype=np.float32)
    equal = (left_values == right_values) | (np.isnan(left_values) & np.isnan(right_values))
    drift_mask = ~equal.all(axis=1)
    if drift_mask.any():
        _raise(
            "margin cross-snapshot value drift detected",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_VALUE_DRIFT",
            value_drift_row_count=int(drift_mask.sum()),
        )
    projection = left_common.reset_index()
    window_left = left_index.loc[
        (left_index.index.get_level_values("datetime") >= source_window_start)
        & (left_index.index.get_level_values("datetime") <= source_window_end)
    ]
    window_right = right_index.loc[
        (right_index.index.get_level_values("datetime") >= source_window_start)
        & (right_index.index.get_level_values("datetime") <= source_window_end)
    ]
    window_common = window_left.index.intersection(window_right.index)
    parity = {
        "schema_version": "advisory_n3_margin_cross_snapshot_parity_v1",
        "source_quality": MARGIN_MVE_SOURCE_QUALITY,
        "source_window_start": source_window_start.date().isoformat(),
        "source_window_end": source_window_end.date().isoformat(),
        "projection_common_key_count": int(len(common)),
        "common_key_count": int(len(window_common)),
        "current_only_key_count": int(len(window_left.index.difference(window_right.index))),
        "secondary_only_key_count": int(len(window_right.index.difference(window_left.index))),
        "value_drift_row_count": 0,
        "source_fields": list(MARGIN_MVE_SOURCE_FIELDS),
        "comparison_semantics": "FLOAT32_EXACT_WITH_PAIRED_NAN",
        "vintage_archive": False,
        "sealed_holdout_accessed": False,
    }
    return projection.sort_values(["datetime", "instrument"]).reset_index(drop=True), parity


def build_margin_source_coverage(
    *,
    parent: pd.DataFrame,
    date_map: pd.DataFrame,
    projection: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, float]]:
    frame = parent.loc[:, PARENT_KEY_COLUMNS].copy()
    frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    frame["instrument"] = frame["instrument"].astype(str)
    frame = frame.merge(date_map, on="decision_as_of_trade_date", how="left", validate="many_to_one")
    ranked = frame.sort_values(
        ["decision_as_of_trade_date", "score", "instrument"],
        ascending=[True, False, True],
        kind="mergesort",
    )
    ranked["parent_position"] = ranked.groupby("decision_as_of_trade_date", sort=False).cumcount() + 1
    available = projection.loc[:, ["datetime", "instrument", *MARGIN_MVE_SOURCE_FIELDS]].rename(
        columns={"datetime": "source_date_d"}
    )
    available["source_row_available"] = True
    ranked = ranked.merge(
        available,
        on=["source_date_d", "instrument"],
        how="left",
        validate="many_to_one",
    )
    ranked["source_row_available"] = ranked["source_row_available"].fillna(False).astype(bool)
    rows: list[dict[str, Any]] = []
    for decision_date, day in ranked.groupby("decision_as_of_trade_date", sort=True):
        top20 = day["parent_position"] <= 20
        top50 = day["parent_position"] <= 50
        rows.append(
            {
                "decision_as_of_trade_date": decision_date,
                "source_date_d": day["source_date_d"].iloc[0],
                "source_date_d1": day["source_date_d1"].iloc[0],
                "source_date_d5": day["source_date_d5"].iloc[0],
                "instrument_count": int(len(day)),
                "source_row_count": int(day["source_row_available"].sum()),
                "source_row_fraction": float(day["source_row_available"].mean()),
                "top20_source_row_count": int(day.loc[top20, "source_row_available"].sum()),
                "top20_source_row_fraction": float(day.loc[top20, "source_row_available"].mean()),
                "top50_source_row_count": int(day.loc[top50, "source_row_available"].sum()),
                "top50_source_row_fraction": float(day.loc[top50, "source_row_available"].mean()),
            }
        )
    source_rows = ranked.loc[ranked["source_row_available"]]
    raw_quality = {
        field: float(np.isfinite(pd.to_numeric(source_rows[field], errors="coerce")).mean())
        for field in MARGIN_MVE_SOURCE_FIELDS
    }
    return pd.DataFrame(rows), raw_quality


def build_margin_feature_panel(
    *,
    parent_outcomes: pd.DataFrame,
    source_projection: pd.DataFrame,
    source_coverage_daily: pd.DataFrame,
) -> pd.DataFrame:
    """Build the exact T-1 margin dynamics roster while retaining every parent key."""

    missing = set(PARENT_OUTCOME_COLUMNS) - set(parent_outcomes.columns)
    if missing:
        _raise(
            "margin parent outcome panel omits required columns",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            missing_columns=sorted(missing),
        )
    frame = parent_outcomes.loc[:, PARENT_OUTCOME_COLUMNS].copy()
    frame = frame.loc[frame["arm_id"].astype(str) == PARENT_ARM_ID].drop(columns="arm_id")
    frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    frame["instrument"] = frame["instrument"].astype(str)
    if frame.empty or frame.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise(
            "margin parent PIT keys are empty or duplicated",
            "ADVISORY_N3_MARGIN_MVE_PIT_VIOLATION",
        )
    if not frame["instrument"].eq(frame["instrument"].str.upper()).all():
        _raise(
            "margin parent instruments are not canonical uppercase",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    score = pd.to_numeric(frame["score"], errors="coerce")
    if not np.isfinite(score.to_numpy(dtype=float)).all():
        _raise(
            "margin parent score is not fully finite",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    frame["score"] = score
    frame["parent_rank_pct"] = (
        frame.groupby("decision_as_of_trade_date", sort=False)["score"]
        .rank(method="average", pct=True, ascending=True)
        .astype("float32")
    )
    mapping = source_coverage_daily.loc[
        :,
        ["decision_as_of_trade_date", "source_date_d", "source_date_d1", "source_date_d5"],
    ].copy()
    for column in mapping.columns:
        mapping[column] = pd.to_datetime(mapping[column]).dt.normalize()
    if mapping.duplicated("decision_as_of_trade_date").any():
        _raise(
            "margin source date map contains duplicate decisions",
            "ADVISORY_N3_MARGIN_MVE_PIT_VIOLATION",
        )
    frame = frame.merge(mapping, on="decision_as_of_trade_date", how="left", validate="many_to_one")
    if frame[["source_date_d", "source_date_d1", "source_date_d5"]].isna().any().any():
        _raise("margin source date map is incomplete", "ADVISORY_N3_MARGIN_MVE_PIT_VIOLATION")
    if not (
        (frame["source_date_d5"] < frame["source_date_d1"])
        & (frame["source_date_d1"] < frame["source_date_d"])
        & (frame["source_date_d"] < frame["decision_as_of_trade_date"])
    ).all():
        _raise(
            "margin source D/D-1/D-5 ordering is not strictly before decision T",
            "ADVISORY_N3_MARGIN_MVE_PIT_VIOLATION",
        )
    projection = _normalize_margin_projection_frame(source_projection, label="source_projection")
    for suffix, date_column in (("d", "source_date_d"), ("d1", "source_date_d1"), ("d5", "source_date_d5")):
        renamed = projection.rename(
            columns={
                "datetime": date_column,
                **{field: f"{field}_{suffix}" for field in MARGIN_MVE_SOURCE_FIELDS},
            }
        )
        renamed[f"margin_available_{suffix}"] = 1.0
        frame = frame.merge(
            renamed,
            on=[date_column, "instrument"],
            how="left",
            validate="many_to_one",
        )
    frame["margin_row_available"] = frame["margin_available_d"].fillna(0.0).astype("float32")
    frame["margin_history_coverage_fraction"] = (
        frame[["margin_available_d", "margin_available_d1", "margin_available_d5"]]
        .fillna(0.0)
        .sum(axis=1)
        .div(3.0)
        .astype("float32")
    )
    for field in MARGIN_MVE_SOURCE_FIELDS:
        for suffix in ("d", "d1", "d5"):
            frame[f"{field}_{suffix}"] = pd.to_numeric(frame[f"{field}_{suffix}"], errors="coerce")
    for field in ("md_rzye", "md_rqye", "md_rqyl", "md_rzrqye"):
        short = field.removeprefix("md_")
        frame[f"{short}_log_delta_1d"] = _log_delta(frame[f"{field}_d"], frame[f"{field}_d1"])
        frame[f"{short}_log_delta_5d"] = _log_delta(frame[f"{field}_d"], frame[f"{field}_d5"])
    frame["rz_buy_to_prev_balance"] = _strict_ratio(frame["md_rzmre_d"], frame["md_rzye_d1"])
    frame["rz_repay_to_prev_balance"] = _strict_ratio(frame["md_rzche_d"], frame["md_rzye_d1"])
    frame["rq_sell_to_prev_balance"] = _strict_ratio(frame["md_rqmcl_d"], frame["md_rqyl_d1"])
    frame["rq_repay_to_prev_balance"] = _strict_ratio(frame["md_rqchl_d"], frame["md_rqyl_d1"])
    ranked = frame.groupby("decision_as_of_trade_date", sort=False)[list(MARGIN_MVE_RAW_DYNAMICS_FEATURES)].rank(
        method="average",
        pct=True,
        ascending=True,
    )
    for raw, ranked_name in zip(
        MARGIN_MVE_RAW_DYNAMICS_FEATURES,
        MARGIN_MVE_RANKED_DYNAMICS_FEATURES,
        strict=True,
    ):
        frame[ranked_name] = ranked[raw].astype("float32")
    ordered = (
        "decision_as_of_trade_date",
        "instrument",
        "score",
        "parent_rank_pct",
        "margin_row_available",
        "margin_history_coverage_fraction",
        *MARGIN_MVE_RAW_DYNAMICS_FEATURES,
        *MARGIN_MVE_RANKED_DYNAMICS_FEATURES,
        "economic_net_excess_bps",
        "outcome_known",
    )
    return frame.loc[:, ordered].sort_values(["decision_as_of_trade_date", "instrument"]).reset_index(drop=True)


def validate_margin_feature_support(features: pd.DataFrame) -> dict[str, Any]:
    available = features["margin_row_available"].eq(1.0)
    fractions: dict[str, float] = {}
    minimum_by_day: dict[str, int] = {}
    reasons: list[str] = []
    for column in MARGIN_MVE_RANKED_DYNAMICS_FEATURES:
        finite = np.isfinite(pd.to_numeric(features[column], errors="coerce").to_numpy(dtype=float))
        fraction = float(finite[available.to_numpy()].mean()) if available.any() else 0.0
        counts = pd.Series(finite, index=features.index).groupby(features["decision_as_of_trade_date"], sort=True).sum()
        minimum = int(counts.min()) if len(counts) else 0
        fractions[column] = fraction
        minimum_by_day[column] = minimum
        if fraction < MARGIN_MVE_MIN_DYNAMICS_FINITE_FRACTION:
            reasons.append(f"{column.upper()}_FINITE_FRACTION_BELOW_MINIMUM")
        if minimum < MARGIN_MVE_MIN_DYNAMICS_FINITE_PER_DAY:
            reasons.append(f"{column.upper()}_FINITE_PER_DAY_BELOW_MINIMUM")
    report = {
        "schema_version": "advisory_n3_margin_feature_support_v1",
        "dynamics_finite_fraction_on_available_source": fractions,
        "dynamics_minimum_finite_per_day": minimum_by_day,
        "minimum_fraction": MARGIN_MVE_MIN_DYNAMICS_FINITE_FRACTION,
        "minimum_finite_per_day": MARGIN_MVE_MIN_DYNAMICS_FINITE_PER_DAY,
        "support_sufficient": not reasons,
        "reason_codes": reasons,
    }
    if reasons:
        _raise(
            "margin dynamics feature support is insufficient",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_SUPPORT_INSUFFICIENT",
            reason_codes=reasons,
        )
    return report


def run_margin_crossfit(
    *,
    features: pd.DataFrame,
    paths: Sequence[Mapping[str, Any]],
    request: FrozenMarginInformationSetRequestV1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fit three frozen Ridge trials and average exactly seven OOF predictions per row."""

    try:
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import Ridge
        from sklearn.preprocessing import StandardScaler
    except ImportError as exc:  # pragma: no cover
        _raise(
            "margin MVE sklearn dependency is unavailable",
            "ADVISORY_N3_MARGIN_MVE_OOF_INVALID",
            error_type=type(exc).__name__,
        )
    frame = features.copy().reset_index(drop=True)
    labels = pd.to_numeric(frame["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    evaluable = frame["outcome_known"].fillna(False).astype(bool).to_numpy() & np.isfinite(labels)
    if len(paths) != request.expected_ready_path_count:
        _raise(
            "margin CPCV path count drift",
            "ADVISORY_N3_MARGIN_MVE_CPCV_INVALID",
            actual=len(paths),
        )
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
                _raise("margin CPCV path is not READY", "ADVISORY_N3_MARGIN_MVE_CPCV_INVALID")
            train_dates = pd.DatetimeIndex(pd.to_datetime(path.get("train_dates", ()))).normalize()
            validation_dates = pd.DatetimeIndex(pd.to_datetime(path.get("validation_dates", ()))).normalize()
            if not len(train_dates) or not len(validation_dates) or set(train_dates) & set(validation_dates):
                _raise("margin CPCV date identity invalid", "ADVISORY_N3_MARGIN_MVE_CPCV_INVALID")
            if not set(train_dates).issubset(source_dates) or not set(validation_dates).issubset(source_dates):
                _raise("margin CPCV date outside source", "ADVISORY_N3_MARGIN_MVE_CPCV_INVALID")
            train_index = np.flatnonzero(dates.isin(train_dates) & evaluable)
            validation_index = np.flatnonzero(dates.isin(validation_dates))
            x_train_raw = frame.loc[train_index, columns].to_numpy(dtype=float)
            x_validation_raw = frame.loc[validation_index, columns].to_numpy(dtype=float)
            if not len(train_index) or not len(validation_index) or not np.isfinite(x_train_raw).any(axis=0).all():
                _raise(
                    "margin CPCV fold has no rows or an all-missing feature",
                    "ADVISORY_N3_MARGIN_MVE_OOF_INVALID",
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
                _raise("margin Ridge produced non-finite OOF", "ADVISORY_N3_MARGIN_MVE_OOF_INVALID")
            sums[validation_index] += predicted
            counts[validation_index] += 1
            diagnostics.append(
                {
                    "trial_id": trial.trial_id,
                    "path_id": str(path.get("path_id")),
                    "train_row_count": int(len(train_index)),
                    "validation_row_count": int(len(validation_index)),
                    "imputer_statistics_json": json.dumps(
                        [float(value) for value in np.asarray(imputer.statistics_)],
                        separators=(",", ":"),
                    ),
                    "coefficient_json": json.dumps(
                        [float(value) for value in np.asarray(model.coef_).reshape(-1)],
                        separators=(",", ":"),
                    ),
                    "intercept": float(model.intercept_),
                }
            )
        if not np.equal(counts, request.expected_oof_predictions_per_row).all():
            unique, frequencies = np.unique(counts, return_counts=True)
            _raise(
                "margin OOF prediction multiplicity drift",
                "ADVISORY_N3_MARGIN_MVE_OOF_INVALID",
                trial_id=trial.trial_id,
                counts={str(int(key)): int(value) for key, value in zip(unique, frequencies, strict=True)},
            )
        column = MODEL_SCORE_COLUMNS[trial.trial_id]
        score_output[column] = (sums / counts).astype("float32")
        score_output[f"{column}_count"] = counts
    diagnostics_frame = pd.DataFrame(diagnostics).sort_values(["trial_id", "path_id"]).reset_index(drop=True)
    return score_output, diagnostics_frame


def evaluate_margin_models(
    *,
    oof_scores: pd.DataFrame,
    regime_daily: pd.DataFrame,
    source_coverage_daily: pd.DataFrame,
    feature_support: Mapping[str, Any],
    request: FrozenMarginInformationSetRequestV1,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Evaluate the dynamics candidate against current parent and both refit controls."""

    score_columns = {
        "parent": "parent_rank_pct",
        "parent_comparator": "parent_comparator_oof_score",
        "membership": "membership_oof_score",
        "candidate": "candidate_oof_score",
    }
    required = {
        "decision_as_of_trade_date",
        "instrument",
        "economic_net_excess_bps",
        "outcome_known",
        *score_columns.values(),
    }
    if not required.issubset(oof_scores.columns):
        _raise(
            "margin OOF score panel schema drift",
            "ADVISORY_N3_MARGIN_MVE_OOF_INVALID",
            missing_columns=sorted(required - set(oof_scores.columns)),
        )
    scores = oof_scores.copy()
    scores["decision_as_of_trade_date"] = pd.to_datetime(scores["decision_as_of_trade_date"]).dt.normalize()
    if scores.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise("margin OOF keys duplicated", "ADVISORY_N3_MARGIN_MVE_OOF_INVALID")
    if not np.isfinite(scores[list(score_columns.values())].to_numpy(dtype=float)).all():
        _raise("margin OOF score non-finite", "ADVISORY_N3_MARGIN_MVE_OOF_INVALID")
    regimes = regime_daily.loc[:, ["decision_as_of_trade_date", "regime"]].copy()
    regimes["decision_as_of_trade_date"] = pd.to_datetime(regimes["decision_as_of_trade_date"]).dt.normalize()
    if regimes.duplicated("decision_as_of_trade_date").any():
        _raise(
            "margin regime dates duplicated",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        )
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
                labeled[column],
                labeled["economic_net_excess_bps"],
                method="spearman",
            )
            row[f"{name}_top5_evaluable"] = _top5_outcome_evaluable(day, ids[name])
            row[f"{name}_top5_net_excess_bps"] = _top5_net_value(day, ids[name])
            row[f"{name}_instruments"] = ",".join(sorted(ids[name]))
            prior = previous[name]
            row[f"{name}_top5_churn"] = None if prior is None else float(1.0 - len(prior & ids[name]) / 5.0)
            previous[name] = ids[name]
        for baseline in ("parent", "parent_comparator", "membership"):
            row[f"candidate_{baseline}_replacement_count"] = int(5 - len(ids["candidate"] & ids[baseline]))
            row[f"candidate_{baseline}_intervened"] = ids["candidate"] != ids[baseline]
        rows.append(row)
    daily = pd.DataFrame(rows)
    for baseline in ("parent", "parent_comparator", "membership"):
        daily[f"candidate_rank_ic_delta_{baseline}"] = daily["candidate_rank_ic"] - daily[f"{baseline}_rank_ic"]
        daily[f"candidate_top5_lift_{baseline}_bps"] = (
            daily["candidate_top5_net_excess_bps"] - daily[f"{baseline}_top5_net_excess_bps"]
        )
    paired_columns = [
        column
        for baseline in ("parent", "parent_comparator", "membership")
        for column in (
            f"candidate_rank_ic_delta_{baseline}",
            f"candidate_top5_lift_{baseline}_bps",
        )
    ]
    daily["evaluable"] = np.isfinite(daily[paired_columns].to_numpy(dtype=float)).all(axis=1)
    coverage = source_coverage_daily.copy()
    coverage["decision_as_of_trade_date"] = pd.to_datetime(coverage["decision_as_of_trade_date"]).dt.normalize()
    daily = daily.merge(coverage, on="decision_as_of_trade_date", how="left", validate="one_to_one")
    if daily["instrument_count"].isna().any():
        _raise(
            "margin daily coverage join drift",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        )
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
        "candidate_membership_rank_ic_delta": _metric_inference(
            daily["candidate_rank_ic_delta_membership"],
            request=request,
            alpha=current_alpha,
            threshold=0.0,
            seed_offset=4,
        ),
        "candidate_membership_top5_lift_bps": _metric_inference(
            daily["candidate_top5_lift_membership_bps"],
            request=request,
            alpha=current_alpha,
            threshold=0.0,
            seed_offset=5,
        ),
    }
    support = {
        baseline: _intervention_support(daily, f"candidate_{baseline}_intervened", request)
        for baseline in ("parent", "parent_comparator", "membership")
    }
    support_sufficient = all(value["support_sufficient"] for value in support.values())
    stability = _stability_report(daily, request=request)
    reasons = sorted(
        {f"{baseline.upper()}__{reason}" for baseline, value in support.items() for reason in value["reason_codes"]}
    )
    inference_thresholds = {
        "candidate_parent_rank_ic_delta": 0.0,
        "candidate_parent_top5_lift_bps": request.minimum_parent_lift_bps,
        "candidate_parent_comparator_rank_ic_delta": 0.0,
        "candidate_parent_comparator_top5_lift_bps": 0.0,
        "candidate_membership_rank_ic_delta": 0.0,
        "candidate_membership_top5_lift_bps": 0.0,
    }
    for name, threshold in inference_thresholds.items():
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
    selected = "N3_MARGIN_DYNAMICS_EXPANDED_V1" if eligible else None
    evidence_class = (
        "EXPLORATORY_CANDIDATE_SELECTED"
        if eligible
        else "EXPLORATORY_NOT_SELECTED"
        if support_sufficient
        else "EXPLORATORY_INSUFFICIENT_SUPPORT"
    )
    summary = {
        "schema_version": "advisory_n3_margin_information_set_model_summary_v1",
        "request_sha256": request.request_sha256,
        "planned_trial_count": 3,
        "generated_trial_count": 3,
        "evaluated_trial_count": 3,
        "selectable_trial_count": 1,
        "cumulative_candidate_index": request.cumulative_candidate_index,
        "current_familywise_hypothesis_count": request.current_familywise_hypothesis_count,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "feature_support": dict(feature_support),
        "intervention_support": support,
        "support_sufficient": support_sufficient,
        "evidence_class": evidence_class,
        "inference": inference,
        "mean_rank_ic": {name: _mean(daily[f"{name}_rank_ic"]) for name in score_columns},
        "mean_top5_net_excess_bps": {name: _mean(daily[f"{name}_top5_net_excess_bps"]) for name in score_columns},
        "top5_evaluable_day_count": {
            name: int(daily[f"{name}_top5_evaluable"].astype(bool).sum()) for name in score_columns
        },
        "candidate_parent_lift_dsr": _deflated_sharpe_diagnostic(
            daily["candidate_top5_lift_parent_bps"].tolist(),
            trial_count=request.cumulative_candidate_index,
        ),
        "candidate_score_spearman_mean": {
            baseline: _mean_by_day_score_correlation(
                scores,
                "candidate_oof_score",
                score_columns[baseline],
            )
            for baseline in ("parent", "parent_comparator", "membership")
        },
        "eligible": eligible,
        "reason_codes": reasons,
        "selected_trial_id": selected,
    }
    frontier = {
        "schema_version": "advisory_n3_margin_information_set_frontier_v1",
        "request_sha256": request.request_sha256,
        "eligible_trial_ids": (["N3_MARGIN_DYNAMICS_EXPANDED_V1"] if eligible else []),
        "selected_trial_id": selected,
        "selected_trial_count": 1 if selected else 0,
        "support_sufficient": support_sufficient,
        "evidence_class": evidence_class,
        "candidate_reselection_allowed": False,
        "exact_retry_allowed": True,
        "selection_rule": "CUMULATIVE_PARENT_AND_CURRENT_DUAL_CONTROL_LOWERS_SUPPORT_STABILITY_SELECT_ONCE",
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    frontier["frontier_sha256"] = canonical_json_sha256(frontier)
    return daily, summary, stability, frontier


def run_margin_information_set_mve(request_path: str | Path) -> dict[str, Any]:
    started = time.monotonic()
    request_file = _resolve_bound_path(request_path)
    try:
        request = FrozenMarginInformationSetRequestV1.model_validate_json(request_file.read_text(encoding="utf-8"))
    except Exception as exc:
        _raise(
            "margin MVE request cannot be loaded",
            "ADVISORY_N3_MARGIN_MVE_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    existing = _find_existing_mve_bundle(request)
    _verify_environment(request)
    if existing is not None:
        delivery = _deliver_bundle(request=request, bundle_path=existing)
        return _run_response(request, existing, delivery, exact_retry=True)
    sources = _load_verified_sources(request)
    _check_resource_limits(request, "sources_loaded")
    features = build_margin_feature_panel(
        parent_outcomes=sources["parent_outcomes"],
        source_projection=sources["source_projection"],
        source_coverage_daily=sources["source_coverage_daily"],
    )
    feature_support = validate_margin_feature_support(features)
    _check_resource_limits(request, "feature_panel_built")
    oof, fold_diagnostics = run_margin_crossfit(
        features=features,
        paths=sources["cpcv"]["paths"],
        request=request,
    )
    _check_resource_limits(request, "crossfit_complete")
    daily, summary, stability, frontier = evaluate_margin_models(
        oof_scores=oof,
        regime_daily=sources["regime_daily"],
        source_coverage_daily=sources["source_coverage_daily"],
        feature_support=feature_support,
        request=request,
    )
    _validate_parent_daily_parity(
        daily=daily,
        parent_top5_daily=sources["parent_top5_daily"],
        parent_signal_daily=sources["parent_signal_daily"],
    )
    _check_resource_limits(request, "evaluation_complete")
    bundle = _publish_mve_bundle(
        request=request,
        features=features,
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


def inspect_margin_source_bundle(bundle_path: str | Path) -> dict[str, Any]:
    loaded = _read_margin_source_bundle(_resolve_bound_path(bundle_path))
    receipt = loaded["receipt"]
    return {
        "status": "VALID",
        "bundle_id": loaded["manifest"]["bundle_id"],
        "source_request_id": loaded["request"].source_request_id,
        "source_receipt_id": receipt.source_receipt_id,
        "source_identity_sha256": receipt.source_identity_sha256,
        "projection_row_count": receipt.projection_row_count,
        "common_key_count": receipt.common_key_count,
        "value_drift_row_count": receipt.value_drift_row_count,
        "source_row_fraction": receipt.source_row_fraction,
        "top20_source_row_fraction": receipt.top20_source_row_fraction,
        "top50_source_row_fraction": receipt.top50_source_row_fraction,
        "source_quality": receipt.source_quality,
        "target_columns_read": False,
        "sealed_holdout_accessed": False,
    }


def inspect_margin_information_set_bundle(bundle_path: str | Path) -> dict[str, Any]:
    path = _resolve_bound_path(bundle_path)
    loaded = _read_margin_mve_bundle(path)
    receipt = loaded["receipt"]
    frontier = _read_json(path / "frontier_receipt.json", "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID")
    return {
        "status": "VALID",
        "bundle_id": loaded["manifest"]["bundle_id"],
        "request_id": loaded["request"].request_id,
        "receipt_id": receipt.receipt_id,
        "selected_trial_id": receipt.selected_trial_id,
        "eligible_trial_ids": list(receipt.eligible_trial_ids),
        "next_task": receipt.next_task,
        "frontier_sha256": frontier["frontier_sha256"],
        "planned_trial_count": 3,
        "generated_trial_count": 3,
        "evaluated_trial_count": 3,
        "selected_trial_count": receipt.selected_trial_count,
        "selectable_trial_count": 1,
        "cumulative_candidate_index": loaded["request"].cumulative_candidate_index,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "final_model_written": False,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "position_weight_output": False,
    }


def _read_parent_panel(path: Path, *, include_outcomes: bool) -> pd.DataFrame:
    columns = list(PARENT_OUTCOME_COLUMNS if include_outcomes else ("arm_id", *PARENT_KEY_COLUMNS))
    frame = _read_parquet(
        path,
        reason_code="ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        columns=columns,
    )
    frame = frame.loc[frame["arm_id"].astype(str) == PARENT_ARM_ID].copy()
    frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    frame["instrument"] = frame["instrument"].astype(str)
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    if (
        len(frame) != 1_710_301
        or frame["decision_as_of_trade_date"].nunique() != 386
        or frame.duplicated(["decision_as_of_trade_date", "instrument"]).any()
        or not frame["instrument"].eq(frame["instrument"].str.upper()).all()
        or not np.isfinite(frame["score"].to_numpy(dtype=float)).all()
        or frame["decision_as_of_trade_date"].min() != pd.Timestamp("2024-07-04")
        or frame["decision_as_of_trade_date"].max() != pd.Timestamp("2026-02-02")
    ):
        _raise(
            "N2-B current-parent panel identity drift",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            row_count=int(len(frame)),
            decision_date_count=int(frame["decision_as_of_trade_date"].nunique()),
        )
    return frame.reset_index(drop=True)


def _read_daily_calendar(path: Path) -> pd.DatetimeIndex:
    try:
        values = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        calendar = pd.DatetimeIndex(pd.to_datetime(values, errors="raise")).normalize()
    except Exception as exc:
        _raise(
            "margin trading calendar cannot be read",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            error_type=type(exc).__name__,
        )
    if not len(calendar) or calendar.has_duplicates or not calendar.is_monotonic_increasing:
        _raise(
            "margin trading calendar identity is invalid",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    return calendar


def _build_source_date_map(parent: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.DataFrame:
    rows: list[dict[str, pd.Timestamp]] = []
    decisions = pd.DatetimeIndex(parent["decision_as_of_trade_date"].unique()).sort_values()
    calendar_positions = {value: index for index, value in enumerate(calendar)}
    for decision in decisions:
        position = calendar_positions.get(decision)
        if position is None or position < 6:
            _raise(
                "decision date cannot map to exact D/D-1/D-5 trading days",
                "ADVISORY_N3_MARGIN_MVE_PIT_VIOLATION",
                decision_date=decision.date().isoformat(),
            )
        rows.append(
            {
                "decision_as_of_trade_date": decision,
                "source_date_d": calendar[position - 1],
                "source_date_d1": calendar[position - 2],
                "source_date_d5": calendar[position - 6],
            }
        )
    result = pd.DataFrame(rows)
    if (
        len(result) != 386
        or result["source_date_d"].min() != pd.Timestamp("2024-07-03")
        or result["source_date_d"].max() != pd.Timestamp("2026-01-30")
        or not (result["source_date_d"] < result["decision_as_of_trade_date"]).all()
    ):
        _raise(
            "margin source date mapping identity drift",
            "ADVISORY_N3_MARGIN_MVE_PIT_VIOLATION",
        )
    return result


def _read_margin_h5(
    path: Path,
    *,
    expected_format: str,
    required_dates: set[pd.Timestamp],
    instruments: set[str],
    calendar: pd.DatetimeIndex,
    chunk_rows: int,
) -> tuple[pd.DataFrame, int, dict[str, int]]:
    frames: list[pd.DataFrame] = []
    total_rows = 0
    invalid_counts = {field: 0 for field in MARGIN_MVE_SOURCE_FIELDS}
    calendar_set = set(calendar)
    try:
        with pd.HDFStore(path, mode="r") as store:
            if store.keys() != ["/data"]:
                _raise(
                    "margin H5 key roster drift",
                    "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
                    keys=store.keys(),
                )
            storer = store.get_storer("/data")
            actual_format = getattr(storer, "format_type", None)
            if actual_format != expected_format:
                _raise(
                    "margin H5 storage format drift",
                    "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
                    expected=expected_format,
                    actual=actual_format,
                )
            chunks: Sequence[pd.DataFrame] | Any
            if expected_format == "table":
                chunks = store.select("/data", chunksize=chunk_rows)
            else:
                chunks = (store.get("/data"),)
            for raw in chunks:
                total_rows += len(raw)
                frame = _hdf_frame_to_columns(raw)
                dates = pd.DatetimeIndex(frame["datetime"]).normalize()
                if any(value not in calendar_set for value in dates.unique()):
                    _raise(
                        "margin H5 contains a date outside the frozen calendar",
                        "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
                    )
                frame["datetime"] = dates
                frame["instrument"] = frame["instrument"].astype(str)
                if not frame["instrument"].eq(frame["instrument"].str.upper()).all():
                    _raise(
                        "margin H5 instruments are not canonical uppercase",
                        "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
                    )
                keep = frame["datetime"].isin(required_dates) & frame["instrument"].isin(instruments)
                if not keep.any():
                    continue
                frame = frame.loc[keep].copy()
                for field in MARGIN_MVE_SOURCE_FIELDS:
                    values = pd.to_numeric(frame[field], errors="coerce").to_numpy(dtype=float)
                    invalid = (~np.isfinite(values) & ~pd.isna(values)) | (values < 0)
                    invalid_counts[field] += int(invalid.sum())
                    values[invalid] = np.nan
                    frame[field] = values.astype("float32")
                frames.append(frame)
    except AdvisoryModelFirstError:
        raise
    except Exception as exc:
        _raise(
            "margin H5 cannot be read",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )
    if not frames:
        _raise(
            "margin H5 projection is empty",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            path=path.as_posix(),
        )
    result = pd.concat(frames, ignore_index=True)
    result = _normalize_margin_projection_frame(result, label=expected_format)
    return result, int(total_rows), invalid_counts


def _hdf_frame_to_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame.index, pd.MultiIndex) or list(frame.index.names) != ["datetime", "instrument"]:
        _raise(
            "margin H5 index schema drift",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            index_names=list(frame.index.names),
        )
    if tuple(frame.columns) != MARGIN_MVE_SOURCE_FIELDS:
        _raise(
            "margin H5 field schema drift",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            columns=[str(value) for value in frame.columns],
        )
    if any(str(frame[field].dtype) != "float32" for field in MARGIN_MVE_SOURCE_FIELDS):
        _raise(
            "margin H5 field dtype drift",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            dtypes={field: str(frame[field].dtype) for field in MARGIN_MVE_SOURCE_FIELDS},
        )
    return frame.reset_index()


def _normalize_margin_projection_frame(frame: pd.DataFrame, *, label: str) -> pd.DataFrame:
    expected = {"datetime", "instrument", *MARGIN_MVE_SOURCE_FIELDS}
    if set(frame.columns) != expected:
        _raise(
            "margin projection schema drift",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            label=label,
            columns=sorted(str(value) for value in frame.columns),
        )
    result = frame.loc[:, ["datetime", "instrument", *MARGIN_MVE_SOURCE_FIELDS]].copy()
    result["datetime"] = pd.to_datetime(result["datetime"], errors="raise").dt.normalize()
    result["instrument"] = result["instrument"].astype(str)
    if result.empty or result.duplicated(["datetime", "instrument"]).any():
        _raise(
            "margin projection keys are empty or duplicated",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            label=label,
        )
    if not result["instrument"].eq(result["instrument"].str.upper()).all():
        _raise(
            "margin projection instruments are not canonical uppercase",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            label=label,
        )
    for field in MARGIN_MVE_SOURCE_FIELDS:
        result[field] = pd.to_numeric(result[field], errors="coerce").astype("float32")
    return result


def _read_stable_candidate_state(path: Path, candidate_root: Path) -> dict[str, Any]:
    try:
        content = path.read_bytes()
        payload = json.loads(content.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        _raise(
            "candidate state cannot be read",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            error_type=type(exc).__name__,
        )
    if not isinstance(payload, dict):
        _raise(
            "candidate state semantic gates are not satisfied",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    components = payload.get("components")
    validation = payload.get("validation")
    if (
        payload.get("status") != "CANDIDATE_READY"
        or payload.get("cutoff") != "2026-08-31"
        or payload.get("production_writes") != 0
        or payload.get("production_pointer_changes") != 0
        or not isinstance(components, dict)
        or set(components) != {"daily_bin", "factor_h5_static", "index_context", "minute_bin", "suspend_d"}
        or any(not isinstance(value, dict) or value.get("status") != "PASS" for value in components.values())
        or not isinstance(validation, dict)
        or validation.get("status") != "PASS"
        or validation.get("qe_multi_dataset_smoke") != "PASS"
        or not isinstance(payload.get("updated_at"), str)
    ):
        _raise(
            "candidate state semantic gates are not satisfied",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    declared_root = _resolve_bound_path(str(payload.get("candidate_root")))
    if declared_root != candidate_root.resolve():
        _raise(
            "candidate state root relation drift",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            declared=declared_root.as_posix(),
            actual=candidate_root.resolve().as_posix(),
        )
    return {
        "bytes": content,
        "payload": payload,
        "size_bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def _assert_state_unchanged(before: Mapping[str, Any], after: Mapping[str, Any]) -> None:
    if (
        before["sha256"] != after["sha256"]
        or before["size_bytes"] != after["size_bytes"]
        or before["bytes"] != after["bytes"]
    ):
        _raise(
            "candidate state changed during margin source prepare",
            "ADVISORY_N3_MARGIN_CANDIDATE_STATE_CHANGED_DURING_PREPARE",
            before_sha256=before["sha256"],
            after_sha256=after["sha256"],
        )


def _verify_file_identity(path: Path, *, expected_size: int, expected_sha256: str) -> None:
    if not path.is_file() or path.stat().st_size != expected_size or sha256_file(path) != expected_sha256:
        _raise(
            "margin source file identity drift",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            path=path.as_posix(),
            expected_size=expected_size,
            actual_size=path.stat().st_size if path.is_file() else None,
        )


def _validate_source_support(*, coverage: pd.DataFrame, raw_quality: Mapping[str, float]) -> None:
    instrument_count = int(coverage["instrument_count"].sum())
    source_count = int(coverage["source_row_count"].sum())
    top20_total = 20 * len(coverage)
    top50_total = 50 * len(coverage)
    aggregate = {
        "source": float(source_count / instrument_count) if instrument_count else 0.0,
        "top20": float(coverage["top20_source_row_count"].sum() / top20_total) if top20_total else 0.0,
        "top50": float(coverage["top50_source_row_count"].sum() / top50_total) if top50_total else 0.0,
    }
    supported_days = int((coverage["top20_source_row_count"] >= MARGIN_MVE_MIN_TOP20_SUPPORTED_COUNT).sum())
    reasons: list[str] = []
    if aggregate["source"] < MARGIN_MVE_MIN_SOURCE_FRACTION:
        reasons.append("SOURCE_ROW_FRACTION_BELOW_MINIMUM")
    if aggregate["top20"] < MARGIN_MVE_MIN_TOP20_SOURCE_FRACTION:
        reasons.append("TOP20_SOURCE_ROW_FRACTION_BELOW_MINIMUM")
    if aggregate["top50"] < MARGIN_MVE_MIN_TOP50_SOURCE_FRACTION:
        reasons.append("TOP50_SOURCE_ROW_FRACTION_BELOW_MINIMUM")
    if supported_days < MARGIN_MVE_MIN_TOP20_SUPPORTED_DAYS:
        reasons.append("TOP20_SUPPORTED_DAY_COUNT_BELOW_MINIMUM")
    for field, fraction in raw_quality.items():
        if not np.isfinite(fraction) or fraction < MARGIN_MVE_MIN_RAW_FIELD_FINITE_FRACTION:
            reasons.append(f"{field.upper()}_FINITE_FRACTION_BELOW_MINIMUM")
    if reasons:
        _raise(
            "margin source support is insufficient",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_SUPPORT_INSUFFICIENT",
            reason_codes=sorted(reasons),
            aggregate=aggregate,
            top20_supported_day_count=supported_days,
        )


def _publish_margin_source_bundle(
    *,
    output_root: Path,
    request: FrozenMarginSourceRequestV1,
    candidate_state_bytes: bytes,
    candidate_state_after_sha256: str,
    projection: pd.DataFrame,
    coverage: pd.DataFrame,
    parity: Mapping[str, Any],
    raw_quality: Mapping[str, float],
    current_rows_read: int,
    secondary_rows_read: int,
    source_bytes_read: int,
    elapsed_seconds: float,
) -> Path:
    root = output_root / "margin_source_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{request.source_request_id}.", dir=root))
    _write_json(temporary / "source_request.json", request.model_dump(mode="json"))
    (temporary / "candidate_state_snapshot.json").write_bytes(candidate_state_bytes)
    projection.to_parquet(temporary / "margin_source_projection.parquet", index=False)
    coverage.to_parquet(temporary / "source_coverage_daily.parquet", index=False)
    _write_json(temporary / "cross_snapshot_parity.json", parity)
    _validate_written_source_payload(
        bundle_path=temporary,
        request=request,
        expected_projection_rows=len(projection),
    )
    descriptors = _file_descriptors(temporary)
    identity_descriptors = {name: descriptors[name] for name in sorted(SOURCE_IDENTITY_MEMBERS)}
    source_identity = canonical_json_sha256(
        {
            "schema_version": SOURCE_BUNDLE_SCHEMA,
            "source_request_sha256": request.source_request_sha256,
            "files": identity_descriptors,
        }
    )
    instrument_count = int(coverage["instrument_count"].sum())
    top20_total = 20 * len(coverage)
    top50_total = 50 * len(coverage)
    temporary_bytes = sum(path.stat().st_size for path in temporary.iterdir() if path.is_file())
    receipt = build_margin_source_receipt(
        source_request_sha256=request.source_request_sha256,
        source_identity_sha256=source_identity,
        projection_sha256=sha256_file(temporary / "margin_source_projection.parquet"),
        projection_row_count=int(len(projection)),
        common_key_count=int(parity["common_key_count"]),
        current_only_key_count=int(parity["current_only_key_count"]),
        secondary_only_key_count=int(parity["secondary_only_key_count"]),
        parent_row_count=instrument_count,
        decision_date_count=int(len(coverage)),
        source_row_fraction=float(coverage["source_row_count"].sum() / instrument_count),
        top20_source_row_fraction=float(coverage["top20_source_row_count"].sum() / top20_total),
        top50_source_row_fraction=float(coverage["top50_source_row_count"].sum() / top50_total),
        top20_supported_day_count=int(
            (coverage["top20_source_row_count"] >= MARGIN_MVE_MIN_TOP20_SUPPORTED_COUNT).sum()
        ),
        raw_field_finite_fraction={key: float(value) for key, value in raw_quality.items()},
        candidate_state_before_sha256=request.candidate_state_sha256,
        candidate_state_after_sha256=candidate_state_after_sha256,
        current_source_row_count_read=current_rows_read,
        secondary_source_row_count_read=secondary_rows_read,
        source_unique_file_count=4,
        source_bytes_read=source_bytes_read,
        elapsed_seconds=elapsed_seconds,
        peak_rss_bytes=_peak_rss_bytes(),
        temporary_bytes=temporary_bytes,
    )
    _write_json(temporary / "source_identity_receipt.json", receipt.model_dump(mode="json"))
    descriptors = _file_descriptors(temporary)
    if set(descriptors) != SOURCE_BUNDLE_MEMBERS:
        _raise(
            "margin source bundle member roster drift",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
            members=sorted(descriptors),
        )
    destination = root / source_identity
    manifest = {
        "schema_version": SOURCE_BUNDLE_SCHEMA,
        "bundle_id": source_identity,
        "source_request_sha256": request.source_request_sha256,
        "source_receipt_sha256": receipt.source_receipt_sha256,
        "source_identity_sha256": source_identity,
        "source_quality": MARGIN_MVE_SOURCE_QUALITY,
        "target_columns_read": False,
        "database_reads": 0,
        "database_writes": 0,
        "network_reads": 0,
        "sealed_holdout_accessed": False,
        "files": descriptors,
    }
    _write_json(temporary / "manifest.json", manifest)
    if destination.exists():
        _read_margin_source_bundle(destination)
        _raise(
            "margin source bundle destination appeared concurrently",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
            bundle_id=source_identity,
        )
    temporary.replace(destination)
    _read_margin_source_bundle(destination)
    return destination


def _validate_written_source_payload(
    *,
    bundle_path: Path,
    request: FrozenMarginSourceRequestV1,
    expected_projection_rows: int,
) -> None:
    projection = _read_parquet(
        bundle_path / "margin_source_projection.parquet",
        reason_code="ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
    )
    if any(str(projection[field].dtype) != "float32" for field in MARGIN_MVE_SOURCE_FIELDS if field in projection):
        _raise(
            "written margin projection dtype drift",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
        )
    projection = _normalize_margin_projection_frame(projection, label="written_source_projection")
    ordered = projection.sort_values(["datetime", "instrument"], kind="mergesort").reset_index(drop=True)
    if (
        len(projection) != expected_projection_rows
        or not projection[["datetime", "instrument"]].reset_index(drop=True).equals(ordered[["datetime", "instrument"]])
        or projection["datetime"].max() > pd.Timestamp(request.source_end)
    ):
        _raise(
            "written margin projection row/order/window drift",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
        )
    coverage = _read_parquet(
        bundle_path / "source_coverage_daily.parquet",
        reason_code="ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
    )
    expected_columns = {
        "decision_as_of_trade_date",
        "source_date_d",
        "source_date_d1",
        "source_date_d5",
        "instrument_count",
        "source_row_count",
        "source_row_fraction",
        "top20_source_row_count",
        "top20_source_row_fraction",
        "top50_source_row_count",
        "top50_source_row_fraction",
    }
    if set(coverage.columns) != expected_columns:
        _raise(
            "written margin coverage schema drift",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
        )
    for column in ("decision_as_of_trade_date", "source_date_d", "source_date_d1", "source_date_d5"):
        coverage[column] = pd.to_datetime(coverage[column], errors="raise").dt.normalize()
    coverage = coverage.sort_values("decision_as_of_trade_date", kind="mergesort").reset_index(drop=True)
    ordered_dates = pd.DatetimeIndex(coverage["decision_as_of_trade_date"])
    ordering_valid = (
        (coverage["source_date_d5"] < coverage["source_date_d1"])
        & (coverage["source_date_d1"] < coverage["source_date_d"])
        & (coverage["source_date_d"] < coverage["decision_as_of_trade_date"])
    ).all()
    counts_valid = (
        (coverage["instrument_count"] > 0)
        & (coverage["source_row_count"] >= 0)
        & (coverage["source_row_count"] <= coverage["instrument_count"])
        & (coverage["top20_source_row_count"] >= 0)
        & (coverage["top20_source_row_count"] <= 20)
        & (coverage["top50_source_row_count"] >= 0)
        & (coverage["top50_source_row_count"] <= 50)
    ).all()
    if (
        len(coverage) != request.expected_decision_date_count
        or ordered_dates.has_duplicates
        or not ordered_dates.is_monotonic_increasing
        or ordered_dates.min() != pd.Timestamp(request.signal_start)
        or ordered_dates.max() != pd.Timestamp(request.signal_end)
        or coverage["source_date_d"].min() != pd.Timestamp(request.source_start)
        or coverage["source_date_d"].max() != pd.Timestamp(request.source_end)
        or int(coverage["instrument_count"].sum()) != request.expected_parent_row_count
        or not ordering_valid
        or not counts_valid
    ):
        _raise(
            "written margin coverage identity drift",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
        )


def _read_margin_source_bundle(path: Path) -> dict[str, Any]:
    manifest = _read_json(path / "manifest.json", "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID")
    descriptors = manifest.get("files")
    _verify_bundle_member_descriptors(
        path=path,
        descriptors=descriptors,
        expected_members=SOURCE_BUNDLE_MEMBERS,
        reason_code="ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
    )
    try:
        request = FrozenMarginSourceRequestV1.model_validate_json(
            (path / "source_request.json").read_text(encoding="utf-8")
        )
        receipt = MarginSourceIdentityReceiptV1.model_validate_json(
            (path / "source_identity_receipt.json").read_text(encoding="utf-8")
        )
    except Exception as exc:
        _raise(
            "margin source bundle contract member is invalid",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
            error_type=type(exc).__name__,
        )
    assert isinstance(descriptors, dict)
    identity_descriptors = {name: descriptors[name] for name in sorted(SOURCE_IDENTITY_MEMBERS)}
    expected_identity = canonical_json_sha256(
        {
            "schema_version": SOURCE_BUNDLE_SCHEMA,
            "source_request_sha256": request.source_request_sha256,
            "files": identity_descriptors,
        }
    )
    state_bytes = (path / "candidate_state_snapshot.json").read_bytes()
    parity = _read_json(path / "cross_snapshot_parity.json", "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID")
    invalid = (
        manifest.get("schema_version") != SOURCE_BUNDLE_SCHEMA
        or path.name != expected_identity
        or manifest.get("bundle_id") != expected_identity
        or manifest.get("source_request_sha256") != request.source_request_sha256
        or manifest.get("source_receipt_sha256") != receipt.source_receipt_sha256
        or manifest.get("source_identity_sha256") != expected_identity
        or receipt.source_request_sha256 != request.source_request_sha256
        or receipt.source_identity_sha256 != expected_identity
        or receipt.projection_sha256 != descriptors["margin_source_projection.parquet"]["sha256"]
        or receipt.projection_row_count != descriptors["margin_source_projection.parquet"]["row_count"]
        or request.candidate_state_sha256 != hashlib.sha256(state_bytes).hexdigest()
        or request.candidate_state_size != len(state_bytes)
        or parity.get("value_drift_row_count") != 0
        or receipt.common_key_count != parity.get("common_key_count")
        or receipt.current_only_key_count != parity.get("current_only_key_count")
        or receipt.secondary_only_key_count != parity.get("secondary_only_key_count")
        or manifest.get("source_quality") != MARGIN_MVE_SOURCE_QUALITY
        or manifest.get("target_columns_read") is not False
        or manifest.get("database_reads") != 0
        or manifest.get("database_writes") != 0
        or manifest.get("network_reads") != 0
        or manifest.get("sealed_holdout_accessed") is not False
    )
    if invalid:
        _raise(
            "margin source bundle relational identity is invalid",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
        )
    return {
        "manifest": manifest,
        "request": request,
        "receipt": receipt,
        "parity": parity,
    }


def _find_existing_source_bundle(output_root: Path, source_request_sha256: str) -> Path | None:
    root = output_root / "margin_source_bundles"
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
        if manifest.get("source_request_sha256") == source_request_sha256:
            matches.append(path)
    if len(matches) > 1:
        _raise(
            "one margin source request maps to multiple bundles",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
        )
    if matches:
        _read_margin_source_bundle(matches[0])
        return matches[0]
    return None


def _validate_bound_sources(
    *,
    generator: Mapping[str, Any],
    n2b: Mapping[str, Any],
    n1: Mapping[str, Any],
) -> None:
    generator_receipt = generator["receipt"]
    generator_record = generator["registry_record"]
    n2b_receipt = n2b["receipt"]
    n2b_record = n2b["record"]
    cpcv = _read_json(
        _resolve_bound_path(n1["request"].output_root)
        / "tier1_bundles"
        / n1["manifest"]["bundle_id"]
        / "n1_label_interval_cpcv.json",
        "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
    )
    if (
        generator_receipt.status != "COMPLETE"
        or generator_receipt.generated_trial_count != 24
        or generator_receipt.evaluated_trial_count != 23
        or generator_receipt.selected_trial_count != 0
        or generator_receipt.next_task != "N3_UPSTREAM_ALPHA_NEW_DATA_SOURCE_MVE_DESIGN"
        or generator_receipt.sealed_holdout_accessed
        or generator_record.experiment_id != "ADVISORY-N3-QE-ALPHA-GENERATOR-MVE-V1"
        or generator_record.decision_use != DecisionUse.NAVIGATION_ONLY
        or n2b_record.experiment_id != "ADVISORY-N2B-INDEPENDENT-PACKAGE-ALPHA-AUDIT-V2"
        or n2b_receipt.decision_date_count != 386
        or n2b_receipt.signal_row_count_by_arm.get(PARENT_ARM_ID) != 1_710_301
        or n2b_receipt.sealed_holdout_accessed
        or n2b_record.policy_identity != generator_record.policy_identity
        or n1["oracle"].sealed_holdout_accessed
        or n1["learnability"].sealed_holdout_accessed
        or n1["quadrant"].sealed_holdout_accessed
        or len(cpcv.get("paths", ())) != 28
        or any(path.get("status") != "READY" for path in cpcv["paths"])
    ):
        _raise(
            "margin MVE bound source lineage is invalid",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        )


def _load_verified_sources(request: FrozenMarginInformationSetRequestV1) -> dict[str, Any]:
    generator_path = _resolve_bound_path(request.generator_bundle_path)
    n2b_path = _resolve_bound_path(request.n2b_bundle_path)
    n1_path = _resolve_bound_path(request.n1_bundle_path)
    source_path = _resolve_bound_path(request.source_bundle_path)
    generator = _read_generator_bundle(generator_path)
    n2b = _read_n2b_bundle(n2b_path)
    n1 = _read_n1_bundle(n1_path)
    source = _read_margin_source_bundle(source_path)
    _validate_bound_sources(generator=generator, n2b=n2b, n1=n1)
    _validate_request_source_identities(
        request=request,
        generator=generator,
        n2b=n2b,
        n1=n1,
        source=source,
    )
    for reference in request.evidence_refs:
        _verify_ref(reference)
    parent_outcomes = _read_parent_panel(
        n2b_path / "arm_signal_outcomes.parquet",
        include_outcomes=True,
    )
    known = parent_outcomes["outcome_known"].fillna(False).astype(bool)
    labels = pd.to_numeric(parent_outcomes["economic_net_excess_bps"], errors="coerce")
    finite = np.isfinite(labels.to_numpy(dtype=float))
    counts = {
        "source": int(len(parent_outcomes)),
        "known": int(known.sum()),
        "evaluable": int((known.to_numpy() & finite).sum()),
        "nonfinite_known": int((known.to_numpy() & ~finite).sum()),
        "unknown": int((~known).sum()),
        "decision_dates": int(parent_outcomes["decision_as_of_trade_date"].nunique()),
    }
    expected = {
        "source": request.expected_source_row_count,
        "known": request.expected_known_row_count,
        "evaluable": request.expected_evaluable_row_count,
        "nonfinite_known": request.expected_nonfinite_known_row_count,
        "unknown": request.expected_unknown_row_count,
        "decision_dates": request.expected_decision_date_count,
    }
    if counts != expected:
        _raise(
            "margin MVE parent outcome counts drift",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            expected=expected,
            actual=counts,
        )
    source_projection = _read_parquet(
        source_path / "margin_source_projection.parquet",
        reason_code="ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
    )
    source_coverage = _read_parquet(
        source_path / "source_coverage_daily.parquet",
        reason_code="ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
    )
    cpcv = _read_json(
        n1_path / "n1_label_interval_cpcv.json",
        "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
    )
    regime_daily = _read_parquet(
        n1_path / "learnability_daily.parquet",
        reason_code="ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        columns=("decision_as_of_trade_date", "regime"),
    )
    parent_top5 = _read_parquet(
        n2b_path / "arm_top5_daily.parquet",
        reason_code="ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
    )
    parent_top5 = parent_top5.loc[parent_top5["arm_id"].astype(str) == PARENT_ARM_ID].copy()
    parent_signal = _read_parquet(
        n2b_path / "signal_metrics_daily.parquet",
        reason_code="ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
    )
    parent_signal = parent_signal.loc[parent_signal["arm_id"].astype(str) == PARENT_ARM_ID].copy()
    if (
        len(source_coverage) != request.expected_decision_date_count
        or source["receipt"].parent_row_count != request.expected_source_row_count
        or source["receipt"].decision_date_count != request.expected_decision_date_count
        or len(parent_top5) != request.expected_decision_date_count
        or len(parent_signal) != request.expected_decision_date_count
    ):
        _raise(
            "margin MVE daily source counts drift",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    return {
        "generator": generator,
        "n2b": n2b,
        "n1": n1,
        "source": source,
        "parent_outcomes": parent_outcomes,
        "source_projection": source_projection,
        "source_coverage_daily": source_coverage,
        "cpcv": cpcv,
        "regime_daily": regime_daily,
        "parent_top5_daily": parent_top5,
        "parent_signal_daily": parent_signal,
    }


def _validate_request_source_identities(
    *,
    request: FrozenMarginInformationSetRequestV1,
    generator: Mapping[str, Any],
    n2b: Mapping[str, Any],
    n1: Mapping[str, Any],
    source: Mapping[str, Any],
) -> None:
    invalid = (
        _resolve_bound_path(request.generator_bundle_path).name != request.generator_bundle_id
        or generator["request"].request_sha256 != request.generator_request_sha256
        or generator["receipt"].receipt_sha256 != request.generator_receipt_sha256
        or _resolve_bound_path(request.n2b_bundle_path).name != request.n2b_bundle_id
        or n2b["request"].request_sha256 != request.n2b_request_sha256
        or n2b["receipt"].receipt_sha256 != request.n2b_receipt_sha256
        or _resolve_bound_path(request.n1_bundle_path).name != request.n1_bundle_id
        or n1["request"].request_sha256 != request.n1_request_sha256
        or n1["request"].split_policy_sha256 != request.n1_split_policy_sha256
        or _resolve_bound_path(request.source_bundle_path).name != request.source_bundle_id
        or source["request"].source_request_sha256 != request.source_request_sha256
        or source["receipt"].source_receipt_sha256 != request.source_receipt_sha256
        or source["receipt"].source_identity_sha256 != request.source_identity_sha256
        or source["request"].n2b_bundle_id != request.n2b_bundle_id
        or n2b["record"].dataset_identity != request.source_dataset_identity
        or generator["registry_record"].dataset_identity != request.route_dataset_identity
        or n2b["record"].policy_identity != request.policy_identity
        or generator["registry_record"].policy_identity != request.policy_identity
    )
    if invalid:
        _raise(
            "margin MVE request/source relation drift",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        )


def _validate_parent_daily_parity(
    *,
    daily: pd.DataFrame,
    parent_top5_daily: pd.DataFrame,
    parent_signal_daily: pd.DataFrame,
) -> None:
    top5 = parent_top5_daily.loc[
        :,
        ["decision_as_of_trade_date", "top5_net_excess_bps", "instruments"],
    ].copy()
    signal = parent_signal_daily.loc[
        :,
        ["decision_as_of_trade_date", "matured_rank_ic"],
    ].copy()
    for frame in (top5, signal):
        frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    expected = daily.loc[
        :,
        [
            "decision_as_of_trade_date",
            "parent_rank_ic",
            "parent_top5_net_excess_bps",
            "parent_instruments",
        ],
    ].merge(top5, on="decision_as_of_trade_date", how="left", validate="one_to_one")
    expected = expected.merge(signal, on="decision_as_of_trade_date", how="left", validate="one_to_one")
    rank_left = pd.to_numeric(expected["parent_rank_ic"], errors="coerce").to_numpy(dtype=float)
    rank_right = pd.to_numeric(expected["matured_rank_ic"], errors="coerce").to_numpy(dtype=float)
    top_left = pd.to_numeric(expected["parent_top5_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    top_right = pd.to_numeric(expected["top5_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    comparable_top5 = np.isfinite(top_left)
    instrument_match = []
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
        or not comparable_top5.any()
        or not np.allclose(
            top_left[comparable_top5],
            top_right[comparable_top5],
            atol=1e-9,
            rtol=0.0,
            equal_nan=False,
        )
        or not all(instrument_match)
    ):
        _raise(
            "margin MVE current-parent daily parity failed",
            "ADVISORY_N3_MARGIN_MVE_BASELINE_PARITY_FAILED",
        )


def _publish_mve_bundle(
    *,
    request: FrozenMarginInformationSetRequestV1,
    features: pd.DataFrame,
    oof_scores: pd.DataFrame,
    fold_diagnostics: pd.DataFrame,
    daily_metrics: pd.DataFrame,
    model_summary: Mapping[str, Any],
    stability: Mapping[str, Any],
    frontier: Mapping[str, Any],
    elapsed_seconds: float,
) -> Path:
    root = _resolve_bound_path(request.output_root) / "margin_information_set_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{request.request_id}.", dir=root))
    _write_json(temporary / "request.json", request.model_dump(mode="json"))
    _write_json(
        temporary / "source_reference.json",
        {
            "schema_version": "advisory_n3_margin_source_reference_v1",
            "source_bundle_id": request.source_bundle_id,
            "source_request_sha256": request.source_request_sha256,
            "source_receipt_sha256": request.source_receipt_sha256,
            "source_identity_sha256": request.source_identity_sha256,
            "source_quality": MARGIN_MVE_SOURCE_QUALITY,
            "target_columns_read_during_prepare": False,
            "sealed_holdout_accessed": False,
        },
    )
    _write_json(
        temporary / "feature_schema.json",
        {
            "schema_version": MARGIN_MVE_FEATURE_SCHEMA_VERSION,
            "feature_schema_hash": MARGIN_MVE_FEATURE_SCHEMA_HASH,
            "parent_features": list(MARGIN_MVE_PARENT_FEATURES),
            "membership_features": list(MARGIN_MVE_MEMBERSHIP_FEATURES),
            "expanded_features": list(MARGIN_MVE_EXPANDED_FEATURES),
            "raw_dynamics_features": list(MARGIN_MVE_RAW_DYNAMICS_FEATURES),
            "ranked_dynamics_features": list(MARGIN_MVE_RANKED_DYNAMICS_FEATURES),
            "rank_semantics": "SAME_DATE_FINITE_CANONICAL_PARENT_AVERAGE_PCT_ASCENDING",
            "normal_missing_policy": "KEEP_ALL_PARENT_KEYS_TRAIN_FOLD_MEDIAN_NO_ZERO_FILL",
            "sealed_holdout_accessed": False,
        },
    )
    features.to_parquet(temporary / "margin_feature_panel.parquet", index=False)
    oof_scores.to_parquet(temporary / "oof_score_panel.parquet", index=False)
    fold_diagnostics.to_parquet(temporary / "fold_diagnostics.parquet", index=False)
    daily_metrics.to_parquet(temporary / "daily_metrics.parquet", index=False)
    _write_json(temporary / "model_summary.json", model_summary)
    _write_json(temporary / "stability_report.json", stability)
    _write_json(temporary / "frontier_receipt.json", frontier)
    temporary_bytes = sum(path.stat().st_size for path in temporary.iterdir() if path.is_file())
    if temporary_bytes > request.resource_max_temp_bytes:
        _raise(
            "margin MVE temporary output exceeds frozen limit",
            "ADVISORY_N3_MARGIN_MVE_RESOURCE_LIMIT_EXCEEDED",
            temporary_bytes=temporary_bytes,
        )
    resource = {
        "schema_version": "advisory_n3_margin_information_set_resource_report_v1",
        "elapsed_seconds": float(elapsed_seconds),
        "peak_rss_bytes": _peak_rss_bytes(),
        "temporary_bytes": temporary_bytes,
        "resource_max_rss_bytes": request.resource_max_rss_bytes,
        "resource_max_temp_bytes": request.resource_max_temp_bytes,
        "wall_time_limit_seconds": None,
        "wall_time_is_telemetry_only": True,
        "database_reads": 0,
        "database_writes": 0,
        "network_reads": 0,
        "sealed_holdout_accessed": False,
    }
    _write_json(temporary / "resource_report.json", resource)
    result_descriptors = {
        name: descriptor for name, descriptor in _file_descriptors(temporary).items() if name in RESULT_IDENTITY_MEMBERS
    }
    selected = model_summary.get("selected_trial_id")
    eligible = ("N3_MARGIN_DYNAMICS_EXPANDED_V1",) if selected else ()
    receipt = build_margin_information_set_receipt(
        request_sha256=request.request_sha256,
        selected_trial_count=1 if selected else 0,
        selected_trial_id=selected,
        eligible_trial_ids=eligible,
        next_task=(
            "N3_MARGIN_INFORMATION_SET_CONFIRMATION_DESIGN"
            if selected
            else "N3_FINANCIAL_EVENT_SOURCE_READINESS_DESIGN"
        ),
        source_identity_sha256=request.source_identity_sha256,
        result_files_sha256=canonical_json_sha256(result_descriptors),
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
    record = _build_registry_record(
        request=request,
        receipt_path=temporary / "learnability_receipt.json",
        receipt_artifact_uri=(destination / "learnability_receipt.json").as_posix(),
        receipt=receipt,
    )
    _write_json(temporary / "registry_record.json", record.model_dump(mode="json"))
    descriptors = _file_descriptors(temporary)
    if set(descriptors) != MVE_BUNDLE_MEMBERS:
        _raise(
            "margin MVE bundle member roster drift",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
            members=sorted(descriptors),
        )
    manifest = {
        "schema_version": MVE_BUNDLE_SCHEMA,
        "bundle_id": bundle_id,
        "request_sha256": request.request_sha256,
        "receipt_sha256": receipt.receipt_sha256,
        "generator_bundle_id": request.generator_bundle_id,
        "n2b_bundle_id": request.n2b_bundle_id,
        "n1_bundle_id": request.n1_bundle_id,
        "source_bundle_id": request.source_bundle_id,
        "objective_contract": ObjectiveContract.ALPHA_RANKING.value,
        "study_type": ResearchStudyType.LEARNABILITY_AUDIT.value,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "result_class": ResearchResultClass.EXPLORATORY.value,
        "planned_trial_count": 3,
        "generated_trial_count": 3,
        "evaluated_trial_count": 3,
        "selectable_trial_count": 1,
        "selected_trial_count": receipt.selected_trial_count,
        "cumulative_candidate_index": request.cumulative_candidate_index,
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
        _read_margin_mve_bundle(destination)
        _raise(
            "margin MVE bundle destination appeared concurrently",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
            bundle_id=bundle_id,
        )
    temporary.replace(destination)
    _read_margin_mve_bundle(destination)
    return destination


def _build_registry_record(
    *,
    request: FrozenMarginInformationSetRequestV1,
    receipt_path: Path,
    receipt_artifact_uri: str,
    receipt: MarginInformationSetReceiptV1,
) -> AdvisoryResearchTrialRecordV1:
    return build_trial_record(
        experiment_id=MARGIN_MVE_EXPERIMENT_ID,
        attempt_id=request.request_id,
        research_stage="N3_MARGIN_INFORMATION_SET_MVE",
        study_type=ResearchStudyType.LEARNABILITY_AUDIT,
        hypothesis_family_id=MARGIN_MVE_HYPOTHESIS_FAMILY_ID,
        parent_lineage=(
            "ADVISORY-N1-TIER1-LEARNABILITY",
            "ADVISORY-N2B-INDEPENDENT-PACKAGE-ALPHA-AUDIT-V2",
            "ADVISORY-N3-QE-ALPHA-GENERATOR-MVE-V1",
        ),
        unique_variable="T_MINUS_1_MARGIN_FINANCING_DYNAMICS",
        objective_contract=ObjectiveContract.ALPHA_RANKING,
        dataset_identity=request.dataset_identity,
        schema_identity=request.feature_schema_hash,
        policy_identity=request.policy_identity,
        planned_trial_count=3,
        generated_trial_count=3,
        evaluated_trial_count=3,
        selected_trial_count=receipt.selected_trial_count,
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
        evidence_refs=(
            evidence_reference_for_file(
                receipt_path,
                role="n3_margin_information_set_learnability_receipt",
            ).model_copy(update={"artifact_uri": receipt_artifact_uri}),
        ),
        recorded_at=datetime.now(timezone.utc),
    )


def _read_margin_mve_bundle(path: Path) -> dict[str, Any]:
    manifest = _read_json(path / "manifest.json", "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID")
    descriptors = manifest.get("files")
    _verify_bundle_member_descriptors(
        path=path,
        descriptors=descriptors,
        expected_members=MVE_BUNDLE_MEMBERS,
        reason_code="ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
    )
    try:
        request = FrozenMarginInformationSetRequestV1.model_validate_json(
            (path / "request.json").read_text(encoding="utf-8")
        )
        receipt = MarginInformationSetReceiptV1.model_validate_json(
            (path / "learnability_receipt.json").read_text(encoding="utf-8")
        )
        record = AdvisoryResearchTrialRecordV1.model_validate_json(
            (path / "registry_record.json").read_text(encoding="utf-8")
        )
    except Exception as exc:
        _raise(
            "margin MVE bundle contract member is invalid",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
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
    frontier = _read_json(path / "frontier_receipt.json", "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID")
    frontier_functional = {key: value for key, value in frontier.items() if key != "frontier_sha256"}
    stability = _read_json(path / "stability_report.json", "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID")
    stability_functional = {key: value for key, value in stability.items() if key != "stability_sha256"}
    summary = _read_json(path / "model_summary.json", "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID")
    resource = _read_json(path / "resource_report.json", "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID")
    receipt_descriptor = descriptors["learnability_receipt.json"]
    invalid = (
        manifest.get("schema_version") != MVE_BUNDLE_SCHEMA
        or path.name != expected_bundle_id
        or manifest.get("bundle_id") != expected_bundle_id
        or manifest.get("request_sha256") != request.request_sha256
        or manifest.get("receipt_sha256") != receipt.receipt_sha256
        or receipt.request_sha256 != request.request_sha256
        or receipt.source_identity_sha256 != request.source_identity_sha256
        or receipt.result_files_sha256 != canonical_json_sha256(result_descriptors)
        or receipt.resource_report_sha256 != descriptors["resource_report.json"]["sha256"]
        or record.experiment_id != MARGIN_MVE_EXPERIMENT_ID
        or record.attempt_id != request.request_id
        or record.hypothesis_family_id != MARGIN_MVE_HYPOTHESIS_FAMILY_ID
        or record.study_type != ResearchStudyType.LEARNABILITY_AUDIT
        or record.decision_use != DecisionUse.NAVIGATION_ONLY
        or record.result_class != ResearchResultClass.EXPLORATORY
        or record.planned_trial_count != 3
        or record.generated_trial_count != 3
        or record.evaluated_trial_count != 3
        or record.selected_trial_count != receipt.selected_trial_count
        or len(record.evidence_refs) != 1
        or record.evidence_refs[0].role != "n3_margin_information_set_learnability_receipt"
        or record.evidence_refs[0].sha256 != receipt_descriptor["sha256"]
        or record.evidence_refs[0].size_bytes != receipt_descriptor["size_bytes"]
        or frontier.get("frontier_sha256") != canonical_json_sha256(frontier_functional)
        or frontier.get("selected_trial_id") != receipt.selected_trial_id
        or tuple(frontier.get("eligible_trial_ids", ())) != receipt.eligible_trial_ids
        or frontier.get("support_sufficient") != summary.get("support_sufficient")
        or frontier.get("evidence_class") != summary.get("evidence_class")
        or stability.get("stability_sha256") != canonical_json_sha256(stability_functional)
        or summary.get("selected_trial_id") != receipt.selected_trial_id
        or manifest.get("generator_bundle_id") != request.generator_bundle_id
        or manifest.get("n2b_bundle_id") != request.n2b_bundle_id
        or manifest.get("n1_bundle_id") != request.n1_bundle_id
        or manifest.get("source_bundle_id") != request.source_bundle_id
        or manifest.get("planned_trial_count") != 3
        or manifest.get("generated_trial_count") != 3
        or manifest.get("evaluated_trial_count") != 3
        or manifest.get("selectable_trial_count") != 1
        or manifest.get("selected_trial_count") != receipt.selected_trial_count
        or manifest.get("cumulative_candidate_index") != MARGIN_MVE_CUMULATIVE_CANDIDATE_INDEX
        or manifest.get("objective_contract") != ObjectiveContract.ALPHA_RANKING.value
        or manifest.get("result_class") != ResearchResultClass.EXPLORATORY.value
        or manifest.get("study_type") != ResearchStudyType.LEARNABILITY_AUDIT.value
        or manifest.get("decision_use") != DecisionUse.NAVIGATION_ONLY.value
        or manifest.get("sealed_holdout_accessed") is not False
        or manifest.get("deployable") is not False
        or manifest.get("runtime_eligible") is not False
        or manifest.get("final_model_written") is not False
        or manifest.get("factor_catalog_written") is not False
        or manifest.get("strategy_package_written") is not False
        or manifest.get("position_weight_output") is not False
        or resource.get("wall_time_limit_seconds") is not None
        or resource.get("wall_time_is_telemetry_only") is not True
        or not isinstance(resource.get("peak_rss_bytes"), int)
        or int(resource.get("peak_rss_bytes", -1)) > request.resource_max_rss_bytes
        or not isinstance(resource.get("temporary_bytes"), int)
        or int(resource.get("temporary_bytes", -1)) > request.resource_max_temp_bytes
        or resource.get("database_reads") != 0
        or resource.get("database_writes") != 0
        or resource.get("network_reads") != 0
        or resource.get("sealed_holdout_accessed") is not False
    )
    if invalid:
        _raise(
            "margin MVE bundle relational identity is invalid",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
        )
    return {
        "manifest": manifest,
        "request": request,
        "receipt": receipt,
        "record": record,
    }


def _find_existing_mve_bundle(request: FrozenMarginInformationSetRequestV1) -> Path | None:
    root = _resolve_bound_path(request.output_root) / "margin_information_set_bundles"
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
        _raise(
            "one margin MVE request maps to multiple bundles",
            "ADVISORY_N3_MARGIN_MVE_BUNDLE_INVALID",
        )
    if matches:
        _read_margin_mve_bundle(matches[0])
        return matches[0]
    return None


def _deliver_bundle(
    *,
    request: FrozenMarginInformationSetRequestV1,
    bundle_path: Path,
) -> dict[str, Any]:
    loaded = _read_margin_mve_bundle(bundle_path)
    registry = AdvisoryResearchTrialRegistryV1(_resolve_bound_path(request.registry_path)).append_batch(
        (loaded["record"],)
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
    request: FrozenMarginInformationSetRequestV1,
    receipt: MarginInformationSetReceiptV1,
    bundle_id: str,
    registry_sha256: str,
) -> dict[str, Any]:
    selected = receipt.selected_trial_id or "NONE"
    content = "\n".join(
        (
            "# Advisory 当前研究路线",
            "",
            "- active_main_line: N3_MARGIN_INFORMATION_SET_MVE",
            "- active_auxiliary_line: NONE",
            f"- next_task: {receipt.next_task}",
            f"- exploratory_candidate: {selected}",
            f"- generator_bundle_id: {request.generator_bundle_id}",
            f"- source_bundle_id: {request.source_bundle_id}",
            f"- margin_information_set_bundle_id: {bundle_id}",
            f"- cumulative_candidate_index: {request.cumulative_candidate_index}",
            f"- trial_registry_sha256: {registry_sha256}",
            "- objective_contract: ALPHA_RANKING",
            "- study_type: LEARNABILITY_AUDIT",
            "- decision_use: NAVIGATION_ONLY",
            "- sealed_holdout_accessed: false",
            "- deployable/runtime/model/factor/strategy_package/position_weight: false/false/false/false/false/false",
            "",
            "该页面只记录开发窗口learnability导航。selected=0只关闭本次十二项动态和冻结Ridge的精确frontier，"
            "不证明全部margin信息全局不可学，也不授权财务事件数据接入、回填或训练。",
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


def _verify_environment(request: FrozenMarginInformationSetRequestV1) -> None:
    _require_formal_environment()
    repository = _resolve_bound_path(request.repository_root)
    if _cross_os_git_commit(repository) != request.repository_commit:
        _raise(
            "margin MVE repository commit drift",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    dirty = _cross_os_git_dirty_paths(repository)
    if dirty:
        _raise(
            "margin MVE repository became dirty",
            "ADVISORY_N3_MARGIN_MVE_SOURCE_IDENTITY_MISMATCH",
            dirty_paths=dirty[:20],
        )
    for reference in request.evidence_refs:
        _verify_ref(reference)
    generator = _read_generator_bundle(_resolve_bound_path(request.generator_bundle_path))
    n2b = _read_n2b_bundle(_resolve_bound_path(request.n2b_bundle_path))
    n1 = _read_n1_bundle(_resolve_bound_path(request.n1_bundle_path))
    source = _read_margin_source_bundle(_resolve_bound_path(request.source_bundle_path))
    _validate_bound_sources(generator=generator, n2b=n2b, n1=n1)
    _validate_request_source_identities(
        request=request,
        generator=generator,
        n2b=n2b,
        n1=n1,
        source=source,
    )


def _run_response(
    request: FrozenMarginInformationSetRequestV1,
    bundle: Path,
    delivery: Mapping[str, Any],
    *,
    exact_retry: bool,
) -> dict[str, Any]:
    return {
        **inspect_margin_information_set_bundle(bundle),
        "request_id": request.request_id,
        "bundle_path": bundle.as_posix(),
        "source_bundle_path": request.source_bundle_path,
        "exact_retry": exact_retry,
        "registry": dict(delivery["registry"]),
        "route": dict(delivery["route"]),
    }


def _stability_report(
    daily: pd.DataFrame,
    *,
    request: FrozenMarginInformationSetRequestV1,
) -> dict[str, Any]:
    ordered = daily.sort_values("decision_as_of_trade_date").reset_index(drop=True)
    date_indexes = np.array_split(np.arange(len(ordered)), 4)
    rows: list[dict[str, Any]] = []
    positive_joint = 0
    for index, positions in enumerate(date_indexes, 1):
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
        "schema_version": "advisory_n3_margin_information_set_stability_v1",
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
    request: FrozenMarginInformationSetRequestV1,
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
    request: FrozenMarginInformationSetRequestV1,
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
    # `_moving_block_interval` returns central two-sided bounds.  Doubling the
    # registered one-sided alpha makes its lower quantile exactly `alpha`.
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
    mde = float((1.959963984540054 + 0.8416212335729143) * standard_error)
    return {
        "point_estimate": float(array.mean()),
        "confidence_lower": ordinary[0],
        "confidence_upper": ordinary[1],
        "familywise_confidence_lower": familywise[0],
        "familywise_confidence_upper": familywise[1],
        "familywise_alpha": float(alpha),
        "bootstrap_standard_error": standard_error,
        "mde": mde,
        "threshold": float(threshold),
        "observation_count": int(len(array)),
    }


def _log_delta(current: pd.Series, lagged: pd.Series) -> np.ndarray:
    left = pd.to_numeric(current, errors="coerce").to_numpy(dtype=float)
    right = pd.to_numeric(lagged, errors="coerce").to_numpy(dtype=float)
    result = np.full(len(left), np.nan, dtype=np.float64)
    valid = np.isfinite(left) & np.isfinite(right) & (left >= 0) & (right >= 0)
    result[valid] = np.log1p(left[valid]) - np.log1p(right[valid])
    return result.astype("float32")


def _strict_ratio(numerator: pd.Series, denominator: pd.Series) -> np.ndarray:
    left = pd.to_numeric(numerator, errors="coerce").to_numpy(dtype=float)
    right = pd.to_numeric(denominator, errors="coerce").to_numpy(dtype=float)
    result = np.full(len(left), np.nan, dtype=np.float64)
    valid = np.isfinite(left) & np.isfinite(right) & (left >= 0) & (right > 0)
    result[valid] = left[valid] / right[valid]
    return result.astype("float32")


def _top_ids(frame: pd.DataFrame, score_column: str) -> set[str]:
    ranked = frame.loc[:, ["instrument", score_column]].copy()
    ranked["instrument"] = ranked["instrument"].astype(str)
    ranked = ranked.sort_values(
        [score_column, "instrument"],
        ascending=[False, True],
        kind="mergesort",
    )
    if len(ranked) < 5:
        _raise("margin daily panel has fewer than five rows", "ADVISORY_N3_MARGIN_MVE_OOF_INVALID")
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


def _mean_by_day_score_correlation(
    frame: pd.DataFrame,
    left: str,
    right: str,
) -> float | None:
    values = [
        _safe_correlation(group[left], group[right], method="spearman")
        for _, group in frame.groupby("decision_as_of_trade_date", sort=True)
    ]
    finite = np.asarray([value for value in values if value is not None], dtype=float)
    return float(finite.mean()) if len(finite) else None


def _check_resource_limits(request: FrozenMarginInformationSetRequestV1, stage: str) -> None:
    rss = _peak_rss_bytes()
    if rss > request.resource_max_rss_bytes:
        _raise(
            "margin MVE resident memory exceeds frozen limit",
            "ADVISORY_N3_MARGIN_MVE_RESOURCE_LIMIT_EXCEEDED",
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
        _raise("bundle descriptor roster is invalid", reason_code)
    actual_files = {item.name for item in path.iterdir() if item.is_file()} - {"manifest.json"}
    if actual_files != expected_members:
        _raise(
            "bundle physical member roster is invalid",
            reason_code,
            members=sorted(actual_files),
        )
    for name, descriptor in descriptors.items():
        if not isinstance(descriptor, dict):
            _raise("bundle member descriptor is invalid", reason_code, member=name)
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
            "ADVISORY_N3_MARGIN_MVE_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    if len(commit) != 40 or any(value not in "0123456789abcdef" for value in commit):
        _raise(
            "origin/main commit is invalid",
            "ADVISORY_N3_MARGIN_MVE_REQUEST_INVALID",
        )
    return commit


def _write_immutable_request(
    path: Path,
    request: FrozenMarginInformationSetRequestV1,
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
            existing = FrozenMarginInformationSetRequestV1.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception as exc:
            _raise(
                "existing margin MVE request is invalid",
                "ADVISORY_N3_MARGIN_MVE_REQUEST_INVALID",
                error_type=type(exc).__name__,
            )
        if existing.request_sha256 != request.request_sha256:
            _raise(
                "margin MVE request path contains a different identity",
                "ADVISORY_N3_MARGIN_MVE_REQUEST_INVALID",
            )
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
            "margin parquet cannot be read",
            reason_code,
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )


def _read_json(path: Path, reason_code: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _raise(
            "margin JSON cannot be read",
            reason_code,
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )
    if not isinstance(payload, dict):
        _raise(
            "margin JSON root is not an object",
            reason_code,
            path=path.as_posix(),
        )
    return payload


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n",
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
        or os.environ.get("AISTOCK_ADVISORY_N3_MARGIN_FORMAL_RUN") != "1"
    ):
        _raise(
            "margin formal prepare/run requires WSL rdagent-gpu and explicit formal flag",
            "ADVISORY_N3_MARGIN_MVE_REQUEST_INVALID",
            os_name=os.name,
            conda_default_env=os.environ.get("CONDA_DEFAULT_ENV"),
        )


def _raise(message: str, reason_code: str, **context: Any) -> NoReturn:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "build_margin_feature_panel",
    "build_margin_source_coverage",
    "build_margin_source_projection",
    "evaluate_margin_models",
    "inspect_margin_information_set_bundle",
    "inspect_margin_source_bundle",
    "prepare_margin_information_set_request",
    "run_margin_crossfit",
    "run_margin_information_set_mve",
    "validate_margin_feature_support",
]
