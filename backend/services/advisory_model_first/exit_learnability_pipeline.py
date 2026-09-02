from __future__ import annotations

import json
import math
import os
import platform
import tempfile
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

try:
    import resource as _resource
except ModuleNotFoundError:  # Windows imports prepare/inspect and runs unit tests.
    _resource = None

from backend.services.advisory_model_first.alpha_signal_audit_pipeline import (
    _git_commit as _cross_os_git_commit,
)
from backend.services.advisory_model_first.alpha_signal_audit_pipeline import (
    _git_dirty_paths as _cross_os_git_dirty_paths,
)
from backend.services.advisory_model_first.entry_exit_formal_contracts import (
    AdvisoryN2ActionAuditReceiptV1,
    FrozenAdvisoryN2ActionAuditRequestV1,
)
from backend.services.advisory_model_first.entry_exit_formal_pipeline import (
    inspect_n2_action_audit_bundle,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.exit_learnability_contracts import (
    EXIT_CATEGORICAL_FEATURE_COLUMNS,
    EXIT_FEATURE_COLUMNS,
    EXIT_FEATURE_SCHEMA_VERSION,
    EXIT_LEARNABILITY_EXPERIMENT_ID,
    EXIT_LEARNABILITY_FAMILY_ID,
    AdvisoryN2ExitLearnabilityReceiptV1,
    ExitLearnabilitySupportV1,
    FrozenAdvisoryN2ExitLearnabilityRequestV1,
    build_exit_learnability_receipt,
    build_exit_learnability_request,
)
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.qe_file_source import initialize_qlib, load_qlib_daily
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    evidence_reference_for_file,
    generate_current_route,
    research_policy_identity,
)
from backend.services.advisory_model_first.research_control_contracts import (
    ConsumedWindowV1,
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
)
from backend.services.advisory_model_first.tier1_oracle_contracts import (
    AdvisoryN1Tier1RequestV1,
    Tier1EvidenceState,
    Tier1MetricInferenceV1,
)
from backend.services.advisory_model_first.tier1_oracle_pipeline import (
    build_tier1_benchmark_regimes,
    inspect_n1_bundle,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


EXIT_LEARNABILITY_BUNDLE_SCHEMA = "advisory_n2_exit_learnability_bundle_v1"
RESULT_IDENTITY_MEMBERS = frozenset(
    {
        "feature_schema.json",
        "features.parquet",
        "oof_predictions.parquet",
        "episode_policy.parquet",
        "daily_policy.parquet",
    }
)
BUNDLE_MEMBERS = RESULT_IDENTITY_MEMBERS | {
    "request.json",
    "source_identity_receipt.json",
    "learnability_receipt.json",
    "resource_report.json",
    "registry_record.json",
}
_Z_975 = 1.959963984540054
_Z_80 = 0.8416212335729143


def prepare_exit_learnability_request(
    *,
    n2_action_request_path: str | Path,
    n2_action_bundle_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
) -> FrozenAdvisoryN2ExitLearnabilityRequestV1:
    action_request_path = Path(n2_action_request_path).resolve()
    action_bundle = Path(n2_action_bundle_path).resolve()
    repo = Path(repository_root).resolve()
    action_request = FrozenAdvisoryN2ActionAuditRequestV1.model_validate_json(
        action_request_path.read_text(encoding="utf-8")
    )
    action_inspection = inspect_n2_action_audit_bundle(action_bundle)
    action_receipt = AdvisoryN2ActionAuditReceiptV1.model_validate_json(
        (action_bundle / "audit_receipt.json").read_text(encoding="utf-8")
    )
    _validate_action_source_summary(
        action_request=action_request,
        action_inspection=action_inspection,
        action_receipt=action_receipt,
    )
    n1_request_path = Path(action_request.n1_request_path).resolve()
    n1_bundle = Path(action_request.n1_bundle_path).resolve()
    n1_request = AdvisoryN1Tier1RequestV1.model_validate_json(n1_request_path.read_text(encoding="utf-8"))
    n1_inspection = inspect_n1_bundle(n1_bundle)
    if n1_inspection["request_sha256"] != n1_request.request_sha256:
        _raise(
            "N1 request/bundle identity mismatch",
            "ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH",
        )
    policy_root = Path(n1_request.policy_dataset_bundle_root).resolve()
    policy_manifest_path = policy_root / "manifest.json"
    policy_manifest = _read_json(
        policy_manifest_path,
        reason_code="ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH",
    )
    if policy_manifest.get("policy_dataset_bundle_id") != action_request.dataset_identity:
        _raise(
            "policy dataset identity differs from N2 action request",
            "ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH",
        )
    commit = _repository_commit(repo)
    dirty = _repository_dirty(repo)
    if dirty:
        _raise(
            "Exit learnability request requires a clean repository",
            "ADVISORY_EXIT_LEARNABILITY_REQUEST_INVALID",
            dirty_paths=dirty[:20],
        )
    feature_schema_hash = canonical_json_sha256(
        {
            "feature_schema_version": EXIT_FEATURE_SCHEMA_VERSION,
            "feature_columns": list(EXIT_FEATURE_COLUMNS),
            "categorical_columns": list(EXIT_CATEGORICAL_FEATURE_COLUMNS),
        }
    )
    request = build_exit_learnability_request(
        n2_action_request_path=action_request_path.as_posix(),
        n2_action_request_ref=evidence_reference_for_file(
            action_request_path,
            role="exit_learnability_n2_action_request",
        ),
        n2_action_bundle_path=action_bundle.as_posix(),
        n2_action_manifest_ref=evidence_reference_for_file(
            action_bundle / "manifest.json",
            role="exit_learnability_n2_action_manifest",
        ),
        n2_action_receipt_ref=evidence_reference_for_file(
            action_bundle / "audit_receipt.json",
            role="exit_learnability_n2_action_receipt",
        ),
        exit_labels_ref=evidence_reference_for_file(
            action_bundle / "exit_labels.parquet",
            role="exit_learnability_exit_labels",
        ),
        exit_decisions_ref=evidence_reference_for_file(
            action_bundle / "exit_decisions.parquet",
            role="exit_learnability_exit_decisions",
        ),
        exit_episode_best_ref=evidence_reference_for_file(
            action_bundle / "exit_episode_best.parquet",
            role="exit_learnability_exit_episode_best",
        ),
        n1_request_path=n1_request_path.as_posix(),
        n1_request_ref=evidence_reference_for_file(
            n1_request_path,
            role="exit_learnability_n1_request",
        ),
        n1_bundle_path=n1_bundle.as_posix(),
        n1_manifest_ref=evidence_reference_for_file(
            n1_bundle / "manifest.json",
            role="exit_learnability_n1_manifest",
        ),
        policy_dataset_root=policy_root.as_posix(),
        policy_dataset_manifest_ref=evidence_reference_for_file(
            policy_manifest_path,
            role="exit_learnability_policy_dataset_manifest",
        ),
        candidate_episode_labels_ref=evidence_reference_for_file(
            policy_root / "candidate_episode_labels.parquet",
            role="exit_learnability_candidate_episode_labels",
        ),
        cpcv_paths_ref=evidence_reference_for_file(
            policy_root / "cpcv_paths.json",
            role="exit_learnability_cpcv_paths",
        ),
        parent_spike_path=action_request.parent_spike_path,
        parent_spike_ref=_reference_with_role(
            action_request.parent_spike_ref,
            "exit_learnability_parent_spike",
        ),
        research_window_contract_ref=action_request.research_window_contract_ref,
        registry_path=action_request.registry_path,
        route_path=action_request.route_path,
        dataset_identity=action_request.dataset_identity,
        parent_feature_schema_hash=action_request.feature_schema_hash,
        feature_schema_hash=feature_schema_hash,
        baseline_policy_sha256=action_request.baseline_policy_sha256,
        shadow_policy_sha256=action_request.shadow_policy_sha256,
        cost_policy_sha256=action_request.cost_policy_sha256,
        intervention_policy_sha256=action_request.exit_intervention_policy_sha256,
        decision_start=action_request.exit_decision_start,
        decision_end=action_request.exit_decision_end,
        outcome_cutoff=action_request.outcome_cutoff,
        qlib_daily_root=action_request.qlib_daily_root,
        repository_root=repo.as_posix(),
        repository_commit=commit,
        output_root=Path(output_root).resolve().as_posix(),
    )
    _write_immutable_request(Path(output_path), request)
    return request


def run_exit_learnability_audit(request_path: str | Path) -> dict[str, Any]:
    started = time.monotonic()
    request = FrozenAdvisoryN2ExitLearnabilityRequestV1.model_validate_json(
        Path(request_path).read_text(encoding="utf-8")
    )
    _verify_environment(request)
    existing = _find_existing_bundle(request)
    if existing is not None:
        delivery = _deliver_bundle(request=request, bundle_path=existing)
        return _run_response(request, existing, delivery, exact_retry=True)
    sources = _load_verified_sources(request)
    _check_rss(request, "sources_loaded")
    initialize_qlib(request.qlib_daily_root)
    metadata = _build_episode_metadata(sources)
    symbols = sorted(metadata["instrument"].astype(str).str.upper().unique())
    market_start = (pd.Timestamp(metadata["entry_trade_date"].min()) - pd.Timedelta(days=60)).date().isoformat()
    daily = load_qlib_daily(
        symbols,
        start=market_start,
        end=request.outcome_cutoff.isoformat(),
    )
    benchmark = load_qlib_daily(
        [sources["n1_request"].cost_policy.benchmark_instrument],
        start=market_start,
        end=request.outcome_cutoff.isoformat(),
        fields=("$open", "$close"),
    )
    features = build_exit_feature_matrix(
        exit_labels=sources["exit_labels"],
        episode_metadata=metadata,
        daily=daily,
        benchmark_daily=benchmark,
        request=request,
    )
    _check_rss(request, "features_built")
    oof = run_exit_crossfit(
        features=features,
        exit_labels=sources["exit_labels"],
        cpcv_payload=sources["cpcv_payload"],
        request=request,
    )
    episode_policy, daily_policy, inference, support, diagnostics = evaluate_exit_policy(
        oof=oof,
        benchmark_daily=benchmark,
        oracle_summary=sources["exit_summary"],
        request=request,
    )
    _check_rss(request, "policy_evaluated")
    bundle = _publish_bundle(
        request=request,
        sources=sources,
        features=features,
        oof=oof,
        episode_policy=episode_policy,
        daily_policy=daily_policy,
        inference=inference,
        support=support,
        diagnostics=diagnostics,
        elapsed_seconds=time.monotonic() - started,
    )
    delivery = _deliver_bundle(request=request, bundle_path=bundle)
    return _run_response(request, bundle, delivery, exact_retry=False)


def inspect_exit_learnability_bundle(bundle_path: str | Path) -> dict[str, Any]:
    loaded = _read_bundle(Path(bundle_path).resolve())
    return {
        "status": "VALID",
        "bundle_id": loaded["manifest"]["bundle_id"],
        "request_id": loaded["request"].request_id,
        "receipt_id": loaded["receipt"].receipt_id,
        "evidence_state": loaded["receipt"].policy_lift.evidence_state.value,
        "evidence_sufficient": loaded["receipt"].evidence_sufficient,
        "decision_use": loaded["receipt"].decision_use.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "planned_trial_count": 1,
        "generated_trial_count": 1,
        "evaluated_trial_count": 1,
        "selected_trial_count": 0,
    }


def build_exit_feature_matrix(
    *,
    exit_labels: pd.DataFrame,
    episode_metadata: pd.DataFrame,
    daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    request: FrozenAdvisoryN2ExitLearnabilityRequestV1,
) -> pd.DataFrame:
    required_labels = {
        "label_id",
        "episode_id",
        "decision_date",
        "target_action_date",
        "instrument",
        "status",
        "incremental_net_value_bps",
        "baseline_policy_sha256",
        "intervention_policy_sha256",
        "cost_policy_sha256",
    }
    required_meta = {
        "episode_id",
        "entry_decision_date",
        "entry_trade_date",
        "instrument",
        "entry_price",
        "selection_rank",
        "selection_score",
    }
    missing = (required_labels - set(exit_labels)) | (required_meta - set(episode_metadata))
    if missing:
        _raise(
            "Exit feature inputs omit required columns",
            "ADVISORY_EXIT_LEARNABILITY_FEATURE_LEAKAGE",
            missing_columns=sorted(missing),
        )
    labels = exit_labels.copy()
    labels["decision_date"] = pd.to_datetime(labels["decision_date"]).dt.normalize()
    labels["target_action_date"] = pd.to_datetime(labels["target_action_date"]).dt.normalize()
    labels["instrument"] = labels["instrument"].astype(str).str.upper()
    metadata = episode_metadata.copy()
    metadata["entry_decision_date"] = pd.to_datetime(metadata["entry_decision_date"]).dt.normalize()
    metadata["entry_trade_date"] = pd.to_datetime(metadata["entry_trade_date"]).dt.normalize()
    metadata["instrument"] = metadata["instrument"].astype(str).str.upper()
    if labels.duplicated(["episode_id", "decision_date"]).any() or metadata.duplicated("episode_id").any():
        _raise(
            "Exit feature identity keys are duplicated",
            "ADVISORY_EXIT_LEARNABILITY_FEATURE_LEAKAGE",
        )
    matrix = labels.merge(
        metadata[list(required_meta)],
        on=["episode_id", "instrument"],
        how="left",
        validate="many_to_one",
    )
    if matrix["entry_trade_date"].isna().any() or len(matrix) != len(labels):
        _raise(
            "Exit labels do not have complete episode metadata",
            "ADVISORY_EXIT_LEARNABILITY_COVERAGE_INSUFFICIENT",
            label_rows=len(labels),
            matched_rows=int(matrix["entry_trade_date"].notna().sum()),
        )
    for field, expected in (
        ("baseline_policy_sha256", request.shadow_policy_sha256),
        ("intervention_policy_sha256", request.intervention_policy_sha256),
        ("cost_policy_sha256", request.cost_policy_sha256),
    ):
        if not matrix[field].astype(str).eq(expected).all():
            _raise(
                "Exit labels differ from frozen policy identity",
                "ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH",
                field=field,
            )
    stock = _normalize_market(daily)
    benchmark = _normalize_market(benchmark_daily)
    benchmark_instruments = benchmark["instrument"].unique()
    if len(benchmark_instruments) != 1:
        _raise(
            "Exit learnability requires exactly one benchmark instrument",
            "ADVISORY_EXIT_LEARNABILITY_FEATURE_LEAKAGE",
        )
    benchmark_series = benchmark.set_index("datetime").sort_index()
    action_dates = sorted(matrix["decision_date"].unique())
    regimes = build_tier1_benchmark_regimes(
        benchmark_daily,
        [pd.Timestamp(value) for value in action_dates],
    )
    stock_by_symbol = {
        str(symbol): group.set_index("datetime").sort_index()
        for symbol, group in stock.groupby("instrument", sort=False)
    }
    benchmark_dates = pd.DatetimeIndex(benchmark_series.index).normalize()
    benchmark_positions = {value: index for index, value in enumerate(benchmark_dates)}
    rows: list[dict[str, Any]] = []
    for item in matrix.sort_values(["entry_decision_date", "episode_id", "decision_date"]).itertuples(index=False):
        review = pd.Timestamp(item.decision_date).normalize()
        entry = pd.Timestamp(item.entry_trade_date).normalize()
        if review < entry:
            _raise(
                "Exit review precedes entry",
                "ADVISORY_EXIT_LEARNABILITY_FEATURE_LEAKAGE",
                episode_id=str(item.episode_id),
            )
        symbol_frame = stock_by_symbol.get(str(item.instrument))
        stock_features = _stock_features_at(
            symbol_frame=symbol_frame,
            entry=entry,
            review=review,
            entry_price=_finite(item.entry_price),
        )
        entry_position = benchmark_positions.get(entry)
        review_position = benchmark_positions.get(review)
        elapsed = (
            review_position - entry_position + 1
            if entry_position is not None and review_position is not None and review_position >= entry_position
            else None
        )
        benchmark_relative = _relative_return_bps(
            benchmark=benchmark_series,
            entry=entry,
            review=review,
            stock_unrealized_bps=stock_features["unrealized_close_return_bps"],
        )
        row = {
            "label_id": str(item.label_id),
            "episode_id": str(item.episode_id),
            "entry_decision_date": pd.Timestamp(item.entry_decision_date).normalize(),
            "entry_trade_date": entry,
            "review_decision_date": review,
            "target_action_date": pd.Timestamp(item.target_action_date).normalize(),
            "instrument": str(item.instrument),
            "label_status": _enum_value(item.status),
            "selection_rank": float(item.selection_rank),
            "selection_score": _finite(item.selection_score),
            "holding_trading_days_elapsed": float(elapsed) if elapsed is not None else None,
            "holding_fraction_of_time_stop": float(elapsed / 20.0) if elapsed is not None else None,
            **stock_features,
            "relative_return_since_entry_bps": benchmark_relative,
            "market_regime": regimes.get(review, "UNAVAILABLE"),
        }
        row["missing_numeric_feature_count"] = sum(
            _finite(row[column]) is None
            for column in EXIT_FEATURE_COLUMNS
            if column not in EXIT_CATEGORICAL_FEATURE_COLUMNS
        )
        rows.append(row)
    result = pd.DataFrame(rows)
    expected_columns = {
        "label_id",
        "episode_id",
        "entry_decision_date",
        "entry_trade_date",
        "review_decision_date",
        "target_action_date",
        "instrument",
        "label_status",
        "missing_numeric_feature_count",
        *EXIT_FEATURE_COLUMNS,
    }
    if set(result) != expected_columns or len(result) != len(labels):
        _raise(
            "Exit feature matrix schema/coverage drift",
            "ADVISORY_EXIT_LEARNABILITY_FEATURE_LEAKAGE",
            unexpected_columns=sorted(set(result) - expected_columns),
            missing_columns=sorted(expected_columns - set(result)),
        )
    if result["market_regime"].isin(["UP_OR_FLAT", "DOWN"]).all() is False:
        _raise(
            "Exit feature matrix has unavailable benchmark regimes",
            "ADVISORY_EXIT_LEARNABILITY_COVERAGE_INSUFFICIENT",
        )
    return result.sort_values(["entry_decision_date", "episode_id", "review_decision_date"]).reset_index(drop=True)


def run_exit_crossfit(
    *,
    features: pd.DataFrame,
    exit_labels: pd.DataFrame,
    cpcv_payload: Mapping[str, Any],
    request: FrozenAdvisoryN2ExitLearnabilityRequestV1,
) -> pd.DataFrame:
    try:
        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import Ridge
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder, StandardScaler
    except ImportError as exc:
        _raise(
            "scikit-learn is unavailable for Exit learnability",
            "ADVISORY_EXIT_LEARNABILITY_CROSSFIT_INVALID",
            error_type=type(exc).__name__,
        )
    forbidden = {
        "incremental_net_value_bps",
        "baseline_net_value_bps",
        "action_net_value_bps",
        "baseline_effective_exit_date",
        "oracle_action",
        "realized_oracle_lift_bps",
    }
    if forbidden & set(EXIT_FEATURE_COLUMNS):
        _raise(
            "future outcome leaked into Exit feature roster",
            "ADVISORY_EXIT_LEARNABILITY_FEATURE_LEAKAGE",
        )
    labels = exit_labels[["label_id", "incremental_net_value_bps", "status"]].copy()
    labels["label_status_from_source"] = labels["status"].map(_enum_value)
    labels = labels.drop(columns="status")
    matrix = features.merge(labels, on="label_id", how="inner", validate="one_to_one")
    if len(matrix) != len(features):
        _raise(
            "Exit features and labels do not have exact coverage",
            "ADVISORY_EXIT_LEARNABILITY_CROSSFIT_INVALID",
        )
    if not matrix["label_status"].eq(matrix["label_status_from_source"]).all():
        _raise(
            "Exit feature label status drift",
            "ADVISORY_EXIT_LEARNABILITY_CROSSFIT_INVALID",
        )
    paths = [item for item in cpcv_payload.get("paths", ()) if item.get("status") == "READY"]
    if len(paths) != request.model_spec.expected_ready_path_count:
        _raise(
            "Exit learnability requires all 28 READY CPCV paths",
            "ADVISORY_EXIT_LEARNABILITY_CROSSFIT_INVALID",
            ready_path_count=len(paths),
        )
    categorical = list(EXIT_CATEGORICAL_FEATURE_COLUMNS)
    numeric = [column for column in EXIT_FEATURE_COLUMNS if column not in categorical]
    predictions: list[pd.DataFrame] = []
    for path in paths:
        train_dates = pd.DatetimeIndex(pd.to_datetime(path.get("train_dates", ()))).normalize()
        validation_dates = pd.DatetimeIndex(pd.to_datetime(path.get("validation_dates", ()))).normalize()
        if not len(train_dates) or not len(validation_dates) or set(train_dates) & set(validation_dates):
            _raise(
                "Exit CPCV train/validation date identity is invalid",
                "ADVISORY_EXIT_LEARNABILITY_CROSSFIT_INVALID",
                path_id=path.get("path_id"),
            )
        train = matrix[
            matrix["entry_decision_date"].isin(train_dates)
            & matrix["label_status"].map(_enum_value).eq("AVAILABLE")
            & pd.to_numeric(matrix["incremental_net_value_bps"], errors="coerce").notna()
        ]
        validation = matrix[matrix["entry_decision_date"].isin(validation_dates)]
        if train.empty or validation.empty:
            _raise(
                "Exit CPCV path has no train or validation rows",
                "ADVISORY_EXIT_LEARNABILITY_CROSSFIT_INVALID",
                path_id=path.get("path_id"),
            )
        if set(train["episode_id"]) & set(validation["episode_id"]):
            _raise(
                "Exit episode crosses CPCV train/validation sides",
                "ADVISORY_EXIT_LEARNABILITY_CROSSFIT_INVALID",
                path_id=path.get("path_id"),
            )
        numeric_pipeline = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median", keep_empty_features=True)),
                ("scaler", StandardScaler()),
            ]
        )
        categorical_pipeline = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="most_frequent")),
                (
                    "encoder",
                    OneHotEncoder(
                        handle_unknown="ignore",
                        sparse_output=False,
                        dtype=np.float64,
                    ),
                ),
            ]
        )
        model = Pipeline(
            steps=[
                (
                    "preprocess",
                    ColumnTransformer(
                        transformers=[
                            ("numeric", numeric_pipeline, numeric),
                            ("categorical", categorical_pipeline, categorical),
                        ],
                        sparse_threshold=0.0,
                    ),
                ),
                (
                    "ridge",
                    Ridge(
                        alpha=request.model_spec.alpha,
                        solver=request.model_spec.solver,
                        fit_intercept=request.model_spec.fit_intercept,
                    ),
                ),
            ]
        )
        model.fit(train[list(EXIT_FEATURE_COLUMNS)], train["incremental_net_value_bps"].astype(float))
        predicted = model.predict(validation[list(EXIT_FEATURE_COLUMNS)])
        if not np.isfinite(predicted).all():
            _raise(
                "Exit Ridge produced non-finite OOF values",
                "ADVISORY_EXIT_LEARNABILITY_CROSSFIT_INVALID",
                path_id=path.get("path_id"),
            )
        one = validation[
            [
                "label_id",
                "episode_id",
                "entry_decision_date",
                "review_decision_date",
                "target_action_date",
                "instrument",
            ]
        ].copy()
        one["path_id"] = str(path["path_id"])
        one["predicted_exit_advantage_bps"] = predicted
        predictions.append(one)
    raw = pd.concat(predictions, ignore_index=True)
    counts = raw.groupby("label_id").size()
    expected = request.model_spec.expected_oof_predictions_per_row
    if len(counts) != len(matrix) or not (counts == expected).all():
        _raise(
            "Exit OOF multiplicity differs from frozen CPCV",
            "ADVISORY_EXIT_LEARNABILITY_CROSSFIT_INVALID",
            row_count=len(counts),
            min_count=int(counts.min()) if len(counts) else 0,
            max_count=int(counts.max()) if len(counts) else 0,
        )
    identity = [
        "label_id",
        "episode_id",
        "entry_decision_date",
        "review_decision_date",
        "target_action_date",
        "instrument",
    ]
    oof = (
        raw.groupby(identity, as_index=False)
        .agg(
            predicted_exit_advantage_bps=("predicted_exit_advantage_bps", "mean"),
            oof_prediction_count=("path_id", "count"),
        )
        .merge(
            matrix[["label_id", "incremental_net_value_bps", "label_status"]],
            on="label_id",
            how="left",
            validate="one_to_one",
        )
    )
    return oof.sort_values(["entry_decision_date", "episode_id", "review_decision_date"]).reset_index(drop=True)


def evaluate_exit_policy(
    *,
    oof: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    oracle_summary: Mapping[str, Any],
    request: FrozenAdvisoryN2ExitLearnabilityRequestV1,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    Tier1MetricInferenceV1,
    ExitLearnabilitySupportV1,
    dict[str, Any],
]:
    threshold = request.inference_spec.exit_threshold_bps
    episode_rows: list[dict[str, Any]] = []
    for episode_id, group in oof.groupby("episode_id", sort=False):
        ordered = group.sort_values(["review_decision_date", "target_action_date", "label_id"])
        available = ordered[
            ordered["label_status"].map(_enum_value).eq("AVAILABLE")
            & pd.to_numeric(ordered["incremental_net_value_bps"], errors="coerce").notna()
        ]
        eligible = available[available["predicted_exit_advantage_bps"].astype(float).gt(threshold)]
        selected = eligible.iloc[0] if len(eligible) else None
        first = ordered.iloc[0]
        episode_rows.append(
            {
                "episode_id": str(episode_id),
                "entry_decision_date": pd.Timestamp(first["entry_decision_date"]).normalize(),
                "instrument": str(first["instrument"]),
                "evaluable": not available.empty,
                "intervened": selected is not None,
                "selected_label_id": None if selected is None else str(selected["label_id"]),
                "selected_review_decision_date": (
                    pd.NaT if selected is None else pd.Timestamp(selected["review_decision_date"]).normalize()
                ),
                "selected_target_action_date": (
                    pd.NaT if selected is None else pd.Timestamp(selected["target_action_date"]).normalize()
                ),
                "selected_predicted_exit_advantage_bps": (
                    None if selected is None else float(selected["predicted_exit_advantage_bps"])
                ),
                "realized_incremental_net_value_bps": (
                    None
                    if available.empty
                    else 0.0
                    if selected is None
                    else float(selected["incremental_net_value_bps"])
                ),
                "review_row_count": len(ordered),
            }
        )
    episode_policy = (
        pd.DataFrame(episode_rows).sort_values(["entry_decision_date", "episode_id"]).reset_index(drop=True)
    )
    daily_rows: list[dict[str, Any]] = []
    for entry_day, group in episode_policy.groupby("entry_decision_date", sort=True):
        if len(group) != 5 or not group["evaluable"].astype(bool).all():
            continue
        values = group["realized_incremental_net_value_bps"].astype(float)
        daily_rows.append(
            {
                "entry_decision_date": pd.Timestamp(entry_day).normalize(),
                "episode_count": 5,
                "intervention_episode_count": int(group["intervened"].sum()),
                "policy_lift_bps": float(values.mean()),
            }
        )
    daily_policy = pd.DataFrame(daily_rows)
    if len(daily_policy) < 60:
        _raise(
            "Exit policy has too few complete five-slot entry days",
            "ADVISORY_EXIT_LEARNABILITY_COVERAGE_INSUFFICIENT",
            complete_day_count=len(daily_policy),
        )
    inference = _infer_daily_lift(daily_policy["policy_lift_bps"].to_numpy(dtype=float), request)
    available_oof = oof[
        oof["label_status"].map(_enum_value).eq("AVAILABLE")
        & pd.to_numeric(oof["incremental_net_value_bps"], errors="coerce").notna()
    ]
    all_action_dates = pd.DatetimeIndex(pd.to_datetime(available_oof["review_decision_date"]).unique()).normalize()
    intervention_dates = (
        pd.DatetimeIndex(
            pd.to_datetime(
                episode_policy.loc[
                    episode_policy["intervened"].astype(bool),
                    "selected_review_decision_date",
                ]
            ).dropna()
        )
        .normalize()
        .unique()
    )
    regimes = build_tier1_benchmark_regimes(benchmark_daily, list(all_action_dates))
    regime_counts = {
        regime: int(sum(regimes.get(value) == regime for value in intervention_dates))
        for regime in request.support_spec.required_regimes
    }
    effective_blocks = math.ceil(len(intervention_dates) / request.support_spec.block_length_trading_days)
    reasons: list[str] = []
    evaluable_episode_policy = episode_policy[episode_policy["evaluable"].astype(bool)]
    intervention_count = int(evaluable_episode_policy["intervened"].sum())
    action_fraction = len(intervention_dates) / len(all_action_dates)
    if intervention_count < request.support_spec.minimum_intervention_count:
        reasons.append("EXPLORATORY_INSUFFICIENT_INTERVENTION_COUNT")
    if action_fraction < request.support_spec.minimum_intervention_day_fraction:
        reasons.append("EXPLORATORY_INSUFFICIENT_INTERVENTION_DAY_FRACTION")
    for regime in request.support_spec.required_regimes:
        if regime_counts[regime] < request.support_spec.minimum_days_per_required_regime:
            reasons.append(f"EXPLORATORY_INSUFFICIENT_{regime}_SUPPORT")
    if effective_blocks < request.support_spec.minimum_effective_intervention_block_count:
        reasons.append("EXPLORATORY_INSUFFICIENT_EFFECTIVE_BLOCKS")
    support = ExitLearnabilitySupportV1(
        evaluated_episode_count=len(evaluable_episode_policy),
        evaluated_entry_day_count=len(daily_policy),
        evaluated_action_day_count=len(all_action_dates),
        intervention_episode_count=intervention_count,
        intervention_action_day_count=len(intervention_dates),
        intervention_action_day_fraction=action_fraction,
        intervention_days_by_regime=regime_counts,
        effective_intervention_block_count=effective_blocks,
        support_sufficient=not reasons,
        reason_codes=tuple(sorted(reasons)),
    )
    actual = available_oof["incremental_net_value_bps"].astype(float)
    predicted = available_oof["predicted_exit_advantage_bps"].astype(float)
    intervened = episode_policy[episode_policy["intervened"].astype(bool)]
    oracle_mean = float(oracle_summary.get("mean_oracle_lift_bps") or 0.0)
    policy_mean = float(daily_policy["policy_lift_bps"].mean())
    diagnostics = {
        "row": {
            "pearson": _correlation(predicted, actual, method="pearson"),
            "spearman": _correlation(predicted, actual, method="spearman"),
            "directional_hit_fraction": float(((predicted > 0) == (actual > 0)).mean()),
        },
        "episode": {
            "mean_realized_lift_bps": float(evaluable_episode_policy["realized_incremental_net_value_bps"].mean()),
            "median_realized_lift_bps": float(evaluable_episode_policy["realized_incremental_net_value_bps"].median()),
            "positive_fraction": float((evaluable_episode_policy["realized_incremental_net_value_bps"] > 0).mean()),
            "negative_fraction": float((evaluable_episode_policy["realized_incremental_net_value_bps"] < 0).mean()),
            "tail_5pct_bps": float(evaluable_episode_policy["realized_incremental_net_value_bps"].quantile(0.05)),
            "intervened_mean_realized_lift_bps": (
                float(intervened["realized_incremental_net_value_bps"].mean()) if len(intervened) else None
            ),
        },
        "daily": {
            "mean_lift_bps": policy_mean,
            "cumulative_lift": _cumulative_return(daily_policy["policy_lift_bps"]),
            "max_drawdown_proxy": _max_drawdown(daily_policy["policy_lift_bps"]),
            "tail_5pct_bps": float(daily_policy["policy_lift_bps"].quantile(0.05)),
        },
        "oracle_mean_lift_bps": oracle_mean,
        "oracle_capture_ratio": policy_mean / oracle_mean if oracle_mean > 0 else None,
    }
    return episode_policy, daily_policy, inference, support, diagnostics


def _build_episode_metadata(sources: Mapping[str, Any]) -> pd.DataFrame:
    best = sources["exit_episode_best"].copy()
    candidates = sources["candidate_episode_labels"].copy()
    candidates = candidates[candidates["selection_rank"].le(5)].copy()
    for frame in (best, candidates):
        frame["entry_trade_date"] = pd.to_datetime(frame["entry_trade_date"]).dt.normalize()
        frame["instrument"] = frame["instrument"].astype(str).str.upper()
    keys = ["entry_trade_date", "instrument", "selection_rank"]
    if best.duplicated(keys).any() or candidates.duplicated(keys).any():
        _raise(
            "Exit episode metadata join keys are duplicated",
            "ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH",
        )
    metadata = best[["episode_id", *keys]].merge(
        candidates[
            [
                *keys,
                "decision_as_of_trade_date",
                "entry_price",
                "selection_score",
                "label_status",
            ]
        ],
        on=keys,
        how="left",
        validate="one_to_one",
    )
    if metadata["decision_as_of_trade_date"].isna().any():
        _raise(
            "Exit episode metadata does not map to the frozen Top5 baseline",
            "ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH",
        )
    metadata = metadata.rename(columns={"decision_as_of_trade_date": "entry_decision_date"})
    return metadata


def _validate_action_source_summary(
    *,
    action_request: FrozenAdvisoryN2ActionAuditRequestV1,
    action_inspection: Mapping[str, Any],
    action_receipt: AdvisoryN2ActionAuditReceiptV1,
) -> None:
    if (
        action_inspection.get("status") != "VALID"
        or action_inspection.get("request_sha256") != action_request.request_sha256
        or action_inspection.get("receipt_sha256") != action_receipt.receipt_sha256
        or action_receipt.request_sha256 != action_request.request_sha256
        or action_inspection.get("sealed_holdout_accessed") is not False
        or action_inspection.get("deployable") is not False
        or action_receipt.sealed_holdout_accessed
        or action_receipt.deployable
    ):
        _raise(
            "N2 action request/bundle public inspection identity mismatch",
            "ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH",
        )


def _load_verified_sources(
    request: FrozenAdvisoryN2ExitLearnabilityRequestV1,
) -> dict[str, Any]:
    refs = (
        (request.n2_action_request_path, request.n2_action_request_ref),
        (Path(request.n2_action_bundle_path) / "manifest.json", request.n2_action_manifest_ref),
        (Path(request.n2_action_bundle_path) / "audit_receipt.json", request.n2_action_receipt_ref),
        (Path(request.n2_action_bundle_path) / "exit_labels.parquet", request.exit_labels_ref),
        (Path(request.n2_action_bundle_path) / "exit_decisions.parquet", request.exit_decisions_ref),
        (Path(request.n2_action_bundle_path) / "exit_episode_best.parquet", request.exit_episode_best_ref),
        (request.n1_request_path, request.n1_request_ref),
        (Path(request.n1_bundle_path) / "manifest.json", request.n1_manifest_ref),
        (Path(request.policy_dataset_root) / "manifest.json", request.policy_dataset_manifest_ref),
        (
            Path(request.policy_dataset_root) / "candidate_episode_labels.parquet",
            request.candidate_episode_labels_ref,
        ),
        (Path(request.policy_dataset_root) / "cpcv_paths.json", request.cpcv_paths_ref),
        (request.parent_spike_path, request.parent_spike_ref),
        (request.research_window_contract_ref.artifact_uri, request.research_window_contract_ref),
    )
    for path, ref in refs:
        _verify_ref(path, ref)
    action_request = FrozenAdvisoryN2ActionAuditRequestV1.model_validate_json(
        Path(request.n2_action_request_path).read_text(encoding="utf-8")
    )
    action = inspect_n2_action_audit_bundle(request.n2_action_bundle_path)
    action_receipt = AdvisoryN2ActionAuditReceiptV1.model_validate_json(
        (Path(request.n2_action_bundle_path) / "audit_receipt.json").read_text(encoding="utf-8")
    )
    _validate_action_source_summary(
        action_request=action_request,
        action_inspection=action,
        action_receipt=action_receipt,
    )
    n1_request = AdvisoryN1Tier1RequestV1.model_validate_json(Path(request.n1_request_path).read_text(encoding="utf-8"))
    n1 = inspect_n1_bundle(request.n1_bundle_path)
    if (
        action_request.dataset_identity != request.dataset_identity
        or action_request.feature_schema_hash != request.parent_feature_schema_hash
        or action_request.shadow_policy_sha256 != request.shadow_policy_sha256
        or action_request.cost_policy_sha256 != request.cost_policy_sha256
        or action_request.exit_intervention_policy_sha256 != request.intervention_policy_sha256
        or n1["request_sha256"] != n1_request.request_sha256
        or n1_request.policy_dataset_bundle_id != request.dataset_identity
    ):
        _raise(
            "Exit learnability source identities differ from the frozen request",
            "ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH",
        )
    exit_labels = pd.read_parquet(Path(request.n2_action_bundle_path) / "exit_labels.parquet")
    exit_decisions = pd.read_parquet(Path(request.n2_action_bundle_path) / "exit_decisions.parquet")
    exit_episode_best = pd.read_parquet(Path(request.n2_action_bundle_path) / "exit_episode_best.parquet")
    candidate_episode_labels = pd.read_parquet(Path(request.policy_dataset_root) / "candidate_episode_labels.parquet")
    cpcv_payload = _read_json(
        Path(request.policy_dataset_root) / "cpcv_paths.json",
        reason_code="ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH",
    )
    exit_summary = _read_json(
        Path(request.n2_action_bundle_path) / "exit_summary.json",
        reason_code="ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH",
    )
    if len(exit_labels) != len(exit_decisions) or len(exit_episode_best) < 1:
        _raise(
            "Exit action source coverage is invalid",
            "ADVISORY_EXIT_LEARNABILITY_COVERAGE_INSUFFICIENT",
        )
    return {
        "action_request": action_request,
        "action_inspection": action,
        "action_receipt": action_receipt,
        "n1_request": n1_request,
        "exit_labels": exit_labels,
        "exit_decisions": exit_decisions,
        "exit_episode_best": exit_episode_best,
        "exit_summary": exit_summary,
        "candidate_episode_labels": candidate_episode_labels,
        "cpcv_payload": cpcv_payload,
    }


def _stock_features_at(
    *,
    symbol_frame: pd.DataFrame | None,
    entry: pd.Timestamp,
    review: pd.Timestamp,
    entry_price: float | None,
) -> dict[str, float | None]:
    names = [
        "unrealized_close_return_bps",
        "return_1d_bps",
        "return_3d_bps",
        "return_5d_bps",
        "return_10d_bps",
        "realized_vol_5d_bps",
        "realized_vol_10d_bps",
        "realized_vol_20d_bps",
        "drawdown_from_peak_since_entry_bps",
        "runup_from_entry_peak_bps",
        "distance_to_stop_bps",
        "distance_to_take_profit_bps",
        "distance_to_trailing_stop_bps",
        "intraday_range_bps",
        "close_location_in_day",
        "volume_ratio_5d_to_20d",
    ]
    missing = {name: None for name in names}
    if symbol_frame is None or review not in symbol_frame.index:
        return missing
    row = symbol_frame.loc[review]
    if isinstance(row, pd.DataFrame):
        _raise(
            "market data has duplicate instrument/date rows",
            "ADVISORY_EXIT_LEARNABILITY_FEATURE_LEAKAGE",
        )
    close = _finite(row.get("close"))
    high = _finite(row.get("high"))
    low = _finite(row.get("low"))
    if close is None or close <= 0 or entry_price is None or entry_price <= 0:
        return missing
    upto = symbol_frame.loc[:review]
    closes = pd.to_numeric(upto["close"], errors="coerce")
    volumes = pd.to_numeric(upto["volume"], errors="coerce")
    path = symbol_frame.loc[entry:review]
    path_closes = pd.to_numeric(path["close"], errors="coerce").dropna()
    unrealized = (close / entry_price - 1.0) * 10000.0
    peak_close = float(path_closes.max()) if len(path_closes) else None
    drawdown = (close / peak_close - 1.0) * 10000.0 if peak_close and peak_close > 0 else None
    runup = (peak_close / entry_price - 1.0) * 10000.0 if peak_close and peak_close > 0 else None
    output: dict[str, float | None] = {
        "unrealized_close_return_bps": unrealized,
        "drawdown_from_peak_since_entry_bps": drawdown,
        "runup_from_entry_peak_bps": runup,
        "distance_to_stop_bps": unrealized + 800.0,
        "distance_to_take_profit_bps": None if runup is None else 1800.0 - runup,
        "distance_to_trailing_stop_bps": None if drawdown is None else drawdown + 700.0,
        "intraday_range_bps": (
            (high - low) / close * 10000.0 if high is not None and low is not None and high >= low else None
        ),
        "close_location_in_day": (
            (close - low) / (high - low) if high is not None and low is not None and high > low else None
        ),
    }
    review_position = len(closes) - 1
    for window in (1, 3, 5, 10):
        current = _finite(closes.iloc[review_position])
        prior = _finite(closes.iloc[review_position - window]) if review_position >= window else None
        output[f"return_{window}d_bps"] = (
            (current / prior - 1.0) * 10000.0 if current is not None and prior is not None and prior > 0 else None
        )
    returns = closes.pct_change(fill_method=None)
    for window in (5, 10, 20):
        sample = returns.iloc[-window:].dropna()
        output[f"realized_vol_{window}d_bps"] = float(sample.std(ddof=1) * 10000.0) if len(sample) == window else None
    volume5 = volumes.iloc[-5:].dropna()
    volume20 = volumes.iloc[-20:].dropna()
    output["volume_ratio_5d_to_20d"] = (
        float(volume5.mean() / volume20.mean())
        if len(volume5) == 5 and len(volume20) == 20 and volume20.mean() > 0
        else None
    )
    return {name: _finite(output.get(name)) for name in names}


def _relative_return_bps(
    *,
    benchmark: pd.DataFrame,
    entry: pd.Timestamp,
    review: pd.Timestamp,
    stock_unrealized_bps: float | None,
) -> float | None:
    if stock_unrealized_bps is None or entry not in benchmark.index or review not in benchmark.index:
        return None
    entry_open = _finite(benchmark.loc[entry].get("open"))
    review_close = _finite(benchmark.loc[review].get("close"))
    if entry_open is None or review_close is None or entry_open <= 0:
        return None
    return float(stock_unrealized_bps - (review_close / entry_open - 1.0) * 10000.0)


def _infer_daily_lift(
    values: Sequence[float] | np.ndarray,
    request: FrozenAdvisoryN2ExitLearnabilityRequestV1,
) -> Tier1MetricInferenceV1:
    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if len(array) < 2:
        _raise(
            "Exit lift inference has too few finite days",
            "ADVISORY_EXIT_LEARNABILITY_COVERAGE_INSUFFICIENT",
        )
    policy = request.inference_spec
    length = min(policy.block_length_trading_days, len(array))
    block_count = math.ceil(len(array) / length)
    rng = np.random.default_rng(policy.bootstrap_seed)
    offsets = np.arange(length)
    sample_means = np.empty(policy.bootstrap_repetitions, dtype=float)
    for index in range(policy.bootstrap_repetitions):
        starts = rng.integers(0, len(array), size=block_count)
        positions = ((starts[:, None] + offsets[None, :]) % len(array)).reshape(-1)
        sample_means[index] = float(array[positions[: len(array)]].mean())
    point = float(array.mean())
    lower, upper = np.quantile(sample_means, [0.025, 0.975]).tolist()
    lower = min(float(lower), point)
    upper = max(float(upper), point)
    standard_error = float(sample_means.std(ddof=1))
    mde = float((_Z_975 + _Z_80) * standard_error)
    threshold = policy.economic_threshold_bps
    state = (
        Tier1EvidenceState.HIGH
        if lower > threshold
        else Tier1EvidenceState.LOW
        if upper < threshold
        else Tier1EvidenceState.INCONCLUSIVE
    )
    return Tier1MetricInferenceV1(
        point_estimate_bps=point,
        confidence_lower_bps=lower,
        confidence_upper_bps=upper,
        bootstrap_standard_error_bps=standard_error,
        mde_bps=mde,
        economic_threshold_bps=threshold,
        evidence_state=state,
        evaluated_day_count=len(array),
    )


def _publish_bundle(
    *,
    request: FrozenAdvisoryN2ExitLearnabilityRequestV1,
    sources: Mapping[str, Any],
    features: pd.DataFrame,
    oof: pd.DataFrame,
    episode_policy: pd.DataFrame,
    daily_policy: pd.DataFrame,
    inference: Tier1MetricInferenceV1,
    support: ExitLearnabilitySupportV1,
    diagnostics: Mapping[str, Any],
    elapsed_seconds: float,
) -> Path:
    root = Path(request.output_root).resolve() / "exit_learnability_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix=".n2-exit-learn-", dir=root))
    try:
        _write_json(temp / "request.json", request.model_dump(mode="json"))
        source_receipt = {
            "schema_version": "advisory_n2_exit_learnability_source_identity_v1",
            "request_sha256": request.request_sha256,
            "dataset_identity": request.dataset_identity,
            "parent_feature_schema_hash": request.parent_feature_schema_hash,
            "feature_schema_hash": request.feature_schema_hash,
            "n2_action_request_sha256": sources["action_request"].request_sha256,
            "n2_action_receipt_sha256": sources["action_receipt"].receipt_sha256,
            "n1_request_sha256": sources["n1_request"].request_sha256,
            "exit_label_row_count": len(sources["exit_labels"]),
            "evaluable_episode_count": int(sources["exit_episode_best"]["episode_id"].nunique()),
            "ready_cpcv_path_count": sum(
                item.get("status") == "READY" for item in sources["cpcv_payload"].get("paths", ())
            ),
            "baseline_policy_sha256": request.baseline_policy_sha256,
            "shadow_policy_sha256": request.shadow_policy_sha256,
            "cost_policy_sha256": request.cost_policy_sha256,
            "intervention_policy_sha256": request.intervention_policy_sha256,
            "repository_commit": request.repository_commit,
            "sealed_holdout_accessed": False,
        }
        _write_json(temp / "source_identity_receipt.json", source_receipt)
        _write_json(
            temp / "feature_schema.json",
            {
                "schema_version": EXIT_FEATURE_SCHEMA_VERSION,
                "feature_columns": list(EXIT_FEATURE_COLUMNS),
                "categorical_columns": list(EXIT_CATEGORICAL_FEATURE_COLUMNS),
                "feature_schema_hash": request.feature_schema_hash,
            },
        )
        _write_parquet(temp / "features.parquet", features)
        _write_parquet(temp / "oof_predictions.parquet", oof)
        _write_parquet(temp / "episode_policy.parquet", episode_policy)
        _write_parquet(temp / "daily_policy.parquet", daily_policy)
        peak_rss_bytes = _peak_rss_bytes()
        if peak_rss_bytes > request.resource_max_rss_bytes:
            _raise(
                "Exit learnability exceeded the frozen RSS limit while publishing",
                "ADVISORY_EXIT_LEARNABILITY_RESOURCE_LIMIT_EXCEEDED",
                stage="publish_bundle",
                peak_rss_bytes=peak_rss_bytes,
                limit_bytes=request.resource_max_rss_bytes,
            )
        resource_report = {
            "schema_version": "advisory_n2_exit_learnability_resource_report_v1",
            "elapsed_seconds": elapsed_seconds,
            "peak_rss_bytes": peak_rss_bytes,
            "resource_max_rss_bytes": request.resource_max_rss_bytes,
            "wall_time_limit_seconds": None,
            "wall_time_is_telemetry_only": True,
            "platform": platform.platform(),
        }
        _write_json(temp / "resource_report.json", resource_report)
        descriptors = _file_descriptors(temp)
        result_descriptors = {name: descriptors[name] for name in sorted(RESULT_IDENTITY_MEMBERS)}
        powered = inference.mde_bps <= max(
            inference.point_estimate_bps,
            inference.economic_threshold_bps,
        )
        evidence_sufficient = support.support_sufficient and powered
        reasons = list(support.reason_codes)
        if not powered:
            reasons.append("EXPLORATORY_UNDERPOWERED")
        decisive = evidence_sufficient and inference.evidence_state in {
            Tier1EvidenceState.HIGH,
            Tier1EvidenceState.LOW,
        }
        decision_use = DecisionUse.DIRECTION_GATE if decisive else DecisionUse.NAVIGATION_ONLY
        result_class = (
            ResearchResultClass.CONTROL_READY
            if decisive and inference.evidence_state == Tier1EvidenceState.HIGH
            else ResearchResultClass.NEGATIVE
            if decisive
            else ResearchResultClass.EXPLORATORY
        )
        receipt = build_exit_learnability_receipt(
            request_sha256=request.request_sha256,
            feature_schema_hash=request.feature_schema_hash,
            feature_row_count=len(features),
            oof_row_count=len(oof),
            evaluated_episode_count=support.evaluated_episode_count,
            evaluated_entry_day_count=len(daily_policy),
            row_diagnostics={
                **dict(diagnostics["row"]),
                "label_status_counts": {
                    str(key): int(value)
                    for key, value in features["label_status"].map(_enum_value).value_counts().sort_index().items()
                },
                "rows_with_numeric_missing": int(features["missing_numeric_feature_count"].astype(int).gt(0).sum()),
                "maximum_missing_numeric_feature_count": int(
                    features["missing_numeric_feature_count"].astype(int).max()
                ),
            },
            episode_diagnostics={
                **dict(diagnostics["episode"]),
                "daily": dict(diagnostics["daily"]),
            },
            policy_lift=inference,
            intervention_support=support,
            oracle_mean_lift_bps=float(diagnostics["oracle_mean_lift_bps"]),
            oracle_capture_ratio=diagnostics["oracle_capture_ratio"],
            evidence_sufficient=evidence_sufficient,
            evidence_reason_codes=tuple(sorted(set(reasons))),
            result_class=result_class,
            decision_use=decision_use,
            source_identity_sha256=descriptors["source_identity_receipt.json"]["sha256"],
            result_files_sha256=canonical_json_sha256(result_descriptors),
            resource_report_sha256=descriptors["resource_report.json"]["sha256"],
        )
        _write_json(temp / "learnability_receipt.json", receipt.model_dump(mode="json"))
        bundle_id = canonical_json_sha256(
            {
                "schema_version": EXIT_LEARNABILITY_BUNDLE_SCHEMA,
                "request_sha256": request.request_sha256,
                "receipt_sha256": receipt.receipt_sha256,
            }
        )
        final = root / bundle_id
        receipt_file = temp / "learnability_receipt.json"
        receipt_ref = EvidenceReferenceV1(
            role="advisory_n2_exit_learnability_receipt",
            artifact_uri=(final / "learnability_receipt.json").as_posix(),
            sha256=sha256_file(receipt_file),
            size_bytes=receipt_file.stat().st_size,
        )
        record = build_trial_record(
            experiment_id=EXIT_LEARNABILITY_EXPERIMENT_ID,
            attempt_id=request.request_id,
            research_stage="N2_EXIT_LEARNABILITY_AUDIT",
            study_type=ResearchStudyType.LEARNABILITY_AUDIT,
            hypothesis_family_id=EXIT_LEARNABILITY_FAMILY_ID,
            parent_lineage=(
                "ADVISORY-N1-TIER1-ORACLE",
                "ADVISORY-N1-TIER1-LEARNABILITY",
                "ADVISORY-N2-EXIT-LABEL-ORACLE",
            ),
            unique_variable="FIXED_T_VISIBLE_EXIT_ADVANTAGE_LEARNABILITY_V1",
            objective_contract=ObjectiveContract.RISK_MANAGED_ADVISORY,
            dataset_identity=request.dataset_identity,
            schema_identity=request.feature_schema_hash,
            policy_identity=research_policy_identity(
                baseline_policy_sha256=request.baseline_policy_sha256,
                shadow_policy_sha256=request.shadow_policy_sha256,
                cost_policy_sha256=request.cost_policy_sha256,
            ),
            planned_trial_count=1,
            generated_trial_count=1,
            evaluated_trial_count=1,
            selected_trial_count=0,
            consumed_windows=(
                ConsumedWindowV1(
                    window_id="P0C_DEVELOPMENT_V1",
                    dataset_identity=request.dataset_identity,
                    start_date=request.decision_start,
                    end_date=request.outcome_cutoff,
                ),
            ),
            result_class=result_class,
            decision_use=decision_use,
            evidence_refs=(receipt_ref,),
        )
        _write_json(temp / "registry_record.json", record.model_dump(mode="json"))
        descriptors = _file_descriptors(temp)
        manifest = {
            "schema_version": EXIT_LEARNABILITY_BUNDLE_SCHEMA,
            "bundle_id": bundle_id,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
            "feature_schema_hash": request.feature_schema_hash,
            "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY.value,
            "study_type": ResearchStudyType.LEARNABILITY_AUDIT.value,
            "decision_use": receipt.decision_use.value,
            "result_class": receipt.result_class.value,
            "planned_trial_count": 1,
            "generated_trial_count": 1,
            "evaluated_trial_count": 1,
            "selected_trial_count": 0,
            "sealed_holdout_accessed": False,
            "deployable": False,
            "runtime_eligible": False,
            "files": descriptors,
        }
        _write_json(temp / "manifest.json", manifest)
        if final.exists():
            _raise(
                "Exit learnability bundle id already exists",
                "ADVISORY_EXIT_LEARNABILITY_BUNDLE_INVALID",
                bundle_path=final.as_posix(),
            )
        os.replace(temp, final)
        _read_bundle(final)
        return final
    except Exception:
        # A failed publish remains under a hidden, request-scoped temp name and
        # has no manifest/registry delivery.  Do not recursively delete paths
        # from an error handler; cleanup requires a separate explicit action.
        raise


def _read_bundle(path: Path) -> dict[str, Any]:
    manifest = _read_json(
        path / "manifest.json",
        reason_code="ADVISORY_EXIT_LEARNABILITY_BUNDLE_INVALID",
    )
    request = FrozenAdvisoryN2ExitLearnabilityRequestV1.model_validate_json(
        (path / "request.json").read_text(encoding="utf-8")
    )
    receipt = AdvisoryN2ExitLearnabilityReceiptV1.model_validate_json(
        (path / "learnability_receipt.json").read_text(encoding="utf-8")
    )
    record_raw = _read_json(
        path / "registry_record.json",
        reason_code="ADVISORY_EXIT_LEARNABILITY_BUNDLE_INVALID",
    )
    from backend.services.advisory_model_first.research_control_contracts import (
        AdvisoryResearchTrialRecordV1,
    )

    record = AdvisoryResearchTrialRecordV1.model_validate(record_raw)
    descriptors = manifest.get("files")
    if not isinstance(descriptors, dict) or set(descriptors) != BUNDLE_MEMBERS:
        _raise(
            "Exit learnability bundle member set is invalid",
            "ADVISORY_EXIT_LEARNABILITY_BUNDLE_INVALID",
        )
    for name, descriptor in descriptors.items():
        member = path / name
        if not member.is_file():
            _raise(
                "Exit learnability bundle member is missing",
                "ADVISORY_EXIT_LEARNABILITY_BUNDLE_INVALID",
                member=name,
            )
        row_count = len(pd.read_parquet(member)) if member.suffix == ".parquet" else None
        if (
            sha256_file(member) != descriptor.get("sha256")
            or member.stat().st_size != descriptor.get("size_bytes")
            or (row_count is not None and descriptor.get("row_count") != row_count)
        ):
            _raise(
                "Exit learnability bundle member drift",
                "ADVISORY_EXIT_LEARNABILITY_BUNDLE_INVALID",
                member=name,
            )
    expected_id = canonical_json_sha256(
        {
            "schema_version": EXIT_LEARNABILITY_BUNDLE_SCHEMA,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
        }
    )
    result_descriptors = {name: descriptors[name] for name in sorted(RESULT_IDENTITY_MEMBERS)}
    receipt_descriptor = descriptors["learnability_receipt.json"]
    resource_report = _read_json(
        path / "resource_report.json",
        reason_code="ADVISORY_EXIT_LEARNABILITY_BUNDLE_INVALID",
    )
    invalid = (
        manifest.get("schema_version") != EXIT_LEARNABILITY_BUNDLE_SCHEMA
        or path.name != expected_id
        or manifest.get("bundle_id") != expected_id
        or manifest.get("request_sha256") != request.request_sha256
        or manifest.get("receipt_sha256") != receipt.receipt_sha256
        or receipt.request_sha256 != request.request_sha256
        or receipt.feature_schema_hash != request.feature_schema_hash
        or receipt.source_identity_sha256 != descriptors["source_identity_receipt.json"]["sha256"]
        or receipt.result_files_sha256 != canonical_json_sha256(result_descriptors)
        or receipt.resource_report_sha256 != descriptors["resource_report.json"]["sha256"]
        or record.attempt_id != request.request_id
        or record.experiment_id != EXIT_LEARNABILITY_EXPERIMENT_ID
        or record.decision_use != receipt.decision_use
        or record.result_class != receipt.result_class
        or len(record.evidence_refs) != 1
        or record.evidence_refs[0].sha256 != receipt_descriptor["sha256"]
        or record.evidence_refs[0].size_bytes != receipt_descriptor["size_bytes"]
        or manifest.get("sealed_holdout_accessed") is not False
        or manifest.get("deployable") is not False
        or manifest.get("runtime_eligible") is not False
        or manifest.get("objective_contract") != ObjectiveContract.RISK_MANAGED_ADVISORY.value
        or manifest.get("study_type") != ResearchStudyType.LEARNABILITY_AUDIT.value
        or manifest.get("decision_use") != receipt.decision_use.value
        or manifest.get("result_class") != receipt.result_class.value
        or not isinstance(resource_report.get("peak_rss_bytes"), int)
        or int(resource_report.get("peak_rss_bytes", -1)) < 0
        or int(resource_report.get("peak_rss_bytes", -1)) > request.resource_max_rss_bytes
        or resource_report.get("resource_max_rss_bytes") != request.resource_max_rss_bytes
        or resource_report.get("wall_time_limit_seconds") is not None
        or resource_report.get("wall_time_is_telemetry_only") is not True
        or any(
            int(manifest.get(name, -1)) != value
            for name, value in (
                ("planned_trial_count", 1),
                ("generated_trial_count", 1),
                ("evaluated_trial_count", 1),
                ("selected_trial_count", 0),
            )
        )
    )
    if invalid:
        _raise(
            "Exit learnability bundle relational identity is invalid",
            "ADVISORY_EXIT_LEARNABILITY_BUNDLE_INVALID",
        )
    return {
        "manifest": manifest,
        "request": request,
        "receipt": receipt,
        "record": record,
    }


def _deliver_bundle(*, request: FrozenAdvisoryN2ExitLearnabilityRequestV1, bundle_path: Path) -> dict[str, Any]:
    loaded = _read_bundle(bundle_path)
    summary = AdvisoryResearchTrialRegistryV1(request.registry_path).append_batch((loaded["record"],))
    route = generate_current_route(
        registry_path=request.registry_path,
        parent_spike_path=request.parent_spike_path,
        window_contract_path=request.research_window_contract_ref.artifact_uri,
        output_path=request.route_path,
    )
    if route.get("next_task") != "N2_ENTRY_EXIT_QE_PREPARATION":
        _raise(
            "Exit learnability delivery changed the frozen N2 route",
            "ADVISORY_EXIT_LEARNABILITY_BUNDLE_INVALID",
            next_task=route.get("next_task"),
        )
    return {"registry": summary, "route": route}


def _find_existing_bundle(
    request: FrozenAdvisoryN2ExitLearnabilityRequestV1,
) -> Path | None:
    root = Path(request.output_root) / "exit_learnability_bundles"
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
            "one Exit learnability request maps to multiple bundles",
            "ADVISORY_EXIT_LEARNABILITY_BUNDLE_INVALID",
        )
    if matches:
        _read_bundle(matches[0])
        return matches[0]
    return None


def _normalize_market(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.reset_index() if isinstance(frame.index, pd.MultiIndex) else frame.copy()
    if "datetime" not in result or "instrument" not in result:
        _raise(
            "market frame omits datetime/instrument",
            "ADVISORY_EXIT_LEARNABILITY_FEATURE_LEAKAGE",
        )
    result["datetime"] = pd.to_datetime(result["datetime"]).dt.normalize()
    result["instrument"] = result["instrument"].astype(str).str.upper()
    if result.duplicated(["datetime", "instrument"]).any():
        _raise(
            "market frame has duplicate datetime/instrument rows",
            "ADVISORY_EXIT_LEARNABILITY_FEATURE_LEAKAGE",
        )
    return result.sort_values(["instrument", "datetime"]).reset_index(drop=True)


def _verify_ref(path: str | Path, expected: EvidenceReferenceV1) -> None:
    actual = evidence_reference_for_file(path, role=expected.role)
    if (actual.sha256, actual.size_bytes) != (expected.sha256, expected.size_bytes):
        _raise(
            "Exit learnability source reference drift",
            "ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH",
            role=expected.role,
        )


def _reference_with_role(value: EvidenceReferenceV1, role: str) -> EvidenceReferenceV1:
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=value.artifact_uri,
        sha256=value.sha256,
        size_bytes=value.size_bytes,
    )


def _file_descriptors(root: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in sorted(root.iterdir()):
        if not path.is_file() or path.name == "manifest.json":
            continue
        descriptor: dict[str, Any] = {
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
        }
        if path.suffix == ".parquet":
            descriptor["row_count"] = len(pd.read_parquet(path))
        result[path.name] = descriptor
    return result


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    frame.to_parquet(path, index=False)


def _write_immutable_request(path: Path, request: FrozenAdvisoryN2ExitLearnabilityRequestV1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        request.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    try:
        with path.open("x", encoding="utf-8") as handle:
            handle.write(payload)
    except FileExistsError:
        existing = FrozenAdvisoryN2ExitLearnabilityRequestV1.model_validate_json(path.read_text(encoding="utf-8"))
        if existing.request_sha256 != request.request_sha256:
            _raise(
                "Exit learnability request path already contains another identity",
                "ADVISORY_EXIT_LEARNABILITY_REQUEST_INVALID",
            )


def _read_json(path: Path, *, reason_code: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _raise(
            "required JSON artifact cannot be read",
            reason_code,
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )
    if not isinstance(value, dict):
        _raise("required JSON artifact is not an object", reason_code, path=path.as_posix())
    return value


def _repository_commit(root: Path) -> str:
    try:
        return _cross_os_git_commit(root)
    except Exception as exc:
        _raise(
            "repository commit cannot be resolved",
            "ADVISORY_EXIT_LEARNABILITY_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )


def _repository_dirty(root: Path) -> list[str]:
    try:
        return _cross_os_git_dirty_paths(root)
    except Exception as exc:
        _raise(
            "repository dirty state cannot be resolved",
            "ADVISORY_EXIT_LEARNABILITY_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )


def _verify_environment(request: FrozenAdvisoryN2ExitLearnabilityRequestV1) -> None:
    commit = _repository_commit(Path(request.repository_root))
    dirty = _repository_dirty(Path(request.repository_root))
    if commit != request.repository_commit or dirty:
        _raise(
            "Exit learnability runtime repository differs from frozen source",
            "ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH",
            expected_commit=request.repository_commit,
            actual_commit=commit,
            dirty_paths=dirty[:20],
        )


def _check_rss(request: FrozenAdvisoryN2ExitLearnabilityRequestV1, stage: str) -> None:
    peak = _peak_rss_bytes()
    if peak > request.resource_max_rss_bytes:
        _raise(
            "Exit learnability exceeded the frozen RSS limit",
            "ADVISORY_EXIT_LEARNABILITY_RESOURCE_LIMIT_EXCEEDED",
            stage=stage,
            peak_rss_bytes=peak,
            limit_bytes=request.resource_max_rss_bytes,
        )


def _peak_rss_bytes() -> int:
    if _resource is None:
        return 0
    value = int(_resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss)
    return value if platform.system() == "Darwin" else value * 1024


def _correlation(left: pd.Series, right: pd.Series, *, method: str) -> float | None:
    value = left.corr(right, method=method)
    return _finite(value)


def _cumulative_return(values: pd.Series) -> float:
    return float(np.prod(1.0 + values.astype(float).to_numpy() / 10000.0) - 1.0)


def _max_drawdown(values: pd.Series) -> float:
    curve = np.cumprod(1.0 + values.astype(float).to_numpy() / 10000.0)
    peaks = np.maximum.accumulate(curve)
    return float(np.min(curve / peaks - 1.0))


def _finite(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _enum_value(value: object) -> str:
    return str(getattr(value, "value", value))


def _run_response(
    request: FrozenAdvisoryN2ExitLearnabilityRequestV1,
    bundle: Path,
    delivery: Mapping[str, Any],
    *,
    exact_retry: bool,
) -> dict[str, Any]:
    inspected = inspect_exit_learnability_bundle(bundle)
    return {
        "status": "ok",
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "bundle_path": bundle.as_posix(),
        "bundle_id": bundle.name,
        "exact_retry": exact_retry,
        "inspection": inspected,
        "delivery": dict(delivery),
    }


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "build_exit_feature_matrix",
    "evaluate_exit_policy",
    "inspect_exit_learnability_bundle",
    "prepare_exit_learnability_request",
    "run_exit_crossfit",
    "run_exit_learnability_audit",
]
