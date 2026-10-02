from __future__ import annotations

import json
import math
import os
import platform
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import NormalDist
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
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.qe_alpha_mve_contracts import (
    DAILY_FIELDS,
    MVE_BLOCK_LENGTH,
    MVE_BOOTSTRAP_REPETITIONS,
    MVE_PROPOSAL_COUNT,
    MVE_RANDOM_SEED,
    N3_EXPERIMENT_ID,
    N3_HYPOTHESIS_FAMILY_ID,
    AdvisoryN3RouteReceiptV1,
    AdvisoryQEAlphaMVEReceiptV1,
    FrozenAdvisoryQEAlphaMVERequestV1,
    QEAlphaProposalV1,
    build_default_proposals,
    build_n3_route_receipt,
    build_qe_alpha_mve_receipt,
    build_qe_alpha_mve_request,
)
from backend.services.advisory_model_first.qe_alpha_mve_preparation import (
    FrozenAdvisoryQEAlphaMVEPreparationV1,
    load_qe_alpha_mve_preparation,
)
from backend.services.advisory_model_first.qe_file_source import (
    initialize_qlib,
    load_qlib_daily,
    load_static_factors,
)
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import (
    AdvisoryResearchTrialRecordV1,
    ConsumedWindowV1,
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


QE_ALPHA_MVE_BUNDLE_SCHEMA = "advisory_qe_alpha_mve_bundle_v1"
CURRENT_PARENT_ARM_ID = "CURRENT_IC_PARENT"
RESULT_IDENTITY_MEMBERS = frozenset(
    {
        "proposal_roster.json",
        "score_panel.parquet",
        "daily_metrics.parquet",
        "proposal_summary.json",
        "frontier_receipt.json",
    }
)
BUNDLE_MEMBERS = RESULT_IDENTITY_MEMBERS | {
    "request.json",
    "route_receipt.json",
    "source_identity_receipt.json",
    "mve_receipt.json",
    "resource_report.json",
    "registry_record.json",
}


def prepare_qe_alpha_mve_request(
    *,
    n1_bundle_path: str | Path,
    n2a_bundle_path: str | Path,
    n2b_bundle_path: str | Path,
    n2_action_bundle_path: str | Path,
    exit_learnability_bundle_path: str | Path,
    preparation_path: str | Path,
    factor_root: str | Path,
    qlib_daily_root: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
) -> FrozenAdvisoryQEAlphaMVERequestV1:
    """Freeze one authorized 24-proposal N3 execution request.

    This function only reads immutable development evidence and source schemas.
    It never evaluates a proposal or appends the research registry.
    """

    n1 = Path(n1_bundle_path).resolve()
    n2a = Path(n2a_bundle_path).resolve()
    n2b = Path(n2b_bundle_path).resolve()
    action = Path(n2_action_bundle_path).resolve()
    exit_bundle = Path(exit_learnability_bundle_path).resolve()
    prep_path = Path(preparation_path).resolve()
    factors = Path(factor_root).resolve()
    qlib_root = Path(qlib_daily_root).resolve()
    repo = Path(repository_root).resolve()
    registry_path, route_path, dataset_identity, policy_identity = _validate_route_sources(
        n1=n1,
        n2a=n2a,
        n2b=n2b,
        action=action,
        exit_bundle=exit_bundle,
    )
    preparation = load_qe_alpha_mve_preparation(prep_path)
    _validate_preparation(preparation)
    static_path = factors / "static_factors.parquet"
    static_ref = evidence_reference_for_file(static_path, role="n3_static_factors_parquet")
    static_schema_sha256 = _static_schema_sha256(static_path)
    proposals = build_default_proposals()
    _validate_static_fields(static_path, proposals)
    if not qlib_root.is_dir():
        _raise(
            "QE alpha MVE Qlib daily root is missing",
            "ADVISORY_QE_ALPHA_MVE_REQUEST_INVALID",
            qlib_daily_root=qlib_root.as_posix(),
        )
    dirty = _cross_os_git_dirty_paths(repo)
    if dirty:
        _raise(
            "QE alpha MVE request requires a clean repository",
            "ADVISORY_QE_ALPHA_MVE_REQUEST_INVALID",
            dirty_paths=dirty[:20],
        )
    commit = _cross_os_git_commit(repo)
    oracle = _read_json(n1 / "oracle_receipt.json", "ADVISORY_N3_ROUTE_EVIDENCE_INVALID")
    route_receipt = build_n3_route_receipt(
        candidate_top50_winner_recall=float(oracle["recall_summary"]["top50"]["mean_winner_recall"]),
        candidate_top50_winner_recall_upper=float(oracle["recall_summary"]["top50"]["confidence_upper"]),
    )
    role_paths = {
        "n3_n1_oracle_receipt": n1 / "oracle_receipt.json",
        "n3_n1_learnability_receipt": n1 / "learnability_receipt.json",
        "n3_n1_quadrant_receipt": n1 / "quadrant_receipt.json",
        "n3_n2a_audit_receipt": n2a / "audit_receipt.json",
        "n3_n2a_arm_summary": n2a / "arm_summary.json",
        "n3_n2b_audit_receipt": n2b / "audit_receipt.json",
        "n3_n2b_arm_summary": n2b / "arm_summary.json",
        "n3_n2b_pairwise_summary": n2b / "pairwise_summary.json",
        "n3_n2_action_receipt": action / "audit_receipt.json",
        "n3_n2_entry_summary": action / "entry_summary.json",
        "n3_n2_entry_support": action / "entry_support.json",
        "n3_n2_exit_summary": action / "exit_summary.json",
        "n3_n2_exit_support": action / "exit_support.json",
        "n3_exit_learnability_receipt": exit_bundle / "learnability_receipt.json",
        "n3_qe_alpha_preparation": prep_path,
        "n3_trial_registry_before": registry_path,
    }
    evidence_refs = tuple(evidence_reference_for_file(path, role=role) for role, path in sorted(role_paths.items()))
    outcomes_path = n2b / "arm_signal_outcomes.parquet"
    if not outcomes_path.is_file():
        _raise(
            "QE alpha MVE outcome source is missing",
            "ADVISORY_QE_ALPHA_MVE_REQUEST_INVALID",
            outcomes_path=outcomes_path.as_posix(),
        )
    outcomes_ref = evidence_reference_for_file(
        outcomes_path,
        role="n3_current_parent_signal_outcomes",
    )
    combined_dataset_identity = canonical_json_sha256(
        {
            "n2b_dataset_identity": dataset_identity,
            "preparation_data_identity": preparation.data_identity.model_dump(mode="json"),
            "static_factor_sha256": static_ref.sha256,
            "static_schema_sha256": static_schema_sha256,
            "policy_identity": policy_identity,
        }
    )
    request = build_qe_alpha_mve_request(
        route_receipt=route_receipt,
        proposals=proposals,
        evidence_refs=evidence_refs,
        preparation_path=prep_path.as_posix(),
        n2b_bundle_path=n2b.as_posix(),
        outcomes_path=outcomes_path.as_posix(),
        outcomes_ref=outcomes_ref,
        factor_root=factors.as_posix(),
        static_factor_ref=static_ref,
        static_schema_sha256=static_schema_sha256,
        qlib_daily_root=qlib_root.as_posix(),
        dataset_identity=combined_dataset_identity,
        registry_path=registry_path.as_posix(),
        route_path=route_path.as_posix(),
        repository_root=repo.as_posix(),
        repository_commit=commit,
        output_root=Path(output_root).resolve().as_posix(),
    )
    _write_immutable_request(Path(output_path).resolve(), request)
    return request


def run_qe_alpha_mve(request_path: str | Path) -> dict[str, Any]:
    started = time.monotonic()
    request = FrozenAdvisoryQEAlphaMVERequestV1.model_validate_json(Path(request_path).read_text(encoding="utf-8"))
    existing = _find_existing_bundle(request)
    _verify_environment(request, verify_registry_before=existing is None)
    if existing is not None:
        delivery = _deliver_bundle(request=request, bundle_path=existing)
        return _run_response(request, existing, delivery, exact_retry=True)
    sources = _load_verified_sources(request)
    _check_resource_limits(request, "sources_loaded")
    panel = build_source_panel(request=request, outcomes=sources["outcomes"], benchmark=sources["benchmark"])
    _check_resource_limits(request, "source_panel_built")
    scores = compile_proposal_scores(panel=panel, proposals=request.proposals)
    _check_resource_limits(request, "proposal_scores_compiled")
    score_panel, daily_metrics, proposal_summary, frontier = evaluate_proposals(
        panel=panel,
        outcomes=sources["outcomes"],
        proposal_scores=scores,
        request=request,
    )
    _check_resource_limits(request, "proposal_metrics_evaluated")
    bundle = _publish_bundle(
        request=request,
        sources=sources,
        score_panel=score_panel,
        daily_metrics=daily_metrics,
        proposal_summary=proposal_summary,
        frontier=frontier,
        elapsed_seconds=time.monotonic() - started,
    )
    delivery = _deliver_bundle(request=request, bundle_path=bundle)
    return _run_response(request, bundle, delivery, exact_retry=False)


def inspect_qe_alpha_mve_bundle(bundle_path: str | Path) -> dict[str, Any]:
    loaded = _read_bundle(Path(bundle_path).resolve())
    receipt = loaded["receipt"]
    frontier = _read_json(Path(bundle_path) / "frontier_receipt.json", "ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID")
    return {
        "status": "VALID",
        "bundle_id": loaded["manifest"]["bundle_id"],
        "request_id": loaded["request"].request_id,
        "receipt_id": receipt.receipt_id,
        "selected_proposal_id": receipt.selected_proposal_id,
        "eligible_proposal_ids": list(receipt.eligible_proposal_ids),
        "next_task": receipt.next_task,
        "frontier_sha256": frontier["frontier_sha256"],
        "planned_trial_count": 24,
        "generated_trial_count": 24,
        "evaluated_trial_count": 24,
        "selected_trial_count": receipt.selected_trial_count,
        "decision_use": receipt.decision_use.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "factor_catalog_written": False,
    }


def build_source_panel(
    *,
    request: FrozenAdvisoryQEAlphaMVERequestV1,
    outcomes: pd.DataFrame,
    benchmark: str,
) -> pd.DataFrame:
    symbols = sorted(outcomes["instrument"].astype(str).str.upper().unique())
    history_start = (pd.Timestamp(request.signal_start) - pd.Timedelta(days=550)).date().isoformat()
    initialize_qlib(request.qlib_daily_root)
    daily = load_qlib_daily(
        symbols,
        start=history_start,
        end=request.signal_end.isoformat(),
        fields=tuple(f"${name}" for name in sorted(DAILY_FIELDS)),
    ).reset_index()
    required_static = sorted(
        {
            field
            for proposal in request.proposals
            for field in proposal.source_fields
            if field not in DAILY_FIELDS and field != "market_regime"
        }
    )
    static = load_static_factors(
        request.factor_root,
        columns=required_static,
        start=history_start,
        end=request.signal_end.isoformat(),
        instruments=symbols,
    ).reset_index()
    daily["datetime"] = pd.to_datetime(daily["datetime"]).dt.normalize()
    daily["instrument"] = daily["instrument"].astype(str).str.upper()
    static["datetime"] = pd.to_datetime(static["datetime"]).dt.normalize()
    static["instrument"] = static["instrument"].astype(str).str.upper()
    if daily.duplicated(["datetime", "instrument"]).any() or static.duplicated(["datetime", "instrument"]).any():
        _raise(
            "QE alpha MVE feature source has duplicate PIT keys",
            "ADVISORY_QE_ALPHA_MVE_PIT_LEAKAGE",
        )
    panel = daily.merge(
        static,
        on=["datetime", "instrument"],
        how="left",
        validate="one_to_one",
    )
    benchmark_daily = load_qlib_daily(
        [benchmark],
        start=history_start,
        end=request.signal_end.isoformat(),
        fields=("$close",),
    ).reset_index()
    benchmark_daily = benchmark_daily.loc[
        benchmark_daily["instrument"].astype(str).str.upper() == str(benchmark).upper()
    ].copy()
    benchmark_daily["datetime"] = pd.to_datetime(benchmark_daily["datetime"]).dt.normalize()
    benchmark_daily = benchmark_daily.sort_values("datetime")
    benchmark_daily["market_regime"] = np.sign(pd.to_numeric(benchmark_daily["close"], errors="coerce").pct_change(20))
    panel = panel.merge(
        benchmark_daily[["datetime", "market_regime"]],
        on="datetime",
        how="left",
        validate="many_to_one",
    )
    pit_keys = outcomes[["decision_as_of_trade_date", "instrument"]].rename(
        columns={"decision_as_of_trade_date": "datetime"}
    )
    if pit_keys.duplicated(["datetime", "instrument"]).any():
        _raise(
            "QE alpha MVE PIT membership keys are duplicated",
            "ADVISORY_QE_ALPHA_MVE_PIT_LEAKAGE",
        )
    pit_keys = pit_keys.assign(pit_eligible=True)
    panel = panel.merge(
        pit_keys,
        on=["datetime", "instrument"],
        how="left",
        validate="one_to_one",
    )
    panel["pit_eligible"] = panel["pit_eligible"].fillna(False).astype(bool)
    panel = panel.sort_values(["instrument", "datetime"]).reset_index(drop=True)
    if panel.empty:
        _raise(
            "QE alpha MVE source panel is empty",
            "ADVISORY_QE_ALPHA_MVE_COVERAGE_INSUFFICIENT",
        )
    return panel.replace([np.inf, -np.inf], np.nan)


def compile_proposal_scores(
    *,
    panel: pd.DataFrame,
    proposals: Sequence[QEAlphaProposalV1],
) -> pd.DataFrame:
    """Interpret the frozen declarative expressions without dynamic code execution."""

    required = {"datetime", "instrument"}
    if not required.issubset(panel.columns):
        _raise(
            "QE alpha MVE panel omits PIT identity columns",
            "ADVISORY_QE_ALPHA_MVE_EXPRESSION_INVALID",
            missing_columns=sorted(required - set(panel.columns)),
        )
    work = panel.sort_values(["instrument", "datetime"]).reset_index(drop=True)
    if work.duplicated(["datetime", "instrument"]).any():
        _raise(
            "QE alpha MVE panel has duplicate PIT identity keys",
            "ADVISORY_QE_ALPHA_MVE_PIT_LEAKAGE",
        )
    cache: dict[str, pd.Series] = {}
    pit_eligible = (
        work["pit_eligible"].fillna(False).astype(bool)
        if "pit_eligible" in work
        else pd.Series(True, index=work.index, dtype=bool)
    )

    def evaluate(node: Mapping[str, Any]) -> pd.Series:
        key = canonical_json_sha256(dict(node))
        cached = cache.get(key)
        if cached is not None:
            return cached
        op = str(node["op"])
        if op == "FIELD":
            name = str(node["field"])
            if name not in work:
                _raise(
                    "QE alpha MVE expression field is missing from the source panel",
                    "ADVISORY_QE_ALPHA_MVE_EXPRESSION_INVALID",
                    field=name,
                )
            result = pd.to_numeric(work[name], errors="coerce").astype(float)
        elif op == "CONST":
            result = pd.Series(float(node["value"]), index=work.index, dtype=float)
        else:
            args = [evaluate(item) for item in node["args"]]
            if op == "ADD":
                result = args[0] + args[1]
            elif op == "SUBTRACT":
                result = args[0] - args[1]
            elif op == "MULTIPLY":
                result = args[0] * args[1]
            elif op == "SAFE_DIVIDE":
                denominator = args[1]
                result = (args[0] / denominator).where(denominator.abs() > 1e-12)
            elif op == "ABS":
                result = args[0].abs()
            elif op == "SIGN":
                result = np.sign(args[0])
            elif op == "LOG1P_ABS":
                result = np.log1p(args[0].abs())
            elif op == "SQRT_ABS":
                result = np.sqrt(args[0].abs())
            elif op == "CLIP":
                result = args[0].clip(lower=float(node["lower"]), upper=float(node["upper"]))
            elif op == "LAG":
                result = args[0].groupby(work["instrument"], sort=False).shift(int(node["periods"]))
            elif op == "DELTA":
                result = args[0] - args[0].groupby(work["instrument"], sort=False).shift(int(node["periods"]))
            elif op.startswith("TRAILING_"):
                result = _trailing_operation(
                    args[0],
                    instruments=work["instrument"],
                    operation=op,
                    window=int(node["window"]),
                )
            elif op == "SAME_DATE_RANK":
                result = (
                    args[0].where(pit_eligible).groupby(work["datetime"], sort=False).rank(method="average", pct=True)
                )
            elif op == "SAME_DATE_ZSCORE":
                visible = args[0].where(pit_eligible)
                grouped = visible.groupby(work["datetime"], sort=False)
                mean = grouped.transform("mean")
                std = grouped.transform("std")
                result = ((visible - mean) / std).where(std > 1e-12)
            else:  # contracts reject this before execution; keep a typed runtime guard.
                _raise(
                    "QE alpha MVE expression operator has no implementation",
                    "ADVISORY_QE_ALPHA_MVE_EXPRESSION_INVALID",
                    operator=op,
                )
        normalized = pd.Series(result, index=work.index, dtype=float).replace([np.inf, -np.inf], np.nan)
        cache[key] = normalized
        return normalized

    output = work[["datetime", "instrument"]].copy()
    for proposal in proposals:
        output[proposal.proposal_id] = evaluate(proposal.expression).astype("float32")
    return output


def _trailing_operation(
    values: pd.Series,
    *,
    instruments: pd.Series,
    operation: str,
    window: int,
) -> pd.Series:
    grouped = values.groupby(instruments, sort=False)
    rolling = grouped.rolling(window=window, min_periods=window)
    if operation == "TRAILING_SUM":
        result = rolling.sum()
    elif operation == "TRAILING_MEAN":
        result = rolling.mean()
    elif operation == "TRAILING_STD":
        result = rolling.std()
    elif operation == "TRAILING_MIN":
        result = rolling.min()
    elif operation == "TRAILING_MAX":
        result = rolling.max()
    else:
        _raise(
            "QE alpha MVE trailing operator has no implementation",
            "ADVISORY_QE_ALPHA_MVE_EXPRESSION_INVALID",
            operator=operation,
        )
    return result.reset_index(level=0, drop=True).reindex(values.index)


def evaluate_proposals(
    *,
    panel: pd.DataFrame,
    outcomes: pd.DataFrame,
    proposal_scores: pd.DataFrame,
    request: FrozenAdvisoryQEAlphaMVERequestV1,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, Any]]:
    del panel  # Scores are already frozen before target data enters this function.
    outcomes = _normalize_outcomes(outcomes, request=request)
    signal_scores = proposal_scores.loc[
        proposal_scores["datetime"].between(pd.Timestamp(request.signal_start), pd.Timestamp(request.signal_end))
    ].copy()
    signal_scores = signal_scores.rename(columns={"datetime": "decision_as_of_trade_date"})
    if signal_scores.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise(
            "QE alpha MVE proposal scores have duplicate signal keys",
            "ADVISORY_QE_ALPHA_MVE_PIT_LEAKAGE",
        )
    score_panel = outcomes.merge(
        signal_scores,
        on=["decision_as_of_trade_date", "instrument"],
        how="left",
        validate="one_to_one",
    )
    if len(score_panel) != len(outcomes):
        _raise(
            "QE alpha MVE source merge changed the outcome row count",
            "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    daily_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    for proposal_index, proposal in enumerate(request.proposals):
        proposal_daily = _evaluate_one_proposal_daily(score_panel, proposal.proposal_id)
        daily_rows.extend(proposal_daily.to_dict("records"))
        summary_rows.append(
            _summarize_one_proposal(
                proposal=proposal,
                daily=proposal_daily,
                score_panel=score_panel,
                request=request,
                seed=request.bootstrap_seed + proposal_index * 101,
            )
        )
    daily_metrics = pd.DataFrame(daily_rows).sort_values(["proposal_id", "decision_as_of_trade_date"])
    eligible = [item for item in summary_rows if item["eligible"]]
    eligible.sort(
        key=lambda item: (
            -float(item["familywise_top5_lift_lower_bps"]),
            -float(item["familywise_rank_ic_lower"]),
            str(item["proposal_id"]),
        )
    )
    selected = eligible[0]["proposal_id"] if eligible else None
    frontier_payload = {
        "schema_version": "advisory_qe_alpha_mve_frontier_v1",
        "request_sha256": request.request_sha256,
        "selection_rule": ("FWER_LIFT_LOWER_DESC__FWER_RANKIC_LOWER_DESC__PROPOSAL_ID_ASC__SELECT_ONCE"),
        "eligible_proposal_ids": [item["proposal_id"] for item in eligible],
        "selected_proposal_id": selected,
        "selected_trial_count": 1 if selected else 0,
        "candidate_reselection_allowed": False,
        "exact_retry_allowed": True,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    frontier_payload["frontier_sha256"] = canonical_json_sha256(frontier_payload)
    proposal_summary = {
        "schema_version": "advisory_qe_alpha_mve_proposal_summary_v1",
        "request_sha256": request.request_sha256,
        "trial_count": len(summary_rows),
        "proposals": summary_rows,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    keep = [
        "decision_as_of_trade_date",
        "instrument",
        "score",
        "economic_net_excess_bps",
        "outcome_known",
        *[item.proposal_id for item in request.proposals],
    ]
    return score_panel[keep], daily_metrics, proposal_summary, frontier_payload


def _evaluate_one_proposal_daily(score_panel: pd.DataFrame, proposal_id: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    previous_top5: set[str] | None = None
    for decision_date, frame in score_panel.groupby("decision_as_of_trade_date", sort=True):
        data = frame[["instrument", "score", proposal_id, "economic_net_excess_bps", "outcome_known"]].copy()
        data[proposal_id] = pd.to_numeric(data[proposal_id], errors="coerce")
        data["score"] = pd.to_numeric(data["score"], errors="coerce")
        data["economic_net_excess_bps"] = pd.to_numeric(data["economic_net_excess_bps"], errors="coerce")
        finite_score = np.isfinite(data[proposal_id])
        known = data["outcome_known"].fillna(False).astype(bool) & np.isfinite(data["economic_net_excess_bps"])
        evaluable = data.loc[finite_score & known].copy()
        rank_ic = _safe_correlation(evaluable[proposal_id], evaluable["economic_net_excess_bps"], method="spearman")
        parent_corr = _safe_correlation(evaluable[proposal_id], evaluable["score"], method="spearman")
        proposal_top = evaluable.nlargest(5, proposal_id, keep="first")
        parent_top = data.loc[known & np.isfinite(data["score"])].nlargest(5, "score", keep="first")
        proposal_top5 = float(proposal_top["economic_net_excess_bps"].mean()) if len(proposal_top) == 5 else np.nan
        parent_top5 = float(parent_top["economic_net_excess_bps"].mean()) if len(parent_top) == 5 else np.nan
        top5_ids = set(proposal_top["instrument"].astype(str)) if len(proposal_top) == 5 else set()
        churn = (
            np.nan if previous_top5 is None or len(top5_ids) != 5 else float(1.0 - len(previous_top5 & top5_ids) / 5.0)
        )
        if len(top5_ids) == 5:
            previous_top5 = top5_ids
        rows.append(
            {
                "proposal_id": proposal_id,
                "decision_as_of_trade_date": pd.Timestamp(decision_date),
                "row_count": len(data),
                "finite_score_count": int(finite_score.sum()),
                "finite_fraction": float(finite_score.mean()) if len(data) else 0.0,
                "evaluable_count": len(evaluable),
                "rank_ic": rank_ic,
                "proposal_top5_net_excess_bps": proposal_top5,
                "parent_top5_net_excess_bps": parent_top5,
                "top5_lift_bps": proposal_top5 - parent_top5,
                "parent_score_spearman": parent_corr,
                "top5_churn": churn,
            }
        )
    return pd.DataFrame(rows)


def _summarize_one_proposal(
    *,
    proposal: QEAlphaProposalV1,
    daily: pd.DataFrame,
    score_panel: pd.DataFrame,
    request: FrozenAdvisoryQEAlphaMVERequestV1,
    seed: int,
) -> dict[str, Any]:
    rank_ic = _finite_array(daily["rank_ic"])
    lift = _finite_array(daily["top5_lift_bps"])
    proposal_top5 = _finite_array(daily["proposal_top5_net_excess_bps"])
    parent_corr = _finite_array(daily["parent_score_spearman"])
    churn = _finite_array(daily["top5_churn"])
    coverage = float(np.isfinite(pd.to_numeric(score_panel[proposal.proposal_id], errors="coerce")).mean())
    raw_rank = _moving_block_interval(
        rank_ic,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=seed,
        alpha=0.05,
    )
    family_rank = _moving_block_interval(
        rank_ic,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=seed,
        alpha=0.05 / request.familywise_trial_count,
    )
    raw_lift = _moving_block_interval(
        lift,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=seed + 1,
        alpha=0.05,
    )
    family_lift = _moving_block_interval(
        lift,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=seed + 1,
        alpha=0.05 / request.familywise_trial_count,
    )
    evaluable_days = min(len(rank_ic), len(lift))
    mean_parent_corr = float(np.mean(parent_corr)) if len(parent_corr) else None
    reason_codes: list[str] = []
    if evaluable_days < request.minimum_evaluable_days:
        reason_codes.append("EVALUABLE_DAYS_BELOW_MINIMUM")
    if coverage < request.minimum_finite_fraction:
        reason_codes.append("FINITE_COVERAGE_BELOW_MINIMUM")
    if family_rank[0] is None or family_rank[0] <= 0:
        reason_codes.append("FAMILYWISE_RANK_IC_LOWER_NOT_POSITIVE")
    if family_lift[0] is None or family_lift[0] <= 0:
        reason_codes.append("FAMILYWISE_TOP5_LIFT_LOWER_NOT_POSITIVE")
    if mean_parent_corr is None or abs(mean_parent_corr) > request.maximum_parent_spearman:
        reason_codes.append("PARENT_SCORE_CORRELATION_ABOVE_MAXIMUM")
    if not len(rank_ic) or not len(lift):
        reason_codes.append("DEGENERATE_DAILY_METRICS")
    dsr = _deflated_sharpe_diagnostic(lift, trial_count=request.familywise_trial_count)
    return {
        "proposal_id": proposal.proposal_id,
        "family": proposal.family,
        "economic_hypothesis": proposal.economic_hypothesis,
        "expression_sha256": proposal.expression_sha256,
        "source_fields": list(proposal.source_fields),
        "row_count": len(score_panel),
        "finite_fraction": coverage,
        "evaluable_day_count": evaluable_days,
        "rank_ic_day_count": len(rank_ic),
        "rank_ic_mean": _mean_or_none(rank_ic),
        "rank_ic_median": _median_or_none(rank_ic),
        "rank_ic_std": _std_or_none(rank_ic),
        "rank_ic_positive_fraction": _positive_fraction(rank_ic),
        "rank_ic_confidence_lower": raw_rank[0],
        "rank_ic_confidence_upper": raw_rank[1],
        "familywise_rank_ic_lower": family_rank[0],
        "familywise_rank_ic_upper": family_rank[1],
        "proposal_top5_mean_net_excess_bps": _mean_or_none(proposal_top5),
        "top5_lift_mean_bps": _mean_or_none(lift),
        "top5_lift_confidence_lower_bps": raw_lift[0],
        "top5_lift_confidence_upper_bps": raw_lift[1],
        "familywise_top5_lift_lower_bps": family_lift[0],
        "familywise_top5_lift_upper_bps": family_lift[1],
        "top5_churn_mean": _mean_or_none(churn),
        "parent_score_spearman_mean": mean_parent_corr,
        "daily_lift_sharpe": dsr["observed_sharpe"],
        "daily_lift_skew": dsr["skew"],
        "daily_lift_kurtosis": dsr["kurtosis"],
        "deflated_sharpe_probability": dsr["deflated_sharpe_probability"],
        "eligible": not reason_codes,
        "reason_codes": reason_codes,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
    }


def _moving_block_interval(
    values: Sequence[float] | np.ndarray,
    *,
    block_length: int = MVE_BLOCK_LENGTH,
    repetitions: int = MVE_BOOTSTRAP_REPETITIONS,
    seed: int = MVE_RANDOM_SEED,
    alpha: float = 0.05,
) -> tuple[float | None, float | None]:
    array = _finite_array(values)
    if not len(array):
        return None, None
    if len(array) == 1:
        value = float(array[0])
        return value, value
    rng = np.random.default_rng(seed)
    block = max(1, min(int(block_length), len(array)))
    blocks_needed = int(math.ceil(len(array) / block))
    starts = rng.integers(0, len(array), size=(repetitions, blocks_needed))
    offsets = np.arange(block)
    indexes = (starts[:, :, None] + offsets[None, None, :]) % len(array)
    samples = array[indexes.reshape(repetitions, -1)[:, : len(array)]]
    means = samples.mean(axis=1)
    lower, upper = np.quantile(means, [alpha / 2.0, 1.0 - alpha / 2.0])
    return float(lower), float(upper)


def _deflated_sharpe_diagnostic(values: Sequence[float], *, trial_count: int) -> dict[str, Any]:
    array = _finite_array(values)
    if len(array) < 3:
        return {
            "observed_sharpe": None,
            "skew": None,
            "kurtosis": None,
            "deflated_sharpe_probability": None,
        }
    std = float(array.std(ddof=1))
    if std <= 0:
        return {
            "observed_sharpe": None,
            "skew": None,
            "kurtosis": None,
            "deflated_sharpe_probability": None,
        }
    observed = float(array.mean() / std)
    centered = array - array.mean()
    skew = float(np.mean(centered**3) / (np.mean(centered**2) ** 1.5))
    kurtosis = float(np.mean(centered**4) / (np.mean(centered**2) ** 2))
    normal = NormalDist()
    euler_gamma = 0.5772156649015329
    maximum_expected = (
        (1.0 - euler_gamma) * normal.inv_cdf(1.0 - 1.0 / trial_count)
        + euler_gamma * normal.inv_cdf(1.0 - 1.0 / (trial_count * math.e))
    ) / math.sqrt(len(array))
    denominator = math.sqrt(max(1e-12, 1.0 - skew * observed + ((kurtosis - 1.0) / 4.0) * observed**2))
    z_value = (observed - maximum_expected) * math.sqrt(len(array) - 1) / denominator
    return {
        "observed_sharpe": observed,
        "skew": skew,
        "kurtosis": kurtosis,
        "deflated_sharpe_probability": float(normal.cdf(z_value)),
    }


def _validate_route_sources(
    *,
    n1: Path,
    n2a: Path,
    n2b: Path,
    action: Path,
    exit_bundle: Path,
) -> tuple[Path, Path, str, str]:
    required_by_root = {
        n1: (
            "manifest.json",
            "request.json",
            "oracle_receipt.json",
            "learnability_receipt.json",
            "quadrant_receipt.json",
        ),
        n2a: ("manifest.json", "audit_receipt.json", "arm_summary.json"),
        n2b: (
            "manifest.json",
            "request.json",
            "audit_receipt.json",
            "arm_summary.json",
            "pairwise_summary.json",
            "arm_signal_outcomes.parquet",
        ),
        action: (
            "manifest.json",
            "request.json",
            "audit_receipt.json",
            "entry_summary.json",
            "entry_support.json",
            "exit_summary.json",
            "exit_support.json",
        ),
        exit_bundle: ("manifest.json", "learnability_receipt.json"),
    }
    for root, members in required_by_root.items():
        missing = [name for name in members if not (root / name).is_file()]
        if missing:
            _raise(
                "N3 route evidence bundle is incomplete",
                "ADVISORY_N3_ROUTE_EVIDENCE_INVALID",
                root=root.as_posix(),
                missing=missing,
            )
        manifest = _read_json(root / "manifest.json", "ADVISORY_N3_ROUTE_EVIDENCE_INVALID")
        if manifest.get("sealed_holdout_accessed") is not False:
            _raise(
                "N3 route evidence accessed the sealed holdout",
                "ADVISORY_N3_ROUTE_EVIDENCE_INVALID",
                root=root.as_posix(),
            )
    oracle = _read_json(n1 / "oracle_receipt.json", "ADVISORY_N3_ROUTE_EVIDENCE_INVALID")
    quadrant = _read_json(n1 / "quadrant_receipt.json", "ADVISORY_N3_ROUTE_EVIDENCE_INVALID")
    n2b_summary = _read_json(n2b / "arm_summary.json", "ADVISORY_N3_ROUTE_EVIDENCE_INVALID")
    entry = _read_json(action / "entry_summary.json", "ADVISORY_N3_ROUTE_EVIDENCE_INVALID")
    entry_support = _read_json(action / "entry_support.json", "ADVISORY_N3_ROUTE_EVIDENCE_INVALID")
    exit_learnability = _read_json(exit_bundle / "learnability_receipt.json", "ADVISORY_N3_ROUTE_EVIDENCE_INVALID")
    try:
        n1_top50 = oracle["recall_summary"]["top50"]
        n2b_top50 = n2b_summary["arms"][CURRENT_PARENT_ARM_ID]["metrics"]["top50_winner_recall"]
        entry_arms = entry["arms"]
    except (KeyError, TypeError) as exc:
        _raise(
            "N3 route evidence schema is incomplete",
            "ADVISORY_N3_ROUTE_EVIDENCE_INVALID",
            error_type=type(exc).__name__,
        )
    if (
        quadrant.get("direction_ready") is not False
        or float(n1_top50["confidence_upper"]) >= 0.20
        or float(n2b_top50["confidence_upper"]) >= 0.20
    ):
        _raise(
            "N3 upstream route preconditions are not satisfied",
            "ADVISORY_N3_ROUTE_EVIDENCE_INVALID",
        )
    policy_by_arm = {
        "FIXED_3_CASH": "FIXED_GAP_3",
        "FIXED_3_REPLACE": "FIXED_GAP_3",
        "FIXED_5_CASH": "FIXED_GAP_5",
        "FIXED_5_REPLACE": "FIXED_GAP_5",
        "DYNAMIC_Q90_CASH": "FROZEN_DYNAMIC",
        "DYNAMIC_Q90_REPLACE": "FROZEN_DYNAMIC",
    }
    entry_confirmatory_positive = any(
        entry_support.get(policy_id, {}).get("evidence_class") == "CONFIRMATORY_ELIGIBLE"
        and float(entry_arms.get(arm_id, {}).get("paired_lift_ci_lower_bps", float("-inf"))) > 0
        for arm_id, policy_id in policy_by_arm.items()
    )
    if entry_confirmatory_positive:
        _raise(
            "N3 route cannot bypass a confirmatory-positive Entry arm",
            "ADVISORY_N3_ROUTE_EVIDENCE_INVALID",
        )
    if (
        exit_learnability.get("evidence_sufficient") is not False
        or exit_learnability.get("policy_lift", {}).get("evidence_state") == "HIGH"
    ):
        _raise(
            "N3 route cannot bypass confirmed Exit learnability",
            "ADVISORY_N3_ROUTE_EVIDENCE_INVALID",
        )
    action_request = _read_json(action / "request.json", "ADVISORY_N3_ROUTE_EVIDENCE_INVALID")
    n2b_request = _read_json(n2b / "request.json", "ADVISORY_N3_ROUTE_EVIDENCE_INVALID")
    n2b_manifest = _read_json(n2b / "manifest.json", "ADVISORY_N3_ROUTE_EVIDENCE_INVALID")
    registry_path = _local_path(action_request["registry_path"]).resolve()
    route_path = _local_path(action_request["route_path"]).resolve()
    if not registry_path.is_file():
        _raise(
            "N3 route trial registry is missing",
            "ADVISORY_N3_ROUTE_EVIDENCE_INVALID",
            registry_path=registry_path.as_posix(),
        )
    dataset_identity = str(n2b_request["dataset_identity"])
    policy_identity = str(n2b_manifest.get("policy_identity", ""))
    if len(dataset_identity) != 64 or len(policy_identity) != 64:
        _raise(
            "N3 route dataset or policy identity is invalid",
            "ADVISORY_N3_ROUTE_EVIDENCE_INVALID",
        )
    return registry_path, route_path, dataset_identity, policy_identity


def _validate_preparation(preparation: FrozenAdvisoryQEAlphaMVEPreparationV1) -> None:
    invalid = (
        preparation.status != "PREPARATION_ONLY_NO_RESEARCH_EVIDENCE"
        or preparation.budget.total_proposal_budget != MVE_PROPOSAL_COUNT
        or preparation.research_evidence_produced
        or preparation.sealed_holdout_accessed
        or preparation.deployable
    )
    if invalid:
        _raise(
            "QE alpha MVE preparation is not the frozen no-evidence preparation",
            "ADVISORY_QE_ALPHA_MVE_REQUEST_INVALID",
        )


def _load_verified_sources(request: FrozenAdvisoryQEAlphaMVERequestV1) -> dict[str, Any]:
    n2b = Path(request.n2b_bundle_path)
    n2b_request = _read_json(n2b / "request.json", "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH")
    n2b_manifest = _read_json(n2b / "manifest.json", "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH")
    n1_oracle_ref = next(item for item in request.evidence_refs if item.role == "n3_n1_oracle_receipt")
    n1_request = _read_json(
        Path(n1_oracle_ref.artifact_uri).parent / "request.json",
        "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
    )
    benchmark = str(n1_request.get("cost_policy", {}).get("benchmark_instrument", ""))
    if not benchmark:
        _raise(
            "QE alpha MVE benchmark identity is missing",
            "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    columns = [
        "arm_id",
        "decision_as_of_trade_date",
        "instrument",
        "score",
        "economic_net_excess_bps",
        "outcome_known",
    ]
    try:
        outcomes = pd.read_parquet(
            request.outcomes_path,
            columns=columns,
            filters=[("arm_id", "=", CURRENT_PARENT_ARM_ID), ("outcome_known", "=", True)],
        )
    except Exception as exc:
        _raise(
            "QE alpha MVE outcome source cannot be projected",
            "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
            error_type=type(exc).__name__,
        )
    outcomes = _normalize_outcomes(outcomes, request=request)
    policy_identity = str(n2b_manifest.get("policy_identity", ""))
    if len(str(n2b_request.get("dataset_identity", ""))) != 64 or len(policy_identity) != 64:
        _raise(
            "QE alpha MVE N2-B source identity is invalid",
            "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    return {
        "outcomes": outcomes,
        "benchmark": benchmark,
        "n2b_request": n2b_request,
        "n2b_manifest": n2b_manifest,
    }


def _normalize_outcomes(
    outcomes: pd.DataFrame,
    *,
    request: FrozenAdvisoryQEAlphaMVERequestV1,
) -> pd.DataFrame:
    required = {
        "arm_id",
        "decision_as_of_trade_date",
        "instrument",
        "score",
        "economic_net_excess_bps",
        "outcome_known",
    }
    if not required.issubset(outcomes.columns):
        _raise(
            "QE alpha MVE outcome source omits required columns",
            "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
            missing_columns=sorted(required - set(outcomes.columns)),
        )
    frame = outcomes.loc[
        (outcomes["arm_id"].astype(str) == CURRENT_PARENT_ARM_ID) & outcomes["outcome_known"].fillna(False).astype(bool)
    ].copy()
    frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    frame["instrument"] = frame["instrument"].astype(str).str.upper()
    frame = frame.loc[
        frame["decision_as_of_trade_date"].between(pd.Timestamp(request.signal_start), pd.Timestamp(request.signal_end))
    ].copy()
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    frame["economic_net_excess_bps"] = pd.to_numeric(frame["economic_net_excess_bps"], errors="coerce")
    if frame.empty or frame.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise(
            "QE alpha MVE outcome PIT keys are empty or duplicated",
            "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    if frame["decision_as_of_trade_date"].nunique() < request.minimum_evaluable_days:
        _raise(
            "QE alpha MVE outcome source has too few decision dates",
            "ADVISORY_QE_ALPHA_MVE_COVERAGE_INSUFFICIENT",
            decision_date_count=int(frame["decision_as_of_trade_date"].nunique()),
        )
    return frame.sort_values(["decision_as_of_trade_date", "instrument"]).reset_index(drop=True)


def _verify_environment(
    request: FrozenAdvisoryQEAlphaMVERequestV1,
    *,
    verify_registry_before: bool,
) -> None:
    repo = Path(request.repository_root)
    if _cross_os_git_commit(repo) != request.repository_commit:
        _raise(
            "QE alpha MVE repository commit drift",
            "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    dirty = _cross_os_git_dirty_paths(repo)
    if dirty:
        _raise(
            "QE alpha MVE repository became dirty",
            "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
            dirty_paths=dirty[:20],
        )
    for reference in request.evidence_refs:
        if reference.role == "n3_trial_registry_before" and not verify_registry_before:
            continue
        _verify_ref(reference)
    _verify_ref(request.static_factor_ref)
    _verify_ref(request.outcomes_ref)
    static_path = Path(request.static_factor_ref.artifact_uri)
    if _static_schema_sha256(static_path) != request.static_schema_sha256:
        _raise(
            "QE alpha MVE static factor schema drift",
            "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    _validate_static_fields(static_path, request.proposals)
    preparation = load_qe_alpha_mve_preparation(request.preparation_path)
    _validate_preparation(preparation)
    if not Path(request.outcomes_path).is_file() or not Path(request.qlib_daily_root).is_dir():
        _raise(
            "QE alpha MVE frozen source path is missing",
            "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
        )


def _publish_bundle(
    *,
    request: FrozenAdvisoryQEAlphaMVERequestV1,
    sources: Mapping[str, Any],
    score_panel: pd.DataFrame,
    daily_metrics: pd.DataFrame,
    proposal_summary: Mapping[str, Any],
    frontier: Mapping[str, Any],
    elapsed_seconds: float,
) -> Path:
    root = Path(request.output_root) / "qe_alpha_mve_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{request.request_id}.", dir=root))
    try:
        _write_json(temporary / "request.json", request.model_dump(mode="json"))
        _write_json(temporary / "route_receipt.json", request.route_receipt.model_dump(mode="json"))
        _write_json(
            temporary / "proposal_roster.json",
            {
                "schema_version": "advisory_qe_alpha_mve_proposal_roster_v1",
                "request_sha256": request.request_sha256,
                "proposals": [item.model_dump(mode="json") for item in request.proposals],
                "proposal_count": len(request.proposals),
                "sealed_holdout_accessed": False,
            },
        )
        score_panel.to_parquet(temporary / "score_panel.parquet", index=False)
        daily_metrics.to_parquet(temporary / "daily_metrics.parquet", index=False)
        _write_json(temporary / "proposal_summary.json", proposal_summary)
        _write_json(temporary / "frontier_receipt.json", frontier)
        _verify_ref(request.static_factor_ref)
        source_payload = {
            "schema_version": "advisory_qe_alpha_mve_source_identity_v1",
            "request_sha256": request.request_sha256,
            "evidence_refs": [item.model_dump(mode="json") for item in request.evidence_refs],
            "static_factor_ref": request.static_factor_ref.model_dump(mode="json"),
            "static_schema_sha256": request.static_schema_sha256,
            "outcomes_ref": request.outcomes_ref.model_dump(mode="json"),
            "outcome_row_count": len(sources["outcomes"]),
            "outcome_decision_date_count": int(sources["outcomes"]["decision_as_of_trade_date"].nunique()),
            "benchmark_instrument": sources["benchmark"],
            "n2b_dataset_identity": sources["n2b_request"]["dataset_identity"],
            "n2b_policy_identity": sources["n2b_manifest"]["policy_identity"],
            "repository_commit": request.repository_commit,
            "sealed_holdout_accessed": False,
            "database_read_performed": False,
            "network_read_performed": False,
        }
        _write_json(temporary / "source_identity_receipt.json", source_payload)
        temporary_bytes = sum(path.stat().st_size for path in temporary.iterdir() if path.is_file())
        if temporary_bytes > request.resource_max_temp_bytes:
            _raise(
                "QE alpha MVE temporary output exceeds the frozen limit",
                "ADVISORY_QE_ALPHA_MVE_RESOURCE_LIMIT_EXCEEDED",
                temporary_bytes=temporary_bytes,
            )
        resource_payload = {
            "schema_version": "advisory_qe_alpha_mve_resource_report_v1",
            "elapsed_seconds": float(elapsed_seconds),
            "peak_rss_bytes": _peak_rss_bytes(),
            "temporary_bytes": temporary_bytes,
            "resource_max_rss_bytes": request.resource_max_rss_bytes,
            "resource_max_temp_bytes": request.resource_max_temp_bytes,
            "wall_time_limit_seconds": None,
            "wall_time_is_telemetry_only": True,
        }
        _write_json(temporary / "resource_report.json", resource_payload)
        result_descriptors = _descriptors_for(temporary, RESULT_IDENTITY_MEMBERS)
        selected = frontier.get("selected_proposal_id")
        eligible = tuple(str(item) for item in frontier.get("eligible_proposal_ids", ()))
        receipt = build_qe_alpha_mve_receipt(
            request_sha256=request.request_sha256,
            selected_trial_count=1 if selected else 0,
            selected_proposal_id=selected,
            eligible_proposal_ids=eligible,
            next_task=("N3_ALPHA_CANDIDATE_CONFIRMATION_DESIGN" if selected else "N3_ALPHA_INFORMATION_SET_REVIEW"),
            source_identity_sha256=sha256_file(temporary / "source_identity_receipt.json"),
            result_files_sha256=canonical_json_sha256(result_descriptors),
            resource_report_sha256=sha256_file(temporary / "resource_report.json"),
        )
        _write_json(temporary / "mve_receipt.json", receipt.model_dump(mode="json"))
        bundle_id = canonical_json_sha256(
            {
                "schema_version": QE_ALPHA_MVE_BUNDLE_SCHEMA,
                "request_sha256": request.request_sha256,
                "receipt_sha256": receipt.receipt_sha256,
            }
        )
        destination = root / bundle_id
        record = _build_registry_record(
            request=request,
            receipt_path=temporary / "mve_receipt.json",
            receipt_artifact_uri=(destination / "mve_receipt.json").as_posix(),
            receipt=receipt,
            policy_identity=str(sources["n2b_manifest"]["policy_identity"]),
        )
        _write_json(temporary / "registry_record.json", record.model_dump(mode="json"))
        descriptors = _file_descriptors(temporary)
        if set(descriptors) != BUNDLE_MEMBERS:
            _raise(
                "QE alpha MVE bundle member roster drift",
                "ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID",
                members=sorted(descriptors),
            )
        manifest = {
            "schema_version": QE_ALPHA_MVE_BUNDLE_SCHEMA,
            "bundle_id": bundle_id,
            "request_sha256": request.request_sha256,
            "route_receipt_sha256": request.route_receipt.receipt_sha256,
            "receipt_sha256": receipt.receipt_sha256,
            "objective_contract": ObjectiveContract.ALPHA_RANKING.value,
            "study_type": ResearchStudyType.EXPLORATORY_SCREEN.value,
            "decision_use": DecisionUse.NAVIGATION_ONLY.value,
            "result_class": ResearchResultClass.EXPLORATORY.value,
            "planned_trial_count": 24,
            "generated_trial_count": 24,
            "evaluated_trial_count": 24,
            "selected_trial_count": receipt.selected_trial_count,
            "sealed_holdout_accessed": False,
            "deployable": False,
            "runtime_eligible": False,
            "factor_catalog_written": False,
            "files": descriptors,
        }
        _write_json(temporary / "manifest.json", manifest)
        if destination.exists():
            _read_bundle(destination)
            return destination
        temporary.replace(destination)
        _read_bundle(destination)
        return destination
    except Exception:
        # Preserve an unpublished hidden directory for forensic inspection.
        # No manifest is delivered and no registry mutation occurs on failure.
        raise


def _build_registry_record(
    *,
    request: FrozenAdvisoryQEAlphaMVERequestV1,
    receipt_path: Path,
    receipt_artifact_uri: str,
    receipt: AdvisoryQEAlphaMVEReceiptV1,
    policy_identity: str,
) -> AdvisoryResearchTrialRecordV1:
    schema_identity = canonical_json_sha256(
        {
            "static_schema_sha256": request.static_schema_sha256,
            "proposal_expression_sha256": [item.expression_sha256 for item in request.proposals],
        }
    )
    return build_trial_record(
        experiment_id=N3_EXPERIMENT_ID,
        attempt_id=request.request_id,
        research_stage="N3_QE_UPSTREAM_ALPHA_MVE_EXPLORATORY_SCREEN",
        study_type=ResearchStudyType.EXPLORATORY_SCREEN,
        hypothesis_family_id=N3_HYPOTHESIS_FAMILY_ID,
        parent_lineage=(
            "ADVISORY-N1-TIER1-ORACLE",
            "ADVISORY-N1-TIER1-LEARNABILITY",
            "ADVISORY-N2A-THREE-ARM-ALPHA-AUDIT",
            "ADVISORY-N2B-INDEPENDENT-PACKAGE-ALPHA-AUDIT-V2",
            "ADVISORY-N2-ENTRY-GUARD-ORACLE",
            "ADVISORY-N2-EXIT-LABEL-ORACLE",
            "ADVISORY-N2-EXIT-LEARNABILITY-V1",
        ),
        unique_variable="FROZEN_24_DECLARATIVE_UPSTREAM_ALPHA_PROPOSALS",
        objective_contract=ObjectiveContract.ALPHA_RANKING,
        dataset_identity=request.dataset_identity,
        schema_identity=schema_identity,
        policy_identity=policy_identity,
        planned_trial_count=24,
        generated_trial_count=24,
        evaluated_trial_count=24,
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
            EvidenceReferenceV1(
                role="n3_qe_alpha_mve_receipt",
                artifact_uri=receipt_artifact_uri,
                sha256=sha256_file(receipt_path),
                size_bytes=receipt_path.stat().st_size,
            ),
        ),
        recorded_at=datetime.now(timezone.utc),
    )


def _read_bundle(path: Path) -> dict[str, Any]:
    manifest = _read_json(path / "manifest.json", "ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID")
    request = FrozenAdvisoryQEAlphaMVERequestV1.model_validate_json((path / "request.json").read_text(encoding="utf-8"))
    route_receipt = AdvisoryN3RouteReceiptV1.model_validate_json(
        (path / "route_receipt.json").read_text(encoding="utf-8")
    )
    receipt = AdvisoryQEAlphaMVEReceiptV1.model_validate_json((path / "mve_receipt.json").read_text(encoding="utf-8"))
    record = AdvisoryResearchTrialRecordV1.model_validate_json(
        (path / "registry_record.json").read_text(encoding="utf-8")
    )
    descriptors = manifest.get("files")
    if not isinstance(descriptors, dict) or set(descriptors) != BUNDLE_MEMBERS:
        _raise(
            "QE alpha MVE bundle descriptor roster is invalid",
            "ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID",
        )
    for name, descriptor in descriptors.items():
        member = path / name
        if not member.is_file():
            _raise(
                "QE alpha MVE bundle member is missing",
                "ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID",
                member=name,
            )
        actual_rows = _parquet_row_count(member) if member.suffix == ".parquet" else None
        if (
            sha256_file(member) != descriptor.get("sha256")
            or member.stat().st_size != descriptor.get("size_bytes")
            or (actual_rows is not None and actual_rows != descriptor.get("row_count"))
        ):
            _raise(
                "QE alpha MVE bundle member identity drift",
                "ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID",
                member=name,
            )
    expected_bundle_id = canonical_json_sha256(
        {
            "schema_version": QE_ALPHA_MVE_BUNDLE_SCHEMA,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
        }
    )
    result_descriptors = {name: descriptors[name] for name in sorted(RESULT_IDENTITY_MEMBERS)}
    receipt_descriptor = descriptors["mve_receipt.json"]
    resource = _read_json(path / "resource_report.json", "ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID")
    frontier = _read_json(path / "frontier_receipt.json", "ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID")
    invalid = (
        manifest.get("schema_version") != QE_ALPHA_MVE_BUNDLE_SCHEMA
        or path.name != expected_bundle_id
        or manifest.get("bundle_id") != expected_bundle_id
        or manifest.get("request_sha256") != request.request_sha256
        or manifest.get("route_receipt_sha256") != route_receipt.receipt_sha256
        or route_receipt.receipt_sha256 != request.route_receipt.receipt_sha256
        or manifest.get("receipt_sha256") != receipt.receipt_sha256
        or receipt.request_sha256 != request.request_sha256
        or receipt.source_identity_sha256 != descriptors["source_identity_receipt.json"]["sha256"]
        or receipt.result_files_sha256 != canonical_json_sha256(result_descriptors)
        or receipt.resource_report_sha256 != descriptors["resource_report.json"]["sha256"]
        or record.attempt_id != request.request_id
        or record.experiment_id != N3_EXPERIMENT_ID
        or record.study_type != ResearchStudyType.EXPLORATORY_SCREEN
        or record.decision_use != DecisionUse.NAVIGATION_ONLY
        or record.result_class != ResearchResultClass.EXPLORATORY
        or record.planned_trial_count != 24
        or record.generated_trial_count != 24
        or record.evaluated_trial_count != 24
        or record.selected_trial_count != receipt.selected_trial_count
        or len(record.evidence_refs) != 1
        or record.evidence_refs[0].sha256 != receipt_descriptor["sha256"]
        or record.evidence_refs[0].size_bytes != receipt_descriptor["size_bytes"]
        or frontier.get("selected_proposal_id") != receipt.selected_proposal_id
        or frontier.get("selected_trial_count") != receipt.selected_trial_count
        or manifest.get("objective_contract") != ObjectiveContract.ALPHA_RANKING.value
        or manifest.get("study_type") != ResearchStudyType.EXPLORATORY_SCREEN.value
        or manifest.get("decision_use") != DecisionUse.NAVIGATION_ONLY.value
        or manifest.get("result_class") != ResearchResultClass.EXPLORATORY.value
        or manifest.get("sealed_holdout_accessed") is not False
        or manifest.get("deployable") is not False
        or manifest.get("runtime_eligible") is not False
        or manifest.get("factor_catalog_written") is not False
        or resource.get("wall_time_limit_seconds") is not None
        or resource.get("wall_time_is_telemetry_only") is not True
        or not isinstance(resource.get("peak_rss_bytes"), int)
        or int(resource.get("peak_rss_bytes", -1)) > request.resource_max_rss_bytes
        or int(resource.get("temporary_bytes", -1)) > request.resource_max_temp_bytes
    )
    if invalid:
        _raise(
            "QE alpha MVE bundle relational identity is invalid",
            "ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID",
        )
    return {
        "manifest": manifest,
        "request": request,
        "route_receipt": route_receipt,
        "receipt": receipt,
        "record": record,
    }


def _find_existing_bundle(request: FrozenAdvisoryQEAlphaMVERequestV1) -> Path | None:
    root = Path(request.output_root) / "qe_alpha_mve_bundles"
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
            "one QE alpha MVE request maps to multiple immutable bundles",
            "ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID",
        )
    if matches:
        _read_bundle(matches[0])
        return matches[0]
    return None


def _deliver_bundle(
    *,
    request: FrozenAdvisoryQEAlphaMVERequestV1,
    bundle_path: Path,
) -> dict[str, Any]:
    loaded = _read_bundle(bundle_path)
    registry = AdvisoryResearchTrialRegistryV1(request.registry_path).append_batch((loaded["record"],))
    route = _write_n3_route_page(
        path=Path(request.route_path),
        request=request,
        receipt=loaded["receipt"],
        bundle_id=loaded["manifest"]["bundle_id"],
        registry_sha256=str(registry["registry_sha256"]),
    )
    return {"registry": registry, "route": route}


def _write_n3_route_page(
    *,
    path: Path,
    request: FrozenAdvisoryQEAlphaMVERequestV1,
    receipt: AdvisoryQEAlphaMVEReceiptV1,
    bundle_id: str,
    registry_sha256: str,
) -> dict[str, Any]:
    selected = receipt.selected_proposal_id or "NONE"
    content = "\n".join(
        (
            "# Advisory 当前研究路线",
            "",
            f"- active_main_line: `{request.route_receipt.selected_route}`",
            "- active_auxiliary_line: `NONE`",
            f"- next_task: `{receipt.next_task}`",
            f"- exploratory_candidate: `{selected}`",
            f"- route_receipt_sha256: `{request.route_receipt.receipt_sha256}`",
            f"- qe_alpha_mve_bundle_id: `{bundle_id}`",
            f"- trial_registry_sha256: `{registry_sha256}`",
            "- objective_contract: `ALPHA_RANKING`",
            "- decision_use: `NAVIGATION_ONLY`",
            "- sealed_holdout_accessed: `false`",
            "- deployable/runtime_eligible/factor_catalog_written: `false/false/false`",
            "",
            "该页面只记录唯一主线，不构成激活、StrategyPackage、Selection、资金仓位或运行时授权。",
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


def _run_response(
    request: FrozenAdvisoryQEAlphaMVERequestV1,
    bundle: Path,
    delivery: Mapping[str, Any],
    *,
    exact_retry: bool,
) -> dict[str, Any]:
    inspected = inspect_qe_alpha_mve_bundle(bundle)
    return {
        **inspected,
        "request_id": request.request_id,
        "bundle_path": bundle.as_posix(),
        "exact_retry": exact_retry,
        "registry": dict(delivery["registry"]),
        "route": dict(delivery["route"]),
    }


def _check_resource_limits(request: FrozenAdvisoryQEAlphaMVERequestV1, stage: str) -> None:
    rss = _peak_rss_bytes()
    if rss > request.resource_max_rss_bytes:
        _raise(
            "QE alpha MVE resident memory exceeds the frozen limit",
            "ADVISORY_QE_ALPHA_MVE_RESOURCE_LIMIT_EXCEEDED",
            stage=stage,
            peak_rss_bytes=rss,
        )


def _static_schema_sha256(path: Path) -> str:
    try:
        import pyarrow.parquet as pq

        schema = pq.ParquetFile(path).schema_arrow
    except Exception as exc:
        _raise(
            "QE alpha MVE static factor schema cannot be read",
            "ADVISORY_QE_ALPHA_MVE_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    return canonical_json_sha256({"fields": [{"name": field.name, "type": str(field.type)} for field in schema]})


def _validate_static_fields(path: Path, proposals: Sequence[QEAlphaProposalV1]) -> None:
    try:
        import pyarrow.parquet as pq

        available = set(pq.ParquetFile(path).schema_arrow.names)
    except Exception as exc:
        _raise(
            "QE alpha MVE static factor schema cannot be read",
            "ADVISORY_QE_ALPHA_MVE_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    required = {
        field
        for proposal in proposals
        for field in proposal.source_fields
        if field not in DAILY_FIELDS and field != "market_regime"
    }
    missing = sorted(required - available)
    if missing:
        _raise(
            "QE alpha MVE static factor source omits proposal fields",
            "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
            missing_columns=missing,
        )


def _verify_ref(reference: EvidenceReferenceV1) -> None:
    path = Path(reference.artifact_uri)
    actual = evidence_reference_for_file(path, role=reference.role)
    if (actual.sha256, actual.size_bytes) != (reference.sha256, reference.size_bytes):
        _raise(
            "QE alpha MVE evidence reference drift",
            "ADVISORY_QE_ALPHA_MVE_SOURCE_IDENTITY_MISMATCH",
            role=reference.role,
        )


def _write_immutable_request(path: Path, request: FrozenAdvisoryQEAlphaMVERequestV1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        request.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    if path.exists():
        try:
            existing = FrozenAdvisoryQEAlphaMVERequestV1.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception as exc:
            _raise(
                "QE alpha MVE request path contains invalid content",
                "ADVISORY_QE_ALPHA_MVE_REQUEST_INVALID",
                error_type=type(exc).__name__,
            )
        if existing.request_sha256 != request.request_sha256:
            _raise(
                "QE alpha MVE request path contains a different identity",
                "ADVISORY_QE_ALPHA_MVE_REQUEST_INVALID",
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
            descriptor["row_count"] = _parquet_row_count(path)
        result[path.name] = descriptor
    return result


def _descriptors_for(root: Path, names: Sequence[str] | frozenset[str]) -> dict[str, dict[str, Any]]:
    all_descriptors = _file_descriptors(root)
    return {name: all_descriptors[name] for name in sorted(names)}


def _parquet_row_count(path: Path) -> int:
    try:
        import pyarrow.parquet as pq

        return int(pq.ParquetFile(path).metadata.num_rows)
    except Exception as exc:
        _raise(
            "QE alpha MVE parquet metadata cannot be read",
            "ADVISORY_QE_ALPHA_MVE_BUNDLE_INVALID",
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )


def _read_json(path: Path, reason_code: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _raise(
            "QE alpha MVE JSON evidence cannot be read",
            reason_code,
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )
    if not isinstance(value, dict):
        _raise("QE alpha MVE JSON evidence is not an object", reason_code, path=path.as_posix())
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ),
        encoding="utf-8",
    )


def _safe_correlation(left: pd.Series, right: pd.Series, *, method: str) -> float:
    data = (
        pd.DataFrame(
            {
                "left": pd.to_numeric(left, errors="coerce"),
                "right": pd.to_numeric(right, errors="coerce"),
            }
        )
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    if len(data) < 3 or data["left"].nunique() < 2 or data["right"].nunique() < 2:
        return np.nan
    return float(data["left"].corr(data["right"], method=method))


def _finite_array(values: Sequence[Any] | pd.Series | np.ndarray) -> np.ndarray:
    array = pd.to_numeric(pd.Series(values), errors="coerce").to_numpy(float)
    return array[np.isfinite(array)]


def _mean_or_none(values: np.ndarray) -> float | None:
    return float(np.mean(values)) if len(values) else None


def _median_or_none(values: np.ndarray) -> float | None:
    return float(np.median(values)) if len(values) else None


def _std_or_none(values: np.ndarray) -> float | None:
    return float(np.std(values, ddof=1)) if len(values) > 1 else None


def _positive_fraction(values: np.ndarray) -> float | None:
    return float(np.mean(values > 0)) if len(values) else None


def _local_path(path: str | Path) -> Path:
    raw = str(path)
    if os.name == "nt" and raw.startswith("/mnt/") and len(raw) > 6:
        drive = raw[5].upper()
        tail = raw[6:].replace("/", "\\")
        return Path(f"{drive}:\\{tail}")
    return Path(raw)


def _peak_rss_bytes() -> int:
    if _resource is None:
        return 0
    value = int(_resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss)
    return value if platform.system() == "Darwin" else value * 1024


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "QE_ALPHA_MVE_BUNDLE_SCHEMA",
    "build_source_panel",
    "compile_proposal_scores",
    "evaluate_proposals",
    "inspect_qe_alpha_mve_bundle",
    "prepare_qe_alpha_mve_request",
    "run_qe_alpha_mve",
]
