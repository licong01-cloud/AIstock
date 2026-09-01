from __future__ import annotations

import gc
import json
import math
import os
import platform
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, time as datetime_time
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

try:
    import resource as _resource
except ModuleNotFoundError:  # Windows imports prepare/inspect and runs unit tests.
    _resource = None

from backend.services.advisory_model_first.action_value_contracts import (
    AdvisoryIncrementalValueLabelV1,
)
from backend.services.advisory_model_first.entry_exit_formal_contracts import (
    ENTRY_ARM_IDS,
    ENTRY_DECISION_END,
    ENTRY_DECISION_START,
    ENTRY_MATURED_ROW_COUNT,
    ENTRY_OVERLAP_DAY_COUNT,
    ENTRY_OVERLAP_ROW_COUNT,
    EXIT_INTERVENTION_POLICY_SHA256,
    ORACLE_ENTRY_POLICY_SHA256,
    ActionSupportSpecV1,
    AdvisoryN2ActionAuditReceiptV1,
    EntryFormalArmSpecV1,
    FrozenAdvisoryN2ActionAuditRequestV1,
    build_n2_action_receipt,
    build_n2_action_request,
)
from backend.services.advisory_model_first.entry_guard_decision import (
    AdvisoryEntryGuardDecisionV1,
    EntryGuardAction,
    EntryGuardMarketObservationV1,
    EntryGuardMode,
    build_entry_guard_policy,
    build_entry_guard_signal,
    evaluate_entry_guard,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.exit_label_oracle import (
    ExitLabelOracleResult,
    build_exit_label_oracle,
)
from backend.services.advisory_model_first.incremental_value_labels import (
    build_entry_incremental_value_labels,
    build_intervention_support_from_labels,
)
from backend.services.advisory_model_first.policy_contracts import (
    transition_policy_from_payload,
)
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.price_range_bundle import (
    read_price_range_bundle_manifest,
)
from backend.services.advisory_model_first.price_range_contracts import (
    FrozenAdvisoryPriceRangeTrainingRequestV1,
)
from backend.services.advisory_model_first.qe_file_source import (
    initialize_qlib,
    load_qlib_daily,
    load_suspend_rows,
)
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
)
from backend.services.advisory_model_first.tier1_oracle_pipeline import (
    authorize_n1_development_access,
    build_tier1_benchmark_regimes,
    inspect_n1_bundle,
    load_verified_n1_sources,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


ENTRY_EXPERIMENT_ID = "ADVISORY-N2-ENTRY-GUARD-ORACLE"
EXIT_EXPERIMENT_ID = "ADVISORY-N2-EXIT-LABEL-ORACLE"
ACTION_FAMILY_ID = "ADVISORY-N2-ACTION-DIAGNOSTICS-V1"
ACTION_BUNDLE_SCHEMA = "advisory_n2_action_audit_bundle_v1"
KEYS = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
BUNDLE_MEMBERS = {
    "request.json",
    "source_identity_receipt.json",
    "entry_decisions.parquet",
    "entry_labels.parquet",
    "entry_daily.parquet",
    "entry_summary.json",
    "entry_support.json",
    "exit_labels.parquet",
    "exit_decisions.parquet",
    "exit_episode_best.parquet",
    "exit_summary.json",
    "exit_support.json",
    "resource_report.json",
    "audit_receipt.json",
    "registry_records.json",
}


def prepare_n2_action_audit_request(
    *,
    n1_request_path: str | Path,
    n1_bundle_path: str | Path,
    m4_request_path: str | Path,
    m4_bundle_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
) -> FrozenAdvisoryN2ActionAuditRequestV1:
    n1_request_file = Path(n1_request_path).resolve()
    n1_bundle = Path(n1_bundle_path).resolve()
    m4_request_file = Path(m4_request_path).resolve()
    m4_bundle = Path(m4_bundle_path).resolve()
    repo = Path(repository_root).resolve()
    n1 = AdvisoryN1Tier1RequestV1.model_validate_json(n1_request_file.read_text(encoding="utf-8"))
    m4 = FrozenAdvisoryPriceRangeTrainingRequestV1.model_validate_json(m4_request_file.read_text(encoding="utf-8"))
    n1_inspection = inspect_n1_bundle(n1_bundle)
    m4_manifest = read_price_range_bundle_manifest(
        m4_bundle,
        expected_bundle_id=m4_bundle.name,
    )
    policy_manifest_path = Path(n1.policy_dataset_bundle_root) / "manifest.json"
    policy_manifest = json.loads(policy_manifest_path.read_text(encoding="utf-8"))
    if n1_inspection["request_sha256"] != n1.request_sha256:
        _raise("N1 bundle/request identity mismatch", "ADVISORY_N2_ACTION_SOURCE_IDENTITY_MISMATCH")
    if m4_manifest.get("request_sha256") != m4.request_sha256:
        _raise("M4 bundle/request identity mismatch", "ADVISORY_N2_ACTION_SOURCE_IDENTITY_MISMATCH")
    _verify_m4_n1_candidate_identity(
        m4_manifest=m4_manifest,
        policy_manifest=policy_manifest,
    )

    overlap = _entry_overlap(
        m4_bundle / "test_predictions.parquet",
        Path(n1.policy_dataset_bundle_root) / "candidate_episode_labels.parquet",
    )
    _validate_overlap_shape(overlap)
    commit = _repository_commit(repo)
    dirty = _repository_dirty(repo)
    if dirty:
        _raise(
            "N2 action request requires a clean repository",
            "ADVISORY_N2_ACTION_REQUEST_INVALID",
            dirty_paths=dirty[:20],
        )

    policies = tuple(
        build_entry_guard_policy(mode)
        for mode in (
            EntryGuardMode.NO_GUARD,
            EntryGuardMode.FIXED_GAP_3,
            EntryGuardMode.FIXED_GAP_5,
            EntryGuardMode.FROZEN_DYNAMIC,
        )
    )
    policy_by_mode = {item.mode: item for item in policies}
    arms = (
        EntryFormalArmSpecV1(
            arm_id="NO_GUARD_BASELINE",
            guard_mode=EntryGuardMode.NO_GUARD,
            guard_policy_sha256=policy_by_mode[EntryGuardMode.NO_GUARD].policy_sha256,
            fill_policy="BASELINE_TOP5",
        ),
        EntryFormalArmSpecV1(
            arm_id="FIXED_3_CASH",
            guard_mode=EntryGuardMode.FIXED_GAP_3,
            guard_policy_sha256=policy_by_mode[EntryGuardMode.FIXED_GAP_3].policy_sha256,
            fill_policy="CASH",
        ),
        EntryFormalArmSpecV1(
            arm_id="FIXED_3_REPLACE",
            guard_mode=EntryGuardMode.FIXED_GAP_3,
            guard_policy_sha256=policy_by_mode[EntryGuardMode.FIXED_GAP_3].policy_sha256,
            fill_policy="RANK_ONLY_REPLACEMENT",
        ),
        EntryFormalArmSpecV1(
            arm_id="FIXED_5_CASH",
            guard_mode=EntryGuardMode.FIXED_GAP_5,
            guard_policy_sha256=policy_by_mode[EntryGuardMode.FIXED_GAP_5].policy_sha256,
            fill_policy="CASH",
        ),
        EntryFormalArmSpecV1(
            arm_id="FIXED_5_REPLACE",
            guard_mode=EntryGuardMode.FIXED_GAP_5,
            guard_policy_sha256=policy_by_mode[EntryGuardMode.FIXED_GAP_5].policy_sha256,
            fill_policy="RANK_ONLY_REPLACEMENT",
        ),
        EntryFormalArmSpecV1(
            arm_id="DYNAMIC_Q90_CASH",
            guard_mode=EntryGuardMode.FROZEN_DYNAMIC,
            guard_policy_sha256=policy_by_mode[EntryGuardMode.FROZEN_DYNAMIC].policy_sha256,
            fill_policy="CASH",
        ),
        EntryFormalArmSpecV1(
            arm_id="DYNAMIC_Q90_REPLACE",
            guard_mode=EntryGuardMode.FROZEN_DYNAMIC,
            guard_policy_sha256=policy_by_mode[EntryGuardMode.FROZEN_DYNAMIC].policy_sha256,
            fill_policy="RANK_ONLY_REPLACEMENT",
        ),
        EntryFormalArmSpecV1(
            arm_id="PERFECT_SKIP_CASH_ORACLE",
            guard_mode=None,
            guard_policy_sha256=ORACLE_ENTRY_POLICY_SHA256,
            fill_policy="CASH",
            oracle=True,
        ),
        EntryFormalArmSpecV1(
            arm_id="PERFECT_SKIP_REPLACE_ORACLE",
            guard_mode=None,
            guard_policy_sha256=ORACLE_ENTRY_POLICY_SHA256,
            fill_policy="RANK_ONLY_REPLACEMENT",
            oracle=True,
        ),
    )
    n0_root = Path(n1.n0_completion_ref.artifact_uri).resolve().parent
    parent_spike = n0_root / "parent_prediction_extension_receipt.json"
    request = build_n2_action_request(
        n1_request_path=n1_request_file.as_posix(),
        n1_request_ref=evidence_reference_for_file(n1_request_file, role="n2_action_n1_request"),
        n1_bundle_path=n1_bundle.as_posix(),
        n1_bundle_manifest_ref=evidence_reference_for_file(
            n1_bundle / "manifest.json", role="n2_action_n1_bundle_manifest"
        ),
        policy_dataset_manifest_ref=evidence_reference_for_file(
            policy_manifest_path,
            role="n2_action_policy_dataset_manifest",
        ),
        m4_request_path=m4_request_file.as_posix(),
        m4_request_ref=evidence_reference_for_file(m4_request_file, role="n2_action_m4_request"),
        m4_bundle_path=m4_bundle.as_posix(),
        m4_bundle_manifest_ref=evidence_reference_for_file(
            m4_bundle / "manifest.json", role="n2_action_m4_bundle_manifest"
        ),
        m4_predictions_ref=evidence_reference_for_file(
            m4_bundle / "test_predictions.parquet", role="n2_action_m4_test_predictions"
        ),
        n0_completion_ref=n1.n0_completion_ref,
        parent_spike_path=parent_spike.as_posix(),
        parent_spike_ref=evidence_reference_for_file(parent_spike, role="n2_action_parent_spike"),
        research_window_contract_ref=n1.research_window_contract_ref,
        registry_path=n1.registry_path,
        route_path=n1.route_path,
        dataset_identity=n1.dataset_identity,
        feature_schema_hash=m4.feature_schema_hash,
        baseline_policy_sha256=n1.baseline_policy_sha256,
        shadow_policy_sha256=n1.shadow_policy_sha256,
        cost_policy_sha256=n1.cost_policy_sha256,
        entry_guard_policies=policies,
        entry_arms=arms,
        exit_intervention_policy_sha256=EXIT_INTERVENTION_POLICY_SHA256,
        entry_support_spec=ActionSupportSpecV1(),
        exit_support_spec=ActionSupportSpecV1(),
        qlib_daily_root=n1.qlib_daily_root,
        suspend_data_root=n1.suspend_data_root,
        repository_root=repo.as_posix(),
        repository_commit=commit,
        output_root=Path(output_root).resolve().as_posix(),
    )
    _write_immutable_request(Path(output_path), request)
    return request


def run_n2_action_audit(request_path: str | Path) -> dict[str, Any]:
    request = FrozenAdvisoryN2ActionAuditRequestV1.model_validate_json(Path(request_path).read_text(encoding="utf-8"))
    _verify_environment(request)
    sources = _load_verified_sources(request)
    existing = _find_existing_bundle(request)
    if existing is not None:
        delivery = _deliver_bundle(request=request, bundle_path=existing)
        return _run_response(request=request, bundle_path=existing, status="EXISTING_BUNDLE", delivery=delivery)

    started = time.monotonic()
    initialize_qlib(request.qlib_daily_root)
    entry = _run_entry_audit(request=request, sources=sources)
    _check_rss(request, "entry_audit")
    gc.collect()
    exit_result = _run_exit_audit(request=request, sources=sources)
    _check_rss(request, "exit_audit")
    elapsed = time.monotonic() - started
    bundle = _publish_bundle(
        request=request,
        sources=sources,
        entry=entry,
        exit_result=exit_result,
        elapsed_seconds=elapsed,
    )
    delivery = _deliver_bundle(request=request, bundle_path=bundle)
    return _run_response(request=request, bundle_path=bundle, status="COMPLETE", delivery=delivery)


def inspect_n2_action_audit_bundle(bundle_path: str | Path) -> dict[str, Any]:
    loaded = _read_bundle(Path(bundle_path).resolve())
    return {
        "status": "VALID",
        "bundle_id": loaded["manifest"]["bundle_id"],
        "request_id": loaded["request"].request_id,
        "request_sha256": loaded["request"].request_sha256,
        "receipt_sha256": loaded["receipt"].receipt_sha256,
        "entry_arm_count": len(loaded["receipt"].entry_summary["arms"]),
        "exit_episode_count": loaded["receipt"].exit_summary["episode_count"],
        "sealed_holdout_accessed": False,
        "deployable": False,
    }


def _run_entry_audit(*, request: FrozenAdvisoryN2ActionAuditRequestV1, sources: dict[str, Any]) -> dict[str, Any]:
    overlap = sources["entry_overlap"].copy()
    symbols = sorted(overlap["instrument"].unique())
    market = load_qlib_daily(
        symbols,
        start=request.entry_decision_start.isoformat(),
        end=pd.Timestamp(overlap["target_trade_date"].max()).date().isoformat(),
    )
    suspend = load_suspend_rows(
        request.suspend_data_root,
        start=request.entry_decision_start.isoformat(),
        end=pd.Timestamp(overlap["target_trade_date"].max()).date().isoformat(),
        instruments=symbols,
        full_day_only=True,
    )
    suspended = {
        (pd.Timestamp(row.trade_date).normalize(), str(row.instrument).upper())
        for row in suspend.itertuples(index=False)
    }
    policies = {item.mode: item for item in request.entry_guard_policies}
    decisions_by_mode: dict[EntryGuardMode, list[AdvisoryEntryGuardDecisionV1]] = {mode: [] for mode in policies}
    decision_records: list[dict[str, Any]] = []
    gap_errors: list[float] = []
    for row in overlap.itertuples(index=False):
        decision_date = pd.Timestamp(row.decision_as_of_trade_date).normalize()
        target_date = pd.Timestamp(row.target_trade_date).normalize()
        symbol = str(row.instrument).upper()
        decision_market = _market_row(market, decision_date, symbol)
        target_market = _market_row(market, target_date, symbol)
        reference_price = _positive(decision_market.get("close"))
        target_open = _positive(target_market.get("open"))
        if reference_price is None or target_open is None:
            _raise(
                "Entry Qlib reference/open price is missing",
                "ADVISORY_N2_ENTRY_GAP_PARITY_FAILED",
                decision_date=decision_date.date().isoformat(),
                instrument=symbol,
            )
        actual_gap = target_open / reference_price - 1.0
        gap_error = abs(actual_gap - float(row.entry_gap_return))
        gap_errors.append(gap_error)
        if gap_error > 1e-10:
            _raise(
                "Entry Qlib gap differs from the frozen M4 label",
                "ADVISORY_N2_ENTRY_GAP_PARITY_FAILED",
                decision_date=decision_date.date().isoformat(),
                instrument=symbol,
                gap_error=gap_error,
            )
        dynamic_gap_bps = max(0.0, float(row.entry_gap_q90) * 10000.0)
        signal = build_entry_guard_signal(
            decision_date=decision_date.date(),
            target_trade_date=target_date.date(),
            instrument=symbol,
            selection_rank=int(row.selection_rank),
            reference_price=reference_price,
            entry_gap_q10=float(row.entry_gap_q10),
            entry_gap_q50=float(row.entry_gap_q50),
            entry_gap_q90=float(row.entry_gap_q90),
            max_acceptable_gap_bps=dynamic_gap_bps,
            max_buy_price=reference_price * (1.0 + dynamic_gap_bps / 10000.0),
            source_binding_sha256=request.m4_bundle_manifest_ref.sha256,
            feature_schema_sha256=request.feature_schema_hash,
            information_cutoff=datetime.combine(decision_date.date(), datetime_time(15, 0)),
        )
        observation = EntryGuardMarketObservationV1(
            target_trade_date=target_date.date(),
            instrument=symbol,
            observed_at=datetime.combine(target_date.date(), datetime_time(9, 31)),
            open_price=target_open,
            current_price=None,
            limit_up_price=_positive(target_market.get("up_limit_price")),
            limit_down_price=_positive(target_market.get("down_limit_price")),
            suspended=(target_date, symbol) in suspended,
            suspend_status="FULL_DAY" if (target_date, symbol) in suspended else None,
        )
        for mode, policy in policies.items():
            decision = evaluate_entry_guard(policy=policy, signal=signal, observation=observation)
            decisions_by_mode[mode].append(decision)
            decision_records.append(
                {
                    **decision.model_dump(mode="python"),
                    "guard_mode": mode.value,
                    "entry_gap_return": float(row.entry_gap_return),
                    "baseline_label_status": str(row.label_status),
                    "baseline_net_return_bps": _finite(row.net_return_bps),
                }
            )
    label_models_by_mode: dict[EntryGuardMode, tuple[AdvisoryIncrementalValueLabelV1, ...]] = {}
    label_records: list[dict[str, Any]] = []
    baseline = sources["entry_baseline"]
    for mode, decisions in decisions_by_mode.items():
        result = build_entry_incremental_value_labels(
            decisions=decisions,
            baseline_episode_labels=baseline,
            baseline_policy_sha256=request.shadow_policy_sha256,
            cost_policy_sha256=request.cost_policy_sha256,
        )
        label_models_by_mode[mode] = result.labels
        for item in result.labels:
            label_records.append({**item.model_dump(mode="python"), "guard_mode": mode.value})
    actions = {
        (item.decision_date, item.target_trade_date, item.instrument, mode): item
        for mode, items in decisions_by_mode.items()
        for item in items
    }
    daily = _build_entry_daily(request=request, overlap=overlap, actions=actions)
    summary = _entry_summary(daily)
    decisions_frame = pd.DataFrame(decision_records)
    labels_frame = pd.DataFrame(label_records)
    summary["decision_action_counts"] = _grouped_value_counts(
        decisions_frame,
        group_column="guard_mode",
        value_column="action",
    )
    summary["label_status_counts"] = _grouped_value_counts(
        labels_frame,
        group_column="guard_mode",
        value_column="status",
    )
    benchmark = load_qlib_daily(
        ["000300.SH"],
        start="2025-10-01",
        end=request.entry_decision_end.isoformat(),
        fields=("$close",),
    )
    regimes = build_tier1_benchmark_regimes(
        benchmark,
        sorted(pd.to_datetime(overlap["decision_as_of_trade_date"]).dt.normalize().unique()),
    )
    support = {}
    spec = request.entry_support_spec
    for mode in (EntryGuardMode.FIXED_GAP_3, EntryGuardMode.FIXED_GAP_5, EntryGuardMode.FROZEN_DYNAMIC):
        top5 = [item for item in label_models_by_mode[mode] if _entry_rank(overlap, item) <= 5]
        support[mode.value] = build_intervention_support_from_labels(
            labels=top5,
            intervention_policy_sha256=policies[mode].policy_sha256,
            regimes_by_decision_date={key.date(): value for key, value in regimes.items()},
            required_regimes=spec.required_regimes,
            minimum_intervention_count=spec.minimum_intervention_count,
            minimum_intervention_day_fraction=spec.minimum_intervention_day_fraction,
            minimum_days_per_required_regime=spec.minimum_days_per_required_regime,
            block_length_trading_days=spec.block_length_trading_days,
            minimum_effective_intervention_block_count=spec.minimum_effective_intervention_block_count,
        ).model_dump(mode="json")
    return {
        "decisions": decisions_frame,
        "labels": labels_frame,
        "daily": daily,
        "summary": summary,
        "support": support,
        "gap_parity": {
            "row_count": len(gap_errors),
            "max_abs_error": max(gap_errors),
            "exact_atol": 1e-10,
        },
    }


def _run_exit_audit(*, request: FrozenAdvisoryN2ActionAuditRequestV1, sources: dict[str, Any]) -> dict[str, Any]:
    n1: AdvisoryN1Tier1RequestV1 = sources["n1_request"]
    n1_sources = sources["n1_sources"]
    pit_symbols = sorted({span.ts_code for span in n1_sources["pit_snapshot"].spans})
    daily = load_qlib_daily(
        pit_symbols,
        start=request.exit_decision_start.isoformat(),
        end=request.outcome_cutoff.isoformat(),
    )
    benchmark = load_qlib_daily(
        [n1.cost_policy.benchmark_instrument],
        start="2023-09-01",
        end=request.outcome_cutoff.isoformat(),
        fields=("$open", "$close"),
    )
    suspend = load_suspend_rows(
        request.suspend_data_root,
        start=request.exit_decision_start.isoformat(),
        end=request.outcome_cutoff.isoformat(),
        instruments=pit_symbols,
        full_day_only=True,
    )
    policy_payload = json.loads(
        (Path(n1.policy_dataset_bundle_root) / "shadow_policy.json").read_text(encoding="utf-8")
    )
    policy = transition_policy_from_payload(policy_payload)
    result = build_exit_label_oracle(
        rankings=n1_sources["historical_rankings"],
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        trading_calendar=n1_sources["n1_calendar"],
        policy=policy,
        policy_sha256=request.shadow_policy_sha256,
        intervention_policy_sha256=request.exit_intervention_policy_sha256,
        cost_policy=n1.cost_policy,
        request_identity={"request_id": request.request_id, "request_sha256": request.request_sha256},
        candidate_decision_dates=n1_sources["decision_dates"],
    )
    baseline_parity = _verify_exit_baseline_parity(
        actual=result.baseline.labels,
        expected=sources["exit_baseline"],
    )
    best, selected_models = _exit_episode_best(result)
    regimes = build_tier1_benchmark_regimes(
        benchmark,
        sorted(pd.to_datetime(best["decision_date"]).dt.normalize().unique()),
    )
    spec = request.exit_support_spec
    support = build_intervention_support_from_labels(
        labels=selected_models,
        intervention_policy_sha256=request.exit_intervention_policy_sha256,
        regimes_by_decision_date={key.date(): value for key, value in regimes.items()},
        required_regimes=spec.required_regimes,
        minimum_intervention_count=spec.minimum_intervention_count,
        minimum_intervention_day_fraction=spec.minimum_intervention_day_fraction,
        minimum_days_per_required_regime=spec.minimum_days_per_required_regime,
        block_length_trading_days=spec.block_length_trading_days,
        minimum_effective_intervention_block_count=spec.minimum_effective_intervention_block_count,
    )
    return {
        "labels": result.label_frame,
        "decisions": result.decision_frame,
        "episode_best": best,
        "summary": _exit_summary(best=best, result=result),
        "support": support.model_dump(mode="json"),
        "baseline_parity": baseline_parity,
    }


def _build_entry_daily(
    *,
    request: FrozenAdvisoryN2ActionAuditRequestV1,
    overlap: pd.DataFrame,
    actions: Mapping[tuple[Any, ...], AdvisoryEntryGuardDecisionV1],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for decision_date, group in overlap.groupby("decision_as_of_trade_date", sort=True):
        ordered = group.sort_values("selection_rank")
        for arm in request.entry_arms:
            selected_values: list[float | None] = []
            selected_ranks: list[int] = []
            cash_slots = 0
            skip_count = 0
            reduce_count = 0
            waiting_count = 0
            oracle = arm.oracle
            candidates = ordered.head(5) if arm.fill_policy in {"BASELINE_TOP5", "CASH"} else ordered
            for candidate in candidates.itertuples(index=False):
                rank = int(candidate.selection_rank)
                known_value = _finite(candidate.net_return_bps) if str(candidate.label_status) == "MATURED" else None
                if oracle:
                    if known_value is None:
                        selected_values.append(None)
                        break
                    if known_value > 0:
                        selected_values.append(known_value)
                        selected_ranks.append(rank)
                    elif arm.fill_policy == "CASH" and rank <= 5:
                        selected_values.append(0.0)
                        cash_slots += 1
                        skip_count += 1
                    else:
                        skip_count += 1
                else:
                    mode = arm.guard_mode
                    if mode is None:
                        raise AssertionError("non-oracle Entry arm requires guard_mode")
                    action = actions[
                        (
                            pd.Timestamp(candidate.decision_as_of_trade_date).date(),
                            pd.Timestamp(candidate.target_trade_date).date(),
                            str(candidate.instrument).upper(),
                            mode,
                        )
                    ].action
                    if action == EntryGuardAction.SKIP:
                        skip_count += 1
                        if arm.fill_policy != "RANK_ONLY_REPLACEMENT" and rank <= 5:
                            selected_values.append(0.0)
                            cash_slots += 1
                        continue
                    if action == EntryGuardAction.WAITING:
                        waiting_count += 1
                        if arm.fill_policy != "RANK_ONLY_REPLACEMENT" and rank <= 5:
                            selected_values.append(0.0)
                            cash_slots += 1
                        continue
                    if action == EntryGuardAction.REDUCE:
                        reduce_count += 1
                    selected_values.append(known_value)
                    selected_ranks.append(rank)
                if arm.fill_policy == "RANK_ONLY_REPLACEMENT" and len(selected_values) >= 5:
                    break
            while len(selected_values) < 5:
                selected_values.append(0.0)
                cash_slots += 1
            available = len(selected_values) == 5 and all(value is not None for value in selected_values)
            rows.append(
                {
                    "decision_date": pd.Timestamp(decision_date).normalize(),
                    "arm_id": arm.arm_id,
                    "status": "AVAILABLE" if available else "DATA_UNAVAILABLE",
                    "daily_net_return_bps": (float(np.mean(selected_values)) if available else None),
                    "cash_slot_count": cash_slots,
                    "replacement_count": sum(rank > 5 for rank in selected_ranks),
                    "skip_count": skip_count,
                    "reduce_count": reduce_count,
                    "waiting_count": waiting_count,
                    "selected_ranks": selected_ranks,
                }
            )
    result = pd.DataFrame(rows)
    counts = result.groupby("arm_id").size().to_dict()
    if set(counts) != set(ENTRY_ARM_IDS) or any(value != ENTRY_OVERLAP_DAY_COUNT for value in counts.values()):
        _raise("Entry daily arm shape is incomplete", "ADVISORY_N2_ENTRY_COVERAGE_INSUFFICIENT")
    return result


def _entry_summary(daily: pd.DataFrame) -> dict[str, Any]:
    baseline = daily[daily["arm_id"].eq("NO_GUARD_BASELINE")][["decision_date", "daily_net_return_bps"]].rename(
        columns={"daily_net_return_bps": "baseline_bps"}
    )
    arms: dict[str, Any] = {}
    for arm_id, group in daily.groupby("arm_id", sort=False):
        available = group[group["status"].eq("AVAILABLE")].copy()
        values = available["daily_net_return_bps"].astype(float)
        paired = available.merge(baseline, on="decision_date", how="inner").dropna(subset=["baseline_bps"])
        lift = paired["daily_net_return_bps"].astype(float) - paired["baseline_bps"].astype(float)
        ci = _moving_block_interval(lift, block_length=20, repetitions=2000, seed=20260902)
        arms[str(arm_id)] = {
            "decision_day_count": len(group),
            "available_day_count": len(available),
            "mean_daily_net_return_bps": _mean(values),
            "cumulative_net_return": _cumulative_return(values),
            "max_drawdown": _max_drawdown(values),
            "tail_5pct_bps": float(values.quantile(0.05)) if len(values) else None,
            "positive_day_fraction": float((values > 0).mean()) if len(values) else None,
            "cash_slot_fraction": float(group["cash_slot_count"].sum() / (len(group) * 5)),
            "replacement_count": int(group["replacement_count"].sum()),
            "skip_count": int(group["skip_count"].sum()),
            "reduce_count": int(group["reduce_count"].sum()),
            "waiting_count": int(group["waiting_count"].sum()),
            "paired_lift_mean_bps": _mean(lift),
            "paired_lift_ci_lower_bps": ci[0],
            "paired_lift_ci_upper_bps": ci[1],
        }
    return {"arms": arms, "deployable": False, "evidence": "HISTORICAL_REPLAY_NAVIGATION_ONLY"}


def _exit_episode_best(
    result: ExitLabelOracleResult,
) -> tuple[pd.DataFrame, tuple[AdvisoryIncrementalValueLabelV1, ...]]:
    models_by_id = {item.label_id: item for item in result.labels}
    available = result.label_frame[result.label_frame["status"].map(_enum_value).eq("AVAILABLE")].copy()
    if available.empty:
        _raise("Exit oracle has no available labels", "ADVISORY_N2_EXIT_BASELINE_PARITY_FAILED")
    indexes = available.groupby("episode_id")["incremental_net_value_bps"].idxmax()
    best = available.loc[indexes].copy().reset_index(drop=True)
    best["oracle_action"] = np.where(
        best["incremental_net_value_bps"].astype(float) > 0.0,
        "EXIT_NEXT_OPEN",
        "HOLD",
    )
    best["realized_oracle_lift_bps"] = best["incremental_net_value_bps"].astype(float).clip(lower=0.0)
    baseline_metadata = result.baseline.labels[
        [
            "episode_label_id",
            "entry_trade_date",
            "effective_exit_date",
            "selection_rank",
            "net_return_bps",
        ]
    ].rename(
        columns={
            "episode_label_id": "episode_id",
            "effective_exit_date": "baseline_effective_exit_date",
            "net_return_bps": "baseline_net_return_bps",
        }
    )
    best = best.merge(
        baseline_metadata,
        on="episode_id",
        how="left",
        validate="one_to_one",
    )
    if best["entry_trade_date"].isna().any():
        _raise(
            "Exit oracle episode metadata is incomplete",
            "ADVISORY_N2_EXIT_BASELINE_PARITY_FAILED",
        )
    best["oracle_review_calendar_days_from_entry"] = (
        pd.to_datetime(best["decision_date"]) - pd.to_datetime(best["entry_trade_date"])
    ).dt.days
    best["baseline_holding_calendar_days"] = (
        pd.to_datetime(best["baseline_effective_exit_date"]) - pd.to_datetime(best["entry_trade_date"])
    ).dt.days
    selected = tuple(models_by_id[str(value)] for value in best["label_id"])
    return best, selected


def _exit_summary(*, best: pd.DataFrame, result: ExitLabelOracleResult) -> dict[str, Any]:
    lift = best["realized_oracle_lift_bps"].astype(float)
    raw = best["incremental_net_value_bps"].astype(float)
    daily = best.groupby(pd.to_datetime(best["decision_date"]).dt.normalize())["realized_oracle_lift_bps"].mean()
    ci = _moving_block_interval(daily, block_length=20, repetitions=2000, seed=20260902)
    decision_states = result.decision_frame["execution_state"].map(_enum_value)
    status_counts = result.label_frame["status"].map(_enum_value).value_counts().sort_index()
    state_counts = decision_states.value_counts().sort_index()
    deferred = pd.to_numeric(
        result.decision_frame.loc[
            decision_states.eq("DEFERRED_TO_FIRST_EXECUTABLE"),
            "deferred_trading_days",
        ],
        errors="coerce",
    ).dropna()
    baseline_episode_count = int(result.baseline.labels["episode_label_id"].nunique())
    return {
        "episode_count": baseline_episode_count,
        "evaluable_episode_count": len(best),
        "unavailable_episode_count": baseline_episode_count - len(best),
        "positive_intervention_count": int((raw > 0).sum()),
        "positive_intervention_fraction": float((raw > 0).mean()),
        "mean_oracle_lift_bps": _mean(lift),
        "median_oracle_lift_bps": float(lift.median()),
        "mean_raw_exit_advantage_bps": _mean(raw),
        "negative_raw_advantage_fraction": float((raw < 0).mean()),
        "raw_exit_advantage_5pct_bps": float(raw.quantile(0.05)),
        "oracle_lift_5pct_bps": float(lift.quantile(0.05)),
        "daily_lift_ci_lower_bps": ci[0],
        "daily_lift_ci_upper_bps": ci[1],
        "daily_lift_mdd_proxy": _max_drawdown(daily),
        "mean_oracle_review_calendar_days_from_entry": _mean(best["oracle_review_calendar_days_from_entry"]),
        "mean_baseline_holding_calendar_days": _mean(best["baseline_holding_calendar_days"]),
        "deferred_decision_count": len(deferred),
        "mean_deferred_trading_days": _mean(deferred),
        "max_deferred_trading_days": int(deferred.max()) if len(deferred) else 0,
        "label_status_counts": {str(key): int(value) for key, value in status_counts.items()},
        "execution_state_counts": {str(key): int(value) for key, value in state_counts.items()},
        "all_label_count": len(result.labels),
        "all_decision_count": len(result.decisions),
        "deployable": False,
        "evidence": "FUTURE_INFORMATION_CEILING",
    }


def _verify_exit_baseline_parity(*, actual: pd.DataFrame, expected: pd.DataFrame) -> dict[str, Any]:
    expected_top5 = expected[expected["selection_rank"].le(5)].copy()
    left = _normalize_keys(actual)
    right = _normalize_keys(expected_top5)
    merged = left.merge(right, on=KEYS, how="outer", suffixes=("_actual", "_expected"), indicator=True)
    if not merged["_merge"].eq("both").all():
        _raise(
            "Exit baseline episode keys differ from the frozen policy dataset",
            "ADVISORY_N2_EXIT_BASELINE_PARITY_FAILED",
            non_common_count=int((merged["_merge"] != "both").sum()),
        )
    if not merged["label_status_actual"].astype(str).eq(merged["label_status_expected"].astype(str)).all():
        _raise("Exit baseline statuses drifted", "ADVISORY_N2_EXIT_BASELINE_PARITY_FAILED")
    mature = merged[merged["label_status_actual"].astype(str).eq("MATURED")]
    for field in ("entry_price", "exit_price", "net_return_bps"):
        errors = (
            pd.to_numeric(mature[f"{field}_actual"], errors="coerce")
            - pd.to_numeric(mature[f"{field}_expected"], errors="coerce")
        ).abs()
        if errors.isna().any() or (errors > 1e-7).any():
            _raise(
                "Exit baseline numeric values drifted",
                "ADVISORY_N2_EXIT_BASELINE_PARITY_FAILED",
                field=field,
                max_error=float(errors.max()) if len(errors) else None,
            )
    for field in ("entry_trade_date", "effective_exit_date"):
        if (
            not pd.to_datetime(mature[f"{field}_actual"])
            .dt.normalize()
            .eq(pd.to_datetime(mature[f"{field}_expected"]).dt.normalize())
            .all()
        ):
            _raise(
                "Exit baseline date values drifted",
                "ADVISORY_N2_EXIT_BASELINE_PARITY_FAILED",
                field=field,
            )
    return {"row_count": len(merged), "matured_row_count": len(mature), "status": "EXACT"}


def _publish_bundle(
    *,
    request: FrozenAdvisoryN2ActionAuditRequestV1,
    sources: dict[str, Any],
    entry: dict[str, Any],
    exit_result: dict[str, Any],
    elapsed_seconds: float,
) -> Path:
    root = Path(request.output_root).resolve() / "action_audit_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix=".n2-action-", dir=root))
    try:
        _write_json(temp / "request.json", request.model_dump(mode="json"))
        source_receipt = {
            "schema_version": "advisory_n2_action_source_identity_receipt_v1",
            "request_sha256": request.request_sha256,
            "entry_overlap_day_count": request.entry_overlap_day_count,
            "entry_overlap_row_count": request.entry_overlap_row_count,
            "entry_gap_parity": entry["gap_parity"],
            "exit_baseline_parity": exit_result["baseline_parity"],
            "refs": sources["ref_payload"],
            "sealed_holdout_accessed": False,
        }
        _write_json(temp / "source_identity_receipt.json", source_receipt)
        _write_parquet(temp / "entry_decisions.parquet", entry["decisions"])
        _write_parquet(temp / "entry_labels.parquet", entry["labels"])
        _write_parquet(temp / "entry_daily.parquet", entry["daily"])
        _write_json(temp / "entry_summary.json", entry["summary"])
        _write_json(temp / "entry_support.json", entry["support"])
        _write_parquet(temp / "exit_labels.parquet", exit_result["labels"])
        _write_parquet(temp / "exit_decisions.parquet", exit_result["decisions"])
        _write_parquet(temp / "exit_episode_best.parquet", exit_result["episode_best"])
        _write_json(temp / "exit_summary.json", exit_result["summary"])
        _write_json(temp / "exit_support.json", exit_result["support"])
        resource_report = {
            "schema_version": "advisory_n2_action_resource_report_v1",
            "elapsed_seconds": elapsed_seconds,
            "peak_rss_bytes": _peak_rss_bytes(),
            "resource_max_rss_bytes": request.resource_max_rss_bytes,
        }
        _write_json(temp / "resource_report.json", resource_report)
        receipt = build_n2_action_receipt(
            request_sha256=request.request_sha256,
            entry_summary=entry["summary"],
            exit_summary=exit_result["summary"],
            source_identity_sha256=sha256_file(temp / "source_identity_receipt.json"),
            resource_report_sha256=sha256_file(temp / "resource_report.json"),
        )
        _write_json(temp / "audit_receipt.json", receipt.model_dump(mode="json"))
        bundle_id = canonical_json_sha256(
            {
                "schema_version": ACTION_BUNDLE_SCHEMA,
                "request_sha256": request.request_sha256,
                "receipt_sha256": receipt.receipt_sha256,
            }
        )
        final = root / bundle_id
        receipt_ref = EvidenceReferenceV1(
            role="n2_action_audit_receipt",
            artifact_uri=(final / "audit_receipt.json").as_posix(),
            sha256=sha256_file(temp / "audit_receipt.json"),
            size_bytes=(temp / "audit_receipt.json").stat().st_size,
        )
        records = _registry_records(request=request, receipt_ref=receipt_ref)
        _write_json(temp / "registry_records.json", [item.model_dump(mode="json") for item in records])
        files = _file_descriptors(temp)
        manifest = {
            "schema_version": ACTION_BUNDLE_SCHEMA,
            "bundle_id": bundle_id,
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
            "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY.value,
            "study_type": ResearchStudyType.ORACLE_DIAGNOSTIC.value,
            "decision_use": DecisionUse.NAVIGATION_ONLY.value,
            "planned_trial_count": 0,
            "generated_trial_count": 0,
            "evaluated_trial_count": 0,
            "selected_trial_count": 0,
            "sealed_holdout_accessed": False,
            "deployable": False,
            "files": files,
        }
        _write_json(temp / "manifest.json", manifest)
        if final.exists():
            existing = _read_bundle(final)
            if existing["request"].request_sha256 != request.request_sha256:
                _raise("N2 action bundle id collision", "ADVISORY_N2_ACTION_BUNDLE_INVALID")
            shutil.rmtree(temp)
            return final
        temp.replace(final)
        _read_bundle(final)
        return final
    except Exception:
        if temp.exists():
            shutil.rmtree(temp, ignore_errors=True)
        raise


def _registry_records(
    *, request: FrozenAdvisoryN2ActionAuditRequestV1, receipt_ref: EvidenceReferenceV1
) -> tuple[Any, Any]:
    consumed = (
        ConsumedWindowV1(
            window_id="P0C_DEVELOPMENT_V1",
            dataset_identity=request.dataset_identity,
            start_date=request.exit_decision_start,
            end_date=request.outcome_cutoff,
        ),
    )
    common = {
        "attempt_id": request.request_id,
        "study_type": ResearchStudyType.ORACLE_DIAGNOSTIC,
        "hypothesis_family_id": ACTION_FAMILY_ID,
        "parent_lineage": ("N1_TIER1", "M4_PRICE_RANGE", "P0C_SHADOW_POLICY"),
        "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY,
        "dataset_identity": request.dataset_identity,
        "schema_identity": canonical_json_sha256(
            {"request_schema": request.schema_version, "feature_schema": request.feature_schema_hash}
        ),
        "planned_trial_count": 0,
        "generated_trial_count": 0,
        "evaluated_trial_count": 0,
        "selected_trial_count": 0,
        "consumed_windows": consumed,
        "result_class": ResearchResultClass.EXPLORATORY,
        "decision_use": DecisionUse.NAVIGATION_ONLY,
        "evidence_refs": (receipt_ref,),
        "recorded_at": request.created_at,
    }
    entry_policy = canonical_json_sha256({"arms": [item.model_dump(mode="json") for item in request.entry_arms]})
    return (
        build_trial_record(
            experiment_id=ENTRY_EXPERIMENT_ID,
            research_stage="N2_ENTRY_GUARD_ORACLE",
            unique_variable="FIXED_3_FIXED_5_M4_Q90_CASH_AND_RANK_REPLACEMENT_V1",
            policy_identity=entry_policy,
            **common,
        ),
        build_trial_record(
            experiment_id=EXIT_EXPERIMENT_ID,
            research_stage="N2_EXIT_LABEL_ORACLE",
            unique_variable="NEXT_EXECUTABLE_OPEN_VS_FROZEN_BASELINE_POLICY_V1",
            policy_identity=research_policy_identity(
                baseline_policy_sha256=request.baseline_policy_sha256,
                shadow_policy_sha256=request.shadow_policy_sha256,
                cost_policy_sha256=request.cost_policy_sha256,
            ),
            **common,
        ),
    )


def _deliver_bundle(*, request: FrozenAdvisoryN2ActionAuditRequestV1, bundle_path: Path) -> dict[str, Any]:
    loaded = _read_bundle(bundle_path)
    registry = AdvisoryResearchTrialRegistryV1(request.registry_path).append_batch(loaded["records"])
    route = generate_current_route(
        registry_path=request.registry_path,
        parent_spike_path=request.parent_spike_path,
        window_contract_path=request.research_window_contract_ref.artifact_uri,
        output_path=request.route_path,
    )
    if route.get("next_task") != "N2_ENTRY_EXIT_QE_PREPARATION":
        _raise(
            "N2 action delivery changed the frozen N2 route",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
            next_task=route.get("next_task"),
        )
    return {"registry": registry, "route": route}


def _load_verified_sources(request: FrozenAdvisoryN2ActionAuditRequestV1) -> dict[str, Any]:
    n1 = AdvisoryN1Tier1RequestV1.model_validate_json(Path(request.n1_request_path).read_text(encoding="utf-8"))
    refs = {
        "n1_request_ref": (request.n1_request_ref, Path(request.n1_request_path)),
        "n1_bundle_manifest_ref": (
            request.n1_bundle_manifest_ref,
            Path(request.n1_bundle_path) / "manifest.json",
        ),
        "policy_dataset_manifest_ref": (
            request.policy_dataset_manifest_ref,
            Path(n1.policy_dataset_bundle_root) / "manifest.json",
        ),
        "m4_request_ref": (request.m4_request_ref, Path(request.m4_request_path)),
        "m4_bundle_manifest_ref": (
            request.m4_bundle_manifest_ref,
            Path(request.m4_bundle_path) / "manifest.json",
        ),
        "m4_predictions_ref": (
            request.m4_predictions_ref,
            Path(request.m4_bundle_path) / "test_predictions.parquet",
        ),
        "parent_spike_ref": (request.parent_spike_ref, Path(request.parent_spike_path)),
        "n0_completion_ref": (
            request.n0_completion_ref,
            Path(request.n0_completion_ref.artifact_uri),
        ),
        "research_window_contract_ref": (
            request.research_window_contract_ref,
            Path(request.research_window_contract_ref.artifact_uri),
        ),
    }
    for name, (expected, path) in refs.items():
        actual = evidence_reference_for_file(path, role=expected.role)
        if actual.model_dump(mode="json") != expected.model_dump(mode="json"):
            _raise(
                "N2 action source file identity drift",
                "ADVISORY_N2_ACTION_SOURCE_IDENTITY_MISMATCH",
                source=name,
            )
    if (
        n1.dataset_identity != request.dataset_identity
        or n1.baseline_policy_sha256 != request.baseline_policy_sha256
        or n1.shadow_policy_sha256 != request.shadow_policy_sha256
        or n1.cost_policy_sha256 != request.cost_policy_sha256
    ):
        _raise("N1 semantic identity drift", "ADVISORY_N2_ACTION_SOURCE_IDENTITY_MISMATCH")
    authorize_n1_development_access(n1)
    m4 = FrozenAdvisoryPriceRangeTrainingRequestV1.model_validate_json(
        Path(request.m4_request_path).read_text(encoding="utf-8")
    )
    m4_manifest = read_price_range_bundle_manifest(
        request.m4_bundle_path,
        expected_bundle_id=Path(request.m4_bundle_path).name,
    )
    policy_manifest = json.loads((Path(n1.policy_dataset_bundle_root) / "manifest.json").read_text(encoding="utf-8"))
    if (
        policy_manifest.get("policy_dataset_bundle_id") != request.dataset_identity
        or policy_manifest.get("request_sha256") != n1.policy_dataset_request_sha256
        or policy_manifest.get("shadow_policy_sha256") != request.shadow_policy_sha256
        or policy_manifest.get("cost_policy_sha256") != request.cost_policy_sha256
    ):
        _raise(
            "N1 policy dataset semantic identity drift",
            "ADVISORY_N2_ACTION_SOURCE_IDENTITY_MISMATCH",
        )
    _verify_m4_n1_candidate_identity(
        m4_manifest=m4_manifest,
        policy_manifest=policy_manifest,
    )
    if (
        m4.feature_schema_hash != request.feature_schema_hash
        or m4_manifest.get("feature_schema_hash") != request.feature_schema_hash
        or m4_manifest.get("request_sha256") != m4.request_sha256
    ):
        _raise(
            "M4 semantic identity drift",
            "ADVISORY_N2_ACTION_SOURCE_IDENTITY_MISMATCH",
        )
    n1_inspection = inspect_n1_bundle(request.n1_bundle_path)
    if n1_inspection.get("request_sha256") != n1.request_sha256:
        _raise(
            "N1 bundle/request semantic identity drift",
            "ADVISORY_N2_ACTION_SOURCE_IDENTITY_MISMATCH",
        )
    overlap = _entry_overlap(
        Path(request.m4_bundle_path) / "test_predictions.parquet",
        Path(n1.policy_dataset_bundle_root) / "candidate_episode_labels.parquet",
    )
    _validate_overlap_shape(overlap)
    n1_sources = load_verified_n1_sources(n1)
    baseline = pd.read_parquet(Path(n1.policy_dataset_bundle_root) / "candidate_episode_labels.parquet")
    return {
        "n1_request": n1,
        "m4_request": m4,
        "entry_overlap": overlap,
        "entry_baseline": baseline,
        "exit_baseline": baseline,
        "n1_sources": n1_sources,
        "ref_payload": {name: expected.model_dump(mode="json") for name, (expected, _) in refs.items()},
    }


def _verify_m4_n1_candidate_identity(
    *,
    m4_manifest: Mapping[str, Any],
    policy_manifest: Mapping[str, Any],
) -> None:
    shared_fields = ("package_id", "manifest_sha256")
    mismatches = {
        field: {
            "m4": m4_manifest.get(field),
            "policy_dataset": policy_manifest.get(field),
        }
        for field in shared_fields
        if not m4_manifest.get(field) or m4_manifest.get(field) != policy_manifest.get(field)
    }
    if mismatches:
        _raise(
            "M4 and N1 do not share the frozen candidate package identity",
            "ADVISORY_N2_ACTION_SOURCE_IDENTITY_MISMATCH",
            mismatches=mismatches,
        )


def _entry_overlap(m4_path: Path, baseline_path: Path) -> pd.DataFrame:
    m4 = _normalize_keys(pd.read_parquet(m4_path))
    baseline = _normalize_keys(pd.read_parquet(baseline_path))
    baseline_columns = [
        *KEYS,
        "episode_label_id",
        "label_status",
        "net_return_bps",
        "shadow_policy_sha256",
        "cost_policy_sha256",
    ]
    merged = m4.merge(
        baseline[baseline_columns],
        on=KEYS,
        how="inner",
        validate="one_to_one",
    )
    return merged.rename(columns={"selection_effective_rank": "selection_rank"})


def _validate_overlap_shape(overlap: pd.DataFrame) -> None:
    dates = pd.DatetimeIndex(overlap["decision_as_of_trade_date"].unique()).sort_values()
    if (
        len(overlap) != ENTRY_OVERLAP_ROW_COUNT
        or len(dates) != ENTRY_OVERLAP_DAY_COUNT
        or dates[0].date() != ENTRY_DECISION_START
        or dates[-1].date() != ENTRY_DECISION_END
        or int(overlap["label_status"].eq("MATURED").sum()) != ENTRY_MATURED_ROW_COUNT
        or not overlap.groupby("decision_as_of_trade_date").size().eq(20).all()
    ):
        _raise(
            "M4/N1 Entry overlap differs from the frozen 60-day identity",
            "ADVISORY_N2_ENTRY_KEY_OVERLAP_INVALID",
            row_count=len(overlap),
            day_count=len(dates),
        )


def _read_bundle(path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        request = FrozenAdvisoryN2ActionAuditRequestV1.model_validate_json(
            (path / "request.json").read_text(encoding="utf-8")
        )
        receipt = AdvisoryN2ActionAuditReceiptV1.model_validate_json(
            (path / "audit_receipt.json").read_text(encoding="utf-8")
        )
        source_receipt = json.loads((path / "source_identity_receipt.json").read_text(encoding="utf-8"))
        resource_report = json.loads((path / "resource_report.json").read_text(encoding="utf-8"))
        entry_summary = json.loads((path / "entry_summary.json").read_text(encoding="utf-8"))
        exit_summary = json.loads((path / "exit_summary.json").read_text(encoding="utf-8"))
        raw_records = json.loads((path / "registry_records.json").read_text(encoding="utf-8"))
        records = tuple(
            build_trial_record(
                **{key: value for key, value in item.items() if key not in {"registry_entry_id", "record_sha256"}}
            )
            for item in raw_records
        )
    except AdvisoryModelFirstError:
        raise
    except Exception as exc:
        _raise(
            "N2 action bundle cannot be read",
            "ADVISORY_N2_ACTION_BUNDLE_INVALID",
            path=str(path),
            error_type=type(exc).__name__,
        )
    descriptors = manifest.get("files")
    if not isinstance(descriptors, dict) or set(descriptors) != BUNDLE_MEMBERS:
        _raise(
            "N2 action bundle member set is invalid",
            "ADVISORY_N2_ACTION_BUNDLE_INVALID",
        )
    actual_members = {
        item.relative_to(path).as_posix() for item in path.rglob("*") if item.is_file() and item.name != "manifest.json"
    }
    if actual_members != BUNDLE_MEMBERS:
        _raise(
            "N2 action bundle contains missing or unexpected members",
            "ADVISORY_N2_ACTION_BUNDLE_INVALID",
        )
    for name, descriptor in descriptors.items():
        member = path / name
        expected_row_count = len(pd.read_parquet(member)) if member.suffix == ".parquet" else None
        if (
            sha256_file(member) != descriptor.get("sha256")
            or member.stat().st_size != descriptor.get("size_bytes")
            or (expected_row_count is not None and descriptor.get("row_count") != expected_row_count)
        ):
            _raise(
                "N2 action bundle member drift",
                "ADVISORY_N2_ACTION_BUNDLE_INVALID",
                member=name,
            )
    expected_bundle_id = canonical_json_sha256(
        {
            "schema_version": ACTION_BUNDLE_SCHEMA,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
        }
    )
    receipt_descriptor = descriptors["audit_receipt.json"]
    records_valid = len(records) == 2 and {item.experiment_id for item in records} == {
        ENTRY_EXPERIMENT_ID,
        EXIT_EXPERIMENT_ID,
    }
    if records_valid:
        for raw, record in zip(raw_records, records, strict=True):
            records_valid = records_valid and (
                raw.get("registry_entry_id") == record.registry_entry_id
                and raw.get("record_sha256") == record.record_sha256
                and len(record.evidence_refs) == 1
                and record.evidence_refs[0].artifact_uri == (path / "audit_receipt.json").as_posix()
                and record.evidence_refs[0].sha256 == receipt_descriptor.get("sha256")
                and record.evidence_refs[0].size_bytes == receipt_descriptor.get("size_bytes")
            )
    invalid = (
        manifest.get("schema_version") != ACTION_BUNDLE_SCHEMA
        or manifest.get("bundle_id") != path.name
        or path.name != expected_bundle_id
        or manifest.get("request_sha256") != request.request_sha256
        or manifest.get("receipt_sha256") != receipt.receipt_sha256
        or receipt.request_sha256 != request.request_sha256
        or receipt.source_identity_sha256 != descriptors["source_identity_receipt.json"].get("sha256")
        or receipt.resource_report_sha256 != descriptors["resource_report.json"].get("sha256")
        or receipt.entry_summary != entry_summary
        or receipt.exit_summary != exit_summary
        or source_receipt.get("request_sha256") != request.request_sha256
        or source_receipt.get("sealed_holdout_accessed") is not False
        or int(resource_report.get("peak_rss_bytes") or 0) > request.resource_max_rss_bytes
        or manifest.get("objective_contract") != ObjectiveContract.RISK_MANAGED_ADVISORY.value
        or manifest.get("study_type") != ResearchStudyType.ORACLE_DIAGNOSTIC.value
        or manifest.get("decision_use") != DecisionUse.NAVIGATION_ONLY.value
        or any(
            int(manifest.get(name, -1)) != 0
            for name in (
                "planned_trial_count",
                "generated_trial_count",
                "evaluated_trial_count",
                "selected_trial_count",
            )
        )
        or manifest.get("sealed_holdout_accessed") is not False
        or manifest.get("deployable") is not False
        or not records_valid
    )
    if invalid:
        _raise(
            "N2 action bundle relational identity is invalid",
            "ADVISORY_N2_ACTION_BUNDLE_INVALID",
        )
    return {
        "manifest": manifest,
        "request": request,
        "receipt": receipt,
        "records": records,
        "source_receipt": source_receipt,
        "resource_report": resource_report,
    }


def _find_existing_bundle(request: FrozenAdvisoryN2ActionAuditRequestV1) -> Path | None:
    root = Path(request.output_root) / "action_audit_bundles"
    if not root.exists():
        return None
    matches = []
    for path in root.iterdir():
        if not path.is_dir() or path.name.startswith("."):
            continue
        manifest_path = path / "manifest.json"
        if not manifest_path.is_file():
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if manifest.get("request_sha256") == request.request_sha256:
            matches.append(path)
    if len(matches) > 1:
        _raise("one request maps to multiple action bundles", "ADVISORY_N2_ACTION_BUNDLE_INVALID")
    if matches:
        _read_bundle(matches[0])
        return matches[0]
    return None


def _entry_rank(overlap: pd.DataFrame, label: AdvisoryIncrementalValueLabelV1) -> int:
    row = overlap[
        overlap["decision_as_of_trade_date"].eq(pd.Timestamp(label.decision_date))
        & overlap["target_trade_date"].eq(pd.Timestamp(label.target_action_date))
        & overlap["instrument"].eq(label.instrument)
    ]
    if len(row) != 1:
        _raise("Entry label rank lookup is not unique", "ADVISORY_N2_ENTRY_KEY_OVERLAP_INVALID")
    return int(row.iloc[0]["selection_rank"])


def _normalize_keys(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for column in ("decision_as_of_trade_date", "target_trade_date"):
        output[column] = pd.to_datetime(output[column]).dt.normalize()
    output["instrument"] = output["instrument"].astype(str).str.strip().str.upper()
    return output


def _market_row(frame: pd.DataFrame, value: pd.Timestamp, instrument: str) -> pd.Series:
    try:
        row = frame.loc[(pd.Timestamp(value).normalize(), instrument)]
    except KeyError:
        _raise(
            "N2 action market row is missing",
            "ADVISORY_N2_ENTRY_GAP_PARITY_FAILED",
            date=pd.Timestamp(value).date().isoformat(),
            instrument=instrument,
        )
    if isinstance(row, pd.DataFrame):
        _raise("N2 action market row is duplicated", "ADVISORY_N2_ENTRY_GAP_PARITY_FAILED")
    return row


def _moving_block_interval(
    values: Sequence[float] | pd.Series,
    *,
    block_length: int,
    repetitions: int,
    seed: int,
) -> tuple[float | None, float | None]:
    array = pd.Series(values, dtype=float).dropna().to_numpy()
    if len(array) < block_length:
        return None, None
    rng = np.random.default_rng(seed)
    starts = np.arange(0, len(array) - block_length + 1)
    block_count = math.ceil(len(array) / block_length)
    estimates = []
    for _ in range(repetitions):
        chosen = rng.choice(starts, size=block_count, replace=True)
        sampled = np.concatenate([array[start : start + block_length] for start in chosen])[: len(array)]
        estimates.append(float(sampled.mean()))
    return float(np.quantile(estimates, 0.025)), float(np.quantile(estimates, 0.975))


def _cumulative_return(values: pd.Series) -> float | None:
    if values.empty:
        return None
    return float((1.0 + values.astype(float) / 10000.0).prod() - 1.0)


def _max_drawdown(values: pd.Series) -> float | None:
    if values.empty:
        return None
    curve = (1.0 + values.astype(float) / 10000.0).cumprod()
    return float((curve / curve.cummax() - 1.0).min())


def _mean(values: Sequence[float] | pd.Series) -> float | None:
    series = pd.Series(values, dtype=float).dropna()
    return float(series.mean()) if len(series) else None


def _enum_value(value: object) -> str:
    return str(getattr(value, "value", value))


def _grouped_value_counts(
    frame: pd.DataFrame,
    *,
    group_column: str,
    value_column: str,
) -> dict[str, dict[str, int]]:
    if frame.empty:
        return {}
    normalized = frame[[group_column, value_column]].copy()
    normalized[group_column] = normalized[group_column].map(_enum_value)
    normalized[value_column] = normalized[value_column].map(_enum_value)
    output: dict[str, dict[str, int]] = {}
    for group, values in normalized.groupby(group_column, sort=True):
        counts = values[value_column].value_counts().sort_index()
        output[str(group)] = {str(key): int(value) for key, value in counts.items()}
    return output


def _positive(value: object) -> float | None:
    result = _finite(value)
    return result if result is not None and result > 0 else None


def _finite(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _file_descriptors(root: Path) -> dict[str, dict[str, Any]]:
    return {
        path.relative_to(root).as_posix(): {
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
            **({"row_count": len(pd.read_parquet(path))} if path.suffix == ".parquet" else {}),
        }
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    }


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False),
        encoding="utf-8",
    )


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    frame.to_parquet(path, index=False)


def _write_immutable_request(path: Path, request: FrozenAdvisoryN2ActionAuditRequestV1) -> None:
    payload = json.dumps(
        request.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = FrozenAdvisoryN2ActionAuditRequestV1.model_validate_json(path.read_text(encoding="utf-8"))
        if existing.request_sha256 != request.request_sha256:
            _raise("N2 action request path already contains different content", "ADVISORY_N2_ACTION_REQUEST_INVALID")
        return
    path.write_text(payload, encoding="utf-8")


def _repository_commit(root: Path) -> str:
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=False)
    value = result.stdout.strip()
    if result.returncode or len(value) != 40:
        _raise("cannot resolve N2 action repository commit", "ADVISORY_N2_ACTION_REQUEST_INVALID")
    return value


def _repository_dirty(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=normal"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        _raise("cannot inspect N2 action repository state", "ADVISORY_N2_ACTION_REQUEST_INVALID")
    return [line for line in result.stdout.splitlines() if line.strip()]


def _verify_environment(request: FrozenAdvisoryN2ActionAuditRequestV1) -> None:
    if os.name == "nt" or "microsoft" not in platform.release().lower():
        _raise("N2 action formal run requires WSL", "ADVISORY_MODEL_TRAINING_REQUIRES_WSL")
    root = Path(request.repository_root)
    if _repository_commit(root) != request.repository_commit or _repository_dirty(root):
        _raise("N2 action repository identity drift", "ADVISORY_N2_ACTION_SOURCE_IDENTITY_MISMATCH")


def _check_rss(request: FrozenAdvisoryN2ActionAuditRequestV1, stage: str) -> None:
    peak = _peak_rss_bytes()
    if peak > request.resource_max_rss_bytes:
        _raise(
            "N2 action audit exceeded frozen RSS limit",
            "ADVISORY_N2_ACTION_RESOURCE_LIMIT_EXCEEDED",
            stage=stage,
            peak_rss_bytes=peak,
            limit_bytes=request.resource_max_rss_bytes,
        )


def _peak_rss_bytes() -> int:
    if _resource is None:
        return 0
    value = _resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss
    return int(value * 1024) if platform.system() != "Darwin" else int(value)


def _run_response(
    *,
    request: FrozenAdvisoryN2ActionAuditRequestV1,
    bundle_path: Path,
    status: str,
    delivery: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": status,
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "bundle_id": bundle_path.name,
        "bundle_path": bundle_path.as_posix(),
        "sealed_holdout_accessed": False,
        "deployable": False,
        "planned_trial_count": 0,
        "delivery": delivery,
        "backend_restart": "noop",
        "production_ddl_gate": "noop",
        "runtime_activation": "noop",
    }


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)
