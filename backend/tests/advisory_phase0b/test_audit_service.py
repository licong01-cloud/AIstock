from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_phase1.label_policy import Projection
from backend.services.advisory_phase0b.audit_service import (
    SUPPORTED_METRIC_IDS,
    Phase0BMetricEngine,
    SignalContext,
    _LazyTargetContextMapping,
    _SliceEvidence,
)
from backend.services.advisory_phase0b.contracts import (
    MetricStatus,
    Phase0BCandidateQualityAuditRequestV1,
    Phase0BEconomicSignificancePolicyV1,
    Phase0BEconomicThresholdV1,
    Phase0BMetricDefinitionV1,
    Phase0BMetricRegistryV1,
    Phase0BMarketRegimeDefinitionV1,
    Phase0BMultipleTestingRegistryV1,
)
from backend.services.advisory_phase0b.errors import Phase0BAuditError
from backend.services.advisory_phase0b.snapshot_reader import Phase0BTargetProgramBindingV1
from backend.services.advisory_phase0b.report_store import Phase0BMetricResultV1
from backend.services.advisory_phase0b.spool import Phase0BBoundedSpool
from backend.tests.advisory_phase0b.test_contracts import _request
from backend.tests.advisory_phase0b.test_spool import _roots


def _full_request(
    *,
    market_regime_definitions: tuple[Phase0BMarketRegimeDefinitionV1, ...] = (),
) -> Phase0BCandidateQualityAuditRequestV1:
    base = _request()
    numeric_hash = str(base.numeric_kernel.kernel_hash)
    winner_id = base.winner_definitions[0].winner_definition_id

    def metric(
        metric_id: str,
        *,
        family: str = "DIAGNOSTIC",
        stages: tuple[str, ...] = ("selection_effective",),
        depths: tuple[int, ...] = (5,),
        winner: bool = False,
        output_unit: str = "DECIMAL_RETURN",
    ) -> Phase0BMetricDefinitionV1:
        return Phase0BMetricDefinitionV1(
            metric_id=metric_id,
            family=family,
            projection="RETURN_NET_EXCESS",
            horizon_source="WINNER_DEFINITION" if winner else "LABEL_POLICY",
            horizons=(5,),
            stages=stages,
            depths=depths,
            cash_policy=(
                "NOT_APPLICABLE"
                if family == "COVERAGE"
                else "PRECISION_EMPTY_FAILURE"
                if metric_id == "precision-at5-v1"
                else "NDCG_EMPTY_ZERO_GAIN"
                if metric_id == "ndcg-at5-v1"
                else "NET_EXCESS_NEGATIVE_BENCHMARK"
            ),
            maturity_eligibility=(
                ("MATURED", "PENDING", "RIGHT_CENSORED", "UNAVAILABLE")
                if family == "COVERAGE"
                else ("MATURED",)
            ),
            event_eligibility=(
                ("TERMINAL", "NON_TERMINAL")
                if family == "COVERAGE"
                else ("TERMINAL",)
            ),
            winner_definition_ids=(winner_id,) if winner else (),
            benchmark_policy_ref="snapshot-benchmark-policy",
            cost_policy_ref="snapshot-cost-policy",
            numeric_kernel_ref=numeric_hash,
            output_unit=output_unit,
        )

    definitions = (
        metric(
            "stage-topk-point-estimate-v1",
            family="PRIMARY",
            stages=("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective"),
            depths=(5, 10, 20),
        ),
        metric(
            "candidate-pool-point-estimate-v1",
            stages=("alpha_raw", "selection_effective"),
            depths=(5, 10, 20),
        ),
        metric(
            "rank-monotonicity-v1",
            stages=("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective"),
            depths=(20,),
            output_unit="CORRELATION",
        ),
        metric(
            "stage-incremental-lift-v1",
            stages=("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective"),
        ),
        metric("random5-v1", stages=("alpha_raw",), depths=(5, 10, 20)),
        metric("precision-at5-v1", winner=True, output_unit="RATIO"),
        metric(
            "ndcg-at5-v1",
            stages=("alpha_raw", "selection_effective"),
            output_unit="RATIO",
        ),
        metric(
            "strategy-recall-v1",
            depths=(5, 10, 20),
            winner=True,
            output_unit="RATIO",
        ),
        metric(
            "conditional-recall-v1",
            stages=("alpha_raw", "selection_effective"),
            depths=(5, 10, 20),
            winner=True,
            output_unit="RATIO",
        ),
        metric("blacklist-diagnostic-v1", stages=("risk_policy_adjusted",)),
        metric("coverage-v1", family="COVERAGE", output_unit="RATIO"),
    )
    registry = Phase0BMetricRegistryV1(metrics=definitions)
    primary = tuple(item.metric_id for item in definitions if item.family == "PRIMARY")
    diagnostics = tuple(item.metric_id for item in definitions if item.family != "PRIMARY")
    testing_payload = base.multiple_testing_registry.model_dump(mode="python")
    testing_payload.update(
        {
            "metric_registry_hash": registry.registry_hash,
            "primary_metric_family": primary,
            "diagnostic_metric_families": diagnostics,
            "market_regime_definitions": market_regime_definitions,
            "economic_significance_policy": Phase0BEconomicSignificancePolicyV1(
                policy_id="phase0b-economic-significance-v1",
                thresholds=tuple(
                    Phase0BEconomicThresholdV1(
                        metric_family=metric_id,
                        minimum_absolute_effect=Decimal("0.01"),
                        output_unit="DECIMAL_RETURN",
                    )
                    for metric_id in primary
                ),
            ),
            "registry_hash": None,
        }
    )
    testing = Phase0BMultipleTestingRegistryV1.model_validate(testing_payload)
    payload = base.model_dump(mode="python")
    payload.update(
        {
            "metric_registry": registry,
            "metric_registry_hash": registry.registry_hash,
            "multiple_testing_registry": testing,
            "multiple_testing_registry_hash": testing.registry_hash,
            "request_hash": None,
        }
    )
    return Phase0BCandidateQualityAuditRequestV1.model_validate(payload)


def _append(
    spool: Phase0BBoundedSpool,
    *,
    snapshot_id: str = "snapshot-1",
    role: str,
    rows: tuple[dict[str, object], ...],
    identity_fields: tuple[str, ...],
    source_char: str,
) -> None:
    spool.append_rows(
        snapshot_id=snapshot_id,
        logical_role=role,
        source_file_sha256=source_char * 64,
        rows=rows,
        identity_fields=identity_fields,
        decision_date_field="decision_as_of_trade_date",
    )


def _single_candidate_context(
    *,
    signal_id: str,
    symbol: str,
    rank: int,
    stage_evidence_id: str,
) -> SignalContext:
    outcome = {
        "label_version_id": f"label-{signal_id}",
        "projection_value_decimal": "0.01",
        "maturity_status": "MATURED",
        "outcome_event_status": "TERMINAL",
    }
    return SignalContext(
        snapshot_id="snapshot-1",
        signal_id=signal_id,
        canonical_signal_scope_hash=canonical_json_sha256(signal_id),
        universe_policy_hash="c" * 64,
        market_regime_at_t=None,
        market_regime_evidence_hash=None,
        candidates_by_stage={
            "alpha_raw": (
                {
                    "stage_evidence_id": stage_evidence_id,
                    "membership_status": "INCLUDED",
                    "symbol": symbol,
                    "rank": rank,
                },
            )
        },
        stage_capability_by_stage={"alpha_raw": "FULL"},
        outcomes_by_stage_symbol={
            (stage_evidence_id, symbol, "RETURN_NET_ABSOLUTE", 1): outcome
        },
        universe_outcomes=(),
    )


def test_date_context_merges_one_candidate_signals_before_topk_validation() -> None:
    contexts = (
        _single_candidate_context(
            signal_id="signal-2",
            symbol="000002.SZ",
            rank=2,
            stage_evidence_id="stage-2",
        ),
        _single_candidate_context(
            signal_id="signal-1",
            symbol="000001.SZ",
            rank=1,
            stage_evidence_id="stage-1",
        ),
    )

    merged = Phase0BMetricEngine._merge_date_contexts(
        decision_date="2026-07-01",
        contexts=contexts,
    )
    outcomes = Phase0BMetricEngine._candidate_outcomes(
        context=merged[0],
        stage="alpha_raw",
        projection=Projection.RETURN_NET_ABSOLUTE,
        horizon=1,
    )

    assert len(merged) == 1
    assert [(item.symbol, item.rank) for item in outcomes] == [
        ("000001.SZ", 1),
        ("000002.SZ", 2),
    ]


def test_date_context_rejects_conflicting_universe_identity() -> None:
    first = _single_candidate_context(
        signal_id="signal-1",
        symbol="000001.SZ",
        rank=1,
        stage_evidence_id="stage-1",
    )
    second = _single_candidate_context(
        signal_id="signal-2",
        symbol="000002.SZ",
        rank=2,
        stage_evidence_id="stage-2",
    )
    second = SignalContext(
        **{
            **second.__dict__,
            "universe_policy_hash": "d" * 64,
        }
    )

    with pytest.raises(Phase0BAuditError, match="conflicting snapshot or universe"):
        Phase0BMetricEngine._merge_date_contexts(
            decision_date="2026-07-01",
            contexts=(first, second),
        )


def test_date_context_preserves_rank_gap_for_fixed_k_cash_slot() -> None:
    merged = Phase0BMetricEngine._merge_date_contexts(
        decision_date="2026-07-01",
        contexts=(
            _single_candidate_context(
                signal_id="signal-1",
                symbol="000001.SZ",
                rank=1,
                stage_evidence_id="stage-1",
            ),
            _single_candidate_context(
                signal_id="signal-3",
                symbol="000003.SZ",
                rank=3,
                stage_evidence_id="stage-3",
            ),
        ),
    )

    outcomes = Phase0BMetricEngine._candidate_outcomes(
        context=merged[0],
        stage="alpha_raw",
        projection=Projection.RETURN_NET_ABSOLUTE,
        horizon=1,
    )

    assert [item.rank for item in outcomes] == [1, 3]


def _populate_metric_spool(
    spool: Phase0BBoundedSpool,
    *,
    snapshot_id: str = "snapshot-1",
    package_id: str = "package-1",
    manifest_sha256: str = "a" * 64,
    range_program_hash: str = "9" * 64,
    market_regime_at_t: str | None = None,
    market_regime_evidence_hash: str | None = None,
) -> None:
    date = "2026-07-01"
    _append(
        spool,
        snapshot_id=snapshot_id,
        role="canonical_signals",
        rows=(
            {
                "canonical_signal_id": "signal-1",
                "decision_as_of_trade_date": date,
                "package_id": package_id,
                "manifest_sha256": manifest_sha256,
                "alpha_mode": "multi_alpha",
                "canonical_signal_scope_hash": "b" * 64,
            },
        ),
        identity_fields=("canonical_signal_id",),
        source_char="1",
    )
    _append(
        spool,
        snapshot_id=snapshot_id,
        role="observation_versions",
        rows=(
            {
                "observation_version_id": "obs-1",
                "canonical_signal_id": "signal-1",
                "universe_policy_hash": "c" * 64,
                "decision_as_of_trade_date": date,
            },
        ),
        identity_fields=("observation_version_id",),
        source_char="9",
    )
    _append(
        spool,
        snapshot_id=snapshot_id,
        role="lineage",
        rows=(
            {
                "lineage_id": "lineage-1",
                "canonical_signal_id": "signal-1",
                "observation_version_id": "obs-1",
                "historical_range_frozen_program_hash": range_program_hash,
                "decision_as_of_trade_date": date,
            },
        ),
        identity_fields=("lineage_id",),
        source_char="2",
    )
    _append(
        spool,
        snapshot_id=snapshot_id,
        role="selected_observations",
        rows=(
            {
                "selected_mapping_id": "selected-obs-1",
                "canonical_signal_id": "signal-1",
                "terminal_observation_version_id": "obs-1",
                "decision_as_of_trade_date": date,
            },
        ),
        identity_fields=("selected_mapping_id",),
        source_char="3",
    )
    stage_rows: list[dict[str, object]] = []
    candidate_rows: list[dict[str, object]] = []
    outcome_rows: list[dict[str, object]] = []
    selected_label_rows: list[dict[str, object]] = []
    for stage_index, stage in enumerate(
        ("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective"),
        start=1,
    ):
        stage_id = f"stage-{stage_index}"
        stage_rows.append(
            {
                "stage_evidence_id": stage_id,
                "observation_version_id": "obs-1",
                "stage": stage,
                "capability_status": "FULL",
                "decision_as_of_trade_date": date,
            }
        )
        for rank in range(1, 21):
            symbol = f"{rank:06d}.SZ"
            candidate_rows.append(
                {
                    "stage_evidence_id": stage_id,
                    "symbol": symbol,
                    "membership_status": "INCLUDED",
                    "rank": rank,
                    "decision_as_of_trade_date": date,
                }
            )
            label_id = f"label-{stage_index}-{rank}"
            label_key = canonical_json_sha256({"label": label_id})
            outcome_rows.append(
                {
                    "label_version_id": label_id,
                    "label_key_hash": label_key,
                    "canonical_signal_id": "signal-1",
                    "candidate_stage_evidence_id": stage_id,
                    "symbol": symbol,
                    "owner_type": "CANDIDATE",
                    "projection": "RETURN_NET_EXCESS",
                    "horizon_trading_days": 5,
                    "label_policy_hash": "a" * 64,
                    "label_source_revision_set_hash": "d" * 64,
                    "maturity_status": "MATURED",
                    "outcome_event_status": "TERMINAL",
                    "projection_value_decimal": str(Decimal(21 - rank) / Decimal("100")),
                    "benchmark_net_total_return": "0.02",
                    "decision_as_of_trade_date": date,
                }
            )
            selected_label_rows.append(
                {
                    "selected_label_mapping_id": f"selected-{label_id}",
                    "terminal_label_version_id": label_id,
                    "selection_status": "SELECTED",
                    "decision_as_of_trade_date": date,
                }
            )
    blacklist_label_id = "label-blacklist-1"
    blacklist_label_key = canonical_json_sha256({"label": blacklist_label_id})
    candidate_rows.append(
        {
            "stage_evidence_id": "stage-3",
            "symbol": "999998.SZ",
            "membership_status": "EXCLUDED",
            "rank": None,
            "input_rank": 8,
            "exclusion_reason_code": "INDUSTRY_BLACKLIST",
            "component_evidence_json": {
                "industry_blacklist_excluded": True,
                "industry_at_t": "TEST_INDUSTRY",
            },
            "decision_as_of_trade_date": date,
        }
    )
    outcome_rows.append(
        {
            "label_version_id": blacklist_label_id,
            "label_key_hash": blacklist_label_key,
            "canonical_signal_id": "signal-1",
            "candidate_stage_evidence_id": "stage-3",
            "symbol": "999998.SZ",
            "owner_type": "CANDIDATE",
            "projection": "RETURN_NET_EXCESS",
            "horizon_trading_days": 5,
            "label_policy_hash": "a" * 64,
            "label_source_revision_set_hash": "d" * 64,
            "maturity_status": "MATURED",
            "outcome_event_status": "TERMINAL",
            "projection_value_decimal": "0.12",
            "benchmark_net_total_return": "0.02",
            "decision_as_of_trade_date": date,
        }
    )
    selected_label_rows.append(
        {
            "selected_label_mapping_id": "selected-label-blacklist-1",
            "terminal_label_version_id": blacklist_label_id,
            "selection_status": "SELECTED",
            "decision_as_of_trade_date": date,
        }
    )
    _append(
        spool,
        snapshot_id=snapshot_id,
        role="stage_summaries",
        rows=tuple(stage_rows),
        identity_fields=("stage_evidence_id",),
        source_char="4",
    )
    _append(
        spool,
        snapshot_id=snapshot_id,
        role="stage_candidates",
        rows=tuple(candidate_rows),
        identity_fields=("stage_evidence_id", "symbol"),
        source_char="5",
    )
    _append(
        spool,
        snapshot_id=snapshot_id,
        role="outcome_labels",
        rows=tuple(outcome_rows),
        identity_fields=("label_version_id",),
        source_char="6",
    )
    _append(
        spool,
        snapshot_id=snapshot_id,
        role="selected_labels",
        rows=tuple(selected_label_rows),
        identity_fields=("selected_label_mapping_id",),
        source_char="7",
    )
    universe_rows = tuple(
        {
            "label_version_id": f"universe-{rank}",
            "symbol": f"{rank:06d}.SZ",
            "owner_type": "UNIVERSE",
            "projection": "RETURN_NET_EXCESS",
            "horizon_trading_days": 5,
            "label_policy_hash": "a" * 64,
            "label_source_revision_set_hash": "d" * 64,
            "universe_layer": "PIT_ELIGIBLE",
            "canonical_signal_id": "signal-1",
            "maturity_status": "MATURED",
            "outcome_event_status": "TERMINAL",
            "projection_value_decimal": str(Decimal(21 - rank) / Decimal("100")),
            "market_regime_at_t": market_regime_at_t,
            "market_regime_evidence_hash": market_regime_evidence_hash,
            "decision_as_of_trade_date": date,
        }
        for rank in range(1, 21)
    )
    _append(
        spool,
        snapshot_id=snapshot_id,
        role="universe_outcomes",
        rows=universe_rows,
        identity_fields=("label_version_id",),
        source_char="8",
    )


def test_metric_engine_runs_every_frozen_family_without_15_day_overclaim(tmp_path: Path) -> None:
    repository_root, dataset_root, output_root = _roots(tmp_path)
    request = _full_request()
    target = request.audit_targets[0]
    with Phase0BBoundedSpool(
        output_root=output_root,
        repository_root=repository_root,
        dataset_root=dataset_root,
        operation_id="metric-engine",
    ) as spool:
        _populate_metric_spool(spool)
        _append(
            spool,
            role="universe_outcomes",
            rows=(
                {
                    "label_version_id": "foreign-universe",
                    "symbol": "999999.SZ",
                    "owner_type": "UNIVERSE",
                    "projection": "RETURN_NET_EXCESS",
                    "horizon_trading_days": 5,
                    "label_policy_hash": "a" * 64,
                    "label_source_revision_set_hash": "e" * 64,
                    "universe_layer": "PIT_ELIGIBLE",
                    "canonical_signal_id": "foreign-signal",
                    "maturity_status": "MATURED",
                    "outcome_event_status": "TERMINAL",
                    "projection_value_decimal": "9.99",
                    "decision_as_of_trade_date": "2026-07-01",
                },
                {
                    "label_version_id": "universe-pending",
                    "symbol": "999996.SZ",
                    "owner_type": "UNIVERSE",
                    "projection": "RETURN_NET_EXCESS",
                    "horizon_trading_days": 5,
                    "label_policy_hash": "a" * 64,
                    "label_source_revision_set_hash": "d" * 64,
                    "universe_layer": "PIT_ELIGIBLE",
                    "canonical_signal_id": "signal-1",
                    "maturity_status": "PENDING",
                    "outcome_event_status": "NON_TERMINAL",
                    "projection_value_decimal": None,
                    "decision_as_of_trade_date": "2026-07-01",
                },
            ),
            identity_fields=("label_version_id",),
            source_char="0",
        )
        report = Phase0BMetricEngine().evaluate_target(
            request=request,
            target=target,
            program_binding=Phase0BTargetProgramBindingV1(
                target_hash=str(target.target_hash),
                range_program_hash="9" * 64,
            ),
            spool=spool,
        )

    assert {item.metric_definition_id for item in report.metric_results} == SUPPORTED_METRIC_IDS
    assert report.decision_date_count == 1
    assert report.package_conclusion is None
    assert all(item.conclusion is None and item.p_value is None for item in report.metric_results)
    assert any(item.status is MetricStatus.INSUFFICIENT_SAMPLE for item in report.metric_results)
    candidate_lift = next(
        item
        for item in report.metric_results
        if item.metric_definition_id == "candidate-pool-point-estimate-v1" and item.depth == 20
    )
    assert candidate_lift.observed_value == Decimal("0.075000000000")
    assert json.loads(candidate_lift.detail_json)["comparison"] == (
        "SELECTION_EFFECTIVE_TOP5_MINUS_ALPHA_RAW_CANDIDATE_POOL_D"
    )
    topk_lift = next(
        item
        for item in report.metric_results
        if item.metric_definition_id == "stage-topk-point-estimate-v1"
        and item.stage == "selection_effective"
        and item.depth == 5
        and json.loads(item.detail_json).get("candidate_pool_depth") == 20
    )
    assert topk_lift.observed_value == Decimal("0.075000000000")
    strategy_recall = next(
        item
        for item in report.metric_results
        if item.metric_definition_id == "strategy-recall-v1" and item.depth == 5
    )
    assert strategy_recall.candidate_count == 21
    assert strategy_recall.matured_label_count == 20
    assert strategy_recall.unavailable_label_count == 1
    assert strategy_recall.status is MetricStatus.INSUFFICIENT_SAMPLE
    assert "ADVISORY_PHASE0B_INSUFFICIENT_WINNER_EVENTS" in strategy_recall.reason_codes
    strategy_recall_detail = json.loads(strategy_recall.detail_json)
    assert strategy_recall_detail["denominator_universe_layer"] == "PIT_ELIGIBLE"
    assert strategy_recall_detail["universe_policy_hashes"] == ["c" * 64]
    assert strategy_recall_detail["universe_policy_set_hash"] == canonical_json_sha256(
        ("c" * 64,)
    )
    assert strategy_recall_detail["source_revision_set_hash"] == "d" * 64
    assert strategy_recall_detail["source_revision_set_hashes"] == ["d" * 64]
    assert strategy_recall_detail["source_revision_set_hash_set_hash"] == (
        canonical_json_sha256(("d" * 64,))
    )
    coverage = next(
        item for item in report.metric_results if item.metric_definition_id == "coverage-v1"
    )
    assert coverage.status is MetricStatus.AVAILABLE
    assert coverage.decision_date_count == coverage.evaluable_date_count == 1
    assert coverage.conclusion is None
    assert json.loads(coverage.detail_json)["market_regime_evidence_status"] == (
        "INPUT_CAPABILITY_NOT_AVAILABLE"
    )
    incremental = next(
        item
        for item in report.metric_results
        if item.metric_definition_id == "stage-incremental-lift-v1"
        and item.stage == "alpha_raw->hmm_adjusted"
    )
    incremental_detail = json.loads(incremental.detail_json)
    assert incremental_detail["mean_jaccard"] == "1.000000000000"
    assert incremental_detail["entered_count"] == 0
    assert incremental_detail["exited_count"] == 0
    blacklist = next(
        item
        for item in report.metric_results
        if item.metric_definition_id == "blacklist-diagnostic-v1"
    )
    blacklist_detail = json.loads(blacklist.detail_json)
    assert blacklist.candidate_count == 1
    assert blacklist_detail["industry_counts"] == {"TEST_INDUSTRY": 1}
    assert blacklist_detail["counterfactual_status"] == "INPUT_CAPABILITY_NOT_AVAILABLE"


def _primary_slice(
    *,
    request: Phase0BCandidateQualityAuditRequestV1,
    stage: str,
    dates: tuple[str, ...],
    observed: Decimal,
) -> _SliceEvidence:
    definition = next(
        item
        for item in request.metric_registry.metrics
        if item.metric_id == "stage-topk-point-estimate-v1"
    )
    result = Phase0BMetricResultV1(
        metric_definition_id=definition.metric_id,
        metric_definition_hash=str(definition.metric_hash),
        slice_id=f"{definition.metric_id}:RETURN_NET_EXCESS:h5:{stage}:k5",
        projection="RETURN_NET_EXCESS",
        horizon_trading_days=5,
        stage=stage,
        depth=5,
        status=MetricStatus.AVAILABLE,
        reason_codes=(),
        decision_date_count=len(dates),
        evaluable_date_count=len(dates),
        effective_sample_count=len(dates),
        missing_decision_date_count=0,
        candidate_count=len(dates) * 5,
        matured_label_count=len(dates) * 5,
        unavailable_label_count=0,
        observed_value=observed,
        conclusion="DESCRIPTIVE_ESTIMATE_AVAILABLE",
        conclusion_scope="DESCRIPTIVE",
    )
    return _SliceEvidence(
        result=result,
        daily_series=tuple((date, observed) for date in dates),
    )


def test_package_conclusion_requires_every_non_regime_primary_baseline_slice() -> None:
    request = _full_request()
    dates = tuple(f"2025-01-{index + 1:02d}" for index in range(28)) + tuple(
        f"2025-02-{index + 1:02d}" for index in range(28)
    ) + tuple(f"2025-03-{index + 1:02d}" for index in range(4))
    available = _primary_slice(
        request=request,
        stage="selection_effective",
        dates=dates,
        observed=Decimal("0.02"),
    ).result
    unavailable_payload = available.model_dump(mode="python")
    unavailable_payload.update(
        {
            "slice_id": f"{available.slice_id}:h10",
            "horizon_trading_days": 10,
            "status": MetricStatus.INSUFFICIENT_SAMPLE,
            "reason_codes": ("ADVISORY_PHASE0B_INSUFFICIENT_DECISION_DATES",),
            "evaluable_date_count": 59,
            "effective_sample_count": 59,
            "missing_decision_date_count": 1,
            "observed_value": Decimal("0.01"),
            "conclusion": None,
            "conclusion_scope": None,
            "result_hash": None,
        }
    )
    unavailable = Phase0BMetricResultV1.model_validate(unavailable_payload)

    conclusion = Phase0BMetricEngine._package_conclusion(
        request=request,
        results=(available, unavailable),
        decision_date_count=60,
    )

    assert conclusion == "RESEARCH_EVIDENCE_UNAVAILABLE"


def test_multiple_testing_requires_paired_dates_and_economic_significance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _full_request()
    dates = tuple(f"2025-{index // 28 + 1:02d}-{index % 28 + 1:02d}" for index in range(252))
    evidence = _primary_slice(
        request=request,
        stage="selection_effective",
        dates=dates,
        observed=Decimal("0.005"),
    )
    monkeypatch.setattr(
        "backend.services.advisory_phase0b.audit_service.hansen_spa_p_value",
        lambda **_kwargs: Decimal("0.01"),
    )

    result = Phase0BMetricEngine()._apply_multiple_testing(
        request=request,
        evidence=(evidence,),
    )[0]

    assert result.p_value == Decimal("0.01")
    assert result.conclusion == "NO_POSITIVE_EVIDENCE"
    assert json.loads(result.detail_json)["economic_significance_minimum_effect"] == "0.01"

    shifted_dates = dates[1:] + ("2035-01-01",)
    mismatched = _primary_slice(
        request=request,
        stage="alpha_raw",
        dates=shifted_dates,
        observed=Decimal("0.02"),
    )
    paired_results = Phase0BMetricEngine()._apply_multiple_testing(
        request=request,
        evidence=(evidence, mismatched),
    )
    assert all(
        item.p_value is None
        and item.conclusion == "DESCRIPTIVE_ESTIMATE_AVAILABLE"
        and item.conclusion_scope == "DESCRIPTIVE"
        for item in paired_results
    )
    assert all(
        "ADVISORY_PHASE0B_INSUFFICIENT_PAIRED_DECISION_DATES" in item.reason_codes
        for item in paired_results
    )


def test_unclassified_target_does_not_run_winner_dependent_metric() -> None:
    request = _full_request()
    payload = request.audit_targets[0].model_dump(mode="python")
    payload.update({"style_hypothesis": "UNCLASSIFIED", "target_hash": None})
    unclassified_target = type(request.audit_targets[0]).model_validate(payload)
    winner_metric = next(
        item
        for item in request.metric_registry.metrics
        if item.metric_id == "precision-at5-v1"
    )

    evidence = Phase0BMetricEngine()._evaluate_definition(
        request=request,
        target=unclassified_target,
        definition=winner_metric,
        contexts_by_date={},
    )

    assert evidence == ()


def test_registered_regime_without_snapshot_evidence_is_explicitly_unavailable(
    tmp_path: Path,
) -> None:
    request = _full_request(
        market_regime_definitions=(
            Phase0BMarketRegimeDefinitionV1(
                regime_definition_id="market-regime-bull-v1",
                regime_value="BULL",
            ),
        )
    )
    repository_root, dataset_root, output_root = _roots(tmp_path)
    target = request.audit_targets[0]
    with Phase0BBoundedSpool(
        output_root=output_root,
        repository_root=repository_root,
        dataset_root=dataset_root,
        operation_id="regime-unavailable",
    ) as spool:
        _populate_metric_spool(spool)
        report = Phase0BMetricEngine().evaluate_target(
            request=request,
            target=target,
            program_binding=Phase0BTargetProgramBindingV1(
                target_hash=str(target.target_hash),
                range_program_hash="9" * 64,
            ),
            spool=spool,
        )

    regime_results = tuple(
        item for item in report.metric_results if item.regime_definition_id is not None
    )
    assert regime_results
    assert all(
        item.status in {MetricStatus.INPUT_CAPABILITY_NOT_AVAILABLE, MetricStatus.NOT_APPLICABLE}
        for item in regime_results
    )
    assert all(
        item.status is MetricStatus.NOT_APPLICABLE
        or "ADVISORY_PHASE0B_REGIME_EVIDENCE_UNAVAILABLE" in item.reason_codes
        for item in regime_results
    )


def test_registered_regime_reports_evidence_dates_separately_from_metric_dates(
    tmp_path: Path,
) -> None:
    request = _full_request(
        market_regime_definitions=(
            Phase0BMarketRegimeDefinitionV1(
                regime_definition_id="market-regime-bull-v1",
                regime_value="BULL",
            ),
        )
    )
    repository_root, dataset_root, output_root = _roots(tmp_path)
    target = request.audit_targets[0]
    with Phase0BBoundedSpool(
        output_root=output_root,
        repository_root=repository_root,
        dataset_root=dataset_root,
        operation_id="regime-present",
    ) as spool:
        _populate_metric_spool(
            spool,
            market_regime_at_t="BULL",
            market_regime_evidence_hash="f" * 64,
        )
        report = Phase0BMetricEngine().evaluate_target(
            request=request,
            target=target,
            program_binding=Phase0BTargetProgramBindingV1(
                target_hash=str(target.target_hash),
                range_program_hash="9" * 64,
            ),
            spool=spool,
        )

    regime_coverage = next(
        item
        for item in report.metric_results
        if item.metric_definition_id == "coverage-v1"
        and item.regime_definition_id == "market-regime-bull-v1"
    )
    assert regime_coverage.regime_count == 1
    assert "ADVISORY_PHASE0B_REGIME_EVIDENCE_UNAVAILABLE" not in regime_coverage.reason_codes


def test_formula_shape_cannot_silently_drop_frozen_stage_or_depth() -> None:
    request = _full_request()
    definition = next(
        item
        for item in request.metric_registry.metrics
        if item.metric_id == "strategy-recall-v1"
    )
    payload = definition.model_dump(mode="python")
    payload.update({"depths": (5, 20), "metric_hash": None})
    incomplete = Phase0BMetricDefinitionV1.model_validate(payload)

    with pytest.raises(Phase0BAuditError, match="complete frozen formula shape"):
        Phase0BMetricEngine._validate_formula_shape(incomplete)


def test_signal_regime_evidence_rejects_partial_rows_and_does_not_fill_forward(
    tmp_path: Path,
) -> None:
    request = _full_request(
        market_regime_definitions=(
            Phase0BMarketRegimeDefinitionV1(
                regime_definition_id="market-regime-bull-v1",
                regime_value="BULL",
            ),
        )
    )
    repository_root, dataset_root, output_root = _roots(tmp_path)
    target = request.audit_targets[0]
    with Phase0BBoundedSpool(
        output_root=output_root,
        repository_root=repository_root,
        dataset_root=dataset_root,
        operation_id="regime-partial",
    ) as spool:
        _populate_metric_spool(spool)
        _append(
            spool,
            role="universe_outcomes",
            rows=(
                {
                    "label_version_id": "universe-regime-partial",
                    "symbol": "999997.SZ",
                    "owner_type": "UNIVERSE",
                    "projection": "RETURN_NET_EXCESS",
                    "horizon_trading_days": 5,
                    "label_policy_hash": "a" * 64,
                    "label_source_revision_set_hash": "d" * 64,
                    "universe_layer": "PIT_ELIGIBLE",
                    "canonical_signal_id": "signal-1",
                    "maturity_status": "MATURED",
                    "outcome_event_status": "TERMINAL",
                    "projection_value_decimal": "0.01",
                    "market_regime_at_t": "BULL",
                    "market_regime_evidence_hash": "f" * 64,
                    "decision_as_of_trade_date": "2026-07-01",
                },
            ),
            identity_fields=("label_version_id",),
            source_char="f",
        )
        with pytest.raises(Phase0BAuditError, match="market regime evidence is incomplete"):
            Phase0BMetricEngine().evaluate_target(
                request=request,
                target=target,
                program_binding=Phase0BTargetProgramBindingV1(
                    target_hash=str(target.target_hash),
                    range_program_hash="9" * 64,
                ),
                spool=spool,
            )


def test_target_date_regime_identity_cannot_split_across_signals() -> None:
    def context(
        signal_id: str,
        regime: str | None,
        evidence_hash: str | None,
    ) -> SignalContext:
        return SignalContext(
            snapshot_id="snapshot-1",
            signal_id=signal_id,
            canonical_signal_scope_hash="a" * 64,
            universe_policy_hash="b" * 64,
            market_regime_at_t=regime,
            market_regime_evidence_hash=evidence_hash,
            candidates_by_stage={},
            stage_capability_by_stage={},
            outcomes_by_stage_symbol={},
            universe_outcomes=(),
        )

    bull = context("signal-1", "BULL", "c" * 64)
    bear = context("signal-2", "BEAR", "d" * 64)
    missing = context("signal-3", None, None)

    with pytest.raises(Phase0BAuditError, match="target decision date has conflicting"):
        Phase0BMetricEngine._validate_date_regime_contexts(
            decision_date="2026-07-01",
            contexts=(bull, bear),
        )
    with pytest.raises(Phase0BAuditError, match="target decision date has conflicting"):
        Phase0BMetricEngine._validate_date_regime_contexts(
            decision_date="2026-07-01",
            contexts=(bull, missing),
        )


def test_target_context_mapping_reloads_one_date_without_retaining_snapshot_rows() -> None:
    calls: list[str] = []
    context = SignalContext(
        snapshot_id="snapshot-1",
        signal_id="signal-1",
        canonical_signal_scope_hash="a" * 64,
        universe_policy_hash="b" * 64,
        market_regime_at_t="BULL",
        market_regime_evidence_hash="c" * 64,
        candidates_by_stage={},
        stage_capability_by_stage={},
        outcomes_by_stage_symbol={},
        universe_outcomes=(),
    )

    def load(decision_date: str) -> tuple[SignalContext, ...]:
        calls.append(decision_date)
        return (context,)

    mapping = _LazyTargetContextMapping(
        decision_dates=("2026-07-01", "2026-07-02"),
        loader=load,
    )
    assert tuple(mapping.items())
    assert calls == ["2026-07-01", "2026-07-02"]
    assert tuple(mapping.items())
    assert calls == [
        "2026-07-01",
        "2026-07-02",
        "2026-07-01",
        "2026-07-02",
    ]
    assert tuple(mapping.for_regime("BEAR").values()) == ((), ())
