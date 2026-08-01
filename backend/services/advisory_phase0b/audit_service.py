from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonical_json_text,
)
from backend.services.advisory_historical_range.summary_service import (
    Phase1WinnerDefinitionV1,
)
from backend.services.advisory_phase1.label_policy import Projection

from .contracts import (
    MetricStatus,
    Phase0BAuditTargetV1,
    Phase0BCandidateQualityAuditRequestV1,
    Phase0BMetricDefinitionV1,
)
from .errors import (
    Phase0BAuditError,
    REASON_BLACKLIST_COUNTERFACTUAL_UNAVAILABLE,
    REASON_INSUFFICIENT_DECISION_DATES,
    REASON_INSUFFICIENT_MATURE_LABELS,
    REASON_INSUFFICIENT_PAIRED_DECISION_DATES,
    REASON_INSUFFICIENT_WINNER_EVENTS,
    REASON_METRIC_REGISTRY_CONFLICT,
    REASON_PROJECTION_NOT_APPLICABLE,
    REASON_REGIME_EVIDENCE_UNAVAILABLE,
    REASON_STAGE_CAPABILITY_UNAVAILABLE,
    REASON_UNIVERSE_OUTCOME_UNAVAILABLE,
)
from .metrics import (
    CandidateOutcome,
    benjamini_yekutieli_adjusted,
    equal_weight_by_decision_date,
    fixed_k_portfolio,
    hansen_spa_p_value,
    iter_stationary_bootstrap_indices,
    ndcg_at_k,
    nearest_rank,
    precision_at_k,
    quantize_metric,
    random5_symbols,
    recall_at_k,
    spearman,
    stage_overlap,
    stationary_bootstrap_mean_interval,
)
from .report_store import Phase0BMetricResultV1, Phase0BTargetAuditReportV1
from .snapshot_reader import Phase0BTargetProgramBindingV1
from .spool import Phase0BBoundedSpool


SUPPORTED_METRIC_IDS = frozenset(
    {
        "stage-topk-point-estimate-v1",
        "candidate-pool-point-estimate-v1",
        "rank-monotonicity-v1",
        "stage-incremental-lift-v1",
        "random5-v1",
        "precision-at5-v1",
        "ndcg-at5-v1",
        "strategy-recall-v1",
        "conditional-recall-v1",
        "blacklist-diagnostic-v1",
        "coverage-v1",
    }
)

STAGE_ORDER = (
    "alpha_raw",
    "hmm_adjusted",
    "risk_policy_adjusted",
    "selection_effective",
)
FORMULA_SHAPES: dict[str, dict[str, object]] = {
    "stage-topk-point-estimate-v1": {
        "family": "PRIMARY",
        "horizon_source": "LABEL_POLICY",
        "stages": frozenset(STAGE_ORDER),
        "depths": frozenset({5, 10, 20}),
        "output_unit": "DECIMAL_RETURN",
    },
    "candidate-pool-point-estimate-v1": {
        "family": "DIAGNOSTIC",
        "horizon_source": "LABEL_POLICY",
        "stages": frozenset({"alpha_raw", "selection_effective"}),
        "depths": frozenset({5, 10, 20}),
        "output_unit": "DECIMAL_RETURN",
    },
    "rank-monotonicity-v1": {
        "family": "DIAGNOSTIC",
        "horizon_source": "LABEL_POLICY",
        "stages": frozenset(STAGE_ORDER),
        "depths": frozenset({20}),
        "output_unit": "CORRELATION",
    },
    "stage-incremental-lift-v1": {
        "family": "DIAGNOSTIC",
        "horizon_source": "LABEL_POLICY",
        "stages": frozenset(STAGE_ORDER),
        "depths": frozenset({5}),
        "output_unit": "DECIMAL_RETURN",
    },
    "random5-v1": {
        "family": "DIAGNOSTIC",
        "horizon_source": "LABEL_POLICY",
        "stages": frozenset({"alpha_raw"}),
        "depths": frozenset({5, 10, 20}),
        "output_unit": "DECIMAL_RETURN",
    },
    "precision-at5-v1": {
        "family": "DIAGNOSTIC",
        "horizon_source": "WINNER_DEFINITION",
        "stages": frozenset({"selection_effective"}),
        "depths": frozenset({5}),
        "output_unit": "RATIO",
    },
    "ndcg-at5-v1": {
        "family": "DIAGNOSTIC",
        "horizon_source": "LABEL_POLICY",
        "stages": frozenset({"alpha_raw", "selection_effective"}),
        "depths": frozenset({5}),
        "output_unit": "RATIO",
    },
    "strategy-recall-v1": {
        "family": "DIAGNOSTIC",
        "horizon_source": "WINNER_DEFINITION",
        "stages": frozenset({"selection_effective"}),
        "depths": frozenset({5, 10, 20}),
        "output_unit": "RATIO",
    },
    "conditional-recall-v1": {
        "family": "DIAGNOSTIC",
        "horizon_source": "WINNER_DEFINITION",
        "stages": frozenset({"alpha_raw", "selection_effective"}),
        "depths": frozenset({5, 10, 20}),
        "output_unit": "RATIO",
    },
    "blacklist-diagnostic-v1": {
        "family": "DIAGNOSTIC",
        "horizon_source": "LABEL_POLICY",
        "stages": frozenset({"risk_policy_adjusted"}),
        "depths": frozenset({5}),
        "output_unit": "DECIMAL_RETURN",
    },
    "coverage-v1": {
        "family": "COVERAGE",
        "horizon_source": "LABEL_POLICY",
        "stages": frozenset({"selection_effective"}),
        "depths": frozenset({5}),
        "output_unit": "RATIO",
    },
}
NON_PORTFOLIO_PROJECTIONS = frozenset(
    {Projection.GAP_1D, Projection.BARRIER, Projection.SURVIVAL}
)


@dataclass(frozen=True)
class SignalContext:
    snapshot_id: str
    signal_id: str
    canonical_signal_scope_hash: str
    universe_policy_hash: str
    market_regime_at_t: str | None
    market_regime_evidence_hash: str | None
    candidates_by_stage: Mapping[str, tuple[dict[str, Any], ...]]
    stage_capability_by_stage: Mapping[str, str]
    outcomes_by_stage_symbol: Mapping[tuple[str, str, str, int], dict[str, Any]]
    universe_outcomes: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class _SliceEvidence:
    result: Phase0BMetricResultV1
    daily_series: tuple[tuple[str, Decimal], ...]

    @property
    def daily_values(self) -> tuple[Decimal, ...]:
        return tuple(value for _decision_date, value in self.daily_series)


@dataclass(frozen=True)
class _LazyTargetContextMapping(Mapping[str, tuple[SignalContext, ...]]):
    decision_dates: tuple[str, ...]
    loader: Callable[[str], tuple[SignalContext, ...]]
    regime_value: str | None = None

    def __getitem__(self, decision_date: str) -> tuple[SignalContext, ...]:
        if decision_date not in self.decision_dates:
            raise KeyError(decision_date)
        contexts = self.loader(decision_date)
        if self.regime_value is None:
            return contexts
        return tuple(
            context
            for context in contexts
            if context.market_regime_at_t == self.regime_value
        )

    def __iter__(self) -> Iterator[str]:
        return iter(self.decision_dates)

    def __len__(self) -> int:
        return len(self.decision_dates)

    def for_regime(self, regime_value: str) -> "_LazyTargetContextMapping":
        return _LazyTargetContextMapping(
            decision_dates=self.decision_dates,
            loader=self.loader,
            regime_value=regime_value,
        )


class Phase0BMetricEngine:
    def evaluate_target(
        self,
        *,
        request: Phase0BCandidateQualityAuditRequestV1,
        target: Phase0BAuditTargetV1,
        program_binding: Phase0BTargetProgramBindingV1,
        spool: Phase0BBoundedSpool,
    ) -> Phase0BTargetAuditReportV1:
        metric_ids = {item.metric_id for item in request.metric_registry.metrics}
        if metric_ids != SUPPORTED_METRIC_IDS:
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "Phase 0B registry must contain every frozen metric family exactly once",
                context={
                    "missing": tuple(sorted(SUPPORTED_METRIC_IDS - metric_ids)),
                    "extra": tuple(sorted(metric_ids - SUPPORTED_METRIC_IDS)),
                },
            )
        for definition in request.metric_registry.metrics:
            self._validate_formula_shape(definition)
        decision_dates = spool.target_decision_dates(
            snapshot_id=target.snapshot_id,
            package_id=target.package_id,
            manifest_sha256=target.manifest_sha256,
            alpha_mode=target.alpha_mode,
            program_id=program_binding.formal_program_id,
            range_program_hash=program_binding.range_program_hash,
        )
        def load_contexts(decision_date: str) -> tuple[SignalContext, ...]:
            signal_rows = tuple(
                spool.iter_target_signals(
                    snapshot_id=target.snapshot_id,
                    package_id=target.package_id,
                    manifest_sha256=target.manifest_sha256,
                    alpha_mode=target.alpha_mode,
                    program_id=program_binding.formal_program_id,
                    range_program_hash=program_binding.range_program_hash,
                    decision_date=decision_date,
                )
            )
            contexts = tuple(
                self._signal_context(
                    snapshot_id=target.snapshot_id,
                    decision_date=decision_date,
                    signal=row,
                    spool=spool,
                )
                for row in signal_rows
            )
            self._validate_date_regime_contexts(
                decision_date=decision_date,
                contexts=contexts,
            )
            return contexts

        contexts_by_date = _LazyTargetContextMapping(
            decision_dates=decision_dates,
            loader=load_contexts,
        )
        evidence: list[_SliceEvidence] = []
        for definition in request.metric_registry.metrics:
            base_evidence = self._evaluate_definition(
                    request=request,
                    target=target,
                    definition=definition,
                    contexts_by_date=contexts_by_date,
            )
            evidence.extend(base_evidence)
            for regime in request.multiple_testing_registry.market_regime_definitions:
                filtered_contexts = contexts_by_date.for_regime(regime.regime_value)
                evidence.extend(
                    self._tag_regime(
                        evidence=self._evaluate_definition(
                            request=request,
                            target=target,
                            definition=definition,
                            contexts_by_date=filtered_contexts,
                        ),
                        regime_definition_id=regime.regime_definition_id,
                        regime_value=regime.regime_value,
                        regime_decision_date_count=sum(
                            bool(contexts) for contexts in filtered_contexts.values()
                        ),
                    )
                )
        results = self._apply_multiple_testing(
            request=request,
            evidence=tuple(evidence),
        )
        package_conclusion = self._package_conclusion(
            request=request,
            results=results,
            decision_date_count=len(decision_dates),
        )
        recommendations = (
            "Generate a new 2/3/5-year PIT SEALED snapshot before model training.",
            f"Retain the frozen {target.style_hypothesis.value} winner and horizon family.",
            "Use 20 candidates as the reranking input and evaluate Top5 as the primary portfolio.",
        )
        return Phase0BTargetAuditReportV1(
            target_hash=str(target.target_hash),
            snapshot_id=target.snapshot_id,
            program_id=target.program_id,
            package_id=target.package_id,
            manifest_sha256=target.manifest_sha256,
            alpha_mode=target.alpha_mode,
            style_hypothesis=target.style_hypothesis.value,
            decision_date_count=len(decision_dates),
            metric_results=results,
            package_conclusion=package_conclusion,
            phase2_phase3_recommendations=recommendations,
        )

    @staticmethod
    def _validate_formula_shape(definition: Phase0BMetricDefinitionV1) -> None:
        shape = FORMULA_SHAPES.get(definition.metric_id)
        if shape is None:
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "metric registry selected an unknown Phase 0B formula",
                context={"metric_id": definition.metric_id},
            )
        actual = {
            "family": definition.family,
            "horizon_source": definition.horizon_source,
            "stages": frozenset(definition.stages),
            "depths": frozenset(definition.depths),
            "output_unit": definition.output_unit,
        }
        if actual != shape:
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "metric definition differs from its complete frozen formula shape",
                context={
                    "metric_id": definition.metric_id,
                    "expected": {
                        key: tuple(sorted(value)) if isinstance(value, frozenset) else value
                        for key, value in shape.items()
                    },
                    "actual": {
                        key: tuple(sorted(value)) if isinstance(value, frozenset) else value
                        for key, value in actual.items()
                    },
                },
            )

    @staticmethod
    def _validate_date_regime_contexts(
        *,
        decision_date: str,
        contexts: tuple[SignalContext, ...],
    ) -> None:
        pairs = {
            (context.market_regime_at_t, context.market_regime_evidence_hash)
            for context in contexts
            if context.market_regime_at_t is not None
            and context.market_regime_evidence_hash is not None
        }
        missing_count = sum(
            context.market_regime_at_t is None
            and context.market_regime_evidence_hash is None
            for context in contexts
        )
        if len(pairs) > 1 or (pairs and missing_count):
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "target decision date has conflicting market regime evidence",
                context={"decision_date": decision_date},
            )

    def _evaluate_definition(
        self,
        *,
        request: Phase0BCandidateQualityAuditRequestV1,
        target: Phase0BAuditTargetV1,
        definition: Phase0BMetricDefinitionV1,
        contexts_by_date: Mapping[str, tuple[SignalContext, ...]],
    ) -> tuple[_SliceEvidence, ...]:
        expected_cash_policy = self._expected_cash_policy(definition)
        if definition.cash_policy != expected_cash_policy:
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "metric cash policy differs from its frozen formula",
                context={
                    "metric_id": definition.metric_id,
                    "projection": definition.projection.value,
                    "expected_cash_policy": expected_cash_policy,
                    "actual_cash_policy": definition.cash_policy,
                },
            )
        dispatch = {
            "stage-topk-point-estimate-v1": self._stage_topk,
            "candidate-pool-point-estimate-v1": self._candidate_pool,
            "rank-monotonicity-v1": self._rank_monotonicity,
            "stage-incremental-lift-v1": self._stage_incremental,
            "random5-v1": self._random5,
            "precision-at5-v1": self._precision,
            "ndcg-at5-v1": self._ndcg,
            "strategy-recall-v1": self._strategy_recall,
            "conditional-recall-v1": self._conditional_recall,
            "blacklist-diagnostic-v1": self._blacklist,
            "coverage-v1": self._coverage,
        }
        evaluator = dispatch.get(definition.metric_id)
        if evaluator is None:
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "metric registry selected an unknown Phase 0B formula",
                context={"metric_id": definition.metric_id},
            )
        style_binding = next(
            item
            for item in request.multiple_testing_registry.horizons_by_style
            if item.style_hypothesis is target.style_hypothesis
        )
        allowed_winner_ids = set(style_binding.winner_definition_ids)
        active_winner_definition_ids = tuple(
            winner_id
            for winner_id in definition.winner_definition_ids
            if winner_id in allowed_winner_ids
        )
        if definition.horizon_source == "WINNER_DEFINITION":
            active_horizons = tuple(
                sorted(
                    {
                        item.horizon_trade_days
                        for item in request.winner_definitions
                        if item.winner_definition_id in active_winner_definition_ids
                    }
                )
            )
        elif target.style_hypothesis.value == "UNCLASSIFIED":
            active_horizons = definition.horizons
        else:
            active_horizons = tuple(
                horizon for horizon in definition.horizons if horizon in style_binding.horizons
            )
        return evaluator(
            request=request,
            definition=definition,
            contexts_by_date=contexts_by_date,
            horizons=active_horizons,
            winner_definition_ids=active_winner_definition_ids,
        )

    @staticmethod
    def _tag_regime(
        *,
        evidence: tuple[_SliceEvidence, ...],
        regime_definition_id: str,
        regime_value: str,
        regime_decision_date_count: int,
    ) -> tuple[_SliceEvidence, ...]:
        output: list[_SliceEvidence] = []
        for item in evidence:
            detail = json.loads(item.result.detail_json)
            detail.update(
                {
                    "regime_definition_id": regime_definition_id,
                    "regime_value": regime_value,
                }
            )
            payload = item.result.model_dump(mode="python")
            payload.update(
                {
                    "slice_id": f"{item.result.slice_id}:regime={regime_definition_id}",
                    "regime_definition_id": regime_definition_id,
                    "regime_value": regime_value,
                    "regime_count": regime_decision_date_count,
                    "detail_json": canonical_json_text(detail),
                    "result_hash": None,
                }
            )
            if (
                regime_decision_date_count == 0
                and not item.daily_series
                and item.result.status is not MetricStatus.NOT_APPLICABLE
            ):
                payload.update(
                    {
                        "status": MetricStatus.INPUT_CAPABILITY_NOT_AVAILABLE,
                        "reason_codes": tuple(
                            sorted(
                                set(item.result.reason_codes)
                                | {REASON_REGIME_EVIDENCE_UNAVAILABLE}
                            )
                        ),
                        "observed_value": None,
                        "conclusion": None,
                        "conclusion_scope": None,
                        "confidence_interval_lower": None,
                        "confidence_interval_upper": None,
                        "p_value": None,
                    }
                )
            output.append(
                _SliceEvidence(
                    result=Phase0BMetricResultV1.model_validate(payload),
                    daily_series=item.daily_series,
                )
            )
        return tuple(output)

    @staticmethod
    def _expected_cash_policy(definition: Phase0BMetricDefinitionV1) -> str:
        if definition.family == "COVERAGE":
            return "NOT_APPLICABLE"
        if definition.metric_id == "precision-at5-v1":
            return "PRECISION_EMPTY_FAILURE"
        if definition.metric_id == "ndcg-at5-v1":
            return "NDCG_EMPTY_ZERO_GAIN"
        if definition.projection in NON_PORTFOLIO_PROJECTIONS:
            return "NOT_APPLICABLE"
        if definition.projection is Projection.RETURN_NET_EXCESS:
            return "NET_EXCESS_NEGATIVE_BENCHMARK"
        if definition.projection in {Projection.RETURN_GROSS, Projection.RETURN_NET_ABSOLUTE}:
            return "RETURN_ZERO"
        if definition.projection in {
            Projection.PATH_MFE,
            Projection.PATH_MAE,
            Projection.EXECUTABLE_MFE,
            Projection.EXECUTABLE_MAE,
        }:
            return "PATH_ZERO_DIAGNOSTIC"
        raise Phase0BAuditError(
            REASON_METRIC_REGISTRY_CONFLICT,
            "metric projection has no frozen cash policy",
            context={"projection": definition.projection.value},
        )

    @staticmethod
    def _signal_context(
        *,
        snapshot_id: str,
        decision_date: str,
        signal: Mapping[str, Any],
        spool: Phase0BBoundedSpool,
    ) -> SignalContext:
        signal_id = str(signal["canonical_signal_id"])
        selected_observations = tuple(
            spool.iter_rows_where(
                snapshot_id=snapshot_id,
                logical_role="selected_observations",
                field_values={"canonical_signal_id": signal_id},
                decision_date=decision_date,
            )
        )
        if len(selected_observations) != 1:
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "metric context requires one selected observation",
                context={"canonical_signal_id": signal_id},
            )
        observation_id = str(selected_observations[0]["terminal_observation_version_id"])
        observation_versions = tuple(
            spool.iter_rows_where(
                snapshot_id=snapshot_id,
                logical_role="observation_versions",
                field_values={"observation_version_id": observation_id},
                decision_date=decision_date,
            )
        )
        if len(observation_versions) != 1:
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "metric context requires one terminal observation version",
                context={"canonical_signal_id": signal_id},
            )
        observation = observation_versions[0]
        stage_rows = tuple(
            spool.iter_rows_where(
                snapshot_id=snapshot_id,
                logical_role="stage_summaries",
                field_values={"observation_version_id": observation_id},
                decision_date=decision_date,
            )
        )
        if len({str(item["stage"]) for item in stage_rows}) != len(stage_rows):
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "metric context has duplicate stage summaries",
                context={"canonical_signal_id": signal_id},
            )
        candidates_by_stage = {
            str(stage["stage"]): tuple(
                spool.iter_rows_where(
                    snapshot_id=snapshot_id,
                    logical_role="stage_candidates",
                    field_values={"stage_evidence_id": str(stage["stage_evidence_id"])},
                    decision_date=decision_date,
                )
            )
            for stage in stage_rows
        }
        selected_labels = tuple(
            spool.iter_rows(
                snapshot_id=snapshot_id,
                logical_role="selected_labels",
                decision_date=decision_date,
            )
        )
        terminal_label_ids = {
            str(item["terminal_label_version_id"])
            for item in selected_labels
            if item.get("selection_status") == "SELECTED"
            and item.get("terminal_label_version_id") is not None
        }
        outcome_rows = tuple(
            spool.iter_rows_where(
                snapshot_id=snapshot_id,
                logical_role="outcome_labels",
                field_values={"canonical_signal_id": signal_id},
                decision_date=decision_date,
            )
        )
        outcomes_by_stage_symbol: dict[tuple[str, str, str, int], dict[str, Any]] = {}
        for outcome in outcome_rows:
            if (
                outcome.get("owner_type") != "CANDIDATE"
                or str(outcome.get("label_version_id")) not in terminal_label_ids
            ):
                continue
            key = (
                str(outcome["candidate_stage_evidence_id"]),
                str(outcome["symbol"]),
                str(outcome["projection"]),
                int(outcome["horizon_trading_days"]),
            )
            if key in outcomes_by_stage_symbol:
                raise Phase0BAuditError(
                    REASON_METRIC_REGISTRY_CONFLICT,
                    "metric context has duplicate terminal candidate outcomes",
                    context={"canonical_signal_id": signal_id},
                )
            outcomes_by_stage_symbol[key] = outcome
        universe_outcomes = tuple(
            spool.iter_rows_where(
                snapshot_id=snapshot_id,
                logical_role="universe_outcomes",
                field_values={"canonical_signal_id": signal_id},
                decision_date=decision_date,
            )
        )
        regime_pairs = {
            (str(item["market_regime_at_t"]), str(item["market_regime_evidence_hash"]))
            for item in universe_outcomes
            if item.get("market_regime_at_t") is not None
            and item.get("market_regime_evidence_hash") is not None
        }
        missing_regime_rows = tuple(
            item
            for item in universe_outcomes
            if item.get("market_regime_at_t") is None
            and item.get("market_regime_evidence_hash") is None
        )
        incomplete_regime_rows = tuple(
            item
            for item in universe_outcomes
            if (item.get("market_regime_at_t") is None)
            != (item.get("market_regime_evidence_hash") is None)
        )
        if (
            incomplete_regime_rows
            or len(regime_pairs) > 1
            or (regime_pairs and missing_regime_rows)
        ):
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "signal/date market regime evidence is incomplete or conflicting",
                context={"canonical_signal_id": signal_id, "decision_date": decision_date},
            )
        regime_value, regime_hash = next(iter(regime_pairs), (None, None))
        return SignalContext(
            snapshot_id=snapshot_id,
            signal_id=signal_id,
            canonical_signal_scope_hash=str(signal["canonical_signal_scope_hash"]),
            universe_policy_hash=str(observation["universe_policy_hash"]),
            market_regime_at_t=regime_value,
            market_regime_evidence_hash=regime_hash,
            candidates_by_stage=candidates_by_stage,
            stage_capability_by_stage={
                str(stage["stage"]): str(stage["capability_status"])
                for stage in stage_rows
            },
            outcomes_by_stage_symbol=outcomes_by_stage_symbol,
            universe_outcomes=universe_outcomes,
        )

    @staticmethod
    def _matching_outcome(
        *,
        context: SignalContext,
        candidate: Mapping[str, Any],
        projection: Projection,
        horizon: int,
    ) -> dict[str, Any] | None:
        outcome = context.outcomes_by_stage_symbol.get(
            (
                str(candidate["stage_evidence_id"]),
                str(candidate["symbol"]),
                projection.value,
                horizon,
            )
        )
        if outcome is None:
            return None
        return outcome

    @staticmethod
    def _optional_decimal(value: Any, *, field_name: str) -> Decimal | None:
        if value is None:
            return None
        try:
            parsed = Decimal(str(value))
        except Exception as error:
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "metric input contains an invalid decimal",
                context={"field": field_name},
            ) from error
        if not parsed.is_finite():
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "metric input contains a non-finite decimal",
                context={"field": field_name},
            )
        return parsed

    @staticmethod
    def _required_decimal(value: Any, *, field_name: str) -> Decimal:
        parsed = Phase0BMetricEngine._optional_decimal(value, field_name=field_name)
        if parsed is None:
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "metric input is missing a required decimal",
                context={"field": field_name},
            )
        return parsed

    @staticmethod
    def _candidate_outcomes(
        *,
        context: SignalContext,
        stage: str,
        projection: Projection,
        horizon: int,
    ) -> tuple[CandidateOutcome, ...]:
        rows = tuple(
            item
            for item in context.candidates_by_stage.get(stage, ())
            if item.get("membership_status") == "INCLUDED" and item.get("rank") is not None
        )
        ranks = tuple(sorted(int(item["rank"]) for item in rows))
        if ranks != tuple(range(1, len(ranks) + 1)):
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "included stage candidate ranks must be unique and contiguous from one",
                context={"signal_id": context.signal_id, "stage": stage, "ranks": ranks},
            )
        output: list[CandidateOutcome] = []
        for row in sorted(rows, key=lambda item: int(item["rank"])):
            outcome = Phase0BMetricEngine._matching_outcome(
                context=context,
                candidate=row,
                projection=projection,
                horizon=horizon,
            )
            output.append(
                CandidateOutcome(
                    symbol=str(row["symbol"]),
                    rank=int(row["rank"]),
                    value=Phase0BMetricEngine._optional_decimal(
                        outcome.get("projection_value_decimal") if outcome is not None else None,
                        field_name="projection_value_decimal",
                    ),
                    maturity_status=str(outcome.get("maturity_status")) if outcome else "UNAVAILABLE",
                    outcome_event_status=str(outcome.get("outcome_event_status")) if outcome else "UNAVAILABLE",
                    benchmark_net_total_return=Phase0BMetricEngine._optional_decimal(
                        outcome.get("benchmark_net_total_return") if outcome is not None else None,
                        field_name="benchmark_net_total_return",
                    ),
                )
            )
        return tuple(output)

    @staticmethod
    def _benchmark(candidates: Sequence[CandidateOutcome]) -> Decimal | None:
        values = {
            item.benchmark_net_total_return
            for item in candidates
            if item.benchmark_net_total_return is not None
        }
        if len(values) > 1:
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "candidate labels disagree on benchmark return",
            )
        return next(iter(values), None)

    def _stage_topk(self, **kwargs: Any) -> tuple[_SliceEvidence, ...]:
        request: Phase0BCandidateQualityAuditRequestV1 = kwargs["request"]
        definition: Phase0BMetricDefinitionV1 = kwargs["definition"]
        contexts_by_date: Mapping[str, tuple[SignalContext, ...]] = kwargs["contexts_by_date"]
        slices: list[_SliceEvidence] = []
        for horizon in kwargs["horizons"]:
            for stage in definition.stages:
                for depth in definition.depths:
                    daily: dict[str, list[Decimal]] = defaultdict(list)
                    counts = [0, 0, 0]
                    stage_unavailable = 0
                    for decision_date, contexts in contexts_by_date.items():
                        for context in contexts:
                            if context.stage_capability_by_stage.get(stage) != "FULL":
                                stage_unavailable += 1
                                continue
                            candidates = self._candidate_outcomes(
                                context=context,
                                stage=stage,
                                projection=definition.projection,
                                horizon=horizon,
                            )
                            result = fixed_k_portfolio(
                                candidates=candidates,
                                k=depth,
                                projection=definition.projection,
                                benchmark_net_total_return=self._benchmark(candidates),
                            )
                            selected_count = sum(item.rank <= depth for item in candidates)
                            counts[0] += selected_count
                            counts[1] += result.qualified_count
                            counts[2] += selected_count - result.qualified_count
                            if result.value is not None:
                                daily[decision_date].append(result.value)
                    slices.append(
                        self._result_from_daily(
                            request=request,
                            definition=definition,
                            slice_id=f"{definition.projection.value}:h{horizon}:{stage}:k{depth}",
                            projection=definition.projection,
                            horizon=horizon,
                            stage=stage,
                            depth=depth,
                            daily=daily,
                            total_dates=len(contexts_by_date),
                            candidate_count=counts[0],
                            matured_count=counts[1],
                            unavailable_count=counts[2],
                            detail={
                                "comparison": "POINT_ESTIMATE",
                                "stage_capability_unavailable_count": stage_unavailable,
                            },
                            reason_override=REASON_STAGE_CAPABILITY_UNAVAILABLE
                            if stage_unavailable and not daily
                            else None,
                            not_applicable=definition.projection in NON_PORTFOLIO_PROJECTIONS,
                        )
                    )
                for depth in definition.depths:
                    for candidate_depth in definition.depths:
                        daily_lift: dict[str, list[Decimal]] = defaultdict(list)
                        total = matured = unavailable = stage_unavailable = 0
                        for decision_date, contexts in contexts_by_date.items():
                            for context in contexts:
                                if (
                                    context.stage_capability_by_stage.get(stage) != "FULL"
                                    or context.stage_capability_by_stage.get("alpha_raw") != "FULL"
                                ):
                                    stage_unavailable += 1
                                    continue
                                stage_candidates = self._candidate_outcomes(
                                    context=context,
                                    stage=stage,
                                    projection=definition.projection,
                                    horizon=horizon,
                                )
                                pool_candidates = self._candidate_outcomes(
                                    context=context,
                                    stage="alpha_raw",
                                    projection=definition.projection,
                                    horizon=horizon,
                                )
                                stage_result = fixed_k_portfolio(
                                    candidates=stage_candidates,
                                    k=depth,
                                    projection=definition.projection,
                                    benchmark_net_total_return=self._benchmark(stage_candidates),
                                )
                                pool_result = fixed_k_portfolio(
                                    candidates=pool_candidates,
                                    k=candidate_depth,
                                    projection=definition.projection,
                                    benchmark_net_total_return=self._benchmark(pool_candidates),
                                )
                                stage_selected = sum(
                                    item.rank <= depth for item in stage_candidates
                                )
                                pool_selected = sum(
                                    item.rank <= candidate_depth for item in pool_candidates
                                )
                                total += stage_selected + pool_selected
                                matured += (
                                    stage_result.qualified_count + pool_result.qualified_count
                                )
                                unavailable += (
                                    stage_selected
                                    + pool_selected
                                    - stage_result.qualified_count
                                    - pool_result.qualified_count
                                )
                                if stage_result.value is not None and pool_result.value is not None:
                                    daily_lift[decision_date].append(
                                        stage_result.value - pool_result.value
                                    )
                        slices.append(
                            self._result_from_daily(
                                request=request,
                                definition=definition,
                                slice_id=(
                                    f"{definition.projection.value}:h{horizon}:{stage}:k{depth}:"
                                    f"lift-vs-alpha-raw-d{candidate_depth}"
                                ),
                                projection=definition.projection,
                                horizon=horizon,
                                stage=stage,
                                depth=depth,
                                daily=daily_lift,
                                total_dates=len(contexts_by_date),
                                candidate_count=total,
                                matured_count=matured,
                                unavailable_count=unavailable,
                                detail={
                                    "comparison": (
                                        "TOPK_PORTFOLIO_MINUS_ALPHA_RAW_CANDIDATE_POOL"
                                    ),
                                    "candidate_pool_depth": candidate_depth,
                                    "stage_capability_unavailable_count": stage_unavailable,
                                },
                                reason_override=REASON_STAGE_CAPABILITY_UNAVAILABLE
                                if stage_unavailable and not daily_lift
                                else None,
                                not_applicable=(
                                    definition.projection in NON_PORTFOLIO_PROJECTIONS
                                ),
                            )
                        )
        return tuple(slices)

    def _candidate_pool(self, **kwargs: Any) -> tuple[_SliceEvidence, ...]:
        request: Phase0BCandidateQualityAuditRequestV1 = kwargs["request"]
        definition: Phase0BMetricDefinitionV1 = kwargs["definition"]
        contexts_by_date: Mapping[str, tuple[SignalContext, ...]] = kwargs["contexts_by_date"]
        output: list[_SliceEvidence] = []
        for horizon in kwargs["horizons"]:
            for depth in definition.depths:
                daily: dict[str, list[Decimal]] = defaultdict(list)
                total = matured = unavailable = stage_unavailable = 0
                for decision_date, contexts in contexts_by_date.items():
                    for context in contexts:
                        if (
                            context.stage_capability_by_stage.get("alpha_raw") != "FULL"
                            or context.stage_capability_by_stage.get("selection_effective") != "FULL"
                        ):
                            stage_unavailable += 1
                            continue
                        candidate_pool = tuple(
                            item
                            for item in self._candidate_outcomes(
                                context=context,
                                stage="alpha_raw",
                                projection=definition.projection,
                                horizon=horizon,
                            )
                            if item.rank <= depth
                        )
                        selection = self._candidate_outcomes(
                            context=context,
                            stage="selection_effective",
                            projection=definition.projection,
                            horizon=horizon,
                        )
                        total += len(candidate_pool)
                        matured += sum(item.evaluable for item in candidate_pool)
                        unavailable += sum(not item.evaluable for item in candidate_pool)
                        candidate_value = fixed_k_portfolio(
                            candidates=candidate_pool,
                            k=depth,
                            projection=definition.projection,
                            benchmark_net_total_return=self._benchmark(candidate_pool),
                        ).value
                        selection_value = fixed_k_portfolio(
                            candidates=selection,
                            k=5,
                            projection=definition.projection,
                            benchmark_net_total_return=self._benchmark(selection),
                        ).value
                        if candidate_value is not None and selection_value is not None:
                            daily[decision_date].append(selection_value - candidate_value)
                output.append(
                    self._result_from_daily(
                        request=request,
                        definition=definition,
                        slice_id=(
                            f"{definition.projection.value}:h{horizon}:"
                            f"selection-top5-vs-candidate-pool-d{depth}"
                        ),
                        projection=definition.projection,
                        horizon=horizon,
                        stage="selection_effective-alpha_raw",
                        depth=depth,
                        daily=daily,
                        total_dates=len(contexts_by_date),
                        candidate_count=total,
                        matured_count=matured,
                        unavailable_count=unavailable,
                        detail={
                            "comparison": "SELECTION_EFFECTIVE_TOP5_MINUS_ALPHA_RAW_CANDIDATE_POOL_D",
                            "stage_capability_unavailable_count": stage_unavailable,
                        },
                        reason_override=REASON_STAGE_CAPABILITY_UNAVAILABLE
                        if stage_unavailable and not daily
                        else None,
                        not_applicable=definition.projection in NON_PORTFOLIO_PROJECTIONS,
                    )
                )
        return tuple(output)

    def _coverage(self, **kwargs: Any) -> tuple[_SliceEvidence, ...]:
        request: Phase0BCandidateQualityAuditRequestV1 = kwargs["request"]
        definition: Phase0BMetricDefinitionV1 = kwargs["definition"]
        contexts_by_date: Mapping[str, tuple[SignalContext, ...]] = kwargs["contexts_by_date"]
        output: list[_SliceEvidence] = []
        allowed_regimes = {
            item.regime_value
            for item in request.multiple_testing_registry.market_regime_definitions
        }
        for horizon in kwargs["horizons"]:
            daily: dict[str, list[Decimal]] = defaultdict(list)
            total = matured = unavailable = 0
            stage_unavailable = 0
            maturity_counts: dict[str, int] = defaultdict(int)
            regime_counts: dict[str, int] = defaultdict(int)
            regime_evidence_missing = 0
            for decision_date, contexts in contexts_by_date.items():
                date_regime = next(
                    (
                        context.market_regime_at_t
                        for context in contexts
                        if context.market_regime_at_t is not None
                    ),
                    None,
                )
                if date_regime is not None:
                    if date_regime not in allowed_regimes:
                        raise Phase0BAuditError(
                            REASON_METRIC_REGISTRY_CONFLICT,
                            "snapshot contains a market regime outside the frozen registry",
                            context={
                                "decision_date": decision_date,
                                "unregistered_regimes": (date_regime,),
                            },
                        )
                    regime_counts[date_regime] += 1
                elif contexts:
                    regime_evidence_missing += 1
                for context in contexts:
                    if context.stage_capability_by_stage.get("selection_effective") != "FULL":
                        stage_unavailable += 1
                        continue
                    candidates = self._candidate_outcomes(
                        context=context,
                        stage="selection_effective",
                        projection=definition.projection,
                        horizon=horizon,
                    )
                    if candidates:
                        matured_signal = sum(item.evaluable for item in candidates)
                        daily[decision_date].append(Decimal(matured_signal) / Decimal(len(candidates)))
                    total += len(candidates)
                    matured += sum(item.evaluable for item in candidates)
                    unavailable += sum(not item.evaluable for item in candidates)
                    for item in candidates:
                        maturity_counts[item.maturity_status] += 1
            output.append(
                self._result_from_daily(
                    request=request,
                    definition=definition,
                    slice_id=f"{definition.projection.value}:h{horizon}:coverage",
                    projection=definition.projection,
                    horizon=horizon,
                    stage=None,
                    depth=None,
                    daily=daily,
                    total_dates=len(contexts_by_date),
                    candidate_count=total,
                    matured_count=matured,
                    unavailable_count=unavailable,
                    detail={
                        "maturity_counts": dict(sorted(maturity_counts.items())),
                        "market_regime_counts": dict(sorted(regime_counts.items())),
                        "market_regime_evidence_status": (
                            "AVAILABLE" if regime_counts else "INPUT_CAPABILITY_NOT_AVAILABLE"
                        ),
                        "market_regime_reason": (
                            None if regime_counts else REASON_REGIME_EVIDENCE_UNAVAILABLE
                        ),
                        "market_regime_evidence_missing_count": regime_evidence_missing,
                        "stage_capability_unavailable_count": stage_unavailable,
                    },
                    regime_count=sum(regime_counts.values()),
                    maturity_counts=maturity_counts,
                    reason_override=REASON_STAGE_CAPABILITY_UNAVAILABLE
                    if stage_unavailable and not daily
                    else None,
                    structural_metric=True,
                )
            )
        return tuple(output)

    def _rank_monotonicity(self, **kwargs: Any) -> tuple[_SliceEvidence, ...]:
        request: Phase0BCandidateQualityAuditRequestV1 = kwargs["request"]
        definition: Phase0BMetricDefinitionV1 = kwargs["definition"]
        contexts_by_date: Mapping[str, tuple[SignalContext, ...]] = kwargs["contexts_by_date"]
        output: list[_SliceEvidence] = []
        for horizon in kwargs["horizons"]:
            for stage in definition.stages:
                daily: dict[str, list[Decimal]] = defaultdict(list)
                inversions = 0
                valid_bucket_dates = 0
                total = matured = unavailable = 0
                stage_unavailable = 0
                bucket_values: dict[str, dict[str, list[Decimal]]] = defaultdict(
                    lambda: defaultdict(list)
                )
                for decision_date, contexts in contexts_by_date.items():
                    for context in contexts:
                        if context.stage_capability_by_stage.get(stage) != "FULL":
                            stage_unavailable += 1
                            continue
                        candidates = self._candidate_outcomes(
                            context=context,
                            stage=stage,
                            projection=definition.projection,
                            horizon=horizon,
                        )
                        total += len(candidates)
                        matured += sum(item.evaluable for item in candidates)
                        unavailable += sum(not item.evaluable for item in candidates)
                        bucket_means: list[Decimal] = []
                        complete = True
                        for start, end in ((1, 5), (6, 10), (11, 20)):
                            rows = tuple(item for item in candidates if start <= item.rank <= end)
                            if len(rows) != end - start + 1 or any(not item.evaluable for item in rows):
                                complete = False
                                break
                            bucket_means.append(
                                sum((item.value or Decimal(0) for item in rows), Decimal(0))
                                / Decimal(len(rows))
                            )
                        if not complete:
                            continue
                        for bucket_id, value in zip(("1-5", "6-10", "11-20"), bucket_means):
                            bucket_values[bucket_id][decision_date].append(value)
                        correlation = spearman(
                            (Decimal(1), Decimal(2), Decimal(3)),
                            tuple(bucket_means),
                        )
                        if correlation is not None:
                            daily[decision_date].append(-correlation)
                        inversions += sum(
                            bucket_means[index] < bucket_means[index + 1]
                            for index in range(len(bucket_means) - 1)
                        )
                        valid_bucket_dates += 1
                output.append(
                    self._result_from_daily(
                        request=request,
                        definition=definition,
                        slice_id=f"{definition.projection.value}:h{horizon}:{stage}:rank-monotonicity",
                        projection=definition.projection,
                        horizon=horizon,
                        stage=stage,
                        depth=20,
                        daily=daily,
                        total_dates=len(contexts_by_date),
                        candidate_count=total,
                        matured_count=matured,
                        unavailable_count=unavailable,
                        detail={
                            "adjacent_bucket_inversions": inversions,
                            "complete_bucket_signal_dates": valid_bucket_dates,
                            "bucket_observed": {
                                bucket_id: {
                                    "mean": str(
                                        quantize_metric(
                                            sum(daily_values, Decimal(0))
                                            / Decimal(len(daily_values))
                                        )
                                    ),
                                    "median": str(
                                        nearest_rank(daily_values, Decimal("0.50"))
                                    ),
                                    "decision_date_count": len(daily_values),
                                }
                                for bucket_id, date_values in sorted(bucket_values.items())
                                if (
                                    daily_values := [
                                        sum(values, Decimal(0)) / Decimal(len(values))
                                        for _date, values in sorted(date_values.items())
                                        if values
                                    ]
                                )
                            },
                            "stage_capability_unavailable_count": stage_unavailable,
                        },
                        reason_override=REASON_STAGE_CAPABILITY_UNAVAILABLE
                        if stage_unavailable and not daily
                        else None,
                        not_applicable=definition.projection in NON_PORTFOLIO_PROJECTIONS,
                    )
                )
        return tuple(output)

    def _stage_incremental(self, **kwargs: Any) -> tuple[_SliceEvidence, ...]:
        request: Phase0BCandidateQualityAuditRequestV1 = kwargs["request"]
        definition: Phase0BMetricDefinitionV1 = kwargs["definition"]
        contexts_by_date: Mapping[str, tuple[SignalContext, ...]] = kwargs["contexts_by_date"]
        output: list[_SliceEvidence] = []
        for horizon in kwargs["horizons"]:
            for left, right in zip(STAGE_ORDER, STAGE_ORDER[1:]):
                daily: dict[str, list[Decimal]] = defaultdict(list)
                overlap_values: list[Decimal] = []
                overlap_count_total = entered_count = exited_count = 0
                rank_deltas: list[Decimal] = []
                total = matured = unavailable = 0
                stage_unavailable = 0
                for decision_date, contexts in contexts_by_date.items():
                    for context in contexts:
                        if (
                            context.stage_capability_by_stage.get(left) != "FULL"
                            or context.stage_capability_by_stage.get(right) != "FULL"
                        ):
                            stage_unavailable += 1
                            continue
                        left_candidates = self._candidate_outcomes(
                            context=context,
                            stage=left,
                            projection=definition.projection,
                            horizon=horizon,
                        )
                        right_candidates = self._candidate_outcomes(
                            context=context,
                            stage=right,
                            projection=definition.projection,
                            horizon=horizon,
                        )
                        total += len(left_candidates) + len(right_candidates)
                        matured += sum(item.evaluable for item in left_candidates + right_candidates)
                        unavailable += sum(not item.evaluable for item in left_candidates + right_candidates)
                        left_value = fixed_k_portfolio(
                            candidates=left_candidates,
                            k=5,
                            projection=definition.projection,
                            benchmark_net_total_return=self._benchmark(left_candidates),
                        ).value
                        right_value = fixed_k_portfolio(
                            candidates=right_candidates,
                            k=5,
                            projection=definition.projection,
                            benchmark_net_total_return=self._benchmark(right_candidates),
                        ).value
                        if left_value is not None and right_value is not None:
                            daily[decision_date].append(right_value - left_value)
                        overlap, jaccard = stage_overlap(
                            left=tuple(item.symbol for item in left_candidates),
                            right=tuple(item.symbol for item in right_candidates),
                            k=5,
                        )
                        left_top = {item.symbol: item.rank for item in left_candidates if item.rank <= 5}
                        right_top = {item.symbol: item.rank for item in right_candidates if item.rank <= 5}
                        overlap_count_total += overlap
                        entered_count += len(set(right_top) - set(left_top))
                        exited_count += len(set(left_top) - set(right_top))
                        rank_deltas.extend(
                            Decimal(right_top[symbol] - left_top[symbol])
                            for symbol in sorted(set(left_top) & set(right_top))
                        )
                        overlap_values.append(jaccard)
                output.append(
                    self._result_from_daily(
                        request=request,
                        definition=definition,
                        slice_id=f"{definition.projection.value}:h{horizon}:{left}-to-{right}:k5",
                        projection=definition.projection,
                        horizon=horizon,
                        stage=f"{left}->{right}",
                        depth=5,
                        daily=daily,
                        total_dates=len(contexts_by_date),
                        candidate_count=total,
                        matured_count=matured,
                        unavailable_count=unavailable,
                        detail={
                            "mean_jaccard": str(
                                quantize_metric(
                                    sum(overlap_values, Decimal(0)) / Decimal(len(overlap_values))
                                )
                            )
                            if overlap_values
                            else None,
                            "overlap_count_total": overlap_count_total,
                            "entered_count": entered_count,
                            "exited_count": exited_count,
                            "mean_rank_delta": str(
                                quantize_metric(
                                    sum(rank_deltas, Decimal(0)) / Decimal(len(rank_deltas))
                                )
                            )
                            if rank_deltas
                            else None,
                            "stage_capability_unavailable_count": stage_unavailable,
                        },
                        reason_override=REASON_STAGE_CAPABILITY_UNAVAILABLE
                        if stage_unavailable and not daily
                        else None,
                        not_applicable=definition.projection in NON_PORTFOLIO_PROJECTIONS,
                    )
                )
        return tuple(output)

    def _random5(self, **kwargs: Any) -> tuple[_SliceEvidence, ...]:
        request: Phase0BCandidateQualityAuditRequestV1 = kwargs["request"]
        definition: Phase0BMetricDefinitionV1 = kwargs["definition"]
        contexts_by_date: Mapping[str, tuple[SignalContext, ...]] = kwargs["contexts_by_date"]
        output: list[_SliceEvidence] = []
        for horizon in kwargs["horizons"]:
            for depth in definition.depths:
                daily: dict[str, list[Decimal]] = defaultdict(list)
                evaluable_replicates = total_replicates = 0
                candidate_count = matured = unavailable = 0
                stage_unavailable = 0
                selection_percentiles: list[Decimal] = []
                selection_stage_unavailable = 0
                distribution_by_signal_date: list[dict[str, Any]] = []
                for decision_date, contexts in contexts_by_date.items():
                    for context in contexts:
                        if context.stage_capability_by_stage.get("alpha_raw") != "FULL":
                            stage_unavailable += 1
                            continue
                        candidates = tuple(
                            item
                            for item in self._candidate_outcomes(
                                context=context,
                                stage="alpha_raw",
                                projection=definition.projection,
                                horizon=horizon,
                            )
                            if item.rank <= depth
                        )
                        by_symbol = {item.symbol: item for item in candidates}
                        candidate_count += len(candidates)
                        matured += sum(item.evaluable for item in candidates)
                        unavailable += sum(not item.evaluable for item in candidates)
                        seed = hashlib.sha256(
                            (
                                str(request.request_hash)
                                + context.snapshot_id
                                + context.signal_id
                                + definition.projection.value
                                + str(horizon)
                            ).encode("utf-8")
                        ).digest()
                        replicate_values: list[Decimal] = []
                        selection_percentile: Decimal | None = None
                        for replicate in range(request.random_replicates):
                            total_replicates += 1
                            symbols = random5_symbols(
                                seed=seed,
                                replicate_no=replicate,
                                symbols=tuple(by_symbol),
                            )
                            sample = tuple(
                                CandidateOutcome(
                                    symbol=symbol,
                                    rank=index,
                                    value=by_symbol[symbol].value,
                                    maturity_status=by_symbol[symbol].maturity_status,
                                    outcome_event_status=by_symbol[symbol].outcome_event_status,
                                    benchmark_net_total_return=(
                                        by_symbol[symbol].benchmark_net_total_return
                                    ),
                                )
                                for index, symbol in enumerate(symbols, start=1)
                            )
                            result = fixed_k_portfolio(
                                candidates=sample,
                                k=5,
                                projection=definition.projection,
                                benchmark_net_total_return=self._benchmark(candidates),
                            )
                            if result.value is not None:
                                evaluable_replicates += 1
                                replicate_values.append(result.value)
                        if replicate_values:
                            daily[decision_date].append(
                                sum(replicate_values, Decimal(0)) / Decimal(len(replicate_values))
                            )
                            selection_candidates = self._candidate_outcomes(
                                context=context,
                                stage="selection_effective",
                                projection=definition.projection,
                                horizon=horizon,
                            )
                            if context.stage_capability_by_stage.get("selection_effective") == "FULL":
                                selection_value = fixed_k_portfolio(
                                    candidates=selection_candidates,
                                    k=5,
                                    projection=definition.projection,
                                    benchmark_net_total_return=self._benchmark(selection_candidates),
                                ).value
                                if selection_value is not None:
                                    less_or_equal = sum(
                                        value <= selection_value for value in replicate_values
                                    )
                                    selection_percentile = (
                                        Decimal(less_or_equal) / Decimal(len(replicate_values))
                                    )
                                    selection_percentiles.append(selection_percentile)
                            else:
                                selection_stage_unavailable += 1
                            distribution_by_signal_date.append(
                                {
                                    "decision_date": decision_date,
                                    "signal_id": context.signal_id,
                                    "evaluable_replicates": len(replicate_values),
                                    "p05": str(nearest_rank(replicate_values, Decimal("0.05"))),
                                    "p50": str(nearest_rank(replicate_values, Decimal("0.50"))),
                                    "p95": str(nearest_rank(replicate_values, Decimal("0.95"))),
                                    "selection_percentile": str(selection_percentile)
                                    if selection_percentile is not None
                                    else None,
                                }
                            )
                output.append(
                    self._result_from_daily(
                        request=request,
                        definition=definition,
                        slice_id=f"{definition.projection.value}:h{horizon}:random5:d{depth}",
                        projection=definition.projection,
                        horizon=horizon,
                        stage="alpha_raw",
                        depth=depth,
                        daily=daily,
                        total_dates=len(contexts_by_date),
                        candidate_count=candidate_count,
                        matured_count=matured,
                        unavailable_count=unavailable,
                        detail={
                            "evaluable_replicates": evaluable_replicates,
                            "total_replicates": total_replicates,
                            "p05": str(nearest_rank(daily_random_means, Decimal("0.05")))
                            if (
                                daily_random_means := [
                                    sum(values, Decimal(0)) / Decimal(len(values))
                                    for _date, values in sorted(daily.items())
                                    if values
                                ]
                            )
                            else None,
                            "p50": str(nearest_rank(daily_random_means, Decimal("0.50")))
                            if daily_random_means
                            else None,
                            "p95": str(nearest_rank(daily_random_means, Decimal("0.95")))
                            if daily_random_means
                            else None,
                            "selection_percentile_mean": str(
                                quantize_metric(
                                    sum(selection_percentiles, Decimal(0))
                                    / Decimal(len(selection_percentiles))
                                )
                            ) if selection_percentiles else None,
                            "stage_capability_unavailable_count": stage_unavailable,
                            "selection_stage_capability_unavailable_count": (
                                selection_stage_unavailable
                            ),
                            "distribution_by_signal_date": distribution_by_signal_date,
                        },
                        reason_override=REASON_STAGE_CAPABILITY_UNAVAILABLE
                        if stage_unavailable and not daily
                        else None,
                        not_applicable=definition.projection in NON_PORTFOLIO_PROJECTIONS,
                    )
                )
        return tuple(output)

    def _winner_definition(
        self,
        *,
        request: Phase0BCandidateQualityAuditRequestV1,
        definition: Phase0BMetricDefinitionV1,
        winner_definition_ids: tuple[str, ...],
        projection: Projection,
        horizon: int,
    ) -> Phase1WinnerDefinitionV1:
        winners = tuple(
            item
            for item in request.winner_definitions
            if item.winner_definition_id in winner_definition_ids
            and item.projection == projection.value
            and item.horizon_trade_days == horizon
        )
        if len(winners) != 1:
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "winner metric slice does not close to one frozen definition",
                context={"metric_id": definition.metric_id, "horizon": horizon},
            )
        return winners[0]

    def _precision(self, **kwargs: Any) -> tuple[_SliceEvidence, ...]:
        request: Phase0BCandidateQualityAuditRequestV1 = kwargs["request"]
        definition: Phase0BMetricDefinitionV1 = kwargs["definition"]
        contexts_by_date: Mapping[str, tuple[SignalContext, ...]] = kwargs["contexts_by_date"]
        output: list[_SliceEvidence] = []
        for horizon in kwargs["horizons"]:
            winner = self._winner_definition(
                request=request,
                definition=definition,
                winner_definition_ids=kwargs["winner_definition_ids"],
                projection=definition.projection,
                horizon=horizon,
            )
            daily: dict[str, list[Decimal]] = defaultdict(list)
            total = matured = unavailable = 0
            stage_unavailable = 0
            for decision_date, contexts in contexts_by_date.items():
                for context in contexts:
                    if context.stage_capability_by_stage.get("selection_effective") != "FULL":
                        stage_unavailable += 1
                        continue
                    candidates = self._candidate_outcomes(
                        context=context,
                        stage="selection_effective",
                        projection=definition.projection,
                        horizon=horizon,
                    )
                    is_winner = {
                        item.symbol: winner.matches(item.value)
                        for item in candidates
                        if item.value is not None and item.evaluable
                    }
                    value = precision_at_k(selected=candidates, k=5, is_winner=is_winner)
                    total += len(candidates)
                    matured += sum(item.evaluable for item in candidates)
                    unavailable += sum(not item.evaluable for item in candidates)
                    if value is not None:
                        daily[decision_date].append(value)
            output.append(
                self._result_from_daily(
                    request=request,
                    definition=definition,
                    slice_id=f"{definition.projection.value}:h{horizon}:precision@5:{winner.winner_definition_id}",
                    projection=definition.projection,
                    horizon=horizon,
                    stage="selection_effective",
                    depth=5,
                    daily=daily,
                    total_dates=len(contexts_by_date),
                    candidate_count=total,
                    matured_count=matured,
                    unavailable_count=unavailable,
                    detail={
                        "winner_definition_hash": winner.winner_definition_hash,
                        "stage_capability_unavailable_count": stage_unavailable,
                    },
                    reason_override=REASON_STAGE_CAPABILITY_UNAVAILABLE
                    if stage_unavailable and not daily
                    else None,
                )
            )
        return tuple(output)

    def _ndcg(self, **kwargs: Any) -> tuple[_SliceEvidence, ...]:
        request: Phase0BCandidateQualityAuditRequestV1 = kwargs["request"]
        definition: Phase0BMetricDefinitionV1 = kwargs["definition"]
        contexts_by_date: Mapping[str, tuple[SignalContext, ...]] = kwargs["contexts_by_date"]
        output: list[_SliceEvidence] = []
        for horizon in kwargs["horizons"]:
            daily: dict[str, list[Decimal]] = defaultdict(list)
            total = matured = unavailable = 0
            stage_unavailable = 0
            for decision_date, contexts in contexts_by_date.items():
                for context in contexts:
                    if (
                        context.stage_capability_by_stage.get("alpha_raw") != "FULL"
                        or context.stage_capability_by_stage.get("selection_effective") != "FULL"
                    ):
                        stage_unavailable += 1
                        continue
                    candidate_pool = self._candidate_outcomes(
                        context=context,
                        stage="alpha_raw",
                        projection=definition.projection,
                        horizon=horizon,
                    )
                    candidate_pool = tuple(
                        item for item in candidate_pool if item.rank <= max(request.candidate_depths)
                    )
                    selected = self._candidate_outcomes(
                        context=context,
                        stage="selection_effective",
                        projection=definition.projection,
                        horizon=horizon,
                    )
                    gains = {item.symbol: item.value if item.evaluable else None for item in candidate_pool}
                    value = ndcg_at_k(selected=selected, candidate_gains=gains, k=5)
                    total += len(candidate_pool)
                    matured += sum(item.evaluable for item in candidate_pool)
                    unavailable += sum(not item.evaluable for item in candidate_pool)
                    if value is not None:
                        daily[decision_date].append(value)
            output.append(
                self._result_from_daily(
                    request=request,
                    definition=definition,
                    slice_id=f"{definition.projection.value}:h{horizon}:ndcg@5",
                    projection=definition.projection,
                    horizon=horizon,
                    stage="selection_effective",
                    depth=5,
                    daily=daily,
                    total_dates=len(contexts_by_date),
                    candidate_count=total,
                    matured_count=matured,
                    unavailable_count=unavailable,
                    detail={"stage_capability_unavailable_count": stage_unavailable},
                    reason_override=REASON_STAGE_CAPABILITY_UNAVAILABLE
                    if stage_unavailable and not daily
                    else None,
                )
            )
        return tuple(output)

    def _strategy_recall(self, **kwargs: Any) -> tuple[_SliceEvidence, ...]:
        return self._recall(denominator="UNIVERSE", **kwargs)

    def _conditional_recall(self, **kwargs: Any) -> tuple[_SliceEvidence, ...]:
        return self._recall(denominator="CANDIDATE", **kwargs)

    def _recall(self, *, denominator: str, **kwargs: Any) -> tuple[_SliceEvidence, ...]:
        request: Phase0BCandidateQualityAuditRequestV1 = kwargs["request"]
        definition: Phase0BMetricDefinitionV1 = kwargs["definition"]
        contexts_by_date: Mapping[str, tuple[SignalContext, ...]] = kwargs["contexts_by_date"]
        output: list[_SliceEvidence] = []
        for horizon in kwargs["horizons"]:
            winner = self._winner_definition(
                request=request,
                definition=definition,
                winner_definition_ids=kwargs["winner_definition_ids"],
                projection=definition.projection,
                horizon=horizon,
            )
            for depth in definition.depths:
                daily: dict[str, list[Decimal]] = defaultdict(list)
                winner_events = no_winner_dates = total = matured = unavailable = 0
                stage_unavailable = 0
                universe_policy_hashes: set[str] = set()
                source_revision_set_hashes: set[str] = set()
                for decision_date, contexts in contexts_by_date.items():
                    for context in contexts:
                        required_stages = ("selection_effective",)
                        if denominator == "CANDIDATE":
                            required_stages += ("alpha_raw",)
                        if any(
                            context.stage_capability_by_stage.get(stage) != "FULL"
                            for stage in required_stages
                        ):
                            stage_unavailable += 1
                            continue
                        universe_policy_hashes.add(context.universe_policy_hash)
                        selected = self._candidate_outcomes(
                            context=context,
                            stage="selection_effective",
                            projection=definition.projection,
                            horizon=horizon,
                        )
                        if denominator == "UNIVERSE":
                            scoped_rows = tuple(
                                item
                                for item in context.universe_outcomes
                                if str(item.get("projection")) == definition.projection.value
                                and int(item.get("horizon_trading_days", -1)) == horizon
                                and str(item.get("label_policy_hash")) == winner.label_policy_hash
                                and str(item.get("universe_layer"))
                                == winner.denominator_universe_layer
                            )
                            denominator_rows = tuple(
                                item
                                for item in scoped_rows
                                if str(item.get("maturity_status")) == "MATURED"
                                and str(item.get("outcome_event_status")) == "TERMINAL"
                                and item.get("projection_value_decimal") is not None
                            )
                            denominator_winners = tuple(
                                str(item["symbol"])
                                for item in denominator_rows
                                if winner.matches(
                                    self._required_decimal(
                                        item["projection_value_decimal"],
                                        field_name="projection_value_decimal",
                                    )
                                )
                            )
                            total += len(scoped_rows)
                            matured += len(denominator_rows)
                            unavailable += len(scoped_rows) - len(denominator_rows)
                            universe_policy_hashes.update(
                                (context.universe_policy_hash,)
                            )
                            source_revision_set_hashes.update(
                                str(item["label_source_revision_set_hash"])
                                for item in scoped_rows
                                if item.get("label_source_revision_set_hash") is not None
                            )
                        else:
                            alpha_rows = tuple(
                                item
                                for item in context.candidates_by_stage.get("alpha_raw", ())
                                if item.get("membership_status") == "INCLUDED"
                                and item.get("rank") is not None
                                and int(item["rank"]) <= max(request.candidate_depths)
                            )
                            for candidate in alpha_rows:
                                outcome = self._matching_outcome(
                                    context=context,
                                    candidate=candidate,
                                    projection=definition.projection,
                                    horizon=horizon,
                                )
                                if outcome is None:
                                    continue
                                if outcome.get("universe_policy_hash") is not None:
                                    universe_policy_hashes.add(str(outcome["universe_policy_hash"]))
                                source_hash = outcome.get(
                                    "label_source_revision_set_hash",
                                    outcome.get("source_revision_set_hash"),
                                )
                                if source_hash is not None:
                                    source_revision_set_hashes.add(str(source_hash))
                            pool = tuple(
                                item
                                for item in self._candidate_outcomes(
                                    context=context,
                                    stage="alpha_raw",
                                    projection=definition.projection,
                                    horizon=horizon,
                                )
                                if item.rank <= max(request.candidate_depths)
                            )
                            denominator_winners = tuple(
                                item.symbol
                                for item in pool
                                if item.evaluable
                                and item.value is not None
                                and winner.matches(item.value)
                            )
                            total += len(pool)
                            matured += sum(item.evaluable for item in pool)
                            unavailable += sum(not item.evaluable for item in pool)
                        recall = recall_at_k(
                            selected_symbols=tuple(item.symbol for item in selected),
                            denominator_winners=denominator_winners,
                            k=depth,
                        )
                        winner_events += recall.denominator_winner_count
                        no_winner_dates += int(recall.no_winner)
                        if recall.recall is not None:
                            daily[decision_date].append(recall.recall)
                reason_override = None
                if stage_unavailable and total == 0:
                    reason_override = REASON_STAGE_CAPABILITY_UNAVAILABLE
                elif denominator == "UNIVERSE" and total == 0:
                    reason_override = REASON_UNIVERSE_OUTCOME_UNAVAILABLE
                elif winner_events < request.multiple_testing_registry.minimum_recall_winner_events:
                    reason_override = REASON_INSUFFICIENT_WINNER_EVENTS
                ordered_universe_policy_hashes = tuple(sorted(universe_policy_hashes))
                ordered_source_revision_hashes = tuple(sorted(source_revision_set_hashes))
                output.append(
                    self._result_from_daily(
                        request=request,
                        definition=definition,
                        slice_id=(
                            f"{definition.projection.value}:h{horizon}:"
                            f"{denominator.lower()}-recall@{depth}:{winner.winner_definition_id}"
                        ),
                        projection=definition.projection,
                        horizon=horizon,
                        stage="selection_effective",
                        depth=depth,
                        daily=daily,
                        total_dates=len(contexts_by_date),
                        candidate_count=total,
                        matured_count=matured,
                        unavailable_count=unavailable,
                        detail={
                            "winner_definition_hash": winner.winner_definition_hash,
                            "winner_event_count": winner_events,
                            "no_winner_date_count": no_winner_dates,
                            "denominator_universe_layer": winner.denominator_universe_layer,
                            "universe_policy_hashes": ordered_universe_policy_hashes,
                            "universe_policy_set_hash": canonical_json_sha256(
                                ordered_universe_policy_hashes
                            )
                            if ordered_universe_policy_hashes
                            else None,
                            "source_revision_set_hash": (
                                ordered_source_revision_hashes[0]
                                if len(ordered_source_revision_hashes) == 1
                                else None
                            ),
                            "source_revision_set_hashes": ordered_source_revision_hashes,
                            "source_revision_set_hash_set_hash": canonical_json_sha256(
                                ordered_source_revision_hashes
                            )
                            if ordered_source_revision_hashes
                            else None,
                            "stage_capability_unavailable_count": stage_unavailable,
                        },
                        winner_event_count=winner_events,
                        reason_override=reason_override,
                    )
                )
        return tuple(output)

    def _blacklist(self, **kwargs: Any) -> tuple[_SliceEvidence, ...]:
        request: Phase0BCandidateQualityAuditRequestV1 = kwargs["request"]
        definition: Phase0BMetricDefinitionV1 = kwargs["definition"]
        contexts_by_date: Mapping[str, tuple[SignalContext, ...]] = kwargs["contexts_by_date"]
        output: list[_SliceEvidence] = []
        for horizon in kwargs["horizons"]:
            daily: dict[str, list[Decimal]] = defaultdict(list)
            excluded = matured = unavailable = 0
            stage_unavailable = 0
            excluded_symbols: set[str] = set()
            reason_counts: dict[str, int] = defaultdict(int)
            industry_counts: dict[str, int] = defaultdict(int)
            maturity_counts: dict[str, int] = defaultdict(int)
            input_ranks: list[int] = []
            for decision_date, contexts in contexts_by_date.items():
                for context in contexts:
                    if context.stage_capability_by_stage.get("risk_policy_adjusted") != "FULL":
                        stage_unavailable += 1
                        continue
                    risk_candidates = context.candidates_by_stage.get("risk_policy_adjusted", ())
                    values: list[Decimal] = []
                    for candidate in risk_candidates:
                        reason = str(candidate.get("exclusion_reason_code") or "").upper()
                        component = candidate.get("component_evidence_json")
                        explicitly_blacklisted = "BLACKLIST" in reason or (
                            isinstance(component, dict)
                            and bool(component.get("industry_blacklist_excluded"))
                        )
                        if not explicitly_blacklisted:
                            continue
                        excluded += 1
                        excluded_symbols.add(str(candidate["symbol"]))
                        reason_counts[reason or "EXPLICIT_COMPONENT_BLACKLIST"] += 1
                        if candidate.get("input_rank") is not None:
                            input_ranks.append(int(candidate["input_rank"]))
                        if isinstance(component, dict):
                            industry = component.get("industry_at_t", component.get("industry"))
                            if industry is not None:
                                industry_counts[str(industry)] += 1
                        outcome = self._matching_outcome(
                            context=context,
                            candidate=candidate,
                            projection=definition.projection,
                            horizon=horizon,
                        )
                        if (
                            outcome is not None
                            and outcome.get("maturity_status") == "MATURED"
                            and outcome.get("outcome_event_status") == "TERMINAL"
                            and outcome.get("projection_value_decimal") is not None
                        ):
                            matured += 1
                            maturity_counts["MATURED_TERMINAL"] += 1
                            parsed_value = self._required_decimal(
                                outcome["projection_value_decimal"],
                                field_name="projection_value_decimal",
                            )
                            values.append(parsed_value)
                        else:
                            unavailable += 1
                            maturity_counts[
                                str(outcome.get("maturity_status")) if outcome is not None else "MISSING"
                            ] += 1
                    if values:
                        daily[decision_date].append(sum(values, Decimal(0)) / Decimal(len(values)))
            output.append(
                self._result_from_daily(
                    request=request,
                    definition=definition,
                    slice_id=f"{definition.projection.value}:h{horizon}:blacklist-exclusion-diagnostic",
                    projection=definition.projection,
                    horizon=horizon,
                    stage="risk_policy_adjusted",
                    depth=None,
                    daily=daily,
                    total_dates=len(contexts_by_date),
                    candidate_count=excluded,
                    matured_count=matured,
                    unavailable_count=unavailable,
                    detail={
                        "counterfactual_status": "INPUT_CAPABILITY_NOT_AVAILABLE",
                        "counterfactual_reason": REASON_BLACKLIST_COUNTERFACTUAL_UNAVAILABLE,
                        "stage_capability_unavailable_count": stage_unavailable,
                        "excluded_symbol_count": len(excluded_symbols),
                        "excluded_decision_date_count": len(daily),
                        "exclusion_reason_counts": dict(sorted(reason_counts.items())),
                        "industry_counts": dict(sorted(industry_counts.items())),
                        "maturity_counts": dict(sorted(maturity_counts.items())),
                        "input_rank_observed_count": len(input_ranks),
                        "input_rank_mean": str(
                            quantize_metric(
                                Decimal(sum(input_ranks)) / Decimal(len(input_ranks))
                            )
                        )
                        if input_ranks
                        else None,
                    },
                    maturity_counts=maturity_counts,
                    reason_override=(
                        REASON_STAGE_CAPABILITY_UNAVAILABLE
                        if stage_unavailable and excluded == 0
                        else REASON_BLACKLIST_COUNTERFACTUAL_UNAVAILABLE
                        if excluded == 0
                        else None
                    ),
                )
            )
        return tuple(output)

    @staticmethod
    def _result_from_daily(
        *,
        request: Phase0BCandidateQualityAuditRequestV1,
        definition: Phase0BMetricDefinitionV1,
        slice_id: str,
        projection: Projection,
        horizon: int,
        stage: str | None,
        depth: int | None,
        daily: Mapping[str, Sequence[Decimal]],
        total_dates: int,
        candidate_count: int,
        matured_count: int,
        unavailable_count: int,
        detail: Mapping[str, Any] | None = None,
        winner_event_count: int = 0,
        regime_count: int = 0,
        maturity_counts: Mapping[str, int] | None = None,
        reason_override: str | None = None,
        not_applicable: bool = False,
        structural_metric: bool = False,
    ) -> _SliceEvidence:
        daily_collapsed = {
            date: (sum(values, Decimal(0)) / Decimal(len(values)),)
            for date, values in daily.items()
            if values
        }
        observed = equal_weight_by_decision_date(daily_collapsed)
        daily_series = tuple(
            (decision_date, values[0])
            for decision_date, values in sorted(daily_collapsed.items())
        )
        values = tuple(value for _decision_date, value in daily_series)
        if not_applicable:
            status = MetricStatus.NOT_APPLICABLE
            reasons = (REASON_PROJECTION_NOT_APPLICABLE,)
            observed = None
            daily_series = ()
            values = ()
        elif reason_override is not None:
            status = (
                MetricStatus.INSUFFICIENT_SAMPLE
                if reason_override
                in {
                    REASON_INSUFFICIENT_DECISION_DATES,
                    REASON_INSUFFICIENT_MATURE_LABELS,
                    REASON_INSUFFICIENT_PAIRED_DECISION_DATES,
                    REASON_INSUFFICIENT_WINNER_EVENTS,
                }
                else MetricStatus.INPUT_CAPABILITY_NOT_AVAILABLE
            )
            reasons = (reason_override,)
        elif observed is None:
            status = MetricStatus.INPUT_CAPABILITY_NOT_AVAILABLE
            reasons = (REASON_INSUFFICIENT_MATURE_LABELS,)
        elif structural_metric:
            status = MetricStatus.AVAILABLE
            reasons = ()
        elif len(values) < 60:
            status = MetricStatus.INSUFFICIENT_SAMPLE
            reasons = (REASON_INSUFFICIENT_DECISION_DATES,)
        else:
            status = MetricStatus.AVAILABLE
            reasons = ()
        confidence_lower = confidence_upper = None
        if status is MetricStatus.AVAILABLE and not structural_metric:
            interval = stationary_bootstrap_mean_interval(
                values=values,
                registry_hash=str(request.multiple_testing_registry_hash),
                replicates=request.stationary_bootstrap_replicates,
            )
            confidence_lower = interval.lower
            confidence_upper = interval.upper
        result = Phase0BMetricResultV1(
            metric_definition_id=definition.metric_id,
            metric_definition_hash=str(definition.metric_hash),
            slice_id=f"{definition.metric_id}:{slice_id}",
            projection=projection.value,
            horizon_trading_days=horizon,
            stage=stage,
            depth=depth,
            status=status,
            reason_codes=tuple(sorted(reasons)),
            decision_date_count=total_dates,
            evaluable_date_count=len(values),
            effective_sample_count=len(values),
            missing_decision_date_count=max(0, total_dates - len(values)),
            candidate_count=candidate_count,
            matured_label_count=matured_count,
            unavailable_label_count=unavailable_count,
            winner_event_count=winner_event_count,
            regime_count=regime_count,
            maturity_counts_json=canonical_json_text(
                dict(
                    maturity_counts
                    or {
                        "MATURED_TERMINAL": matured_count,
                        "UNAVAILABLE_OR_NON_EVALUABLE": unavailable_count,
                    }
                )
            ),
            observed_value=observed,
            conclusion=(
                "DESCRIPTIVE_ESTIMATE_AVAILABLE"
                if status is MetricStatus.AVAILABLE and not structural_metric
                else None
            ),
            conclusion_scope=(
                "DESCRIPTIVE"
                if status is MetricStatus.AVAILABLE and not structural_metric
                else None
            ),
            confidence_interval_lower=confidence_lower,
            confidence_interval_upper=confidence_upper,
            detail_json=canonical_json_text(dict(detail or {})),
        )
        return _SliceEvidence(result=result, daily_series=daily_series)

    @staticmethod
    def _one_sided_bootstrap_p_value(
        *,
        values: Sequence[Decimal],
        registry_hash: str,
        replicates: int,
    ) -> Decimal:
        mean = sum(values, Decimal(0)) / Decimal(len(values))
        centered = tuple(value - mean for value in values)
        exceedances = sum(
            sum((centered[index] for index in sample), Decimal(0)) / Decimal(len(sample))
            >= mean
            for sample in iter_stationary_bootstrap_indices(
                sample_size=len(values),
                replicates=replicates,
                registry_hash=registry_hash,
            )
        )
        return quantize_metric(Decimal(exceedances + 1) / Decimal(replicates + 1))

    def _apply_multiple_testing(
        self,
        *,
        request: Phase0BCandidateQualityAuditRequestV1,
        evidence: tuple[_SliceEvidence, ...],
    ) -> tuple[Phase0BMetricResultV1, ...]:
        by_slice = {item.result.slice_id: item for item in evidence}
        if len(by_slice) != len(evidence):
            raise Phase0BAuditError(
                REASON_METRIC_REGISTRY_CONFLICT,
                "metric engine generated duplicate slice identities",
            )
        inferential = {
            key: item
            for key, item in by_slice.items()
            if len(item.daily_values) >= request.multiple_testing_registry.minimum_inferential_dates
            and item.result.conclusion_scope == "DESCRIPTIVE"
        }
        p_values: dict[str, Decimal] = {}
        adjustment_by_slice: dict[str, str] = {}
        additional_reasons: dict[str, set[str]] = defaultdict(set)
        for key, item in by_slice.items():
            if (
                item.result.status is MetricStatus.AVAILABLE
                and item.result.conclusion_scope == "DESCRIPTIVE"
                and len(item.daily_values)
                < request.multiple_testing_registry.minimum_inferential_dates
            ):
                additional_reasons[key].add(REASON_INSUFFICIENT_DECISION_DATES)
        primary_ids = set(request.multiple_testing_registry.primary_metric_family)
        primary_groups: dict[
            tuple[str | None, int | None, int | None, str | None, str],
            dict[str, _SliceEvidence],
        ] = defaultdict(dict)
        for key, item in inferential.items():
            if item.result.metric_definition_id in primary_ids:
                primary_groups[
                    (
                        item.result.projection,
                        item.result.horizon_trading_days,
                        item.result.depth,
                        item.result.regime_definition_id,
                        canonical_json_text(
                            {
                                "comparison": json.loads(item.result.detail_json).get(
                                    "comparison",
                                    "DEFAULT",
                                ),
                                "candidate_pool_depth": json.loads(
                                    item.result.detail_json
                                ).get("candidate_pool_depth"),
                            }
                        ),
                    )
                ][key] = item
        for group in primary_groups.values():
            date_sets = [
                {decision_date for decision_date, _value in item.daily_series}
                for item in group.values()
            ]
            paired_dates = set.intersection(*date_sets) if date_sets else set()
            if len(paired_dates) < request.multiple_testing_registry.minimum_inferential_dates:
                for key in group:
                    additional_reasons[key].add(REASON_INSUFFICIENT_PAIRED_DECISION_DATES)
                continue
            performance_by_model = {
                key: tuple(
                    value
                    for decision_date, value in item.daily_series
                    if decision_date in paired_dates
                )
                for key, item in group.items()
            }
            if performance_by_model:
                spa = hansen_spa_p_value(
                    performance_by_model=performance_by_model,
                    registry_hash=str(request.multiple_testing_registry_hash),
                    replicates=request.stationary_bootstrap_replicates,
                )
                for key in group:
                    p_values[key] = spa
                    adjustment_by_slice[key] = request.multiple_testing_registry.spa_policy.policy_id
        diagnostics = {
            key: item
            for key, item in inferential.items()
            if item.result.metric_definition_id
            in set(request.multiple_testing_registry.diagnostic_metric_families)
        }
        raw_diagnostic = {
            key: self._one_sided_bootstrap_p_value(
                values=item.daily_values,
                registry_hash=canonical_json_sha256(
                    {"registry": request.multiple_testing_registry_hash, "slice": key}
                ),
                replicates=request.stationary_bootstrap_replicates,
            )
            for key, item in diagnostics.items()
        }
        adjusted_diagnostic = benjamini_yekutieli_adjusted(raw_diagnostic)
        p_values.update(adjusted_diagnostic)
        adjustment_by_slice.update(
            {
                key: request.multiple_testing_registry.by_fdr_policy.policy_id
                for key in adjusted_diagnostic
            }
        )
        economic_thresholds = {
            item.metric_family: item.minimum_absolute_effect
            for item in request.multiple_testing_registry.economic_significance_policy.thresholds
        }
        output: list[Phase0BMetricResultV1] = []
        for key, item in sorted(by_slice.items()):
            result = item.result
            p_value = p_values.get(key)
            conclusion = result.conclusion
            conclusion_scope = result.conclusion_scope
            threshold = economic_thresholds.get(result.metric_definition_id)
            if p_value is not None:
                statistical_threshold = (
                    Decimal(request.multiple_testing_registry.spa_policy.alpha)
                    if result.metric_definition_id in primary_ids
                    else Decimal(request.multiple_testing_registry.by_fdr_policy.q)
                )
                statistically_significant = p_value <= statistical_threshold
                economically_significant = (
                    threshold is None
                    or (
                        result.observed_value is not None
                        and result.observed_value >= threshold
                    )
                )
                conclusion = (
                    "POSITIVE_EVIDENCE"
                    if statistically_significant and economically_significant
                    else "NO_POSITIVE_EVIDENCE"
                )
                conclusion_scope = "INFERENTIAL"
            detail = json.loads(result.detail_json)
            detail.update(
                {
                    "multiple_testing_adjustment": adjustment_by_slice.get(key),
                    "economic_significance_minimum_effect": (
                        str(threshold) if threshold is not None else None
                    ),
                }
            )
            payload = result.model_dump(mode="python")
            payload.update(
                {
                    "p_value": p_value,
                    "conclusion": conclusion,
                    "conclusion_scope": conclusion_scope,
                    "reason_codes": tuple(
                        sorted(set(result.reason_codes) | additional_reasons.get(key, set()))
                    ),
                    "detail_json": canonical_json_text(detail),
                    "result_hash": None,
                }
            )
            output.append(Phase0BMetricResultV1.model_validate(payload))
        return tuple(output)

    @staticmethod
    def _package_conclusion(
        *,
        request: Phase0BCandidateQualityAuditRequestV1,
        results: tuple[Phase0BMetricResultV1, ...],
        decision_date_count: int,
    ) -> str | None:
        if decision_date_count < request.multiple_testing_registry.minimum_descriptive_dates:
            return None
        primary = tuple(
            item
            for item in results
            if item.metric_definition_id == "stage-topk-point-estimate-v1"
            and item.stage == "selection_effective"
            and item.depth == 5
            and item.regime_definition_id is None
            and json.loads(item.detail_json).get("comparison") == "POINT_ESTIMATE"
        )
        if not primary or any(
            item.status is not MetricStatus.AVAILABLE or item.conclusion is None
            for item in primary
        ):
            return "RESEARCH_EVIDENCE_UNAVAILABLE"
        return "RESEARCH_EVIDENCE_AVAILABLE"
