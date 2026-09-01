from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from statistics import mean, stdev
from typing import Any, Iterable, Literal, Sequence

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import canonical_json_sha256

from backend.services.advisory_modeling.dataset_spool import RerankerDatasetSpool
from backend.services.advisory_modeling.errors import (
    AdvisoryModelingError,
    REASON_FEATURE_CLOSURE_INCOMPLETE,
)

from backend.services.advisory_modeling.feature_schema import (
    FeatureFormulaDefinitionV1,
    FeatureFormulaRegistryV1,
    FeatureRowIdentityV1,
    feature_payload_hash,
    frozen_feature_schema_v1,
)
from backend.services.advisory_modeling.identity import FrozenModel, validated_hash


MONEYFLOW_AMOUNT_MULTIPLIER = Decimal("10000")


def frozen_formula_registry_v1() -> FeatureFormulaRegistryV1:
    """Return the complete, canonical v1 formula payload used by export and inference."""

    formulas = (
        FeatureFormulaDefinitionV1(
            formula_id="frozen_evidence_projection_v1",
            formula_version="1",
            expression="value := exact frozen base/source evidence field",
            input_roles=("sealed_base_snapshot", "frozen_feature_source_partition"),
            parameters={"transformation": "identity", "imputation": "forbidden"},
            pit_constraints=(
                "identity, rank, score, HMM, risk and mapping fields retain their frozen values",
            ),
            missing_behavior="required evidence fails the group; optional evidence keeps null plus flag",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="candidate_rank_percentile_v1",
            formula_version="1",
            expression="n > 1 ? (n - rank) / (n - 1) : 0.5",
            input_roles=("same_day_stage_rank", "same_day_stage_candidate_count"),
            parameters={"single_candidate_value": "0.5", "rank_base": 1},
            pit_constraints=("base snapshot only", "computed independently for each stage"),
            missing_behavior="required rank or candidate count fails the ranking group",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="candidate_score_gap_v1",
            formula_version="1",
            expression="score - previous_rank_score; score - next_rank_score",
            input_roles=("same_day_stage_score", "same_day_stage_summary"),
            parameters={"boundary_value": None},
            pit_constraints=("score direction and tie policy come from the same stage summary",),
            missing_behavior="boundary gaps are null with an explicit missing indicator",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="multi_alpha_consensus_v1",
            formula_version="1",
            expression=(
                "weighted_mean, weighted_std, sign_agreement, "
                "max(abs(weight*score))/sum(abs(weight*score))"
            ),
            input_roles=("frozen_leg_score", "frozen_leg_weight", "component_evidence_json"),
            parameters={"weight_normalization": "sum_positive_weight"},
            pit_constraints=("leg, weight, model and asset identities come from base snapshot",),
            missing_behavior="missing component evidence fails the ranking group",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="adjusted_return_v1",
            formula_version="1",
            expression="adj_close_T / adj_close_T_minus_h - 1",
            input_roles=("raw_close", "adj_factor", "trading_calendar"),
            parameters={"horizons": [1, 3, 5, 10, 20, 60]},
            pit_constraints=(
                "close and adj_factor business dates do not exceed decision_as_of_trade_date",
                "source revision admissibility is RETROSPECTIVE_DB_CONTENT_HASH",
            ),
            missing_behavior="insufficient valid trading days produce null with indicator",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="realized_volatility_v1",
            formula_version="1",
            expression="sample_std(log(adj_close_t / adj_close_t_minus_1), ddof=1)",
            input_roles=("adjusted_close_history", "trading_calendar"),
            parameters={"ddof": 1},
            pit_constraints=("requires h valid trading-day returns",),
            missing_behavior="fewer than h valid returns produce null with indicator",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="distance_to_extreme_v1",
            formula_version="1",
            expression=(
                "adj_close_T/max(adj_high_T_minus_h_plus_1_to_T)-1; "
                "adj_close_T/min(adj_low_T_minus_h_plus_1_to_T)-1"
            ),
            input_roles=("raw_high", "raw_low", "raw_close", "adj_factor"),
            parameters={"windows": [20, 60]},
            pit_constraints=("high, low and close use the same corporate-action revision",),
            missing_behavior="incomplete or non-positive window produces null with indicator",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="liquidity_state_v1",
            formula_version="1",
            expression="amount/turnover mean_5, mean_20, current/mean_20, log_amount_z20",
            input_roles=("amount", "turnover", "trading_calendar"),
            parameters={"windows": [5, 20], "zscore_ddof": 1},
            pit_constraints=("suspension-day zero turnover remains an observed fact",),
            missing_behavior="no forward fill; zero denominator produces null with indicator",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="moneyflow_state_v1",
            formula_version="1",
            expression="main_net_inflow/amount, sum_5, sum_20, sign_consistency",
            input_roles=("main_net_inflow", "amount", "trading_calendar"),
            parameters={
                "windows": [5, 20],
                "source_unit_contract": "tushare_moneyflow_shares_yuan_v1",
                "source_amount_multiplier": "10000",
            },
            pit_constraints=("moneyflow and amount are converted to the same monetary unit",),
            missing_behavior="zero denominator produces null with indicator, never zero fill",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="industry_context_v1",
            formula_version="1",
            expression="member_equal_weight_return_5_20, breadth_above_ma20, positive_flow_ratio",
            input_roles=("pit_industry_membership", "member_market_history", "member_moneyflow"),
            parameters={"return_windows": [5, 20], "moving_average_window": 20},
            pit_constraints=(
                "membership effective interval contains decision_as_of_trade_date",
                "requested and effective member sets enter the row identity",
            ),
            missing_behavior="missing membership or source closure fails required industry context",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="market_context_v1",
            formula_version="1",
            expression=(
                "pit_universe_equal_weight_return_5_20, cross_section_volatility, "
                "breadth_above_ma20, up_limit_ratio, down_limit_ratio"
            ),
            input_roles=("pit_universe", "member_market_history", "price_limit_policy"),
            parameters={"return_windows": [5, 20], "moving_average_window": 20},
            pit_constraints=("universe, price-limit policy and effective member set enter identity",),
            missing_behavior="incomplete universe closure fails required market context",
        ),
        FeatureFormulaDefinitionV1(
            formula_id="candidate_group_context_v1",
            formula_version="1",
            expression="candidate_count, industry_herfindahl, stage_score_std, mean_leg_disagreement",
            input_roles=("same_day_base_candidates", "same_day_stage_scores", "same_day_leg_scores"),
            parameters={"stages": [
                "alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective"
            ]},
            pit_constraints=("only same-day base candidates are used",),
            missing_behavior="missing group evidence fails the ranking group",
        ),
    )
    return FeatureFormulaRegistryV1(formulas=formulas)


class ShortReboundFeatureFormulaKernelV1:
    """Pure frozen calculations consumed by the Batch B feature builder."""

    formula_registry = frozen_formula_registry_v1()

    @staticmethod
    def candidate_rank_percentile(*, rank: int, candidate_count: int) -> Decimal:
        if candidate_count < 1 or rank < 1 or rank > candidate_count:
            raise ValueError("rank must be within the candidate group")
        if candidate_count == 1:
            return Decimal("0.5")
        return Decimal(candidate_count - rank) / Decimal(candidate_count - 1)

    @staticmethod
    def adjusted_return(adjusted_closes: Sequence[Decimal]) -> Decimal | None:
        if len(adjusted_closes) < 2 or adjusted_closes[0] <= 0:
            return None
        return adjusted_closes[-1] / adjusted_closes[0] - Decimal(1)

    @staticmethod
    def realized_volatility(adjusted_closes: Sequence[Decimal], *, horizon: int) -> float | None:
        if horizon < 2 or len(adjusted_closes) != horizon + 1:
            return None
        closes = [float(value) for value in adjusted_closes]
        if any(value <= 0 for value in closes):
            return None
        returns = [math.log(current / previous) for previous, current in zip(closes, closes[1:])]
        return stdev(returns) if len(returns) >= horizon else None

    @staticmethod
    def distance_to_extreme(
        *,
        adjusted_close: Decimal,
        adjusted_highs: Sequence[Decimal],
        adjusted_lows: Sequence[Decimal],
    ) -> tuple[Decimal | None, Decimal | None]:
        if not adjusted_highs or len(adjusted_highs) != len(adjusted_lows):
            return None, None
        high = max(adjusted_highs)
        low = min(adjusted_lows)
        if adjusted_close <= 0 or high <= 0 or low <= 0:
            return None, None
        return adjusted_close / high - Decimal(1), adjusted_close / low - Decimal(1)

    @staticmethod
    def multi_alpha_consensus(
        *,
        scores: Sequence[Decimal],
        weights: Sequence[Decimal],
    ) -> dict[str, Decimal]:
        if not scores or len(scores) != len(weights):
            raise ValueError("scores and weights must be non-empty and aligned")
        if any(weight <= 0 for weight in weights):
            raise ValueError("multi-alpha weights must satisfy the admitted positive-weight contract")
        denominator = sum(weights, Decimal(0))
        if denominator == 0:
            raise ValueError("multi-alpha weights must have non-zero absolute sum")
        normalized = [weight / denominator for weight in weights]
        mean = sum((weight * score for weight, score in zip(normalized, scores)), Decimal(0))
        variance = sum(
            (abs(weight) * (score - mean) ** 2 for weight, score in zip(normalized, scores)),
            Decimal(0),
        )
        non_zero = [(score, weight) for score, weight in zip(scores, normalized) if score != 0]
        weighted_positive = sum((abs(weight) for score, weight in non_zero if score > 0), Decimal(0))
        weighted_negative = sum((abs(weight) for score, weight in non_zero if score < 0), Decimal(0))
        sign_agreement = max(weighted_positive, weighted_negative)
        contributions = [abs(weight * score) for score, weight in zip(scores, normalized)]
        contribution_sum = sum(contributions, Decimal(0))
        dominance = max(contributions) / contribution_sum if contribution_sum else Decimal(0)
        return {
            "weighted_mean": mean,
            "weighted_std": variance.sqrt(),
            "sign_agreement": sign_agreement,
            "max_leg_dominance": dominance,
        }

    @staticmethod
    def candidate_group_industry_herfindahl(industry_ids: Iterable[str]) -> Decimal:
        counts: dict[str, int] = {}
        total = 0
        for industry_id in industry_ids:
            if not industry_id:
                raise ValueError("industry ids must be non-empty")
            counts[industry_id] = counts.get(industry_id, 0) + 1
            total += 1
        if total == 0:
            raise ValueError("candidate group must not be empty")
        return sum((Decimal(count) / Decimal(total)) ** 2 for count in counts.values())


class StageCandidateInputV1(FrozenModel):
    stage: Literal["alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective"]
    rank: int = Field(ge=1)
    score: Decimal
    stage_candidate_count: int = Field(ge=1)
    previous_rank_score: Decimal | None = None
    next_rank_score: Decimal | None = None
    stage_evidence_id: str = Field(min_length=1, max_length=160)
    candidate_content_hash: str = Field(min_length=64, max_length=64)

    @field_validator("candidate_content_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return str(validated_hash(value, field_name="candidate_content_hash"))

    @model_validator(mode="after")
    def _rank(self) -> "StageCandidateInputV1":
        if self.rank > self.stage_candidate_count:
            raise ValueError("stage rank exceeds frozen stage candidate count")
        if self.rank == 1 and self.previous_rank_score is not None:
            raise ValueError("top-ranked stage candidate cannot have a previous-rank score")
        if self.rank == self.stage_candidate_count and self.next_rank_score is not None:
            raise ValueError("last-ranked stage candidate cannot have a next-rank score")
        return self


class MultiAlphaLegInputV1(FrozenModel):
    component_id: str = Field(min_length=1, max_length=160)
    score: Decimal
    weight: Decimal = Field(gt=0)
    model_identity_hash: str = Field(min_length=64, max_length=64)

    @field_validator("model_identity_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return str(validated_hash(value, field_name="model_identity_hash"))


class FrozenCandidateFeatureInputV1(FrozenModel):
    base_snapshot_id: str = Field(min_length=1, max_length=160)
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    stable_signal_semantics_hash: str = Field(min_length=64, max_length=64)
    canonical_signal_scope_hash: str = Field(min_length=64, max_length=64)
    observation_version_id: str = Field(min_length=1, max_length=160)
    observation_content_hash: str = Field(min_length=64, max_length=64)
    symbol: str = Field(pattern=r"^[0-9]{6}\.(SH|SZ|BJ)$")
    decision_trade_date: date
    decision_cutoff_ts: datetime
    target_trade_date: date
    stage_candidates: tuple[StageCandidateInputV1, ...]
    multi_alpha_legs: tuple[MultiAlphaLegInputV1, ...]
    component_evidence_hash: str = Field(min_length=64, max_length=64)
    hmm_enabled: bool
    hmm_snapshot_id: str | None = Field(default=None, min_length=1, max_length=160)
    hmm_snapshot_hash: str | None = Field(default=None, min_length=64, max_length=64)
    hmm_snapshot_status: str = Field(min_length=1, max_length=80)
    hmm_freshness_trade_days: int | None = Field(default=None, ge=0)
    hmm_coefficient: Decimal | None = None
    risk_enabled: bool
    risk_policy_hash: str = Field(min_length=64, max_length=64)
    risk_can_buy: bool | None = None
    risk_multiplier: Decimal | None = None
    risk_delta: Decimal | None = None
    risk_penalty: Decimal | None = None
    universe_policy_hash: str = Field(min_length=64, max_length=64)

    @field_validator(
        "canonical_signal_scope_hash",
        "stable_signal_semantics_hash",
        "observation_content_hash",
        "component_evidence_hash",
        "hmm_snapshot_hash",
        "risk_policy_hash",
        "universe_policy_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _closure(self) -> "FrozenCandidateFeatureInputV1":
        expected_stages = (
            "alpha_raw",
            "hmm_adjusted",
            "risk_policy_adjusted",
            "selection_effective",
        )
        if tuple(item.stage for item in self.stage_candidates) != expected_stages:
            raise ValueError("candidate input requires four frozen stages in canonical order")
        if not self.multi_alpha_legs:
            raise ValueError("SHORT_REBOUND target requires frozen multi-alpha leg evidence")
        if tuple(item.component_id for item in self.multi_alpha_legs) != tuple(
            sorted(item.component_id for item in self.multi_alpha_legs)
        ):
            raise ValueError("multi-alpha legs must be ordered by component_id")
        if self.hmm_enabled != (self.hmm_snapshot_id is not None):
            raise ValueError("HMM enabled state differs from frozen snapshot identity")
        if self.hmm_enabled != (self.hmm_snapshot_hash is not None):
            raise ValueError("HMM enabled state differs from frozen snapshot hash")
        if self.hmm_enabled and (
            self.hmm_coefficient is None or self.hmm_freshness_trade_days is None
        ):
            raise ValueError("enabled HMM evidence requires coefficient and freshness")
        if self.risk_enabled and any(
            value is None
            for value in (
                self.risk_can_buy,
                self.risk_multiplier,
                self.risk_delta,
                self.risk_penalty,
            )
        ):
            raise ValueError("enabled risk evidence requires exact candidate decision fields")
        return self


class BuiltFeatureRowV1(FrozenModel):
    identity: FeatureRowIdentityV1
    decision_trade_date: date
    target_trade_date: date
    canonical_signal_scope_hash: str = Field(min_length=64, max_length=64)
    stable_signal_semantics_hash: str = Field(min_length=64, max_length=64)
    features: dict[str, Any]

    @model_validator(mode="after")
    def _closure(self) -> "BuiltFeatureRowV1":
        if self.identity.feature_payload_hash != feature_payload_hash(self.features):
            raise ValueError("feature payload differs from row identity")
        return self


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    parsed = Decimal(str(value))
    return parsed if parsed.is_finite() else None


def _float(value: Decimal | float | int | None) -> float | None:
    return None if value is None else float(value)


def _sample_std(values: Sequence[float]) -> float:
    return stdev(values) if len(values) > 1 else 0.0


class ShortReboundFeatureBuilderV1:
    """Complete pure feature builder shared by Batch B export and shadow inference."""

    formula_registry = frozen_formula_registry_v1()
    feature_schema = frozen_feature_schema_v1()

    def __init__(self, *, source_spool: RerankerDatasetSpool, source_identity: str) -> None:
        self._spool = source_spool
        self._source_identity = source_identity
        self._kernel = ShortReboundFeatureFormulaKernelV1()
        self._calendar_cache: tuple[str, ...] | None = None
        self._industry_cache: tuple[dict[str, Any], ...] | None = None
        self._market_cache: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._moneyflow_cache: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        self._last_source_date: str | None = None

    def build_group(
        self,
        *,
        candidates: tuple[FrozenCandidateFeatureInputV1, ...],
        query_registry_hash: str,
        feature_source_revision_set_hash: str,
        builder_code_closure_hash: str,
    ) -> tuple[BuiltFeatureRowV1, ...]:
        if not candidates:
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "feature build group must not be empty",
            )
        first = candidates[0]
        if tuple(item.symbol for item in candidates) != tuple(sorted(item.symbol for item in candidates)):
            raise ValueError("feature build candidates must use canonical symbol order")
        group_fields = (
            "base_snapshot_id",
            "decision_trade_date",
            "decision_cutoff_ts",
            "target_trade_date",
            "stable_signal_semantics_hash",
        )
        if any(
            any(getattr(item, field) != getattr(first, field) for field in group_fields)
            for item in candidates[1:]
        ):
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "feature candidates do not belong to one ranking group",
            )
        decision_text = first.decision_trade_date.isoformat()
        if self._calendar_cache is None:
            self._calendar_cache = tuple(
                str(row["cal_date"])
                for row in self._spool.iter_rows(
                    source_kind="FEATURE_SOURCE",
                    source_identity=self._source_identity,
                    logical_role="historical_trading_calendar_window",
                )
                if bool(row["is_trading"])
            )
        calendar = tuple(day for day in self._calendar_cache if day <= decision_text)
        if not calendar or calendar[-1] != decision_text:
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "decision date is absent from frozen trading calendar",
                context={"decision_trade_date": decision_text},
            )
        history_start = calendar[max(0, len(calendar) - 61)]
        universe = tuple(
            str(row["ts_code"])
            for row in self._spool.iter_rows(
                source_kind="FEATURE_SOURCE",
                source_identity=self._source_identity,
                logical_role="historical_pit_universe_existing_readonly",
                start_date=decision_text,
                end_date=decision_text,
            )
        )
        if not universe or tuple(sorted(set(universe))) != universe:
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "frozen PIT universe is empty, duplicated, or unordered",
                context={"decision_trade_date": decision_text},
            )
        market_by_symbol, moneyflow_by_symbol = self._rolling_sources(
            history_start=history_start,
            decision_text=decision_text,
        )
        limit_rows = tuple(
            self._spool.iter_rows(
                source_kind="FEATURE_SOURCE",
                source_identity=self._source_identity,
                logical_role="historical_decision_mark_daily_market",
                start_date=decision_text,
                end_date=decision_text,
            )
        )
        if self._industry_cache is None:
            self._industry_cache = tuple(
                self._spool.iter_rows(
                    source_kind="FEATURE_SOURCE",
                    source_identity=self._source_identity,
                    logical_role="historical_industry_membership",
                )
            )
        industry_rows = self._industry_cache
        limits_by_symbol = {str(row["ts_code"]): row for row in limit_rows}
        industry_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in industry_rows:
            start = str(row["in_date"])
            end = str(row["out_date"]) if row.get("out_date") is not None else "9999-12-31"
            if start <= decision_text <= end and str(row.get("l2_code") or ""):
                industry_by_symbol[str(row["ts_code"])].append(row)

        industry_code: dict[str, str] = {}
        for symbol in universe:
            matches = industry_by_symbol.get(symbol, [])
            codes = sorted({str(item["l2_code"]) for item in matches})
            if len(codes) == 1:
                industry_code[symbol] = codes[0]
        candidate_industries = []
        for candidate in candidates:
            code = industry_code.get(candidate.symbol)
            if code is None:
                raise AdvisoryModelingError(
                    REASON_FEATURE_CLOSURE_INCOMPLETE,
                    "candidate lacks one exact PIT industry mapping",
                    context={"symbol": candidate.symbol, "decision_trade_date": decision_text},
                )
            candidate_industries.append(code)
        stage_groups = {
            stage: sorted(
                ((item.stage_candidates[index].rank, item.stage_candidates[index].score, item.symbol) for item in candidates),
                key=lambda value: (value[0], value[2]),
            )
            for index, stage in enumerate(
                ("alpha_raw", "hmm_adjusted", "risk_policy_adjusted", "selection_effective")
            )
        }
        group_context = self._group_context(
            candidates=candidates,
            industries=tuple(candidate_industries),
        )
        market_context, market_effective_members = self._cross_section_context(
            members=universe,
            market_by_symbol=market_by_symbol,
            moneyflow_by_symbol=moneyflow_by_symbol,
            limits_by_symbol=limits_by_symbol,
        )
        results: list[BuiltFeatureRowV1] = []
        industry_context_by_code: dict[
            str, tuple[tuple[str, ...], dict[str, float], tuple[str, ...]]
        ] = {}
        for candidate, candidate_industry in zip(candidates, candidate_industries, strict=True):
            cached_context = industry_context_by_code.get(candidate_industry)
            if cached_context is None:
                industry_members = tuple(
                    symbol for symbol in universe if industry_code.get(symbol) == candidate_industry
                )
                industry_context, industry_effective_members = self._cross_section_context(
                    members=industry_members,
                    market_by_symbol=market_by_symbol,
                    moneyflow_by_symbol=moneyflow_by_symbol,
                    limits_by_symbol=limits_by_symbol,
                )
                industry_context_by_code[candidate_industry] = (
                    industry_members,
                    industry_context,
                    industry_effective_members,
                )
            else:
                industry_members, industry_context, industry_effective_members = cached_context
            features = self._candidate_features(
                candidate=candidate,
                candidate_industry=candidate_industry,
                stage_groups=stage_groups,
                market_history=market_by_symbol.get(candidate.symbol, []),
                moneyflow_history=moneyflow_by_symbol.get(candidate.symbol, {}),
                industry_members=industry_members,
                industry_effective_members=industry_effective_members,
                universe_members=universe,
                market_effective_members=market_effective_members,
                industry_context=industry_context,
                market_context=market_context,
                group_context=group_context,
            )
            self._validate_payload(features)
            stage_evidence_set_hash = canonical_json_sha256(
                tuple(item.stage_evidence_id for item in candidate.stage_candidates)
            )
            base_candidate_hash = canonical_json_sha256(
                tuple(item.candidate_content_hash for item in candidate.stage_candidates)
            )
            payload_hash = feature_payload_hash(features)
            identity = FeatureRowIdentityV1(
                base_snapshot_id=candidate.base_snapshot_id,
                canonical_signal_id=candidate.canonical_signal_id,
                observation_version_id=candidate.observation_version_id,
                symbol=candidate.symbol,
                decision_cutoff_ts=candidate.decision_cutoff_ts,
                base_candidate_hash=base_candidate_hash,
                stage_evidence_set_hash=stage_evidence_set_hash,
                feature_payload_hash=payload_hash,
                formula_registry_hash=str(self.formula_registry.registry_hash),
                query_registry_hash=query_registry_hash,
                feature_source_revision_set_hash=feature_source_revision_set_hash,
                builder_code_closure_hash=builder_code_closure_hash,
            )
            results.append(
                BuiltFeatureRowV1(
                    identity=identity,
                    decision_trade_date=candidate.decision_trade_date,
                    target_trade_date=candidate.target_trade_date,
                    canonical_signal_scope_hash=candidate.canonical_signal_scope_hash,
                    stable_signal_semantics_hash=candidate.stable_signal_semantics_hash,
                    features=features,
                )
            )
        return tuple(results)

    def _rolling_sources(
        self,
        *,
        history_start: str,
        decision_text: str,
    ) -> tuple[
        dict[str, list[dict[str, Any]]],
        dict[str, dict[str, dict[str, Any]]],
    ]:
        if self._last_source_date is None or decision_text <= self._last_source_date:
            self._market_cache = defaultdict(list)
            self._moneyflow_cache = defaultdict(dict)
            query_start = history_start
        else:
            query_start = next(
                (
                    day
                    for day in self._calendar_cache or ()
                    if self._last_source_date < day <= decision_text
                ),
                decision_text,
            )
        for row in self._spool.iter_rows(
            source_kind="FEATURE_SOURCE",
            source_identity=self._source_identity,
            logical_role="historical_market_history_window",
            start_date=query_start,
            end_date=decision_text,
        ):
            self._market_cache[str(row["ts_code"])].append(row)
        for row in self._spool.iter_rows(
            source_kind="FEATURE_SOURCE",
            source_identity=self._source_identity,
            logical_role="historical_fundamental_moneyflow_window",
            start_date=query_start,
            end_date=decision_text,
        ):
            self._moneyflow_cache[str(row["ts_code"])][str(row["trade_date"])] = row
        for symbol in tuple(self._market_cache):
            retained = [
                row for row in self._market_cache[symbol] if str(row["trade_date"]) >= history_start
            ]
            if retained:
                self._market_cache[symbol] = retained
            else:
                del self._market_cache[symbol]
        for symbol in tuple(self._moneyflow_cache):
            retained = {
                day: row
                for day, row in self._moneyflow_cache[symbol].items()
                if day >= history_start
            }
            if retained:
                self._moneyflow_cache[symbol] = retained
            else:
                del self._moneyflow_cache[symbol]
        self._last_source_date = decision_text
        return self._market_cache, self._moneyflow_cache

    def _candidate_features(
        self,
        *,
        candidate: FrozenCandidateFeatureInputV1,
        candidate_industry: str,
        stage_groups: dict[str, list[tuple[int, Decimal, str]]],
        market_history: list[dict[str, Any]],
        moneyflow_history: dict[str, dict[str, Any]],
        industry_members: tuple[str, ...],
        industry_effective_members: tuple[str, ...],
        universe_members: tuple[str, ...],
        market_effective_members: tuple[str, ...],
        industry_context: dict[str, float],
        market_context: dict[str, float],
        group_context: dict[str, float | int],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "industry_l2_code": candidate_industry,
            "industry_mapping_hash": canonical_json_sha256(
                {"symbol": candidate.symbol, "decision_trade_date": candidate.decision_trade_date, "l2_code": candidate_industry}
            ),
            "hmm_enabled": candidate.hmm_enabled,
            "hmm_snapshot_id": candidate.hmm_snapshot_id,
            "hmm_snapshot_hash": candidate.hmm_snapshot_hash,
            "hmm_snapshot_status": candidate.hmm_snapshot_status,
            "hmm_freshness_trade_days": candidate.hmm_freshness_trade_days,
            "hmm_coefficient": _float(candidate.hmm_coefficient),
            "risk_enabled": candidate.risk_enabled,
            "risk_policy_hash": candidate.risk_policy_hash,
            "risk_can_buy": candidate.risk_can_buy,
            "risk_multiplier": _float(candidate.risk_multiplier),
            "risk_delta": _float(candidate.risk_delta),
            "risk_penalty": _float(candidate.risk_penalty),
            "universe_policy_hash": candidate.universe_policy_hash,
        }
        for stage_input in candidate.stage_candidates:
            stage = stage_input.stage
            group = stage_groups[stage]
            if not any(value[2] == candidate.symbol for value in group):
                raise AdvisoryModelingError(
                    REASON_FEATURE_CLOSURE_INCOMPLETE,
                    "candidate is absent from the frozen stage group",
                    context={"symbol": candidate.symbol, "stage": stage},
                )
            previous = (
                None
                if stage_input.previous_rank_score is None
                else stage_input.score - stage_input.previous_rank_score
            )
            following = (
                None
                if stage_input.next_rank_score is None
                else stage_input.score - stage_input.next_rank_score
            )
            payload.update(
                {
                    f"{stage}_rank": stage_input.rank,
                    f"{stage}_score": float(stage_input.score),
                    f"{stage}_rank_percentile": float(
                        self._kernel.candidate_rank_percentile(
                            rank=stage_input.rank,
                            candidate_count=stage_input.stage_candidate_count,
                        )
                    ),
                    f"{stage}_score_gap_previous": _float(previous),
                    f"{stage}_score_gap_previous_missing": previous is None,
                    f"{stage}_score_gap_next": _float(following),
                    f"{stage}_score_gap_next_missing": following is None,
                }
            )
        consensus = self._kernel.multi_alpha_consensus(
            scores=tuple(item.score for item in candidate.multi_alpha_legs),
            weights=tuple(item.weight for item in candidate.multi_alpha_legs),
        )
        payload.update(
            {
                "multi_alpha_leg_count": len(candidate.multi_alpha_legs),
                "multi_alpha_weighted_mean": float(consensus["weighted_mean"]),
                "multi_alpha_weighted_std": float(consensus["weighted_std"]),
                "multi_alpha_sign_agreement": float(consensus["sign_agreement"]),
                "multi_alpha_max_leg_dominance": float(consensus["max_leg_dominance"]),
                "multi_alpha_component_evidence_hash": candidate.component_evidence_hash,
            }
        )
        adjusted = self._adjusted_history(market_history)
        for horizon in (1, 3, 5, 10, 20, 60):
            values = tuple(item[1] for item in adjusted[-(horizon + 1) :])
            value = self._kernel.adjusted_return(values) if len(values) == horizon + 1 else None
            payload[f"return_{horizon}"] = _float(value)
            payload[f"return_{horizon}_missing"] = value is None
        for horizon in (5, 20, 60):
            values = tuple(item[1] for item in adjusted[-(horizon + 1) :])
            value = self._kernel.realized_volatility(values, horizon=horizon)
            payload[f"realized_volatility_{horizon}"] = value
            payload[f"realized_volatility_{horizon}_missing"] = value is None
        for horizon in (20, 60):
            rows = market_history[-horizon:]
            close = adjusted[-1][1] if adjusted else Decimal(0)
            highs = tuple(
                Decimal(str(row["high_li"])) * Decimal(str(row["adj_factor"]))
                for row in rows
                if row.get("high_li") is not None and row.get("adj_factor") is not None
            )
            lows = tuple(
                Decimal(str(row["low_li"])) * Decimal(str(row["adj_factor"]))
                for row in rows
                if row.get("low_li") is not None and row.get("adj_factor") is not None
            )
            high_distance, low_distance = (
                self._kernel.distance_to_extreme(
                    adjusted_close=close,
                    adjusted_highs=highs,
                    adjusted_lows=lows,
                )
                if len(highs) == horizon and len(lows) == horizon
                else (None, None)
            )
            payload[f"distance_to_high_{horizon}"] = _float(high_distance)
            payload[f"distance_to_high_{horizon}_missing"] = high_distance is None
            payload[f"distance_to_low_{horizon}"] = _float(low_distance)
            payload[f"distance_to_low_{horizon}_missing"] = low_distance is None
        payload.update(self._liquidity_moneyflow(market_history, moneyflow_history))
        payload.update({f"industry_{key}": value for key, value in industry_context.items()})
        payload["industry_member_set_hash"] = canonical_json_sha256(
            {
                "requested_members": industry_members,
                "effective_members": industry_effective_members,
            }
        )
        payload.update({f"market_{key}": value for key, value in market_context.items()})
        payload["market_member_set_hash"] = canonical_json_sha256(
            {
                "requested_members": universe_members,
                "effective_members": market_effective_members,
            }
        )
        payload.update(group_context)
        return payload

    @staticmethod
    def _adjusted_history(rows: Sequence[dict[str, Any]]) -> list[tuple[str, Decimal]]:
        result: list[tuple[str, Decimal]] = []
        for row in rows:
            close = _decimal(row.get("close_li"))
            factor = _decimal(row.get("adj_factor"))
            if close is not None and factor is not None and close > 0 and factor > 0:
                result.append((str(row["trade_date"]), close * factor))
        return result

    def _cross_section_context(
        self,
        *,
        members: tuple[str, ...],
        market_by_symbol: dict[str, list[dict[str, Any]]],
        moneyflow_by_symbol: dict[str, dict[str, dict[str, Any]]],
        limits_by_symbol: dict[str, dict[str, Any]],
    ) -> tuple[dict[str, float], tuple[str, ...]]:
        if not members:
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "cross-section context has no frozen members",
            )
        returns_5: list[float] = []
        returns_20: list[float] = []
        above_ma20: list[bool] = []
        positive_flow: list[bool] = []
        up_limits = 0
        down_limits = 0
        effective_members: list[str] = []
        for symbol in members:
            adjusted = self._adjusted_history(market_by_symbol.get(symbol, []))
            flows = moneyflow_by_symbol.get(symbol, {})
            limit = limits_by_symbol.get(symbol)
            decision_day = str(limit.get("trade_date")) if limit is not None else ""
            decision_flow = flows.get(decision_day)
            latest_flow = (
                decision_flow.get("net_mf_amount") if decision_flow is not None else None
            )
            up = _decimal(limit.get("up_limit")) if limit is not None else None
            down = _decimal(limit.get("down_limit")) if limit is not None else None
            close_li = _decimal(limit.get("close_li")) if limit is not None else None
            if (
                len(adjusted) < 21
                or adjusted[-1][0] != decision_day
                or latest_flow is None
                or up is None
                or down is None
                or close_li is None
            ):
                continue
            return_5 = self._kernel.adjusted_return(
                tuple(item[1] for item in adjusted[-6:])
            )
            return_20 = self._kernel.adjusted_return(
                tuple(item[1] for item in adjusted[-21:])
            )
            if return_5 is None or return_20 is None:
                continue
            effective_members.append(symbol)
            returns_5.append(float(return_5))
            returns_20.append(float(return_20))
            current = float(adjusted[-1][1])
            ma20 = mean(float(item[1]) for item in adjusted[-20:])
            above_ma20.append(current > ma20)
            positive_flow.append(Decimal(str(latest_flow)) > 0)
            close_yuan = close_li / Decimal(1000)
            up_limits += int(close_yuan >= up * Decimal("0.9995"))
            down_limits += int(close_yuan <= down * Decimal("1.0005"))
        if not effective_members:
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "cross-section source coverage is incomplete",
                context={"member_count": len(members)},
            )
        denominator = len(effective_members)
        return {
            "return_5_mean": mean(returns_5),
            "return_20_mean": mean(returns_20),
            "cross_section_volatility_20": _sample_std(returns_20),
            "breadth_above_ma20": sum(above_ma20) / len(above_ma20),
            "positive_moneyflow_ratio": sum(positive_flow) / len(positive_flow),
            "up_limit_ratio": up_limits / denominator,
            "down_limit_ratio": down_limits / denominator,
        }, tuple(effective_members)

    @staticmethod
    def _liquidity_moneyflow(
        market_history: Sequence[dict[str, Any]],
        moneyflow_history: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        market_by_date = {str(row["trade_date"]): row for row in market_history}
        ordered_dates = tuple(sorted(market_by_date))
        window_dates = ordered_dates[-20:]
        amounts = [
            (
                float(Decimal(str(market_by_date[day]["amount_li"])) / Decimal(1000))
                if market_by_date[day].get("amount_li") is not None
                else None
            )
            for day in window_dates
        ]
        turnover = [
            (
                float(moneyflow_history[day]["turnover_rate"])
                if day in moneyflow_history
                and moneyflow_history[day].get("turnover_rate") is not None
                else None
            )
            for day in window_dates
        ]
        complete_amounts = [value for value in amounts if value is not None]
        complete_turnover = [value for value in turnover if value is not None]
        liquidity_missing = (
            len(window_dates) < 20
            or len(complete_amounts) != 20
            or len(complete_turnover) != 20
            or mean(complete_amounts) == 0
        )
        if liquidity_missing:
            liquidity = {
                "amount_mean_5": None,
                "amount_mean_20": None,
                "turnover_mean_5": None,
                "turnover_mean_20": None,
                "amount_to_mean_20": None,
                "log_amount_zscore_20": None,
                "liquidity_missing": True,
            }
        else:
            log_amounts = [math.log(value) for value in complete_amounts if value > 0]
            zscore = None
            if len(log_amounts) == 20 and stdev(log_amounts) > 0:
                zscore = (log_amounts[-1] - mean(log_amounts)) / stdev(log_amounts)
            liquidity = {
                "amount_mean_5": mean(complete_amounts[-5:]),
                "amount_mean_20": mean(complete_amounts),
                "turnover_mean_5": mean(complete_turnover[-5:]),
                "turnover_mean_20": mean(complete_turnover),
                "amount_to_mean_20": complete_amounts[-1] / mean(complete_amounts),
                "log_amount_zscore_20": zscore,
                "liquidity_missing": zscore is None,
            }
        ratios: list[float | None] = []
        for day, amount in zip(window_dates, amounts, strict=True):
            source = moneyflow_history.get(day)
            flow = (
                float(Decimal(str(source["net_mf_amount"])) * MONEYFLOW_AMOUNT_MULTIPLIER)
                if source is not None and source.get("net_mf_amount") is not None
                else None
            )
            ratios.append(
                None if flow is None or amount is None or amount == 0 else flow / amount
            )
        last_five = ratios[-5:]
        moneyflow_missing = len(ratios) < 20 or any(value is None for value in ratios)
        moneyflow = {
            "moneyflow_ratio_current": ratios[-1] if ratios else None,
            "moneyflow_ratio_5": (
                mean(value for value in last_five if value is not None)
                if len(last_five) == 5 and all(value is not None for value in last_five)
                else None
            ),
            "moneyflow_ratio_20": (
                mean(value for value in ratios if value is not None)
                if not moneyflow_missing
                else None
            ),
            "moneyflow_sign_consistency_20": (
                max(
                    sum(value > 0 for value in ratios if value is not None),
                    sum(value < 0 for value in ratios if value is not None),
                )
                / len(ratios)
                if not moneyflow_missing
                else None
            ),
            "moneyflow_missing": moneyflow_missing,
        }
        return {**liquidity, **moneyflow}

    def _group_context(
        self,
        *,
        candidates: tuple[FrozenCandidateFeatureInputV1, ...],
        industries: tuple[str, ...],
    ) -> dict[str, float | int]:
        score_by_stage = {
            stage: [float(item.stage_candidates[index].score) for item in candidates]
            for index, stage in enumerate(
                ("alpha", "hmm", "risk", "selection")
            )
        }
        disagreements = []
        for candidate in candidates:
            consensus = self._kernel.multi_alpha_consensus(
                scores=tuple(item.score for item in candidate.multi_alpha_legs),
                weights=tuple(item.weight for item in candidate.multi_alpha_legs),
            )
            disagreements.append(float(consensus["weighted_std"]))
        return {
            "candidate_count": len(candidates),
            "candidate_industry_herfindahl": float(
                self._kernel.candidate_group_industry_herfindahl(industries)
            ),
            "candidate_alpha_score_std": _sample_std(score_by_stage["alpha"]),
            "candidate_hmm_score_std": _sample_std(score_by_stage["hmm"]),
            "candidate_risk_score_std": _sample_std(score_by_stage["risk"]),
            "candidate_selection_score_std": _sample_std(score_by_stage["selection"]),
            "candidate_mean_leg_disagreement": mean(disagreements),
        }

    def _validate_payload(self, payload: dict[str, Any]) -> None:
        expected = {item.name for item in self.feature_schema.definitions}
        if set(payload) != expected:
            raise AdvisoryModelingError(
                REASON_FEATURE_CLOSURE_INCOMPLETE,
                "feature payload differs from frozen schema",
                context={
                    "missing_features": tuple(sorted(expected - set(payload))),
                    "unexpected_features": tuple(sorted(set(payload) - expected)),
                },
            )
        for definition in self.feature_schema.definitions:
            value = payload[definition.name]
            if value is None and definition.missing_policy == "REQUIRED_FAIL_GROUP":
                raise AdvisoryModelingError(
                    REASON_FEATURE_CLOSURE_INCOMPLETE,
                    "required feature is missing",
                    context={"feature_name": definition.name},
                )
