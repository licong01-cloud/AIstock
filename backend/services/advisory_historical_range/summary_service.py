"""Frozen Phase 1R summary formulas over exact outcome refs only."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal, ROUND_HALF_EVEN
import json
import logging
from statistics import median
from typing import Any, Iterable, Literal, Mapping, Protocol

import psycopg2.extras

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonical_json_sha256, canonicalize
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeArtifactV2,
    HistoricalRangeOutcomeFactV1,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeOutcomeSubjectType,
    HistoricalRangeSummaryArtifactV2,
    HistoricalRangeSummaryFactV1,
    HistoricalRangeSummaryPolicyV1,
    derive_prefixed_id,
    require_sha256,
)
from backend.services.advisory_historical_range.outcome_source import (
    HistoricalRangeOutcomeSourceError,
    PostgresHistoricalRangeOutcomeSourceProvider,
)
from backend.services.advisory_historical_range.outcome_projection import (
    HistoricalRangeAggregateCalculationResultV1,
    map_historical_range_maturity,
)
from backend.services.advisory_phase1.outcome_engine import (
    OutcomeCalculationResult,
    OutcomeOwner,
)
from backend.services.advisory_phase1.dataset_build import DatasetSnapshotFile
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore
from backend.services.advisory_phase1.snapshot_writer import (
    DatasetManifest,
    read_verified_snapshot_parquet_rows,
)


_SCALE = Decimal("0.000000000001")
_NUMERIC_STATUSES = {HistoricalRangeOutcomeStatus.COMPLETE, HistoricalRangeOutcomeStatus.TERMINAL}


class HistoricalRangeSummaryError(ValueError):
    pass


logger = logging.getLogger(__name__)


class Phase1WinnerDefinitionV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    winner_definition_id: str = Field(min_length=1, max_length=160)
    winner_definition_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    projection: str = Field(min_length=1, max_length=160)
    comparison_operator: Literal["GT", "GTE", "LT", "LTE"]
    threshold: Decimal
    ranking_direction: Literal["DESC", "ASC"]
    horizon_trade_days: int = Field(ge=1)
    label_policy_hash: str = Field(min_length=64, max_length=64)
    denominator_universe_layer: str = Field(min_length=1, max_length=160)
    evidence_scope: Literal["RETROSPECTIVE_RESEARCH_ONLY"] = (
        "RETROSPECTIVE_RESEARCH_ONLY"
    )

    @field_validator("winner_definition_hash", "label_policy_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return (
            require_sha256(value, field_name=info.field_name)
            if value is not None
            else None
        )

    @model_validator(mode="after")
    def _identity(self) -> "Phase1WinnerDefinitionV1":
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"winner_definition_hash"})
        )
        if (
            self.winner_definition_hash is not None
            and self.winner_definition_hash != digest
        ):
            raise ValueError("winner definition hash differs from frozen content")
        object.__setattr__(self, "winner_definition_hash", digest)
        return self

    def matches(self, value: Decimal) -> bool:
        return {
            "GT": value > self.threshold,
            "GTE": value >= self.threshold,
            "LT": value < self.threshold,
            "LTE": value <= self.threshold,
        }[self.comparison_operator]


class Phase1UniverseOutcomeEvidenceRefV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    snapshot_id: str = Field(min_length=1, max_length=160)
    snapshot_content_hash: str = Field(min_length=64, max_length=64)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    snapshot_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    logical_path: str = Field(min_length=1, max_length=1024)
    file_sha256: str = Field(min_length=64, max_length=64)
    file_size_bytes: int = Field(gt=0)
    file_row_count: int = Field(ge=1)
    partition_content_hash: str = Field(min_length=64, max_length=64)
    raw_outcome_id: str = Field(min_length=1, max_length=160)
    raw_outcome_hash: str = Field(min_length=64, max_length=64)
    canonical_row_hash: str = Field(min_length=64, max_length=64)
    decision_trade_date: date
    symbol: str = Field(min_length=1, max_length=32)
    projection: str = Field(min_length=1, max_length=160)
    horizon_trade_days: int = Field(ge=1)
    projection_value_decimal: Decimal | None = None
    maturity_status: str = Field(min_length=1, max_length=80)
    outcome_event_status: str = Field(min_length=1, max_length=80)
    label_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    label_policy_hash: str = Field(min_length=64, max_length=64)
    universe_policy_hash: str = Field(min_length=64, max_length=64)
    universe_layer: str = Field(min_length=1, max_length=160)
    calculation_evidence_sha256: str = Field(min_length=64, max_length=64)
    calculation_evidence_size_bytes: int = Field(gt=0)
    calculation_evidence_store_backend_hash: str = Field(
        min_length=64, max_length=64
    )
    label_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    industry_at_t: str | None = Field(default=None, min_length=1, max_length=160)
    industry_evidence_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )
    market_regime_at_t: str | None = Field(
        default=None, min_length=1, max_length=160
    )
    market_regime_evidence_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )

    @field_validator(
        "snapshot_content_hash",
        "manifest_sha256",
        "snapshot_source_revision_set_hash",
        "file_sha256",
        "partition_content_hash",
        "raw_outcome_hash",
        "canonical_row_hash",
        "label_policy_bundle_hash",
        "label_policy_hash",
        "universe_policy_hash",
        "calculation_evidence_sha256",
        "calculation_evidence_store_backend_hash",
        "label_source_revision_set_hash",
        "industry_evidence_hash",
        "market_regime_evidence_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return (
            require_sha256(value, field_name=info.field_name)
            if value is not None
            else None
        )

    @model_validator(mode="after")
    def _closure(self) -> "Phase1UniverseOutcomeEvidenceRefV1":
        symbol = self.symbol.upper()
        if symbol != self.symbol:
            raise ValueError("Phase 1 universe evidence symbol must be uppercase")
        if (self.industry_at_t is None) != (self.industry_evidence_hash is None):
            raise ValueError("industry bucket and exact evidence hash are nullable together")
        if (self.market_regime_at_t is None) != (
            self.market_regime_evidence_hash is None
        ):
            raise ValueError("regime bucket and exact evidence hash are nullable together")
        identity_payload = self.model_dump(
            mode="json", exclude={"canonical_row_hash"}
        )
        if canonical_json_sha256(identity_payload) != self.canonical_row_hash:
            raise ValueError("canonical_row_hash differs from exact universe evidence")
        return self


class HistoricalRangeRecallDenominatorSetV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_phase1r_r4_recall_denominator_set_v1"] = (
        "advisory_phase1r_r4_recall_denominator_set_v1"
    )
    availability: Literal["AVAILABLE", "UNAVAILABLE", "NOT_CONFIGURED"]
    reason_codes: tuple[str, ...] = ()
    denominators: tuple["HistoricalRangeRecallDenominatorV1", ...] = ()
    denominator_set_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )

    @field_validator("denominator_set_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return (
            require_sha256(value, field_name="denominator_set_hash")
            if value is not None
            else None
        )

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeRecallDenominatorSetV1":
        reasons = tuple(sorted({item.strip() for item in self.reason_codes if item.strip()}))
        denominators = tuple(
            sorted(
                self.denominators,
                key=lambda item: (
                    item.decision_trade_date,
                    item.projection_group.value,
                    item.projection,
                    item.horizon_trade_days,
                    item.condition_key or "",
                    item.k,
                ),
            )
        )
        if self.availability == "AVAILABLE":
            if not denominators or reasons:
                raise ValueError("available Recall evidence requires denominators only")
        elif denominators or not reasons:
            raise ValueError("unavailable Recall evidence requires reasons and no denominators")
        keys = [
            (
                item.decision_trade_date,
                item.projection_group.value,
                item.projection,
                item.horizon_trade_days,
                item.condition_key,
                item.k,
            )
            for item in denominators
        ]
        if len(keys) != len(set(keys)):
            raise ValueError("Recall denominator keys must be unique")
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "denominators", denominators)
        digest = canonical_json_sha256(
            self.model_dump(mode="json", exclude={"denominator_set_hash"})
        )
        if self.denominator_set_hash is not None and self.denominator_set_hash != digest:
            raise ValueError("Recall denominator set hash differs from typed evidence")
        object.__setattr__(self, "denominator_set_hash", digest)
        return self

    @classmethod
    def not_configured(cls) -> "HistoricalRangeRecallDenominatorSetV1":
        return cls(
            availability="NOT_CONFIGURED",
            reason_codes=("PIT_RECALL_PROVIDER_NOT_CONFIGURED",),
        )


class HistoricalRangeSummaryInputRowV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    outcome_ref: HistoricalRangeArtifactRefV1
    outcome_logical_id: str = Field(min_length=1, max_length=160)
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    subject_type: HistoricalRangeOutcomeSubjectType
    subject_id: str = Field(min_length=1, max_length=160)
    projection_group: HistoricalRangeOutcomeProjection
    projection: str = Field(min_length=1, max_length=160)
    evaluation_window_type: HistoricalRangeEvaluationWindowType
    horizon_trade_days: int = Field(ge=0)
    decision_trade_date: date
    list_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    symbol: str | None = Field(default=None, min_length=1, max_length=32)
    strategy_rank: int | None = Field(default=None, ge=1)
    maturity_status: HistoricalRangeOutcomeStatus
    value: Decimal | None = None
    observed_holding_trading_days: int | None = Field(default=None, ge=0)
    episode_closed: bool | None = None
    industry_at_t: str | None = Field(default=None, min_length=1, max_length=160)
    market_regime_at_t: str | None = Field(default=None, min_length=1, max_length=160)

    @model_validator(mode="after")
    def _semantic_closure(self) -> "HistoricalRangeSummaryInputRowV1":
        if self.outcome_ref.artifact_kind is not HistoricalRangeArtifactKind.OUTCOME:
            raise ValueError("summary rows require exact OUTCOME refs")
        if self.value is not None and self.maturity_status not in _NUMERIC_STATUSES:
            raise ValueError("only COMPLETE or TERMINAL outcomes may carry numeric values")
        if self.value is None and self.maturity_status in _NUMERIC_STATUSES and self.projection.startswith("RETURN_"):
            raise ValueError("complete return outcome cannot omit its value")
        if self.subject_type is HistoricalRangeOutcomeSubjectType.EPISODE:
            if self.evaluation_window_type is not HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE:
                raise ValueError("episode summary row requires lifecycle window")
            if (
                self.episode_closed
                and self.maturity_status in _NUMERIC_STATUSES
                and self.observed_holding_trading_days is None
            ):
                raise ValueError("closed episode requires observed holding trading days")
        if self.symbol is not None:
            object.__setattr__(self, "symbol", self.symbol.upper())
        return self


class HistoricalRangeRecallDenominatorV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    decision_trade_date: date
    projection_group: HistoricalRangeOutcomeProjection
    projection: str = Field(min_length=1, max_length=160)
    horizon_trade_days: int = Field(ge=1)
    condition_key: str | None = Field(default=None, min_length=1, max_length=160)
    k: int = Field(ge=1)
    strategy_symbols: tuple[str, ...]
    positive_target_symbols: tuple[str, ...]
    eligible_universe_symbols: tuple[str, ...]
    eligible_universe_refs: tuple[Phase1UniverseOutcomeEvidenceRefV1, ...]
    eligible_universe_set_hash: str = Field(min_length=64, max_length=64)
    eligible_universe_source_hash: str = Field(min_length=64, max_length=64)
    label_policy_bundle_hash: str = Field(min_length=64, max_length=64)
    label_policy_hash: str = Field(min_length=64, max_length=64)
    universe_policy_hash: str = Field(min_length=64, max_length=64)
    universe_layer: str = Field(min_length=1, max_length=160)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    winner_definition: Phase1WinnerDefinitionV1
    condition_evidence_hash: str | None = Field(
        default=None, min_length=64, max_length=64
    )

    @field_validator(
        "eligible_universe_set_hash",
        "eligible_universe_source_hash",
        "label_policy_bundle_hash",
        "label_policy_hash",
        "universe_policy_hash",
        "source_revision_set_hash",
        "condition_evidence_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return (
            require_sha256(value, field_name=info.field_name)
            if value is not None
            else None
        )

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeRecallDenominatorV1":
        ranked_symbols = tuple(item.upper() for item in self.strategy_symbols)
        if ranked_symbols != self.strategy_symbols or len(ranked_symbols) != len(
            set(ranked_symbols)
        ):
            raise ValueError("strategy_symbols must preserve an uppercase unique rank order")
        normalized_targets = tuple(
            sorted({item.upper() for item in self.positive_target_symbols})
        )
        if normalized_targets != self.positive_target_symbols:
            raise ValueError(
                "positive_target_symbols must be uppercase, sorted, and unique"
            )
        normalized_universe = tuple(
            sorted({item.upper() for item in self.eligible_universe_symbols})
        )
        if normalized_universe != self.eligible_universe_symbols:
            raise ValueError(
                "eligible_universe_symbols must be uppercase, sorted, and unique"
            )
        if not set(self.strategy_symbols) <= set(self.eligible_universe_symbols):
            raise ValueError("strategy symbols must be inside the eligible universe")
        if not set(self.positive_target_symbols) <= set(self.eligible_universe_symbols):
            raise ValueError("positive targets must be inside the eligible universe")
        refs = tuple(
            sorted(
                self.eligible_universe_refs,
                key=lambda item: (
                    item.symbol,
                    item.raw_outcome_hash,
                    item.logical_path,
                ),
            )
        )
        if not refs or len(refs) != len({item.canonical_row_hash for item in refs}):
            raise ValueError("Recall denominator requires unique exact eligible-universe refs")
        if {item.symbol for item in refs} != set(self.eligible_universe_symbols):
            raise ValueError("Recall exact evidence must cover every eligible universe symbol")
        if any(
            item.decision_trade_date != self.decision_trade_date
            or item.projection != self.projection
            or item.horizon_trade_days != self.horizon_trade_days
            or item.label_policy_bundle_hash != self.label_policy_bundle_hash
            or item.label_policy_hash != self.label_policy_hash
            or item.universe_policy_hash != self.universe_policy_hash
            or item.universe_layer != self.universe_layer
            for item in refs
        ):
            raise ValueError("Recall exact universe evidence differs from denominator identity")
        if (
            self.winner_definition.projection != self.projection
            or self.winner_definition.horizon_trade_days != self.horizon_trade_days
            or self.winner_definition.label_policy_hash != self.label_policy_hash
            or self.winner_definition.denominator_universe_layer
            != self.universe_layer
        ):
            raise ValueError("winner definition differs from Recall denominator identity")
        if (self.condition_key is None) != (self.condition_evidence_hash is None):
            raise ValueError("conditional Recall requires one exact condition evidence hash")
        expected_universe_hash = canonical_json_sha256(
            {
                "decision_trade_date": self.decision_trade_date,
                "eligible_symbols": self.eligible_universe_symbols,
                "eligible_outcome_refs": [
                    item.model_dump(mode="json") for item in refs
                ],
            }
        )
        if self.eligible_universe_set_hash != expected_universe_hash:
            raise ValueError(
                "eligible_universe_set_hash does not match exact Recall evidence"
            )
        if len(self.positive_target_symbols) > self.k:
            raise ValueError("positive target set cannot exceed K")
        object.__setattr__(self, "eligible_universe_refs", refs)
        return self


class HistoricalRangeSummaryRepository(Protocol):
    def append_summary(self, fact: HistoricalRangeSummaryFactV1) -> bool: ...

    def load_latest_summary(self, *, range_run_id: str) -> HistoricalRangeSummaryFactV1 | None: ...

    def find_summary_by_input(
        self, *, range_run_id: str, summary_input_hash: str
    ) -> HistoricalRangeSummaryFactV1 | None: ...


class HistoricalRangeSummaryOutcomeSetV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    range_run_id: str = Field(min_length=1, max_length=160)
    label_as_of_trade_date: date
    resolved_request_hash: str = Field(min_length=64, max_length=64)
    rows: tuple[HistoricalRangeSummaryInputRowV1, ...]
    recall_denominator_set: HistoricalRangeRecallDenominatorSetV1 = Field(
        default_factory=HistoricalRangeRecallDenominatorSetV1.not_configured
    )

    @field_validator("resolved_request_hash")
    @classmethod
    def _request_hash(cls, value: str) -> str:
        return require_sha256(value, field_name="resolved_request_hash")


class HistoricalRangeSummaryOutcomeSetLoader(Protocol):
    def load(
        self,
        *,
        range_run_id: str,
        label_as_of_trade_date: date,
        policy: HistoricalRangeSummaryPolicyV1,
    ) -> HistoricalRangeSummaryOutcomeSetV1: ...


class HistoricalRangeSummarySourceRepository(Protocol):
    def list_outcomes_for_summary(
        self, *, range_run_id: str, label_as_of_trade_date: date
    ) -> tuple[HistoricalRangeOutcomeFactV1, ...]: ...

    def load_run_resolved_request_hash(self, *, range_run_id: str) -> str: ...


class HistoricalRangeSummaryRowContextV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    canonical_signal_id: str = Field(min_length=1, max_length=240)
    decision_trade_date: date
    list_version_id: str | None = Field(default=None, min_length=1, max_length=160)
    symbol: str | None = Field(default=None, min_length=1, max_length=32)
    strategy_rank: int | None = Field(default=None, ge=1)
    episode_closed: bool | None = None
    industry_at_t: str | None = Field(default=None, min_length=1, max_length=160)
    market_regime_at_t: str | None = Field(default=None, min_length=1, max_length=160)


class HistoricalRangeSummaryContextProvider(Protocol):
    def load(
        self,
        *,
        fact: HistoricalRangeOutcomeFactV1,
        artifact: HistoricalRangeOutcomeArtifactV2,
        calculation: OutcomeCalculationResult | None,
    ) -> HistoricalRangeSummaryRowContextV1: ...


class HistoricalRangeRecallDenominatorProvider(Protocol):
    def load(
        self,
        *,
        range_run_id: str,
        rows: tuple[HistoricalRangeSummaryInputRowV1, ...],
        policy: HistoricalRangeSummaryPolicyV1,
    ) -> HistoricalRangeRecallDenominatorSetV1: ...


class PostgresHistoricalRangeRecallDenominatorProvider:
    """Read Recall only from exact uninvalidated SEALED Phase 1 snapshots."""

    def __init__(
        self,
        *,
        conn_factory,
        dataset_store: LocalContentAddressedStore,
        calculation_evidence_reader: Any,
        winner_definitions: tuple[Phase1WinnerDefinitionV1, ...],
    ) -> None:
        if conn_factory is None or dataset_store is None or calculation_evidence_reader is None:
            raise ValueError("Recall provider requires DB, dataset CAS, and evidence CAS")
        self._conn_factory = conn_factory
        self._dataset_store = dataset_store
        self._evidence_reader = calculation_evidence_reader
        self._winner_definitions = tuple(
            sorted(
                winner_definitions,
                key=lambda item: str(item.winner_definition_hash),
            )
        )
        if not self._winner_definitions or len(
            {item.winner_definition_hash for item in self._winner_definitions}
        ) != len(self._winner_definitions):
            raise ValueError("Recall provider requires unique frozen winner definitions")

    def load(
        self,
        *,
        range_run_id: str,
        rows: tuple[HistoricalRangeSummaryInputRowV1, ...],
        policy: HistoricalRangeSummaryPolicyV1,
    ) -> HistoricalRangeRecallDenominatorSetV1:
        groups: dict[
            tuple[date, HistoricalRangeOutcomeProjection, str, int],
            tuple[HistoricalRangeSummaryInputRowV1, ...],
        ] = {}
        for row in rows:
            if (
                row.subject_type is not HistoricalRangeOutcomeSubjectType.CANDIDATE
                or row.evaluation_window_type
                is not HistoricalRangeEvaluationWindowType.FIXED_HORIZON
                or row.horizon_trade_days < 1
                or row.symbol is None
            ):
                continue
            key = (
                row.decision_trade_date,
                row.projection_group,
                row.projection,
                row.horizon_trade_days,
            )
            groups[key] = (*groups.get(key, ()), row)

        denominators: list[HistoricalRangeRecallDenominatorV1] = []
        unavailable_reasons: set[str] = set()
        for (decision_date, projection_group, projection, horizon), group in sorted(
            groups.items(), key=lambda item: item[0]
        ):
            if any(item.strategy_rank is None for item in group):
                unavailable_reasons.add("PIT_STRATEGY_RANK_UNAVAILABLE")
                continue
            definitions = tuple(
                item
                for item in self._winner_definitions
                if item.projection == projection
                and item.horizon_trade_days == horizon
            )
            if len(definitions) != 1:
                unavailable_reasons.add("PIT_WINNER_DEFINITION_UNAVAILABLE")
                continue
            winner = definitions[0]
            try:
                refs = self._load_snapshot_evidence(
                    decision_date=decision_date,
                    projection=projection,
                    horizon=horizon,
                    winner=winner,
                )
            except (ValueError, psycopg2.Error) as error:
                unavailable_reasons.add(
                    str(getattr(error, "reason_code", "PIT_SNAPSHOT_EVIDENCE_UNAVAILABLE"))
                )
                continue
            eligible_symbols = tuple(sorted(item.symbol for item in refs))
            if not refs or len(eligible_symbols) != len(set(eligible_symbols)):
                unavailable_reasons.add("PIT_ELIGIBLE_UNIVERSE_INCOMPLETE")
                continue
            ranked_strategy = tuple(
                symbol
                for symbol, _rank in sorted(
                    {
                        str(item.symbol).upper(): int(item.strategy_rank)
                        for item in group
                    }.items(),
                    key=lambda item: (item[1], item[0]),
                )
            )
            for condition_key, condition_refs, condition_strategy in self._conditions(
                refs=refs,
                strategy_group=group,
                ranked_strategy=ranked_strategy,
            ):
                numeric = tuple(
                    item
                    for item in condition_refs
                    if item.maturity_status == "MATURED"
                    and item.projection_value_decimal is not None
                )
                eligible_targets = tuple(
                    item
                    for item in numeric
                    if winner.matches(item.projection_value_decimal)
                )
                ranked_targets = tuple(
                    item.symbol
                    for item in sorted(
                        eligible_targets,
                        key=lambda item: (
                            (
                                -item.projection_value_decimal
                                if winner.ranking_direction == "DESC"
                                else item.projection_value_decimal
                            ),
                            item.symbol,
                        ),
                    )
                )
                for k in policy.recall_k_values:
                    target_symbols = tuple(sorted(ranked_targets[:k]))
                    condition_hash = (
                        canonical_json_sha256(
                            {
                                "condition_key": condition_key,
                                "row_hashes": [
                                    item.canonical_row_hash for item in condition_refs
                                ],
                            }
                        )
                        if condition_key is not None
                        else None
                    )
                    denominators.append(
                        HistoricalRangeRecallDenominatorV1(
                            decision_trade_date=decision_date,
                            projection_group=projection_group,
                            projection=projection,
                            horizon_trade_days=horizon,
                            condition_key=condition_key,
                            k=k,
                            strategy_symbols=condition_strategy,
                            positive_target_symbols=target_symbols,
                            eligible_universe_symbols=tuple(
                                sorted(item.symbol for item in condition_refs)
                            ),
                            eligible_universe_refs=condition_refs,
                            eligible_universe_set_hash=canonical_json_sha256(
                                {
                                    "decision_trade_date": decision_date,
                                    "eligible_symbols": tuple(
                                        sorted(item.symbol for item in condition_refs)
                                    ),
                                    "eligible_outcome_refs": [
                                        item.model_dump(mode="json")
                                        for item in condition_refs
                                    ],
                                }
                            ),
                            eligible_universe_source_hash=canonical_json_sha256(
                                {
                                    "snapshot_ids": sorted(
                                        {item.snapshot_id for item in condition_refs}
                                    ),
                                    "file_hashes": sorted(
                                        {item.file_sha256 for item in condition_refs}
                                    ),
                                }
                            ),
                            label_policy_bundle_hash=condition_refs[0].label_policy_bundle_hash,
                            label_policy_hash=condition_refs[0].label_policy_hash,
                            universe_policy_hash=condition_refs[0].universe_policy_hash,
                            universe_layer=condition_refs[0].universe_layer,
                            source_revision_set_hash=canonical_json_sha256(
                                sorted(
                                    {
                                        item.label_source_revision_set_hash
                                        for item in condition_refs
                                    }
                                )
                            ),
                            winner_definition=winner,
                            condition_evidence_hash=condition_hash,
                        )
                    )
        if denominators:
            return HistoricalRangeRecallDenominatorSetV1(
                availability="AVAILABLE",
                denominators=tuple(denominators),
            )
        return HistoricalRangeRecallDenominatorSetV1(
            availability="UNAVAILABLE",
            reason_codes=tuple(sorted(unavailable_reasons))
            or ("PIT_ELIGIBLE_DENOMINATOR_UNAVAILABLE",),
        )

    def _load_snapshot_evidence(
        self,
        *,
        decision_date: date,
        projection: str,
        horizon: int,
        winner: Phase1WinnerDefinitionV1,
    ) -> tuple[Phase1UniverseOutcomeEvidenceRefV1, ...]:
        with self._conn_factory() as conn:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT snapshot.snapshot_id, snapshot.snapshot_content_hash,
                           snapshot.manifest_sha256,
                           snapshot.snapshot_source_revision_set_hash,
                           snapshot.build_id,
                           file.logical_path, file.logical_role,
                           file.partition_key_hash, file.ordinal, file.content_uri,
                           file.sha256, file.size_bytes, file.row_count,
                           file.schema_fingerprint, file.partition_content_hash,
                           file.store_backend_hash, file.blob_sha256,
                           build.build_request_payload_jsonb
                    FROM app.advisory_dataset_snapshot snapshot
                    JOIN app.advisory_dataset_build build
                      ON build.build_id = snapshot.build_id
                    JOIN app.advisory_dataset_snapshot_file file
                      ON file.snapshot_id = snapshot.snapshot_id
                    WHERE snapshot.snapshot_state = 'SEALED'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM app.advisory_dataset_snapshot_invalidation invalidation
                          WHERE invalidation.snapshot_id = snapshot.snapshot_id
                      )
                      AND COALESCE(snapshot.lineage_identity_type, 'PHASE0A') = 'PHASE0A'
                      AND file.logical_role IN (
                          'universe_outcomes', 'outcome_source_evidence',
                          'source_revisions'
                      )
                    ORDER BY snapshot.snapshot_id, file.logical_path
                    """,
                )
                result = tuple(dict(row) for row in cur.fetchall())
            conn.rollback()
        by_snapshot: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in result:
            request = dict(row["build_request_payload_jsonb"])
            targets = request.get("label_targets")
            if (
                request.get("label_policy_bundle_hash") is None
                or request.get("universe_policy_hash") is None
                or not isinstance(targets, list)
                or not any(
                    int(item.get("horizon_trading_days", -1)) == horizon
                    and item.get("projection") == projection
                    for item in targets
                    if isinstance(item, dict)
                )
            ):
                continue
            by_snapshot[str(row["snapshot_id"])].append(row)
        matching: list[tuple[dict[str, Any], ...]] = []
        for snapshot_rows in by_snapshot.values():
            evidence = self._read_snapshot_rows(snapshot_rows)
            selected = tuple(
                item
                for item in evidence
                if item.decision_trade_date == decision_date
                and item.projection == projection
                and item.horizon_trade_days == horizon
                and item.label_policy_hash == winner.label_policy_hash
                and item.universe_layer == winner.denominator_universe_layer
            )
            if selected:
                matching.append(selected)
        if len(matching) != 1:
            raise HistoricalRangeSummaryError(
                "PIT_SNAPSHOT_EVIDENCE_AMBIGUOUS"
            )
        return matching[0]

    def _read_snapshot_rows(
        self, snapshot_rows: list[dict[str, Any]]
    ) -> tuple[Phase1UniverseOutcomeEvidenceRefV1, ...]:
        header = snapshot_rows[0]
        manifest_payload = self._dataset_store.read_document_bytes(
            kind="manifests", sha256=str(header["manifest_sha256"])
        )
        manifest = DatasetManifest.model_validate(json.loads(manifest_payload))
        if (
            manifest.manifest_sha256 != header["manifest_sha256"]
            or manifest.core.manifest_core_sha256
            != header["snapshot_content_hash"]
        ):
            raise HistoricalRangeSummaryError("PIT_SNAPSHOT_MANIFEST_CONFLICT")
        files = tuple(self._snapshot_file(item) for item in snapshot_rows)
        if canonicalize(
            [
                item.model_dump(mode="json")
                for item in sorted(
                    manifest.core.files, key=lambda value: value.logical_path
                )
            ]
        ) != canonicalize(
            [
                item.model_dump(mode="json")
                for item in sorted(files, key=lambda value: value.logical_path)
            ]
        ):
            raise HistoricalRangeSummaryError("PIT_SNAPSHOT_FILE_SET_CONFLICT")
        universe_rows: list[tuple[DatasetSnapshotFile, dict[str, Any]]] = []
        evidence_by_id: dict[str, dict[str, Any]] = {}
        source_revision_rows: list[dict[str, Any]] = []
        for file in files:
            rows = read_verified_snapshot_parquet_rows(
                file=file,
                store=self._dataset_store,
                lineage_identity_type="PHASE0A",
            )
            if file.logical_role == "universe_outcomes":
                universe_rows.extend((file, item) for item in rows)
            elif file.logical_role == "outcome_source_evidence":
                for item in rows:
                    raw_id = item.get("raw_outcome_id")
                    if raw_id is not None:
                        if raw_id in evidence_by_id:
                            raise HistoricalRangeSummaryError(
                                "PIT_SNAPSHOT_EVIDENCE_DUPLICATED"
                            )
                        evidence_by_id[str(raw_id)] = item
            else:
                source_revision_rows.extend(rows)
        if not source_revision_rows:
            raise HistoricalRangeSummaryError("PIT_SOURCE_REVISION_EVIDENCE_MISSING")
        refs: list[Phase1UniverseOutcomeEvidenceRefV1] = []
        for file, row in universe_rows:
            raw_id = str(row["raw_outcome_id"])
            evidence = evidence_by_id.get(raw_id)
            if evidence is None:
                raise HistoricalRangeSummaryError("PIT_CALCULATION_EVIDENCE_MISSING")
            bundle = self._evidence_reader.get(
                uri=str(row["calculation_evidence_uri"]),
                sha256=str(row["calculation_evidence_sha256"]),
                size_bytes=int(row["calculation_evidence_size_bytes"]),
                store_backend_hash=str(
                    row["calculation_evidence_store_backend_hash"]
                ),
            )
            if canonicalize(bundle.model_dump(mode="json")) != canonicalize(
                evidence["calculation_evidence_json"]
            ):
                raise HistoricalRangeSummaryError(
                    "PIT_CALCULATION_EVIDENCE_CONFLICT"
                )
            payload = {
                "snapshot_id": str(header["snapshot_id"]),
                "snapshot_content_hash": str(header["snapshot_content_hash"]),
                "manifest_sha256": str(header["manifest_sha256"]),
                "snapshot_source_revision_set_hash": str(
                    header["snapshot_source_revision_set_hash"]
                ),
                "logical_path": file.logical_path,
                "file_sha256": file.sha256,
                "file_size_bytes": file.size_bytes,
                "file_row_count": file.row_count,
                "partition_content_hash": file.partition_content_hash,
                **{
                    key: row[key]
                    for key in Phase1UniverseOutcomeEvidenceRefV1.model_fields
                    if key
                    not in {
                        "snapshot_id", "snapshot_content_hash", "manifest_sha256",
                        "snapshot_source_revision_set_hash", "logical_path",
                        "file_sha256", "file_size_bytes", "file_row_count",
                        "partition_content_hash", "canonical_row_hash",
                    }
                },
            }
            payload["canonical_row_hash"] = canonical_json_sha256(payload)
            refs.append(Phase1UniverseOutcomeEvidenceRefV1.model_validate(payload))
        return tuple(sorted(refs, key=lambda item: item.symbol))

    @staticmethod
    def _snapshot_file(row: Mapping[str, Any]) -> DatasetSnapshotFile:
        from backend.services.advisory_phase1.dataset_build import DatasetBlobHeader

        return DatasetSnapshotFile(
            logical_path=str(row["logical_path"]),
            logical_role=str(row["logical_role"]),
            partition_key_hash=str(row["partition_key_hash"]),
            ordinal=int(row["ordinal"]),
            content_uri=str(row["content_uri"]),
            sha256=str(row["sha256"]),
            size_bytes=int(row["size_bytes"]),
            row_count=int(row["row_count"]),
            schema_fingerprint=str(row["schema_fingerprint"]),
            partition_content_hash=str(row["partition_content_hash"]),
            blob=DatasetBlobHeader(
                store_backend_hash=str(row["store_backend_hash"]),
                blob_sha256=str(row["blob_sha256"]),
                size_bytes=int(row["size_bytes"]),
            ),
        )

    @staticmethod
    def _conditions(
        *,
        refs: tuple[Phase1UniverseOutcomeEvidenceRefV1, ...],
        strategy_group: tuple[HistoricalRangeSummaryInputRowV1, ...],
        ranked_strategy: tuple[str, ...],
    ) -> tuple[
        tuple[
            str | None,
            tuple[Phase1UniverseOutcomeEvidenceRefV1, ...],
            tuple[str, ...],
        ],
        ...,
    ]:
        conditions: list[
            tuple[
                str | None,
                tuple[Phase1UniverseOutcomeEvidenceRefV1, ...],
                tuple[str, ...],
            ]
        ] = [(None, refs, ranked_strategy)]
        industry_keys = {
            item.industry_at_t or "UNKNOWN_AT_T" for item in strategy_group
        }
        for industry in sorted(industry_keys):
            selected = tuple(item for item in refs if item.industry_at_t == industry)
            if selected and all(item.industry_evidence_hash is not None for item in selected):
                strategy = tuple(
                    item.symbol
                    for item in strategy_group
                    if (item.industry_at_t or "UNKNOWN_AT_T") == industry
                )
                conditions.append((f"INDUSTRY:{industry}", selected, strategy))
        regime_keys = {
            item.market_regime_at_t
            for item in strategy_group
            if item.market_regime_at_t is not None
        }
        for regime in sorted(regime_keys):
            selected = tuple(item for item in refs if item.market_regime_at_t == regime)
            if selected and all(
                item.market_regime_evidence_hash is not None for item in selected
            ):
                strategy = tuple(
                    item.symbol
                    for item in strategy_group
                    if item.market_regime_at_t == regime
                )
                conditions.append((f"REGIME:{regime}", selected, strategy))
        return tuple(conditions)


class PostgresHistoricalRangeSummaryContextProvider:
    """Resolve only decision-T context; current/latest lookups are forbidden."""

    def __init__(
        self,
        *,
        conn_factory,
        artifact_store: HistoricalRangeArtifactStore,
        source_provider: PostgresHistoricalRangeOutcomeSourceProvider,
    ) -> None:
        self._conn_factory = conn_factory
        self._artifact_store = artifact_store
        self._source_provider = source_provider
        self._metadata_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._industry_cache: dict[tuple[str, date], str] = {}

    def load(
        self,
        *,
        fact: HistoricalRangeOutcomeFactV1,
        artifact: HistoricalRangeOutcomeArtifactV2,
        calculation: OutcomeCalculationResult | None,
    ) -> HistoricalRangeSummaryRowContextV1:
        metadata_key = (
            fact.subject_type.value,
            fact.subject_id,
            artifact.subject_ref.semantic_content_hash,
        )
        metadata = self._metadata_cache.get(metadata_key)
        if metadata is None:
            metadata = self._subject_metadata(fact, subject_ref=artifact.subject_ref)
            self._metadata_cache[metadata_key] = metadata
        symbol = (
            str(calculation.owner.symbol).upper()
            if calculation is not None
            else metadata.get("symbol")
        )
        decision_date = (
            calculation.decision_trade_date
            if calculation is not None
            else metadata["decision_trade_date"]
        )
        industry = None
        if symbol is not None:
            industry_key = (symbol, decision_date)
            industry = self._industry_cache.get(industry_key)
            if industry is None:
                industry = self._load_summary_industry_at_t(
                    symbol=symbol,
                    decision_trade_date=decision_date,
                )
                self._industry_cache[industry_key] = industry
        canonical_signal_id = self._canonical_signal_id(
            fact=fact,
            calculation=calculation,
        )
        return HistoricalRangeSummaryRowContextV1(
            canonical_signal_id=canonical_signal_id,
            decision_trade_date=decision_date,
            list_version_id=metadata.get("list_version_id"),
            symbol=symbol,
            strategy_rank=(
                int(metadata["strategy_rank"])
                if metadata.get("strategy_rank") is not None
                else None
            ),
            episode_closed=metadata.get("episode_closed"),
            industry_at_t=industry,
            market_regime_at_t=self._regime_from_subject_ref(artifact.subject_ref),
        )

    @staticmethod
    def _canonical_signal_id(
        *,
        fact: HistoricalRangeOutcomeFactV1,
        calculation: OutcomeCalculationResult | None,
    ) -> str:
        if calculation is not None and isinstance(calculation.owner, OutcomeOwner):
            return calculation.owner.canonical_signal_id
        return f"range-subject:{fact.subject_type.value}:{fact.subject_id}"

    def _load_summary_industry_at_t(
        self,
        *,
        symbol: str,
        decision_trade_date: date,
    ) -> str:
        with self._conn_factory() as conn:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT l1_code, l1_name, l2_code, l2_name,
                           l3_code, l3_name, in_date, out_date
                    FROM market.sw_index_member
                    WHERE ts_code = %s
                      AND in_date <= %s
                      AND (out_date IS NULL OR out_date >= %s)
                    ORDER BY in_date DESC, out_date DESC NULLS LAST,
                             l3_code NULLS LAST, l2_code NULLS LAST,
                             l1_code NULLS LAST
                    """,
                    (symbol.upper(), decision_trade_date, decision_trade_date),
                )
                rows = tuple(dict(item) for item in cur.fetchall())
            conn.rollback()
        if not rows:
            return "UNKNOWN_AT_T"
        latest_in_date = rows[0]["in_date"]
        latest = tuple(row for row in rows if row["in_date"] == latest_in_date)
        industry_fields = (
            "l1_code",
            "l1_name",
            "l2_code",
            "l2_name",
            "l3_code",
            "l3_name",
        )
        distinct = {
            tuple(row.get(field) for field in industry_fields) for row in latest
        }
        if len(distinct) > 1:
            raise HistoricalRangeOutcomeSourceError(
                "ADVISORY_HR_OUTCOME_INDUSTRY_MEMBERSHIP_CONFLICT",
                "latest effective PIT industry memberships conflict at T",
                context={
                    "symbol": symbol.upper(),
                    "decision_trade_date": decision_trade_date.isoformat(),
                    "latest_in_date": latest_in_date.isoformat(),
                    "latest_membership_count": len(latest),
                    "distinct_industry_count": len(distinct),
                    "resolution_policy": "LATEST_EFFECTIVE_IN_DATE_V1",
                },
            )
        selected = latest[0]
        return str(
            selected.get("l3_code")
            or selected.get("l2_code")
            or selected.get("l1_code")
            or "UNKNOWN_AT_T"
        )

    def _subject_metadata(
        self,
        fact: HistoricalRangeOutcomeFactV1,
        *,
        subject_ref: HistoricalRangeArtifactRefV1,
    ) -> dict[str, Any]:
        with self._conn_factory() as conn:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if fact.subject_type is HistoricalRangeOutcomeSubjectType.CANDIDATE:
                    cur.execute(
                        """
                        SELECT candidate.symbol, candidate.selection_effective_rank AS strategy_rank,
                               day.decision_trade_date,
                               CASE WHEN item.action IN ('ENTER', 'HOLD')
                                    THEN list.list_version_id ELSE NULL END AS list_version_id,
                               NULL::BOOLEAN AS episode_closed
                        FROM app.advisory_historical_range_candidate candidate
                        JOIN app.advisory_historical_range_day_run day
                          ON day.day_run_id = candidate.day_run_id
                        LEFT JOIN app.advisory_historical_range_list_version list
                          ON list.day_run_id = day.day_run_id
                        LEFT JOIN app.advisory_historical_range_list_item item
                          ON item.list_version_id = list.list_version_id
                         AND item.symbol = candidate.symbol
                        WHERE candidate.candidate_id = %s
                          AND candidate.artifact_ref = %s
                        """,
                        (
                            fact.subject_id,
                            psycopg2.extras.Json(subject_ref.model_dump(mode="json")),
                        ),
                    )
                elif fact.subject_type is HistoricalRangeOutcomeSubjectType.EPISODE:
                    cur.execute(
                        """
                        SELECT episode.symbol, NULL::INTEGER AS strategy_rank,
                               episode.enter_decision_trade_date AS decision_trade_date,
                               episode.list_version_id,
                               (episode.recommendation_state = 'EXITED') AS episode_closed
                        FROM app.advisory_historical_range_episode_snapshot episode
                        JOIN app.advisory_historical_range_list_version list
                          ON list.list_version_id = episode.list_version_id
                        JOIN app.advisory_historical_range_day_run day
                          ON day.day_run_id = list.day_run_id
                        WHERE episode.episode_id = %s
                          AND day.day_receipt_ref = %s
                        """,
                        (
                            fact.subject_id,
                            psycopg2.extras.Json(subject_ref.model_dump(mode="json")),
                        ),
                    )
                elif fact.subject_type is HistoricalRangeOutcomeSubjectType.LIST_VERSION:
                    cur.execute(
                        """
                        SELECT NULL::TEXT AS symbol, NULL::INTEGER AS strategy_rank,
                               day.decision_trade_date,
                               list.list_version_id, NULL::BOOLEAN AS episode_closed
                        FROM app.advisory_historical_range_list_version list
                        JOIN app.advisory_historical_range_day_run day
                          ON day.day_run_id = list.day_run_id
                        WHERE list.list_version_id = %s
                          AND day.day_receipt_ref = %s
                        """,
                        (
                            fact.subject_id,
                            psycopg2.extras.Json(subject_ref.model_dump(mode="json")),
                        ),
                    )
                else:
                    cur.execute(
                        """
                        SELECT NULL::TEXT AS symbol, NULL::INTEGER AS strategy_rank,
                               MIN(day.decision_trade_date) AS decision_trade_date,
                               NULL::TEXT AS list_version_id,
                               NULL::BOOLEAN AS episode_closed
                        FROM app.advisory_historical_range_run run
                        JOIN app.advisory_historical_range_day_run day
                          ON day.range_run_id = run.range_run_id
                        WHERE run.range_run_id = %s
                          AND run.final_receipt_ref = %s
                        """,
                        (
                            fact.subject_id,
                            psycopg2.extras.Json(subject_ref.model_dump(mode="json")),
                        ),
                    )
                rows = tuple(dict(row) for row in cur.fetchall())
            conn.rollback()
        if len(rows) != 1 or rows[0]["decision_trade_date"] is None:
            raise HistoricalRangeSummaryError(
                "summary subject metadata is unavailable or ambiguous"
            )
        return rows[0]

    def _regime_from_subject_ref(
        self, subject_ref: HistoricalRangeArtifactRefV1
    ) -> str | None:
        envelope = self._artifact_store.load(subject_ref)
        payload = envelope.payload
        if subject_ref.artifact_kind is HistoricalRangeArtifactKind.DAY_RECEIPT:
            candidate_ref = payload.get("candidate_artifact_ref")
            if isinstance(candidate_ref, dict):
                envelope = self._artifact_store.load(
                    HistoricalRangeArtifactRefV1.model_validate(candidate_ref)
                )
                payload = envelope.payload
        trace = payload.get("stage_trace")
        metadata = trace.get("metadata") if isinstance(trace, dict) else None
        hmm = metadata.get("hmm") if isinstance(metadata, dict) else None
        if not isinstance(hmm, dict):
            return None
        values = {
            str(hmm[key])
            for key in ("market_regime", "regime", "regime_label", "state_label")
            if hmm.get(key) not in (None, "")
        }
        if len(values) > 1:
            raise HistoricalRangeSummaryError("frozen T-date regime evidence is conflicting")
        return next(iter(values), None)


class PostgresHistoricalRangeSummaryOutcomeSetLoader:
    def __init__(
        self,
        *,
        repository: HistoricalRangeSummarySourceRepository,
        artifact_store: HistoricalRangeArtifactStore,
        context_provider: HistoricalRangeSummaryContextProvider,
        denominator_provider: HistoricalRangeRecallDenominatorProvider | None = None,
    ) -> None:
        self._repository = repository
        self._artifact_store = artifact_store
        self._context_provider = context_provider
        self._denominator_provider = denominator_provider

    def load(
        self,
        *,
        range_run_id: str,
        label_as_of_trade_date: date,
        policy: HistoricalRangeSummaryPolicyV1,
    ) -> HistoricalRangeSummaryOutcomeSetV1:
        versions = self._repository.list_outcomes_for_summary(
            range_run_id=range_run_id,
            label_as_of_trade_date=label_as_of_trade_date,
        )
        latest: dict[str, HistoricalRangeOutcomeFactV1] = {}
        for fact in versions:
            previous = latest.get(fact.outcome_logical_id)
            if previous is None or fact.outcome_version > previous.outcome_version:
                latest[fact.outcome_logical_id] = fact
        rows: list[HistoricalRangeSummaryInputRowV1] = []
        for fact in sorted(latest.values(), key=lambda item: item.outcome_logical_id):
            if (
                fact.subject_type not in policy.subject_types
                or fact.projection not in policy.projection_groups
                or fact.evaluation_window_type not in policy.evaluation_window_types
                or fact.horizon_trade_days not in policy.horizons
                or fact.historical_range_policy_bundle_hash
                != policy.outcome_policy_bundle_hash
            ):
                continue
            envelope = self._artifact_store.load(fact.outcome_artifact_ref)
            artifact = HistoricalRangeOutcomeArtifactV2.model_validate(envelope.payload)
            if canonicalize(artifact.model_dump(mode="json")) != canonicalize(fact.outcome_json):
                raise HistoricalRangeSummaryError("outcome DB/artifact payloads differ")
            calculations = tuple(
                HistoricalRangeAggregateCalculationResultV1.model_validate(item)
                if item.get("calculation_kind") == "AGGREGATE_COHORT"
                else OutcomeCalculationResult.model_validate(item)
                for item in artifact.calculation_results
            )
            if not calculations:
                context = self._context_provider.load(
                    fact=fact,
                    artifact=artifact,
                    calculation=None,
                )
                rows.append(
                    HistoricalRangeSummaryInputRowV1(
                        outcome_ref=fact.outcome_artifact_ref,
                        outcome_logical_id=fact.outcome_logical_id,
                        canonical_signal_id=context.canonical_signal_id,
                        subject_type=fact.subject_type,
                        subject_id=fact.subject_id,
                        projection_group=fact.projection,
                        projection="OUTER_GROUP",
                        evaluation_window_type=fact.evaluation_window_type,
                        horizon_trade_days=fact.horizon_trade_days,
                        decision_trade_date=context.decision_trade_date,
                        list_version_id=context.list_version_id,
                        symbol=context.symbol,
                        strategy_rank=context.strategy_rank,
                        maturity_status=fact.maturity_status,
                        episode_closed=context.episode_closed,
                        industry_at_t=context.industry_at_t,
                        market_regime_at_t=context.market_regime_at_t,
                    )
                )
                continue
            aggregate_kinds = {
                isinstance(item, HistoricalRangeAggregateCalculationResultV1)
                for item in calculations
            }
            if len(aggregate_kinds) != 1:
                raise HistoricalRangeSummaryError(
                    "one outcome artifact cannot mix aggregate and position calculations"
                )
            aggregate_calculations = True in aggregate_kinds
            context = self._context_provider.load(
                fact=fact,
                artifact=artifact,
                calculation=(None if aggregate_calculations else calculations[0]),
            )
            for calculation in calculations:
                if isinstance(calculation, HistoricalRangeAggregateCalculationResultV1):
                    rows.append(
                        HistoricalRangeSummaryInputRowV1(
                            outcome_ref=fact.outcome_artifact_ref,
                            outcome_logical_id=fact.outcome_logical_id,
                            canonical_signal_id=context.canonical_signal_id,
                            subject_type=fact.subject_type,
                            subject_id=fact.subject_id,
                            projection_group=fact.projection,
                            projection=calculation.projection.value,
                            evaluation_window_type=fact.evaluation_window_type,
                            horizon_trade_days=fact.horizon_trade_days,
                            decision_trade_date=context.decision_trade_date,
                            list_version_id=context.list_version_id,
                            symbol=context.symbol,
                            strategy_rank=context.strategy_rank,
                            maturity_status=calculation.maturity_status,
                            value=(
                                calculation.projection_value_decimal
                                if calculation.maturity_status
                                in {
                                    HistoricalRangeOutcomeStatus.COMPLETE,
                                    HistoricalRangeOutcomeStatus.TERMINAL,
                                }
                                else None
                            ),
                            episode_closed=context.episode_closed,
                            industry_at_t=context.industry_at_t,
                            market_regime_at_t=context.market_regime_at_t,
                        )
                    )
                    continue
                calculation_maturity = map_historical_range_maturity(calculation)
                rows.append(
                    HistoricalRangeSummaryInputRowV1(
                        outcome_ref=fact.outcome_artifact_ref,
                        outcome_logical_id=fact.outcome_logical_id,
                        canonical_signal_id=context.canonical_signal_id,
                        subject_type=fact.subject_type,
                        subject_id=fact.subject_id,
                        projection_group=fact.projection,
                        projection=calculation.projection.value,
                        evaluation_window_type=fact.evaluation_window_type,
                        horizon_trade_days=fact.horizon_trade_days,
                        decision_trade_date=context.decision_trade_date,
                        list_version_id=context.list_version_id,
                        symbol=context.symbol,
                        strategy_rank=context.strategy_rank,
                        maturity_status=calculation_maturity,
                        value=(
                            calculation.projection_value_decimal
                            if calculation_maturity
                            in {
                                HistoricalRangeOutcomeStatus.COMPLETE,
                                HistoricalRangeOutcomeStatus.TERMINAL,
                            }
                            else None
                        ),
                        observed_holding_trading_days=calculation.observed_holding_trading_days,
                        episode_closed=context.episode_closed,
                        industry_at_t=context.industry_at_t,
                        market_regime_at_t=context.market_regime_at_t,
                    )
                )
        materialized = tuple(rows)
        denominator_set = (
            self._denominator_provider.load(
                range_run_id=range_run_id,
                rows=materialized,
                policy=policy,
            )
            if self._denominator_provider is not None
            else HistoricalRangeRecallDenominatorSetV1.not_configured()
        )
        return HistoricalRangeSummaryOutcomeSetV1(
            range_run_id=range_run_id,
            label_as_of_trade_date=label_as_of_trade_date,
            resolved_request_hash=self._repository.load_run_resolved_request_hash(
                range_run_id=range_run_id
            ),
            rows=materialized,
            recall_denominator_set=denominator_set,
        )


class HistoricalRangeSummaryCoordinatorService:
    """Freeze the latest eligible outcome set and append one exact summary version."""

    def __init__(
        self,
        *,
        repository: HistoricalRangeSummaryRepository,
        artifact_store: HistoricalRangeArtifactStore,
        outcome_set_loader: HistoricalRangeSummaryOutcomeSetLoader,
        policy: HistoricalRangeSummaryPolicyV1,
        label_as_of_trade_date: date,
        producer_code_hash: str,
        calculator: "HistoricalRangeSummaryService | None" = None,
    ) -> None:
        self._repository = repository
        self._artifact_store = artifact_store
        self._outcome_set_loader = outcome_set_loader
        self._policy = policy
        self._label_as_of_trade_date = label_as_of_trade_date
        self._producer_code_hash = require_sha256(
            producer_code_hash, field_name="producer_code_hash"
        )
        self._calculator = calculator or HistoricalRangeSummaryService()

    def refresh(self, *, range_run_id: str) -> HistoricalRangeArtifactRefV1:
        frozen = self._outcome_set_loader.load(
            range_run_id=range_run_id,
            label_as_of_trade_date=self._label_as_of_trade_date,
            policy=self._policy,
        )
        if frozen.range_run_id != range_run_id:
            raise HistoricalRangeSummaryError("outcome-set loader returned another range run")
        artifact = self._calculator.calculate(
            range_run_id=range_run_id,
            rows=frozen.rows,
            policy=self._policy,
            producer_code_hash=self._producer_code_hash,
            recall_denominator_set=frozen.recall_denominator_set,
        )
        existing = self._repository.find_summary_by_input(
            range_run_id=range_run_id,
            summary_input_hash=artifact.summary_input_hash,
        )
        if existing is not None:
            envelope = self._artifact_store.load(existing.summary_artifact_ref)
            existing_artifact = HistoricalRangeSummaryArtifactV2.model_validate(
                envelope.payload
            )
            retry_payload = artifact.model_dump(mode="python")
            retry_payload["predecessor_summary_ref"] = (
                existing_artifact.predecessor_summary_ref
            )
            retry_artifact = HistoricalRangeSummaryArtifactV2.model_validate(
                retry_payload
            )
            if canonicalize(existing_artifact.model_dump(mode="json")) != canonicalize(
                retry_artifact.model_dump(mode="json")
            ):
                raise HistoricalRangeSummaryError("summary exact retry payload differs")
            return existing.summary_artifact_ref
        predecessor = self._repository.load_latest_summary(range_run_id=range_run_id)
        version = 1 if predecessor is None else predecessor.summary_version + 1
        artifact_payload = artifact.model_dump(mode="python")
        artifact_payload["predecessor_summary_ref"] = (
            predecessor.summary_artifact_ref if predecessor is not None else None
        )
        artifact = HistoricalRangeSummaryArtifactV2.model_validate(
            artifact_payload
        )
        stored = self._artifact_store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.SUMMARY,
            producer_contract_version="advisory_phase1r_r4_summary_v1",
            payload_schema_version=artifact.schema_version,
            resolved_request_hash=frozen.resolved_request_hash,
            payload=artifact.model_dump(mode="json"),
            range_run_id=range_run_id,
            upstream_refs=tuple(
                (*artifact.covered_outcome_refs,)
                + (
                    (artifact.predecessor_summary_ref,)
                    if artifact.predecessor_summary_ref is not None
                    else ()
                )
            ),
        )
        readback = self._artifact_store.load(stored.ref)
        HistoricalRangeSummaryArtifactV2.model_validate(readback.payload)
        fact = self._calculator.build_fact(
            artifact=artifact,
            summary_artifact_ref=stored.ref,
            summary_version=version,
            predecessor=predecessor,
        )
        self._repository.append_summary(fact)
        return stored.ref


class HistoricalRangeSummaryService:
    """Compute every metric independently by projection/window/horizon key."""

    def calculate(
        self,
        *,
        range_run_id: str,
        rows: Iterable[HistoricalRangeSummaryInputRowV1],
        policy: HistoricalRangeSummaryPolicyV1,
        producer_code_hash: str,
        recall_denominator_set: HistoricalRangeRecallDenominatorSetV1 | None = None,
    ) -> HistoricalRangeSummaryArtifactV2:
        producer_code_hash = require_sha256(producer_code_hash, field_name="producer_code_hash")
        materialized = tuple(rows)
        refs_by_hash = {item.outcome_ref.semantic_content_hash: item.outcome_ref for item in materialized}
        refs = tuple(refs_by_hash[key] for key in sorted(refs_by_hash))
        covered_hash = canonical_json_sha256([item.model_dump(mode="json") for item in refs])
        grouped: dict[tuple[str, str, str, str, int], list[HistoricalRangeSummaryInputRowV1]] = defaultdict(list)
        for row in materialized:
            key = (
                row.subject_type.value,
                row.projection_group.value,
                row.projection,
                row.evaluation_window_type.value,
                row.horizon_trade_days,
            )
            grouped[key].append(row)
        metrics: list[dict[str, Any]] = []
        unavailable: list[dict[str, Any]] = []
        coverage_by_key: dict[str, Any] = {}
        for key in sorted(grouped):
            key_rows = _deduplicate_economic_samples(grouped[key])
            coverage = _coverage(key_rows)
            key_text = "|".join(map(str, key))
            coverage_by_key[key_text] = coverage
            metric, missing = _return_metrics(key=key, rows=key_rows, coverage=coverage)
            metrics.extend(metric)
            unavailable.extend(missing)
            regime_metrics, regime_missing = _regime_bucket_metrics(
                key=key,
                rows=key_rows,
            )
            metrics.extend(regime_metrics)
            unavailable.extend(regime_missing)
            if key[0] in {
                HistoricalRangeOutcomeSubjectType.CANDIDATE.value,
                HistoricalRangeOutcomeSubjectType.LIST_VERSION.value,
                HistoricalRangeOutcomeSubjectType.RANGE.value,
            }:
                cohort_metrics, cohort_missing = _cohort_metrics(key=key, rows=key_rows)
                metrics.extend(cohort_metrics)
                unavailable.extend(cohort_missing)
                industry_metrics = _industry_hhi(key=key, rows=key_rows)
                metrics.extend(industry_metrics)
            if key[0] == HistoricalRangeOutcomeSubjectType.EPISODE.value:
                holding_metrics, holding_missing = _holding_metrics(key=key, rows=key_rows)
                metrics.extend(holding_metrics)
                unavailable.extend(holding_missing)
        recall_evidence = (
            recall_denominator_set
            or HistoricalRangeRecallDenominatorSetV1.not_configured()
        )
        denominator_rows = recall_evidence.denominators
        recall_metrics, recall_missing = _recall_metrics(denominator_rows)
        metrics.extend(recall_metrics)
        unavailable.extend(recall_missing)
        unavailable.extend(
            _missing_recall_denominators(
                rows=materialized,
                policy=policy,
                denominators=denominator_rows,
            )
        )
        metrics.sort(key=lambda item: (item["metric_key"], item.get("group_key") or ""))
        unavailable.sort(key=lambda item: (item["metric_key"], item.get("group_key") or ""))
        metrics_tuple = tuple(metrics)
        coverage_hash = canonical_json_sha256(coverage_by_key)
        metrics_hash = canonical_json_sha256(list(metrics_tuple))
        summary_input_hash = canonical_json_sha256(
            {
                "covered_outcome_set_hash": covered_hash,
                "summary_policy_hash": policy.summary_policy_hash,
                "recall_denominator_set_hash": recall_evidence.denominator_set_hash,
                "producer_code_hash": producer_code_hash,
            }
        )
        return HistoricalRangeSummaryArtifactV2(
            range_run_id=range_run_id,
            summary_input_hash=summary_input_hash,
            summary_policy_hash=str(policy.summary_policy_hash),
            covered_outcome_refs=refs,
            covered_outcome_set_hash=covered_hash,
            recall_denominator_evidence=recall_evidence.model_dump(mode="json"),
            recall_denominator_set_hash=str(recall_evidence.denominator_set_hash),
            maturity_coverage=coverage_by_key,
            maturity_coverage_hash=coverage_hash,
            metrics=metrics_tuple,
            metrics_hash=metrics_hash,
            unavailable_metrics=tuple(unavailable),
            producer_code_hash=producer_code_hash,
        )

    def build_fact(
        self,
        *,
        artifact: HistoricalRangeSummaryArtifactV2,
        summary_artifact_ref: HistoricalRangeArtifactRefV1,
        summary_version: int,
        predecessor: HistoricalRangeSummaryFactV1 | None = None,
    ) -> HistoricalRangeSummaryFactV1:
        if summary_artifact_ref.artifact_kind is not HistoricalRangeArtifactKind.SUMMARY:
            raise HistoricalRangeSummaryError("summary_artifact_ref must be SUMMARY")
        if (predecessor is None) != (summary_version == 1):
            raise HistoricalRangeSummaryError("summary version/predecessor pair is invalid")
        if artifact.predecessor_summary_ref != (
            predecessor.summary_artifact_ref if predecessor is not None else None
        ):
            raise HistoricalRangeSummaryError(
                "summary artifact predecessor differs from version chain"
            )
        summary_id = derive_prefixed_id(
            "ahrs",
            {
                "range_run_id": artifact.range_run_id,
                "summary_version": summary_version,
                "summary_input_hash": artifact.summary_input_hash,
            },
        )
        return HistoricalRangeSummaryFactV1(
            summary_id=summary_id,
            range_run_id=artifact.range_run_id,
            summary_version=summary_version,
            covered_outcome_set_hash=artifact.covered_outcome_set_hash,
            summary_policy_hash=artifact.summary_policy_hash,
            summary_input_hash=artifact.summary_input_hash,
            recall_denominator_set_hash=artifact.recall_denominator_set_hash,
            recall_denominator_evidence_json=artifact.recall_denominator_evidence,
            producer_code_hash=artifact.producer_code_hash,
            maturity_coverage_json=artifact.maturity_coverage,
            maturity_coverage_hash=artifact.maturity_coverage_hash,
            predecessor_summary_id=predecessor.summary_id if predecessor else None,
            predecessor_summary_hash=predecessor.summary_content_hash if predecessor else None,
            summary_artifact_ref=summary_artifact_ref,
            summary_json=artifact.model_dump(mode="json"),
        )


def _deduplicate_economic_samples(
    rows: Iterable[HistoricalRangeSummaryInputRowV1],
) -> list[HistoricalRangeSummaryInputRowV1]:
    selected: dict[str, HistoricalRangeSummaryInputRowV1] = {}
    for row in sorted(rows, key=lambda item: (item.canonical_signal_id, item.outcome_logical_id)):
        previous = selected.get(row.canonical_signal_id)
        if previous is not None and previous.outcome_logical_id != row.outcome_logical_id:
            if previous.value != row.value or previous.maturity_status is not row.maturity_status:
                raise HistoricalRangeSummaryError("duplicate canonical signal has conflicting economic outcome")
            continue
        selected[row.canonical_signal_id] = row
    return list(selected.values())


def _coverage(rows: Iterable[HistoricalRangeSummaryInputRowV1]) -> dict[str, int]:
    counts = {status.value.lower(): 0 for status in HistoricalRangeOutcomeStatus}
    for row in rows:
        counts[row.maturity_status.value.lower()] += 1
    counts["eligible_total"] = sum(counts.values())
    counts["numeric_return_count"] = sum(row.value is not None for row in rows)
    return counts


def _return_metrics(
    *, key: tuple[str, str, str, str, int], rows: list[HistoricalRangeSummaryInputRowV1], coverage: Mapping[str, int]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    values = [row.value for row in rows if row.value is not None and row.maturity_status in _NUMERIC_STATUSES]
    numeric = [value for value in values if value is not None]
    prefix = _metric_prefix(key)
    if not numeric:
        return [], [_unavailable(f"{prefix}:return_statistics", "EMPTY_NUMERIC_DENOMINATOR", coverage)]
    positive = [value for value in numeric if value > 0]
    negative = [value for value in numeric if value < 0]
    result = [
        _metric(f"{prefix}:mean_return", sum(numeric) / Decimal(len(numeric)), coverage),
        _metric(f"{prefix}:median_return", Decimal(median(numeric)), coverage),
        _metric(f"{prefix}:win_rate", Decimal(len(positive)) / Decimal(len(numeric)), coverage),
    ]
    missing: list[dict[str, Any]] = []
    if positive and negative:
        odds = (sum(positive) / Decimal(len(positive))) / abs(sum(negative) / Decimal(len(negative)))
        result.append(_metric(f"{prefix}:odds", odds, coverage))
    else:
        missing.append(_unavailable(f"{prefix}:odds", "POSITIVE_OR_NEGATIVE_DENOMINATOR_EMPTY", coverage))
    return result, missing


def _cohort_metrics(
    *, key: tuple[str, str, str, str, int], rows: list[HistoricalRangeSummaryInputRowV1]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    daily: dict[tuple[date, str], dict[str, Decimal]] = defaultdict(dict)
    symbols_by_list: dict[tuple[date, str], set[str]] = defaultdict(set)
    for row in rows:
        if row.value is None or row.list_version_id is None or row.symbol is None:
            continue
        daily[(row.decision_trade_date, row.list_version_id)][row.symbol] = row.value
        symbols_by_list[(row.decision_trade_date, row.list_version_id)].add(row.symbol)
    prefix = _metric_prefix(key)
    if not daily:
        return [], [_unavailable(f"{prefix}:cohort", "COHORT_DENOMINATOR_EMPTY", {})]
    series = [
        (day, sum(values.values()) / Decimal(len(values)))
        for (day, _list_id), values in sorted(daily.items())
    ]
    nav = Decimal("1")
    peak = Decimal("1")
    drawdown = Decimal("0")
    for _day, value in series:
        nav *= Decimal("1") + value
        peak = max(peak, nav)
        drawdown = min(drawdown, nav / peak - Decimal("1"))
    adjacent = 0
    turnovers: list[Decimal] = []
    ordered_lists = sorted(symbols_by_list.items())
    for (_prior_key, prior), (_current_key, current) in zip(ordered_lists, ordered_lists[1:]):
        adjacent += 1
        universe = prior | current
        prior_weight = Decimal("1") / Decimal(len(prior)) if prior else Decimal("0")
        current_weight = Decimal("1") / Decimal(len(current)) if current else Decimal("0")
        turnover = Decimal("0.5") * sum(
            abs((current_weight if symbol in current else Decimal("0")) - (prior_weight if symbol in prior else Decimal("0")))
            for symbol in universe
        )
        turnovers.append(turnover)
    metrics = [
        _metric(f"{prefix}:equal_weight_cohort_return", sum(value for _, value in series) / Decimal(len(series)), {"cohort_day_count": len(series)}),
        _metric(f"{prefix}:max_drawdown", drawdown, {"cohort_day_count": len(series)}),
    ]
    missing: list[dict[str, Any]] = []
    if turnovers:
        metrics.append(_metric(f"{prefix}:turnover", sum(turnovers) / Decimal(len(turnovers)), {"adjacent_pair_count": adjacent}))
    else:
        missing.append(_unavailable(f"{prefix}:turnover", "ADJACENT_LIST_PAIR_EMPTY", {"adjacent_pair_count": 0}))
    return metrics, missing


def _industry_hhi(
    *, key: tuple[str, str, str, str, int], rows: list[HistoricalRangeSummaryInputRowV1]
) -> list[dict[str, Any]]:
    by_list: dict[tuple[date, str], list[str]] = defaultdict(list)
    for row in rows:
        if row.list_version_id is not None and row.symbol is not None:
            by_list[(row.decision_trade_date, row.list_version_id)].append(row.industry_at_t or "UNKNOWN_AT_T")
    values: list[Decimal] = []
    for industries in by_list.values():
        counts: dict[str, int] = defaultdict(int)
        for industry in industries:
            counts[industry] += 1
        total = Decimal(len(industries))
        values.append(sum((Decimal(count) / total) ** 2 for count in counts.values()))
    if not values:
        return []
    return [_metric(f"{_metric_prefix(key)}:industry_hhi", sum(values) / Decimal(len(values)), {"list_count": len(values)})]


def _holding_metrics(
    *, key: tuple[str, str, str, str, int], rows: list[HistoricalRangeSummaryInputRowV1]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    values = [
        Decimal(row.observed_holding_trading_days)
        for row in rows
        if row.episode_closed
        and row.maturity_status in _NUMERIC_STATUSES
        and row.observed_holding_trading_days is not None
    ]
    prefix = _metric_prefix(key)
    coverage = {"closed_count": len(values), "open_or_censored_count": sum(not bool(row.episode_closed) for row in rows)}
    if not values:
        return [], [_unavailable(f"{prefix}:holding_period", "CLOSED_EPISODE_DENOMINATOR_EMPTY", coverage)]
    return [
        _metric(f"{prefix}:holding_period_mean", sum(values) / Decimal(len(values)), coverage),
        _metric(f"{prefix}:holding_period_median", Decimal(median(values)), coverage),
    ], []


def _regime_bucket_metrics(
    *,
    key: tuple[str, str, str, str, int],
    rows: list[HistoricalRangeSummaryInputRowV1],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[str, list[HistoricalRangeSummaryInputRowV1]] = defaultdict(list)
    missing_count = 0
    for row in rows:
        if row.market_regime_at_t is None:
            missing_count += 1
        else:
            grouped[row.market_regime_at_t].append(row)
    metrics: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []
    prefix = _metric_prefix(key)
    if missing_count:
        unavailable.append(
            _unavailable(
                f"{prefix}:regime_bucket",
                "DECISION_T_REGIME_UNAVAILABLE",
                {"missing_regime_count": missing_count, "eligible_total": len(rows)},
            )
        )
    for regime, regime_rows in sorted(grouped.items()):
        coverage = _coverage(regime_rows)
        available, missing = _return_metrics(key=key, rows=regime_rows, coverage=coverage)
        group_key = f"REGIME:{regime}"
        for item in (*available, *missing):
            item["group_key"] = group_key
            item["metric_key"] = f"{item['metric_key']}:{group_key}"
        metrics.extend(available)
        unavailable.extend(missing)
    return metrics, unavailable


def _recall_metrics(
    denominators: Iterable[HistoricalRangeRecallDenominatorV1],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    metrics: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []
    for item in denominators:
        kind = "conditional_recall" if item.condition_key is not None else "strategy_recall"
        key = f"{kind}@{item.k}:{item.decision_trade_date}:{item.projection_group.value}:{item.projection}:h{item.horizon_trade_days}"
        if item.condition_key is not None:
            key += f":{item.condition_key}"
        denominator = len(item.positive_target_symbols)
        coverage = {
            "target_set_size": denominator,
            "eligible_universe_ref_count": len(item.eligible_universe_refs),
        }
        if denominator == 0:
            unavailable.append(_unavailable(key, "PIT_TARGET_SET_EMPTY", coverage))
            continue
        intersection = set(item.strategy_symbols[: item.k]) & set(item.positive_target_symbols)
        metrics.append(_metric(key, Decimal(len(intersection)) / Decimal(denominator), coverage))
    return metrics, unavailable


def _missing_recall_denominators(
    *,
    rows: tuple[HistoricalRangeSummaryInputRowV1, ...],
    policy: HistoricalRangeSummaryPolicyV1,
    denominators: tuple[HistoricalRangeRecallDenominatorV1, ...],
) -> list[dict[str, Any]]:
    supplied = {
        (
            item.decision_trade_date,
            item.projection_group,
            item.projection,
            item.horizon_trade_days,
            item.condition_key,
            item.k,
        )
        for item in denominators
    }
    expected: set[tuple[date, HistoricalRangeOutcomeProjection, str, int, str | None, int]] = set()
    for row in rows:
        if (
            row.subject_type is not HistoricalRangeOutcomeSubjectType.CANDIDATE
            or row.evaluation_window_type is not HistoricalRangeEvaluationWindowType.FIXED_HORIZON
            or row.horizon_trade_days < 1
        ):
            continue
        conditions = {
            None,
            f"INDUSTRY:{row.industry_at_t or 'UNKNOWN_AT_T'}",
        }
        if row.market_regime_at_t is not None:
            conditions.add(f"REGIME:{row.market_regime_at_t}")
        for k in policy.recall_k_values:
            for condition in conditions:
                expected.add(
                    (
                        row.decision_trade_date,
                        row.projection_group,
                        row.projection,
                        row.horizon_trade_days,
                        condition,
                        k,
                    )
                )
    missing: list[dict[str, Any]] = []
    for day, group, projection, horizon, condition, k in sorted(
        expected - supplied,
        key=lambda item: (item[0], item[1].value, item[2], item[3], item[4] or "", item[5]),
    ):
        kind = "conditional_recall" if condition is not None else "strategy_recall"
        metric_key = f"{kind}@{k}:{day}:{group.value}:{projection}:h{horizon}"
        if condition is not None:
            metric_key += f":{condition}"
        missing.append(
            _unavailable(
                metric_key,
                "PIT_ELIGIBLE_DENOMINATOR_UNAVAILABLE",
                {"target_set_size": 0, "eligible_universe_ref_count": 0},
            )
        )
    return missing


def _metric_prefix(key: tuple[str, str, str, str, int]) -> str:
    return f"{key[0]}:{key[1]}:{key[2]}:{key[3]}:h{key[4]}"


def _metric(metric_key: str, value: Decimal, coverage: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "metric_key": metric_key,
        "status": "AVAILABLE",
        "value": format(value.quantize(_SCALE, rounding=ROUND_HALF_EVEN), "f"),
        "coverage": dict(coverage),
    }


def _unavailable(metric_key: str, reason_code: str, coverage: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "metric_key": metric_key,
        "status": "UNAVAILABLE",
        "value": None,
        "reason_code": reason_code,
        "coverage": dict(coverage),
    }
