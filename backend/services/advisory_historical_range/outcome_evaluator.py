"""Production adapter that wires read-only source slices to R4 projection engines."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, Protocol

import psycopg2.extras
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_historical_range.models import (
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeOutcomeArtifactV2,
    HistoricalRangeOutcomeFactV1,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeOutcomeSubjectType,
    HistoricalRangeOutcomeWorkItemV1,
)
from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.outcome_projection import (
    EpisodeLifecycleOutcomeEngine,
    ExecutablePathOutcomeEngine,
    HistoricalRangeAggregateCalculationResultV1,
    HistoricalRangeProjectionResultV1,
    RecommendationPathOutcomeEngine,
)
from backend.services.advisory_historical_range.outcome_source import (
    HistoricalRangeOutcomeSourceError,
    HistoricalRangeSymbolPathReceiptV1,
    HistoricalRangeSymbolPathRequestV1,
    PostgresHistoricalRangeOutcomeSourceProvider,
)
from backend.services.advisory_historical_range.outcome_planner import (
    HistoricalRangeOutcomeInputIdentityV1,
    HistoricalRangeOutcomeSubjectSeedV1,
)
from backend.services.advisory_historical_range.retrospective_projection import (
    PostgresHistoricalRangeCandidateProjectionLoader,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_phase1.label_policy import (
    BarrierPolicy,
    BenchmarkPolicy,
    CashReturnPolicy,
    CostPolicy,
    EntryExecutionPolicy,
    MarketDataPolicy,
    Projection,
    TerminalPolicy,
    TradingCalendar,
)
from backend.services.advisory_phase1.outcome_engine import (
    BenchmarkPortfolio,
    CorporateActionEffect,
    MissingSourceReceipt,
    OutcomeCalculationRequest,
    OutcomeCalculationResult,
    OutcomeOwner,
    HistoricalRangeSubjectOwner,
    PricePath,
    SourceMemberBinding,
    TerminalDisposition,
    TerminalResolution,
)
from backend.services.advisory_phase1.source_revision import SourceRevisionSet
from backend.services.advisory_historical_range.models import require_sha256


class HistoricalRangeValuationPolicyBundleV1(BaseModel):
    """Normalized executable values derived from the range-native policy artifact.

    This deliberately omits Phase 0A handoff/admission identity.  A composition
    adapter must supply the typed component payloads and prove their hashes before
    constructing this value object.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    policy_bundle_hash: str = Field(min_length=64, max_length=64)
    calendar_version: str = Field(min_length=1, max_length=160)
    calendar_hash: str = Field(min_length=64, max_length=64)
    component_hashes: dict[str, str]
    component_set_hash: str | None = Field(default=None, min_length=64, max_length=64)
    horizons: tuple[int, ...] = Field(min_length=1)
    projections_by_horizon: dict[int, tuple[Projection, ...]]
    gap_1d_enabled: bool = False
    candidate_reference_notional: Decimal = Field(gt=Decimal("0"))
    benchmark_portfolio_notional: Decimal = Field(gt=Decimal("0"))

    @field_validator("policy_bundle_hash", "calendar_hash", "component_set_hash")
    @classmethod
    def _hash(cls, value: str | None, info) -> str | None:  # type: ignore[no-untyped-def]
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeValuationPolicyBundleV1":
        if self.horizons != tuple(sorted(set(self.horizons))) or any(item < 1 for item in self.horizons):
            raise ValueError("range valuation horizons must be sorted, unique, and positive")
        if set(self.projections_by_horizon) != set(self.horizons):
            raise ValueError("range valuation projection keys must exactly cover horizons")
        required_roles = {
            "CALENDAR",
            "MARKET_DATA",
            "EXECUTION",
            "COST",
            "BENCHMARK",
            "CASH_RETURN",
            "TERMINAL",
            "BARRIER",
            "CORPORATE_ACTION",
        }
        if set(self.component_hashes) != required_roles:
            raise ValueError("range valuation policy requires the complete component hash set")
        hashes = {
            role: require_sha256(value, field_name=f"component_hashes[{role}]")
            for role, value in sorted(self.component_hashes.items())
        }
        digest = canonical_json_sha256(hashes)
        if self.component_set_hash is not None and self.component_set_hash != digest:
            raise ValueError("component_set_hash does not match range policy components")
        object.__setattr__(self, "component_hashes", hashes)
        object.__setattr__(self, "component_set_hash", digest)
        return self


class HistoricalRangeValuationPolicySetV1(BaseModel):
    """Frozen component payloads used by the shared valuation core."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    bundle: HistoricalRangeValuationPolicyBundleV1
    calendar: TradingCalendar
    market_data: MarketDataPolicy
    execution: EntryExecutionPolicy
    cost: CostPolicy
    benchmark: BenchmarkPolicy
    cash_return: CashReturnPolicy
    barrier: BarrierPolicy
    terminal: TerminalPolicy

    @model_validator(mode="after")
    def _calendar_identity(self) -> "HistoricalRangeValuationPolicySetV1":
        if (
            self.bundle.calendar_version != self.calendar.calendar_version
            or self.bundle.calendar_hash != self.calendar.calendar_hash
        ):
            raise ValueError("range policy and calendar identities differ")
        return self


class HistoricalRangeOutcomeCalculationRequestV1(BaseModel):
    """R4 calculation request with an explicit lifecycle-window discriminator."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    owner: OutcomeOwner | HistoricalRangeSubjectOwner
    policies: HistoricalRangeValuationPolicySetV1
    evaluation_window_type: HistoricalRangeEvaluationWindowType
    horizon_trading_days: int = Field(ge=0)
    projection: Projection
    label_as_of_ts: datetime
    label_source_revision_set: SourceRevisionSet
    price_path: PricePath
    corporate_actions: tuple[CorporateActionEffect, ...] = ()
    terminal: TerminalResolution
    benchmark: BenchmarkPortfolio | None = None
    missing_source_receipts: tuple[MissingSourceReceipt, ...] = ()

    @field_validator("label_as_of_ts")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("label_as_of_ts must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeOutcomeCalculationRequestV1":
        if self.evaluation_window_type is HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE:
            if self.horizon_trading_days != 0:
                raise ValueError("episode lifecycle calculation requires horizon sentinel zero")
            allowed = {
                projection
                for projections in self.policies.bundle.projections_by_horizon.values()
                for projection in projections
            }
        else:
            if self.horizon_trading_days < 1:
                raise ValueError("fixed-horizon calculation requires a positive horizon")
            allowed = set(self.policies.bundle.projections_by_horizon.get(self.horizon_trading_days, ()))
        if self.projection not in allowed:
            raise ValueError("projection is absent from the frozen range policy")
        if not self.label_source_revision_set.research_only:
            raise ValueError("range outcome source revision set must be research only")
        if self.label_source_revision_set.label_as_of_ts != self.label_as_of_ts:
            raise ValueError("label as-of must exactly match source revision set")
        if self.price_path.symbol != self.owner.symbol:
            raise ValueError("owner symbol does not match price path")
        members = {member.member_key: member for member in self.label_source_revision_set.members}
        bindings = [
            *(bar.price_source for bar in self.price_path.bars),
            *(bar.adjustment_source for bar in self.price_path.bars),
            *(bar.tradability_source for bar in self.price_path.bars),
        ]
        bindings.extend(action.source for action in self.corporate_actions)
        if self.terminal.source is not None:
            bindings.append(self.terminal.source)
        if self.benchmark is not None:
            bindings.append(self.benchmark.constituent_source)
            for leg in self.benchmark.legs:
                for bar in leg.price_path.bars:
                    bindings.extend(
                        (bar.price_source, bar.adjustment_source, bar.tradability_source)
                    )
                bindings.extend(action.source for action in leg.corporate_actions)
                if leg.terminal.source is not None:
                    bindings.append(leg.terminal.source)
        for binding in bindings:
            member = members.get(binding.source_member_key)
            if member is None or member.source_role != binding.source_role or member.partition_content_hash != binding.partition_content_hash:
                raise ValueError("range calculation source binding is absent from its frozen revision set")
        if len({receipt.source_role for receipt in self.missing_source_receipts}) != len(self.missing_source_receipts):
            raise ValueError("range source failure roles must be unique")
        if any(receipt.source_revision_set_hash != self.label_source_revision_set.source_revision_set_hash for receipt in self.missing_source_receipts):
            raise ValueError("range source failure receipt does not belong to the frozen revision set")
        if self.terminal.disposition is not TerminalDisposition.NONE:
            if self.terminal.event_closed_at is None or self.terminal.event_closed_at > self.label_as_of_ts:
                raise ValueError("range terminal evidence must be observed by label-as-of")
        return self

    def missing_receipt_for(self, source_role: str) -> MissingSourceReceipt | None:
        return next(
            (item for item in self.missing_source_receipts if item.source_role == source_role),
            None,
        )


class HistoricalRangeOutcomeSubjectInputV1(BaseModel):
    """Exact R3 subject data required to build a calculation request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    owner: OutcomeOwner | HistoricalRangeSubjectOwner
    label_as_of_ts: datetime
    source_artifact_ref_set_hash: str = Field(min_length=64, max_length=64)
    source_revision_set: SourceRevisionSet
    price_source: SourceMemberBinding
    adjustment_source: SourceMemberBinding
    tradability_source: SourceMemberBinding
    terminal: TerminalResolution = TerminalResolution(disposition=TerminalDisposition.NONE)
    corporate_actions: tuple[CorporateActionEffect, ...] = ()
    benchmark: BenchmarkPortfolio | None = None
    lifecycle_timeline: tuple[date, date, date, date] | None = None
    episode_closed: bool = False

    @field_validator("source_artifact_ref_set_hash")
    @classmethod
    def _source_ref_hash(cls, value: str) -> str:
        return require_sha256(value, field_name="source_artifact_ref_set_hash")


class HistoricalRangeOutcomeSubjectInputProvider(Protocol):
    def load(
        self, work_item: HistoricalRangeOutcomeWorkItemV1
    ) -> HistoricalRangeOutcomeSubjectInputV1: ...


class PostgresHistoricalRangeOutcomeSubjectInputProvider:
    """Resolve candidate/episode identity from immutable R3 facts only."""

    def __init__(
        self,
        *,
        conn_factory,
        source_provider: PostgresHistoricalRangeOutcomeSourceProvider,
        policy_provider: "HistoricalRangeOutcomePolicyProvider",
        candidate_projection_loader: PostgresHistoricalRangeCandidateProjectionLoader,
    ) -> None:
        self._conn_factory = conn_factory
        self._source_provider = source_provider
        self._policy_provider = policy_provider
        self._candidate_projection_loader = candidate_projection_loader

    def load(
        self, work_item: HistoricalRangeOutcomeWorkItemV1
    ) -> HistoricalRangeOutcomeSubjectInputV1:
        metadata = self._metadata(work_item)
        self._policy_provider.load(work_item.policy_bundle_hash)
        if any(
            value is None
            for value in (
                work_item.intended_entry_trade_date,
                work_item.earliest_sell_trade_date,
                work_item.exit_trade_date,
            )
        ):
            raise ValueError("planned outcome timeline is incomplete")
        timeline = (
            work_item.decision_trade_date,
            work_item.intended_entry_trade_date,
            work_item.earliest_sell_trade_date,
            work_item.exit_trade_date,
        )
        label_as_of_ts = datetime.combine(
            work_item.label_as_of_trade_date,
            time(23, 59, 59),
            tzinfo=UTC,
        )
        source_bundle = self._source_provider.resolve_source_revision_bundle(
            symbol=metadata["symbol"],
            start_trade_date=timeline[0],
            end_trade_date=min(timeline[3], work_item.label_as_of_trade_date),
            label_as_of_ts=label_as_of_ts,
        )
        if (
            source_bundle.source_revision_set.source_revision_set_hash
            != work_item.source_revision_set_hash
        ):
            raise HistoricalRangeOutcomeSourceError(
                "ADVISORY_HR_OUTCOME_SOURCE_REVISION_CONFLICT",
                "historical source changed after work-item planning",
                context={
                    "range_run_id": work_item.range_run_id,
                    "subject_id": work_item.subject_id,
                },
            )
        terminal = TerminalResolution(disposition=TerminalDisposition.NONE)
        if (
            work_item.subject_type is HistoricalRangeOutcomeSubjectType.EPISODE
            and not metadata["episode_closed"]
        ):
            terminal = TerminalResolution(
                disposition=TerminalDisposition.RIGHT_CENSORED,
                symbol=metadata["symbol"],
                event_trade_date=timeline[3],
                event_closed_at=label_as_of_ts,
                source=source_bundle.tradability_source,
                censor_reason_code="RANGE_END_ACTIVE",
            )
        if work_item.subject_type is HistoricalRangeOutcomeSubjectType.CANDIDATE:
            owner = self._candidate_projection_loader.load(
                candidate_id=work_item.subject_id,
                range_run_id=work_item.range_run_id,
                policy_bundle_ref=work_item.policy_bundle_ref,
                policy_bundle_hash=work_item.policy_bundle_hash,
            ).owner
        else:
            owner = HistoricalRangeSubjectOwner(
                owner_key=work_item.subject_id,
                range_run_id=work_item.range_run_id,
                subject_ref_hash=work_item.subject_ref.semantic_content_hash,
                symbol=metadata["symbol"],
                decision_as_of_trade_date=work_item.decision_trade_date,
            )
        return HistoricalRangeOutcomeSubjectInputV1(
            owner=owner,
            label_as_of_ts=label_as_of_ts,
            source_artifact_ref_set_hash=str(
                work_item.source_artifact_ref_set_hash
            ),
            source_revision_set=source_bundle.source_revision_set,
            price_source=source_bundle.price_source,
            adjustment_source=source_bundle.adjustment_source,
            tradability_source=source_bundle.tradability_source,
            terminal=terminal,
            lifecycle_timeline=timeline
            if work_item.evaluation_window_type
            is HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE
            else None,
            episode_closed=bool(metadata["episode_closed"]),
        )

    def _metadata(self, work_item: HistoricalRangeOutcomeWorkItemV1) -> dict[str, Any]:
        with self._conn_factory() as conn:
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if work_item.subject_type is HistoricalRangeOutcomeSubjectType.CANDIDATE:
                    cur.execute(
                        """
                        SELECT candidate.symbol, day.decision_trade_date AS end_decision_trade_date,
                               FALSE AS episode_closed
                        FROM app.advisory_historical_range_candidate candidate
                        JOIN app.advisory_historical_range_day_run day
                          ON day.day_run_id = candidate.day_run_id
                        WHERE candidate.candidate_id = %s
                          AND day.range_run_id = %s
                          AND candidate.artifact_ref = %s
                        """,
                        (
                            work_item.subject_id,
                            work_item.range_run_id,
                            psycopg2.extras.Json(
                                work_item.subject_ref.model_dump(mode="json")
                            ),
                        ),
                    )
                elif work_item.subject_type is HistoricalRangeOutcomeSubjectType.EPISODE:
                    cur.execute(
                        """
                        SELECT episode.symbol,
                               COALESCE(episode.exit_decision_trade_date, episode.decision_trade_date)
                                    AS end_decision_trade_date,
                               (episode.recommendation_state = 'EXITED') AS episode_closed
                        FROM app.advisory_historical_range_episode_snapshot episode
                        JOIN app.advisory_historical_range_list_version list
                          ON list.list_version_id = episode.list_version_id
                        JOIN app.advisory_historical_range_day_run day
                          ON day.day_run_id = list.day_run_id
                        WHERE episode.episode_id = %s
                          AND episode.range_run_id = %s
                          AND day.day_receipt_ref = %s
                        """,
                        (
                            work_item.subject_id,
                            work_item.range_run_id,
                            psycopg2.extras.Json(
                                work_item.subject_ref.model_dump(mode="json")
                            ),
                        ),
                    )
                else:
                    raise ValueError("raw valuation input is only valid for candidate or episode subjects")
                rows = tuple(dict(row) for row in cur.fetchall())
            conn.rollback()
        if len(rows) != 1:
            raise ValueError("R4 outcome subject metadata is unavailable or ambiguous")
        return rows[0]


class HistoricalRangeOutcomePolicyProvider(Protocol):
    def load(self, policy_bundle_hash: str) -> HistoricalRangeValuationPolicySetV1: ...


class FrozenHistoricalRangeOutcomeInputFactory:
    """Build typed valuation inputs from immutable subject/policy providers."""

    def __init__(
        self,
        *,
        subject_provider: HistoricalRangeOutcomeSubjectInputProvider,
        policy_provider: HistoricalRangeOutcomePolicyProvider,
    ) -> None:
        self._subject_provider = subject_provider
        self._policy_provider = policy_provider

    def _subject_and_policy(
        self, work_item: HistoricalRangeOutcomeWorkItemV1
    ) -> tuple[HistoricalRangeOutcomeSubjectInputV1, HistoricalRangeValuationPolicySetV1]:
        subject = self._subject_provider.load(work_item)
        if (
            subject.source_artifact_ref_set_hash
            != work_item.source_artifact_ref_set_hash
        ):
            raise ValueError("subject source artifact set differs from the planned work item")
        policy = self._policy_provider.load(work_item.policy_bundle_hash)
        if policy.bundle.policy_bundle_hash != work_item.policy_bundle_hash:
            raise ValueError("policy provider returned a different frozen policy bundle")
        return subject, policy

    def source_request(
        self, work_item: HistoricalRangeOutcomeWorkItemV1
    ) -> HistoricalRangeSymbolPathRequestV1:
        subject, policy = self._subject_and_policy(work_item)
        timeline = self.timeline(work_item)
        # Future bars are intentionally not requested before their label cutoff;
        # the shared core then emits NOT_DUE/MATURING instead of a fabricated value.
        end_trade_date = min(timeline[3], work_item.label_as_of_trade_date)
        observed_at = max(member.available_at_min for member in subject.source_revision_set.members)
        return HistoricalRangeSymbolPathRequestV1(
            symbol=subject.owner.symbol,
            start_trade_date=timeline[0],
            end_trade_date=end_trade_date,
            label_as_of_trade_date=work_item.label_as_of_trade_date,
            source_available_at=observed_at,
            price_source=subject.price_source,
            adjustment_source=subject.adjustment_source,
            tradability_source=subject.tradability_source,
            expected_source_revision_set_hash=work_item.source_revision_set_hash,
        )

    def calculation_requests(
        self,
        work_item: HistoricalRangeOutcomeWorkItemV1,
        source: HistoricalRangeSymbolPathReceiptV1,
    ) -> Mapping[Projection, HistoricalRangeOutcomeCalculationRequestV1]:
        subject, policy = self._subject_and_policy(work_item)
        if source.request_hash != canonical_json_sha256(self.source_request(work_item).model_dump(mode="json")):
            raise ValueError("source receipt does not match the frozen source request")
        if source.source_revision_set_hash != work_item.source_revision_set_hash:
            raise ValueError("source receipt revision set differs from the planned work item")
        if work_item.evaluation_window_type is HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE:
            allowed = {
                projection
                for projections in policy.bundle.projections_by_horizon.values()
                for projection in projections
            }
        else:
            allowed = set(policy.bundle.projections_by_horizon.get(work_item.horizon_trade_days, ()))
        if work_item.projection is HistoricalRangeOutcomeProjection.RECOMMENDATION:
            allowed &= {
                Projection.RETURN_GROSS,
                Projection.RETURN_NET_ABSOLUTE,
                Projection.RETURN_NET_EXCESS,
                Projection.PATH_MFE,
                Projection.PATH_MAE,
            }
        else:
            allowed &= {
                Projection.RETURN_GROSS,
                Projection.RETURN_NET_ABSOLUTE,
                Projection.RETURN_NET_EXCESS,
                Projection.EXECUTABLE_MFE,
                Projection.EXECUTABLE_MAE,
            }
        if not allowed:
            raise ValueError("frozen policy has no calculation projections for the requested group")
        return {
            projection: HistoricalRangeOutcomeCalculationRequestV1(
                owner=subject.owner,
                policies=policy,
                evaluation_window_type=work_item.evaluation_window_type,
                horizon_trading_days=work_item.horizon_trade_days,
                projection=projection,
                label_as_of_ts=subject.label_as_of_ts,
                label_source_revision_set=subject.source_revision_set,
                price_path=source.price_path,
                corporate_actions=subject.corporate_actions,
                terminal=subject.terminal,
                benchmark=subject.benchmark,
            )
            for projection in sorted(allowed, key=lambda item: item.value)
        }

    def timeline(
        self, work_item: HistoricalRangeOutcomeWorkItemV1
    ) -> tuple[date, date, date, date]:
        subject, policy = self._subject_and_policy(work_item)
        if work_item.evaluation_window_type is HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE:
            if subject.lifecycle_timeline is None:
                raise ValueError("episode subject lacks its exact lifecycle timeline")
            return subject.lifecycle_timeline
        return policy.calendar.timeline(
            decision_date=work_item.decision_trade_date,
            horizon_trading_days=work_item.horizon_trade_days,
        )

    def episode_closed(self, work_item: HistoricalRangeOutcomeWorkItemV1) -> bool:
        subject, _policy = self._subject_and_policy(work_item)
        return subject.episode_closed


class HistoricalRangeOutcomeInputFactory(Protocol):
    def source_request(
        self, work_item: HistoricalRangeOutcomeWorkItemV1
    ) -> HistoricalRangeSymbolPathRequestV1: ...

    def calculation_requests(
        self,
        work_item: HistoricalRangeOutcomeWorkItemV1,
        source: HistoricalRangeSymbolPathReceiptV1,
    ) -> Mapping[Projection, OutcomeCalculationRequest | HistoricalRangeOutcomeCalculationRequestV1]: ...

    def timeline(
        self, work_item: HistoricalRangeOutcomeWorkItemV1
    ) -> tuple[date, date, date, date]: ...

    def episode_closed(self, work_item: HistoricalRangeOutcomeWorkItemV1) -> bool: ...


class HistoricalRangeAggregateOutcomeProvider(Protocol):
    def list_child_outcomes_for_aggregate(
        self, *, work_item: HistoricalRangeOutcomeWorkItemV1
    ) -> tuple[HistoricalRangeOutcomeFactV1, ...]: ...

    def list_child_outcomes_for_identity(
        self,
        *,
        range_run_id: str,
        subject_type: HistoricalRangeOutcomeSubjectType,
        subject_id: str,
        projection: HistoricalRangeOutcomeProjection,
        evaluation_window_type: HistoricalRangeEvaluationWindowType,
        horizon_trade_days: int,
        policy_bundle_hash: str,
        label_as_of_trade_date: date,
    ) -> tuple[HistoricalRangeOutcomeFactV1, ...]: ...


class PostgresHistoricalRangeOutcomeInputIdentityResolver:
    """Freeze DB source revisions or exact aggregate children before work hashing."""

    def __init__(
        self,
        *,
        source_provider: PostgresHistoricalRangeOutcomeSourceProvider,
        aggregate_provider: HistoricalRangeAggregateOutcomeProvider,
    ) -> None:
        self._source_provider = source_provider
        self._aggregate_provider = aggregate_provider

    def resolve(
        self,
        *,
        request: Any,
        seed: HistoricalRangeOutcomeSubjectSeedV1,
        projection: HistoricalRangeOutcomeProjection,
        evaluation_window_type: HistoricalRangeEvaluationWindowType,
        horizon_trade_days: int,
        timeline: tuple[date, date, date, date],
    ) -> HistoricalRangeOutcomeInputIdentityV1:
        self._source_provider.begin_operation(str(request.request_hash))
        if seed.subject_type in {
            HistoricalRangeOutcomeSubjectType.CANDIDATE,
            HistoricalRangeOutcomeSubjectType.EPISODE,
        }:
            if seed.symbol is None:
                raise ValueError("raw outcome subject lacks its immutable symbol")
            label_as_of_ts = datetime.combine(
                request.label_as_of_trade_date,
                time(23, 59, 59),
                tzinfo=UTC,
            )
            bundle = self._source_provider.resolve_source_revision_bundle(
                symbol=seed.symbol,
                start_trade_date=timeline[0],
                end_trade_date=min(timeline[3], request.label_as_of_trade_date),
                label_as_of_ts=label_as_of_ts,
            )
            refs = tuple((*seed.source_revision_refs, request.policy_bundle_ref))
            source_hash = bundle.source_revision_set.source_revision_set_hash
        else:
            children = self._aggregate_provider.list_child_outcomes_for_identity(
                range_run_id=seed.range_run_id,
                subject_type=seed.subject_type,
                subject_id=seed.subject_id,
                projection=projection,
                evaluation_window_type=evaluation_window_type,
                horizon_trade_days=horizon_trade_days,
                policy_bundle_hash=request.policy_bundle_hash,
                label_as_of_trade_date=request.label_as_of_trade_date,
            )
            child_refs = tuple(item.outcome_artifact_ref for item in children)
            refs = tuple(
                (*seed.source_revision_refs, request.policy_bundle_ref, *child_refs)
            )
            source_hash = canonical_json_sha256(
                [item.model_dump(mode="json") for item in child_refs]
            )
        return HistoricalRangeOutcomeInputIdentityV1(
            source_revision_refs=refs,
            source_revision_set_hash=source_hash,
        )


class HistoricalRangeAggregateOutcomeEvaluator:
    """Aggregate list/range outcomes from frozen child outcome artifacts only."""

    def __init__(
        self,
        *,
        provider: HistoricalRangeAggregateOutcomeProvider,
        artifact_store: HistoricalRangeArtifactStore,
    ) -> None:
        self._provider = provider
        self._artifact_store = artifact_store

    def evaluate(
        self, work_item: HistoricalRangeOutcomeWorkItemV1
    ) -> HistoricalRangeProjectionResultV1:
        children = self._provider.list_child_outcomes_for_aggregate(work_item=work_item)
        child_source_hash = canonical_json_sha256(
            [
                item.outcome_artifact_ref.model_dump(mode="json")
                for item in children
            ]
        )
        if child_source_hash != work_item.source_revision_set_hash:
            raise HistoricalRangeOutcomeSourceError(
                "ADVISORY_HR_OUTCOME_SOURCE_REVISION_CONFLICT",
                "aggregate child outcome set changed after work-item planning",
                context={
                    "range_run_id": work_item.range_run_id,
                    "subject_id": work_item.subject_id,
                },
            )
        if any(
            child.maturity_status is HistoricalRangeOutcomeStatus.FAILED
            for child in children
        ):
            return HistoricalRangeProjectionResultV1(
                projection_group=work_item.projection,
                evaluation_window_type=work_item.evaluation_window_type,
                horizon_trade_days=work_item.horizon_trade_days,
                maturity_status=HistoricalRangeOutcomeStatus.FAILED,
                reason_codes=("CHILD_OUTCOME_FAILED",),
            )
        fine: dict[Projection, list[tuple[HistoricalRangeOutcomeFactV1, Any]]] = {}
        for child in children:
            envelope = self._artifact_store.load(child.outcome_artifact_ref)
            artifact = HistoricalRangeOutcomeArtifactV2.model_validate(envelope.payload)
            for raw in artifact.calculation_results:
                calculation = (
                    HistoricalRangeAggregateCalculationResultV1.model_validate(raw)
                    if raw.get("calculation_kind") == "AGGREGATE_COHORT"
                    else OutcomeCalculationResult.model_validate(raw)
                )
                fine.setdefault(calculation.projection, []).append((child, calculation))
        projections = sorted(fine, key=lambda item: item.value)
        if not projections:
            projections = [
                Projection.RETURN_GROSS
            ]
        results = tuple(
            self._aggregate_projection(
                work_item=work_item,
                projection=projection,
                children=children,
                values=fine.get(projection, []),
            )
            for projection in projections
        )
        statuses = {item.maturity_status for item in results}
        maturity = _aggregate_maturity(statuses)
        next_refresh = None
        if maturity in {
            HistoricalRangeOutcomeStatus.NOT_DUE,
            HistoricalRangeOutcomeStatus.MATURING,
        }:
            next_refresh = min(
                item.next_refresh_trade_date
                for item in results
                if item.next_refresh_trade_date is not None
            )
        return HistoricalRangeProjectionResultV1(
            projection_group=work_item.projection,
            evaluation_window_type=work_item.evaluation_window_type,
            horizon_trade_days=work_item.horizon_trade_days,
            maturity_status=maturity,
            next_refresh_trade_date=next_refresh,
            calculation_results=results,
            reason_codes=("VALID_EMPTY_CHILD_OUTCOME_SET",) if not children else (),
        )

    def _aggregate_projection(
        self,
        *,
        work_item: HistoricalRangeOutcomeWorkItemV1,
        projection: Projection,
        children: tuple[HistoricalRangeOutcomeFactV1, ...],
        values: list[tuple[HistoricalRangeOutcomeFactV1, Any]],
    ) -> HistoricalRangeAggregateCalculationResultV1:
        counts = {status.value.lower(): 0 for status in HistoricalRangeOutcomeStatus}
        for child in children:
            counts[child.maturity_status.value.lower()] += 1
        counts["eligible_total"] = len(children)
        statuses = {child.maturity_status for child in children}
        maturity = _aggregate_maturity(statuses) if statuses else HistoricalRangeOutcomeStatus.CENSORED
        numeric = [
            item.projection_value_decimal
            for _child, item in values
            if item.projection_value_decimal is not None
        ]
        value = None
        if (
            children
            and len(numeric) == len(children)
            and maturity
            in {
                HistoricalRangeOutcomeStatus.COMPLETE,
                HistoricalRangeOutcomeStatus.TERMINAL,
            }
        ):
            value = sum(numeric, Decimal("0")) / Decimal(len(numeric))
        elif maturity in {
            HistoricalRangeOutcomeStatus.COMPLETE,
            HistoricalRangeOutcomeStatus.TERMINAL,
        }:
            maturity = HistoricalRangeOutcomeStatus.CENSORED
        next_dates = [
            child.next_refresh_trade_date
            for child in children
            if child.next_refresh_trade_date is not None
        ]
        next_refresh = min(next_dates) if maturity in {
            HistoricalRangeOutcomeStatus.NOT_DUE,
            HistoricalRangeOutcomeStatus.MATURING,
        } and next_dates else None
        if maturity in {
            HistoricalRangeOutcomeStatus.NOT_DUE,
            HistoricalRangeOutcomeStatus.MATURING,
        } and next_refresh is None:
            next_refresh = work_item.label_as_of_trade_date
        decision_dates = [
            getattr(item, "decision_trade_date") for _child, item in values
        ]
        return HistoricalRangeAggregateCalculationResultV1(
            projection=projection,
            horizon_trading_days=work_item.horizon_trade_days,
            decision_trade_date=min(decision_dates) if decision_dates else work_item.decision_trade_date,
            maturity_status=maturity,
            projection_value_decimal=value,
            next_refresh_trade_date=next_refresh,
            child_outcome_refs=tuple(child.outcome_artifact_ref for child in children),
            maturity_coverage=counts,
        )


class PostgresHistoricalRangeOutcomeEvaluator:
    """Evaluate exact work items without consulting mutable/current tables."""

    def __init__(
        self,
        *,
        source_provider: PostgresHistoricalRangeOutcomeSourceProvider,
        input_factory: HistoricalRangeOutcomeInputFactory,
        aggregate_evaluator: HistoricalRangeAggregateOutcomeEvaluator | None = None,
    ) -> None:
        self._source_provider = source_provider
        self._input_factory = input_factory
        self._aggregate_evaluator = aggregate_evaluator

    def evaluate(
        self, work_item: HistoricalRangeOutcomeWorkItemV1
    ) -> HistoricalRangeProjectionResultV1:
        if work_item.subject_type in {
            HistoricalRangeOutcomeSubjectType.LIST_VERSION,
            HistoricalRangeOutcomeSubjectType.RANGE,
        }:
            if self._aggregate_evaluator is None:
                raise ValueError("list/range outcome requires the aggregate evaluator")
            return self._aggregate_evaluator.evaluate(work_item)
        source = self._source_provider.load_symbol_path(
            self._input_factory.source_request(work_item)
        )
        requests = self._input_factory.calculation_requests(work_item, source)
        if not requests:
            raise ValueError("R4 outcome evaluator received no typed projection requests")
        timeline = self._input_factory.timeline(work_item)
        if work_item.evaluation_window_type is HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE:
            return EpisodeLifecycleOutcomeEngine().calculate(
                projection_group=work_item.projection,
                requests=requests,
                timeline=timeline,
                episode_closed=self._input_factory.episode_closed(work_item),
            )
        if work_item.projection is HistoricalRangeOutcomeProjection.RECOMMENDATION:
            return RecommendationPathOutcomeEngine().calculate(
                requests=requests,
                timeline=timeline,
                evaluation_window_type=work_item.evaluation_window_type,
                horizon_trade_days=work_item.horizon_trade_days,
            )
        return ExecutablePathOutcomeEngine().calculate(
            requests=requests,
            timeline=timeline,
            evaluation_window_type=work_item.evaluation_window_type,
            horizon_trade_days=work_item.horizon_trade_days,
        )


def _aggregate_maturity(
    statuses: set[HistoricalRangeOutcomeStatus],
) -> HistoricalRangeOutcomeStatus:
    if HistoricalRangeOutcomeStatus.FAILED in statuses:
        return HistoricalRangeOutcomeStatus.FAILED
    if HistoricalRangeOutcomeStatus.MATURING in statuses:
        return HistoricalRangeOutcomeStatus.MATURING
    if HistoricalRangeOutcomeStatus.NOT_DUE in statuses:
        return HistoricalRangeOutcomeStatus.NOT_DUE
    if HistoricalRangeOutcomeStatus.CENSORED in statuses:
        return HistoricalRangeOutcomeStatus.CENSORED
    if HistoricalRangeOutcomeStatus.TERMINAL in statuses:
        return HistoricalRangeOutcomeStatus.TERMINAL
    return HistoricalRangeOutcomeStatus.COMPLETE
