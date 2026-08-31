"""Deterministic R4 outcome work-set expansion over immutable R3 subject seeds."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import date
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactRefV1,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeOutcomeFactV1,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeRefreshRequestV1,
    HistoricalRangeOutcomeRevisionReason,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeOutcomeSubjectType,
    HistoricalRangeOutcomeWorkItemV1,
    require_sha256,
)


class HistoricalRangeOutcomeSubjectSeedV1(BaseModel):
    """Exact R3 subject identity used to expand candidate/episode/list/range work."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    range_run_id: str = Field(min_length=1, max_length=160)
    subject_type: HistoricalRangeOutcomeSubjectType
    subject_id: str = Field(min_length=1, max_length=160)
    subject_ref: HistoricalRangeArtifactRefV1
    symbol: str | None = Field(default=None, min_length=1, max_length=32)
    decision_trade_date: date
    label_as_of_trade_date: date
    source_revision_refs: tuple[HistoricalRangeArtifactRefV1, ...] = Field(min_length=1)
    intended_entry_trade_date: date | None = None
    earliest_sell_trade_date: date | None = None
    exit_trade_date: date | None = None
    episode_closed: bool = False

    @field_validator("source_revision_refs")
    @classmethod
    def _source_refs(cls, value: tuple[HistoricalRangeArtifactRefV1, ...]) -> tuple[HistoricalRangeArtifactRefV1, ...]:
        ordered = tuple(sorted(value, key=lambda item: item.semantic_content_hash))
        if len(ordered) != len({item.semantic_content_hash for item in ordered}):
            raise ValueError("subject source refs must be unique")
        return ordered


class HistoricalRangeOutcomeSubjectSeedProvider(Protocol):
    def list_subject_seeds(
        self,
        *,
        request: HistoricalRangeOutcomeRefreshRequestV1,
        after_key: tuple[str, str, str] | None,
        limit: int,
    ) -> tuple[HistoricalRangeOutcomeSubjectSeedV1, ...]: ...


class HistoricalRangeOutcomeCalendar(Protocol):
    def timeline(
        self,
        *,
        policy_bundle_hash: str,
        decision_trade_date: date,
        horizon_trade_days: int,
    ) -> tuple[date, date, date, date]: ...

    def next_trading_day(self, *, policy_bundle_hash: str, current_trade_date: date) -> date: ...


class HistoricalRangeOutcomeInputIdentityV1(BaseModel):
    """Exact upstream artifacts plus the frozen calculation-source identity."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_revision_refs: tuple[HistoricalRangeArtifactRefV1, ...] = Field(min_length=1)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)

    @field_validator("source_revision_refs")
    @classmethod
    def _refs(cls, value: tuple[HistoricalRangeArtifactRefV1, ...]) -> tuple[HistoricalRangeArtifactRefV1, ...]:
        ordered = tuple(
            sorted(
                value,
                key=lambda item: (
                    item.artifact_kind.value,
                    item.semantic_content_hash,
                    item.relative_path,
                ),
            )
        )
        if len(ordered) != len(
            {(item.artifact_kind, item.semantic_content_hash, item.relative_path) for item in ordered}
        ):
            raise ValueError("outcome input artifact refs must be unique")
        return ordered

    @field_validator("source_revision_set_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return require_sha256(value, field_name="source_revision_set_hash")


class HistoricalRangeOutcomeInputIdentityResolver(Protocol):
    def resolve(
        self,
        *,
        request: HistoricalRangeOutcomeRefreshRequestV1,
        seed: HistoricalRangeOutcomeSubjectSeedV1,
        projection: Any,
        evaluation_window_type: HistoricalRangeEvaluationWindowType,
        horizon_trade_days: int,
        timeline: tuple[date, date, date, date],
    ) -> HistoricalRangeOutcomeInputIdentityV1: ...


class SeedArtifactOutcomeInputIdentityResolver:
    """Deterministic contract-test resolver; production composition injects DB freezing."""

    def resolve(
        self,
        *,
        request: HistoricalRangeOutcomeRefreshRequestV1,
        seed: HistoricalRangeOutcomeSubjectSeedV1,
        projection: Any,
        evaluation_window_type: HistoricalRangeEvaluationWindowType,
        horizon_trade_days: int,
        timeline: tuple[date, date, date, date],
    ) -> HistoricalRangeOutcomeInputIdentityV1:
        del projection, evaluation_window_type, horizon_trade_days, timeline
        refs = tuple((*seed.source_revision_refs, request.policy_bundle_ref))
        return HistoricalRangeOutcomeInputIdentityV1(
            source_revision_refs=refs,
            source_revision_set_hash=canonical_json_sha256(
                [item.model_dump(mode="json") for item in seed.source_revision_refs]
            ),
        )


class HistoricalRangeOutcomePlanner:
    """Expand immutable subjects into one item per projection/window/horizon."""

    def __init__(
        self,
        *,
        subject_provider: HistoricalRangeOutcomeSubjectSeedProvider,
        calendar: HistoricalRangeOutcomeCalendar,
        producer_code_hash: str,
        outcome_contract_version: str,
        input_identity_resolver: HistoricalRangeOutcomeInputIdentityResolver | None = None,
        latest_outcome: Callable[[str], HistoricalRangeOutcomeFactV1 | None] | None = None,
    ) -> None:
        self._subject_provider = subject_provider
        self._calendar = calendar
        self._producer_code_hash = require_sha256(producer_code_hash, field_name="producer_code_hash")
        self._outcome_contract_version = outcome_contract_version
        self._input_identity_resolver = input_identity_resolver or SeedArtifactOutcomeInputIdentityResolver()
        self._latest_outcome = latest_outcome or (lambda _logical_id: None)

    def plan_slice(
        self,
        *,
        request: HistoricalRangeOutcomeRefreshRequestV1,
        cursor: dict[str, Any] | None,
        limit: int,
    ) -> "HistoricalRangeOutcomeSliceV1":
        if limit < 1:
            raise ValueError("outcome planner limit must be positive")
        if request.producer_code_hash != self._producer_code_hash:
            raise ValueError("planner producer code hash differs from refresh request")
        if request.outcome_contract_version != self._outcome_contract_version:
            raise ValueError("planner outcome contract version differs from refresh request")
        after_key = _cursor_key(cursor)
        # The provider includes the cursor's subject so any remaining
        # projection/horizon items can be reconstructed after a checkpoint.
        # A subject expands into multiple projection/horizon work items.  Keep
        # subject paging bounded so a 500-item slice cannot precompute and then
        # discard thousands of later-subject source revisions.
        seed_limit = min(limit + 1, 51)
        if request.requested_outcome_logical_ids:
            seed_pages: list[HistoricalRangeOutcomeSubjectSeedV1] = []
            scan_after = after_key[:3] if after_key is not None else None
            while True:
                page = self._subject_provider.list_subject_seeds(
                    request=request,
                    after_key=scan_after,
                    limit=seed_limit,
                )
                fresh = tuple(
                    seed
                    for seed in page
                    if scan_after is None
                    or (seed.range_run_id, seed.subject_type.value, seed.subject_id)
                    > scan_after
                )
                seed_pages.extend(fresh)
                if len(page) < seed_limit or not fresh:
                    break
                last = fresh[-1]
                scan_after = (
                    last.range_run_id,
                    last.subject_type.value,
                    last.subject_id,
                )
            seeds = tuple(seed_pages)
            all_seeds_loaded = True
        else:
            seeds = self._subject_provider.list_subject_seeds(
                request=request,
                after_key=after_key[:3] if after_key is not None else None,
                limit=seed_limit,
            )
            all_seeds_loaded = len(seeds) < seed_limit
        items: list[HistoricalRangeOutcomeWorkItemV1] = []
        for seed in seeds:
            if seed.label_as_of_trade_date != request.label_as_of_trade_date:
                raise ValueError("subject seed label-as-of differs from refresh request")
            windows = (
                ((HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE, 0),)
                if seed.subject_type is HistoricalRangeOutcomeSubjectType.EPISODE
                else tuple((HistoricalRangeEvaluationWindowType.FIXED_HORIZON, horizon) for horizon in request.horizons)
            )
            for window, horizon in windows:
                projections = request.requested_projections
                if window is HistoricalRangeEvaluationWindowType.EPISODE_LIFECYCLE:
                    projections = request.requested_projections
                for projection in projections:
                    if window is HistoricalRangeEvaluationWindowType.FIXED_HORIZON:
                        timeline = self._calendar.timeline(
                            policy_bundle_hash=str(request.policy_bundle_hash),
                            decision_trade_date=seed.decision_trade_date,
                            horizon_trade_days=horizon,
                        )
                        intended_entry, earliest_sell, exit_date = timeline[1:]
                    else:
                        intended_entry = self._calendar.next_trading_day(
                            policy_bundle_hash=str(request.policy_bundle_hash),
                            current_trade_date=seed.decision_trade_date,
                        )
                        earliest_sell = self._calendar.next_trading_day(
                            policy_bundle_hash=str(request.policy_bundle_hash),
                            current_trade_date=intended_entry,
                        )
                        if seed.exit_trade_date is None:
                            raise ValueError("episode seed lacks exact EXIT/range-end decision date")
                        if seed.episode_closed:
                            projected_exit_date = (
                                self._calendar.next_trading_day(
                                    policy_bundle_hash=str(request.policy_bundle_hash),
                                    current_trade_date=seed.exit_trade_date,
                                )
                                if projection is HistoricalRangeOutcomeProjection.EXECUTABLE
                                else seed.exit_trade_date
                            )
                            exit_date = max(projected_exit_date, earliest_sell)
                        else:
                            exit_date = seed.exit_trade_date
                        timeline = (
                            seed.decision_trade_date,
                            intended_entry,
                            earliest_sell,
                            exit_date,
                        )
                    input_identity = self._input_identity_resolver.resolve(
                        request=request,
                        seed=seed,
                        projection=projection,
                        evaluation_window_type=window,
                        horizon_trade_days=horizon,
                        timeline=timeline,
                    )
                    logical_hint = HistoricalRangeOutcomeWorkItemV1(
                        range_run_id=seed.range_run_id,
                        subject_type=seed.subject_type,
                        subject_id=seed.subject_id,
                        subject_ref=seed.subject_ref,
                        policy_bundle_ref=request.policy_bundle_ref,
                        projection=projection,
                        evaluation_window_type=window,
                        horizon_trade_days=horizon,
                        policy_bundle_hash=str(request.policy_bundle_hash),
                        decision_trade_date=seed.decision_trade_date,
                        intended_entry_trade_date=intended_entry,
                        earliest_sell_trade_date=earliest_sell,
                        exit_trade_date=exit_date,
                        label_as_of_trade_date=seed.label_as_of_trade_date,
                        source_revision_refs=input_identity.source_revision_refs,
                        source_revision_set_hash=input_identity.source_revision_set_hash,
                        producer_code_hash=self._producer_code_hash,
                        outcome_contract_version=self._outcome_contract_version,
                        revision_reason=HistoricalRangeOutcomeRevisionReason.INITIAL,
                    )
                    if (
                        request.requested_outcome_logical_ids
                        and str(logical_hint.outcome_logical_id)
                        not in request.requested_outcome_logical_ids
                    ):
                        continue
                    predecessor = self._latest_outcome(str(logical_hint.outcome_logical_id))
                    if predecessor is not None:
                        if request.correction_reason is None:
                            if predecessor.source_revision_set_hash != logical_hint.source_revision_set_hash:
                                raise ValueError(
                                    "changed source revision set requires explicit SOURCE_CORRECTION evidence"
                                )
                            if (
                                predecessor.producer_code_hash != logical_hint.producer_code_hash
                                or predecessor.outcome_contract_version != logical_hint.outcome_contract_version
                            ):
                                raise ValueError(
                                    "changed producer identity requires explicit CALCULATION_CORRECTION evidence"
                                )
                            if predecessor.maturity_status in {
                                HistoricalRangeOutcomeStatus.COMPLETE,
                                HistoricalRangeOutcomeStatus.CENSORED,
                                HistoricalRangeOutcomeStatus.TERMINAL,
                                HistoricalRangeOutcomeStatus.FAILED,
                            }:
                                continue
                            if (
                                predecessor.next_refresh_trade_date is not None
                                and predecessor.next_refresh_trade_date > request.label_as_of_trade_date
                            ):
                                continue
                            revision_reason = HistoricalRangeOutcomeRevisionReason.MATURITY_ADVANCE
                            revision_evidence_ref = None
                        else:
                            if (
                                request.correction_reason
                                is HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION
                                and predecessor.producer_code_hash
                                == logical_hint.producer_code_hash
                                and predecessor.outcome_contract_version
                                == logical_hint.outcome_contract_version
                            ):
                                if (
                                    predecessor.source_revision_set_hash
                                    != logical_hint.source_revision_set_hash
                                ):
                                    raise ValueError(
                                        "changed source revision set requires explicit SOURCE_CORRECTION evidence"
                                    )
                                continue
                            if (
                                request.correction_reason
                                is HistoricalRangeOutcomeRevisionReason.SOURCE_CORRECTION
                                and predecessor.source_revision_set_hash
                                == logical_hint.source_revision_set_hash
                            ):
                                if (
                                    predecessor.producer_code_hash
                                    != logical_hint.producer_code_hash
                                    or predecessor.outcome_contract_version
                                    != logical_hint.outcome_contract_version
                                ):
                                    raise ValueError(
                                        "changed producer identity requires explicit CALCULATION_CORRECTION evidence"
                                    )
                                continue
                            revision_reason = request.correction_reason
                            revision_evidence_ref = request.correction_evidence_ref
                        payload = logical_hint.model_dump(
                            mode="python",
                            exclude={"outcome_logical_id", "outcome_input_hash"},
                        )
                        payload.update(
                            revision_reason=revision_reason,
                            predecessor_outcome_ref=predecessor.outcome_artifact_ref,
                            revision_evidence_ref=revision_evidence_ref,
                        )
                        logical_hint = HistoricalRangeOutcomeWorkItemV1.model_validate(payload)
                    items.append(logical_hint)
        items.sort(key=_item_key)
        if after_key is not None:
            items = [item for item in items if _item_key(item) > after_key]
        selected = tuple(items[:limit])
        exhausted = all_seeds_loaded and len(items) <= limit
        next_cursor = None
        if not exhausted and selected:
            next_cursor = {"key": list(_item_key(selected[-1]))}
        return HistoricalRangeOutcomeSliceV1(
            items=selected,
            next_cursor=next_cursor,
            exhausted=exhausted,
        )


class HistoricalRangeOutcomeSliceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    items: tuple[HistoricalRangeOutcomeWorkItemV1, ...]
    next_cursor: dict[str, Any] | None = None
    exhausted: bool

    @model_validator(mode="after")
    def _closure(self) -> "HistoricalRangeOutcomeSliceV1":
        if self.exhausted == (self.next_cursor is not None):
            raise ValueError("exhausted slice must omit cursor and non-exhausted slice must retain cursor")
        keys = [_item_key(item) for item in self.items]
        if keys != sorted(keys) or len(keys) != len(set(keys)):
            raise ValueError("outcome slice items must be sorted by unique stable key")
        return self

    @classmethod
    def from_items(
        cls, items: Iterable[HistoricalRangeOutcomeWorkItemV1], *, exhausted: bool
    ) -> "HistoricalRangeOutcomeSliceV1":
        ordered = tuple(sorted(items, key=_item_key))
        return cls(
            items=ordered,
            exhausted=exhausted,
            next_cursor=({"key": list(_item_key(ordered[-1]))} if ordered and not exhausted else None),
        )


def _item_key(item: HistoricalRangeOutcomeWorkItemV1) -> tuple[str, str, str, str, str, int]:
    return (
        item.range_run_id,
        item.subject_type.value,
        item.subject_id,
        item.projection.value,
        item.evaluation_window_type.value,
        item.horizon_trade_days,
    )


def _cursor_key(
    cursor: dict[str, Any] | None,
) -> tuple[str, str, str, str, str, int] | None:
    if not cursor:
        return None
    key = cursor.get("key")
    if not isinstance(key, list) or len(key) != 6:
        raise ValueError("outcome planner cursor is malformed")
    return (
        str(key[0]),
        str(key[1]),
        str(key[2]),
        str(key[3]),
        str(key[4]),
        int(key[5]),
    )
