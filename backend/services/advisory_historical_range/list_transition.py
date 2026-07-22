"""Historical candidate/mark adapter and deterministic Phase 1R list projection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from backend.services.advisory_list_transition import (
    AdvisoryListTransitionEngine,
    AdvisoryListTransitionError,
    AdvisoryTransitionCandidateV1,
    AdvisoryTransitionDecisionV1,
    AdvisoryTransitionEpisodeV1,
    AdvisoryTransitionPolicyV1,
    AdvisoryTransitionRankObservationV1,
    REVIEW_REASON_NOT_IN_CURRENT_TOPK,
    REVIEW_REASON_VALID_EMPTY,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactRefV1,
    HistoricalRangeCandidateArtifactPayloadV2,
    HistoricalRangeContractError,
    HistoricalRangeDecisionMarkSetV1,
    HistoricalRangeDecisionMarkV2,
    HistoricalRangeEpisodeMarkV2,
    HistoricalRangeEpisodeSnapshotFactV1,
    HistoricalRangeFrozenProgramV1,
    HistoricalRangeListAction,
    HistoricalRangeListItemFactV1,
    HistoricalRangeListSummaryV2,
    HistoricalRangeListVersionFactV1,
    HistoricalRangeRankObservationV2,
    HistoricalRangeActiveRankObservationV2,
    HistoricalRangeRuleGuidanceV2,
    derive_episode_id,
    derive_list_content_hash,
    derive_prefixed_id,
)
from backend.services.advisory_historical_range.semantics import HistoricalRangeListSemanticsV2


@dataclass(frozen=True)
class HistoricalRangeListProjectionResultV1:
    rank_observation: HistoricalRangeRankObservationV2
    list_version: HistoricalRangeListVersionFactV1 | None
    items: tuple[HistoricalRangeListItemFactV1, ...]
    episodes: tuple[HistoricalRangeEpisodeSnapshotFactV1, ...]
    transition_decisions: tuple[AdvisoryTransitionDecisionV1, ...]
    blocking_diagnostics: tuple[str, ...]


class HistoricalRangeListTransitionAdapter:
    """Adapt immutable candidate v2 and decision-mark evidence into the neutral engine."""

    def __init__(self, *, engine: AdvisoryListTransitionEngine | None = None) -> None:
        self._engine = engine or AdvisoryListTransitionEngine()

    def build_projection(
        self,
        *,
        program: HistoricalRangeFrozenProgramV1,
        candidate_payload: HistoricalRangeCandidateArtifactPayloadV2,
        decision_mark_set: HistoricalRangeDecisionMarkSetV1,
        decision_mark_set_ref: HistoricalRangeArtifactRefV1,
        previous_episodes: Sequence[HistoricalRangeEpisodeSnapshotFactV1],
        entry_sequences_by_symbol: Mapping[str, int],
        previous_list_version_id: str | None,
        previous_list_hash: str | None,
        previous_day_receipt_hash: str | None,
        day_input_hash: str,
        next_trade_date: date | None,
        is_range_end: bool,
        decision_cutoff: datetime,
        semantics: HistoricalRangeListSemanticsV2,
    ) -> HistoricalRangeListProjectionResultV1:
        if program.list_semantics_version != semantics.schema_version or program.list_semantics_hash != semantics.semantics_hash:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_LIST_SEMANTICS_MISMATCH",
                "frozen Program list semantics differ from the recomputable R3 payload",
            )
        if decision_mark_set_ref.artifact_kind.value != "DECISION_MARK_SET":
            raise HistoricalRangeContractError(
                "ADVISORY_HR_DECISION_MARK_REF_INVALID",
                "list projection requires a DECISION_MARK_SET artifact ref",
            )
        if candidate_payload.day_run_id != decision_mark_set.day_run_id:
            raise HistoricalRangeContractError("ADVISORY_HR_DAY_IDENTITY_MISMATCH", "candidate and mark set day ids differ")
        if candidate_payload.decision_trade_date != decision_mark_set.decision_trade_date:
            raise HistoricalRangeContractError("ADVISORY_HR_DAY_IDENTITY_MISMATCH", "candidate and mark set dates differ")
        if (previous_list_hash is None) != (previous_day_receipt_hash is None):
            raise HistoricalRangeContractError("ADVISORY_HR_PREDECESSOR_INCOMPLETE", "previous list/day hashes must be supplied together")

        marks = {item.symbol: item for item in decision_mark_set.marks}
        previous_by_symbol = _previous_by_symbol(previous_episodes)
        rank_observation = self._rank_observation(
            candidate_payload=candidate_payload,
            previous_by_symbol=previous_by_symbol,
            marks=marks,
            rank_exit_threshold=_policy_from_program(program).rank_exit_threshold,
        )
        if rank_observation.status == "DATA_UNAVAILABLE":
            return self._blocked_result(rank_observation)

        candidates = _transition_candidates(candidate_payload, marks)
        candidates.extend(
            _active_synthetic_candidates(
                rank_observation=rank_observation,
                previous_by_symbol=previous_by_symbol,
                marks=marks,
            )
        )
        policy = _policy_from_program(program)
        previous_core = tuple(_core_episode(item) for item in previous_by_symbol.values())
        rank_port = AdvisoryTransitionRankObservationV1(
            status=rank_observation.status,
            observed_max_selection_rank=rank_observation.observed_max_selection_rank,
            active_rank_by_symbol={
                item.symbol: item.review_rank for item in rank_observation.active_observations
            },
        )
        try:
            transition = self._engine.transition(
                policy=policy,
                decision_trade_date=candidate_payload.decision_trade_date,
                candidates=tuple(candidates),
                active_episodes=previous_core,
                rank_observation=rank_port,
                episode_identity_allocator=lambda candidate: _allocate_episode_id(
                    range_run_id=candidate_payload.range_run_id,
                    symbol=candidate.symbol,
                    decision_trade_date=candidate_payload.decision_trade_date,
                    entry_sequences_by_symbol=entry_sequences_by_symbol,
                ),
                effective_entry_date=lambda _candidate: next_trade_date or candidate_payload.decision_trade_date,
                effective_exit_date=lambda _episode: next_trade_date or candidate_payload.decision_trade_date,
                defer_stop_before_effective_entry=False,
                historical_mode=True,
            )
        except AdvisoryListTransitionError as exc:
            raise HistoricalRangeContractError(exc.reason_code, str(exc)) from exc
        if transition.blocking_diagnostics:
            return self._blocked_result(rank_observation, transition.blocking_diagnostics)
        return self._project(
            program=program,
            candidate_payload=candidate_payload,
            decision_mark_set=decision_mark_set,
            decision_mark_set_ref=decision_mark_set_ref,
            previous_by_symbol=previous_by_symbol,
            previous_list_version_id=previous_list_version_id,
            previous_list_hash=previous_list_hash,
            previous_day_receipt_hash=previous_day_receipt_hash,
            day_input_hash=day_input_hash,
            next_trade_date=next_trade_date,
            is_range_end=is_range_end,
            decision_cutoff=decision_cutoff,
            rank_observation=rank_observation,
            transition=transition,
            policy=policy,
            entry_sequences_by_symbol=entry_sequences_by_symbol,
        )

    @staticmethod
    def _blocked_result(
        rank_observation: HistoricalRangeRankObservationV2,
        diagnostics: Sequence[str] = (),
    ) -> HistoricalRangeListProjectionResultV1:
        return HistoricalRangeListProjectionResultV1(
            rank_observation=rank_observation,
            list_version=None,
            items=(),
            episodes=(),
            transition_decisions=(),
            blocking_diagnostics=tuple(sorted(diagnostics or ("ADVISORY_HR_WAITING_INPUT",))),
        )

    def _rank_observation(
        self,
        *,
        candidate_payload: HistoricalRangeCandidateArtifactPayloadV2,
        previous_by_symbol: Mapping[str, HistoricalRangeEpisodeSnapshotFactV1],
        marks: Mapping[str, HistoricalRangeDecisionMarkV2],
        rank_exit_threshold: int,
    ) -> HistoricalRangeRankObservationV2:
        included = {
            item.symbol: item
            for item in candidate_payload.candidates
            if item.membership_status == "INCLUDED"
        }
        all_facts = {item.symbol: item for item in candidate_payload.candidates}
        raw_score_count = int(candidate_payload.raw_inference_receipt["score_count"])
        status = "VALID_EMPTY_NO_SIGNAL" if raw_score_count == 0 else "COMPLETE"
        max_rank = max((item.selection_effective_rank or 0 for item in included.values()), default=0)
        observations: list[HistoricalRangeActiveRankObservationV2] = []
        for symbol in sorted(previous_by_symbol):
            fact = all_facts.get(symbol)
            mark = marks.get(symbol)
            if mark is None or mark.availability == "DATA_UNAVAILABLE":
                return HistoricalRangeRankObservationV2(
                    status="DATA_UNAVAILABLE",
                    observed_max_selection_rank=max_rank,
                    rank_exit_threshold=rank_exit_threshold,
                    source_stage_closure_hash=str(candidate_payload.stage_closure_hash),
                    universe_evidence_hash=candidate_payload.universe_identity_hash,
                )
            if symbol in included:
                selected = included[symbol]
                observations.append(
                    HistoricalRangeActiveRankObservationV2(
                        symbol=symbol,
                        classification="INCLUDED_SELECTION_RANK",
                        review_rank=selected.selection_effective_rank,
                        review_score=selected.selection_effective_score,
                        increments_weak_confirmation=(selected.selection_effective_rank or 0) > rank_exit_threshold,
                        evidence_hash=selected.candidate_content_hash or canonical_json_sha256(selected.model_dump(mode="json")),
                    )
                )
            elif raw_score_count == 0:
                observations.append(
                    HistoricalRangeActiveRankObservationV2(
                        symbol=symbol,
                        classification="VALID_EMPTY_NO_SIGNAL",
                        review_rank=None,
                        review_score=None,
                        increments_weak_confirmation=False,
                        evidence_hash=canonical_json_sha256(
                            {"candidate_artifact": candidate_payload.stage_closure_hash, "symbol": symbol, "raw_score_count": 0}
                        ),
                        reason_codes=("ADVISORY_HR_VALID_EMPTY_NO_RANK_SIGNAL",),
                    )
                )
            else:
                classification = (
                    "OUTSIDE_PIT_UNIVERSE"
                    if mark is not None and "OUTSIDE_PIT_UNIVERSE" in mark.tradability_status
                    else "EXCLUDED_BY_STAGE"
                    if fact is not None
                    else "ABSENT_FROM_RAW_SIGNAL"
                )
                observations.append(
                    HistoricalRangeActiveRankObservationV2(
                        symbol=symbol,
                        classification=classification,
                        review_rank=max_rank + 1,
                        review_score=None,
                        increments_weak_confirmation=(max_rank + 1) > rank_exit_threshold,
                        evidence_hash=canonical_json_sha256(
                            {
                                "candidate_artifact": candidate_payload.stage_closure_hash,
                                "symbol": symbol,
                                "classification": classification,
                            }
                        ),
                        reason_codes=(REVIEW_REASON_NOT_IN_CURRENT_TOPK,),
                    )
                )
        return HistoricalRangeRankObservationV2(
            status=status,
            observed_max_selection_rank=max_rank,
            rank_exit_threshold=rank_exit_threshold,
            active_observations=tuple(observations),
            source_stage_closure_hash=str(candidate_payload.stage_closure_hash),
            universe_evidence_hash=candidate_payload.universe_identity_hash,
        )

    def _project(
        self,
        *,
        program: HistoricalRangeFrozenProgramV1,
        candidate_payload: HistoricalRangeCandidateArtifactPayloadV2,
        decision_mark_set: HistoricalRangeDecisionMarkSetV1,
        decision_mark_set_ref: HistoricalRangeArtifactRefV1,
        previous_by_symbol: Mapping[str, HistoricalRangeEpisodeSnapshotFactV1],
        previous_list_version_id: str | None,
        previous_list_hash: str | None,
        previous_day_receipt_hash: str | None,
        day_input_hash: str,
        next_trade_date: date | None,
        is_range_end: bool,
        decision_cutoff: datetime,
        rank_observation: HistoricalRangeRankObservationV2,
        transition: Any,
        policy: AdvisoryTransitionPolicyV1,
        entry_sequences_by_symbol: Mapping[str, int],
    ) -> HistoricalRangeListProjectionResultV1:
        list_version_id = derive_prefixed_id(
            "ahrl",
            {
                "day_run_id": candidate_payload.day_run_id,
                "day_input_hash": day_input_hash,
                "list_semantics_hash": program.list_semantics_hash,
            },
        )
        marks = {item.symbol: item for item in decision_mark_set.marks}
        observation_by_symbol = {item.symbol: item for item in rank_observation.active_observations}
        items: list[HistoricalRangeListItemFactV1] = []
        episodes: list[HistoricalRangeEpisodeSnapshotFactV1] = []
        for decision in transition.decisions:
            candidate = decision.candidate
            episode = decision.episode
            if candidate is None:
                raise HistoricalRangeContractError("ADVISORY_HR_DECISION_EVIDENCE_MISSING", "transition decision lacks candidate evidence")
            action = HistoricalRangeListAction(decision.action)
            guidance = _guidance(
                action=action,
                entry_basis=_program_basis(program, "entry_price_basis"),
                exit_basis=_program_basis(program, "exit_price_basis"),
                decision_trade_date=candidate_payload.decision_trade_date,
                next_trade_date=next_trade_date,
                is_range_end=is_range_end,
                market_state_reason=candidate.reason_code,
            )
            episode_id = episode.episode_id if episode is not None else None
            item_id = derive_prefixed_id(
                "ahrli",
                {"list_version_id": list_version_id, "symbol": candidate.symbol, "action": action.value},
            )
            items.append(
                HistoricalRangeListItemFactV1(
                    list_item_id=item_id,
                    list_version_id=list_version_id,
                    symbol=candidate.symbol,
                    action=action,
                    rank=candidate.rank,
                    score=Decimal(str(candidate.score)) if candidate.score is not None else None,
                    reason_codes=(decision.reason_code,),
                    episode_id=episode_id,
                    rule_guidance_json=guidance.model_dump(mode="json"),
                    intended_execution_trade_date=guidance.intended_execution_trade_date,
                    intended_execution_basis=guidance.intended_execution_basis,
                    execution_status=guidance.execution_status,
                )
            )
            if episode is not None:
                previous = previous_by_symbol.get(candidate.symbol)
                entry_sequence = (
                    previous.entry_sequence
                    if previous is not None
                    else int(entry_sequences_by_symbol.get(candidate.symbol, 0)) + 1
                )
                mark = _episode_mark(
                    episode=episode,
                    current_mark=marks.get(candidate.symbol),
                    observation=observation_by_symbol.get(candidate.symbol),
                    decision_cutoff=decision_cutoff,
                )
                episodes.append(
                    HistoricalRangeEpisodeSnapshotFactV1(
                        episode_snapshot_id=derive_prefixed_id(
                            "ahres",
                            {
                                "list_version_id": list_version_id,
                                "episode_id": episode.episode_id,
                                "decision_trade_date": candidate_payload.decision_trade_date,
                            },
                        ),
                        range_run_id=candidate_payload.range_run_id,
                        list_version_id=list_version_id,
                        episode_id=episode.episode_id,
                        symbol=episode.symbol,
                        decision_trade_date=candidate_payload.decision_trade_date,
                        entry_sequence=entry_sequence,
                        enter_decision_trade_date=episode.entry_signal_date,
                        exit_decision_trade_date=candidate_payload.decision_trade_date if action is HistoricalRangeListAction.EXIT else None,
                        recommendation_state=("EXITED" if action is HistoricalRangeListAction.EXIT else "ACTIVE_AT_RANGE_END" if is_range_end else "ACTIVE"),
                        action=action.value,
                        execution_status=guidance.execution_status,
                        price_quality=mark.mark_quality,
                        weak_rank_confirmation_count=episode.weak_rank_confirm_days,
                        mark_json=mark.model_dump(mode="json"),
                    )
                )
        active_count = sum(item.action in {HistoricalRangeListAction.ENTER, HistoricalRangeListAction.HOLD} for item in items)
        enter_count = sum(item.action is HistoricalRangeListAction.ENTER for item in items)
        hold_count = sum(item.action is HistoricalRangeListAction.HOLD for item in items)
        exit_count = sum(item.action is HistoricalRangeListAction.EXIT for item in items)
        watch_count = sum(item.action is HistoricalRangeListAction.WATCH for item in items)
        previous_active = set(previous_by_symbol)
        active_symbols = {item.symbol for item in items if item.action in {HistoricalRangeListAction.ENTER, HistoricalRangeListAction.HOLD}}
        summary = HistoricalRangeListSummaryV2(
            candidate_outcome=candidate_payload.candidate_outcome,
            stage_closure_hash=str(candidate_payload.stage_closure_hash),
            enter_count=enter_count,
            hold_count=hold_count,
            exit_count=exit_count,
            watch_count=watch_count,
            active_count=active_count,
            overlap_rate=Decimal(len(active_symbols & previous_active)) / Decimal(len(previous_active)) if previous_active else None,
            turnover_rate=Decimal(enter_count + exit_count) / Decimal(policy.target_count),
            replacement_budget_used=transition.replacement_budget_used,
            replacement_budget_remaining=max(policy.daily_replacement_budget - transition.replacement_budget_used, 0),
            rank_observation_status=rank_observation.status,
            observed_max_selection_rank=rank_observation.observed_max_selection_rank,
            mark_policy_version=decision_mark_set.mark_policy_version,
            mark_policy_hash=decision_mark_set.mark_policy_hash,
            decision_mark_set_ref=decision_mark_set_ref,
            previous_list_hash=previous_list_hash,
            previous_day_receipt_hash=previous_day_receipt_hash,
        )
        provisional = HistoricalRangeListVersionFactV1(
            list_version_id=list_version_id,
            day_run_id=candidate_payload.day_run_id,
            range_run_id=candidate_payload.range_run_id,
            previous_list_version_id=previous_list_version_id,
            previous_list_hash=previous_list_hash,
            previous_day_receipt_hash=previous_day_receipt_hash,
            target_count=policy.target_count,
            active_count=active_count,
            enter_count=enter_count,
            hold_count=hold_count,
            exit_count=exit_count,
            watch_count=watch_count,
            summary_json=summary.model_dump(mode="json"),
            list_content_hash="0" * 64,
        )
        list_version = provisional.model_copy(update={"list_content_hash": derive_list_content_hash(provisional, items, episodes)})
        return HistoricalRangeListProjectionResultV1(
            rank_observation=rank_observation,
            list_version=list_version,
            items=tuple(sorted(items, key=lambda item: (item.symbol, item.action.value))),
            episodes=tuple(sorted(episodes, key=lambda item: (item.symbol, item.episode_id))),
            transition_decisions=tuple(transition.decisions),
            blocking_diagnostics=(),
        )


def _policy_from_program(program: HistoricalRangeFrozenProgramV1) -> AdvisoryTransitionPolicyV1:
    raw = dict(program.review_policy)
    required = (
        "rank_enter_threshold",
        "rank_exit_threshold",
        "rank_exit_confirm_days",
        "daily_replacement_budget",
        "stop_loss_bps",
        "take_profit_bps",
        "trailing_stop_bps",
        "time_stop_days",
    )
    missing = [key for key in required if key not in raw]
    if missing:
        raise HistoricalRangeContractError(
            "ADVISORY_HR_REVIEW_POLICY_INCOMPLETE",
            f"frozen review policy is missing required fields: {missing}",
        )
    target_count = _program_target_count(program)
    return AdvisoryTransitionPolicyV1(
        target_count=target_count,
        rank_enter_threshold=int(raw["rank_enter_threshold"]),
        rank_exit_threshold=int(raw["rank_exit_threshold"]),
        rank_exit_confirm_days=int(raw["rank_exit_confirm_days"]),
        daily_replacement_budget=int(raw["daily_replacement_budget"]),
        stop_loss_bps=int(raw["stop_loss_bps"]),
        take_profit_bps=int(raw["take_profit_bps"]),
        trailing_stop_bps=int(raw["trailing_stop_bps"]),
        time_stop_days=int(raw["time_stop_days"]),
        take_profit_mode=str(raw.get("take_profit_mode") or "trailing"),
    )


def _program_target_count(program: HistoricalRangeFrozenProgramV1) -> int:
    candidates = (program.program_config.get("target_count"), program.runtime_config.get("target_count"))
    for value in candidates:
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            return value
    raise HistoricalRangeContractError("ADVISORY_HR_TARGET_COUNT_MISSING", "frozen Program has no target_count")


def _program_basis(program: HistoricalRangeFrozenProgramV1, field: str) -> str:
    value = program.program_config.get(field)
    if not isinstance(value, str) or not value.strip():
        raise HistoricalRangeContractError("ADVISORY_HR_PRICE_BASIS_MISSING", f"frozen Program has no {field}")
    return value.strip()


def _transition_candidates(
    candidate_payload: HistoricalRangeCandidateArtifactPayloadV2,
    marks: Mapping[str, HistoricalRangeDecisionMarkV2],
) -> list[AdvisoryTransitionCandidateV1]:
    result: list[AdvisoryTransitionCandidateV1] = []
    for fact in candidate_payload.candidates:
        if fact.membership_status != "INCLUDED":
            continue
        mark = marks.get(fact.symbol)
        result.append(
            AdvisoryTransitionCandidateV1(
                symbol=fact.symbol,
                rank=int(fact.selection_effective_rank or 0),
                score=float(fact.selection_effective_score) if fact.selection_effective_score is not None else None,
                entry_mark=float(mark.normalized_reference_mark) if mark and mark.availability == "AVAILABLE" else None,
                exit_mark=float(mark.normalized_reference_mark) if mark and mark.availability != "DATA_UNAVAILABLE" else None,
                entry_mark_available=bool(mark and mark.availability == "AVAILABLE"),
                exit_mark_available=bool(mark and mark.availability != "DATA_UNAVAILABLE"),
                evidence=fact.model_dump(mode="json"),
            )
        )
    return result


def _active_synthetic_candidates(
    *,
    rank_observation: HistoricalRangeRankObservationV2,
    previous_by_symbol: Mapping[str, HistoricalRangeEpisodeSnapshotFactV1],
    marks: Mapping[str, HistoricalRangeDecisionMarkV2],
) -> list[AdvisoryTransitionCandidateV1]:
    result: list[AdvisoryTransitionCandidateV1] = []
    for observation in rank_observation.active_observations:
        if observation.classification == "INCLUDED_SELECTION_RANK":
            continue
        previous = previous_by_symbol[observation.symbol]
        mark = marks.get(observation.symbol)
        if mark is None:
            raise HistoricalRangeContractError(
                "ADVISORY_HR_DECISION_MARK_MISSING",
                "active predecessor episode has no decision mark",
                context={"symbol": observation.symbol},
            )
        result.append(
            AdvisoryTransitionCandidateV1(
                symbol=observation.symbol,
                rank=observation.review_rank or rank_observation.synthetic_missing_rank,
                score=float(observation.review_score) if observation.review_score is not None else None,
                entry_mark=None,
                exit_mark=float(mark.normalized_reference_mark) if mark.availability != "DATA_UNAVAILABLE" else None,
                entry_mark_available=False,
                exit_mark_available=mark.availability != "DATA_UNAVAILABLE",
                reason_code=REVIEW_REASON_VALID_EMPTY if observation.classification == "VALID_EMPTY_NO_SIGNAL" else REVIEW_REASON_NOT_IN_CURRENT_TOPK,
                evidence={"previous_episode_id": previous.episode_id, "rank_observation": observation.model_dump(mode="json")},
            )
        )
    return result


def _previous_by_symbol(rows: Sequence[HistoricalRangeEpisodeSnapshotFactV1]) -> dict[str, HistoricalRangeEpisodeSnapshotFactV1]:
    active = {item.symbol: item for item in rows if item.recommendation_state == "ACTIVE"}
    if len(active) != sum(item.recommendation_state == "ACTIVE" for item in rows):
        raise HistoricalRangeContractError("ADVISORY_HR_PREDECESSOR_DUPLICATE", "predecessor active episode symbols are not unique")
    return active


def _core_episode(snapshot: HistoricalRangeEpisodeSnapshotFactV1) -> AdvisoryTransitionEpisodeV1:
    mark = HistoricalRangeEpisodeMarkV2.model_validate(snapshot.mark_json)
    return AdvisoryTransitionEpisodeV1(
        episode_id=snapshot.episode_id,
        symbol=snapshot.symbol,
        entry_signal_date=snapshot.enter_decision_trade_date,
        effective_entry_date=snapshot.enter_decision_trade_date,
        entry_price=float(mark.recommendation_anchor),
        entry_rank=mark.review_rank or 1,
        entry_score=float(mark.review_score) if mark.review_score is not None else None,
        current_rank=mark.review_rank,
        current_score=float(mark.review_score) if mark.review_score is not None else None,
        holding_trading_days=mark.holding_trading_days,
        return_bps=float(mark.current_normalized_mark / mark.recommendation_anchor * Decimal("10000") - Decimal("10000"))
        if mark.current_normalized_mark is not None
        else None,
        max_runup_bps=float(mark.runup_bps) if mark.runup_bps is not None else None,
        max_drawdown_bps=float(mark.drawdown_bps) if mark.drawdown_bps is not None else None,
        still_active_mark_price=float(mark.current_normalized_mark) if mark.current_normalized_mark is not None else None,
        weak_rank_confirm_days=mark.weak_rank_confirmation_count,
        evidence={"previous_snapshot": snapshot.model_dump(mode="json")},
    )


def _allocate_episode_id(
    *,
    range_run_id: str,
    symbol: str,
    decision_trade_date: date,
    entry_sequences_by_symbol: Mapping[str, int],
) -> str:
    sequence = int(entry_sequences_by_symbol.get(symbol, 0)) + 1
    return derive_episode_id(range_run_id, symbol, decision_trade_date, sequence)


def _guidance(
    *,
    action: HistoricalRangeListAction,
    entry_basis: str,
    exit_basis: str,
    decision_trade_date: date,
    next_trade_date: date | None,
    is_range_end: bool,
    market_state_reason: str | None,
) -> HistoricalRangeRuleGuidanceV2:
    if action in {HistoricalRangeListAction.HOLD, HistoricalRangeListAction.WATCH}:
        return HistoricalRangeRuleGuidanceV2(
            action=action,
            execution_status="NOT_APPLICABLE",
            market_state_reason=market_state_reason,
        )
    basis = entry_basis if action is HistoricalRangeListAction.ENTER else exit_basis
    if basis == "signal_close":
        return HistoricalRangeRuleGuidanceV2(
            action=action,
            intended_execution_trade_date=decision_trade_date,
            intended_execution_basis=basis,
            execution_status="NOT_DUE",
            market_state_reason=market_state_reason,
        )
    if next_trade_date is None and is_range_end:
        return HistoricalRangeRuleGuidanceV2(
            action=action,
            execution_status="NOT_DUE",
            market_state_reason=market_state_reason,
            requested_execution_basis=basis,
            range_end_reason="NEXT_SESSION_OUTSIDE_FROZEN_DATE_PLAN",
        )
    if next_trade_date is None:
        raise HistoricalRangeContractError("ADVISORY_HR_FROZEN_NEXT_DATE_MISSING", "non-final day has no frozen next trade date")
    return HistoricalRangeRuleGuidanceV2(
        action=action,
        intended_execution_trade_date=next_trade_date,
        intended_execution_basis=basis,
        execution_status="NOT_DUE",
        market_state_reason=market_state_reason,
    )


def _episode_mark(
    *,
    episode: AdvisoryTransitionEpisodeV1,
    current_mark: HistoricalRangeDecisionMarkV2 | None,
    observation: HistoricalRangeActiveRankObservationV2 | None,
    decision_cutoff: datetime,
) -> HistoricalRangeEpisodeMarkV2:
    if current_mark is None:
        raise HistoricalRangeContractError("ADVISORY_HR_DECISION_MARK_MISSING", "episode has no decision mark")
    return HistoricalRangeEpisodeMarkV2(
        recommendation_anchor=Decimal(str(episode.entry_price)),
        current_raw_reference_yuan=current_mark.raw_reference_yuan,
        current_adjustment_factor=current_mark.adjustment_factor_as_of_t,
        current_normalized_mark=current_mark.normalized_reference_mark,
        holding_trading_days=episode.holding_trading_days,
        runup_bps=Decimal(str(episode.max_runup_bps)) if episode.max_runup_bps is not None else None,
        drawdown_bps=Decimal(str(episode.max_drawdown_bps)) if episode.max_drawdown_bps is not None else None,
        rank_classification=observation.classification if observation is not None else "INCLUDED_SELECTION_RANK",
        review_rank=observation.review_rank if observation is not None else episode.current_rank,
        review_score=observation.review_score if observation is not None else Decimal(str(episode.current_score)) if episode.current_score is not None else None,
        weak_rank_confirmation_count=episode.weak_rank_confirm_days,
        decision_cutoff=decision_cutoff,
        tradability_status=current_mark.tradability_status,
        mark_quality=current_mark.mark_quality,
        source_evidence_hash=current_mark.source_evidence_hash,
    )
