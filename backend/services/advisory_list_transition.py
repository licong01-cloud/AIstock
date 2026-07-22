"""Neutral Advisory list lifecycle engine.

Both the current Advisory wrapper and Phase 1R historical range adapter must
use this module for ENTER/HOLD/EXIT/WATCH semantics.  It deliberately has no
database, repository, CAS, calendar, package, or runtime imports.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from math import isfinite


ACTION_ENTER = "ENTER"
ACTION_HOLD = "HOLD"
ACTION_EXIT = "EXIT"
ACTION_WATCH = "WATCH"
ACTION_WAITING = "WAITING"

EXIT_NONE = "NONE"
EXIT_STOP_LOSS = "STOP_LOSS"
EXIT_STOP_LOSS_DEFERRED_T1 = "STOP_LOSS_DEFERRED_T1"
EXIT_TAKE_PROFIT = "TAKE_PROFIT"
EXIT_TRAILING_TAKE_PROFIT = "TRAILING_TAKE_PROFIT"
EXIT_ALPHA_RANK_DROP = "ALPHA_RANK_DROP_EXIT"
EXIT_TIME_STOP = "TIME_STOP"
EXIT_REPLACEMENT_BUDGET = "REPLACEMENT_BUDGET_LIMIT"
REVIEW_REASON_WAITING_PRICE = "WAITING_PRICE"
REVIEW_REASON_NOT_IN_CURRENT_TOPK = "NOT_IN_CURRENT_TOPK"
REVIEW_REASON_VALID_EMPTY = "ADVISORY_HR_VALID_EMPTY_NO_RANK_SIGNAL"
REVIEW_REASON_ENTRY_MARK_NOT_AVAILABLE = "ADVISORY_HR_ENTRY_MARK_NOT_AVAILABLE"


class AdvisoryListTransitionError(ValueError):
    """Visible lifecycle contract error with stable optional context."""

    def __init__(self, message: str, *, reason_code: str = "ADVISORY_LIST_TRANSITION_CONTRACT_INVALID") -> None:
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True)
class AdvisoryTransitionPolicyV1:
    target_count: int
    rank_enter_threshold: int
    rank_exit_threshold: int
    rank_exit_confirm_days: int
    daily_replacement_budget: int
    stop_loss_bps: int
    take_profit_bps: int
    trailing_stop_bps: int
    time_stop_days: int
    take_profit_mode: str = "trailing"

    def __post_init__(self) -> None:
        values = {
            "target_count": self.target_count,
            "rank_enter_threshold": self.rank_enter_threshold,
            "rank_exit_threshold": self.rank_exit_threshold,
            "rank_exit_confirm_days": self.rank_exit_confirm_days,
            "daily_replacement_budget": self.daily_replacement_budget,
            "stop_loss_bps": self.stop_loss_bps,
            "take_profit_bps": self.take_profit_bps,
            "trailing_stop_bps": self.trailing_stop_bps,
            "time_stop_days": self.time_stop_days,
        }
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in values.values()):
            raise AdvisoryListTransitionError("transition policy values must be non-negative integers")
        if self.target_count < 1 or self.rank_enter_threshold < 1 or self.rank_exit_threshold < self.rank_enter_threshold:
            raise AdvisoryListTransitionError("transition policy rank thresholds are invalid")
        if self.daily_replacement_budget < 1:
            raise AdvisoryListTransitionError("daily_replacement_budget must be positive")
        if self.take_profit_mode not in {"fixed", "trailing"}:
            raise AdvisoryListTransitionError("take_profit_mode must be fixed or trailing")


@dataclass(frozen=True)
class AdvisoryTransitionCandidateV1:
    symbol: str
    rank: int
    score: float | None
    entry_mark: float | None
    exit_mark: float | None
    entry_mark_available: bool = True
    exit_mark_available: bool = True
    stock_name: str | None = None
    source_run_id: str | None = None
    reason_code: str | None = None
    evidence: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        normalized = str(self.symbol or "").strip().upper()
        if not normalized:
            raise AdvisoryListTransitionError("candidate symbol is required")
        if not isinstance(self.rank, int) or isinstance(self.rank, bool) or self.rank < 1:
            raise AdvisoryListTransitionError("candidate rank must be a positive integer")
        _finite_optional(self.score, field_name="candidate score")
        _positive_optional(self.entry_mark, field_name="candidate entry mark")
        _positive_optional(self.exit_mark, field_name="candidate exit mark")
        object.__setattr__(self, "symbol", normalized)


@dataclass(frozen=True)
class AdvisoryTransitionEpisodeV1:
    episode_id: str
    symbol: str
    entry_signal_date: date
    effective_entry_date: date
    entry_price: float
    entry_rank: int
    entry_score: float | None = None
    current_rank: int | None = None
    current_score: float | None = None
    holding_trading_days: int = 0
    return_bps: float | None = None
    max_runup_bps: float | None = None
    max_drawdown_bps: float | None = None
    still_active_mark_price: float | None = None
    weak_rank_confirm_days: int = 0
    stock_name: str | None = None
    source_run_id: str | None = None
    evidence: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        if not str(self.episode_id or "").strip() or not str(self.symbol or "").strip():
            raise AdvisoryListTransitionError("episode identity is required")
        _positive_optional(self.entry_price, field_name="episode entry price", required=True)
        if self.holding_trading_days < 0 or self.weak_rank_confirm_days < 0:
            raise AdvisoryListTransitionError("episode counters cannot be negative")
        for name, value in (
            ("entry_score", self.entry_score),
            ("current_score", self.current_score),
            ("return_bps", self.return_bps),
            ("max_runup_bps", self.max_runup_bps),
            ("max_drawdown_bps", self.max_drawdown_bps),
        ):
            _finite_optional(value, field_name=name)
        _positive_optional(self.still_active_mark_price, field_name="episode current mark")
        object.__setattr__(self, "symbol", str(self.symbol).strip().upper())


@dataclass(frozen=True)
class AdvisoryTransitionRankObservationV1:
    status: str
    observed_max_selection_rank: int
    active_rank_by_symbol: Mapping[str, int | None]
    valid_empty_reason_by_symbol: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        if self.status not in {"COMPLETE", "VALID_EMPTY_NO_SIGNAL", "DATA_UNAVAILABLE"}:
            raise AdvisoryListTransitionError("rank observation status is invalid")
        if self.observed_max_selection_rank < 0:
            raise AdvisoryListTransitionError("observed_max_selection_rank cannot be negative")
        normalized = {str(symbol).strip().upper(): rank for symbol, rank in self.active_rank_by_symbol.items()}
        if any(not symbol for symbol in normalized):
            raise AdvisoryListTransitionError("rank observation symbol is required")
        if any(rank is not None and (not isinstance(rank, int) or isinstance(rank, bool) or rank < 1) for rank in normalized.values()):
            raise AdvisoryListTransitionError("rank observation ranks must be positive integers")
        object.__setattr__(self, "active_rank_by_symbol", normalized)

    @property
    def synthetic_missing_rank(self) -> int:
        return self.observed_max_selection_rank + 1


@dataclass(frozen=True)
class AdvisoryTransitionDecisionV1:
    action: str
    symbol: str
    reason_code: str
    episode: AdvisoryTransitionEpisodeV1 | None
    candidate: AdvisoryTransitionCandidateV1 | None
    entry_price: float | None = None
    exit_price: float | None = None


@dataclass(frozen=True)
class AdvisoryListTransitionResultV1:
    decisions: tuple[AdvisoryTransitionDecisionV1, ...]
    active_episodes: tuple[AdvisoryTransitionEpisodeV1, ...]
    exited_episodes: tuple[AdvisoryTransitionEpisodeV1, ...]
    watch_candidates: tuple[AdvisoryTransitionCandidateV1, ...]
    blocking_diagnostics: tuple[str, ...]
    replacement_budget_used: int


class AdvisoryListTransitionEngine:
    """Pure deterministic implementation of the approved lifecycle order."""

    def transition(
        self,
        *,
        policy: AdvisoryTransitionPolicyV1,
        decision_trade_date: date,
        candidates: Sequence[AdvisoryTransitionCandidateV1],
        active_episodes: Sequence[AdvisoryTransitionEpisodeV1],
        rank_observation: AdvisoryTransitionRankObservationV1,
        episode_identity_allocator: Callable[[AdvisoryTransitionCandidateV1], str],
        effective_entry_date: Callable[[AdvisoryTransitionCandidateV1], date],
        effective_exit_date: Callable[[AdvisoryTransitionEpisodeV1], date],
        defer_stop_before_effective_entry: bool,
        historical_mode: bool,
        entry_mark_unavailable_action: str = ACTION_WATCH,
    ) -> AdvisoryListTransitionResultV1:
        candidate_by_symbol = _unique_candidates(candidates)
        active_by_symbol = _unique_episodes(active_episodes)
        if rank_observation.status == "DATA_UNAVAILABLE":
            return AdvisoryListTransitionResultV1(
                decisions=(),
                active_episodes=tuple(active_episodes),
                exited_episodes=(),
                watch_candidates=(),
                blocking_diagnostics=("ADVISORY_HR_RANK_OBSERVATION_DATA_UNAVAILABLE",),
                replacement_budget_used=0,
            )

        decisions: list[AdvisoryTransitionDecisionV1] = []
        snapshots: list[AdvisoryTransitionEpisodeV1] = []
        exited: list[AdvisoryTransitionEpisodeV1] = []
        rank_drop: list[tuple[AdvisoryTransitionEpisodeV1, AdvisoryTransitionCandidateV1]] = []
        diagnostics: list[str] = []

        for episode in active_by_symbol.values():
            candidate = candidate_by_symbol.get(episode.symbol)
            if candidate is None:
                candidate = self._missing_candidate(
                    episode=episode,
                    observation=rank_observation,
                )
                if candidate is None:
                    if rank_observation.status == "VALID_EMPTY_NO_SIGNAL":
                        candidate = AdvisoryTransitionCandidateV1(
                            symbol=episode.symbol,
                            rank=rank_observation.synthetic_missing_rank,
                            score=episode.current_score,
                            entry_mark=None,
                            exit_mark=None,
                            reason_code=REVIEW_REASON_VALID_EMPTY,
                        )
                        # Valid empty freezes rank confirmation but still needs a
                        # mark.  The existing rank is retained for display only.
                        marked = self._mark_episode(episode, candidate, policy, increment_weak=None)
                    else:
                        diagnostics.append("ADVISORY_HR_ACTIVE_RANK_EVIDENCE_MISSING")
                        continue
                else:
                    marked = self._mark_episode(episode, candidate, policy, increment_weak=True)
            else:
                marked = self._mark_episode(
                    episode,
                    candidate,
                    policy,
                    increment_weak=(
                        None
                        if candidate.reason_code == REVIEW_REASON_VALID_EMPTY
                        else candidate.rank > policy.rank_exit_threshold
                    ),
                )
            if not candidate.exit_mark_available or candidate.exit_mark is None:
                if historical_mode:
                    diagnostics.append(f"ADVISORY_HR_MARK_DATA_UNAVAILABLE:{episode.symbol}")
                    continue
                waiting = replace(marked, current_rank=candidate.rank, current_score=candidate.score)
                snapshots.append(waiting)
                decisions.append(
                    AdvisoryTransitionDecisionV1(
                        action=ACTION_WAITING,
                        symbol=episode.symbol,
                        reason_code=REVIEW_REASON_WAITING_PRICE,
                        episode=waiting,
                        candidate=candidate,
                    )
                )
                continue
            exit_reason = _exit_reason(marked, candidate=candidate, policy=policy)
            if exit_reason == EXIT_ALPHA_RANK_DROP and marked.weak_rank_confirm_days < policy.rank_exit_confirm_days:
                exit_reason = None
            if exit_reason == EXIT_STOP_LOSS and defer_stop_before_effective_entry and decision_trade_date < marked.effective_entry_date:
                deferred = replace(marked)
                snapshots.append(deferred)
                decisions.append(
                    AdvisoryTransitionDecisionV1(
                        action=ACTION_WAITING,
                        symbol=episode.symbol,
                        reason_code=EXIT_STOP_LOSS_DEFERRED_T1,
                        episode=deferred,
                        candidate=candidate,
                    )
                )
            elif exit_reason == EXIT_ALPHA_RANK_DROP:
                rank_drop.append((marked, candidate))
            elif exit_reason is not None:
                completed = _exit_episode(marked, exit_price=candidate.exit_mark)
                exited.append(completed)
                decisions.append(
                    AdvisoryTransitionDecisionV1(
                        action=ACTION_EXIT,
                        symbol=episode.symbol,
                        reason_code=exit_reason,
                        episode=completed,
                        candidate=candidate,
                        exit_price=candidate.exit_mark,
                    )
                )
            else:
                snapshots.append(marked)
                decisions.append(
                    AdvisoryTransitionDecisionV1(
                        action=ACTION_HOLD,
                        symbol=episode.symbol,
                        reason_code=candidate.reason_code or EXIT_NONE,
                        episode=marked,
                        candidate=candidate,
                    )
                )

        if diagnostics:
            return AdvisoryListTransitionResultV1(
                decisions=tuple(decisions),
                active_episodes=tuple(sorted(snapshots, key=lambda row: row.symbol)),
                exited_episodes=tuple(sorted(exited, key=lambda row: row.symbol)),
                watch_candidates=(),
                blocking_diagnostics=tuple(sorted(diagnostics)),
                replacement_budget_used=0,
            )

        rank_drop.sort(key=lambda item: ((item[0].current_rank or 0), item[0].symbol), reverse=True)
        for index, (episode, candidate) in enumerate(rank_drop):
            if index < policy.daily_replacement_budget:
                completed = _exit_episode(episode, exit_price=candidate.exit_mark)
                exited.append(completed)
                decisions.append(
                    AdvisoryTransitionDecisionV1(
                        action=ACTION_EXIT,
                        symbol=episode.symbol,
                        reason_code=EXIT_ALPHA_RANK_DROP,
                        episode=completed,
                        candidate=candidate,
                        exit_price=candidate.exit_mark,
                    )
                )
            else:
                snapshots.append(episode)
                decisions.append(
                    AdvisoryTransitionDecisionV1(
                        action=ACTION_HOLD,
                        symbol=episode.symbol,
                        reason_code=EXIT_REPLACEMENT_BUDGET,
                        episode=episode,
                        candidate=candidate,
                    )
                )

        active_symbols = {item.symbol for item in snapshots}
        exited_symbols = {item.symbol for item in exited}
        slots = max(policy.target_count - len(active_symbols), 0)
        entry_limit = slots if not active_episodes else min(slots, policy.daily_replacement_budget)
        entered = 0
        watch: list[AdvisoryTransitionCandidateV1] = []
        for candidate in sorted(candidate_by_symbol.values(), key=lambda row: (row.rank, row.symbol)):
            if candidate.symbol in active_symbols or candidate.symbol in exited_symbols:
                continue
            if candidate.rank > policy.rank_enter_threshold:
                watch.append(candidate)
                continue
            if entered >= entry_limit:
                watch.append(candidate)
                continue
            if not candidate.entry_mark_available or candidate.entry_mark is None:
                unavailable = replace(candidate, reason_code=REVIEW_REASON_ENTRY_MARK_NOT_AVAILABLE)
                if entry_mark_unavailable_action == ACTION_WAITING:
                    decisions.append(
                        AdvisoryTransitionDecisionV1(
                            action=ACTION_WAITING,
                            symbol=unavailable.symbol,
                            reason_code="MISSING_ENTRY_PRICE",
                            episode=None,
                            candidate=unavailable,
                        )
                    )
                elif entry_mark_unavailable_action == ACTION_WATCH:
                    watch.append(unavailable)
                else:
                    raise AdvisoryListTransitionError("entry_mark_unavailable_action is invalid")
                continue
            episode = AdvisoryTransitionEpisodeV1(
                episode_id=episode_identity_allocator(candidate),
                symbol=candidate.symbol,
                entry_signal_date=decision_trade_date,
                effective_entry_date=effective_entry_date(candidate),
                entry_price=candidate.entry_mark,
                entry_rank=candidate.rank,
                entry_score=candidate.score,
                current_rank=candidate.rank,
                current_score=candidate.score,
                still_active_mark_price=candidate.entry_mark,
                max_runup_bps=0.0,
                max_drawdown_bps=0.0,
                stock_name=candidate.stock_name,
                source_run_id=candidate.source_run_id,
                evidence=candidate.evidence,
            )
            snapshots.append(episode)
            active_symbols.add(candidate.symbol)
            entered += 1
            decisions.append(
                AdvisoryTransitionDecisionV1(
                    action=ACTION_ENTER,
                    symbol=candidate.symbol,
                    reason_code=ACTION_ENTER,
                    episode=episode,
                    candidate=candidate,
                    entry_price=candidate.entry_mark,
                )
            )
        handled = {decision.symbol for decision in decisions} | {candidate.symbol for candidate in watch}
        watch.extend(
            candidate
            for candidate in sorted(candidate_by_symbol.values(), key=lambda row: (row.rank, row.symbol))
            if candidate.symbol not in handled and candidate.rank <= policy.rank_enter_threshold
        )
        for candidate in watch:
            decisions.append(
                AdvisoryTransitionDecisionV1(
                    action=ACTION_WATCH,
                    symbol=candidate.symbol,
                    reason_code=candidate.reason_code or ACTION_WATCH,
                    episode=None,
                    candidate=candidate,
                )
            )
        active = tuple(sorted(snapshots, key=lambda row: row.symbol))
        if len(active) > policy.target_count:
            raise AdvisoryListTransitionError("transition produced more active episodes than target_count")
        return AdvisoryListTransitionResultV1(
            decisions=tuple(decisions),
            active_episodes=active,
            exited_episodes=tuple(sorted(exited, key=lambda row: row.symbol)),
            watch_candidates=tuple(sorted(watch, key=lambda row: (row.rank, row.symbol))),
            blocking_diagnostics=(),
            replacement_budget_used=min(len(rank_drop), policy.daily_replacement_budget),
        )

    @staticmethod
    def _missing_candidate(
        *,
        episode: AdvisoryTransitionEpisodeV1,
        observation: AdvisoryTransitionRankObservationV1,
    ) -> AdvisoryTransitionCandidateV1 | None:
        rank = observation.active_rank_by_symbol.get(episode.symbol)
        if rank is None:
            return None
        return AdvisoryTransitionCandidateV1(
            symbol=episode.symbol,
            rank=rank,
            score=episode.current_score,
            entry_mark=None,
            exit_mark=episode.still_active_mark_price,
            reason_code=REVIEW_REASON_NOT_IN_CURRENT_TOPK,
        )

    @staticmethod
    def _mark_episode(
        episode: AdvisoryTransitionEpisodeV1,
        candidate: AdvisoryTransitionCandidateV1,
        policy: AdvisoryTransitionPolicyV1,
        *,
        increment_weak: bool | None,
    ) -> AdvisoryTransitionEpisodeV1:
        price = candidate.exit_mark
        if price is None:
            return replace(episode, current_rank=candidate.rank, current_score=candidate.score)
        return_bps = (price / episode.entry_price - 1.0) * 10000.0
        return replace(
            episode,
            current_rank=candidate.rank,
            current_score=candidate.score,
            holding_trading_days=episode.holding_trading_days + 1,
            return_bps=return_bps,
            max_runup_bps=max(_coalesce(episode.max_runup_bps, return_bps), return_bps),
            max_drawdown_bps=min(_coalesce(episode.max_drawdown_bps, return_bps), return_bps),
            still_active_mark_price=price,
            weak_rank_confirm_days=(
                episode.weak_rank_confirm_days + 1
                if increment_weak is True
                else episode.weak_rank_confirm_days
                if increment_weak is None
                else 0
            ),
            evidence=candidate.evidence,
        )


def _unique_candidates(rows: Sequence[AdvisoryTransitionCandidateV1]) -> dict[str, AdvisoryTransitionCandidateV1]:
    result = {row.symbol: row for row in rows}
    if len(result) != len(rows):
        raise AdvisoryListTransitionError("candidate symbols must be unique")
    return result


def _unique_episodes(rows: Sequence[AdvisoryTransitionEpisodeV1]) -> dict[str, AdvisoryTransitionEpisodeV1]:
    result = {row.symbol: row for row in rows}
    if len(result) != len(rows):
        raise AdvisoryListTransitionError("active episode symbols must be unique")
    return result


def _exit_reason(
    episode: AdvisoryTransitionEpisodeV1,
    *,
    candidate: AdvisoryTransitionCandidateV1,
    policy: AdvisoryTransitionPolicyV1,
) -> str | None:
    if episode.return_bps is None:
        return None
    if policy.stop_loss_bps > 0 and episode.return_bps <= -policy.stop_loss_bps:
        return EXIT_STOP_LOSS
    if policy.time_stop_days > 0 and episode.holding_trading_days >= policy.time_stop_days:
        return EXIT_TIME_STOP
    if candidate.rank > policy.rank_exit_threshold:
        return EXIT_ALPHA_RANK_DROP
    if policy.take_profit_bps > 0:
        if policy.take_profit_mode == "fixed" and episode.return_bps >= policy.take_profit_bps:
            return EXIT_TAKE_PROFIT
        if (
            (episode.max_runup_bps or 0) >= policy.take_profit_bps
            and policy.trailing_stop_bps > 0
            and episode.return_bps <= (episode.max_runup_bps or 0) - policy.trailing_stop_bps
        ):
            return EXIT_TRAILING_TAKE_PROFIT
    return None


def _exit_episode(episode: AdvisoryTransitionEpisodeV1, *, exit_price: float | None) -> AdvisoryTransitionEpisodeV1:
    if exit_price is None:
        raise AdvisoryListTransitionError("cannot exit an episode without a mark")
    return replace(episode, still_active_mark_price=None)


def _coalesce(value: float | None, fallback: float) -> float:
    return fallback if value is None else float(value)


def _finite_optional(value: float | None, *, field_name: str) -> None:
    if value is not None and (not isinstance(value, (float, int)) or isinstance(value, bool) or not isfinite(float(value))):
        raise AdvisoryListTransitionError(f"{field_name} must be finite")


def _positive_optional(value: float | None, *, field_name: str, required: bool = False) -> None:
    if value is None:
        if required:
            raise AdvisoryListTransitionError(f"{field_name} is required")
        return
    if not isinstance(value, (float, int)) or isinstance(value, bool) or not isfinite(float(value)) or float(value) <= 0:
        raise AdvisoryListTransitionError(f"{field_name} must be a positive finite number")
