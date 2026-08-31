from __future__ import annotations

from datetime import date

from backend.services.advisory_list_transition import (
    ACTION_EXIT,
    ACTION_HOLD,
    ACTION_WATCH,
    AdvisoryListTransitionEngine,
    AdvisoryTransitionCandidateV1,
    AdvisoryTransitionEpisodeV1,
    AdvisoryTransitionPolicyV1,
    AdvisoryTransitionRankObservationV1,
    EXIT_ALPHA_RANK_DROP,
    REVIEW_REASON_VALID_EMPTY,
)


def _policy() -> AdvisoryTransitionPolicyV1:
    return AdvisoryTransitionPolicyV1(
        target_count=2,
        rank_enter_threshold=2,
        rank_exit_threshold=3,
        rank_exit_confirm_days=2,
        daily_replacement_budget=1,
        stop_loss_bps=800,
        take_profit_bps=1800,
        trailing_stop_bps=600,
        time_stop_days=0,
    )


def _episode(symbol: str = "000001.SZ", *, weak_days: int = 1) -> AdvisoryTransitionEpisodeV1:
    return AdvisoryTransitionEpisodeV1(
        episode_id=f"episode_{symbol}",
        symbol=symbol,
        entry_signal_date=date(2026, 6, 1),
        effective_entry_date=date(2026, 6, 2),
        entry_price=10.0,
        entry_rank=1,
        current_rank=1,
        still_active_mark_price=10.0,
        weak_rank_confirm_days=weak_days,
    )


def _transition(*, candidates, active, observation):
    return AdvisoryListTransitionEngine().transition(
        policy=_policy(),
        decision_trade_date=date(2026, 6, 3),
        candidates=candidates,
        active_episodes=active,
        rank_observation=observation,
        episode_identity_allocator=lambda candidate: f"new_{candidate.symbol}",
        effective_entry_date=lambda _candidate: date(2026, 6, 4),
        effective_exit_date=lambda _episode: date(2026, 6, 4),
        defer_stop_before_effective_entry=False,
        historical_mode=True,
    )


def test_valid_empty_keeps_existing_weak_confirmation_and_evaluates_mark() -> None:
    episode = _episode(weak_days=1)
    result = _transition(
        candidates=(
            AdvisoryTransitionCandidateV1(
                symbol=episode.symbol,
                rank=1,
                score=None,
                entry_mark=None,
                exit_mark=10.2,
                entry_mark_available=False,
                reason_code=REVIEW_REASON_VALID_EMPTY,
            ),
        ),
        active=(episode,),
        observation=AdvisoryTransitionRankObservationV1(
            status="VALID_EMPTY_NO_SIGNAL",
            observed_max_selection_rank=0,
            active_rank_by_symbol={episode.symbol: None},
        ),
    )

    assert result.blocking_diagnostics == ()
    assert [(item.action, item.reason_code) for item in result.decisions] == [(ACTION_HOLD, REVIEW_REASON_VALID_EMPTY)]
    assert result.active_episodes[0].weak_rank_confirm_days == 1
    assert result.active_episodes[0].still_active_mark_price == 10.2


def test_missing_active_rank_uses_observed_max_plus_one_without_depth_gate() -> None:
    episode = _episode(weak_days=1)
    result = _transition(
        candidates=(
            AdvisoryTransitionCandidateV1(
                symbol=episode.symbol,
                rank=4,
                score=None,
                entry_mark=None,
                exit_mark=10.0,
                entry_mark_available=False,
                reason_code="NOT_IN_CURRENT_TOPK",
            ),
        ),
        active=(episode,),
        observation=AdvisoryTransitionRankObservationV1(
            status="COMPLETE",
            observed_max_selection_rank=3,
            active_rank_by_symbol={episode.symbol: 4},
        ),
    )

    assert result.decisions[0].action == ACTION_EXIT
    assert result.decisions[0].reason_code == EXIT_ALPHA_RANK_DROP
    assert result.exited_episodes[0].symbol == episode.symbol


def test_unavailable_entry_mark_projects_watch_without_consuming_slot() -> None:
    candidate = AdvisoryTransitionCandidateV1(
        symbol="000002.SZ",
        rank=1,
        score=0.5,
        entry_mark=None,
        exit_mark=None,
        entry_mark_available=False,
        exit_mark_available=False,
    )
    result = _transition(
        candidates=(candidate,),
        active=(),
        observation=AdvisoryTransitionRankObservationV1(
            status="COMPLETE",
            observed_max_selection_rank=1,
            active_rank_by_symbol={},
        ),
    )

    assert [(item.action, item.symbol) for item in result.decisions] == [(ACTION_WATCH, candidate.symbol)]
    assert result.active_episodes == ()
    assert result.replacement_budget_used == 0


def test_data_unavailable_rank_observation_blocks_without_synthesizing_success() -> None:
    result = _transition(
        candidates=(),
        active=(_episode(),),
        observation=AdvisoryTransitionRankObservationV1(
            status="DATA_UNAVAILABLE",
            observed_max_selection_rank=0,
            active_rank_by_symbol={},
        ),
    )

    assert result.decisions == ()
    assert result.blocking_diagnostics == ("ADVISORY_HR_RANK_OBSERVATION_DATA_UNAVAILABLE",)
