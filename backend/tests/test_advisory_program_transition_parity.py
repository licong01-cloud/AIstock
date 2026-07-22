from datetime import date
from types import SimpleNamespace

import pytest

from backend.services.advisory_list_transition import (
    AdvisoryListTransitionEngine,
    AdvisoryTransitionCandidateV1,
    AdvisoryTransitionEpisodeV1,
    AdvisoryTransitionPolicyV1,
    AdvisoryTransitionRankObservationV1,
)
from backend.services.advisory_program import (
    DEFAULT_REVIEW_POLICY,
    PRICE_BASIS_SIGNAL_CLOSE,
    AdvisoryCandidate,
    AdvisoryEpisode,
    AdvisoryProgram,
    AdvisoryProgramService,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256


_DAY = date(2026, 7, 22)


def _program() -> AdvisoryProgram:
    review_policy = dict(DEFAULT_REVIEW_POLICY)
    return AdvisoryProgram(
        program_id="advp-parity",
        program_name="parity",
        status="ACTIVE",
        target_count=1,
        package_mode="SINGLE_PACKAGE",
        package_ids=["pkg-parity"],
        package_weights={"pkg-parity": 1.0},
        fusion_method=None,
        package_set_hash=canonical_json_sha256(["pkg-parity"]),
        fusion_policy_sha256=None,
        review_policy=review_policy,
        review_policy_sha256=canonical_json_sha256(review_policy),
        entry_price_basis=PRICE_BASIS_SIGNAL_CLOSE,
        exit_price_basis=PRICE_BASIS_SIGNAL_CLOSE,
        review_schedule={},
    )


def _episode() -> AdvisoryEpisode:
    return AdvisoryEpisode(
        episode_id="episode-parity",
        program_id="advp-parity",
        program_version=1,
        symbol="000001.SZ",
        status="ACTIVE",
        signal_date=date(2026, 7, 18),
        effective_entry_date=date(2026, 7, 18),
        entry_price=10.0,
        entry_price_basis=PRICE_BASIS_SIGNAL_CLOSE,
        entry_rank=1,
        entry_score=0.9,
        current_rank=1,
        current_score=0.9,
        holding_trading_days=2,
        still_active_mark_price=10.0,
    )


@pytest.mark.parametrize(("mark", "expected_action"), ((10.5, "HOLD"), (9.0, "EXIT")))
def test_current_wrapper_matches_shared_lifecycle_engine(mark: float, expected_action: str) -> None:
    program = _program()
    episode = _episode()
    candidate = AdvisoryCandidate(
        symbol=episode.symbol,
        rank=1,
        score=0.95,
        signal_close=mark,
    )
    service = AdvisoryProgramService(
        repository=SimpleNamespace(),
        selection_service=SimpleNamespace(),
        calendar_provider=SimpleNamespace(),
        symbol_name_resolver=SimpleNamespace(),
    )
    current = service._evaluate_review(
        program=program,
        trade_date=_DAY,
        candidates=[candidate],
        market_by_symbol={},
        active_episodes=[episode],
        preview=False,
    )
    policy = AdvisoryTransitionPolicyV1(target_count=1, **program.review_policy)
    direct = AdvisoryListTransitionEngine().transition(
        policy=policy,
        decision_trade_date=_DAY,
        candidates=(
            AdvisoryTransitionCandidateV1(
                symbol=episode.symbol,
                rank=1,
                score=0.95,
                entry_mark=mark,
                exit_mark=mark,
            ),
        ),
        active_episodes=(
            AdvisoryTransitionEpisodeV1(
                episode_id=episode.episode_id,
                symbol=episode.symbol,
                entry_signal_date=episode.signal_date,
                effective_entry_date=episode.effective_entry_date,
                entry_price=episode.entry_price,
                entry_rank=episode.entry_rank,
                entry_score=episode.entry_score,
                current_rank=episode.current_rank,
                current_score=episode.current_score,
                holding_trading_days=episode.holding_trading_days,
                still_active_mark_price=episode.still_active_mark_price,
            ),
        ),
        rank_observation=AdvisoryTransitionRankObservationV1(
            status="COMPLETE",
            observed_max_selection_rank=1,
            active_rank_by_symbol={episode.symbol: 1},
        ),
        episode_identity_allocator=lambda _candidate: "unused",
        effective_entry_date=lambda _candidate: _DAY,
        effective_exit_date=lambda _episode: _DAY,
        defer_stop_before_effective_entry=True,
        historical_mode=False,
        entry_mark_unavailable_action="WAITING",
    )
    assert [item.action for item in current.decisions] == [expected_action]
    assert [item.action for item in direct.decisions if item.action != "WATCH"] == [expected_action]
