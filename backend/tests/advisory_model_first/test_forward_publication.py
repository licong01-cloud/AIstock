from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from backend.services.advisory_forward.service import ACTION_WATCH, _build_publication_list
from backend.services.advisory_program import ACTION_HOLD, ACTION_WAITING, AdvisoryCandidate, AdvisoryProgram


def _program() -> AdvisoryProgram:
    return AdvisoryProgram(
        program_id="advp_test",
        program_name="test",
        status="ENABLED",
        target_count=20,
        package_mode="single_package",
        package_ids=["pkg_test"],
        package_weights={"pkg_test": 1.0},
        fusion_method=None,
        package_set_hash="a" * 64,
        fusion_policy_sha256=None,
        review_policy={
            "rank_enter_threshold": 20,
            "rank_exit_threshold": 40,
            "rank_exit_confirm_days": 2,
            "daily_replacement_budget": 5,
            "stop_loss_bps": 800,
            "take_profit_bps": 1800,
            "trailing_stop_bps": 700,
            "time_stop_days": 20,
            "take_profit_mode": "trailing",
        },
        review_policy_sha256="b" * 64,
        entry_price_basis="next_open_executable",
        exit_price_basis="next_open_executable",
        review_schedule={"frequency": "daily_after_close"},
    )


def test_after_close_publication_uses_watch_not_fake_enter_and_preserves_active_hold() -> None:
    candidates = [
        AdvisoryCandidate(symbol="000001.SZ", rank=1, score=1.0, next_open_executable=99.0),
        AdvisoryCandidate(symbol="000002.SZ", rank=2, score=0.9, next_open_executable=88.0),
    ]
    active = [SimpleNamespace(symbol="000002.SZ", episode_id="ep-2")]

    version, items = _build_publication_list(
        program=_program(),
        binding_version_id="advb_test",
        review_run_id="review-test",
        target_trade_date=date(2026, 8, 17),
        decision_as_of_trade_date=date(2026, 8, 14),
        selection_run_id="sel-test",
        candidates=candidates,
        active_episodes=active,
        previous_list=None,
        previous_items=[],
    )

    assert version.trade_date == date(2026, 8, 17)
    assert version.entered_count == 0
    assert [item.action for item in items] == [ACTION_WATCH, ACTION_HOLD]
    assert all(item.entry_price is None for item in items)
    assert all(item.effective_trade_date == date(2026, 8, 17) for item in items)
    assert items[0].evidence_json["decision_as_of_trade_date"] == "2026-08-14"


def test_after_close_publication_keeps_active_episode_missing_from_selection_as_waiting() -> None:
    candidates = [AdvisoryCandidate(symbol="000001.SZ", rank=1, score=1.0)]
    active = [SimpleNamespace(
        symbol="000099.SZ",
        episode_id="ep-missing",
        stock_name="missing active",
        current_rank=35,
        current_score=0.2,
    )]

    version, items = _build_publication_list(
        program=_program(),
        binding_version_id="advb_test",
        review_run_id="review-test",
        target_trade_date=date(2026, 8, 17),
        decision_as_of_trade_date=date(2026, 8, 14),
        selection_run_id="sel-test",
        candidates=candidates,
        active_episodes=active,
        previous_list=None,
        previous_items=[],
    )

    waiting = next(item for item in items if item.symbol == "000099.SZ")
    assert waiting.action == ACTION_WAITING
    assert waiting.episode_id == "ep-missing"
    assert waiting.rank is None
    assert waiting.entry_price is None
    assert version.waiting_count == 1
