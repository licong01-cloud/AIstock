from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from backend.services.advisory_lifecycle import (
    AdvisoryLifecycleService,
    AdvisoryMarketSnapshot,
    AdvisoryWatchlistItem,
    DailySelectionEvidenceSnapshot,
    FusionPolicy,
    InMemoryAdvisoryReviewRepository,
    LIFECYCLE_CANDIDATE,
    LIFECYCLE_ENTERED,
    LIFECYCLE_EXITED,
    LIFECYCLE_HOLDING,
    PackageSelectionEvidence,
    adjust_price_for_factor,
    adjusted_stop_take,
    build_fusion_evidence_by_code,
)
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError
from backend.services.trading_core.exit_guard import ExitGuardPolicy
from backend.services.trading_core.price_guard import (
    ALPHA_RANK_DROP_EXIT,
    STOP_LOSS_DEFERRED_T1,
    STOP_LOSS_TRIGGERED,
    WAITING_FOR_PRICE_GUARD_INPUT,
)


def _service() -> tuple[AdvisoryLifecycleService, InMemoryAdvisoryReviewRepository]:
    repo = InMemoryAdvisoryReviewRepository()
    return AdvisoryLifecycleService(repository=repo), repo


def _policy() -> ExitGuardPolicy:
    return ExitGuardPolicy(policy_sha256="exit-sha")


def test_s1_6_lifecycle_state_machine_legal_and_illegal_transitions() -> None:
    service, repo = _service()

    assert service.transition_status(LIFECYCLE_CANDIDATE, LIFECYCLE_ENTERED, watchlist_item_id=1) == LIFECYCLE_ENTERED
    assert service.transition_status(LIFECYCLE_ENTERED, LIFECYCLE_HOLDING, watchlist_item_id=1) == LIFECYCLE_HOLDING
    assert service.transition_status(LIFECYCLE_HOLDING, LIFECYCLE_EXITED, watchlist_item_id=1, exit_reason="x") == LIFECYCLE_EXITED
    assert repo.lifecycle_updates[-1]["exit_reason"] == "x"

    with pytest.raises(InvalidStateTransitionError):
        service.transition_status(LIFECYCLE_EXITED, LIFECYCLE_HOLDING)
    with pytest.raises(InvalidStateTransitionError):
        service.transition_status(LIFECYCLE_CANDIDATE, LIFECYCLE_EXITED)


def test_s1_7_daily_review_is_append_only_idempotent_and_updates_holding_once() -> None:
    service, repo = _service()
    item = AdvisoryWatchlistItem(
        watchlist_item_id=1,
        code="000001.SZ",
        lifecycle_status=LIFECYCLE_ENTERED,
        actual_entry_price=10.0,
        actual_entry_date=date(2026, 6, 1),
    )
    evidence = {
        item.code: DailySelectionEvidenceSnapshot(
            evidence_id="ev1",
            code=item.code,
            trade_date=date(2026, 6, 2),
            score=0.9,
            rank=2,
            latest_rank_pct=0.02,
            feature_availability_ts=datetime(2026, 6, 2, 9, 20, tzinfo=UTC),
        )
    }
    market = {item.code: AdvisoryMarketSnapshot(code=item.code, trade_date=date(2026, 6, 2), current_price=10.2)}

    first = service.run_daily_review(items=[item], evidence_by_code=evidence, market_by_code=market, trade_date=date(2026, 6, 2), policy=_policy())
    second = service.run_daily_review(items=[item], evidence_by_code=evidence, market_by_code=market, trade_date=date(2026, 6, 2), policy=_policy())

    assert first[0] is second[0]
    assert len(repo.reviews) == 1
    assert repo.lifecycle_updates[0]["status"] == LIFECYCLE_HOLDING
    assert first[0].layer == "advisory"


def test_s1_8_same_day_hard_stop_deferred_t1_does_not_exit() -> None:
    service, repo = _service()
    item = AdvisoryWatchlistItem(
        watchlist_item_id=1,
        code="000001.SZ",
        lifecycle_status=LIFECYCLE_ENTERED,
        actual_entry_price=10.0,
        actual_entry_date=date(2026, 6, 2),
    )
    market = {item.code: AdvisoryMarketSnapshot(code=item.code, trade_date=date(2026, 6, 2), current_price=9.3)}

    records = service.run_daily_review(items=[item], evidence_by_code={}, market_by_code=market, trade_date=date(2026, 6, 2), policy=_policy())

    assert records[0].reason_code == STOP_LOSS_DEFERRED_T1
    assert records[0].t1_note == "T+1 defer_to_next_tradable_day"
    assert repo.lifecycle_updates == []


def test_s1_9_next_day_stop_rank_drop_factor_adjustment_and_suspend_carry() -> None:
    service, repo = _service()
    stop_item = AdvisoryWatchlistItem(
        watchlist_item_id=1,
        code="000001.SZ",
        lifecycle_status=LIFECYCLE_HOLDING,
        actual_entry_price=10.0,
        actual_entry_date=date(2026, 6, 1),
    )
    stop_market = {stop_item.code: AdvisoryMarketSnapshot(code=stop_item.code, trade_date=date(2026, 6, 2), current_price=9.3)}
    stop_records = service.run_daily_review(items=[stop_item], evidence_by_code={}, market_by_code=stop_market, trade_date=date(2026, 6, 2), policy=_policy())
    assert stop_records[0].reason_code == STOP_LOSS_TRIGGERED
    assert repo.lifecycle_updates[-1]["status"] == LIFECYCLE_EXITED

    rank_item = AdvisoryWatchlistItem(
        watchlist_item_id=2,
        code="000002.SZ",
        lifecycle_status=LIFECYCLE_HOLDING,
        actual_entry_price=10.0,
        actual_entry_date=date(2026, 6, 1),
    )
    rank_evidence = {
        rank_item.code: DailySelectionEvidenceSnapshot(
            evidence_id="ev2",
            code=rank_item.code,
            trade_date=date(2026, 6, 2),
            rank=80,
            latest_rank_pct=0.8,
            alpha_decay_confirm_days=2,
        )
    }
    rank_market = {rank_item.code: AdvisoryMarketSnapshot(code=rank_item.code, trade_date=date(2026, 6, 2), current_price=10.0)}
    rank_records = service.run_daily_review(items=[rank_item], evidence_by_code=rank_evidence, market_by_code=rank_market, trade_date=date(2026, 6, 2), policy=_policy())
    assert rank_records[0].reason_code == ALPHA_RANK_DROP_EXIT

    assert adjust_price_for_factor(10.0, base_factor=1.0, current_factor=0.5) == pytest.approx(5.0)
    assert adjusted_stop_take(stop_price=9.4, take_price=11.2, base_factor=1.0, current_factor=0.5) == {
        "stop_price": pytest.approx(4.7),
        "take_price": pytest.approx(5.6),
    }

    suspend_item = AdvisoryWatchlistItem(
        watchlist_item_id=3,
        code="000003.SZ",
        lifecycle_status=LIFECYCLE_HOLDING,
        actual_entry_price=10.0,
        actual_entry_date=date(2026, 6, 1),
    )
    suspend_market = {suspend_item.code: AdvisoryMarketSnapshot(code=suspend_item.code, trade_date=date(2026, 6, 2), current_price=None, suspend_status="SUSPENDED")}
    suspend_records = service.run_daily_review(items=[suspend_item], evidence_by_code={}, market_by_code=suspend_market, trade_date=date(2026, 6, 2), policy=_policy())
    assert suspend_records[0].action == "WAITING"
    assert suspend_records[0].reason_code == WAITING_FOR_PRICE_GUARD_INPUT
    assert suspend_records[0].t1_note == "suspended_carry"


def test_s1_10_advisory_lifecycle_has_no_oms_broker_or_paper_ledger_writes() -> None:
    import inspect
    import backend.services.advisory_lifecycle as module

    source = inspect.getsource(module)

    assert "create_order" not in source
    assert "submit_order" not in source
    assert "position_ledger" not in source
    assert "paper_v2." not in source


def test_s1_11_multi_package_fusion_uses_canonical_rank_and_preserves_package_evidence() -> None:
    trade_date = date(2026, 6, 3)
    policy = FusionPolicy(package_weights={"pkg_a": 0.25, "pkg_b": 0.75}, candidate_top_k=40)
    evidence = build_fusion_evidence_by_code(
        package_evidence_by_code={
            "000001.SZ": {
                "pkg_a": PackageSelectionEvidence(
                    package_id="pkg_a",
                    evidence_id="ev_a_1",
                    code="000001.SZ",
                    trade_date=trade_date,
                    score=100.0,
                    rank=1,
                    candidate_count=2,
                )
            },
            "000002.SZ": {
                "pkg_a": PackageSelectionEvidence(
                    package_id="pkg_a",
                    evidence_id="ev_a_2",
                    code="000002.SZ",
                    trade_date=trade_date,
                    score=90.0,
                    rank=2,
                    candidate_count=2,
                ),
                "pkg_b": PackageSelectionEvidence(
                    package_id="pkg_b",
                    evidence_id="ev_b_1",
                    code="000002.SZ",
                    trade_date=trade_date,
                    score=0.1,
                    rank=1,
                    candidate_count=2,
                ),
            },
        },
        trade_date=trade_date,
        fusion_policy=policy,
    )

    assert evidence["000002.SZ"].rank == 1
    assert evidence["000002.SZ"].fusion_rank == 1
    assert evidence["000002.SZ"].fusion_score == pytest.approx(0.75)
    assert evidence["000002.SZ"].package_raw_scores == {"pkg_a": 90.0, "pkg_b": 0.1}
    assert evidence["000002.SZ"].package_raw_ranks == {"pkg_a": 2, "pkg_b": 1}
    assert evidence["000002.SZ"].package_rank_scores == {"pkg_a": 0.0, "pkg_b": 1.0}
    assert evidence["000002.SZ"].support_count == 2
    assert evidence["000002.SZ"].rank_dispersion == 1
    assert evidence["000002.SZ"].fusion_policy_sha256
    assert evidence["000002.SZ"].latest_rank_pct == pytest.approx(1 / 40)
    assert evidence["000002.SZ"].evidence_id is None
    assert evidence["000001.SZ"].package_presence["pkg_b"] == "not_selected_in_full_evidence"


def test_s1_12_multi_package_daily_review_consumes_fusion_rank_for_alpha_decay() -> None:
    service, repo = _service()
    trade_date = date(2026, 6, 3)
    item = AdvisoryWatchlistItem(
        watchlist_item_id=7,
        code="000007.SZ",
        lifecycle_status=LIFECYCLE_HOLDING,
        actual_entry_price=10.0,
        actual_entry_date=date(2026, 6, 1),
    )
    package_evidence = {
        "000001.SZ": {
            "pkg_a": PackageSelectionEvidence(
                package_id="pkg_a",
                evidence_id="ev_a_1",
                code="000001.SZ",
                trade_date=trade_date,
                score=2.0,
                rank=1,
                candidate_count=100,
            ),
            "pkg_b": PackageSelectionEvidence(
                package_id="pkg_b",
                evidence_id="ev_b_1",
                code="000001.SZ",
                trade_date=trade_date,
                score=2.0,
                rank=1,
                candidate_count=100,
            ),
        },
        item.code: {
            "pkg_a": PackageSelectionEvidence(
                package_id="pkg_a",
                evidence_id="ev_a_7",
                code=item.code,
                trade_date=trade_date,
                score=1.0,
                rank=80,
                candidate_count=100,
            ),
            "pkg_b": PackageSelectionEvidence(
                package_id="pkg_b",
                evidence_id="ev_b_7",
                code=item.code,
                trade_date=trade_date,
                score=0.2,
                rank=70,
                candidate_count=100,
            ),
        }
    }
    market = {item.code: AdvisoryMarketSnapshot(code=item.code, trade_date=trade_date, current_price=10.0)}

    records = service.run_multi_package_daily_review(
        items=[item],
        package_evidence_by_code=package_evidence,
        market_by_code=market,
        trade_date=trade_date,
        policy=ExitGuardPolicy(
            policy_sha256="exit-sha",
            alpha_decay_exit={"enabled": True, "rank_drop_below": "top1", "confirm_days": 0},
        ),
        fusion_policy=FusionPolicy(package_weights={"pkg_a": 0.5, "pkg_b": 0.5}, candidate_top_k=2),
    )

    assert records[0].rank == 2
    assert records[0].score == pytest.approx((20 / 99 + 30 / 99) / 2)
    assert records[0].evidence_id is None
    assert records[0].reason_code == ALPHA_RANK_DROP_EXIT
    assert repo.lifecycle_updates[-1]["status"] == LIFECYCLE_EXITED


def test_s1_13_multi_package_fusion_fails_fast_on_missing_data_presence() -> None:
    trade_date = date(2026, 6, 3)

    with pytest.raises(DataUnavailableError, match="data_missing"):
        build_fusion_evidence_by_code(
            package_evidence_by_code={
                "000001.SZ": {
                    "pkg_a": PackageSelectionEvidence(
                        package_id="pkg_a",
                        evidence_id="ev_a",
                        code="000001.SZ",
                        trade_date=trade_date,
                        rank=1,
                        candidate_count=10,
                    ),
                    "pkg_b": PackageSelectionEvidence(
                        package_id="pkg_b",
                        evidence_id=None,
                        code="000001.SZ",
                        trade_date=trade_date,
                        presence="data_missing",
                    ),
                }
            },
            trade_date=trade_date,
            fusion_policy=FusionPolicy(package_weights={"pkg_a": 1.0, "pkg_b": 1.0}),
        )


def test_s1_14_multi_package_daily_review_requires_active_item_coverage() -> None:
    service, _ = _service()
    trade_date = date(2026, 6, 3)
    item = AdvisoryWatchlistItem(
        watchlist_item_id=8,
        code="000008.SZ",
        lifecycle_status=LIFECYCLE_HOLDING,
        actual_entry_price=10.0,
        actual_entry_date=date(2026, 6, 1),
    )

    with pytest.raises(DataUnavailableError, match="requires fusion evidence coverage"):
        service.run_multi_package_daily_review(
            items=[item],
            package_evidence_by_code={
                "000001.SZ": {
                    "pkg_a": PackageSelectionEvidence(
                        package_id="pkg_a",
                        evidence_id="ev_a_1",
                        code="000001.SZ",
                        trade_date=trade_date,
                        rank=1,
                        candidate_count=10,
                    )
                }
            },
            market_by_code={item.code: AdvisoryMarketSnapshot(code=item.code, trade_date=trade_date, current_price=10.0)},
            trade_date=trade_date,
            policy=_policy(),
            fusion_policy=FusionPolicy(package_weights={"pkg_a": 1.0}),
        )
