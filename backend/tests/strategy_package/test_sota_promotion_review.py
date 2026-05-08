from __future__ import annotations

import inspect

import pytest

from backend.routers import quantevolver_evolution
from backend.services.quantevolver import qe_evolution_service
from backend.services.strategy_package.promotion_review import (
    InMemoryPromotionReviewRepository,
    PromotionReviewService,
    PromotionReviewStatus,
)
from backend.services.trading_core.errors import InvalidStateTransitionError


def test_manual_loop_promotion_creates_review_pending_not_approved() -> None:
    repo = InMemoryPromotionReviewRepository()
    service = PromotionReviewService(repository=repo)

    review = service.request_loop_review(
        task_id="qe_dev_20260509_manual",
        loop_id="Loop1",
        requested_by="unit_test",
        review_reason="candidate looks strong",
        source_metrics={"IC": 0.061},
        experiment_id="qe_dev_20260509_manual_L1",
    )

    assert review.status == PromotionReviewStatus.REVIEW_PENDING
    assert review.loop_id == "qe_dev_20260509_manual_Loop1"
    assert review.source_metrics["IC"] == 0.061
    assert review.audit_context["approved_sota"] is False
    assert review.audit_context["paper_enabled"] is False


def test_manual_loop_promotion_is_idempotent_while_pending() -> None:
    repo = InMemoryPromotionReviewRepository()
    service = PromotionReviewService(repository=repo)

    first = service.request_loop_review(
        task_id="qe_dev_20260509_manual",
        loop_id="qe_dev_20260509_manual_Loop2",
        requested_by="unit_test",
    )
    second = service.request_loop_review(
        task_id="qe_dev_20260509_manual",
        loop_id="qe_dev_20260509_manual_Loop2",
        requested_by="unit_test_again",
    )

    assert second.review_id == first.review_id
    assert second.status == PromotionReviewStatus.REVIEW_PENDING


def test_manual_loop_promotion_fail_fast_after_decision() -> None:
    repo = InMemoryPromotionReviewRepository()
    service = PromotionReviewService(repository=repo)
    review = service.request_loop_review(
        task_id="qe_dev_20260509_manual",
        loop_id="Loop3",
        requested_by="unit_test",
    )
    repo.records[(review.source_type, review.source_id)] = review.model_copy(
        update={"status": PromotionReviewStatus.REVIEW_REJECTED}
    )

    with pytest.raises(InvalidStateTransitionError, match="already exists"):
        service.request_loop_review(
            task_id="qe_dev_20260509_manual",
            loop_id="Loop3",
            requested_by="unit_test",
        )


def test_qe_evaluator_no_longer_auto_inserts_approved_sota_registry() -> None:
    source = inspect.getsource(qe_evolution_service.AutoEvolutionScheduler.process_completed_loop)

    assert "INSERT INTO qe_sota_registry" not in source
    assert "automatic candidate only" in source


def test_legacy_sota_leaderboard_read_path_still_uses_registry() -> None:
    source = inspect.getsource(quantevolver_evolution.get_sota_leaderboard)

    assert "FROM qe_sota_registry" in source
    assert "JOIN qe_evolution_loops" in source
    assert "automatic_candidates" in source
    assert "AUTO_CANDIDATE" in source
    assert "approved_sota" in source
    assert "FALSE AS approved_sota" in source
    assert "leaderboard" in source
