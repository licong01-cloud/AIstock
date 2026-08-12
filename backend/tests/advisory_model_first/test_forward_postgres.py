from __future__ import annotations

from datetime import date
import pytest

from backend.services.advisory_forward.models import AdvisoryForwardModelObservationV1
from backend.services.advisory_forward.repository import (
    AdvisoryForwardPGRepository,
    _validate_observation_identity,
)
from backend.services.trading_core.errors import InvalidStateTransitionError
from backend.services.advisory_program import (
    AdvisoryProgram,
    AdvisoryRecommendationListItem,
    AdvisoryRecommendationListVersion,
    AdvisoryReviewRun,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


class _Cursor:
    def __init__(self, *, fail_on: str | None = None) -> None:
        self.fail_on = fail_on
        self.one = None
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        assert str(sql).count("%s") == len(params), str(sql)
        normalized = " ".join(str(sql).split())
        if self.fail_on and self.fail_on in normalized:
            raise RuntimeError("injected transaction failure")
        self.one = None
        self.rows = []
        if "FROM app.advisory_forward_run" in normalized:
            self.one = {
                "forward_run_id": "advfwd-test",
                "publication_status": "PENDING",
                "settlement_status": "NOT_DUE",
            }
        elif "SELECT version, status FROM app.advisory_program" in normalized:
            self.one = {"version": 3, "status": "ENABLED"}
        elif "SELECT binding_version_id FROM app.advisory_strategy_binding_version" in normalized:
            self.one = {"binding_version_id": "advb-test"}
        elif "UPDATE app.advisory_forward_run" in normalized:
            self.one = {"forward_run_id": "advfwd-test", "publication_status": "PUBLISHED"}

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, *, fail_on: str | None = None) -> None:
        self.cursor_instance = _Cursor(fail_on=fail_on)
        self.rolled_back = False
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_args):
        self.rolled_back = exc_type is not None
        self.committed = exc_type is None
        return False

    def cursor(self, **_kwargs):
        return self.cursor_instance


def _program() -> AdvisoryProgram:
    return AdvisoryProgram(
        program_id="advp-test",
        program_name="test",
        status="ENABLED",
        version=3,
        target_count=20,
        package_mode="single_package",
        package_ids=["pkg-test"],
        package_weights={"pkg-test": 1.0},
        fusion_method=None,
        package_set_hash="a" * 64,
        fusion_policy_sha256=None,
        review_policy={"rank_enter_threshold": 20},
        review_policy_sha256="b" * 64,
        entry_price_basis="next_open_executable",
        exit_price_basis="next_open_executable",
        review_schedule={"frequency": "daily_after_close"},
    )


def _publication_inputs():
    review = AdvisoryReviewRun(
        review_run_id="review-test",
        program_id="advp-test",
        binding_version_id="advb-test",
        trade_date=date(2026, 8, 17),
        run_type="RUN",
        status="WAITING_DATA",
        data_source="DB_HISTORICAL",
    )
    version = AdvisoryRecommendationListVersion(
        list_version_id="list-test",
        program_id="advp-test",
        binding_version_id="advb-test",
        review_run_id="review-test",
        trade_date=date(2026, 8, 17),
        previous_list_version_id=None,
        version_status="PUBLISHED",
        target_count=20,
        active_count=0,
        entered_count=0,
        held_count=0,
        exited_count=0,
        waiting_count=0,
        changed_count=1,
        turnover_rate=0.0,
        overlap_rate=None,
    )
    item = AdvisoryRecommendationListItem(
        list_item_id="item-test",
        list_version_id="list-test",
        program_id="advp-test",
        binding_version_id="advb-test",
        symbol="000001.SZ",
        item_state="WATCH",
        action="WATCH",
        reason_code="PENDING_TARGET_OPEN_ENTRY",
    )
    payload = {"active_episode_state_hash": canonical_json_sha256([])}
    return review, version, item, payload


def test_publication_sql_contract_commits_as_one_connection_transaction() -> None:
    conn = _Connection()
    review, version, item, payload = _publication_inputs()
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: conn)

    repository.commit_publication(
        forward_run_id="advfwd-test",
        expected_program_version=3,
        expected_binding_version_id="advb-test",
        review_run=review,
        list_version=version,
        items=[item],
        model_resolution={},
        publication_payload=payload,
    )

    assert conn.committed is True
    assert conn.rolled_back is False


def test_publication_item_failure_rolls_back_review_and_list_transaction() -> None:
    conn = _Connection(fail_on="INSERT INTO app.advisory_recommendation_list_item")
    review, version, item, payload = _publication_inputs()
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: conn)

    with pytest.raises(RuntimeError, match="injected transaction failure"):
        repository.commit_publication(
            forward_run_id="advfwd-test",
            expected_program_version=3,
            expected_binding_version_id="advb-test",
            review_run=review,
            list_version=version,
            items=[item],
            model_resolution={},
            publication_payload=payload,
        )

    assert conn.rolled_back is True
    assert conn.committed is False


def test_mark_failure_sql_contract_preserves_terminal_settlement() -> None:
    conn = _Connection()
    conn.cursor_instance.one = {"forward_run_id": "advfwd-test"}
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: conn)

    repository.mark_failure(
        forward_run_id="advfwd-test",
        stage="TARGET_OPEN_SETTLE",
        reason_code="ADVISORY_FORWARD_ACTIVE_EPISODE_STATE_CONFLICT",
        error={"message": "conflict"},
        waiting_data=False,
    )

    assert conn.committed is True


def test_model_observation_rejects_cross_forward_identity() -> None:
    observation = AdvisoryForwardModelObservationV1(
        forward_run_id="advfwd-test",
        program_id="advp-wrong",
        binding_version_id="advb-test",
        decision_as_of_trade_date=date(2026, 8, 14),
        target_trade_date=date(2026, 8, 17),
        status="UNAVAILABLE",
    )
    forward = {
        "program_id": "advp-test",
        "binding_version_id": "advb-test",
        "decision_as_of_trade_date": date(2026, 8, 14),
        "target_trade_date": date(2026, 8, 17),
        "publication_status": "PUBLISHED",
        "model_resolution_json": {"status": "UNAVAILABLE"},
    }

    with pytest.raises(InvalidStateTransitionError, match="identity differs"):
        _validate_observation_identity(observation, forward)
