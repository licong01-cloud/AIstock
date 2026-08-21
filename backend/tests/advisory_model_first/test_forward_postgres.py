from __future__ import annotations

from datetime import date
from types import SimpleNamespace
import pytest

from backend.services.advisory_forward.models import AdvisoryForwardModelObservationV1
from backend.services.advisory_forward.repository import (
    AdvisoryForwardPGRepository,
    _validate_observation_identity,
)
from backend.services.trading_core.errors import InvalidStateTransitionError
from backend.services.advisory_program import (
    AdvisoryProgram,
    AdvisoryEpisode,
    AdvisoryRecommendationListItem,
    AdvisoryRecommendationListVersion,
    AdvisoryReviewRun,
    AdvisoryReviewDecision,
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


class _SettlementCursor(_Cursor):
    def __init__(self, *, fail_on: str | None = None, terminal_hash: str | None = None) -> None:
        super().__init__(fail_on=fail_on)
        self.terminal_hash = terminal_hash

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
                "program_id": "advp-test",
                "publication_status": "PUBLISHED",
                "settlement_status": "SETTLED" if self.terminal_hash else "NOT_DUE",
                "settlement_payload_sha256": self.terminal_hash,
                "review_run_id": "review-test",
            }
        elif "SELECT version, status FROM app.advisory_program" in normalized:
            self.one = {"version": 3, "status": "ENABLED"}
        elif "SELECT DISTINCT ON (episode_id)" in normalized:
            self.rows = []
        elif "UPDATE app.advisory_forward_run" in normalized:
            self.one = {"forward_run_id": "advfwd-test", "settlement_status": "SETTLED"}


class _SettlementConnection(_Connection):
    def __init__(self, *, fail_on: str | None = None, terminal_hash: str | None = None) -> None:
        self.cursor_instance = _SettlementCursor(fail_on=fail_on, terminal_hash=terminal_hash)
        self.rolled_back = False
        self.committed = False


class _ObservationCursor(_Cursor):
    def __init__(self, *, existing: dict | None = None) -> None:
        super().__init__()
        self.sql: list[str] = []
        self.existing = existing

    def execute(self, sql, params=()):
        assert str(sql).count("%s") == len(params), str(sql)
        normalized = " ".join(str(sql).split())
        self.sql.append(normalized)
        self.one = None
        self.rows = []
        if "FROM app.advisory_forward_run" in normalized:
            self.one = {
                "program_id": "advp-test",
                "binding_version_id": "advb-test",
                "decision_as_of_trade_date": date(2026, 8, 14),
                "target_trade_date": date(2026, 8, 17),
                "publication_status": "PUBLISHED",
                "model_resolution_json": {"status": "UNAVAILABLE"},
            }
        elif "FROM app.advisory_forward_model_observation" in normalized:
            self.one = self.existing
        elif "INSERT INTO app.advisory_forward_model_observation" in normalized:
            self.one = {"forward_run_id": "advfwd-test", "status": "UNAVAILABLE"}
        elif "SET updated_at=NOW()" in normalized:
            self.one = dict(self.existing or {})
        elif "UPDATE app.advisory_forward_model_observation" in normalized:
            self.one = {"forward_run_id": "advfwd-test", "status": params[0]}


class _ObservationConnection(_Connection):
    def __init__(self, *, existing: dict | None = None) -> None:
        self.cursor_instance = _ObservationCursor(existing=existing)
        self.rolled_back = False
        self.committed = False


class _RetryableObservationCursor(_Cursor):
    def __init__(self) -> None:
        super().__init__()
        self.params = None

    def execute(self, sql, params=()):
        assert str(sql).count("%s") == len(params), str(sql)
        normalized = " ".join(str(sql).split())
        assert "JOIN app.advisory_forward_model_observation" in normalized
        assert "model_descriptor_sha256 IS NOT NULL" in normalized
        assert "INTERVAL '5 minutes'" in normalized
        assert "ORDER BY observation.updated_at ASC" in normalized
        self.params = params
        self.rows = [{"forward_run_id": "advfwd-retry", "publication_status": "PUBLISHED"}]


class _RetryableObservationConnection(_Connection):
    def __init__(self) -> None:
        self.cursor_instance = _RetryableObservationCursor()
        self.rolled_back = False
        self.committed = False


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


def _settlement_inputs():
    episode = AdvisoryEpisode(
        episode_id="episode-test",
        program_id="advp-test",
        program_version=3,
        symbol="000001.SZ",
        status="ACTIVE",
        signal_date=date(2026, 8, 14),
        effective_entry_date=date(2026, 8, 17),
        entry_price=10.0,
        entry_price_basis="next_open_executable",
        entry_rank=1,
    )
    decision = AdvisoryReviewDecision(
        program_id="advp-test",
        program_version=3,
        trade_date=date(2026, 8, 17),
        symbol=episode.symbol,
        action="ENTER",
        reason_code="ENTER_RANK",
        review_status="SUCCEEDED",
        episode_id=episode.episode_id,
        entry_price=episode.entry_price,
        binding_version_id="advb-test",
        review_run_id="review-test",
        list_version_id="list-test",
    )
    result = SimpleNamespace(
        active_pool=[episode],
        metrics={},
        review_status="SUCCEEDED",
    )
    payload = {
        "schema_version": "advisory_forward_settlement_v1",
        "program_id": "advp-test",
        "target_trade_date": "2026-08-17",
        "decisions": [{"symbol": episode.symbol, "action": decision.action, "entry_price": 10.0}],
    }
    return episode, decision, result, payload


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


def test_settlement_decision_failure_rolls_back_episode_and_decision_transaction() -> None:
    conn = _SettlementConnection(fail_on="INSERT INTO app.advisory_daily_review")
    _episode, decision, result, payload = _settlement_inputs()
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: conn)

    with pytest.raises(RuntimeError, match="injected transaction failure"):
        repository.commit_settlement(
            forward_run_id="advfwd-test",
            expected_active_episode_state_hash=canonical_json_sha256([]),
            expected_program_version=3,
            expected_program_status="ENABLED",
            result=result,
            decisions=[decision],
            program=_program(),
            settlement_payload=payload,
        )

    assert conn.rolled_back is True
    assert conn.committed is False


def test_terminal_settlement_rejects_changed_economic_payload_hash() -> None:
    _episode, decision, result, payload = _settlement_inputs()
    conn = _SettlementConnection(terminal_hash="a" * 64)
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: conn)

    with pytest.raises(InvalidStateTransitionError, match="payload conflicts"):
        repository.commit_settlement(
            forward_run_id="advfwd-test",
            expected_active_episode_state_hash=canonical_json_sha256([]),
            expected_program_version=3,
            expected_program_status="ENABLED",
            result=result,
            decisions=[decision],
            program=_program(),
            settlement_payload=payload,
        )

    assert conn.rolled_back is True
    assert conn.committed is False


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


def test_model_observation_serializes_missing_row_insert_on_forward_parent() -> None:
    conn = _ObservationConnection()
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: conn)
    observation = AdvisoryForwardModelObservationV1(
        observation_id="advobs-test",
        forward_run_id="advfwd-test",
        program_id="advp-test",
        binding_version_id="advb-test",
        decision_as_of_trade_date=date(2026, 8, 14),
        target_trade_date=date(2026, 8, 17),
        status="UNAVAILABLE",
    )

    repository.save_observation(observation)

    parent_reads = [
        sql for sql in conn.cursor_instance.sql if "FROM app.advisory_forward_run" in sql
    ]
    assert len(parent_reads) == 1
    assert parent_reads[0].endswith("FOR UPDATE")
    assert "FOR SHARE" not in parent_reads[0]
    assert conn.committed is True


def test_successful_model_observation_rejects_different_payload() -> None:
    existing = {
        "forward_run_id": "advfwd-test",
        "status": "EXPERIMENTAL_SHADOW",
        "payload_sha256": "a" * 64,
        "model_descriptor_sha256": "b" * 64,
    }
    conn = _ObservationConnection(existing=existing)
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: conn)
    observation = AdvisoryForwardModelObservationV1(
        observation_id="advobs-test",
        forward_run_id="advfwd-test",
        program_id="advp-test",
        binding_version_id="advb-test",
        decision_as_of_trade_date=date(2026, 8, 14),
        target_trade_date=date(2026, 8, 17),
        status="EXPERIMENTAL_SHADOW",
        model_descriptor_sha256="b" * 64,
        prediction_payload_json={"candidate_count": 1},
    )

    with pytest.raises(InvalidStateTransitionError, match="payload cannot change"):
        repository.save_observation(observation)

    assert not any(
        "UPDATE app.advisory_forward_model_observation" in sql
        for sql in conn.cursor_instance.sql
    )


def test_failed_model_observation_can_recover_under_same_descriptor() -> None:
    existing = {
        "forward_run_id": "advfwd-test",
        "status": "FAILED",
        "payload_sha256": "a" * 64,
        "model_descriptor_sha256": "b" * 64,
    }
    conn = _ObservationConnection(existing=existing)
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: conn)
    observation = AdvisoryForwardModelObservationV1(
        observation_id="advobs-test",
        forward_run_id="advfwd-test",
        program_id="advp-test",
        binding_version_id="advb-test",
        decision_as_of_trade_date=date(2026, 8, 14),
        target_trade_date=date(2026, 8, 17),
        status="EXPERIMENTAL_SHADOW",
        model_descriptor_sha256="b" * 64,
        bundle_id=None,
        prediction_payload_json={"candidate_count": 1},
    )

    saved = repository.save_observation(observation)

    assert saved["status"] == "EXPERIMENTAL_SHADOW"
    assert any(
        "UPDATE app.advisory_forward_model_observation" in sql
        for sql in conn.cursor_instance.sql
    )


def test_transient_unavailable_model_observation_can_recover_under_same_descriptor() -> None:
    existing = {
        "forward_run_id": "advfwd-test",
        "status": "UNAVAILABLE",
        "reason_code": "ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
        "payload_sha256": "a" * 64,
        "model_descriptor_sha256": "b" * 64,
    }
    conn = _ObservationConnection(existing=existing)
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: conn)
    observation = AdvisoryForwardModelObservationV1(
        observation_id="advobs-test",
        forward_run_id="advfwd-test",
        program_id="advp-test",
        binding_version_id="advb-test",
        decision_as_of_trade_date=date(2026, 8, 14),
        target_trade_date=date(2026, 8, 17),
        status="EXPERIMENTAL_SHADOW",
        model_descriptor_sha256="b" * 64,
        prediction_payload_json={"candidate_count": 1},
    )

    saved = repository.save_observation(observation)

    assert saved["status"] == "EXPERIMENTAL_SHADOW"
    assert any(
        "UPDATE app.advisory_forward_model_observation" in sql
        for sql in conn.cursor_instance.sql
    )


def test_permanent_unavailable_model_observation_remains_immutable() -> None:
    existing = {
        "forward_run_id": "advfwd-test",
        "status": "UNAVAILABLE",
        "reason_code": "ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
        "payload_sha256": "a" * 64,
        "model_descriptor_sha256": "b" * 64,
    }
    conn = _ObservationConnection(existing=existing)
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: conn)
    observation = AdvisoryForwardModelObservationV1(
        observation_id="advobs-test",
        forward_run_id="advfwd-test",
        program_id="advp-test",
        binding_version_id="advb-test",
        decision_as_of_trade_date=date(2026, 8, 14),
        target_trade_date=date(2026, 8, 17),
        status="EXPERIMENTAL_SHADOW",
        model_descriptor_sha256="b" * 64,
        prediction_payload_json={"candidate_count": 1},
    )

    with pytest.raises(InvalidStateTransitionError, match="payload cannot change"):
        repository.save_observation(observation)

    assert not any(
        "UPDATE app.advisory_forward_model_observation" in sql
        for sql in conn.cursor_instance.sql
    )


def test_same_retryable_payload_refreshes_attempt_time_for_fair_bounded_retry() -> None:
    observation = AdvisoryForwardModelObservationV1(
        observation_id="advobs-test",
        forward_run_id="advfwd-test",
        program_id="advp-test",
        binding_version_id="advb-test",
        decision_as_of_trade_date=date(2026, 8, 14),
        target_trade_date=date(2026, 8, 17),
        status="FAILED",
        reason_code="ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
        model_descriptor_sha256="b" * 64,
    )
    existing = {
        "forward_run_id": "advfwd-test",
        "status": "FAILED",
        "reason_code": observation.reason_code,
        "payload_sha256": observation.payload_sha256(),
        "model_descriptor_sha256": "b" * 64,
    }
    conn = _ObservationConnection(existing=existing)
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: conn)

    saved = repository.save_observation(observation)

    assert saved["status"] == "FAILED"
    assert any("SET updated_at=NOW()" in sql for sql in conn.cursor_instance.sql)


def test_retryable_model_observation_query_is_bounded_and_includes_legacy_reasons() -> None:
    conn = _RetryableObservationConnection()
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: conn)

    rows = repository.retryable_model_observations(limit=1)

    assert rows == [{"forward_run_id": "advfwd-retry", "publication_status": "PUBLISHED"}]
    assert conn.cursor_instance.params == (
        [
            "ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
            "ADVISORY_MODEL_REALTIME_DATA_UNAVAILABLE",
        ],
        1,
    )

    with pytest.raises(ValueError, match="limit must be positive"):
        repository.retryable_model_observations(limit=0)
