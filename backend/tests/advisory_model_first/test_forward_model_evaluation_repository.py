from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from backend.services.advisory_forward.models import (
    AdvisoryForwardModelEvaluationV1,
    AdvisoryForwardModelObservationOutcomeV1,
)
from backend.services.advisory_forward.repository import AdvisoryForwardPGRepository
from backend.services.trading_core.errors import InvalidStateTransitionError


class _Cursor:
    def __init__(self, state: dict) -> None:
        self.state = state
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params=()) -> None:
        normalized = " ".join(sql.split())
        self.state.setdefault("sql", []).append(normalized)
        self.row = None
        if normalized.startswith("SELECT * FROM app.advisory_forward_model_evaluation"):
            self.row = self.state.get("evaluation")
        elif normalized.startswith("INSERT INTO app.advisory_forward_model_evaluation"):
            self.state["evaluation_insert_count"] = self.state.get("evaluation_insert_count", 0) + 1
            self.state["evaluation"] = {"evaluation_id": params[0], "payload_sha256": params[-2]}
            self.row = self.state["evaluation"]
        elif normalized.startswith("SELECT payload_sha256 FROM app.advisory_forward_model_observation_outcome"):
            payload_hash = self.state.setdefault("outcomes", {}).get(params[0])
            self.row = {"payload_sha256": payload_hash} if payload_hash else None
        elif normalized.startswith("INSERT INTO app.advisory_forward_model_observation_outcome"):
            self.state["outcome_insert_count"] = self.state.get("outcome_insert_count", 0) + 1
            self.state.setdefault("outcomes", {})[params[2]] = params[-2]

    def fetchone(self):
        return self.row


class _Connection:
    def __init__(self, state: dict) -> None:
        self.state = state

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return _Cursor(self.state)


def _evaluation() -> AdvisoryForwardModelEvaluationV1:
    return AdvisoryForwardModelEvaluationV1(
        evaluation_id="adveval_test",
        program_id="advp_test",
        model_descriptor_sha256="d" * 64,
        bundle_id="e" * 64,
        shadow_policy_sha256="a" * 64,
        cost_policy_sha256="b" * 64,
        first_observation_id="advobs_1",
        last_due_observation_id="advobs_1",
        first_target_trade_date=date(2026, 1, 5),
        as_of_trade_date=date(2026, 1, 7),
        last_due_maturity_trade_date=date(2026, 1, 7),
        observation_count=3,
        due_observation_count=1,
        matured_outcome_count=1,
        observation_roster_sha256="1" * 64,
        selection_input_sha256="2" * 64,
        market_input_sha256="3" * 64,
        metrics_json={"coverage": 1.0},
        result_payload_json={"daily": []},
    )


def _outcome() -> AdvisoryForwardModelObservationOutcomeV1:
    return AdvisoryForwardModelObservationOutcomeV1(
        outcome_id="advout_test",
        observation_id="advobs_1",
        evaluation_id="adveval_test",
        program_id="advp_test",
        model_descriptor_sha256="d" * 64,
        bundle_id="e" * 64,
        target_trade_date=date(2026, 1, 5),
        maturity_trade_date=date(2026, 1, 7),
        status="MATURED",
        entered_episode_count=1,
        exited_episode_count=1,
        completed_episode_hit_rate=1.0,
        mean_net_return_bps=10.0,
        outcome_payload_json={"episodes": [{"status": "EXITED"}]},
    )


def test_forward_model_evaluation_repository_is_idempotent_and_rejects_payload_drift() -> None:
    state: dict = {}
    repository = AdvisoryForwardPGRepository(conn_factory=lambda: _Connection(state))
    evaluation = _evaluation()
    outcome = _outcome()

    first = repository.commit_model_evaluation(
        evaluation=evaluation,
        outcomes=[outcome],
        unresolved_observation_ids=[],
    )
    second = repository.commit_model_evaluation(
        evaluation=evaluation,
        outcomes=[outcome],
        unresolved_observation_ids=[],
    )

    assert first == second
    assert state["evaluation_insert_count"] == 1
    assert state["outcome_insert_count"] == 1
    assert any("pg_advisory_xact_lock" in sql for sql in state["sql"])
    assert any("evaluation_status='READY'" in sql for sql in state["sql"])

    changed = replace(evaluation, metrics_json={"coverage": 0.5})
    with pytest.raises(InvalidStateTransitionError):
        repository.commit_model_evaluation(
            evaluation=changed,
            outcomes=[outcome],
            unresolved_observation_ids=[],
        )
