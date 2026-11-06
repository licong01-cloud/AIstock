"""Rollback-only checks against the existing DEV DB, never a new database."""
from __future__ import annotations

from datetime import timedelta
import os
from types import SimpleNamespace

import psycopg2
import pytest

from backend.services.advisory_forward.models import AdvisoryForwardRunV1
from backend.services.advisory_forward.repository import AdvisoryForwardPGRepository, LATE_PUBLICATION_REASON
from backend.services.advisory_forward.service import AdvisoryForwardService, _build_publication_list, _active_episode_state_hash
from backend.services.advisory_program import (
    AdvisoryCandidate, AdvisoryProgramPGRepository, AdvisoryProgramService, AdvisoryReviewRun,
)
from backend.tests.e2e.test_advisory_binding_lifecycle_devdb import _Calendar, _conn_factory
from backend.tests.paper_trading_v2.fixtures_dev_db import _dev_dsn

pytest_plugins = ("backend.tests.paper_trading_v2.fixtures_dev_db",)
pytestmark = pytest.mark.skipif(os.environ.get("AISTOCK_DEV_DB_E2E") != "1", reason="explicit existing DEV DB gate")


@pytest.mark.parametrize("late", [True, False])
def test_dev_publication_clock_idempotence_and_rollback(dev_db_conn, late):
    def factory():
        return _conn_factory(dev_db_conn)
    programs = AdvisoryProgramPGRepository(conn_factory=factory)
    program = AdvisoryProgramService(repository=programs, calendar_provider=_Calendar()).create_program(
        program_name="test_int_cross_day_recovery", package_mode="single_package",
        package_ids=["test_int_cross_day_pkg"], target_count=5, status="ENABLED",
        review_schedule={"frequency": "daily_after_close"},
    )
    binding = programs.get_active_binding_version(program.program_id)
    repo = AdvisoryForwardPGRepository(conn_factory=factory)
    with dev_db_conn.cursor() as cur:
        cur.execute("SELECT (clock_timestamp() AT TIME ZONE 'Asia/Shanghai')::date")
        today = cur.fetchone()[0]
    target = today + timedelta(days=-1 if late else 1)
    run = AdvisoryForwardRunV1(program_id=program.program_id, program_version=program.version,
        binding_version_id=binding.binding_version_id, decision_as_of_trade_date=target-timedelta(days=1),
        target_trade_date=target)
    persisted = repo.begin_attempt(run)
    assert repo.pending_publications(before=target, program_ids=[program.program_id])[0]["forward_run_id"] == run.forward_run_id
    review_id = "test_int_review_" + run.forward_run_id
    version, items = _build_publication_list(program=program, binding_version_id=binding.binding_version_id,
        review_run_id=review_id, target_trade_date=target, decision_as_of_trade_date=run.decision_as_of_trade_date,
        selection_run_id="test_int_selection", candidates=[AdvisoryCandidate(symbol="000001.SZ", rank=1, score=0.5)],
        active_episodes=[], previous_list=None, previous_items=[])
    review = AdvisoryReviewRun(review_run_id=review_id, program_id=program.program_id,
        binding_version_id=binding.binding_version_id, trade_date=target, run_type="RUN", status="WAITING_DATA",
        data_source="test_int", selection_run_id="test_int_selection", selection_run_ids=["test_int_selection"],
        runtime_config_json={})
    kwargs = dict(forward_run_id=persisted["forward_run_id"], expected_program_version=program.version,
        expected_binding_version_id=binding.binding_version_id, review_run=review, list_version=version, items=items,
        model_resolution={"status": "CONFIGURED"}, publication_payload={"active_episode_state_hash": _active_episode_state_hash([])})
    result = repo.commit_publication(**kwargs)
    assert result["publication_status"] == "PUBLISHED"
    assert result["settlement_status"] == ("NOT_ENTERED" if late else "NOT_DUE")
    timing = result["run_payload_json"]["publication_timing"]
    assert timing["execution_eligible"] is not late
    assert result["published_at"].date() >= today
    again = repo.commit_publication(**kwargs)
    assert again["published_at"] == result["published_at"]
    assert repo.pending_publications(before=target, program_ids=[program.program_id]) == []
    if late:
        assert result["last_reason_code"] == LATE_PUBLICATION_REASON
        assert result["model_resolution_json"]["status"] == "UNAVAILABLE"
        assert not any(r["forward_run_id"] == run.forward_run_id for r in repo.pending_settlements(on_or_before=today))
        service = AdvisoryForwardService(repository=repo, program_service=SimpleNamespace(), model_service=SimpleNamespace())
        observation = service._model_observation(forward_run_id=run.forward_run_id, program=program,
            binding_version_id=binding.binding_version_id, decision_date=run.decision_as_of_trade_date, target_date=target,
            frozen_resolution=result["model_resolution_json"], selection_run_id="test_int_selection",
            review_run_id=review_id, list_version_id=version.list_version_id)
        repo.save_observation(observation)
        assert repo.get(run.forward_run_id)["forward_run"]["last_reason_code"] == LATE_PUBLICATION_REASON
    with dev_db_conn.cursor() as cur:
        cur.execute("SELECT summary_json FROM app.advisory_recommendation_list_version WHERE list_version_id=%s", (version.list_version_id,))
        assert cur.fetchone()[0]["publication_timing"] == timing
        cur.execute("SELECT count(*) FROM app.advisory_episode_return WHERE program_id=%s", (program.program_id,))
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM app.advisory_recommendation_list_item WHERE list_version_id=%s", (version.list_version_id,))
        assert cur.fetchone()[0] == 1
    dev_db_conn.rollback()
    with dev_db_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM app.advisory_forward_run WHERE forward_run_id=%s", (run.forward_run_id,))
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM app.advisory_program WHERE program_id=%s", (program.program_id,))
        assert cur.fetchone()[0] == 0


def test_dev_cycle_lock_serializes_distinct_sessions_and_releases(dev_db_conn):
    repo = AdvisoryForwardPGRepository(conn_factory=lambda: _conn_factory(dev_db_conn))
    second = psycopg2.connect(**_dev_dsn())
    other = AdvisoryForwardPGRepository(conn_factory=lambda: _conn_factory(second))
    try:
        with repo.cycle_lock() as first:
            assert first
            with other.cycle_lock() as blocked:
                assert not blocked
        with other.cycle_lock() as released:
            assert released
    finally:
        second.rollback()
        second.close()
