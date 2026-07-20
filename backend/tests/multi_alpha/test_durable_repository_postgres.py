from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import pytest
from psycopg2.extensions import parse_dsn
from psycopg2.extras import Json, RealDictCursor

from backend.services.multi_alpha.durable_backfill import MultiAlphaLegacyBackfill
from backend.services.multi_alpha.combine_backtest import parse_request
from backend.services.multi_alpha.durable_models import (
    DurableAttemptSpec,
    DurableChildSpec,
    DurableRunSpec,
    DurableTaskSpec,
    OwnershipToken,
    artifact_manifest_hash_for,
    durable_run_request_payload,
    make_attempt_id,
    make_child_id,
    request_hash_for,
    submission_intent_hash_for,
)
from backend.services.multi_alpha.durable_repository import (
    MultiAlphaDurableRepository,
    MultiAlphaDurableRepositoryError,
)
from backend.services.multi_alpha.durable_plan import DeterministicChildPlanner
from backend.services.multi_alpha.durable_submission import DurableCombineSubmissionService


DSN = os.getenv("AISTOCK_MULTI_ALPHA_TEST_PG_DSN", "").strip()
pytestmark = pytest.mark.skipif(
    not DSN,
    reason="set AISTOCK_MULTI_ALPHA_TEST_PG_DSN to a disposable PostgreSQL database",
)
REPO_ROOT = Path(__file__).resolve().parents[3]


@contextmanager
def _connection_provider() -> Iterator[Any]:
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@pytest.fixture(scope="module", autouse=True)
def disposable_schema() -> Iterator[None]:
    if not DSN:
        yield
        return
    dbname = str(parse_dsn(DSN).get("dbname") or "")
    if not dbname.startswith("aistock_test"):
        pytest.fail(
            "AISTOCK_MULTI_ALPHA_TEST_PG_DSN must target a disposable database whose name starts with aistock_test"
        )
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS strategy_pkg CASCADE")
            cur.execute(
                (REPO_ROOT / "backend/migrations/multi_alpha_combine_backtest_result_20260620.sql").read_text(
                    encoding="utf-8"
                )
            )
            cur.execute(
                """
                INSERT INTO strategy_pkg.multi_alpha_combine_backtest_run
                    (id, roster_hash, roster_json, oos_start, oos_end, normalize_method,
                     walk_forward_json, backtest_config_json, baseline_leg_id, status, reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'succeeded', %s)
                """,
                (
                    "macb_legacy_pg",
                    "legacy_roster",
                    Json([{"leg_id": "L1"}, {"leg_id": "L2"}]),
                    "2026-01-01",
                    "2026-06-29",
                    "rank",
                    Json({"enabled": True, "window": 60, "min_periods": 20, "expanding": False}),
                    Json({"topk": 25, "initial_cash": 10_000_000}),
                    "L1",
                    Json({"logical_status": "succeeded", "source": "legacy"}),
                ),
            )
            cur.execute(
                """
                INSERT INTO strategy_pkg.multi_alpha_combine_backtest_scheme_result
                    (run_id, weighting_scheme, weights_json, cagr, sharpe, skipped)
                VALUES ('macb_legacy_pg', 'equal', '{"L1":0.5,"L2":0.5}'::jsonb, 0.42, 1.5, FALSE)
                """
            )
            cur.execute(
                """
                INSERT INTO strategy_pkg.multi_alpha_combine_backtest_scheme_result
                    (run_id, weighting_scheme, weights_json, skipped, skipped_reason)
                VALUES
                    (
                        'macb_legacy_pg',
                        'risk_parity',
                        '{"L1":0.5,"L2":0.5}'::jsonb,
                        TRUE,
                        '{"reason_code":"scheme_not_computable","detail":"zero covariance"}'
                    ),
                    (
                        'macb_legacy_pg',
                        'ic_weighted',
                        '{"L1":0.5,"L2":0.5}'::jsonb,
                        TRUE,
                        '{"reason_code":"pred_backtest_failed","detail":"artifact unavailable"}'
                    )
                """
            )
            cur.execute(
                """
                INSERT INTO strategy_pkg.multi_alpha_combine_backtest_loo
                    (run_id, weighting_scheme, dropped_leg_id, marginal_sharpe)
                VALUES ('macb_legacy_pg', 'equal', 'L2', 0.11)
                """
            )
            cur.execute(
                (REPO_ROOT / "backend/migrations/multi_alpha_durable_orchestration_20260718.preflight.sql").read_text(
                    encoding="utf-8"
                )
            )
            migration = (
                REPO_ROOT / "backend/migrations/multi_alpha_durable_orchestration_20260718.sql"
            ).read_text(encoding="utf-8")
            cur.execute(migration)
            first_digest = _schema_digest(cur)
            cur.execute(migration)
            second_digest = _schema_digest(cur)
            assert first_digest == second_digest
            cur.execute(
                (REPO_ROOT / "backend/migrations/multi_alpha_durable_orchestration_20260718.preflight.sql").read_text(
                    encoding="utf-8"
                )
            )
            assert dict(zip([item.name for item in cur.description], cur.fetchone()))["preflight_status"] == "ready"
        yield
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS strategy_pkg CASCADE")
        conn.close()


def test_schema_has_required_comments_constraints_and_indexes() -> None:
    repository = MultiAlphaDurableRepository(connection_provider=_connection_provider)
    assert repository.preflight_schema(raise_on_error=True).ready is True

    with _connection_provider() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT relname, obj_description(oid, 'pg_class') AS comment
                FROM pg_class
                WHERE oid IN (
                    'strategy_pkg.multi_alpha_combine_task'::regclass,
                    'strategy_pkg.multi_alpha_combine_backtest_child'::regclass,
                    'strategy_pkg.multi_alpha_combine_backtest_child_attempt'::regclass,
                    'strategy_pkg.multi_alpha_combine_backtest_event'::regclass
                )
                """
            )
            comments = {row["relname"]: row["comment"] for row in cur.fetchall()}
            assert len(comments) == 4
            assert all(comments.values())

    with _connection_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_event
                DROP CONSTRAINT ck_macb_event_type
                """
            )
    try:
        health = repository.preflight_schema()
        assert health.ready is False
        assert "ck_macb_event_type" in health.missing_constraints
        with pytest.raises(MultiAlphaDurableRepositoryError) as caught:
            repository.preflight_schema(raise_on_error=True)
        assert caught.value.reason_code == "multi_alpha_schema_unavailable"
    finally:
        with _connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_event
                    ADD CONSTRAINT ck_macb_event_type CHECK (
                        event_type IN (
                            'created', 'claimed', 'submitted', 'status', 'log', 'reconciled',
                            'control', 'result', 'error', 'terminal'
                        )
                    )
                    """
                )
    assert repository.preflight_schema(raise_on_error=True).ready is True

    with _connection_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE strategy_pkg.multi_alpha_combine_task ALTER COLUMN description TYPE VARCHAR(100)"
            )
    try:
        health = repository.preflight_schema()
        assert health.ready is False
        assert health.type_mismatches["multi_alpha_combine_task"]["description"] == {
            "expected": "text",
            "actual": "character varying",
        }
    finally:
        with _connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "ALTER TABLE strategy_pkg.multi_alpha_combine_task ALTER COLUMN description TYPE TEXT"
                )

    with _connection_provider() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP INDEX strategy_pkg.idx_mact_roster_hash")
    try:
        health = repository.preflight_schema()
        assert health.ready is False
        assert "idx_mact_roster_hash" in health.missing_indexes
    finally:
        with _connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE INDEX idx_mact_roster_hash
                    ON strategy_pkg.multi_alpha_combine_task(roster_hash, created_at DESC)
                    """
                )

    with _connection_provider() as conn:
        with conn.cursor() as cur:
            cur.execute("COMMENT ON TABLE strategy_pkg.multi_alpha_combine_task IS NULL")
    try:
        health = repository.preflight_schema()
        assert health.ready is False
        assert "multi_alpha_combine_task" in health.missing_table_comments
    finally:
        with _connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    COMMENT ON TABLE strategy_pkg.multi_alpha_combine_task IS
                    'First-class QE-only multi-alpha combine research task. Status and metrics are derived from runs; no approval semantics.'
                    """
                )
    assert repository.preflight_schema(raise_on_error=True).ready is True


def test_historical_backfill_is_idempotent_and_preserves_metrics_status_reason() -> None:
    with _connection_provider() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT run.status, run.reason, run.created_at, result.cagr, result.sharpe
                FROM strategy_pkg.multi_alpha_combine_backtest_run AS run
                JOIN strategy_pkg.multi_alpha_combine_backtest_scheme_result AS result
                  ON result.run_id = run.id
                WHERE run.id = 'macb_legacy_pg' AND result.weighting_scheme = 'equal'
                """
            )
            before = dict(cur.fetchone())

    backfill = MultiAlphaLegacyBackfill(connection_provider=_connection_provider)
    assert backfill.dry_run()["run_assignment_count"] == 1
    first = backfill.execute()
    repository = MultiAlphaDurableRepository(connection_provider=_connection_provider)
    modern_task = DurableTaskSpec(
        task_id="mact_pg_modern_backfill_guard",
        task_name="Modern first-class run excluded from legacy backfill",
        roster_hash="modern_roster",
        roster=[{"leg_id": "L1"}],
        default_request={"topk": 25},
        source_kind="api",
        created_by="pytest",
    )
    repository.create_task(modern_task)
    modern_request = durable_run_request_payload(
        roster_hash=modern_task.roster_hash,
        roster=modern_task.roster,
        oos_start="2026-01-01",
        oos_end="2026-06-29",
        normalize_method="rank",
        walk_forward={"enabled": True},
        backtest_config={"topk": 25},
    )
    repository.create_run(
        DurableRunSpec(
            run_id="macb_pg_modern_backfill_guard",
            task_id=modern_task.task_id,
            request_hash=request_hash_for(modern_request),
            roster_hash=modern_task.roster_hash,
            roster=modern_task.roster,
            oos_start="2026-01-01",
            oos_end="2026-06-29",
            normalize_method="rank",
            walk_forward={"enabled": True},
            backtest_config={"topk": 25},
        )
    )
    assert backfill.dry_run()["run_assignment_count"] == 1
    second = backfill.execute()
    assert first["readback"]["ready"] is True
    assert second["readback"]["ready"] is True
    assert backfill.readback()["readback"]["legacy_attempt_count"] == 0

    with _connection_provider() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT run.status, run.reason, run.created_at, result.cagr, result.sharpe
                FROM strategy_pkg.multi_alpha_combine_backtest_run AS run
                JOIN strategy_pkg.multi_alpha_combine_backtest_scheme_result AS result
                  ON result.run_id = run.id
                WHERE run.id = 'macb_legacy_pg' AND result.weighting_scheme = 'equal'
                """
            )
            after = dict(cur.fetchone())
            cur.execute(
                """
                SELECT child_key, status
                FROM strategy_pkg.multi_alpha_combine_backtest_child
                WHERE run_id = 'macb_legacy_pg' AND source_kind = 'legacy_result_backfill'
                ORDER BY child_key
                """
            )
            legacy_children = {row["child_key"]: row["status"] for row in cur.fetchall()}
            cur.execute(
                """
                SELECT COUNT(*) AS child_count
                FROM strategy_pkg.multi_alpha_combine_backtest_child
                WHERE run_id = 'macb_pg_modern_backfill_guard'
                  AND source_kind = 'legacy_result_backfill'
                """
            )
            modern_legacy_child_count = int(cur.fetchone()["child_count"])
    assert after == before
    assert legacy_children == {
        "loo:equal:drop:L2": "succeeded",
        "scheme:equal": "succeeded",
        "scheme:ic_weighted": "failed",
        "scheme:risk_parity": "not_computable",
    }
    assert modern_legacy_child_count == 0
    with _connection_provider() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM strategy_pkg.multi_alpha_combine_backtest_run WHERE id = %s", (
                "macb_pg_modern_backfill_guard",
            ))
            cur.execute("DELETE FROM strategy_pkg.multi_alpha_combine_task WHERE task_id = %s", (
                "mact_pg_modern_backfill_guard",
            ))


def test_eight_workers_claim_once_event_rollback_and_stale_fencing() -> None:
    repository = MultiAlphaDurableRepository(connection_provider=_connection_provider)
    task = DurableTaskSpec(
        task_id="mact_pg_concurrency",
        task_name="PostgreSQL concurrency contract",
        roster_hash="pg_roster",
        roster=[{"leg_id": "L1"}],
        default_request={"topk": 25},
        source_kind="api",
        created_by="pytest",
    )
    repository.create_task(task)
    request = durable_run_request_payload(
        roster_hash=task.roster_hash,
        roster=task.roster,
        oos_start="2026-01-01",
        oos_end="2026-06-29",
        normalize_method="rank",
        walk_forward={"enabled": True},
        backtest_config={"topk": 25},
    )
    repository.create_run(
        DurableRunSpec(
            run_id="macb_pg_concurrency",
            task_id=task.task_id,
            request_hash=request_hash_for(request),
            roster_hash=task.roster_hash,
            roster=task.roster,
            oos_start="2026-01-01",
            oos_end="2026-06-29",
            normalize_method="rank",
            walk_forward={"enabled": True},
            backtest_config={"topk": 25},
        )
    )

    def claim(worker_no: int) -> dict[str, Any] | None:
        worker_repository = MultiAlphaDurableRepository(connection_provider=_connection_provider)
        return worker_repository.claim_next_run(
            owner_id=f"worker_{worker_no}", lease_seconds=30, statuses=("queued",)
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        claims = list(executor.map(claim, range(8)))
    winners = [row for row in claims if row is not None]
    assert len(winners) == 1
    winner = winners[0]
    old_token = OwnershipToken(
        owner_id=str(winner["owner_id"]),
        fencing_token=int(winner["fencing_token"]),
        row_version=int(winner["row_version"]),
    )

    with _connection_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE OR REPLACE FUNCTION strategy_pkg.reject_test_event()
                RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    RAISE EXCEPTION 'forced event failure';
                END
                $$
                """
            )
            cur.execute(
                """
                CREATE TRIGGER reject_test_event
                BEFORE INSERT ON strategy_pkg.multi_alpha_combine_backtest_event
                FOR EACH ROW EXECUTE FUNCTION strategy_pkg.reject_test_event()
                """
            )
    try:
        with pytest.raises(psycopg2.Error, match="forced event failure"):
            repository.transition_run_with_event(
                "macb_pg_concurrency",
                token=old_token,
                expected_statuses=("queued",),
                next_status="preparing",
                phase="prepare",
            )
    finally:
        with _connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DROP TRIGGER IF EXISTS reject_test_event ON strategy_pkg.multi_alpha_combine_backtest_event"
                )
                cur.execute("DROP FUNCTION IF EXISTS strategy_pkg.reject_test_event()")

    unchanged = repository.get_run("macb_pg_concurrency")
    assert unchanged is not None
    assert unchanged["status"] == "queued"
    assert int(unchanged["row_version"]) == old_token.row_version

    with _connection_provider() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE strategy_pkg.multi_alpha_combine_backtest_run
                SET lease_expires_at = NOW() - INTERVAL '1 second'
                WHERE id = 'macb_pg_concurrency'
                """
            )
    with pytest.raises(MultiAlphaDurableRepositoryError) as expired_heartbeat:
        repository.heartbeat_run("macb_pg_concurrency", token=old_token, lease_seconds=30)
    assert expired_heartbeat.value.reason_code == "multi_alpha_lease_expired"
    with pytest.raises(MultiAlphaDurableRepositoryError) as expired_transition:
        repository.transition_run_with_event(
            "macb_pg_concurrency",
            token=old_token,
            expected_statuses=("queued",),
            next_status="preparing",
            phase="prepare",
        )
    assert expired_transition.value.reason_code == "multi_alpha_lease_expired"
    new_owner = repository.claim_next_run(owner_id="worker_new", lease_seconds=30, statuses=("queued",))
    assert new_owner is not None
    with pytest.raises(MultiAlphaDurableRepositoryError) as caught:
        repository.transition_run_with_event(
            "macb_pg_concurrency",
            token=old_token,
            expected_statuses=("queued",),
            next_status="preparing",
            phase="prepare",
        )
    assert caught.value.reason_code == "multi_alpha_stale_fencing_token"


def test_child_attempt_repository_persists_remote_identity_and_terminal_result() -> None:
    repository = MultiAlphaDurableRepository(connection_provider=_connection_provider)
    task = DurableTaskSpec(
        task_id="mact_pg_attempt",
        task_name="PostgreSQL child attempt contract",
        roster_hash="attempt_roster",
        roster=[{"leg_id": "L1"}],
        default_request={"topk": 25},
        source_kind="api",
        created_by="pytest",
    )
    repository.create_task(task)
    request = durable_run_request_payload(
        roster_hash=task.roster_hash,
        roster=task.roster,
        oos_start="2026-01-01",
        oos_end="2026-06-29",
        normalize_method="rank",
        walk_forward={"enabled": True},
        backtest_config={"topk": 25},
    )
    repository.create_run(
        DurableRunSpec(
            run_id="macb_pg_attempt",
            task_id=task.task_id,
            request_hash=request_hash_for(request),
            roster_hash=task.roster_hash,
            roster=task.roster,
            oos_start="2026-01-01",
            oos_end="2026-06-29",
            normalize_method="rank",
            walk_forward={"enabled": True},
            backtest_config={"topk": 25},
        )
    )
    claimed_run = repository.claim_next_run(owner_id="attempt_run_worker", lease_seconds=30, statuses=("queued",))
    assert claimed_run is not None and claimed_run["id"] == "macb_pg_attempt"
    run_token = OwnershipToken(
        owner_id="attempt_run_worker",
        fencing_token=int(claimed_run["fencing_token"]),
        row_version=int(claimed_run["row_version"]),
    )
    preparing_run = repository.transition_run_with_event(
        "macb_pg_attempt",
        token=run_token,
        expected_statuses=("queued",),
        next_status="preparing",
        phase="prepare",
    )
    run_token = OwnershipToken(
        owner_id="attempt_run_worker",
        fencing_token=int(preparing_run["fencing_token"]),
        row_version=int(preparing_run["row_version"]),
    )
    running_run = repository.transition_run_with_event(
        "macb_pg_attempt",
        token=run_token,
        expected_statuses=("preparing",),
        next_status="running",
        phase="execute",
    )
    running_run_token = OwnershipToken(
        owner_id="attempt_run_worker",
        fencing_token=int(running_run["fencing_token"]),
        row_version=int(running_run["row_version"]),
    )
    child_key = "scheme:equal"
    child_id = make_child_id("macb_pg_attempt", child_key)
    manifest = {"run_id": "macb_pg_attempt", "child_key": child_key, "topk": 25}
    repository.create_child(
        DurableChildSpec(
            child_id=child_id,
            run_id="macb_pg_attempt",
            child_key=child_key,
            child_kind="scheme",
            weighting_scheme="equal",
            ordinal=0,
            input_manifest=manifest,
            input_manifest_hash=artifact_manifest_hash_for(manifest),
        )
    )
    attempt_id = make_attempt_id(child_id, 1)
    submission_hash = submission_intent_hash_for(
        child_id=child_id,
        attempt_no=1,
        retry_mode="initial",
        retry_of_attempt_id=None,
        node_id="wsl2-5080",
        qe_task_id="qe_pg_attempt",
        qe_loop_id="Loop1",
    )
    repository.create_attempt(
        DurableAttemptSpec(
            attempt_id=attempt_id,
            child_id=child_id,
            attempt_no=1,
            retry_mode="initial",
            node_id="wsl2-5080",
            qe_task_id="qe_pg_attempt",
            qe_loop_id="Loop1",
            submission_intent_hash=submission_hash,
        )
    )
    claimed = repository.claim_next_attempt(
        owner_id="attempt_worker", lease_seconds=30, claim_kind="dispatch", node_id="wsl2-5080"
    )
    assert claimed is not None
    token = OwnershipToken(
        owner_id="attempt_worker",
        fencing_token=int(claimed["fencing_token"]),
        row_version=int(claimed["row_version"]),
    )
    submitting = repository.transition_attempt_with_event(
        attempt_id,
        token=token,
        expected_statuses=("queued",),
        next_status="submitting",
        phase="remote_submit",
        remote_status="accepted",
    )
    running_token = OwnershipToken(
        owner_id="attempt_worker",
        fencing_token=int(submitting["fencing_token"]),
        row_version=int(submitting["row_version"]),
    )
    running = repository.transition_attempt_with_event(
        attempt_id,
        token=running_token,
        expected_statuses=("submitting",),
        next_status="running",
        phase="remote_running",
        remote_status="running",
    )
    result_token = OwnershipToken(
        owner_id="attempt_worker",
        fencing_token=int(running["fencing_token"]),
        row_version=int(running["row_version"]),
    )
    succeeded = repository.transition_attempt_with_event(
        attempt_id,
        token=result_token,
        expected_statuses=("running",),
        next_status="succeeded",
        phase="result_persisted",
        remote_status="completed",
        result_manifest={"metrics": {"sharpe": 1.25}, "artifact_hash": "e" * 64},
    )
    assert succeeded["owner_id"] is None
    child_running = repository.transition_child_with_event(
        child_id,
        expected_statuses=("pending",),
        next_status="queued",
        phase="attempt_created",
    )
    assert child_running["status"] == "queued"
    child_running = repository.transition_child_with_event(
        child_id,
        expected_statuses=("queued",),
        next_status="running",
        phase="remote_running",
    )
    assert child_running["status"] == "running"
    child_done = repository.transition_child_with_event(
        child_id,
        expected_statuses=("running",),
        next_status="succeeded",
        phase="result_persisted",
        selected_attempt_id=attempt_id,
    )
    assert child_done["selected_attempt_id"] == attempt_id
    assert repository.list_attempts(child_id)[0]["qe_task_id"] == "qe_pg_attempt"
    assert repository.get_child(child_id)["status"] == "succeeded"  # type: ignore[index]
    assert repository.get_attempt(attempt_id)["run_id"] == "macb_pg_attempt"  # type: ignore[index]
    assert repository.list_runs(task_id=task.task_id)[0]["id"] == "macb_pg_attempt"
    listed_task = next(row for row in repository.list_tasks(source_kind="api") if row["task_id"] == task.task_id)
    assert int(listed_task["run_count"]) == 1
    assert int(listed_task["run_status_counts"]["running"]) == 1

    repository.transition_run_with_event(
        "macb_pg_attempt",
        token=running_run_token,
        expected_statuses=("running",),
        next_status="cancel_requested",
        phase="cancel_requested",
        reason_code="user_requested_cancel",
    )
    queued_child_key = "scheme:risk_parity"
    queued_child_id = make_child_id("macb_pg_attempt", queued_child_key)
    queued_manifest = {"run_id": "macb_pg_attempt", "child_key": queued_child_key, "topk": 25}
    repository.create_child(
        DurableChildSpec(
            child_id=queued_child_id,
            run_id="macb_pg_attempt",
            child_key=queued_child_key,
            child_kind="scheme",
            weighting_scheme="risk_parity",
            ordinal=1,
            input_manifest=queued_manifest,
            input_manifest_hash=artifact_manifest_hash_for(queued_manifest),
        )
    )
    queued_attempt_id = make_attempt_id(queued_child_id, 1)
    repository.create_attempt(
        DurableAttemptSpec(
            attempt_id=queued_attempt_id,
            child_id=queued_child_id,
            attempt_no=1,
            retry_mode="initial",
            node_id="wsl2-5080",
        )
    )
    assert repository.claim_next_attempt(
        owner_id="cancel_worker",
        lease_seconds=30,
        claim_kind="cancel",
        node_id="wsl2-5080",
    ) is None
    queued_attempt = repository.get_attempt(queued_attempt_id)
    assert queued_attempt is not None
    assert queued_attempt["owner_id"] is None


def test_durable_submission_and_planner_are_idempotent_in_postgres() -> None:
    repository = MultiAlphaDurableRepository(connection_provider=_connection_provider)
    service = DurableCombineSubmissionService(
        repository=repository,
        runtime_preflight=lambda **_kwargs: None,
        execution_schema_preflight=lambda: None,
        orchestrator_readiness_preflight=lambda: {"ready": True},
        clock=lambda: datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc),
    )
    first_payload = {
        "roster": [
            {"leg_id": "pg_leg_a", "seed_run_ids": ["qe_pg_a_L1"], "metadata": {}},
            {"leg_id": "pg_leg_b", "seed_run_ids": ["qe_pg_b_L1"], "metadata": {}},
            {"leg_id": "pg_leg_c", "seed_run_ids": ["qe_pg_c_L1"], "metadata": {}},
        ],
        "oos_start": "2024-07-01",
        "oos_end": "2026-06-29",
        "weighting_schemes": ["equal", "ic_weighted"],
        "normalize_method": "rank",
        "walk_forward": {"enabled": True, "window": 60, "min_periods": 20, "expanding": False},
        "backtest_config": {
            "node_id": "wsl2-5080",
            "node_parallelism": {"wsl2-5080": 2},
            "topk": 25,
            "initial_cash": 10_000_000,
        },
        "baseline_leg_id": "pg_leg_a",
        "topk": 25,
        "run_async": True,
        "scheme_timeout_seconds": 120,
        "run_timeout_seconds": 600,
    }
    first = service.submit(first_payload)
    second_payload = dict(first_payload)
    second_payload["oos_start"] = "2025-01-01"
    second_payload["topk"] = 50
    second_payload["baseline_leg_id"] = "pg_leg_b"
    second_payload["backtest_config"] = {
        **dict(first_payload["backtest_config"]),
        "topk": 50,
        "initial_cash": 100_000_000,
    }
    second = service.submit(second_payload)

    assert first["task_id"] == second["task_id"]
    assert first["run_id"] != second["run_id"]
    task = repository.get_task(first["task_id"])
    assert task is not None
    assert task["default_request_json"]["topk"] == 25
    assert task["default_request_json"]["backtest_config"]["initial_cash"] == 10_000_000
    assert len(repository.list_runs(task_id=first["task_id"])) == 2

    run = repository.get_run(first["run_id"])
    assert run is not None
    request = parse_request(first_payload)
    run_spec = DurableRunSpec(
        run_id=run["id"],
        task_id=run["task_id"],
        request_hash=run["request_hash"],
        roster_hash=run["roster_hash"],
        roster=run["roster_json"],
        oos_start=run["oos_start"],
        oos_end=run["oos_end"],
        normalize_method=run["normalize_method"],
        walk_forward=run["walk_forward_json"],
        backtest_config=run["backtest_config_json"],
        baseline_leg_id=run["baseline_leg_id"],
        retry_of_run_id=run["retry_of_run_id"],
        node_parallelism=run["node_parallelism_json"],
    )
    planner = DeterministicChildPlanner(repository)
    first_plan = planner.plan(run_spec=run_spec, request=request)
    second_plan = planner.plan(run_spec=run_spec, request=request)

    assert first_plan == second_plan
    assert len(first_plan.children) == 9
    assert len(first_plan.initial_attempts) == 0
    assert len(repository.list_children(first["run_id"])) == 9
    initial_attempts = [
        planner.ensure_initial_attempt(
            child_id=str(child["child_id"]),
            node_id="wsl2-5080",
        )
        for child in first_plan.children
    ]
    assert len({row["attempt_id"] for row in initial_attempts}) == 9


def _schema_digest(cur: Any) -> str:
    cur.execute(
        """
        SELECT table_name, column_name, data_type, is_nullable, COALESCE(column_default, '') AS column_default
        FROM information_schema.columns
        WHERE table_schema = 'strategy_pkg'
          AND table_name LIKE 'multi_alpha_combine%'
        ORDER BY table_name, ordinal_position
        """
    )
    columns = cur.fetchall()
    cur.execute(
        """
        SELECT cls.relname, con.conname, pg_get_constraintdef(con.oid, TRUE)
        FROM pg_constraint AS con
        JOIN pg_class AS cls ON cls.oid = con.conrelid
        JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
        WHERE ns.nspname = 'strategy_pkg' AND cls.relname LIKE 'multi_alpha_combine%'
        ORDER BY cls.relname, con.conname
        """
    )
    constraints = cur.fetchall()
    cur.execute(
        """
        SELECT tablename, indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'strategy_pkg' AND tablename LIKE 'multi_alpha_combine%'
        ORDER BY tablename, indexname
        """
    )
    indexes = cur.fetchall()
    return json.dumps(
        {"columns": columns, "constraints": constraints, "indexes": indexes},
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
