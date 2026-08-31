from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import psycopg2
import pytest

from backend.services.quantevolver import long_trend_resource_session_repair as repair


class _Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []
        self.row = None
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        normalized = " ".join(sql.split())
        self.connection.sql.append(normalized)
        self.rows = []
        self.row = None
        self.rowcount = 0
        if normalized.startswith("SELECT s.session_id, s.source_run_key"):
            self.rows = [dict(item) for item in self.connection.preflight_rows]
        elif (
            normalized.startswith("UPDATE qe_archive.run_resource_session s SET status = %s")
            and "terminal_reason_code = NULL" in normalized
        ):
            (
                after_status,
                after_phase,
                session_id,
                source_run_key,
                before_status,
                before_phase,
                last_sequence,
                before_reason,
                before_completed_at,
                _evaluation_id,
                _request_sha,
                _max_sequence,
            ) = params
            state = self.connection.states[session_id]
            if (
                state["source_run_key"] == source_run_key
                and state["status"] == before_status
                and state["current_phase"] == before_phase
                and state["last_sequence_no"] == last_sequence
                and state["terminal_reason_code"] == before_reason
                and state["completed_at"] == before_completed_at
                and not state["has_later_phase"]
            ):
                state.update(
                    {
                        "status": after_status,
                        "current_phase": after_phase,
                        "terminal_reason_code": None,
                        "completed_at": None,
                    }
                )
                self.rowcount = 1
        elif normalized.startswith("SELECT s.status, s.current_phase"):
            _max_sequence, _evaluation_id, _request_sha, session_id, source_run_key = params
            state = self.connection.states.get(session_id)
            self.row = dict(state) if state and state["source_run_key"] == source_run_key else None
            if self.row is not None:
                self.row["control_matches"] = True
        elif normalized.startswith("SELECT session_id, source_run_key, status"):
            selected = set(params[0])
            self.rows = [
                {"session_id": session_id, **dict(state)}
                for session_id, state in self.connection.states.items()
                if session_id in selected
            ]
        elif (
            normalized.startswith("UPDATE qe_archive.run_resource_session s SET status = %s")
            and "terminal_reason_code = %s" in normalized
        ):
            (
                before_status,
                before_phase,
                before_reason,
                before_completed_at,
                session_id,
                source_run_key,
                after_status,
                after_phase,
                last_sequence,
                _evaluation_id,
                _request_sha,
                _max_sequence,
            ) = params
            state = self.connection.states[session_id]
            if (
                state["source_run_key"] == source_run_key
                and state["status"] == after_status
                and state["current_phase"] == after_phase
                and state["last_sequence_no"] == last_sequence
                and state["terminal_reason_code"] is None
                and state["completed_at"] is None
                and not state["has_later_phase"]
            ):
                state.update(
                    {
                        "status": before_status,
                        "current_phase": before_phase,
                        "terminal_reason_code": before_reason,
                        "completed_at": before_completed_at,
                    }
                )
                self.rowcount = 1
        else:
            raise AssertionError(f"unhandled SQL: {normalized}")

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.row


class _Connection:
    def __init__(self, preflight_rows):
        self.preflight_rows = preflight_rows
        self.states = {
            row["session_id"]: {
                "source_run_key": row["source_run_key"],
                "status": row["status"],
                "current_phase": row["current_phase"],
                "last_sequence_no": row["last_sequence_no"],
                "terminal_reason_code": row["terminal_reason_code"],
                "completed_at": row["completed_at"],
                "has_later_phase": False,
            }
            for row in preflight_rows
        }
        self.sql = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, **_kwargs):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _candidate(sequence: int) -> dict:
    suffix = str(sequence + 1)
    evaluation_id = "qelt_" + suffix * 64
    return {
        "session_id": "qers_" + suffix * 32,
        "source_run_key": f"qelt:{evaluation_id}",
        "task_id": f"task-{suffix}",
        "loop_id": f"Loop{sequence + 1}",
        "loop_index": sequence + 1,
        "node_id": "node-1",
        "status": "completed",
        "current_phase": "completed",
        "last_sequence_no": sequence,
        "terminal_reason_code": repair.RECONCILER_REASON,
        "completed_at": "2026-07-23T12:00:00Z",
        "evaluation_id": evaluation_id,
        "request_sha": "a" * 64,
        "resource_session_id": "qers_" + suffix * 32,
        "parent_task_id": f"task-{suffix}",
        "parent_loop_index": sequence + 1,
        "control_node_id": "node-1",
        "phase_row_count": sequence,
        "max_phase_sequence": sequence or None,
        "phase_event_hashes": {},
    }


def _write_workspace_evidence(workspace: Path, row: dict) -> Path:
    job_dir = (
        workspace
        / row["task_id"]
        / row["loop_id"]
        / "long_trend_evaluations"
        / row["evaluation_id"]
    )
    outbox_dir = job_dir / "outbox"
    outbox_dir.mkdir(parents=True)
    (job_dir / "job.json").write_text(
        json.dumps(
            {
                "evaluation_id": row["evaluation_id"],
                "task_id": row["task_id"],
                "loop_id": row["loop_id"],
            }
        ),
        encoding="utf-8",
    )
    (job_dir / "request.json").write_text(
        json.dumps(
            {
                "node_id": row["node_id"],
                "resource_session": {
                    "session_id": row["session_id"],
                    "source_run_key": row["source_run_key"],
                },
            }
        ),
        encoding="utf-8",
    )
    next_outbox = None
    for sequence_no, phase in ((1, "long_trend_eval"), (2, "completed")):
        payload = {
            "session_id": row["session_id"],
            "source_run_key": row["source_run_key"],
            "task_id": row["task_id"],
            "loop_id": row["loop_id"],
            "loop_index": row["loop_index"],
            "node_id": row["node_id"],
            "sequence_no": sequence_no,
            "phase": phase,
            "phase_status": "running" if phase == "long_trend_eval" else phase,
            "metadata": {"evaluation_id": row["evaluation_id"]},
        }
        event_hash = repair._canonical_sha256(payload)
        legacy_event_hash = repair._legacy_api_event_sha256(payload)
        delivered = sequence_no <= row["last_sequence_no"]
        if delivered:
            row["phase_event_hashes"][str(sequence_no)] = legacy_event_hash
        outbox = outbox_dir / f"{sequence_no:06d}.json"
        outbox.write_text(
            json.dumps({"payload": payload, "event_sha256": event_hash, "delivered": delivered}),
            encoding="utf-8",
        )
        if sequence_no == row["last_sequence_no"] + 1:
            next_outbox = outbox
    assert next_outbox is not None
    return next_outbox


def test_preflight_requires_exact_control_phase_and_durable_outbox_identity(tmp_path: Path) -> None:
    rows = [_candidate(0), _candidate(1)]
    for row in rows:
        _write_workspace_evidence(tmp_path, row)
    connection = _Connection(rows)

    plan = repair.collect_preflight(
        connection,
        tmp_path,
        expected_count=2,
        expected_outbox_count=4,
        expected_pending_outbox_count=3,
    )

    assert plan["candidate_count"] == 2
    assert [item["after"]["current_phase"] for item in plan["candidates"]] == ["created", "long_trend_eval"]
    assert [item["after"]["status"] for item in plan["candidates"]] == ["reserved", "running"]
    assert [item["outbox_evidence"]["next_sequence_no"] for item in plan["candidates"]] == [1, 2]
    assert plan["outbox_count"] == 4
    assert plan["pending_outbox_count"] == 3
    assert repair.validate_plan(plan)["plan_sha256"] == plan["plan_sha256"]


def test_preflight_fails_closed_on_count_or_outbox_hash_mismatch(tmp_path: Path) -> None:
    row = _candidate(0)
    outbox = _write_workspace_evidence(tmp_path, row)
    connection = _Connection([row])
    with pytest.raises(repair.QELTResourceRepairError, match="expected 2 repair candidates"):
        repair.collect_preflight(
            connection,
            tmp_path,
            expected_count=2,
            expected_outbox_count=2,
            expected_pending_outbox_count=2,
        )

    durable = json.loads(outbox.read_text(encoding="utf-8"))
    durable["event_sha256"] = "f" * 64
    outbox.write_text(json.dumps(durable), encoding="utf-8")
    with pytest.raises(repair.QELTResourceRepairError, match="event hash is inconsistent"):
        repair.collect_preflight(
            connection,
            tmp_path,
            expected_count=1,
            expected_outbox_count=2,
            expected_pending_outbox_count=2,
        )


def test_apply_is_guarded_idempotent_and_does_not_touch_phase_control_or_archive_rows(tmp_path: Path) -> None:
    rows = [_candidate(0), _candidate(1)]
    for row in rows:
        _write_workspace_evidence(tmp_path, row)
    connection = _Connection(rows)
    plan = repair.collect_preflight(
        connection,
        tmp_path,
        expected_count=2,
        expected_outbox_count=4,
        expected_pending_outbox_count=3,
    )

    first = repair.apply_plan(connection, plan)
    second = repair.apply_plan(connection, plan)
    readback = repair.readback(connection, plan, expect_repaired=True)

    assert first["updated_count"] == 2
    assert first["already_applied_count"] == 0
    assert second["updated_count"] == 0
    assert second["already_applied_count"] == 2
    assert readback["status"] == "passed"
    assert connection.rollbacks == 0
    write_sql = [sql for sql in connection.sql if sql.startswith("UPDATE ")]
    assert write_sql
    assert all(sql.startswith("UPDATE qe_archive.run_resource_session") for sql in write_sql)
    assert all("DELETE " not in sql and "INSERT " not in sql for sql in connection.sql)

    rolled_back = repair.rollback_plan(connection, first)
    assert rolled_back["updated_count"] == 2
    assert repair.readback(connection, plan, expect_repaired=False)["status"] == "passed"


def test_guarded_rollback_rejects_rows_with_newly_accepted_phase(tmp_path: Path) -> None:
    row = _candidate(0)
    _write_workspace_evidence(tmp_path, row)
    connection = _Connection([row])
    plan = repair.collect_preflight(
        connection,
        tmp_path,
        expected_count=1,
        expected_outbox_count=2,
        expected_pending_outbox_count=2,
    )
    receipt = repair.apply_plan(connection, plan)
    connection.states[row["session_id"]]["has_later_phase"] = True

    with pytest.raises(repair.QELTResourceRepairError, match="guarded rollback"):
        repair.rollback_plan(connection, receipt)
    assert connection.rollbacks == 1


def test_repair_contract_excludes_legitimate_terminal_reasons_and_guards_rollback() -> None:
    assert "s.terminal_reason_code = %s" in repair.PREFLIGHT_SQL
    assert "QELT_BUNDLE_INVALID" not in repair.PREFLIGHT_SQL
    assert "s.source_run_key LIKE 'qelt:%%'" in repair.PREFLIGHT_SQL
    assert "s.last_sequence_no IN (0, 1)" in repair.PREFLIGHT_SQL
    assert "qe_archive.run_evaluation" in repair.PREFLIGHT_SQL
    assert "NOT EXISTS" in repair.APPLY_SQL
    assert "NOT EXISTS" in repair.ROLLBACK_SQL
    assert "UPDATE qe_archive.run_resource_phase" not in repair.APPLY_SQL + repair.ROLLBACK_SQL
    assert "qe_archive.run_evaluation e" in repair.APPLY_SQL
    assert "qe_evolution_loops" not in repair.APPLY_SQL + repair.ROLLBACK_SQL


def test_dev_db_transactional_apply_readback_and_guarded_rollback() -> None:
    if os.getenv("AISTOCK_BUG847_DEV_DB_E2E") != "1":
        pytest.skip("set AISTOCK_BUG847_DEV_DB_E2E=1 to run the DEV-only transactional repair smoke")
    config = {
        "host": os.environ["TDX_DB_DEV_HOST"],
        "port": int(os.environ["TDX_DB_DEV_PORT"]),
        "dbname": os.environ["TDX_DB_DEV_NAME"],
        "user": os.environ["TDX_DB_DEV_USER"],
        "password": os.environ["TDX_DB_DEV_PASSWORD"],
    }
    assert config["host"] == "127.0.0.1"
    assert config["port"] == 5433
    assert "dev" in config["dbname"].lower()
    identity = uuid.uuid4().hex
    session_id = f"qers_{identity}"
    evaluation_id = f"qelt_{identity}{identity}"
    source_run_key = f"qelt:{evaluation_id}"
    request_sha = "a" * 64
    before = {
        "status": "completed",
        "current_phase": "completed",
        "last_sequence_no": 0,
        "terminal_reason_code": repair.RECONCILER_REASON,
        "completed_at": None,
    }
    candidate = {
        "session_id": session_id,
        "source_run_key": source_run_key,
        "evaluation_id": evaluation_id,
        "request_sha": request_sha,
        "before": before,
        "after": {
            "status": "reserved",
            "current_phase": "created",
            "last_sequence_no": 0,
            "terminal_reason_code": None,
            "completed_at": None,
        },
        "outbox_evidence": {"dev_transaction_smoke": True},
    }
    plan = {
        "schema_version": repair.PLAN_SCHEMA,
        "reason_code": "BUG_847_QELT_RESOURCE_SESSION_REPAIR",
        "expected_count": 1,
        "candidate_count": 1,
        "outbox_count": 2,
        "pending_outbox_count": 2,
        "workspace": "DEV_TRANSACTION_SMOKE",
        "candidates": [candidate],
    }
    plan["plan_sha256"] = repair._canonical_sha256(plan)
    connection = psycopg2.connect(**config)
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('qe_archive.run_resource_session'), "
                "to_regclass('qe_archive.run_evaluation'), to_regclass('qe_archive.run_resource_phase')"
            )
            relations = cur.fetchone()
            cur.execute("CREATE SCHEMA IF NOT EXISTS qe_archive")
            if relations[0] is None:
                cur.execute(
                    """
                    CREATE TABLE qe_archive.run_resource_session (
                        session_id text PRIMARY KEY,
                        source_run_key text NOT NULL UNIQUE,
                        attempt_no integer NOT NULL,
                        task_id text NOT NULL,
                        loop_id text NOT NULL,
                        loop_index integer NOT NULL,
                        node_id text NOT NULL,
                        token_sha256 text NOT NULL,
                        phase_pipeline_enabled boolean NOT NULL,
                        gpu_training_policy text NOT NULL,
                        current_phase text NOT NULL,
                        last_sequence_no integer NOT NULL,
                        status text NOT NULL,
                        terminal_reason_code text,
                        completed_at timestamptz,
                        updated_at timestamptz DEFAULT NOW()
                    )
                    """
                )
            if relations[1] is None:
                cur.execute(
                    """
                    CREATE TABLE qe_archive.run_evaluation (
                        evaluation_id text PRIMARY KEY,
                        parent_task_id text NOT NULL,
                        parent_loop_index integer NOT NULL,
                        profile_id text NOT NULL,
                        profile_sha256 text NOT NULL,
                        evaluator_version text NOT NULL,
                        evaluator_source_sha256 text NOT NULL,
                        execution_environment_snapshot_id text NOT NULL,
                        execution_environment_manifest_sha256 text NOT NULL,
                        bundle_sha256 text NOT NULL,
                        qe_dataset_contract_id text NOT NULL,
                        input_manifest_sha256 text NOT NULL,
                        node_id text NOT NULL,
                        request_sha text NOT NULL,
                        request_json jsonb,
                        resource_session_id text,
                        evaluation_type text,
                        status text
                    )
                    """
                )
            if relations[2] is None:
                cur.execute(
                    """
                    CREATE TABLE qe_archive.run_resource_phase (
                        session_id text NOT NULL,
                        sequence_no integer NOT NULL,
                        event_sha256 text,
                        PRIMARY KEY (session_id, sequence_no)
                    )
                    """
                )
            cur.execute(
                """
                INSERT INTO qe_archive.run_resource_session (
                    session_id, source_run_key, attempt_no, task_id, loop_id, loop_index,
                    node_id, token_sha256, phase_pipeline_enabled, gpu_training_policy,
                    current_phase, last_sequence_no, status, terminal_reason_code
                ) VALUES (%s, %s, 1, %s, 'Loop1', 1, 'bug847-dev-node', %s,
                          FALSE, 'exclusive', 'completed', 0, 'completed', %s)
                """,
                (session_id, source_run_key, f"bug847-dev-{identity}", "b" * 64, repair.RECONCILER_REASON),
            )
            cur.execute(
                """
                INSERT INTO qe_archive.run_evaluation (
                    evaluation_id, parent_task_id, parent_loop_index, profile_id, profile_sha256,
                    evaluator_version, evaluator_source_sha256, execution_environment_snapshot_id,
                    execution_environment_manifest_sha256, bundle_sha256, qe_dataset_contract_id,
                    input_manifest_sha256, node_id, request_sha, request_json, resource_session_id,
                    evaluation_type, status
                ) VALUES (%s, %s, 1, 'bug847-dev-profile', %s, 'bug847-dev', %s,
                          'bug847-dev-env', %s, %s, 'bug847-dev-dataset', %s,
                          'bug847-dev-node', %s, '{}'::jsonb, %s, 'long_trend', 'queued')
                """,
                (
                    evaluation_id,
                    f"bug847-dev-{identity}",
                    "c" * 64,
                    "d" * 64,
                    "e" * 64,
                    "f" * 64,
                    "1" * 64,
                    request_sha,
                    session_id,
                ),
            )

        receipt = repair.apply_plan(connection, plan, commit=False)
        assert repair.readback(connection, plan, expect_repaired=True)["status"] == "passed"
        assert repair.rollback_plan(connection, receipt, commit=False)["updated_count"] == 1
        assert repair.readback(connection, plan, expect_repaired=False)["status"] == "passed"
    finally:
        connection.rollback()
        connection.close()
