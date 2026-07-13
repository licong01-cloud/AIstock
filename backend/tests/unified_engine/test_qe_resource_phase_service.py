from __future__ import annotations

import pytest

from backend.services.quantevolver.qe_resource_phase_service import (
    AUTH_FAILED_REASON,
    PHASE_INVALID_REASON,
    QEResourcePhaseError,
    _canonical_sha256,
    _token_sha256,
    QEResourcePhaseService,
    RESOURCE_SCHEMA_REASON,
    validate_phase_transition,
)


class _FakeResourceState:
    def __init__(self):
        self.tables_ready = True
        self.sessions = {}
        self.phases = {}
        self.list_rows = []
        self.has_unreleased = False
        self.commits = 0


class _FakeResourceCursor:
    def __init__(self, state):
        self.state = state
        self.row = None
        self.rows = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):  # type: ignore[no-untyped-def]
        normalized = " ".join(sql.split())
        params = tuple(params or ())
        self.row = None
        self.rows = []
        self.rowcount = 0
        if "SELECT to_regclass" in normalized:
            self.row = ("run_resource_session", "run_resource_phase") if self.state.tables_ready else (None, None)
        elif "pg_advisory_xact_lock" in normalized:
            self.row = (None,)
        elif "SELECT COALESCE(MAX(attempt_no)" in normalized:
            source_run_key = params[0]
            attempts = [row["attempt_no"] for row in self.state.sessions.values() if row["source_run_key"] == source_run_key]
            self.row = (max(attempts, default=0) + 1,)
        elif "INSERT INTO qe_archive.run_resource_session" in normalized:
            (
                session_id,
                source_run_key,
                attempt_no,
                task_id,
                loop_id,
                loop_index,
                node_id,
                token_sha256,
                phase_pipeline_enabled,
            ) = params
            self.state.sessions[session_id] = {
                "session_id": session_id,
                "source_run_key": source_run_key,
                "attempt_no": attempt_no,
                "task_id": task_id,
                "loop_id": loop_id,
                "loop_index": loop_index,
                "node_id": node_id,
                "archive_run_id": None,
                "token_sha256": token_sha256,
                "phase_pipeline_enabled": phase_pipeline_enabled,
                "current_phase": "created",
                "last_sequence_no": 0,
                "status": "reserved",
                "gpu_phase_released_at": None,
                "terminal_reason_code": None,
                "created_at": None,
                "updated_at": None,
                "completed_at": None,
            }
            self.rowcount = 1
        elif "SET status = 'running'" in normalized:
            session = self.state.sessions.get(params[0])
            if session and session["status"] == "reserved":
                session["status"] = "running"
                self.rowcount = 1
        elif normalized.startswith("SELECT status FROM qe_archive.run_resource_session"):
            session = self.state.sessions.get(params[0])
            self.row = (session["status"],) if session else None
        elif "SET status = %s," in normalized and "terminal_reason_code" in normalized:
            status, phase, reason_code, session_id = params
            session = self.state.sessions[session_id]
            session["status"] = status
            if session["current_phase"] not in {"completed", "failed", "cancelled"}:
                session["current_phase"] = phase
            session["terminal_reason_code"] = reason_code or session["terminal_reason_code"]
            self.rowcount = 1
        elif "SELECT 1 FROM qe_archive.run_resource_session" in normalized:
            self.row = (1,) if self.state.has_unreleased else None
        elif normalized.startswith("SELECT session_id, source_run_key"):
            session = self.state.sessions.get(params[0])
            self.row = dict(session) if session else None
        elif "SELECT * FROM qe_archive.run_resource_session" in normalized:
            session = self.state.sessions.get(params[0])
            self.row = dict(session) if session else None
        elif "SELECT id, event_sha256 FROM qe_archive.run_resource_phase" in normalized:
            prior = self.state.phases.get((params[0], params[1]))
            self.row = {"id": 1, "event_sha256": prior["event_sha256"]} if prior else None
        elif "INSERT INTO qe_archive.run_resource_phase" in normalized:
            self.state.phases[(params[0], params[2])] = {
                "session_id": params[0],
                "source_run_key": params[1],
                "sequence_no": params[2],
                "phase": params[3],
                "event_sha256": params[-1],
            }
            self.rowcount = 1
        elif "SET current_phase = %s," in normalized:
            phase, sequence_no, terminal_status, _phase_again, _terminal_1, _terminal_2, reason_code, session_id = params
            session = self.state.sessions[session_id]
            session["current_phase"] = phase
            session["last_sequence_no"] = sequence_no
            if terminal_status:
                session["status"] = terminal_status
                session["terminal_reason_code"] = reason_code
            self.rowcount = 1
        elif "jsonb_agg" in normalized:
            self.rows = list(self.state.list_rows)
        elif "SET archive_run_id = %s" in normalized:
            archive_run_id = params[0]
            candidates = [
                row
                for row in self.state.sessions.values()
                if row["task_id"] == params[1] and row["loop_index"] == params[2] and row["archive_run_id"] is None
            ]
            if len(params) == 4:
                candidates = [row for row in candidates if row["attempt_no"] == params[3]]
            elif candidates:
                candidates = [max(candidates, key=lambda row: row["attempt_no"])]
            for row in candidates:
                row["archive_run_id"] = archive_run_id
            self.rowcount = len(candidates)
        else:  # pragma: no cover - keeps fake strict as SQL evolves
            raise AssertionError(f"unhandled SQL: {normalized}")

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows


class _FakeResourceConn:
    def __init__(self, state):
        self.state = state

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, *_args, **_kwargs):
        return _FakeResourceCursor(self.state)

    def commit(self):
        self.state.commits += 1


def _service_with_fake_state():
    state = _FakeResourceState()
    return QEResourcePhaseService(connection_provider=lambda: _FakeResourceConn(state)), state


def test_resource_phase_token_hash_and_event_hash_are_deterministic():
    assert _token_sha256("secret") == _token_sha256("secret")
    assert _token_sha256("secret") != _token_sha256("other")
    assert _canonical_sha256({"b": 2, "a": 1}) == _canonical_sha256({"a": 1, "b": 2})


def test_gpu_phase_release_requires_positive_release_proof():
    event = {
        "phase": "gpu_phase_released",
        "release_check_passed": True,
        "reason_code": "QE_GPU_PHASE_RELEASE_CONFIRMED",
    }
    validate_phase_transition("predict", event)

    with pytest.raises(QEResourcePhaseError) as exc_info:
        validate_phase_transition(
            "predict",
            {**event, "release_check_passed": False},
        )
    assert exc_info.value.reason_code == PHASE_INVALID_REASON

    with pytest.raises(QEResourcePhaseError) as exc_info:
        validate_phase_transition(
            "train",
            event,
        )
    assert exc_info.value.reason_code == PHASE_INVALID_REASON


def test_resource_phase_state_machine_rejects_regression_and_allows_safe_terminal():
    validate_phase_transition("created", {"phase": "bootstrap"})
    validate_phase_transition("bootstrap", {"phase": "train"})
    validate_phase_transition("release_rejected", {"phase": "backtest"})
    validate_phase_transition("backtest", {"phase": "completed"})

    with pytest.raises(QEResourcePhaseError, match="not allowed"):
        validate_phase_transition("backtest", {"phase": "train"})
    with pytest.raises(QEResourcePhaseError, match="unknown phase"):
        validate_phase_transition("created", {"phase": "guessed_from_log"})


def test_resource_session_lifecycle_and_archive_binding():
    service, state = _service_with_fake_state()
    secret = service.create_session(
        task_id="qe_task",
        loop_index=1,
        node_id="wsl2-5080",
        phase_pipeline_enabled=True,
    )
    assert secret.source_run_key == "qe_task_L1"
    assert secret.attempt_no == 1
    assert state.sessions[secret.session_id]["token_sha256"] == _token_sha256(secret.token)

    service.mark_session_submitted(secret.session_id)
    assert service.get_session_state(secret.session_id)["status"] == "running"
    state.has_unreleased = True
    assert service.has_unreleased_gpu_session(node_id="wsl2-5080") is True
    state.has_unreleased = False
    assert service.has_unreleased_gpu_session(node_id="wsl2-5080") is False

    assert service.bind_archive_run(
        task_id="qe_task",
        loop_index=1,
        archive_run_id="qear_run_1",
        attempt_no=1,
    ) == 1
    assert state.sessions[secret.session_id]["archive_run_id"] == "qear_run_1"

    service.mark_session_terminal(secret.session_id, status="completed", reason_code="done")
    assert service.get_session_state(secret.session_id)["current_phase"] == "completed"


def test_resource_schema_readiness_fails_loudly():
    service, state = _service_with_fake_state()
    state.tables_ready = False
    with pytest.raises(QEResourcePhaseError) as exc_info:
        service.ensure_schema_ready()
    assert exc_info.value.reason_code == RESOURCE_SCHEMA_REASON


def test_resource_event_ingestion_is_ordered_authenticated_and_idempotent():
    service, state = _service_with_fake_state()
    secret = service.create_session(
        task_id="qe_task",
        loop_index=2,
        node_id="wsl2-5080",
        phase_pipeline_enabled=True,
    )
    base = {
        "session_id": secret.session_id,
        "source_run_key": secret.source_run_key,
        "task_id": "qe_task",
        "loop_id": "Loop2",
        "loop_index": 2,
        "node_id": "wsl2-5080",
        "phase_status": "completed",
        "sample_count": 1,
        "metadata": {},
    }
    events = [
        {**base, "sequence_no": 1, "phase": "bootstrap"},
        {**base, "sequence_no": 2, "phase": "train"},
        {**base, "sequence_no": 3, "phase": "predict"},
        {
            **base,
            "sequence_no": 4,
            "phase": "gpu_phase_released",
            "phase_status": "released",
            "release_check_passed": True,
            "reason_code": "QE_GPU_PHASE_RELEASE_CONFIRMED",
        },
    ]
    for event in events:
        assert service.ingest_event(token=secret.token, payload=event)["status"] == "accepted"
    assert service.get_session_state(secret.session_id)["current_phase"] == "gpu_phase_released"
    assert service.ingest_event(token=secret.token, payload=events[-1])["status"] == "idempotent"

    with pytest.raises(QEResourcePhaseError, match="token") as exc_info:
        service.ingest_event(token="wrong", payload={**base, "sequence_no": 5, "phase": "backtest"})
    assert exc_info.value.reason_code == AUTH_FAILED_REASON

    with pytest.raises(QEResourcePhaseError, match="expected sequence"):
        service.ingest_event(token=secret.token, payload={**base, "sequence_no": 6, "phase": "backtest"})


def test_resource_phase_query_returns_bounded_rows():
    service, state = _service_with_fake_state()
    state.list_rows = [{"session_id": "qers_1", "phases": [{"phase": "train"}]}]
    rows = service.list_resource_phases(task_id="qe_task", loop_index=1, limit=999)
    assert rows == state.list_rows
