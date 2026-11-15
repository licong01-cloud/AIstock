from __future__ import annotations

from dataclasses import replace
import asyncio
from typing import Any
from unittest.mock import Mock
import json
import os

import pytest

from backend.services.quantevolver.qe_active_execution_capacity import (
    MAX_RESTART_SAFE_LEASE_SECONDS,
    QEActiveExecutionCapacityService,
    QEExecutionReservationReconciler,
    QEWorkspaceSubmissionCoordinator,
    QEWorkspaceSubmissionPayload,
    QEWorkspaceSubmissionCoordinatorError,
    QEWorkspaceSubmissionSource,
    submission_intent_hash_for_source,
)
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceSubmissionInspection
from backend.services.quantevolver.qe_execution_reservation import (
    QEExecutionCapacityObservation,
    QEExecutionReservationAcquireResult,
    QEExecutionReservationRepository,
    QEExecutionReservationSpec,
)
from backend.tests.multi_alpha.test_durable_capacity import (
    ScriptedCursor, ScriptedProvider, Step, _spec, _reservation_row,
)
from backend.services.multi_alpha.durable_execution_adapter import (
    DurableSubmissionIntent,
    QEWorkspacePredBacktestAdapter,
)
from backend.services.quantevolver.qe_wsl_workload_isolation import isolate_payload as real_isolate_payload


@pytest.fixture(autouse=True)
def protocol_tests_do_not_materialize_linux_runtime(monkeypatch):
    """These tests use dummy shell strings; real envelopes have their own matrix."""
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_wsl_workload_isolation.isolate_payload",
        lambda payload, **_: payload,
    )


@pytest.mark.parametrize("node_id,api_url", [
    ("wsl2-5080", "http://127.0.0.1:9000"),
    ("rdagent-node1", "http://192.0.2.1:9000"),
])
def test_real_multi_alpha_pred_backtest_command_reaches_isolated_transport(
    monkeypatch, tmp_path, node_id, api_url,
):
    import hashlib
    import json
    import shlex
    from backend.services.multi_alpha.remote_dispatch import ComputeNodeInfo, _remote_wsl_command

    # Undo the protocol-only fixture here: this is the real producer/guard seam.
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_wsl_workload_isolation.isolate_payload",
        real_isolate_payload,
    )
    workspace_base = "/mnt/f/QE runs/author's assets" if node_id.startswith("wsl") else "/home/qe/workspaces"
    paths = {
        "workspace_base": workspace_base,
        "artifact_path": "/artifacts/factors",
        "prediction_artifact_path": "/artifacts/predictions",
        "qlib_data_path": "/datasets/frozen/components/daily_bin_candidate",
        "factor_cache_dir": "/datasets/frozen/components/factor_h5_static_candidate_v2",
    }
    command = _remote_wsl_command(
        workspace=tmp_path,
        node=ComputeNodeInfo(node_id=node_id, api_base_url=api_url),
        remote_paths=paths,
        backtest_config={
            "remote_task_id": "macb_exact", "remote_loop_index": 3,
            "wsl_conda_env": "frozen-env", "wsl_conda_sh": "/home/qe/conda.sh",
        },
    )
    payload = QEWorkspaceSubmissionPayload("macb_exact", 3, {}, {"conf.yaml": "frozen"}, command)
    seen = []

    class CapturedTransport(Exception):
        pass

    class Client:
        async def submit_loop(self, *args, **kwargs):
            seen.append(args)
            raise CapturedTransport

    with pytest.raises(CapturedTransport):
        asyncio.run(QEWorkspaceSubmissionCoordinator(reservation_repository=Mock())._post_with_reconciliation(
            client=Client(), source=_source(node_id=node_id, backtest_only=True), payload=payload,
            spec=None, acquired=None, reservation={}, token=None,
        ))
    assert len(seen) == 1
    assert "python qrun_limit_minute.py conf.yaml --pred-backtest combined_prediction.pkl" in command
    assert "reason_code=qe_subprocess_bash_required" in command
    if node_id.startswith("wsl"):
        assert shlex.split(command)[:4] == ["cd", "--", workspace_base + "/macb_exact/Loop3", "&&"]
        files = seen[0][3]
        assert files["qe_wsl_workload_command.sh"] == command
        policy = json.loads(files["qe_wsl_workload_policy.json"])
        assert policy["kind"] == "replay" and policy["peak_bytes"] == 20 * 1024**3
        assert policy["original_command_sha256"] == hashlib.sha256(command.encode()).hexdigest()
        assert policy["original_files_sha256"] == {"conf.yaml": hashlib.sha256(b"frozen").hexdigest()}
    else:
        assert command.startswith("bash --noprofile --norc -c ")
        assert seen[0][3] == {"conf.yaml": "frozen"} and seen[0][4] == command


@pytest.mark.parametrize("workspace_base", ["", "relative/path", "C:\\jobs"])
def test_real_multi_alpha_wsl_command_requires_explicit_linux_workspace(tmp_path, workspace_base):
    from backend.services.multi_alpha.combine_backtest import MultiAlphaCombineBacktestError
    from backend.services.multi_alpha.remote_dispatch import ComputeNodeInfo, _remote_wsl_command

    with pytest.raises(MultiAlphaCombineBacktestError) as error:
        _remote_wsl_command(
            workspace=tmp_path,
            node=ComputeNodeInfo(node_id="wsl2-5080", api_base_url="http://127.0.0.1:9000"),
            remote_paths={"workspace_base": workspace_base, "artifact_path": "/a", "prediction_artifact_path": "/b",
                          "qlib_data_path": "/data", "factor_cache_dir": "/factors"},
            backtest_config={"wsl_conda_env": "frozen-env", "wsl_conda_sh": "/home/qe/conda.sh"},
        )
    assert error.value.reason_code == "remote_workspace_path_invalid"


@pytest.mark.parametrize("released,remote_status,pid", [(True, "not_reserved", None),
                                                       (False, "not_reserved", None),
                                                       (False, "running", None),
                                                       (False, "unreachable", None),
                                                       (False, "not_reserved", 123)])
def test_reconciler_rechecks_deleted_source_even_when_not_reserved_is_unchanged(
    monkeypatch: pytest.MonkeyPatch, released: bool, remote_status: str, pid: int | None,
) -> None:
    row = _reservation_row(replace(_spec(), source_kind="qe_evolution_loop"),
                           status="reconciling", remote_status="not_reserved")
    repository = Mock()
    repository.release_deleted_evolution_reservation.return_value = row if released else None
    reconciler = QEExecutionReservationReconciler(repository=repository, owner_id="reconciler")

    class Client:
        async def __aenter__(self) -> Any:
            return self

        async def __aexit__(self, *_: Any) -> None:
            pass

        async def inspect_loop_submission(self, task_id: str, loop_id: str, **_: Any) -> Any:
            if remote_status == "unreachable":
                raise ConnectionError("node receipt unavailable")
            return QEWorkspaceSubmissionInspection(
                "qe_submission_receipt_v1", task_id, loop_id, remote_status,
                submission_intent_hash="a" * 64 if remote_status != "not_reserved" else None,
                pid=pid,
            )

    monkeypatch.setattr(
        "backend.services.quantevolver.qe_active_execution_capacity.QEWorkspaceClient.for_node",
        lambda _: Client(),
    )
    notify = Mock()
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_reconciliation_coordinator.notify_qe_reconciliation",
        notify,
    )
    if remote_status == "running":
        row.update(status="running", remote_status="running", submission_intent_hash="a" * 64)
    if remote_status == "unreachable":
        with pytest.raises(ConnectionError):
            asyncio.run(reconciler._reconcile_one(row))
        repository.release_deleted_evolution_reservation.assert_not_called()
        return
    if pid is not None:
        with pytest.raises(QEWorkspaceSubmissionCoordinatorError) as exc:
            asyncio.run(reconciler._reconcile_one(row))
        assert exc.value.reason_code == "qe_workspace_submission_receipt_invalid"
        repository.release_deleted_evolution_reservation.assert_not_called()
        return
    result = asyncio.run(reconciler._reconcile_one(row))
    assert result == ("terminal_released" if released else None)
    assert repository.release_deleted_evolution_reservation.call_count == (
        1 if remote_status == "not_reserved" else 0
    )
    repository.claim_reservation_for_source.assert_not_called()
    repository.transition_execution_reservation.assert_not_called()
    assert notify.call_count == (2 if released else 0)


@pytest.mark.parametrize("eligible,incompatible,capacity", [(1, 0, 4), (1, 1, 1), (0, 0, 1)])
def test_durable_replay_cohort_proof_preserves_global_capacity_and_training_exclusion(
    eligible: int, incompatible: int, capacity: int,
) -> None:
    spec = _spec()
    source = {"qe_task_id": spec.qe_task_id, "qe_loop_id": spec.qe_loop_id,
              "submission_intent_hash": spec.submission_intent_hash} if eligible else None
    steps = [Step("FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt", one=source)]
    if eligible:
        steps.append(Step("AS incompatible_count", one={"incompatible_count": incompatible}))
    cursor = ScriptedCursor(steps)
    result = QEExecutionReservationRepository._effective_node_capacity_for_source(
        cursor, spec, requested_node_capacity=4,
        allow_same_task_backtest_parallelism=False,
        allow_same_task_parallel_training=False,
        allow_durable_prediction_replay_parallelism=True,
    )
    assert result == capacity
    sql, params = cursor.executions[0]
    assert "child.input_manifest_json" in sql
    assert params == (spec.source_execution_id, spec.node_id)
    if eligible:
        cohort_sql, _ = cursor.executions[1]
        assert "attempt.submission_intent_hash = reservation.submission_intent_hash" in cohort_sql
        assert "reservation.qe_task_id = %s" not in cohort_sql  # Independent parent runs can replay.
    assert not cursor.steps


def test_durable_replay_capacity_proof_cannot_be_used_for_unrelated_training_source() -> None:
    spec = QEExecutionReservationSpec(
        node_id="wsl2-5080", source_kind="qe_experiment", source_execution_id="training1",
        qe_task_id="remote_task", qe_loop_id="Loop1", submission_intent_hash="a" * 64,
    )
    cursor = ScriptedCursor([])
    assert QEExecutionReservationRepository._effective_node_capacity_for_source(
        cursor, spec, requested_node_capacity=4, allow_same_task_backtest_parallelism=False,
        allow_same_task_parallel_training=False, allow_durable_prediction_replay_parallelism=True,
    ) == 1
    assert not cursor.executions


def _queued_replay_source(retry_mode: str = "initial", attempt_no: int = 1):
    from backend.services.multi_alpha.durable_models import (
        artifact_manifest_hash_for, make_attempt_id, sha256_identity,
    )
    from backend.tests.multi_alpha.test_durable_control import _identity

    payload = _identity()
    manifest = {"execution_identity": payload, "execution_identity_hash": sha256_identity(payload)}
    row = {
        "run_id": "macb_capacity_queued", "child_id": "macbc_capacity_queued",
        "attempt_no": attempt_no, "retry_mode": retry_mode, "execution_kind": "remote_execution",
        "retry_of_attempt_id": make_attempt_id("macbc_capacity_queued", attempt_no - 1) if attempt_no > 1 else None,
        "status": "queued", "node_id": "wsl2-5080", "qe_task_id": None,
        "qe_loop_id": None, "submission_intent_hash": None,
        "input_manifest_json": manifest, "input_manifest_hash": artifact_manifest_hash_for(manifest),
        "run_execution_identity_json": payload, "run_execution_identity_hash": sha256_identity(payload),
    }
    row["attempt_id"] = make_attempt_id(row["child_id"], attempt_no)
    # The expected intent comes from the actual adapter, not a second test formula.
    intent = QEWorkspacePredBacktestAdapter(repository=Mock()).prepare_submission_intent(
        run={"id": row["run_id"], "execution_identity_json": payload, "execution_identity_hash": sha256_identity(payload)},
        child={"child_id": row["child_id"], "input_manifest_json": manifest},
        attempt=row, node_id=row["node_id"],
    )
    spec = QEExecutionReservationSpec(row["node_id"], "multi_alpha_durable_attempt",
        row["attempt_id"], intent.qe_task_id, intent.qe_loop_id, intent.submission_intent_hash)
    return row, spec


def _replay_capacity_case(case: str):
    from backend.services.multi_alpha.durable_models import artifact_manifest_hash_for
    mode, number = ("backtest_only", 2) if case == "retry" else (
        ("rematerialize_and_backtest", 2) if case == "rematerialize" else ("initial", 1))
    row, spec = _queued_replay_source(mode, number)
    expected = 4
    if case == "partial":
        row["qe_task_id"] = spec.qe_task_id
    elif case == "empty":
        row.update(qe_task_id="", qe_loop_id="", submission_intent_hash="")
    elif case == "conflict":
        row.update(qe_task_id="other", qe_loop_id="Loop1", submission_intent_hash="f" * 64)
    elif case == "bound":
        row.update(qe_task_id=spec.qe_task_id, qe_loop_id=spec.qe_loop_id,
                   submission_intent_hash=spec.submission_intent_hash)
    elif case == "wrong_hash":
        spec = replace(spec, submission_intent_hash="f" * 64)
    elif case == "wrong_task":
        spec = replace(spec, qe_task_id="different_task")
    elif case == "wrong_loop":
        spec = replace(spec, qe_loop_id="Loop2")
    elif case == "wrong_attempt":
        spec = replace(spec, source_execution_id="another_attempt")
    elif case == "wrong_node":
        spec = replace(spec, node_id="rdagent-node1")
    elif case == "manifest_hash":
        row["input_manifest_hash"] = "f" * 64
    elif case == "identity_hash":
        row["input_manifest_json"]["execution_identity_hash"] = "f" * 64
        row["input_manifest_hash"] = artifact_manifest_hash_for(row["input_manifest_json"])
    elif case == "parent_identity":
        row["run_execution_identity_hash"] = "f" * 64
    elif case == "terminal":
        row["status"] = "cancelled"
    elif case == "reference":
        row["execution_kind"] = "reference_result"
    elif case == "mixed_training":
        pass
    elif case not in {"initial", "retry", "rematerialize"}:
        raise AssertionError(case)
    if case not in {"initial", "retry", "rematerialize", "bound"}:
        expected = 1
    return row, spec, expected


REPLAY_QUEUED_CASES = ["initial", "retry", "rematerialize", "bound", "partial", "empty", "conflict",
    "wrong_hash", "wrong_task", "wrong_loop", "wrong_attempt", "wrong_node", "manifest_hash",
    "identity_hash", "parent_identity", "terminal", "reference", "mixed_training"]


@pytest.mark.parametrize("case", REPLAY_QUEUED_CASES)
def test_queued_durable_replay_uses_frozen_adapter_identity_before_source_claim(case: str) -> None:
    row, spec, expected = _replay_capacity_case(case)
    if case == "wrong_node":
        # This is a helper-level check; the SQL WHERE already excludes another node.
        assert not QEExecutionReservationRepository._durable_replay_source_matches(row, spec)
        return
    steps = [Step("FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt", one=row)]
    if expected == 4 or case == "mixed_training":
        steps.append(Step("AS incompatible_count", one={"incompatible_count": int(case == "mixed_training")}))
    cur = ScriptedCursor(steps)
    actual = QEExecutionReservationRepository._effective_node_capacity_for_source(cur, spec,
        requested_node_capacity=4, allow_same_task_backtest_parallelism=False,
        allow_same_task_parallel_training=False, allow_durable_prediction_replay_parallelism=True)
    assert actual == expected
    assert not cur.steps


@pytest.mark.parametrize("case", REPLAY_QUEUED_CASES)
def test_queued_replay_identity_sql_on_existing_dev_readonly(case: str) -> None:
    """Execute the real repository SELECTs against typed CTE rows in existing DEV.

    This opt-in test changes no tables, schema, persistent rows, or runtime status.
    It exercises PostgreSQL NULL/join semantics that scripted count fixtures missed.
    """
    if os.getenv("AISTOCK_QE_REPLAY_QUEUED_DEV_VERIFY") != "1":
        pytest.skip("explicit existing DEV SELECT verification not requested")
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from dotenv import dotenv_values

    cfg = dotenv_values("F:/Dev/AIstock/.env")
    credentials = {"host": cfg["TDX_DB_DEV_HOST"], "port": int(cfg["TDX_DB_DEV_PORT"]),
        "dbname": cfg["TDX_DB_DEV_NAME"], "user": cfg["TDX_DB_DEV_USER"], "password": cfg["TDX_DB_DEV_PASSWORD"]}
    assert credentials["port"] == 5433 and "dev" in credentials["dbname"].lower()
    row, spec, expected = _replay_capacity_case(case)
    columns = ["attempt_id", "child_id", "attempt_no", "retry_mode", "retry_of_attempt_id",
        "execution_kind", "status", "node_id", "qe_task_id", "qe_loop_id", "submission_intent_hash"]
    selected = ",".join("%s::" + ("integer" if c == "attempt_no" else "text") + " AS " + c for c in columns)
    prefix = ("WITH bug2227_attempt AS (SELECT " + selected + "),"
        " bug2227_child AS (SELECT %s::text AS child_id,%s::text AS run_id,%s::jsonb AS input_manifest_json,%s::text AS input_manifest_hash),"
        " bug2227_run AS (SELECT %s::text AS id,%s::jsonb AS execution_identity_json,%s::text AS execution_identity_hash),"
        " bug2227_reservation AS (SELECT 'wsl2-5080'::text AS node_id,'running'::text AS status,"
        " 'qe_evolution_loop'::text AS source_kind,'other'::text AS source_execution_id,'other'::text AS qe_task_id,"
        " 'Loop1'::text AS qe_loop_id,repeat('a',64)::text AS submission_intent_hash WHERE %s::boolean) ")
    fixture_params = tuple(row[c] for c in columns) + (row["child_id"], row["run_id"],
        json.dumps(row["input_manifest_json"]), row["input_manifest_hash"], row["run_id"],
        json.dumps(row["run_execution_identity_json"]), row["run_execution_identity_hash"], case == "mixed_training")
    conn = psycopg2.connect(**credentials, connect_timeout=5)
    conn.set_session(readonly=True, autocommit=False)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as real:
            class ReadOnlyFixtureCursor:
                def execute(self, query, params):
                    # Also resolve the untouched SQL against the actual DEV schema.
                    real.execute("EXPLAIN (FORMAT JSON) " + query, params)
                    real.fetchall()
                    query = query.replace("strategy_pkg.multi_alpha_combine_backtest_child_attempt", "bug2227_attempt")
                    query = query.replace("strategy_pkg.multi_alpha_combine_backtest_child", "bug2227_child")
                    query = query.replace("strategy_pkg.multi_alpha_combine_backtest_run", "bug2227_run")
                    query = query.replace("infra.qe_execution_reservation", "bug2227_reservation")
                    assert query.lstrip().startswith("SELECT")
                    real.execute(prefix + query, fixture_params + tuple(params))

                def fetchone(self): return real.fetchone()

            actual = QEExecutionReservationRepository._effective_node_capacity_for_source(ReadOnlyFixtureCursor(), spec,
                requested_node_capacity=4, allow_same_task_backtest_parallelism=False,
                allow_same_task_parallel_training=False, allow_durable_prediction_replay_parallelism=True)
            assert actual == expected
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.parametrize("active,accepted", [(3, True), (4, False)])
def test_durable_replay_admission_keeps_node_lock_and_once_only_transaction(
    active: int, accepted: bool,
) -> None:
    spec = _spec(qe_task_id="different_parent_remote_task")
    steps = [
        Step("pg_advisory_xact_lock"), Step("pg_advisory_xact_lock"),
        Step("SELECT node_id FROM infra.compute_nodes", one={"node_id": spec.node_id}),
        Step("FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt", one={
            "qe_task_id": spec.qe_task_id, "qe_loop_id": spec.qe_loop_id,
            "submission_intent_hash": spec.submission_intent_hash,
        }),
        Step("AS incompatible_count", one={"incompatible_count": 0}),
        Step("WHERE source_kind = %s AND source_execution_id = %s", one=None),
        Step("WHERE node_id = %s AND qe_task_id = %s", one=None),
        Step("SELECT COUNT(*) AS active_count", one={"active_count": active}),
    ]
    if accepted:
        steps.extend([Step("CLAIM_SOURCE", one={"status": "submitting"}),
                      Step("INSERT INTO infra.qe_execution_reservation", one=_reservation_row(spec))])
    else:
        steps.append(Step("WAIT_CAPACITY", one={"status": "queued"}))
    provider = ScriptedProvider(steps)

    def claim(cur: Any) -> Any:
        cur.execute("CLAIM_SOURCE")
        return cur.fetchone()

    def waiting(cur: Any, actual: int, capacity: int) -> Any:
        assert actual == capacity == 4
        cur.execute("WAIT_CAPACITY")
        return cur.fetchone()

    result = QEExecutionReservationRepository(provider).reserve_execution_and_claim_source(
        spec, node_capacity=4, allow_durable_prediction_replay_parallelism=True,
        owner_id="worker_1", lease_seconds=30, claim_source=claim,
        record_waiting_capacity=waiting,
    )
    assert result.acquired is accepted and result.node_capacity == 4
    assert result.active_count == 4
    assert provider.commits == 1 and provider.rollbacks == 0
    assert not provider.cursor.steps


@pytest.mark.parametrize(
    ("node_id", "requested", "backtest_only", "parallel_training", "expected"),
    [
        ("wsl2-5080", None, False, False, 1),
        ("WSL2-5080", 8, False, False, 1),
        ("wsl2-5080", None, True, False, 2),
        ("wsl2-5080", 1, True, False, 1),
        ("wsl2-5080", 2, True, False, 2),
        ("wsl2-5080", 3, True, False, 3),
        ("wsl2-5080", 4, True, False, 4),
        ("wsl2-5080", 8, True, False, 4),
        ("wsl2-5080", 8, False, True, 2),
        ("rdagent-node1", None, False, False, 4),
        ("rdagent-node1", 3, False, False, 3),
        ("rdagent-node1", 8, True, False, 4),
    ],
)
def test_node_capacity_contract(
    node_id: str,
    requested: int | None,
    backtest_only: bool,
    parallel_training: bool,
    expected: int,
) -> None:
    service = QEActiveExecutionCapacityService()

    assert service.resolve_node_capacity(
        node_id,
        requested,
        backtest_only=backtest_only,
        parallel_training=parallel_training,
    ) == expected


@pytest.mark.parametrize(
    ("node_id", "requested", "reason_code"),
    [
        ("wsl", None, "qe_execution_capacity_node_alias_noncanonical"),
        ("LOCAL", None, "qe_execution_capacity_node_alias_noncanonical"),
        ("rdagent-node1", 0, "qe_execution_capacity_request_invalid"),
    ],
)
def test_invalid_capacity_requests_fail_closed(
    node_id: str,
    requested: int | None,
    reason_code: str,
) -> None:
    with pytest.raises(QEWorkspaceSubmissionCoordinatorError) as excinfo:
        QEActiveExecutionCapacityService().resolve_node_capacity(node_id, requested)

    assert excinfo.value.reason_code == reason_code


def test_capacity_mode_is_unambiguous() -> None:
    with pytest.raises(QEWorkspaceSubmissionCoordinatorError) as excinfo:
        QEActiveExecutionCapacityService().resolve_node_capacity(
            "wsl2-5080",
            2,
            backtest_only=True,
            parallel_training=True,
        )

    assert excinfo.value.reason_code == "qe_execution_capacity_contract_invalid"


def _source(**overrides: Any) -> QEWorkspaceSubmissionSource:
    values: dict[str, Any] = {
        "source_kind": "qe_evolution_loop",
        "source_execution_id": "qe_task_1_Loop1",
        "node_id": "wsl2-5080",
        "submission_intent_hash": "a" * 64,
        "owner_id": "worker_1",
        "claim_source": lambda _cursor: {"status": "running"},
        "record_waiting_capacity": lambda _cursor, _active, _capacity: {
            "status": "pending"
        },
    }
    values.update(overrides)
    return QEWorkspaceSubmissionSource(**values)


def test_submission_lease_is_restart_safe() -> None:
    source = _source()

    assert source.lease_seconds == MAX_RESTART_SAFE_LEASE_SECONDS == 45
    with pytest.raises(QEWorkspaceSubmissionCoordinatorError) as excinfo:
        replace(source, lease_seconds=MAX_RESTART_SAFE_LEASE_SECONDS + 1)

    assert excinfo.value.reason_code == "qe_execution_reservation_lease_not_restart_safe"


def test_submission_intent_is_deterministic_and_attempt_scoped() -> None:
    fields = {
        "source_kind": "qe_evolution_loop",
        "source_execution_id": "qe_task_1_Loop1",
        "node_id": "wsl2-5080",
        "task_id": "qe_task_1",
        "loop_id": "Loop1",
    }

    first = submission_intent_hash_for_source(**fields)
    assert first == submission_intent_hash_for_source(**fields)
    assert first != submission_intent_hash_for_source(
        **{**fields, "loop_id": "Loop2"}
    )


@pytest.mark.parametrize("kind,backtest,training,capacity", [
    ("multi_alpha_durable_attempt", True, False, 4),
    ("multi_alpha_durable_attempt", False, False, 1),
    ("qe_experiment", True, False, 1),
    ("qe_evolution_loop", True, False, 4),
    ("qe_evolution_loop", False, True, 2),
])
def test_wsl_submission_uses_execution_mode_not_durable_training_default(
    kind: str, backtest: bool, training: bool, capacity: int,
) -> None:
    repository = Mock(spec=QEExecutionReservationRepository)
    repository.reserve_execution_and_claim_source.return_value = QEExecutionReservationAcquireResult(
        acquired=False, duplicate_replay=False, active_count=capacity,
        node_capacity=capacity, reservation=None, source_claim=None,
    )
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)
    client = Mock()
    result = asyncio.run(coordinator.submit(
        client=client,
        source=_source(source_kind=kind, backtest_only=backtest,
                       parallel_training_eligible=training, requested_node_capacity=4),
        payload=QEWorkspaceSubmissionPayload("remote_task", 1, {}, {}, "qrun --pred-backtest pred.pkl"),
    ))
    assert result.waiting_capacity and result.node_capacity == capacity
    call = repository.reserve_execution_and_claim_source.call_args.kwargs
    assert call["node_capacity"] == capacity
    assert call["allow_durable_prediction_replay_parallelism"] == (
        kind == "multi_alpha_durable_attempt" and backtest
    )
    client.assert_not_called()


@pytest.mark.parametrize("backtest,expected", [(False, 4), (True, 4)])
def test_remote_cpu_fit_and_replay_preserve_four_loop_capacity(backtest, expected):
    repository = Mock(spec=QEExecutionReservationRepository)
    repository.reserve_execution_and_claim_source.return_value = QEExecutionReservationAcquireResult(
        acquired=False, duplicate_replay=False, active_count=expected,
        node_capacity=expected, reservation=None, source_claim=None,
    )
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)
    result = asyncio.run(coordinator.submit(
        client=Mock(),
        source=_source(node_id="rdagent-node1", source_kind="qe_evolution_loop",
                       backtest_only=backtest, requested_node_capacity=4),
        payload=QEWorkspaceSubmissionPayload("remote_task", 1, {}, {}, "qrun"),
    ))
    assert result.waiting_capacity
    assert repository.reserve_execution_and_claim_source.call_args.kwargs["node_capacity"] == expected


@pytest.mark.parametrize("device,capacity", [("cpu", 4), ("gpu", 2)])
def test_wsl_actual_cpu_model_gets_four_but_gpu_override_keeps_two(monkeypatch, device, capacity):
    monkeypatch.setattr(
        "backend.services.quantevolver.config_composer.ConfigComposer._get_model_info",
        lambda self, model_id: {"model_type": "LGBModel"},
    )
    repository = Mock(spec=QEExecutionReservationRepository)
    repository.reserve_execution_and_claim_source.return_value = QEExecutionReservationAcquireResult(
        acquired=False, duplicate_replay=False, active_count=capacity,
        node_capacity=capacity, reservation=None, source_claim=None,
    )
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)
    result = asyncio.run(coordinator.submit(
        client=Mock(),
        source=_source(source_kind="qe_evolution_loop", parallel_training_eligible=True, requested_node_capacity=4),
        payload=QEWorkspaceSubmissionPayload("cpu_task", 1, {"model_id": "lgb", "model_params": {"device_type": device}}, {}, "qrun"),
    ))
    assert result.node_capacity == capacity
    kwargs = repository.reserve_execution_and_claim_source.call_args.kwargs
    assert kwargs["node_capacity"] == capacity and kwargs["allow_same_task_parallel_training"]


def test_wsl_unresolved_model_cannot_raise_capacity(monkeypatch):
    monkeypatch.setattr(
        "backend.services.quantevolver.config_composer.ConfigComposer._get_model_info", lambda self, model_id: None,
    )
    repository = Mock(spec=QEExecutionReservationRepository)
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)
    with pytest.raises(ValueError, match="MODEL_METADATA_REQUIRED"):
        asyncio.run(coordinator.submit(
            client=Mock(), source=_source(source_kind="qe_evolution_loop", parallel_training_eligible=True, requested_node_capacity=4),
            payload=QEWorkspaceSubmissionPayload("cpu_task", 1, {"model_id": "missing"}, {}, "qrun"),
        ))
    repository.reserve_execution_and_claim_source.assert_not_called()


@pytest.mark.parametrize("kind,backtest,capacity", [
    ("multi_alpha_durable_attempt", True, 4),
    ("multi_alpha_durable_attempt", False, 1),
    ("qe_experiment", True, 1),
])
def test_waiting_capacity_probe_preserves_durable_replay_mode(
    kind: str, backtest: bool, capacity: int,
) -> None:
    repository = Mock(spec=QEExecutionReservationRepository)
    repository.observe_execution_capacity.return_value = QEExecutionCapacityObservation(
        available=False, duplicate_replay=False, active_count=4,
        node_capacity=capacity, reservation=None,
    )
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)
    coordinator.observe_capacity(
        node_id="wsl2-5080", requested_node_capacity=4, source_kind=kind,
        source_execution_id="attempt1", qe_task_id="remote_task", qe_loop_id="Loop1",
        submission_intent_hash="a" * 64, backtest_only=backtest,
    )
    assert repository.observe_execution_capacity.call_args.kwargs["node_capacity"] == capacity


def test_durable_adapter_capacity_observation_marks_prediction_replay(tmp_path: Any) -> None:
    coordinator = Mock(spec=QEWorkspaceSubmissionCoordinator)
    adapter = QEWorkspacePredBacktestAdapter(
        repository=Mock(), model_store=Mock(), panel_builder=Mock(),
        workspace_root=tmp_path, submission_coordinator=coordinator,
    )
    intent = DurableSubmissionIntent("parent", "child", "attempt", 1, "wsl2-5080",
                                     "remote_task", "Loop1", "a" * 64)
    adapter.observe_submission_capacity(run={"node_parallelism_json": {"wsl2-5080": 4}},
                                        intent=intent)
    arguments = coordinator.observe_capacity.call_args.kwargs
    assert arguments["backtest_only"] is True
    assert arguments["requested_node_capacity"] == 4
    assert arguments["source_execution_id"] == intent.attempt_id
