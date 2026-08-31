from __future__ import annotations

import ast
import json
import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.services.quantevolver.experiment_config import LongTrendEvaluationOptIn
from backend.services.quantevolver.config_composer import ConfigComposer
from backend.services.quantevolver import long_trend_evaluation_phase2 as phase2_module
from backend.services.quantevolver.long_trend_evaluation_phase2 import (
    QELongTrendControlSecretStore,
    QELongTrendPhase2Error,
    QELongTrendPhase2Service,
    ResolvedLongTrendEvaluationRequest,
    _long_trend_snapshot,
    _merge_registration_catalog,
    _evaluation_parent_identity,
    _recorder_catalog_digest,
)
from backend.services.quantevolver.long_trend_artifact_resolver import RecorderArtifactInventory
from backend.services.quantevolver.long_trend_evaluation_bundle import QELongTrendEvaluatorBundle
from backend.services.quantevolver.long_trend_evaluation_contract import (
    QEDatasetSnapshotIdentity,
    QELongTrendReason,
)
from backend.services.quantevolver.long_trend_evaluation_control_repository import QELongTrendControlLease
from backend.services.quantevolver.qe_resource_phase_service import (
    PHASE_INVALID_REASON,
    QEResourcePhaseError,
    validate_phase_transition,
)
from backend.services.quantevolver.qe_workspace_client import (
    QELongTrendJobInspection,
    QELongTrendWorkspaceError,
    QEWorkspaceClient,
    QEWorkspaceDatasetIdentity,
)
from backend.services.quantevolver.long_trend_pickle_parser_entry import ParserContractError, _reject_secrets
from backend.services.quantevolver.templates import long_trend_postprocess_adapter as postprocess_adapter
from backend.services.qe_archive.long_trend_repository import (
    PersistedEvaluationReceipt,
    QELongTrendResultRepositoryError,
    QELongTrendResultSchemaNotReady,
)


def test_long_trend_opt_in_is_explicit_strict_and_qe_only() -> None:
    value = LongTrendEvaluationOptIn(
        feature_data_root_uri="/home/qe/factor_data",
        outcome_data_root_uri="/home/qe/factor_data",
    )
    assert value.mode == "normal_postprocess"
    assert value.enabled is True
    with pytest.raises(ValidationError):
        LongTrendEvaluationOptIn.model_validate(
            {
                "feature_data_root_uri": "/home/qe/factor_data",
                "outcome_data_root_uri": "/home/qe/factor_data",
                "unapproved_gate": True,
            }
        )
    with pytest.raises(ValidationError):
        LongTrendEvaluationOptIn(
            feature_data_root_uri="/home/qe/factor_data",
            outcome_data_root_uri="/home/qe/factor_data",
            mode="paper_trading",
        )


def test_registration_success_retains_pending_receipt_when_index_cleanup_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    descriptor = {
        "schema_version": postprocess_adapter.DESCRIPTOR_SCHEMA,
        "task_id": "task-1",
        "loop_index": 1,
        "node_id": "node-1",
        "backtest_freq": "1day",
        "long_trend_evaluation": {"enabled": True},
        "frozen_identity": {},
    }
    (tmp_path / "qe_long_trend_postprocess_descriptor.json").write_text(
        json.dumps(descriptor),
        encoding="utf-8",
    )
    (tmp_path / "qe_current_recorder.json").write_text(
        json.dumps({"experiment_id": "exp-1", "recorder_id": "rec-1"}),
        encoding="utf-8",
    )
    (tmp_path / "qe_resource_session_secret.json").write_text(
        json.dumps({"token": "secret", "session_id": "session-1", "source_run_key": "source-1"}),
        encoding="utf-8",
    )
    pending_path = tmp_path / "postprocess_registration_pending.json"
    pending_path.write_text('{"status":"pending"}', encoding="utf-8")

    class Response:
        status = 200

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args):  # type: ignore[no-untyped-def]
            return False

        @staticmethod
        def read() -> bytes:
            return json.dumps(
                {
                    "data": {
                        "schema_version": postprocess_adapter.REGISTRATION_SCHEMA,
                        "evaluation_id": "qelt_" + "a" * 64,
                        "task_status": "queued",
                    },
                },
            ).encode("utf-8")

    monkeypatch.setattr(postprocess_adapter, "_build_registration_catalog", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(postprocess_adapter, "_fsync_directory", lambda _path: None)
    monkeypatch.setattr(postprocess_adapter, "_registration_url", lambda: "http://aistock.invalid/register")
    monkeypatch.setattr(postprocess_adapter.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())
    monkeypatch.setattr(
        postprocess_adapter,
        "_clear_pending_index",
        lambda _descriptor: (_ for _ in ()).throw(PermissionError("index busy")),
    )

    assert postprocess_adapter.main() == 0
    assert (tmp_path / "qe_long_trend_registration.json").is_file()
    assert pending_path.is_file()


def test_read_exp_res_marks_malformed_pending_receipt_invalid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_path = Path(__file__).resolve().parents[2] / "services" / "quantevolver" / "templates" / "read_exp_res.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_load_long_trend_registration_observation"
    )
    module = ast.Module(body=[function], type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {"Path": Path, "json": json}
    exec(compile(module, str(source_path), "exec"), namespace)
    load_observation = namespace["_load_long_trend_registration_observation"]

    monkeypatch.chdir(tmp_path)
    (tmp_path / "postprocess_registration_pending.json").write_text(
        json.dumps({"unexpected": True}),
        encoding="utf-8",
    )

    result = load_observation()
    assert result["status"] == "invalid"
    assert result["reason_code"] == "QELT_REGISTRATION_PENDING_INVALID"


def test_snapshot_evidence_remains_usable_when_legacy_manifest_is_incomplete() -> None:
    identity = QEWorkspaceDatasetIdentity(
        schema_version="qe_dataset_identity_evidence_v1",
        complete=False,
        reason_code="qe_dataset_manifest_missing",
        missing=("qe_dataset_manifest.json",),
        acquisition_suggestions=("publish legacy manifest",),
        dataset=None,
        long_trend_snapshot={
            "snapshot_id": "qlib-st-pit-active-h5-daily-20180801-20260630",
            "manifest_sha256": "a" * 64,
            "start_date": "2018-08-01",
            "end_date": "2026-06-30",
            "lineage_parent_ids": [],
            "files": {},
        },
        long_trend_snapshot_reason=None,
        detail=None,
    )
    snapshot, action = _long_trend_snapshot(identity, family="feature")
    assert snapshot is not None
    assert snapshot.end_date == "2026-06-30"
    assert action is None


def test_known_missing_snapshot_creates_partial_control_before_node_or_recorder_access(tmp_path: Path) -> None:
    class Repository:
        def __init__(self) -> None:
            self.row: dict[str, object] = {}

        def create_or_get_queued(self, spec, *, qelt_resource):  # type: ignore[no-untyped-def]
            self.row = {
                **spec.__dict__,
                "status": "queued",
                "owner_id": None,
                "fencing_token": 0,
                "row_version": 1,
                "job_id": None,
                "reason_code": None,
                "qelt_resource": qelt_resource,
            }
            return dict(self.row)

        def claim(self, _evaluation_id, **_kwargs):  # type: ignore[no-untyped-def]
            self.row.update({"owner_id": "owner", "fencing_token": 1, "row_version": 2})
            return dict(self.row)

        @staticmethod
        def lease_from(row):  # type: ignore[no-untyped-def]
            return QELongTrendControlLease(row["evaluation_id"], "owner", 1, row["row_version"])

        def transition(self, _lease, *, updates, **_kwargs):  # type: ignore[no-untyped-def]
            self.row.update(updates)
            self.row.update({"owner_id": None, "row_version": 3})
            return dict(self.row)

    class ResourceService:
        def __init__(self) -> None:
            self.events: list[dict[str, object]] = []

        def ingest_event(self, *, token, payload):  # type: ignore[no-untyped-def]
            assert token
            self.events.append(dict(payload))

    class Client:
        @staticmethod
        async def get_execution_environment():
            raise AssertionError("known missing snapshots must not depend on node environment")

    missing = QEWorkspaceDatasetIdentity(
        schema_version="qe_dataset_identity_evidence_v1",
        complete=False,
        reason_code="QELT_REQUESTED_OUTCOME_SNAPSHOT_UNAVAILABLE",
        missing=("snapshot",),
        acquisition_suggestions=("register_snapshot",),
        dataset=None,
        long_trend_snapshot=None,
        long_trend_snapshot_reason="QELT_REQUESTED_OUTCOME_SNAPSHOT_UNAVAILABLE",
    )
    repository = Repository()
    resources = ResourceService()
    service = QELongTrendPhase2Service(
        control_repository=repository,  # type: ignore[arg-type]
        resource_service=resources,  # type: ignore[arg-type]
        secret_store=QELongTrendControlSecretStore(tmp_path / "secrets"),
        owner_id="owner",
    )

    prepared = asyncio.run(
        service.prepare_long_trend_only_resolved(
            run_id="run-1",
            task_id="task-1",
            loop_index=1,
            node_id="wsl",
            resolved_request=ResolvedLongTrendEvaluationRequest(
                profile_id="qe_long_trend_v1",
                evaluator_version="f014_v1",
                feature_data_root_uri="",
                outcome_data_root_uri=None,
                backtest_freq="1day",
                requested_outcome_snapshot_id="missing-snapshot",
                feature_identity=missing,
                outcome_identity=missing,
            ),
            registration_catalog={},
            label_horizon=60,
            strategy_topk=25,
            client=Client(),  # type: ignore[arg-type]
        )
    )

    assert prepared.ready_for_node is False
    assert prepared.control_row["status"] == "partial"
    assert prepared.control_row["reason_code"] == "QELT_DATASET_IDENTITY_INCOMPLETE"
    assert prepared.control_row["request_json"] == {"platform_status": "dataset_identity_incomplete"}
    assert "data_action_plan" not in prepared.control_row["request_json"]
    assert [event["phase_status"] for event in resources.events] == ["not_submitted", "partial"]


def test_registration_catalog_must_match_live_size_and_hash() -> None:
    path = "mlruns/exp/rec/artifacts/pred.pkl"
    live = {
        "schema_version": "qe_workspace_catalog_v1",
        "task_id": "task",
        "loop_name": "Loop1",
        "catalog_completeness": "complete",
        "files": [{"relative_path": path, "size_bytes": 5}],
    }
    registered = {
        "schema_version": "qe_long_trend_registration_catalog_v1",
        "files": [{"relative_path": path, "size_bytes": 5, "sha256": "b" * 64}],
    }
    merged = _merge_registration_catalog(live, registered)
    assert merged["files"][0]["sha256"] == "b" * 64

    with pytest.raises(Exception) as exc_info:
        _merge_registration_catalog(
            live,
            {**registered, "files": [{"relative_path": path, "size_bytes": 6, "sha256": "b" * 64}]},
        )
    assert getattr(exc_info.value, "reason_code", None) == "QELT_ARTIFACT_HASH_MISMATCH"


def test_qelt_resource_phase_is_cpu_only_and_requires_evaluation_identity() -> None:
    validate_phase_transition(
        "created",
        {
            "phase": "long_trend_eval",
            "phase_status": "running",
            "metadata": {"evaluation_id": "qelt_" + "a" * 64},
        },
    )
    with pytest.raises(QEResourcePhaseError) as exc_info:
        validate_phase_transition(
            "created",
            {
                "phase": "long_trend_eval",
                "phase_status": "running",
                "metadata": {"evaluation_id": "qelt_" + "a" * 64},
                "gpu_utilization_peak_pct": 1.0,
            },
        )
    assert exc_info.value.reason_code == PHASE_INVALID_REASON


def test_control_secret_store_is_idempotent_and_never_embeds_identity_drift(tmp_path: Path) -> None:
    store = QELongTrendControlSecretStore(tmp_path / "secrets")
    evaluation_id = "qelt_" + "a" * 64
    first, created = store.load_or_create(
        evaluation_id,
        session_id="qers-qelt-1",
        source_run_key=f"qelt:{evaluation_id}",
    )
    second, replay_created = store.load_or_create(
        evaluation_id,
        session_id="qers-qelt-1",
        source_run_key=f"qelt:{evaluation_id}",
    )
    assert created is True
    assert replay_created is False
    assert second == first
    persisted = json.loads((tmp_path / "secrets" / f"{evaluation_id}.json").read_text(encoding="utf-8"))
    assert persisted["token"] == first

    with pytest.raises(QELongTrendPhase2Error):
        store.load_or_create(
            evaluation_id,
            session_id="different",
            source_run_key=f"qelt:{evaluation_id}",
        )

    (tmp_path / "secrets" / f"{evaluation_id}.json").write_text("{", encoding="utf-8")
    with pytest.raises(QELongTrendPhase2Error, match="secret is malformed") as exc_info:
        store.load(
            evaluation_id,
            session_id="qers-qelt-1",
            source_run_key=f"qelt:{evaluation_id}",
        )
    assert exc_info.value.reason_code == QELongTrendReason.CONTROL_STATE_CONFLICT.value


def test_control_secret_store_concurrent_creation_publishes_one_token(tmp_path: Path) -> None:
    store = QELongTrendControlSecretStore(tmp_path / "secrets")
    evaluation_id = "qelt_" + "c" * 64

    def create() -> tuple[str, bool]:
        return store.load_or_create(
            evaluation_id,
            session_id="qers-qelt-concurrent",
            source_run_key=f"qelt:{evaluation_id}",
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _index: create(), range(16)))

    assert len({token for token, _created in results}) == 1
    assert sum(1 for _token, created in results if created) == 1
    assert store.load(
        evaluation_id,
        session_id="qers-qelt-concurrent",
        source_run_key=f"qelt:{evaluation_id}",
    ) == results[0][0]


def test_artifact_stream_uses_bounded_client_timeout_and_cleans_partial(tmp_path: Path) -> None:
    payload = b"long-trend-artifact"
    digest = hashlib.sha256(payload).hexdigest()
    calls: list[dict[str, object]] = []

    class Response:
        @staticmethod
        def raise_for_status() -> None:
            return None

        async def aiter_bytes(self, *, chunk_size: int):  # type: ignore[no-untyped-def]
            assert chunk_size == 1024 * 1024
            yield payload

    class Stream:
        async def __aenter__(self):  # type: ignore[no-untyped-def]
            return Response()

        async def __aexit__(self, *_args):  # type: ignore[no-untyped-def]
            return False

    class HttpClient:
        def stream(self, method, url, **kwargs):  # type: ignore[no-untyped-def]
            calls.append({"method": method, "url": url, **kwargs})
            return Stream()

    client = QEWorkspaceClient("http://node/api/v1/qe_workspace")
    original = client.client
    client.client = HttpClient()  # type: ignore[assignment]
    try:
        result = asyncio.run(
            client.stream_long_trend_artifact(
                task_id="task-1",
                loop_id="Loop1",
                evaluation_id="qelt_" + "d" * 64,
                artifact_path="attempts/attempt-1/artifacts/worker_terminal_receipt.json",
                destination=tmp_path / "terminal.json",
                expected_sha256=digest,
                expected_size_bytes=len(payload),
            )
        )
    finally:
        asyncio.run(original.aclose())
    assert result["sha256"] == digest
    assert calls and "timeout" not in calls[0]
    assert not (tmp_path / "terminal.json.partial").exists()


def test_collection_uses_zero_periodic_database_lease_heartbeats() -> None:
    renewals: list[tuple[QELongTrendControlLease, int]] = []

    class Repository:
        def renew_lease(self, lease, *, lease_seconds):  # type: ignore[no-untyped-def]
            renewals.append((lease, lease_seconds))

    service = QELongTrendPhase2Service(control_repository=Repository(), owner_id="owner")  # type: ignore[arg-type]
    lease = QELongTrendControlLease(
        evaluation_id="qelt_" + "e" * 64,
        owner_id="owner",
        fencing_token=3,
        row_version=7,
    )
    async def work() -> str:
        await asyncio.sleep(0.02)
        return "published"

    assert asyncio.run(service._await_with_fenced_lease(lease=lease, awaitable=work())) == "published"
    assert renewals == []


def test_collection_cancellation_propagates_without_detached_heartbeat_task() -> None:
    cancelled = False

    class Repository:
        @staticmethod
        def renew_lease(_lease, *, lease_seconds):  # type: ignore[no-untyped-def]
            pytest.fail("periodic database lease renewal is forbidden")

    service = QELongTrendPhase2Service(control_repository=Repository(), owner_id="owner")  # type: ignore[arg-type]
    lease = QELongTrendControlLease(
        evaluation_id="qelt_" + "9" * 64,
        owner_id="owner",
        fencing_token=8,
        row_version=13,
    )
    async def work() -> None:
        nonlocal cancelled
        try:
            await asyncio.Event().wait()
        finally:
            cancelled = True

    async def scenario() -> None:
        task = asyncio.create_task(
            service._await_with_fenced_lease(lease=lease, awaitable=work())
        )
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert cancelled is True


def test_reconcile_persists_running_state_and_releases_claim() -> None:
    evaluation_id = "qelt_" + "f" * 64

    class Repository:
        def __init__(self) -> None:
            self.transitions: list[dict[str, object]] = []

        @staticmethod
        def bind_available_archive_run(_evaluation_id):  # type: ignore[no-untyped-def]
            return {
                "evaluation_id": evaluation_id,
                "parent_task_id": "task-1",
                "parent_loop_index": 2,
                "node_id": "node-1",
                "job_id": "job-1",
                "status": "submitted",
            }

        @staticmethod
        def claim(_evaluation_id, *, owner_id, lease_seconds):  # type: ignore[no-untyped-def]
            assert owner_id == "owner"
            assert lease_seconds == phase2_module.COLLECT_LEASE_SECONDS
            return {
                "evaluation_id": evaluation_id,
                "parent_task_id": "task-1",
                "parent_loop_index": 2,
                "node_id": "node-1",
                "job_id": "job-1",
                "status": "submitted",
                "owner_id": "owner",
                "fencing_token": 4,
                "row_version": 9,
            }

        @staticmethod
        def lease_from(row):  # type: ignore[no-untyped-def]
            return QELongTrendControlLease(
                evaluation_id=row["evaluation_id"],
                owner_id=row["owner_id"],
                fencing_token=row["fencing_token"],
                row_version=row["row_version"],
            )

        def transition(self, _lease, **kwargs):  # type: ignore[no-untyped-def]
            self.transitions.append(kwargs)
            return {"row_version": 10, **kwargs["updates"]}

    class Client:
        @staticmethod
        async def inspect_long_trend_evaluation(**_kwargs):  # type: ignore[no-untyped-def]
            return QELongTrendJobInspection(
                schema_version="qe_long_trend_job_receipt_v1",
                task_id="task-1",
                loop_id="Loop2",
                evaluation_id=evaluation_id,
                job_id="job-1",
                request_sha="a" * 64,
                status="running",
                current_attempt_id="attempt-1",
                process_identity={"pid": 123},
                terminal_receipt=None,
                updated_at="2026-07-22T00:00:00Z",
            )

    repository = Repository()
    service = QELongTrendPhase2Service(control_repository=repository, owner_id="owner")  # type: ignore[arg-type]
    result = asyncio.run(service.reconcile(row={"evaluation_id": evaluation_id}, client=Client()))  # type: ignore[arg-type]

    assert result["status"] == "running"
    assert repository.transitions[-1]["updates"]["status"] == "running"  # type: ignore[index]
    assert repository.transitions[-1]["release_owner"] is True


def test_prepare_requeues_only_definitive_bundle_rejection_without_remote_job() -> None:
    evaluation_id = "qelt_" + "d" * 64

    class Repository:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def requeue_definitive_prejob_failure(self, value, **kwargs):  # type: ignore[no-untyped-def]
            self.calls.append({"evaluation_id": value, **kwargs})
            return {"evaluation_id": value, "status": "queued", "reason_code": None}

    repository = Repository()
    service = QELongTrendPhase2Service(control_repository=repository, owner_id="owner")  # type: ignore[arg-type]
    recovered = service._recover_definitive_prejob_failure(
        {
            "evaluation_id": evaluation_id,
            "status": "failed",
            "job_id": None,
            "reason_code": "QELT_BUNDLE_INVALID",
        },
        evaluation_id=evaluation_id,
        request_sha="a" * 64,
    )

    assert recovered["status"] == "queued"
    assert repository.calls == [
        {
            "evaluation_id": evaluation_id,
            "expected_request_sha": "a" * 64,
            "allowed_reason_codes": phase2_module.RETRYABLE_PREJOB_REJECTION_REASONS,
        }
    ]

    for row in (
        {"status": "failed", "job_id": "job-1", "reason_code": "QELT_BUNDLE_INVALID"},
        {"status": "failed", "job_id": None, "reason_code": "QELT_NODE_JOB_IDENTITY_CONFLICT"},
        {"status": "remote_state_unknown", "job_id": None, "reason_code": "QELT_NODE_STATE_UNKNOWN"},
    ):
        unchanged = service._recover_definitive_prejob_failure(
            row,
            evaluation_id=evaluation_id,
            request_sha="a" * 64,
        )
        assert unchanged == row
    assert len(repository.calls) == 1


def test_submit_reports_terminal_no_job_state_instead_of_failing_repository_claim() -> None:
    evaluation_id = "qelt_" + "e" * 64
    prepared = phase2_module.PreparedLongTrendEvaluation(
        evaluation_id=evaluation_id,
        control_row={
            "evaluation_id": evaluation_id,
            "status": "failed",
            "job_id": None,
            "reason_code": "QELT_NODE_JOB_IDENTITY_CONFLICT",
        },
        request_payload={"schema_version": "qe_long_trend_job_request_v1"},
        resource_token="secret",
        ready_for_node=True,
        data_action_plan=(),
    )
    service = QELongTrendPhase2Service(owner_id="owner")

    with pytest.raises(QELongTrendPhase2Error) as exc_info:
        asyncio.run(
            service.submit(
                prepared=prepared,
                task_id="task-1",
                loop_index=1,
                client=object(),  # type: ignore[arg-type]
            )
        )

    assert exc_info.value.reason_code == "QELT_CONTROL_STATE_CONFLICT"
    assert exc_info.value.context == {
        "evaluation_id": evaluation_id,
        "status": "failed",
        "reason_code": "QELT_NODE_JOB_IDENTITY_CONFLICT",
    }


@pytest.mark.parametrize(
    ("reason_code", "expected_status", "expected_delivery", "resource_terminal"),
    [
        (
            "QELT_BUNDLE_INVALID",
            "failed",
            {"worker": "rejected", "cas": "not_started"},
            True,
        ),
        (
            "QELT_NODE_STATE_UNKNOWN",
            "remote_state_unknown",
            {"worker": "remote_state_unknown", "cas": "awaiting_worker"},
            False,
        ),
    ],
)
def test_submit_persists_truthful_delivery_state_for_prejob_failure(
    reason_code: str,
    expected_status: str,
    expected_delivery: dict[str, str],
    resource_terminal: bool,
) -> None:
    evaluation_id = "qelt_" + "c" * 64

    class Repository:
        def __init__(self) -> None:
            self.row = {
                "evaluation_id": evaluation_id,
                "status": "queued",
                "request_sha": "a" * 64,
                "resource_session_id": "qers-qelt-1",
                "owner_id": None,
                "fencing_token": 2,
                "row_version": 4,
            }
            self.transitions: list[dict[str, object]] = []

        def claim(self, _evaluation_id, *, owner_id):  # type: ignore[no-untyped-def]
            self.row.update({"owner_id": owner_id, "fencing_token": 3, "row_version": 5})
            return dict(self.row)

        @staticmethod
        def lease_from(row):  # type: ignore[no-untyped-def]
            return QELongTrendControlLease(
                evaluation_id=row["evaluation_id"],
                owner_id=row["owner_id"],
                fencing_token=row["fencing_token"],
                row_version=row["row_version"],
            )

        def transition(self, _lease, **kwargs):  # type: ignore[no-untyped-def]
            self.transitions.append(kwargs)
            self.row.update(kwargs["updates"])
            self.row["row_version"] = int(self.row["row_version"]) + 1
            if kwargs.get("release_owner"):
                self.row["owner_id"] = None
            return dict(self.row)

    class ResourceService:
        def __init__(self) -> None:
            self.terminals: list[dict[str, object]] = []

        def mark_session_terminal(self, session_id, **kwargs):  # type: ignore[no-untyped-def]
            self.terminals.append({"session_id": session_id, **kwargs})

    class Client:
        @staticmethod
        async def submit_long_trend_evaluation(**_kwargs):  # type: ignore[no-untyped-def]
            raise QELongTrendWorkspaceError(
                "node rejected request",
                reason_code=reason_code,
                context={"status_code": 409},
            )

    repository = Repository()
    resource_service = ResourceService()
    service = QELongTrendPhase2Service(
        control_repository=repository,  # type: ignore[arg-type]
        resource_service=resource_service,  # type: ignore[arg-type]
        owner_id="owner",
    )
    prepared = phase2_module.PreparedLongTrendEvaluation(
        evaluation_id=evaluation_id,
        control_row=dict(repository.row),
        request_payload={"schema_version": "qe_long_trend_job_request_v1"},
        resource_token="secret",
        ready_for_node=True,
        data_action_plan=(),
    )

    with pytest.raises(QELongTrendWorkspaceError, match="node rejected request"):
        asyncio.run(
            service.submit(
                prepared=prepared,
                task_id="task-1",
                loop_index=1,
                client=Client(),  # type: ignore[arg-type]
            )
        )

    failure = repository.transitions[-1]
    assert failure["updates"]["status"] == expected_status  # type: ignore[index]
    assert failure["updates"]["platform_delivery_status_json"] == expected_delivery  # type: ignore[index]
    assert bool(resource_service.terminals) is resource_terminal


def test_submit_persists_identity_conflict_after_remote_receipt() -> None:
    evaluation_id = "qelt_" + "b" * 64

    class Repository:
        def __init__(self) -> None:
            self.row = {
                "evaluation_id": evaluation_id,
                "status": "queued",
                "request_sha": "a" * 64,
                "resource_session_id": "qers-qelt-1",
                "owner_id": None,
                "fencing_token": 2,
                "row_version": 4,
            }
            self.transitions: list[dict[str, object]] = []

        def claim(self, _evaluation_id, *, owner_id):  # type: ignore[no-untyped-def]
            self.row.update({"owner_id": owner_id, "fencing_token": 3, "row_version": 5})
            return dict(self.row)

        @staticmethod
        def lease_from(row):  # type: ignore[no-untyped-def]
            return QELongTrendControlLease(
                evaluation_id=row["evaluation_id"],
                owner_id=row["owner_id"],
                fencing_token=row["fencing_token"],
                row_version=row["row_version"],
            )

        def transition(self, _lease, **kwargs):  # type: ignore[no-untyped-def]
            self.transitions.append(kwargs)
            self.row.update(kwargs["updates"])
            self.row["row_version"] = int(self.row["row_version"]) + 1
            if kwargs.get("release_owner"):
                self.row["owner_id"] = None
            return dict(self.row)

    class ResourceService:
        def __init__(self) -> None:
            self.terminals: list[dict[str, object]] = []

        def mark_session_terminal(self, session_id, **kwargs):  # type: ignore[no-untyped-def]
            self.terminals.append({"session_id": session_id, **kwargs})

    class Client:
        @staticmethod
        async def submit_long_trend_evaluation(**_kwargs):  # type: ignore[no-untyped-def]
            return phase2_module.QELongTrendJobReceipt(
                schema_version="qe_long_trend_job_receipt_v1",
                task_id="task-1",
                loop_id="Loop1",
                evaluation_id=evaluation_id,
                job_id="job-1",
                request_sha="f" * 64,
                status="queued",
                duplicate_replay=False,
                current_attempt_id="attempt-1",
                execution_environment_snapshot_id="env-1",
                execution_environment_manifest_sha256="e" * 64,
            )

    repository = Repository()
    resource_service = ResourceService()
    service = QELongTrendPhase2Service(
        control_repository=repository,  # type: ignore[arg-type]
        resource_service=resource_service,  # type: ignore[arg-type]
        owner_id="owner",
    )
    prepared = phase2_module.PreparedLongTrendEvaluation(
        evaluation_id=evaluation_id,
        control_row=dict(repository.row),
        request_payload={"schema_version": "qe_long_trend_job_request_v1"},
        resource_token="secret",
        ready_for_node=True,
        data_action_plan=(),
    )

    with pytest.raises(QELongTrendPhase2Error) as exc_info:
        asyncio.run(
            service.submit(
                prepared=prepared,
                task_id="task-1",
                loop_index=1,
                client=Client(),  # type: ignore[arg-type]
            )
        )

    assert exc_info.value.reason_code == "QELT_NODE_JOB_IDENTITY_CONFLICT"
    failure = repository.transitions[-1]
    assert failure["updates"]["status"] == "failed"  # type: ignore[index]
    assert failure["updates"]["platform_delivery_status_json"] == {  # type: ignore[index]
        "worker": "identity_conflict",
        "cas": "not_started",
    }
    assert resource_service.terminals == [
        {
            "session_id": "qers-qelt-1",
            "status": "failed",
            "reason_code": "QELT_NODE_JOB_IDENTITY_CONFLICT",
        }
    ]


def test_backend_wires_continuous_long_trend_reconciliation() -> None:
    source = (Path(__file__).resolve().parents[3] / "backend/main.py").read_text(encoding="utf-8")
    assert "run_qe_reconciliation_coordinator(shutdown_event)" in source
    assert 'name="qe-reconciliation-coordinator"' in source
    assert "QELongTrendPhase2Service().reconcile_nonterminal" not in source
    assert 'name="qe-long-trend-reconciler"' not in source


def test_reconcile_node_resolution_failure_is_persisted_for_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evaluation_id = "qelt_" + "1" * 64

    class Repository:
        def __init__(self) -> None:
            self.row = {
                "evaluation_id": evaluation_id,
                "parent_task_id": "task-1",
                "parent_loop_index": 1,
                "node_id": "missing-node",
                "job_id": "job-1",
                "status": "submitted",
                "owner_id": None,
                "fencing_token": 2,
                "row_version": 5,
            }
            self.transitions: list[dict[str, object]] = []

        def list_nonterminal(self, *, limit):  # type: ignore[no-untyped-def]
            assert limit == 100
            return [dict(self.row)]

        def bind_available_archive_run(self, _evaluation_id):  # type: ignore[no-untyped-def]
            return dict(self.row)

        def claim(
            self,
            _evaluation_id,
            *,
            owner_id,
            lease_seconds,
            expected_row_version=None,
        ):  # type: ignore[no-untyped-def]
            assert expected_row_version == 5
            self.row.update(
                {
                    "owner_id": owner_id,
                    "fencing_token": 3,
                    "row_version": 6,
                }
            )
            assert lease_seconds == phase2_module.COLLECT_LEASE_SECONDS
            return dict(self.row)

        def get(self, _evaluation_id):  # type: ignore[no-untyped-def]
            return dict(self.row)

        @staticmethod
        def lease_from(row):  # type: ignore[no-untyped-def]
            return QELongTrendControlLease(
                evaluation_id=row["evaluation_id"],
                owner_id=row["owner_id"],
                fencing_token=row["fencing_token"],
                row_version=row["row_version"],
            )

        def transition(self, _lease, **kwargs):  # type: ignore[no-untyped-def]
            self.transitions.append(kwargs)
            self.row.update(kwargs["updates"])
            if kwargs.get("release_owner"):
                self.row["owner_id"] = None
            self.row["row_version"] = int(self.row["row_version"]) + 1
            return dict(self.row)

    def fail_node_resolution(_cls, _node_id):  # type: ignore[no-untyped-def]
        raise RuntimeError("node catalog unavailable")

    repository = Repository()
    monkeypatch.setattr(QEWorkspaceClient, "for_node", classmethod(fail_node_resolution))
    service = QELongTrendPhase2Service(control_repository=repository, owner_id="owner")  # type: ignore[arg-type]

    results = asyncio.run(service.reconcile_nonterminal(limit=100))

    assert results[0]["status"] == "platform_error"
    assert results[0]["recovery_persisted"] is True
    assert repository.transitions[-1]["updates"]["status"] == "remote_state_unknown"  # type: ignore[index]
    assert repository.transitions[-1]["release_owner"] is True


def test_reconcile_unchanged_running_observation_100x_has_zero_claims_and_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evaluation_id = "qelt_" + "2" * 64
    row = {
        "evaluation_id": evaluation_id,
        "parent_task_id": "task-1",
        "parent_loop_index": 1,
        "node_id": "node-1",
        "job_id": "job-1",
        "status": "running",
        "current_attempt_id": "attempt-1",
        "platform_delivery_status_json": {
            "worker": "running",
            "cas": "awaiting_worker",
        },
        "reason_code": None,
        "reason_json": {},
        "owner_id": None,
        "fencing_token": 2,
        "row_version": 5,
    }

    class Repository:
        def __init__(self) -> None:
            self.claims = 0
            self.transitions = 0

        def list_nonterminal(self, *, limit):  # type: ignore[no-untyped-def]
            assert limit == 100
            return [dict(row)]

        def claim(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
            self.claims += 1
            pytest.fail("unchanged remote state must not claim a control lease")

        def transition(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
            self.transitions += 1
            pytest.fail("unchanged remote state must not write")

    class Client:
        inspections = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def inspect_long_trend_evaluation(self, **_kwargs):  # type: ignore[no-untyped-def]
            self.inspections += 1
            return QELongTrendJobInspection(
                schema_version="qe_long_trend_job_receipt_v1",
                task_id="task-1",
                loop_id="Loop1",
                evaluation_id=evaluation_id,
                job_id="job-1",
                request_sha="a" * 64,
                status="running",
                current_attempt_id="attempt-1",
                process_identity={"pid": 123},
                terminal_receipt=None,
                updated_at="2026-08-11T00:00:00Z",
            )

    repository = Repository()
    client = Client()
    monkeypatch.setattr(
        QEWorkspaceClient,
        "for_node",
        classmethod(lambda _cls, _node_id: client),
    )
    service = QELongTrendPhase2Service(
        control_repository=repository,  # type: ignore[arg-type]
        owner_id="owner",
    )

    for _ in range(100):
        result = asyncio.run(service.reconcile_nonterminal(limit=100))
        assert result == [
            {
                "evaluation_id": evaluation_id,
                "status": "running",
                "unchanged": True,
            }
        ]

    assert client.inspections == 100
    assert repository.claims == 0
    assert repository.transitions == 0


def test_normal_loop_command_orders_qrun_registration_and_read_result() -> None:
    parent_token = "parent-" + "secret"
    _env, parts = ConfigComposer()._build_auto_wsl_command_parts(
        "/home/qe/workspace/task/Loop1",
        backtest_freq="day",
        factor_cache_dir="/home/qe/factor_cache/factor_values",
        factor_data_dir="/home/qe/factor_data",
        node_id="rdagent-node1",
        task_id="task-1",
        loop_index=1,
        resource_session_id="qers-parent",
        resource_source_run_key="task-1_L1",
        resource_session_token=parent_token,
        long_trend_postprocess_enabled=True,
    )
    qrun_index = parts.index("python qrun_limit.py conf.yaml")
    adapter_index = parts.index("python long_trend_postprocess_adapter.py")
    read_index = parts.index("QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py")
    assert qrun_index < adapter_index < read_index
    assert not any("nvidia" in part.lower() or "nvml" in part.lower() for part in parts)


def test_collect_failure_is_persisted_as_recoverable_platform_state(monkeypatch: pytest.MonkeyPatch) -> None:
    class Repository:
        def __init__(self) -> None:
            self.transitions = []

        def claim(self, evaluation_id, **_kwargs):  # type: ignore[no-untyped-def]
            return {
                "evaluation_id": evaluation_id,
                "owner_id": "owner",
                "fencing_token": 1,
                "row_version": 1,
                "status": "submitted",
            }

        @staticmethod
        def lease_from(row):  # type: ignore[no-untyped-def]
            return QELongTrendControlLease(
                evaluation_id=row["evaluation_id"],
                owner_id="owner",
                fencing_token=1,
                row_version=row["row_version"],
            )

        def transition(self, lease, **kwargs):  # type: ignore[no-untyped-def]
            self.transitions.append((lease, kwargs))
            return {
                "evaluation_id": lease.evaluation_id,
                "owner_id": "owner",
                "fencing_token": 1,
                "row_version": lease.row_version + 1,
                "status": kwargs["updates"]["status"],
            }

    class Client:
        async def inspect_long_trend_evaluation(self, **_kwargs):  # type: ignore[no-untyped-def]
            return QELongTrendJobInspection(
                schema_version="qe_long_trend_job_receipt_v1",
                task_id="task-1",
                loop_id="Loop1",
                evaluation_id="qelt_" + "a" * 64,
                job_id="job-1",
                request_sha="b" * 64,
                status="succeeded",
                current_attempt_id="attempt-1",
                process_identity=None,
                terminal_receipt=None,
                updated_at="2026-07-22T00:00:00Z",
            )

    repository = Repository()
    service = QELongTrendPhase2Service(control_repository=repository, owner_id="owner")  # type: ignore[arg-type]

    async def fail_publish(**_kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("stream interrupted")

    monkeypatch.setattr(service, "_publish_remote_artifacts", fail_publish)
    with pytest.raises(RuntimeError, match="stream interrupted"):
        asyncio.run(
            service.collect_and_publish(
                evaluation_id="qelt_" + "a" * 64,
                task_id="task-1",
                loop_index=1,
                client=Client(),  # type: ignore[arg-type]
            )
        )
    recovery = repository.transitions[-1][1]
    assert recovery["updates"]["status"] == "remote_state_unknown"
    assert recovery["updates"]["platform_delivery_status_json"]["cas"] == "collect_failed"
    assert recovery["release_owner"] is True


@pytest.mark.parametrize("schema_ready", [True, False])
def test_collect_materializes_phase3_or_records_schema_action_without_losing_cas(
    monkeypatch: pytest.MonkeyPatch,
    schema_ready: bool,
) -> None:
    evaluation_id = "qelt_" + "b" * 64

    class Repository:
        def __init__(self) -> None:
            self.transitions = []

        @staticmethod
        def claim(value, **_kwargs):  # type: ignore[no-untyped-def]
            return {"evaluation_id": value, "owner_id": "owner", "fencing_token": 1, "row_version": 1}

        @staticmethod
        def lease_from(row):  # type: ignore[no-untyped-def]
            return QELongTrendControlLease(
                evaluation_id=row["evaluation_id"],
                owner_id="owner",
                fencing_token=1,
                row_version=row["row_version"],
            )

        def transition(self, lease, **kwargs):  # type: ignore[no-untyped-def]
            self.transitions.append((lease, kwargs))
            return {
                "evaluation_id": lease.evaluation_id,
                "owner_id": "owner",
                "fencing_token": 1,
                "row_version": lease.row_version + 1,
                "status": kwargs["updates"]["status"],
                "platform_delivery_status_json": kwargs["updates"].get("platform_delivery_status_json", {}),
            }

    class ResultRepository:
        @staticmethod
        def persist_published_receipt(**_kwargs):
            if not schema_ready:
                raise QELongTrendResultSchemaNotReady("apply migration")
            return PersistedEvaluationReceipt(
                evaluation_id=evaluation_id,
                metric_count=2,
                artifact_count=4,
                control_row={
                    "evaluation_id": evaluation_id,
                    "owner_id": "owner",
                    "fencing_token": 1,
                    "row_version": 3,
                    "platform_delivery_status_json": {"cas": "published", "db": "published"},
                },
                replayed=False,
            )

    class Client:
        @staticmethod
        async def inspect_long_trend_evaluation(**_kwargs):
            return QELongTrendJobInspection(
                schema_version="qe_long_trend_job_receipt_v1",
                task_id="task-1",
                loop_id="Loop1",
                evaluation_id=evaluation_id,
                job_id="job-1",
                request_sha="b" * 64,
                status="partial",
                current_attempt_id="attempt-1",
                process_identity=None,
                terminal_receipt=None,
                updated_at="2026-07-22T00:00:00Z",
            )

    repository = Repository()
    service = QELongTrendPhase2Service(
        control_repository=repository,  # type: ignore[arg-type]
        result_repository=ResultRepository(),  # type: ignore[arg-type]
        owner_id="owner",
    )
    terminal = {
        "status": "partial",
        "family_status": {"signal_path": {"status": "COMPUTED"}},
        "data_action_plan": [],
        "stats": {},
    }

    async def publish(**_kwargs):
        return (
            terminal,
            {"uri": "aistock-qe-long-trend://evaluations/x", "artifact_manifest_sha256": "c" * 64},
            {"platform_delivery_status": {"worker": "partial", "cas": "published"}},
            {"sha256": "d" * 64},
            {"worker_terminal_receipt": {"sha256": "e" * 64}},
        )

    monkeypatch.setattr(service, "_publish_remote_artifacts", publish)
    result = asyncio.run(
        service.collect_and_publish(
            evaluation_id=evaluation_id,
            task_id="task-1",
            loop_index=1,
            client=Client(),  # type: ignore[arg-type]
        )
    )

    final_updates = repository.transitions[-1][1]["updates"]
    assert result["status"] == "partial"
    if schema_ready:
        assert final_updates["platform_delivery_status_json"]["db"] == "published"
        assert repository.transitions[-1][0].row_version == 3
    else:
        assert final_updates["platform_delivery_status_json"]["db"] == "schema_not_ready"
        assert final_updates["data_action_plan_json"][-1]["action"] == (
            "apply_phase3_result_schema_then_materialize_existing_receipt"
        )


def test_collect_phase3_conflict_is_loud_and_recoverable(monkeypatch: pytest.MonkeyPatch) -> None:
    evaluation_id = "qelt_" + "c" * 64

    class Repository:
        def __init__(self) -> None:
            self.transitions = []

        @staticmethod
        def claim(value, **_kwargs):  # type: ignore[no-untyped-def]
            return {"evaluation_id": value, "owner_id": "owner", "fencing_token": 1, "row_version": 1}

        @staticmethod
        def lease_from(row):  # type: ignore[no-untyped-def]
            return QELongTrendControlLease(row["evaluation_id"], "owner", 1, row["row_version"])

        def transition(self, lease, **kwargs):  # type: ignore[no-untyped-def]
            self.transitions.append((lease, kwargs))
            return {"evaluation_id": lease.evaluation_id, "row_version": lease.row_version + 1}

    class ResultRepository:
        @staticmethod
        def persist_published_receipt(**_kwargs):
            raise QELongTrendResultRepositoryError("content drift")

    class Client:
        @staticmethod
        async def inspect_long_trend_evaluation(**_kwargs):
            return QELongTrendJobInspection(
                "qe_long_trend_job_receipt_v1", "task-1", "Loop1", evaluation_id, "job-1",
                "b" * 64, "succeeded", "attempt-1", None, None, "2026-07-22T00:00:00Z"
            )

    repository = Repository()
    service = QELongTrendPhase2Service(
        control_repository=repository,  # type: ignore[arg-type]
        result_repository=ResultRepository(),  # type: ignore[arg-type]
        owner_id="owner",
    )

    async def publish(**_kwargs):
        return (
            {"status": "succeeded", "data_action_plan": []},
            {"uri": "aistock-qe-long-trend://evaluations/x", "artifact_manifest_sha256": "c" * 64},
            {"platform_delivery_status": {"worker": "succeeded", "cas": "published"}},
            {"sha256": "d" * 64},
            {"worker_terminal_receipt": {"sha256": "e" * 64}},
        )

    monkeypatch.setattr(service, "_publish_remote_artifacts", publish)
    with pytest.raises(QELongTrendResultRepositoryError, match="content drift"):
        asyncio.run(
            service.collect_and_publish(
                evaluation_id=evaluation_id,
                task_id="task-1",
                loop_index=1,
                client=Client(),  # type: ignore[arg-type]
            )
        )

    conflict = repository.transitions[-1][1]
    assert conflict["updates"]["status"] == "succeeded"
    assert conflict["updates"]["platform_delivery_status_json"]["db"] == "conflict"
    assert conflict["updates"]["data_action_plan_json"][-1]["action"] == (
        "inspect_phase3_receipt_content_conflict"
    )
    assert conflict["release_owner"] is True


def test_params_hash_is_identity_evidence_but_not_a_worker_pickle_input() -> None:
    service = QELongTrendPhase2Service(owner_id="owner")
    inventory = RecorderArtifactInventory(
        task_id="task-1",
        loop_id="Loop1",
        experiment_id="exp",
        recorder_id="rec",
        artifact_prefix="mlruns/exp/rec/artifacts",
        backtest_freq="1day",
        catalog_completeness="complete",
        artifacts={
            "prediction": {"relative_path": "mlruns/exp/rec/artifacts/pred.pkl", "sha256": "a" * 64},
            "params": {"relative_path": "mlruns/exp/rec/artifacts/params.pkl", "sha256": "b" * 64},
        },
        warnings=(),
        input_manifest_sha256="c" * 64,
    )
    bundle = QELongTrendEvaluatorBundle(
        schema_version="qe_long_trend_bundle_v1",
        bundle_sha256="d" * 64,
        evaluator_source_sha256="e" * 64,
        execution_environment_snapshot_id="qeenv-fixture",
        execution_environment_manifest_sha256="f" * 64,
        manifest={},
        files={},
    )
    snapshot = QEDatasetSnapshotIdentity(
        snapshot_id="qlib-st-pit-active-h5-daily-20180801-20260630",
        manifest_sha256="1" * 64,
        start_date="2018-08-01",
        end_date="2026-06-30",
    )
    payload = service._request_payload(
        evaluation_id="qelt_" + "2" * 64,
        run_id="run-1",
        task_id="task-1",
        loop_index=1,
        node_id="wsl2-5080",
        opt_in=LongTrendEvaluationOptIn(
            feature_data_root_uri="/home/qe/factor_data",
            outcome_data_root_uri="/home/qe/factor_data",
            backtest_freq="1day",
        ),
        profile_sha256="3" * 64,
        bundle=bundle,
        feature_snapshot=snapshot,
        outcome_snapshot=snapshot,
        input_manifest_sha="4" * 64,
        input_hashes={"prediction_sha256": "a" * 64, "params_sha256": "b" * 64},
        inventory=inventory,
        catalog_digest="5" * 64,
        label_horizon=60,
        strategy_topk=25,
        session_id="qers-qelt",
        source_run_key="qelt:qelt_" + "2" * 64,
        resource_token="secret",
        callback_url="http://127.0.0.1:8001/resource",
    )
    assert payload["input_artifact_hashes"]["params_sha256"] == "b" * 64
    assert set(payload["artifact_paths"]) == {"prediction"}
    assert set(payload["artifact_hashes"]) == {"prediction"}


def test_normal_and_historical_entries_share_stable_task_loop_evaluation_parent() -> None:
    assert _evaluation_parent_identity(task_id="task-1", loop_index=3) == "qe_task_loop:task-1:Loop3"
    # Archive run creation timing does not participate in the evaluator identity;
    # the verified run_id is bound separately on the durable control row.
    with pytest.raises(QELongTrendPhase2Error):
        _evaluation_parent_identity(task_id="../outside", loop_index=3)


def test_recorder_catalog_digest_ignores_unrelated_workspace_warnings() -> None:
    base = RecorderArtifactInventory(
        task_id="task-1",
        loop_id="Loop1",
        experiment_id="exp",
        recorder_id="rec",
        artifact_prefix="mlruns/exp/rec/artifacts",
        backtest_freq="1day",
        catalog_completeness="complete",
        artifacts={},
        warnings=(),
        input_manifest_sha256="a" * 64,
    )
    changed_warning = RecorderArtifactInventory(
        **{**base.__dict__, "warnings": ("restricted_qelt_secret_not_catalogued",)}
    )
    assert _recorder_catalog_digest(base) == _recorder_catalog_digest(changed_warning)


def test_isolated_pickle_parser_rejects_database_and_resource_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://secret")
    with pytest.raises(ParserContractError, match="forbidden credentials"):
        _reject_secrets()
