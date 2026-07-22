from __future__ import annotations

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
    _long_trend_snapshot,
    _merge_registration_catalog,
    _evaluation_parent_identity,
    _recorder_catalog_digest,
)
from backend.services.quantevolver.long_trend_artifact_resolver import RecorderArtifactInventory
from backend.services.quantevolver.long_trend_evaluation_bundle import QELongTrendEvaluatorBundle
from backend.services.quantevolver.long_trend_evaluation_contract import QEDatasetSnapshotIdentity
from backend.services.quantevolver.long_trend_evaluation_control_repository import QELongTrendControlLease
from backend.services.quantevolver.qe_resource_phase_service import (
    PHASE_INVALID_REASON,
    QEResourcePhaseError,
    validate_phase_transition,
)
from backend.services.quantevolver.qe_workspace_client import (
    QELongTrendJobInspection,
    QEWorkspaceClient,
    QEWorkspaceDatasetIdentity,
)
from backend.services.quantevolver.long_trend_pickle_parser_entry import ParserContractError, _reject_secrets


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


def test_collection_renews_fenced_lease_until_publish_finishes(monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr(phase2_module, "COLLECT_LEASE_HEARTBEAT_SECONDS", 0.01)

    async def work() -> str:
        for _attempt in range(100):
            if len(renewals) >= 2:
                return "published"
            await asyncio.sleep(0.005)
        pytest.fail("lease heartbeat did not run twice before the deterministic test deadline")

    assert asyncio.run(service._await_with_lease_heartbeat(lease=lease, awaitable=work())) == "published"
    assert len(renewals) >= 2
    assert all(seconds == phase2_module.COLLECT_LEASE_SECONDS for _lease, seconds in renewals)


def test_collection_aborts_when_fenced_lease_heartbeat_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    cancelled = False

    class Repository:
        @staticmethod
        def renew_lease(_lease, *, lease_seconds):  # type: ignore[no-untyped-def]
            assert lease_seconds == phase2_module.COLLECT_LEASE_SECONDS
            raise RuntimeError("lease owner changed")

    service = QELongTrendPhase2Service(control_repository=Repository(), owner_id="owner")  # type: ignore[arg-type]
    lease = QELongTrendControlLease(
        evaluation_id="qelt_" + "9" * 64,
        owner_id="owner",
        fencing_token=8,
        row_version=13,
    )
    monkeypatch.setattr(phase2_module, "COLLECT_LEASE_HEARTBEAT_SECONDS", 0.01)

    async def work() -> None:
        nonlocal cancelled
        try:
            await asyncio.Event().wait()
        finally:
            cancelled = True

    with pytest.raises(QELongTrendPhase2Error, match="heartbeat failed"):
        asyncio.run(service._await_with_lease_heartbeat(lease=lease, awaitable=work()))
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


def test_backend_wires_continuous_long_trend_reconciliation() -> None:
    source = (Path(__file__).resolve().parents[3] / "backend/main.py").read_text(encoding="utf-8")
    assert "async def _qe_long_trend_reconcile_loop(stop_event: asyncio.Event)" in source
    assert "while not stop_event.is_set():" in source
    assert "results = await service.reconcile_nonterminal(limit=100)" in source
    assert "QELongTrendPhase2Service().reconcile_nonterminal" not in source
    assert 'name="qe-long-trend-reconciler"' in source


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

        def claim(self, _evaluation_id, *, owner_id, lease_seconds):  # type: ignore[no-untyped-def]
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
