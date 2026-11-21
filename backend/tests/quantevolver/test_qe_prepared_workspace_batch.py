from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from backend.services.quantevolver.qe_active_execution_capacity import QEWorkspaceSubmissionPayload
from backend.services.quantevolver.qe_execution_reservation import (
    QEExecutionReservationError,
    QEExecutionReservationRepository,
    QEExecutionReservationSpec,
)
from backend.services.quantevolver.qe_prepared_workspace_batch import (
    PreparedWorkspaceJob,
    QEPreparedWorkspaceBatch,
)
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient


def test_ready_seed_replays_while_independent_seed_is_still_training(monkeypatch):
    async def scenario():
        second_started = asyncio.Event()
        first_replayed = asyncio.Event()
        batch = QEPreparedWorkspaceBatch()

        async def execute(_batch, _manifest, job, payload):
            if job.key == "train42":
                await second_started.wait()
                return "prediction42"
            if job.key == "train2024":
                second_started.set()
                await first_replayed.wait()
                return "prediction2024"
            first_replayed.set()
            return payload.config["dependencies"]

        monkeypatch.setattr(batch, "_execute", execute)

        def job(key, prerequisites=()):
            async def build(dependencies):
                return QEWorkspaceSubmissionPayload(key, 1, {"dependencies": dict(dependencies)}, {}, "")

            return PreparedWorkspaceJob(key, key, "rdagent-node1", key, build, Mock(), prerequisites)

        result = await batch.run("a" * 64, [
            job("train42"), job("train2024"), job("replay42", ("train42",)),
        ], dataset_manifest_sha256="b" * 64, timeout_seconds=2)
        assert result["train2024"].state == "verified"
        assert result["replay42"].evidence == {"train42": "prediction42"}

    asyncio.run(scenario())


@pytest.mark.parametrize(("same_cohort", "expected"), [(True, 2), (False, 1)])
def test_prepared_wsl_sibling_must_prove_same_cohort(same_cohort, expected):
    spec = QEExecutionReservationSpec("wsl2-5080", "qe_experiment", "id", "task", "Loop1", "a" * 64)
    contract = {"batch_sha256": "b" * 64, "workload": "TRAIN_ONLY", "compute_kind": "gpu"}
    cursor = Mock()
    cursor.fetchone.side_effect = [{"prepared": contract}, {"incompatible_count": int(not same_cohort)}]
    capacity = QEExecutionReservationRepository._prepared_workspace_capacity(
        cursor, spec, 2, contract, isolate_cohort=True,
    )
    assert capacity == expected
    query, params = cursor.execute.call_args.args
    assert "submission_intent_hash" in query and "parallel_eligible" in query
    assert contract["batch_sha256"] in params


def test_prepared_workspace_does_not_reuse_a_changed_registered_intent():
    spec = QEExecutionReservationSpec("wsl2-5080", "qe_experiment", "id", "task", "Loop1", "a" * 64)
    cursor = Mock()
    cursor.fetchone.return_value = {"prepared": {"intent_sha256": "c" * 64}}
    with pytest.raises(QEExecutionReservationError, match="does not match"):
        QEExecutionReservationRepository._prepared_workspace_capacity(
            cursor, spec, 2, {"intent_sha256": "a" * 64}, isolate_cohort=True,
        )
    cursor.execute.assert_called_once()


def test_prepared_workspace_keeps_same_worker_and_registration_on_resume(monkeypatch):
    batch = QEPreparedWorkspaceBatch()
    saved = {}
    register = Mock(side_effect=lambda **kw: saved.update(custom_params=kw["custom_params"]))
    monkeypatch.setattr(batch._registry, "reserve_single", register)
    monkeypatch.setattr(batch._composer, "_get_experiment_record", lambda _: dict(saved) if saved else None)
    seen = []

    async def submit(*, client, source, payload):
        seen.append((source.submission_intent_hash, payload.config["model_params"]))
        return SimpleNamespace(submitted=True, waiting_capacity=False)

    monkeypatch.setattr(batch._coordinator, "submit", submit)
    client = Mock()
    client.get_loop_status = AsyncMock(return_value={"status": "completed"})
    client.get_dataset_identity = AsyncMock(return_value=SimpleNamespace(
        complete=True, dataset={"dataset_manifest_sha256": "b" * 64},
    ))

    @asynccontextmanager
    async def open_client(_):
        yield client

    monkeypatch.setattr(QEWorkspaceClient, "for_node", open_client)

    async def build(_):
        return QEWorkspaceSubmissionPayload(
            "prepared", 4, {"workload_mode": "PREDICTION_REPLAY", "model_params": {
                "_qe_active_dataset_summary": {"dataset_manifest_sha256": "b" * 64},
                "_qe_direct_v2_dataset_binding": {"candidate_root": "/data/release"},
            }}, {"conf.yaml": "frozen", "qe_prediction_replay_source_ref.json":
                '{"schema_version":"qe_prediction_replay_source_ref_v1","source_task_id":"prepared",'
                '"source_loop_index":2,"sha256":"' + "d" * 64 + '","size_bytes":123}'
            }, "qrun --pred-backtest prediction.pkl",
            execution_environment_snapshot_id="node_snap", execution_environment_manifest_sha256="c" * 64,
        )

    job = PreparedWorkspaceJob("replay", "replay", "rdagent-node1", "/w/prepared/Loop4",
                               build, AsyncMock(return_value={"recorder": "finished"}))

    async def scenario():
        for _ in range(2):
            result = await batch.run("a" * 64, [job], dataset_manifest_sha256="b" * 64, timeout_seconds=1)
            assert result["replay"].state == "verified"

    asyncio.run(scenario())
    register.assert_called_once()
    assert len(seen) == 2 and seen[0] == seen[1]
    assert seen[0][1] == saved["custom_params"]
    assert saved["custom_params"]["_qe_run_registration"]["loop_index"] == 4
    assert saved["custom_params"]["_qe_archive_replay_source"]["source_loop_index"] == 2
