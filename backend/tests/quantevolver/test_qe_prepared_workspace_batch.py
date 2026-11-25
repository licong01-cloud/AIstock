from __future__ import annotations

import asyncio
from base64 import b64encode
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from hashlib import sha256
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from backend.services.quantevolver.qe_active_execution_capacity import (
    QEWorkspaceSubmissionCoordinator,
    QEWorkspaceSubmissionPayload,
    QEWorkspaceSubmissionSource,
)
from backend.services.quantevolver.qe_execution_reservation import (
    QEExecutionReservationAcquireResult,
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


@pytest.mark.parametrize(("workload", "kind"), [
    ("TRAIN_ONLY", "gpu"), ("ABSOLUTE_TARGET_REPLAY", "absolute_target_replay"),
])
@pytest.mark.parametrize(("same_cohort", "expected"), [(True, 2), (False, 1)])
def test_prepared_wsl_sibling_must_prove_same_cohort(workload, kind, same_cohort, expected):
    spec = QEExecutionReservationSpec("wsl2-5080", "qe_experiment", "id", "task", "Loop1", "a" * 64)
    contract = {"batch_sha256": "b" * 64, "workload": workload, "compute_kind": kind}
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

def _absolute_payload(loop=1):
    raw_targets = b"frozen target bytes"
    contract = {
        "schema_version": "qe_absolute_target_replay_v2", "arm": "T",
        "targets": {"path": "/assets/frozen.parquet", "sha256": sha256(raw_targets).hexdigest()},
        "dataset": {"release_id": "frozen", "manifest_sha256": "b" * 64, "profile_sha256": "e" * 64},
        "window": {
            "signal_start": "2026-10-28", "signal_end": "2026-10-29",
            "account_start": "2026-10-29", "account_end": "2026-10-30", "account_days": 2,
        },
        "initial_cash": 10_000_000,
        "execution_reference": {"path": "/assets/reference.yaml", "sha256": "f" * 64},
        "sector_ids": [1], "known_at": "UNVERIFIED",
        "exposure_policy": "per_arm_frozen_targets_no_renormalization",
    }
    return QEWorkspaceSubmissionPayload(
        task_id="frozen_targets", loop_index=loop,
        config={"absolute_target_replay": contract, "model_params": {
            "_qe_active_dataset_summary": {
                "release_id": "frozen", "dataset_manifest_sha256": "b" * 64, "profile_sha256": "e" * 64,
            },
            "_qe_direct_v2_dataset_binding": {"candidate_root": "/data/release"},
        }},
        experiment_files={
            "conf.yaml": "frozen",
            "absolute_targets.parquet.b64": b64encode(raw_targets).decode("ascii"),
            "qe_absolute_target_manifest.json": json.dumps(contract),
            "h40_execution_basis.py": "execution_basis_from_review",
        },
        wsl_command="qrun conf.yaml --absolute-target-replay", execution_identity_hash="c" * 64,
        execution_environment_snapshot_id="node_snap", execution_environment_manifest_sha256="d" * 64,
    )


def _absolute_source(node):
    return QEWorkspaceSubmissionSource(
        source_kind="qe_experiment", source_execution_id="account", node_id=node,
        submission_intent_hash="c" * 64, owner_id="reviewed_batch",
        claim_source=Mock(), record_waiting_capacity=Mock(), backtest_only=True,
        prepared_batch_sha256="a" * 64,
    )


@pytest.mark.parametrize(("node", "capacity"), [("wsl2-5080", 2), ("rdagent-node1", 4)])
def test_prepared_absolute_target_uses_replay_capacity(node, capacity):
    repository = Mock(spec=QEExecutionReservationRepository)
    repository.reserve_execution_and_claim_source.return_value = QEExecutionReservationAcquireResult(
        acquired=False, duplicate_replay=False, active_count=capacity, node_capacity=capacity,
        reservation=None, source_claim=None,
    )
    outcome = asyncio.run(QEWorkspaceSubmissionCoordinator(reservation_repository=repository).submit(
        client=Mock(), source=_absolute_source(node), payload=_absolute_payload(),
    ))
    assert outcome.waiting_capacity and outcome.node_capacity == capacity
    admission = repository.reserve_execution_and_claim_source.call_args.kwargs
    assert admission["node_capacity"] == capacity
    assert admission["prepared_workspace_contract"]["workload"] == "ABSOLUTE_TARGET_REPLAY"
    assert admission["isolate_prepared_workspace_cohort"] == (node == "wsl2-5080")


@pytest.mark.parametrize("flag", ["--train-only", "--pred-backtest", "--backtest-only", "--absolute-target-replay"])
def test_prepared_absolute_target_rejects_mixed_commands(flag):
    payload = _absolute_payload()
    mixed = replace(payload, wsl_command=f"{payload.wsl_command} {flag}")
    with pytest.raises(ValueError, match="MODE_MISMATCH"):
        QEWorkspaceSubmissionCoordinator().prepared_workspace_contract(_absolute_source("wsl2-5080"), mixed)


def test_prepared_absolute_target_requires_the_standalone_command_flag():
    payload = replace(_absolute_payload(), wsl_command="qrun conf.yaml --absolute-target-replay-preview")
    with pytest.raises(ValueError, match="MODE_MISMATCH"):
        QEWorkspaceSubmissionCoordinator().prepared_workspace_contract(_absolute_source("wsl2-5080"), payload)


def test_prepared_absolute_target_rejects_conflicting_nested_contract():
    payload = _absolute_payload()
    payload.config["model_params"]["absolute_target_replay"] = {**payload.config["absolute_target_replay"], "arm": "B"}
    with pytest.raises(ValueError, match="ABSOLUTE_TARGET_CONTRACT_MISMATCH"):
        QEWorkspaceSubmissionCoordinator().prepared_workspace_contract(_absolute_source("wsl2-5080"), payload)


@pytest.mark.parametrize("encoded", [None, "not base64", "d3Jvbmc="])
def test_prepared_absolute_target_rejects_corrupt_bytes_before_registration(monkeypatch, encoded):
    payload = _absolute_payload()
    files = dict(payload.experiment_files)
    if encoded is None:
        files.pop("absolute_targets.parquet.b64")
    else:
        files["absolute_targets.parquet.b64"] = encoded

    async def build(_):
        return replace(payload, experiment_files=files)

    client = Mock()
    monkeypatch.setattr(QEWorkspaceClient, "for_node", client)
    batch = QEPreparedWorkspaceBatch()
    register = Mock()
    monkeypatch.setattr(batch._registry, "reserve_single", register)
    job = PreparedWorkspaceJob("Loop1", "account-id", "wsl2-5080", "/w/frozen/Loop1", build, Mock())
    result = asyncio.run(batch.run("a" * 64, [job], dataset_manifest_sha256="b" * 64, timeout_seconds=2))
    assert result["Loop1"].state == "requires_reconciliation"
    assert "QE_PREPARED_ABSOLUTE_TARGET_BYTES_MISMATCH" in result["Loop1"].detail
    client.assert_not_called()
    register.assert_not_called()


def test_prepared_absolute_target_validates_manifest_and_release():
    payload = _absolute_payload()
    wrong_manifest = replace(payload, experiment_files={"qe_absolute_target_manifest.json": "{}"})
    coordinator = QEWorkspaceSubmissionCoordinator()
    with pytest.raises(ValueError, match="ABSOLUTE_TARGET_MANIFEST_MISMATCH"):
        coordinator.prepared_workspace_contract(_absolute_source("wsl2-5080"), wrong_manifest)

    payload.config["model_params"]["_qe_active_dataset_summary"]["profile_sha256"] = "1" * 64
    with pytest.raises(ValueError, match="ABSOLUTE_TARGET_ACTIVE_IDENTITY_MISMATCH"):
        coordinator.prepared_workspace_contract(_absolute_source("wsl2-5080"), payload)


def test_prepared_absolute_target_retains_reviewed_files_and_submits_two_siblings(monkeypatch):
    async def scenario():
        batch = QEPreparedWorkspaceBatch()
        saved = {}
        persisted_indexes = {}
        cursor = Mock()

        def sql(query, values):
            experiment_id = values[0]
            if query.lstrip().startswith("INSERT INTO qe_experiments"):
                assert experiment_id not in saved
                saved[experiment_id] = json.loads(values[6])
                persisted_indexes[experiment_id] = values[-1]
            else:
                assert query.startswith("SELECT experiment_id, status, custom_params")
                cursor.fetchone.return_value = (experiment_id, "created", saved[experiment_id])

        cursor.execute.side_effect = sql
        connection = SimpleNamespace(cursor=lambda: nullcontext(cursor))
        monkeypatch.setattr(batch._registry, "_transaction", lambda: nullcontext(connection))
        monkeypatch.setattr(batch._composer, "_get_experiment_record",
                            lambda key: {"custom_params": saved[key]} if key in saved else None)
        both_submitted = asyncio.Event()
        seen = {}

        async def submit(*, client, source, payload):
            assert source.backtest_only
            assert payload.wsl_command == _absolute_payload().wsl_command
            assert payload.experiment_files == _absolute_payload().experiment_files
            metadata = payload.config["model_params"]["_qe_prepared_workspace"]
            assert metadata["maximum"] == 2
            assert metadata["intent_sha256"] == source.submission_intent_hash
            assert "workload_mode" not in payload.config
            assert seen.setdefault(payload.loop_index, source.submission_intent_hash) == source.submission_intent_hash
            if len(seen) == 2:
                both_submitted.set()
            return SimpleNamespace(submitted=True, waiting_capacity=False)

        async def status(*_):
            await both_submitted.wait()
            return {"status": "completed"}

        monkeypatch.setattr(batch._coordinator, "submit", submit)
        client = Mock()
        client.get_loop_status = status
        client.get_dataset_identity = AsyncMock(return_value=SimpleNamespace(
            complete=True, dataset={"dataset_manifest_sha256": "b" * 64},
        ))

        @asynccontextmanager
        async def open_client(_):
            yield client

        monkeypatch.setattr(QEWorkspaceClient, "for_node", open_client)

        def job(loop):
            async def build(_):
                return _absolute_payload(loop)
            key = f"Loop{loop}"
            return PreparedWorkspaceJob(key, key, "wsl2-5080", f"/w/frozen_targets/{key}",
                                        build, AsyncMock(return_value={"verified": loop}))

        for _ in range(2):
            results = await batch.run("a" * 64, [job(1), job(2)], dataset_manifest_sha256="b" * 64, timeout_seconds=2)
            assert all(r.state == "verified" for r in results.values()), results
        assert len(set(seen.values())) == 2
        assert persisted_indexes == {"Loop1": 1, "Loop2": 2}
        assert all(p["absolute_target_replay"] == _absolute_payload().config["absolute_target_replay"]
                   for p in saved.values())

    asyncio.run(scenario())
