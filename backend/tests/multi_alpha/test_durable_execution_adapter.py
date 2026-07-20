from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import pytest
import yaml

from backend.services.multi_alpha.combine_backtest import (
    parse_request,
    persisted_backtest_config_for,
    request_snapshot_for,
)
from backend.services.multi_alpha.durable_execution_adapter import (
    DurableExecutionAdapterError,
    QEWorkspacePredBacktestAdapter,
)
from backend.services.multi_alpha.durable_models import (
    OwnershipToken,
    artifact_manifest_hash_for,
    make_attempt_id,
    make_child_id,
    make_remote_task_id,
    submission_intent_hash_for,
)
from backend.services.multi_alpha.remote_dispatch import ComputeNodeInfo
from backend.services.quantevolver.qe_active_execution_capacity import (
    QEWorkspaceSubmissionOutcome,
)
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceSubmissionInspection,
    QEWorkspaceFileNotFound,
)


RUN_ID = "macb_adapter_test"
CHILD_ID = make_child_id(RUN_ID, "scheme:equal")
ATTEMPT_ID = make_attempt_id(CHILD_ID, 1)


def _prediction(offset: float) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for date_index, trade_date in enumerate(pd.date_range("2026-01-02", periods=3, freq="D")):
        for instrument_index, instrument in enumerate(("A", "B", "C")):
            rows.append(
                {
                    "trade_date": trade_date.date(),
                    "instrument": instrument,
                    "score": offset + date_index + instrument_index,
                }
            )
    return pd.DataFrame(rows)


def _label(_run_id: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for date_index, trade_date in enumerate(pd.date_range("2026-01-02", periods=3, freq="D")):
        for instrument_index, instrument in enumerate(("A", "B", "C")):
            rows.append(
                {
                    "trade_date": trade_date.date(),
                    "instrument": instrument,
                    "forward_return": (date_index + instrument_index + 1) / 100,
                }
            )
    return pd.DataFrame(rows)


def _request(tmp_path: Path):
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    conf = {
        "port_analysis_config": {
            "strategy": {
                "class": "ScoreWeightedTopkStrategyV2",
                "kwargs": {"topk": 20, "n_drop": 2},
            }
        }
    }
    (runtime / "conf.yaml").write_text(
        yaml.safe_dump(conf, sort_keys=False),
        encoding="utf-8",
    )
    (runtime / "qrun_limit_minute.py").write_text("# runtime\n", encoding="utf-8")
    (runtime / "read_exp_res.py").write_text("# reader\n", encoding="utf-8")
    (runtime / "combined_factors_df.parquet").write_bytes(b"parquet")
    return parse_request(
        {
            "roster": [
                {"leg_id": "leg_a", "seed_run_ids": ["a1"]},
                {"leg_id": "leg_b", "seed_run_ids": ["b1"]},
            ],
            "oos_start": "2026-01-02",
            "oos_end": "2026-01-04",
            "weighting_schemes": ["equal"],
            "normalize_method": "rank",
            "walk_forward": {"enabled": True, "window": 2, "min_periods": 1},
            "backtest_config": {
                "node_id": "wsl2-5080",
                "node_parallelism": {"wsl2-5080": 2},
                "runtime_template_dir": str(runtime),
                "remote_artifact_store_root": "/remote/artifacts",
            },
            "baseline_leg_id": "leg_a",
            "topk": 1,
            "min_date_coverage": 1.0,
            "scheme_timeout_seconds": 60,
            "run_timeout_seconds": 300,
        }
    )


class _Repository:
    def __init__(self, request: Any) -> None:
        snapshot = request_snapshot_for(request)
        manifest = {
            "schema_version": "multi_alpha_child_input_manifest_v1",
            "run_id": RUN_ID,
            "request_hash": "a" * 64,
            "roster_hash": "roster",
            "child_key": "scheme:equal",
        }
        self.run = {
            "id": RUN_ID,
            "backtest_config_json": persisted_backtest_config_for(request),
            "node_parallelism_json": {"wsl2-5080": 2, "rdagent-node1": 4},
            "roster_json": snapshot["roster"],
        }
        self.child = {
            "child_id": CHILD_ID,
            "run_id": RUN_ID,
            "child_key": "scheme:equal",
            "child_kind": "scheme",
            "weighting_scheme": "equal",
            "dropped_leg_id": None,
            "input_manifest_json": manifest,
            "input_manifest_hash": artifact_manifest_hash_for(manifest),
        }
        self.attempt = {
            "attempt_id": ATTEMPT_ID,
            "child_id": CHILD_ID,
            "run_id": RUN_ID,
            "attempt_no": 1,
            "retry_mode": "initial",
            "retry_of_attempt_id": None,
            "node_id": "wsl2-5080",
        }
        self.claim_calls: list[dict[str, Any]] = []
        self.wait_calls: list[dict[str, Any]] = []
        self.persist_attempt = True

    def get_run(self, run_id: str) -> Mapping[str, Any] | None:
        return self.run if run_id == RUN_ID else None

    def get_child(self, child_id: str) -> Mapping[str, Any] | None:
        return self.child if child_id == CHILD_ID else None

    def get_attempt(self, attempt_id: str) -> Mapping[str, Any] | None:
        return self.attempt if self.persist_attempt and attempt_id == ATTEMPT_ID else None

    def claim_attempt_submission_in_transaction(self, _cur: Any, **kwargs: Any) -> Mapping[str, Any]:
        self.claim_calls.append(dict(kwargs))
        return {**self.attempt, "status": "submitting", "row_version": 2}

    def record_attempt_waiting_capacity_in_transaction(
        self,
        _cur: Any,
        **kwargs: Any,
    ) -> Mapping[str, Any]:
        self.wait_calls.append(dict(kwargs))
        return {**self.attempt, "status": "queued", "phase": "waiting_capacity"}


class _ArtifactClient:
    def __init__(self) -> None:
        self.paths: list[Path] = []

    def ensure_artifact(self, path: Path, *, node_id: str) -> dict[str, Any]:
        self.paths.append(path)
        return {
            "sha256": ("a" if path.suffix == ".parquet" else "b") * 64,
            "size": path.stat().st_size,
            "uploaded": False,
            "status": {
                "exists": True,
                "size": path.stat().st_size,
                "artifact_store_root": "/remote/artifacts",
            },
        }


class _WorkspaceClient:
    def __init__(self, node_id: str, nodes: list[str]) -> None:
        self.node_id = node_id
        nodes.append(node_id)

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _tb):
        return None

    async def inspect_loop_submission(self, task_id: str, loop_id: str, **_kwargs: Any):
        qe_task_id = make_remote_task_id(RUN_ID, CHILD_ID, 1)
        return QEWorkspaceSubmissionInspection(
            schema_version="qe_submission_receipt_v1",
            task_id=task_id,
            loop_id=loop_id,
            status="running",
            submission_intent_hash=submission_intent_hash_for(
                child_id=CHILD_ID,
                attempt_no=1,
                retry_mode="initial",
                retry_of_attempt_id=None,
                node_id=self.node_id,
                qe_task_id=qe_task_id,
                qe_loop_id="Loop1",
            ),
            request_digest="d" * 64,
        )

    async def get_loop_status(self, _task_id: str, _loop_id: str) -> dict[str, Any]:
        return {"status": "running"}

    async def get_workspace_file(self, _task_id: str, _loop_id: str, _path: str) -> dict[str, Any]:
        return {
            "absolute_returns": {
                "cagr": 0.5,
                "max_drawdown": -0.1,
                "sharpe": 2.0,
                "calmar": 5.0,
            },
            "summary": {"Rank IC": 0.08},
        }


class _MissingResultWorkspaceClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _tb):
        return None

    async def get_workspace_file(self, task_id: str, loop_id: str, path: str) -> Any:
        raise QEWorkspaceFileNotFound(task_id, loop_id, path, "http://qe/files")


class _InvalidResultWorkspaceClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _tb):
        return None

    async def get_workspace_file(self, _task_id: str, _loop_id: str, _path: str) -> str:
        return "not-json"


class _SubmissionCoordinator:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def submit(self, *, client: Any, source: Any, payload: Any):
        self.calls.append(
            {
                "node_id": client.node_id,
                "source": source,
                "payload": payload,
            }
        )
        source_claim = source.claim_source(object())
        return QEWorkspaceSubmissionOutcome(
            state="submitted",
            task_id=payload.task_id,
            loop_id=payload.loop_id,
            reservation_id="qer_" + "e" * 64,
            reservation_status="submitting",
            remote_status="reserved",
            active_count=1,
            node_capacity=2 if client.node_id == "wsl2-5080" else 4,
            duplicate_replay=False,
            remote_acceptance_unknown=False,
            source_claim=source_claim,
        )


def _adapter(tmp_path: Path):
    request = _request(tmp_path)
    repository = _Repository(request)
    predictions = {"a1": _prediction(0.0), "b1": _prediction(0.5)}
    used_nodes: list[str] = []
    artifact_client = _ArtifactClient()
    coordinator = _SubmissionCoordinator()
    adapter = QEWorkspacePredBacktestAdapter(
        repository=repository,  # type: ignore[arg-type]
        prediction_loader=lambda run_id: predictions[run_id],
        label_loader=_label,
        workspace_root=tmp_path / "workspaces",
        workspace_client_factory=lambda node_id: _WorkspaceClient(node_id, used_nodes),
        node_resolver=lambda node_id: ComputeNodeInfo(
            node_id=node_id,
            api_base_url="http://127.0.0.1:9000"
            if node_id == "wsl2-5080"
            else "http://192.168.50.215:9000",
            qlib_data_path="/home/node/data/qlib_bin",
            factor_data_dir="/home/node/data/factor_values",
            workspace_base="/home/node/workspaces",
        ),
        artifact_client_factory=lambda _node_id: artifact_client,
        submission_coordinator=coordinator,  # type: ignore[arg-type]
    )
    return adapter, repository, coordinator, artifact_client, used_nodes


def test_materialize_and_atomic_publish_reuses_existing_combiner_and_runtime(tmp_path: Path) -> None:
    adapter, _repository, _coordinator, _artifact_client, _used_nodes = _adapter(tmp_path)

    materialized = adapter.materialize_child_input(
        run_id=RUN_ID,
        child_id=CHILD_ID,
        attempt_id=ATTEMPT_ID,
    )
    published = adapter.publish_artifacts(materialized)
    replay = adapter.publish_artifacts(materialized)

    assert not materialized.prediction_frame.empty
    assert set(materialized.weights) == {"leg_a", "leg_b"}
    assert published.prediction_path.exists()
    assert (published.workspace / "conf.yaml").exists()
    assert published.artifact_manifest["schema_version"] == "multi_alpha_child_artifact_manifest_v1"
    assert replay.artifact_manifest == published.artifact_manifest
    assert not list(published.workspace.parent.glob("*.tmp"))


def test_materialization_precedes_initial_attempt_persistence(tmp_path: Path) -> None:
    adapter, repository, _coordinator, _artifact_client, _used_nodes = _adapter(tmp_path)
    repository.persist_attempt = False

    materialized = adapter.materialize_child_input(
        run_id=RUN_ID,
        child_id=CHILD_ID,
        attempt_id=ATTEMPT_ID,
    )
    published = adapter.publish_artifacts(materialized)

    assert materialized.attempt == {
        "attempt_id": ATTEMPT_ID,
        "child_id": CHILD_ID,
        "attempt_no": 1,
        "retry_mode": "initial",
        "retry_of_attempt_id": None,
    }
    assert published.artifact_manifest["attempt_id"] == ATTEMPT_ID


def test_existing_artifact_byte_mismatch_is_loud(tmp_path: Path) -> None:
    adapter, _repository, _coordinator, _artifact_client, _used_nodes = _adapter(tmp_path)
    materialized = adapter.materialize_child_input(
        run_id=RUN_ID,
        child_id=CHILD_ID,
        attempt_id=ATTEMPT_ID,
    )
    published = adapter.publish_artifacts(materialized)
    published.prediction_path.write_bytes(b"corrupt")

    with pytest.raises(DurableExecutionAdapterError) as caught:
        adapter.publish_artifacts(materialized)

    assert caught.value.reason_code == "multi_alpha_artifact_hash_mismatch"


def test_local_and_remote_nodes_use_same_qe_workspace_client_and_coordinator(tmp_path: Path) -> None:
    adapter, repository, coordinator, artifact_client, used_nodes = _adapter(tmp_path)
    materialized = adapter.materialize_child_input(
        run_id=RUN_ID,
        child_id=CHILD_ID,
        attempt_id=ATTEMPT_ID,
    )
    published = adapter.publish_artifacts(materialized)
    token = OwnershipToken(owner_id="worker_1", fencing_token=1, row_version=1)

    local_intent = adapter.prepare_submission_intent(
        run=repository.run,
        child=repository.child,
        attempt=repository.attempt,
        node_id="wsl2-5080",
    )
    local_outcome = asyncio.run(
        adapter.submit(
            artifacts=published,
            intent=local_intent,
            attempt_token=token,
        )
    )

    remote_root = tmp_path / "remote_case"
    remote_root.mkdir()
    remote_adapter, remote_repository, remote_coordinator, remote_artifact_client, remote_nodes = _adapter(
        remote_root
    )
    remote_repository.attempt["node_id"] = "rdagent-node1"
    remote_materialized = remote_adapter.materialize_child_input(
        run_id=RUN_ID,
        child_id=CHILD_ID,
        attempt_id=ATTEMPT_ID,
    )
    remote_published = remote_adapter.publish_artifacts(remote_materialized)
    remote_intent = remote_adapter.prepare_submission_intent(
        run=remote_repository.run,
        child=remote_repository.child,
        attempt=remote_repository.attempt,
        node_id="rdagent-node1",
    )
    remote_outcome = asyncio.run(
        remote_adapter.submit(
            artifacts=remote_published,
            intent=remote_intent,
            attempt_token=token,
        )
    )

    assert local_outcome.state == remote_outcome.state == "submitted"
    assert used_nodes + remote_nodes == ["wsl2-5080", "rdagent-node1"]
    assert [call["node_id"] for call in coordinator.calls + remote_coordinator.calls] == used_nodes + remote_nodes
    assert repository.claim_calls[0]["qe_loop_id"] == "Loop1"
    assert coordinator.calls[0]["payload"].experiment_files
    assert all(
        "combined_prediction.pkl.b64" not in call["payload"].experiment_files
        for call in coordinator.calls + remote_coordinator.calls
    )
    assert len(artifact_client.paths) + len(remote_artifact_client.paths) == 4


def test_submission_intent_is_deterministic_and_attempt_scoped(tmp_path: Path) -> None:
    adapter, repository, _coordinator, _artifact_client, _used_nodes = _adapter(tmp_path)
    first = adapter.prepare_submission_intent(
        run=repository.run,
        child=repository.child,
        attempt=repository.attempt,
        node_id="wsl2-5080",
    )
    second = adapter.prepare_submission_intent(
        run=repository.run,
        child=repository.child,
        attempt=repository.attempt,
        node_id="wsl2-5080",
    )

    assert first == second
    assert first.qe_loop_id == "Loop1"
    assert first.qe_task_id.startswith("macb_remote_")
    assert len(first.submission_intent_hash) == 64


def test_collect_result_persists_hash_linked_manifest_idempotently(tmp_path: Path) -> None:
    adapter, repository, _coordinator, _artifact_client, _used_nodes = _adapter(tmp_path)
    materialized = adapter.materialize_child_input(
        run_id=RUN_ID,
        child_id=CHILD_ID,
        attempt_id=ATTEMPT_ID,
    )
    published = adapter.publish_artifacts(materialized)
    intent = adapter.prepare_submission_intent(
        run=repository.run,
        child=repository.child,
        attempt=repository.attempt,
        node_id="wsl2-5080",
    )

    deadline_evidence = {
        "scheme": {
            "timeout_seconds": 60,
            "started_at": "2026-01-01T00:00:00Z",
            "deadline_at": "2026-01-01T00:01:00Z",
            "effective_observed_at": "2026-01-01T00:02:00Z",
            "elapsed_seconds": 120.0,
            "timestamp_source": "submission_receipt.finished_at",
            "remote_status": "completed",
        }
    }
    first = asyncio.run(
        adapter.collect_result(
            intent=intent,
            artifacts=published,
            execution_deadline_evidence=deadline_evidence,
        )
    )
    second = asyncio.run(
        adapter.collect_result(
            intent=intent,
            artifacts=published,
            execution_deadline_evidence=deadline_evidence,
        )
    )

    assert first.metrics["cagr"] == 0.5
    assert first.result_manifest == second.result_manifest
    assert first.result_manifest["completed_after_deadline"] is True
    assert first.result_manifest["execution_deadline"] == deadline_evidence
    assert first.result_manifest_path.exists()
    assert json.loads(first.result_manifest_path.read_text(encoding="utf-8")) == first.result_manifest


def test_collect_result_distinguishes_not_visible_from_invalid_content(tmp_path: Path) -> None:
    adapter, repository, _coordinator, _artifact_client, _used_nodes = _adapter(tmp_path)
    materialized = adapter.materialize_child_input(
        run_id=RUN_ID,
        child_id=CHILD_ID,
        attempt_id=ATTEMPT_ID,
    )
    published = adapter.publish_artifacts(materialized)
    intent = adapter.prepare_submission_intent(
        run=repository.run,
        child=repository.child,
        attempt=repository.attempt,
        node_id="wsl2-5080",
    )

    adapter._workspace_client_factory = lambda _node_id: _MissingResultWorkspaceClient()
    with pytest.raises(DurableExecutionAdapterError) as not_visible:
        asyncio.run(adapter.collect_result(intent=intent, artifacts=published))
    assert not_visible.value.reason_code == "multi_alpha_child_result_not_visible"

    adapter._workspace_client_factory = lambda _node_id: _InvalidResultWorkspaceClient()
    with pytest.raises(DurableExecutionAdapterError) as invalid:
        asyncio.run(adapter.collect_result(intent=intent, artifacts=published))
    assert invalid.value.reason_code == "multi_alpha_child_result_invalid"


def test_inspect_remote_returns_receipt_and_status_without_fallback(tmp_path: Path) -> None:
    adapter, repository, _coordinator, _artifact_client, _used_nodes = _adapter(tmp_path)
    intent = adapter.prepare_submission_intent(
        run=repository.run,
        child=repository.child,
        attempt=repository.attempt,
        node_id="wsl2-5080",
    )

    inspection = asyncio.run(adapter.inspect_remote(intent=intent))

    assert inspection.receipt.status == "running"
    assert inspection.status == {"status": "running"}
