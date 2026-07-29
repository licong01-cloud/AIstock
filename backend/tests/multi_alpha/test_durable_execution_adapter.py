from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
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
from backend.services.multi_alpha.durable_identity import (
    build_execution_identity,
    legacy_execution_identity_evidence,
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
        if path.name not in {"combined_factors_df.parquet", "combined_prediction.pkl"}:
            digest = _sha256(path)
        else:
            digest = ("a" if path.suffix == ".parquet" else "b") * 64
        return {
            "sha256": digest,
            "size": path.stat().st_size,
            "uploaded": False,
            "status": {
                "exists": True,
                "size": path.stat().st_size,
                "artifact_store_root": "/remote/artifacts",
            },
        }


class _PredictionModelStore:
    def __init__(self, paths: Mapping[str, Path]) -> None:
        self.paths = dict(paths)

    def prediction_path(self, *, run_id: str) -> Path:
        return self.paths[run_id]


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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rematerialize_fixture(tmp_path: Path):
    request = _request(tmp_path)
    repository = _Repository(request)
    source_paths: dict[str, Path] = {}
    predictions = {"a1": _prediction(0.0), "b1": _prediction(0.5)}
    for run_id, frame in predictions.items():
        path = tmp_path / f"{run_id}.pkl"
        frame.to_pickle(path)
        source_paths[run_id] = path
    model_store = _PredictionModelStore(source_paths)
    materializer_identity = {
        "aistock_commit": "123456abcdef",
        "planner_version": "multi_alpha_child_plan_v1",
        "combiner_file_sha256": "4" * 64,
        "panel_builder_file_sha256": "5" * 64,
        "materializer_file_set_sha256": "6" * 64,
    }
    execution_identity = build_execution_identity(
        dataset={
            "deployment_snapshot_id": "qe-dataset-20260720",
            "dataset_manifest_sha256": "a" * 64,
            "cutoff_trade_date": "2026-06-30",
            "qlib_calendar_sha256": "b" * 64,
            "qlib_instruments_sha256": "c" * 64,
            "st_pit_snapshot_id": "st-pit-20260630",
            "st_pit_manifest_sha256": "d" * 64,
            "resolved_node_id": "wsl2-5080",
            "resolved_data_root_uri": "/home/lc999/data/factor_data",
        },
        prediction_sources=[
            {
                "leg_id": "leg_a",
                "seed_run_id": "a1",
                "artifact_uri": source_paths["a1"].as_uri(),
                "artifact_sha256": _sha256(source_paths["a1"]),
            },
            {
                "leg_id": "leg_b",
                "seed_run_id": "b1",
                "artifact_uri": source_paths["b1"].as_uri(),
                "artifact_sha256": _sha256(source_paths["b1"]),
            },
        ],
        runtime={
            "qlib_runtime_template_sha256": "e" * 64,
            "conda_environment_lock_sha256": "f" * 64,
            "execution_environment_snapshot_id": "rdagent-gpu-20260720",
            "execution_environment_manifest_sha256": "0" * 64,
            "executor_code_commit": "abcdef012345",
            "executor_file_set_sha256": "1" * 64,
            "backtest_config_sha256": "2" * 64,
        },
        materializer=materializer_identity,
        business_formula={
            "formula_version": "durable_business_result_v1",
            "assembler_file_sha256": "3" * 64,
            "delta_formula_sha256": "7" * 64,
        },
    )
    source_input_manifest = {
        **dict(repository.child["input_manifest_json"]),
        "execution_identity": execution_identity.payload,
        "execution_identity_hash": execution_identity.identity_hash,
        "execution_identity_evidence": legacy_execution_identity_evidence(
            execution_identity.payload
        ),
    }
    repository.child["input_manifest_json"] = source_input_manifest
    repository.child["input_manifest_hash"] = artifact_manifest_hash_for(source_input_manifest)
    adapter = QEWorkspacePredBacktestAdapter(
        repository=repository,  # type: ignore[arg-type]
        prediction_loader=lambda run_id: predictions[run_id],
        label_loader=_label,
        model_store=model_store,  # type: ignore[arg-type]
        workspace_root=tmp_path / "recovery-workspaces",
        recovery_materializer_identity_resolver=(
            lambda **_kwargs: materializer_identity
        ),
    )
    successor_run_id = "macb_recovery_test"
    successor_child_id = make_child_id(successor_run_id, "scheme:equal")
    successor_attempt_id = make_attempt_id(successor_child_id, 1)
    successor_input_manifest = {
        **source_input_manifest,
        "run_id": successor_run_id,
        "child_id": successor_child_id,
    }
    successor_run_spec = SimpleNamespace(
        run_id=successor_run_id,
        backtest_config=repository.run["backtest_config_json"],
    )
    successor_child_spec = SimpleNamespace(
        run_id=successor_run_id,
        child_id=successor_child_id,
        child_key="scheme:equal",
        child_kind="scheme",
        weighting_scheme="equal",
        dropped_leg_id=None,
        input_manifest=successor_input_manifest,
        input_manifest_hash=artifact_manifest_hash_for(successor_input_manifest),
    )
    successor_attempt_spec = SimpleNamespace(
        attempt_id=successor_attempt_id,
        child_id=successor_child_id,
        attempt_no=1,
        retry_mode="rematerialize_and_backtest",
        source_attempt_id=ATTEMPT_ID,
    )
    lineage = {
        "source_input_manifest": source_input_manifest,
        "source_input_manifest_hash": repository.child["input_manifest_hash"],
        "recovery_materializer_identity": materializer_identity,
    }
    return (
        adapter,
        repository,
        source_paths,
        successor_run_spec,
        successor_child_spec,
        successor_attempt_spec,
        lineage,
    )


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
    assert published.artifact_manifest["l2_artifact"]["path"] == "combined_factors_df.parquet"
    assert replay.artifact_manifest == published.artifact_manifest
    assert not list(published.workspace.parent.glob("*.tmp"))


def test_backtest_only_recovery_preserves_l2_and_execution_identity_bindings(
    tmp_path: Path,
) -> None:
    adapter, repository, _coordinator, _artifact_client, _used_nodes = _adapter(tmp_path)
    execution_identity = build_execution_identity(
        dataset={
            "deployment_snapshot_id": "qe-dataset-20260720",
            "dataset_manifest_sha256": "a" * 64,
            "cutoff_trade_date": "2026-06-30",
            "qlib_calendar_sha256": "b" * 64,
            "qlib_instruments_sha256": "c" * 64,
            "st_pit_snapshot_id": "st-pit-20260630",
            "st_pit_manifest_sha256": "d" * 64,
            "resolved_node_id": "wsl2-5080",
            "resolved_data_root_uri": "/home/lc999/data/factor_data",
        },
        prediction_sources=[
            {
                "leg_id": "leg_a",
                "seed_run_id": "a1",
                "artifact_uri": "file:///predictions/a1.pkl",
                "artifact_sha256": "e" * 64,
            }
        ],
        runtime={
            "qlib_runtime_template_sha256": "f" * 64,
            "conda_environment_lock_sha256": "0" * 64,
            "execution_environment_snapshot_id": "rdagent-gpu-20260720",
            "execution_environment_manifest_sha256": "1" * 64,
            "executor_code_commit": "abcdef012345",
            "executor_file_set_sha256": "2" * 64,
            "backtest_config_sha256": "3" * 64,
        },
        materializer={
            "aistock_commit": "123456abcdef",
            "planner_version": "multi_alpha_child_plan_v1",
            "combiner_file_sha256": "4" * 64,
            "panel_builder_file_sha256": "5" * 64,
            "materializer_file_set_sha256": "6" * 64,
        },
        business_formula={
            "formula_version": "durable_business_result_v1",
            "assembler_file_sha256": "7" * 64,
            "delta_formula_sha256": "8" * 64,
        },
    )
    repository.child["input_manifest_json"] = {
        **dict(repository.child["input_manifest_json"]),
        "execution_identity": execution_identity.payload,
        "execution_identity_hash": execution_identity.identity_hash,
        "execution_identity_evidence": legacy_execution_identity_evidence(
            execution_identity.payload
        ),
    }
    repository.child["input_manifest_hash"] = artifact_manifest_hash_for(
        repository.child["input_manifest_json"]
    )
    materialized = adapter.materialize_child_input(
        run_id=RUN_ID,
        child_id=CHILD_ID,
        attempt_id=ATTEMPT_ID,
    )
    source = adapter.publish_artifacts(materialized)
    successor_run_id = "macb_recovery_backtest_only"
    successor_child_id = make_child_id(successor_run_id, "scheme:equal")
    successor_attempt_id = make_attempt_id(successor_child_id, 1)

    recovered = adapter.stage_backtest_only_recovery_artifacts(
        source_run_id=RUN_ID,
        source_child_id=CHILD_ID,
        source_attempt_id=ATTEMPT_ID,
        successor_run_id=successor_run_id,
        successor_child_id=successor_child_id,
        successor_attempt_id=successor_attempt_id,
        successor_input_manifest_hash="9" * 64,
        source_lineage_hash="a" * 64,
    )

    assert recovered.artifact_manifest["l2_artifact"] == {
        "path": "combined_factors_df.parquet",
        **recovered.artifact_manifest["files"]["combined_factors_df.parquet"],
    }
    assert recovered.artifact_manifest["execution_identity"] == execution_identity.payload
    assert (
        recovered.artifact_manifest["execution_identity_hash"]
        == execution_identity.identity_hash
    )
    assert recovered.artifact_manifest["execution_identity_evidence"]["complete"] is True
    assert recovered.artifact_manifest["recovery_source_lineage_hash"] == "a" * 64
    assert recovered.artifact_manifest["manifest_hash"] == artifact_manifest_hash_for(
        {
            key: value
            for key, value in recovered.artifact_manifest.items()
            if key != "manifest_hash"
        }
    )
    assert (recovered.workspace / "combined_factors_df.parquet").read_bytes() == (
        source.workspace / "combined_factors_df.parquet"
    ).read_bytes()


def test_publish_rejects_missing_l2_artifact_before_artifact_manifest(tmp_path: Path) -> None:
    adapter, _repository, _coordinator, _artifact_client, _used_nodes = _adapter(tmp_path)
    (tmp_path / "runtime" / "combined_factors_df.parquet").unlink()
    materialized = adapter.materialize_child_input(
        run_id=RUN_ID,
        child_id=CHILD_ID,
        attempt_id=ATTEMPT_ID,
    )

    with pytest.raises(DurableExecutionAdapterError) as caught:
        adapter.publish_artifacts(materialized)

    assert caught.value.reason_code == "multi_alpha_l2_artifact_missing"
    assert not (materialized.workspace / "artifact_manifest.json").exists()


def test_publish_copies_explicit_absolute_l2_source_into_immutable_workspace(tmp_path: Path) -> None:
    adapter, repository, _coordinator, artifact_client, _used_nodes = _adapter(tmp_path)
    runtime_l2 = tmp_path / "runtime" / "combined_factors_df.parquet"
    runtime_l2.unlink()
    external_l2 = tmp_path / "authoritative" / "g14-fp-h40.parquet"
    external_l2.parent.mkdir()
    external_l2.write_bytes(b"authoritative-l2")
    repository.run["backtest_config_json"]["combined_factors_path"] = str(external_l2)
    repository.run["backtest_config_json"]["_combine_request_v1"]["backtest_config"][
        "combined_factors_path"
    ] = str(external_l2)
    materialized = adapter.materialize_child_input(
        run_id=RUN_ID,
        child_id=CHILD_ID,
        attempt_id=ATTEMPT_ID,
    )

    published = adapter.publish_artifacts(materialized)
    external_l2.unlink()
    published_l2 = published.workspace / "combined_factors_df.parquet"

    assert published_l2.read_bytes() == b"authoritative-l2"
    assert published.artifact_manifest["l2_artifact"] == {
        "path": "combined_factors_df.parquet",
        **published.artifact_manifest["files"]["combined_factors_df.parquet"],
    }
    assert adapter._published_l2_artifact_path(published) == published_l2
    assert artifact_client.paths == []


def test_publish_excludes_and_records_node_bound_qe_data_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _repository, _coordinator, _artifact_client, _used_nodes = _adapter(tmp_path)
    external_data = tmp_path / "runtime" / "bak_basic.h5"
    external_data.write_bytes(b"stand-in for an unreadable DrvFS data link")
    monkeypatch.setattr(
        "backend.services.multi_alpha.durable_execution_adapter.is_runtime_external_data_link",
        lambda path: path.name == "bak_basic.h5",
    )
    monkeypatch.setattr(
        "backend.services.multi_alpha.combine_backtest.is_runtime_external_data_link",
        lambda path: path.name == "bak_basic.h5",
    )

    materialized = adapter.materialize_child_input(
        run_id=RUN_ID,
        child_id=CHILD_ID,
        attempt_id=ATTEMPT_ID,
    )
    published = adapter.publish_artifacts(materialized)

    expected_binding = {
        "name": "bak_basic.h5",
        "binding": "node_canonical_qe_data",
        "published": False,
    }
    assert published.artifact_manifest["external_runtime_data_bindings"] == [expected_binding]
    assert "bak_basic.h5" not in published.artifact_manifest["files"]
    assert not (published.workspace / "bak_basic.h5").exists()
    materialization = json.loads((published.workspace / "materialization.json").read_text(encoding="utf-8"))
    assert materialization["external_runtime_data_bindings"] == [expected_binding]


def test_builtin_rematerialize_recomputes_from_verified_frozen_prediction_sources(
    tmp_path: Path,
) -> None:
    (
        adapter,
        repository,
        _source_paths,
        successor_run_spec,
        successor_child_spec,
        successor_attempt_spec,
        lineage,
    ) = _rematerialize_fixture(tmp_path)

    published = adapter.stage_rematerialized_recovery_artifacts(
        source_run=repository.run,
        source_child=repository.child,
        source_attempt_id=ATTEMPT_ID,
        successor_run_spec=successor_run_spec,
        successor_child_spec=successor_child_spec,
        successor_attempt_spec=successor_attempt_spec,
        source_lineage=lineage,
    )

    assert published.prediction_path.is_file()
    assert published.artifact_manifest["recovery_source"] == {
        "source_run_id": RUN_ID,
        "source_child_id": CHILD_ID,
        "source_attempt_id": ATTEMPT_ID,
    }
    assert published.artifact_manifest["recovery_materializer_identity_hash"] == (
        artifact_manifest_hash_for(lineage["recovery_materializer_identity"])
    )
    recovered = pd.read_pickle(published.prediction_path)
    assert not recovered.empty


def test_builtin_rematerialize_rejects_changed_prediction_source_bytes(tmp_path: Path) -> None:
    (
        adapter,
        repository,
        source_paths,
        successor_run_spec,
        successor_child_spec,
        successor_attempt_spec,
        lineage,
    ) = _rematerialize_fixture(tmp_path)
    source_paths["a1"].write_bytes(b"mutated-after-preview")

    with pytest.raises(DurableExecutionAdapterError) as caught:
        adapter.stage_rematerialized_recovery_artifacts(
            source_run=repository.run,
            source_child=repository.child,
            source_attempt_id=ATTEMPT_ID,
            successor_run_spec=successor_run_spec,
            successor_child_spec=successor_child_spec,
            successor_attempt_spec=successor_attempt_spec,
            source_lineage=lineage,
        )

    assert caught.value.reason_code == "backtest_prediction_hash_mismatch"


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


def test_published_manifest_rejects_path_escape_before_reading_external_file(
    tmp_path: Path,
) -> None:
    adapter, repository, _coordinator, _artifact_client, _used_nodes = _adapter(tmp_path)
    workspace = tmp_path / "manifest-scope"
    workspace.mkdir()
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"external")
    manifest = {
        "schema_version": "multi_alpha_child_artifact_manifest_v1",
        "run_id": RUN_ID,
        "child_id": CHILD_ID,
        "attempt_id": ATTEMPT_ID,
        "input_manifest_hash": repository.child["input_manifest_hash"],
        "prediction_file": "combined_prediction.pkl",
        "files": {
            "../outside.bin": {
                "sha256": _sha256(outside),
                "size": outside.stat().st_size,
            }
        },
        "execution_identity": None,
        "execution_identity_hash": None,
        "execution_identity_evidence": None,
    }
    manifest["manifest_hash"] = artifact_manifest_hash_for(manifest)

    with pytest.raises(DurableExecutionAdapterError) as caught:
        adapter._verify_published_manifest(
            manifest,
            workspace=workspace,
            expected_input_manifest_hash=repository.child["input_manifest_hash"],
        )

    assert caught.value.reason_code == "multi_alpha_artifact_manifest_invalid"


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
    remote_payload = remote_coordinator.calls[0]["payload"]
    assert remote_payload.task_id == remote_intent.qe_task_id
    assert (
        f"/home/node/workspaces/{remote_intent.qe_task_id}/Loop1"
        in remote_payload.wsl_command
    )
    fallback_task_id = (
        f"macb_remote_{remote_published.workspace.parent.name}_"
        f"{remote_published.workspace.name}"
    )
    assert fallback_task_id not in remote_payload.wsl_command
    assert len(artifact_client.paths) + len(remote_artifact_client.paths) == 4


def test_durable_submit_freezes_oversized_runtime_artifact_cas_binding(tmp_path: Path) -> None:
    adapter, repository, coordinator, artifact_client, _used_nodes = _adapter(tmp_path)
    repository.attempt["node_id"] = "rdagent-node1"
    runtime_overlay = tmp_path / "runtime" / "qe_sector_risk_overlay.parquet"
    runtime_overlay.write_bytes(b"r" * (8 * 1024 * 1024))
    materialized = adapter.materialize_child_input(
        run_id=RUN_ID,
        child_id=CHILD_ID,
        attempt_id=ATTEMPT_ID,
    )
    published = adapter.publish_artifacts(materialized)
    nested_runtime = published.workspace / "aistock_models" / "model.py"
    nested_runtime.parent.mkdir()
    nested_runtime.write_text("VALUE = 1\n", encoding="utf-8")
    (nested_runtime.parent / "__init__.py").write_bytes(b"")
    intent = adapter.prepare_submission_intent(
        run=repository.run,
        child=repository.child,
        attempt=repository.attempt,
        node_id="rdagent-node1",
    )
    token = OwnershipToken(owner_id="worker_1", fencing_token=1, row_version=1)

    outcome = asyncio.run(
        adapter.submit(
            artifacts=published,
            intent=intent,
            attempt_token=token,
        )
    )

    published_overlay = published.workspace / runtime_overlay.name
    expected_sha = _sha256(published_overlay)
    assert outcome.state == "submitted"
    assert artifact_client.paths == [
        published.workspace / "combined_factors_df.parquet",
        published.prediction_path,
        nested_runtime,
        published_overlay,
    ]
    payload = coordinator.calls[0]["payload"]
    bindings = payload.config["runtime_artifact_bindings"]
    overlay_binding = next(item for item in bindings if item["name"] == runtime_overlay.name)
    assert overlay_binding["remote_path"] == f"/remote/artifacts/{expected_sha}"
    assert f"{runtime_overlay.name}.b64" not in payload.experiment_files
    assert "sha256sum --" in payload.wsl_command
    assert f"/remote/artifacts/{expected_sha}" in payload.wsl_command
    claimed_manifest = repository.claim_calls[0]["artifact_manifest"]
    assert claimed_manifest["remote_runtime_artifact_bindings"] == bindings
    runtime_manifest = claimed_manifest["remote_runtime_file_manifest"]
    runtime_entries = {item["path"]: item for item in runtime_manifest["files"]}
    assert runtime_entries[runtime_overlay.name]["sha256"] == expected_sha
    assert runtime_entries[runtime_overlay.name]["transfer"] == "cas"
    assert runtime_entries["aistock_models/model.py"]["transfer"] == "cas"
    assert runtime_entries["aistock_models/__init__.py"]["transfer"] == "empty_file"
    assert claimed_manifest["manifest_hash"] == artifact_manifest_hash_for(
        {key: value for key, value in claimed_manifest.items() if key != "manifest_hash"}
    )
    assert "remote_runtime_artifact_bindings" not in published.artifact_manifest
    repository.attempt["artifact_manifest_json"] = claimed_manifest
    collected = asyncio.run(adapter.collect_result(intent=intent, artifacts=published))
    assert collected.result_manifest["submission_artifact_manifest_hash"] == claimed_manifest["manifest_hash"]
    assert collected.result_manifest["remote_runtime_file_manifest"] == runtime_manifest
    assert collected.result_manifest["remote_runtime_artifact_bindings"] == bindings


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
