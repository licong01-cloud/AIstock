from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.util
import sys
import types
from pathlib import Path

import pytest

from backend.services.quantevolver.long_trend_artifact_resolver import (
    resolve_long_trend_recorder_artifacts,
)
from backend.services.quantevolver.long_trend_evaluation_bundle import (
    BUNDLE_SOURCE_PATHS,
    build_long_trend_evaluator_bundle,
)
from backend.services.quantevolver.long_trend_evaluation_contract import (
    QEDatasetSnapshotIdentity,
    QELongTrendError,
    QELongTrendReason,
)
from backend.services.quantevolver.qe_workspace_client import QELongTrendWorkspaceError, QEWorkspaceClient


def _environment(digest: str = "a" * 64) -> dict[str, object]:
    return {
        "execution_environment_snapshot_id": "qeenv_fixture",
        "execution_environment_manifest_sha256": digest,
        "manifest": {
            "python": {
                "implementation": "CPython",
                "version": "3.10.0",
                "cache_tag": "cpython-310",
            },
        },
    }


def _catalog(*, frequency: str = "1day", completeness: str = "complete") -> dict[str, object]:
    prefix = "mlruns/exp-1/rec-1/artifacts"
    paths = (
        f"{prefix}/pred.pkl",
        f"{prefix}/label.pkl",
        f"{prefix}/portfolio_analysis/report_normal_1day.pkl",
        f"{prefix}/portfolio_analysis/positions_normal_1day.pkl",
        f"{prefix}/portfolio_analysis/indicators_normal_{frequency}.pkl",
        f"{prefix}/portfolio_analysis/indicators_normal_{frequency}_obj.pkl",
    )
    return {
        "schema_version": "qe_workspace_catalog_v1",
        "task_id": "task-1",
        "loop_name": "Loop3",
        "catalog_completeness": completeness,
        "warnings": [],
        "files": [
            {
                "relative_path": path,
                "sha256": hashlib.sha256(path.encode()).hexdigest(),
                "size_bytes": len(path),
            }
            for path in paths
        ],
    }


def test_bundle_is_exact_allowlisted_environment_bound_and_deterministic() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    first = build_long_trend_evaluator_bundle(repo_root=repo_root, execution_environment=_environment())
    second = build_long_trend_evaluator_bundle(repo_root=repo_root, execution_environment=_environment())

    assert first == second
    assert set(first.files) == {*BUNDLE_SOURCE_PATHS, "bundle_manifest.json"}
    assert first.manifest["bundle_sha256"] == first.bundle_sha256
    assert first.execution_environment_manifest_sha256 == "a" * 64

    changed = build_long_trend_evaluator_bundle(
        repo_root=repo_root,
        execution_environment=_environment("b" * 64),
    )
    assert changed.bundle_sha256 != first.bundle_sha256

    with pytest.raises(QELongTrendError) as exc_info:
        build_long_trend_evaluator_bundle(
            repo_root=repo_root,
            execution_environment={**_environment(), "execution_environment_manifest_sha256": "bad"},
        )
    assert exc_info.value.reason_code == QELongTrendReason.EXECUTION_ENVIRONMENT_MISMATCH.value


def test_worker_snapshot_identity_normalizes_json_lineage_without_weakening_content_check(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    worker_module = "backend.services.quantevolver.long_trend_worker_entry"
    if importlib.util.find_spec("resource") is None:
        monkeypatch.setitem(sys.modules, "resource", types.ModuleType("resource"))
        request.addfinalizer(lambda: sys.modules.pop(worker_module, None))
    worker_entry = importlib.import_module(worker_module)
    actual = QEDatasetSnapshotIdentity(
        snapshot_id="snapshot-v1",
        manifest_sha256="a" * 64,
        start_date="2018-08-01",
        end_date="2026-06-30",
        lineage_parent_ids=(),
    )
    expected = {
        "snapshot_id": "snapshot-v1",
        "manifest_sha256": "a" * 64,
        "start_date": "2018-08-01",
        "end_date": "2026-06-30",
        "lineage_parent_ids": [],
    }

    worker_entry._require_snapshot_identity(expected, actual, "feature")

    with pytest.raises(ValueError, match="snapshot identity differs"):
        worker_entry._require_snapshot_identity(
            {**expected, "lineage_parent_ids": ["different-parent"]},
            actual,
            "feature",
        )

    with pytest.raises(ValueError, match="lineage_parent_ids must be an array"):
        worker_entry._require_snapshot_identity(
            {**expected, "lineage_parent_ids": "not-an-array"},
            actual,
            "feature",
        )


def test_resolver_freezes_frequency_and_keeps_summary_and_object_distinct() -> None:
    inventory = resolve_long_trend_recorder_artifacts(
        task_id="task-1",
        loop_id="Loop3",
        recorder_ref={"experiment_id": "exp-1", "recorder_id": "rec-1"},
        catalog=_catalog(),
        backtest_freq="1day",
    )

    assert inventory.artifacts["prediction"]["relative_path"].endswith("/pred.pkl")
    assert inventory.artifacts["indicator_summary"]["relative_path"].endswith("indicators_normal_1day.pkl")
    assert inventory.artifacts["indicator_object"]["relative_path"].endswith("indicators_normal_1day_obj.pkl")
    assert inventory.artifacts["indicator_summary"] != inventory.artifacts["indicator_object"]
    assert len(inventory.input_manifest_sha256) == 64

    with pytest.raises(QELongTrendError) as exc_info:
        resolve_long_trend_recorder_artifacts(
            task_id="task-1",
            loop_id="Loop3",
            recorder_ref={"experiment_id": "exp-1", "recorder_id": "rec-1"},
            catalog=_catalog(frequency="5min"),
            backtest_freq="1day",
        )
    assert exc_info.value.reason_code == QELongTrendReason.INDICATOR_FREQUENCY_CONFLICT.value


def test_incomplete_base_manifest_can_still_carry_exact_long_trend_snapshot() -> None:
    snapshot = {
        "snapshot_id": "qlib-st-pit-active-h5-daily-20180801-20260630",
        "manifest_sha256": "c" * 64,
        "start_date": "2018-08-01",
        "end_date": "2026-06-30",
        "lineage_parent_ids": [],
        "files": {"daily_pv.h5": {"sha256": "d" * 64, "size": 1}},
    }
    parsed = QEWorkspaceClient._parse_dataset_identity(
        {
            "schema_version": "qe_dataset_identity_evidence_v1",
            "complete": False,
            "reason_code": "qe_dataset_manifest_missing",
            "missing": ["qe_dataset_manifest.json"],
            "acquisition_suggestions": ["publish the legacy manifest"],
            "dataset": None,
            "long_trend_snapshot": snapshot,
            "long_trend_snapshot_reason": None,
        }
    )

    assert parsed.complete is False
    assert parsed.long_trend_snapshot == snapshot
    assert parsed.reason_code == "qe_dataset_manifest_missing"


def test_job_receipt_parser_rejects_string_boolean_and_unknown_status() -> None:
    client = object.__new__(QEWorkspaceClient)
    base = {
        "schema_version": "qe_long_trend_job_receipt_v1",
        "task_id": "task-1",
        "loop_id": "Loop1",
        "evaluation_id": "qelt_" + "a" * 64,
        "job_id": "job-1",
        "request_sha": "b" * 64,
        "status": "queued",
        "duplicate_replay": False,
        "current_attempt_id": None,
        "execution_environment_snapshot_id": "qeenv-fixture",
        "execution_environment_manifest_sha256": "c" * 64,
    }
    parsed = client._parse_long_trend_job_receipt(
        base,
        task_id="task-1",
        loop_id="Loop1",
        evaluation_id="qelt_" + "a" * 64,
    )
    assert parsed.duplicate_replay is False

    with pytest.raises(QELongTrendWorkspaceError):
        client._parse_long_trend_job_receipt(
            {**base, "duplicate_replay": "false"},
            task_id="task-1",
            loop_id="Loop1",
            evaluation_id="qelt_" + "a" * 64,
        )
    with pytest.raises(QELongTrendWorkspaceError):
        client._parse_long_trend_job_receipt(
            {**base, "status": "research_rejected"},
            task_id="task-1",
            loop_id="Loop1",
            evaluation_id="qelt_" + "a" * 64,
        )


def test_stream_publish_is_durable_and_replaces_only_after_complete_file(tmp_path: Path) -> None:
    source = tmp_path / "artifact.partial"
    target = tmp_path / "artifact.parquet"
    source.write_bytes(b"complete-artifact")
    asyncio.run(QEWorkspaceClient._durable_replace_stream(source, target))
    assert target.read_bytes() == b"complete-artifact"
    assert not source.exists()
