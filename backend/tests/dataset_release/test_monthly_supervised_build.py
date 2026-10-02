from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import backend.services.dataset_release.monthly_supervised_build as module
from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.daily_minute_materializer import QlibDumpToolchain
from backend.services.dataset_release.monthly_supervised_build import (
    MonthlySupervisedBuildError,
    SupervisedMonthlyConsumerSmoke,
    SupervisedMonthlyQlibWriter,
)
from backend.services.dataset_release.monthly_worker import ProducerContext


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _toolchain(tmp_path: Path) -> QlibDumpToolchain:
    files = []
    for name in ("dump.py", "guardian.py", "runner.py"):
        path = tmp_path / name
        path.write_text(name, encoding="utf-8")
        files.append(path)
    return QlibDumpToolchain(
        distro="Ubuntu-22.04",
        conda_sh="/opt/conda/etc/profile.d/conda.sh",
        conda_env="rdagent-gpu",
        dump_script_wsl="/mnt/f/work/dump.py",
        dump_script_windows=files[0],
        dump_script_sha256=_sha(files[0]),
        guardian_python="python3",
        guardian_script_wsl="/mnt/f/work/guardian.py",
        guardian_script_windows=files[1],
        guardian_script_sha256=_sha(files[1]),
        heartbeat_path_wsl="/unused",
        runner_python_wsl="python3",
        runner_script_wsl="/mnt/f/work/runner.py",
        runner_script_windows=files[2],
        runner_script_sha256=_sha(files[2]),
    )


def _profile(tmp_path: Path):  # type: ignore[no-untyped-def]
    return SimpleNamespace(
        profile="qe_hmm_full_v2",
        pressure_ladder={"dump_workers": [3]},
        stage_timeouts_seconds={"qlib_dump": 7200, "consumer": 3600},
        resource_policy=SimpleNamespace(validation_read_chunk_rows=1000),
        index_codes=("000001.SH",),
        candidate_root=tmp_path,
    )


def _context() -> ProducerContext:
    return ProducerContext(
        stage="BUILD",
        operation_id="dmr_" + "1" * 32,
        attempt=2,
        request={},
        plan={
            "release_id": "qe_hmm_full_v2_20260930",
            "target_cutoff": "2026-09-30",
        },
        prior_receipts={},
    )


def _receipt(execution_id: str, timeout: int) -> dict[str, object]:
    return {
        "schema_version": "dataset_supervised_execution_receipt_v1",
        "execution_id": execution_id,
        "runtime": "wsl",
        "timeout_seconds": float(timeout),
        "returncode": 0,
        "active_processes": 0,
        "wsl_readback": {"active_state": "inactive"},
        "log_segments": [],
        "resource_gate_receipt": {
            "final_status": "READY",
            "checkpoint_requested": False,
            "system_admission_thresholds_blocking": False,
            "data_scope_changed": False,
        },
    }


class _Supervisor:
    def __init__(self, root: Path, *, semantic: dict[str, object] | None = None) -> None:
        self.attempt_id = "dmr_" + "1" * 32 + "-attempt-2"
        self.fence = 2
        self.control_root = root
        heartbeat_root = root / "heartbeats"
        heartbeat_root.mkdir()
        self.heartbeat_path = heartbeat_root / "attempt.json"
        self.heartbeat_path.write_text("{}", encoding="utf-8")
        self.semantic = semantic
        self.calls: list[tuple[tuple[str, ...], dict[str, object]]] = []

    def run_supervised(self, command, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append((tuple(command), dict(kwargs)))
        execution_id = str(kwargs["execution_id"])
        if self.semantic is not None:
            result = (
                self.control_root
                / "attempt_runs"
                / f"{self.attempt_id}-{self.fence}"
                / execution_id
                / "semantic_result.json"
            )
            result.parent.mkdir(parents=True)
            result.write_text(json.dumps(self.semantic), encoding="utf-8")
        return _receipt(execution_id, int(float(kwargs["timeout_seconds"])))


def _operation(staging: Path) -> dict[str, object]:
    csv_root = staging / "daily_bin" / ".writer-private" / "daily" / "csv"
    csv_root.mkdir(parents=True)
    (csv_root / "batch_manifest.json").write_text("{}", encoding="utf-8")
    target = "daily_bin/.writer-private/daily/qlib"
    return {
        "operation_id": "daily",
        "dataset": "daily_bin",
        "mode": "batched_full",
        "component_action": "FULL_REBUILD",
        "csv_relative_path": "daily_bin/.writer-private/daily/csv",
        "qlib_relative_path": target,
        "writer_targets_digest": digest_named_fields(
            "dataset_release_qlib_dump_writer_targets_v1",
            {"dataset": "daily_bin", "mode": "batched_full", "target": target},
        ),
        "batch_manifest_identity": "a" * 64,
        "batch_manifest_sha256": "b" * 64,
    }


def test_monthly_writer_uses_only_supervised_wsl_boundary(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    staging = tmp_path / ".staging" / "candidate.building"
    staging.parent.mkdir()
    staging.mkdir()
    control = tmp_path / "control"
    control.mkdir()
    supervisor = _Supervisor(control)
    writer = SupervisedMonthlyQlibWriter(
        profile=_profile(tmp_path),
        project_root=project,
        toolchain=_toolchain(tmp_path),
        supervisor=supervisor,
    )

    receipt = writer.execute(
        context=_context(),
        staging_root=staging,
        operation=_operation(staging),
    )

    command, kwargs = supervisor.calls[0]
    assert command[:2] == ("bash", "-lc")
    assert "dataset_release_qlib_batched_dump.py" in command[2]
    assert kwargs["runtime"] == "wsl"
    assert kwargs["environment_scope"] == "build"
    assert kwargs["credential_env_keys"] == ()
    assert kwargs["pressure_rung"] == 0
    assert receipt["execution_id"] == "build-dump-daily"


def test_monthly_writer_rejects_operation_target_drift(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    staging = tmp_path / ".staging" / "candidate.building"
    staging.parent.mkdir()
    staging.mkdir()
    control = tmp_path / "control"
    control.mkdir()
    supervisor = _Supervisor(control)
    operation = _operation(staging)
    operation["writer_targets_digest"] = "0" * 64
    writer = SupervisedMonthlyQlibWriter(
        profile=_profile(tmp_path),
        project_root=project,
        toolchain=_toolchain(tmp_path),
        supervisor=supervisor,
    )

    with pytest.raises(MonthlySupervisedBuildError, match="target identity"):
        writer.execute(context=_context(), staging_root=staging, operation=operation)


def test_consumer_smoke_separates_semantic_and_resource_receipts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    scripts = project / "scripts"
    scripts.mkdir(parents=True)
    (scripts / "dataset_release_candidate_consumer_smoke.py").write_text("pass", encoding="utf-8")
    staging = tmp_path / ".staging" / "candidate.building"
    staging.parent.mkdir()
    staging.mkdir()
    control = tmp_path / "control"
    control.mkdir()
    semantic = {"schema_version": "fixture", "status": "PASS"}
    supervisor = _Supervisor(control, semantic=semantic)
    monkeypatch.setattr(module, "validate_candidate_consumer_smoke_receipt", lambda value, **_kwargs: dict(value))
    smoke = SupervisedMonthlyConsumerSmoke(
        profile=_profile(tmp_path),
        project_root=project,
        toolchain=_toolchain(tmp_path),
        supervisor=supervisor,
    )

    result = smoke.execute(
        context=_context(),
        staging_root=staging,
        prepare_result={"consumer_smoke_instrument": "000001.SZ"},
        release_digest="c" * 64,
    )

    command, kwargs = supervisor.calls[0]
    assert "dataset_release_candidate_consumer_smoke.py" in command[2]
    assert kwargs["environment_scope"] == "validation"
    assert result.semantic_receipt == semantic
    assert result.resource_receipt["execution_id"] == "build-consumer-smoke"


def test_supervised_receipt_fails_closed_on_resource_checkpoint(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    staging = tmp_path / ".staging" / "candidate.building"
    staging.parent.mkdir()
    staging.mkdir()
    control = tmp_path / "control"
    control.mkdir()
    supervisor = _Supervisor(control)
    original = supervisor.run_supervised

    def failed(command, **kwargs):  # type: ignore[no-untyped-def]
        value = dict(original(command, **kwargs))
        value["resource_gate_receipt"] = {
            **value["resource_gate_receipt"],
            "checkpoint_requested": True,
        }
        return value

    supervisor.run_supervised = failed  # type: ignore[method-assign]
    writer = SupervisedMonthlyQlibWriter(
        profile=_profile(tmp_path),
        project_root=project,
        toolchain=_toolchain(tmp_path),
        supervisor=supervisor,
    )

    with pytest.raises(MonthlySupervisedBuildError, match="receipt differs"):
        writer.execute(
            context=_context(),
            staging_root=staging,
            operation=_operation(staging),
        )
