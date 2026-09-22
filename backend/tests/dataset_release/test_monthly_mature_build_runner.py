from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

import backend.services.dataset_release.monthly_mature_build_runner as runner_module
from backend.services.dataset_release.cas_store import CASStore
from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.monthly_build_bridge import CompiledMonthlyBuild
from backend.services.dataset_release.monthly_build_executor import PhysicalBuildResult
from backend.services.dataset_release.monthly_mature_build_runner import (
    MatureMonthlyPhysicalBuildRunner,
    MonthlyBuildExecutionTools,
    MonthlyConsumerSmokeResult,
    MonthlyMatureBuildError,
)
from backend.services.dataset_release.monthly_worker import ProducerContext


def _context() -> ProducerContext:
    return ProducerContext(
        stage="BUILD",
        operation_id="dmr_" + "1" * 32,
        attempt=2,
        request={},
        plan={"release_id": "qe_hmm_full_v2_20260930", "target_cutoff": "2026-09-30"},
        prior_receipts={},
    )


def _compiled() -> CompiledMonthlyBuild:
    release_digest = digest_named_fields(
        "aistock_monthly_physical_release_v1",
        {
            "release_id": "qe_hmm_full_v2_20260930",
            "target_cutoff": "2026-09-30",
            "source_bundle_sha256": "a" * 64,
            "action_plan_digest": "b" * 64,
        },
    )
    return CompiledMonthlyBuild(
        source_bundle_path=Path("bundle.json"),
        source_bundle_sha256="a" * 64,
        source_stage_receipt_ref=SimpleNamespace(),
        monthly_actions={},
        physical_plan={
            "action_plan_digest": "b" * 64,
            "release_digest": release_digest,
            "build_inputs": {},
        },
    )


def _cas(tmp_path: Path) -> CASStore:
    store = ControlStore.initialize(tmp_path / "control")
    return CASStore(store.root)


class _Writer:
    def __init__(self) -> None:
        self.operations: list[str] = []

    def execute(self, *, context, staging_root, operation):  # type: ignore[no-untyped-def]
        del context, staging_root
        self.operations.append(operation["operation_id"])
        return {"kind": "supervised", "operation_id": operation["operation_id"]}


class _Smoke:
    def execute(self, *, context, staging_root, prepare_result, release_digest):  # type: ignore[no-untyped-def]
        del context, staging_root, prepare_result
        return MonthlyConsumerSmokeResult(
            semantic_receipt={"status": "PASS", "release_digest": release_digest},
            resource_receipt={"schema_version": "supervised"},
        )


class _Finalizer:
    def __init__(self) -> None:
        self.refs: set[str] = set()

    def execute(self, *, context, staging_root, compiled, validation_result, stage_refs):  # type: ignore[no-untyped-def]
        del context, compiled, validation_result
        self.refs = set(stage_refs)
        manifest = staging_root / "qe_dataset_manifest.json"
        component = staging_root / "component.bin"
        manifest.write_bytes(b"manifest")
        component.write_bytes(b"component")
        return PhysicalBuildResult(manifest_path=manifest, component_artifacts=(component,))


def test_runner_executes_mature_stages_in_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stages: list[tuple[str, set[str]]] = []

    def run(invocation):  # type: ignore[no-untyped-def]
        stages.append((invocation.stage, set(invocation.prerequisites)))
        value = {
            "schema_version": "dataset_release_build_stage_result_v1",
            "stage": invocation.stage,
            "status": "PASS",
        }
        if invocation.stage == "prepare":
            value["qlib_dump_operations"] = [
                {"operation_id": "daily"},
                {"operation_id": "minute"},
            ]
        return value

    monkeypatch.setattr(runner_module, "run_build_stage", run)
    writer = _Writer()
    finalizer = _Finalizer()
    staging = tmp_path / ".staging" / "candidate.building"
    staging.parent.mkdir()
    staging.mkdir()
    runner = MatureMonthlyPhysicalBuildRunner(
        profile=SimpleNamespace(
            stage_timeouts_seconds={"full_build": 3600},
            candidate_root=tmp_path,
        ),
        cas=_cas(tmp_path),
        project_root=tmp_path,
        qlib_writer=writer,
        consumer_smoke=_Smoke(),
        finalizer=finalizer,
    )

    result = runner.execute(context=_context(), staging_root=staging, compiled=_compiled())

    assert writer.operations == ["daily", "minute"]
    assert [stage for stage, _ in stages] == ["prepare", "finalize-bins", "validate"]
    assert stages[1][1] == {"prepare", "qlib_dump_daily", "qlib_dump_minute"}
    assert stages[2][1] == {
        "prepare",
        "qlib_dump_daily",
        "qlib_dump_minute",
        "finalize_bins",
        "consumer_smoke",
    }
    assert finalizer.refs == stages[2][1] | {"consumer_smoke_resource", "validate"}
    assert result.manifest_path == staging / "qe_dataset_manifest.json"


def test_runner_creates_attempt_bound_execution_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(invocation):  # type: ignore[no-untyped-def]
        value = {
            "schema_version": "dataset_release_build_stage_result_v1",
            "stage": invocation.stage,
            "status": "PASS",
        }
        if invocation.stage == "prepare":
            value["qlib_dump_operations"] = [{"operation_id": "daily"}]
        return value

    monkeypatch.setattr(runner_module, "run_build_stage", run)
    scoped_writer = _Writer()
    scoped_smoke = _Smoke()
    lifecycle: list[tuple[str, int]] = []

    @contextmanager
    def scope(context):  # type: ignore[no-untyped-def]
        lifecycle.append(("enter", context.attempt))
        try:
            yield MonthlyBuildExecutionTools(scoped_writer, scoped_smoke)
        finally:
            lifecycle.append(("exit", context.attempt))

    finalizer = _Finalizer()
    staging = tmp_path / ".staging" / "candidate.building"
    staging.parent.mkdir()
    staging.mkdir()
    runner = MatureMonthlyPhysicalBuildRunner(
        profile=SimpleNamespace(
            stage_timeouts_seconds={"full_build": 3600},
            candidate_root=tmp_path,
        ),
        cas=_cas(tmp_path),
        project_root=tmp_path,
        finalizer=finalizer,
        execution_scope_factory=scope,
    )

    runner.execute(context=_context(), staging_root=staging, compiled=_compiled())

    assert lifecycle == [("enter", 2), ("exit", 2)]
    assert scoped_writer.operations == ["daily"]


def test_runner_rejects_process_scoped_and_attempt_scoped_tools(tmp_path: Path) -> None:
    with pytest.raises(MonthlyMatureBuildError, match="exactly one"):
        MatureMonthlyPhysicalBuildRunner(
            profile=SimpleNamespace(
                stage_timeouts_seconds={"full_build": 3600},
                candidate_root=tmp_path,
            ),
            cas=_cas(tmp_path),
            project_root=tmp_path,
            qlib_writer=_Writer(),
            consumer_smoke=_Smoke(),
            finalizer=_Finalizer(),
            execution_scope_factory=lambda _context: pytest.fail("not entered"),
        )


def test_runner_rejects_non_pass_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner_module,
        "run_build_stage",
        lambda invocation: {
            "schema_version": "dataset_release_build_stage_result_v1",
            "stage": invocation.stage,
            "status": "FAILED",
        },
    )
    runner = MatureMonthlyPhysicalBuildRunner(
        profile=SimpleNamespace(
            stage_timeouts_seconds={"full_build": 3600},
            candidate_root=tmp_path,
        ),
        cas=_cas(tmp_path),
        project_root=tmp_path,
        qlib_writer=_Writer(),
        consumer_smoke=_Smoke(),
        finalizer=_Finalizer(),
    )
    (tmp_path / ".staging").mkdir()

    with pytest.raises(MonthlyMatureBuildError, match="prepare"):
        runner.execute(
            context=_context(),
            staging_root=tmp_path / ".staging" / "candidate.building",
            compiled=_compiled(),
        )


def test_runner_rejects_bridge_release_digest_drift(tmp_path: Path) -> None:
    runner = MatureMonthlyPhysicalBuildRunner(
        profile=SimpleNamespace(
            stage_timeouts_seconds={"full_build": 3600},
            candidate_root=tmp_path,
        ),
        cas=_cas(tmp_path),
        project_root=tmp_path,
        qlib_writer=_Writer(),
        consumer_smoke=_Smoke(),
        finalizer=_Finalizer(),
    )
    staging = tmp_path / ".staging" / "candidate.building"
    staging.parent.mkdir()
    compiled = _compiled()
    drifted = CompiledMonthlyBuild(
        source_bundle_path=compiled.source_bundle_path,
        source_bundle_sha256=compiled.source_bundle_sha256,
        source_stage_receipt_ref=compiled.source_stage_receipt_ref,
        monthly_actions=compiled.monthly_actions,
        physical_plan={**compiled.physical_plan, "release_digest": "f" * 64},
    )

    with pytest.raises(MonthlyMatureBuildError, match="release digest differs"):
        runner.execute(context=_context(), staging_root=staging, compiled=drifted)
