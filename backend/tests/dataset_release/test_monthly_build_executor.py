from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import hashlib

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
import backend.services.dataset_release.monthly_build_executor as executor_module
from backend.services.dataset_release.monthly_build_bridge import CompiledMonthlyBuild
from backend.services.dataset_release.monthly_build_executor import (
    MonthlyBuildExecutorError,
    PhysicalBuildResult,
    SealedMonthlyBuildExecutor,
)
from backend.services.dataset_release.monthly_official_adapters import StageWorkload
from backend.services.dataset_release.monthly_unified import COMPONENTS
from backend.services.dataset_release.monthly_worker import ProducerContext


def _context(target: Path) -> ProducerContext:
    actions = {name: "COMPONENT_REBUILD" for name in COMPONENTS}
    return ProducerContext(
        stage="BUILD",
        operation_id="dmr_" + "1" * 32,
        attempt=1,
        request={},
        plan={
            "candidate_root": str(target),
            "target_cutoff": "2026-09-30",
            "release_id": "qe_hmm_full_v2_20260930",
            "revision": "20260930-monthly-v2",
            "predecessor": {"dataset_manifest_sha256": "a" * 64},
        },
        prior_receipts={"SOURCE": {"scope": {"component_actions": actions}}},
    )


class _Runner:
    def __init__(self, *, database_read: bool = False) -> None:
        self.database_read = database_read
        self.seen: CompiledMonthlyBuild | None = None

    def execute(self, *, context, staging_root, compiled):  # type: ignore[no-untyped-def]
        self.seen = compiled
        staging_root.mkdir()
        manifest = staging_root / "qe_dataset_manifest.json"
        component = staging_root / "component.bin"
        component.write_bytes(b"component")
        components = {
            "component": {
                "path": "component.bin",
                "sha256": hashlib.sha256(component.read_bytes()).hexdigest(),
                "size": component.stat().st_size,
            }
        }
        value = {
            "schema_version": "qe_dataset_manifest_v1",
            "release_id": context.plan["release_id"],
            "revision": context.plan["revision"],
            "cutoff_trade_date": context.plan["target_cutoff"],
            "components": components,
        }
        value["dataset_manifest_sha256"] = hashlib.sha256(
            canonical_json_bytes(value)
        ).hexdigest()
        manifest.write_bytes(canonical_json_bytes(value) + b"\n")
        return PhysicalBuildResult(
            manifest_path=manifest,
            component_artifacts=(component,),
            workload=StageWorkload(computed_rows=3),
            database_read_performed=self.database_read,
        )


class _InvalidManifestRunner:
    def execute(self, *, context, staging_root, compiled):  # type: ignore[no-untyped-def]
        del context, compiled
        staging_root.mkdir()
        manifest = staging_root / "qe_dataset_manifest.json"
        component = staging_root / "component.bin"
        manifest.write_bytes(canonical_json_bytes({}) + b"\n")
        component.write_bytes(b"component")
        return PhysicalBuildResult(
            manifest_path=manifest,
            component_artifacts=(component,),
        )


def _compiled(tmp_path: Path) -> CompiledMonthlyBuild:
    bundle = tmp_path / "artifacts" / "bundle.json"
    bundle.parent.mkdir(exist_ok=True)
    bundle.write_text("{}\n", encoding="utf-8")
    return CompiledMonthlyBuild(
        source_bundle_path=bundle,
        source_bundle_sha256="b" * 64,
        source_stage_receipt_ref=SimpleNamespace(),
        monthly_actions={name: "COMPONENT_REBUILD" for name in COMPONENTS},
        physical_plan={"schema_version": "test"},
    )


def test_executor_publishes_one_candidate_from_sealed_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_root = tmp_path / "releases"
    artifact_root = tmp_path / "artifacts"
    release_root.mkdir()
    artifact_root.mkdir()
    target = release_root / "20260930-candidate"
    compiled = _compiled(tmp_path)
    monkeypatch.setattr(
        executor_module,
        "compile_initial_monthly_build",
        lambda **_kwargs: compiled,
    )
    runner = _Runner()
    executor = SealedMonthlyBuildExecutor(
        profile=SimpleNamespace(),
        cas=SimpleNamespace(),
        artifact_roots=(artifact_root,),
        controller_release_root=release_root,
        runner=runner,
    )
    actions = {name: "COMPONENT_REBUILD" for name in COMPONENTS}

    result = executor.execute(_context(target), component_actions=actions)

    assert runner.seen is compiled
    assert result.manifest_path == target / "qe_dataset_manifest.json"
    assert result.component_artifacts == (target / "component.bin",)
    assert result.input_artifacts == (compiled.source_bundle_path,)
    assert result.workload.computed_rows == 3


def test_executor_rejects_existing_candidate(
    tmp_path: Path,
) -> None:
    release_root = tmp_path / "releases"
    artifact_root = tmp_path / "artifacts"
    release_root.mkdir()
    artifact_root.mkdir()
    target = release_root / "existing"
    target.mkdir()
    executor = SealedMonthlyBuildExecutor(
        profile=SimpleNamespace(),
        cas=SimpleNamespace(),
        artifact_roots=(artifact_root,),
        controller_release_root=release_root,
        runner=_Runner(),
    )

    with pytest.raises(MonthlyBuildExecutorError, match="already exists"):
        executor.execute(
            _context(target),
            component_actions={name: "COMPONENT_REBUILD" for name in COMPONENTS},
        )


def test_executor_rejects_database_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_root = tmp_path / "releases"
    artifact_root = tmp_path / "artifacts"
    release_root.mkdir()
    artifact_root.mkdir()
    target = release_root / "candidate"
    monkeypatch.setattr(
        executor_module,
        "compile_initial_monthly_build",
        lambda **_kwargs: _compiled(tmp_path),
    )
    executor = SealedMonthlyBuildExecutor(
        profile=SimpleNamespace(),
        cas=SimpleNamespace(),
        artifact_roots=(artifact_root,),
        controller_release_root=release_root,
        runner=_Runner(database_read=True),
    )

    with pytest.raises(MonthlyBuildExecutorError, match="must not access a database"):
        executor.execute(
            _context(target),
            component_actions={name: "COMPONENT_REBUILD" for name in COMPONENTS},
        )
    assert not target.exists()


def test_executor_validates_manifest_before_atomic_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_root = tmp_path / "releases"
    artifact_root = tmp_path / "artifacts"
    release_root.mkdir()
    artifact_root.mkdir()
    target = release_root / "candidate"
    monkeypatch.setattr(
        executor_module,
        "compile_initial_monthly_build",
        lambda **_kwargs: _compiled(tmp_path),
    )
    executor = SealedMonthlyBuildExecutor(
        profile=SimpleNamespace(),
        cas=SimpleNamespace(),
        artifact_roots=(artifact_root,),
        controller_release_root=release_root,
        runner=_InvalidManifestRunner(),
    )

    with pytest.raises(RuntimeError, match="manifest schema differs"):
        executor.execute(
            _context(target),
            component_actions={name: "COMPONENT_REBUILD" for name in COMPONENTS},
        )
    assert not target.exists()
