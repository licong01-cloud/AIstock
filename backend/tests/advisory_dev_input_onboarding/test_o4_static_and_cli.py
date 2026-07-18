from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys
import pytest

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    O4ArtifactKind,
    O4_ARTIFACT_STORE_POLICY_HASH,
)
from backend.services.advisory_dev_input_onboarding.phase1e_inputs import Phase1EInputArtifactStore
from backend.services.advisory_dev_input_onboarding.phase1e_orchestration import AdvisoryPhase1EOrchestrationService
from backend.services.advisory_phase1.source_capacity import Phase1ECapacityPolicyV1
from backend.services.advisory_phase1.source_observer import registered_source_observer_configs
from scripts import advisory_real_dev_onboarding as authoritative_cli


ROOT = Path(__file__).resolve().parents[3]
PHASE1E_MODULES = (
    ROOT / "backend/services/advisory_dev_input_onboarding/phase1e_derived_pit.py",
    ROOT / "backend/services/advisory_dev_input_onboarding/phase1e_input_builder.py",
    ROOT / "backend/services/advisory_dev_input_onboarding/phase1e_inputs.py",
    ROOT / "backend/services/advisory_dev_input_onboarding/phase1e_source_mapping.py",
)
ORCHESTRATION_MODULE = ROOT / "backend/services/advisory_dev_input_onboarding/phase1e_orchestration.py"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    names.update(node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom))
    return names


def test_phase1e_modules_do_not_import_shared_runtime_or_consumer_domains() -> None:
    forbidden = (
        "backend.services.strategy_package",
        "backend.services.selection_center",
        "backend.services.simulation_runtime",
        "backend.services.paper_trading",
        "backend.services.miniqmt_execution_runtime",
        "backend.inference_engine",
        "backend.services.quantevolver",
        "backend.services.rdagent",
        "backend.qlib_exporter",
        "backend.infra.qmt_client",
    )
    for path in PHASE1E_MODULES:
        imports = _imports(path)
        assert not {name for name in imports if name.startswith(forbidden)}


def test_o4_files_contain_no_unrequested_gate_approval_or_production_switch() -> None:
    paths = (
        *PHASE1E_MODULES,
        ORCHESTRATION_MODULE,
        ROOT / "backend/services/strategy_package/advisory_input_projection.py",
        ROOT / "scripts/advisory_phase1_source_observer.py",
        ROOT / "scripts/advisory_phase1e_readiness_plan.py",
        ROOT / "scripts/advisory_real_dev_onboarding.py",
    )
    source = "\n".join(path.read_text(encoding="utf-8").lower() for path in paths)
    forbidden = (
        "approved_by",
        "approval_status",
        "manual_approval",
        "acknowledgement",
        "source_observer_enabled",
        "--target-db",
        "--prod",
        "backup_gate",
        "force_gate",
        "skip_gate",
    )
    assert all(token not in source for token in forbidden)


def test_o4_orchestration_does_not_import_selection_simulation_paper_or_trading_runtime() -> None:
    imports = _imports(ORCHESTRATION_MODULE)
    forbidden = (
        "backend.services.strategy_package",
        "backend.services.selection_center",
        "backend.services.simulation_runtime",
        "backend.services.paper_trading",
        "backend.services.miniqmt_execution_runtime",
        "backend.inference_engine",
        "backend.infra.qmt_client",
    )
    assert not {name for name in imports if name.startswith(forbidden)}


def test_authoritative_cli_injects_standalone_package_projection_provider() -> None:
    service = authoritative_cli._o4_service()

    assert service.package_projection_provider is authoritative_cli._project_admitted_package_inputs


def test_authoritative_cli_exposes_all_four_o4_commands_and_compat_has_no_target_selector() -> None:
    authoritative = subprocess.run(
        [sys.executable, str(ROOT / "scripts/advisory_real_dev_onboarding.py"), "--help"],
        cwd=ROOT,
        capture_output=True,
        check=False,
    )
    assert authoritative.returncode == 0
    for command in ("observe-source", "build-phase1e-inputs", "plan-capacity", "compile-phase1e"):
        assert command.encode("ascii") in authoritative.stdout
    compatibility = subprocess.run(
        [sys.executable, str(ROOT / "scripts/advisory_phase1e_readiness_plan.py"), "compile-batch", "--help"],
        cwd=ROOT,
        capture_output=True,
        check=False,
    )
    assert compatibility.returncode == 0
    assert b"--input-bundle-ref" in compatibility.stdout
    assert b"--target-db" not in compatibility.stdout


@pytest.mark.parametrize(
    ("command", "extra_args", "expected_code"),
    (
        (
            "observe-source",
            (
                "--historical-request-ref", "{ref}",
                "--capacity-policy", "{policy}",
                "--evidence-root", "{root}",
            ),
            0,
        ),
        (
            "build-phase1e-inputs",
            (
                "--historical-request-ref", "{ref}",
                "--historical-receipt-ref", "{ref}",
                "--observation-scope-ref", "{ref}",
                "--source-mapping-registry-ref", "{ref}",
                "--capacity-policy-ref", "{ref}",
                "--evidence-root", "{root}",
            ),
            0,
        ),
        ("plan-capacity", ("--input-bundle-ref", "{ref}",), 0),
        ("compile-phase1e", ("--input-bundle-ref", "{ref}",), 0),
    ),
)
def test_authoritative_cli_dispatches_each_o4_command(
    tmp_path: Path,
    monkeypatch,
    command: str,
    extra_args: tuple[str, ...],
    expected_code: int,
) -> None:
    ref = AdvisoryImmutableArtifactRef(
        artifact_kind="fixture",
        store_policy_hash=O4_ARTIFACT_STORE_POLICY_HASH,
        relative_path="fixture/aa/" + "a" * 64 + ".json",
        semantic_hash="a" * 64,
        file_sha256="b" * 64,
    )
    ref_path = tmp_path / "ref.json"
    ref_path.write_text(ref.model_dump_json(), encoding="utf-8")
    env_file = tmp_path / ".env"
    env_file.write_text("TDX_DB_DEV_HOST=unused\n", encoding="utf-8")
    capacity_policy = Phase1ECapacityPolicyV1(
        policy_id="phase1e_capacity_dev",
        policy_version="1",
        retained_snapshot_count=3,
        concurrent_build_count=1,
        staging_copy_count=1,
        parquet_target_file_bytes=128 * 1024 * 1024,
        memory_budget_bytes=8 * 1024 * 1024 * 1024,
        worker_memory_overheads={
            "arrow_builder_bytes": 256 * 1024 * 1024,
            "hash_buffer_bytes": 128 * 1024 * 1024,
            "verifier_bytes": 256 * 1024 * 1024,
        },
        orphan_reserve_bytes=1024 * 1024 * 1024,
        manifest_overhead_bytes_per_snapshot=1024 * 1024,
        parquet_measurement_snapshot_limit=5,
        parquet_measurement_file_limit=500,
    )
    policy_path = tmp_path / "capacity-policy.json"
    policy_path.write_text(capacity_policy.model_dump_json(), encoding="utf-8")
    calls: dict[str, dict[str, object]] = {}

    class FakeService:
        class ResultModel:
            def __init__(self, field_name: str, value: str) -> None:
                setattr(self, field_name, type("Value", (), {"value": value})())
                self._field_name = field_name
                self._value = value

            def model_dump(self, *, mode: str):
                return {self._field_name: self._value}

        def observe_source(self, **kwargs):
            calls["observe-source"] = kwargs
            return {"ok": True, "aggregate_status": "COMPLETE", "program_results": []}

        def build_phase1e_inputs(self, **kwargs):
            return {"ok": True, "bundle": self.ResultModel("aggregate_readiness", "ALL_FULL_READY")}

        def plan_capacity(self, **kwargs):
            return {"ok": True, "bundle": self.ResultModel("aggregate_readiness", "ALL_FULL_READY")}

        def compile_phase1e(self, **kwargs):
            return {"ok": True, "compile_receipt": self.ResultModel("aggregate_status", "COMPLETE")}

    monkeypatch.setattr(authoritative_cli, "_o4_service", lambda: FakeService())
    values = {
        "{ref}": str(ref_path),
        "{root}": str(tmp_path),
        "{policy}": str(policy_path),
    }
    argv = [command]
    argv.extend(values.get(item, item) for item in extra_args)
    argv.extend(("--env-file", str(env_file), "--artifact-root", str(tmp_path)))
    assert authoritative_cli.main(argv) == expected_code
    if command == "observe-source":
        observed_policy = calls[command]["capacity_policy"]
        assert isinstance(observed_policy, Phase1ECapacityPolicyV1)
        assert observed_policy.policy_hash == capacity_policy.policy_hash


def test_common_artifacts_publish_exact_typed_capacity_policy(tmp_path: Path) -> None:
    policy = Phase1ECapacityPolicyV1(
        policy_id="phase1e_capacity_dev",
        policy_version="1",
        retained_snapshot_count=3,
        concurrent_build_count=1,
        staging_copy_count=1,
        parquet_target_file_bytes=128 * 1024 * 1024,
        memory_budget_bytes=8 * 1024 * 1024 * 1024,
        worker_memory_overheads={
            "arrow_builder_bytes": 256 * 1024 * 1024,
            "hash_buffer_bytes": 128 * 1024 * 1024,
            "verifier_bytes": 256 * 1024 * 1024,
        },
        orphan_reserve_bytes=1024 * 1024 * 1024,
        manifest_overhead_bytes_per_snapshot=1024 * 1024,
        parquet_measurement_snapshot_limit=5,
        parquet_measurement_file_limit=500,
    )
    config = registered_source_observer_configs()[("phase1e_advisory_inputs_dev_v2", "v2")]
    store = Phase1EInputArtifactStore(root=tmp_path / "artifacts")
    refs = AdvisoryPhase1EOrchestrationService(repository_root=ROOT)._publish_common_artifacts(
        store=store,
        config=config,
        store_backend_root=tmp_path / "dataset-store",
        policy=None,
        capacity_policy=policy,
    )

    ref = refs["capacity_policy_ref"]
    assert ref.artifact_kind == O4ArtifactKind.CAPACITY_POLICY.value
    assert ref.semantic_hash == policy.policy_hash
    assert store.load(ref=ref, model_type=Phase1ECapacityPolicyV1) == policy


def test_strategy_package_projection_has_no_io_or_secondary_validation_calls() -> None:
    path = ROOT / "backend/services/strategy_package/advisory_input_projection.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports = _imports(path)
    forbidden_imports = (
        "repository",
        "asset_store",
        "validator",
        "health",
        "live_inference",
        "multi_alpha_live",
        "selection_center",
        "simulation",
        "paper_trading",
        "psycopg",
        "requests",
        "urllib",
    )
    assert not {name for name in imports if any(fragment in name for fragment in forbidden_imports)}

    forbidden_calls = {"open", "connect", "urlopen", "request", "read_text", "read_bytes", "write_text", "write_bytes"}
    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    called_names.update(
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    )
    assert called_names.isdisjoint(forbidden_calls)
