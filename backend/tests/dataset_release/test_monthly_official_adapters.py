from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_official_adapters import (
    BuildExecution,
    ConsumerReadback,
    ConsumerValidationExecution,
    DeployExecution,
    DerivedAsset,
    DeriveExecution,
    LocalValidationExecution,
    NodeDeployment,
    OfficialBuildAdapter,
    OfficialConsumerValidateAdapter,
    OfficialDeployAdapter,
    OfficialDeriveAdapter,
    OfficialLocalValidateAdapter,
    OfficialMonthlyAdapterError,
    build_official_monthly_registry,
)
from backend.services.dataset_release.monthly_immutable_deploy import (
    ExistingTreeNodeTransport,
    ImmutableFilesystemNodeTransport,
    ImmutableMonthlyDeployExecutor,
    MonthlyImmutableDeployError,
)
from backend.services.dataset_release.monthly_hmm_derive import (
    FrozenHMMCoefficientAuthority,
    HMMCoefficientProduct,
    LocalPythonHMMCoefficientProcess,
    MonthlyHMMCoefficientExecutor,
    MonthlyHMMDeriveError,
    SHARED_HMM_COEFFICIENT_SCHEMA,
)
from backend.services.dataset_release.monthly_stage_adapter import CodeOwnedMonthlyStageProducer
from backend.services.dataset_release.monthly_source_producer import AuditedMonthlySourceProducer
from backend.services.dataset_release.monthly_unified import (
    COMPONENTS,
    CONSUMER_VALIDATION_BINDING_SCHEMA,
    REQUIRED_CONSUMERS,
    REQUIRED_NODES,
)
from backend.services.dataset_release.monthly_worker import (
    ProducerContext,
    RegisteredMonthlyPipeline,
)
from backend.services.dataset_release.profile_contract import (
    ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS,
)


_COMPONENT_BYTES = canonical_json_bytes({"status": "PASS"}) + b"\n"
_MANIFEST_UNSIGNED = {
    "schema_version": "qe_dataset_manifest_v1",
    "release_id": "qe_hmm_full_v2_20260930",
    "revision": "20260930-monthly-v2",
    "cutoff_trade_date": "2026-09-30",
    "components": {
        "component": {
            "path": "component.json",
            "sha256": hashlib.sha256(_COMPONENT_BYTES).hexdigest(),
            "size": len(_COMPONENT_BYTES),
        }
    },
}
MANIFEST = hashlib.sha256(canonical_json_bytes(_MANIFEST_UNSIGNED)).hexdigest()
PREDECESSOR = "a" * 64


def _write(path: Path, value: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) + b"\n")
    return path


def _ref(root: Path, path: Path) -> dict[str, Any]:
    return {
        "id": path.relative_to(root).as_posix(),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "size": path.stat().st_size,
    }


def _context(
    stage: str,
    *,
    source_scope: Mapping[str, Any] | None = None,
    build_scope: Mapping[str, Any] | None = None,
    derive_scope: Mapping[str, Any] | None = None,
    local_scope: Mapping[str, Any] | None = None,
    candidate_root: Path | None = None,
    target_roots: Mapping[str, str] | None = None,
) -> ProducerContext:
    scopes = {
        "SOURCE": source_scope,
        "BUILD": build_scope,
        "DERIVE": derive_scope,
        "LOCAL_VALIDATE": local_scope,
    }
    plan: dict[str, Any] = {
        "release_id": "qe_hmm_full_v2_20260930",
        "generation": "20261001-v1",
        "target_cutoff": "2026-09-30",
        "revision": "20260930-monthly-v2",
        "predecessor": {"dataset_manifest_sha256": PREDECESSOR},
    }
    if candidate_root is not None:
        plan["candidate_root"] = str(candidate_root)
        plan["profile_candidate"] = str(candidate_root.parent / "profile-v4.json")
    if target_roots is not None:
        plan["target_roots"] = dict(target_roots)
    return ProducerContext(
        stage=stage,
        operation_id="dmr_" + "1" * 32,
        attempt=1,
        request={},
        plan=plan,
        prior_receipts={name: {"scope": dict(scope)} for name, scope in scopes.items() if scope is not None},
    )


def _source_scope() -> dict[str, Any]:
    return {"component_actions": {name: "REUSE" for name in COMPONENTS}}


def _run_pipeline(root: Path, adapter, context: ProducerContext):  # type: ignore[no-untyped-def]
    producer = CodeOwnedMonthlyStageProducer(adapter)
    pipeline = RegisteredMonthlyPipeline(
        {
            stage: producer
            for stage in (
                "SOURCE",
                "BUILD",
                "DERIVE",
                "LOCAL_VALIDATE",
                "DEPLOY",
                "CONSUMER_VALIDATE",
            )
        },
        artifact_roots=(root,),
    )
    return pipeline.run_stage(
        stage=context.stage,
        operation_id=context.operation_id,
        attempt=context.attempt,
        request=context.request,
        plan=context.plan,
        prior_receipts=context.prior_receipts,
    )


@dataclass
class BuildExecutor:
    root: Path
    noncanonical: bool = False
    claimed_identity: str | None = None

    def execute(self, _context, *, component_actions):  # type: ignore[no-untyped-def]
        assert set(component_actions) == set(COMPONENTS)
        manifest = self.root / "candidate" / "qe_dataset_manifest.json"
        component = _write(self.root / "candidate" / "component.json", {"status": "PASS"})
        if self.noncanonical:
            manifest.parent.mkdir(parents=True, exist_ok=True)
            manifest.write_text('{"dataset_manifest_sha256": "' + MANIFEST + '"}\n', encoding="utf-8")
        else:
            value: dict[str, Any] = {
                "schema_version": "qe_dataset_manifest_v1",
                "release_id": "qe_hmm_full_v2_20260930",
                "revision": "20260930-monthly-v2",
                "cutoff_trade_date": "2026-09-30",
                "components": {
                    "component": {
                        "path": "component.json",
                        "sha256": hashlib.sha256(component.read_bytes()).hexdigest(),
                        "size": component.stat().st_size,
                    }
                },
            }
            value["dataset_manifest_sha256"] = hashlib.sha256(canonical_json_bytes(value)).hexdigest()
            if self.claimed_identity is not None:
                value["dataset_manifest_sha256"] = self.claimed_identity
            _write(manifest, value)
        return BuildExecution(manifest_path=manifest, component_artifacts=(component,))


def test_build_adapter_derives_manifest_and_frozen_actions(tmp_path: Path) -> None:
    adapter = OfficialBuildAdapter(tmp_path, BuildExecutor(tmp_path))
    context = _context("BUILD", source_scope=_source_scope())
    result = adapter.execute(context)
    assert result.scope["dataset_manifest_sha256"] == MANIFEST
    assert result.scope["predecessor_manifest_sha256"] == PREDECESSOR
    assert set(result.scope["component_actions"]) == set(COMPONENTS)
    assert len(result.output_artifacts) == 2


def test_build_adapter_passes_registered_pipeline_semantics(tmp_path: Path) -> None:
    context = _context("BUILD", source_scope=_source_scope())
    receipt = _run_pipeline(
        tmp_path,
        OfficialBuildAdapter(tmp_path, BuildExecutor(tmp_path)),
        context,
    )
    assert receipt["status"] == "PASS"
    assert receipt["scope"]["dataset_manifest_sha256"] == MANIFEST


def test_build_adapter_supports_separate_evidence_and_candidate_roots(tmp_path: Path) -> None:
    evidence_root = tmp_path / "evidence"
    candidate_root = tmp_path / "candidates"
    evidence_root.mkdir()
    candidate_root.mkdir()
    context = _context("BUILD", source_scope=_source_scope())
    adapter = OfficialBuildAdapter(
        evidence_root,
        BuildExecutor(candidate_root),
        additional_artifact_roots=(candidate_root,),
    )
    producer = CodeOwnedMonthlyStageProducer(adapter)
    pipeline = RegisteredMonthlyPipeline(
        {stage: producer for stage in ("SOURCE", "BUILD", "DERIVE", "LOCAL_VALIDATE", "DEPLOY", "CONSUMER_VALIDATE")},
        artifact_roots=(evidence_root, candidate_root),
    )
    receipt = pipeline.run_stage(
        stage="BUILD",
        operation_id=context.operation_id,
        attempt=1,
        request=context.request,
        plan=context.plan,
        prior_receipts=context.prior_receipts,
    )
    assert receipt["status"] == "PASS"
    assert receipt["scope"]["dataset_manifest_ref"]["id"].startswith("candidate/")


def test_build_adapter_rejects_noncanonical_manifest(tmp_path: Path) -> None:
    adapter = OfficialBuildAdapter(tmp_path, BuildExecutor(tmp_path, noncanonical=True))
    with pytest.raises(OfficialMonthlyAdapterError, match="canonical JSON"):
        adapter.execute(_context("BUILD", source_scope=_source_scope()))


def test_build_adapter_rejects_self_claimed_manifest_identity(tmp_path: Path) -> None:
    adapter = OfficialBuildAdapter(
        tmp_path,
        BuildExecutor(tmp_path, claimed_identity="f" * 64),
    )
    with pytest.raises(OfficialMonthlyAdapterError, match="canonical identity differs"):
        adapter.execute(_context("BUILD", source_scope=_source_scope()))


@dataclass
class DeriveExecutor:
    root: Path

    def execute(self, _context, *, dataset_manifest_sha256):  # type: ignore[no-untyped-def]
        assert dataset_manifest_sha256 == MANIFEST
        coefficient = _write(
            self.root / "candidate" / "derived" / "coefficients.json",
            {"schema_version": "hmm_coefficients_v1", "dataset_manifest_sha256": MANIFEST},
        )
        return DeriveExecution(assets=(DerivedAsset("hmm_coefficients", coefficient, "hmm_coefficients_v1"),))


def test_derive_adapter_builds_registry_and_forbids_fit_by_contract(tmp_path: Path) -> None:
    adapter = OfficialDeriveAdapter(tmp_path, DeriveExecutor(tmp_path))
    result = adapter.execute(
        _context(
            "DERIVE",
            build_scope={"dataset_manifest_sha256": MANIFEST},
            candidate_root=tmp_path / "candidate",
        )
    )
    assert result.scope["hmm_fit_count"] == 0
    assert result.scope["training_started"] is False
    registry = next(item.path for item in result.output_artifacts if item.path.name == "derived_asset_registry.json")
    value = _read(registry)
    assert value["source_dataset_manifest_sha256"] == MANIFEST
    assert value["assets"][0]["asset_id"] == "hmm_coefficients"
    assert value["assets"][0]["path"] == "coefficients.json"


def test_derive_adapter_passes_registered_pipeline_semantics(tmp_path: Path) -> None:
    context = _context(
        "DERIVE",
        build_scope={"dataset_manifest_sha256": MANIFEST},
        candidate_root=tmp_path / "candidate",
    )
    receipt = _run_pipeline(
        tmp_path,
        OfficialDeriveAdapter(tmp_path, DeriveExecutor(tmp_path)),
        context,
    )
    assert receipt["status"] == "PASS"
    assert receipt["scope"]["hmm_fit_count"] == 0


def test_derive_adapter_rejects_asset_outside_candidate(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    adapter = OfficialDeriveAdapter(tmp_path, DeriveExecutor(tmp_path / "external"))

    with pytest.raises(OfficialMonthlyAdapterError, match="escapes the monthly candidate"):
        adapter.execute(
            _context(
                "DERIVE",
                build_scope={"dataset_manifest_sha256": MANIFEST},
                candidate_root=candidate,
            )
        )


@dataclass
class _FakeFrozenHMMProcess:
    script_path: Path

    @property
    def script_sha256(self) -> str:
        return hashlib.sha256(self.script_path.read_bytes()).hexdigest()

    def execution_path(self, path: Path) -> str:
        return str(path.resolve(strict=True))

    def run(self, payload: Mapping[str, Any]) -> bytes:
        bundle = payload["frozen_input_bundle"]
        assert isinstance(bundle, Mapping)
        files = bundle["files"]
        assert isinstance(files, Mapping)
        daily = {
            day: {"801011.SI": 1.0}
            for day in ("2026-09-01", "2026-09-29")
            if str(payload["test_start"]) <= day <= str(payload["backtest_end"])
        }
        return json.dumps(
            {
                "model_path": payload["model_path"],
                "model_sha256": payload["model_sha256"],
                "preset_key": payload["preset_key"],
                "preset_coeffs": payload["preset_coeffs"],
                "test_start": payload["test_start"],
                "backtest_end": payload["backtest_end"],
                "daily_coefficients": daily,
                "stock_sector_membership_spans": {
                    "000001.SZ": [
                        {
                            "start_date": "2026-09-01",
                            "end_date": "2026-09-29",
                            "sector_code": "801011.SI",
                        }
                    ]
                },
                "dataset_identity": bundle["dataset_identity"],
                "input_file_sha256": {str(key): value["sha256"] for key, value in files.items()},
            },
            ensure_ascii=False,
        ).encode("utf-8")


def _hmm_candidate(root: Path) -> tuple[Path, FrozenHMMCoefficientAuthority, _FakeFrozenHMMProcess]:
    candidate = root / "candidate"
    inputs = {
        "factor_bundle/sector_data.h5": b"sector",
        "index_context/index_daily.h5": b"index",
        "components/sector_context_candidate_v1/sector_code_map.json": b"{}\n",
        "components/sector_context_candidate_v1/market_context.parquet": b"market",
        "components/sector_context_candidate_v1/sector_membership_spans.parquet": b"members",
        "components/sector_context_candidate_v1/sector_quote_availability.json": b"{}\n",
        "daily_bin/qlib/calendars/day.txt": b"2026-09-01\n2026-09-29\n2026-09-30\n",
    }
    components: dict[str, dict[str, Any]] = {}
    for index, (relative, content) in enumerate(inputs.items()):
        path = candidate / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        components[f"component_{index}"] = {
            "path": relative,
            "sha256": hashlib.sha256(content).hexdigest(),
            "size": len(content),
        }
    unsigned = {
        "schema_version": "qe_dataset_manifest_v1",
        "release_id": "qe_hmm_full_v2_20260930",
        "revision": "20260930-monthly-v2",
        "cutoff_trade_date": "2026-09-30",
        "components": components,
    }
    identity = hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
    _write(candidate / "qe_dataset_manifest.json", {**unsigned, "dataset_manifest_sha256": identity})
    model = root / "authority" / "model.json"
    config = root / "authority" / "config.json"
    script = root / "project" / "scripts" / "precompute_hmm_coefficients.py"
    model.parent.mkdir(parents=True)
    script.parent.mkdir(parents=True)
    model.write_text('{"801011.SI": {}}\n', encoding="utf-8")
    config.write_text('{"method":"pup"}\n', encoding="utf-8")
    script.write_text("# formal producer\n", encoding="utf-8")
    process = _FakeFrozenHMMProcess(script)
    authority = FrozenHMMCoefficientAuthority(
        authority_id="monthly-hmm-fixture-v1",
        model_path=model,
        model_sha256=hashlib.sha256(model.read_bytes()).hexdigest(),
        config_path=config,
        config_sha256=hashlib.sha256(config.read_bytes()).hexdigest(),
        script_sha256=process.script_sha256,
        products=(
            HMMCoefficientProduct(
                asset_id="hmm.coefficients.preset_a",
                preset_key="preset_A",
                preset_coefficients={"trending": 1.05, "neutral": 1.0, "fading": 0.96},
                test_start=date(2026, 9, 1),
            ),
        ),
    )
    return candidate, authority, process


def test_formal_hmm_derive_binds_candidate_without_fit_or_database(tmp_path: Path) -> None:
    candidate, authority, process = _hmm_candidate(tmp_path)
    manifest = _read(candidate / "qe_dataset_manifest.json")
    executor = MonthlyHMMCoefficientExecutor(authority, process, tmp_path)
    adapter = OfficialDeriveAdapter(tmp_path, executor)
    assert (
        adapter.contract_sha256
        != hashlib.sha256(
            canonical_json_bytes(
                {
                    "schema_version": "aistock_monthly_official_stage_contract_v1",
                    "stage": "DERIVE",
                    "adapter_version": "1",
                    "evidence_mode": "typed_files_and_readback",
                }
            )
        ).hexdigest()
    )

    result = adapter.execute(
        _context(
            "DERIVE",
            build_scope={"dataset_manifest_sha256": manifest["dataset_manifest_sha256"]},
            candidate_root=candidate,
        )
    )

    coefficient = next(item.path for item in result.output_artifacts if item.path.name.startswith("coefficients_"))
    value = _read(coefficient)
    assert value["schema_version"] == SHARED_HMM_COEFFICIENT_SCHEMA
    assert value["dataset_manifest_sha256"] == manifest["dataset_manifest_sha256"]
    assert value["backtest_end"] == "2026-09-29"
    assert value["derivation_identity"]["fit_performed"] is False
    assert value["derivation_identity"]["database_access"] is False
    assert coefficient.read_bytes() == canonical_json_bytes(value) + b"\n"
    registry = _read(candidate / "derived" / "derived_asset_registry.json")
    assert registry["assets"][0]["path"] == coefficient.name


def test_formal_hmm_derive_rejects_candidate_input_drift(tmp_path: Path) -> None:
    candidate, authority, process = _hmm_candidate(tmp_path)
    manifest = _read(candidate / "qe_dataset_manifest.json")
    (candidate / "factor_bundle" / "sector_data.h5").write_bytes(b"tampered")
    executor = MonthlyHMMCoefficientExecutor(authority, process, tmp_path)

    with pytest.raises(MonthlyHMMDeriveError, match="input pin differs"):
        executor.execute(
            _context("DERIVE", candidate_root=candidate),
            dataset_manifest_sha256=str(manifest["dataset_manifest_sha256"]),
        )


def test_formal_hmm_derive_resume_accepts_only_identical_candidate_assets(
    tmp_path: Path,
) -> None:
    candidate, authority, process = _hmm_candidate(tmp_path)
    manifest = _read(candidate / "qe_dataset_manifest.json")
    adapter = OfficialDeriveAdapter(
        tmp_path,
        MonthlyHMMCoefficientExecutor(authority, process, tmp_path),
    )
    first = _context(
        "DERIVE",
        build_scope={"dataset_manifest_sha256": manifest["dataset_manifest_sha256"]},
        candidate_root=candidate,
    )
    adapter.execute(first)
    resumed = ProducerContext(
        stage=first.stage,
        operation_id=first.operation_id,
        attempt=2,
        request=first.request,
        plan=first.plan,
        prior_receipts=first.prior_receipts,
    )

    result = adapter.execute(resumed)

    assert len(result.scope["derived_assets"]) == 1
    assert _read(candidate / "derived" / "derived_asset_registry.json")["assets"]


def test_local_hmm_process_uses_json_stdin_without_a_shell(tmp_path: Path) -> None:
    script = tmp_path / "producer.py"
    script.write_text(
        "import sys\nsys.stdout.buffer.write(sys.stdin.buffer.read())\n",
        encoding="utf-8",
    )
    process = LocalPythonHMMCoefficientProcess(
        python_executable=Path(sys.executable),
        script_path=script,
        project_root=tmp_path,
    )

    output = process.run({"value": "$(must-not-execute)"})

    assert json.loads(output.decode("utf-8")) == {"value": "$(must-not-execute)"}


def _read(path: Path) -> Mapping[str, Any]:
    import json

    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, Mapping)
    return value


@dataclass
class LocalExecutor:
    root: Path

    def execute(self, _context, *, dataset_manifest_sha256):  # type: ignore[no-untyped-def]
        assert dataset_manifest_sha256 == MANIFEST
        return LocalValidationExecution(
            pool_gap_counts={
                name: 0 for name in ("stock_universe", "csi300", "csi500", "csi1000", "star50", "star100")
            },
            dataset_identity_complete=True,
            consumer_contracts=(
                _write(
                    self.root / "provenance" / "consumer-contract.json",
                    {"version": "1", "dataset_manifest_sha256": MANIFEST},
                ),
            ),
            source_readiness=(
                _write(
                    self.root / "provenance" / "source-readiness.json",
                    {"status": "PASS", "dataset_manifest_sha256": MANIFEST},
                ),
            ),
            component_validations=(
                _write(
                    self.root / "provenance" / "component-validation.json",
                    {
                        "status": "PASS",
                        "gaps": 0,
                        "dataset_manifest_sha256": MANIFEST,
                    },
                ),
            ),
            lineage_path=_write(
                self.root / "provenance" / "lineage.json",
                {
                    "predecessor": PREDECESSOR,
                    "dataset_manifest_sha256": MANIFEST,
                },
            ),
        )


class ProfileBuilder:
    def execute(
        self,
        context,
        *,
        release_closure_path,
        derived_asset_registry_path,
    ):  # type: ignore[no-untyped-def]
        output = Path(str(context.plan["profile_candidate"]))
        return _write(
            output,
            {
                "schema_version": "aistock_active_dataset_profile_v4",
                "generation": context.plan["generation"],
                "release_id": context.plan["release_id"],
                "cutoff": context.plan["target_cutoff"],
                "components": {
                    "dataset_manifest_sha256": MANIFEST,
                    "derived_asset_registry_sha256": hashlib.sha256(
                        Path(derived_asset_registry_path).read_bytes()
                    ).hexdigest(),
                    "release_closure_sha256": hashlib.sha256(
                        Path(release_closure_path).read_bytes()
                    ).hexdigest(),
                },
            },
        )


def test_local_validate_adapter_constructs_release_closure(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    manifest = _write(candidate / "qe_dataset_manifest.json", {"x": 1})
    asset = _write(candidate / "derived" / "coefficients.json", {"x": 2})
    registry = _write(
        candidate / "derived" / "derived_asset_registry.json",
        {
            "schema_version": "aistock_dataset_derived_asset_registry_v1",
            "source_dataset_manifest_sha256": MANIFEST,
            "assets": [
                {
                    "asset_id": "hmm",
                    "path": "coefficients.json",
                    "sha256": hashlib.sha256(asset.read_bytes()).hexdigest(),
                    "size": asset.stat().st_size,
                    "schema_version": "hmm-test-v1",
                }
            ],
        },
    )
    adapter = OfficialLocalValidateAdapter(
        tmp_path,
        LocalExecutor(candidate),
        ProfileBuilder(),
    )
    result = adapter.execute(
        _context(
            "LOCAL_VALIDATE",
            build_scope={
                "dataset_manifest_sha256": MANIFEST,
                "dataset_manifest_ref": _ref(tmp_path, manifest),
            },
            derive_scope={
                "derived_assets": [_ref(tmp_path, asset)],
                "derived_asset_registry_ref": _ref(tmp_path, registry),
            },
            candidate_root=candidate,
        )
    )
    assert result.scope["dataset_identity_complete"] is True
    assert set(result.scope["pool_gap_counts"].values()) == {0}
    assert result.scope["release_closure"]["canonical_sha256"]
    assert (candidate / "release_closure_receipt.json").is_file()
    assert all(
        not Path(ref["id"]).is_absolute()
        for field in (
            "derived_asset_refs",
            "consumer_contract_refs",
            "source_readiness_refs",
            "component_validation_refs",
        )
        for ref in result.scope["release_closure"][field]
    )


def test_local_validate_adapter_rejects_evidence_outside_candidate(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    manifest = _write(candidate / "qe_dataset_manifest.json", {"x": 1})
    asset = _write(candidate / "derived" / "coefficients.json", {"x": 2})
    registry = _write(
        candidate / "derived" / "derived_asset_registry.json",
        {
            "schema_version": "aistock_dataset_derived_asset_registry_v1",
            "source_dataset_manifest_sha256": MANIFEST,
            "assets": [
                {
                    "asset_id": "hmm",
                    "path": "coefficients.json",
                    "sha256": hashlib.sha256(asset.read_bytes()).hexdigest(),
                    "size": asset.stat().st_size,
                    "schema_version": "hmm-test-v1",
                }
            ],
        },
    )
    adapter = OfficialLocalValidateAdapter(
        tmp_path,
        LocalExecutor(tmp_path / "external"),
        ProfileBuilder(),
    )

    with pytest.raises(OfficialMonthlyAdapterError, match="provenance root is unavailable"):
        adapter.execute(
            _context(
                "LOCAL_VALIDATE",
                build_scope={
                    "dataset_manifest_sha256": MANIFEST,
                    "dataset_manifest_ref": _ref(tmp_path, manifest),
                },
                derive_scope={
                    "derived_assets": [_ref(tmp_path, asset)],
                    "derived_asset_registry_ref": _ref(tmp_path, registry),
                },
                candidate_root=candidate,
            )
        )


@dataclass
class DeployExecutor:
    root: Path
    drift_node: str | None = None

    def execute(self, _context, *, dataset_manifest_sha256):  # type: ignore[no-untyped-def]
        shared = _write(self.root / "candidate" / "component.json", {"x": 1})
        nodes = []
        for node in REQUIRED_NODES:
            receipt = _write(
                self.root / "deploy" / f"{node}.json",
                {
                    "status": "PASS",
                    "node": node,
                    "dataset_manifest_sha256": dataset_manifest_sha256,
                },
            )
            nodes.append(
                NodeDeployment(
                    node_id=node,
                    candidate_root=f"/releases/{node}",
                    manifest_sha256=PREDECESSOR if node == self.drift_node else dataset_manifest_sha256,
                    relative_files=(shared,),
                    deployment_receipt=receipt,
                )
            )
        return DeployExecution(nodes=tuple(nodes))


def _identity_scopes(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = _write(root / "candidate" / "qe_dataset_manifest.json", {"x": 1})
    closure = _write(root / "candidate" / "release-closure.json", {"x": 2})
    return (
        {"dataset_manifest_sha256": MANIFEST, "dataset_manifest_ref": _ref(root, manifest)},
        {"release_closure_ref": _ref(root, closure)},
    )


def test_deploy_adapter_requires_exact_three_node_identity(tmp_path: Path) -> None:
    build_scope, local_scope = _identity_scopes(tmp_path)
    adapter = OfficialDeployAdapter(tmp_path, DeployExecutor(tmp_path))
    result = adapter.execute(_context("DEPLOY", build_scope=build_scope, local_scope=local_scope))
    assert set(result.scope["node_registrations"]) == set(REQUIRED_NODES)
    assert set(result.scope["node_manifest_sha256"].values()) == {MANIFEST}


def test_deploy_adapter_rejects_manifest_drift(tmp_path: Path) -> None:
    build_scope, local_scope = _identity_scopes(tmp_path)
    adapter = OfficialDeployAdapter(tmp_path, DeployExecutor(tmp_path, drift_node="rdagent-node1"))
    with pytest.raises(OfficialMonthlyAdapterError, match="manifest identities differ"):
        adapter.execute(_context("DEPLOY", build_scope=build_scope, local_scope=local_scope))


def _sealed_candidate(root: Path, *, with_hardlink: bool = False) -> tuple[str, Path, Path]:
    component = _write(root / "component.json", {"status": "PASS"})
    components = {
        "component": {
            "path": "component.json",
            "sha256": hashlib.sha256(component.read_bytes()).hexdigest(),
            "size": component.stat().st_size,
        }
    }
    if with_hardlink:
        alias = root / "components" / "component-alias.json"
        alias.parent.mkdir(parents=True)
        os.link(component, alias)
        components["component_alias"] = {
            "path": "components/component-alias.json",
            "sha256": hashlib.sha256(alias.read_bytes()).hexdigest(),
            "size": alias.stat().st_size,
        }
    unsigned: dict[str, Any] = {
        "schema_version": "qe_dataset_manifest_v1",
        "release_id": "qe_hmm_full_v2_20260930",
        "revision": "20260930-monthly-v2",
        "cutoff_trade_date": "2026-09-30",
        "components": components,
    }
    manifest_sha = hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
    manifest = dict(unsigned)
    manifest["dataset_manifest_sha256"] = manifest_sha
    manifest_path = _write(root / "qe_dataset_manifest.json", manifest)
    asset = _write(
        root / "derived" / "coefficients.json",
        {
            "schema_version": "hmm_coefficients_v1",
            "dataset_manifest_sha256": manifest_sha,
        },
    )
    registry = _write(
        root / "derived" / "derived_asset_registry.json",
        {
            "schema_version": "aistock_dataset_derived_asset_registry_v1",
            "source_dataset_manifest_sha256": manifest_sha,
            "assets": [
                {
                    "asset_id": "hmm_coefficients",
                    "path": "coefficients.json",
                    "sha256": hashlib.sha256(asset.read_bytes()).hexdigest(),
                    "size": asset.stat().st_size,
                    "schema_version": "hmm_coefficients_v1",
                }
            ],
        },
    )
    provenance = root / "provenance"
    contract = _write(provenance / "consumer-contract.json", {"status": "PASS"})
    source = _write(provenance / "source-readiness.json", {"status": "PASS"})
    validation = _write(provenance / "component-validation.json", {"status": "PASS"})
    lineage = _write(provenance / "lineage.json", {"status": "PASS"})

    def closure_ref(path: Path) -> dict[str, Any]:
        return {
            "id": path.relative_to(root).as_posix(),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "size": path.stat().st_size,
        }

    closure: dict[str, Any] = {
        "schema_version": "aistock_release_closure_v1",
        "dataset_manifest_ref": closure_ref(manifest_path),
        "derived_asset_refs": [closure_ref(registry), closure_ref(asset)],
        "consumer_contract_refs": [closure_ref(contract)],
        "source_readiness_refs": [closure_ref(source)],
        "component_validation_refs": [closure_ref(validation)],
        "lineage_ref": closure_ref(lineage),
    }
    closure["canonical_sha256"] = hashlib.sha256(
        canonical_json_bytes(closure)
    ).hexdigest()
    closure_path = _write(root / "release_closure_receipt.json", closure)
    return manifest_sha, registry, closure_path


def _immutable_deploy_fixture(
    tmp_path: Path, *, attempt: int = 1, with_hardlink: bool = False
):
    controller = tmp_path / "controller" / "candidate"
    wsl_parent = tmp_path / "wsl"
    node_parent = tmp_path / "node1"
    artifacts = tmp_path / "artifacts"
    for path in (wsl_parent, node_parent, artifacts):
        path.mkdir(parents=True)
    manifest_sha, registry, closure = _sealed_candidate(
        controller, with_hardlink=with_hardlink
    )
    targets = {
        "controller": str(controller.resolve()),
        "wsl2-5080": str(wsl_parent / "candidate"),
        "rdagent-node1": str(node_parent / "candidate"),
    }
    context = _context(
        "DEPLOY",
        candidate_root=controller,
        target_roots=targets,
        derive_scope={"derived_asset_registry_ref": _ref(tmp_path, registry)},
        local_scope={"release_closure_ref": _ref(tmp_path, closure)},
    )
    context = ProducerContext(
        stage=context.stage,
        operation_id=context.operation_id,
        attempt=attempt,
        request=context.request,
        plan=context.plan,
        prior_receipts=context.prior_receipts,
    )
    executor = ImmutableMonthlyDeployExecutor(
        artifact_root=artifacts,
        transports={
            "controller": ExistingTreeNodeTransport(),
            "wsl2-5080": ImmutableFilesystemNodeTransport("wsl2-5080", wsl_parent),
            "rdagent-node1": ImmutableFilesystemNodeTransport("rdagent-node1", node_parent),
        },
    )
    return executor, context, manifest_sha, targets


def test_immutable_deploy_copies_one_exact_inventory_to_all_nodes(
    tmp_path: Path,
) -> None:
    executor, context, manifest_sha, targets = _immutable_deploy_fixture(tmp_path)

    result = executor.execute(context, dataset_manifest_sha256=manifest_sha)

    assert {item.node_id for item in result.nodes} == set(REQUIRED_NODES)
    assert result.workload.bytes_transferred > 0
    for node in result.nodes:
        receipt = _read(node.deployment_receipt)
        assert receipt["status"] == "PASS"
        assert receipt["dataset_manifest_sha256"] == manifest_sha
        assert receipt["overwrite_performed"] is False
        assert Path(targets[node.node_id]).joinpath("qe_dataset_manifest.json").is_file()


def test_immutable_deploy_preserves_hardlink_aliases_without_duplicate_transfer(
    tmp_path: Path,
) -> None:
    executor, context, manifest_sha, targets = _immutable_deploy_fixture(
        tmp_path, with_hardlink=True
    )
    controller = Path(targets["controller"])
    logical_bytes = sum(path.stat().st_size for path in controller.rglob("*") if path.is_file())

    result = executor.execute(context, dataset_manifest_sha256=manifest_sha)

    assert result.workload.bytes_transferred < logical_bytes * 2
    for node_id in ("wsl2-5080", "rdagent-node1"):
        root = Path(targets[node_id])
        assert os.path.samefile(root / "component.json", root / "components/component-alias.json")


def test_immutable_deploy_rejects_invalid_closure_reference_hash(
    tmp_path: Path,
) -> None:
    executor, context, manifest_sha, _targets = _immutable_deploy_fixture(tmp_path)
    candidate = Path(str(context.plan["candidate_root"]))
    closure_path = candidate / "release_closure_receipt.json"
    closure = dict(_read(closure_path))
    manifest_ref = dict(closure["dataset_manifest_ref"])
    manifest_ref["sha256"] = "invalid"
    closure["dataset_manifest_ref"] = manifest_ref
    unsigned = dict(closure)
    unsigned.pop("canonical_sha256")
    closure["canonical_sha256"] = hashlib.sha256(
        canonical_json_bytes(unsigned)
    ).hexdigest()
    closure_path.unlink()
    _write(closure_path, closure)
    local_scope = dict(context.prior_receipts["LOCAL_VALIDATE"]["scope"])
    local_scope["release_closure_ref"] = _ref(tmp_path, closure_path)
    context = ProducerContext(
        stage=context.stage,
        operation_id=context.operation_id,
        attempt=context.attempt,
        request=context.request,
        plan=context.plan,
        prior_receipts={
            **context.prior_receipts,
            "LOCAL_VALIDATE": {"scope": local_scope},
        },
    )

    with pytest.raises(MonthlyImmutableDeployError, match="hash is invalid"):
        executor.execute(context, dataset_manifest_sha256=manifest_sha)


def test_immutable_deploy_resume_accepts_only_identical_existing_targets(
    tmp_path: Path,
) -> None:
    executor, context, manifest_sha, targets = _immutable_deploy_fixture(tmp_path)
    executor.execute(context, dataset_manifest_sha256=manifest_sha)
    resumed = ProducerContext(
        stage=context.stage,
        operation_id=context.operation_id,
        attempt=2,
        request=context.request,
        plan=context.plan,
        prior_receipts=context.prior_receipts,
    )

    result = executor.execute(resumed, dataset_manifest_sha256=manifest_sha)

    assert result.workload.bytes_transferred == 0
    Path(targets["wsl2-5080"]).joinpath("component.json").write_bytes(b"drift")
    drifted = ProducerContext(
        stage=context.stage,
        operation_id=context.operation_id,
        attempt=3,
        request=context.request,
        plan=context.plan,
        prior_receipts=context.prior_receipts,
    )
    with pytest.raises(MonthlyImmutableDeployError, match="target bytes differ"):
        executor.execute(drifted, dataset_manifest_sha256=manifest_sha)


def test_immutable_deploy_rejects_unregistered_candidate_file(tmp_path: Path) -> None:
    executor, context, manifest_sha, targets = _immutable_deploy_fixture(tmp_path)
    Path(targets["controller"]).joinpath("unregistered.bin").write_bytes(b"unexpected")

    with pytest.raises(MonthlyImmutableDeployError, match="unregistered"):
        executor.execute(context, dataset_manifest_sha256=manifest_sha)


@dataclass
class ConsumerExecutor:
    root: Path
    omit: str | None = None
    fail: str | None = None

    def execute(self, _context, *, dataset_manifest_sha256):  # type: ignore[no-untyped-def]
        component = _write(self.root / "candidate" / "component.json", {"x": 1})
        derived = _write(self.root / "candidate" / "derived.json", {"x": 2})
        rows = []
        for name in REQUIRED_CONSUMERS:
            if name == self.omit:
                continue
            uses_derived = (
                "derived_assets"
                in ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS[name]
            )
            binding = _write(
                self.root / "consumer" / f"{name}-binding.json",
                {
                    "schema_version": CONSUMER_VALIDATION_BINDING_SCHEMA,
                    "dataset_manifest_sha256": dataset_manifest_sha256,
                    "resolved_component_refs": [_ref(self.root, component)],
                    "derived_asset_refs": (
                        [_ref(self.root, derived)]
                        if uses_derived
                        else []
                    ),
                },
            )
            result = _write(
                self.root / "consumer" / f"{name}-result.json",
                {
                    "status": "FAILED" if name == self.fail else "PASS",
                    "dataset_manifest_sha256": dataset_manifest_sha256,
                },
            )
            rows.append(
                ConsumerReadback(
                    consumer_id=name,
                    node_id="controller",
                    binding_path=binding,
                    required_window={"start": "2018-08-01", "end": "2026-09-30"},
                    resolved_component_paths=(component,),
                    derived_asset_paths=(
                        (derived,)
                        if uses_derived
                        else ()
                    ),
                    coverage_counts={"unresolved_count": 0},
                    adapter_version="consumer-smoke-v1",
                    result_path=result,
                )
            )
        return ConsumerValidationExecution(readbacks=tuple(rows))


def test_consumer_adapter_builds_all_readbacks_with_no_side_effects(tmp_path: Path) -> None:
    adapter = OfficialConsumerValidateAdapter(tmp_path, ConsumerExecutor(tmp_path))
    result = adapter.execute(_context("CONSUMER_VALIDATE", build_scope={"dataset_manifest_sha256": MANIFEST}))
    assert set(result.scope["consumer_readbacks"]) == set(REQUIRED_CONSUMERS)
    assert result.scope["outcomes_read"] is False
    assert result.scope["training_started"] is False
    assert all(
        not any(readback["side_effect_flags"].values()) for readback in result.scope["consumer_readbacks"].values()
    )


def test_consumer_adapter_passes_registered_pipeline_semantics(tmp_path: Path) -> None:
    context = _context("CONSUMER_VALIDATE", build_scope={"dataset_manifest_sha256": MANIFEST})
    receipt = _run_pipeline(
        tmp_path,
        OfficialConsumerValidateAdapter(tmp_path, ConsumerExecutor(tmp_path)),
        context,
    )
    assert receipt["status"] == "PASS"
    assert set(receipt["scope"]["consumer_readbacks"]) == set(REQUIRED_CONSUMERS)


def test_consumer_adapter_rejects_partial_registry(tmp_path: Path) -> None:
    adapter = OfficialConsumerValidateAdapter(tmp_path, ConsumerExecutor(tmp_path, omit="qe_p11"))
    with pytest.raises(OfficialMonthlyAdapterError, match="coverage differs"):
        adapter.execute(_context("CONSUMER_VALIDATE", build_scope={"dataset_manifest_sha256": MANIFEST}))


def test_consumer_adapter_rejects_failed_result_file(tmp_path: Path) -> None:
    adapter = OfficialConsumerValidateAdapter(
        tmp_path,
        ConsumerExecutor(tmp_path, fail="qe_p11"),
    )
    with pytest.raises(OfficialMonthlyAdapterError, match="did not report PASS"):
        adapter.execute(_context("CONSUMER_VALIDATE", build_scope={"dataset_manifest_sha256": MANIFEST}))


@dataclass
class SourceProducer:
    producer_id: str = "aistock.monthly.source.official"
    producer_version: str = "1"

    def produce(self, _context):  # type: ignore[no-untyped-def]
        return {}


@dataclass
class SourceAdapter:
    adapter_id: str = "aistock.monthly.source.adapter"
    adapter_version: str = "1"
    contract_sha256: str = PREDECESSOR

    def read(self, *_args):  # type: ignore[no-untyped-def]
        raise AssertionError("registry composition must not query the source")


def test_registry_composition_is_code_owned_and_exact(tmp_path: Path) -> None:
    source = AuditedMonthlySourceProducer(
        producer_id="aistock.monthly.source.official",
        producer_version="1",
        artifact_root=tmp_path,
        connection_factory=lambda: None,  # type: ignore[arg-type,return-value]
        adapter=SourceAdapter(),
    )
    registry = build_official_monthly_registry(
        source=source,
        build=OfficialBuildAdapter(tmp_path, BuildExecutor(tmp_path)),
        derive=OfficialDeriveAdapter(tmp_path, DeriveExecutor(tmp_path)),
        local_validate=OfficialLocalValidateAdapter(
            tmp_path,
            LocalExecutor(tmp_path),
            ProfileBuilder(),
        ),
        deploy=OfficialDeployAdapter(tmp_path, DeployExecutor(tmp_path)),
        consumer_validate=OfficialConsumerValidateAdapter(tmp_path, ConsumerExecutor(tmp_path)),
    )
    assert tuple(registry.as_mapping()) == (
        "SOURCE",
        "BUILD",
        "DERIVE",
        "LOCAL_VALIDATE",
        "DEPLOY",
        "CONSUMER_VALIDATE",
    )
    assert all(
        isinstance(producer, CodeOwnedMonthlyStageProducer)
        for stage, producer in registry.as_mapping().items()
        if stage != "SOURCE"
    )


def test_registry_rejects_non_audited_source(tmp_path: Path) -> None:
    with pytest.raises(OfficialMonthlyAdapterError, match="audited snapshot producer"):
        build_official_monthly_registry(
            source=SourceProducer(),  # type: ignore[arg-type]
            build=OfficialBuildAdapter(tmp_path, BuildExecutor(tmp_path)),
            derive=OfficialDeriveAdapter(tmp_path, DeriveExecutor(tmp_path)),
            local_validate=OfficialLocalValidateAdapter(
                tmp_path,
                LocalExecutor(tmp_path),
                ProfileBuilder(),
            ),
            deploy=OfficialDeployAdapter(tmp_path, DeployExecutor(tmp_path)),
            consumer_validate=OfficialConsumerValidateAdapter(tmp_path, ConsumerExecutor(tmp_path)),
        )
