from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

import backend.services.dataset_release.monthly_official_adapters as official_module
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_consumer_validation import (
    CONSUMER_PROBE_RESULT_SCHEMA,
    ConsumerProbeRequest,
    MonthlyConsumerValidationError,
    RegisteredMonthlyConsumerValidationExecutor,
)
from backend.services.dataset_release.monthly_official_adapters import (
    OfficialConsumerValidateAdapter,
    OfficialMonthlyAdapterError,
)
from backend.services.dataset_release.monthly_unified import (
    REQUIRED_CONSUMERS,
    REQUIRED_NODES,
)
from backend.services.dataset_release.monthly_worker import ProducerContext
from backend.services.dataset_release.profile_contract import (
    ACTIVE_PROFILE_SCHEMA_V4,
    ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS,
)


MANIFEST = "a" * 64


def _json(path: Path, value: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) + b"\n")
    return path


def _file(path: Path, value: bytes = b"value") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@dataclass
class Probe:
    root: Path
    probe_id: str
    probe_version: str = "1"
    side_effect: bool = False
    mutate_component: bool = False

    def run(self, request: ConsumerProbeRequest) -> Path:
        if self.mutate_component:
            next(
                path
                for path in request.resolved_component_paths
                if path.as_posix().endswith("components/day/value.bin")
            ).write_bytes(b"mutated")
        return _json(
            self.root / f"{request.consumer_id}.json",
            {
                "schema_version": CONSUMER_PROBE_RESULT_SCHEMA,
                "status": "PASS",
                "consumer_id": request.consumer_id,
                "node_id": request.node_id,
                "dataset_manifest_sha256": request.dataset_manifest_sha256,
                "binding_sha256": _sha(request.binding_path),
                "required_window": {
                    "start": "2018-08-01",
                    "end": "2026-09-30",
                },
                "coverage_counts": {"unresolved_count": 0, "files_read": 1},
                "adapter": {"id": self.probe_id, "version": self.probe_version},
                "evidence_refs": [
                    {"id": "probe/readback.json", "sha256": "f" * 64, "size": 1}
                ],
                "side_effect_flags": {
                    "outcomes_read": self.side_effect,
                    "training_started": False,
                    "experiment_started": False,
                    "runtime_action_performed": False,
                },
            },
        )


def _fixture(
    tmp_path: Path,
    *,
    side_effect_consumer: str | None = None,
    mutating_consumer: str | None = None,
    profile_node_drift: bool = False,
) -> tuple[
    RegisteredMonthlyConsumerValidationExecutor,
    ProducerContext,
    Path,
]:
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    candidate = tmp_path / "candidate"
    logical_paths = {
        "manifest": _json(
            candidate / "qe_dataset_manifest.json",
            {"dataset_manifest_sha256": MANIFEST},
        ),
        "day": _file(candidate / "components/day/value.bin"),
        "minute": _file(candidate / "components/minute/value.bin"),
        "factor": _file(candidate / "components/factor/value.bin"),
        "index": _file(candidate / "components/index/value.bin"),
        "suspend": _file(candidate / "components/suspend/value.bin"),
        "benchmark": _file(candidate / "components/benchmark/value.bin"),
        "coverage": _file(candidate / "reports/coverage.json"),
        "stock_pools": _file(candidate / "stock_pools/all.txt"),
        "sector_context": _file(candidate / "components/sector/value.bin"),
        "derived_assets": _json(
            candidate / "derived/coefficients.json",
            {"dataset_manifest_sha256": MANIFEST},
        ),
    }
    for consumer_id in REQUIRED_CONSUMERS:
        required = sorted(ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS[consumer_id])
        _json(
            candidate / "provenance" / f"consumer-contract-{consumer_id}.json",
            {
                "schema_version": "aistock_monthly_consumer_contract_v1",
                "consumer_id": consumer_id,
                "dataset_manifest_sha256": MANIFEST,
                "required_components": required,
                "component_refs": {
                    name: [
                        {
                            "path": logical_paths[name]
                            .relative_to(candidate)
                            .as_posix(),
                            "sha256": _sha(logical_paths[name]),
                            "size": logical_paths[name].stat().st_size,
                        }
                    ]
                    for name in required
                },
                "status": "READY_FOR_NODE_READBACK",
            },
        )

    node_roots = {
        "wsl2-5080": "/mnt/wsl/releases/candidate",
        "rdagent-node1": "/home/data/releases/candidate",
    }
    profile_raw = {
        "schema_version": ACTIVE_PROFILE_SCHEMA_V4,
        "components": {"dataset_manifest_sha256": MANIFEST},
        "consumers": {
            name: {
                "required_components": sorted(
                    ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS[name]
                )
            }
            for name in REQUIRED_CONSUMERS
        },
        "node_bindings": {
            node: {
                "candidate_root": (
                    "/drift" if profile_node_drift and node == "rdagent-node1" else root
                )
            }
            for node, root in node_roots.items()
        },
    }
    profile_path = _json(tmp_path / "profile.json", profile_raw)
    registrations = {
        "controller": {"candidate_root": str(candidate)},
        **{
            node: {"candidate_root": root}
            for node, root in node_roots.items()
        },
    }
    registration_refs = {
        node: {"id": f"{node}.json", "sha256": str(index + 1) * 64, "size": 1}
        for index, node in enumerate(REQUIRED_NODES)
    }
    context = ProducerContext(
        stage="CONSUMER_VALIDATE",
        operation_id="dmr_" + "1" * 32,
        attempt=1,
        request={},
        plan={
            "candidate_root": str(candidate.resolve()),
            "profile_candidate": str(profile_path.resolve()),
        },
        prior_receipts={
            "BUILD": {"scope": {"dataset_manifest_sha256": MANIFEST}},
            "LOCAL_VALIDATE": {
                "scope": {
                    "dataset_manifest_sha256": MANIFEST,
                    "profile_candidate_ref": {
                        "id": profile_path.name,
                        "sha256": _sha(profile_path),
                        "size": profile_path.stat().st_size,
                    },
                }
            },
            "DEPLOY": {
                "scope": {
                    "dataset_manifest_sha256": MANIFEST,
                    "node_registrations": registrations,
                    "node_registration_refs": registration_refs,
                }
            },
        },
    )
    nodes = {
        name: (
            "rdagent-node1"
            if name in {"qe_custom", "qe_multi_alpha", "qe_p11"}
            else "wsl2-5080"
        )
        for name in REQUIRED_CONSUMERS
    }
    probes = {
        name: Probe(
            artifact_root / "probe-results",
            f"aistock.consumer.{name}",
            side_effect=name == side_effect_consumer,
            mutate_component=name == mutating_consumer,
        )
        for name in REQUIRED_CONSUMERS
    }

    def resolver(*, consumer_id: str, node_id: str, profile: Any) -> dict[str, Any]:
        del profile
        return {
            "consumer_id": consumer_id,
            "node_id": node_id,
            "dataset_manifest_sha256": MANIFEST,
            "required_components": sorted(
                ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS[consumer_id]
            ),
            "resolved_once": True,
            "legacy_fallback": False,
        }

    executor = RegisteredMonthlyConsumerValidationExecutor(
        artifact_root=artifact_root,
        probes=probes,
        consumer_nodes=nodes,
        profile_loader=lambda _path: SimpleNamespace(raw=profile_raw),
        binding_resolver=resolver,
    )
    return executor, context, candidate


def test_registered_consumer_validation_runs_exact_registry(tmp_path: Path) -> None:
    executor, context, candidate = _fixture(tmp_path)

    result = executor.execute(context, dataset_manifest_sha256=MANIFEST)

    assert [item.consumer_id for item in result.readbacks] == list(REQUIRED_CONSUMERS)
    assert {item.node_id for item in result.readbacks} == {
        "wsl2-5080",
        "rdagent-node1",
    }
    assert all(item.coverage_counts["unresolved_count"] == 0 for item in result.readbacks)
    assert all(
        not item.derived_asset_paths
        for item in result.readbacks
        if item.consumer_id not in {"qe_p10", "qe_p11"}
    )
    assert all(
        item.derived_asset_paths
        for item in result.readbacks
        if item.consumer_id in {"qe_p10", "qe_p11"}
    )
    assert result.workload.computed_rows == len(REQUIRED_CONSUMERS)
    assert result.workload.bytes_transferred == 0
    assert not (candidate / "release_closure_receipt.json").exists()


def test_registered_consumer_validation_closes_official_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor, context, _candidate = _fixture(tmp_path)
    adapter = OfficialConsumerValidateAdapter(tmp_path, executor)
    original_sha256 = official_module._sha256
    hashed: list[Path] = []

    def tracked_sha256(path: Path) -> str:
        hashed.append(path.resolve())
        return original_sha256(path)

    monkeypatch.setattr(official_module, "_sha256", tracked_sha256)

    result = adapter.execute(context)

    assert set(result.scope["consumer_readbacks"]) == set(REQUIRED_CONSUMERS)
    assert result.scope["outcomes_read"] is False
    assert result.scope["training_started"] is False
    assert result.scope["experiment_started"] is False
    assert result.scope["runtime_action_performed"] is False
    day = (tmp_path / "candidate/components/day/value.bin").resolve()
    assert hashed.count(day) == 1


def test_consumer_validation_rejects_probe_side_effect(tmp_path: Path) -> None:
    executor, context, _candidate = _fixture(
        tmp_path,
        side_effect_consumer="position_timing",
    )

    with pytest.raises(MonthlyConsumerValidationError, match="probe result differs"):
        executor.execute(context, dataset_manifest_sha256=MANIFEST)


def test_consumer_validation_rejects_profile_node_drift(tmp_path: Path) -> None:
    executor, context, _candidate = _fixture(tmp_path, profile_node_drift=True)

    with pytest.raises(MonthlyConsumerValidationError, match="node registration differs"):
        executor.execute(context, dataset_manifest_sha256=MANIFEST)


def test_official_adapter_rejects_candidate_mutation_by_probe(tmp_path: Path) -> None:
    executor, context, _candidate = _fixture(
        tmp_path,
        mutating_consumer="qe_single",
    )

    with pytest.raises(
        OfficialMonthlyAdapterError,
        match="consumer resolved bytes differ after probe",
    ):
        OfficialConsumerValidateAdapter(tmp_path, executor).execute(context)


def test_consumer_validation_requires_exact_probe_registry(tmp_path: Path) -> None:
    executor, _context, _candidate = _fixture(tmp_path)
    probes = dict(executor.probes)
    probes.pop("qe_p11")

    with pytest.raises(ValueError, match="exact required consumers"):
        RegisteredMonthlyConsumerValidationExecutor(
            artifact_root=executor.artifact_root,
            probes=probes,
            consumer_nodes=executor.consumer_nodes,
        )


def test_consumer_validation_rejects_component_drift(tmp_path: Path) -> None:
    executor, context, candidate = _fixture(tmp_path)
    (candidate / "components/day/value.bin").write_bytes(b"drift")

    with pytest.raises(MonthlyConsumerValidationError, match="component bytes differ"):
        executor.execute(context, dataset_manifest_sha256=MANIFEST)
