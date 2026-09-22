from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.monthly_runtime import (
    MonthlyRuntimeConfigurationError,
    MonthlyRuntimeSettings,
)
from backend.services.dataset_release.monthly_source_producer import (
    AuditedMonthlySourceProducer,
)
from backend.services.dataset_release.monthly_unified import STAGES
from backend.services.dataset_release.monthly_worker_composition import (
    MonthlyWorkerExecutors,
    build_monthly_worker_registry,
)
from backend.services.dataset_release.monthly_worker_nodes import (
    MonthlyNodeRuntimeSettings,
)


class SourceAdapter:
    adapter_id = "source.adapter"
    adapter_version = "1"
    contract_sha256 = "a" * 64

    def read(self, connection, identity, context):  # type: ignore[no-untyped-def]
        raise AssertionError("composition test must not execute SOURCE")


def _runtime(tmp_path: Path) -> MonthlyRuntimeSettings:
    roots = [tmp_path / name for name in ("state", "controller", "profiles", "artifacts", "auth")]
    for root in roots:
        root.mkdir()
    active = tmp_path / "active.json"
    active.write_text("{}\n", encoding="utf-8")
    return MonthlyRuntimeSettings(
        state_root=roots[0],
        active_profile=active,
        controller_release_root=roots[1],
        profile_candidate_root=roots[2],
        artifact_root=roots[3],
        wsl_release_root="/mnt/wsl/releases",
        node1_release_root="/home/lc999/data/releases",
        authorization_root=roots[4],
    )


def _nodes() -> MonthlyNodeRuntimeSettings:
    return MonthlyNodeRuntimeSettings(
        wsl_distro="Ubuntu-24.04",
        wsl_project_root="/mnt/f/Dev/AIstock",
        wsl_python="/opt/conda/envs/rdagent-gpu/bin/python",
        node1_host="rdagent-node1",
        node1_project_root="/home/lc999/AIstock",
        node1_python="/home/lc999/miniconda3/envs/rdagent-gpu/bin/python",
    )


def _executors(runtime: MonthlyRuntimeSettings) -> MonthlyWorkerExecutors:
    source = AuditedMonthlySourceProducer(
        producer_id="source.producer",
        producer_version="1",
        artifact_root=runtime.artifact_root,
        connection_factory=lambda: None,  # type: ignore[arg-type]
        adapter=SourceAdapter(),
    )
    return MonthlyWorkerExecutors(
        source=source,
        build=SimpleNamespace(),  # type: ignore[arg-type]
        derive=SimpleNamespace(),  # type: ignore[arg-type]
        local_validate=SimpleNamespace(),  # type: ignore[arg-type]
        profile_builder=SimpleNamespace(),  # type: ignore[arg-type]
    )


def test_worker_composition_registers_exact_six_stage_pipeline(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)

    registry = build_monthly_worker_registry(
        runtime=runtime,
        nodes=_nodes(),
        executors=_executors(runtime),
    )
    contract = registry.contract()

    assert tuple(registry.as_mapping()) == STAGES
    assert set(contract["stages"]) == set(STAGES)
    assert contract["stages"]["SOURCE"]["id"] == "source.producer"
    assert contract["stages"]["CONSUMER_VALIDATE"]["id"] == (
        "aistock.monthly.consumer_validate.official"
    )
    assert contract["registry_sha256"]


def test_worker_composition_rejects_cross_runtime_source_root(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    other = tmp_path / "other-artifacts"
    other.mkdir()
    executors = _executors(runtime)
    source = AuditedMonthlySourceProducer(
        producer_id="source.producer",
        producer_version="1",
        artifact_root=other,
        connection_factory=lambda: None,  # type: ignore[arg-type]
        adapter=SourceAdapter(),
    )
    drift = MonthlyWorkerExecutors(
        source=source,
        build=executors.build,
        derive=executors.derive,
        local_validate=executors.local_validate,
        profile_builder=executors.profile_builder,
    )

    with pytest.raises(MonthlyRuntimeConfigurationError, match="SOURCE artifact root differs"):
        build_monthly_worker_registry(runtime=runtime, nodes=_nodes(), executors=drift)
