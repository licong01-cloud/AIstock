"""Production runtime for the durable unified monthly release worker."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from backend.services.quantevolver.qe_active_dataset_profile import (
    load_qe_profile,
    validate_controller_snapshot,
)

from .monthly_production import (
    MonthlyProductionSettings,
    build_monthly_production_registry,
)
from .monthly_registry import OfficialMonthlyProducerRegistry
from .monthly_runtime import MonthlyRuntimeSettings
from .monthly_unified import ActionAuthorizationStore
from .monthly_worker import MonthlyReleaseWorker
from .monthly_worker_nodes import MonthlyNodeRuntimeSettings


@dataclass(frozen=True, slots=True)
class MonthlyWorkerRuntime:
    settings: MonthlyRuntimeSettings
    nodes: MonthlyNodeRuntimeSettings
    production: MonthlyProductionSettings
    registry: OfficialMonthlyProducerRegistry
    worker: MonthlyReleaseWorker

    def preflight_receipt(self) -> dict[str, Any]:
        return {
            "schema_version": "aistock_monthly_release_worker_preflight_v1",
            "status": "PASS",
            "profile": "qe_hmm_full_v2",
            "profile_path": str(self.production.profile_path),
            "hmm_authority_path": str(self.production.hmm_authority_path),
            "registry": self.registry.contract(),
            "nodes": ["controller", "wsl2-5080", "rdagent-node1"],
            "safety": {
                "operation_claimed": False,
                "database_opened": False,
                "database_write_performed": False,
                "candidate_write_performed": False,
                "profile_write_performed": False,
                "runtime_action_performed": False,
                "training_started": False,
                "experiment_started": False,
            },
        }


def _activation_verifier(
    active_profile: Path,
):
    def verify(ready: Mapping[str, Any]) -> dict[str, Any]:
        profile = load_qe_profile(active_profile)
        validate_controller_snapshot(profile)
        return {
            "status": "PASS",
            "dataset_manifest_sha256": str(
                profile.raw["components"]["dataset_manifest_sha256"]
            ),
            "profile_sha256": profile.profile_sha256,
            "expected_manifest_sha256": str(ready["dataset_manifest_sha256"]),
        }

    return verify


def build_monthly_worker_runtime(*, project_root: Path) -> MonthlyWorkerRuntime:
    """Resolve fixed production wiring without claiming or running an operation."""

    settings = MonthlyRuntimeSettings.from_env()
    nodes = MonthlyNodeRuntimeSettings.from_env()
    production = MonthlyProductionSettings.from_env(project_root=project_root)
    registry = build_monthly_production_registry(
        runtime=settings,
        nodes=nodes,
        production=production,
    )
    service = settings.worker_service(registry)
    worker = MonthlyReleaseWorker(
        service,
        authorization_store=ActionAuthorizationStore(settings.authorization_root),
        activation_verifier=_activation_verifier(settings.active_profile),
    )
    return MonthlyWorkerRuntime(
        settings=settings,
        nodes=nodes,
        production=production,
        registry=registry,
        worker=worker,
    )


__all__: Sequence[str] = (
    "MonthlyWorkerRuntime",
    "build_monthly_worker_runtime",
)
