"""Production runtime for the durable unified monthly release worker."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import httpx

from backend.services.quantevolver.qe_active_dataset_profile import (
    load_qe_profile,
    validate_controller_snapshot,
)

from .monthly_production import (
    MonthlyProductionSettings,
    build_monthly_production_registry,
)
from .monthly_registry import OfficialMonthlyProducerRegistry
from .monthly_postgres_source import POSTGRES_SOURCE_ADAPTER_VERSION
from .monthly_runtime import MonthlyRuntimeConfigurationError, MonthlyRuntimeSettings
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

    def _source_preparation_contract(self) -> dict[str, Any]:
        """Read installed wiring only; never claim SOURCE or execute preparation."""
        adapter = getattr(getattr(self.registry, "source", None), "adapter", None)
        adapter_id = getattr(adapter, "adapter_id", None)
        version = getattr(adapter, "adapter_version", None)
        if adapter_id != "aistock.monthly.postgres_source" or version not in ("5", "6", "7", POSTGRES_SOURCE_ADAPTER_VERSION):
            raise MonthlyRuntimeConfigurationError("monthly worker SOURCE contract is unsupported")
        preparation = getattr(adapter, "preparation_executor", None)
        shared_scope = False
        if version == "5":
            if preparation is not None:
                raise MonthlyRuntimeConfigurationError("SOURCE5 cannot install private preparation")
            mode = "FULL_SOURCE_ONLY"
        else:
            build_adapter = getattr(getattr(self.registry, "build", None), "adapter", None)
            executor = getattr(build_adapter, "executor", None)
            runner = getattr(executor, "runner", None)
            build_scope = getattr(runner, "execution_scope_factory", None)
            preparation_scope = getattr(preparation, "execution_scope_factory", None)
            if (
                not callable(preparation)
                or not callable(build_scope)
                or preparation_scope is not build_scope
            ):
                raise MonthlyRuntimeConfigurationError(
                    "SOURCE preparation must share the installed formal BUILD execution scope"
                )
            shared_scope = True
            mode = "INDEPENDENT_UNPUBLISHED_PREPARATION"
        return {
            "adapter_id": adapter_id,
            "adapter_version": version,
            "mode": mode,
            "shared_build_execution_scope": shared_scope,
            "publication_allowed": False,
        }

    def preflight_receipt(self) -> dict[str, Any]:
        preparation_contract = self._source_preparation_contract()
        return {
            "schema_version": "aistock_monthly_release_worker_preflight_v1",
            "status": "PASS",
            "profile": "qe_hmm_full_v2",
            "profile_path": str(self.production.profile_path),
            "hmm_authority_path": str(self.production.hmm_authority_path),
            "registry": self.registry.contract(),
            "source_preparation": preparation_contract,
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
    nodes: MonthlyNodeRuntimeSettings,
):
    def verify(ready: Mapping[str, Any]) -> dict[str, Any]:
        profile = load_qe_profile(active_profile)
        validate_controller_snapshot(profile)
        expected_manifest = str(ready["dataset_manifest_sha256"])
        actual_manifest = str(profile.raw["components"]["dataset_manifest_sha256"])
        if actual_manifest != expected_manifest:
            raise RuntimeError("active controller manifest differs from activated successor")
        bindings = profile.raw.get("node_bindings")
        if not isinstance(bindings, Mapping):
            raise RuntimeError("active profile node bindings are unavailable")
        readbacks: dict[str, Any] = {}
        with httpx.Client(
            timeout=httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0),
            trust_env=False,
        ) as client:
            for node_id, url in nodes.dataset_identity_urls().items():
                binding = bindings.get(node_id)
                root = binding.get("candidate_root") if isinstance(binding, Mapping) else None
                if not isinstance(root, str) or not root.strip():
                    raise RuntimeError(f"active profile node binding is incomplete: {node_id}")
                response = client.get(
                    url,
                    params={"node_id": node_id, "data_root_uri": root},
                )
                response.raise_for_status()
                payload = response.json()
                dataset = payload.get("dataset") if isinstance(payload, Mapping) else None
                if (
                    not isinstance(payload, Mapping)
                    or payload.get("complete") is not True
                    or not isinstance(dataset, Mapping)
                    or dataset.get("dataset_manifest_sha256") != expected_manifest
                    or dataset.get("resolved_node_id") != node_id
                    or dataset.get("resolved_data_root_uri") != root
                ):
                    raise RuntimeError(f"running node dataset identity differs: {node_id}")
                readbacks[node_id] = {
                    "complete": True,
                    "dataset_manifest_sha256": dataset["dataset_manifest_sha256"],
                    "resolved_data_root_uri": dataset["resolved_data_root_uri"],
                }
        return {
            "status": "PASS",
            "dataset_manifest_sha256": actual_manifest,
            "profile_sha256": profile.profile_sha256,
            "expected_manifest_sha256": expected_manifest,
            "node_dataset_identity_readbacks": readbacks,
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
        activation_verifier=_activation_verifier(settings.active_profile, nodes),
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
