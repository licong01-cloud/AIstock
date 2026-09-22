"""Single code-owned composition root for the unified monthly worker stages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from .monthly_immutable_deploy import ImmutableMonthlyDeployExecutor
from .monthly_official_adapters import (
    MonthlyBuildExecutor,
    MonthlyDeriveExecutor,
    MonthlyLocalValidationExecutor,
    MonthlyProfileCandidateBuilder,
    OfficialBuildAdapter,
    OfficialConsumerValidateAdapter,
    OfficialDeployAdapter,
    OfficialDeriveAdapter,
    OfficialLocalValidateAdapter,
    build_official_monthly_registry,
)
from .monthly_registry import OfficialMonthlyProducerRegistry
from .monthly_runtime import MonthlyRuntimeConfigurationError, MonthlyRuntimeSettings
from .monthly_source_producer import AuditedMonthlySourceProducer
from .monthly_worker_nodes import MonthlyNodeRuntimeSettings


@dataclass(frozen=True, slots=True)
class MonthlyWorkerExecutors:
    source: AuditedMonthlySourceProducer
    build: MonthlyBuildExecutor
    derive: MonthlyDeriveExecutor
    local_validate: MonthlyLocalValidationExecutor
    profile_builder: MonthlyProfileCandidateBuilder


def build_monthly_worker_registry(
    *,
    runtime: MonthlyRuntimeSettings,
    nodes: MonthlyNodeRuntimeSettings,
    executors: MonthlyWorkerExecutors,
) -> OfficialMonthlyProducerRegistry:
    """Bind all six stages to one runtime without caller-supplied commands."""

    artifact = runtime.artifact_root.resolve(strict=True)
    controller = runtime.controller_release_root.resolve(strict=True)
    profiles = runtime.profile_candidate_root.resolve(strict=True)
    if executors.source.artifact_root.resolve(strict=True) != artifact:
        raise MonthlyRuntimeConfigurationError(
            "monthly SOURCE artifact root differs from worker runtime"
        )
    deploy = ImmutableMonthlyDeployExecutor(
        artifact_root=artifact,
        transports=nodes.deploy_transports(runtime),
    )
    consumer = nodes.consumer_executor(artifact_root=artifact)
    return build_official_monthly_registry(
        source=executors.source,
        build=OfficialBuildAdapter(
            artifact_root=controller,
            executor=executors.build,
            additional_artifact_roots=(artifact,),
        ),
        derive=OfficialDeriveAdapter(
            artifact_root=controller,
            executor=executors.derive,
            additional_artifact_roots=(artifact,),
        ),
        local_validate=OfficialLocalValidateAdapter(
            artifact_root=controller,
            executor=executors.local_validate,
            profile_builder=executors.profile_builder,
            additional_artifact_roots=(artifact, profiles),
        ),
        deploy=OfficialDeployAdapter(
            artifact_root=artifact,
            executor=deploy,
            additional_artifact_roots=(controller,),
        ),
        consumer_validate=OfficialConsumerValidateAdapter(
            artifact_root=artifact,
            executor=consumer,
            additional_artifact_roots=(controller, profiles),
        ),
    )


__all__: Sequence[str] = (
    "MonthlyWorkerExecutors",
    "build_monthly_worker_registry",
)
