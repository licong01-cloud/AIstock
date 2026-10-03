"""Default SOURCE-blocked preparation, never a second release entry point."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Mapping

from .artifact_ready_build_source import ArtifactReadyPreparationBuildSource
from .artifact_ready_source import ArtifactReadySourceBuilder
from .cas_store import CASStore
from .canonical import ensure_sha256
from .contracts import Component
from .monthly_component_preparation import ComponentPreparationError, preparation_plan
from .monthly_mature_build_runner import MonthlyBuildExecutionScopeFactory
from .monthly_preparation_artifacts import (
    _GATES as PHYSICAL_GATES,
    normalize_preparation_artifacts,
    require_preparation_domain_audit,
)
from .monthly_preparation_executor import MonthlyPrivatePhysicalPreparationExecutor
from .monthly_preparation_shared import (
    _GATES as SHARED_GATES,
    MonthlyPrivateSharedPreparationExecutor,
)
from .monthly_preparation_source import PreparationSourceSnapshot
from .monthly_snapshot import MonthlySnapshotIdentity
from .monthly_worker import ProducerContext
from .profile import DatasetProfile


_PHYSICAL = {"day": Component.DAILY_BIN, "minute": Component.MINUTE_BIN,
             "index": Component.DOMESTIC_INDEX_CONTEXT}


def preparation_eligible_domains(
    *, profile: DatasetProfile, snapshot: PreparationSourceSnapshot, audit: Mapping[str, Any],
) -> Mapping[str, Mapping[str, Any]]:
    # Calendar/lifecycle is a common mandatory dependency. This validates the
    # entire audit's typed counts/snapshot identity before selecting any work.
    require_preparation_domain_audit(
        profile=profile, snapshot=snapshot, audit=audit,
        required_gates=frozenset({"calendar_lifecycle"}),
    )
    expected = preparation_plan(operation_id=snapshot.operation_id,
                                cutoff=snapshot.official_cutoff,
                                blocking_datasets=snapshot.omitted_datasets)
    if audit.get("plan") != expected:
        raise ComponentPreparationError("private preparation dependency plan differs")
    gates = {row["gate_id"]: row for row in audit["gates"]}
    available = {row.spec.dataset for row in snapshot.partitions} | {"stock_universe_pit"}
    domains = {}
    for name, row in expected["components"].items():
        if row["status"] == "DEFERRED":
            domains[name] = {"status": "DEFERRED", "blocking_datasets": row["blocking_datasets"],
                             "blocking_gates": []}
            continue
        missing = set(row["required_datasets"]) - available
        if missing:
            raise ComponentPreparationError(f"eligible private source datasets are missing: {name}:{sorted(missing)}")
        required = PHYSICAL_GATES.get(name, SHARED_GATES.get(name))
        if required is None:
            raise ComponentPreparationError(f"private executor has no implementation for eligible domain: {name}")
        blocked = sorted(gate for gate in required if any(
            gates[gate][field] for field in ("unexplained_missing_count", "duplicate_count", "invalid_value_count")
        ))
        domains[name] = {"status": "DEFERRED" if blocked else "ELIGIBLE",
                         "blocking_datasets": [], "blocking_gates": blocked}
    if domains["day"]["status"] == "ELIGIBLE" and domains["index"]["status"] != "ELIGIBLE":
        domains["day"] = {"status": "DEFERRED", "blocking_datasets": ["index_daily"],
                          "blocking_gates": domains["index"]["blocking_gates"]}
    return domains


@dataclass(frozen=True, slots=True)
class MonthlyPrivatePreparationExecutor:
    profile: DatasetProfile
    cas: CASStore
    project_root: Path
    execution_scope_factory: MonthlyBuildExecutionScopeFactory
    sector_membership_start: date

    def __call__(
        self, context: ProducerContext, snapshot: PreparationSourceSnapshot,
        audit: Mapping[str, Any], identity: MonthlySnapshotIdentity,
        *, checkpoint: Callable[[], None] = lambda: None,
    ) -> Mapping[str, Any]:
        snapshot_id = f"postgres:{identity.snapshot_id}"
        if (
            context.stage != "SOURCE" or context.operation_id != snapshot.operation_id
            or type(context.attempt) is not int or context.attempt < 1
            or not isinstance(context.plan.get("predecessor"), Mapping)
            or context.plan.get("target_cutoff") != snapshot.official_cutoff.isoformat()
            or any(gate.get("snapshot_group_id") != snapshot_id for gate in audit.get("gates", ()))
        ):
            raise ComponentPreparationError("default private executor handoff differs")
        ensure_sha256(str(context.plan["predecessor"].get("profile_sha256") or ""), field="predecessor profile")
        domains = {name: dict(row) for name, row in preparation_eligible_domains(
            profile=self.profile, snapshot=snapshot, audit=audit,
        ).items()}
        selected = tuple(component for name, component in _PHYSICAL.items()
                         if domains[name]["status"] == "ELIGIBLE")
        if selected:
            bundle = normalize_preparation_artifacts(
                ArtifactReadySourceBuilder(self.profile, self.cas), snapshot=snapshot,
                audit=audit, components=selected, checkpoint=checkpoint,
            )
            source = ArtifactReadyPreparationBuildSource(
                cas=self.cas, profile=self.profile, snapshot=snapshot, reference=bundle.reference,
            )
            result = MonthlyPrivatePhysicalPreparationExecutor(
                self.profile, self.cas, self.project_root, self.execution_scope_factory,
            ).execute(context=context, source=source, source_snapshot_id=snapshot_id, checkpoint=checkpoint)
            for name, component in _PHYSICAL.items():
                if component in selected:
                    domains[name] = {"status": "PREPARED_UNPUBLISHED",
                                     "record": result["prepared_components"][component.value]}
        shared = MonthlyPrivateSharedPreparationExecutor(self.profile, self.cas, self.sector_membership_start)
        for name in SHARED_GATES:
            checkpoint()
            if domains[name]["status"] == "ELIGIBLE":
                record = shared.execute(context=context, snapshot=snapshot, audit=audit, component=name,
                                        source_snapshot_id=snapshot_id, checkpoint=checkpoint)
                domains[name] = {"status": "PREPARED_UNPUBLISHED", "record": record}
        count = sum(row["status"] == "PREPARED_UNPUBLISHED" for row in domains.values())
        return {
            "status": "COMPONENTS_PREPARED_UNPUBLISHED" if count else "SOURCE_PREPARED_UNPUBLISHED",
            "operation_id": context.operation_id, "components": domains,
            "prepared_component_count": count,
            "deferred_component_count": sum(row["status"] == "DEFERRED" for row in domains.values()),
            "consistent_input_set_complete": False, "publication_allowed": False,
            "database_write_performed": False,
        }
