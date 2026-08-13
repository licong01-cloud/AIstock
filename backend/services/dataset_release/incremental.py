from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

from .canonical import digest_named_fields, ensure_sha256
from .contracts import Component, ComponentAction
from .copy_on_write import (
    CopyOnWritePlan,
    assert_writer_targets_private,
    normalize_relative_path,
    prepare_copy_on_write_tree,
    verify_source_unchanged,
)
from .decision import ActionPlan, ComponentPlan
from .errors import DecisionError


INCREMENTAL_PLAN_SCHEMA_VERSION = "dataset_release_incremental_plan_v1"


@dataclass(frozen=True)
class MutationScope:
    component: Component
    partition_key: str
    instruments: tuple[str, ...]
    start: date | None
    end: date | None
    chunk_ids: tuple[str, ...]
    writer_targets: tuple[str, ...]
    invalidation_edges: tuple[str, ...]
    reason: str
    create_new_targets: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.partition_key or not self.reason.strip():
            raise DecisionError("incremental mutation scope requires identity and reason")
        if self.start is not None and self.end is not None and self.end < self.start:
            raise DecisionError("incremental mutation scope end precedes start")
        if not self.writer_targets:
            raise DecisionError("incremental mutation scope requires explicit writer targets")
        normalized_targets = tuple(sorted(normalize_relative_path(value) for value in self.writer_targets))
        if len(normalized_targets) != len(set(normalized_targets)):
            raise DecisionError("incremental mutation scope has duplicate writer targets")
        object.__setattr__(self, "writer_targets", normalized_targets)
        normalized_create = tuple(sorted(normalize_relative_path(value) for value in self.create_new_targets))
        if len(normalized_create) != len(set(normalized_create)) or not set(normalized_create).issubset(
            normalized_targets
        ):
            raise DecisionError("create-new targets must be a unique subset of writer targets")
        object.__setattr__(self, "create_new_targets", normalized_create)

    @property
    def replace_existing_targets(self) -> tuple[str, ...]:
        return tuple(value for value in self.writer_targets if value not in self.create_new_targets)

    def as_dict(self) -> dict[str, Any]:
        return {
            "component": self.component.value,
            "partition_key": self.partition_key,
            "instruments": list(self.instruments),
            "start": self.start.isoformat() if self.start else None,
            "end": self.end.isoformat() if self.end else None,
            "chunk_ids": list(self.chunk_ids),
            "writer_targets": list(self.writer_targets),
            "replace_existing_targets": list(self.replace_existing_targets),
            "create_new_targets": list(self.create_new_targets),
            "invalidation_edges": list(self.invalidation_edges),
            "reason": self.reason,
        }


@dataclass(frozen=True)
class IncrementalComponentPlan:
    decision: ComponentPlan
    mutation_scopes: tuple[MutationScope, ...]

    @property
    def writer_targets(self) -> tuple[str, ...]:
        return tuple(sorted({target for scope in self.mutation_scopes for target in scope.writer_targets}))

    @property
    def create_new_targets(self) -> tuple[str, ...]:
        return tuple(sorted({target for scope in self.mutation_scopes for target in scope.create_new_targets}))

    @property
    def replace_existing_targets(self) -> tuple[str, ...]:
        creates = set(self.create_new_targets)
        return tuple(value for value in self.writer_targets if value not in creates)

    def as_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision.as_dict(),
            "mutation_scopes": [item.as_dict() for item in self.mutation_scopes],
            "writer_targets": list(self.writer_targets),
            "replace_existing_targets": list(self.replace_existing_targets),
            "create_new_targets": list(self.create_new_targets),
        }


@dataclass(frozen=True)
class IncrementalExecutionPlan:
    action_plan_digest: str
    components: tuple[IncrementalComponentPlan, ...]

    @property
    def digest(self) -> str:
        return digest_named_fields(
            INCREMENTAL_PLAN_SCHEMA_VERSION,
            {
                "action_plan_digest": self.action_plan_digest,
                "components": [
                    item.as_dict()
                    for item in sorted(
                        self.components,
                        key=lambda value: (
                            value.decision.component.value,
                            value.decision.partition_key,
                        ),
                    )
                ],
            },
        )

    def component(
        self,
        component: Component,
        partition_key: str,
    ) -> IncrementalComponentPlan:
        for item in self.components:
            if item.decision.component is component and item.decision.partition_key == partition_key:
                return item
        raise KeyError((component, partition_key))


def compile_incremental_plan(
    action_plan: ActionPlan,
    mutation_scopes: Mapping[tuple[Component, str], Iterable[MutationScope]],
) -> IncrementalExecutionPlan:
    compiled: list[IncrementalComponentPlan] = []
    seen_keys = {(item.component, item.partition_key) for item in action_plan.actions}
    extra = sorted((component.value, partition) for component, partition in set(mutation_scopes).difference(seen_keys))
    if extra:
        raise DecisionError(f"mutation scopes reference decisions absent from action plan: {extra}")

    for decision in action_plan.actions:
        key = (decision.component, decision.partition_key)
        scopes = tuple(mutation_scopes.get(key, ()))
        for scope in scopes:
            if scope.component is not decision.component or scope.partition_key != decision.partition_key:
                raise DecisionError("mutation scope identity differs from component decision")
        mutating = decision.action in {
            ComponentAction.INCREMENTAL,
            ComponentAction.SELECTIVE_REBUILD,
        }
        if mutating and not scopes:
            raise DecisionError(f"{decision.action.value} requires explicit mutation scopes")
        if not mutating and scopes:
            raise DecisionError(f"{decision.action.value} cannot contain writer mutation scopes")
        if mutating:
            if decision.frozen_reuse is None:
                raise DecisionError(f"{decision.action.value} lacks a frozen baseline")
            if decision.frozen_reuse.component_partition_key != decision.partition_key:
                raise DecisionError("frozen baseline partition differs from component decision")
            planned_targets = tuple(sorted({target for scope in scopes for target in scope.writer_targets}))
            frozen_targets = tuple(
                sorted(normalize_relative_path(target) for target in decision.frozen_reuse.mutation_set)
            )
            if planned_targets != frozen_targets:
                raise DecisionError(
                    "mutation scope writer targets differ from immutable decision",
                    context={
                        "planned": list(planned_targets),
                        "frozen": list(frozen_targets),
                    },
                )
        compiled.append(IncrementalComponentPlan(decision, scopes))
    return IncrementalExecutionPlan(action_plan.digest, tuple(compiled))


def prepare_incremental_component_tree(
    component_plan: IncrementalComponentPlan,
    *,
    source_root: Path,
    target_root: Path,
    source_file_identity: str,
) -> CopyOnWritePlan:
    if component_plan.decision.action not in {
        ComponentAction.INCREMENTAL,
        ComponentAction.SELECTIVE_REBUILD,
    }:
        raise DecisionError("copy-on-write preparation requires an incremental/selective action")
    baseline = component_plan.decision.frozen_reuse
    if baseline is None:
        raise DecisionError("copy-on-write preparation requires frozen baseline evidence")
    actual_file_identity = ensure_sha256(
        source_file_identity,
        field="source_file_identity",
    )
    if actual_file_identity != baseline.file_identity:
        raise DecisionError(
            "source file identity differs from frozen reuse baseline",
            context={
                "expected": baseline.file_identity,
                "actual": actual_file_identity,
            },
        )
    plan = prepare_copy_on_write_tree(
        source_root,
        target_root,
        replace_existing_targets=component_plan.replace_existing_targets,
        create_new_targets=component_plan.create_new_targets,
        source_sealed=True,
        expected_source_merkle=baseline.manifest_root,
    )
    assert_writer_targets_private(plan, component_plan.writer_targets)
    return plan


def verify_incremental_source_unchanged(plan: CopyOnWritePlan) -> str:
    return verify_source_unchanged(plan)


def qfq_denominator_mutation_scope(
    *,
    component: Component,
    partition_key: str,
    instrument: str,
    dataset_start: date,
    cutoff: date,
    writer_targets: Iterable[str],
) -> MutationScope:
    targets = tuple(sorted(set(str(value).replace("\\", "/") for value in writer_targets)))
    return MutationScope(
        component=component,
        partition_key=partition_key,
        instruments=(instrument.upper(),),
        start=dataset_start,
        end=cutoff,
        chunk_ids=(),
        writer_targets=targets,
        invalidation_edges=("adj_factor.denominator->qfq_history",),
        reason="QFQ denominator changed; rebuild the affected instrument history",
    )
