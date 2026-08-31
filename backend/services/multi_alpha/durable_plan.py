from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from backend.services.multi_alpha.combine_backtest import (
    CombineBacktestRequest,
    request_snapshot_for,
    resolved_prediction_task_selection,
)
from backend.services.multi_alpha.durable_models import (
    DurableAttemptSpec,
    DurableChildSpec,
    DurableRunSpec,
    artifact_manifest_hash_for,
    make_attempt_id,
    make_child_id,
)
from backend.services.multi_alpha.durable_repository import MultiAlphaDurableRepository


PLANNER_VERSION = "multi_alpha_child_plan_v1"


@dataclass(frozen=True)
class DurableChildPlanResult:
    run_id: str
    planner_version: str
    children: tuple[Mapping[str, Any], ...]
    initial_attempts: tuple[Mapping[str, Any], ...]


class DeterministicChildPlanner:
    """Persist the complete child identity plan before materialization starts."""

    def __init__(self, repository: MultiAlphaDurableRepository) -> None:
        self._repository = repository

    def plan(
        self,
        *,
        run_spec: DurableRunSpec,
        request: CombineBacktestRequest,
    ) -> DurableChildPlanResult:
        child_specs = self.build_child_specs(run_spec=run_spec, request=request)
        children: list[Mapping[str, Any]] = []

        for child_spec in child_specs:
            child = self._repository.create_child(child_spec)
            children.append(child)

        return DurableChildPlanResult(
            run_id=run_spec.run_id,
            planner_version=PLANNER_VERSION,
            children=tuple(children),
            initial_attempts=(),
        )

    def ensure_initial_attempt(
        self,
        *,
        child_id: str,
        node_id: str,
    ) -> Mapping[str, Any]:
        existing_attempts = self._repository.list_attempts(child_id)
        initial = next(
            (row for row in existing_attempts if int(row.get("attempt_no") or 0) == 1),
            None,
        )
        if initial is not None:
            return initial
        return self._repository.create_attempt(
            DurableAttemptSpec(
                attempt_id=make_attempt_id(child_id, 1),
                child_id=child_id,
                attempt_no=1,
                retry_mode="initial",
                node_id=node_id,
                status="queued",
                phase="queued",
            )
        )

    @staticmethod
    def build_child_specs(
        *,
        run_spec: DurableRunSpec,
        request: CombineBacktestRequest,
    ) -> tuple[DurableChildSpec, ...]:
        snapshot = request_snapshot_for(request)
        roster = tuple(snapshot["roster"])
        prediction_source_refs = [
            {
                "leg_id": row["leg_id"],
                "seed_run_ids": list(row.get("seed_run_ids") or []),
            }
            for row in roster
        ]
        common_manifest: dict[str, Any] = {
            "schema_version": "multi_alpha_child_input_manifest_v1",
            "planner_version": PLANNER_VERSION,
            "run_id": run_spec.run_id,
            "request_hash": run_spec.request_hash,
            "roster_hash": run_spec.roster_hash,
            "oos_start": str(run_spec.oos_start),
            "oos_end": str(run_spec.oos_end),
            "normalize_method": run_spec.normalize_method,
            "walk_forward_hash": artifact_manifest_hash_for(dict(run_spec.walk_forward)),
            "backtest_config_hash": artifact_manifest_hash_for(dict(run_spec.backtest_config)),
            "prediction_source_refs": prediction_source_refs,
        }
        if request.prediction_task_selection is not None:
            common_manifest["prediction_task_selection"] = dict(
                request.prediction_task_selection
            )
        # Preserve the P0-1B child-manifest byte shape when the additive P0-2
        # persistence schema is not deployed.  Once P0-2 is available, even
        # an incomplete identity is an explicit persisted evidence payload.
        if (
            run_spec.execution_identity is not None
            or run_spec.execution_identity_hash is not None
            or run_spec.execution_identity_evidence is not None
        ):
            common_manifest.update(
                {
                    "execution_identity": (
                        dict(run_spec.execution_identity)
                        if run_spec.execution_identity is not None
                        else None
                    ),
                    "execution_identity_hash": run_spec.execution_identity_hash,
                    "execution_identity_evidence": (
                        dict(run_spec.execution_identity_evidence)
                        if run_spec.execution_identity_evidence is not None
                        else None
                    ),
                }
            )
        specs: list[DurableChildSpec] = []
        ordinal = 0
        task_selection = resolved_prediction_task_selection(request)

        if request.baseline_leg_id and task_selection["include_baseline"]:
            specs.append(
                _child_spec(
                    run_spec=run_spec,
                    child_key=f"baseline:{request.baseline_leg_id}",
                    child_kind="baseline",
                    ordinal=ordinal,
                    common_manifest=common_manifest,
                    baseline_leg_id=request.baseline_leg_id,
                )
            )
            ordinal += 1

        leg_ids = sorted(str(row["leg_id"]) for row in roster)
        for scheme in request.weighting_schemes:
            specs.append(
                _child_spec(
                    run_spec=run_spec,
                    child_key=f"scheme:{scheme}",
                    child_kind="scheme",
                    ordinal=ordinal,
                    common_manifest=common_manifest,
                    weighting_scheme=scheme,
                )
            )
            ordinal += 1
            if len(roster) <= 2 or not task_selection["include_loo"]:
                continue
            for dropped_leg_id in leg_ids:
                specs.append(
                    _child_spec(
                        run_spec=run_spec,
                        child_key=f"loo:{scheme}:drop:{dropped_leg_id}",
                        child_kind="loo",
                        ordinal=ordinal,
                        common_manifest=common_manifest,
                        weighting_scheme=scheme,
                        dropped_leg_id=dropped_leg_id,
                    )
                )
                ordinal += 1
        return tuple(specs)


def _child_spec(
    *,
    run_spec: DurableRunSpec,
    child_key: str,
    child_kind: str,
    ordinal: int,
    common_manifest: Mapping[str, Any],
    baseline_leg_id: str | None = None,
    weighting_scheme: str | None = None,
    dropped_leg_id: str | None = None,
) -> DurableChildSpec:
    input_manifest = {
        **dict(common_manifest),
        "child_key": child_key,
        "child_kind": child_kind,
        "baseline_leg_id": baseline_leg_id,
        "weighting_scheme": weighting_scheme,
        "dropped_leg_id": dropped_leg_id,
    }
    return DurableChildSpec(
        child_id=make_child_id(run_spec.run_id, child_key),
        run_id=run_spec.run_id,
        child_key=child_key,
        child_kind=child_kind,
        ordinal=ordinal,
        input_manifest=input_manifest,
        input_manifest_hash=artifact_manifest_hash_for(input_manifest),
        weighting_scheme=weighting_scheme,
        dropped_leg_id=dropped_leg_id,
    )
