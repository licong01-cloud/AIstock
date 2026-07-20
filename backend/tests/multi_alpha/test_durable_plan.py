from __future__ import annotations

from copy import deepcopy
from typing import Any

from backend.services.multi_alpha.combine_backtest import parse_request, request_snapshot_for, roster_hash_for
from backend.services.multi_alpha.durable_models import (
    DurableRunSpec,
    durable_run_request_payload,
    request_hash_for,
)
from backend.services.multi_alpha.durable_plan import DeterministicChildPlanner, PLANNER_VERSION


def _payload() -> dict[str, Any]:
    return {
        "roster": [
            {"leg_id": "leg_b", "seed_run_ids": ["qe_b_L1"], "metadata": {}},
            {"leg_id": "leg_a", "seed_run_ids": ["qe_a_L1"], "metadata": {}},
            {"leg_id": "leg_c", "seed_run_ids": ["qe_c_L1"], "metadata": {}},
        ],
        "oos_start": "2024-07-01",
        "oos_end": "2026-06-29",
        "weighting_schemes": ["ic_weighted", "equal"],
        "normalize_method": "rank",
        "walk_forward": {"enabled": True, "window": 60, "min_periods": 20, "expanding": False},
        "backtest_config": {
            "node_id": "wsl2-5080",
            "node_parallelism": {"wsl2-5080": 2},
            "topk": 25,
        },
        "baseline_leg_id": "leg_b",
        "topk": 25,
        "run_async": True,
        "scheme_timeout_seconds": 120,
        "run_timeout_seconds": 600,
    }


class InMemoryPlanningRepository:
    def __init__(self) -> None:
        self.children: dict[str, dict[str, Any]] = {}
        self.attempts: dict[str, dict[str, Any]] = {}

    def create_child(self, spec: Any) -> dict[str, Any]:
        existing = self.children.get(spec.child_id)
        row = {
            "child_id": spec.child_id,
            "run_id": spec.run_id,
            "child_key": spec.child_key,
            "child_kind": spec.child_kind,
            "weighting_scheme": spec.weighting_scheme,
            "dropped_leg_id": spec.dropped_leg_id,
            "ordinal": spec.ordinal,
            "status": spec.status,
            "input_manifest_json": deepcopy(dict(spec.input_manifest)),
            "input_manifest_hash": spec.input_manifest_hash,
        }
        if existing is not None:
            assert existing == row
            return deepcopy(existing)
        self.children[spec.child_id] = row
        return deepcopy(row)

    def list_attempts(self, child_id: str) -> list[dict[str, Any]]:
        return [
            deepcopy(row)
            for row in self.attempts.values()
            if row["child_id"] == child_id
        ]

    def create_attempt(self, spec: Any) -> dict[str, Any]:
        existing = next(
            (
                row
                for row in self.attempts.values()
                if row["child_id"] == spec.child_id and row["attempt_no"] == spec.attempt_no
            ),
            None,
        )
        row = {
            "attempt_id": spec.attempt_id,
            "child_id": spec.child_id,
            "attempt_no": spec.attempt_no,
            "retry_mode": spec.retry_mode,
            "node_id": spec.node_id,
            "status": spec.status,
            "phase": spec.phase,
        }
        if existing is not None:
            assert existing == row
            return deepcopy(existing)
        self.attempts[spec.attempt_id] = row
        return deepcopy(row)


def _request_and_run() -> tuple[Any, DurableRunSpec]:
    request = parse_request(_payload())
    roster_hash = roster_hash_for(request.roster)
    roster = request_snapshot_for(request)["roster"]
    run_payload = durable_run_request_payload(
        roster_hash=roster_hash,
        roster=roster,
        oos_start=request.oos_start,
        oos_end=request.oos_end,
        normalize_method=request.normalize_method,
        walk_forward=request.walk_forward,
        backtest_config=request.backtest_config,
        baseline_leg_id=request.baseline_leg_id,
        node_parallelism={"wsl2-5080": 2},
    )
    return request, DurableRunSpec(
        run_id="macb_plan_test",
        task_id="mact_plan_test",
        request_hash=request_hash_for(run_payload),
        roster_hash=roster_hash,
        roster=roster,
        oos_start=request.oos_start,
        oos_end=request.oos_end,
        normalize_method=request.normalize_method,
        walk_forward=request.walk_forward,
        backtest_config=request.backtest_config,
        baseline_leg_id=request.baseline_leg_id,
        node_parallelism={"wsl2-5080": 2},
    )


def test_repeated_planner_is_idempotent_and_creates_complete_plan() -> None:
    request, run_spec = _request_and_run()
    repository = InMemoryPlanningRepository()
    planner = DeterministicChildPlanner(repository)  # type: ignore[arg-type]

    first = planner.plan(run_spec=run_spec, request=request)
    second = planner.plan(run_spec=run_spec, request=request)

    assert first == second
    assert first.planner_version == PLANNER_VERSION
    assert len(first.children) == 9
    assert len(first.initial_attempts) == 0
    assert len(repository.children) == 9
    assert len(repository.attempts) == 0
    assert [row["ordinal"] for row in first.children] == list(range(9))
    assert [row["child_key"] for row in first.children] == [
        "baseline:leg_b",
        "scheme:ic_weighted",
        "loo:ic_weighted:drop:leg_a",
        "loo:ic_weighted:drop:leg_b",
        "loo:ic_weighted:drop:leg_c",
        "scheme:equal",
        "loo:equal:drop:leg_a",
        "loo:equal:drop:leg_b",
        "loo:equal:drop:leg_c",
    ]


def test_child_manifest_contains_frozen_request_and_prediction_lineage() -> None:
    request, run_spec = _request_and_run()
    specs = DeterministicChildPlanner.build_child_specs(run_spec=run_spec, request=request)
    manifest = specs[0].input_manifest

    assert manifest["schema_version"] == "multi_alpha_child_input_manifest_v1"
    assert manifest["planner_version"] == PLANNER_VERSION
    assert manifest["request_hash"] == run_spec.request_hash
    assert manifest["roster_hash"] == run_spec.roster_hash
    assert manifest["oos_start"] == "2024-07-01"
    assert manifest["oos_end"] == "2026-06-29"
    assert manifest["backtest_config_hash"]
    assert manifest["walk_forward_hash"]
    assert manifest["prediction_source_refs"] == [
        {"leg_id": "leg_b", "seed_run_ids": ["qe_b_L1"]},
        {"leg_id": "leg_a", "seed_run_ids": ["qe_a_L1"]},
        {"leg_id": "leg_c", "seed_run_ids": ["qe_c_L1"]},
    ]


def test_initial_attempt_is_created_only_after_child_is_materialized_and_is_deterministic() -> None:
    request, run_spec = _request_and_run()
    first_repository = InMemoryPlanningRepository()
    second_repository = InMemoryPlanningRepository()

    first_planner = DeterministicChildPlanner(first_repository)  # type: ignore[arg-type]
    second_planner = DeterministicChildPlanner(second_repository)  # type: ignore[arg-type]
    first = first_planner.plan(
        run_spec=run_spec,
        request=request,
    )
    second = second_planner.plan(
        run_spec=run_spec,
        request=request,
    )

    first_attempts = [
        first_planner.ensure_initial_attempt(
            child_id=str(child["child_id"]),
            node_id="wsl2-5080",
        )
        for child in first.children
    ]
    second_attempts = [
        second_planner.ensure_initial_attempt(
            child_id=str(child["child_id"]),
            node_id="wsl2-5080",
        )
        for child in second.children
    ]
    replay = first_planner.ensure_initial_attempt(
        child_id=str(first.children[0]["child_id"]),
        node_id="wsl2-5080",
    )

    assert len(first_attempts) == len(second_attempts) == 9
    assert [row["attempt_id"] for row in first_attempts] == [
        row["attempt_id"] for row in second_attempts
    ]
    assert replay == first_attempts[0]
