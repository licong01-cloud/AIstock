from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from backend.services.multi_alpha.combine_backtest import (
    InMemoryCombineBacktestRepository,
    MultiAlphaCombineBacktestService,
    _PredictionTask,
    _PredictionTaskOutcome,
)
from backend.services.multi_alpha.durable_execution_adapter import DurablePublishedArtifacts
from backend.services.multi_alpha.durable_orchestrator import DurableBusinessResultAssembler


class _DurableRepository:
    def __init__(self) -> None:
        self.attempts = {
            "baseline_attempt": {"status": "succeeded"},
            "scheme_attempt": {"status": "succeeded"},
            "loo_attempt": {"status": "succeeded"},
        }
        self.scheme: dict[str, Any] | None = None
        self.loo: dict[str, Any] | None = None

    def get_attempt(self, attempt_id: str) -> Mapping[str, Any] | None:
        return self.attempts.get(attempt_id)

    def finalize_scheme_child_result(
        self,
        _child_id: str,
        *,
        selected_attempt_id: str,
        result: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        self.scheme = dict(result)
        return {"status": "succeeded", "selected_attempt_id": selected_attempt_id}

    def finalize_loo_child_result(
        self,
        _child_id: str,
        *,
        selected_attempt_id: str,
        result: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        self.loo = dict(result)
        return {"status": "succeeded", "selected_attempt_id": selected_attempt_id}


class _Adapter:
    def __init__(self, root: Path, metrics: Mapping[str, Mapping[str, Any]]) -> None:
        self.root = root
        self.metrics = metrics

    def load_published_artifacts(
        self,
        *,
        run_id: str,
        child_id: str,
        attempt_id: str,
    ) -> DurablePublishedArtifacts:
        workspace = self.root / child_id / attempt_id
        return DurablePublishedArtifacts(
            workspace=workspace,
            prediction_path=workspace / "combined_prediction.pkl",
            artifact_manifest_path=workspace / "artifact_manifest.json",
            artifact_manifest={"manifest_hash": "a" * 64},
        )

    def load_collected_metrics(self, artifacts: DurablePublishedArtifacts) -> Mapping[str, Any]:
        return self.metrics[artifacts.workspace.parent.name]

    def load_materialization_metadata(self, _artifacts: DurablePublishedArtifacts) -> Mapping[str, Any]:
        return {
            "weights": {"leg_a": 0.55, "leg_b": 0.45},
            "per_window_weights": [{"window": 1, "weights": {"leg_a": 0.55, "leg_b": 0.45}}],
        }


def test_durable_result_rows_match_existing_combine_persistence_formula(tmp_path: Path) -> None:
    metrics = {
        "baseline": {"cagr": 0.3, "sharpe": 1.0, "calmar": 2.0},
        "scheme": {
            "cagr": 0.5,
            "max_drawdown": -0.1,
            "sharpe": 1.4,
            "calmar": 3.0,
            "topk_return_20": 0.08,
            "topk_hit_rate_20": 0.6,
            "turnover": 1.2,
            "pred_persisted": True,
        },
        "loo": {"cagr": 0.42, "sharpe": 1.1, "calmar": 2.4},
    }
    legacy_repository = InMemoryCombineBacktestRepository()
    legacy_service = MultiAlphaCombineBacktestService(
        repository=legacy_repository,
        legacy_execution_mode_for_tests=True,
    )
    outcomes = [
        _PredictionTaskOutcome(
            task=_PredictionTask(name="baseline_leg_a", kind="baseline", frame=None),  # type: ignore[arg-type]
            metrics=metrics["baseline"],
        ),
        _PredictionTaskOutcome(
            task=_PredictionTask(
                name="combined_equal",
                kind="scheme",
                scheme="equal",
                frame=None,  # type: ignore[arg-type]
                weights_json={"leg_a": 0.55, "leg_b": 0.45},
                per_window_weights_json=(
                    {"window": 1, "weights": {"leg_a": 0.55, "leg_b": 0.45}},
                ),
            ),
            metrics=metrics["scheme"],
        ),
        _PredictionTaskOutcome(
            task=_PredictionTask(
                name="loo_equal_drop_leg_b",
                kind="loo",
                scheme="equal",
                dropped_leg_id="leg_b",
                frame=None,  # type: ignore[arg-type]
            ),
            metrics=metrics["loo"],
        ),
    ]
    legacy = legacy_service._persist_task_outcomes(run_id="macb_parity", outcomes=outcomes)

    durable_repository = _DurableRepository()
    assembler = DurableBusinessResultAssembler(
        repository=durable_repository,  # type: ignore[arg-type]
        adapter=_Adapter(tmp_path, metrics),  # type: ignore[arg-type]
    )
    run = {"id": "macb_parity", "baseline_leg_id": "leg_a"}
    children = [
        {
            "child_id": "baseline",
            "child_kind": "baseline",
            "status": "succeeded",
            "selected_attempt_id": "baseline_attempt",
        },
        {
            "child_id": "scheme",
            "child_kind": "scheme",
            "weighting_scheme": "equal",
            "status": "reconciling",
            "selected_attempt_id": "scheme_attempt",
        },
        {
            "child_id": "loo",
            "child_kind": "loo",
            "weighting_scheme": "equal",
            "dropped_leg_id": "leg_b",
            "status": "reconciling",
            "selected_attempt_id": "loo_attempt",
        },
    ]
    assert assembler.assemble_child(run=run, child=children[1], children=children)
    children[1]["status"] = "succeeded"
    assert assembler.assemble_child(run=run, child=children[2], children=children)

    assert durable_repository.scheme == legacy["scheme_results"][0]
    assert durable_repository.loo == legacy["loo"][0]
