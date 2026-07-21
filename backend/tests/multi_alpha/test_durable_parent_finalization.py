from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

import pytest

from backend.services.multi_alpha.durable_execution_adapter import DurablePublishedArtifacts
from backend.services.multi_alpha.durable_orchestrator import (
    DurableBusinessResultAssembler,
    _parent_status,
)


class _Repository:
    def __init__(self) -> None:
        self.attempts = {
            "baseline_attempt": {"attempt_id": "baseline_attempt", "status": "succeeded"},
            "scheme_attempt": {"attempt_id": "scheme_attempt", "status": "succeeded"},
            "loo_attempt": {"attempt_id": "loo_attempt", "status": "succeeded"},
        }
        self.scheme_results: list[dict[str, Any]] = []
        self.loo_results: list[dict[str, Any]] = []
        self.child_transitions: list[dict[str, Any]] = []

    def get_attempt(self, attempt_id: str) -> Mapping[str, Any] | None:
        return self.attempts.get(attempt_id)

    def transition_child_with_event(self, child_id: str, **kwargs: Any) -> dict[str, Any]:
        self.child_transitions.append({"child_id": child_id, **kwargs})
        return {"child_id": child_id, "status": kwargs["next_status"]}

    def finalize_scheme_child_result(
        self,
        child_id: str,
        *,
        selected_attempt_id: str,
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        self.scheme_results.append(
            {
                "child_id": child_id,
                "selected_attempt_id": selected_attempt_id,
                **dict(result),
            }
        )
        return {"child_id": child_id, "status": "succeeded"}

    def finalize_scheme_child_without_result(self, child_id: str, **kwargs: Any) -> dict[str, Any]:
        self.scheme_results.append({"child_id": child_id, **kwargs})
        return {"child_id": child_id, "status": kwargs["next_status"]}

    def finalize_loo_child_result(
        self,
        child_id: str,
        *,
        selected_attempt_id: str,
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        self.loo_results.append(
            {
                "child_id": child_id,
                "selected_attempt_id": selected_attempt_id,
                **dict(result),
            }
        )
        return {"child_id": child_id, "status": "succeeded"}


class _Adapter:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.metrics = {
            "baseline": {"cagr": 0.30, "sharpe": 1.00, "calmar": 2.00},
            "scheme": {
                "cagr": 0.50,
                "max_drawdown": -0.10,
                "sharpe": 1.40,
                "calmar": 3.00,
                "topk_return_20": 0.08,
                "topk_hit_rate_20": 0.60,
                "turnover": 1.20,
                "pred_persisted": True,
            },
            "loo": {"cagr": 0.42, "sharpe": 1.10, "calmar": 2.40},
        }

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
            "weights": {"leg_a": 0.6, "leg_b": 0.4},
            "per_window_weights": [{"window": 1, "weights": {"leg_a": 0.6, "leg_b": 0.4}}],
        }


def _children() -> list[dict[str, Any]]:
    return [
        {
            "child_id": "baseline",
            "child_key": "baseline:leg_a",
            "child_kind": "baseline",
            "status": "succeeded",
            "selected_attempt_id": "baseline_attempt",
        },
        {
            "child_id": "scheme",
            "child_key": "scheme:equal",
            "child_kind": "scheme",
            "weighting_scheme": "equal",
            "status": "reconciling",
            "selected_attempt_id": "scheme_attempt",
        },
        {
            "child_id": "loo",
            "child_key": "loo:equal:drop:leg_b",
            "child_kind": "loo",
            "weighting_scheme": "equal",
            "dropped_leg_id": "leg_b",
            "status": "reconciling",
            "selected_attempt_id": "loo_attempt",
        },
    ]


def test_scheme_and_loo_business_rows_reuse_existing_metric_formulas(tmp_path: Path) -> None:
    repository = _Repository()
    adapter = _Adapter(tmp_path)
    assembler = DurableBusinessResultAssembler(
        repository=repository,  # type: ignore[arg-type]
        adapter=adapter,  # type: ignore[arg-type]
    )
    run = {"id": "macb_test", "baseline_leg_id": "leg_a"}
    children = _children()

    assert assembler.assemble_child(run=run, child=children[1], children=children) is True
    scheme = repository.scheme_results[0]
    assert scheme["sharpe"] == 1.40
    assert scheme["vs_baseline_sharpe_delta"] == pytest.approx(0.40)
    assert scheme["vs_baseline_calmar_delta"] == pytest.approx(1.00)
    assert scheme["weights_json"] == {"leg_a": 0.6, "leg_b": 0.4}
    assert scheme["pred_persisted"] is True

    children[1]["status"] = "succeeded"
    assert assembler.assemble_child(run=run, child=children[2], children=children) is True
    loo = repository.loo_results[0]
    assert loo["marginal_sharpe"] == pytest.approx(0.30)
    assert loo["marginal_calmar"] == pytest.approx(0.60)
    assert loo["marginal_cagr"] == pytest.approx(0.08)


def test_loo_waits_for_full_scheme_and_becomes_explicitly_not_computable() -> None:
    repository = _Repository()
    assembler = DurableBusinessResultAssembler(
        repository=repository,  # type: ignore[arg-type]
        adapter=_Adapter(Path("unused")),  # type: ignore[arg-type]
    )
    run = {"id": "macb_test", "baseline_leg_id": "leg_a"}
    children = _children()

    assert assembler.assemble_child(run=run, child=children[2], children=children) is False
    children[1]["status"] = "failed"
    assert assembler.assemble_child(run=run, child=children[2], children=children) is True
    transition = repository.child_transitions[-1]
    assert transition["next_status"] == "not_computable"
    assert transition["reason_code"] == "loo_full_scheme_unavailable"


def test_parent_status_uses_baseline_and_successful_scheme_business_semantics() -> None:
    children: Sequence[Mapping[str, Any]] = _children()
    children[1]["status"] = "succeeded"  # type: ignore[index]
    children[2]["status"] = "not_computable"  # type: ignore[index]
    status, reason = _parent_status(
        children=children,
        run={"baseline_leg_id": "leg_a"},
    )
    assert status == "partial_failed"
    assert reason["failed_child_tasks"] == {"loo:equal:drop:leg_b": "not_computable"}

    failed_baseline = [dict(item) for item in children]
    failed_baseline[0]["status"] = "failed"
    status, reason = _parent_status(
        children=failed_baseline,
        run={"baseline_leg_id": "leg_a"},
    )
    assert status == "failed"
    assert reason["reason_code"] == "multi_alpha_baseline_failed"


def test_partial_recovered_requires_every_child_in_recovery_scope_to_succeed() -> None:
    children = [
        {
            "child_id": "macbc_target",
            "child_key": "scheme:equal",
            "child_kind": "scheme",
            "status": "failed",
        },
        {
            "child_id": "macbc_preserved",
            "child_key": "scheme:risk_parity",
            "child_kind": "scheme",
            "status": "not_recovered",
        },
    ]

    status, reason = _parent_status(
        children=children,
        run={"recovery_kind": "child_targeted", "baseline_leg_id": None},
    )

    assert status == "failed"
    assert reason["reason_code"] == "multi_alpha_no_successful_scheme"
    assert reason["failed_child_tasks"] == {
        "scheme:equal": "failed",
        "scheme:risk_parity": "not_recovered",
    }


def test_partial_recovered_preserves_unavailable_siblings_after_successful_recovery() -> None:
    children = [
        {
            "child_id": "macbc_target",
            "child_key": "scheme:equal",
            "child_kind": "scheme",
            "status": "succeeded",
        },
        {
            "child_id": "macbc_preserved",
            "child_key": "scheme:risk_parity",
            "child_kind": "scheme",
            "status": "not_recovered",
        },
    ]

    status, reason = _parent_status(
        children=children,
        run={"recovery_kind": "child_targeted", "baseline_leg_id": None},
    )

    assert status == "partial_recovered"
    assert reason["reason_code"] == "recovery_scope_completed_with_preserved_unavailable"
    assert reason["preserved_unavailable"] == [
        {
            "child_id": "macbc_preserved",
            "child_key": "scheme:risk_parity",
            "status": "not_recovered",
        }
    ]


def test_zero_child_cancel_is_cancelled_not_vacuous_success() -> None:
    status, reason = _parent_status(
        children=[],
        run={"status": "cancel_requested", "baseline_leg_id": None},
    )

    assert status == "cancelled"
    assert reason["reason_code"] == "operator_cancelled"
    assert reason["successful_child_count"] == 0
    assert reason["preserved_results"] is False
