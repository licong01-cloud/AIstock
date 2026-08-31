from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

from backend.services.multi_alpha.durable_models import (
    OwnershipToken,
    artifact_manifest_hash_for,
    durable_run_request_payload,
    make_attempt_id,
    make_child_id,
    request_hash_for,
)
from backend.services.multi_alpha.durable_recovery import (
    DurableRecoveryWorker,
    DurableRecoveryService,
    RecoveryPlanEntry,
    build_successor_recovery_specs,
)
from backend.services.multi_alpha.durable_execution_adapter import (
    DurableCollectedResult,
    DurablePublishedArtifacts,
    DurableSubmissionIntent,
)


RUN_ID = "macb_recovery_source"
TASK_ID = "mact_recovery_source"
BASELINE_ID = make_child_id(RUN_ID, "baseline:leg_a")
SCHEME_ID = make_child_id(RUN_ID, "scheme:equal")
LOO_ID = make_child_id(RUN_ID, "loo:equal:drop:leg_b")


def _input_manifest(*, child_id: str, child_key: str) -> dict[str, Any]:
    return {
        "schema_version": "multi_alpha_child_input_manifest_v1",
        "run_id": RUN_ID,
        "child_id": child_id,
        "child_key": child_key,
        "prediction_source_refs": [{"leg_id": "leg_a", "seed_run_ids": ["seed_1"]}],
    }


def _child(
    *,
    child_id: str,
    child_key: str,
    child_kind: str,
    status: str,
    ordinal: int,
    weighting_scheme: str | None = None,
    dropped_leg_id: str | None = None,
    selected_attempt_id: str | None = None,
) -> dict[str, Any]:
    manifest = _input_manifest(child_id=child_id, child_key=child_key)
    return {
        "child_id": child_id,
        "run_id": RUN_ID,
        "child_key": child_key,
        "child_kind": child_kind,
        "status": status,
        "ordinal": ordinal,
        "weighting_scheme": weighting_scheme,
        "dropped_leg_id": dropped_leg_id,
        "selected_attempt_id": selected_attempt_id,
        "input_manifest_json": manifest,
        "input_manifest_hash": artifact_manifest_hash_for(manifest),
        "prediction_artifact_uri": f"workspace://{child_id}/combined_prediction.pkl",
        "prediction_artifact_hash": "a" * 64,
    }


def _attempt(*, child_id: str, status: str, attempt_no: int = 1) -> dict[str, Any]:
    return {
        "attempt_id": make_attempt_id(child_id, attempt_no),
        "child_id": child_id,
        "attempt_no": attempt_no,
        "status": status,
        "execution_kind": "remote_execution",
        "retry_mode": "initial",
        "artifact_manifest_json": {"manifest_hash": "b" * 64},
        "result_manifest_json": {"schema_version": "multi_alpha_child_result_manifest_v1"},
        "result_manifest_hash": "c" * 64 if status == "succeeded" else None,
    }


class _Repository:
    def __init__(self, *, run_status: str, target_status: str = "failed") -> None:
        baseline_attempt = _attempt(child_id=BASELINE_ID, status="succeeded")
        scheme_attempt = _attempt(child_id=SCHEME_ID, status="succeeded")
        loo_attempt = _attempt(child_id=LOO_ID, status="failed")
        self.run = {
            "id": RUN_ID,
            "task_id": TASK_ID,
            "status": run_status,
            "request_hash": "d" * 64,
            "roster_hash": "e" * 64,
        }
        self.children = {
            BASELINE_ID: _child(
                child_id=BASELINE_ID,
                child_key="baseline:leg_a",
                child_kind="baseline",
                status="succeeded",
                ordinal=0,
                selected_attempt_id=baseline_attempt["attempt_id"],
            ),
            SCHEME_ID: _child(
                child_id=SCHEME_ID,
                child_key="scheme:equal",
                child_kind="scheme",
                status="succeeded",
                ordinal=1,
                weighting_scheme="equal",
                selected_attempt_id=scheme_attempt["attempt_id"],
            ),
            LOO_ID: _child(
                child_id=LOO_ID,
                child_key="loo:equal:drop:leg_b",
                child_kind="loo",
                status=target_status,
                ordinal=2,
                weighting_scheme="equal",
                dropped_leg_id="leg_b",
                selected_attempt_id=loo_attempt["attempt_id"],
            ),
        }
        self.attempts = {
            BASELINE_ID: [baseline_attempt],
            SCHEME_ID: [scheme_attempt],
            LOO_ID: [loo_attempt],
        }

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return deepcopy(self.run) if run_id == RUN_ID else None

    def list_children(self, run_id: str) -> list[dict[str, Any]]:
        assert run_id == RUN_ID
        return [deepcopy(row) for row in self.children.values()]

    def list_attempts(self, child_id: str) -> list[dict[str, Any]]:
        return deepcopy(self.attempts[child_id])


class _RecoverySpecRepository(_Repository):
    """Frozen source fixture sufficient to build successor durable rows."""

    def __init__(self, *, retry_mode_target_status: str, target_attempt_status: str) -> None:
        super().__init__(run_status="partial_failed", target_status=retry_mode_target_status)
        roster = [{"leg_id": "leg_a", "seed_run_ids": ["seed_1"]}]
        walk_forward = {"enabled": True, "window": 60, "min_periods": 2}
        backtest_config = {
            "request_snapshot": {
                "roster": roster,
                "oos_start": "2026-01-01",
                "oos_end": "2026-02-01",
                "normalize_method": "zscore",
                "weighting_schemes": ["equal"],
                "walk_forward": walk_forward,
                "backtest_config": {},
                "topk": 20,
            },
        }
        run_payload = durable_run_request_payload(
            roster_hash="e" * 64,
            roster=roster,
            oos_start="2026-01-01",
            oos_end="2026-02-01",
            normalize_method="zscore",
            walk_forward=walk_forward,
            backtest_config=backtest_config,
            baseline_leg_id=None,
            retry_of_run_id=None,
            node_parallelism={"wsl2-5080": 1},
        )
        self.run.update(
            {
                "request_hash": request_hash_for(run_payload),
                "roster_json": roster,
                "oos_start": "2026-01-01",
                "oos_end": "2026-02-01",
                "normalize_method": "zscore",
                "walk_forward_json": walk_forward,
                "backtest_config_json": backtest_config,
                "baseline_leg_id": None,
                "node_parallelism_json": {"wsl2-5080": 1},
            },
        )
        target = self.attempts[LOO_ID][0]
        target["status"] = target_attempt_status
        target["node_id"] = "wsl2-5080"
        for child_id, attempts in self.attempts.items():
            for attempt in attempts:
                attempt["node_id"] = "wsl2-5080"
                attempt["result_manifest_json"] = {
                    "schema_version": "multi_alpha_child_result_manifest_v1",
                    "source": attempt["attempt_id"],
                }
                if attempt["status"] == "succeeded":
                    attempt["result_manifest_hash"] = artifact_manifest_hash_for(
                        attempt["result_manifest_json"]
                    )
                attempt["artifact_manifest_json"] = {
                    "schema_version": "multi_alpha_child_artifact_manifest_v1",
                    "manifest_hash": artifact_manifest_hash_for(
                        {"source": attempt["attempt_id"]}
                    ),
                }
        self.children[LOO_ID]["selected_attempt_id"] = target["attempt_id"]


class _SiblingCompletedRecoveryRepository(_RecoverySpecRepository):
    def __init__(self) -> None:
        super().__init__(
            retry_mode_target_status="not_recovered",
            target_attempt_status="failed",
        )
        original_attempt_id = "macba_" + "1" * 64
        self.children[LOO_ID]["selected_attempt_id"] = None
        self.children[LOO_ID]["input_manifest_json"]["recovery"] = {
            "source_attempt_id": original_attempt_id,
        }
        self.children[LOO_ID]["input_manifest_hash"] = artifact_manifest_hash_for(
            self.children[LOO_ID]["input_manifest_json"]
        )
        self.attempts[LOO_ID] = []
        self.sibling_run = {
            **deepcopy(self.run),
            "id": "macb_sibling_recovery",
            "status": "failed",
        }
        self.sibling_child = {
            **deepcopy(self.children[LOO_ID]),
            "child_id": "macbc_sibling_loo",
            "run_id": self.sibling_run["id"],
            "status": "failed",
        }
        self.sibling_attempt = {
            "attempt_id": "macba_" + "2" * 64,
            "run_id": self.sibling_run["id"],
            "child_id": self.sibling_child["child_id"],
            "attempt_no": 1,
            "status": "failed",
            "phase": "reconcile_failed",
            "error_json": {
                "reason_code": "qe_execution_reservation_owner_mismatch",
            },
            "remote_status": "reconcile_failed",
            "source_attempt_id": original_attempt_id,
            "execution_kind": "remote_execution",
            "retry_mode": "backtest_only",
            "node_id": "rdagent-node1",
            "qe_task_id": "macb_remote_sibling",
            "qe_loop_id": "Loop1",
            "submission_intent_hash": "3" * 64,
            "artifact_manifest_json": {"manifest_hash": "4" * 64},
            "result_manifest_json": {},
            "result_manifest_hash": None,
            "finished_at": "2026-07-25T19:00:00+08:00",
        }

    def list_runs(
        self,
        *,
        task_id: str | None = None,
        status: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        assert task_id == TASK_ID
        assert status is None
        assert limit == 1000
        return [deepcopy(self.run), deepcopy(self.sibling_run)]

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        if run_id == self.sibling_run["id"]:
            return deepcopy(self.sibling_run)
        return super().get_run(run_id)

    def list_children(self, run_id: str) -> list[dict[str, Any]]:
        if run_id == self.sibling_run["id"]:
            return [deepcopy(self.sibling_child)]
        return super().list_children(run_id)

    def list_attempts(self, child_id: str) -> list[dict[str, Any]]:
        if child_id == self.sibling_child["child_id"]:
            return [deepcopy(self.sibling_attempt)]
        return super().list_attempts(child_id)

    def get_attempt(self, attempt_id: str) -> dict[str, Any] | None:
        if attempt_id == self.sibling_attempt["attempt_id"]:
            return deepcopy(self.sibling_attempt)
        return None

    def get_child(self, child_id: str) -> dict[str, Any] | None:
        if child_id == self.sibling_child["child_id"]:
            return deepcopy(self.sibling_child)
        return deepcopy(self.children.get(child_id))


class _CumulativeSiblingRecoveryRepository(_SiblingCompletedRecoveryRepository):
    def __init__(self) -> None:
        super().__init__()
        original_attempt_id = str(self.sibling_attempt["source_attempt_id"])
        self.ancestor_child = deepcopy(self.children[LOO_ID])
        self.ancestor_child["child_id"] = "macbc_ancestor_loo"
        self.ancestor_child["input_manifest_json"]["recovery"] = {
            "source_attempt_id": original_attempt_id,
        }
        self.children[LOO_ID]["source_child_id"] = self.ancestor_child["child_id"]
        self.children[LOO_ID]["input_manifest_json"]["recovery"] = {
            "source_child_id": self.ancestor_child["child_id"],
            "source_attempt_id": None,
        }
        self.children[LOO_ID]["source_lineage_json"] = {
            "source_child_id": self.ancestor_child["child_id"],
            "source_attempt_id": None,
        }

    def get_child(self, child_id: str) -> dict[str, Any] | None:
        if child_id == self.ancestor_child["child_id"]:
            return deepcopy(self.ancestor_child)
        return super().get_child(child_id)


class _RemoteCompletedRepairAdapter:
    def __init__(self, root: Path, *, remote_status: str = "completed") -> None:
        self.artifacts = DurablePublishedArtifacts(
            workspace=root,
            prediction_path=root / "combined_prediction.pkl",
            artifact_manifest_path=root / "artifact_manifest.json",
            artifact_manifest={"manifest_hash": "4" * 64},
        )
        self.inspect_calls = 0
        self.collect_calls = 0
        self.remote_status = remote_status

    @staticmethod
    def recovery_materializer_identity_for_run(_run: Mapping[str, Any]) -> Mapping[str, Any]:
        return {"materializer": "test"}

    def load_published_artifacts(self, **_kwargs: Any) -> DurablePublishedArtifacts:
        return self.artifacts

    def prepare_submission_intent(self, **_kwargs: Any) -> DurableSubmissionIntent:
        attempt = _kwargs["attempt"]
        return DurableSubmissionIntent(
            run_id=str(_kwargs["run"]["id"]),
            child_id=str(_kwargs["child"]["child_id"]),
            attempt_id=str(attempt["attempt_id"]),
            attempt_no=1,
            node_id=str(attempt["node_id"]),
            qe_task_id=str(attempt["qe_task_id"]),
            qe_loop_id=str(attempt["qe_loop_id"]),
            submission_intent_hash=str(attempt["submission_intent_hash"]),
        )

    async def inspect_remote(self, **_kwargs: Any) -> Any:
        self.inspect_calls += 1
        return SimpleNamespace(
            receipt=SimpleNamespace(status=self.remote_status),
            status={"status": self.remote_status},
        )

    async def collect_result(self, **_kwargs: Any) -> DurableCollectedResult:
        self.collect_calls += 1
        manifest = {
            "schema_version": "multi_alpha_child_result_manifest_v1",
            "manifest_hash": "5" * 64,
        }
        return DurableCollectedResult(
            metrics={"cagr": 0.5, "sharpe": 1.5},
            result_manifest=manifest,
            result_manifest_path=self.artifacts.workspace / "result_manifest.json",
        )

    @staticmethod
    def load_materialization_metadata(_artifacts: DurablePublishedArtifacts) -> Mapping[str, Any]:
        return {"weights": {}, "per_window_weights": []}


def _source_result_payloads(repository: _RecoverySpecRepository) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for attempts in repository.attempts.values():
        for attempt in attempts:
            if attempt["status"] == "succeeded":
                payloads[attempt["attempt_id"]] = {
                    "metrics": {"sharpe": 1.0, "calmar": 2.0, "cagr": 0.3},
                    "materialization_metadata": {"weights": {}, "per_window_weights": []},
                }
    return payloads


def test_terminal_targeted_recovery_freezes_dependency_closure_and_preserves_siblings() -> None:
    service = DurableRecoveryService(_Repository(run_status="partial_failed"))

    preview = service.preview(
        source_run_id=RUN_ID,
        target_child_id=LOO_ID,
        retry_mode="backtest_only",
        idempotency_key="retry_loo_1",
    )

    assert preview.topology == "successor_recovery_run"
    assert preview.state_allowed is True
    assert preview.successor_run_id is not None
    assert preview.scope_hash == preview.plan.scope_hash  # type: ignore[union-attr]
    dispositions = {entry.child_key: entry.disposition for entry in preview.plan.entries}  # type: ignore[union-attr]
    assert dispositions == {
        "baseline:leg_a": "reuse_result",
        "scheme:equal": "reuse_result",
        "loo:equal:drop:leg_b": "execute",
    }
    assert preview.evidence["execution_identity"]["complete"] is False
    assert "legacy_execution_identity_incomplete" in preview.evidence["evidence_gaps"]


def test_recovery_preview_replay_keeps_command_and_successor_identity_stable() -> None:
    service = DurableRecoveryService(_Repository(run_status="partial_failed"))

    first = service.preview(
        source_run_id=RUN_ID,
        target_child_id=LOO_ID,
        retry_mode="backtest_only",
        idempotency_key="stable-recovery-key",
    )
    replay = service.preview(
        source_run_id=RUN_ID,
        target_child_id=LOO_ID,
        retry_mode="backtest_only",
        idempotency_key="stable-recovery-key",
    )

    assert replay.command_id == first.command_id
    assert replay.scope_hash == first.scope_hash
    assert replay.successor_run_id == first.successor_run_id


def test_nonterminal_recovery_only_exposes_narrow_results_reference_topology() -> None:
    repository = _Repository(run_status="running", target_status="reconciling")
    target_attempt = repository.attempts[LOO_ID][0]
    target_attempt["status"] = "succeeded"
    service = DurableRecoveryService(repository)

    preview = service.preview(
        source_run_id=RUN_ID,
        target_child_id=LOO_ID,
        retry_mode="results_only",
        idempotency_key="retry_results_1",
    )

    assert preview.topology == "append_results_reference_in_place"
    assert preview.state_allowed is True
    assert preview.successor_run_id is None
    assert preview.scope["dependency_plan"][0]["disposition"] == "reuse_result"


def test_nonterminal_backtest_retry_is_visible_but_not_silently_retyped_as_results_only() -> None:
    service = DurableRecoveryService(_Repository(run_status="running", target_status="reconciling"))

    preview = service.preview(
        source_run_id=RUN_ID,
        target_child_id=LOO_ID,
        retry_mode="backtest_only",
        idempotency_key="retry_backtest_1",
    )

    assert preview.topology == "append_results_reference_in_place"
    assert preview.retry_mode == "backtest_only"
    assert preview.state_allowed is False
    assert "recovery_source_run_nonterminal" in preview.evidence["evidence_gaps"]
    assert preview.scope["retry_mode"] == "backtest_only"


def test_terminal_successful_child_is_immutable_under_targeted_recovery() -> None:
    service = DurableRecoveryService(_Repository(run_status="succeeded"))

    with pytest.raises(ValueError) as caught:
        service.preview(
            source_run_id=RUN_ID,
            target_child_id=SCHEME_ID,
            retry_mode="backtest_only",
            idempotency_key="retry_success_1",
        )

    assert getattr(caught.value, "reason_code", None) == "multi_alpha_recovery_successful_child_immutable"


def test_results_only_successor_references_verified_results_and_never_creates_remote_attempt() -> None:
    repository = _RecoverySpecRepository(
        retry_mode_target_status="failed",
        target_attempt_status="succeeded",
    )
    preview = DurableRecoveryService(repository).preview(
        source_run_id=RUN_ID,
        target_child_id=LOO_ID,
        retry_mode="results_only",
        idempotency_key="results_reference_successor",
    )
    assert preview.plan is not None
    specs = build_successor_recovery_specs(
        plan=preview.plan,
        source_run=repository.get_run(RUN_ID),  # type: ignore[arg-type]
        source_children=repository.list_children(RUN_ID),
        source_attempts_by_child={child_id: repository.list_attempts(child_id) for child_id in repository.children},
        source_result_payloads=_source_result_payloads(repository),
    )
    assert all(spec.execution_kind != "remote_execution" for spec in specs.attempt_specs)
    assert {spec.execution_kind for spec in specs.attempt_specs} == {"reference_result"}
    target = next(spec for spec in specs.child_specs if spec.source_child_id == LOO_ID)
    assert target.execution_disposition == "reuse_result"
    assert target.status == "reconciling"


def test_results_only_preview_finds_exact_completed_sibling_owner_race_attempt() -> None:
    repository = _SiblingCompletedRecoveryRepository()

    preview = DurableRecoveryService(repository).preview(
        source_run_id=RUN_ID,
        target_child_id=LOO_ID,
        retry_mode="results_only",
        idempotency_key="recover_completed_sibling",
    )

    assert preview.plan is not None
    target = next(
        entry for entry in preview.plan.entries if entry.source_child_id == LOO_ID
    )
    assert target.disposition == "reuse_result"
    assert target.source_attempt_id == repository.sibling_attempt["attempt_id"]
    assert target.source_lineage["source_run_id"] == repository.sibling_run["id"]
    assert target.source_lineage["source_child_id"] == repository.sibling_child["child_id"]
    assert target.source_lineage["remote_result_collection_required"] is True
    assert "results_only_successful_source_attempt_missing" not in preview.evidence[
        "evidence_gaps"
    ]
    assert "results_only_result_manifest_missing" not in preview.evidence[
        "evidence_gaps"
    ]


def test_results_only_preview_resolves_original_attempt_through_source_child_ancestry() -> None:
    repository = _CumulativeSiblingRecoveryRepository()

    preview = DurableRecoveryService(repository).preview(
        source_run_id=RUN_ID,
        target_child_id=LOO_ID,
        retry_mode="results_only",
        idempotency_key="recover_cumulative_sibling",
    )

    assert preview.plan is not None
    target = next(
        entry for entry in preview.plan.entries if entry.source_child_id == LOO_ID
    )
    assert target.disposition == "reuse_result"
    assert target.source_attempt_id == repository.sibling_attempt["attempt_id"]
    assert target.source_lineage["source_run_id"] == repository.sibling_run["id"]


def test_results_only_preview_rejects_cyclic_source_child_ancestry() -> None:
    repository = _CumulativeSiblingRecoveryRepository()
    repository.children[LOO_ID]["source_child_id"] = LOO_ID
    repository.children[LOO_ID]["input_manifest_json"]["recovery"] = {
        "source_child_id": LOO_ID,
        "source_attempt_id": None,
    }
    repository.children[LOO_ID]["source_lineage_json"] = {
        "source_child_id": LOO_ID,
        "source_attempt_id": None,
    }

    with pytest.raises(ValueError) as caught:
        DurableRecoveryService(repository).preview(
            source_run_id=RUN_ID,
            target_child_id=LOO_ID,
            retry_mode="results_only",
            idempotency_key="reject_cyclic_cumulative_source",
        )

    assert getattr(caught.value, "reason_code", None) == "source_lineage_mismatch"


def test_results_only_worker_collects_exact_remote_completed_artifact_without_rerun(
    tmp_path: Path,
) -> None:
    repository = _SiblingCompletedRecoveryRepository()
    adapter = _RemoteCompletedRepairAdapter(tmp_path)
    worker = DurableRecoveryWorker(
        repository=repository,  # type: ignore[arg-type]
        adapter=adapter,  # type: ignore[arg-type]
    )
    entry = RecoveryPlanEntry(
        source_child_id=LOO_ID,
        child_key="loo:equal:drop:leg_b",
        child_kind="loo",
        source_status="not_recovered",
        disposition="reuse_result",
        source_attempt_id=repository.sibling_attempt["attempt_id"],
        source_attempt_status="failed",
        source_lineage={
            "source_run_id": repository.sibling_run["id"],
            "source_child_id": repository.sibling_child["child_id"],
            "remote_result_collection_required": True,
        },
    )

    payload, effective_attempt = worker._load_recovery_result_payload(
        entry=entry,
        source_attempt=repository.sibling_attempt,
    )

    assert adapter.inspect_calls == 1
    assert adapter.collect_calls == 1
    assert payload["metrics"] == {"cagr": 0.5, "sharpe": 1.5}
    assert effective_attempt["status"] == "succeeded"
    assert effective_attempt["result_manifest_hash"] == artifact_manifest_hash_for(
        effective_attempt["result_manifest_json"]
    )


def test_results_only_worker_refuses_owner_race_candidate_without_completed_receipt(
    tmp_path: Path,
) -> None:
    repository = _SiblingCompletedRecoveryRepository()
    adapter = _RemoteCompletedRepairAdapter(tmp_path, remote_status="running")
    worker = DurableRecoveryWorker(
        repository=repository,  # type: ignore[arg-type]
        adapter=adapter,  # type: ignore[arg-type]
    )
    entry = RecoveryPlanEntry(
        source_child_id=LOO_ID,
        child_key="loo:equal:drop:leg_b",
        child_kind="loo",
        source_status="not_recovered",
        disposition="reuse_result",
        source_attempt_id=repository.sibling_attempt["attempt_id"],
        source_attempt_status="failed",
        source_lineage={
            "source_run_id": repository.sibling_run["id"],
            "source_child_id": repository.sibling_child["child_id"],
            "remote_result_collection_required": True,
        },
    )

    with pytest.raises(ValueError) as caught:
        worker._load_recovery_result_payload(
            entry=entry,
            source_attempt=repository.sibling_attempt,
        )

    assert getattr(caught.value, "reason_code", None) == (
        "results_only_remote_result_not_completed"
    )
    assert adapter.inspect_calls == 1
    assert adapter.collect_calls == 0


def test_results_only_worker_reuses_hashed_inline_reference_result_without_raw_artifact(
    tmp_path: Path,
) -> None:
    repository = _SiblingCompletedRecoveryRepository()
    adapter = _RemoteCompletedRepairAdapter(tmp_path)
    worker = DurableRecoveryWorker(
        repository=repository,  # type: ignore[arg-type]
        adapter=adapter,  # type: ignore[arg-type]
    )
    result_manifest = {
        "schema_version": "multi_alpha_recovery_reference_result_v1",
        "source_attempt_id": "macba_original",
        "source_result_manifest": {"schema_version": "multi_alpha_child_result_manifest_v1"},
        "source_result_manifest_hash": "1" * 64,
        "source_artifact_manifest": {"manifest_hash": "2" * 64},
        "metrics": {"cagr": 0.5, "sharpe": 1.5},
        "materialization_metadata": {"weights": {"leg_a": 1.0}, "per_window_weights": []},
        "business_formula_version": "legacy_execution_identity_incomplete",
        "execution_disposition": "reuse_result",
    }
    source_attempt = {
        "attempt_id": "macba_reference",
        "status": "succeeded",
        "execution_kind": "reference_result",
        "result_manifest_json": result_manifest,
        "result_manifest_hash": artifact_manifest_hash_for(result_manifest),
        "artifact_manifest_json": {"manifest_hash": "3" * 64},
    }
    entry = RecoveryPlanEntry(
        source_child_id=LOO_ID,
        child_key="loo:equal:drop:leg_b",
        child_kind="loo",
        source_status="succeeded",
        disposition="reuse_result",
        source_attempt_id=source_attempt["attempt_id"],
        source_attempt_status="succeeded",
        source_lineage={
            "source_run_id": RUN_ID,
            "source_child_id": LOO_ID,
            "remote_result_collection_required": False,
        },
    )

    payload, effective_attempt = worker._load_recovery_result_payload(
        entry=entry,
        source_attempt=source_attempt,
    )

    assert payload["metrics"] == result_manifest["metrics"]
    assert payload["materialization_metadata"] == result_manifest["materialization_metadata"]
    assert payload["result_manifest"] == result_manifest
    assert effective_attempt is source_attempt
    assert adapter.inspect_calls == 0
    assert adapter.collect_calls == 0


def test_results_only_worker_rejects_tampered_inline_reference_result(
    tmp_path: Path,
) -> None:
    repository = _SiblingCompletedRecoveryRepository()
    worker = DurableRecoveryWorker(
        repository=repository,  # type: ignore[arg-type]
        adapter=_RemoteCompletedRepairAdapter(tmp_path),  # type: ignore[arg-type]
    )
    source_attempt = {
        "attempt_id": "macba_reference",
        "status": "succeeded",
        "execution_kind": "reference_result",
        "result_manifest_json": {
            "schema_version": "multi_alpha_recovery_reference_result_v1",
            "metrics": {"cagr": 0.5},
            "materialization_metadata": {"weights": {}, "per_window_weights": []},
        },
        "result_manifest_hash": "0" * 64,
        "artifact_manifest_json": {},
    }
    entry = RecoveryPlanEntry(
        source_child_id=LOO_ID,
        child_key="loo:equal:drop:leg_b",
        child_kind="loo",
        source_status="succeeded",
        disposition="reuse_result",
        source_attempt_id=source_attempt["attempt_id"],
        source_attempt_status="succeeded",
        source_lineage={"source_run_id": RUN_ID, "source_child_id": LOO_ID},
    )

    with pytest.raises(ValueError) as caught:
        worker._load_recovery_result_payload(
            entry=entry,
            source_attempt=source_attempt,
        )

    assert getattr(caught.value, "reason_code", None) == "results_only_artifact_missing"


def test_backtest_only_successor_keeps_exact_source_attempt_and_creates_one_remote_attempt() -> None:
    repository = _RecoverySpecRepository(
        retry_mode_target_status="failed",
        target_attempt_status="failed",
    )
    preview = DurableRecoveryService(repository).preview(
        source_run_id=RUN_ID,
        target_child_id=LOO_ID,
        retry_mode="backtest_only",
        idempotency_key="backtest_exact_successor",
    )
    assert preview.plan is not None
    specs = build_successor_recovery_specs(
        plan=preview.plan,
        source_run=repository.get_run(RUN_ID),  # type: ignore[arg-type]
        source_children=repository.list_children(RUN_ID),
        source_attempts_by_child={child_id: repository.list_attempts(child_id) for child_id in repository.children},
        source_result_payloads=_source_result_payloads(repository),
    )
    remote = [spec for spec in specs.attempt_specs if spec.execution_kind == "remote_execution"]
    assert len(remote) == 1
    assert remote[0].retry_mode == "backtest_only"
    assert remote[0].source_attempt_id == repository.attempts[LOO_ID][0]["attempt_id"]
    assert remote[0].node_id == "wsl2-5080"


class _RecoveryPublicationRepository(_RecoverySpecRepository):
    def __init__(self) -> None:
        super().__init__(retry_mode_target_status="failed", target_attempt_status="failed")
        self.operations: list[str] = []
        self.materialized_specs = None

    def record_recovery_staging_manifest(
        self,
        command_id: str,
        *,
        token: OwnershipToken,
        staging_manifest: Mapping[str, Any],
    ) -> dict[str, Any]:
        assert token == OwnershipToken(owner_id="worker", fencing_token=1, row_version=1)
        assert staging_manifest["staged_execute_attempts"]
        self.operations.append("staging_manifest_persisted")
        return {
            "command_id": command_id,
            "owner_id": "worker",
            "fencing_token": 1,
            "row_version": 2,
            "status": "reconciling",
        }

    def materialize_successor_recovery(
        self,
        *,
        command_id: str,
        token: OwnershipToken,
        recovery_specs: Any,
        staging_manifest: Mapping[str, Any],
    ) -> None:
        assert token == OwnershipToken(owner_id="worker", fencing_token=1, row_version=2)
        assert command_id
        assert staging_manifest["staged_execute_attempts"]
        assert self.operations[-1] == "artifact_published"
        self.operations.append("database_visible")
        self.materialized_specs = recovery_specs


class _RecoveryPublicationAdapter:
    def __init__(self, repository: _RecoveryPublicationRepository) -> None:
        self.repository = repository

    def recovery_materializer_identity_for_run(self, _source_run: Mapping[str, Any]):
        return {"materializer_file_set_sha256": "6" * 64}

    def load_recovery_source_result_payload(self, *, source_attempt_id: str, **_kwargs: Any):
        return _source_result_payloads(self.repository)[source_attempt_id]

    def stage_backtest_only_recovery_artifacts(self, **kwargs: Any) -> None:
        assert kwargs["source_attempt_id"] == self.repository.attempts[LOO_ID][0]["attempt_id"]
        assert self.repository.operations == ["staging_manifest_persisted"]
        self.repository.operations.append("artifact_published")


def test_successor_files_publish_before_database_visibility() -> None:
    repository = _RecoveryPublicationRepository()
    preview = DurableRecoveryService(repository).preview(
        source_run_id=RUN_ID,
        target_child_id=LOO_ID,
        retry_mode="backtest_only",
        idempotency_key="atomic-successor-publication",
    )
    adapter = _RecoveryPublicationAdapter(repository)
    worker = DurableRecoveryWorker(
        repository=repository,  # type: ignore[arg-type]
        adapter=adapter,  # type: ignore[arg-type]
        recovery_service=DurableRecoveryService(repository),
    )

    worker._execute_successor(
        command={"command_id": preview.command_id},
        token=OwnershipToken(owner_id="worker", fencing_token=1, row_version=1),
        preview=preview,
    )

    assert repository.operations == [
        "staging_manifest_persisted",
        "artifact_published",
        "database_visible",
    ]
    assert repository.materialized_specs is not None
