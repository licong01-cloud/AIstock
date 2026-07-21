from __future__ import annotations

from copy import deepcopy
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
    build_successor_recovery_specs,
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
