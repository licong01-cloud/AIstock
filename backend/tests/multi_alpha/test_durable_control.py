from __future__ import annotations

import pytest

from backend.services.multi_alpha.durable_identity import (
    build_execution_identity,
    legacy_execution_identity_evidence,
    validate_execution_identity,
)
from backend.services.multi_alpha.durable_models import (
    DurableCommandSpec,
    DurableContractError,
    command_target_key_for,
    control_command_payload,
    make_command_id,
    sha256_identity,
)
from backend.services.multi_alpha.durable_recovery import (
    build_recovery_plan,
    recovery_execution_evidence,
)
from backend.services.multi_alpha.durable_control import (
    DurableControlError,
    DurableMultiAlphaControlService,
)


def _identity(*, dataset_manifest_sha256: str = "a" * 64) -> dict[str, object]:
    return build_execution_identity(
        dataset={
            "deployment_snapshot_id": "qe-dataset-20260720",
            "dataset_manifest_sha256": dataset_manifest_sha256,
            "cutoff_trade_date": "2026-06-30",
            "qlib_calendar_sha256": "b" * 64,
            "qlib_instruments_sha256": "c" * 64,
            "st_pit_snapshot_id": "st-pit-20260630",
            "st_pit_manifest_sha256": "d" * 64,
            "resolved_node_id": "wsl2-5080",
            "resolved_data_root_uri": "/home/lc999/data/factor_data",
        },
        prediction_sources=[
            {
                "leg_id": "L1",
                "seed_run_id": "qe_1",
                "artifact_uri": "cas://prediction/L1",
                "artifact_sha256": "e" * 64,
            }
        ],
        runtime={
            "qlib_runtime_template_sha256": "f" * 64,
            "conda_environment_lock_sha256": "0" * 64,
            "execution_environment_snapshot_id": "rdagent-gpu-20260720",
            "execution_environment_manifest_sha256": "1" * 64,
            "executor_code_commit": "abcdef012345",
            "executor_file_set_sha256": "2" * 64,
            "backtest_config_sha256": "3" * 64,
        },
        materializer={
            "aistock_commit": "123456abcdef",
            "planner_version": "multi_alpha_child_plan_v1",
            "combiner_file_sha256": "4" * 64,
            "panel_builder_file_sha256": "5" * 64,
            "materializer_file_set_sha256": "6" * 64,
        },
        business_formula={
            "formula_version": "durable_business_result_v1",
            "assembler_file_sha256": "7" * 64,
            "delta_formula_sha256": "8" * 64,
        },
    ).payload


def test_execution_identity_is_content_based_not_path_based() -> None:
    first = _identity(dataset_manifest_sha256="a" * 64)
    second = _identity(dataset_manifest_sha256="9" * 64)

    assert first["dataset"]["resolved_data_root_uri"] == second["dataset"]["resolved_data_root_uri"]
    assert sha256_identity(first) != sha256_identity(second)
    assert validate_execution_identity(payload=first, identity_hash=sha256_identity(first)).identity_hash == sha256_identity(first)


def test_legacy_identity_gap_is_explicit_evidence_not_research_rejection() -> None:
    evidence = legacy_execution_identity_evidence(None)

    assert evidence["complete"] is False
    assert evidence["reason_code"] == "legacy_execution_identity_incomplete"
    assert evidence["acquisition_suggestions"]


def test_control_command_canonical_idempotency_binds_exact_target() -> None:
    run_id = "macb_source"
    child_id = "macbc_child"
    attempt_id = "macba_attempt"
    request = {"requested_alias": "stop"}
    payload_hash = sha256_identity(
        control_command_payload(
            action="attempt_cancel",
            run_id=run_id,
            child_id=child_id,
            attempt_id=attempt_id,
            request=request,
            scope=None,
        )
    )
    spec = DurableCommandSpec(
        command_id=make_command_id(run_id, "request-1"),
        run_id=run_id,
        action="attempt_cancel",
        target_key=command_target_key_for(
            action="attempt_cancel",
            run_id=run_id,
            child_id=child_id,
            attempt_id=attempt_id,
        ),
        idempotency_key="request-1",
        payload_hash=payload_hash,
        request=request,
        requested_by="ui-user",
        child_id=child_id,
        attempt_id=attempt_id,
    )

    assert spec.command_id.startswith("macmd_")
    with pytest.raises(DurableContractError) as caught:
        DurableCommandSpec(
            **{**spec.__dict__, "target_key": "not-the-canonical-target"},
        )
    assert caught.value.reason_code == "multi_alpha_identity_hash_mismatch"


def test_scheme_recovery_recomputes_only_its_loo_descendants() -> None:
    identity = _identity()
    source_run = {
        "id": "macb_source",
        "status": "partial_failed",
        "request_hash": "a" * 64,
        "roster_hash": "roster-hash",
    }
    children = [
        {
            "child_id": "macbc_baseline",
            "child_key": "baseline:L1",
            "child_kind": "baseline",
            "status": "succeeded",
            "ordinal": 0,
            "input_manifest_json": {"kind": "baseline"},
            "input_manifest_hash": "b" * 64,
            "prediction_artifact_uri": "cas://baseline",
            "prediction_artifact_hash": "c" * 64,
            "selected_attempt_id": "macba_baseline",
        },
        {
            "child_id": "macbc_scheme",
            "child_key": "scheme:equal",
            "child_kind": "scheme",
            "weighting_scheme": "equal",
            "status": "failed",
            "ordinal": 1,
            "input_manifest_json": {"kind": "scheme"},
            "input_manifest_hash": "d" * 64,
            "prediction_artifact_uri": "cas://scheme",
            "prediction_artifact_hash": "e" * 64,
            "selected_attempt_id": None,
        },
        {
            "child_id": "macbc_loo",
            "child_key": "loo:equal:drop:L2",
            "child_kind": "loo",
            "weighting_scheme": "equal",
            "dropped_leg_id": "L2",
            "status": "succeeded",
            "ordinal": 2,
            "input_manifest_json": {"kind": "loo"},
            "input_manifest_hash": "f" * 64,
            "prediction_artifact_uri": "cas://loo",
            "prediction_artifact_hash": "0" * 64,
            "selected_attempt_id": "macba_loo",
        },
        {
            "child_id": "macbc_other",
            "child_key": "scheme:risk_parity",
            "child_kind": "scheme",
            "weighting_scheme": "risk_parity",
            "status": "cancelled",
            "ordinal": 3,
            "input_manifest_json": {"kind": "other"},
            "input_manifest_hash": "1" * 64,
            "prediction_artifact_uri": None,
            "prediction_artifact_hash": None,
            "selected_attempt_id": None,
        },
    ]
    attempts = {
        "macbc_baseline": [
            {
                "attempt_id": "macba_baseline",
                "attempt_no": 1,
                "status": "succeeded",
                "execution_kind": "remote_execution",
                "result_manifest_json": {"sharpe": 1.2},
                "result_manifest_hash": "2" * 64,
            }
        ],
        "macbc_scheme": [
            {
                "attempt_id": "macba_scheme",
                "attempt_no": 1,
                "status": "failed",
                "execution_kind": "remote_execution",
                "result_manifest_json": {},
                "result_manifest_hash": None,
            }
        ],
        "macbc_loo": [
            {
                "attempt_id": "macba_loo",
                "attempt_no": 1,
                "status": "succeeded",
                "execution_kind": "remote_execution",
                "result_manifest_json": {"marginal_sharpe": 0.2},
                "result_manifest_hash": "3" * 64,
            }
        ],
    }

    plan = build_recovery_plan(
        source_run=source_run,
        source_children=children,
        source_attempts_by_child=attempts,
        command_id="macmd_request",
        target_child_id="macbc_scheme",
        retry_mode="backtest_only",
        execution_identity=identity,
        recovery_materializer_identity={"materializer_file_set_sha256": "6" * 64},
        business_formula_version="durable_business_result_v1",
    )
    dispositions = {entry.child_key: entry.disposition for entry in plan.entries}

    assert dispositions == {
        "baseline:L1": "reuse_result",
        "scheme:equal": "execute",
        "loo:equal:drop:L2": "recompute_derived",
        "scheme:risk_parity": "preserve_unavailable",
    }
    assert plan.successor_run_id.startswith("macb_recovery_")
    assert recovery_execution_evidence(plan)["complete"] is True


def test_code_identity_change_expands_dependency_closure_without_mixed_results() -> None:
    identity = _identity()
    source_run = {
        "id": "macb_source",
        "status": "partial_failed",
        "request_hash": "a" * 64,
        "roster_hash": "roster-hash",
    }
    children = [
        {
            "child_id": "macbc_baseline",
            "child_key": "baseline:L1",
            "child_kind": "baseline",
            "status": "succeeded",
            "ordinal": 0,
            "input_manifest_json": {"kind": "baseline"},
            "input_manifest_hash": "b" * 64,
            "prediction_artifact_uri": "cas://baseline",
            "prediction_artifact_hash": "c" * 64,
            "selected_attempt_id": "macba_baseline",
        },
        {
            "child_id": "macbc_scheme",
            "child_key": "scheme:equal",
            "child_kind": "scheme",
            "weighting_scheme": "equal",
            "status": "failed",
            "ordinal": 1,
            "input_manifest_json": {"kind": "scheme"},
            "input_manifest_hash": "d" * 64,
            "prediction_artifact_uri": "cas://scheme",
            "prediction_artifact_hash": "e" * 64,
            "selected_attempt_id": "macba_scheme",
        },
        {
            "child_id": "macbc_loo",
            "child_key": "loo:equal:drop:L2",
            "child_kind": "loo",
            "weighting_scheme": "equal",
            "dropped_leg_id": "L2",
            "status": "succeeded",
            "ordinal": 2,
            "input_manifest_json": {"kind": "loo"},
            "input_manifest_hash": "f" * 64,
            "prediction_artifact_uri": "cas://loo",
            "prediction_artifact_hash": "0" * 64,
            "selected_attempt_id": "macba_loo",
        },
        {
            "child_id": "macbc_unavailable",
            "child_key": "scheme:risk_parity",
            "child_kind": "scheme",
            "weighting_scheme": "risk_parity",
            "status": "failed",
            "ordinal": 3,
            "input_manifest_json": {"kind": "unavailable"},
            "input_manifest_hash": "1" * 64,
            "prediction_artifact_uri": None,
            "prediction_artifact_hash": None,
            "selected_attempt_id": None,
        },
    ]
    attempts = {
        child["child_id"]: [
            {
                "attempt_id": child["selected_attempt_id"],
                "attempt_no": 1,
                "status": child["status"],
                "execution_kind": "remote_execution",
                "artifact_manifest_json": {"manifest_hash": "2" * 64},
                "result_manifest_json": {},
                "result_manifest_hash": None,
            }
        ]
        if child["selected_attempt_id"]
        else []
        for child in children
    }
    recovery_identity = {
        **dict(identity["materializer"]),
        "materializer_file_set_sha256": "9" * 64,
    }

    plan = build_recovery_plan(
        source_run=source_run,
        source_children=children,
        source_attempts_by_child=attempts,
        command_id="macmd_rematerialize",
        target_child_id="macbc_scheme",
        retry_mode="rematerialize_and_backtest",
        execution_identity=identity,
        recovery_materializer_identity=recovery_identity,
        business_formula_version="durable_business_result_v1",
    )

    assert plan.scope["materializer_identity_changed"] is True
    assert {entry.child_key: entry.disposition for entry in plan.entries} == {
        "baseline:L1": "execute",
        "scheme:equal": "execute",
        "loo:equal:drop:L2": "execute",
        "scheme:risk_parity": "preserve_unavailable",
    }


def test_results_only_evidence_gap_is_reported_without_mode_fallback() -> None:
    identity = _identity()
    plan = build_recovery_plan(
        source_run={
            "id": "macb_source",
            "status": "failed",
            "request_hash": "a" * 64,
            "roster_hash": "roster-hash",
        },
        source_children=[
            {
                "child_id": "macbc_target",
                "child_key": "scheme:equal",
                "child_kind": "scheme",
                "weighting_scheme": "equal",
                "status": "failed",
                "ordinal": 0,
                "input_manifest_json": {},
                "input_manifest_hash": "b" * 64,
                "prediction_artifact_uri": None,
                "prediction_artifact_hash": None,
                "selected_attempt_id": None,
            }
        ],
        source_attempts_by_child={"macbc_target": []},
        command_id="macmd_results",
        target_child_id="macbc_target",
        retry_mode="results_only",
        execution_identity=identity,
        recovery_materializer_identity=None,
        business_formula_version="durable_business_result_v1",
    )

    evidence = recovery_execution_evidence(plan)
    assert evidence["complete"] is False
    assert "results_only_successful_source_attempt_missing" in evidence["evidence_gaps"]
    assert plan.retry_mode == "results_only"


class _CapabilityRepository:
    def __init__(self) -> None:
        self.runs = {
            "macb_a": {"id": "macb_a", "status": "running"},
            "macb_b": {"id": "macb_b", "status": "running"},
        }
        self.children = {
            "macbc_a": {"child_id": "macbc_a", "run_id": "macb_a", "status": "running"},
            "macbc_b": {"child_id": "macbc_b", "run_id": "macb_b", "status": "running"},
        }
        self.attempts = {
            "macba_a": {
                "attempt_id": "macba_a",
                "run_id": "macb_a",
                "child_id": "macbc_a",
                "status": "running",
            },
            "macba_b": {
                "attempt_id": "macba_b",
                "run_id": "macb_b",
                "child_id": "macbc_b",
                "status": "running",
            },
        }

    def get_run(self, run_id: str):
        return self.runs.get(run_id)

    def get_child(self, child_id: str):
        return self.children.get(child_id)

    def get_attempt(self, attempt_id: str):
        return self.attempts.get(attempt_id)


@pytest.mark.parametrize(
    ("child_id", "attempt_id"),
    [
        ("macbc_b", None),
        (None, "macba_b"),
        ("macbc_a", "macba_b"),
    ],
)
def test_capabilities_never_leak_cross_run_child_or_attempt_evidence(
    child_id: str | None,
    attempt_id: str | None,
) -> None:
    service = DurableMultiAlphaControlService(_CapabilityRepository())  # type: ignore[arg-type]

    with pytest.raises(DurableControlError) as caught:
        service.capabilities(
            run_id="macb_a",
            child_id=child_id,
            attempt_id=attempt_id,
        )

    assert caught.value.reason_code == "multi_alpha_entity_not_found"
