from __future__ import annotations

import json
from datetime import date

from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    N0_EXPERIMENT_ID,
    N1_LEARNABILITY_EXPERIMENT_ID,
    N1_ORACLE_EXPERIMENT_ID,
    P0C_DATASET_IDENTITY,
    P0_FAMILY_EXPERIMENT_IDS,
    freeze_default_research_windows,
    research_policy_identity,
)
from backend.services.advisory_model_first.research_control_contracts import (
    ConsumedWindowV1,
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ParentLegEvidenceV1,
    ParentPredictionExtensionStatus,
    PostCutoffInferenceEvidenceV1,
    ResearchResultClass,
    ResearchStudyType,
    build_parent_extension_receipt,
    build_trial_record,
)
from backend.services.advisory_model_first.target_binding import (
    EXPECTED_RUNTIME_SEMANTICS_HASH,
    MANIFEST_SHA256,
    PACKAGE_ID,
    RUNTIME_SEMANTICS_ID,
)
from scripts.advisory_n0_research_control import main


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def _evidence(role: str) -> EvidenceReferenceV1:
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=f"F:/fixture/{role}.json",
        sha256=HASH_A,
        size_bytes=10,
    )


def test_freeze_windows_cli_prints_one_json_summary_and_exact_retry_is_stable(tmp_path, capsys):
    output = tmp_path / "windows.json"

    assert main(["freeze-windows", "--output", str(output)]) == 0
    first = json.loads(capsys.readouterr().out)
    first_bytes = output.read_bytes()
    assert main(["freeze-windows", "--output", str(output)]) == 0
    second = json.loads(capsys.readouterr().out)

    assert first["contract_sha256"] == second["contract_sha256"]
    assert output.read_bytes() == first_bytes
    assert first["research_policy_identity"]


def test_cli_failure_is_nonzero_with_typed_single_json(tmp_path, capsys):
    output = tmp_path / "windows.json"
    assert main(["freeze-windows", "--output", str(output)]) == 0
    summary = json.loads(capsys.readouterr().out)

    result = main(
        [
            "check-window-access",
            "--contract",
            str(output),
            "--study-type",
            "ORACLE_DIAGNOSTIC",
            "--objective-contract",
            "ALPHA_RANKING",
            "--decision-use",
            "NAVIGATION_ONLY",
            "--dataset-identity",
            summary["sealed_dataset_identity"],
            "--start-date",
            "2026-08-31",
            "--end-date",
            "2026-11-30",
        ]
    )
    failure = json.loads(capsys.readouterr().out)

    assert result == 1
    assert failure["status"] == "failed"
    assert failure["reason_code"] == "ADVISORY_SEALED_HOLDOUT_ACCESS_DENIED"


def test_cli_argument_error_is_typed_json_instead_of_argparse_process_exit(capsys):
    result = main(["freeze-windows"])
    failure = json.loads(capsys.readouterr().out)

    assert result == 1
    assert failure["status"] == "failed"
    assert failure["reason_code"] == "ADVISORY_N0_REQUEST_INVALID"
    assert failure["context"]["error_type"] == "AdvisoryN0ArgumentError"


def test_complete_n0_cli_appends_control_record_and_derives_route(tmp_path, capsys):
    registry_path = tmp_path / "registry.jsonl"
    records = []
    for index, experiment_id in enumerate(P0_FAMILY_EXPERIMENT_IDS):
        records.append(
            build_trial_record(
                experiment_id=experiment_id,
                attempt_id="formal-v1",
                research_stage="STAGE_A",
                study_type=ResearchStudyType.CANDIDATE_MODEL,
                hypothesis_family_id="p0-family",
                parent_lineage=("P0-C",),
                unique_variable=f"variable-{index}",
                objective_contract=ObjectiveContract.ALPHA_RANKING,
                dataset_identity="p0c",
                schema_identity="schema-v2",
                policy_identity=HASH_A,
                planned_trial_count=1,
                generated_trial_count=1,
                evaluated_trial_count=1,
                selected_trial_count=0,
                consumed_windows=(),
                result_class=(
                    ResearchResultClass.FAMILY_FROZEN if experiment_id.endswith("L") else ResearchResultClass.NEGATIVE
                ),
                decision_use=DecisionUse.NAVIGATION_ONLY,
                evidence_refs=(_evidence(f"p0-{index}"),),
            )
        )
    AdvisoryResearchTrialRegistryV1(registry_path).append_batch(records)

    leg = ParentLegEvidenceV1(
        leg_id="leg",
        representative_run_id="run",
        prediction_ref=_evidence("prediction"),
        prediction_row_count=10,
        prediction_date_start=date(2024, 7, 4),
        prediction_date_end=date(2026, 3, 10),
        runtime_asset_root="F:/runtime",
        runtime_ready=True,
        runtime_refs=(_evidence("model"),),
    )
    parent = build_parent_extension_receipt(
        status=ParentPredictionExtensionStatus.FROZEN_MODEL_CAN_INFER,
        package_id=PACKAGE_ID,
        manifest_sha256=MANIFEST_SHA256,
        runtime_semantics_id=RUNTIME_SEMANTICS_ID,
        runtime_semantics_hash=EXPECTED_RUNTIME_SEMANTICS_HASH,
        common_historical_prediction_cutoff=date(2026, 3, 10),
        target_extension_start=date(2026, 3, 11),
        target_extension_end=date(2026, 6, 30),
        legs=(leg,),
        post_cutoff_evidence=PostCutoffInferenceEvidenceV1(
            artifact_ref=_evidence("post"),
            comparison_state_ref=_evidence("state"),
            decision_trade_date=date(2026, 5, 20),
            target_trade_date=date(2026, 5, 21),
            candidate_count=20,
            parent_candidate_artifact_hash=HASH_B,
            parent_candidate_set_hash=HASH_C,
            observed_duration_seconds=12.0,
        ),
    )
    parent_path = tmp_path / "parent.json"
    parent_path.write_text(parent.model_dump_json(indent=2), encoding="utf-8")
    window = freeze_default_research_windows(artifact_root_uri=tmp_path)
    window_path = tmp_path / "window.json"
    window_path.write_text(window.model_dump_json(indent=2), encoding="utf-8")
    route_path = tmp_path / "route.md"
    completion_path = tmp_path / "completion.json"

    result = main(
        [
            "complete-n0",
            "--registry",
            str(registry_path),
            "--parent-spike",
            str(parent_path),
            "--window-contract",
            str(window_path),
            "--route",
            str(route_path),
            "--output",
            str(completion_path),
        ]
    )
    summary = json.loads(capsys.readouterr().out)
    registry = AdvisoryResearchTrialRegistryV1(registry_path).read()

    assert result == 0
    assert summary["n0_status"] == "COMPLETE"
    assert summary["next_task"] == "N1_TIER1_ORACLE_LEARNABILITY"
    assert N0_EXPERIMENT_ID in {item.experiment_id for item in registry}
    route = route_path.read_text(encoding="utf-8")
    assert "P0-D..P0-L" in route
    assert "active main research line | `NONE`" in route
    assert completion_path.is_file()

    route_mtime = route_path.stat().st_mtime_ns
    completion_bytes = completion_path.read_bytes()
    assert (
        main(
            [
                "complete-n0",
                "--registry",
                str(registry_path),
                "--parent-spike",
                str(parent_path),
                "--window-contract",
                str(window_path),
                "--route",
                str(route_path),
                "--output",
                str(completion_path),
            ]
        )
        == 0
    )
    retry = json.loads(capsys.readouterr().out)
    assert retry["receipt_sha256"] == summary["receipt_sha256"]
    assert route_path.stat().st_mtime_ns == route_mtime
    assert completion_path.read_bytes() == completion_bytes

    n1_window = ConsumedWindowV1(
        window_id="P0C_DEVELOPMENT_V1",
        dataset_identity=P0C_DATASET_IDENTITY,
        start_date=date(2024, 7, 4),
        end_date=date(2026, 3, 10),
    )
    n1_policy_identity = research_policy_identity(
        baseline_policy_sha256=window.baseline_policy_sha256,
        shadow_policy_sha256=window.shadow_policy_sha256,
        cost_policy_sha256=window.cost_policy_sha256,
    )
    n1_records = []
    for experiment_id, study_type in (
        (N1_ORACLE_EXPERIMENT_ID, ResearchStudyType.ORACLE_DIAGNOSTIC),
        (N1_LEARNABILITY_EXPERIMENT_ID, ResearchStudyType.LEARNABILITY_AUDIT),
    ):
        n1_records.append(
            build_trial_record(
                experiment_id=experiment_id,
                attempt_id="advn1req_" + "1" * 24,
                research_stage="N1_TIER1",
                study_type=study_type,
                hypothesis_family_id="n1-tier1",
                parent_lineage=("N0-RESEARCH-CONTROL",),
                unique_variable=experiment_id,
                objective_contract=ObjectiveContract.ALPHA_RANKING,
                dataset_identity=P0C_DATASET_IDENTITY,
                schema_identity="feature-schema-v2",
                policy_identity=n1_policy_identity,
                planned_trial_count=1,
                generated_trial_count=1,
                evaluated_trial_count=1,
                selected_trial_count=0,
                consumed_windows=(n1_window,),
                result_class=ResearchResultClass.NEGATIVE,
                decision_use=DecisionUse.DIRECTION_GATE,
                evidence_refs=(_evidence(experiment_id.lower()),),
            )
        )
    AdvisoryResearchTrialRegistryV1(registry_path).append_batch(n1_records[:1])
    assert (
        main(
            [
                "generate-route",
                "--registry",
                str(registry_path),
                "--parent-spike",
                str(parent_path),
                "--window-contract",
                str(window_path),
                "--output",
                str(route_path),
            ]
        )
        == 1
    )
    partial_failure = json.loads(capsys.readouterr().out)
    assert partial_failure["reason_code"] == "ADVISORY_RESEARCH_ROUTE_INCONSISTENT"
    AdvisoryResearchTrialRegistryV1(registry_path).append_batch(n1_records[1:])
    assert (
        main(
            [
                "generate-route",
                "--registry",
                str(registry_path),
                "--parent-spike",
                str(parent_path),
                "--window-contract",
                str(window_path),
                "--output",
                str(route_path),
            ]
        )
        == 0
    )
    n1_route = json.loads(capsys.readouterr().out)
    assert n1_route["n1_state"] == "COMPLETE"
    assert n1_route["next_task"] == "N2_ENTRY_EXIT_QE_PREPARATION"
    assert "N1 Tier-1 oracle + learnability | `COMPLETE`" in route_path.read_text(encoding="utf-8")

    parent_path.write_text(parent_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    poisoned_result = main(
        [
            "generate-route",
            "--registry",
            str(registry_path),
            "--parent-spike",
            str(parent_path),
            "--window-contract",
            str(window_path),
            "--output",
            str(tmp_path / "poisoned-route.md"),
        ]
    )
    failure = json.loads(capsys.readouterr().out)
    assert poisoned_result == 1
    assert failure["reason_code"] == "ADVISORY_RESEARCH_ROUTE_INCONSISTENT"
