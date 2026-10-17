from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first import qe_alpha_mve_preparation as preparation_module
from backend.services.advisory_model_first.qe_alpha_mve_preparation import (
    ALLOWED_OPERATORS,
    FORBIDDEN_OPERATIONS,
    FUTURE_EVIDENCE_OBLIGATIONS,
    INPUT_FAMILIES,
    KNOWN_EFFECTS,
    PARENT_EXPERIMENT_IDS,
    SIGNAL_FAMILIES,
    FrozenAdvisoryQEAlphaMVEPreparationV1,
    QEAlphaPreparationOperation,
    build_default_qe_alpha_mve_preparation,
    inspect_qe_alpha_mve_preparation,
    load_qe_alpha_mve_preparation,
    require_preparation_operation,
    write_qe_alpha_mve_preparation,
)
from backend.services.quantevolver.qe_dataset_contract import (
    QE_DATASET_CONTRACT_ID,
    QE_DATASET_SIGNAL_END_DATE,
    QE_DATASET_START_DATE,
    QE_FROZEN_BIN_SNAPSHOT_ID,
    QE_FROZEN_SUSPEND_DATASET_ID,
)


ROOT = Path(__file__).resolve().parents[3]
CLI = ROOT / "scripts" / "advisory_qe_alpha_mve_prepare.py"


def _mutated_payload(*, section: str, field: str, value: object) -> dict[str, object]:
    payload = build_default_qe_alpha_mve_preparation().model_dump(mode="json")
    target = payload[section]
    assert isinstance(target, dict)
    target[field] = value
    return payload


def test_default_preparation_freezes_source_identity_budget_and_no_evidence_boundary():
    preparation = build_default_qe_alpha_mve_preparation()

    assert preparation.data_identity.qe_dataset_contract_id == QE_DATASET_CONTRACT_ID
    assert preparation.data_identity.signal_start_date == QE_DATASET_START_DATE
    assert preparation.data_identity.signal_end_date == QE_DATASET_SIGNAL_END_DATE
    assert preparation.data_identity.qlib_bin_snapshot_id == QE_FROZEN_BIN_SNAPSHOT_ID
    assert preparation.data_identity.suspend_dataset_id == QE_FROZEN_SUSPEND_DATASET_ID
    assert preparation.expression_policy.input_families == INPUT_FAMILIES
    assert preparation.expression_policy.allowed_operators == ALLOWED_OPERATORS
    assert preparation.expression_policy.forbidden_operations == FORBIDDEN_OPERATIONS
    assert preparation.budget.signal_families == SIGNAL_FAMILIES
    assert preparation.budget.proposals_per_family == 4
    assert preparation.budget.total_proposal_budget == 24
    assert preparation.budget.current_generated_trial_count == 0
    assert preparation.budget.current_evaluated_trial_count == 0
    assert preparation.budget.current_selected_trial_count == 0
    assert preparation.budget.future_execution_concurrency == 1
    assert preparation.budget.future_resource_max_rss_bytes == 16 * 1024**3
    assert preparation.budget.future_resource_max_temp_bytes == 32 * 1024**3
    assert preparation.budget.future_resource_max_wall_seconds is None
    assert preparation.future_evidence.obligations == FUTURE_EVIDENCE_OBLIGATIONS
    assert preparation.future_evidence.known_effects == KNOWN_EFFECTS
    assert preparation.future_lineage.parent_experiment_ids == PARENT_EXPERIMENT_IDS
    assert preparation.future_lineage.future_planned_trial_count == 24
    assert preparation.status == "PREPARATION_ONLY_NO_RESEARCH_EVIDENCE"
    assert preparation.generation_authorized is False
    assert preparation.execution_authorized is False
    assert preparation.economic_evaluation_authorized is False
    assert preparation.registry_append_authorized is False
    assert preparation.research_evidence_produced is False
    assert preparation.sealed_holdout_accessed is False
    assert preparation.deployable is False
    assert "evidence_refs" not in preparation.model_dump(mode="json")


def test_semantic_identity_is_stable_across_build_time():
    first_at = datetime(2026, 9, 2, tzinfo=timezone.utc)
    first = build_default_qe_alpha_mve_preparation(created_at=first_at)
    second = build_default_qe_alpha_mve_preparation(created_at=first_at + timedelta(hours=1))

    assert first.created_at != second.created_at
    assert first.preparation_id == second.preparation_id
    assert first.preparation_sha256 == second.preparation_sha256
    assert first.functional_payload() == second.functional_payload()


@pytest.mark.parametrize(
    ("operation", "reason_code"),
    (
        (QEAlphaPreparationOperation.GENERATE, "ADVISORY_QE_ALPHA_GENERATION_NOT_AUTHORIZED"),
        (QEAlphaPreparationOperation.EXECUTE, "ADVISORY_QE_ALPHA_GENERATION_NOT_AUTHORIZED"),
        (
            QEAlphaPreparationOperation.ECONOMIC_EVALUATE,
            "ADVISORY_QE_ALPHA_EVALUATION_NOT_AUTHORIZED",
        ),
        (
            QEAlphaPreparationOperation.APPEND_REGISTRY,
            "ADVISORY_QE_ALPHA_REGISTRY_APPEND_NOT_AUTHORIZED",
        ),
    ),
)
def test_research_operations_fail_closed(operation, reason_code):
    preparation = build_default_qe_alpha_mve_preparation()

    with pytest.raises(AdvisoryModelFirstError) as captured:
        require_preparation_operation(preparation, operation)

    assert captured.value.reason_code == reason_code
    assert captured.value.context["operation"] == operation.value


def test_unknown_operation_uses_typed_invalid_contract():
    preparation = build_default_qe_alpha_mve_preparation()

    with pytest.raises(AdvisoryModelFirstError) as captured:
        require_preparation_operation(preparation, "TRAIN_AND_SCORE")

    assert captured.value.reason_code == "ADVISORY_QE_ALPHA_PREPARATION_INVALID"
    assert captured.value.context["operation"] == "TRAIN_AND_SCORE"


@pytest.mark.parametrize(
    ("section", "field", "value", "message"),
    (
        ("data_identity", "rolling_latest_allowed", True, "Input should be False"),
        ("expression_policy", "allowed_operators", list(ALLOWED_OPERATORS[:-1]), "drift"),
        (
            "expression_policy",
            "forbidden_operations",
            list(FORBIDDEN_OPERATIONS[:-1]),
            "drift",
        ),
        ("budget", "total_proposal_budget", 25, "Input should be 24"),
        ("future_lineage", "sealed_holdout_accessed", True, "Input should be False"),
    ),
)
def test_contract_mutations_fail_closed(section, field, value, message):
    with pytest.raises(ValidationError, match=message):
        FrozenAdvisoryQEAlphaMVEPreparationV1.model_validate(
            _mutated_payload(section=section, field=field, value=value)
        )


def test_unknown_field_and_self_hash_mutation_fail_closed():
    payload = build_default_qe_alpha_mve_preparation().model_dump(mode="json")
    payload["unexpected"] = True
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        FrozenAdvisoryQEAlphaMVEPreparationV1.model_validate(payload)

    payload.pop("unexpected")
    payload["preparation_sha256"] = "f" * 64
    with pytest.raises(ValidationError, match="identity mismatch"):
        FrozenAdvisoryQEAlphaMVEPreparationV1.model_validate(payload)


def test_immutable_write_exact_retry_and_invalid_existing_content(tmp_path):
    target = tmp_path / "preparation.json"
    preparation = build_default_qe_alpha_mve_preparation()

    first = write_qe_alpha_mve_preparation(target, preparation)
    original = target.read_bytes()
    second = write_qe_alpha_mve_preparation(target, preparation)

    assert first["status"] == "PREPARATION_WRITTEN"
    assert second["status"] == "EXISTING_PREPARATION"
    assert target.read_bytes() == original
    assert load_qe_alpha_mve_preparation(target) == preparation
    assert inspect_qe_alpha_mve_preparation(target)["status"] == "VALID_PREPARATION"

    invalid_target = tmp_path / "invalid.json"
    invalid_target.write_text("{}", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as captured:
        write_qe_alpha_mve_preparation(invalid_target, preparation)
    assert captured.value.reason_code == "ADVISORY_QE_ALPHA_PREPARATION_CONFLICT"


def test_writer_revalidates_model_construct_objects_before_touching_target(tmp_path):
    target = tmp_path / "must-not-exist.json"
    valid = build_default_qe_alpha_mve_preparation()
    bypassed = valid.model_copy(update={"generation_authorized": True})

    with pytest.raises(AdvisoryModelFirstError) as captured:
        write_qe_alpha_mve_preparation(target, bypassed)

    assert captured.value.reason_code == "ADVISORY_QE_ALPHA_PREPARATION_INVALID"
    assert not target.exists()


def test_inspect_rejects_source_pin_drift(tmp_path, monkeypatch):
    target = tmp_path / "preparation.json"
    preparation = build_default_qe_alpha_mve_preparation()
    write_qe_alpha_mve_preparation(target, preparation)
    original_projection = preparation_module._current_data_identity_payload

    def _drifted_projection():
        payload = original_projection()
        payload["qe_dataset_contract_id"] = "drifted-contract"
        return payload

    monkeypatch.setattr(preparation_module, "_current_data_identity_payload", _drifted_projection)
    with pytest.raises(AdvisoryModelFirstError) as captured:
        inspect_qe_alpha_mve_preparation(target)

    assert captured.value.reason_code == "ADVISORY_QE_ALPHA_PREPARATION_INVALID"


def test_cli_build_inspect_and_invalid_input_return_one_json_document(tmp_path):
    target = tmp_path / "preparation.json"

    built = subprocess.run(
        [sys.executable, str(CLI), "build", "--output", str(target)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert built.returncode == 0
    assert built.stderr == ""
    build_payload = json.loads(built.stdout)
    assert build_payload["status"] == "PREPARATION_WRITTEN"
    assert build_payload["generation_authorized"] is False

    inspected = subprocess.run(
        [sys.executable, str(CLI), "inspect", "--preparation", str(target)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert inspected.returncode == 0
    assert json.loads(inspected.stdout)["status"] == "VALID_PREPARATION"

    failed = subprocess.run(
        [sys.executable, str(CLI), "inspect", "--preparation", str(tmp_path / "missing.json")],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert failed.returncode == 1
    assert failed.stderr == ""
    assert json.loads(failed.stdout)["reason_code"] == "ADVISORY_QE_ALPHA_PREPARATION_INVALID"


def test_preparation_module_has_no_research_executor_or_external_io_imports():
    source = (ROOT / "backend" / "services" / "advisory_model_first" / "qe_alpha_mve_preparation.py").read_text(
        encoding="utf-8"
    )

    for forbidden_import in (
        "import requests",
        "import httpx",
        "import psycopg",
        "import sqlalchemy",
        "import subprocess",
        "rdagent",
        "alpha_signal_audit_pipeline",
        "strategy_package_batch_prediction",
    ):
        assert forbidden_import not in source
