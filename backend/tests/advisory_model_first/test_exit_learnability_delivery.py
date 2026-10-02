from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.exit_learnability_contracts import (
    ExitLearnabilitySupportV1,
)
from backend.services.advisory_model_first.exit_learnability_pipeline import (
    _find_existing_bundle,
    _publish_bundle,
    _validate_action_source_summary,
    inspect_exit_learnability_bundle,
)
from backend.services.advisory_model_first.tier1_oracle_contracts import (
    Tier1EvidenceState,
    Tier1MetricInferenceV1,
)
from backend.tests.advisory_model_first.test_exit_learnability_contracts import (
    make_exit_learnability_request,
)
from scripts.advisory_exit_learnability_audit import main


def test_publish_inspect_and_mutation_fail_closed(tmp_path) -> None:
    request = make_exit_learnability_request(output_root=tmp_path.as_posix())
    inference = Tier1MetricInferenceV1(
        point_estimate_bps=10.0,
        confidence_lower_bps=6.0,
        confidence_upper_bps=14.0,
        bootstrap_standard_error_bps=1.0,
        mde_bps=2.8,
        economic_threshold_bps=5.0,
        evidence_state=Tier1EvidenceState.HIGH,
        evaluated_day_count=100,
    )
    support = ExitLearnabilitySupportV1(
        evaluated_episode_count=500,
        evaluated_entry_day_count=100,
        evaluated_action_day_count=120,
        intervention_episode_count=100,
        intervention_action_day_count=60,
        intervention_action_day_fraction=0.5,
        intervention_days_by_regime={"UP_OR_FLAT": 30, "DOWN": 30},
        effective_intervention_block_count=3,
        support_sufficient=True,
        reason_codes=(),
    )
    sources = {
        "action_request": SimpleNamespace(request_sha256="3" * 64),
        "action_inspection": {"receipt_sha256": "4" * 64},
        "action_receipt": SimpleNamespace(receipt_sha256="4" * 64),
        "n1_request": SimpleNamespace(request_sha256="5" * 64),
        "exit_labels": pd.DataFrame({"episode_id": ["episode-1"]}),
        "exit_episode_best": pd.DataFrame({"episode_id": ["episode-1"]}),
        "cpcv_payload": {"paths": [{"status": "READY", "path_id": str(index)} for index in range(28)]},
    }
    features = pd.DataFrame(
        {
            "value": [1.0],
            "label_status": ["AVAILABLE"],
            "missing_numeric_feature_count": [0],
        }
    )
    one = pd.DataFrame({"value": [1.0]})
    bundle = _publish_bundle(
        request=request,
        sources=sources,
        features=features,
        oof=one,
        episode_policy=one,
        daily_policy=one,
        inference=inference,
        support=support,
        diagnostics={
            "row": {},
            "episode": {},
            "daily": {},
            "oracle_mean_lift_bps": 100.0,
            "oracle_capture_ratio": 0.1,
        },
        elapsed_seconds=1.0,
    )

    inspected = inspect_exit_learnability_bundle(bundle)
    assert inspected["status"] == "VALID"
    assert inspected["decision_use"] == "DIRECTION_GATE"
    assert inspected["deployable"] is False
    assert _find_existing_bundle(request) == bundle

    pd.DataFrame({"value": [2.0]}).to_parquet(bundle / "features.parquet", index=False)
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        inspect_exit_learnability_bundle(bundle)
    assert exc_info.value.reason_code == "ADVISORY_EXIT_LEARNABILITY_BUNDLE_INVALID"


def test_action_source_uses_public_inspection_summary_contract() -> None:
    request = SimpleNamespace(request_sha256="a" * 64)
    receipt = SimpleNamespace(
        request_sha256="a" * 64,
        receipt_sha256="b" * 64,
        sealed_holdout_accessed=False,
        deployable=False,
    )
    public_summary = {
        "status": "VALID",
        "request_sha256": "a" * 64,
        "receipt_sha256": "b" * 64,
        "sealed_holdout_accessed": False,
        "deployable": False,
    }

    _validate_action_source_summary(
        action_request=request,
        action_inspection=public_summary,
        action_receipt=receipt,
    )
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        _validate_action_source_summary(
            action_request=request,
            action_inspection={**public_summary, "receipt_sha256": "c" * 64},
            action_receipt=receipt,
        )
    assert exc_info.value.reason_code == ("ADVISORY_EXIT_LEARNABILITY_SOURCE_IDENTITY_MISMATCH")


def test_cli_argument_failure_is_typed(capsys) -> None:
    assert main(["prepare"]) == 1
    payload = capsys.readouterr().out
    assert "ADVISORY_EXIT_LEARNABILITY_REQUEST_INVALID" in payload
