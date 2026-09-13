import json
from pathlib import Path

import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing.action_value_data import file_reference
from backend.services.position_timing.contracts import (
    canonical_json_bytes,
    canonical_sha256,
)
from backend.services.position_timing.pattern_research import (
    FACTOR_ACTION_COVERAGE_POLICY,
    FACTOR_ACTION_COVERAGE_POLICY_SHA256,
)
from backend.services.position_timing.pattern_rights_issue import (
    RIGHTS_ISSUE_PARTICIPATION_POLICY,
    RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256,
)
from backend.services.position_timing.policy import COST_POLICY_SHA256
from backend.services.position_timing.volatility_contraction_breakout import (
    RULE_SPEC,
    RULE_SPEC_SHA256,
)
from backend.services.position_timing.volatility_contraction_breakout_research import (
    BUNDLE_SCHEMA,
    FALSE_WRITE_FLAGS,
    FORMAL_COMPARISON,
    PARENT_COUNTS,
    PARENT_PATTERN_REQUEST_SHA256,
    PIPELINE_ID,
    RECEIPT_SCHEMA,
    REQUEST_SCHEMA,
    RESULT_CLASS,
    SOURCE_CODE_KEYS,
    _comparison,
    _load_request,
    _publish_bundle,
    _same_canonical_identity,
    _scenario_coverage_complete,
    inspect_bundle,
    run_request,
)


def _hashed(payload: dict, field: str) -> dict:
    result = dict(payload)
    result[field] = canonical_sha256(result)
    return result


def _request(tmp_path: Path) -> tuple[Path, dict]:
    parent = tmp_path / "parent-request.json"
    parent.write_text("{}", encoding="utf-8")
    policy = tmp_path / "rights-policy.json"
    policy.write_bytes(canonical_json_bytes(RIGHTS_ISSUE_PARTICIPATION_POLICY))
    source = Path(
        "backend/services/position_timing/volatility_contraction_breakout.py"
    ).resolve()
    factor_audit = _hashed(
        {
            "coverage_complete": True,
            "unbound_material_factor_change_count": 0,
            "insufficient_factor_symbol_count": 0,
        },
        "audit_sha256",
    )
    corporate_application = _hashed(
        {"scope": "test"}, "application_sha256"
    )
    rights_application = _hashed(
        {"applications": []}, "application_sha256"
    )
    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "repository_root": tmp_path.as_posix(),
        "repository_commit": "a" * 40,
        "timing_root": (tmp_path / "timing").as_posix(),
        "parent_pattern_request": file_reference(parent),
        "parent_pattern_request_sha256": PARENT_PATTERN_REQUEST_SHA256,
        "parent_result_read": False,
        "candidate_root": (tmp_path / "candidate").as_posix(),
        "candidate_manifest_sha256": (
            "ed8375696030ca95b4a1f30167c2ac956e69b8276ba981babd301682dcea78de"
        ),
        "candidate_dataset_manifest_sha256": (
            "1db13b2129409c2ee4aabd8bc83c3f5e5eee1fde2a859a3cb2a722d885dd5c49"
        ),
        "candidate_source_identity": {"source_sha256": "b" * 64},
        "evaluation_symbols": [f"{item:06d}.SZ" for item in range(64)],
        "evaluation_symbol_count": 64,
        "population_reuse_disclosure": RESULT_CLASS,
        "population_spec": {
            "source_start": "2018-08-01",
            "source_end": "2026-08-31",
            "initial_history_sessions": 756,
            "evaluation_start": "2021-08-01",
            "evaluation_end": "2026-08-31",
        },
        "rule_spec": RULE_SPEC,
        "rule_spec_sha256": RULE_SPEC_SHA256,
        "formal_comparison": FORMAL_COMPARISON,
        "formal_hypothesis_count": 1,
        "bootstrap_samples": 5000,
        "block_sessions": 25,
        "inference_seed": 20260914,
        "economic_threshold_bps": 0.0,
        "parent_order_scenarios": PARENT_COUNTS,
        "cost_policy_sha256": COST_POLICY_SHA256,
        "corporate_action_snapshot": {"path": "unused", "sha256": "c" * 64},
        "corporate_action_snapshot_sha256": "d" * 64,
        "suspension_snapshot": {"path": "unused", "sha256": "e" * 64},
        "suspension_snapshot_sha256": "f" * 64,
        "corporate_action_application_audit": corporate_application,
        "corporate_action_application_sha256": corporate_application[
            "application_sha256"
        ],
        "corporate_action_source_snapshot": {"snapshot_sha256": "1" * 64},
        "corporate_action_source_snapshot_sha256": "1" * 64,
        "rights_issue_authority": {"path": "unused", "sha256": "2" * 64},
        "rights_issue_authority_canonical_sha256": (
            "4a7cdb79e968f33a000f2e9b81196986349cff26100688794b87f6a1f454f10c"
        ),
        "rights_issue_source_documents_sha256": "3" * 64,
        "rights_issue_participation_policy": RIGHTS_ISSUE_PARTICIPATION_POLICY,
        "rights_issue_participation_policy_sha256": (
            RIGHTS_ISSUE_PARTICIPATION_POLICY_SHA256
        ),
        "rights_issue_participation_policy_artifact": file_reference(policy),
        "rights_issue_application_audit": rights_application,
        "rights_issue_application_sha256": rights_application[
            "application_sha256"
        ],
        "factor_action_coverage_policy": FACTOR_ACTION_COVERAGE_POLICY,
        "factor_action_coverage_policy_sha256": FACTOR_ACTION_COVERAGE_POLICY_SHA256,
        "factor_action_coverage_audit": factor_audit,
        "factor_action_coverage_audit_sha256": factor_audit["audit_sha256"],
        "source_code": {key: file_reference(source) for key in SOURCE_CODE_KEYS},
        "result_class": RESULT_CLASS,
        **{flag: False for flag in FALSE_WRITE_FLAGS},
    }
    request["request_sha256"] = canonical_sha256(request)
    request_path = tmp_path / "request.json"
    request_path.write_bytes(canonical_json_bytes(request))
    return request_path, request


def _receipt(request: dict, coverage: dict) -> dict:
    identity = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "repository_commit": request["repository_commit"],
        "request_sha256": request["request_sha256"],
        "parent_pattern_request_sha256": PARENT_PATTERN_REQUEST_SHA256,
        "candidate_manifest_sha256": request["candidate_manifest_sha256"],
        "candidate_dataset_manifest_sha256": request[
            "candidate_dataset_manifest_sha256"
        ],
        "rule_spec_sha256": RULE_SPEC_SHA256,
        "cost_policy_sha256": COST_POLICY_SHA256,
        "coverage_sha256": coverage["coverage_sha256"],
        "result_class": RESULT_CLASS,
        "formal_hypothesis_count": 1,
        "formal_comparison": FORMAL_COMPARISON,
        "selected_trial_count": 0,
        **{flag.replace("_write", "_written"): False for flag in FALSE_WRITE_FLAGS},
    }
    return {**identity, "receipt_sha256": canonical_sha256(identity)}


def test_request_identity_rejects_result_read_or_audit_tamper(tmp_path: Path):
    path, request = _request(tmp_path)
    assert _load_request(path, verify_external_references=True)["parent_result_read"] is False

    request["parent_result_read"] = True
    request["request_sha256"] = canonical_sha256(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    path.write_bytes(canonical_json_bytes(request))
    with pytest.raises(ActionValueError, match="VCB_REQUEST_IDENTITY_MISMATCH"):
        _load_request(path)


def test_source_identity_comparison_accepts_json_tuple_list_round_trip_only():
    assert _same_canonical_identity(
        {"files": ("a.parquet", "b.parquet")},
        {"files": ["a.parquet", "b.parquet"]},
    )
    assert not _same_canonical_identity(
        {"files": ("a.parquet", "b.parquet")},
        {"files": ["a.parquet", "changed.parquet"]},
    )


def test_comparison_uses_one_daily_cross_symbol_estimand_and_coverage_constraint():
    rows = pd.DataFrame(
        {
            "comparison": ["P_MINUS_ALWAYS_OPEN_RISK_MANAGED"] * 20,
            "valuation_date": pd.bdate_range("2026-01-02", periods=10).repeat(2),
            "incremental_net_value_bps": [1.0] * 20,
            "incremental_gross_value_bps": [1.2] * 20,
        }
    )
    supported = _comparison(rows, coverage_complete=True, seed=7)
    constrained = _comparison(rows, coverage_complete=False, seed=7)

    assert supported["unit"] == "DAILY_CROSS_SYMBOL_MEAN_INCREMENTAL_BPS"
    assert supported["effective_trading_days"] == 10
    assert supported["effect_evidence"] == "SUPPORTED"
    assert constrained["effect_evidence"] == "INCONCLUSIVE"


def test_diagnostic_parent_split_failure_does_not_invalidate_primary_coverage():
    evaluated = {"1": {"A", "B"}, "2": {"A"}, "3": {"A"}}
    errors = [
        {
            "parent_order_count": 2,
            "symbol": "B",
            "error_code": "LEGAL_PARENT_SPLIT_UNAVAILABLE",
        },
        {
            "parent_order_count": 3,
            "symbol": "B",
            "error_code": "LEGAL_PARENT_SPLIT_UNAVAILABLE",
        },
    ]
    arguments = {
        "expected_symbols": 2,
        "evaluated_symbols": evaluated,
        "feature_errors": [],
        "path_errors": errors,
        "source_coverage_complete": True,
    }

    assert _scenario_coverage_complete(1, **arguments)
    assert not _scenario_coverage_complete(2, **arguments)
    assert not _scenario_coverage_complete(3, **arguments)


def test_bundle_is_recursive_immutable_and_exact_retry_is_noop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    request_path, request = _request(tmp_path)
    coverage_identity = {
        "schema_version": "position_timing_volatility_contraction_breakout_coverage_v1",
        "coverage_complete": True,
    }
    coverage = {
        **coverage_identity,
        "coverage_sha256": canonical_sha256(coverage_identity),
    }
    receipt = _receipt(request, coverage)
    bundle = (
        tmp_path
        / "timing"
        / "research"
        / "volatility_contraction_breakout_v1"
        / "bundles"
        / request["request_sha256"]
    )
    frame = pd.DataFrame({"value": [1]})
    _publish_bundle(
        bundle,
        request=request,
        receipt=receipt,
        coverage=coverage,
        signal_observations=frame,
        sleeve_days=frame,
        fills=frame,
    )
    first = inspect_bundle(bundle)
    manifest_bytes = (bundle / "manifest.json").read_bytes()

    monkeypatch.setattr(
        "backend.services.position_timing.volatility_contraction_breakout_research._clean_repository_commit",
        lambda _root: request["repository_commit"],
    )
    retry = run_request(request_path)
    assert retry["status"] == "ALREADY_MATERIALIZED"
    assert retry["manifest_sha256"] == first["manifest"]["manifest_sha256"]
    assert (bundle / "manifest.json").read_bytes() == manifest_bytes
    assert first["manifest"]["schema_version"] == BUNDLE_SCHEMA

    with (bundle / "sleeve_days.parquet").open("ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(ActionValueError, match="VCB_BUNDLE_FILE_IDENTITY_MISMATCH"):
        inspect_bundle(bundle)


def test_request_file_reference_tamper_fails_closed(tmp_path: Path):
    path, request = _request(tmp_path)
    Path(request["rights_issue_participation_policy_artifact"]["path"]).write_text(
        json.dumps({"tampered": True}), encoding="utf-8"
    )
    with pytest.raises(ActionValueError, match="VCB_RIGHTS_POLICY_REFERENCE_MISMATCH"):
        _load_request(path, verify_external_references=True)

    # Bundle inspection remains possible after an external source has moved;
    # exact execution still validates the live reference above.
    assert _load_request(path)["request_sha256"] == request["request_sha256"]
