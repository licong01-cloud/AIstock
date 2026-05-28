from __future__ import annotations

from backend.routers import execution_policy


def test_execution_policy_validation_rejects_implicit_twap_fallback() -> None:
    blockers = execution_policy._validate_policy_contract(
        {
            "algo_code": "POV",
            "fallback_algo_code": "TWAP",
            "algo_config": {"max_participation_rate": 0.1},
            "data_requirements": {"required": ["minute_bar", "pre_close", "limit", "suspend_d", "participation_cap"]},
        }
    )
    assert "fallback_policy_required_when_fallback_algo_code_is_set" in blockers


def test_execution_policy_validation_requires_minute_market_data_contract() -> None:
    blockers = execution_policy._validate_policy_contract({"algo_code": "TWAP", "data_requirements": {"required": ["minute_bar"]}})
    assert "missing_data_requirements:limit,pre_close,suspend_d" in blockers


def test_execution_policy_algo_catalog_summary_does_not_inline_model_paths_as_weights() -> None:
    catalog = execution_policy._algo_catalog()
    assert "TWAP" in catalog
    assert "V25_TWO_STAGE" in catalog
    v25 = catalog["V25_TWO_STAGE"]
    assert v25["artifact_refs"]
    assert "model_weights" not in str(v25)
