import numpy as np
import pytest

from backend.services.position_timing.contracts import POSITION_TIMING_L2_RESEARCH_CONTRACT_V1
from backend.services.position_timing.learnability_pipeline import (
    POPULATION_END,
    PositionTimingL2Error,
    _directional_status,
    _validate_candidate_source,
)


def test_l2_contract_is_frozen_without_pipeline_imports() -> None:
    contract = POSITION_TIMING_L2_RESEARCH_CONTRACT_V1
    assert contract.implementation_status == "OFFLINE_PIPELINE_AVAILABLE_NO_RUNTIME_MODEL"
    assert contract.hypothesis_count == 2
    assert contract.economic_threshold_bps == 0.0
    assert [model.model_id for model in contract.models] == ["SKLEARN_RIDGE_V1", "LIGHTGBM_GBDT_V1"]
    assert contract.cross_validation_spec["paths"] == 28
    assert contract.inference_spec["familywise_hypothesis_count"] == 2
    assert contract.inference_spec["mde_is_admission_gate"] is False
    assert contract.registry_spec["generate_current_route"] is False
    assert contract.registry_spec["direction_gate_scope"] == "L3_LABEL_ONLY_NEVER_L1_L1A_OR_RELEASE"
    assert "hmm" not in " ".join(contract.feature_order).lower()
    assert "selection" not in " ".join(contract.feature_order).lower()
    assert contract.population_spec["selection_features"].startswith("EXCLUDED_FROM_V1")
    assert "action_side" in contract.required_card_issued_fields


def test_l2_directional_status_blocks_only_the_supplied_directional_limit() -> None:
    buy = _directional_status(
        side="BUY",
        open_values=np.asarray([10.0, 10.0, np.nan]),
        high_values=np.asarray([10.0, 10.2, np.nan]),
        low_values=np.asarray([10.0, 9.9, np.nan]),
        close_values=np.asarray([10.0, 10.1, np.nan]),
        factor_values=np.asarray([1.0, 1.0, np.nan]),
        limit_values=np.asarray([10.0, 11.0, np.nan]),
        suspended=np.asarray([False, False, True]),
    )
    sell = _directional_status(
        side="SELL",
        open_values=np.asarray([9.0, 10.0, np.nan]),
        high_values=np.asarray([9.0, 10.2, np.nan]),
        low_values=np.asarray([9.0, 9.9, np.nan]),
        close_values=np.asarray([9.0, 10.1, np.nan]),
        factor_values=np.asarray([1.0, 1.0, np.nan]),
        limit_values=np.asarray([9.0, 9.0, np.nan]),
        suspended=np.asarray([False, False, False]),
    )
    assert buy.tolist() == [1, 0, 1]
    assert sell.tolist() == [1, 0, 2]


def test_l2_candidate_validation_returns_typed_failure_for_bad_dates() -> None:
    state = {
        "schema_version": "qe_direct_monthly_state_v3",
        "status": "CANDIDATE_READY",
        "cutoff": "not-a-date",
        "components": {
            "daily_bin": {"status": "PASS"},
            "suspend_d": {"status": "PASS"},
            "index_context": {"status": "PASS"},
        },
        "production_writes": 0,
        "production_pointer_changes": 0,
    }
    daily = {
        "universe_key": "aistock_equity_pit_canonical_v2",
        "rule_version": "shsz_a_252td_st_delist_asof_v2",
        "survivorship_bias": "canonical_lifecycle_pit",
        "st_pit": True,
        "exclude_st": True,
        "start": "2018-08-01",
        "end": POPULATION_END.isoformat(),
    }
    suspend = {"universe_key": "aistock_equity_pit_canonical_v2"}
    with pytest.raises(PositionTimingL2Error) as exc_info:
        _validate_candidate_source(state=state, daily_meta=daily, suspend_meta=suspend)
    assert exc_info.value.reason_code == "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH"
