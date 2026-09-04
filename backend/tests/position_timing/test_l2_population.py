from backend.services.position_timing.contracts import POSITION_TIMING_L2_RESEARCH_CONTRACT_V1


def test_l2_contract_is_frozen_without_pipeline_imports() -> None:
    contract = POSITION_TIMING_L2_RESEARCH_CONTRACT_V1
    assert contract.implementation_status == "PIPELINE_DEFERRED_BY_APPROVED_SCOPE"
    assert contract.hypothesis_count == 2
    assert contract.economic_threshold_bps == 0.0
    assert [model.model_id for model in contract.models] == ["SKLEARN_RIDGE_V1", "LIGHTGBM_GBDT_V1"]
    assert contract.cross_validation_spec["paths"] == 28
    assert contract.inference_spec["familywise_hypothesis_count"] == 2
    assert contract.inference_spec["mde_is_admission_gate"] is False
    assert contract.registry_spec["generate_current_route"] is False
    assert contract.registry_spec["direction_gate_scope"] == "L3_LABEL_ONLY_NEVER_L1_L1A_OR_RELEASE"
    assert "hmm" not in " ".join(contract.feature_order).lower()
    assert "action_side" in contract.required_card_issued_fields
