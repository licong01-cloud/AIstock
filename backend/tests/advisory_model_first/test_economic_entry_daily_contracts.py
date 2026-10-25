from datetime import datetime

import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryDailyInputV1, EconomicEntryPriceNodeV1, EconomicEntryCandidateProjectionV1
from backend.tests.advisory_model_first.test_economic_entry_daily_inference import _daily

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def test_restored_cohort_cannot_be_promoted_to_native_or_backdated_prospective(study):
    original = _daily(study)["prediction_input"].model_dump()
    cohort = EconomicEntryDailyInputV1.model_validate({**original, "run_id": None, "list_id": None, "restored_cohort_sha256": "1" * 64})
    assert cohort.run_id is None and cohort.identity_sha256 != _daily(study)["prediction_input"].identity_sha256
    for changes in ({"run_id": "invented"}, {"restored_cohort_sha256": None}, {"source_evidence": "NATIVE_COMPLETE"},
                    {"evidence_level": "PROSPECTIVE_INPUT"}, {"captured_at": datetime(2025, 1, 28)}, {"selection_rank": True}):
        with pytest.raises(ValidationError):
            EconomicEntryDailyInputV1.model_validate({**cohort.model_dump(), **changes})


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True])
def test_public_price_nodes_never_publish_nonfinite_or_boolean_estimates(value):
    with pytest.raises(ValidationError):
        EconomicEntryPriceNodeV1(price_cny=10., status="ACCEPTABLE", expected_net_return_bps=value,
                                entry_net_max_loss_q90_bps=100., support_observations=50, support_decision_days=10)


def test_projection_contract_does_not_fake_two_legs_native_scope_or_activation(study):
    raw = dict(scope=_daily(study)["model_scope"], component_roles={"lstm": "unit-a", "fund": "unit-b"},
        terminal_weights={"unit-a": .4, "unit-b": .6}, universe_selection={"mode": "stock_universe", "pool_ids": []},
        review_policy_sha256="1" * 64)
    contract = EconomicEntryCandidateProjectionV1(**raw)
    assert contract.scope.universe_definition_evidence == "UNPROVEN" and len(contract.projection_sha256) == 64
    for changes in ({"terminal_weights": {"unit-a": True, "unit-b": 0.}},
                    {"terminal_weights": {"unit-a": .3, "unit-b": .6}},
                    {"component_roles": {"lstm": "unit-a", "fund": "unit-a"}}, {"deployable": True}):
        with pytest.raises(ValidationError):
            EconomicEntryCandidateProjectionV1(**{**raw, **changes})
