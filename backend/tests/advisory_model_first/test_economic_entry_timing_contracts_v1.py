import pytest
from pydantic import ValidationError

from backend.tests.advisory_model_first.test_economic_entry_timing_training_v1 import arguments
from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES
from backend.services.advisory_model_first.economic_entry_timing_contracts_v1 import TimingTrainingRequestV1

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


@pytest.mark.parametrize("change", [{"candidate_names": ECONOMIC_FEATURE_NAMES}, {"fitted_head_count": 5}, {"deployable": True}, {"decision_use": "ACTIVATION_EVIDENCE"}])
def test_legacy_order_or_budget_activation_changes_are_rejected(study, change):
    request = arguments(study)[0]["request"]
    assert len(request.control_names) == 13 and len(request.candidate_names) == 15
    assert request.control_names[-1] == request.candidate_names[-1] == "query_gap_bps"
    with pytest.raises(ValidationError):
        TimingTrainingRequestV1.model_validate({**request.model_dump(), **change})
