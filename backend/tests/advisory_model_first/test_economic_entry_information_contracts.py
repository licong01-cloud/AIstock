import pytest
from pydantic import ValidationError

from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryTrainingRequestV3
from backend.services.advisory_model_first.economic_entry_information_contracts import (
    EconomicEntryInformationScopeV4, EconomicEntryInformationTrainingRequestV4, INFORMATION_FEATURE_NAMES,
)
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EconomicModelScopeV2
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def test_separate_schema_keeps_old_nine_strict_and_counts_control(study):
    parent = EconomicModelScopeV2(package_id="package", manifest_sha256="a"*64, selection_runtime_semantics_hash="b"*64,
        feature_schema_sha256="c"*64, shadow_policy_sha256="d"*64, cost_policy_sha256="e"*64, coordinate_algorithm_sha256="f"*64)
    scope = EconomicEntryInformationScopeV4(parent_scope=parent, training_information_sha256="0"*64)
    assert len(scope.feature_names)==13 and scope.feature_names[8]=="query_gap_bps" and len(parent.feature_names)==9
    with pytest.raises(ValidationError):
        EconomicModelScopeV2.model_validate({**parent.model_dump(), "feature_names": INFORMATION_FEATURE_NAMES})
    original = AlignedEntryTrainingRequestV3(source_request=study["request"], risk_labels_content_sha256="1"*64,
        common_fit_rows_sha256="2"*64, implementation_sha256="3"*64, lightgbm_version="4.6.0")
    ref = EvidenceReferenceV1(role="entry_information_v4_manifest",artifact_uri="F:/unit_fixture/info.json",sha256="4"*64,size_bytes=20)
    request = EconomicEntryInformationTrainingRequestV4(parent_request=original,information_ref=ref,
        common_fit_rows_sha256="5"*64,implementation_sha256="6"*64,information_rows_sha256="7"*64)
    assert request.model_configuration_count==2 and request.fitted_head_count==4 and request.economic_candidate_count==1
    assert original.source_request == study["request"] and not request.deployable
    for change in ({"feature_names": tuple(reversed(INFORMATION_FEATURE_NAMES))}, {"model_configuration_count":1},
                   {"deployable":True},{"information_semantics_sha256":"a"*64}):
        with pytest.raises(ValidationError):
            EconomicEntryInformationTrainingRequestV4.model_validate({**request.model_dump(),**change})
