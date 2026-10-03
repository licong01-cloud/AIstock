import pytest

from backend.services.advisory_model_first.economic_context_value_contracts_v1 import ContextValuePlanV1
from backend.services.advisory_model_first.economic_context_value_pipeline_v1 import _root


def plan_fixture():
    evidence={'artifact_uri':'F:/unit/manifest.json','sha256':'a'*64,'size_bytes':1}
    return ContextValuePlanV1(parent_plan_ref={**evidence,'role':'context_parent_plan'},
        parent_prepared_manifest_ref={**evidence,'role':'context_prices_snapshot'},
        feature_manifest_ref={**evidence,'role':'context_d_snapshot'},profile_path='F:/unit/profile.json',
        profile_sha256='b'*64,authority_root='X:/unit/classification',universe_selection={'mode':'stock_universe','pool_ids':[]},
        implementation_sha256='c'*64)


def test_plan_is_navigation_only_fixed_four_heads_not_activation_or_global_date_override():
    plan=plan_fixture()
    assert plan.parameters['physical_fit_budget']==4 and not plan.deployable
    assert plan.parameters['candidate_count']==1 and 'train_start' not in plan.parameters
    for update in ({'deployable':True},{'study_type':'CONFIRMATION'},{'decision_use':'ACTIVATION_EVIDENCE'},
            {'universe_selection':{'mode':'stock_universe','pool_ids':['csi300']}}):
        with pytest.raises(ValueError):
            ContextValuePlanV1.model_validate({**plan.model_dump(),**update})
    for path in ('relative','C:/tmp/context'):
        with pytest.raises(ValueError):
            _root(plan,path)
