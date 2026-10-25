import pytest

from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import FIT_BUDGETS, PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _root


def plan_fixture(model_id='M2'):
    evidence = dict(artifact_uri='F:/unit/manifest.json', sha256='a'*64, size_bytes=1)
    refs = {name: {**evidence, 'role': role} for name, role in
        (('parent_plan_ref', 'campaign_parent_plan'), ('parent_prepared_manifest_ref', 'campaign_prices_snapshot'),
         ('feature_manifest_ref', 'campaign_d_snapshot'), ('reusable_prepared_manifest_ref', 'campaign_value_labels'))}
    return PriceCampaignPlanV2(**refs, model_id=model_id, profile_path='F:/unit/profile.json', profile_sha256='b'*64,
        universe_selection={'mode': 'stock_universe', 'pool_ids': []}, implementation_sha256='c'*64)


def test_fixed_three_recipes_distinct_identity_and_no_activation_or_date_override():
    plans = [plan_fixture(model) for model in FIT_BUDGETS]
    assert sum(plan.parameters['physical_fit_budget'] for plan in plans) == 11
    assert len({plan.experiment_id for plan in plans}) == 3
    assert all(plan.parameters['candidate_count'] == 1 and not plan.deployable and 'train_start' not in plan.parameters for plan in plans)
    for update in ({'model_id': 'M1'}, {'deployable': True}, {'decision_use': 'ACTIVATION_EVIDENCE'}, {'study_type': 'CONFIRMATION'}, {'train_start': '2020-01-01'}):
        with pytest.raises(ValueError):
            PriceCampaignPlanV2.model_validate({**plans[0].model_dump(), **update})
    for path in ('relative', 'C:/temporary'):
        with pytest.raises(ValueError):
            _root(plans[0], path)
