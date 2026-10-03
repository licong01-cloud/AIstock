from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_price_campaign_evaluation_v2 import campaign_actual_decisions_v2, campaign_navigation_v2
from backend.tests.advisory_model_first.test_economic_price_campaign_inference_v2 import fit_fixture


def test_candidate_failure_does_not_stop_campaign_unknown_takes_cannot_advance():
    metrics = {arm: dict(max_drawdown=-.05, tail_mean_bps=-30.) for arm in ('baseline', 'matched', 'candidate')}
    args = dict(metrics=metrics, increments={arm: dict(mean_bps=10.) for arm in ('baseline', 'matched')},
        interventions={arm: list(range(20)) for arm in ('baseline', 'matched')}, take_episodes=0, decision_days=81)
    result = campaign_navigation_v2(**args)
    assert result['navigation'] == 'STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION'
    assert result['continue_next_preregistered_model'] and not result['entire_campaign_stopped']
    assert campaign_navigation_v2(**{**args, 'take_episodes': 30})['navigation'] == 'CONSIDER_CONFIRMATION_DESIGN_ONLY'


def test_actual_query_only_uses_T_open_not_future_HLC_and_missing_rows_preserved():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    candidates = pd.DataFrame([{**key, 'selection_effective_rank': 1}])
    inputs = pd.DataFrame([{**key, **dict.fromkeys(D_FEATURES, 0.), 'feature_visible_through': key[KEY[0]]}])
    prices = pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.06,
        raw_close_cny=-999., raw_low_cny=-999., raw_high_cny=-999., suspended=False, tradability_unknown=False,
        up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)])
    references = pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}])
    args = dict(fitted=fit_fixture(), candidates=candidates, inputs=inputs, prices=prices, references=references,
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64), arm='candidate')
    assert campaign_actual_decisions_v2(**args).model_action.tolist() == ['TAKE']
    missing = campaign_actual_decisions_v2(**{**args, 'prices': prices.iloc[:0]})
    assert len(missing) == 1 and not missing.market_admissible.iloc[0]
    inputs['feature_visible_through'] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        campaign_actual_decisions_v2(**args)
