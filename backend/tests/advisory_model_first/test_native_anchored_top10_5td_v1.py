"""Direct rank policy, population and staged offline-consumption contracts."""
from copy import deepcopy
from types import SimpleNamespace
import time

import numpy as np
import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_multiperiod_top10_5td_v1 import rows as rows
from backend.services.advisory_model_first import native_anchored_top10_5td_v1 as study
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import FINANCE


def predictions(rows):
    p = rows.loc[:, study.IDENTITY].copy()
    p['predicted_relative_net_bps'] = -p.selection_effective_rank.astype(float)
    p['no_rank_advice'] = False
    p['advisory_rank'] = p.selection_effective_rank
    p['selected'] = p.advisory_rank.le(10)
    return p


def test_same_model_order_has_no_intervention(rows):
    original = predictions(rows)
    assert study.combine(original).equals(original)


def test_one_boundary_challenger_enters_but_scores_and_native_are_unchanged(rows):
    original = predictions(rows)
    original.loc[original.selection_effective_rank.eq(11), 'predicted_relative_net_bps'] = 100.
    saved = original.copy(deep=True)
    mixed = study.combine(original)
    for _, g in mixed.groupby(['package_id', study.previous.KEY[0]]):
        assert set(g.loc[g.selected, 'selection_effective_rank']) == set(range(1, 10)) | {11}
    assert original.equals(saved)
    assert mixed.predicted_relative_net_bps.equals(original.predicted_relative_net_bps)
    study.same_population(original, mixed)


def test_unknown_slot_and_beyond_top20_are_not_replaced(rows):
    g = predictions(rows).iloc[:20]
    p = pd.concat([g]*3, ignore_index=True).iloc[:50].copy()
    p['instrument'] = [f'{i:06}.SZ' for i in range(50)]
    p['selection_effective_rank'] = range(1, 51)
    p['candidate_group_size'] = 50
    p['advisory_rank'] = p.selection_effective_rank
    p['selected'] = p.advisory_rank.le(10)
    p.loc[20:, 'predicted_relative_net_bps'] = 999.
    p.loc[10, ['no_rank_advice', 'predicted_relative_net_bps']] = [True, 100.]
    result = study.combine(p)
    assert result.loc[10, 'advisory_rank'] == 11
    assert result.loc[20:, 'advisory_rank'].equals(p.loc[20:, 'selection_effective_rank'])
    assert len(result) == 50 and result.selected.sum() == 10


def test_equal_score_preserves_native_ties(rows):
    p = predictions(rows)
    p['predicted_relative_net_bps'] = 1.
    assert study.combine(p).advisory_rank.equals(p.selection_effective_rank)


@pytest.mark.parametrize('fault', ['duplicate', 'rank_gap', 'nonfinite', 'nonbool'])
def test_invalid_population_and_advice_fail(rows, fault):
    p = predictions(rows)
    if fault == 'duplicate':
        p = pd.concat([p, p.iloc[:1]], ignore_index=True)
    elif fault == 'rank_gap':
        p.loc[0, 'selection_effective_rank'] = 22
    elif fault == 'nonfinite':
        p.loc[0, 'predicted_relative_net_bps'] = np.inf
    else:
        p['no_rank_advice'] = p.no_rank_advice.astype(int)
    with pytest.raises(ValueError):
        study.combine(p)


def test_cross_package_identity_mismatch_fails(rows):
    p = predictions(rows)
    wrong = p.copy()
    wrong.loc[0, 'manifest_sha256'] = 'b'*64
    with pytest.raises(ValueError, match='identity'):
        study.same_population(p, wrong)


def test_common_days_keep_holes_and_no_triple_population_substitution(rows):
    p = predictions(rows)
    finance = rows.loc[:, [*study.KEY, *FINANCE]]
    panel = study.comparison_panel(p, p, finance)
    panel.loc[0, 'candidate_full_model'] = np.nan
    result, daily = study.summarize(panel)
    assert len(daily) == 3 and result['statistics']['paired_days'] == 2
    assert pd.isna(daily.delta.iloc[0])
    assert result['individual_available_d']['candidate']['known'] == 3
    with pytest.raises(ValueError, match='axis'):
        study.summarize(panel.iloc[1:])


def test_finance_projects_future_H_without_decoding_poison(tmp_path, rows):
    path = tmp_path/'prepared'
    path.mkdir()
    finance = rows.loc[:, [*study.KEY, *FINANCE]].copy()
    poison = finance[study.previous.KEY[0]].eq('2025-01-03')
    finance.loc[poison, 'label_information_end'] = pd.Timestamp('2025-03-01')
    finance.loc[poison, 'hypothetical_liquidation_net_bps'] = np.inf
    finance.to_parquet(path/'train_finance.parquet', index=False)
    obj = study.Study.__new__(study.Study)
    obj.source = SimpleNamespace(root=tmp_path)
    observed = obj.finance('2025-01-02', '2025-01-31')
    assert len(observed) == 80 and not np.isinf(observed.hypothetical_liquidation_net_bps).any()
    assert observed.loc[observed[study.previous.KEY[0]].eq('2025-01-03'), 'valuation_status'].eq('IMMATURE').all()
    assert observed[study.previous.KEY[0]].max() < pd.Timestamp('2026-01-01')


def test_insufficient_intervention_does_not_advance_even_with_positive_point_value():
    panel = pd.DataFrame([dict(package_id=p, decision_date=d, fold=1+i//10, mature=True,
        intervention_count=0, candidate_unknown=0, baseline_unknown=0, original_candidates=50,
        candidate=101., baseline=100., candidate_full_model=99., delta=1.)
        for i, d in enumerate(pd.bdate_range('2025-04-01', periods=30)) for p in ['a', 'b']])
    result, _ = study.summarize(panel)
    assert not result['proceed_outer']
    panel['intervention_count'] = 1
    assert study.summarize(panel)[0]['proceed_outer']


@pytest.mark.parametrize('proceed', [False, True])
def test_run_uses_outer_barrier_and_exact_retry_without_fit(tmp_path, monkeypatch, rows, proceed):
    obj = study.Study.__new__(study.Study)
    obj.root, obj.digest, obj.started = tmp_path, 'a'*64, time.monotonic()
    obj.plan = {'experiment_id':'test_run'}
    obj.source = SimpleNamespace()
    monkeypatch.setattr(obj, 'verify_source', lambda: None)
    monkeypatch.setattr(obj, 'register', lambda *args: None)
    obj.publish('preregistered', 'preregistered', None, {'plan.json':b'{}'})
    monkeypatch.setattr(study, 'Study', lambda root: obj)
    inner_result = dict(proceed_outer=proceed, classification='INNER_SUPPORTED' if proceed else 'INNER_NOT_SUPPORTED')
    def fake_inner(s, parent):
        digest = s.publish('inner/evaluated', 'evaluated', parent, {'report.json':study._json_bytes(inner_result)})
        return inner_result, digest
    monkeypatch.setattr(study, 'inner', fake_inner)
    counter = {'predictions':0, 'finance':0}
    def get_predictions(first, last):
        counter['predictions'] += 1
        return predictions(rows)
    def get_finance(first, last, outer=False):
        counter['finance'] += 1
        assert outer and (tmp_path/'outer/prepared/manifest.json').exists()
        return rows.loc[:, [*study.KEY, *FINANCE]]
    monkeypatch.setattr(obj, 'predictions', get_predictions)
    monkeypatch.setattr(obj, 'finance', get_finance)
    result = study.run(tmp_path)
    assert result['outer_started'] is proceed and result['physical_fit_count'] == 0
    assert counter == {'predictions':int(proceed), 'finance':int(proceed)}
    before = deepcopy(counter)
    assert study.run(tmp_path)['exact_retry']
    assert counter == before
    (tmp_path/'evaluated/result.json').write_text('{}', encoding='utf8')
    with pytest.raises(AdvisoryModelFirstError):
        study.run(tmp_path)


def test_unknown_challenger_keeps_hole_not_fake_gain_or_cash(rows):
    control = predictions(rows)
    control.loc[control.selection_effective_rank.eq(11), 'predicted_relative_net_bps'] = 100.
    candidate = study.combine(control)
    finance = rows.loc[:, [*study.KEY, *FINANCE]].copy()
    finance.loc[rows.selection_effective_rank.eq(11), ['valuation_status', 'hypothetical_liquidation_net_bps']] = ['UNKNOWN_SOURCE', np.nan]
    panel = study.comparison_panel(candidate, control, finance)
    result, daily = study.summarize(panel)
    assert result['candidate_unknown_slots'] == 6 and result['baseline_unknown_slots'] == 0
    assert result['statistics']['mean_bps'] is None and daily.delta.isna().all()
    assert result['original_candidates'] == len(rows)


def test_preregistration_normalizes_json_contract_before_identity_check(tmp_path, monkeypatch):
    source = SimpleNamespace(root=tmp_path/'source', read=lambda stage:'b'*64,
        plan={'experiment_id':'parent', 'binding':{'binding_hash':'c'*64}})
    monkeypatch.setattr(study.previous, 'Study', lambda path:source)
    monkeypatch.setattr(study.previous, 'load', lambda path:{'config':{'id':study.CONTRACT['config_id']}})
    result = study.preregister(tmp_path/'study', source.root, 'd'*40)
    assert result.plan['contract']['folds'] == [list(f) for f in study.base.FOLDS]
    assert (result.root/'preregistered/manifest.json').exists()
