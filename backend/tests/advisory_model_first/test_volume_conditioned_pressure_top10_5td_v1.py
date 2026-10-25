"""Synthetic contract tests are not research trials or economic evidence."""
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import volume_conditioned_pressure_top10_5td_v1 as core
from backend.services.advisory_model_first import volume_conditioned_pressure_top10_5td_pipeline_v1 as pipe
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import FINANCE
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def population(days=('2025-01-02', '2025-01-03', '2026-01-02')):
    rows = []
    for d in pd.to_datetime(days):
        for package in ('a', 'b'):
            for rank in range(1, 51):
                y = float(100-rank)
                rows.append(dict(package_id=package, manifest_sha256='a'*64, run_id='run_'+package,
                    decision_as_of_trade_date=d, target_trade_date=d+pd.offsets.BDay(1), instrument=f'{rank:06}.SZ',
                    selection_effective_rank=rank, candidate_group_size=50, parent_score=float(-rank), source_id='source',
                    **{n:rank/100 for n in core.FEATURES}, label_information_end=d+pd.offsets.BDay(5),
                    valuation_status='AVAILABLE', hypothetical_liquidation_net_bps=y, observed_gap_bps=0.,
                    valuation_gross_terminal_ratio=(1+y/10000)*(1+POLICY['buy_bps']/10000)/(1-POLICY['sell_bps']/10000),
                    exit_execution_status='NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF', valuation_policy_sha256=core.base.VALUATION_POLICY_SHA256))
    return pd.DataFrame(rows)


def sequence():
    slots = pd.date_range('2025-01-02 09:31', periods=20, freq='min')
    close = 10*np.exp(np.cumsum(np.sin(np.arange(20)*1.3)*.001))
    return slots, close, np.arange(1., 21.)


def test_joint_weighted_formula_common_mask_and_invariance():
    s, p, v = sequence()
    r = core.conditional(s, p, v, day=s[0])
    a, b, w = np.diff(np.log(p))[:-1], np.diff(np.log(p))[1:], v[1:-1]
    cov = np.cov(np.array([a, b]), aweights=w, ddof=0)
    expected = cov[0, 1]/np.sqrt(cov[0, 0]*cov[1, 1])-np.corrcoef(a, b)[0, 1]
    assert r['volume_conditioned_serial_delta'] == pytest.approx(expected)
    assert core.conditional(s, p, v*1e200, day=s[0])['volume_conditioned_serial_delta'] == pytest.approx(expected)
    assert core.conditional(s, p, np.ones(20), day=s[0])['volume_conditioned_serial_delta'] == pytest.approx(0)
    v[5] = np.nan
    r = core.conditional(s, p, v, day=s[0])
    mask = np.isfinite(v[1:-1])
    assert r['joint_pair_correlation'] == pytest.approx(np.corrcoef(a[mask], b[mask])[0, 1])
    assert r['joint_pairs'] == 17 and r['joint_triple_coverage'] == pytest.approx(17/18)


@pytest.mark.parametrize('kind,reason', [('zero', 'ZERO_OBSERVED_VOLUME'), ('constant', 'CONSTANT_RETURN_VARIANCE'),
    ('sparse', 'INSUFFICIENT_JOINT_TRIPLE_COVERAGE'), ('one_weight', 'ZERO_WEIGHTED_RETURN_VARIANCE')])
def test_unknown_is_not_zero_or_deleted(kind, reason):
    s, p, v = sequence()
    if kind == 'zero':
        v[:] = 0
    elif kind == 'constant':
        p[:] = 10
    elif kind == 'sparse':
        p[5:10] = np.nan
    else:
        v[:] = 0
        v[5] = 1
    r = core.conditional(s, p, v, day=s[0])
    assert r['conditional_reason'] == reason and pd.isna(r['volume_conditioned_serial_delta'])


def test_clock_gaps_lunch_future_and_invalid_volume():
    s, p, v = sequence()
    s = s[:10].append(pd.date_range('2025-01-02 13:01', periods=10, freq='min'))
    assert core.conditional(s, p, v, day=s[0])['joint_possible'] == 16
    with pytest.raises(ValueError, match='calendar/clock'):
        core.conditional(s, p, v, day='2025-01-03')
    for bad in (-1., np.inf):
        v[3] = bad
        with pytest.raises(ValueError, match='volume'):
            core.conditional(s, p, v, day=s[0])


def test_maturity_projection_before_poison_and_json_predict():
    rows = population()
    rows.loc[:, core.FEATURES[0]] = np.nan
    for name in (core.NEW[0], 'hypothetical_liquidation_net_bps', 'label_information_end'):
        rows[name] = rows[name].astype(object)
        rows.loc[rows[core.base.KEY[0]].gt('2025-12-31'), name] = 'FUTURE_POISON'
    calls = []
    fitted = core.fit(rows, '2025-12-31', 'candidate', before_fit=lambda:calls.append(1))
    assert calls == [1] and fitted['train_rows'] == 200 and core.FEATURES[0] in fitted['inactive_fields']
    targets = population(('2026-01-02',))
    targets['hypothetical_liquidation_net_bps'] = 'UNREADABLE'
    targets.loc[0, core.NEW[0]] = np.nan
    p = core.predict(targets, json.loads(json.dumps(fitted)))
    assert p.loc[0, 'advisory_rank'] == 1 and p.loc[0, 'no_rank_advice']
    assert p.groupby(core.GROUP).selected.sum().eq(10).all()
    assert p.loc[:, core.IDENTITY].equals(targets.loc[:, core.IDENTITY])
    flat = deepcopy(fitted)
    flat['coef'] = [0.]*len(flat['coef'])
    assert core.predict(targets, flat).advisory_rank.equals(targets.selection_effective_rank)


def test_training_rejects_contradictions_cash_and_population():
    rows = population()
    rows.loc[0, ['valuation_status', 'hypothetical_liquidation_net_bps']] = ['CASH_ENTRY_NOT_EXECUTABLE', 1.]
    with pytest.raises(ValueError, match='cash label'):
        core.training(rows, '2025-12-31')
    rows = population()
    rows.loc[0, 'instrument'] = rows.loc[1, 'instrument']
    with pytest.raises(ValueError, match='duplicate'):
        core.validate_population(rows)
    with pytest.raises(ValueError, match='Top50'):
        core.validate_population(population().iloc[1:])


def test_attach_preserves_unknown_daily_fields_and_original_order():
    rows = population()
    source = rows.loc[:, [*core.IDENTITY, 'parent_score', 'source_id', *core.old.DAILY[:6]]]
    minute = rows.loc[:, ['instrument', core.base.KEY[0], *[f for f in core.FEATURES if f not in (*core.old.DAILY, 'native_score_z', 'native_rank_fraction')]]].drop_duplicates(['instrument', core.base.KEY[0]])
    attached, missing = core.attach(source, minute)
    assert set(missing) == set(core.old.DAILY[6:]) and attached[missing].isna().all().all()
    pd.testing.assert_frame_equal(attached.loc[:, source.columns], source)
    with pytest.raises(ValueError, match='population differs'):
        core.attach(source, minute.iloc[1:])


def test_population_economic_unknown_policy_and_joint_support():
    rows = population()
    fitted = core.fit(rows, '2025-12-31', 'candidate')
    p = core.predict(rows, fitted)
    finance = rows.loc[:, [*core.KEY, *FINANCE]].copy()
    finance.loc[0, ['valuation_status', 'hypothetical_liquidation_net_bps']] = ['UNKNOWN_TEST', np.nan]
    cohorts = core.panel(p, p.copy(), finance)
    assert len(cohorts) == 6 and cohorts.iloc[0].candidate_unknown == 1
    report, _ = core.summarize(cohorts)
    assert report['statistics']['declared_days'] == 3 and report['matched_intervention_days'] == 0
    changed = p.copy()
    changed.loc[0, 'no_rank_advice'] = True
    with pytest.raises(ValueError, match='common support'):
        core.panel(p, changed, finance)
    finance.loc[0, 'valuation_policy_sha256'] = 'drift'
    with pytest.raises(ValueError, match='identity mismatch'):
        core.panel(p, p.copy(), finance)


def test_inner_gate_requires_new_information_not_only_native_gain():
    rows = []
    for fold, start in enumerate(('2025-04-01', '2025-06-02', '2025-08-01'), 1):
        for d in pd.bdate_range(start, periods=12):
            for p in ('a', 'b'):
                rows.append(dict(package_id=p, decision_date=d, candidate=30., baseline=10., candidate_full_model=31.,
                    mature=True, intervention_count=2, matched_intervention_count=2, candidate_unknown=0, baseline_unknown=0,
                    original_candidates=50, delta=20., fold=fold))
    panel = pd.DataFrame(rows)
    assert core.summarize(panel)[0]['classification'] == 'INNER_NOT_SUPPORTED'
    panel.candidate_full_model = 20.
    assert core.summarize(panel)[0]['classification'] == 'INNER_SUPPORTED'
    panel.matched_intervention_count = 0
    assert core.summarize(panel)[0]['classification'] == 'INNER_NOT_SUPPORTED'


def test_financial_projection_pushes_h_filter_before_decode(tmp_path):
    rows = population()
    path = tmp_path/'prepared'
    path.mkdir()
    finance = rows[[*core.KEY, *FINANCE]].copy()
    finance['hypothetical_liquidation_net_bps'] = finance.hypothetical_liquidation_net_bps.astype(str)
    future = finance[core.base.KEY[0]].eq('2026-01-02')
    finance.loc[future, 'hypothetical_liquidation_net_bps'] = 'FUTURE_POISON'
    finance.loc[future, 'label_information_end'] = pd.Timestamp('2026-01-30')
    finance.to_parquet(path/'train_finance.parquet')
    fake = SimpleNamespace(source=SimpleNamespace(root=tmp_path))
    projected = pipe.Study.finance(fake, '2025-01-02', '2026-01-02', cutoff='2026-01-10')
    assert projected.loc[future, 'valuation_status'].eq('IMMATURE').all()
    assert projected.loc[future, 'hypothetical_liquidation_net_bps'].isna().all()
    with pytest.raises(ValueError, match='outside'):
        pipe.Study.finance(fake, '2026-08-31', '2026-09-02', cutoff='2026-09-02')


@pytest.fixture
def staged(tmp_path, monkeypatch):
    source_root = tmp_path/'source'
    (source_root/'prepared').mkdir(parents=True)
    (source_root/'evaluated').mkdir()
    days = ['2025-01-02', '2025-01-03', '2025-04-01', '2025-04-02', '2025-06-02', '2025-06-03',
        '2025-08-01', '2025-08-04', '2025-10-08', '2025-10-09']
    rows = population(days)
    columns = [*core.IDENTITY, 'parent_score', 'source_id', *core.old.DAILY[:6]]
    rows[columns].to_parquet(source_root/'prepared/inputs.parquet')
    for outer, path in ((False, 'prepared/train_finance.parquet'), (True, 'evaluated/finance.parquet')):
        rows.loc[rows[core.base.KEY[0]].gt(core.CONTRACT['train_end']).eq(outer), [*core.KEY, *FINANCE]].to_parquet(source_root/path)
    source = SimpleNamespace(root=source_root, plan=dict(binding={'binding_hash':'a'*64}, experiment_id='original', node_python_uri=str(Path(pipe.sys.executable))))
    source.read = lambda stage:'b'*64
    monkeypatch.setattr(pipe.previous, 'Study', lambda root:source)
    monkeypatch.setattr(pipe.Study, 'node', lambda self:None)
    def minute(plan, inputs, budget, progress):
        part = rows.merge(inputs[core.KEY], on=core.KEY)
        keys = ['instrument', core.base.KEY[0]]
        fields = [f for f in core.FEATURES if f not in (*core.old.DAILY, 'native_score_z', 'native_rank_fraction')]
        result = part[[*keys, *fields]].drop_duplicates(keys)
        result['conditional_reason'] = 'AVAILABLE'
        return result, {'sql_count':0}
    monkeypatch.setattr(pipe, 'read_minutes', minute)
    return pipe.preregister(tmp_path/'output', source_root, 'c'*40)


@pytest.mark.parametrize('outer', [False, True])
def test_real_stages_fit_budget_barrier_retry_and_drift(staged, monkeypatch, outer):
    original_summary = core.summarize
    def summary(panel):
        result, daily = original_summary(panel)
        if 'fold' in panel:
            result.update(proceed_outer=outer, classification='INNER_SUPPORTED' if outer else 'INNER_NOT_SUPPORTED')
        return result, daily
    monkeypatch.setattr(core, 'summarize', summary)
    original_finance = pipe.Study.finance
    def finance(self, *args, **kwargs):
        if kwargs.get('outer'):
            assert outer and (self.root/'final_candidate/trained/manifest.json').exists()
            assert (self.root/'final_matched/trained/manifest.json').exists()
        return original_finance(self, *args, **kwargs)
    monkeypatch.setattr(pipe.Study, 'finance', finance)
    result = pipe.run(staged)
    assert result['physical_fit_count'] == (8 if outer else 6) and result['outer_finance_decoded'] is outer
    assert not result['sealed_finance_decoded'] and not result['activation_evidence']
    def no_fit(*args, **kwargs):
        raise AssertionError('retry must not fit')
    monkeypatch.setattr(core, 'fit', no_fit)
    assert pipe.run(staged)['exact_retry']
    path = staged/'fold1_matched/trained/model.json'
    path.write_bytes(path.read_bytes()+b' ')
    with pytest.raises(AdvisoryModelFirstError, match='hash mismatch'):
        pipe.run(staged)


def test_unpublished_fit_cannot_retry(staged):
    study = pipe.Study(staged)
    study.claim('fold1_matched')
    with pytest.raises(ValueError, match='no automatic refit'):
        pipe.run(staged)


def test_publication_completion_boundary_resumes_without_fit(staged, monkeypatch):
    pipe.run(staged)
    done = staged/'fit_claims/fold1_matched_finished.json'
    done.unlink()  # Synthetic own receipt: emulate interruption after model publication.
    monkeypatch.setattr(core, 'fit', lambda *a, **k:pytest.fail('refit'))
    assert pipe.run(staged)['exact_retry'] and done.exists()


@pytest.mark.parametrize('drift', [False, True])
def test_batch_reader_exact_day_streams_and_drift(tmp_path, monkeypatch, drift):
    s, p, v = sequence()
    calendar = tmp_path/'calendar.txt'
    calendar.write_text('\n'.join(str(d) for d in s), encoding='utf-8')
    meta = tmp_path/'meta.json'
    meta.write_text('{}', encoding='utf-8')
    lease = {}
    for rank in range(1, 51):
        for field in core.old.FIELDS:
            relative = f'features/{rank:06}.sz/{field}.1min.bin'
            path = tmp_path/relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(np.r_[0., v if field == 'volume' else p].astype('<f4').tobytes())
            lease[relative] = pipe.stamp(path)
    closed = []
    fake = SimpleNamespace(root=tmp_path, _bin_lease=lease, close=lambda:closed.append(True), assert_unchanged=lambda:None,
        file=lambda key:calendar if key == 'minute_calendar' else meta)
    monkeypatch.setattr(pipe, 'file_dataset', lambda plan:fake)
    original = core.minute_features
    def aggregate(*args, **kwargs):
        result = original(*args, **kwargs)
        if drift:
            path = tmp_path/'features/000001.sz/close.1min.bin'
            path.write_bytes(path.read_bytes()+b'xxxx')
        return result
    monkeypatch.setattr(core, 'minute_features', aggregate)
    inputs = population(('2025-01-02',))
    if drift:
        with pytest.raises(ValueError, match='changed during read'):
            pipe.read_minutes({'binding':{'binding_hash':'a'*64}}, inputs, lambda:None, lambda e:None)
    else:
        frame, receipt = pipe.read_minutes({'binding':{'binding_hash':'a'*64}}, inputs, lambda:None, lambda e:None)
        assert len(frame) == 50 and len(receipt['files']) == 250
        assert receipt['consumed_bin_bytes'] == 250*21*4 and receipt['sql_count'] == 0
        assert frame[core.NEW[0]].notna().all()
        assert frame.conditional_reason.eq('UNKNOWN_CLOSING_RETURN_ENDPOINT').all()  # Morning-only fixture.
    assert closed == [True]
