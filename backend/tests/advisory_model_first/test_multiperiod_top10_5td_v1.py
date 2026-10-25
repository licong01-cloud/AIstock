"""Direct contracts for the bounded study; these fits are not research trials."""
from copy import deepcopy
from datetime import date
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY, POLICY
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import (
    FINANCE, INPUT_FEATURES, build_features, make_finance, read_daily,
)
from backend.services.advisory_model_first import multiperiod_top10_5td_pipeline_v1 as pipeline
from backend.services.advisory_model_first import multiperiod_top10_5td_study_v1 as study
from backend.services.advisory_model_first import multiperiod_top10_5td_inputs_v1 as inputs_module
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


@pytest.fixture
def rows():
    records = []
    for d in pd.to_datetime(['2025-01-02', '2025-01-03', '2026-01-02']):
        for package in ('a', 'b'):
            for rank in range(1, 21):
                y = float(100-rank)
                records.append(dict(package_id=package, manifest_sha256='a'*64, run_id='run_'+package,
                    decision_as_of_trade_date=d, target_trade_date=d+pd.offsets.BDay(1), instrument=f'{rank:06}.SZ',
                    selection_effective_rank=rank, candidate_group_size=20, parent_score=float(-rank), source_id='source',
                    **{n: float(rank/100) for n in INPUT_FEATURES},
                    label_information_end=d+pd.offsets.BDay(5), valuation_status='AVAILABLE',
                    hypothetical_liquidation_net_bps=y, observed_gap_bps=0.,
                    valuation_gross_terminal_ratio=(1+y/10000)*(1+POLICY['buy_bps']/10000)/(1-POLICY['sell_bps']/10000),
                    exit_execution_status='NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF', valuation_policy_sha256=VALUATION_POLICY_SHA256))
    return pd.DataFrame(records)


class FlatModel:
    def predict(self, x):
        return np.zeros(len(x))


def test_clock_projection_does_not_decode_future_poison_and_purges_h(rows):
    future = rows[KEY[0]].gt('2025-12-31')
    rows[[INPUT_FEATURES[0], 'hypothetical_liquidation_net_bps']] = rows[[INPUT_FEATURES[0], 'hypothetical_liquidation_net_bps']].astype(object)
    rows['label_information_end'] = rows.label_information_end.astype(object)
    rows.loc[future, INPUT_FEATURES[0]] = 'FUTURE_POISON'
    rows.loc[future, 'hypothetical_liquidation_net_bps'] = 'FUTURE_POISON'
    rows.loc[future, 'label_information_end'] = 'FUTURE_POISON'
    rows.loc[rows[KEY[0]].eq('2025-01-03'), 'label_information_end'] = pd.Timestamp('2026-01-01')
    # Only one mature D is insufficient; clock validation must happen before parsing poison.
    with pytest.raises(ValueError, match='insufficient mature'):
        study.training_population(rows, '2025-12-31')
    rows.loc[rows[KEY[0]].eq('2025-01-03'), 'label_information_end'] = pd.Timestamp('2025-01-10')
    part, target, weight = study.training_population(rows, '2025-12-31')
    assert len(part) == 80 and part[KEY[0]].max().year == 2025
    assert np.isfinite(target).all()
    assert pd.Series(weight, index=part[KEY[0]]).groupby(level=0).sum().nunique() == 1


def test_duplicate_stock_day_finance_is_not_independent_or_contradictory(rows):
    bad = rows.index[rows.package_id.eq('b')][0]
    rows.loc[bad, INPUT_FEATURES[0]] += .01
    with pytest.raises(ValueError, match='contradictory shared finance'):
        study.training_population(rows, '2025-12-31')


def test_encoding_inactive_column_and_unknown_are_not_future_refit(rows):
    train = rows.iloc[:20].copy()
    train[INPUT_FEATURES[0]] = np.nan
    enc = study.fit_encoding(train)
    future = train.copy()
    future[INPUT_FEATURES[0]] = 999.
    x, known = study.encode(future, enc)
    assert np.all(x[:, 0] == 0) and np.all(x[:, len(INPUT_FEATURES)] == 0) and known.all()
    unknown = train.copy()
    unknown.loc[:, INPUT_FEATURES] = np.nan
    prediction = study.predict(unknown, FlatModel(), enc)
    assert prediction.no_rank_advice.all()
    assert prediction.advisory_rank.equals(unknown.selection_effective_rank)
    assert prediction.selected.sum() == 10


def test_predict_never_reads_y_and_preserves_native_ties(rows):
    enc = study.fit_encoding(rows)
    original = rows.copy(deep=True)
    rows['hypothetical_liquidation_net_bps'] = 'UNREADABLE_OUTER_Y'
    p = study.predict(rows, FlatModel(), enc)
    assert p.advisory_rank.equals(original.selection_effective_rank)
    assert p.groupby(['package_id', KEY[0]]).selected.sum().eq(10).all()
    assert original.selection_effective_rank.equals(rows.selection_effective_rank)


def test_unknown_finance_keeps_original_day_and_slots(rows):
    p = study.predict(rows, FlatModel(), study.fit_encoding(rows))
    rows.loc[0, ['valuation_status', 'hypothetical_liquidation_net_bps']] = ['UNKNOWN_TRADED_PRICE_OR_COORDINATE', np.nan]
    panel = study.cohort(p, rows[['package_id', *KEY, *FINANCE]])
    assert len(panel) == 6
    assert panel.iloc[0].candidate_unknown == 1 and pd.isna(panel.iloc[0].candidate)
    with pytest.raises(ValueError, match='populations differ'):
        study.cohort(p.iloc[1:], rows[['package_id', *KEY, *FINANCE]])


def test_cost_and_policy_identity_fail_closed(rows):
    p = study.predict(rows, FlatModel(), study.fit_encoding(rows))
    rows.loc[0, 'valuation_policy_sha256'] = 'b'*64
    with pytest.raises(ValueError, match='identity mismatch'):
        study.cohort(p, rows[['package_id', *KEY, *FINANCE]])
    rows.loc[0, 'valuation_policy_sha256'] = VALUATION_POLICY_SHA256
    rows.loc[0, 'hypothetical_liquidation_net_bps'] += 10
    with pytest.raises(ValueError, match='cost/coordinate mismatch'):
        study.cohort(p, rows[['package_id', *KEY, *FINANCE]])


def test_paired_bootstrap_retains_holes_not_stock_iid():
    interval = study.paired_interval([10., np.nan]*20)
    assert interval['declared_days'] == 40 and interval['paired_days'] == 20
    assert interval['ci95_bps'] == [10., 10.] and interval['mde80_bps'] == 0
    assert interval['conservative_blocks'] == 2


@pytest.mark.parametrize('mean,ci,tail,days,fraction,blocks,coverage,excess,expected', [
    (11., [1., 20.], 0., 30, .3, 2, .9, .01, 'WORTH_INDEPENDENT_CONFIRMATION'),
    (11., [-1., 20.], 0., 30, .3, 2, .9, .01, 'INCONCLUSIVE'),
    (11., [1., 20.], 0., 5, .05, 1, .9, .01, 'INCONCLUSIVE'),
    (-1., [-20., 20.], 0., 30, .3, 2, .9, .01, 'NOT_WORTH_CONTINUING_THIS_CANDIDATE'),
    (11., [1., 20.], -101., 30, .3, 2, .9, .01, 'NOT_WORTH_CONTINUING_THIS_CANDIDATE'),
])
def test_preregistered_classification(mean, ci, tail, days, fraction, blocks, coverage, excess, expected):
    assert study.classify(dict(mean_bps=mean, ci95_bps=ci), tail_delta=tail, intervention_days=days,
        intervention_fraction=fraction, intervention_blocks=blocks, coverage=coverage, unknown_excess=excess) == expected


def test_file_projection_restores_native_li_prices_and_raw_shares():
    class Dataset:
        def h5(self, *args):
            index = pd.MultiIndex.from_tuples([(pd.Timestamp('2025-01-02'), '000001.SZ')], names=['datetime', 'instrument'])
            return pd.DataFrame(dict(open=[20.], high=[22.], low=[18.], close=[20.], factor=[2.], volume=[500.]), index=index)
    result = read_daily(Dataset(), ['000001.SZ'], date(2025, 1, 2), date(2025, 1, 2))
    assert result.raw_close_cny.iloc[0] == 10. and result.volume_hand.iloc[0] == 10.


def test_file_features_use_only_d_history_not_later_daily_prices(rows):
    roster = rows.iloc[:20].copy()
    d = roster[KEY[0]].iloc[0]
    calendar = pd.bdate_range(d-pd.offsets.BDay(19), d+pd.offsets.BDay(7)).date.tolist()
    daily = pd.DataFrame([dict(trade_date=pd.Timestamp(day), instrument=s, adj_factor=1., volume_hand=100.,
        raw_open_cny=10., raw_high_cny=11., raw_low_cny=9., raw_close_cny=10.) for day in calendar for s in roster.instrument])
    expected = build_features(roster, daily, calendar)
    daily['raw_close_cny'] = daily.raw_close_cny.astype(object)
    daily.loc[daily.trade_date.gt(d), 'raw_close_cny'] = 'FUTURE_UNREADABLE'
    actual = build_features(roster, daily, calendar)
    pd.testing.assert_frame_equal(expected, actual)


@pytest.mark.parametrize('kind,expected', [('normal', 'AVAILABLE'), ('suspended', 'CASH_ENTRY_NOT_EXECUTABLE'),
    ('missing', 'UNKNOWN'), ('immature', 'IMMATURE')])
def test_valuation_normal_suspend_missing_and_immature(rows, kind, expected):
    roster = rows.iloc[:20].copy()
    d = roster[KEY[0]].iloc[0]
    calendar = pd.bdate_range(d, periods=7).date.tolist()
    cutoff = pd.Timestamp(calendar[3 if kind == 'immature' else 5])
    daily = pd.DataFrame([dict(trade_date=pd.Timestamp(day), instrument=s, adj_factor=1., volume_hand=100.,
        raw_open_cny=10., raw_high_cny=11., raw_low_cny=9., raw_close_cny=10.) for day in calendar if pd.Timestamp(day) <= cutoff for s in roster.instrument])
    limits = daily[['trade_date', 'instrument']].assign(up_limit=12., down_limit=8.)
    suspension = []
    if kind == 'suspended':
        suspension = [(s, calendar[1], 'S', None) for s in roster.instrument]
        daily = daily.loc[daily.trade_date.ne(pd.Timestamp(calendar[1]))]
    if kind == 'missing':
        daily = daily.loc[daily.trade_date.ne(pd.Timestamp(calendar[2]))]
    f = make_finance(roster, daily, limits, suspension, calendar, cutoff)
    assert f.valuation_status.eq(expected).all() and len(f) == 20
    if kind == 'suspended':
        assert f.hypothetical_liquidation_net_bps.eq(0).all()


def _plan():
    p = dict(contract=deepcopy(study.CONTRACT), implementation_sha256=pipeline.implementation_sha256(),
        start=study.CONTRACT['start'], end=study.CONTRACT['end'], source_commit='a'*40,
        sources=[dict(package_id='a', run_id='ra'), dict(package_id='b', run_id='rb')],
        binding={'binding_hash': 'b'*64})
    p = json.loads(json.dumps(p))
    p['experiment_id'] = 'advmultitop10_'+sha(p)[:24]
    return p


def test_stage_integrity_exact_retry_and_interrupted_claim(tmp_path, monkeypatch):
    # Windows CI/local --basetemp must be X. Linux test temp is not a research output.
    monkeypatch.setattr(pipeline, 'node_path', lambda value: Path(value))
    monkeypatch.setattr(pipeline.Study, '__init__', _test_init)
    s = pipeline.Study(tmp_path, _plan())
    s.publish('preregistered', {'plan.json': pipeline._json_bytes(s.plan)})
    s.publish('prepared', {'inputs.json': b'{"known":true}'})
    before = s.read('prepared')
    assert before == s.read('prepared')
    s.fit_event('ridge_1.0_fold1')
    with pytest.raises(FileExistsError):
        s.fit_event('ridge_1.0_fold1')
    (tmp_path/'prepared/inputs.json').write_bytes(b'{}')
    with pytest.raises(Exception, match='hash mismatch'):
        s.read('prepared')


def _test_init(self, root, plan=None):
    import time
    self.root, self.plan = Path(root), plan
    self.digest, self.started, self.peak = sha(plan), time.monotonic(), 0


def test_claim_cap_and_environment_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline.Study, '__init__', _test_init)
    s = pipeline.Study(tmp_path, _plan())
    for i in range(19):
        s.fit_event('test_'+str(i))
    with pytest.raises(ValueError, match='budget exhausted'):
        s.fit_event('test_20')
    monkeypatch.setattr(pipeline.os, 'name', 'nt')
    with pytest.raises(ValueError, match='existing WSL'):
        s.node()


def test_selection_budget_ties_and_outer_poison_are_predeclared(rows, monkeypatch):
    original = rows.iloc[:40].copy()
    blocks = []
    for d in pd.to_datetime(['2025-04-01', '2025-06-02', '2025-08-01', '2026-01-02']):
        block = original.copy()
        block[KEY[0]], block[KEY[1]], block['label_information_end'] = d, d+pd.offsets.BDay(1), d+pd.offsets.BDay(5)
        blocks.append(block)
    inputs = pd.concat(blocks, ignore_index=True)
    inputs[INPUT_FEATURES[0]] = inputs[INPUT_FEATURES[0]].astype(object)
    inputs.loc[inputs[KEY[0]].gt('2025-09-30'), INPUT_FEATURES[0]] = 'OUTER_POISON'
    calls = []
    def fit(frame, end, config, before_fit, after_fit):
        assert frame[KEY[0]].max() <= pd.Timestamp('2025-09-30')
        before_fit()
        calls.append((config['id'], end))
        after_fit()
        return FlatModel(), study.fit_encoding(frame)
    monkeypatch.setattr(study, 'fit_model', fit)
    selected, predictions, panel = study.select_configuration(inputs)
    assert len(calls) == 18 and selected['config']['id'] == study.CONFIGS[0]['id']
    assert predictions[KEY[0]].max() <= pd.Timestamp('2025-09-30') and len(panel) == 36


@pytest.mark.parametrize('config', [study.CONFIGS[0], study.CONFIGS[3]])
def test_registered_model_families_fit_and_predict_finite_without_outer_labels(rows, config):
    events = []
    model, encoding = study.fit_model(rows, '2025-12-31', config,
        before_fit=lambda: events.append('STARTED'), after_fit=lambda: events.append('COMPLETED'))
    prediction = study.predict(rows, model, encoding)
    assert events == ['STARTED', 'COMPLETED']
    assert np.isfinite(prediction.predicted_relative_net_bps).all()
    assert prediction.groupby(['package_id', KEY[0]]).selected.sum().eq(10).all()


def test_modified_ranks_fail_closed_and_attribution_matches_primary(rows):
    p = study.predict(rows, FlatModel(), study.fit_encoding(rows))
    p.loc[0, 'advisory_rank'] = 2
    with pytest.raises(ValueError, match='derived ranks'):
        study.cohort(p, rows[['package_id', *KEY, *FINANCE]])
    p = study.predict(rows, FlatModel(), study.fit_encoding(rows))
    result, _, _ = study.evaluate(p, rows[['package_id', *KEY, *FINANCE]])
    attribution = result['avoided_loss_bps']-result['missed_profit_bps']+result['added_profit_bps']-result['added_loss_bps']
    assert attribution == result['statistics']['mean_bps'] == 0.


@pytest.mark.parametrize('cutoff_index,known_count', [(106, 202), (100, 192)])
def test_finance_batches_preserve_package_population_beyond_first_hundred_days(rows, cutoff_index, known_count):
    calendar = pd.bdate_range('2025-01-02', periods=107).date.tolist()
    original = rows.iloc[:2].copy()
    original['candidate_group_size'] = 2
    roster = pd.concat([original.assign(decision_as_of_trade_date=pd.Timestamp(day),
        target_trade_date=pd.Timestamp(calendar[i+1])) for i, day in enumerate(calendar[:101])], ignore_index=True)
    daily = pd.DataFrame([dict(trade_date=pd.Timestamp(day), instrument=s, adj_factor=1., volume_hand=100.,
        raw_open_cny=10., raw_high_cny=11., raw_low_cny=9., raw_close_cny=10.) for day in calendar for s in original.instrument])
    limits = daily[['trade_date', 'instrument']].assign(up_limit=12., down_limit=8.)
    result = make_finance(roster, daily, limits, [], calendar, calendar[cutoff_index])
    assert len(result) == 202 and result[KEY[0]].nunique() == 101
    assert result.valuation_status.eq('AVAILABLE').sum() == known_count
    assert result.valuation_status.eq('IMMATURE').sum() == 202-known_count


def test_finance_reuses_cached_daily_and_projects_cutoff_before_future_values(rows, monkeypatch):
    roster = rows.iloc[:2].copy()
    roster['candidate_group_size'] = 2
    calendar = pd.bdate_range('2025-01-02', periods=8).date.tolist()
    daily = pd.DataFrame([dict(trade_date=pd.Timestamp(day), instrument=s, adj_factor=1., volume_hand=100.,
        raw_open_cny=10., raw_high_cny=11., raw_low_cny=9., raw_close_cny=10.) for day in calendar for s in roster.instrument])
    limits = daily[['trade_date', 'instrument']].assign(up_limit=12., down_limit=8.)
    daily['raw_close_cny'] = daily.raw_close_cny.astype(object)
    daily.loc[daily.trade_date.gt(pd.Timestamp(calendar[5])), 'raw_close_cny'] = 'FUTURE_UNREADABLE'
    class Dataset:
        read_receipts = []
        def suspension_rows(self, *args):
            return []
    dataset = Dataset()
    dataset.calendar = calendar
    def forbidden_read(*args):
        raise AssertionError('second H5 scan forbidden with existing cached daily data')
    monkeypatch.setattr(inputs_module, 'read_daily', forbidden_read)
    monkeypatch.setattr(inputs_module, 'read_limits', lambda *args: (limits, []))
    result, _ = inputs_module.finance_inputs(dataset, roster, calendar[5], daily=daily)
    assert len(result) == 2 and result.valuation_status.eq('AVAILABLE').all()
