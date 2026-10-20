"""Focused leakage, population and economic contracts; not alpha evidence."""
import base64
import json
from types import SimpleNamespace
import zlib

import numpy as np
import pandas as pd
import pytest

from backend.data_service.frozen_dataset import stamp
from backend.services.advisory_model_first import serial_price_pressure_top10_5td_inputs_v1 as source
from backend.services.advisory_model_first import serial_price_pressure_top10_5td_study_v1 as study
from backend.services.advisory_model_first import serial_price_pressure_top10_5td_pipeline_v1 as pipeline
from backend.services.advisory_model_first.generic_minute_price_5td_contracts_v1 import MINUTE_FEATURES
from backend.services.advisory_model_first.generic_volume_path_price_5td_source_v1 import aggregate_d_volume_path_features_v1
from backend.services.advisory_model_first.generic_volume_path_price_5td_contracts_v1 import VOLUME_FEATURES
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def bars(day='2025-05-30'):
    day = pd.Timestamp(day)
    slots = pd.date_range(day+pd.Timedelta('9h30min'), day+pd.Timedelta('11h30min'), freq='min').append(
        pd.date_range(day+pd.Timedelta('13h1min'), day+pd.Timedelta('15h'), freq='min'))
    close = 100+np.sin(np.arange(len(slots)))*.1
    opening = np.r_[100., close[:-1]]
    return slots, dict(open=opening, high=np.maximum(opening, close), low=np.minimum(opening, close), close=close, volume=np.ones(len(slots)))


def roster(days=3):
    records = []
    for day in pd.bdate_range('2025-01-02', periods=days):
        for package in ('a', 'b'):
            for rank in range(1, 21):
                records.append(dict(package_id=package, manifest_sha256=package*64, run_id='run_'+package,
                    decision_as_of_trade_date=day, target_trade_date=day+pd.offsets.BDay(), instrument=f'{rank:06d}.SZ',
                    selection_effective_rank=rank, candidate_group_size=50, parent_score=np.float32(rank/20),
                    **{name: float(rank+day.day/10) for name in source.CANDIDATE}))
    result = pd.DataFrame(records)
    result['parent_score'] = result.parent_score.astype('float32')
    result['csi300_ret_5'] = result.decision_as_of_trade_date.dt.day/100
    return result


def test_old_43_summaries_do_not_determine_serial_structure():
    slots, _ = bars()
    old, serial = [], []
    for peaks in ((101., 102., 103.), (101., 103., 102.)):
        close = np.full(len(slots), 100.)
        at = slots.get_loc(pd.Timestamp('2025-05-30 11:01'))
        close[at:at+6] = [v for p in peaks for v in (p, 100.)]
        op = np.r_[100., close[:-1]]
        arrays = dict(open=op, high=np.maximum(op, close), low=np.minimum(op, close), close=close, volume=np.ones(len(slots)))
        f = source.minute_features(slots, arrays, day='2025-05-30')
        original = aggregate_d_volume_path_features_v1(slots=slots, arrays={**arrays, 'amount': close.copy(),
            'limit_up': np.zeros(len(slots)), 'limit_down': np.zeros(len(slots))})
        old.append([*(original[n] for n in (*MINUTE_FEATURES, *VOLUME_FEATURES)), *(f[n] for n in source.ORDERED_FEATURES)])
        serial.append(f['serial_correlation'])
    assert len(old[0]) == 43 and np.array_equal(old[0], old[1], equal_nan=True)
    assert abs(serial[0]-serial[1]) > .01


def test_serial_clock_missing_flat_bad_values_and_scale():
    slots, arrays = bars()
    close = arrays['close']
    value = source.serial_stat(slots, close, day=slots[0])
    assert value['serial_possible'] == 237
    assert value['serial_correlation'] == pytest.approx(source.serial_stat(slots, close*7, day=slots[0])['serial_correlation'])
    flat = source.serial_stat(slots, np.ones(len(slots)), day=slots[0])
    assert np.isnan(flat['serial_correlation']) and 'VARIANCE_UNDEFINED' in flat['serial_reason']
    changed = close.copy()
    changed[20:100] = np.nan
    missing = source.serial_stat(slots, changed, day=slots[0])
    assert np.isnan(missing['serial_correlation']) and missing['serial_coverage'] < .8
    gap = source.serial_stat(slots.delete(20), np.delete(close, 20), day=slots[0])
    assert gap['serial_possible'] == 234  # The removed clock is not bridged.
    for invalid_slots, invalid_close in ((slots.append(slots[-1:]+pd.Timedelta(days=1)), np.r_[close, 100.]),
            (pd.DatetimeIndex(['2025-05-30 12:00']), np.array([100.])), (slots, np.where(np.arange(len(close)) == 0, np.inf, close))):
        with pytest.raises(ValueError):
            source.serial_stat(invalid_slots, invalid_close, day=slots[0])


def test_native_projection_dtype_and_duplicate_identity():
    rows = roster()
    daily = rows.loc[:, [*source.KEY, *source.DAILY]].copy()
    minute = rows.loc[:, ['instrument', 'decision_as_of_trade_date', *source.OLD_MINUTE,
        'serial_correlation', 'serial_closing_pressure', 'nonzero_return_fraction', 'serial_coverage']].drop_duplicates()
    attached = source.attach_features(rows.loc[:, list(source.ROSTER)], daily, minute)
    assert attached.parent_score.dtype == np.dtype('float32')
    assert attached.loc[:, list(source.ROSTER)].equals(rows.loc[:, list(source.ROSTER)])
    with pytest.raises(ValueError, match='duplicate'):
        source.attach_features(rows, pd.concat([daily, daily.iloc[:1]]), minute)


def test_train_only_encoding_shared_supervision_and_json_prediction(monkeypatch):
    rows = roster()
    rows['valuation_status'] = 'AVAILABLE'
    rows['valuation_policy_sha256'] = VALUATION_POLICY_SHA256
    rows['label_information_end'] = rows.target_trade_date+pd.offsets.BDay(4)
    rows['valuation_gross_terminal_ratio'] = 1.+rows.selection_effective_rank/1000
    rows['observed_gap_bps'] = 0.
    rows['hypothetical_liquidation_net_bps'] = 10000*(rows.valuation_gross_terminal_ratio*(1-5.95/10000)/(1+.95/10000)-1)
    rows['exit_execution_status'] = 'EXECUTABLE'
    rows[source.CANDIDATE[-1]] = np.nan
    calls = []
    class PureContractDouble:
        def __init__(self, **kwargs):
            assert kwargs['fit_intercept'] is False and kwargs['alpha'] == 10.
        def fit(self, x, y, sample_weight):
            calls.append((y.copy(), sample_weight.copy()))
            self.coef_, self.n_iter_ = np.ones(x.shape[1]), np.array([1])
            return self
        def predict(self, x):
            return x @ self.coef_
    monkeypatch.setattr('sklearn.linear_model.Ridge', PureContractDouble)  # No Windows supervised fit.
    fitted = study.train_models(rows, train_start='2025-01-02', train_end='2025-01-20')
    assert len(calls) == 2 and np.array_equal(calls[0][0], calls[1][0]) and np.array_equal(calls[0][1], calls[1][1])
    encoded = study.fit_encoding(study.numeric(rows, source.CANDIDATE))
    assert source.CANDIDATE[-1] in encoded['inactive_fields']
    future = rows.iloc[:1].copy()
    future['decision_as_of_trade_date'] = pd.Timestamp('2025-02-01')
    for column in (*source.CANDIDATE, 'hypothetical_liquidation_net_bps'):
        future[column] = 'POISON'
    changed = study.train_models(pd.concat([rows, future]), train_start='2025-01-02', train_end='2025-01-20')
    assert fitted == changed
    prediction = study.predict(rows, json.loads(json.dumps(fitted)))
    assert prediction.parent_score.dtype == rows.parent_score.dtype
    assert prediction.groupby(study.GROUP).candidate_selected.sum().eq(10).all()
    outside_cutoff = rows.copy()
    outside_cutoff['hypothetical_liquidation_net_bps'] = outside_cutoff.hypothetical_liquidation_net_bps.astype(object)
    outside_cutoff.loc[0, 'label_information_end'] = pd.Timestamp('2025-02-01')
    outside_cutoff.loc[0, 'hypothetical_liquidation_net_bps'] = 'FUTURE_TARGET_POISON'
    original, valid = study.training_population(outside_cutoff, train_start='2025-01-02', train_end='2025-01-20')
    assert len(original) == len(rows) and len(valid) == len(rows)-1


def test_strict_two_swap_ties_unknown_and_native_order():
    rank = np.arange(1, 21)
    unchanged, original = study.choose_top10(np.zeros(20), rank)
    assert np.array_equal(unchanged, rank <= 10) and np.array_equal(original, rank)
    chosen, _ = study.choose_top10(np.arange(20.), rank)
    assert chosen.sum() == 10 and np.sum(chosen & (rank > 10)) == 2
    unknown, _ = study.choose_top10(np.full(20, np.nan), rank)
    assert np.array_equal(unknown, rank <= 10)


def test_economic_common_axis_keeps_all_dates_holes_and_packages():
    inputs = roster(days=86)
    predictions = inputs.loc[:, list(source.ROSTER)].copy()
    for arm in source.ARMS:
        predictions[arm+'_selected'] = inputs.selection_effective_rank.le(10)
        predictions[arm+'_score'] = inputs.selection_effective_rank.astype(float)
    finance = inputs.loc[:, list(source.ROSTER)].copy()
    finance['valuation_status'] = 'AVAILABLE'
    finance['valuation_policy_sha256'] = VALUATION_POLICY_SHA256
    finance['label_information_end'] = finance.target_trade_date+pd.offsets.BDay(4)
    finance['valuation_gross_terminal_ratio'], finance['observed_gap_bps'] = 1.01, 0.
    finance['hypothetical_liquidation_net_bps'] = 10000*(1.01*(1-5.95/10000)/(1+.95/10000)-1)
    finance['exit_execution_status'] = 'EXECUTABLE'
    dates = sorted(inputs.decision_as_of_trade_date.unique())
    finance.loc[finance.package_id.eq('a') & finance.decision_as_of_trade_date.eq(dates[40]), 'valuation_status'] = 'UNKNOWN_TEST'
    days = inputs[study.GROUP].drop_duplicates().rename(columns={study.GROUP[1]: 'decision_date'})
    result = study.evaluate(inputs, finance, days, predictions, evaluation_end=dates[-1])
    assert result['original_days'] == 86 and result['original_package_days'] == 172
    assert result['daily'][40]['candidate_minus_native'] is None and not result['activation']
    assert result['common_complete_days'] == 80 and result['outcome'] == 'STOP_THIS_CANDIDATE'
    sparse = np.tile([1., np.nan], 43)
    assert study.paired_interval(sparse)['ci'] is None


def test_batch_file_identity_future_isolation_and_immutable_stage(tmp_path, monkeypatch):
    slots, arrays = bars()
    calendar = tmp_path/'calendar.txt'
    calendar.write_text('\n'.join(str(x) for x in slots.append(slots+pd.Timedelta(days=1))), encoding='utf-8')
    meta = tmp_path/'minute/meta.json'
    meta.parent.mkdir()
    meta.write_text('{}')
    stock = meta.parent/'features/000001.sz'
    stock.mkdir(parents=True)
    entries = []
    for field, values in arrays.items():
        path = stock/f'{field}.1min.bin'
        path.write_bytes(np.asarray([0., *values, *np.full(len(slots), np.inf)], dtype='<f4').tobytes())
        entries.append([path.relative_to(tmp_path).as_posix(), *stamp(path)])
    binding = dict(binding_hash='f'*64, bin_stat_lease=dict(zlib_base64=base64.b64encode(zlib.compress(json.dumps(entries).encode())).decode()))
    fake = SimpleNamespace(root=tmp_path, file=lambda k: calendar if k == 'minute_calendar' else meta, assert_unchanged=lambda: None, close=lambda: None)
    monkeypatch.setattr(source, 'FrozenDataset', lambda *a, **kw: fake)
    rows = pd.DataFrame([dict(instrument='000001.SZ', decision_as_of_trade_date=slots[0].normalize())]*2)
    features, receipt = source.read_minute_inputs(binding, rows, run_id='test')
    assert len(features) == 1 and receipt['consumed_bin_bytes'] == 5*(4+4*len(slots))
    assert np.isfinite(features.serial_correlation.iloc[0])  # Future poison never decoded.
    with (stock/'close.1min.bin').open('ab') as stream:
        stream.write(b'bad')
    with pytest.raises(ValueError, match='lease'):
        source.read_minute_inputs(binding, rows, run_id='test')
    root = tmp_path/'study'
    plan = dict(policy_sha256=source.POLICY_SHA256, implementation_sha256=pipeline.implementation_sha256())
    publish_stage(study_root=root, stage='preregistered', plan_sha256=sha(plan), parent_sha256=None, artifacts={'plan.json': _json_bytes(plan)})
    assert pipeline.Study(root).read('preregistered', None)
    with pytest.raises(AdvisoryModelFirstError, match='conflicts'):
        publish_stage(study_root=root, stage='preregistered', plan_sha256=sha(plan), parent_sha256=None, artifacts={'plan.json': b'{"changed":true}'})
    monkeypatch.setattr(pipeline, 'implementation_sha256', lambda: 'drift')
    with pytest.raises(ValueError, match='drift'):
        pipeline.Study(root)
