"""Original-population, causal encoding and one-candidate execution contracts."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first import lstm_overnight_increment_v1 as core
from backend.tests.advisory_model_first.test_lstm_factor_nonlinear_v1 import inputs as base_inputs  # noqa: F401
from scripts import advisory_lstm_overnight_increment as runner


@pytest.fixture
def inputs(base_inputs):  # noqa: F811
    frame = base_inputs.copy()
    frame[core.NEW] = np.random.default_rng(1272).normal(0, .02, len(frame))
    frame.loc[0, core.NEW] = np.nan
    return frame


def projection(frame):
    output = frame.loc[:, [*core.KEY, 'manifest_sha256', 'selection_effective_rank']].copy()
    output['catalog_id'], output['signal_name'] = 1272, core.SIGNAL
    output['signal_value'] = frame[core.NEW].to_numpy()
    output['signal_status'] = np.where(frame[core.NEW].isna(), core.MISSING[0], 'AVAILABLE_NATIVE_RATIO')
    output['calculation_visible_through'] = output[core.DAY]
    output['declared_next_session_consumption'] = output.target_trade_date
    output['historical_first_availability'] = 'UNKNOWN_NON_VINTAGE'
    return output


def control_model(frame, fold=1):
    cutoff = core.info.spo.base.FOLDS[fold - 1][0]
    rows = frame.loc[frame[core.DAY].le(cutoff)].reset_index(drop=True)
    part, _ = core.info.spo.training(rows, cutoff)
    with threadpool_limits(limits=2):
        model, _ = core.control.fit(rows, fold, 'ridge', core.key_hash(part), before_fit=lambda: None)
    return rows, model


def test_exact_join_preserves_order_na_and_all_original_fields(inputs):
    source = inputs.drop(columns=core.NEW)
    before = source.copy(deep=True)
    signal = projection(inputs).sample(frac=1, random_state=8)
    result = core.assemble(source, signal)
    pd.testing.assert_frame_equal(result.drop(columns=core.NEW), before)
    np.testing.assert_allclose(result[core.NEW], inputs[core.NEW], equal_nan=True)
    pd.testing.assert_frame_equal(source, before)


@pytest.mark.parametrize('fault', ['duplicate', 'missing', 'foreign', 'rank', 'manifest', 'clock', 'available',
    'catalog', 'name', 'status', 'unknown_reason', 'inf', 'text', 'bool'])
def test_projection_rejects_identity_or_invalid_values_without_calling_them_missing(inputs, fault):
    signal = projection(inputs)
    if fault == 'duplicate':
        signal = pd.concat([signal.iloc[1:], signal.iloc[[1]]], ignore_index=True)
    elif fault == 'missing':
        signal = signal.iloc[:-1]
    elif fault == 'foreign':
        signal.loc[0, 'instrument'] = '999999.SZ'
    elif fault in ('rank', 'manifest', 'clock', 'available', 'catalog', 'name', 'status', 'unknown_reason'):
        field, value = {'rank': ('selection_effective_rank', 99), 'manifest': ('manifest_sha256', 'b' * 64),
            'clock': ('calculation_visible_through', pd.Timestamp('2099-01-01')),
            'available': ('historical_first_availability', 'PIT_PROVEN'), 'catalog': ('catalog_id', 1273),
            'name': ('signal_name', 'other'), 'status': ('signal_status', 'AVAILABLE_NATIVE_RATIO'),
            'unknown_reason': ('signal_status', 'FILL_ZERO')}[fault]
        signal.loc[0, field] = value
    elif fault == 'inf':
        signal.loc[1, 'signal_value'] = np.inf
    elif fault == 'text':
        signal['signal_value'] = '0.1'
    else:
        signal['signal_value'] = True
    with pytest.raises((ValueError, AssertionError)):
        core.assemble(inputs.drop(columns=core.NEW), signal)


def test_same_control_keys_target_encoding_and_missing_bit_offset(inputs):
    rows, old = control_model(inputs)
    x, y, meta = core.training(rows, 1, old)
    bx, by, _ = core.control.training(rows, 1, old['training_keys_sha256'])
    np.testing.assert_array_equal(x[:, list(range(54)) + list(range(55, 109))], bx)
    np.testing.assert_array_equal(y, by)
    assert meta['fields'][-1] == core.NEW and x.shape[1] == 110
    missing_index = meta['fields'].index(core.NEW) + len(meta['fields'])
    assert np.ptp(x[:, missing_index]) > 0


@pytest.mark.parametrize('fault', ['keys', 'target', 'encoding', 'support', 'all_unknown', 'constant', 'future'])
def test_control_mismatch_or_no_information_stops_before_start(inputs, fault):
    rows, old = control_model(inputs)
    if fault == 'keys':
        old['training_keys_sha256'] = 'c' * 64
    elif fault == 'target':
        rows['hypothetical_liquidation_net_bps'] += np.arange(len(rows))
        rows['valuation_gross_terminal_ratio'] = (1 + rows.hypothetical_liquidation_net_bps / 10000) * (1 + .95 / 10000) / (1 - 5.95 / 10000)
    elif fault == 'encoding':
        old['median'][0] += 1
    elif fault == 'support':
        rows.loc[0, list(core.info.spo.FEATURES)] = np.nan
    elif fault == 'all_unknown':
        rows[core.NEW] = np.nan
    elif fault == 'constant':
        rows[core.NEW] = .1
    else:
        rows = inputs.copy()
    old['payload_sha256'] = core.info.spo.sha({k: v for k, v in old.items() if k != 'payload_sha256'})
    starts = []
    with pytest.raises((ValueError, AssertionError)):
        core.fit(rows, 1, old, before_fit=lambda: starts.append(1))
    assert starts == []


def test_synthetic_fit_export_independent_matrix_prediction_and_label_poison_isolation(inputs):
    rows, old = control_model(inputs)
    starts = []
    with threadpool_limits(limits=2):
        model, estimator = core.fit(rows, 1, old, before_fit=lambda: starts.append(1))
    target = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    result = core.predict(target, model, phase='fold', oracle=estimator)
    raw = target.loc[:, model['fields']].to_numpy(float)
    missing = np.isnan(raw)
    a, b = np.asarray(model['median']), np.asarray(model['scale'])
    matrix = np.c_[(np.where(missing, a, raw) - a) / b, missing.astype(float)]
    matrix -= matrix.mean(axis=0)
    scores = matrix @ np.asarray(model['coef'])
    scores -= scores.mean()
    np.testing.assert_allclose(result.decision_score, scores, rtol=1e-11, atol=1e-8)
    expected_order = np.lexsort((target.selection_effective_rank.to_numpy(), -scores))
    np.testing.assert_array_equal(result.sort_values('advisory_rank').selection_effective_rank, target.iloc[expected_order].selection_effective_rank)
    poisoned = target.copy()
    poisoned['hypothetical_liquidation_net_bps'] = np.inf
    poisoned['label_information_end'] = pd.Timestamp('2099-01-01')
    poisoned['valuation_status'] = 'UNKNOWN'
    pd.testing.assert_frame_equal(result, core.predict(poisoned, json.loads(json.dumps(model)), phase='fold'))
    assert starts == [1] and result.selected.sum() == 10 and len(result) == 50


def test_unsupported_original_slots_and_ties_are_preserved(inputs):
    rows, old = control_model(inputs)
    model, _ = core.fit(rows, 1, old, before_fit=lambda: None)
    model['coef'] = [0.] * 110
    core.sign(model)
    target = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    target.loc[0, list(core.info.spo.FEATURES)] = np.nan
    result = core.predict(target, model, phase='fold')
    assert result.loc[0, 'no_rank_advice'] and np.isnan(result.loc[0, 'decision_score'])
    np.testing.assert_array_equal(result.advisory_rank, target.selection_effective_rank)
    assert result.selected.sum() == 10


@pytest.mark.parametrize('fault', ['hash', 'order', 'coef', 'scale', 'bool', 'foreign', 'phase'])
def test_model_or_prediction_contract_rejects_drift(inputs, fault):
    rows, old = control_model(inputs)
    model, _ = core.fit(rows, 1, old, before_fit=lambda: None)
    target = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    phase = 'fold'
    if fault == 'hash':
        model['payload_sha256'] = '0' * 64
    elif fault == 'order':
        model['fields'][0], model['fields'][1] = model['fields'][1], model['fields'][0]
    elif fault == 'coef':
        model['coef'].pop()
    elif fault == 'scale':
        model['scale'][0] = 0
    elif fault == 'bool':
        model['coef'][0] = True
    elif fault == 'foreign':
        model['fields'].append('unknown')
    else:
        phase = 'live'
    if fault != 'hash':
        core.sign(model)
    with pytest.raises(ValueError):
        core.predict(target, model, phase=phase)


def test_three_claim_budget_duplicate_foreign_and_finished_binding(tmp_path):
    claims = core.FitClaims(tmp_path, 'a' * 64)
    with pytest.raises(ValueError):
        claims.write(1, 'b' * 64)
    for fold in (1, 2, 3):
        claims.write(fold)
        claims.write(fold, 'b' * 64)
    with pytest.raises(ValueError, match='budget exhausted'):
        claims.write(1)
    with pytest.raises(ValueError, match='foreign'):
        core.FitClaims(tmp_path, 'c' * 64).write(1, 'd' * 64)


@pytest.mark.parametrize('action', ['freeze', 'run'])
def test_authorization_is_required_before_source_label_reads_lock_or_output(tmp_path, monkeypatch, action):
    monkeypatch.setattr(runner, 'OUTPUT', tmp_path / 'never_created')
    reads = []
    monkeypatch.setattr(runner, 'load', lambda *a: reads.append(a))
    monkeypatch.setattr(runner.pd, 'read_parquet', lambda *a, **k: reads.append(a))
    with pytest.raises(ValueError, match='authorization'):
        getattr(runner, action)(None, None)
    with pytest.raises(ValueError, match='authorization'):
        runner.main([action])
    assert reads == [] and not runner.OUTPUT.exists()


def test_partial_started_blocks_before_any_label_decode(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'OUTPUT', tmp_path)
    study = tmp_path / 'study/fit_claims'
    study.mkdir(parents=True)
    (study / 'fold1_started.json').write_text('{}')
    monkeypatch.setattr(runner, 'load', lambda *a: {})
    monkeypatch.setattr(runner, 'verify', lambda *a: None)
    reads = []
    monkeypatch.setattr(runner.pd, 'read_parquet', lambda *a, **k: reads.append(a))
    with pytest.raises(ValueError, match='partial STARTED'):
        runner.run('synthetic test only', 3)
    assert reads == []


@pytest.mark.parametrize('qe', [[dict(active=10, node='remote')], [dict(active=10, node='local')], [dict(active=None, observed=False)]])
def test_qe_running_or_unknown_does_not_veto_real_local_capacity(monkeypatch, qe):
    monkeypatch.setattr(runner, 'local_capacity', lambda: dict(free_cpu_threads=2., available_memory_bytes=2 * 1024**3, free_output_bytes=200 * 1024**2))
    monkeypatch.setattr(runner.legacy, 'qe_observation', lambda: qe)
    assert runner.resource_admission()['capacity_sufficient']


def test_insufficient_capacity_stops_before_labels(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'OUTPUT', tmp_path)
    monkeypatch.setattr(runner, 'load', lambda *a: {})
    monkeypatch.setattr(runner, 'verify', lambda *a: None)
    monkeypatch.setattr(runner, 'local_capacity', lambda: dict(free_cpu_threads=1., available_memory_bytes=2 * 1024**3, free_output_bytes=200 * 1024**2))
    reads = []
    monkeypatch.setattr(runner.pd, 'read_parquet', lambda *a, **k: reads.append(a))
    with pytest.raises(ValueError, match='insufficient local'):
        runner.run('synthetic test only', 3)
    assert reads == [] and not (tmp_path / 'study/fit_claims').exists()


def test_economic_rule_keeps_primary_target_auxiliary_and_denominators_separate():
    good = dict(statistics=dict(mean_bps=110.), versus_matched_54_ridge=dict(mean_bps=10.),
        tail_delta_bps=0., unknown_excess=0., coverage=.9, auxiliary_coverage=.9, intervention_days=50, intervention_fraction=.5)
    folds, interventions = {'1': 1., '2': 1., '3': -1.}, {'1': 1, '2': 1, '3': 1}
    assert core.decision(good, good, folds, interventions)['action'] == 'DESIGN_INDEPENDENT_CONFIRMATION_ONLY'
    below = deepcopy(good)
    below['statistics']['mean_bps'] = 99.
    assert core.decision(below, good, folds, interventions)['action'] == 'BELOW_BUSINESS_TARGET_NO_EXPANSION'
    negative = deepcopy(good)
    negative['versus_matched_54_ridge']['mean_bps'] = -1.
    assert core.decision(negative, good, folds, interventions)['action'] == 'STOP_EXACT_OVERNIGHT_INCREMENT_CANDIDATE'
    missing = deepcopy(good)
    missing['statistics']['mean_bps'] = None
    assert core.decision(missing, good, folds, interventions)['action'] == 'INCONCLUSIVE_NO_EXPANSION'
    sparse = deepcopy(good)
    sparse['auxiliary_coverage'] = .79
    assert core.decision(sparse, good, folds, interventions)['action'] == 'INCONCLUSIVE_NO_EXPANSION'


def test_paired_auxiliary_uses_joint_complete_days_and_unknown_is_not_zero():
    days = pd.date_range('2025-04-01', periods=3)
    panel = pd.DataFrame(dict(package_id='p', decision_date=days, original_candidates=50, mature=True,
        candidate=[100., np.nan, 300.], baseline=[10., 20., 30.], delta=[90., np.nan, 270.],
        intervention_count=1, candidate_unknown=[0, 1, 0], baseline_unknown=0,
        avoided_loss=0., added_loss=0., added_profit=0., missed_profit=0.))
    old = panel.copy()
    old['candidate'] = [50., 60., np.nan]
    result = core.summarize(panel, old)
    assert result['complete_days'] == 2 and result['auxiliary_complete_days'] == 1
    assert result['statistics']['mean_bps'] == 180.
    assert result['versus_matched_54_ridge']['mean_bps'] == 50.
    assert result['candidate_unknown_slots'] == 1


def test_frozen_source_hash_failure_precedes_any_array_read(monkeypatch):
    monkeypatch.setattr(runner, 'paths', lambda: {'inputs': 'original', 'signal': 'signal'})
    monkeypatch.setattr(runner, 'ref', lambda path: {'sha256': 'bad'})
    reads = []
    monkeypatch.setattr(runner.pd, 'read_parquet', lambda *a, **k: reads.append(a))
    with pytest.raises(ValueError, match='frozen original'):
        runner.preflight()
    assert reads == []


def test_synthetic_three_fold_study_freezes_before_labels_and_reuses_zero_fits(tmp_path, monkeypatch):
    """Random mathematical fixtures only; no historical finance file is opened."""
    from backend.tests.advisory_model_first.test_spo_top10_5td_v1 import population

    template = population().query("package_id == 'a'").iloc[:50].copy()
    train_days = list(pd.bdate_range('2024-07-04', periods=179))
    evaluation_days = [day for _, first, last in core.info.spo.base.FOLDS for day in pd.bdate_range(first, last)[:42]]
    pressure_days = list(pd.bdate_range('2026-03-11', '2026-08-28')[:118])
    assert len(train_days + evaluation_days) == 305 and len(pressure_days) == 118
    rng, frames = np.random.default_rng(1272), []
    for day in train_days + evaluation_days + pressure_days:
        frame = template.copy()
        for name, value in core.info.prior.LSTM.items():
            frame[name] = value
        frame[core.DAY] = day
        frame['target_trade_date'] = day + pd.offsets.BDay(1)
        frame['label_information_end'] = day + pd.offsets.BDay(5)
        values = rng.normal(size=(50, 55))
        for j, name in enumerate(core.FEATURES):
            frame[name] = values[:, j]
        frame['hypothetical_liquidation_net_bps'] = 50 * values[:, 0] + 30 * values[:, -1]
        frame['valuation_gross_terminal_ratio'] = (1 + frame.hypothetical_liquidation_net_bps / 10000) * (1 + .95 / 10000) / (1 - 5.95 / 10000)
        frames.append(frame)
    full = pd.concat(frames, ignore_index=True)
    assembled = full.loc[:, [*core.INPUT_COLUMNS, core.NEW]]
    data = tmp_path / 'sources'
    data.mkdir()
    source_map = {'inputs': data / 'inputs.parquet', 'signal': data / 'signal.parquet',
        'control_protocol': data / 'control_protocol.json', 'finance': data / 'finance.parquet',
        'stress_finance': data / 'stress_finance.parquet'}
    assembled.to_parquet(source_map['inputs'], index=False)
    projection(assembled).to_parquet(source_map['signal'], index=False)
    source_map['control_protocol'].write_text(json.dumps({'experiment_id': 'synthetic_control'}))
    financial = [*core.KEY, 'label_information_end', 'valuation_status', 'hypothetical_liquidation_net_bps',
        'observed_gap_bps', 'valuation_gross_terminal_ratio', 'exit_execution_status', 'valuation_policy_sha256']
    heldout = []
    with threadpool_limits(limits=2):
        for fold, (cutoff, first, last) in enumerate(core.info.spo.base.FOLDS, 1):
            source_map['train' + str(fold)] = data / f'train{fold}.parquet'
            source_map['control' + str(fold)] = data / f'control{fold}.json'
            rows, model = control_model(full, fold)
            rows.loc[:, financial].to_parquet(source_map['train' + str(fold)], index=False)
            source_map['control' + str(fold)].write_text(json.dumps(model))
            part = full.loc[full[core.DAY].between(first, last), financial].copy()
            part['fold'] = fold
            heldout.append(part)
    pd.concat(heldout, ignore_index=True).to_parquet(source_map['finance'], index=False)
    full.loc[full[core.DAY].ge('2026-03-11'), financial].to_parquet(source_map['stress_finance'], index=False)
    monkeypatch.setattr(runner, 'OUTPUT', tmp_path / 'experiment')
    monkeypatch.setattr(runner, 'paths', lambda: source_map)
    monkeypatch.setattr(runner, 'versions', lambda: {})
    monkeypatch.setattr(runner, 'closure', lambda committed=False: {'synthetic_producer': 'a' * 64})
    monkeypatch.setattr(runner, 'verify_control', lambda: runner.CONTROL_TERMINAL)
    monkeypatch.setattr(runner, 'preflight', lambda: (assembled.copy(), {
        'sources': {k: runner.ref(v) for k, v in source_map.items()}, 'control_terminal': runner.CONTROL_TERMINAL}))
    monkeypatch.setattr(runner, 'resource_admission', lambda require_capacity=True: {'capacity_sufficient': True})
    monkeypatch.setattr(runner.subprocess, 'check_output', lambda *a, **k: '' if k.get('text') else b'')
    events = []
    real_read = pd.read_parquet

    def chronological_read(path, *args, **kwargs):
        for fold in (1, 2, 3):
            if path == source_map['train' + str(fold)]:
                if fold > 1:
                    assert (runner.OUTPUT / f'study/fold{fold-1}/trained/manifest.json').exists()
                events.append('train' + str(fold))
        if path in (source_map['finance'], source_map['stress_finance']):
            assert all((runner.OUTPUT / f'study/fold{f}/trained/manifest.json').exists() for f in (1, 2, 3))
            events.append('evaluation')
        return real_read(path, *args, **kwargs)

    monkeypatch.setattr(runner.pd, 'read_parquet', chronological_read)
    assert runner.freeze('synthetic test only', 3)['status'] == 'PREREGISTERED'
    with threadpool_limits(limits=2):
        result = runner.run('synthetic test only', 3)
    assert result['status'] == 'COMPLETED' and result['actual_new_financial_fits'] == 3
    assert events[:3] == ['train1', 'train2', 'train3'] and set(events[3:]) == {'evaluation'}
    count = len(events)
    assert runner.run('synthetic test only', 3)['status'] == 'STUDY_REUSED'
    assert len(events) == count
    checkpoint = runner.OUTPUT / 'study/fit_checkpoints/fold1.json'
    saved = checkpoint.read_bytes()
    checkpoint.write_bytes(b'{}')
    with pytest.raises(ValueError, match='checkpoint'):
        runner.run('synthetic test only', 3)
    checkpoint.write_bytes(saved)
    ledger = runner.OUTPUT / 'trial_registry.jsonl'
    ledger.write_text('')
    with pytest.raises(ValueError, match='registry'):
        runner.run('synthetic test only', 3)
    assert len(list((runner.OUTPUT / 'study/fit_claims').glob('*_started.json'))) == 3
