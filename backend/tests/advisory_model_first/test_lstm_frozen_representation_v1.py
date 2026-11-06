"""Synthetic-only contracts; never reads financial research artifacts."""
from copy import deepcopy
from fractions import Fraction
import json

import numpy as np
import pandas as pd
import pytest
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first import lstm_frozen_representation_v1 as core
from backend.tests.advisory_model_first.test_lstm_factor_nonlinear_v1 import inputs as base_inputs, keys, packet as old_packet
from scripts import advisory_lstm_frozen_representation as runner

inputs = base_inputs


@pytest.fixture
def data(inputs):
    h = np.random.default_rng(981).uniform(-1, 1, size=(len(inputs), 64))
    result = pd.concat([inputs, pd.DataFrame(h, columns=core.HIDDEN)], axis=1)
    result['representation_supported'], result['representation_reason'] = True, 'AVAILABLE'
    return result


def packet():
    p = old_packet('ridge')
    p.update(schema=core.CONTRACT['schema'], contract_sha256=core.info.spo.sha(core.CONTRACT),
        hidden_fields=list(core.HIDDEN), hidden_median=[0.] * 64, hidden_scale=[1.] * 64,
        representation_sha256=core.CONTRACT['representation_sha256'])
    p['learner']['coef'] += [0.] * 65
    return resign(p)


def resign(p):
    p['payload_sha256'] = core.info.spo.sha({k: v for k, v in p.items() if k != 'payload_sha256'})
    return p


def target(data):
    return data.loc[data[core.DAY].eq('2025-04-01')].reset_index(drop=True)


def test_fixed_readout_training_reuses_control_and_173_coordinates(data):
    train = data.loc[data[core.DAY].lt('2025-04-01')].reset_index(drop=True)
    starts = []
    with threadpool_limits(limits=2):
        # Two synthetic fits only, deliberately separate from financial claims.
        control, _ = core.old.fit(train, 1, 'ridge', keys(train), before_fit=lambda: None)
        model, estimator = core.fit(train, 1, control, before_fit=lambda: starts.append(1))
    x, y, meta = core.training(train, 1, control)
    assert x.shape == (300, 173) and starts == [1]
    assert model['training_keys_sha256'] == control['training_keys_sha256']
    for name in ('fields', 'median', 'scale', 'training_support', 'train_rows', 'train_days'):
        assert model[name] == control[name]
    assert model['certificate']['relative_normal_equation_residual'] <= 1e-8
    np.testing.assert_allclose(x @ model['learner']['coef'], estimator.predict(x))
    pred = core.predict(target(data), model, phase='fold')
    pd.testing.assert_frame_equal(pred, core.predict(target(data), json.loads(json.dumps(model)), phase='fold'))
    assert pred.selected.sum() == 10 and len(pred) == 50 and meta['hidden_train_missing'] == 0
    corrupt = deepcopy(control)
    corrupt['median'][0] += 1
    with pytest.raises(ValueError, match='training contract differs'):
        core.fit(train, 1, resign(corrupt), before_fit=lambda: starts.append(2))
    with pytest.raises(ValueError, match='future training rows'):
        core.fit(data, 1, control, before_fit=lambda: starts.append(2))
    assert starts == [1] and np.isfinite(y).all()


@pytest.mark.parametrize('problem', ['partial', 'inf', 'bound', 'false_finite', 'reason', 'dtype', 'missing', 'duplicate', 'package'])
def test_input_contract_rejects_corruption(data, problem):
    f = target(data)
    if problem == 'partial':
        f.loc[0, core.HIDDEN[0]] = np.nan
    elif problem == 'inf':
        f.loc[0, core.HIDDEN[0]] = np.inf
    elif problem == 'bound':
        f.loc[0, core.HIDDEN[0]] = 1.1
    elif problem == 'false_finite':
        f.loc[0, 'representation_supported'] = False
    elif problem == 'reason':
        f.loc[0, 'representation_reason'] = 'unknown'
    elif problem == 'dtype':
        f['representation_supported'] = 'True'
    elif problem == 'missing':
        f = f.iloc[:-1]
    elif problem == 'duplicate':
        f = pd.concat([f, f.iloc[:1]], ignore_index=True)
    else:
        f['package_id'] = 'other'
    with pytest.raises(ValueError):
        core.predict(f, packet(), phase='fold')


def test_missing_block_and_original_no_advice_retained_without_label_read(data):
    f = target(data)
    f.loc[0, list(core.HIDDEN)] = np.nan
    f.loc[0, 'representation_supported'], f.loc[0, 'representation_reason'] = False, 'SOURCE_SEQUENCE_UNAVAILABLE'
    f.loc[49, list(core.info.spo.FEATURES)] = np.nan
    before = core.predict(f, packet(), phase='fold')
    assert before.loc[49, 'no_rank_advice'] and before.loc[49, 'advisory_rank'] == 50
    encoded = core.hidden_matrix(f.loc[:, core.HIDDEN].to_numpy(), np.zeros(64), np.ones(64))
    np.testing.assert_array_equal(encoded[0], [0.] * 64 + [1.])
    f['hypothetical_liquidation_net_bps'] = np.inf
    f['label_information_end'] = pd.Timestamp('2099-01-01')
    pd.testing.assert_frame_equal(before, core.predict(f, packet(), phase='fold'))
    assert f.loc[0, list(core.HIDDEN)].isna().all()


@pytest.mark.parametrize('problem', ['hash', 'cutoff', 'identity', 'hidden_order', 'scale', 'coef', 'params'])
def test_model_clock_identity_and_encoding_rejected(data, problem):
    p = packet()
    if problem == 'hash':
        p['learner']['coef'][0] = 5
    else:
        if problem == 'cutoff':
            p['cutoff'] = '2025-04-01'
        elif problem == 'identity':
            p['representation_sha256'] = 'a' * 64
        elif problem == 'hidden_order':
            p['hidden_fields'].reverse()
        elif problem == 'scale':
            p['hidden_scale'][0] = 0
        elif problem == 'coef':
            p['learner']['coef'].pop()
        else:
            p['params'] = dict(p['params'], alpha=1.)
        resign(p)
    with pytest.raises(ValueError):
        core.predict(target(data), p, phase='fold')


def test_exact_order_does_not_collapse_extreme_common_offset_and_keeps_ties(data):
    f, p = target(data), packet()
    f.loc[:, core.info.FEATURES[0]] = 1e30
    f.loc[:, core.info.FEATURES[1]] = np.arange(50) // 2
    p['learner']['coef'][1] = 1.
    resign(p)
    pred = core.predict(f, p, phase='fold')
    assert pred.loc[48, 'advisory_rank'] == 1 and pred.loc[49, 'advisory_rank'] == 2
    assert Fraction(pred.loc[48, 'ranking_score_exact']) > Fraction(pred.loc[46, 'ranking_score_exact'])
    control = old_packet('ridge')
    control['learner']['coef'][1] = 1.
    pd.testing.assert_series_equal(pred.advisory_rank, core.predict(f, resign(control), phase='fold', control=True).advisory_rank)
    with pytest.raises(ValueError):
        core.predict(f, p, phase='live')


def test_three_starts_no_duplicate_or_foreign_finish(tmp_path):
    claims = core.FitClaims(tmp_path, 'a' * 64)
    with pytest.raises(ValueError, match='requires valid start'):
        claims.write(1, 'b' * 64)
    claims.write(1)
    with pytest.raises(FileExistsError):
        claims.write(1)
    for fold in (2, 3):
        claims.write(fold)
    for fold in (1, 2, 3):
        claims.write(fold, 'b' * 64)
    with pytest.raises(ValueError, match='exhausted'):
        claims.write(1)
    with pytest.raises(ValueError, match='foreign'):
        core.FitClaims(tmp_path, 'c' * 64).write(1, 'd' * 64)
    assert len(list(tmp_path.glob('*_started.json'))) == 3


def test_partial_or_resource_shortage_never_reads_labels_or_refits(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'OUTPUT', tmp_path)
    monkeypatch.setattr(runner, 'load', lambda p: {})
    monkeypatch.setattr(runner, 'verify', lambda p: None)
    reads = []
    monkeypatch.setattr(runner.pd, 'read_parquet', lambda *a, **k: reads.append(a))

    def insufficient():
        raise ValueError('insufficient actual local capacity')

    monkeypatch.setattr(runner, 'resource_admission', insufficient)
    with pytest.raises(ValueError, match='insufficient'):
        runner.run()
    root = tmp_path / 'study/fit_claims'
    root.mkdir(parents=True)
    (root / 'candidate1_started.json').write_text('{}')
    with pytest.raises(ValueError, match='partial STARTED'):
        runner.run()
    assert reads == []


def test_complete_reentry_checks_chain_registry_and_claims_without_finance(tmp_path, monkeypatch, capsys):
    folder = tmp_path / 'study/terminal/evaluated'
    folder.mkdir(parents=True)
    (folder / 'receipt.json').write_text('{}')
    monkeypatch.setattr(runner, 'OUTPUT', tmp_path)
    monkeypatch.setattr(runner, 'load', lambda p: {})
    monkeypatch.setattr(runner, 'verify', lambda p: None)
    observed = []
    monkeypatch.setattr(runner, 'read_stage', lambda p, **kw: observed.append(p) or {'stage_sha256': 'a' * 64})
    monkeypatch.setattr(runner, 'verify_registry', lambda p: observed.append('registry'))
    monkeypatch.setattr(runner, 'verify_claims', lambda p: observed.append('claims'))
    monkeypatch.setattr(runner.pd, 'read_parquet', lambda *a, **kw: pytest.fail('reentry decoded finance'))
    monkeypatch.setattr(core, 'fit', lambda *a, **kw: pytest.fail('reentry fitted'))
    runner.run()
    assert len(observed) == 9 and observed[-2:] == ['registry', 'claims']
    assert json.loads(capsys.readouterr().out)['new_financial_fits'] == 0


@pytest.mark.parametrize('change', ['contract', 'closure', 'versions', 'budget', 'resource', 'source', 'chain'])
def test_immutable_protocol_drift_rejected(monkeypatch, change):
    plan = dict(contract=json.loads(runner.json_bytes(core.CONTRACT)), closure={}, versions={}, max_new_fits=3,
        resource_policy=runner.RESOURCE_POLICY, control_terminal_sha256='old', sources={'input': {}})
    monkeypatch.setattr(runner, 'closure', lambda: {})
    monkeypatch.setattr(runner.previous, 'versions', lambda: {})
    monkeypatch.setattr(runner, 'paths', lambda: {'input': 'path'})
    monkeypatch.setattr(runner, 'ref', lambda p: {})
    monkeypatch.setattr(runner, 'verify_control_chain', lambda: 'old')
    runner.verify(plan)
    if change in ('contract', 'closure', 'versions'):
        plan[change] = {'bad': 1}
    elif change == 'budget':
        plan['max_new_fits'] = 4
    elif change == 'resource':
        plan['resource_policy'] = {}
    elif change == 'source':
        plan['sources']['input'] = {'bad': 1}
    else:
        plan['control_terminal_sha256'] = 'new'
    with pytest.raises(ValueError):
        runner.verify(plan)


@pytest.mark.parametrize('mean,tail,unknown,coverage,expected', [
    (None, 0., 0., 1., 'NOT_EVALUABLE'), (-1., 0., 0., 1., 'STOP_EXACT_REPRESENTATION_CANDIDATE'),
    (99., 0., 0., 1., 'BELOW_TARGET_STOP'), (100., -101., 0., 1., 'STOP_EXACT_REPRESENTATION_CANDIDATE'),
    (100., 0., .021, 1., 'STOP_EXACT_REPRESENTATION_CANDIDATE'),
    (100., 0., 0., .79, 'INCONCLUSIVE_NO_EXPANSION'), (100., 0., 0., 1., 'DESIGN_CONFIRMATION_ONLY'),
])
def test_business_threshold_and_all_stop_branches(mean, tail, unknown, coverage, expected):
    r = dict(statistics=dict(mean_bps=mean), tail_delta_bps=tail, unknown_excess=unknown, coverage=coverage,
        intervention_days=100, intervention_fraction=1., versus_frozen_ridge=dict(mean_bps=-1.))
    result = core.decision(r, r, {'1': 1., '2': 1., '3': -1.})
    assert result['action'] == expected and result['extra_fit_budget'] == 0 and not result['production_ready']
    assert not result['representation_increment_supported_descriptively']


def test_unknown_keeps_original_axis_and_control_does_not_restrict_primary():
    p = pd.DataFrame(dict(package_id=['p'] * 4, decision_date=pd.date_range('2025-04-01', periods=4),
        mature=[True] * 3 + [False], original_candidates=[50] * 4, baseline=[0., 0., 0., np.nan],
        candidate=[100., 200., np.nan, np.nan], delta=[100., 200., np.nan, np.nan], intervention_count=[1] * 4,
        candidate_unknown=[0, 0, 1, 10], baseline_unknown=[0, 0, 0, 10], avoided_loss=[0.] * 4,
        added_loss=[0.] * 4, added_profit=[100., 200., np.nan, np.nan], missed_profit=[0.] * 4))
    control = p.copy()
    control.loc[1, 'candidate'] = np.nan
    report = core.summarize(p, control)
    assert report['original_days'] == 4 and report['mature_days'] == 3 and report['complete_days'] == 2
    assert report['statistics']['mean_bps'] == 150. and report['statistics']['declared_days'] == 4
    assert report['auxiliary_complete_days'] == 1 and report['versus_frozen_ridge']['mean_bps'] == 0.
    assert report['unknown_excess'] == 1 / 30 and report['candidate_unknown_slots'] == 11


def test_actual_capacity_only_without_qe_or_network_query(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'OUTPUT', tmp_path)
    monkeypatch.setattr(runner.psutil, 'cpu_percent', lambda **kw: 0.)
    monkeypatch.setattr(runner.previous, 'qe_observation', lambda: pytest.fail('QE queried'))
    assert runner.resource_admission()['capacity_sufficient']
    monkeypatch.setattr(runner.psutil, 'cpu_percent', lambda **kw: 100.)
    with pytest.raises(ValueError, match='insufficient actual'):
        runner.resource_admission()
    assert not runner.resource_admission(require_capacity=False)['capacity_sufficient']
