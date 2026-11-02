"""Frozen same-input learning, numeric-tree export and bounded-research contracts."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first import lstm_factor_nonlinear_v1 as core
from backend.tests.advisory_model_first.test_spo_top10_5td_v1 import population
from scripts import advisory_lstm_factor_nonlinear as runner


@pytest.fixture
def inputs():
    base = population().query("package_id == 'a'").iloc[:50].copy().reset_index(drop=True)
    rng, rows = np.random.default_rng(20261102), []
    for day in pd.bdate_range('2024-11-01', periods=6).append(pd.DatetimeIndex(['2025-04-01'])):
        frame = base.copy()
        for name, value in core.info.prior.LSTM.items():
            frame[name] = value
        frame[core.DAY] = day
        frame['target_trade_date'] = day + pd.offsets.BDay(1)
        frame['label_information_end'] = day + pd.offsets.BDay(5)
        features = rng.normal(size=(50, 54))
        for j, field in enumerate(core.FEATURES):
            frame[field] = features[:, j]
        frame['hypothetical_liquidation_net_bps'] = 50 * features[:, 0] + 80 * (features[:, 8] > 0)
        frame['valuation_gross_terminal_ratio'] = (1 + frame['hypothetical_liquidation_net_bps'] / 10000) * (1 + .95 / 10000) / (1 - 5.95 / 10000)
        rows.append(frame)
    return pd.concat(rows, ignore_index=True)


def keys(frame):
    part, _ = core.info.spo.training(frame, '2025-03-31')
    return core.info.spo.sha([[str(v) for v in row] for row in part[core.KEY].itertuples(index=False, name=None)])


def packet(arm='hgb', fold=1):
    learner = dict(bias=0., trees=[[dict(leaf=True, value=0.)] for _ in range(120)])
    if arm == 'ridge':
        learner = dict(coef=[1.] + [0.] * 107)
    p = dict(schema=core.CONTRACT['schema'], contract_sha256=core.info.spo.sha(core.CONTRACT),
        target=core.info.prior.LSTM, arm=arm, fold=fold, cutoff=core.info.spo.base.FOLDS[fold - 1][0],
        params=core.PARAMS[arm], fields=list(core.FEATURES), median=[0.] * 54, scale=[1.] * 54, learner=learner)
    p['payload_sha256'] = core.info.spo.sha(p)
    return p


def resign(p):
    p['payload_sha256'] = core.info.spo.sha({k: v for k, v in p.items() if k != 'payload_sha256'})
    return p


def test_both_arms_same_training_and_official_export_parity(inputs):
    rows = inputs.loc[inputs[core.DAY].lt('2025-04-01')].reset_index(drop=True)
    target = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    models, starts = {}, []
    with threadpool_limits(limits=2):
        for arm in core.PARAMS:
            model, estimator = core.fit(rows, 1, arm, keys(rows), before_fit=lambda a=arm: starts.append(a))
            pred = core.predict(target, model, phase='fold', oracle=estimator)
            assert len(pred) == 50 and pred.selected.sum() == 10
            restored = json.loads(json.dumps(model, allow_nan=False))
            pd.testing.assert_frame_equal(pred, core.predict(target, restored, phase='fold'))
            assert estimator.get_params(deep=False) == core.PARAMS[arm]
            models[arm] = model
    assert starts == ['ridge', 'hgb']
    for name in ['fields', 'median', 'scale', 'training_keys_sha256', 'training_support', 'train_rows', 'train_days']:
        assert models['ridge'][name] == models['hgb'][name]
    assert models['ridge']['certificate']['relative_normal_equation_residual'] <= 1e-8
    assert models['hgb']['certificate']['fixed_iterations'] == 120
    assert any(len(t) > 1 for t in models['hgb']['learner']['trees'])


def test_future_or_wrong_training_population_cannot_start(inputs):
    starts = []
    with pytest.raises(ValueError, match='future training rows'):
        core.fit(inputs, 1, 'hgb', 'wrong', before_fit=lambda: starts.append(1))
    rows = inputs.loc[inputs[core.DAY].lt('2025-04-01')]
    with pytest.raises(ValueError, match='training keys'):
        core.fit(rows, 1, 'ridge', 'wrong', before_fit=lambda: starts.append(1))
    assert starts == []


def test_unknown_slot_and_future_labels_do_not_change_predictions(inputs):
    f = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    f.loc[49, list(core.info.spo.FEATURES)] = np.nan
    before = core.predict(f, packet('ridge'), phase='fold')
    assert before.loc[49, 'no_rank_advice'] and before.loc[49, 'advisory_rank'] == 50
    f['hypothetical_liquidation_net_bps'] = np.inf
    f['label_information_end'] = pd.Timestamp('2099-01-01')
    pd.testing.assert_frame_equal(before, core.predict(f, packet('ridge'), phase='fold'))


def test_documented_infinity_encoded_but_unreviewed_infinity_rejected(inputs):
    f = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    f.loc[0, core.info.EXTRA[0]] = -np.inf
    core.predict(f, packet(), phase='fold')
    assert np.isneginf(f.loc[0, core.info.EXTRA[0]])
    f.loc[0, core.info.EXTRA[1]] = np.inf
    with pytest.raises(ValueError, match='nonfinite'):
        core.predict(f, packet(), phase='fold')


@pytest.mark.parametrize('problem', ['hash', 'cutoff', 'fields', 'params', 'scale', 'tree_cycle', 'orphan', 'threshold', 'feature', 'iterations'])
def test_corrupt_or_unregistered_models_fail(inputs, problem):
    f = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    p = deepcopy(packet())
    if problem == 'hash':
        p['learner']['bias'] = 1.
    elif problem == 'cutoff':
        p['cutoff'] = '2025-04-02'
    elif problem == 'fields':
        p['fields'] = p['fields'][::-1]
        resign(p)
    elif problem == 'params':
        p['params']['max_leaf_nodes'] = 7
        resign(p)
    elif problem == 'scale':
        p['scale'][0] = 0.
        resign(p)
    elif problem == 'iterations':
        p['learner']['trees'].pop()
        resign(p)
    else:
        tree = [dict(leaf=False, feature=0, threshold=0., left=1, right=2),
                dict(leaf=True, value=1.), dict(leaf=True, value=-1.)]
        if problem == 'tree_cycle':
            tree[0]['left'] = 0
        elif problem == 'orphan':
            tree.append(dict(leaf=True, value=0.))
        elif problem == 'threshold':
            tree[0]['threshold'] = True
        else:
            tree[0]['feature'] = 108
        p['learner']['trees'][0] = tree
        resign(p)
    with pytest.raises(ValueError):
        core.predict(f, p, phase='fold')


@pytest.mark.parametrize('problem', ['wrong_package', 'missing', 'duplicate', 'sealed', 'text', 'live'])
def test_original_axis_and_phase_fail_closed(inputs, problem):
    f = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    phase = 'fold'
    if problem == 'wrong_package':
        f['package_id'] = 'other'
    elif problem == 'missing':
        f = f.iloc[:-1]
    elif problem == 'duplicate':
        f = pd.concat([f, f.iloc[:1]], ignore_index=True)
    elif problem == 'sealed':
        f[core.DAY] = pd.Timestamp('2025-10-10')
    elif problem == 'text':
        f[core.info.EXTRA[0]] = '1'
    else:
        phase = 'live'
    with pytest.raises(ValueError):
        core.predict(f, packet(), phase=phase)


def test_numeric_tree_threshold_boundary_and_stable_ties(inputs):
    tree = [dict(leaf=False, feature=0, threshold=0., left=1, right=2),
            dict(leaf=True, value=1.), dict(leaf=True, value=-1.)]
    close = core.score_matrix(np.array([[0.], [1e-15], [-1e-15]]), 'hgb', dict(bias=0., trees=[tree]))
    np.testing.assert_array_equal(close, [1., -1., 1.])
    f = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    pred = core.predict(f, packet(), phase='fold')
    np.testing.assert_array_equal(pred.advisory_rank, f.selection_effective_rank)


def test_six_financial_claims_no_seventh_or_cross_protocol(tmp_path):
    claims = core.FitClaims(tmp_path, 'a' * 64)
    for fold in (1, 2, 3):
        for arm in ('ridge', 'hgb'):
            claims.write(arm, fold)
            claims.write(arm, fold, 'b' * 64)
    assert len(list(tmp_path.glob('*_started.json'))) == 6
    with pytest.raises(ValueError, match='budget exhausted'):
        claims.write('hgb', 1)
    with pytest.raises(ValueError, match='foreign fit claim'):
        core.FitClaims(tmp_path, 'c' * 64).write('hgb', 1, 'd' * 64)


def test_registry_required_for_complete_reuse(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'OUTPUT', tmp_path)
    with pytest.raises(FileNotFoundError):
        runner.verify_registry(dict(experiment_id='one'))
    (tmp_path / 'trial_registry.jsonl').write_text(json.dumps(dict(experiment_id='one', evaluated_trial_count=1, selected_trial_count=0)) + '\n')
    runner.verify_registry(dict(experiment_id='one'))
    with pytest.raises(ValueError):
        runner.verify_registry(dict(experiment_id='another'))


def test_active_qe_blocks_before_any_training_label_or_claim(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'OUTPUT', tmp_path)
    monkeypatch.setattr(runner, 'load', lambda path: {'experiment_id': 'one'})
    monkeypatch.setattr(runner, 'verify', lambda plan: None)
    reads = []
    monkeypatch.setattr(runner.pd, 'read_parquet', lambda *a, **k: reads.append(a))

    def busy():
        raise ValueError('QE training owner is active; no research fit started')

    monkeypatch.setattr(runner.previous, 'qe_idle', busy)
    with pytest.raises(ValueError, match='QE training owner is active'):
        runner.run()
    assert reads == []
    assert not (tmp_path / 'study/fit_claims').exists()
    assert not (tmp_path / 'execution_failure.json').exists()


def test_primary_pairs_do_not_depend_on_ridge_completeness_or_old_arm_label():
    p = pd.DataFrame(dict(package_id=['p'] * 3, decision_date=pd.date_range('2025-04-01', periods=3),
        mature=[True] * 3, original_candidates=[50] * 3, baseline=[0.] * 3, candidate=[100., 200., 300.],
        delta=[100., 200., 300.], intervention_count=[1] * 3, candidate_unknown=[0] * 3, baseline_unknown=[0] * 3,
        avoided_loss=[0.] * 3, added_loss=[0.] * 3, added_profit=[100., 200., 300.], missed_profit=[0.] * 3))
    ridge = p.copy()
    ridge.loc[1, 'candidate'] = np.nan
    result = core.summarize(p, ridge)
    assert result['complete_days'] == 3 and result['auxiliary_complete_days'] == 2
    assert 'versus_eight_fields' not in result and 'versus_matched_ridge' in result
    assert result['statistics']['mean_bps'] == 200.


@pytest.mark.parametrize(('mean', 'expected'), [
    (-1., 'STOP_NONLINEAR_CANDIDATE_PAUSE_CURRENT_FACTOR_MODEL_SEARCH'),
    (99., 'BELOW_TARGET_PAUSE_CURRENT_FACTOR_MODEL_SEARCH'),
    (100., 'DESIGN_INDEPENDENT_CONFIRMATION_ONLY'),
])
def test_fixed_business_target_and_pause_branch(mean, expected):
    stats = dict(statistics=dict(mean_bps=mean), tail_delta_bps=0., unknown_excess=0.,
        coverage=1., intervention_days=100, intervention_fraction=1.)
    d = core.decision(stats, stats, {'1': 1., '2': 1., '3': 1.})
    assert d['action'] == expected and not d['production_ready'] and not d['v2_changed']
    assert d['current_factor_model_search_paused'] == (mean < 100)
