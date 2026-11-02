"""Real input/clock/rank/attribution failure points; no financial research fits."""
from copy import deepcopy

import numpy as np
import pandas as pd
import pytest
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first import lstm_factor_information_v1 as core
from backend.tests.advisory_model_first.test_spo_top10_5td_v1 import population
from scripts import advisory_lstm_factor_information as runner


@pytest.fixture
def inputs():
    f = population().query("package_id == 'a'").reset_index(drop=True)
    for k, v in core.prior.LSTM.items():
        f[k] = v
    extra = {k: np.sin(np.arange(len(f)) * (i + 1) / 57.) for i, k in enumerate(core.EXTRA)}
    return pd.concat([f, pd.DataFrame(extra)], axis=1)


def packet(fold=1):
    m = dict(schema=core.CONTRACT['schema'], contract_sha256=core.spo.sha(core.CONTRACT),
        target=core.prior.LSTM, fold=fold, cutoff=core.spo.base.FOLDS[fold - 1][0],
        fields=list(core.FEATURES), median=[0.] * 54, scale=[1.] * 54,
        coef=[1.] + [0.] * 107)
    m['payload_sha256'] = core.spo.sha(m)
    return m


def comparator(rows, cutoff):
    part, stats = core.spo.training(rows, cutoff)
    active, median, scale = core.encoding(core.spo.numeric(part))
    p = dict(contract_sha256=core.spo.sha(core.spo.CONTRACT), arm='candidate', cutoff=cutoff,
        fields=[f for f, a in zip(core.spo.FEATURES, active, strict=True) if a],
        median=median.tolist(), scale=scale.tolist(), coef=[0.] * (2 * active.sum()),
        training_keys_sha256=core.spo.sha([[str(v) for v in row] for row in part[core.KEY].itertuples(index=False, name=None)]),
        training_support=stats, train_rows=len(part), train_days=int(part[core.DAY].nunique()))
    p['model_payload_sha256'] = core.spo.sha(p)
    return p


def test_same_fixed_population_unknown_slot_and_no_target_dependency(inputs):
    f = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    f.loc[49, list(core.spo.FEATURES)] = np.nan
    before = f.copy(deep=True)
    pred = core.predict(f, packet(), phase='fold')
    assert pred.loc[49, 'no_rank_advice'] and pred.loc[49, 'advisory_rank'] == 50
    assert pred.selected.sum() == 10 and len(pred) == 50
    expected = f.iloc[:49].sort_values([core.spo.FEATURES[0], 'selection_effective_rank'], ascending=[False, True]).instrument[:10]
    assert set(pred.loc[pred.selected, 'instrument']) == set(expected)
    f['hypothetical_liquidation_net_bps'] = np.inf
    f['label_information_end'] = pd.Timestamp('2099-01-01')
    pd.testing.assert_frame_equal(pred, core.predict(f, packet(), phase='fold'))
    pd.testing.assert_frame_equal(before.loc[:, list(core.FEATURES)], f.loc[:, list(core.FEATURES)])


@pytest.mark.parametrize('problem', ['missing', 'duplicate', 'manifest', 'sealed', 'bool', 'text', 'unreviewed_inf'])
def test_input_failures_are_not_silently_missing(inputs, problem):
    f = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    if problem == 'missing':
        f = f.iloc[:-1]
    elif problem == 'duplicate':
        f = pd.concat([f, f.iloc[:1]], ignore_index=True)
    elif problem == 'manifest':
        f['manifest_sha256'] = 'wrong'
    elif problem == 'sealed':
        f[core.DAY] = pd.Timestamp('2025-10-10')
    elif problem == 'bool':
        f[core.EXTRA[0]] = True
    elif problem == 'text':
        f[core.EXTRA[0]] = '1.0'
    else:
        f.loc[0, core.EXTRA[1]] = np.inf
    with pytest.raises(ValueError):
        core.predict(f, packet(), phase='fold')


def test_documented_nonfinite_is_explicit_model_missing_and_source_retained(inputs):
    f = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    f.loc[0, core.EXTRA[0]] = -np.inf
    raw = core.numeric(f)
    assert np.isnan(raw[0, 8]) and np.isneginf(f.loc[0, core.EXTRA[0]])
    active, median, scale = core.encoding(raw)
    encoded = core.transform(raw[:, active], median, scale)
    assert encoded.shape == (50, 108) and encoded[0, 54 + 8] == 1.
    assert np.isfinite(encoded).all()


@pytest.mark.parametrize('problem', ['coef', 'fold', 'cutoff', 'field_order', 'zero_scale', 'live'])
def test_model_clock_identity_and_encoding_are_validated(inputs, problem):
    f = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    p, phase = packet(), 'fold'
    if problem == 'coef':
        p['coef'][0] = 2.
    elif problem == 'fold':
        p['fold'] = 4
    elif problem == 'cutoff':
        p['cutoff'] = '2025-04-02'
    elif problem == 'field_order':
        p['fields'] = list(reversed(p['fields']))
        p['payload_sha256'] = core.spo.sha({k: v for k, v in p.items() if k != 'payload_sha256'})
    elif problem == 'zero_scale':
        p['scale'][0] = 0.
        p['payload_sha256'] = core.spo.sha({k: v for k, v in p.items() if k != 'payload_sha256'})
    else:
        phase = 'live'
    with pytest.raises(ValueError):
        core.predict(f, p, phase=phase)


def test_training_match_and_future_guard_before_fit_claim(inputs, monkeypatch):
    rows = inputs.loc[inputs[core.DAY].lt('2025-04-01')].reset_index(drop=True)
    cutoff = '2025-03-31'
    old = comparator(rows, cutoff)
    part, stats, _keys = core.matched_training(rows, cutoff, old)
    assert len(part) == 100 and stats['train_groups'] == 2
    old_bad = deepcopy(old)
    old_bad['training_keys_sha256'] = 'bad'
    with pytest.raises(ValueError, match='training identity'):
        core.matched_training(rows, cutoff, old_bad)
    monkeypatch.setattr(core.spo, 'solve_spo', lambda *_a, **_k: pytest.fail('claim failure must precede fitting'))
    with pytest.raises(RuntimeError, match='stop_before_fit'):
        core.fit(rows, 1, old, before_fit=lambda: (_ for _ in ()).throw(RuntimeError('stop_before_fit')))
    with pytest.raises(ValueError, match='future training rows'):
        core.fit(inputs, 1, old, before_fit=lambda: pytest.fail('future rows cannot claim budget'))


def test_existing_convex_solver_handles_108_coordinates_with_certificate():
    rng = np.random.default_rng(20261102)
    x = rng.normal(size=(12, 108))
    y = np.linspace(-1., 1., 12)
    with threadpool_limits(limits=2):
        coef, certificate = core.spo.solve_spo(x, y, [np.arange(12)], [1.], k=2)
    assert coef.shape == (108,) and certificate['converged']
    assert certificate['gap'] <= 1e-5 * max(1., abs(certificate['upper']))


def test_primary_pairs_are_not_filtered_by_comparator_completeness():
    p = pd.DataFrame(dict(package_id=['p'] * 3, decision_date=pd.date_range('2025-04-01', periods=3),
        mature=[True] * 3, original_candidates=[50] * 3, baseline=[0.] * 3, candidate=[100., 200., 300.],
        delta=[100., 200., 300.], intervention_count=[1] * 3, candidate_unknown=[0] * 3, baseline_unknown=[0] * 3,
        avoided_loss=[0.] * 3, added_loss=[0.] * 3, added_profit=[100., 200., 300.], missed_profit=[0.] * 3))
    old = p.copy()
    old.loc[1, 'candidate'] = np.nan
    result = core.summarize(p, old)
    assert result['complete_days'] == 3 and result['auxiliary_complete_days'] == 2
    assert result['statistics']['mean_bps'] == 200.


@pytest.mark.parametrize(('mean', 'tail', 'unknown', 'expected'), [
    (-1., 0., 0., 'STOP_THIS_FACTOR_CANDIDATE'),
    (150., -101., 0., 'STOP_THIS_FACTOR_CANDIDATE'),
    (150., 0., .03, 'STOP_THIS_FACTOR_CANDIDATE'),
    (99., 0., 0., 'BELOW_BUSINESS_TARGET_NO_EXPANSION'),
    (100., 0., 0., 'DESIGN_INDEPENDENT_CONFIRMATION_ONLY'),
    (None, None, None, 'INCONCLUSIVE_NO_EXPANSION'),
])
def test_business_target_and_negative_branches(mean, tail, unknown, expected):
    r = dict(statistics=dict(mean_bps=mean), tail_delta_bps=tail, unknown_excess=unknown,
        coverage=1., intervention_days=100, intervention_fraction=1.)
    result = core.decision(r, r, {'1': 1., '2': 1., '3': 1.})
    assert result['action'] == expected and not result['production_ready'] and not result['v2_changed']


def test_source_drift_rejected_before_reading_or_fitting(tmp_path, monkeypatch):
    p = tmp_path / 'input.json'
    p.write_text('{}', encoding='utf8')
    plan = dict(sources=dict(source=runner.ref(p)))
    monkeypatch.setattr(runner, 'source_paths', lambda: {'source': p})
    assert runner.checked(plan, 'source') == p
    p.write_text('{"changed":true}', encoding='utf8')
    with pytest.raises(ValueError, match='source drift'):
        runner.checked(plan, 'source')
