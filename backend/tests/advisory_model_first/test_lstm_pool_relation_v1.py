"""Original population, causal peer construction and bounded offline execution."""
from copy import deepcopy
import json

import numpy as np
import pandas as pd
import pytest
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first import lstm_pool_relation_v1 as core
from backend.tests.advisory_model_first import test_lstm_factor_nonlinear_v1 as fixtures
from scripts import advisory_lstm_pool_relation as runner


@pytest.fixture(name='inputs')
def reuse_inputs():
    return fixtures.inputs.__wrapped__()


def metadata(frame):
    part, _ = core.info.spo.training(frame, '2025-03-31')
    key = core.info.spo.sha([[str(v) for v in row] for row in part[core.KEY].itertuples(index=False, name=None)])
    return core.previous.training(frame, 1, key)[2]


def toy(frame):
    f = frame.loc[frame[core.DAY].eq('2025-04-01')].copy().reset_index(drop=True)
    for col in core.info.EXTRA:
        f[col] = np.arange(50, dtype=float)
    f['ret_5'] = np.arange(50, dtype=float) / 100
    return f, dict(fields=list(core.FEATURES), median=[0.] * 54, scale=[1.] * 54)


def test_local_relationship_changes_with_pairing_not_global_marginals(inputs):
    f, meta = toy(inputs)
    original = core.contexts(f, meta)
    changed = f.copy()
    changed.loc[1:5, core.info.EXTRA] = f.loc[40:44, core.info.EXTRA].to_numpy()
    changed.loc[40:44, core.info.EXTRA] = f.loc[1:5, core.info.EXTRA].to_numpy()
    result = core.contexts(changed, meta)
    assert original.loc[0, 'candidate_neighbors'] != result.loc[0, 'candidate_neighbors']
    assert original.loc[0, 'candidate__ret_5'] != result.loc[0, 'candidate__ret_5']
    pd.testing.assert_frame_equal(original.filter(like='matched'), result.filter(like='matched'))
    np.testing.assert_array_equal(np.sort(f[list(core.info.EXTRA)].to_numpy(), axis=0),
                                  np.sort(changed[list(core.info.EXTRA)].to_numpy(), axis=0))


def test_order_self_tie_and_day_isolation(inputs):
    f, meta = toy(inputs)
    f.loc[:, list(core.info.EXTRA)] = 0.
    before = core.contexts(f, meta)
    assert before.loc[0, 'candidate_neighbors'].split('|') == f.instrument.iloc[1:6].tolist()
    shuffled = core.contexts(f.iloc[::-1], meta).sort_values(core.KEY).reset_index(drop=True)
    pd.testing.assert_frame_equal(before.sort_values(core.KEY).reset_index(drop=True), shuffled)
    for _, row in before.iterrows():
        assert row.instrument not in row.candidate_neighbors.split('|')
    future = f.copy()
    future[core.DAY] = pd.Timestamp('2025-04-02')
    future['target_trade_date'] = pd.Timestamp('2025-04-03')
    future.loc[:, list(core.info.EXTRA)] = 999.
    mixed = core.contexts(pd.concat([f, future], ignore_index=True), meta)
    pd.testing.assert_frame_equal(before, mixed.iloc[:50].reset_index(drop=True))


def test_missing_peers_never_shrink_or_replace_and_source_preserved(inputs):
    f, meta = toy(inputs)
    before = core.contexts(f, meta)
    f.loc[1, 'ret_5'] = np.nan
    result = core.contexts(f, meta)
    assert result.loc[0, 'candidate_neighbors'] == before.loc[0, 'candidate_neighbors']
    assert np.isnan(result.loc[0, 'candidate__ret_5'])
    assert np.isfinite(result.loc[0, 'candidate__atr14_close'])
    assert np.isnan(f.loc[1, 'ret_5'])
    f.loc[0, list(core.info.EXTRA[:9])] = np.nan
    assert core.contexts(f, meta).loc[0, 'candidate_neighbors']  # 37/46 passes.
    f.loc[0, core.info.EXTRA[9]] = np.nan
    assert core.contexts(f, meta).loc[0, 'candidate_neighbors'] == ''  # 36/46 fails.


def test_original_population_bad_sources_and_unreviewed_infinity(inputs):
    f, meta = toy(inputs)
    for broken in (f.iloc[:-1], pd.concat([f, f.iloc[:1]])):
        with pytest.raises(ValueError):
            core.contexts(broken, meta)
    f.loc[0, core.info.EXTRA[0]] = -np.inf
    core.contexts(f, meta)
    assert np.isneginf(f.loc[0, core.info.EXTRA[0]])
    f.loc[0, core.info.EXTRA[1]] = np.inf
    with pytest.raises(ValueError, match='nonfinite'):
        core.contexts(f, meta)


def test_both_arms_train_keys_dimensions_and_json_replay(inputs):
    train = inputs.loc[inputs[core.DAY].lt('2025-04-01')].copy()
    target = inputs.loc[inputs[core.DAY].eq('2025-04-01')].reset_index(drop=True)
    frozen = metadata(train)
    starts, packets = [], []
    with threadpool_limits(limits=2):
        for arm in core.ARMS:
            packet, estimator = core.fit(train, 1, arm, frozen, before_fit=lambda a=arm: starts.append(a))
            prediction = core.predict(target, packet, phase='fold', oracle=estimator)
            pd.testing.assert_frame_equal(prediction, core.predict(target, json.loads(json.dumps(packet)), phase='fold'))
            assert prediction.selected.sum() == 10
            assert packet['certificate']['relative_normal_equation_residual'] < 1e-8
            poisoned = target.copy()
            poisoned['hypothetical_liquidation_net_bps'] = np.inf
            poisoned['label_information_end'] = pd.Timestamp('2099-01-01')
            pd.testing.assert_frame_equal(prediction, core.predict(poisoned, packet, phase='fold'))
            poisoned.loc[49, list(core.info.spo.FEATURES)] = np.nan
            unknown = core.predict(poisoned, packet, phase='fold')
            assert unknown.loc[49, 'no_rank_advice'] and unknown.loc[49, 'advisory_rank'] == 50
            with pytest.raises(ValueError, match='phase'):
                core.predict(target, packet, phase='live')
            packets.append(packet)
    assert starts == ['matched', 'candidate']
    for field in ['fields', 'median', 'scale', 'train_rows', 'training_keys_sha256', 'relative_fields']:
        assert packets[0][field] == packets[1][field]
    assert len(packets[0]['learner']['coef']) == len(packets[1]['learner']['coef']) == 114


def test_future_or_wrong_training_identity_fails_before_fit(inputs):
    train = inputs.loc[inputs[core.DAY].lt('2025-04-01')]
    frozen = metadata(train)
    starts = []
    with pytest.raises(ValueError, match='future'):
        core.fit(inputs, 1, 'candidate', frozen, before_fit=lambda: starts.append(1))
    frozen['training_keys_sha256'] = 'wrong'
    with pytest.raises(ValueError, match='training keys'):
        core.fit(train, 1, 'candidate', frozen, before_fit=lambda: starts.append(1))
    assert starts == []


@pytest.mark.parametrize('problem', ['coef', 'clock', 'order', 'scale', 'arm'])
def test_corrupt_model_rejected_before_use(inputs, problem):
    train = inputs.loc[inputs[core.DAY].lt('2025-04-01')]
    # Synthetic fixture only; no financial inputs or budget claims.
    with threadpool_limits(limits=2):
        packet, _ = core.fit(train, 1, 'matched', metadata(train), before_fit=lambda: None)
    p = deepcopy(packet)
    if problem == 'coef':
        p['learner']['coef'][0] += 1
    elif problem == 'clock':
        p['cutoff'] = '2099-01-01'
    elif problem == 'order':
        p['relative_fields'] = list(reversed(p['relative_fields']))
    elif problem == 'scale':
        p['relative_scale'][0] = 0
    else:
        p['arm'] = 'ridge'
    if problem != 'coef':
        p['payload_sha256'] = core.info.spo.sha({k: v for k, v in p.items() if k != 'payload_sha256'})
    with pytest.raises(ValueError):
        core.validate_model(p)


def test_six_starts_no_implicit_retry_and_foreign_claims(tmp_path):
    claims = core.FitClaims(tmp_path, 'a' * 64)
    for f in (1, 2, 3):
        for arm in core.ARMS:
            claims.write(arm, f)
            claims.write(arm, f, 'b' * 64)
    with pytest.raises(ValueError, match='six-start'):
        claims.write('candidate', 3)
    with pytest.raises(ValueError, match='unregistered'):
        claims.write('hgb', 1)
    (tmp_path / 'foreign_started.json').write_text('{}')
    with pytest.raises(ValueError, match='foreign'):
        claims.write('matched', 1)


def test_partial_run_fails_before_any_resource_or_label_read(tmp_path, monkeypatch):
    (tmp_path / 'study/preregistered').mkdir(parents=True)
    (tmp_path / 'study/preregistered/protocol.json').write_text('{}')
    (tmp_path / 'study/fit_claims').mkdir()
    (tmp_path / 'study/fit_claims/matched1_started.json').write_text('{}')
    monkeypatch.setattr(runner, 'OUTPUT', tmp_path)
    monkeypatch.setattr(runner, 'verify', lambda _: None)
    monkeypatch.setattr(runner.prior, 'resource_admission', lambda: pytest.fail('no further work after partial start'))
    with pytest.raises(ValueError, match='partial STARTED'):
        runner.run()


def test_decision_keeps_native_target_and_not_evaluable():
    good = dict(statistics=dict(mean_bps=110), tail_delta_bps=0, unknown_excess=0,
                coverage=.9, intervention_days=100, intervention_fraction=.9)
    assert core.decision(good, good, {'1': 110, '2': 100, '3': -1})['action'] == 'DESIGN_CONFIRMATION_ONLY'
    for mean, expected in [(-1, 'STOP_EXACT_CONTEXT_CANDIDATE'), (50, 'BELOW_TARGET_STOP'), (None, 'NOT_EVALUABLE')]:
        other = {**good, 'statistics': dict(mean_bps=mean)}
        assert core.decision(good, other, {})['action'] == expected
