"""BUG-2178 regression: exact linear order, frozen inputs, no training calls."""
from copy import deepcopy
from decimal import Decimal, localcontext
from fractions import Fraction

import numpy as np
import pandas as pd
import pytest

from scripts import advisory_lstm_linear_rank_replay as replay
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first import test_lstm_factor_nonlinear_v1 as fixtures


def decimal_scores(x, weights):
    with localcontext() as ctx:
        ctx.prec = 2200
        return [sum((Decimal.from_float(float(a)) * Decimal.from_float(float(b))
                     for a, b in zip(row, weights, strict=True)), Decimal(0)) for row in x]


def test_outlier_and_common_offset_do_not_destroy_other_order():
    x = np.column_stack((np.r_[1e20, np.zeros(49)], np.arange(50, dtype=float)))
    legacy = (x - x.mean(axis=0)) @ np.ones(2)
    assert len(np.unique(legacy[1:])) == 1
    exact = replay.exact_scores(x, np.ones(2))
    assert len(set(exact[1:])) == 49
    expected = decimal_scores(x, np.ones(2))
    assert sorted(range(50), key=exact.__getitem__) == sorted(range(50), key=expected.__getitem__)
    # A large shared feature contribution also requires exact accumulation.
    x[:, 0] = 1e20
    assert len(np.unique(x @ np.ones(2))) == 1
    assert replay.exact_scores(x, np.ones(2)) == [Fraction(10**20 + i) for i in range(50)]


def test_cancelling_products_and_subnormal_terms_match_decimal():
    x = np.array([[1e200, 1e200, 7.], [1e200, 1e200, 9.], [1e-200, 0., 0.]])
    weights = np.array([1e200, -1e200, 1.])
    expected = decimal_scores(x, weights)
    exact = replay.exact_scores(x, weights)
    assert exact[:2] == [7, 9]
    assert sorted(range(3), key=exact.__getitem__) == sorted(range(3), key=expected.__getitem__)
    assert replay.exact_scores([[1e-200]], [1e-200])[0] > 0


@pytest.mark.parametrize('x,w', [([[np.inf]], [1.]), ([[1.]], [np.nan]), ([[1., 2.]], [1.]), ([1.], [1.])])
def test_bad_linear_dimensions_or_nonfinite_rejected(x, w):
    with pytest.raises(ValueError):
        replay.exact_scores(x, w)


@pytest.fixture
def case():
    core = replay.core
    x = fixtures.inputs.__wrapped__()
    x = x.loc[x[core.DAY].eq('2025-04-01')].copy().reset_index(drop=True)
    for field in core.info.EXTRA:
        x[field] = np.arange(50, dtype=float) / 100
    x.loc[0, 'factor__VSTD5'] = 1e20
    x['ret_1'] = np.arange(50, dtype=float)
    weights = [0.] * 114
    weights[core.FEATURES.index('factor__VSTD5')] = 1.
    weights[core.FEATURES.index('ret_1')] = 1.
    packet = dict(schema=core.CONTRACT['schema'], contract_sha256=core.info.spo.sha(core.CONTRACT),
        target=core.info.prior.LSTM, arm='candidate', fold=1, params=core.PARAMS, cutoff='2025-03-31',
        fields=list(core.FEATURES), median=[0.] * 54, scale=[1.] * 54,
        relative_fields=list(core.RELATIVE), relative_median=[0.] * 3, relative_scale=[1.] * 3,
        learner=dict(coef=weights))
    packet['payload_sha256'] = core.info.spo.sha(packet)
    return x, packet


def test_corrected_predict_preserves_population_context_and_unknown_slots(case, monkeypatch):
    x, packet = case
    before = x.copy(deep=True)
    monkeypatch.setattr(replay.core, 'fit', lambda *a, **k: pytest.fail('fit is forbidden'))
    fixed, context = replay.predict(x, packet, phase='fold')
    old = replay.core.predict(x, packet, phase='fold')
    assert old.decision_score.iloc[1:].nunique() == 1
    assert fixed.ranking_score_exact.iloc[1:].nunique() == 49
    assert fixed.loc[fixed.selected, 'selection_effective_rank'].tolist() == [1, *range(42, 51)]
    pd.testing.assert_frame_equal(context, replay.core.contexts(x, packet))
    pd.testing.assert_frame_equal(before, x)
    x.loc[49, list(replay.core.info.spo.FEATURES)] = np.nan
    fixed, _ = replay.predict(x, packet, phase='fold')
    assert fixed.loc[49, 'no_rank_advice'] and fixed.loc[49, 'advisory_rank'] == 50
    assert pd.isna(fixed.loc[49, 'ranking_score_exact']) and fixed.selected.sum() == 10


def test_exact_ties_permutation_and_future_columns(case):
    x, packet = case
    packet['learner']['coef'] = [0.] * 114
    packet['payload_sha256'] = replay.core.info.spo.sha({k: v for k, v in packet.items() if k != 'payload_sha256'})
    a, _ = replay.predict(x, packet, phase='fold')
    np.testing.assert_array_equal(a.advisory_rank, a.selection_effective_rank)
    x['hypothetical_liquidation_net_bps'] = np.inf
    x['label_information_end'] = pd.Timestamp('2099-01-01')
    b, _ = replay.predict(x.iloc[::-1], packet, phase='fold')
    pd.testing.assert_frame_equal(a.sort_values(replay.core.KEY).reset_index(drop=True),
                                  b.sort_values(replay.core.KEY).reset_index(drop=True))


def test_all_unknown_and_wrong_clock_identity_fail_closed(case):
    x, packet = case
    x.loc[:, list(replay.core.info.spo.FEATURES)] = np.nan
    result, _ = replay.predict(x, packet, phase='fold')
    assert result.no_rank_advice.all() and result.ranking_score_exact.isna().all()
    np.testing.assert_array_equal(result.advisory_rank, result.selection_effective_rank)
    with pytest.raises(ValueError, match='phase'):
        replay.predict(x, packet, phase='live')
    with pytest.raises(ValueError):
        replay.predict(x.iloc[:-1], packet, phase='fold')
    broken = deepcopy(packet)
    broken['learner']['coef'][0] = 999.
    with pytest.raises(ValueError, match='hash'):
        replay.predict(x, broken, phase='fold')


def test_completed_replay_is_readonly_and_uses_source_verification(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(replay, 'OUTPUT', tmp_path)
    identity = dict(terminal_sha256=replay.TERMINAL)
    monkeypatch.setattr(replay, 'source_identity', lambda: identity)
    monkeypatch.setattr(replay, 'code_identity', lambda: {})
    monkeypatch.setattr(replay.frozen.prior, 'versions', lambda: {})
    contract = dict(schema='advisory_linear_rank_correction_v1', semantics=replay.SEMANTICS, source=identity,
        closure={}, versions={}, scope='FROZEN_POOL_MATCHED_AND_CANDIDATE_ONLY', new_fits=0,
        new_return_labels=0, original_result_unchanged=True, production_ready=False)
    replay.publish_stage(study_root=tmp_path, stage='evaluated', plan_sha256=replay.core.info.spo.sha(contract),
        parent_sha256=replay.TERMINAL, artifacts={'receipt.json': b'{}'})
    monkeypatch.setattr(replay.pd, 'read_parquet', lambda *a, **k: pytest.fail('completed replay must not decode inputs'))
    replay.run()
    assert 'REPLAY_REUSED' in capsys.readouterr().out
    (tmp_path / 'evaluated/receipt.json').write_text('corrupt')
    with pytest.raises(AdvisoryModelFirstError):
        replay.run()
