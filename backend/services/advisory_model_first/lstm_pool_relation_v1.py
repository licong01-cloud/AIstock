"""Fixed local-peer representation on the original Top50, never native selection.

F-961..F-967: advisory_lstm_pool_relation_v1_f1_design_20261106.md.
"""
from datetime import datetime, timezone
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from . import lstm_factor_nonlinear_v1 as previous

info = previous.info
DAY, KEY, GROUP, FEATURES = info.DAY, info.KEY, info.GROUP, info.FEATURES
ARMS = ('matched', 'candidate')
RELATIVE = ('ret_5', 'atr14_close', 'volume_ratio_5_to_20')
PARAMS = dict(previous.PARAMS['ridge'])
CONTRACT = dict(schema='lstm_pool_relation_v1', hypothesis='LSTM_FACTOR_NEIGHBOR_CONTEXT_FIXED5_V1',
    target=info.prior.LSTM, fields=FEATURES, neighbor_fields=info.EXTRA, relative_fields=RELATIVE,
    folds=info.spo.base.FOLDS, params=PARAMS, arms=ARMS, k=5, common_factor_fraction=.8,
    distance='MEAN_SQUARED_TRAIN_SCALED_COMMON_ACTIVE_FACTORS', tie='ORIGINAL_RANK_THEN_INSTRUMENT',
    development=info.CONTRACT['development'], stress=info.CONTRACT['stress'],
    max_new_fits=6, prior_fits=257, old_index_count=1, final_fit=False, target_bps=100.,
    label='GROUP_CENTERED_FIXED5_NET_BPS', support='ORIGINAL_EIGHT_UNCHANGED',
    score='UNCALIBRATED_RELATIVE_DECISION_SCORE', production_ready=False, independent_confirmation=False)


def encoding_valid(meta):
    fields = meta.get('fields', [])
    if not fields or fields != [f for f in FEATURES if f in fields]:
        raise ValueError('encoding field order/duplicates differ')
    for name in ('median', 'scale'):
        values = meta[name]
        if len(values) != len(fields) or any(type(v) not in (int, float) or not math.isfinite(v) for v in values):
            raise ValueError('invalid encoding numeric values')
    if any(v <= 0 for v in meta['scale']):
        raise ValueError('nonpositive encoding scale')


def contexts(inputs, encoding):
    """Construct both local contexts before any financial eligibility projection."""
    info.check_inputs(inputs)
    info.numeric(inputs)
    encoding_valid(encoding)
    fields = [f for f in encoding['fields'] if f in info.EXTRA]
    positions = [encoding['fields'].index(f) for f in fields]
    median, scale = (np.asarray(encoding[k], float)[positions] for k in ('median', 'scale'))
    result = inputs.loc[:, [*KEY, 'selection_effective_rank']].copy().reset_index(drop=True)
    data = inputs.reset_index(drop=True)
    for arm in ARMS:
        result[arm + '_neighbors'] = ''
        result[arm + '_unknown_reason'] = 'INSUFFICIENT_COMMON_FACTOR_PEERS'
        for f in RELATIVE:
            result[arm + '__' + f] = np.nan
    for _, group in data.groupby(GROUP, sort=False):
        raw = info.numeric(group, fields)
        with np.errstate(over='raise', invalid='raise'):
            normalized = (raw - median) / scale
            if np.isinf(normalized).any():
                raise ValueError('normalized factor overflow')
            values = group.loc[:, RELATIVE].to_numpy(float)
            rank = group.selection_effective_rank.to_numpy()
            names = group.instrument.to_numpy()
            for i in range(50):
                common = np.isfinite(normalized[i]) & np.isfinite(normalized)
                allowed = np.flatnonzero((common.sum(axis=1) >= math.ceil(.8 * len(fields))) & (np.arange(50) != i))
                if not fields or len(allowed) < 5:
                    continue
                distance = {int(j): float(np.mean((normalized[i, common[j]] - normalized[j, common[j]]) ** 2))
                            for j in allowed}
                for arm in ARMS:
                    order = sorted(allowed, key=lambda j: (distance[int(j)] if arm == 'candidate' else abs(rank[j] - rank[i]),
                                                          rank[j], names[j]))[:5]
                    out_index = group.index[i]
                    result.at[out_index, arm + '_neighbors'] = '|'.join(map(str, names[order]))
                    missing = False
                    for k, f in enumerate(RELATIVE):
                        if np.isfinite(values[i, k]) and np.isfinite(values[order, k]).all():
                            value = float(values[i, k] - values[order, k].mean())
                            if not math.isfinite(value):
                                raise ValueError('relative context overflow')
                            result.at[out_index, arm + '__' + f] = value
                        else:
                            missing = True
                    result.at[out_index, arm + '_unknown_reason'] = ('MISSING_SELECTED_PEER_OR_OWN_VALUE'
                                                                    if missing else 'AVAILABLE')
    return result


def relative_raw(context, arm):
    if arm not in ARMS:
        raise ValueError('unregistered arm')
    return context[[arm + '__' + f for f in RELATIVE]].to_numpy(float)


def validate_model(packet):
    arm, fold = packet.get('arm'), packet.get('fold')
    if arm not in ARMS or type(fold) is not int or fold not in (1, 2, 3):
        raise ValueError('unregistered arm/fold')
    if (packet.get('schema') != CONTRACT['schema'] or packet.get('contract_sha256') != info.spo.sha(CONTRACT)
            or packet.get('payload_sha256') != info.spo.sha({k: v for k, v in packet.items() if k != 'payload_sha256'})
            or packet.get('target') != info.prior.LSTM or packet.get('params') != PARAMS
            or packet.get('cutoff') != info.spo.base.FOLDS[fold - 1][0]):
        raise ValueError('model identity/hash/parameters/clock differ')
    encoding_valid(packet)
    fields = packet['relative_fields']
    if fields != [f for f in RELATIVE if f in fields]:
        raise ValueError('relative field order differs')
    for name in ('relative_median', 'relative_scale'):
        values = packet[name]
        if len(values) != len(fields) or any(type(v) not in (int, float) or not math.isfinite(v) for v in values):
            raise ValueError('invalid relative encoding')
    if any(v <= 0 for v in packet['relative_scale']):
        raise ValueError('invalid relative scale')
    coef = packet['learner']['coef']
    if len(coef) != 2 * (len(packet['fields']) + len(fields)) or any(
            type(v) not in (int, float) or not math.isfinite(v) for v in coef):
        raise ValueError('invalid coefficients')


def matrix(inputs, context, meta, arm):
    pd.testing.assert_frame_equal(inputs[KEY].reset_index(drop=True), context[KEY].reset_index(drop=True), check_dtype=False)
    raw = info.numeric(inputs, meta['fields'])
    base = info.transform(raw, np.asarray(meta['median']), np.asarray(meta['scale']))
    positions = [RELATIVE.index(f) for f in meta['relative_fields']]
    rel = relative_raw(context, arm)[:, positions]
    block = info.transform(rel, np.asarray(meta['relative_median']), np.asarray(meta['relative_scale']))
    return info.spo.group_center(np.column_stack((base, block)), inputs)


def fit(rows, fold, arm, frozen_encoder, *, before_fit, budget=lambda: None):
    if arm not in ARMS:
        raise ValueError('unregistered arm')
    _, y, meta = previous.training(rows, fold, frozen_encoder['training_keys_sha256'])
    for name in ('fields', 'median', 'scale', 'training_keys_sha256', 'training_support', 'train_rows', 'train_days', 'cutoff'):
        if meta[name] != frozen_encoder[name]:
            raise ValueError('original training encoding/identity differs: ' + name)
    whole_context = contexts(rows, meta)
    part, _ = info.spo.training(rows, meta['cutoff'])
    selected = part[KEY].merge(whole_context, on=KEY, how='left', validate='one_to_one')
    raws = {a: relative_raw(selected, a) for a in ARMS}
    active = {a: np.isfinite(v).any(axis=0) for a, v in raws.items()}
    if not np.array_equal(active['candidate'], active['matched']):
        raise ValueError('asymmetric all-missing relative fields')
    mask = active[arm]
    if mask.any():
        _, median, scale = info.encoding(raws[arm][:, mask])
    else:
        median, scale = np.array([]), np.array([])
    meta.update(relative_fields=[f for f, keep in zip(RELATIVE, mask, strict=True) if keep],
        relative_median=median.tolist(), relative_scale=scale.tolist(),
        inactive_relative_fields=[f for f, keep in zip(RELATIVE, mask, strict=True) if not keep])
    x = matrix(part, selected, meta, arm)
    estimator = previous.factory('ridge')
    budget()
    before_fit()
    estimator.fit(x, y, sample_weight=np.ones(len(y)))
    budget()
    values = x @ estimator.coef_
    np.testing.assert_allclose(values, estimator.predict(x), rtol=1e-11, atol=1e-8)
    lhs, rhs = x.T @ x + 100 * np.eye(x.shape[1]), x.T @ y
    residual = float(np.max(np.abs(lhs @ estimator.coef_ - rhs)) / max(1., np.max(np.abs(rhs))))
    if residual > 1e-8:
        raise ValueError('normal equation residual exceeds tolerance')
    packet = dict(schema=CONTRACT['schema'], contract_sha256=info.spo.sha(CONTRACT), target=info.prior.LSTM,
        arm=arm, fold=fold, params=PARAMS, learner=dict(coef=estimator.coef_.tolist()), **meta,
        certificate=dict(relative_normal_equation_residual=residual, official_predict_parity=True,
                         train_mse=float(np.mean((values - y) ** 2))), production_ready=False)
    packet['payload_sha256'] = info.spo.sha(packet)
    validate_model(packet)
    return packet, estimator


def predict(inputs, packet, *, phase, oracle=None):
    validate_model(packet)
    _, first, last = info.spo.base.FOLDS[packet['fold'] - 1]
    if phase == 'stress' and packet['fold'] == 3:
        first, last = CONTRACT['stress']
    elif phase != 'fold':
        raise ValueError('registered offline phase required')
    if not inputs[DAY].between(pd.Timestamp(first), pd.Timestamp(last)).all():
        raise ValueError('prediction outside fixed phase')
    context = contexts(inputs, packet)
    eligible = np.isfinite(info.spo.numeric(inputs)).any(axis=1)
    active = inputs.loc[eligible].reset_index(drop=True)
    x = matrix(active, context.loc[eligible].reset_index(drop=True), packet, packet['arm']) if eligible.any() else np.empty((0, len(packet['learner']['coef'])))
    values = x @ np.asarray(packet['learner']['coef'])
    if oracle is not None and len(x):
        np.testing.assert_allclose(values, oracle.predict(x), rtol=1e-11, atol=1e-8)
    if len(values):
        values = info.spo.group_center(values[:, None], active).ravel()
    if not np.isfinite(values).all():
        raise ValueError('nonfinite scores')
    result = inputs.loc[:, [*info.spo.IDENTITY, 'parent_score', 'source_id']].copy().reset_index(drop=True)
    result['decision_score'], result['no_rank_advice'] = np.nan, ~eligible
    result['advisory_rank'] = result.selection_effective_rank.copy()
    result.loc[eligible, 'decision_score'] = values
    for _, group in result.groupby(GROUP, sort=False):
        known = group.loc[~group.no_rank_advice]
        order = known.sort_values(['decision_score', 'selection_effective_rank'], ascending=[False, True], kind='stable').index
        result.loc[order, 'advisory_rank'] = sorted(known.selection_effective_rank)
    result['selected'] = result.advisory_rank.le(10)
    info.spo.evaluation.same_population(inputs, result)
    return result


def summarize(panel, matched):
    result = info.summarize(panel, matched)
    result['versus_rank_neighbor'] = result.pop('versus_eight_fields')
    return result


def decision(development, stress, fold_means):
    result = info.decision(development, stress, fold_means)
    result['action'] = dict(STOP_THIS_FACTOR_CANDIDATE='STOP_EXACT_CONTEXT_CANDIDATE',
        BELOW_BUSINESS_TARGET_NO_EXPANSION='BELOW_TARGET_STOP',
        DESIGN_INDEPENDENT_CONFIRMATION_ONLY='DESIGN_CONFIRMATION_ONLY').get(result['action'], result['action'])
    if any(r['statistics']['mean_bps'] is None for r in (development, stress)):
        result['action'] = 'NOT_EVALUABLE'
    return result


class FitClaims:
    """Create-only starts copied from the accepted six-fit pattern, new arm identities."""
    def __init__(self, root, digest):
        self.root, self.digest = Path(root), digest

    def write(self, arm, fold, stage_sha256=None):
        if arm not in ARMS or type(fold) is not int or fold not in (1, 2, 3):
            raise ValueError('unregistered fit identity')
        self.root.mkdir(parents=True, exist_ok=True)
        starts = list(self.root.glob('*_started.json'))
        allowed = {f'{a}{f}_started.json' for a in ARMS for f in (1, 2, 3)}
        for path in starts:
            if path.name not in allowed or json.loads(path.read_bytes())['plan_sha256'] != self.digest:
                raise ValueError('foreign fit claim')
        path = self.root / f'{arm}{fold}_started.json'
        if stage_sha256 is None:
            if len(starts) >= 6:
                raise ValueError('six-start budget exhausted')
            payload = dict(status='STARTED', arm=arm, fold=fold, plan_sha256=self.digest,
                           observed_at=datetime.now(timezone.utc).isoformat())
        else:
            if not path.exists() or len(stage_sha256) != 64:
                raise ValueError('finish requires valid prior start')
            path = self.root / f'{arm}{fold}_finished.json'
            payload = dict(status='FINISHED', arm=arm, fold=fold, plan_sha256=self.digest, stage_sha256=stage_sha256)
        with path.open('x', encoding='utf-8') as handle:
            json.dump(payload, handle, indent=2)
