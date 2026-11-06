"""D-only serial-pressure research inputs; no SQL or native selection changes."""
from __future__ import annotations

import base64
from contextlib import ExitStack
import hashlib
import json
import zlib

import numpy as np
import pandas as pd

from backend.data_service.frozen_dataset import FrozenDataset, inside, stamp
from backend.services.advisory_model_first.generic_daily_price_input_v1 import FEATURES as DAILY
from backend.services.advisory_model_first.generic_minute_price_5td_contracts_v1 import MINUTE_FEATURES
from backend.services.advisory_model_first.generic_ordered_path_price_5td_contracts_v1 import ORDERED_FEATURES
from backend.services.advisory_model_first.generic_ordered_path_price_5td_source_v1 import aggregate_ordered_path_v1
from backend.services.advisory_model_first.generic_volume_path_price_5td_contracts_v1 import VOLUME_FEATURES
from backend.services.advisory_model_first.generic_volume_path_price_5td_source_v1 import aggregate_d_volume_path_features_v1
from backend.services.advisory_model_first.joint_risk_top10_5td_inputs_v1 import KEY, ROSTER, validate_roster
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY_SHA256 as LABEL_POLICY
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

OLD_MINUTE = (*MINUTE_FEATURES[:4], *VOLUME_FEATURES, *ORDERED_FEATURES)
MATCHED = (*DAILY, *OLD_MINUTE, 'native_score_z', 'native_rank_fraction', 'nonzero_return_fraction', 'serial_coverage')
CANDIDATE = (*MATCHED, 'serial_correlation', 'serial_closing_pressure')
ARMS = ('native', 'matched', 'candidate')
FIELDS = ('open', 'high', 'low', 'close', 'volume')
CONTRACT = dict(schema='serial_price_pressure_top10_5td_v1', hypothesis='SERIAL_PRICE_PRESSURE_TOP10_5TD_1',
    objective_contract='ALPHA_RANKING', study_type='LEARNABILITY_AUDIT', decision_use='NAVIGATION_ONLY',
    matched_fields=MATCHED, candidate_fields=CANDIDATE, alpha=10., solver='lsqr', tol=1e-6,
    max_iter=2000, fit_intercept=False, max_fits=2, planned_candidates=1, candidate_topk=20, topk=10,
    max_replacements=2, minimum_triple_coverage=.8, label_policy_sha256=LABEL_POLICY,
    valuation_policy_sha256=VALUATION_POLICY_SHA256, buy_bps=.95, sell_bps=5.95, horizon_sessions=5,
    max_rows=12200, max_symbols=1253, max_slots=300, max_seconds=1800, max_rss_bytes=1024**3,
    block_sessions=5, bootstrap_count=2000, bootstrap_seed=20261021, endpoint_confidence=.975,
    minimum_complete_days=50, minimum_complete_fraction=.8, minimum_gain_bps=5.,
    minimum_intervention_days=20, minimum_package_intervention_days=10)
POLICY_SHA256 = sha(CONTRACT)


def serial_stat(slots, close, *, day):
    """Pearson correlation of adjacent one-minute returns, never across gaps/lunch."""
    slots, close = pd.DatetimeIndex(slots), np.asarray(close, float)
    minutes = np.asarray(slots.hour*60+slots.minute)
    legal = ((minutes >= 570) & (minutes <= 690)) | ((minutes >= 780) & (minutes <= 900))
    if (slots.tz is not None or slots.hasnans or slots.has_duplicates or not slots.is_monotonic_increasing
            or len(slots) > CONTRACT['max_slots'] or close.shape != (len(slots),)
            or not (slots.normalize() == pd.Timestamp(day).normalize()).all() or not legal.all()
            or (slots.second != 0).any() or (slots.microsecond != 0).any() or (slots.nanosecond != 0).any()):
        raise ValueError('serial D calendar/clock alignment differs')
    if np.isinf(close).any() or (close[np.isfinite(close)] <= 0).any():
        raise ValueError('invalid known minute close')
    adjacent = np.diff(slots.asi8) == pd.Timedelta(minutes=1).value
    valid = np.isfinite(close)
    observed_edge = adjacent & valid[1:] & valid[:-1]
    returns = np.full(max(0, len(close)-1), np.nan)
    returns[observed_edge] = np.log(close[1:][observed_edge])-np.log(close[:-1][observed_edge])
    possible = adjacent[1:] & adjacent[:-1]
    observed = possible & observed_edge[1:] & observed_edge[:-1]
    a, b = returns[1:][observed], returns[:-1][observed]
    coverage = float(observed.sum()/possible.sum()) if possible.any() else 0.
    result = dict(serial_correlation=np.nan, serial_coverage=coverage, serial_pairs=int(observed.sum()),
        serial_possible=int(possible.sum()), nonzero_return_fraction=float(np.count_nonzero(returns[observed_edge])/observed_edge.sum())
        if observed_edge.any() else np.nan, serial_reason='INSUFFICIENT_OBSERVED_ADJACENT_TRIPLES')
    if coverage < CONTRACT['minimum_triple_coverage'] or len(a) < 2:
        return result
    ac, bc = a-a.mean(), b-b.mean()
    denominator = np.sqrt(np.dot(ac, ac)*np.dot(bc, bc))
    if denominator == 0:
        result['serial_reason'] = 'CONSTANT_RETURN_SERIAL_VARIANCE_UNDEFINED'
        return result
    value = float(np.dot(ac, bc)/denominator)
    if not np.isfinite(value) or abs(value) > 1+1e-12:
        raise ValueError('serial correlation arithmetic is invalid')
    result.update(serial_correlation=float(np.clip(value, -1, 1)), serial_reason='OBSERVED_D_SERIAL_DEPENDENCE')
    return result


def minute_features(slots, arrays, *, day):
    if set(arrays) != set(FIELDS):
        raise ValueError('OHLCV fields differ')
    serial = serial_stat(slots, arrays['close'], day=day)
    volume = aggregate_d_volume_path_features_v1(slots=slots, arrays={**arrays,
        **{name: np.full(len(slots), np.nan) for name in ('amount', 'limit_up', 'limit_down')}})
    ordered = aggregate_ordered_path_v1(slots=slots, arrays=arrays)
    values = {name: (ordered if name in ORDERED_FEATURES else volume)[name] for name in OLD_MINUTE}
    return {**values, **serial, 'serial_closing_pressure': serial['serial_correlation']*values['closing_30m_return_bps']/10000}


def attach_features(roster, daily, minute):
    """One-to-one attachment; original order, score dtype and group size survive."""
    if (roster.duplicated(KEY).any() or daily.duplicated(KEY).any()
            or minute.duplicated(['instrument', 'decision_as_of_trade_date']).any()):
        raise ValueError('duplicate input identity')
    joined = roster.loc[:, list(ROSTER)].merge(daily.loc[:, [*KEY, *DAILY]], on=KEY, how='left', validate='one_to_one', indicator=True)
    if not joined['_merge'].eq('both').all() or len(joined) != len(roster):
        raise ValueError('daily source population differs')
    joined = joined.drop(columns='_merge').merge(minute, on=['instrument', 'decision_as_of_trade_date'],
        how='left', validate='many_to_one', indicator=True)
    if not joined['_merge'].eq('both').all():
        raise ValueError('minute source population differs')
    joined = joined.drop(columns='_merge')
    score = pd.to_numeric(joined.parent_score, errors='raise')
    if np.isinf(score).any():
        raise ValueError('nonfinite native score')
    groups = [joined.package_id, joined.decision_as_of_trade_date]
    sd = score.groupby(groups, sort=False).transform(lambda x: x.std(ddof=0))
    joined['native_score_z'] = (score-score.groupby(groups, sort=False).transform('mean'))/sd.replace(0, np.nan)
    joined['native_rank_fraction'] = (joined.selection_effective_rank-1)/19
    if (not joined.loc[:, list(ROSTER)].equals(roster.loc[:, list(ROSTER)].reset_index(drop=True))
            or np.isinf(joined.loc[:, list(CANDIDATE)].to_numpy(float)).any()):
        raise ValueError('native projection changed or invalid feature')
    return joined


def read_minute_inputs(binding, roster, *, run_id, budget=lambda: None):
    """Reuse each stock's five streams; seek only its required D slices in place."""
    if (not 0 < len(roster) <= CONTRACT['max_rows'] or roster.instrument.nunique() > CONTRACT['max_symbols']):
        raise ValueError('input population budget exceeded')
    pairs = roster[['instrument', 'decision_as_of_trade_date']].drop_duplicates()
    results, consumed, files, byte_count = [], hashlib.sha256(), [], 0
    with_dataset = FrozenDataset(binding, consumer_scope='ADVISORY', run_id=run_id)
    try:
        calendar_path = with_dataset.file('minute_calendar')
        calendar = pd.DatetimeIndex(pd.to_datetime(calendar_path.read_text(encoding='utf-8-sig').splitlines()))
        if calendar.hasnans or calendar.has_duplicates or not calendar.is_monotonic_increasing:
            raise ValueError('invalid minute calendar')
        dates = calendar.normalize()
        starts = np.r_[0, np.flatnonzero(dates[1:].to_numpy() != dates[:-1].to_numpy())+1]
        positions = {dates[a]: (int(a), int(b)) for a, b in zip(starts, np.r_[starts[1:], len(calendar)], strict=True)}
        minute_root = with_dataset.file('minute_meta_export').parent
        lease = {r[0]: tuple(r[1:]) for r in json.loads(zlib.decompress(base64.b64decode(binding['bin_stat_lease']['zlib_base64'])))}
        for symbol, group in pairs.groupby('instrument', sort=True):
            budget()
            with ExitStack() as stack:
                streams = {}
                for field in FIELDS:
                    relative = (minute_root.relative_to(with_dataset.root)/'features'/symbol.lower()/f'{field}.1min.bin').as_posix()
                    path = inside(with_dataset.root, relative)
                    before = stamp(path)
                    if lease.get(relative) != before or before[0] < 4 or before[0] % 4:
                        raise ValueError('minute stream differs from frozen lease')
                    stream = stack.enter_context(path.open('rb'))
                    raw = stream.read(4)
                    start = float(np.frombuffer(raw, dtype='<f4')[0])
                    length = before[0]//4-1
                    if not np.isfinite(start) or start < 0 or int(start) != start or start+length > len(calendar):
                        raise ValueError('invalid minute stream header/length')
                    streams[field] = (stream, int(start), length)
                    files.append(dict(path=relative, stat=list(before)))
                    consumed.update(relative.encode()+raw)
                    byte_count += 4
                for day in sorted(group.decision_as_of_trade_date):
                    if day not in positions:
                        raise ValueError('required D absent from frozen minute calendar')
                    lo, hi = positions[day]
                    indices = np.arange(lo, hi)
                    arrays = {}
                    consumed.update(pd.Timestamp(day).isoformat().encode())
                    for field, (stream, start, length) in streams.items():
                        selected = (indices >= start) & (indices < start+length)
                        values = np.full(len(indices), np.nan)
                        consumed.update(field.encode()+selected.tobytes())
                        if selected.any():
                            actual = indices[selected]
                            stream.seek(4*(1+int(actual[0])-start))
                            raw = stream.read(4*len(actual))
                            if len(raw) != 4*len(actual):
                                raise ValueError('truncated minute slice')
                            values[selected] = np.frombuffer(raw, dtype='<f4')
                            consumed.update(raw)
                            byte_count += len(raw)
                        arrays[field] = values
                    results.append(dict(instrument=symbol, decision_as_of_trade_date=day,
                        **minute_features(calendar[lo:hi], arrays, day=day)))
        for file in files:
            if stamp(inside(with_dataset.root, file['path'])) != tuple(file['stat']):
                raise ValueError('minute stream changed during input read')
        with_dataset.assert_unchanged()
        return pd.DataFrame(results), dict(binding_hash=binding['binding_hash'], files=files,
            consumed_bin_bytes=byte_count, consumed_slice_sha256=consumed.hexdigest(),
            identity_semantics='PINNED_METADATA_AND_STAT_LEASE_PLUS_ACTUAL_CONSUMED_SLICES_NOT_WHOLE_BIN_HASH',
            unique_stock_days=len(results), sql_count=0)
    finally:
        with_dataset.close()


__all__ = ['KEY', 'ROSTER', 'validate_roster', 'DAILY', 'MATCHED', 'CANDIDATE', 'ARMS', 'CONTRACT',
    'POLICY_SHA256', 'serial_stat', 'minute_features', 'attach_features', 'read_minute_inputs']
