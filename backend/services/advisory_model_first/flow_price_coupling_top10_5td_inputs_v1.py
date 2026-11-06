"""Frozen, projected five-session flow/price inputs; no labels or SQL."""
import hashlib
import json

import numpy as np
import pandas as pd

from backend.data_service.frozen_dataset import inside, stamp
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import node_path

CONTROL = ('fp_flow_D', 'fp_flow_share_D', 'fp_flow_weighted5', 'fp_flow_mean5',
    'fp_flow_sd5', 'fp_flow_positive_share5', 'fp_flow_trailing_positive5',
    'fp_flow_abs_step5', 'fp_intraday_D_bps', 'fp_intraday_mean5_bps', 'fp_intraday_sd5_bps')
NEW = 'fp_flow_intraday_cov5_bps'
FIELDS = (*CONTROL, NEW)
COLUMNS = dict(moneyflow=[f'mf_{size}_{side}_amt' for size in ('sm', 'md', 'lg', 'elg')
    for side in ('buy', 'sell')], daily_pv=['open', 'close'])
UNIT_PATH = 'reports/moneyflow_alias_repair_receipt.json'
UNIT_SHA = '83e575ff286fc15fa1b8a508e1c6c1cba5c5136791ae0e4213421b219c6680ff'
MAX_CELLS = 20_000_000


def file_sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for part in iter(lambda: stream.read(4*1024**2), b''):
            h.update(part)
    return h.hexdigest()


def sources(plan, expected=None):
    binding = plan['binding']
    root = node_path(binding['profile']['controller_paths']['candidate_root'])
    names = ('moneyflow', 'daily_pv', 'day_calendar', 'factor_meta')
    pins = {name: binding['files'][name] for name in names}
    paths = {name: inside(root, pin['path']) for name, pin in pins.items()}
    before = {name: list(stamp(path)) for name, path in paths.items()}
    if any(before[name][0] != pins[name]['size'] for name in names) or (expected is not None and before != expected):
        raise ValueError('frozen input generation differs')
    for name in ('day_calendar', 'factor_meta'):
        if file_sha(paths[name]) != pins[name]['sha256']:
            raise ValueError('small frozen input hash differs')
    unit_path = inside(root, UNIT_PATH)
    if file_sha(unit_path) != UNIT_SHA:
        raise ValueError('canonical unit receipt differs')
    unit = json.loads(unit_path.read_text())
    if (unit['moneyflow_after_sha256'] != pins['moneyflow']['sha256']
            or unit['unit_contract']['version'] != 'tushare_moneyflow_shares_yuan_v1'
            or unit['unit_contract']['factor_amount_unit'] != 'cny'):
        raise ValueError('unit receipt does not bind this moneyflow')
    return paths, pins, before


def measure(money, prices):
    money, prices = np.asarray(money), np.asarray(prices)
    if money.shape != (5, 8) or prices.shape != (5, 2):
        raise ValueError('expected exact five-session source projection')
    if money.dtype.kind not in 'fi' or prices.dtype.kind not in 'fi':
        raise ValueError('non-numeric or bool source')
    m, p = money.astype(float), prices.astype(float)
    if np.isinf(m).any() or np.isinf(p).any() or (m < 0).any() or (p[np.isfinite(p)] <= 0).any():
        raise ValueError('invalid known amount or price')
    out = dict.fromkeys(FIELDS, np.nan)
    out.update(fp_reason='UNKNOWN_MISSING_REQUIRED_SOURCE', fp_constant_flow=False, fp_constant_price=False)
    if np.isnan(m).any() or np.isnan(p).any():
        return out
    maximum = m.max()
    if maximum == 0:
        return dict(out, fp_reason='UNKNOWN_ZERO_LARGE_FLOW_MASS')
    scaled = m/maximum
    if ((m > 0) & (scaled == 0)).any():
        raise ValueError('nonzero amount lost in numeric scaling')
    buy, sell = scaled[:, 4]+scaled[:, 6], scaled[:, 5]+scaled[:, 7]
    gross, total = buy+sell, scaled.sum(axis=1)
    if (gross == 0).any() or total[-1] == 0:
        return dict(out, fp_reason='UNKNOWN_ZERO_LARGE_FLOW_MASS')
    imbalance = (buy-sell)/gross
    r = (np.log(p[:, 1])-np.log(p[:, 0]))*10000
    trailing = 0
    for value in imbalance[::-1]:
        if value <= 0:
            break
        trailing += 1
    values = [imbalance[-1], gross[-1]/total[-1], (buy-sell).sum()/gross.sum(),
        imbalance.mean(), imbalance.std(ddof=0), (imbalance > 0).mean(), trailing/5,
        np.abs(np.diff(imbalance)).mean(), r[-1], r.mean(), r.std(ddof=0),
        ((imbalance-imbalance.mean())*(r-r.mean())).mean()]
    if not np.isfinite(values).all():
        raise ValueError('nonfinite measurement')
    out.update(zip(FIELDS, map(float, values)))
    out.update(fp_reason='AVAILABLE', fp_constant_flow=bool(np.ptp(imbalance) == 0),
        fp_constant_price=bool(np.ptp(r) == 0))
    return out


def read_panel(plan, inputs, expected, budget):
    paths, pins, before = sources(plan, expected)
    calendar = pd.DatetimeIndex(pd.to_datetime(paths['day_calendar'].read_text().splitlines()))
    if calendar.has_duplicates or not calendar.is_monotonic_increasing:
        raise ValueError('invalid original calendar')
    days = pd.DatetimeIndex(inputs.decision_as_of_trade_date.unique()).sort_values()
    if not days.isin(calendar).all() or calendar.get_loc(days[0]) < 4:
        raise ValueError('five-session history unavailable')
    first, last = calendar[calendar.get_loc(days[0])-4], days[-1]
    members = sorted(set(inputs.instrument))
    condition = [f"datetime >= '{first.date()}'", f"datetime <= '{last.date()}'", f'instrument in {members!r}']
    frames, reads, consumed = {}, [], 0
    for name, cols in COLUMNS.items():
        chunks, digest = [], hashlib.sha256()
        with pd.HDFStore(paths[name], mode='r') as store:
            for part in store.select('data', where=condition, columns=cols, chunksize=10000):
                budget()
                consumed += len(part)*len(cols)
                if consumed > MAX_CELLS:
                    raise ValueError('numeric source budget exceeded')
                times = part.index.get_level_values('datetime')
                if (set(part.index.get_level_values('instrument'))-set(members)
                        or not ((times >= first) & (times <= last)).all() or not times.isin(calendar).all()):
                    raise ValueError('source outside registered date/member projection')
                if list(part.columns) != cols:
                    raise ValueError('source column projection differs')
                if any(dtype.kind not in 'fi' for dtype in part.dtypes):
                    raise ValueError('non-numeric or bool source column')
                digest.update(part.to_csv().encode())
                chunks.append(part)
        frame = pd.concat(chunks) if chunks else pd.DataFrame(columns=cols,
            index=pd.MultiIndex.from_arrays([[], []], names=['datetime', 'instrument']))
        if frame.index.has_duplicates:
            raise ValueError('duplicate source key')
        frames[name] = frame.reorder_levels(['datetime', 'instrument']).sort_index()
        reads.append(dict(source=name, rows=len(frame), columns=cols, slice_sha256=digest.hexdigest()))
    if before != {name: list(stamp(path)) for name, path in paths.items()}:
        raise ValueError('source changed during read')
    receipt = dict(binding_hash=plan['binding']['binding_hash'], pins=pins, source_stamps=before,
        reads=reads, consumed_numeric_cells=consumed, first=str(first.date()), last=str(last.date()),
        member_count=len(members), full_large_h5_sha256_recomputed=False,
        identity_semantics='PINNED_HASH_SIZE_STABLE_STAT_AND_CONSUMED_SLICES',
        unit_receipt_sha256=UNIT_SHA, sql_count=0, source_label_arrays_decoded=0)
    return frames, calendar, receipt


def attach(inputs, frames, calendar, budget=lambda: None, progress=lambda event: None):
    """At most 30,500 original rows; project 5 x <=100 members per D, join by original unique index."""
    from backend.services.advisory_model_first.peer_residual_top10_5td_v1 import validate_population
    validate_population(inputs)
    if len(inputs) > 30500 or inputs.index.has_duplicates or inputs.empty:
        raise ValueError('input row/index budget differs')
    calendar = pd.DatetimeIndex(calendar)
    if calendar.has_duplicates or not calendar.is_monotonic_increasing:
        raise ValueError('invalid original calendar')
    results = []
    for day, group in inputs.groupby('decision_as_of_trade_date', sort=True):
        budget()
        position = calendar.get_loc(day)
        window = calendar[max(0, position-4):position+1]
        if len(window) != 5 or position+1 >= len(calendar) or not group.target_trade_date.eq(calendar[position+1]).all():
            raise ValueError('five-session or D/T calendar mismatch')
        members = sorted(group.instrument.unique())
        keys = pd.MultiIndex.from_product([window, members], names=['datetime', 'instrument'])
        # Only this D's source cells can reach numeric validation or the formula.
        projected = {name: frame.reindex(keys).to_numpy().reshape(5, len(members), len(COLUMNS[name]))
            for name, frame in frames.items()}
        values = {symbol: measure(projected['moneyflow'][:, j], projected['daily_pv'][:, j])
            for j, symbol in enumerate(members)}
        block = pd.DataFrame([values[s] for s in group.instrument], index=group.index)
        results.append(block)
        if len(results) % 50 == 0:
            progress(dict(phase='FEATURE_DAYS', days=len(results)))
    result = inputs.join(pd.concat(results).sort_index())
    if not result[list(IDENTITY)].equals(inputs[list(IDENTITY)]):
        raise ValueError('original population changed')
    return result
