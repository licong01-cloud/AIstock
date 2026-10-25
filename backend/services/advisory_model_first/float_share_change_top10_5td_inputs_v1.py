"""Lagged share endpoints only; frozen source and label-free screen identity."""
import hashlib
import json

import numpy as np
import pandas as pd

from backend.data_service.frozen_dataset import inside, stamp
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import node_path

CONTROL = ('ff_fraction_lag1', 'ff_log_total_shares_lag1', 'ff_log_total_growth5')
NEW = 'ff_fraction_delta5_bps'
FIELDS = (*CONTROL, NEW)
COLUMNS = {'daily_basic': ['db_total_share', 'db_float_share']}
MAX_CELLS = 4_000_000
SCREEN_ROOT = 'X:/AIstock_temp/ADVISORY/p6-float-share-change-20261022'
SCREEN_SHA = '435e2b1c4928139e26998c6abb88518dd5c6339fe10389789e703a0deaafe571'


def file_sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for part in iter(lambda: stream.read(4*1024**2), b''):
            h.update(part)
    return h.hexdigest()


def screen_evidence():
    root = node_path(SCREEN_ROOT)
    if file_sha(root/'screen_receipt.json') != SCREEN_SHA:
        raise ValueError('input screen receipt identity differs')
    receipt = json.loads((root/'screen_receipt.json').read_text())
    pairs = {'input_spec.json':'spec_sha256', 'screen.py':'code_sha256',
             'task_plan.md':'plan_sha256', 'factor_audit.json':'factor_audit_sha256',
             'screen_rows.parquet':'output_sha256'}
    if any(file_sha(root/name) != receipt[key] for name,key in pairs.items()):
        raise ValueError('input screen evidence drift')
    if receipt['decision'] != 'INPUT_SUPPORTED_FOR_F1' or receipt['fit_count'] or receipt['label_reads']:
        raise ValueError('screen is not the registered label-free support result')
    return receipt


def sources(plan, expected=None):
    evidence = screen_evidence()
    binding = plan['binding']
    if file_sha(node_path(SCREEN_ROOT)/'input_spec.json') != evidence['spec_sha256']:
        raise ValueError('input specification drift')
    spec = json.loads((node_path(SCREEN_ROOT)/'input_spec.json').read_text())
    if binding['binding_hash'] != spec['binding_hash']:
        raise ValueError('screen and study bindings differ')
    root = node_path(binding['profile']['controller_paths']['candidate_root'])
    names = ('daily_basic', 'day_calendar', 'factor_meta')
    pins = {name: binding['files'][name] for name in names}
    paths = {name: inside(root, pin['path']) for name, pin in pins.items()}
    before = {name: list(stamp(path)) for name,path in paths.items()}
    if any(before[name][0] != pins[name]['size'] for name in names) or (expected is not None and before != expected):
        raise ValueError('frozen input generation differs')
    for name in names:
        if pins[name] != evidence['source_pins'][name] or before[name] != evidence['source_stamps'][name]:
            raise ValueError('input no longer matches verified screen generation')
    for name in ('day_calendar','factor_meta'):
        if file_sha(paths[name]) != pins[name]['sha256']:
            raise ValueError('small frozen input hash differs')
    return paths,pins,before


def measure(shares):
    a = np.asarray(shares)
    if a.shape != (2,2) or a.dtype.kind not in 'fi':
        raise ValueError('expected two numeric [total,float] endpoints')
    a = a.astype(float)
    if np.isinf(a).any() or (a[np.isfinite(a)] <= 0).any() or (a[:,1] > a[:,0]).any():
        raise ValueError('invalid known share value')
    out = dict.fromkeys(FIELDS,np.nan)
    out['ff_reason'] = 'UNKNOWN_ENDPOINT'
    if np.isnan(a).any():
        return out
    fraction = a[:,1]/a[:,0]
    values = [fraction[1],np.log(a[1,0])+np.log(10000),
              np.log(a[1,0])-np.log(a[0,0]),10000*(fraction[1]-fraction[0])]
    if not np.isfinite(values).all():
        raise ValueError('nonfinite share measurement')
    out.update(zip(FIELDS,map(float,values)))
    out['ff_reason'] = 'AVAILABLE'
    return out


def read_panel(plan, inputs, expected, budget):
    paths,pins,before = sources(plan,expected)
    calendar = pd.DatetimeIndex(pd.to_datetime(paths['day_calendar'].read_text().splitlines()))
    if calendar.has_duplicates or not calendar.is_monotonic_increasing:
        raise ValueError('invalid original calendar')
    days = pd.DatetimeIndex(inputs.decision_as_of_trade_date.unique()).sort_values()
    if len(days)==0 or not days.isin(calendar).all() or calendar.get_loc(days[0]) < 6:
        raise ValueError('six-session history unavailable')
    first,last = calendar[calendar.get_loc(days[0])-6],calendar[calendar.get_loc(days[-1])-1]
    members = sorted(set(inputs.instrument))
    condition = [f"datetime >= '{first.date()}'",f"datetime <= '{last.date()}'",f'instrument in {members!r}']
    frames,reads,consumed = {},[],0
    for name,cols in COLUMNS.items():
        chunks,digest = [],hashlib.sha256()
        with pd.HDFStore(paths[name],mode='r') as store:
            for part in store.select('data',where=condition,columns=cols,chunksize=10000):
                budget()
                consumed += part.size
                if consumed > MAX_CELLS:
                    raise ValueError('numeric source budget exceeded')
                times = part.index.get_level_values('datetime')
                if (set(part.index.get_level_values('instrument'))-set(members)
                        or not ((times >= first)&(times <= last)).all() or not times.isin(calendar).all()):
                    raise ValueError('source outside registered date/member projection')
                if list(part.columns)!=cols or any(dtype.kind not in 'fi' for dtype in part.dtypes):
                    raise ValueError('source column/type projection differs')
                digest.update(part.to_csv().encode())
                chunks.append(part)
        frame = pd.concat(chunks) if chunks else pd.DataFrame(columns=cols,
            index=pd.MultiIndex.from_arrays([[],[]],names=['datetime','instrument']))
        if frame.index.has_duplicates:
            raise ValueError('duplicate source key')
        frames[name] = frame.reorder_levels(['datetime','instrument']).sort_index()
        reads.append(dict(source=name,rows=len(frame),columns=cols,slice_sha256=digest.hexdigest()))
    if before != {name:list(stamp(path)) for name,path in paths.items()}:
        raise ValueError('source changed during read')
    receipt = dict(binding_hash=plan['binding']['binding_hash'],pins=pins,source_stamps=before,
        reads=reads,consumed_numeric_cells=consumed,first=str(first.date()),last=str(last.date()),
        member_count=len(members),full_large_h5_sha256_recomputed=False,
        identity_semantics='SCREEN_FULL_HASH_PIN_SIZE_STABLE_STAT_AND_CONSUMED_SLICES',
        input_screen_sha256=SCREEN_SHA,sql_count=0,source_label_arrays_decoded=0)
    return frames,calendar,receipt


def attach(inputs, frames, calendar, budget=lambda: None, progress=lambda event: None):
    """At most 30,500 rows; only D-6/D-1 cells reach value validation."""
    from backend.services.advisory_model_first.peer_residual_top10_5td_v1 import validate_population
    validate_population(inputs)
    if len(inputs)>30500 or inputs.index.has_duplicates or inputs.empty:
        raise ValueError('input row/index budget differs')
    calendar = pd.DatetimeIndex(calendar)
    if calendar.has_duplicates or not calendar.is_monotonic_increasing:
        raise ValueError('invalid original calendar')
    results = []
    for day,group in inputs.groupby('decision_as_of_trade_date',sort=True):
        budget()
        position = calendar.get_loc(day)
        if position < 6 or position+1 >= len(calendar) or not group.target_trade_date.eq(calendar[position+1]).all():
            raise ValueError('endpoint or D/T calendar mismatch')
        endpoints = calendar[[position-6,position-1]]
        members = sorted(group.instrument.unique())
        keys = pd.MultiIndex.from_product([endpoints,members],names=['datetime','instrument'])
        projected = frames['daily_basic'].reindex(keys).to_numpy().reshape(2,len(members),2)
        values = {symbol:measure(projected[:,j]) for j,symbol in enumerate(members)}
        results.append(pd.DataFrame([values[s] for s in group.instrument],index=group.index))
        if len(results)%50 == 0:
            progress(dict(phase='FEATURE_DAYS',days=len(results)))
    result = inputs.join(pd.concat(results).sort_index())
    if not result[list(IDENTITY)].equals(inputs[list(IDENTITY)]):
        raise ValueError('original population changed')
    return result


def verify_screen_alignment(frame):
    receipt = screen_evidence()
    original = pd.read_parquet(node_path(SCREEN_ROOT)/'screen_rows.parquet').rename(
        columns={'float_fraction_delta5_bps': NEW})
    current = frame.loc[frame.decision_as_of_trade_date.between('2024-07-04','2025-03-31'),
                        [*IDENTITY,NEW,'ff_reason']]
    if len(current)!=receipt['original_rows']:
        raise ValueError('screen comparison original population differs')
    compare = original.merge(current,on=list(IDENTITY),how='outer',validate='one_to_one',indicator=True,
                             suffixes=('_screen','_full'))
    if not compare['_merge'].eq('both').all():
        raise ValueError('screen comparison identity differs')
    if (not np.array_equal(compare[NEW+'_screen'].to_numpy(),compare[NEW+'_full'].to_numpy(),equal_nan=True)
            or not compare.reason.equals(compare.ff_reason)):
        raise ValueError('screen and full preparation differ')
    return dict(rows=len(compare),exact_equal_including_nan=True,screen_receipt_sha256=SCREEN_SHA)
