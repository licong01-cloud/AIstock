"""Bounded file-only risk inputs. No SQL, parent training or selection algorithms."""
from __future__ import annotations

import base64
import hashlib
import json
import re
import zlib

import numpy as np
import pandas as pd

from backend.data_service.frozen_dataset import FrozenDataset, inside, stamp
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

ROSTER = ('package_id', 'manifest_sha256', 'run_id', 'decision_as_of_trade_date',
          'target_trade_date', 'instrument', 'selection_effective_rank', 'candidate_group_size', 'parent_score')
KEY = ['package_id', 'decision_as_of_trade_date', 'instrument']
CONTRACT = dict(schema='advisory_joint_risk_top10_5td_v1', hypothesis='JOINT_RISK_TOP10_5TD_1',
    window_sessions=120, minimum_common_observations=60, horizon_sessions=5, topk=10,
    candidate_topk=20, max_replacements=2, estimator='sklearn.LedoitWolf',
    assume_centered=False, store_precision=False, block_size=1000, risk_basis='NATIVE_QFQ_CLOSE_CLOSE_5TD',
    missing='NO_FILL_NO_DROP_NO_RANK_ADVICE', max_updates=610, max_bytes=100*1024**2,
    max_seconds=1800, max_rss_bytes=1024**3, block_sessions=5, bootstrap_count=2000,
    bootstrap_seed=20261020, minimum_skill_days=50, minimum_skill_coverage=.8,
    minimum_intervention_stocks=30, minimum_intervention_days=20, minimum_intervention_fraction=.25,
    minimum_intervention_months=2, tail_bps=-800, valuation_policy_sha256=VALUATION_POLICY_SHA256)
POLICY_SHA256 = sha(CONTRACT)


def validate_roster(rows, days, calendar, sources, start, end):
    """Check the full original roster before returning the unchanged original Top20."""
    if rows.empty or len(rows)>50000 or rows.duplicated(KEY).any() or not set(ROSTER).issubset(rows):
        raise ValueError('invalid or duplicate native roster')
    sessions = pd.DatetimeIndex(calendar)
    if sessions.hasnans or list(sessions)!=sorted(set(sessions)):
        raise ValueError('invalid original calendar')
    expected = sessions[(sessions>=pd.Timestamp(start)) & (sessions<=pd.Timestamp(end))]
    source_map = {s['package_id']:s for s in sources}
    if (len(source_map)!=len(sources) or set(rows.package_id)!=set(source_map)
            or set(days.package_id)!=set(source_map) or days.duplicated(['package_id','decision_date']).any()
            or not rows.decision_as_of_trade_date.isin(expected).all()):
        raise ValueError('native source/date identity mismatch')
    for package, source in source_map.items():
        rr = rows.loc[rows.package_id.eq(package)]
        dd = days.loc[days.package_id.eq(package)]
        if (dd.decision_date.tolist()!=list(expected)
                or not rr.manifest_sha256.eq(source['manifest_sha256']).all()
                or not rr.run_id.eq(source['run_id']).all()):
            raise ValueError('native source/date identity mismatch')
        for day in dd.itertuples(index=False):
            group = rr.loc[rr.decision_as_of_trade_date.eq(day.decision_date)]
            pos = sessions.get_loc(day.decision_date)
            if (pos+5>=len(sessions) or day.target_date!=sessions[pos+1] or day.horizon_end!=sessions[pos+5]
                    or len(group)!=day.original_candidates or not 20<=len(group)<=50
                    or group.selection_effective_rank.tolist()!=list(range(1,len(group)+1))
                    or not group.candidate_group_size.eq(len(group)).all()
                    or not group.target_trade_date.eq(day.target_date).all()):
                raise ValueError('native ranks/counts or D/T/H differ')
    return rows.loc[rows.selection_effective_rank.le(20), list(ROSTER)].reset_index(drop=True)


def load_panel(binding, symbols, *, start, end, run_id):
    """One bounded read per stock/field, with generation and consumed-content identity."""
    symbols = sorted(set(symbols))
    if not symbols or len(symbols)>10000 or any(re.fullmatch(r'[0-9]{6}\.(SH|SZ|BJ)',s) is None for s in symbols):
        raise ValueError('invalid risk instruments')
    ds = FrozenDataset(binding, consumer_scope='ADVISORY', run_id=run_id)
    try:
        ds.require_day(start)
        ds.require_day(end)
        first, last = ds.calendar.index(start), ds.calendar.index(end)
        if first>last or (last-first+1)*len(symbols)>2_000_000:
            raise ValueError('invalid or oversized risk input interval')
        root = ds.file('day_meta_export').parent
        lease = {r[0]:tuple(r[1:]) for r in json.loads(zlib.decompress(
            base64.b64decode(ds.binding['bin_stat_lease']['zlib_base64'],validate=True)))}
        data = {f:np.full((last-first+1,len(symbols)),np.nan) for f in ('close','factor')}
        pins, paths, consumed = [], {}, 0
        for j,symbol in enumerate(symbols):
            for field in data:
                rel = (root.relative_to(ds.root)/'features'/symbol.lower()/f'{field}.day.bin').as_posix()
                path = inside(ds.root,rel)
                before = stamp(path)
                if before!=lease.get(rel) or before[0]<4 or before[0]%4:
                    raise ValueError('risk bin generation/size mismatch')
                with path.open('rb') as stream:
                    header = stream.read(4)
                    offset = float(np.frombuffer(header,dtype='<f4')[0])
                    length = before[0]//4-1
                    if not np.isfinite(offset) or offset<0 or offset!=int(offset) or offset+length>len(ds.calendar):
                        raise ValueError('invalid risk bin calendar offset')
                    a,b = max(first,int(offset)),min(last+1,int(offset)+length)
                    content = b''
                    if b>a:
                        stream.seek(4*(1+a-int(offset)))
                        content = stream.read(4*(b-a))
                        if len(content)!=4*(b-a):
                            raise ValueError('truncated risk bin')
                        data[field][a-first:b-first,j] = np.frombuffer(content,dtype='<f4')
                if stamp(path)!=before:
                    raise ValueError('risk bin changed during read')
                paths[path] = before
                consumed += len(content)+4
                if consumed>CONTRACT['max_bytes']:
                    raise ValueError('risk file byte budget exceeded')
                pins.append(dict(relative_path=rel, calendar_indices=[first,last], bin_offset=int(offset),
                                 consumed_sha256=hashlib.sha256(header+content).hexdigest(), bytes=len(content)+4))
        ds.assert_unchanged()
        if any(stamp(p)!=s for p,s in paths.items()):
            raise ValueError('risk input drift at end')
        calendar = pd.DatetimeIndex(ds.calendar[first:last+1])
        frame = pd.DataFrame(dict(trade_date=np.repeat(calendar,len(symbols)),instrument=symbols*len(calendar),
                                  close=data['close'].ravel(),factor=data['factor'].ravel()))
        receipt = dict(binding_hash=ds.identity, calendar=[str(start),str(end)], pins=pins,
                       input_content_sha256=sha(pins), bytes_read=consumed, sql_count=0)
        return frame,receipt
    finally:
        ds.close()


class RiskPricePanel:
    """Only in-memory prices. Each observation is sliced at its own D before use."""
    def __init__(self, frame):
        if frame.empty or frame.duplicated(['trade_date','instrument']).any():
            raise ValueError('invalid risk price panel')
        self.close = frame.pivot(index='trade_date',columns='instrument',values='close').sort_index()
        self.factor = frame.pivot(index='trade_date',columns='instrument',values='factor').reindex_like(self.close)


def history_vectors(panel, symbols, day):
    panel = panel if isinstance(panel,RiskPricePanel) else RiskPricePanel(panel)
    day = pd.Timestamp(day)
    if len(symbols)!=20 or len(set(symbols))!=20 or day not in panel.close.index:
        raise ValueError('risk population/calendar mismatch')
    # Slice BEFORE validation: a future malformed value cannot alter an earlier prediction.
    close = panel.close.loc[:day,symbols].tail(125).to_numpy(float)
    factor = panel.factor.loc[:day,symbols].tail(125).to_numpy(float)
    if any(np.isinf(a).any() or (np.isfinite(a)&(a<=0)).any() for a in (close,factor)):
        raise ValueError('non-positive or invalid observed risk price/factor')
    observed = np.isfinite(close) & np.isfinite(factor)
    valid = observed[5:] & observed[:-5]
    common = valid.all(axis=1)
    with np.errstate(invalid='ignore',divide='ignore'):
        values = np.minimum((close[5:]/close[:-5]-1)*10000,0)[common]
    available = len(close)>=125 and len(values)>=CONTRACT['minimum_common_observations']
    ends = panel.close.loc[:day].tail(125).index[5:][common]
    receipt = dict(status='AVAILABLE' if available else 'NO_RANK_ADVICE',
                   common_observations=len(values),decision_cutoff=day.date().isoformat(),
                   maximum_observation_end=ends[-1].date().isoformat() if len(ends) else None,
                   observation_end_dates=[d.date().isoformat() for d in ends],
                   observed_history_sha256=sha(dict(instruments=symbols,ends=[d.isoformat() for d in ends],values_bps=values.tolist())))
    return values,receipt
