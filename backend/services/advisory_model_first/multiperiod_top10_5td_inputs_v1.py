"""Read-only file inputs for a bounded Advisory study, never native selection."""
from contextlib import ExitStack
from decimal import Decimal
import hashlib

import numpy as np
import pandas as pd

from backend.data_service.frozen_dataset import FrozenDataset, inside, raw_price_li, stamp
from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import build_shared_daily_features
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY, ROSTER
from backend.services.advisory_model_first.generic_price_5td_labels_v1 import PRICE_FIELDS, REFERENCE_FIELDS
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import build_generic_price_5td_valuation_v2
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import node_path
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import freeze_rosters, parquet_bytes
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

STOCK_FEATURES = ('ret_1', 'ret_5', 'ret_10', 'atr14_close', 'close_location_in_day', 'volume_ratio_5_to_20')
INPUT_FEATURES = (*STOCK_FEATURES, 'parent_score_z', 'parent_rank_fraction')
IDENTITY = ('package_id', 'manifest_sha256', 'run_id', *KEY, 'selection_effective_rank', 'candidate_group_size')
FINANCE = ('label_information_end', 'valuation_status', 'hypothetical_liquidation_net_bps',
           'valuation_gross_terminal_ratio', 'observed_gap_bps', 'exit_execution_status', 'valuation_policy_sha256')


def original_rosters(plan, calendar, api):
    from types import SimpleNamespace
    from datetime import date
    sources = [SimpleNamespace(**s) for s in plan['sources']]
    spec = SimpleNamespace(train_start=date.fromisoformat(plan['start']),
        evaluation_end=date.fromisoformat(plan['end']), sources=sources)
    roster, days, receipts = freeze_rosters(plan=spec, calendar=calendar, api=api)
    if any(r['missing_decision_dates'] for r in receipts):
        raise ValueError('missing original score dates; no synthetic empty roster')
    return roster, days, receipts


def _raw(adjusted, factor):
    if pd.isna(adjusted) or pd.isna(factor):
        return np.nan
    return float(raw_price_li(float(adjusted), Decimal(str(float(factor))))/Decimal(1000))


def read_daily(dataset, symbols, start, end):
    """One bounded H5 scan. Exported volume is factor-adjusted, not raw shares."""
    frame = dataset.h5('daily_pv', symbols, start, end, ['open', 'high', 'low', 'close', 'volume', 'factor']).reset_index()
    if frame.empty:
        raise ValueError('file daily window is empty')
    frame = frame.rename(columns={'datetime': 'trade_date', 'factor': 'adj_factor'})
    values = frame[['open', 'high', 'low', 'close', 'adj_factor', 'volume']].to_numpy(float)
    if np.isinf(values).any() or (values[:, :5][np.isfinite(values[:, :5])] <= 0).any() or (values[:, 5] < 0).any():
        raise ValueError('known file OHLC/factor/volume contradicts its units')
    for field in ('open', 'high', 'low', 'close'):
        frame['raw_'+field+'_cny'] = [_raw(a, f) for a, f in zip(frame[field], frame.adj_factor, strict=True)]
    # The canonical Qlib export stores volume_hand*100/factor.
    frame['volume_hand'] = frame.volume*frame.adj_factor/100.
    return frame.loc[:, ['trade_date', 'instrument', 'adj_factor', 'volume_hand',
                         'raw_open_cny', 'raw_high_cny', 'raw_low_cny', 'raw_close_cny']]


def build_features(roster, daily, calendar):
    roster = roster.loc[:, [*IDENTITY, 'source_id', 'parent_score']].copy()
    empty_index = pd.DataFrame(columns=['trade_date', 'instrument', 'close'])
    features, _ = build_shared_daily_features(rosters=roster, calendar=calendar, daily=daily, index_daily=empty_index)
    groups = features.groupby(['package_id', KEY[0]], sort=False).parent_score
    mean, std = groups.transform('mean'), groups.transform(lambda x: x.std(ddof=0))
    features['parent_score_z'] = (features.parent_score-mean)/std.where(std.gt(0))
    features['parent_rank_fraction'] = features.selection_effective_rank/features.candidate_group_size
    return features.loc[:, [*IDENTITY, 'source_id', 'parent_score', *INPUT_FEATURES]]


def read_limits(dataset, pairs, budget=lambda: None):
    """Batch only required day slices; reuse each stream without copying assets."""
    root = dataset.file('day_meta_export').parent
    positions = {pd.Timestamp(d): i for i, d in enumerate(dataset.calendar)}
    results, receipts = [], []
    for symbol, group in pairs.groupby('instrument', sort=True):
        budget()
        days = sorted(group.trade_date.unique())
        indices = np.array([positions[pd.Timestamp(d)] for d in days])
        columns = {}
        with ExitStack() as stack:
            for field in ('up_limit_price', 'down_limit_price'):
                relative = (root.relative_to(dataset.root)/'features'/symbol.lower()/(field+'.day.bin')).as_posix()
                path = inside(dataset.root, relative)
                before = stamp(path)
                if dataset._bin_lease.get(relative) != before or before[0] < 4 or before[0] % 4:
                    raise ValueError('limit bin differs from frozen generation')
                stream = stack.enter_context(path.open('rb'))
                raw = stream.read(4)
                offset = float(np.frombuffer(raw, dtype='<f4')[0])
                length = before[0]//4-1
                if not np.isfinite(offset) or offset < 0 or int(offset) != offset or offset+length > len(dataset.calendar):
                    raise ValueError('limit bin has invalid header or length')
                values = np.full(len(indices), np.nan)
                selected = (indices >= offset) & (indices < offset+length)
                digest = hashlib.sha256(raw)
                # Gaps are separate reads: never decode excluded/future sessions.
                for i in np.flatnonzero(selected):
                    stream.seek(4*(1+int(indices[i]-offset)))
                    content = stream.read(4)
                    if len(content) != 4:
                        raise ValueError('truncated limit slice')
                    value = float(np.frombuffer(content, dtype='<f4')[0])
                    if np.isinf(value) or np.isfinite(value) and value <= 0:
                        raise ValueError('known limit value is invalid')
                    values[i] = round(value, 3)
                    digest.update(str(int(indices[i])).encode()+b':'+content)
                if stamp(path) != before:
                    raise ValueError('limit stream changed during read')
                receipts.append(dict(path=relative, stat=list(before), slice_sha256=digest.hexdigest(),
                    indices=indices[selected].tolist()))
                columns[field] = values
            for i, day in enumerate(days):
                results.append(dict(instrument=symbol, trade_date=pd.Timestamp(day),
                    up_limit=columns['up_limit_price'][i], down_limit=columns['down_limit_price'][i]))
    dataset.assert_unchanged()
    return pd.DataFrame(results), receipts


def make_finance(roster, daily, limits, suspension, calendar, cutoff):
    """Use the existing fixed-five-session label/valuation contract unmodified."""
    daily = daily.merge(limits, on=['trade_date', 'instrument'], how='outer', validate='one_to_one')
    full = {(s, pd.Timestamp(d)) for s, d, kind, timing in suspension if kind == 'S' and not str(timing or '').strip()}
    partial = {(s, pd.Timestamp(d)) for s, d, kind, timing in suspension if kind == 'S' and str(timing or '').strip()}
    daily['suspended'] = [(s, d) in full for s, d in zip(daily.instrument, daily.trade_date, strict=True)]
    daily['tradability_unknown'] = [(s, d) in partial for s, d in zip(daily.instrument, daily.trade_date, strict=True)]
    quotes = daily.set_index(['trade_date', 'instrument']).sort_index()
    sessions = list(pd.to_datetime(calendar))
    positions = {d: i for i, d in enumerate(sessions)}
    result = []
    for package, rr in roster.groupby('package_id', sort=False):
        dates = sorted(rr[KEY[0]].unique())
        for start in range(0, len(dates), 100):
            original = rr.loc[rr[KEY[0]].isin(dates[start:start+100])].copy()
            prices, refs = [], []
            for d, group in original.groupby(KEY[0], sort=True):
                symbols = group.instrument.tolist()
                anchor = quotes.reindex(pd.MultiIndex.from_product(([d], symbols), names=quotes.index.names))
                factors = dict(zip(symbols, anchor.adj_factor, strict=True))
                ref = group.loc[:, KEY].copy()
                ref['reference_cny'] = anchor.raw_close_cny.to_numpy()
                ref['reference_visible_through'] = pd.Timestamp(d).date()
                refs.append(ref)
                horizon = [day for day in sessions[positions[d]+1:positions[d]+6] if day <= pd.Timestamp(cutoff)]
                path = quotes.reindex(pd.MultiIndex.from_product((horizon, symbols), names=quotes.index.names)).reset_index()
                path[KEY[0]] = d
                path['d_anchor_factor'] = path.adj_factor/path.instrument.map(factors)
                for field in ('open', 'high', 'low', 'close'):
                    path[field] = path['raw_'+field+'_cny']
                # Missing quotes stay unknown; a verified full-day suspension is explicit.
                path['suspended'] = [(s, day) in full for s, day in zip(path.instrument, path.trade_date, strict=True)]
                path['tradability_unknown'] = [(s, day) in partial for s, day in zip(path.instrument, path.trade_date, strict=True)]
                prices.append(path.loc[:, PRICE_FIELDS])
            pp, reference_frame = pd.concat(prices, ignore_index=True), pd.concat(refs, ignore_index=True).loc[:, REFERENCE_FIELDS]
            context = dict(source_evidence='FROZEN_FILE_CURRENT_RELEASE_NON_VINTAGE', label_price_basis='D_REFERENCE_POLICY_RATIO',
                calendar_sha256=sha([str(d) for d in calendar]), prices_sha256=hashlib.sha256(parquet_bytes(pp)).hexdigest(),
                references_sha256=hashlib.sha256(parquet_bytes(reference_frame)).hexdigest())
            measured, _ = build_generic_price_5td_valuation_v2(candidates=original.loc[:, ROSTER],
                decision_dates=[pd.Timestamp(d).date() for d in dates[start:start+100]], calendar=calendar,
                prices=pp, references=reference_frame, source_context=context)
            immature = measured.label_information_end.isna() | measured.label_information_end.gt(pd.Timestamp(cutoff))
            measured.loc[immature, ['valuation_status', 'exit_execution_status']] = 'IMMATURE'
            measured.loc[immature, ['hypothetical_liquidation_net_bps', 'valuation_gross_terminal_ratio', 'observed_gap_bps']] = np.nan
            measured['package_id'] = package
            result.append(measured.loc[:, ['package_id', *KEY, *FINANCE]])
    finance = pd.concat(result, ignore_index=True)
    if len(finance) != len(roster) or finance.duplicated(['package_id', *KEY]).any():
        raise ValueError('valuation changed the original candidate population')
    return finance


def file_dataset(plan):
    binding = plan['binding']
    return FrozenDataset(binding, consumer_scope='ADVISORY', run_id=plan['experiment_id'],
        root_override=node_path(binding['profile']['controller_paths']['candidate_root']))


def finance_inputs(dataset, roster, cutoff, budget=lambda: None):
    start = roster[KEY[0]].min().date()
    cutoff = pd.Timestamp(cutoff).date()
    symbols = sorted(set(roster.instrument))
    daily = read_daily(dataset, symbols, start, cutoff)
    positions = {pd.Timestamp(d): i for i, d in enumerate(dataset.calendar)}
    pairs = set()
    for d, s in roster[[KEY[0], 'instrument']].itertuples(index=False, name=None):
        for day in dataset.calendar[positions[d]+1:positions[d]+6]:
            if day <= cutoff:
                pairs.add((s, pd.Timestamp(day)))
    limits, receipt = read_limits(dataset, pd.DataFrame(sorted(pairs), columns=['instrument', 'trade_date']), budget)
    suspension = dataset.suspension_rows(symbols, start, cutoff)
    result = make_finance(roster, daily, limits, suspension, list(dataset.calendar), cutoff)
    return result, dict(limit_slices=receipt, h5_reads=dataset.read_receipts, sql_count=0)
