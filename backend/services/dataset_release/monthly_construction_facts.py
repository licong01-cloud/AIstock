"""Exact construction facts for month-only SOURCE, never historical QA."""
from __future__ import annotations

from datetime import date
from contextlib import AbstractContextManager
import hashlib
import re
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd

from .monthly_legacy_factor import legacy_factor_inventory, read_legacy_factor_tail
from .errors import SourceManifestError
from .index_contract import DOMESTIC_INDEX_DEFINITIONS
from .monthly_legacy_prefix import (
    LegacyMonthlyPrefix, LegacyMonthlyPrefixError, _plain_chain, _relative, _signature,
    read_legacy_qfq_anchors, resolve_legacy_component_pin,
)


DAILY_CATALOG_SCHEMA = 'aistock_monthly_daily_catalog_construction_v1'


def daily_catalog_provider(prefix):
    """Resolve a pinned provider in direct-v2 or unified manifest layout."""
    pin = resolve_legacy_component_pin(prefix, 'day_meta_export')
    return prefix.root / _relative(pin['path']).parent


def capture_daily_catalog_history(*, prefix, fact_stock_codes, cutoff, connection, checkpoint=lambda: None):
    """Capture only missing catalogue identities, never scan retained history.

    The seven QFQ price/volume fields already exist in the shared factor H5.
    SOURCE freezes the missing identities' observed dates and raw reference
    fields from the same database snapshot as the ordinary new month.
    """
    from .source_fact_history import fact_codes
    requested = fact_codes(fact_stock_codes)
    if not requested:
        return None
    provider = daily_catalog_provider(prefix)
    catalog = provider / 'instruments/all.txt'
    _plain_chain(catalog)
    present = {line.split()[0].upper() for line in catalog.read_text(encoding='utf-8').splitlines() if line.strip()}
    missing = sorted(set(requested) - present)
    if not missing:
        return None
    calendar_path = provider / 'calendars/day.txt'
    _plain_chain(calendar_path)
    start = date.fromisoformat(calendar_path.read_text(encoding='utf-8').splitlines()[0])
    end = min(prefix.cutoff, cutoff)
    checkpoint()
    with connection.cursor() as cursor:
        cursor.execute('SELECT current_database(),txid_current_snapshot()::text')
        database, snapshot = cursor.fetchone()
        cursor.execute(
            'SELECT r.ts_code,r.trade_date,l.pre_close,l.up_limit,l.down_limit '
            'FROM market.kline_daily_raw r LEFT JOIN market.stk_limit l '
            'ON l.trade_date=r.trade_date AND l.ts_code=r.ts_code::text '
            "WHERE r.ts_code=ANY(%s::bpchar[]) AND r.trade_date BETWEEN %s AND %s AND r.adjust_type='none' "
            'AND r.open_li>0 AND r.high_li>0 AND r.low_li>0 AND r.close_li>0 '
            'AND (r.volume_hand>0 OR r.volume_shares>0 OR r.amount_li>0) '
            'ORDER BY r.ts_code,r.trade_date', (missing, start, end))
        rows = cursor.fetchall()
    checkpoint()
    if not rows:
        return None  # E.g. new-month IPO: there is no old history to invent.
    facts, seen = [], set()
    for code, day, reference, upper, lower in rows:
        key = str(code), str(day)
        if key in seen or code not in missing or not start <= day <= end:
            raise ValueError('daily catalogue construction source identity differs')
        seen.add(key)
        facts.append(dict(ts_code=code, trade_date=day.isoformat(),
            pre_close=None if reference is None else float(reference),
            up_limit=None if upper is None else float(upper),
            down_limit=None if lower is None else float(lower)))
    return dict(schema_version=DAILY_CATALOG_SCHEMA, database=database, database_snapshot=snapshot,
        codes=sorted({row['ts_code'] for row in facts}), stock_days=len(facts), facts=facts,
        start=start.isoformat(), end=end.isoformat(), database_read=True, database_write=False)


def daily_catalog_feature_payloads(*, factor_path, provider, calendar, facts, checkpoint=lambda: None):
    """Reuse same-release frozen PV; fill only absent daily cells of 12 fields."""
    import numpy as np
    from collections import defaultdict
    from .canonical_stock_transformer import VALUE_COMPARE_ABS_TOL
    from .monthly_build_resume import _plain
    from .monthly_quote_fact_correction import validate_missing_only
    from .source_fact_history import is_a_share
    from .stock_schema import QLIB_STOCK_FIELDS
    if not calendar or list(calendar) != sorted(set(calendar)):
        raise ValueError('daily catalogue calendar is empty, duplicated or unordered')
    positions = {day:i for i,day in enumerate(calendar)}
    grouped, seen = defaultdict(list), set()
    for row in facts:
        key = row['ts_code'], row['trade_date']
        if key in seen or not is_a_share(key[0]) or key[1] not in positions:
            raise ValueError('daily catalogue fact identity is invalid or duplicated')
        from ..tushare_dataset_specs import stk_limit_prices_valid
        if not stk_limit_prices_valid(row):
            raise ValueError('daily catalogue construction reference is missing or invalid')
        grouped[key[0]].append(row)
        seen.add(key)
    if not seen:
        return []
    _plain_chain(factor_path)
    before = _signature(factor_path)
    columns = ['open','high','low','close','volume','amount','factor']
    with pd.HDFStore(factor_path,'r') as store:
        # Instrument is an indexed storage dimension, not a full H5 scan.
        if 'instrument' not in store.get_storer('data').table.colindexes:
            raise ValueError('daily catalogue construction requires the existing factor instrument index')
        frame = store.select('data',where=f'instrument in {sorted(grouped)!r}',columns=columns)
    if frame.index.has_duplicates or _signature(factor_path) != before:
        raise ValueError('daily catalogue factor input changed or is duplicated')
    output = []
    for code, rows in sorted(grouped.items()):
        checkpoint()
        if code not in frame.index.get_level_values('instrument'):
            raise ValueError(f'daily catalogue price history is absent from frozen PV: {code}')
        selected = frame.xs(code,level='instrument')
        values_by_field = {field:{} for field in QLIB_STOCK_FIELDS}
        for row in rows:
            stamp = pd.Timestamp(row['trade_date'])
            if stamp not in selected.index:
                raise ValueError(f'daily catalogue price date is absent from frozen PV: {code}/{stamp.date()}')
            price = selected.loc[stamp]
            if (not np.isfinite(price.to_numpy(dtype=float)).all() or price['factor']<=0
                    or (price[['open','high','low','close']]<=0).any()
                    or price['volume']<0 or price['amount']<0):
                raise ValueError('daily catalogue PV has invalid observed values')
            values = {**price.to_dict(),'prev_close':row['pre_close'],
                'up_limit_price':row['up_limit'],'down_limit_price':row['down_limit'],
                'limit_up':float(price['close']/price['factor']>=row['up_limit']-VALUE_COMPARE_ABS_TOL),
                'limit_down':float(price['close']/price['factor']<=row['down_limit']+VALUE_COMPARE_ABS_TOL)}
            for field in QLIB_STOCK_FIELDS:
                values_by_field[field][positions[row['trade_date']]] = np.float32(values[field])
        for field, additions in values_by_field.items():
            relative = f'daily_bin/qlib/features/{code.lower()}/{field}.day.bin'
            path = provider/'features'/code.lower()/f'{field}.day.bin'
            _plain(path)
            original = None
            start, end = min(additions), max(additions)
            if path.exists():
                signature = _signature(path)
                original = path.read_bytes()
                old = np.frombuffer(original,dtype='<f4')
                if (_signature(path)!=signature or len(original)%4 or len(old)<2
                        or not np.isfinite(old[0]) or int(old[0])!=old[0] or old[0]<0):
                    raise ValueError('daily catalogue original bin is invalid or changed')
                start, end = min(start,int(old[0])), max(end,int(old[0])+len(old)-2)
            if end>=len(calendar):
                raise ValueError('daily catalogue feature exceeds the frozen calendar')
            target = np.full(end-start+2,np.nan,dtype='<f4')
            target[0] = start
            if original is not None:
                offset = int(old[0])-start+1
                target[offset:offset+len(old)-1] = old[1:]
            for index,value in additions.items():
                previous = target[index-start+1]
                if np.isfinite(previous) and previous!=value:
                    raise ValueError('daily catalogue correction would replace an existing finite fact')
                target[index-start+1] = value
            payload = target.tobytes()
            if original == payload:
                continue
            validate_missing_only(original,payload)
            output.append(dict(relative_path=relative,payload=payload,
                original_sha256=None if original is None else hashlib.sha256(original).hexdigest()))
    return output


def daily_catalog_repair_plan(plan, *, resume, prepare, build_inputs, cas):
    """Feed the existing missing-only transaction from automatic SOURCE delta."""
    from .monthly_build_resume import load_sealed_build_json
    from .monthly_quote_fact_correction import BINDING, SCHEMA
    reference = build_inputs.get('historical_daily_catalog_ref')
    if reference is None:
        return plan
    source = load_sealed_build_json(cas,reference)
    if source.get('schema_version') != DAILY_CATALOG_SCHEMA or source.get('database_write') is not False:
        raise ValueError('daily catalogue construction SOURCE differs')
    factor = load_sealed_build_json(cas,load_sealed_build_json(cas,prepare['prepare_receipt_ref'])['refs']['factor_receipt'])
    prices = [row for row in factor['files'] if row['dataset']=='daily_pv']
    if len(prices)!=1:
        raise ValueError('daily catalogue construction has no unique frozen PV')
    explicit = plan.get(BINDING)
    parents = [reference] + ([] if explicit is None else [explicit['authority_ref']])

    def bound(authority_ref):
        return {**plan,BINDING:{'operation_id':resume.context.operation_id,
            'target_cutoff':plan['target_cutoff'],'authority_ref':authority_ref,
            'automatic_population_history_delta':True}}

    retained = resume.record['steps'].get('quote-facts')
    if retained is not None:
        authority = load_sealed_build_json(cas,retained)
        if authority.get('parent_authority_refs')==parents and authority.get('daily_pv_sha256')==prices[0]['sha256']:
            return bound(retained)
    provider = resume.root/'daily_bin/qlib'
    calendar = (provider/'calendars/day.txt').read_text(encoding='utf-8').splitlines()
    payloads = daily_catalog_feature_payloads(factor_path=resume.root/'factor_bundle/daily_pv.h5',
        provider=provider,calendar=calendar,facts=source['facts'],checkpoint=resume.context.checkpoint)
    if not payloads:
        return plan  # A full component build may already include these facts.
    files = [{key:value for key,value in row.items() if key!='payload'} |
             {'target_ref':cas.put_bytes(row['payload']).as_dict()} for row in payloads]
    ready = plan.get('monthly_ready_correction',{})
    base = ready.get('base_manifest_ref',plan.get('predecessor_manifest_ref'))
    if base is None:
        raise ValueError('daily catalogue construction lost predecessor identity')
    sources = [reference]
    inherited_coverage = None
    if explicit is not None:
        other = load_sealed_build_json(cas,explicit['authority_ref'])
        if other.get('schema_version')!=SCHEMA or other.get('daily_pv_sha256')!=prices[0]['sha256'] or other.get('base_manifest_ref')!=base:
            raise ValueError('explicit and automatic quote corrections use different frozen facts')
        keyed = {row['relative_path']:row for row in files}
        for row in other['files']:
            key = row['relative_path']
            if key in keyed and keyed[key]!=row:
                raise ValueError('explicit and automatic quote corrections conflict')
            keyed[key] = row
        files = [keyed[key] for key in sorted(keyed)]
        sources.extend(other.get('source_capture_refs',[]))
        inherited_coverage = other['coverage']
    authority = dict(schema_version=SCHEMA,base_manifest_ref=base,files=files,
        source_capture_refs=sources,parent_authority_refs=parents,daily_pv_sha256=prices[0]['sha256'],
        coverage={'stock_days':source['stock_days'],'automatic_catalog_codes':source['codes'],
                  'explicit_coverage':inherited_coverage},database_write=False)
    return bound(cas.put_json(authority).as_dict())


def collect_suspended_daily_facts(
    *,
    descriptors: Sequence[Mapping[str, Any]],
    partition_rows: Callable[[Mapping[str, Any]], AbstractContextManager[Iterable[Mapping[str, Any]]]],
    suspended: frozenset[tuple[str, date]],
    trading_dates: Sequence[date],
    checkpoint: Callable[[], None],
) -> Mapping[tuple[str, date], tuple[Mapping[str, Any], ...]]:
    """Retain independent no-trade facts only inside this construction window.

    A sealed empty current partition proves absence; a missing or old-only
    partition does not. The supplied context verifies CAS streams on exit.
    """
    if not suspended:
        return {}
    if not descriptors:
        raise SourceManifestError("sealed daily partitions are missing for suspension proof")
    facts: dict[tuple[str, date], list[Mapping[str, Any]]] = {}
    covered_dates: set[date] = set()
    for descriptor in descriptors:
        match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})", str(descriptor.get("partition_key", "")))
        if match is None:
            raise SourceManifestError("daily suspension proof partition identity is invalid")
        start, end = (date.fromisoformat(value) for value in match.groups())
        if end < trading_dates[0] or start > trading_dates[-1]:
            continue
        covered_dates.update(day for day in trading_dates if start <= day <= end)
        with partition_rows(descriptor) as rows:
            for ordinal, row in enumerate(rows):
                key = (str(row.get("ts_code", "")).upper(), date.fromisoformat(str(row.get("trade_date"))[:10]))
                if key in suspended:
                    values = facts.setdefault(key, [])
                    values.append(dict(row))
                    if len(values) > 1:
                        raise SourceManifestError(
                            "daily suspension proof is duplicated",
                            context={"ts_code": key[0], "trade_date": key[1].isoformat()},
                        )
                if ordinal % 10_000 == 0:
                    checkpoint()
        checkpoint()
    if any(day not in covered_dates for _, day in suspended if day in trading_dates):
        raise SourceManifestError("sealed daily partitions do not cover the suspension proof window")
    return {key: tuple(values) for key, values in facts.items()}


def collect_qfq_construction_anchors(
    prefix: LegacyMonthlyPrefix, *, month_start: date,
    instruments: tuple[str, ...],
    checkpoint: Callable[[], None] = lambda: None,
    progress: Callable[[Mapping], None] = lambda _: None,
) -> tuple[tuple[str, date], ...]:
    """Collect dates, not invented raw factors or a historical completeness PASS.

    The provider catalog bounds existing physical stocks. New-month IPOs have
    no inherited files. Each Bin contributes its last real factor date; the
    factor aggregate contributes its actual one-row rolling boundary. SOURCE
    then queries precisely these raw facts in the same snapshot as the month.
    """
    if prefix.cutoff != month_start - date.resolution:
        raise LegacyMonthlyPrefixError("construction prefix does not precede target month")
    provider = _relative(resolve_legacy_component_pin(prefix, "day_meta_export")["path"]).parent
    catalog = prefix.root / provider / "instruments/all.txt"
    _plain_chain(catalog)
    lines = [line.split("\t") for line in catalog.read_text(encoding="utf-8").splitlines()]
    if any(len(row) != 3 for row in lines):
        raise LegacyMonthlyPrefixError("construction provider catalog schema differs")
    # Ended securities have no September tail to rebase. Do not look up their
    # old Bin/H5 values; SQL still computes genuine cutoff denominators for PIT.
    index_codes = {item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS}
    codes = tuple(sorted({row[0].upper() for row in lines
                          if row[0].upper().endswith((".SH", ".SZ"))
                          and row[0].upper() not in index_codes
                          and row[0].upper() in instruments}))
    result = set()
    for dataset in ("daily_bin", "minute_bin"):
        progress({"phase": "SOURCE_CONSTRUCTION_BOUNDARIES",
                  "query_id": "adj_factor_construction", "partition_key": dataset})
        for code, row in read_legacy_qfq_anchors(prefix, dataset=dataset, instruments=codes, checkpoint=checkpoint).items():
            result.add((code, date.fromisoformat(row["trade_date"])))
    inventory, factor_root = legacy_factor_inventory(prefix)
    factor_path = prefix.root / factor_root / "daily_pv.h5"
    _plain_chain(factor_path)
    pins = [item for item in inventory.get("files", ())
            if item.get("path") == factor_path.relative_to(prefix.root).as_posix()]
    if len(pins) != 1 or factor_path.stat().st_size != pins[0]["size"]:
        raise LegacyMonthlyPrefixError("construction factor boundary physical pin differs")
    def rolling_progress(_value: Mapping) -> None:
        # The rolling reader emits local physical-read/index counters. Those
        # are not validated/sealed SOURCE facts and must neither reset the
        # durable monotonic counters nor expand the strict progress schema.
        progress({"phase": "SOURCE_CONSTRUCTION_ROLLING_SEED",
                  "query_id": "adj_factor_construction", "partition_key": factor_path.stem})

    tail = read_legacy_factor_tail(factor_path, instruments=codes, rows_per_instrument=1,
                                  before=month_start, checkpoint=checkpoint, progress=rolling_progress)
    result.update((str(code), pd.Timestamp(stamp).date()) for stamp, code in tail.index)
    if any(day >= month_start for _, day in result):
        raise LegacyMonthlyPrefixError("construction boundary is not prior to target month")
    return tuple(sorted(result))
