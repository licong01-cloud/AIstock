"""Small US source adapter within the existing native sync engine/scheduler."""
from __future__ import annotations

import datetime as dt
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

US_INDICES = ('SPX', 'IXIC', 'DJI')
US_DATASETS = frozenset({'index_global', 'vix_index', 'us_tradecal'})


def parse_day(value):
    text = str(value)
    return dt.date.fromisoformat(text) if '-' in text else dt.datetime.strptime(text, '%Y%m%d').date()


def normalize_us_rows(spec, rows):
    output = []
    for source in rows:
        row = dict(source)
        day = parse_day(row.get(spec.date_column))
        if spec.name == 'us_tradecal':
            flag = row.get('is_open')
            if isinstance(flag, bool) or str(flag) not in {'0', '1'}:
                raise ValueError('us_tradecal.is_open: expected 0/1')
            row['is_open'] = int(flag)
            if row.get('pretrade_date') is not None and parse_day(row['pretrade_date']) >= day:
                raise ValueError('us_tradecal.pretrade_date: must precede cal_date')
        else:
            if row.get('ts_code') not in US_INDICES:
                raise ValueError('index_global: unexpected index code')
            for field, kind in spec.columns.items():
                if kind != 'numeric':
                    continue
                value = row.get(field)
                required = field in {'open', 'close', 'high', 'low', 'pre_close', 'pct_chg'}
                try:
                    if value is None and not required:
                        row[field] = None
                        continue
                    if isinstance(value, bool):
                        raise ValueError('boolean')
                    number = Decimal(str(value))
                    if not number.is_finite():
                        raise ValueError('non-finite')
                    if field in {'open', 'close', 'high', 'low', 'pre_close'} and number <= 0:
                        raise ValueError('non-positive index')
                    if field in {'vol', 'amount', 'swing'} and number < 0:
                        raise ValueError('negative volume/amount/swing')
                    row[field] = number
                except (ValueError, InvalidOperation) as exc:
                    raise ValueError(f'index_global.{field}: invalid provider fact') from exc
            if not row['low'] <= min(row['open'], row['close']) <= max(row['open'], row['close']) <= row['high']:
                raise ValueError('index_global: OHLC relation differs')
        output.append(row)
    return output


def default_end(dataset, now=None):
    from .tushare_supplemental_inputs import latest_vix_query_date
    if dataset == 'us_tradecal':
        today = (now or dt.datetime.now(dt.timezone.utc)).astimezone(ZoneInfo('America/New_York')).date()
        return today + dt.timedelta(days=370)
    return latest_vix_query_date() if now is None else latest_vix_query_date(now)


def incremental_start(conn, spec, end):
    # Small source tables, never scan equity/minute history or buy-eligible PIT.
    with conn.cursor() as cur:
        cur.execute(f'SELECT MAX({spec.date_column}) FROM {spec.target_table}')
        maximum = cur.fetchone()[0]
    floor = parse_day(spec.initial_start_date)
    if maximum is None:
        return floor, {'bootstrap': True, 'calendar': 'US'}
    if spec.name == 'us_tradecal':
        return max(floor, end - dt.timedelta(days=377)), {'calendar': 'US', 'forward_days': 370}
    # If collection was offline, retain the whole catch-up interval; otherwise
    # only recheck the recent week for delayed source publication/revisions.
    return max(floor, min(maximum + dt.timedelta(days=1), end - dt.timedelta(days=6))), {
        'calendar': 'US', 'publication_recheck_calendar_days': 7}


def partition_quality(dataset, is_open, row_count):
    if is_open is None:
        return 'unknown', None
    expected = (3 if dataset == 'index_global' else 1) if is_open else 0
    if not is_open and row_count:
        raise ValueError(f'{dataset}: quotes returned for a closed US date')
    return ('empty_valid' if not is_open else 'ok' if row_count == expected else 'low_coverage'), expected


def read_calendar(conn, start, end):
    with conn.cursor() as cur:
        cur.execute('SELECT cal_date,is_open FROM market.us_tradecal WHERE cal_date BETWEEN %s AND %s',
                    (start, end))
        return dict(cur.fetchall())


def sync_ranges(engine, conn, spec, start, end, job_id):
    from .tushare_supplemental_inputs import normalize_rows, request_windows, raw_observation, store_raw_observations
    from .tushare_sync_engine import SyncResult, _date_range

    windows = list(request_windows(start, end))
    windows.extend((day, day) for day in engine._late_recheck_dates.get(spec.name, [])
                   if day < start)
    result = SyncResult(dataset=spec.name, mode='sync', job_id=job_id, total_batches=len(windows))
    for first, last in windows:
        params = {'start_date': first.strftime('%Y%m%d'), 'end_date': last.strftime('%Y%m%d')}
        try:
            rows = []
            for code in US_INDICES if spec.name == 'index_global' else (None,):
                batch = engine._fetch_from_tushare(spec, {**params, **({'ts_code': code} if code else {})})
                if code and any(row.get('ts_code') != code for row in batch):
                    raise ValueError('index_global: requested code differs from returned code')
                rows.extend(batch)
            by_date, keys = {}, set()
            for row in rows:
                day = parse_day(row[spec.date_column])
                key = (day, row.get('ts_code'))
                if not first <= day <= last or key in keys:
                    raise ValueError(f'{spec.name}: duplicate or out-of-window US date/code')
                keys.add(key)
                by_date.setdefault(day, []).append(row)
            calendar = read_calendar(conn, first, last) if spec.name != 'us_tradecal' else {}
            # Protocol/key ambiguity still rejects the request. A known bad
            # source value quarantines only that key, not 90 days of neighbours.
            qualities, rejected, rejected_rows = {}, {}, {}
            for day in _date_range(first, last):
                facts, invalid, originals = [], [], []
                for row in by_date.get(day, []):
                    try:
                        normalize_rows(spec, [row])
                        if spec.name != 'us_tradecal' and calendar.get(day) == 0:
                            raise ValueError('provider_quote_on_closed_US_date')
                        facts.append(row)
                    except ValueError as exc:
                        originals.append(row)
                        invalid.append({'ts_code':row.get('ts_code'), 'reason':str(exc),
                                        'source_row_sha256':raw_observation(spec,row)[4]})
                by_date[day], rejected[day], rejected_rows[day] = facts, invalid, originals
                qualities[day] = (('ok', 1) if facts else ('low_coverage', 1)) if spec.name == 'us_tradecal' else (
                    partition_quality(spec.name, calendar.get(day), len(facts)))
                if invalid and qualities[day][0] == 'empty_valid':
                    qualities[day] = ('unknown', 0)  # conflict is not a clean empty response
            for day in _date_range(first, last):
                facts = by_date.get(day, [])
                quality, expected = qualities[day]
                if quality not in {'ok', 'empty_valid'} or rejected[day]:
                    if result.quality_warnings is None:
                        result.quality_warnings = []
                    result.quality_warnings.append({'trade_date': str(day), 'quality_status': quality,
                                                   'expected_rows': expected, 'source_rows': len(facts),
                                                   'invalid_source_rows': len(rejected[day])})
                store_raw_observations(conn, spec, rejected_rows[day])
                written = engine._upsert_batch(conn, spec, facts)
                engine._refresh_audit.record_success(
                    dataset=spec.name, trade_date=day, row_count=len(facts), written_rows=written,
                    expected_rows=expected, job_id=str(job_id), data_source='tushare', conn=conn,
                    quality_status=quality,
                    metadata={'tushare_api': spec.tushare_api, 'provider_query_complete': True,
                              'request': params, 'session_date_timezone': 'America/New_York',
                              'calendar_authority': 'tushare.us_tradecal', 'is_open': calendar.get(day),
                              'publication_time_quality': 'UNKNOWN', 'vintage_quality': 'NON_VINTAGE',
                              'rejected_source_rows': rejected[day],
                              'coverage_complete': quality in {'ok', 'empty_valid'},
                              'session_calendar_completeness': 'COMPLETE' if expected is not None else 'NOT_EVALUATED',
                              'missing_reason': None if quality in {'ok', 'empty_valid'} else (
                                  'calendar_unknown' if expected is None else 'provider_not_published_or_missing')})
                result.inserted_rows += written
            result.success_batches += 1  # successful transport, not a claim of complete facts
        except Exception as exc:
            result.failed_batches += 1
            result.error = '; '.join(filter(None, (result.error, str(exc))))[:4000]
            for day in _date_range(first, last):
                engine._refresh_audit.record_failure(
                    dataset=spec.name, trade_date=day, error_message=str(exc), job_id=str(job_id),
                    data_source='tushare', conn=conn, quality_status='error',
                    failure_category='provider_or_persistence_error', metadata={'request': params})
        engine._update_progress(conn, job_id, result)
    return result


def gap_report(conn, spec, start, end):
    """Physical US-key coverage, not equity validation or inferred holidays."""
    from .tushare_sync_engine import _date_range
    with conn.cursor() as cur:
        cur.execute(f'SELECT {spec.date_column}' + (',ts_code' if spec.name == 'index_global' else '') +
                    f' FROM {spec.target_table} WHERE {spec.date_column} BETWEEN %s AND %s', (start, end))
        observed = {(row[0], row[1] if len(row) > 1 else None) for row in cur.fetchall()}
    calendar = read_calendar(conn, start, end)
    missing, unknown = [], []
    for day in _date_range(start, end):
        if spec.name == 'us_tradecal':
            expected = (None,)
        elif day not in calendar:
            unknown.append(str(day))
            continue
        else:
            expected = (US_INDICES if spec.name == 'index_global' else (None,)) if calendar[day] else ()
        for code in expected:
            if (day, code) not in observed:
                missing.append({'trade_date': str(day), 'ts_code': code})
    unexpected = [{'trade_date':str(day),'ts_code':code} for day,code in sorted(observed)
                  if spec.name != 'us_tradecal' and calendar.get(day) == 0]
    dates = sorted({key['trade_date'] for key in missing})
    return {'data_kind': spec.name, 'status': 'INCOMPLETE' if missing else 'CONFLICT' if unexpected else 'UNKNOWN' if unknown else 'COMPLETE',
            'start_date': str(start), 'end_date': str(end), 'missing_keys': missing,
            'missing_days': len(dates), 'missing_key_count': len(missing),
            'missing_ranges': [{'start_date': day, 'end_date': day, 'days': 1} for day in dates],
            'unknown_calendar_dates': unknown, 'calendar_authority': 'tushare.us_tradecal',
            'unexpected_keys':unexpected, 'unexpected_key_count':len(unexpected),
            'total_trading_days': sum(calendar.values()), 'session_date_timezone': 'America/New_York'}
