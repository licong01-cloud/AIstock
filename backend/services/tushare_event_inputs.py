"""Bounded raw acquisition for five optional event APIs, using the native jobs.

No trading/PIT decisions live here. Announcement dates are not publication
timestamps; provider-reconstructed history remains explicitly NON_VINTAGE.
"""
from __future__ import annotations

from contextlib import contextmanager
import datetime as dt
import hashlib
import json
import re
import time
from zoneinfo import ZoneInfo

from psycopg2 import sql
from psycopg2.extras import Json, execute_values

EVENT_DATASETS = frozenset({'stk_shock', 'stk_high_shock', 'stk_alert', 'idx_anns', 'stk_seasoned'})
INDEX_SOURCES = ('中证指数', '国证指数', '恒生指数', '华证指数')
_DATE_FIELD = {'stk_alert': 'start_date', 'idx_anns': 'ann_date', 'stk_seasoned': 'ann_date'}
_STOCK = re.compile(r'^(?:(?:00|30)\d{4}\.SZ|6\d{5}\.SH)$')
_SECURITY = re.compile(r'^\d{6}\.(?:SH|SZ|BJ)$')


def parse_date(value):
    if isinstance(value, dt.datetime):
        raise ValueError('event date must not silently discard a time')
    if isinstance(value, dt.date):
        return value
    text = str(value)
    return dt.date.fromisoformat(text) if '-' in text else dt.datetime.strptime(text, '%Y%m%d').date()


def payload_bytes(payload):
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
                      allow_nan=False).encode('utf-8')


def observation(name, row, request_date):
    from .tushare_dataset_specs import DATASET_REGISTRY
    spec = DATASET_REGISTRY[name]
    if set(row) != set(spec.columns):
        raise ValueError(f'{name}: provider fields differ')
    day = parse_date(row[_DATE_FIELD.get(name, 'trade_date')])
    # Serialize the original JSON, not an imputed or coalesced projection.
    encoded = payload_bytes(row)
    return {'source_api': name, 'request_date': request_date, 'event_date': day,
            'ts_code': row.get('ts_code'), 'source_row_sha256': hashlib.sha256(encoded).hexdigest(),
            'raw_payload': json.loads(encoded), 'source_record_key': 'UNKNOWN', 'published_at': None}


def accepted_rows(name, rows, start, end, *, code=None, source=None):
    output = []
    for row in rows:
        value = observation(name, row, start)
        if not start <= value['event_date'] <= end:
            raise ValueError(f'{name}: provider returned out-of-window date')
        if name != 'idx_anns':
            symbol = row.get('ts_code')
            if not isinstance(symbol, str) or not _SECURITY.fullmatch(symbol):
                raise ValueError(f'{name}: malformed security identity')
            if code is not None and symbol != code:
                raise ValueError(f'{name}: provider ignored exact code')
            if not _STOCK.fullmatch(symbol):
                continue  # explicit equity scope; ETF/BJ/B are not buy-PIT decisions
        elif source is not None and row.get('source') != source:
            raise ValueError('idx_anns: provider ignored exact source')
        if name == 'idx_anns' and any(not isinstance(row.get(f), str) or not row[f].strip()
                                      for f in ('title', 'url', 'source')):
            raise ValueError('idx_anns: missing announcement identity')
        # Optional dates are parsed only when provided. Unknown stays unknown.
        if name == 'stk_alert' and row.get('end_date') not in (None, ''):
            if parse_date(row['end_date']) < value['event_date']:
                raise ValueError('stk_alert: inverted reference interval')
        if name == 'stk_seasoned' and row.get('first_ann_date') not in (None, ''):
            if parse_date(row['first_ann_date']) > value['event_date']:
                raise ValueError('stk_seasoned: first announcement follows announcement')
        output.append(value)
    return output


@contextmanager
def transaction(conn):
    owns = conn.autocommit
    if owns:
        conn.autocommit = False
    try:
        yield
        if owns:
            conn.commit()
    except BaseException:
        if owns:
            conn.rollback()
        raise
    finally:
        if owns:
            conn.autocommit = True


class EventBudgetWait(RuntimeError):
    """A finite job/account budget stopped before any truncated success."""


def claim_index_request(conn, job_id):
    """Count attempts in existing job logs, across cooperating owner processes.

    This is not a claim to own the vendor account's other external clients.
    Claim is committed before HTTP; no DB lock is held during network waits.
    """
    if not conn.autocommit:
        raise ValueError('request budget requires an idle connection')
    today = dt.datetime.now(ZoneInfo('Asia/Shanghai')).date()
    start = dt.datetime.combine(today, dt.time(), ZoneInfo('Asia/Shanghai'))
    prefix = 'tushare-idx_anns-request:'
    with transaction(conn), conn.cursor() as cur:
        cur.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', (prefix+str(today),))
        cur.execute('SELECT count(*) FROM market.ingestion_logs WHERE ts>=%s AND ts<%s '
                    'AND message LIKE %s', (start, start+dt.timedelta(days=1), prefix+'%'))
        if cur.fetchone()[0] >= 1000:
            raise EventBudgetWait('idx_anns daily request budget exhausted; resume next day')
        cur.execute('INSERT INTO market.ingestion_logs(job_id,ts,level,message) '
                    "VALUES(%s,clock_timestamp(),'info',%s)", (str(job_id), prefix+today.isoformat()))


class EventFetcher:
    def __init__(self, engine, conn, spec, job_id):
        self.engine, self.conn, self.spec, self.job_id = engine, conn, spec, job_id
        self.attempts = 0

    def request(self, params):
        from .tushare_rate_limiter import get_limiter
        from .tushare_sync_engine import _query_tushare_dataapi
        failure = None
        for attempt in range(3):
            if self.attempts >= (800 if self.spec.name == 'idx_anns' else 10000):
                raise EventBudgetWait('event job request budget exhausted; completed windows retained')
            if self.spec.name == 'idx_anns':
                claim_index_request(self.conn, self.job_id)
            self.attempts += 1
            get_limiter(self.spec.tushare_api, self.spec.rate_per_minute).acquire()
            time.sleep(self.spec.batch_sleep)  # no initial token-bucket burst above the documented rate
            try:
                return _query_tushare_dataapi(self.spec.tushare_api, params, ','.join(self.spec.columns),
                                             token=self.engine._tushare_token())
            except Exception as exc:
                failure = exc
                if attempt < 2:
                    time.sleep(5 * 2**attempt)
        raise RuntimeError(f'{self.spec.name}: provider failed after 3 attempts: {failure}')

    def windows(self, start, end):
        """Recursively split caps. A capped one-day shard is never complete."""
        params = {'start_date': start.strftime('%Y%m%d'), 'end_date': end.strftime('%Y%m%d')}
        rows = self.request(params)
        accepted = accepted_rows(self.spec.name, rows, start, end)
        if len(rows) < self.spec.row_limit:
            yield start, end, accepted, params
            return
        if start < end:
            mid = start + (end-start)//2
            yield from self.windows(start, mid)
            yield from self.windows(mid+dt.timedelta(days=1), end)
            return
        if self.spec.name == 'idx_anns':
            shards = [('src', source) for source in INDEX_SOURCES]
        else:
            # Source catalogue includes ended/ST/new securities, not a buy pool.
            with self.conn.cursor() as cur:
                cur.execute("SELECT DISTINCT ts_code FROM market.stock_basic "
                            "WHERE ts_code ~ '^(00[0-9]{4}[.]SZ|30[0-9]{4}[.]SZ|6[0-9]{5}[.]SH)$' ORDER BY ts_code")
                shards = [('ts_code', str(row[0]).strip()) for row in cur.fetchall()]
            if not shards:
                raise ValueError('event cap sharding requires the full supported stock source catalogue')
        complete = []
        for key, value in shards:
            part = self.request({**params, key: value})
            filtered = accepted_rows(self.spec.name, part, start, end,
                                     code=value if key == 'ts_code' else None,
                                     source=value if key == 'src' else None)
            if len(part) >= self.spec.row_limit:
                raise ValueError(f'{self.spec.name}: one-day exact shard still reaches cap')
            complete.extend(filtered)
        # Every row in the unsharded prefix must be reproduced. Catalogue/source
        # incompleteness must not silently lose the capped prefix.
        if not {r['source_row_sha256'] for r in accepted} <= {r['source_row_sha256'] for r in complete}:
            raise ValueError('event shards do not reproduce unsharded prefix')
        yield start, end, complete, {**params, 'complete_shard_count': len(shards)}


def store_day(engine, conn, spec, day, facts, params, job_id):
    """Original versions + revision + successful query audit commit together."""
    from .data_integrity.repository import mark_changed
    unique = {row['source_row_sha256']: row for row in facts}
    table = sql.Identifier(*spec.target_table.split('.'))
    with transaction(conn):
        changed = 0
        with conn.cursor() as cur:
            cur.execute("SELECT metadata FROM market.dataset_date_refresh_audit WHERE dataset=%s "
                        "AND trade_date=%s AND data_source='tushare'", (spec.name, day))
            previous = cur.fetchone()
            prior_scan = (previous[0] or {}).get('event_scan', {}) if previous else {}
            if unique:
                data = [(spec.name, day, row['event_date'], row['ts_code'], key,
                         Json(row['raw_payload']), str(job_id)) for key, row in unique.items()]
                inserted = execute_values(cur, sql.SQL('''INSERT INTO {} AS old
                    (source_api,request_date,event_date,ts_code,source_row_sha256,raw_payload,batch_id)
                    VALUES %s ON CONFLICT(source_api,source_row_sha256) DO NOTHING
                    RETURNING source_row_sha256''').format(table).as_string(conn), data, fetch=True)
                changed = len(inserted)
                cur.execute(sql.SQL('UPDATE {} SET last_observed_at=clock_timestamp() '
                    'WHERE source_api=%s AND source_row_sha256=ANY(%s)').format(table),
                    (spec.name, list(unique)))
            cur.execute(sql.SQL('SELECT count(*) FROM {} WHERE source_api=%s AND request_date=%s').format(table),
                        (spec.name, day))
            retained = cur.fetchone()[0]
        if changed:
            mark_changed(conn, spec.name, [day], reason='new_raw_event_version', source=spec.tushare_api)
        evidence_keys = set(unique)
        scan_params = params
        if params.get('query_kind') == 'known_project_scope_review' and prior_scan.get('complete') is True:
            # A targeted refresh must not erase the prior full-date query's
            # coverage or pretend this exact-code request is a market scan.
            evidence_keys.update(prior_scan.get('source_rows_sha256', []))
            scan_params = prior_scan.get('parameters', params)
        metadata = {'event_scan': {'schema_version': 'tushare_event_scan_v1', 'complete': True,
                    'source_rows_sha256': sorted(evidence_keys), 'observed_rows': len(facts),
                    'duplicate_rows': len(facts)-len(unique), 'parameters': scan_params,
                    'last_query_parameters': params,
                    'query_completed_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                    'full_query_completed_at': (prior_scan.get('full_query_completed_at')
                        if params.get('ts_code') else dt.datetime.now(dt.timezone.utc).isoformat())},
                    'publication_time_quality': 'UNKNOWN', 'vintage_quality': 'NON_VINTAGE',
                    'coverage_basis': 'completed_provider_query_not_historical_PIT',
                    'optional_for_daily_complete': True}
        engine._refresh_audit.record_success(dataset=spec.name, trade_date=day, row_count=retained,
            written_rows=changed, job_id=str(job_id), data_source='tushare', conn=conn,
            quality_status='ok' if facts else 'empty_valid', metadata=metadata)
    return changed, retained


def incremental_start(engine, conn, spec, end):
    # Shared raw tables must never seed one API's cursor from another API's facts.
    with conn.cursor() as cur:
        cur.execute("SELECT max(trade_date) FROM market.dataset_date_refresh_audit "
                    "WHERE dataset=%s AND data_source='tushare' AND status='success' "
                    "AND metadata#>>'{event_scan,complete}'='true' AND trade_date<=%s", (spec.name, end))
        cursor = cur.fetchone()[0]
    floor = parse_date(spec.initial_start_date)
    tail = 30 if spec.name == 'stk_seasoned' else 7
    start = max(floor, min(cursor+dt.timedelta(days=1), end-dt.timedelta(days=tail-1))) if cursor else floor
    engine._late_recheck_dates[spec.name] = engine._unclosed_publication_dates(conn, spec, end)
    return start, {'safe_audit_cursor_date': str(cursor) if cursor else None,
                   'late_publication_tail_days': tail, 'event_source_cursor_only': True}


def sync_events(engine, conn, spec, start, end, job_id):
    from .tushare_sync_engine import SyncResult, _date_range
    from .tushare_supplemental_inputs import request_windows
    result = SyncResult(dataset=spec.name, mode='sync', job_id=job_id)
    fetcher = EventFetcher(engine, conn, spec, job_id)
    ranges = [(day, day) for day in engine._late_recheck_dates.get(spec.name, []) if not start <= day <= end]
    ranges += list(request_windows(max(start, parse_date(spec.initial_start_date)), end))
    for outer_start, outer_end in ranges:
        completed = set()
        try:
            for left, right, facts, params in fetcher.windows(outer_start, outer_end):
                by_day = {}
                for row in facts:
                    by_day.setdefault(row['event_date'], []).append(row)
                for day in _date_range(left, right):
                    target = engine._upsert_sync_target(spec, day, metadata={'event_query': params})
                    written, retained = store_day(engine, conn, spec, day, by_day.get(day, []), params, job_id)
                    completed.add(day)
                    result.inserted_rows += written
                    result.success_batches += 1
                    result.total_batches += 1
                    engine._record_sync_attempt(target, 'reconciled', job_id=job_id,
                        rows_written=written, rows_observed=retained,
                        context={'event_query_complete': True})
                engine._update_progress(conn, job_id, result)
        except Exception as exc:
            result.failed_batches += 1
            result.total_batches += 1
            result.error = str(exc)[:4000]
            for day in _date_range(outer_start, outer_end):
                if day in completed:
                    continue
                target = engine._upsert_sync_target(spec, day)
                engine._refresh_audit.record_failure(dataset=spec.name, trade_date=day,
                    error_message=str(exc), job_id=str(job_id), data_source='tushare', conn=conn,
                    quality_status='error', failure_category='event_query_incomplete')
                engine._record_sync_attempt(target, 'retry', job_id=job_id, rows_written=0,
                    rows_observed=0, error_message=str(exc))
            break  # retain completed windows; never advance past an unexplained failed range
    return result


def review_seasoned_projects(engine, conn, spec, end, job_id):
    """Monthly bounded requery of known scopes, NOT an invented project entity.

    Reuses raw rows as the resumable work list. A success marks the actual
    review time; failure leaves it eligible for the next hourly small batch.
    """
    from .tushare_sync_engine import SyncResult
    result = SyncResult(dataset=spec.name, mode='sync', job_id=job_id)
    month = end.replace(day=1)
    with conn.cursor() as cur:
        cur.execute('''SELECT ts_code,raw_payload->>'first_ann_date'
            FROM market.stock_seasoned_raw
            WHERE raw_payload->>'first_ann_date' ~ '^[0-9]{8}$'
            GROUP BY ts_code,raw_payload->>'first_ann_date'
            HAVING NOT bool_and(coalesce(raw_payload->>'cur_stage','')='终止')
              AND (max(last_project_review_at) IS NULL OR max(last_project_review_at)<%s)
            ORDER BY ts_code,raw_payload->>'first_ann_date' LIMIT 20''', (month,))
        # A shared query scope is not a unique project. Conflicting/multiple
        # stages stay eligible; one terminal row cannot hide another project.
        scopes = cur.fetchall()
    fetcher = EventFetcher(engine, conn, spec, job_id)
    for code, first in scopes:
        start = max(parse_date(first), parse_date(spec.initial_start_date))
        result.total_batches += 1
        try:
            # Exact-code fetches retain all versions for that query scope. A cap
            # is split by date rather than assumed to support offset.
            stack, facts = [(start, end)], []
            while stack:
                left, right = stack.pop()
                params = {'ts_code': code, 'start_date': left.strftime('%Y%m%d'),
                          'end_date': right.strftime('%Y%m%d')}
                raw = fetcher.request(params)
                values = accepted_rows(spec.name, raw, left, right, code=code)
                if len(raw) >= spec.row_limit:
                    if left == right:
                        raise ValueError('stk_seasoned exact scope reaches one-day cap')
                    mid = left+(right-left)//2
                    stack.extend([(mid+dt.timedelta(days=1), right), (left, mid)])
                else:
                    facts.extend(values)
            by_day = {}
            for fact in facts:
                by_day.setdefault(fact['event_date'], []).append(fact)
            # No inferred project disappearance/termination on an empty query.
            with transaction(conn):
                for day, values in by_day.items():
                    written, _ = store_day(engine, conn, spec, day, values,
                        {'ts_code': code, 'start_date': str(start), 'end_date': str(end),
                         'query_kind': 'known_project_scope_review', 'first_ann_date': first}, job_id)
                    result.inserted_rows += written
                with conn.cursor() as cur:
                    cur.execute('''UPDATE market.stock_seasoned_raw SET last_project_review_at=clock_timestamp()
                        WHERE ts_code=%s AND raw_payload->>'first_ann_date'=%s''', (code, first))
                engine._log(conn, job_id, 'info', f'seasoned-scope-review:{code}:{first}:rows={len(facts)}')
            result.success_batches += 1
        except Exception as exc:
            result.failed_batches += 1
            result.error = str(exc)[:4000]
            break
        engine._update_progress(conn, job_id, result)
    return result


def validate_partition(conn, spec, day, revision):
    from .data_integrity.contracts import CheckCounts, PartitionState, ValidationResult
    from .data_integrity.validation import rows, unverified
    audit = rows(conn, "SELECT metadata,status FROM market.dataset_date_refresh_audit "
                 "WHERE dataset=%s AND trade_date=%s AND data_source='tushare'", (spec.name, day))
    metadata = audit[0]['metadata'] if audit else {}
    scan = metadata.get('event_scan', {})
    if (not audit or audit[0]['status'] != 'success' or scan.get('complete') is not True
            or scan.get('schema_version') != 'tushare_event_scan_v1'
            or metadata.get('integrity_observed_revision') != revision):
        return unverified('complete_current_event_query_required')
    expected = set(scan.get('source_rows_sha256', []))
    physical = rows(conn, sql.SQL('SELECT source_row_sha256,raw_payload,event_date,ts_code,published_at,'
        'publication_time_quality,vintage_quality FROM {} WHERE source_api=%s AND source_row_sha256=ANY(%s)').format(
            sql.Identifier(*spec.target_table.split('.'))), (spec.name, sorted(expected)))
    found, invalid = set(), set()
    for row in physical:
        key = row['source_row_sha256']
        found.add(key)
        try:
            fact = observation(spec.name, row['raw_payload'], day)
            if (fact['source_row_sha256'] != key or fact['event_date'] != day
                    or row['event_date'] != fact['event_date'] or row['ts_code'] != fact['ts_code']
                    or row['published_at'] is not None or row['publication_time_quality'] != 'UNKNOWN'
                    or row['vintage_quality'] != 'NON_VINTAGE'):
                invalid.add(key)
        except (ValueError, TypeError, KeyError):
            invalid.add(key)
    missing = expected-found
    count = CheckCounts('observed_raw_version', len(expected), len(found-invalid), 0, len(missing), len(invalid), 0)
    # The completed query itself is a non-vacuous check even on a legitimate
    # sparse empty day. It does not claim the provider's historic completeness.
    query = CheckCounts('completed_event_query', 1, 1, 0, 0, 0, 0)
    return ValidationResult(PartitionState.VALID if count.complete else PartitionState.INCOMPLETE,
        (query, count), {'physical_rows': len(found), 'provider_observed_rows': scan['observed_rows'],
                        'publication_time_quality': 'UNKNOWN', 'vintage_quality': 'NON_VINTAGE',
                        'coverage_basis': 'completed_provider_query_not_historical_PIT'},
        tuple({'classification': kind, 'source_rows_sha256': sorted(keys)}
              for kind, keys in (('MISSING', missing), ('INVALID', invalid)) if keys))
