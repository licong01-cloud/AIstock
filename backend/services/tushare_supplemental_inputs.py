"""Small source contracts for optional VIX and after-hours inputs, not a new engine."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from psycopg2.extras import Json, execute_values

RAW_DATASETS = frozenset({'vix_index', 'stock_daily_afterhours'})


def latest_vix_query_date(now=None):
    """Conservative ended-session date bound, not a holiday/publication oracle."""
    current = (now or dt.datetime.now(dt.timezone.utc)).astimezone(ZoneInfo('America/New_York'))
    return current.date() if current.hour >= 17 else current.date() - dt.timedelta(days=1)


def normalize_rows(spec, rows):
    result = []
    for source in rows:
        row = dict(source)
        fields = ('limit_status',) if spec.name == 'daily_basic' else (
            ('close', 'pct_change') if spec.name == 'vix_index' else ('ah_vol', 'ah_amount'))
        for field in fields:
            value = row.get(field)
            if value is None:
                if spec.name == 'vix_index':
                    raise ValueError(f'{spec.name}.{field}: missing required fact')
                row[field] = None
                continue
            try:
                if isinstance(value, bool):
                    raise ValueError('boolean is not a number')
                number = Decimal(str(value))
                if not number.is_finite():
                    raise ValueError('non-finite')
                if field == 'limit_status' and (number != number.to_integral_value() or not 0 <= number <= 6):
                    raise ValueError('expected integer 0..6')
                if field == 'close' and number <= 0:
                    raise ValueError('expected positive close')
                if field in {'ah_vol', 'ah_amount'} and number < 0:
                    raise ValueError('expected non-negative fact')
            except (ValueError, InvalidOperation) as exc:
                raise ValueError(f'{spec.name}.{field}: invalid provider fact') from exc
            row[field] = int(number) if field == 'limit_status' else number
        result.append(row)
    return result


def request_windows(start, end):
    while start <= end:
        stop = min(end, start + dt.timedelta(days=89))
        yield start, stop
        start = stop + dt.timedelta(days=1)


def raw_observation(spec, source):
    # Stable row-content identity, not an invented supplier record ID.
    payload = {key: source.get(key) for key in spec.columns}
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(',', ':'),
                         allow_nan=False, default=str)
    digest = hashlib.sha256(encoded.encode('utf-8')).hexdigest()
    day = dt.date.fromisoformat(str(source['trade_date'])) if '-' in str(source['trade_date']) else (
        dt.datetime.strptime(str(source['trade_date']), '%Y%m%d').date())
    return spec.name, spec.tushare_api, day, source.get('ts_code'), digest, Json(json.loads(encoded))


def store_observations(conn, spec, rows, write_facts):
    """Atomic current projection + original versions, even with autocommit pools."""
    if not rows:
        return 0
    normalized = normalize_rows(spec, rows)  # reject before writing any original row
    observations = [raw_observation(spec, row) for row in rows]
    owns_transaction = conn.autocommit
    if owns_transaction:
        conn.autocommit = False
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO market.tushare_supplemental_raw
                  (dataset, source_api, trade_date, ts_code, source_row_sha256, raw_payload)
                VALUES %s ON CONFLICT (dataset, source_row_sha256) DO UPDATE
                  SET last_observed_at=clock_timestamp()
            """, observations)
        count = write_facts(normalized)
        if owns_transaction:
            conn.commit()
        return count
    except Exception:
        if owns_transaction:
            conn.rollback()
        raise
    finally:
        if owns_transaction:
            conn.autocommit = True
