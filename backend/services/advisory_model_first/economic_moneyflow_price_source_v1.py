"""Two bounded read-only SELECTs over original keys; no QE cache or DB writes."""
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import math

import numpy as np
import pandas as pd

from backend.data_service.moneyflow_contract import moneyflow_unit_contract_receipt, normalize_tushare_moneyflow_units
from backend.services.advisory_model_first.economic_entry_labels import KEY, _frame
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import AMOUNT_FIELDS, moneyflow_calendar_v1
from backend.services.advisory_model_first.entry_price_daily_service import BoundedEntryReadSession, EntryWorkBudget


@dataclass(frozen=True)
class MoneyflowSourceBatchV1:
    amounts: pd.DataFrame
    calendar: pd.DatetimeIndex
    receipt: dict


def normalize_moneyflow_boundary_v1(raw):
    if raw.attrs.get('moneyflow_unit_contract') or raw.attrs.get('moneyflow_amount_unit'):
        raise ValueError('moneyflow source is already normalized')
    if not {'instrument', 'trade_date', *AMOUNT_FIELDS}.issubset(raw.columns):
        raise ValueError('moneyflow strict eight-field source schema differs')
    for value in raw.loc[:, AMOUNT_FIELDS].to_numpy().ravel():
        if value is None or value is pd.NA:
            continue
        if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, Decimal, np.integer, np.floating)) or not math.isfinite(float(value)) or float(value) < 0:
            raise ValueError('moneyflow raw amount must be numeric finite nonnegative or NULL')
    amounts = normalize_tushare_moneyflow_units(raw, copy=True, require_all=False)
    amounts.attrs['moneyflow_amount_unit'] = 'cny'
    return amounts


def load_moneyflow_source_v1(*, candidates, session_factory=None):
    roster = _frame(candidates.loc[:, KEY], KEY, set(KEY))
    if roster.empty or len(roster) > 7720:
        raise ValueError('moneyflow source needs bounded original candidate keys')
    first, last = roster[KEY[0]].min().date(), roster[KEY[0]].max().date()
    session = (session_factory or (lambda: BoundedEntryReadSession(EntryWorkBudget(30))))()
    requested_at = datetime.now(timezone.utc).isoformat()
    try:
        with session.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''SELECT (SELECT jsonb_object_agg(column_name,data_type)
                    FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=ANY(%s)),
                    ARRAY(SELECT cal_date FROM market.trading_calendar WHERE is_trading=TRUE
                      AND cal_date>=(SELECT MIN(cal_date) FROM (SELECT cal_date FROM market.trading_calendar
                          WHERE is_trading=TRUE AND cal_date<=%s ORDER BY cal_date DESC LIMIT 5) warmup)
                      AND cal_date<=%s ORDER BY cal_date)''',
                    ('market', 'moneyflow_ts', ['ts_code', 'trade_date', *AMOUNT_FIELDS], first, roster[KEY[1]].max().date()))
                schema, calendar = cursor.fetchone()
                if (set(schema or {}) != {'ts_code', 'trade_date', *AMOUNT_FIELDS}
                        or schema['ts_code'] not in ('text', 'character varying') or schema['trade_date'] != 'date'
                        or any(schema[field] not in ('numeric', 'double precision', 'real') for field in AMOUNT_FIELDS)):
                    raise ValueError('moneyflow database schema differs')
                _, days, windows = moneyflow_calendar_v1(roster, calendar)
                pairs = sorted({(item[KEY[2]], day.date()) for item in roster.to_dict('records') for day in windows[item[KEY[0]]]})
                if not pairs or len(pairs) > 38600:
                    raise ValueError('moneyflow exact stock-day budget differs')
                symbols, dates = zip(*pairs, strict=True)
                # Literal date bounds permit chunk pruning while retaining ALL original keys.
                columns = ','.join('m.'+name for name in AMOUNT_FIELDS)
                cursor.execute(f'''WITH requested AS (SELECT * FROM unnest(%s::text[],%s::date[]) AS p(ts_code,trade_date))
                    SELECT p.ts_code,p.trade_date,{columns} FROM requested p LEFT JOIN market.moneyflow_ts m
                    ON m.ts_code=p.ts_code AND m.trade_date=p.trade_date AND m.trade_date>=%s::date AND m.trade_date<=%s::date
                    ORDER BY p.ts_code,p.trade_date''', (list(symbols), list(dates), min(dates), last))
                rows = cursor.fetchall()
    finally:
        session.close()
    if len(rows) != len(pairs) or len({(row[0], row[1]) for row in rows}) != len(rows):
        raise ValueError('moneyflow duplicate/missing requested source keys')
    raw = pd.DataFrame(rows, columns=['instrument', 'trade_date', *AMOUNT_FIELDS], dtype=object)
    if set(zip(raw.instrument, raw.trade_date, strict=True)) != set(pairs):
        raise ValueError('moneyflow returned foreign source keys')
    amounts = normalize_moneyflow_boundary_v1(raw)
    return MoneyflowSourceBatchV1(amounts, days, dict(query_at=requested_at, selects=2, requested_stock_days=len(pairs),
        source_table='market.moneyflow_ts', query_revision='EXACT_ORIGINAL_KEYS_CALENDAR_RANGE_V1', unit=moneyflow_unit_contract_receipt(),
        source_evidence='CURRENT_DB_HISTORICAL_NON_VINTAGE', native_identity='UNPROVEN', sealed_accessed=False, database_written=False))
