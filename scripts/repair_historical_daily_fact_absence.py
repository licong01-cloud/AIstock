"""Prepare pinned, new-key-only historical daily repairs; DEV apply is explicit.

No provider calls, production apply switch, PIT changes, or release mutations.
Tushare daily OHLC is CNY, vol is hands, and amount is thousand CNY.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

COLUMNS = (
    'trade_date', 'ts_code', 'open_li', 'high_li', 'low_li', 'close_li',
    'volume_hand', 'amount_li', 'adjust_type', 'source', 'volume_shares',
    'volume_shares_source', 'volume_shares_sha256',
)
START, END = date(2018, 8, 1), date(2026, 9, 30)


def _integer(value, scale, maximum):
    if isinstance(value, bool) or value is None:
        raise ValueError('source numeric value is missing or boolean')
    try:
        number = Decimal(str(value)) * scale
    except InvalidOperation as exc:
        raise ValueError('source numeric value is invalid') from exc
    if (not number.is_finite() or number < 0 or number > maximum
            or number != number.to_integral_value()):
        raise ValueError('source numeric precision or range is invalid')
    return int(number)


def normalize_row(row, source_sha256):
    if re.fullmatch(r'[0-9a-f]{64}', source_sha256) is None:
        raise ValueError('source pin is invalid')
    code = row.get('ts_code', '')
    if not isinstance(code, str) or re.fullmatch(r'\d{6}\.(SH|SZ)', code) is None:
        raise ValueError('source security code is invalid')
    text_date = row.get('trade_date', '')
    if not isinstance(text_date, str) or re.fullmatch(r'\d{8}', text_date) is None:
        raise ValueError('source trade date is invalid')
    day = datetime.strptime(text_date, '%Y%m%d').date()
    if not START <= day <= END:
        raise ValueError('source trade date is outside the historical repair')
    prices = tuple(_integer(row.get(key), 1000, 2**31 - 1)
                   for key in ('open', 'high', 'low', 'close'))
    op, hi, lo, cl = prices
    if min(prices) <= 0 or not lo <= min(op, cl) <= max(op, cl) <= hi:
        raise ValueError('source OHLC is inconsistent')
    shares = _integer(row.get('vol'), 100, 2**63 - 1)
    amount = _integer(row.get('amount'), 1_000_000, 2**63 - 1)
    if shares == 0 and amount == 0:
        raise ValueError('source row does not prove actual trading')
    return (day, code, *prices, shares // 100, amount, 'none', 'tushare_api',
            shares, 'tushare_daily', source_sha256)


def load_capture(path, expected_sha256=None):
    raw = Path(path).read_bytes()
    if expected_sha256 is not None and hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('capture receipt pin differs')
    receipt = json.loads(raw)
    if (receipt.get('schema_version') != 'aistock_daily_source_absence_provider_readback_v1'
            or receipt.get('database_write') is not False or not receipt.get('symbols')):
        raise ValueError('capture receipt is invalid')
    facts, seen, symbols = [], set(), set()
    for ref in receipt['symbols']:
        if ref['symbol'] in symbols:
            raise ValueError('duplicate source symbol')
        symbols.add(ref['symbol'])
        source_path = Path(ref['source_path'])
        if source_path.is_symlink() or not source_path.is_file():
            raise ValueError('source must be a regular file')
        source_raw = source_path.read_bytes()
        if hashlib.sha256(source_raw).hexdigest() != ref['source_sha256']:
            raise ValueError('source content pin differs')
        source = json.loads(source_raw)
        response = source['response']
        if (source.get('api_name') != 'daily' or type(ref.get('provider_code')) is not int
                or ref['provider_code'] != 0 or type(response.get('code')) is not int
                or response['code'] != 0
                or source['request'] != {'ts_code': ref['symbol'],
                                        'start_date': '20180801', 'end_date': '20260930'}):
            raise ValueError('source request/response identity differs')
        data = response['data']
        fields = data['fields']
        if len(fields) != len(set(fields)):
            raise ValueError('duplicate source field')
        count = 0
        for values in data['items']:
            if len(values) != len(fields):
                raise ValueError('source row width differs')
            row = dict(zip(fields, values))
            if row.get('ts_code') != ref['symbol']:
                raise ValueError('source security identity differs')
            fact = normalize_row(row, ref['source_sha256'])
            if fact[:2] in seen:
                raise ValueError('duplicate source key')
            seen.add(fact[:2])
            facts.append(fact)
            count += 1
        if count != ref['actual_traded_rows']:
            raise ValueError('source row count differs')
    if len(facts) != receipt['actual_traded_rows'] or not facts:
        raise ValueError('capture total row count differs')
    return sorted(facts)


def _literal(value):
    if isinstance(value, date):
        return "DATE '" + value.isoformat() + "'"
    if isinstance(value, int):
        return str(value)
    return "'" + value.replace("'", "''") + "'"


def build_sql(facts, *, database):
    if database not in ('aistock', 'aistock_dev') or not facts:
        raise ValueError('exact database and nonempty facts are required')
    months = defaultdict(list)
    keys = set()
    for fact in facts:
        if len(fact) != len(COLUMNS) or fact[:2] in keys:
            raise ValueError('invalid or duplicate fact')
        keys.add(fact[:2])
        months[fact[0].strftime('%Y-%m')].append(fact)
    sql = ["BEGIN;\nSET LOCAL lock_timeout='5s';\nSET LOCAL statement_timeout='180s';",
           f"DO $guard$ BEGIN IF current_database() <> '{database}' THEN "
           "RAISE EXCEPTION 'wrong repair database'; END IF; END; $guard$;"]
    columns = ','.join(COLUMNS)
    comparisons = ','.join('r.' + c for c in COLUMNS)
    expected = ','.join('f.' + c for c in COLUMNS)
    for batch in months.values():
        tuples = []
        for fact in batch:
            values = [_literal(v) for v in fact]
            values[1] += '::bpchar'
            values[8] += '::bpchar'
            tuples.append('(' + ','.join(values) + ')')
        values_sql = ',\n'.join(tuples)
        source = f'(VALUES {values_sql}) AS f({columns})'
        bounds = (f"r.trade_date BETWEEN DATE '{min(f[0] for f in batch)}' "
                  f"AND DATE '{max(f[0] for f in batch)}'")
        join = ("r.trade_date=f.trade_date AND r.ts_code=f.ts_code "
                "AND r.adjust_type=f.adjust_type")
        sql.append(f"DO $guard$ BEGIN IF EXISTS (SELECT 1 FROM {source} "
                   f"JOIN market.kline_daily_raw r ON {join} WHERE {bounds} AND "
                   f"ROW({comparisons}) IS DISTINCT FROM ROW({expected})) THEN "
                   "RAISE EXCEPTION 'existing raw daily fact conflicts'; END IF; END; $guard$;")
        from backend.services.data_integrity.writes import tracked_repair_sql
        sql.append(tracked_repair_sql(f'INSERT INTO market.kline_daily_raw ({columns}) VALUES {values_sql}\n'
                   'ON CONFLICT (ts_code,trade_date) DO NOTHING RETURNING trade_date', 'kline_daily_raw'))
        sql.append(f"DO $guard$ BEGIN IF EXISTS (SELECT 1 FROM {source} "
                   f"LEFT JOIN market.kline_daily_raw r ON {join} AND {bounds} "
                   f"WHERE r.ts_code IS NULL OR ROW({comparisons}) IS DISTINCT FROM ROW({expected})) "
                   "THEN RAISE EXCEPTION 'readback is incomplete or changed'; END IF; END; $guard$;")
    return '\n'.join(sql) + '\nCOMMIT;\n'


def readback(conn, facts):
    from psycopg2.extras import execute_values

    actual = []
    months = defaultdict(list)
    for fact in facts:
        months[fact[0].strftime('%Y-%m')].append(fact)
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout='180s'")
        for batch in months.values():
            query = ('SELECT ' + ','.join('r.' + c for c in COLUMNS)
                     + ' FROM market.kline_daily_raw r JOIN (VALUES %s) k(day,code)'
                       ' ON r.trade_date=k.day AND r.ts_code=k.code::bpchar'
                       " WHERE r.adjust_type='none' AND r.trade_date BETWEEN %s AND %s")
            # Date literals are validated date objects, not free-form input.
            query = query.replace('BETWEEN %s AND %s',
                                  f"BETWEEN DATE '{min(f[0] for f in batch)}' "
                                  f"AND DATE '{max(f[0] for f in batch)}'")
            actual.extend(execute_values(cur, query, [f[:2] for f in batch],
                                         page_size=len(batch), fetch=True))
    return sorted(actual)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture', type=Path, required=True)
    parser.add_argument('--capture-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--dotenv', type=Path, default=Path('F:/Dev/AIstock/.env'))
    parser.add_argument('--apply-dev', action='store_true')
    args = parser.parse_args()
    facts = load_capture(args.capture, args.capture_sha256)
    args.output.mkdir(parents=True, exist_ok=False)
    sql_pins = {}
    for database in ('aistock_dev', 'aistock'):
        raw = build_sql(facts, database=database).encode('utf8')
        path = args.output / (database + '-repair.sql')
        with path.open('xb') as stream:
            stream.write(raw)
        sql_pins[database] = {'path': str(path.resolve()),
                              'sha256': hashlib.sha256(raw).hexdigest()}
    dev = {'performed': False}
    if args.apply_dev:
        import psycopg2
        from scripts.prepare_canonical_pit_monthly import _load_database_config

        config = _load_database_config('dev', args.dotenv)
        if config.dbname != 'aistock_dev':
            raise ValueError('DEV target identity differs')
        with psycopg2.connect(host=config.host, port=config.port, dbname=config.dbname,
                              user=config.user, password=config.password) as conn:
            before = readback(conn, facts)
            conn.commit()
            raw = Path(sql_pins['aistock_dev']['path']).read_bytes()
            if hashlib.sha256(raw).hexdigest() != sql_pins['aistock_dev']['sha256']:
                raise ValueError('DEV SQL pin differs')
            with conn.cursor() as cur:
                cur.execute(raw.decode('utf8'))
            after = readback(conn, facts)
            if after != facts:
                raise ValueError('DEV precise readback differs')
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(raw.decode('utf8'))
            repeated = readback(conn, facts)
            if repeated != after:
                raise ValueError('DEV repeat changed facts')
            dev = {'performed': True, 'before_rows': len(before),
                   'after_rows': len(after), 'inserted_rows': len(after) - len(before),
                   'repeat_inserted_rows': 0, 'readback_equal': True}
    receipt = {'schema_version': 'aistock_historical_daily_absence_repair_v1',
               'captured_at': datetime.now(timezone.utc).isoformat(),
               'source_receipt_sha256': args.capture_sha256,
               'rows': len(facts), 'symbols': len({f[1] for f in facts}),
               'start_date': str(min(f[0] for f in facts)),
               'end_date': str(max(f[0] for f in facts)), 'sql': sql_pins,
               'dev_validation': dev, 'production_database_write': False,
               'candidate_write': False, 'profile_write': False,
               'runtime_action': False, 'outcomes_read': False}
    with (args.output / 'receipt.json').open('x', encoding='utf8') as stream:
        json.dump(receipt, stream, sort_keys=True, separators=(',', ':'), allow_nan=False)
        stream.write('\n')
    print(json.dumps(receipt, sort_keys=True))


if __name__ == '__main__':
    main()
