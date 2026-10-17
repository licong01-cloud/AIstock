"""Pinned, bounded, NULL-only daily share precision repair (dry-run default).

No provider calls, raw OHLC/amount rewrites, schema changes or candidate writes.
The additive migration is independently DEV-validated/production-authorized.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sys

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.prepare_canonical_pit_monthly import _load_database_config
from backend.services.dataset_release.canonical import canonical_json_bytes


SCHEMA = 'aistock_raw_volume_precision_repair_v1'
OLD = ('trade_date', 'ts_code', 'open_li', 'high_li', 'low_li', 'close_li',
       'volume_hand', 'amount_li', 'adjust_type', 'source')
PRECISE = ('volume_shares', 'volume_shares_source', 'volume_shares_sha256')


def pinned_json(path, pin):
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != pin:
        raise ValueError('source content pin differs')
    return json.loads(raw)


def load_facts(manifest):
    if manifest.get('schema_version') != SCHEMA or not manifest.get('sources') or not manifest.get('keys'):
        raise ValueError('repair manifest is incomplete')
    start, end = (date.fromisoformat(manifest[k]) for k in ('start_date', 'end_date'))
    if (start.year, start.month) != (end.year, end.month) or start > end or end >= date.today():
        raise ValueError('repair is bounded to one completed calendar month')
    keys = [(r['symbol'], date.fromisoformat(r['trade_date'])) for r in manifest['keys']]
    if len(keys) != len(set(keys)) or any(not start <= d <= end or re.fullmatch(r'\d{6}\.(SZ|SH)', c) is None for c,d in keys):
        raise ValueError('repair keys are duplicate or outside the declared month')
    expected, facts = set(keys), {}
    for ref in manifest['sources']:
        source = pinned_json(ref['path'], ref['sha256'])
        if source.get('api') != 'daily' or not source.get('captured_at'):
            raise ValueError('repair source is not a captured official daily response')
        for row in source['rows']:
            key = (row['ts_code'], datetime.strptime(row['trade_date'], '%Y%m%d').date())
            if key not in expected:
                continue
            if key in facts:
                raise ValueError('duplicate authoritative daily key')
            if isinstance(row.get('vol'), bool) or not isinstance(row.get('vol'), (int,float)):
                raise ValueError('official daily volume is not numeric')
            shares = Decimal(str(row['vol'])) * 100  # Tushare daily vol: hands.
            if not shares.is_finite() or shares < 0 or shares != shares.to_integral_value():
                raise ValueError('official daily volume is not a finite whole-share fact')
            prices = tuple(Decimal(str(row[f])) * 1000 for f in ('open', 'high', 'low', 'close'))
            if any(not v.is_finite() or v <= 0 for v in prices):
                raise ValueError('official daily OHLC is invalid')
            facts[key] = (shares, ref['sha256'], prices)
    if set(facts) != expected:
        raise ValueError('repair contains keys without authoritative facts')
    return facts


def connect(database, dotenv):
    cfg = _load_database_config(database, dotenv)
    return psycopg2.connect(host=cfg.host, port=cfg.port, dbname=cfg.dbname,
                            user=cfg.user, password=cfg.password)


def read_rows(conn, keys):
    result = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SET LOCAL statement_timeout='45s'")
        for offset in range(0, len(keys), 200):
            batch = keys[offset:offset+200]
            start = min(k[1] for k in batch).isoformat()
            end = max(k[1] for k in batch).isoformat()
            # Validated date objects rendered as constants allow Timescale
            # partition pruning; the exact symbol/date join remains mandatory.
            cur.execute(f"""SELECT d.* FROM market.kline_daily_raw d
                        JOIN unnest(%s::text[],%s::date[]) k(code,day)
                          ON d.ts_code=k.code AND d.trade_date=k.day
                        WHERE d.adjust_type='none'
                          AND d.trade_date BETWEEN DATE '{start}' AND DATE '{end}'""", ([k[0] for k in batch], [k[1] for k in batch]))
            for row in cur.fetchall():
                key = (row['ts_code'], row['trade_date'])
                if key in result:
                    raise ValueError('duplicate raw daily row')
                result[key] = dict(row)
    return result


def apply_precision_patches(cur, patches):
    start = min(p[0] for p in patches).isoformat()
    end = max(p[0] for p in patches).isoformat()
    return execute_values(cur, f"""UPDATE market.kline_daily_raw t
        SET volume_shares=v.shares,volume_shares_source='tushare_daily',volume_shares_sha256=v.pin
        FROM (VALUES %s) v(day,code,shares,pin,hands)
        WHERE t.trade_date=v.day AND t.ts_code=v.code AND t.adjust_type='none'
          AND t.trade_date BETWEEN DATE '{start}' AND DATE '{end}'
          AND t.volume_hand=v.hands AND t.volume_shares IS NULL
          AND t.volume_shares_source IS NULL AND t.volume_shares_sha256 IS NULL
        RETURNING t.ts_code,t.trade_date""", patches, page_size=200, fetch=True)


def run(args):
    manifest_raw = args.manifest.read_bytes()
    manifest_pin = hashlib.sha256(manifest_raw).hexdigest()
    manifest = json.loads(manifest_raw)
    facts = load_facts(manifest)
    keys = sorted(facts)
    if args.receipt.exists():
        raise ValueError('refuse to overwrite immutable receipt')
    if args.seed_dev and (args.database != 'dev' or not args.apply):
        raise ValueError('seeding is exclusively DEV apply')
    if args.apply and args.database == 'production':
        if args.dev_validation is None:
            raise ValueError('production repair requires exact DEV readback')
        dev = json.loads(args.dev_validation.read_bytes())
        if (dev.get('database') != 'dev' or not dev.get('apply') or dev.get('status') != 'PASS'
                or dev.get('manifest_sha256') != manifest_pin or not dev.get('protected_columns_unchanged')):
            raise ValueError('DEV receipt is not bound to this repair')
    with connect(args.database, args.dotenv) as conn:
        conn.set_session(readonly=not args.apply, isolation_level='REPEATABLE READ')
        with conn.cursor() as cur:
            cur.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='market' AND table_name='kline_daily_raw'")
            columns = dict(cur.fetchall())
            if columns.get('volume_shares') != 'numeric' or not set(PRECISE) <= set(columns):
                raise ValueError('precision schema migration has not been applied')
        before = read_rows(conn, keys)
        seeded = 0
        if args.seed_dev and set(keys) - set(before):
            missing = sorted(set(keys) - set(before))
            with connect('production', args.dotenv) as source:
                source.set_session(readonly=True, isolation_level='REPEATABLE READ')
                seed = read_rows(source, missing)
            if set(seed) != set(missing):
                raise ValueError('DEV seeds lack genuine production raw rows')
            with conn.cursor() as cur:
                execute_values(cur, 'INSERT INTO market.kline_daily_raw (' + ','.join(OLD) + ') VALUES %s ON CONFLICT DO NOTHING',
                               [tuple(seed[k][f] for f in OLD) for k in missing], page_size=200)
            seeded = len(missing)
            before = read_rows(conn, keys)
        if set(before) != set(keys):
            raise ValueError('raw daily row absent; precision repair cannot synthesize OHLC')
        patches = []
        for key, (shares, pin, prices) in facts.items():
            row = before[key]
            if abs(shares - Decimal(str(row['volume_hand'])) * 100) >= 100:
                raise ValueError(f'repair is not a whole-hand precision correction at {key}')
            if tuple(Decimal(str(row[f])) for f in ('open_li','high_li','low_li','close_li')) != prices:
                raise ValueError(f'official daily OHLC differs at {key}')
            proposed = {**row, 'volume_shares': shares, 'volume_shares_source': 'tushare_daily', 'volume_shares_sha256': pin}
            if row['volume_shares'] is not None:
                if any(row[f] != proposed[f] for f in PRECISE):
                    raise ValueError(f'existing precision differs at {key}')
                continue
            if any(row[f] is not None for f in PRECISE[1:]):
                raise ValueError(f'orphan precision provenance at {key}')
            patches.append((key[1], key[0], shares, pin, row['volume_hand']))
        if args.apply and patches:
            with conn.cursor() as cur:
                changed = apply_precision_patches(cur, patches)
                if len(changed) != len(patches):
                    raise ValueError('NULL-only precision CAS affected-row mismatch')
        after = read_rows(conn, keys)
        if any(tuple(before[k][f] for f in OLD) != tuple(after[k][f] for f in OLD) for k in keys):
            raise ValueError('protected raw columns changed')
        if args.apply:
            for key, (shares,pin,_prices) in facts.items():
                if (after[key]['volume_shares'],after[key]['volume_shares_source'],after[key]['volume_shares_sha256']) != (shares,'tushare_daily',pin):
                    raise ValueError('precision write readback differs')
        conn.commit()
    # Independent fresh-connection readback after the commit.
    if args.apply:
        with connect(args.database, args.dotenv) as conn:
            conn.set_session(readonly=True, isolation_level='REPEATABLE READ')
            final = read_rows(conn, keys)
        if any(tuple(final[k][f] for f in (*OLD,*PRECISE)) != tuple(after[k][f] for f in (*OLD,*PRECISE)) for k in keys):
            raise ValueError('fresh-process precision readback differs')
    result = dict(schema_version=SCHEMA, database=args.database, apply=args.apply,
                  start_date=manifest['start_date'],end_date=manifest['end_date'],
                  manifest_sha256=manifest_pin, expected_keys=len(keys), precision_patch_count=len(patches),
                  updated_count=len(patches) if args.apply else 0, dev_seeded_rows=seeded,
                  protected_columns_unchanged=True, source_pins=manifest['sources'], status='PASS',
                  database_write=args.apply, production_ddl=False, candidate_write=False,
                  profile_write=False, runtime_action=False, captured_at=datetime.now(timezone.utc).isoformat())
    raw = canonical_json_bytes(result) + b'\n'
    with args.receipt.open('xb') as stream:
        stream.write(raw)
    print(json.dumps({'receipt':str(args.receipt),'sha256':hashlib.sha256(raw).hexdigest(),
                      'expected_keys':len(keys),'updated_count':result['updated_count'],'status':'PASS'}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--database', choices=('dev','production'), required=True)
    parser.add_argument('--dotenv', type=Path, default=Path('F:/Dev/AIstock/.env'))
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--seed-dev', action='store_true')
    parser.add_argument('--dev-validation', type=Path)
    run(parser.parse_args())
