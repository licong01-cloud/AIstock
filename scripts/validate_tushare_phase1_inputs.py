"""Existing-DEV-only bounded smoke; default mode is independent readback.

No production option. Explicit --migrate-dev / --sync-dev are separately recorded.
No daemon, scheduler, model or data release is started.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--start', type=dt.date.fromisoformat, default=dt.date(2026, 11, 4))
    parser.add_argument('--end', type=dt.date.fromisoformat, default=dt.date(2026, 11, 5))
    parser.add_argument('--migrate-dev', action='store_true')
    parser.add_argument('--sync-dev', action='store_true')
    args = parser.parse_args()
    if not 0 <= (args.end-args.start).days <= 3:
        parser.error('DEV smoke window must be 1..4 calendar days')
    if args.output.resolve().drive.upper() != 'X:':
        parser.error('temporary outputs must be on X:')

    from scripts.prepare_canonical_pit_monthly import _load_database_config, _database_target_environment
    config = _load_database_config('dev', args.env_file)
    if config.dbname != 'aistock_dev' or config.port != 5433 or config.host not in {'127.0.0.1', 'localhost'}:
        raise ValueError('existing DEV identity does not match; no connection opened')
    connection = dict(host=config.host, port=config.port, dbname=config.dbname,
                      user=config.user, password=config.password, connect_timeout=10)
    conn = psycopg2.connect(**connection)
    conn.autocommit = True
    migration = ROOT / 'backend/db/migrations/add_tushare_phase1_inputs_v1.sql'
    report = {'database_target': 'dev', 'production_write': False, 'process_control': False,
              'migration_applied': False, 'source_sync': [], 'window': [str(args.start), str(args.end)],
              'migration_sha256': hashlib.sha256(migration.read_bytes()).hexdigest()}
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT current_database()')
            if cur.fetchone()[0] != 'aistock_dev':
                raise ValueError('unexpected DEV database')
            if args.migrate_dev:
                # Repeat migration intentionally to validate idempotent DDL.
                for _ in range(2):
                    cur.execute(migration.read_text(encoding='utf-8'))
                report['migration_applied'] = True
        if args.sync_dev:
            values = dotenv_values(args.env_file)
            if not values.get('TUSHARE_TOKEN'):
                raise ValueError('provider credential location has no token')
            os.environ['TUSHARE_TOKEN'] = str(values['TUSHARE_TOKEN'])
            os.environ['AISTOCK_TRADING_CALENDAR_CACHE'] = str(args.output.parent / 'dev-calendar.json')
            with _database_target_environment(config):
                from backend.db import pg_pool
                from backend.services.tushare_sync_engine import TushareSyncEngine
                from backend.services.tushare_dataset_specs import DATASET_REGISTRY
                try:
                    for name in ('vix_index', 'stock_daily_afterhours', 'daily_basic'):
                        # Same native public sync path as scheduler; two reads prove idempotent facts.
                        results = []
                        for _ in range(2):
                            result = TushareSyncEngine().sync(DATASET_REGISTRY[name], mode='init',
                                                             start_date=args.start, end_date=args.end)
                            results.append(result.as_dict())
                            if not result.ok:
                                raise ValueError(f'{name}: DEV native sync failed; see job record')
                        report['source_sync'].append({'dataset': name, 'results': results})
                    # Existing single-dataset statistics refresh, never scan unrelated minute/history tables.
                    from backend.routers.ingestion import _refresh_one_data_stats
                    from psycopg2.extras import RealDictCursor
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""SELECT * FROM market.data_stats_config
                          WHERE data_kind IN ('vix_index','stock_daily_afterhours') ORDER BY data_kind""")
                        configs = [dict(row) for row in cur.fetchall()]
                    report['stats_refresh'] = [_refresh_one_data_stats(cfg) for cfg in configs]
                finally:
                    pg_pool.close_db_pool()
        with conn.cursor() as cur:
            cur.execute("""SELECT table_name, count(*), count(col_description(
              (quote_ident(table_schema)||'.'||quote_ident(table_name))::regclass, ordinal_position))
              FROM information_schema.columns WHERE table_schema='market'
              AND table_name IN ('vix_index','stock_daily_afterhours','tushare_supplemental_raw')
              GROUP BY table_schema,table_name ORDER BY table_name""")
            report['column_comments'] = cur.fetchall()
            cur.execute("""SELECT data_kind,table_name,date_column,updated_column FROM market.data_stats_config
              WHERE data_kind IN ('vix_index','stock_daily_afterhours') ORDER BY data_kind""")
            report['stats_config'] = cur.fetchall()
            cur.execute("""SELECT dataset,frequency,enabled,options FROM market.ingestion_schedules
              WHERE dataset IN ('vix_index','_vix_index_midday','stock_daily_afterhours') ORDER BY dataset""")
            report['schedules'] = cur.fetchall()
            cur.execute("""SELECT trade_date,close,pct_change,updated_at FROM market.vix_index
              WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date""", (args.start, args.end))
            report['vix_facts'] = cur.fetchall()
            cur.execute("""SELECT trade_date,count(*),count(ah_vol),count(ah_amount),
              count(*) FILTER (WHERE ah_vol>0),sum(ah_vol),sum(ah_amount)
              FROM market.stock_daily_afterhours WHERE trade_date BETWEEN %s AND %s
              GROUP BY trade_date ORDER BY trade_date""", (args.start, args.end))
            report['afterhours_facts'] = cur.fetchall()
            cur.execute("""SELECT trade_date,count(*),count(limit_status),min(limit_status),max(limit_status)
              FROM market.daily_basic WHERE trade_date BETWEEN %s AND %s
              GROUP BY trade_date ORDER BY trade_date""", (args.start, args.end))
            report['limit_status_facts'] = cur.fetchall()
            cur.execute("""SELECT dataset,trade_date,count(*),min(captured_at),max(last_observed_at),
              count(*) FILTER (WHERE published_at IS NOT NULL OR publication_time_quality<>'UNKNOWN'
                              OR vintage_quality<>'NON_VINTAGE')
              FROM market.tushare_supplemental_raw WHERE trade_date BETWEEN %s AND %s
              GROUP BY dataset,trade_date ORDER BY dataset,trade_date""", (args.start, args.end))
            report['original_versions'] = cur.fetchall()
            cur.execute("""SELECT dataset,trade_date,data_revision FROM market.dataset_date_refresh_audit
              WHERE dataset IN ('vix_index','stock_daily_afterhours','daily_basic')
              AND data_source='canonical_db' AND trade_date BETWEEN %s AND %s
              ORDER BY dataset,trade_date""", (args.start, args.end))
            report['revisions'] = cur.fetchall()
            cur.execute("""SELECT raw_payload,source_row_sha256 FROM market.tushare_supplemental_raw
              WHERE trade_date BETWEEN %s AND %s""", (args.start, args.end))
            originals = cur.fetchall()
            mismatches = sum(hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False,
                separators=(',', ':'), allow_nan=False).encode()).hexdigest() != digest
                for payload, digest in originals)
            report['raw_identity_readback'] = {'rows': len(originals), 'hash_mismatches': mismatches}
            if mismatches:
                raise ValueError('raw observation hash readback differs')
            from backend.services.data_integrity.catalog import optional_input_contract
            from backend.services.data_integrity.validation import PhysicalValidator
            report['optional_partition_readback'] = []
            for name, day, revision in report['revisions']:
                contract = optional_input_contract(name)
                if contract:
                    result = PhysicalValidator().validate(conn, contract, day, revision)
                    report['optional_partition_readback'].append({'dataset': name, 'day': str(day),
                                                                  'revision': revision, **result.as_dict()})
        report['status'] = 'READBACK_COMPLETE'
    except Exception as exc:
        report['status'] = 'FAILED'
        report['error_type'] = type(exc).__name__
        raise
    finally:
        conn.close()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        print(json.dumps({'status': report['status'], 'output': str(args.output),
                          'production_write': False}, ensure_ascii=False))


if __name__ == '__main__':
    main()
