"""Existing DEV only: explicit migration/native short sync and independent readback.

No production flag, service start, release write, business consumer or new DB.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sys

from dotenv import dotenv_values
import psycopg2
from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def readback(conn, start, end):
    from backend.services.tushare_dataset_specs import DATASET_REGISTRY
    from backend.services.tushare_event_inputs import EVENT_DATASETS, payload_bytes, validate_partition
    output = {}
    with conn.cursor() as cur:
        for name in sorted(EVENT_DATASETS):
            spec = DATASET_REGISTRY[name]
            cur.execute(sql.SQL('SELECT source_row_sha256,raw_payload,published_at,publication_time_quality,'
                'vintage_quality,captured_at,last_observed_at FROM {} '
                'WHERE source_api=%s AND request_date BETWEEN %s AND %s ORDER BY source_row_sha256').format(
                    sql.Identifier(*spec.target_table.split('.'))), (name, start, end))
            values = cur.fetchall()
            mismatches = sum(hashlib.sha256(payload_bytes(r[1])).hexdigest() != r[0] for r in values)
            unknown = all(r[2] is None and r[3] == 'UNKNOWN' and r[4] == 'NON_VINTAGE' for r in values)
            cur.execute("SELECT trade_date,data_revision FROM market.dataset_date_refresh_audit "
                        "WHERE dataset=%s AND data_source='canonical_db' AND trade_date BETWEEN %s AND %s "
                        "ORDER BY trade_date", (name, start, end))
            revisions = cur.fetchall()
            results = [{'day': str(day), 'revision': revision,
                        **validate_partition(conn, spec, day, revision).as_dict()} for day, revision in revisions]
            cur.execute('SELECT data_kind,table_name,date_column,updated_column FROM market.data_stats_config '
                        'WHERE data_kind=%s', (name,))
            stats = cur.fetchone()
            output[name] = {'rows': len(values), 'hash_mismatches': mismatches, 'clock_unknown_preserved': unknown,
                            'first_capture': min((r[5] for r in values), default=None),
                            'last_observed': max((r[6] for r in values), default=None),
                            'partitions': results, 'stats_config': stats}
            if mismatches or not unknown or not stats or len(results) != (end-start).days+1:
                raise ValueError(f'{name}: incomplete independent readback')
            if any(r['state'] != 'VALID' for r in results):
                raise ValueError(f'{name}: partition validation failed')
        cur.execute("SELECT table_name,count(*),count(col_description(('market.'||quote_ident(table_name))::regclass,ordinal_position)) "
                    "FROM information_schema.columns WHERE table_schema='market' AND table_name IN "
                    "('tushare_risk_event_raw','index_announcements_raw','stock_seasoned_raw') GROUP BY table_name")
        comments = cur.fetchall()
        if len(comments) != 3 or any(total != described for _, total, described in comments):
            raise ValueError('raw table column documentation incomplete')
        output['column_comments'] = comments
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--start', type=dt.date.fromisoformat, default=dt.date(2026, 10, 30))
    parser.add_argument('--end', type=dt.date.fromisoformat, default=dt.date(2026, 10, 30))
    parser.add_argument('--migrate-dev', action='store_true')
    parser.add_argument('--sync-dev', action='store_true')
    parser.add_argument('--check-management-dev', action='store_true', help='refresh only these five small statistics')
    args = parser.parse_args()
    if args.output.resolve().drive.upper() != 'X:' or not 0 <= (args.end-args.start).days <= 3:
        parser.error('requires X output and 1..4 calendar-day DEV smoke')
    from scripts.prepare_canonical_pit_monthly import _load_database_config, _database_target_environment
    cfg = _load_database_config('dev', args.env_file)
    if (cfg.host, cfg.port, cfg.dbname) != ('127.0.0.1', 5433, 'aistock_dev'):
        raise ValueError('existing DEV target differs; no connection opened')
    migration = ROOT/'backend/db/migrations/add_tushare_event_inputs_v1.sql'
    report = {'target': 'dev', 'production_write': False, 'runtime_action': False,
              'business_modules_write': False, 'migration_sha256': hashlib.sha256(migration.read_bytes()).hexdigest(),
              'window': [str(args.start), str(args.end)], 'migration': 'not_requested', 'sync': []}
    conn = psycopg2.connect(host=cfg.host, port=cfg.port, dbname=cfg.dbname,
                            user=cfg.user, password=cfg.password, connect_timeout=10)
    conn.autocommit = True
    try:
        if args.migrate_dev:
            with conn.cursor() as cur:
                cur.execute("SET lock_timeout='5s'")
                cur.execute("SET statement_timeout='60s'")
                for _ in range(2):
                    cur.execute(migration.read_text(encoding='utf-8'))
            report['migration'] = 'applied_twice'
        if args.sync_dev:
            token = dotenv_values(args.env_file).get('TUSHARE_TOKEN')
            if not token:
                raise ValueError('provider credential location has no token')
            os.environ['TUSHARE_TOKEN'] = str(token)
            with _database_target_environment(cfg):
                from backend.db.pg_pool import close_db_pool
                from backend.services.tushare_sync_engine import TushareSyncEngine
                from backend.services.tushare_dataset_specs import DATASET_REGISTRY
                from backend.services.tushare_event_inputs import EVENT_DATASETS
                try:
                    for name in sorted(EVENT_DATASETS):
                        for repetition in range(2):
                            result = TushareSyncEngine().sync(DATASET_REGISTRY[name], mode='init',
                                start_date=args.start, end_date=args.end)
                            report['sync'].append({'repetition': repetition, **result.as_dict()})
                            if not result.ok:
                                raise ValueError(f'{name}: native DEV sync failed')
                finally:
                    close_db_pool()
        if args.sync_dev or args.check_management_dev:
            with _database_target_environment(cfg):
                from backend.db.pg_pool import close_db_pool
                from backend.routers.ingestion import _refresh_one_data_stats, get_ingestion_auto_range, get_data_gaps
                from backend.services.tushare_event_inputs import EVENT_DATASETS
                from psycopg2.extras import RealDictCursor
                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute('SELECT * FROM market.data_stats_config WHERE data_kind=ANY(%s)',
                                    (sorted(EVENT_DATASETS),))
                        configs = [dict(r) for r in cur.fetchall()]
                    report['stats'] = [_refresh_one_data_stats(config) for config in configs]
                    report['auto_range'] = [get_ingestion_auto_range(data_kind=name) for name in sorted(EVENT_DATASETS)]
                    report['query_gaps'] = [get_data_gaps(data_kind=name, start_date=str(args.start),
                        end_date=str(args.end), refresh=True) for name in sorted(EVENT_DATASETS)]
                finally:
                    close_db_pool()
        report['readback'] = readback(conn, args.start, args.end)
        report['status'] = 'PASS'
    except BaseException as exc:
        report['status'], report['error_type'] = 'FAILED', type(exc).__name__
        raise
    finally:
        conn.close()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, default=str, indent=2), encoding='utf-8')
        print(json.dumps({'status': report['status'], 'output': str(args.output)}, ensure_ascii=False))


if __name__ == '__main__':
    main()
