"""Existing DEV only: US migration, native short sync and independent readback."""
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
    parser.add_argument('--end', type=dt.date.fromisoformat, default=dt.date(2026, 11, 7))
    parser.add_argument('--migrate-dev', action='store_true')
    parser.add_argument('--sync-dev', action='store_true')
    args = parser.parse_args()
    if args.output.resolve().drive.upper() != 'X:' or not 0 <= (args.end-args.start).days <= 3:
        parser.error('requires X output and 1..4 day smoke')
    from scripts.prepare_canonical_pit_monthly import _load_database_config, _database_target_environment
    cfg = _load_database_config('dev', args.env_file)
    if cfg.dbname != 'aistock_dev' or cfg.port != 5433 or cfg.host not in {'127.0.0.1', 'localhost'}:
        raise ValueError('existing DEV identity differs; no connection opened')
    conn = psycopg2.connect(host=cfg.host, port=cfg.port, dbname=cfg.dbname,
                           user=cfg.user, password=cfg.password, connect_timeout=10)
    conn.autocommit = True
    migration = ROOT/'backend/db/migrations/add_tushare_us_market_inputs_v1.sql'
    report = {'schema_version': 'tushare_us_market_dev_validation_v1', 'database': 'aistock_dev',
              'production_write': False, 'process_control': False, 'migration_applied': False,
              'migration_sha256': hashlib.sha256(migration.read_bytes()).hexdigest(), 'sync': []}
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT current_database()')
            if cur.fetchone()[0] != 'aistock_dev':
                raise ValueError('database differs')
            if args.migrate_dev:
                for _ in range(2):
                    cur.execute(migration.read_text(encoding='utf-8'))
                report['migration_applied'] = True
        if args.sync_dev:
            token = dotenv_values(args.env_file).get('TUSHARE_TOKEN')
            if not token:
                raise ValueError('TUSHARE_TOKEN location has no credential')
            os.environ['TUSHARE_TOKEN'] = token
            with _database_target_environment(cfg):
                from backend.db import pg_pool
                from backend.services.tushare_sync_engine import TushareSyncEngine
                from backend.services.tushare_dataset_specs import DATASET_REGISTRY
                from backend.routers.ingestion import _refresh_one_data_stats
                from psycopg2.extras import RealDictCursor
                try:
                    for name in ('us_tradecal', 'index_global', 'vix_index'):
                        for attempt in range(2):
                            result = TushareSyncEngine().sync(DATASET_REGISTRY[name], mode='init',
                                                             start_date=args.start, end_date=args.end)
                            report['sync'].append({'dataset': name, 'attempt': attempt+1, **result.as_dict()})
                            if not result.ok:
                                raise ValueError(f'{name}: DEV native sync failed; see DEV job')
                            if attempt == 1 and result.inserted_rows:
                                raise ValueError(f'{name}: second sync changed facts; inspect source revisions')
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT * FROM market.data_stats_config WHERE data_kind=ANY(%s)",
                                    (['us_tradecal', 'index_global', 'vix_index'],))
                        report['statistics'] = [_refresh_one_data_stats(dict(row)) for row in cur.fetchall()]
                finally:
                    pg_pool.close_db_pool()
        from backend.services.tushare_dataset_specs import DATASET_REGISTRY
        from backend.services.tushare_us_market_inputs import gap_report
        from backend.services.tushare_supplemental_inputs import normalize_rows, raw_observation
        from backend.services.tushare_us_market_inputs import parse_day
        from psycopg2.extras import RealDictCursor
        from decimal import Decimal
        report['coverage'] = {name: gap_report(conn, DATASET_REGISTRY[name], args.start, args.end)
                              for name in ('us_tradecal', 'index_global', 'vix_index')}
        roundtrip = {}
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for name in ('us_tradecal', 'index_global', 'vix_index'):
                spec = DATASET_REGISTRY[name]
                cur.execute(f'SELECT * FROM {spec.target_table} WHERE {spec.date_column} BETWEEN %s AND %s',
                            (args.start, args.end))
                facts = {(row[spec.date_column], row.get('ts_code')): dict(row) for row in cur.fetchall()}
                cur.execute("SELECT DISTINCT ON (trade_date,coalesce(ts_code,'')) * "
                            "FROM market.tushare_supplemental_raw WHERE dataset=%s AND trade_date BETWEEN %s AND %s "
                            "ORDER BY trade_date,coalesce(ts_code,''),last_observed_at DESC",
                            (name, args.start, args.end))
                originals = cur.fetchall()
                for original in originals:
                    raw = original['raw_payload']
                    if raw_observation(spec, raw)[4] != original['source_row_sha256']:
                        raise ValueError(f'{name}: raw content hash differs')
                    if (original['published_at'] is not None or original['vintage_quality'] != 'NON_VINTAGE'
                            or original['publication_time_quality'] != 'UNKNOWN'):
                        raise ValueError(f'{name}: publication contract differs')
                    normalized = normalize_rows(spec, [raw])[0]
                    fact = facts[(parse_day(raw[spec.date_column]), raw.get('ts_code'))]
                    for column, kind in spec.columns.items():
                        value = normalized.get(column)
                        if value is None:
                            continue  # current projection may preserve an earlier finite optional fact
                        expected = parse_day(value) if kind == 'date' else Decimal(str(value)) if kind == 'numeric' else value
                        if fact[column] != expected:
                            raise ValueError(f'{name}.{column}: source/current roundtrip differs')
                roundtrip[name] = {'facts': len(facts), 'latest_originals': len(originals),
                                   'source_sha_verified': True, 'all_fields_readback': True}
        report['roundtrip'] = roundtrip
        with _database_target_environment(cfg):
            from backend.db import pg_pool
            from backend.routers.ingestion import get_data_gaps, get_ingestion_auto_range
            try:
                report['native_api_readback'] = {name: {
                    'gap': get_data_gaps(name, start_date=str(args.start), end_date=str(args.end), refresh=True),
                    'auto_range': get_ingestion_auto_range(name),
                } for name in ('us_tradecal', 'index_global', 'vix_index')}
            finally:
                pg_pool.close_db_pool()
        with conn.cursor() as cur:
            cur.execute("SELECT dataset,count(*) FROM market.tushare_supplemental_raw "
                        "WHERE dataset=ANY(%s) AND trade_date BETWEEN %s AND %s GROUP BY dataset ORDER BY dataset",
                        (['us_tradecal', 'index_global', 'vix_index'], args.start, args.end))
            report['raw_versions'] = cur.fetchall()
            cur.execute("SELECT dataset,trade_date,row_count,written_rows,quality_status,expected_rows "
                        "FROM market.dataset_date_refresh_audit WHERE data_source='tushare' "
                        "AND dataset=ANY(%s) AND trade_date BETWEEN %s AND %s ORDER BY dataset,trade_date",
                        (['us_tradecal', 'index_global', 'vix_index'], args.start, args.end))
            report['audits'] = cur.fetchall()
            cur.execute("SELECT table_name,count(*),count(col_description("
                        "(quote_ident(table_schema)||'.'||quote_ident(table_name))::regclass,ordinal_position)) "
                        "FROM information_schema.columns WHERE table_schema='market' "
                        "AND table_name IN ('us_tradecal','index_global') GROUP BY table_name ORDER BY table_name")
            report['column_comments'] = cur.fetchall()
        report['passed'] = all(item['status'] == 'COMPLETE' for item in report['coverage'].values())
    finally:
        conn.close()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(json.dumps({'receipt': str(args.output), 'passed': report.get('passed', False),
                      'sha256': hashlib.sha256(args.output.read_bytes()).hexdigest()}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
