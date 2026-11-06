"""Read and freeze optional Tushare facts; never decides equity PIT membership."""
from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import re
import stat
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import capture_read_snapshot
from backend.services.tushare_dataset_specs import DATASET_REGISTRY
from backend.services.tushare_event_inputs import payload_bytes

VERSION = 'aistock_shared_tushare_inputs_v1'
DATASETS = ('vix_index', 'stock_daily_afterhours', 'stk_shock', 'stk_high_shock',
            'stk_alert', 'idx_anns', 'stk_seasoned')
_MARKET_WIDE = frozenset({'vix_index', 'idx_anns'})
_EQUITY = re.compile(r'^(?:00\d{4}\.SZ|30\d{4}\.SZ|6\d{5}\.SH)$')
_BASE = [pa.field('source_api', pa.string()), pa.field('event_date', pa.date32()),
         pa.field('ts_code', pa.string()), pa.field('source_row_sha256', pa.string()),
         pa.field('raw_payload', pa.string()),
         *[pa.field(x, pa.timestamp('us', tz='UTC')) for x in
           ('captured_at', 'last_observed_at', 'published_at')],
         pa.field('publication_time_quality', pa.string()), pa.field('vintage_quality', pa.string()),
         pa.field('source_record_key', pa.string())]


@dataclass
class SharedInputHistory:
    data: pd.DataFrame
    metadata: dict


def _date(value):
    if isinstance(value, dt.datetime):
        raise ValueError('A fact date must not silently discard time')
    if isinstance(value, dt.date):
        return value
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}|\d{8}', value):
        raise ValueError('Expected an exact ISO or YYYYMMDD fact date')
    return dt.date.fromisoformat(value) if '-' in value else dt.datetime.strptime(value, '%Y%m%d').date()


def _request(dataset, start, end, symbols, observed_by):
    if dataset not in DATASETS:
        raise ValueError('Unsupported shared input dataset')
    start, end = _date(start), _date(end)
    if start > end:
        raise ValueError('Inverted source date window')
    if symbols is not None:
        if dataset in _MARKET_WIDE or isinstance(symbols, str):
            raise ValueError('Stock filtering is not applicable to market-wide data')
        symbols = sorted(set(symbols))
        if any(not isinstance(x, str) or not _EQUITY.fullmatch(x) for x in symbols):
            raise ValueError('Expected explicit SH/SZ A-share provider source codes')
    if observed_by is not None:
        if not isinstance(observed_by, dt.datetime) or observed_by.utcoffset() is None:
            raise ValueError('observed_by must be a timezone-aware capture cutoff')
        observed_by = observed_by.astimezone(dt.timezone.utc)
    return start, end, symbols, observed_by


def input_contract(dataset):
    if dataset not in DATASETS:
        raise ValueError('Unsupported shared input dataset')
    spec = DATASET_REGISTRY[dataset]
    units = ({'close': 'index_points', 'pct_change': 'percent'} if dataset == 'vix_index' else
             {'ah_vol': 'hands_100_shares', 'ah_amount': 'CNY_1000'} if dataset == 'stock_daily_afterhours'
             else {'provider_fields': 'source_text_and_original_units_unchanged'})
    return {'schema_version': VERSION, 'dataset': dataset, 'source_api': spec.tushare_api,
            'fields': dict(spec.columns), 'units': units,
            'security_identity': 'NOT_APPLICABLE' if dataset in _MARKET_WIDE else
                                 'PROVIDER_SOURCE_CODE_NOT_ALIAS_NORMALIZED',
            'date_semantics': 'US_SESSION_DATE' if dataset == 'vix_index' else 'PROVIDER_FACT_DATE',
            'vintage_quality': 'NON_VINTAGE', 'publication_time_quality': 'UNKNOWN',
            'historical_PIT_proven': False, 'pit_filter_applied': False,
            'universe_scope': 'ALL_RETAINED_SOURCE_FACTS_NOT_BUY_PIT',
            'capture_cutoff_semantics': 'LOCAL_OBSERVATION_TIME_NOT_FIRST_PUBLICATION',
            'last_observed_at_semantics': 'CURRENT_AUDIT_METADATA_NOT_HISTORICAL_FEATURE',
            'version_policy': 'ALL_RETAINED_RAW_VERSIONS_NO_BUSINESS_DEDUP',
            'session_calendar_completeness': 'NOT_EVALUATED' if dataset == 'vix_index' else 'NOT_APPLICABLE'}


def _schema(dataset, source_fields=None):
    fields = list(_BASE)
    names = {x.name for x in fields}
    types = {'date': pa.date32(), 'numeric': pa.float64(), 'text': pa.string()}
    source_fields = DATASET_REGISTRY[dataset].columns if source_fields is None else source_fields
    if not isinstance(source_fields, dict) or not source_fields:
        raise ValueError('Frozen source field contract is missing')
    for name, kind in source_fields.items():
        if not isinstance(name, str) or kind not in types or (name in names and name != 'ts_code'):
            raise ValueError('Unsupported or conflicting source field contract')
        if name not in names:
            fields.append(pa.field(name, types[kind]))
    return pa.schema(fields)


def _flatten(dataset, row, source_fields=None):
    spec = DATASET_REGISTRY[dataset]
    source_fields = spec.columns if source_fields is None else source_fields
    raw = row['raw_payload']
    if not isinstance(raw, dict) or set(raw) != set(source_fields):
        raise ValueError(f'{dataset}: raw source fields differ')
    encoded = payload_bytes(raw)
    if hashlib.sha256(encoded).hexdigest() != row['source_row_sha256']:
        raise ValueError(f'{dataset}: raw source content identity differs')
    field = 'start_date' if dataset == 'stk_alert' else 'ann_date' if dataset in {
        'idx_anns', 'stk_seasoned'} else 'trade_date'
    day = _date(raw[field])
    if day != row['event_date'] or row['source_api'] != spec.tushare_api:
        raise ValueError(f'{dataset}: source/date projection differs')
    code = raw.get('ts_code')
    if code != row['ts_code'] or (dataset not in _MARKET_WIDE and
                                  (not isinstance(code, str) or not _EQUITY.fullmatch(code))):
        raise ValueError(f'{dataset}: source security identity differs')
    if (row['published_at'] is not None or row['publication_time_quality'] != 'UNKNOWN'
            or row['vintage_quality'] != 'NON_VINTAGE'):
        raise ValueError(f'{dataset}: unsupported historical availability claim')
    for name in ('captured_at', 'last_observed_at'):
        if not isinstance(row[name], dt.datetime) or row[name].utcoffset() is None:
            raise ValueError(f'{dataset}: actual capture timestamp required')
    value = {name: row.get(name) for name in _schema(dataset, source_fields).names if name not in source_fields}
    value.update(event_date=day, ts_code=code, raw_payload=encoded.decode('utf-8'),
                 source_record_key=row.get('source_record_key', 'UNKNOWN'))
    for name, kind in source_fields.items():
        original = raw[name]
        if original is None:
            value[name] = None
        elif kind == 'date':
            value[name] = _date(original)
        elif kind == 'numeric':
            if isinstance(original, bool):
                raise ValueError(f'{dataset}.{name}: boolean is not a numeric fact')
            number = float(Decimal(str(original)))
            if not math.isfinite(number):
                raise ValueError(f'{dataset}.{name}: non-finite fact')
            value[name] = number
        else:
            value[name] = str(original)
    return value


def _batches(conn, dataset, start, end, symbols, observed_by, batch_size=10000):
    supplemental = dataset in {'vix_index', 'stock_daily_afterhours'}
    table = 'market.tushare_supplemental_raw' if supplemental else DATASET_REGISTRY[dataset].target_table
    date_field = 'trade_date' if supplemental else 'event_date'
    selector = 'dataset' if supplemental else 'source_api'
    conditions = [sql.SQL('{}=%s').format(sql.Identifier(selector)),
                  sql.SQL('{} BETWEEN %s AND %s').format(sql.Identifier(date_field))]
    params = [dataset, start, end]
    if symbols is not None:
        conditions.append(sql.SQL('ts_code=ANY(%s)'))
        params.append(symbols)  # [] means empty, never silently becomes all equities.
    if observed_by is not None:
        conditions.append(sql.SQL('captured_at<=%s'))
        params.append(observed_by)
    statement = sql.SQL('SELECT source_api,{} AS event_date,ts_code,source_row_sha256,raw_payload,'
        'captured_at,last_observed_at,published_at,publication_time_quality,vintage_quality,{} '
        'FROM {} WHERE {} ORDER BY {},ts_code NULLS FIRST,source_row_sha256').format(
        sql.Identifier(date_field), sql.SQL("'UNKNOWN' AS source_record_key") if supplemental else
        sql.Identifier('source_record_key'), sql.Identifier(*table.split('.')),
        sql.SQL(' AND ').join(conditions), sql.Identifier(date_field))
    with conn.cursor(name='shared_input_'+uuid.uuid4().hex, cursor_factory=RealDictCursor) as cur:
        cur.itersize = batch_size
        cur.execute(statement, params)
        while rows := cur.fetchmany(batch_size):
            yield pa.Table.from_pylist([_flatten(dataset, dict(row)) for row in rows], schema=_schema(dataset))


def _coverage(conn, dataset, start, end):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('SELECT trade_date,status FROM market.dataset_date_refresh_audit '
                    "WHERE dataset=%s AND data_source='tushare' AND trade_date BETWEEN %s AND %s "
                    'ORDER BY trade_date', (dataset, start, end))
        rows = cur.fetchall()
    failed = [str(r['trade_date']) for r in rows if r['status'] != 'success']
    return {'requested_start': str(start), 'requested_end': str(end),
            'successful_query_days': sum(r['status'] == 'success' for r in rows),
            'unsuccessful_query_dates': failed,
            'semantics': 'CURRENT_QUERY_LEDGER_NOT_HISTORICAL_PIT_OR_PROVIDER_COMPLETENESS'}


def _summary(dataset, tables, coverage, observed_by):
    count = sum(t.num_rows for t in tables)
    days = [v for t in tables for v in t['event_date'].to_pylist()]
    return {**input_contract(dataset), 'rows': count,
            'first_fact_date': str(min(days)) if days else None,
            'last_fact_date': str(max(days)) if days else None, 'query_coverage': coverage,
            'observed_by': observed_by.isoformat() if observed_by else None}


def read_history(dataset, start, end, *, symbols=None, observed_by=None):
    """All retained versions, optionally bounded by actual local capture time."""
    start, end, symbols, observed_by = _request(dataset, start, end, symbols, observed_by)
    with capture_read_snapshot() as conn:
        tables = list(_batches(conn, dataset, start, end, symbols, observed_by))
        coverage = _coverage(conn, dataset, start, end)
    table = pa.concat_tables(tables) if tables else pa.Table.from_pylist([], schema=_schema(dataset))
    metadata = _summary(dataset, tables, coverage, observed_by)
    metadata['requested_source_codes'] = symbols
    return SharedInputHistory(table.to_pandas(), metadata)


def _sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _json(path, value):
    with path.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)


def _digest(value):
    return hashlib.sha256(payload_bytes(value)).hexdigest()


def _plain(path):
    info = path.lstat()
    if (path.is_symlink() or getattr(info, 'st_file_attributes', 0) &
            getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 1024)):
        raise ValueError('Frozen shared input must not be a link or reparse point')
    return path


def export_bundle(destination, start, end, *, datasets=DATASETS, quality_report=None):
    """A new, non-active raw snapshot; never overwrites an existing directory."""
    datasets = tuple(datasets)
    if not datasets or len(set(datasets)) != len(datasets):
        raise ValueError('Expected distinct explicit datasets')
    for dataset in datasets:
        _request(dataset, start, end, None, None)
    start, end = _date(start), _date(end)
    notes = {}
    if quality_report is not None:
        source = Path(quality_report)
        original = json.loads(source.read_text(encoding='utf-8'))
        notes = {'source_report_sha256': _sha(source), 'source_status': original.get('status'),
                 'unknown_upstream_records': original.get('unknown_upstream_records', []),
                 'historical_PIT_proven': False}
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    _plain(destination)
    manifest = {'schema_version': VERSION, 'created_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                'read_consistency': 'REPEATABLE_READ_READ_ONLY', 'database_write': False,
                'pit_filter_applied': False, 'active_profile_write': False,
                'historical_PIT_proven': False, 'source_quality': notes, 'datasets': {}, 'files': {}}
    with capture_read_snapshot() as conn:
        for dataset in datasets:
            path = destination / (dataset+'.parquet')
            rows, first, last = 0, None, None
            with pq.ParquetWriter(path, _schema(dataset), compression='zstd') as writer:
                for table in _batches(conn, dataset, start, end, None, None):
                    if not table.num_rows:
                        continue
                    writer.write_table(table)
                    rows += table.num_rows
                    days = table['event_date'].to_pylist()
                    first = min(days) if first is None else min(first, min(days))
                    last = max(days) if last is None else max(last, max(days))
            meta = {**input_contract(dataset), 'rows': rows,
                    'first_fact_date': str(first) if first else None, 'last_fact_date': str(last) if last else None,
                    'query_coverage': _coverage(conn, dataset, start, end),
                    'file': path.name, 'schema': [{'name': f.name, 'type': str(f.type)} for f in _schema(dataset)]}
            manifest['datasets'][dataset] = meta
            manifest['files'][path.name] = {'sha256': _sha(path), 'bytes': path.stat().st_size}
    _json(destination/'source_quality.json', notes)
    readme = destination/'README.md'
    with readme.open('x', encoding='utf-8') as stream:
        stream.write('# 共享Tushare研究输入\n\n'
            '各Parquet同时含来源字段、原始JSON、原始内容SHA及真实捕获时间。\n'
            'NON_VINTAGE/UNKNOWN：日期不能证明历史首次公开；不按买入PIT裁剪，不做业务去重或前填。\n'
            '股票代码为供应商来源代码，不是私有canonical映射；全局PIT由消费者按既有权威应用。\n'
            'observed_by仅过滤实际captured_at；last_observed_at是当前审计元数据，不能用作历史特征。\n'
            '盘后量为手、金额为千元人民币；VIX日期为美国session date。\n'
            'source_quality.json保留本批已知来源限制；查询成功不证明供应商历史全集。\n\n'
            '```python\nfrom backend.data_service.shared_tushare_inputs import read_bundle\n'
            'result = read_bundle("本目录绝对路径", "vix_index")\n'
            'print(result.metadata)\nprint(result.data.head())\n```\n')
    for path in (destination/'source_quality.json', readme):
        manifest['files'][path.name] = {'sha256': _sha(path), 'bytes': path.stat().st_size}
    manifest['manifest_digest'] = _digest(manifest)
    _json(destination/'manifest.json', manifest)  # Last file: partial exports have no ready manifest.
    return {'root': str(destination.resolve()), 'manifest_sha256': _sha(destination/'manifest.json'),
            'manifest_digest': manifest['manifest_digest'],
            'datasets': {name: value['rows'] for name, value in manifest['datasets'].items()},
            'source_warning_count': len(notes.get('unknown_upstream_records', [])),
            'database_write': False, 'active_profile_write': False}


def _manifest(root, expected_manifest_sha256=None):
    root = _plain(Path(root))
    path = _plain(root/'manifest.json')
    if expected_manifest_sha256 is not None and _sha(path) != expected_manifest_sha256:
        raise ValueError('Frozen manifest file identity differs')
    value = json.loads(path.read_text(encoding='utf-8'))
    body = {key: item for key, item in value.items() if key != 'manifest_digest'}
    if value.get('schema_version') != VERSION or _digest(body) != value.get('manifest_digest'):
        raise ValueError('Frozen manifest contract or digest differs')
    if not value.get('datasets') or any(name not in DATASETS for name in value['datasets']):
        raise ValueError('Unsupported frozen shared dataset')
    if any(value.get(key) is not False for key in
           ('database_write', 'pit_filter_applied', 'active_profile_write', 'historical_PIT_proven')):
        raise ValueError('Frozen shared source semantics differ')
    for dataset, meta in value['datasets'].items():
        if any(meta.get(key) != expected for key, expected in input_contract(dataset).items() if key != 'fields'):
            raise ValueError('Frozen source schema, units or time contract differs')
        _frozen_schema(dataset, meta)
    return root, value


def _frozen_schema(dataset, meta):
    if not isinstance(meta.get('fields'), dict) or not meta['fields']:
        raise ValueError('Frozen source field contract is missing')
    expected = _schema(dataset, meta.get('fields'))
    fields = meta.get('schema', [])
    names = [f['name'] for f in fields]
    if (len(names) != len(set(names)) or set(names) != set(expected.names)
            or any(f['type'] != str(expected.field(f['name']).type) for f in fields)):
        raise ValueError('Frozen schema metadata differs')
    # JSON object keys are sorted in the manifest; frozen field order is explicit.
    return pa.schema([expected.field(name) for name in names])


def _file(root, manifest, name):
    if Path(name).name != name or name not in manifest['files']:
        raise ValueError('Invalid frozen file reference')
    path = _plain(root/name)
    if not path.is_file() or path.resolve().parent != root.resolve():
        raise ValueError('Frozen file is not a regular in-bundle file')
    if path.stat().st_size != manifest['files'][name]['bytes'] or _sha(path) != manifest['files'][name]['sha256']:
        raise ValueError('Frozen shared input file identity differs')
    return path


def read_bundle(root, dataset, *, expected_manifest_sha256=None):
    root, manifest = _manifest(root, expected_manifest_sha256)
    if dataset not in manifest['datasets']:
        raise ValueError('Dataset not included in this frozen bundle')
    meta = manifest['datasets'][dataset]
    if meta['file'] != dataset+'.parquet':
        raise ValueError('Unexpected shared dataset file reference')
    table = pq.read_table(_file(root, manifest, meta['file']))
    if table.num_rows != meta['rows'] or not table.schema.equals(_frozen_schema(dataset, meta)):
        raise ValueError('Frozen shared input schema or row count differs')
    return SharedInputHistory(table.to_pandas(), {**meta, 'source_quality': manifest.get('source_quality', {})})


def verify_bundle(root, *, expected_manifest_sha256=None):
    root, manifest = _manifest(root, expected_manifest_sha256)
    for name in manifest['files']:
        _file(root, manifest, name)
    counts = {}
    for dataset, meta in manifest['datasets'].items():
        if meta['file'] != dataset+'.parquet':
            raise ValueError('Unexpected shared dataset file reference')
        with pq.ParquetFile(root/meta['file']) as reader:
            schema = _frozen_schema(dataset, meta)
            if reader.metadata.num_rows != meta['rows'] or not reader.schema_arrow.equals(schema):
                raise ValueError('Frozen shared input schema or row count differs')
            for batch in reader.iter_batches(batch_size=10000):
                actual = pa.Table.from_batches([batch])
                expected = []
                for row in actual.to_pylist():
                    row['raw_payload'] = json.loads(row['raw_payload'])
                    expected.append(_flatten(dataset, row, meta['fields']))
                reconstructed = pa.Table.from_pylist(expected, schema=schema)
                if not actual.equals(reconstructed, check_metadata=False):
                    raise ValueError('Frozen source fields and raw payload differ')
        counts[dataset] = meta['rows']
    return {'status': 'VERIFIED_WITH_DECLARED_SOURCE_LIMITATIONS', 'datasets': counts,
            'historical_PIT_proven': False, 'database_access': False, 'network_access': False,
            'manifest_sha256': _sha(root/'manifest.json'), 'manifest_digest': manifest['manifest_digest']}
