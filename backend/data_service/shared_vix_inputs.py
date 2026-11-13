"""VIX-only, file-only official-close reconciliation; never changes source DB facts."""
from __future__ import annotations

import csv
import datetime as dt
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

from backend.data_service.shared_tushare_inputs import (
    SharedInputHistory, _date, _digest, _file, _flatten, _json, _plain, _sha, read_bundle,
)

VERSION = 'aistock_shared_vix_official_close_v1'
AUTHORITY = 'CBOE_VIX_DAILY_CLOSE'
SOURCE_URLS = frozenset({
    'https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv',
    'https://cdn-api.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv',
})
CONTRACT = {
    'schema_version':VERSION,'authority':AUTHORITY,'date_semantics':'US_SESSION_DATE',
    'session_timezone':'America/New_York',
    'publication_calendar':'DATES_PUBLISHED_IN_PINNED_CBOE_VIX_HISTORY_NOT_EQUITY_CALENDAR',
    'vintage_quality':'NON_VINTAGE','historical_PIT_proven':False,
    'published_at':None,'publication_time_quality':'UNKNOWN',
    'pct_change_definition':'100*(CBOE_CLOSE/PREVIOUS_PUBLISHED_CBOE_CLOSE-1); DERIVED_NOT_PROVIDER_ORIGINAL',
    'server_last_modified_semantics':'RESPONSE_METADATA_NOT_PER_SESSION_PUBLICATION_TIME',
    'resolution_policy':'PINNED_OFFICIAL_DAILY_CLOSE_NOT_LATEST_CAPTURE',
    'database_write':False,'active_profile_write':False,'runtime_action':False,
}
REQUIRED_FILES = frozenset({'vix_index.parquet','vix_versions.parquet','predecessor_manifest.json',
                            'cboe_VIX_History.csv','cboe_capture.json','differences.json'})
DAILY_SCHEMA = pa.schema([
    pa.field('trade_date',pa.date32()),pa.field('close',pa.float64()),
    pa.field('pct_change',pa.float64()),pa.field('previous_close',pa.float64()),
    pa.field('previous_session_date',pa.date32()),pa.field('source_api',pa.string()),
    pa.field('source_row_sha256',pa.string()),pa.field('raw_payload',pa.string()),
    pa.field('reference_response_sha256',pa.string()),
    pa.field('captured_at',pa.timestamp('us',tz='UTC')),
    pa.field('published_at',pa.timestamp('us',tz='UTC')),
    pa.field('publication_time_quality',pa.string()),pa.field('vintage_quality',pa.string()),
    pa.field('tushare_version_count',pa.int32()),
])


def _encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')


def parse_official_history(content):
    """DATE is the publisher's session date, never a timestamp to timezone-shift."""
    reader=csv.DictReader(io.StringIO(content.decode('utf-8-sig')))
    if reader.fieldnames!=['DATE','OPEN','HIGH','LOW','CLOSE']:
        raise ValueError('Unexpected Cboe VIX history columns')
    history={}
    for source in reader:
        try:
            day=dt.datetime.strptime(source['DATE'],'%m/%d/%Y').date()
            if day in history or None in source:
                raise ValueError('Ambiguous official date/row')
            close=Decimal(source['CLOSE'])
            if not close.is_finite() or close<=0:
                raise ValueError('Invalid official close')
        except (ValueError,TypeError,InvalidOperation) as exc:
            raise ValueError('Invalid or duplicate official VIX row') from exc
        history[day]=source
    if not history:
        raise ValueError('Official VIX history is empty')
    return history


def reconcile(history, originals, start, end, captured_at, response_sha256):
    start,end=_date(start),_date(end)
    if start>end or captured_at.utcoffset() is None:
        raise ValueError('Invalid window or actual capture clock')
    observed={}
    for row in originals:
        day=_date(row['event_date'])
        if not start<=day<=end:
            raise ValueError('Original VIX observation outside frozen window')
        observed.setdefault(day,[]).append(row)
    daily,dates=[],[]
    previous=None
    for day in sorted(history):
        source=history[day]
        close=Decimal(source['CLOSE'])
        if start<=day<=end:
            encoded=_encoded(source)
            daily.append(dict(trade_date=day,close=float(close),
                pct_change=float(100*(close/previous[1]-1)) if previous else None,
                previous_close=float(previous[1]) if previous else None,
                previous_session_date=previous[0] if previous else None,
                source_api='cboe_vix_history',source_row_sha256=hashlib.sha256(encoded).hexdigest(),
                raw_payload=encoded.decode(),reference_response_sha256=response_sha256,
                captured_at=captured_at,published_at=None,publication_time_quality='UNKNOWN',
                vintage_quality='NON_VINTAGE',tushare_version_count=len(observed.get(day,[]))))
        previous=(day,close)
    if not daily or end>max(history):
        raise ValueError('Requested end exceeds available official history; do not claim completeness')
    expected={row['trade_date']:row for row in daily}
    for day in sorted(set(expected)|set(observed)):
        versions=observed.get(day,[])
        official=expected.get(day)
        mismatch=bool(official) and any(Decimal(str(v['close']))!=Decimal(history[day]['CLOSE']) for v in versions)
        cause=('PROVIDER_DATE_ABSENT_FROM_OFFICIAL_VIX_HISTORY' if official is None else
               'OFFICIAL_PUBLISHED_DATE_ABSENT_FROM_PROVIDER_BUNDLE' if not versions else
               'PROVIDER_CLOSE_DIFFERS_FROM_OFFICIAL_CLOSE' if mismatch else 'MATCH')
        if cause!='MATCH' or len(versions)>1:
            dates.append(dict(trade_date=str(day),cause=cause,
                official_close=official['close'] if official else None,
                official_source_row_sha256=official['source_row_sha256'] if official else None,
                provider_date_is_weekend=day.weekday()>=5,
                revision_or_definition_cause='UNKNOWN',
                original_versions=[dict(close=v['close'],pct_change=v.get('pct_change'),
                    source_row_sha256=v['source_row_sha256'],captured_at=v['captured_at'].isoformat()) for v in versions]))
    summary={'old_observation_rows':len(originals),'old_unique_dates':len(observed),
        'official_published_dates':len(daily),'added_published_dates':len(set(expected)-set(observed)),
        'quarantined_provider_dates':len(set(observed)-set(expected)),
        'differing_close_dates':sum(d['cause']=='PROVIDER_CLOSE_DIFFERS_FROM_OFFICIAL_CLOSE' for d in dates),
        'conflicting_provider_dates':sum(len({Decimal(str(v['close'])) for v in vs})>1 for vs in observed.values()),
        'resolved_missing_published_dates':0,'resolved_conflicting_dates':0}
    return daily,{'summary':summary,'dates':dates}


def _original_rows(path):
    rows=pq.read_table(path).to_pylist()
    for row in rows:
        source=dict(row,raw_payload=json.loads(row['raw_payload']))
        if _flatten('vix_index',source)!=row:
            raise ValueError('Original VIX fields/raw identity mismatch')
    return rows


def _capture(content, metadata):
    if metadata.get('url') not in SOURCE_URLS or metadata.get('effective_url') not in SOURCE_URLS:
        raise ValueError('Reference must be the official Cboe VIX daily-price file')
    if hashlib.sha256(content).hexdigest()!=metadata.get('sha256'):
        raise ValueError('Captured official response hash differs')
    clock=dt.datetime.fromisoformat(metadata['captured_at'])
    started=dt.datetime.fromisoformat(metadata['request_started_at'])
    if (clock.utcoffset() is None or started.utcoffset() is None or clock<started
            or metadata.get('published_at') is not None or metadata.get('publication_time_quality')!='UNKNOWN'
            or metadata.get('vintage_quality')!='NON_VINTAGE'):
        raise ValueError('Capture must not claim a historical publication timestamp')
    return clock


def export_corrected_bundle(old_root, expected_old_sha, official_csv, capture_json, destination, *, source_files=()):
    """Preserve the old package; make one immutable, independently readable VIX package."""
    old_root=Path(old_root)
    old=read_bundle(old_root,'vix_index',expected_manifest_sha256=expected_old_sha)
    meta=old.metadata
    start,end=(_date(meta['query_coverage'][key]) for key in ('requested_start','requested_end'))
    original_path=_plain(old_root/'vix_index.parquet')
    originals=_original_rows(original_path)
    official_csv,capture_json=_plain(Path(official_csv)),_plain(Path(capture_json))
    content=official_csv.read_bytes()
    capture=json.loads(capture_json.read_text(encoding='utf-8'))
    clock=_capture(content,capture)
    history=parse_official_history(content)
    daily,report=reconcile(history,originals,start,end,clock,capture['sha256'])
    destination=Path(destination)
    destination.parent.mkdir(parents=True,exist_ok=True)
    _plain(destination.parent)
    destination.mkdir(exist_ok=False)
    _plain(destination)
    for source,name in [(original_path,'vix_versions.parquet'),(old_root/'manifest.json','predecessor_manifest.json'),
                        (official_csv,'cboe_VIX_History.csv'),(capture_json,'cboe_capture.json')]:
        _plain(source)
        shutil.copyfile(source,destination/name)
    if _sha(destination/'predecessor_manifest.json')!=expected_old_sha or _sha(destination/'vix_versions.parquet')!=_sha(original_path):
        raise ValueError('Predecessor changed during export')
    extras=[]
    for source in source_files:
        source=_plain(Path(source))
        name='source-'+source.name
        if (destination/name).exists():
            raise ValueError('Duplicate source attachment name')
        shutil.copyfile(source,destination/name)
        extras.append(name)
    pq.write_table(pa.Table.from_pylist(daily,schema=DAILY_SCHEMA),destination/'vix_index.parquet',compression='zstd')
    _json(destination/'differences.json',report)
    manifest={**CONTRACT,'created_at':dt.datetime.now(dt.timezone.utc).isoformat(),
        'predecessor_manifest_sha256':expected_old_sha,'predecessor_root':str(old_root.resolve()),
        'window':{'start':str(start),'end':str(end)},'summary':report['summary'],
        'source_capture':capture,'attachments':extras,
        'files':{}}
    for path in sorted(destination.iterdir()):
        manifest['files'][path.name]={'sha256':_sha(path),'bytes':path.stat().st_size}
    manifest['manifest_digest']=_digest(manifest)
    _json(destination/'manifest.json',manifest)
    return verify_corrected_bundle(destination,expected_manifest_sha256=_sha(destination/'manifest.json'))


def _read(root,expected_manifest_sha256):
    root=_plain(Path(root))
    path=_plain(root/'manifest.json')
    if expected_manifest_sha256 is not None and _sha(path)!=expected_manifest_sha256:
        raise ValueError('VIX manifest identity differs')
    manifest=json.loads(path.read_text(encoding='utf-8'))
    body={k:v for k,v in manifest.items() if k!='manifest_digest'}
    if manifest.get('schema_version')!=VERSION or _digest(body)!=manifest.get('manifest_digest'):
        raise ValueError('Invalid VIX manifest contract/digest')
    if any(key not in manifest or manifest[key]!=value for key,value in CONTRACT.items()):
        raise ValueError('Unsupported VIX authority or availability claim')
    if not REQUIRED_FILES.issubset(manifest.get('files',{})):
        raise ValueError('Missing required VIX source asset')
    for name in manifest['files']:
        _file(root,manifest,name)
    return root,manifest


def verify_corrected_bundle(root,*,expected_manifest_sha256=None):
    root,manifest=_read(root,expected_manifest_sha256)
    reference=(root/'cboe_VIX_History.csv').read_bytes()
    capture=json.loads((root/'cboe_capture.json').read_text(encoding='utf-8'))
    if manifest['source_capture']!=capture:
        raise ValueError('Capture metadata and manifest differ')
    clock=_capture(reference,capture)
    originals=_original_rows(root/'vix_versions.parquet')
    if _sha(root/'predecessor_manifest.json')!=manifest['predecessor_manifest_sha256']:
        raise ValueError('Old manifest was not preserved')
    predecessor=json.loads((root/'predecessor_manifest.json').read_text(encoding='utf-8'))
    if _sha(root/'vix_versions.parquet')!=predecessor['files']['vix_index.parquet']['sha256']:
        raise ValueError('Original VIX versions drifted')
    daily,report=reconcile(parse_official_history(reference),originals,manifest['window']['start'],
                           manifest['window']['end'],clock,capture['sha256'])
    actual=pq.read_table(root/'vix_index.parquet')
    if not actual.equals(pa.Table.from_pylist(daily,schema=DAILY_SCHEMA),check_metadata=False):
        raise ValueError('Resolved VIX series differs from pinned official reference')
    if report!=json.loads((root/'differences.json').read_text(encoding='utf-8')) or report['summary']!=manifest['summary']:
        raise ValueError('Per-date VIX reconciliation differs')
    return {'root':str(root.resolve()),'manifest_sha256':_sha(root/'manifest.json'),
        'status':'VERIFIED_OFFICIAL_CLOSE_NON_VINTAGE','rows':actual.num_rows,
        'summary':report['summary'],'historical_PIT_proven':False,'database_access':False,'network_access':False}


def read_corrected_bundle(root,*,expected_manifest_sha256=None):
    verify_corrected_bundle(root,expected_manifest_sha256=expected_manifest_sha256)
    root,manifest=_read(root,expected_manifest_sha256)
    return SharedInputHistory(pq.read_table(root/'vix_index.parquet').to_pandas(),manifest)
