import datetime as dt
import json

import pytest

from backend.data_service.shared_vix_inputs import parse_official_history, reconcile


CSV = b'DATE,OPEN,HIGH,LOW,CLOSE\n01/16/2026,15,17,14,16\n01/19/2026,17,19,16,18\n01/20/2026,18,20,17,19\n'


def observation(day, close, digest):
    return dict(event_date=dt.date.fromisoformat(day), close=close, pct_change=None,
                source_row_sha256=digest, captured_at=dt.datetime(2026,11,7,tzinfo=dt.timezone.utc))


def test_official_published_holiday_is_not_filtered_by_equity_calendar():
    history=parse_official_history(CSV)
    daily, report=reconcile(history, [], dt.date(2026,1,19),dt.date(2026,1,20),
                           dt.datetime(2026,11,9,tzinfo=dt.timezone.utc),'a'*64)
    assert [row['trade_date'] for row in daily]==[dt.date(2026,1,19),dt.date(2026,1,20)]
    assert daily[0]['pct_change']==12.5
    assert report['summary']['added_published_dates']==2


def test_latest_provider_version_never_wins_over_official_close():
    rows=[observation('2026-01-19',18,'a'*64),observation('2026-01-19',25,'b'*64)]
    daily, report=reconcile(parse_official_history(CSV),rows,dt.date(2026,1,19),dt.date(2026,1,19),
                           dt.datetime(2026,11,9,tzinfo=dt.timezone.utc),'a'*64)
    assert daily[0]['close']==18
    assert daily[0]['published_at'] is None
    assert daily[0]['vintage_quality']=='NON_VINTAGE'
    assert len(report['dates'][0]['original_versions'])==2
    assert report['summary']['conflicting_provider_dates']==1


def test_extra_provider_sunday_is_quarantined_not_shifted_or_filled():
    daily, report=reconcile(parse_official_history(CSV),[observation('2026-01-18',17,'a'*64)],
                           dt.date(2026,1,18),dt.date(2026,1,19),dt.datetime(2026,11,9,tzinfo=dt.timezone.utc),'a'*64)
    assert [row['trade_date'] for row in daily]==[dt.date(2026,1,19)]
    assert report['summary']['quarantined_provider_dates']==1
    assert report['dates'][0]['cause']=='PROVIDER_DATE_ABSENT_FROM_OFFICIAL_VIX_HISTORY'


@pytest.mark.parametrize('bad',[
    CSV+CSV.splitlines(keepends=True)[1],
    CSV.replace(b'01/19/2026',b'2026/01/19'),
    CSV.replace(b',18\n',b',NaN\n'),
    CSV.replace(b',18\n',b',0\n'),
])
def test_bad_official_reference_fails_closed(bad):
    with pytest.raises(ValueError):
        parse_official_history(bad)


def test_no_historical_publication_or_unknown_source_change_claim():
    daily, report=reconcile(parse_official_history(CSV),[observation('2026-01-19',17,'a'*64)],
                           dt.date(2026,1,19),dt.date(2026,1,19),dt.datetime(2026,11,9,tzinfo=dt.timezone.utc),'a'*64)
    assert daily[0]['captured_at'].year==2026
    assert daily[0]['publication_time_quality']=='UNKNOWN'
    assert report['dates'][0]['revision_or_definition_cause']=='UNKNOWN'
    assert json.loads(daily[0]['raw_payload'])['DATE']=='01/19/2026'


@pytest.fixture
def bundle_inputs(tmp_path):
    import hashlib
    import pyarrow as pa
    import pyarrow.parquet as pq
    from backend.data_service.shared_tushare_inputs import _digest, _schema, input_contract
    from backend.data_service.shared_vix_inputs import SOURCE_URLS, _encoded
    old=tmp_path/'old'
    old.mkdir()
    clock=dt.datetime(2026,11,7,tzinfo=dt.timezone.utc)
    rows=[]
    for day,close in [('20260118','17'),('20260119','18'),('20260119','25')]:
        raw={'trade_date':day,'close':close,'pct_change':'0'}
        row={'source_api':'vix_index','event_date':dt.datetime.strptime(day,'%Y%m%d').date(),
             'ts_code':None,'source_row_sha256':hashlib.sha256(_encoded(raw)).hexdigest(),
             'raw_payload':_encoded(raw).decode(),'captured_at':clock,'last_observed_at':clock,
             'published_at':None,'publication_time_quality':'UNKNOWN','vintage_quality':'NON_VINTAGE',
             'source_record_key':'UNKNOWN','trade_date':dt.datetime.strptime(day,'%Y%m%d').date(),
             'close':float(close),'pct_change':0.0}
        rows.append(row)
    schema=_schema('vix_index')
    pq.write_table(pa.Table.from_pylist(rows,schema=schema),old/'vix_index.parquet')
    digest=hashlib.sha256((old/'vix_index.parquet').read_bytes()).hexdigest()
    manifest={'schema_version':'aistock_shared_tushare_inputs_v1','database_write':False,
        'pit_filter_applied':False,'active_profile_write':False,'historical_PIT_proven':False,
        'datasets':{'vix_index':{**input_contract('vix_index'),'file':'vix_index.parquet','rows':3,
            'query_coverage':{'requested_start':'2026-01-18','requested_end':'2026-01-20'},
            'schema':[{'name':f.name,'type':str(f.type)} for f in schema]}},
        'files':{'vix_index.parquet':{'sha256':digest,'bytes':(old/'vix_index.parquet').stat().st_size}}}
    manifest['manifest_digest']=_digest(manifest)
    (old/'manifest.json').write_text(json.dumps(manifest),encoding='utf-8')
    official=tmp_path/'official.csv'
    official.write_bytes(CSV)
    capture=tmp_path/'capture.json'
    capture.write_text(json.dumps({'url':sorted(SOURCE_URLS)[0],'effective_url':sorted(SOURCE_URLS)[0],
        'request_started_at':clock.isoformat(),'captured_at':clock.isoformat(),
        'sha256':hashlib.sha256(CSV).hexdigest(),'published_at':None,
        'publication_time_quality':'UNKNOWN','vintage_quality':'NON_VINTAGE'}),encoding='utf-8')
    return old,hashlib.sha256((old/'manifest.json').read_bytes()).hexdigest(),official,capture


def test_export_readback_preserves_originals_and_never_overwrites(bundle_inputs,tmp_path):
    import hashlib
    from backend.data_service.shared_vix_inputs import export_corrected_bundle,read_corrected_bundle
    old,oldsha,official,capture=bundle_inputs
    before=hashlib.sha256((old/'vix_index.parquet').read_bytes()).hexdigest()
    root=tmp_path/'corrected'
    result=export_corrected_bundle(old,oldsha,official,capture,root)
    read=read_corrected_bundle(root,expected_manifest_sha256=result['manifest_sha256'])
    assert read.data.close.tolist()==[18.0,19.0]
    assert hashlib.sha256((root/'vix_versions.parquet').read_bytes()).hexdigest()==before
    assert hashlib.sha256((old/'vix_index.parquet').read_bytes()).hexdigest()==before
    assert result['summary']['conflicting_provider_dates']==1
    with pytest.raises(FileExistsError):
        export_corrected_bundle(old,oldsha,official,capture,root)


@pytest.mark.parametrize('tamper',['raw','resolved','capture','missing_manifest_asset'])
def test_reader_rejects_tampering_even_when_files_rehashed(bundle_inputs,tmp_path,tamper):
    import hashlib
    import pyarrow as pa
    import pyarrow.parquet as pq
    from backend.data_service.shared_tushare_inputs import _digest
    from backend.data_service.shared_vix_inputs import export_corrected_bundle,verify_corrected_bundle
    root=tmp_path/'corrected'
    export_corrected_bundle(*bundle_inputs,root)
    path=root/'manifest.json'
    manifest=json.loads(path.read_text())
    changed=None
    if tamper in {'raw','resolved'}:
        changed=root/('vix_index.parquet' if tamper=='resolved' else 'vix_versions.parquet')
        table=pq.read_table(changed)
        rows=table.to_pylist()
        rows[0]['close']=99.0
        pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),changed)
    elif tamper=='capture':
        manifest['source_capture']['captured_at']='2020-01-01T00:00:00+00:00'
    else:
        del manifest['files']['cboe_VIX_History.csv']
    if changed:
        manifest['files'][changed.name]={'sha256':hashlib.sha256(changed.read_bytes()).hexdigest(),'bytes':changed.stat().st_size}
    manifest['manifest_digest']=_digest({k:v for k,v in manifest.items() if k!='manifest_digest'})
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        verify_corrected_bundle(root)
