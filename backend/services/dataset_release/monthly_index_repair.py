"""Adopt already prepared, bounded index facts into a PRIVATE retained build."""
from __future__ import annotations

import copy
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import tempfile

import numpy as np

from .canonical import canonical_json_bytes
from .index_contract import DOMESTIC_INDEX_DEFINITIONS, INDEX_QLIB_FIELDS
from .monthly_build_resume import _atomic, _plain, _signature, load_sealed_build_json
from .monthly_fact_correction import _under
from .monthly_quote_fact_correction import _array

BINDING = 'monthly_index_fact_repairs'
SCHEMA = 'aistock_monthly_bounded_index_correction_v1'
PRODUCER_SCHEMA = 'aistock_private_index_history_fact_rebuild_v1'
CODES = frozenset(d.daily_code for d in DOMESTIC_INDEX_DEFINITIONS)
META = {'daily_bin/materialization_receipt.json','index_context/index_materialization_receipt.json'}
AGGREGATES = {'index_context/index_daily.h5','index_context/index_context.parquet'}


def allowed(relative):
    parts = relative.split('/')
    return relative in META | AGGREGATES or (len(parts) == 5 and parts[:3] == ['daily_bin','qlib','features']
        and parts[3].upper() in CODES and parts[4] in {f'{f}.day.bin' for f in INDEX_QLIB_FIELDS})


def capture_index_repairs(inputs, *, repair_root, base_manifest_ref, cutoff, cas):
    """Freeze existing producer outputs, not runtime/provider fallback values."""
    if set(inputs) != {'receipt_path','receipt_sha256'}:
        raise ValueError('index repair requires a producer receipt')
    receipt_path = _under(inputs['receipt_path'],repair_root)
    before, raw = _signature(receipt_path), receipt_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != inputs['receipt_sha256'] or _signature(receipt_path) != before:
        raise ValueError('index producer receipt changed')
    receipt = json.loads(raw)
    if (receipt.get('schema_version') != PRODUCER_SCHEMA or receipt.get('status') != 'PRIVATE_COMPONENT_NOT_PUBLISHED'
        or any(receipt.get(k) is not False for k in ('database_write','profile_write','runtime_action'))
        or receipt.get('full_source_calendar_coverage_verified') is not True
        or receipt.get('no_quote_interpolation') is not True
        or receipt.get('raw_h5_amount_unit') != 'thousand_cny'
        or receipt.get('normalized_and_bin_amount_unit') != 'cny'):
        raise ValueError('index repair producer or units differ')
    baseline = Path(receipt['baseline_candidate'])
    _plain(baseline)
    manifest = baseline/'qe_dataset_manifest.json'
    manifest = _under(manifest,Path(repair_root).parent/'backtest_dataset_candidates')
    signature = _signature(manifest)
    if hashlib.sha256(manifest.read_bytes()).hexdigest() != base_manifest_ref['sha256'] or _signature(manifest) != signature:
        raise ValueError('index repair baseline manifest differs')

    def freeze(path,expected):
        path = _under(path,repair_root)
        signature = _signature(path)
        with path.open('rb') as reader:
            ref = cas.put_stream(iter(lambda:reader.read(1024*1024),b''))
        if ref.sha256 != expected or _signature(path) != signature:
            raise ValueError('index repair frozen output/source changed')
        return ref.as_dict()

    source_ref = freeze(receipt_path.parent/'source.json',receipt['source_sha256'])
    source = load_sealed_build_json(cas,source_ref)
    rows = source['rows']
    keys = {(r['ts_code'],r['trade_date']) for r in rows}
    if (source.get('schema_version') != 'aistock_frozen_index_facts_v1' or source.get('source_dataset') != 'market.index_daily'
        or len(keys) != len(rows) or len(rows) != receipt['source_rows']
        or {k[0] for k in keys} != CODES or max(k[1] for k in keys) != cutoff
        or any(date.fromisoformat(k[1]) > date.fromisoformat(cutoff) for k in keys)):
        raise ValueError('index source population or cutoff differs')
    files,seen = [],set()
    for item in receipt['inventory']:
        relative = f"daily_bin/qlib/features/{item['code'].lower()}/{item['feature']}.day.bin"
        if relative in seen or not allowed(relative) or item['feature'] not in INDEX_QLIB_FIELDS:
            raise ValueError('index correction escaped its component or is duplicated')
        seen.add(relative)
        files.append(dict(relative_path=relative,code=item['code'],feature=item['feature'],
            original_sha256=item['original_sha256'],target_ref=freeze(item['target_path'],item['corrected_sha256'])))
    if {(r['code'],r['feature']) for r in files} != {(c,f) for c in CODES for f in INDEX_QLIB_FIELDS}:
        raise ValueError('index correction feature population is incomplete')
    for name,key in [('index_daily.h5','raw_h5_sha256'),('index_context.parquet','context_parquet_sha256')]:
        old = _under(baseline/'index_context'/name,baseline)
        signature = _signature(old)
        digest = hashlib.sha256(old.read_bytes()).hexdigest()
        if _signature(old) != signature:
            raise ValueError('index baseline aggregate changed')
        files.append(dict(relative_path='index_context/'+name,original_sha256=digest,
            target_ref=freeze(receipt_path.parent/name,receipt[key])))
    return dict(schema_version=SCHEMA,producer_receipt=dict(inputs),base_manifest_ref=dict(base_manifest_ref),
        target_cutoff=cutoff,source_ref=source_ref,source_rows=len(rows),files=files,
        raw_rows=receipt.get('corrected_raw_rows',len(rows)),database_write=False)


def _pending(root):
    return root.parent/f'.{root.name}.index-fact-commit.json'


def recover_index_repairs(*,root,journal,record,cas):
    intent = _pending(root)
    _plain(intent)
    if not intent.exists():
        return record
    commit = json.loads(intent.read_bytes())
    if (commit.get('schema_version') != SCHEMA or commit.get('binding_digest') != record['binding_digest']
        or not commit.get('files') or not all(allowed(r) for r in commit['files'])):
        raise ValueError('index repair transaction identity differs')
    completed = record['steps'].get('index-facts') == commit['target_steps'].get('index-facts')
    if not completed and record['steps'] != commit['original_steps']:
        raise ValueError('index repair lost original step provenance')
    for relative,item in commit['files'].items():
        path = root/relative
        _plain(path)
        actual = list(_signature(path))
        if actual == item['target_signature']:
            continue
        temp = Path(item['temporary_path'])
        _plain(temp)
        if (completed or actual != item['original_signature'] or temp.parent != root.parent
            or not temp.name.startswith(f'.{root.name}.index-repair-')
            or list(_signature(temp)) != item['target_signature']
            or hashlib.sha256(temp.read_bytes()).hexdigest() != item['target_sha256']):
            raise ValueError('index repair target changed during recovery')
    result = copy.deepcopy(record)
    for relative,item in commit['files'].items():
        path = root/relative
        if list(_signature(path)) != item['target_signature']:
            os.replace(item['temporary_path'],path)
        result['files'][relative] = list(_signature(path))
    if not completed:
        result['steps'] = commit['target_steps']
    _atomic(journal,result)
    intent.unlink()
    return result


def validate_feature_change(original,target,field):
    """Only missing facts and the diagnosed thousand-CNY -> CNY error change."""
    old,new = _array(original),_array(target)
    finite = np.flatnonzero(np.isfinite(old[1:]))
    positions = finite+int(old[0])-int(new[0])+1
    if len(positions) and (positions.min()<1 or positions.max()>=len(new)):
        raise ValueError('index correction truncated existing finite facts')
    before,after = old[1:][finite],new[positions]
    exact = before == after
    if field == 'amount':
        exact |= np.isclose(before.astype('float64')*1000,after,rtol=4*np.finfo('float32').eps,atol=0)
    if not exact.all():
        raise ValueError('index correction conflicts with an existing finite fact')


def apply_index_repairs(resume, *,prepare,finalized,plan,cas):
    binding = plan.get(BINDING)
    if binding is None:
        return prepare,finalized
    root = resume.root
    if (resume.path is None or root.parent.name != '.staging' or (root/'qe_dataset_manifest.json').exists()
        or binding['operation_id'] != resume.context.operation_id or binding['target_cutoff'] != plan['target_cutoff']):
        raise ValueError('index repairs require the same unpublished build')
    if 'index-facts' in resume.record['steps']:
        if resume.record['steps']['index-facts'] != binding['authority_ref']:
            raise ValueError('index repair authority changed')
        return (load_sealed_build_json(cas,resume.record['steps']['prepare']),
                load_sealed_build_json(cas,resume.record['steps']['finalize-bins']))
    authority = load_sealed_build_json(cas,binding['authority_ref'])
    if (authority.get('schema_version') != SCHEMA or authority['base_manifest_ref'] != plan['monthly_ready_correction']['base_manifest_ref']
        or authority['target_cutoff'] != plan['target_cutoff']):
        raise ValueError('index repair authority binding differs')
    p = copy.deepcopy(load_sealed_build_json(cas,prepare['prepare_receipt_ref']))
    f = copy.deepcopy(load_sealed_build_json(cas,finalized['finalize_receipt_ref']))
    index = copy.deepcopy(load_sealed_build_json(cas,p['refs']['index_receipt']))
    daily = copy.deepcopy(load_sealed_build_json(cas,f['daily_receipt_ref']))
    entries = {(str(r['instrument']).upper(),r['feature']):r for r in daily['native_writer']['feature_receipts']}
    aggregates = {r['relative_path']:r for r in index['files']}
    files = {}

    def staged(relative,payload,original_sha=None):
        path = root/relative
        _plain(path)
        signature = list(_signature(path))
        raw = path.read_bytes()
        if (signature != resume.record['files'].get(relative) or list(_signature(path)) != signature
            or original_sha is not None and hashlib.sha256(raw).hexdigest() != original_sha):
            raise ValueError('index repair lost original writer provenance or target changed')
        fd,name = tempfile.mkstemp(prefix=f'.{root.name}.index-repair-',dir=root.parent)
        with os.fdopen(fd,'wb') as writer:
            writer.write(payload)
            writer.flush()
            os.fsync(writer.fileno())
        temporary = Path(name)
        pin = dict(original_signature=signature,target_signature=list(_signature(temporary)),
            target_sha256=hashlib.sha256(payload).hexdigest(),temporary_path=str(temporary))
        files[relative] = pin
        return pin,raw

    for row in authority['files']:
        resume.context.checkpoint()
        relative = row['relative_path']
        if not allowed(relative) or relative in META or relative in files:
            raise ValueError('index repair escaped its component or duplicated a file')
        target = cas.get_bytes(cas.validate_reference_metadata(row['target_ref']))
        pin,old = staged(relative,target,row['original_sha256'])
        if relative.endswith('.bin'):
            validate_feature_change(old,target,row['feature'])
            values = _array(target)
            item = entries[(row['code'],row['feature'])]
            item.update(target_sha256=pin['target_sha256'],target_size=len(target),target_signature=pin['target_signature'],
                start_index=int(values[0]),end_index=int(values[0])+len(values)-2)
        else:
            item = aggregates[relative.removeprefix('index_context/')]
            item.update(sha256=pin['target_sha256'],size=len(target),target_signature=pin['target_signature'])
    proof = dict(schema_version=SCHEMA,authority_ref=binding['authority_ref'],source_ref=authority['source_ref'])
    index.update(index_fact_correction=proof,normalized_parquet_scope='frozen_history_and_month_delta',
        rows=authority['raw_rows'],inherited_rows=authority['raw_rows']-index['month_rows'])
    daily['index_fact_correction'] = proof
    staged('daily_bin/materialization_receipt.json',canonical_json_bytes(daily)+b'\n')
    staged('index_context/index_materialization_receipt.json',canonical_json_bytes(index)+b'\n')
    p['refs'].update(index_receipt=cas.put_json(index).as_dict(),index_materialization_receipt_file=cas.put_json(index).as_dict())
    f.update(daily_receipt_ref=cas.put_json(daily).as_dict(),daily_materialization_receipt_file_ref=cas.put_json(daily).as_dict())
    result_p = {**prepare,'prepare_receipt_ref':cas.put_json(p).as_dict()}
    result_f = {**finalized,'finalize_receipt_ref':cas.put_json(f).as_dict()}
    steps = {**resume.record['steps'],'prepare':cas.put_json(result_p).as_dict(),
             'finalize-bins':cas.put_json(result_f).as_dict(),'index-facts':binding['authority_ref']}
    _atomic(_pending(root),dict(schema_version=SCHEMA,binding_digest=resume.record['binding_digest'],
        original_steps=resume.record['steps'],target_steps=steps,files=files))
    resume.record = recover_index_repairs(root=root,journal=resume.path,record=resume.record,cas=cas)
    return result_p,result_f
