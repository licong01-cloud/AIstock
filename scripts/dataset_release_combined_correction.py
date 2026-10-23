"""Compose completed exact-key corrections through the existing minute release lane.

This is a file-only preparer. It does not collect data, activate, deploy, write
databases, or change consumer contracts. Every input pins its real predecessor.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.dataset_release.canonical import canonical_json_bytes  # noqa: E402
from backend.services.dataset_release.monthly_late_publication import file_pin  # noqa: E402
from backend.services.dataset_release.monthly_minute_adoption import pins  # noqa: E402
from scripts.dataset_release_minute_coverage_correction import _read, _write, assemble  # noqa: E402


def replace_json(path, value, work):
    temporary = work / (path.name + '.replacement')
    _write(temporary, value)
    os.replace(temporary, path)  # Never truncate an inherited hardlink.


def prepare(args):
    if (args.output / 'release_closure_receipt.json').exists():
        raise ValueError('sealed successor is immutable; resume its readback, not assembly')
    args.work.mkdir(parents=True, exist_ok=True)
    profile = _read(args.profile)
    base = Path(profile['controller_paths']['candidate_root'])
    manifest = _read(base / 'qe_dataset_manifest.json')
    before = pins(manifest)
    physical_profile = _read(args.physical_profile)
    physical = Path(physical_profile['controller_paths']['candidate_root'])
    if file_pin(physical / 'qe_dataset_manifest.json')['sha256'] != physical_profile['components']['dataset_manifest_file_sha256']:
        raise ValueError('physical manifest identity changed')
    original = _read(physical / 'reports/minute-coverage-amendment.json')
    addition = _read(args.minute_component / 'component-amendment.json')
    if (original['predecessor_profile']['sha256'] != file_pin(args.profile)['sha256']
            or addition['predecessor_profile']['sha256'] != file_pin(args.physical_profile)['sha256']):
        raise ValueError('correction input chain differs')
    correction = copy.deepcopy(original)
    keys = {(r['ts_code'], r['trade_date']) for r in original['stock_dates']}
    extra_keys = {(r['ts_code'], r['trade_date']) for r in addition['stock_dates']}
    if keys & extra_keys:
        raise ValueError('correction source keys overlap')
    correction['stock_dates'] = [{'ts_code': s, 'trade_date': d} for s, d in sorted(keys | extra_keys)]
    correction['files'] = {}
    for relative in sorted(set(original['files']) | set(addition['files'])):
        src = args.minute_component / relative if relative in addition['files'] else physical / 'components/minute_bin_candidate' / relative
        expected = addition['files'][relative]['after'] if relative in addition['files'] else original['files'][relative]['after']
        if file_pin(src) != expected:
            raise ValueError('prepared minute file changed')
        old_pin = before.get('components/minute_bin_candidate/' + relative)
        correction['files'][relative] = {'before': None if old_pin is None else {k: old_pin[k] for k in ('sha256', 'size')},
                                         'after': expected}
        out = args.work / relative
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.exists():
            if file_pin(out) != expected:
                raise ValueError('resumed minute component differs')
        else:
            os.link(src, out)
    for field in ('checked_cells', 'filled_cells', 'source_rows'):
        correction[field] = original[field] + addition[field]
    correction['status'] = 'PRIVATE_COMPONENT_NOT_A_RELEASE'
    correction['existing_finite_values_unchanged'] = True
    source = {'schema_version': 'aistock_combined_frozen_minute_source_refs_v1',
              'sources': [original['source_ref'], addition['source_ref']],
              'database_write': False, 'active_profile_write': False, 'runtime_action': False}
    for entry in source['sources']:
        if file_pin(Path(entry['path'])) != {k: entry[k] for k in ('sha256', 'size')}:
            raise ValueError('frozen minute source changed')
    _write(args.work / 'source-refs.json', source)
    correction['source_ref'] = {'path': str(args.work / 'source-refs.json'), **file_pin(args.work / 'source-refs.json')}
    targets = {}
    for relative, entry in addition['daily_reference_files'].items():
        src = Path(addition['daily_reference_component']) / relative
        if file_pin(src) != entry['after']:
            raise ValueError('daily reference changed')
        for prefix in ('components/daily_bin_candidate/', 'daily_bin/qlib/'):
            targets[prefix + relative] = src
    industry = _read(args.sector_static / 'component-amendment.json')
    if industry['predecessor_profile_sha256'] != file_pin(args.physical_profile)['sha256']:
        raise ValueError('static amendment physical predecessor differs')
    h5 = _read(args.sector_h5_receipt)['components']['sector_data.h5']
    if (file_pin(args.sector_h5) != {'sha256': h5['sha256'], 'size': h5['bytes']}
            or h5['predecessor_sha256'] != before['factor_bundle/sector_data.h5']['sha256']
            or h5['exact_affected_readback'] is not True or h5['unchanged_predecessor_stat'] is not True
            or h5['inserted_rows'] != industry['exact_stock_dates']):
        raise ValueError('sector H5 validated component differs')
    minute_readback = _read(args.minute_readback)
    if (minute_readback['status'] != 'PASS' or minute_readback['unresolved_stock_days'] != 0
            or minute_readback['component_ref']['sha256'] != file_pin(args.minute_component / 'component-amendment.json')['sha256']
            or minute_readback['exact_matched_cells'] != addition['checked_cells']):
        raise ValueError('minute independent readback differs')
    for filename, src in [('sector_data.h5', args.sector_h5), ('static_factors.parquet', args.sector_static / 'static_factors.parquet')]:
        for prefix in ('components/factor_h5_static_candidate_v2/', 'factor_bundle/'):
            targets[prefix + filename] = src
    if file_pin(args.sector_static / 'static_factors.parquet') != {k: industry[k] for k in ('sha256', 'size')}:
        raise ValueError('static fact component changed')
    sector_receipt = _read(physical / 'components/sector_context_candidate_v1/component_receipt.json')
    sector_receipt['sector_data'].update(sha256=file_pin(args.sector_h5)['sha256'], byte_size=args.sector_h5.stat().st_size)
    _write(args.work / 'sector-context-receipt.json', sector_receipt)
    targets['components/sector_context_candidate_v1/component_receipt.json'] = args.work / 'sector-context-receipt.json'
    fact = {'files': {}, 'evidence': {}, 'stock_dates': industry['exact_stock_dates'],
            'existing_finite_values_unchanged': True, 'database_write': False,
            'active_profile_write': False, 'runtime_action': False}
    for relative, src in targets.items():
        fact['files'][relative] = {'before': before[relative], 'after': {'path': relative, **file_pin(src)}}
    for label, src in [('sector-static', args.sector_static / 'component-amendment.json'),
                       ('sector-h5', args.sector_h5_receipt), ('minute', args.minute_readback),
                       ('preclose', Path(addition['production_reference_readback']['path']))]:
        relative = 'reports/combined-facts/' + label + '.json'
        fact['evidence'][relative] = {'path': relative, **file_pin(src)}
        targets[relative] = src
    correction['additional_fact_amendment'] = fact
    _write(args.work / 'component-amendment.json', correction)
    assembly_receipt = args.work / 'assembly-result.json'
    result = _read(assembly_receipt) if assembly_receipt.exists() else assemble(
        SimpleNamespace(profile=args.profile, component=args.work, output=args.output))
    for relative, src in targets.items():
        destination = args.output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = destination.with_name(destination.name + '.new')
        if not destination.exists() or file_pin(destination) != file_pin(src):
            with src.open('rb') as reader, temp.open('xb') as writer:
                shutil.copyfileobj(reader, writer, 8 * 1024 * 1024)
            os.replace(temp, destination)
    value = _read(args.output / 'qe_dataset_manifest.json')
    value['revision'] = manifest['revision'] + '-facts1'
    by_path = {p['path']: p for p in value['components'].values()}
    for relative in targets:
        pin = {'path': relative, **file_pin(args.output / relative)}
        if relative in by_path:
            by_path[relative].update(pin)
        else:
            value['components'][relative.replace('/', '__')] = pin
    value['deployment_content_sha256'] = hashlib.sha256(canonical_json_bytes(value['components'])).hexdigest()
    value['deployment_snapshot_id'] = value['release_id'] + '_' + value['deployment_content_sha256'][:16]
    value.pop('dataset_manifest_sha256')
    value['dataset_manifest_sha256'] = hashlib.sha256(canonical_json_bytes(value)).hexdigest()
    replace_json(args.output / 'qe_dataset_manifest.json', value, args.work)
    draft = _read(args.work / 'profile-draft.json')
    draft['generation'] = profile['generation'] + '-facts1'
    draft['components'].update(dataset_manifest_sha256=value['dataset_manifest_sha256'],
                               dataset_manifest_file_sha256=file_pin(args.output / 'qe_dataset_manifest.json')['sha256'])
    replace_json(args.work / 'profile-draft.json', draft, args.work)
    window = _read(physical / 'reports/minute-full-window-validation.json')
    window.update(generation=draft['generation'], physical_gap_days_before=len(keys | extra_keys),
                  predecessor_manifest_sha256=manifest['dataset_manifest_sha256'],
                  inherited_complete_window_receipt={'path': str(physical / 'reports/minute-full-window-validation.json'),
                      **file_pin(physical / 'reports/minute-full-window-validation.json')},
                  validation_method='INHERITED_WINDOW_PLUS_EXACT_FINITE_PRESERVING_ADDITIONS',
                  field_readback={k: correction[k] for k in ('checked_cells', 'filled_cells', 'fields', 'existing_finite_values_unchanged')},
                  additional_readback_ref={'path': str(args.minute_readback), **file_pin(args.minute_readback)},
                  minute_content_digest=hashlib.sha256(canonical_json_bytes({
                      p['path']: p for p in value['components'].values()
                      if p['path'].startswith('components/minute_bin_candidate/')})).hexdigest())
    _write(args.work / 'full-window-validation.json', window)
    result.update(dataset_manifest_sha256=value['dataset_manifest_sha256'],
                  generation=draft['generation'], revision=value['revision'],
                  component_stock_dates=len(keys | extra_keys), fact_stock_dates=fact['stock_dates'])
    _write(args.work / 'combined-assembly-result.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('profile', 'physical-profile', 'minute-component', 'minute-readback', 'sector-static',
                 'sector-h5', 'sector-h5-receipt', 'work', 'output'):
        parser.add_argument('--' + name, required=True, type=Path)
    print(json.dumps(prepare(parser.parse_args()), ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
