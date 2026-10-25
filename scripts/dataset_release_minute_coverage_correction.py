"""Bounded DB capture and private immutable minute coverage amendment.

No database writes, deployment, activation, experiments or process control.
Unchanged files are adopted by existing manifest pins, not re-exported.
"""
from __future__ import annotations

import argparse
import copy
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.dataset_release.canonical import canonical_json_bytes  # noqa: E402
from backend.services.dataset_release.monthly_late_publication import file_pin  # noqa: E402
from backend.services.dataset_release.monthly_legacy_prefix import _plain_chain, _relative  # noqa: E402
from backend.services.dataset_release.monthly_minute_coverage import calendar_sessions, patch_minute_fields  # noqa: E402
from backend.services.dataset_release.monthly_minute_repair import capture_database_minutes  # noqa: E402
from backend.qlib_exporter.quote_catalog import build_minute_quote_catalog  # noqa: E402

SCHEMA = 'aistock_monthly_minute_coverage_amendment_v1'


def _read(path):
    return json.loads(path.read_text(encoding='utf-8'))


def _write(path, value):
    raw = canonical_json_bytes(value) + b'\n'
    if path.exists():
        if path.is_symlink() or path.read_bytes() != raw:
            raise FileExistsError(path)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as handle:
        handle.write(raw)


def capture(args):
    import psycopg2
    from dotenv import load_dotenv
    from scripts.backfill_tushare_daily_basic_fields import db_config

    if args.output.exists():
        raise FileExistsError(args.output)
    profile = _read(args.profile)
    root = Path(profile['controller_paths']['candidate_root'])
    calendar = root / 'components/minute_bin_candidate/calendars/1min.txt'
    sessions = calendar_sessions(calendar)
    sessions = {day: [stamp for stamp in rows if stamp[11:] != '13:00:00']
                for day, rows in sessions.items()}
    requested = _read(args.keys)
    keys = requested.get('stock_dates')
    if keys is None:
        if 'groups' in requested:
            keys = [{'ts_code': code, 'trade_date': day}
                    for code, group in requested['groups'].items() for day in group['missing_days']]
        else:
            keys = [{'ts_code': code, 'trade_date': day}
                    for code, group in requested['missing'].items() for day in group]
    load_dotenv(args.env_file, override=False)
    config = db_config()
    config.update(application_name='BUG-2070-minute-coverage-readonly', connect_timeout=10,
        options='-c client_encoding=utf8 -c statement_timeout=120000 -c default_transaction_read_only=on')
    authority = capture_database_minutes({'stock_dates': keys}, cutoff=date.fromisoformat(profile['cutoff']),
        frozen_sessions=sessions, connection_factory=lambda: psycopg2.connect(**config))
    if authority['database'] != 'aistock' or authority['absent_stock_dates']:
        raise ValueError('bounded production source capture is absent or target differs')
    authority.update(predecessor_profile={'path': str(args.profile.absolute()), **file_pin(args.profile)},
        predecessor_manifest_sha256=profile['components']['dataset_manifest_sha256'],
        requested_keys_ref={'path': str(args.keys.absolute()), **file_pin(args.keys)},
        calendar_sha256=file_pin(calendar)['sha256'])
    _write(args.output / 'minute-source.json', authority)
    return {k: v for k, v in authority.items() if k not in ('facts', 'inputs')}


def patch(args):
    profile, authority = _read(args.profile), _read(args.source / 'minute-source.json')
    root = Path(profile['controller_paths']['candidate_root'])
    minute, daily = root / 'components/minute_bin_candidate', root / 'components/daily_bin_candidate'
    calendar = minute / 'calendars/1min.txt'
    if (authority['predecessor_profile']['sha256'] != file_pin(args.profile)['sha256']
            or authority['calendar_sha256'] != file_pin(calendar)['sha256']):
        raise ValueError('minute capture and patch predecessor differ')
    payloads, summary = patch_minute_fields(rows=authority['facts'], daily=daily, minute=minute,
        day_calendar=(daily / 'calendars/day.txt').read_text(encoding='utf-8').splitlines(),
        minute_calendar=calendar.read_text(encoding='utf-8').splitlines())
    if not payloads:
        raise ValueError('minute amendment has no missing cells')
    args.output.mkdir(parents=True, exist_ok=False)
    files = {}
    for relative, raw in payloads.items():
        destination = args.output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open('xb') as handle:
            handle.write(raw)
        old = minute / relative
        files[relative] = {'before': file_pin(old) if old.exists() else None, 'after': file_pin(destination)}
    receipt = {'schema_version': SCHEMA, 'status': 'PRIVATE_COMPONENT_NOT_A_RELEASE',
        'predecessor_profile': authority['predecessor_profile'], 'predecessor_candidate_root': str(root),
        'predecessor_manifest_sha256': profile['components']['dataset_manifest_sha256'],
        'source_ref': {'path': str((args.source / 'minute-source.json').absolute()),
                       **file_pin(args.source / 'minute-source.json')},
        'stock_dates': authority['inputs']['stock_dates'], 'source_rows': authority['row_count'],
        'non_calendar_source_rows_ignored': authority['non_calendar_source_rows_ignored'],
        'calendar_sha256': authority['calendar_sha256'], **summary, 'files': files,
        'database_write': False, 'active_profile_write': False, 'runtime_action': False}
    _write(args.output / 'component-amendment.json', receipt)
    return {k: v for k, v in receipt.items() if k not in ('files', 'stock_dates')}


def assemble(args):
    profile, correction = _read(args.profile), _read(args.component / 'component-amendment.json')
    base, target = Path(profile['controller_paths']['candidate_root']), args.output.absolute()
    if (correction['predecessor_profile']['sha256'] != file_pin(args.profile)['sha256']
            or target == base or target.parent != base.parent):
        raise ValueError('minute amendment requires its exact predecessor and a new sibling root')
    _plain_chain(base)
    if (target / 'release_closure_receipt.json').exists():
        raise ValueError('sealed minute successor is immutable')
    manifest = _read(base / 'qe_dataset_manifest.json')
    if file_pin(base / 'qe_dataset_manifest.json')['sha256'] != profile['components']['dataset_manifest_file_sha256']:
        raise ValueError('minute predecessor manifest bytes differ')
    replaced = {}
    for relative, pin in correction['files'].items():
        path = args.component / _relative(relative)
        if file_pin(path) != pin['after']:
            raise ValueError('private minute patch changed')
        old = base / 'components/minute_bin_candidate' / relative
        if (file_pin(old) if old.exists() else None) != pin['before']:
            raise ValueError('minute patch predecessor bytes changed')
        for prefix in ('components/minute_bin_candidate/', 'minute_bin/qlib/'):
            replaced[prefix + relative] = path
    special = {prefix + relative for prefix in ('components/minute_bin_candidate/', 'minute_bin/qlib/')
        for relative in ('instruments/all.txt', 'meta_export.json')}
    special.add('minute_bin/materialization_receipt.json')
    marker = target / '.minute-assembly-inputs.json'
    assembly_inputs = {'predecessor_profile_sha256': file_pin(args.profile)['sha256'],
                       'correction_sha256': file_pin(args.component / 'component-amendment.json')['sha256']}
    if target.exists():
        if not marker.is_file() or _read(marker) != assembly_inputs:
            raise ValueError('existing minute candidate belongs to another assembly')
    else:
        target.mkdir(exist_ok=False)
        _write(marker, assembly_inputs)
    seen, reused = set(), []
    for count, pin in enumerate(manifest['components'].values(), 1):
        relative = _relative(pin['path']).as_posix()
        if relative in seen:
            continue
        seen.add(relative)
        if relative in replaced or relative in special:
            continue
        source, destination = base / relative, target / relative
        _plain_chain(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            if not os.path.samefile(source, destination):
                raise ValueError('resumed unchanged pin differs')
        else:
            os.link(source, destination)
        reused.append({'path': relative, 'sha256': pin['sha256'], 'size': pin['size']})
        if count % 20000 == 0:
            print(json.dumps({'phase': 'adopt_unchanged_pins', 'files': count, 'total': len(manifest['components'])}), flush=True)
    for relative, source in replaced.items():
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            if file_pin(destination) != file_pin(source):
                raise ValueError('resumed repaired pin differs')
        else:
            shutil.copyfile(source, destination)
    provider = target / 'components/minute_bin_candidate'
    catalog = build_minute_quote_catalog(provider)
    for prefix in ('components/minute_bin_candidate/', 'minute_bin/qlib/'):
        path = target / prefix / 'instruments/all.txt'
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() != catalog.payload:
                raise ValueError('resumed quote catalog differs')
        else:
            with path.open('xb') as handle:
                handle.write(catalog.payload)
        meta = _read(base / prefix / 'meta_export.json')
        meta['minute_coverage_amendment'] = {'receipt': 'reports/minute-coverage-amendment.json',
            'calendar_sha256': catalog.calendar_sha256, 'quote_catalog_sha256': hashlib.sha256(catalog.payload).hexdigest()}
        _write(target / prefix / 'meta_export.json', meta)
    materialization = _read(base / 'minute_bin/materialization_receipt.json')
    materialization['historical_coverage_amendment'] = {'receipt': 'reports/minute-coverage-amendment.json',
        'source_ref': correction['source_ref'], 'fields': correction['fields'], 'filled_cells': correction['filled_cells']}
    _write(target / 'minute_bin/materialization_receipt.json', materialization)
    _write(target / 'reports/minute-coverage-amendment.json', correction)
    source_path = Path(correction['source_ref']['path'])
    if file_pin(source_path) != {k: correction['source_ref'][k] for k in ('sha256', 'size')}:
        raise ValueError('minute source changed during assembly')
    target_source = target / 'reports/minute-coverage-source.json'
    if target_source.exists():
        if file_pin(target_source) != file_pin(source_path):
            raise ValueError('resumed frozen source differs')
    else:
        shutil.copyfile(source_path, target_source)
    revised = copy.deepcopy(manifest)
    by_path = {pin['path']: pin for pin in revised['components'].values()}
    changed = set(replaced) | special | {'reports/minute-coverage-amendment.json', 'reports/minute-coverage-source.json'}
    for relative in sorted(changed):
        pin = {'path': relative, **file_pin(target / relative)}
        if relative in by_path:
            by_path[relative].update(pin)
        else:
            revised['components'][relative.replace('/', '__')] = pin
    revised['revision'] += '-minute1'
    revised['source_contract']['minute_coverage_amendment'] = {
        'predecessor_manifest_sha256': manifest['dataset_manifest_sha256'],
        'receipt_sha256': file_pin(target / 'reports/minute-coverage-amendment.json')['sha256']}
    revised['deployment_content_sha256'] = hashlib.sha256(canonical_json_bytes(revised['components'])).hexdigest()
    revised['deployment_snapshot_id'] = revised['release_id'] + '_' + revised['deployment_content_sha256'][:16]
    revised.pop('dataset_manifest_sha256', None)
    revised['dataset_manifest_sha256'] = hashlib.sha256(canonical_json_bytes(revised)).hexdigest()
    _write(target / 'qe_dataset_manifest.json', revised)
    draft = copy.deepcopy(profile)
    draft['generation'] += '-minute1'
    draft['controller_paths'] = {key: value.replace(str(base), str(target)) for key, value in profile['controller_paths'].items()}
    for binding in draft['node_bindings'].values():
        binding['candidate_root'] = binding['candidate_root'].rsplit('/', 1)[0] + '/' + target.name
    draft['components'].update(dataset_manifest_sha256=revised['dataset_manifest_sha256'],
        dataset_manifest_file_sha256=file_pin(target / 'qe_dataset_manifest.json')['sha256'])
    _write(args.component / 'profile-draft.json', draft)
    _write(args.component / 'reused-pins.json', reused)
    result = {'status': 'ASSEMBLED_PENDING_VALIDATION_AND_SEAL', 'candidate_root': str(target),
        'generation': draft['generation'], 'revision': revised['revision'], 'release_id': revised['release_id'],
        'dataset_manifest_sha256': revised['dataset_manifest_sha256'],
        'dataset_manifest_file_sha256': draft['components']['dataset_manifest_file_sha256'],
        'changed_paths': sorted(changed), 'unchanged_pins': len(reused),
        'database_write': False, 'active_profile_write': False, 'runtime_action': False}
    _write(args.component / 'assembly-result.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    p = sub.add_parser('capture')
    p.add_argument('--profile', type=Path, required=True)
    p.add_argument('--keys', type=Path, required=True)
    p.add_argument('--env-file', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p = sub.add_parser('patch')
    p.add_argument('--profile', type=Path, required=True)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p = sub.add_parser('assemble')
    p.add_argument('--profile', type=Path, required=True)
    p.add_argument('--component', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p = sub.add_parser('seal')
    p.add_argument('--env-file', type=Path, required=True)
    p.add_argument('--component', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    from scripts.dataset_release_margin_detail_correction import seal
    print(json.dumps({'capture': capture, 'patch': patch, 'assemble': assemble, 'seal': seal}[args.action](args), ensure_ascii=False))


if __name__ == '__main__':
    main()
