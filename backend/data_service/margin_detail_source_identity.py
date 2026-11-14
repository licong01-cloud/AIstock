"""Publish a standalone immutable shared identity asset; never rewrite a release.

Audit the frozen margin source identities against the current read-only DB.
Dates with no row on either side are NOT invented expected financing sessions.
Alias validity identifies a security, not financing eligibility or availability.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import urllib.request

import numpy as np
import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.data_service.security_source_identity import (  # noqa: E402
    MARGIN_DETAIL_DATASET, canonical_json_bytes, canonical_sha256,
    load_security_source_identity_manifest,
)
from backend.data_service.qe_data_service import MARGIN_DETAIL_FIELD_MAP  # noqa: E402
from backend.services.data_integrity.stock_scope import is_supported_stock_code  # noqa: E402
from scripts.prepare_canonical_pit_monthly import _load_database_config  # noqa: E402


def file_sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def compare_alias_rows(identity, canonical, frozen, source, columns):
    """One result per observed canonical/date; preserve raw and frozen values."""
    def indexed(frame):
        result = {}
        for record in frame.to_dict('records'):
            key = (str(record['ts_code']), pd.Timestamp(record['trade_date']).date())
            if key in result:
                raise ValueError(f'duplicate financing source identity: {key}')
            result[key] = record
        return result

    actual, expected = indexed(frozen), indexed(source)
    codes_by_day = {}
    for code, day in actual.keys() | expected.keys():
        codes_by_day.setdefault(day, set()).add(code)
    result = []
    for day in sorted(codes_by_day):
        resolution = identity.resolve(canonical, day, MARGIN_DETAIL_DATASET)
        key = (resolution.source_ts_code, day)
        f, s = actual.get(key), expected.get(key)
        wrong = sorted(codes_by_day[day]-{key[0]})
        changed, nulls, invalid = [], [], []
        if wrong:
            kind = 'SOURCE_IDENTITY_CONFLICT'
        elif f is None:
            kind = 'EXPORT_MISSING'
        elif s is None:
            kind = 'SOURCE_UNVERIFIED'
        else:
            for field in columns:
                left, right = f[field], s[field]
                left_na, right_na = bool(pd.isna(left)), bool(pd.isna(right))
                if left_na and right_na:
                    nulls.append(field)
                    continue
                if left_na != right_na:
                    changed.append(field)
                    continue
                if not np.isfinite(float(left)) or not np.isfinite(float(right)):
                    invalid.append(field)
                # Frozen factor fields are formally float32. This is an exact
                # comparison at that representation, not a looser tolerance.
                elif float(left) != float(np.float32(right)):
                    changed.append(field)
            kind = 'INVALID_VALUE' if invalid else 'VALUE_MISMATCH' if changed else 'MATCHED'
        values = {field: None if pd.isna(f[field]) else float(f[field]) for field in columns} if f else None
        result.append(dict(canonical_ts_code=canonical, trade_date=str(day), source_dataset=MARGIN_DETAIL_DATASET,
            effective_source_code=key[0], classification=kind, identity_row_sha256=resolution.row_hash,
            field_count=len(columns), source_null_fields=nulls, changed_fields=changed,
            invalid_fields=invalid, conflicting_source_codes=wrong,
            frozen_fact_sha256=canonical_sha256(values) if values is not None and not invalid else None))
    return result


def audit(root, identity_path, expected_manifest, *, connection):
    root, identity_path = Path(root).resolve(strict=True), Path(identity_path).resolve(strict=True)
    manifest_path = root/'qe_dataset_manifest.json'
    margin_path = root/'components/factor_h5_static_candidate_v2/margin_detail.h5'
    old_identity = root/'components/factor_h5_static_candidate_v2/security_source_identity.json'
    inputs = [manifest_path, margin_path, old_identity]
    for path in [*inputs, identity_path]:
        if not path.is_file() or path.is_symlink():
            raise ValueError(f'expected a regular frozen input: {path}')
    before = {str(p): file_sha256(p) for p in inputs}
    manifest = json.loads(manifest_path.read_bytes())
    if manifest['dataset_manifest_sha256'] != expected_manifest:
        raise ValueError('predecessor dataset identity differs')
    for path in [margin_path, old_identity]:
        relative = path.relative_to(root).as_posix()
        pins = [v for v in manifest['components'].values() if v.get('path') == relative]
        if len(pins) != 1 or pins[0]['sha256'] != before[str(path)] or pins[0]['size'] != path.stat().st_size:
            raise ValueError(f'predecessor component pin differs: {relative}')
    identity = load_security_source_identity_manifest(identity_path)
    aliases = [r for r in identity.rows if r.source_dataset == MARGIN_DETAIL_DATASET]
    if not aliases:
        raise ValueError('explicit margin identity is absent')
    symbols = sorted({r.canonical_ts_code for r in aliases})
    source_codes = sorted(set(symbols) | {r.source_ts_code for r in aliases})
    frozen = []
    columns = list(MARGIN_DETAIL_FIELD_MAP.values())
    with pd.HDFStore(margin_path, mode='r') as store:
        inventory = sorted(str(c) for c in store.select_column('data', 'instrument').unique())
        for code in source_codes:
            frame = store.select('data', where=f"instrument == '{code}'")
            if list(frame.columns) != columns or any(dtype != np.dtype('float32') for dtype in frame.dtypes):
                raise ValueError('frozen margin schema or representation differs')
            frozen.append(frame.reset_index().rename(columns={'datetime':'trade_date', 'instrument':'ts_code'}))
    frozen = pd.concat(frozen, ignore_index=True)
    if frozen.empty:
        raise ValueError('no observed frozen alias facts')
    start = pd.Timestamp(frozen['trade_date'].min()).date()
    end = date.fromisoformat(manifest['cutoff_trade_date'])
    with connection.cursor() as cur:
        cur.execute('SELECT current_database(),inet_server_port(),current_setting(\'transaction_read_only\')')
        database, port, read_only = cur.fetchone()
        if (database, port, read_only) != ('aistock', 5432, 'on'):
            raise ValueError('source audit requires the declared read-only production database')
        cur.execute('SELECT ts_code FROM market.stock_basic')
        stock_master = {str(r[0]).strip() for r in cur.fetchall()}
        cur.execute('SELECT trade_date,ts_code,'+','.join(MARGIN_DETAIL_FIELD_MAP)+
            ' FROM market.margin_detail WHERE ts_code=ANY(%s::text[]) AND trade_date BETWEEN %s AND %s '
            'ORDER BY trade_date,ts_code', (source_codes, start, end))
        source = pd.DataFrame(cur.fetchall(), columns=['trade_date','ts_code',*MARGIN_DETAIL_FIELD_MAP])
    source = source.rename(columns=MARGIN_DETAIL_FIELD_MAP)
    results, summaries = [], []
    for symbol in symbols:
        codes = identity.query_source_codes([symbol], start, end, MARGIN_DETAIL_DATASET)
        f, s = frozen[frozen['ts_code'].isin(codes)], source[source['ts_code'].isin(codes)]
        records = compare_alias_rows(identity, symbol, f, s, columns)
        results.extend(records)
        summaries.append(dict(canonical_ts_code=symbol, frozen_rows=len(f), source_rows=len(s),
            matched=sum(r['classification']=='MATCHED' for r in records),
            observed_ranges=[dict(source_code=code, row_count=len(group),
                first_date=str(pd.Timestamp(group['trade_date'].min()).date()),
                last_date=str(pd.Timestamp(group['trade_date'].max()).date())) for code, group in f.groupby('ts_code')]))
    unregistered = sorted(c for c in inventory if is_supported_stock_code(c)
        and c not in stock_master and c not in {r.source_ts_code for r in aliases})
    after = {str(p): file_sha256(p) for p in inputs}
    if before != after:
        raise ValueError('frozen predecessor changed during readback')
    return dict(schema_version='aistock_margin_source_identity_readback_v1',
        status='PASS' if not unregistered and all(r['classification']=='MATCHED' for r in results) else 'BLOCKED',
        predecessor_manifest_identity=expected_manifest, predecessor_manifest_file_sha256=before[str(manifest_path)],
        predecessor_revision=manifest['revision'], release_id=manifest['release_id'], cutoff=str(end),
        predecessor_root=str(root), margin_detail_sha256=before[str(margin_path)],
        predecessor_identity_sha256=before[str(old_identity)], identity=identity.evidence(),
        instrument_count=len(inventory), equity_instrument_count=sum(is_supported_stock_code(c) for c in inventory),
        unregistered_legacy_equity_source_codes=unregistered, alias_count=len(aliases),
        registered_margin_aliases_without_observed_facts=sorted(r.canonical_ts_code for r in aliases
            if not {r.canonical_ts_code, r.source_ts_code}.intersection(set(frozen['ts_code']))),
        summaries=summaries, keys=results, predecessor_bytes_unchanged=before==after,
        scope='observed_source_facts_and_equity_identity_inventory_not_financing_eligibility_or_provider_absence',
        units='CNY amounts and share counts unchanged; frozen float32 representation',
        database_write=False, active_profile_write=False, dataset_export=False, runtime_action=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate-root', required=True, type=Path)
    parser.add_argument('--identity-manifest', required=True, type=Path)
    parser.add_argument('--expected-manifest', required=True)
    parser.add_argument('--output-root', required=True, type=Path)
    args = parser.parse_args()
    if args.output_root.exists():
        raise ValueError('immutable output already exists; do not overwrite')
    if args.output_root.resolve().is_relative_to(args.candidate_root.resolve()):
        raise ValueError('standalone authority must be outside the frozen candidate')
    cfg = _load_database_config('production', Path('F:/Dev/AIstock/.env'))
    conn = psycopg2.connect(host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password,
        dbname=cfg.dbname, connect_timeout=8)
    conn.set_session(readonly=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout='25s'")
            cur.execute("SET lock_timeout='3s'")
        report = audit(args.candidate_root, args.identity_manifest, args.expected_manifest, connection=conn)
    finally:
        conn.rollback()
        conn.close()
    if report['status'] != 'PASS':
        print(json.dumps({'status':'BLOCKED', 'summaries':report['summaries'],
            'unregistered':report['unregistered_legacy_equity_source_codes'],
            'failures':[r for r in report['keys'] if r['classification']!='MATCHED'][:20]}, ensure_ascii=False))
        raise ValueError('source identity readback has unresolved differences; no package published')
    authority = load_security_source_identity_manifest(args.identity_manifest)
    source_proofs = []
    for row in authority.rows:
        if row.source_dataset != MARGIN_DETAIL_DATASET:
            continue
        with urllib.request.urlopen(row.authority_ref, timeout=30) as response:
            payload = response.read()
        if hashlib.sha256(payload).hexdigest() != row.authority_hash or not payload.startswith(b'%PDF-'):
            raise ValueError('official source content changed; do not substitute an approved hash')
        source_proofs.append(dict(url=row.authority_ref, sha256=row.authority_hash,
            captured_at=datetime.now(timezone.utc).isoformat(), historical_first_publication_time='UNKNOWN',
            identity_window_is_not_financing_eligibility=True))
    args.output_root.mkdir(parents=True, exist_ok=False)
    files = {'security_source_identity.json': args.identity_manifest.read_bytes(),
             'margin_source_identity_readback.json': canonical_json_bytes(report)+b'\n'}
    for name, payload in files.items():
        with (args.output_root/name).open('xb') as output:
            output.write(payload)
    package = dict(schema_version='aistock_shared_security_source_identity_package_v1',
        authority_version=authority.manifest_version, predecessor_manifest_identity=args.expected_manifest,
        predecessor_data_sha256=report['margin_detail_sha256'], data_files_reused_unchanged=True,
        official_sources=source_proofs, files={name:dict(sha256=hashlib.sha256(payload).hexdigest(),size=len(payload))
                                             for name,payload in files.items()})
    raw = canonical_json_bytes(package)+b'\n'
    with (args.output_root/'manifest.json').open('xb') as output:
        output.write(raw)
    print(json.dumps(dict(status='PASS', package_root=str(args.output_root.resolve()),
        package_manifest_sha256=hashlib.sha256(raw).hexdigest(), authority=authority.evidence(),
        key_count=len(report['keys']), summaries=report['summaries'], successor_required=False), ensure_ascii=False))


if __name__ == '__main__':
    main()
