#!/usr/bin/env python
"""Targeted dynamic truncation leakage audit for QE factor scripts.

This script runs selected factor scripts twice in an isolated temp workspace:
1. with full input data;
2. with pandas.read_hdf/read_parquet monkeypatched to truncate PIT data to a cutoff.

For a no-leak factor, values on dates <= cutoff must match between the full and
truncated runs. This is intentionally targeted because full 50+ factor dynamic
recompute is expensive.
"""
from __future__ import annotations

import argparse
import json
import os
import runpy
import shutil
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def _parse_loops(value: str) -> list[int]:
    out=[]
    for part in value.split(','):
        part=part.strip()
        if not part: continue
        if '-' in part:
            a,b=part.split('-',1); out.extend(range(int(a), int(b)+1))
        else: out.append(int(part))
    return sorted(dict.fromkeys(out))


def _pick_result_path(root: Path, factor_name: str) -> Path:
    candidates = [root / 'result.h5', root / 'factors' / 'result.h5', root / 'factors' / factor_name / 'result.h5']
    for p in candidates:
        if p.exists(): return p
    found = list(root.rglob('result.h5'))
    if found: return found[0]
    raise FileNotFoundError(f'factor {factor_name} did not write result.h5 under {root}')


def _filter_pit(df: pd.DataFrame | pd.Series, cutoff: pd.Timestamp):
    if isinstance(df.index, pd.MultiIndex):
        names = list(df.index.names)
        if 'datetime' in names:
            vals = pd.to_datetime(df.index.get_level_values('datetime'))
            return df[vals <= cutoff]
        vals = pd.to_datetime(df.index.get_level_values(0), errors='coerce')
        return df[vals <= cutoff]
    if isinstance(df.index, pd.DatetimeIndex):
        return df[df.index <= cutoff]
    return df


def _prepare_workspace(loop_dir: Path, factor_file: Path, temp_root: Path) -> Path:
    (temp_root / 'factors').mkdir(parents=True, exist_ok=True)
    shutil.copy2(factor_file, temp_root / 'factors' / factor_file.name)
    # Link/copy the data files referenced by generated factor scripts.
    for name in ['daily_basic.h5','daily_pv.h5','moneyflow.h5','bak_basic.h5','cyq_perf.h5','sector_data.h5','static_factors.parquet','margin_detail.h5']:
        src = loop_dir / name
        if src.exists():
            dst = temp_root / name
            try:
                os.symlink(src, dst)
            except Exception:
                shutil.copy2(src, dst)
    return temp_root / 'factors' / factor_file.name


def _run_factor(loop_dir: Path, factor_name: str, cutoff: str | None) -> pd.DataFrame:
    factor_file = loop_dir / 'factors' / f'{factor_name}.py'
    if not factor_file.exists():
        raise FileNotFoundError(f'missing factor script: {factor_file}')
    with tempfile.TemporaryDirectory(prefix='qe_factor_dyn_', dir=str(Path.cwd() / '.codex_tmp')) as td:
        root = Path(td)
        script_path = _prepare_workspace(loop_dir, factor_file, root)
        cutoff_ts = pd.Timestamp(cutoff) if cutoff else None
        orig_read_hdf = pd.read_hdf
        orig_read_parquet = pd.read_parquet
        def read_hdf_pit(*args, **kwargs):
            df = orig_read_hdf(*args, **kwargs)
            return _filter_pit(df, cutoff_ts) if cutoff_ts is not None else df
        def read_parquet_pit(*args, **kwargs):
            df = orig_read_parquet(*args, **kwargs)
            return _filter_pit(df, cutoff_ts) if cutoff_ts is not None else df
        old_cwd = Path.cwd()
        try:
            os.chdir(root)
            pd.read_hdf = read_hdf_pit
            pd.read_parquet = read_parquet_pit
            runpy.run_path(str(script_path), run_name='__main__')
        finally:
            pd.read_hdf = orig_read_hdf
            pd.read_parquet = orig_read_parquet
            os.chdir(old_cwd)
        res = pd.read_hdf(_pick_result_path(root, factor_name), key='data')
        if isinstance(res, pd.Series):
            res = res.to_frame(factor_name)
        return res.sort_index()


def _compare_factor(loop_dir: Path, factor_name: str, cutoff: str, max_dates: int) -> dict[str, Any]:
    full = _run_factor(loop_dir, factor_name, None)
    trunc = _run_factor(loop_dir, factor_name, cutoff)
    cutoff_ts = pd.Timestamp(cutoff)
    if not isinstance(full.index, pd.MultiIndex) or not isinstance(trunc.index, pd.MultiIndex):
        raise ValueError(f'{factor_name}: result index must be MultiIndex')
    full_dates = pd.Index(pd.to_datetime(full.index.get_level_values('datetime')).unique()).sort_values()
    test_dates = [d for d in full_dates if d <= cutoff_ts][-max_dates:]
    rows=[]
    max_abs=0.0
    mismatch=0
    compared=0
    col = factor_name if factor_name in full.columns else full.columns[0]
    tcol = factor_name if factor_name in trunc.columns else trunc.columns[0]
    for d in test_dates:
        f = full.loc[pd.IndexSlice[d, :], col].dropna() if d in full.index.get_level_values('datetime') else pd.Series(dtype=float)
        t = trunc.loc[pd.IndexSlice[d, :], tcol].dropna() if d in trunc.index.get_level_values('datetime') else pd.Series(dtype=float)
        common = f.index.intersection(t.index)
        if len(common) == 0:
            rows.append({'date': str(pd.Timestamp(d).date()), 'common': 0, 'max_abs_diff': None, 'mismatch': None})
            continue
        diff = (f.loc[common].astype(float) - t.loc[common].astype(float)).abs()
        local_max = float(diff.max()) if len(diff) else 0.0
        local_mismatch = int((diff > 1e-10).sum())
        rows.append({'date': str(pd.Timestamp(d).date()), 'common': int(len(common)), 'max_abs_diff': local_max, 'mismatch': local_mismatch})
        max_abs=max(max_abs, local_max); mismatch += local_mismatch; compared += len(common)
    return {'factor': factor_name, 'cutoff': cutoff, 'dates': rows, 'compared': compared, 'mismatch': mismatch, 'max_abs_diff': max_abs, 'status': 'PASS' if compared > 0 and mismatch == 0 else 'FAIL'}


def _table(rows, headers):
    widths=[len(h) for h in headers]
    for row in rows:
        for i,c in enumerate(row): widths[i]=max(widths[i], len(str(c)))
    out=['  '.join(h.ljust(widths[i]) for i,h in enumerate(headers))]
    out.append('  '.join('-'*w for w in widths))
    out += ['  '.join(str(c).ljust(widths[i]) for i,c in enumerate(row)) for row in rows]
    return '\n'.join(out)


def _fmt(v):
    if v is None: return 'NA'
    try: return f'{float(v):.3e}'
    except Exception: return str(v)


def write_md(result: dict[str, Any], output: Path):
    lines=[f"# QE Dynamic Factor Truncation Audit: {result['task_id']}", '']
    lines.append('Scope: targeted dynamic no-future-leakage check. Factor scripts are run in a temp workspace; source workspaces are not modified.')
    lines.append('')
    rows=[]
    for r in result['results']:
        rows.append([r['loop'], r['factor'], r['cutoff'], r['compared'], r['mismatch'], _fmt(r['max_abs_diff']), r['status']])
    lines += ['## P0 Dynamic Truncation Summary', '', '```text', _table(rows, ['Loop','Factor','Cutoff','Compared','Mismatch','MaxAbsDiff','Status']), '```', '']
    detail=[]
    for r in result['results']:
        for d in r['dates']:
            detail.append([r['loop'], r['factor'], d['date'], d['common'], _fmt(d['max_abs_diff']), d['mismatch']])
    lines += ['## P0 Date-Level Details', '', '```text', _table(detail, ['Loop','Factor','Date','Common','MaxAbsDiff','Mismatch']), '```', '']
    lines += ['## Evidence Notes', '', '- PASS means the selected factor values on audited dates are identical after input data is truncated to the cutoff.', '- This targeted dynamic audit complements the static leakage scan; it is not a full recompute of every factor unless all factors are explicitly requested.', '']
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text('\n'.join(lines), encoding='utf-8')


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('task_id')
    ap.add_argument('--workspace', default=None)
    ap.add_argument('--loops', default='19')
    ap.add_argument('--factors', required=True, help='comma-separated factor names without .py')
    ap.add_argument('--cutoff', default='2025-12-31')
    ap.add_argument('--max-dates', type=int, default=3)
    ap.add_argument('--output-json', required=True)
    ap.add_argument('--output-md', required=True)
    args=ap.parse_args()
    workspace=Path(args.workspace) if args.workspace else Path('/mnt/f/Dev/RD-Agent-main/qe_workspace') / args.task_id
    factors=[f.strip().removesuffix('.py') for f in args.factors.split(',') if f.strip()]
    results=[]
    for loop in _parse_loops(args.loops):
        loop_dir=workspace/f'Loop{loop}'
        for factor in factors:
            r=_compare_factor(loop_dir, factor, args.cutoff, args.max_dates)
            r['loop']=loop
            results.append(r)
    result={'task_id': args.task_id, 'workspace': str(workspace), 'results': results}
    out=Path(args.output_json); out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    write_md(result, Path(args.output_md))
    print(f'wrote {out}')
    print(f'wrote {args.output_md}')

if __name__ == '__main__':
    main()
