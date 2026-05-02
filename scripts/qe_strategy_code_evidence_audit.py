#!/usr/bin/env python
"""QE strategy code evidence audit.

Read-only audit of daily strategy, V25 minute execution, tail substitute logic,
and Qlib cost accounting code paths. Run in WSL/rdagent-gpu when Qlib source
inspection is required.
"""

from __future__ import annotations

import argparse
import inspect
import json
import re
from pathlib import Path
from typing import Any

import yaml


def _parse_loops(value: str) -> list[int]:
    out: list[int] = []
    for part in value.split(','):
        part = part.strip()
        if not part:
            continue
        if '-' in part:
            a, b = part.split('-', 1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return sorted(dict.fromkeys(out))


def _read_yaml_lenient(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding='utf-8')
    text = re.sub(r"\{\{\s*[^{}]+\s*\}\}", "0", text)
    return yaml.safe_load(text) or {}


def _line_hits(path: Path, patterns: dict[str, str]) -> dict[str, Any]:
    text = path.read_text(encoding='utf-8', errors='replace') if path.exists() else ''
    lines = text.splitlines()
    out: dict[str, Any] = {}
    for key, pattern in patterns.items():
        rgx = re.compile(pattern)
        hits = []
        for idx, line in enumerate(lines, start=1):
            if rgx.search(line):
                hits.append({"line": idx, "text": line.strip()[:220]})
        out[key] = {"present": bool(hits), "hits": hits[:5]}
    return out


def _first_line(hit: dict[str, Any]) -> str:
    hits = hit.get('hits') or []
    if not hits:
        return 'NA'
    return str(hits[0]['line'])


def _qlib_source_evidence() -> dict[str, Any]:
    import qlib.backtest as qb
    from qlib.backtest.executor import SimulatorExecutor, NestedExecutor, BaseExecutor
    from qlib.backtest.exchange import Exchange
    from qlib.backtest.account import Account
    from qlib.backtest.position import Position

    objects = {
        'get_strategy_executor': qb.get_strategy_executor,
        'get_exchange': qb.get_exchange,
        'NestedExecutor': NestedExecutor,
        'SimulatorExecutor': SimulatorExecutor,
        'BaseExecutor': BaseExecutor,
        'Exchange': Exchange,
        'Account': Account,
        'Position': Position,
    }
    patterns = {
        'get_strategy_executor': {
            'exchange_kwargs_passed': r'trade_exchange\s*=\s*get_exchange\(\*\*exchange_kwargs\)',
        },
        'get_exchange': {
            'exchange_cost_kwargs': r'open_cost=open_cost|close_cost=close_cost|min_cost=min_cost',
        },
        'NestedExecutor': {
            'inner_account_copy': r'inner_executor\.reset_common_infra\(common_infra,\s*copy_trade_account=True\)',
        },
        'SimulatorExecutor': {
            'deal_order_call': r'trade_exchange\.deal_order\(',
        },
        'BaseExecutor': {
            'port_metric_reset': r'port_metr_enabled=self\.generate_portfolio_metrics',
        },
        'Exchange': {
            'trade_cost_calc': r'trade_cost\s*=\s*max\(trade_val \* cost_ratio, self\.min_cost\)',
            'open_cost_ratio': r'cost_ratio\s*=\s*self\.open_cost',
            'close_cost_ratio': r'cost_ratio\s*=\s*self\.close_cost',
        },
        'Account': {
            'cost_metric_guard': r'if self\.is_port_metr_enabled\(\):',
            'position_update_buy': r'self\.current_position\.update_order\(order, trade_val, cost, trade_price\)',
        },
        'Position': {
            'buy_subtracts_cost': r'cash"\]\s*-=',
            'sell_subtracts_cost': r'new_cash\s*=\s*trade_val - cost',
        },
    }
    out: dict[str, Any] = {}
    for name, obj in objects.items():
        file_path = inspect.getsourcefile(obj) or ''
        source_lines, start_line = inspect.getsourcelines(obj)
        source = ''.join(source_lines)
        item = {'file': file_path, 'start_line': start_line, 'patterns': {}}
        for key, pattern in patterns.get(name, {}).items():
            hits = []
            rgx = re.compile(pattern)
            for offset, line in enumerate(source_lines):
                if rgx.search(line):
                    hits.append({'line': start_line + offset, 'text': line.strip()[:220]})
            item['patterns'][key] = {'present': bool(hits), 'hits': hits[:5]}
        out[name] = item
    return out


def audit_loop(workspace: Path, loop: int) -> dict[str, Any]:
    loop_dir = workspace / f'Loop{loop}'
    if not loop_dir.exists():
        raise FileNotFoundError(f'missing {loop_dir}')
    conf = _read_yaml_lenient(loop_dir / 'conf.yaml')
    pac = conf.get('port_analysis_config') or {}
    strategy = pac.get('strategy') or {}
    executor = pac.get('executor') or {}
    inner = (((executor.get('kwargs') or {}).get('inner_strategy') or {}))
    inner_exec = (((executor.get('kwargs') or {}).get('inner_executor') or {}))
    backtest = pac.get('backtest') or {}
    exchange_kwargs = backtest.get('exchange_kwargs') or {}

    custom_hits = _line_hits(loop_dir / 'custom_strategy.py', {
        'daily_signal_shift_1': r'get_step_time\(trade_step,\s*shift=1\)',
        'topk_ranking': r'ranked\.head\(self\.topk\)',
        'ghost_sell': r'ghost_sells\s*=\s*\[',
        'backup_candidates': r'_backup_candidates\s*=|backup_sids\s*=\s*ranked\.iloc\[self\.topk',
    })
    v25_hits = _line_hits(loop_dir / 'tail_twap_v25_strategy.py', {
        'raw_price_conversion': r'def _to_raw_price|return float\(adjusted_price\) / float\(factor\)',
        'factor_required': r'factor_missing_data_error|_is_valid_factor\(factor\)',
        'limit_buy_block': r'limit_up_buy_blocked',
        'limit_sell_block': r'limit_down_sell_blocked',
        'limit_price_basis_raw': r'price_basis=raw',
        'plan_weight_guard': r'V25 plan weight mismatch|EARLY_WEIGHT',
        'no_twap_fallback': r'refusing to fall back to TWAP',
        'tail_substitute_call': r'_do_realloc_substitute\(',
    })
    tail_hits = _line_hits(loop_dir / 'tail_twap_strategy.py', {
        'blocked_threshold': r'BLOCKED_FILL_THRESHOLD\s*=\s*0\.2',
        'substitute_method': r'def _do_realloc_substitute',
        'blocked_cash': r'blocked_cash \+= remain \* price',
        'topk_limit': r'topk - effective_count|max_new',
        'backup_candidate_use': r'backup_candidates',
        'fallback_to_realloc': r'self\._do_realloc\(trade_start_time, trade_end_time\)',
    })
    run_log = (loop_dir / 'run.log').read_text(encoding='utf-8', errors='replace') if (loop_dir / 'run.log').exists() else ''
    return {
        'loop': loop,
        'config': {
            'outer_strategy_class': strategy.get('class'),
            'topk': (strategy.get('kwargs') or {}).get('topk'),
            'n_drop': (strategy.get('kwargs') or {}).get('n_drop'),
            'risk_degree': (strategy.get('kwargs') or {}).get('risk_degree'),
            'inner_strategy_class': inner.get('class'),
            'inner_generate_portfolio_metrics': (inner_exec.get('kwargs') or {}).get('generate_portfolio_metrics'),
            'outer_generate_portfolio_metrics': (executor.get('kwargs') or {}).get('generate_portfolio_metrics'),
            'unfilled_handler': (inner.get('kwargs') or {}).get('unfilled_handler'),
            'unfilled_backup_depth': (inner.get('kwargs') or {}).get('unfilled_backup_depth'),
            'exchange_freq': exchange_kwargs.get('freq'),
            'open_cost': exchange_kwargs.get('open_cost'),
            'close_cost': exchange_kwargs.get('close_cost'),
            'min_cost': exchange_kwargs.get('min_cost'),
            'trade_unit': exchange_kwargs.get('trade_unit'),
        },
        'daily_code': custom_hits,
        'v25_code': v25_hits,
        'tail_code': tail_hits,
        'runlog': {
            'v25_plan_lines': len(re.findall(r'generated plan stock=', run_log)),
            'tail_substitute_config_mentions': len(re.findall(r'TAIL_SUBSTITUTE|_do_realloc_substitute', run_log)),
            'tail_substitute_event_lines': len(re.findall(r'tail[_ -]?substitute|substitute', run_log, flags=re.I)) - len(re.findall(r'TAIL_SUBSTITUTE|_do_realloc_substitute', run_log)),
            'market_state_lines': len(re.findall(r'market-state stock=', run_log)),
            'missing_data_error_lines': len(re.findall(r'missing_data_error', run_log, flags=re.I)),
        },
    }


def _fmt_bool(v: Any) -> str:
    return 'OK' if bool(v) else 'FAIL'


def _table(rows: list[list[str]], headers: list[str]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    out = ['  '.join(h.ljust(widths[i]) for i, h in enumerate(headers))]
    out.append('  '.join('-' * w for w in widths))
    out.extend('  '.join(str(c).ljust(widths[i]) for i, c in enumerate(row)) for row in rows)
    return '\n'.join(out)


def write_md(result: dict[str, Any], output: Path) -> None:
    loops = result['loops']
    lines = [f"# QE Strategy Code Evidence Audit: {result['task_id']}", '']
    lines.append('Scope: read-only code/config/source audit for daily strategy, V25 minute execution, tail substitute, and Qlib cost accounting path. No QE loop is rerun.')
    lines.append('')

    rows = []
    for a in loops:
        c = a['config']
        d = a['daily_code']
        rows.append([
            str(a['loop']), str(c['outer_strategy_class']), str(c['topk']), str(c['n_drop']),
            _fmt_bool(d['daily_signal_shift_1']['present']), _first_line(d['daily_signal_shift_1']),
            _fmt_bool(d['topk_ranking']['present']), _fmt_bool(d['ghost_sell']['present']),
            _fmt_bool(d['backup_candidates']['present']),
        ])
    lines += ['## P0 Daily Strategy Code Evidence', '', '```text', _table(rows, ['Loop', 'Strategy', 'TopK', 'NDrop', 'Shift1', 'Line', 'TopKRank', 'GhostSell', 'Backup']), '```', '']

    rows = []
    for a in loops:
        c = a['config']; v = a['v25_code']; r = a['runlog']
        rows.append([
            str(a['loop']), str(c['inner_strategy_class']), str(c['exchange_freq']),
            _fmt_bool(v['raw_price_conversion']['present']), _fmt_bool(v['factor_required']['present']),
            _fmt_bool(v['limit_price_basis_raw']['present']), _fmt_bool(v['plan_weight_guard']['present']),
            _fmt_bool(v['no_twap_fallback']['present']), str(r['v25_plan_lines']), str(r['missing_data_error_lines']),
        ])
    lines += ['## P0 V25 Minute Execution Code Evidence', '', '```text', _table(rows, ['Loop', 'InnerClass', 'Freq', 'RawPx', 'FactorReq', 'LimitRaw', 'PlanGuard', 'NoFallback', 'PlanLogs', 'DataErr']), '```', '']

    rows = []
    for a in loops:
        c = a['config']; t = a['tail_code']; r = a['runlog']
        rows.append([
            str(a['loop']), str(c['unfilled_handler']), str(c['unfilled_backup_depth']),
            _fmt_bool(t['blocked_threshold']['present']), _fmt_bool(t['substitute_method']['present']),
            _fmt_bool(t['blocked_cash']['present']), _fmt_bool(t['topk_limit']['present']),
            _fmt_bool(t['backup_candidate_use']['present']), _fmt_bool(t['fallback_to_realloc']['present']),
            str(r['tail_substitute_config_mentions']), str(max(r['tail_substitute_event_lines'], 0)),
        ])
    lines += ['## P0 Tail Substitute / Tail Buy Code Evidence', '', '```text', _table(rows, ['Loop', 'Mode', 'Depth', 'Block20', 'Method', 'CashCalc', 'TopKCap', 'BackupUse', 'BoostFallback', 'CfgLog', 'EventLog']), '```', '']

    q = result.get('qlib_source') or {}
    rows = []
    checks = [
        ('get_strategy_executor', 'exchange_kwargs_passed'),
        ('get_exchange', 'exchange_cost_kwargs'),
        ('SimulatorExecutor', 'deal_order_call'),
        ('Exchange', 'trade_cost_calc'),
        ('Account', 'cost_metric_guard'),
        ('Account', 'position_update_buy'),
        ('Position', 'buy_subtracts_cost'),
        ('Position', 'sell_subtracts_cost'),
        ('NestedExecutor', 'inner_account_copy'),
        ('BaseExecutor', 'port_metric_reset'),
    ]
    for obj, key in checks:
        item = q.get(obj, {})
        pat = (item.get('patterns') or {}).get(key, {})
        rows.append([obj, key, _fmt_bool(pat.get('present')), str((pat.get('hits') or [{'line': 'NA'}])[0]['line']), str(item.get('file', 'NA'))])
    lines += ['## P0 Qlib Cost Accounting Code Evidence', '', '```text', _table(rows, ['Object', 'Check', 'Status', 'Line', 'File']), '```', '']

    rows = []
    for a in loops:
        c = a['config']
        rows.append([
            str(a['loop']), str(c['open_cost']), str(c['close_cost']), str(c['min_cost']),
            str(c['outer_generate_portfolio_metrics']), str(c['inner_generate_portfolio_metrics']),
        ])
    lines += ['## P0 Cost Config vs Metric Recording Gate', '', '```text', _table(rows, ['Loop', 'OpenCost', 'CloseCost', 'MinCost', 'OuterPortMetric', 'InnerPortMetric']), '```', '']

    lines += [
        '## Evidence Notes',
        '',
        '- `Shift1=OK` proves the daily strategy reads the previous trading step signal, not same-day future scores.',
        '- `RawPx/FactorReq/LimitRaw=OK` proves V25 converts adjusted Qlib prices back to raw basis before comparing with raw limit/pre-close fields.',
        '- `BoostFallback=OK` means tail substitute code can fall back to proportional tail boost if no valid backup candidate is selected; current artifacts do not persist whether this branch fired.',
        '- Qlib source evidence proves costs are subtracted from `Position.cash`; because inner executor portfolio metrics are disabled, cost metrics are not accumulated into report cost columns.',
        '- `PlanLogs=0` and `EventLog=0` mean order-level V25 plan/no-fill/tail-substitute traces are not persisted in current run logs; `CfgLog` is only the startup config line, not execution evidence.',
        '',
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text('\n'.join(lines), encoding='utf-8')


def main() -> int:
    ap = argparse.ArgumentParser(description='QE strategy code evidence audit')
    ap.add_argument('task_id')
    ap.add_argument('--workspace', default=None)
    ap.add_argument('--loops', default='19-28')
    ap.add_argument('--output-json', required=True)
    ap.add_argument('--output-md', required=True)
    ap.add_argument('--skip-qlib-source', action='store_true')
    args = ap.parse_args()
    workspace = Path(args.workspace) if args.workspace else Path('/mnt/f/Dev/RD-Agent-main/qe_workspace') / args.task_id
    loops = [audit_loop(workspace, loop) for loop in _parse_loops(args.loops)]
    result = {'task_id': args.task_id, 'workspace': str(workspace), 'loops': loops}
    if not args.skip_qlib_source:
        result['qlib_source'] = _qlib_source_evidence()
    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    write_md(result, Path(args.output_md))
    print(f'wrote {out_json}')
    print(f'wrote {args.output_md}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
