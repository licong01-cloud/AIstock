"""No-runner opening, immutable retry and fail-closed source contracts."""

from copy import deepcopy
from datetime import date
from pathlib import Path

import pytest

from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import CostContract
from backend.services.position_timing.intraday.store import IntradayError
from backend.services.simulation_execution.localsim import natural_period_preparation as natural
from backend.services.simulation_execution.localsim import period_continuation as period
from backend.services.simulation_execution.localsim.timing_research import ref
from backend.services.simulation_execution.localsim.timing_store import publish, read_json


def put(path, value):
    publish(path, value)
    return ref(path)


@pytest.fixture
def cost():
    return CostContract(open_rate='0.0003', close_rate='0.0013', minimum_cny='5')


def test_native_settlement_preserves_cash_quantity_cost_and_input(cost):
    day = {'cash_cny': '123.45', 'positions': {'600000.SH': {
        'portfolio_id': 'parent', 'symbol': '600000.SH', 'quantity': 200,
        'available_quantity': 100, 'avg_cost': 12.345, 'trade_date': '2026-09-30'}}}
    before = deepcopy(day)
    positions = natural.settled_positions(day, 'child', '2026-10-08', cost)
    assert positions['600000.SH'] == {**day['positions']['600000.SH'],
                                        'portfolio_id': 'child', 'available_quantity': 200}
    assert day == before


@pytest.fixture
def prepared(tmp_path, cost):
    """Prepared-contract fixture only; no strategy or account execution."""
    dates = [str(date(2026, 10, d)) for d in (8, 9, 12, 13, 14, 15, 16, 19, 20, 21, 22, 23, 26, 27, 28, 29, 30)]
    inventory = put(tmp_path/'sources/inventory.json', {'days': [
        {'trade_date': d, 'target_count': 50, 'targets': [{'symbol': '600000.SH'}]} for d in dates]})
    cost_ref = put(tmp_path/'sources/cost.json', cost.model_dump(mode='json'))
    parents, summaries, values = [], [], []
    operation = {'operation_mode': 'HUMAN_SIM'}
    for arm, rule in zip(natural.ARMS, natural.RULES, strict=True):
        aid = 'parent_' + arm
        receipt = put(tmp_path/f'sources/{arm}_receipt.json', {'fixture': 'no economic execution'})
        day = {'account_id': aid, 'cash_cny': '123.45', 'positions': {
            '600000.SH': {'portfolio_id': aid, 'symbol': '600000.SH', 'quantity': 200,
                          'available_quantity': 100, 'avg_cost': 12.345, 'trade_date': '2026-09-30'}},
            'rule': rule, 'delay_seconds': 60, 'operation': operation, 'account_receipt': receipt,
            'terminal_as_of': '2026-09-30T14:57:00+08:00', 'terminal_nav_cny': '3123.45',
            'marks_raw_cny': {'600000.SH': '15'}}
        day_ref = put(tmp_path/f'sources/{arm}_day.json', day)
        parents.append({'arm_id': arm, 'account_id': aid, 'days': [day_ref]*181})
        summaries.append({'arm_id': arm, 'account_id': aid, 'rule': rule, 'delay_seconds': 60,
                          'operation': operation, 'origin_initial_cash_cny': '10000000',
                          'terminal_nav_cny': '3123.45', 'nav_high_water_cny': '11000000',
                          'cumulative_max_drawdown': '0.3', 'cumulative_fees_cny': '100'})
        values.append((day, day_ref))
    report = put(tmp_path/'sources/report.json', {'account_rows': summaries})
    spec = put(tmp_path/'sources/spec.json', {'analysis_spec': report, 'analysis_report': report})
    request = {'schema_version': natural.SCHEMA, 'parent_accounts': parents,
               'diagnostic_spec': spec, 'cost_policy': cost_ref, 'trade_dates': dates,
               'selection_inventory': inventory, 'executable_study_request': False,
               'account_count': 4, 'primary_delay_seconds': 60, 'decision_ttl_seconds': 90,
               'common_terminal_local': '14:57', 'wait_budget_minutes': 15,
               'price_research_threshold_fraction': '0.005'}
    identity = digest(request)
    request['identity'] = identity
    root = tmp_path/'outputs'/identity
    request_ref = put(root/'preparation_request.json', request)
    opening_refs = []
    for parent, summary, (day, day_ref) in zip(parents, summaries, values, strict=True):
        aid = 'lsnatural_' + digest([identity, parent['account_id']])[:40]
        opening = {'schema_version': 'localsim_natural_period_opening_v1', 'preparation_identity': identity,
                   'arm_id': parent['arm_id'], 'account_id': aid, 'parent_account_id': parent['account_id'],
                   **{k: day[k] for k in ('rule', 'delay_seconds', 'operation')},
                   'parent_day': day_ref, 'parent_account_receipt': day['account_receipt'],
                   'parent_state': {k: day[k] for k in ('cash_cny', 'positions')},
                   'parent_terminal_as_of': day['terminal_as_of'],
                   'new_capital_added_cny': '0', 'cash_cny': day['cash_cny'],
                   'positions': natural.settled_positions(day, aid, dates[0], cost),
                   'opening_nav_cny': day['terminal_nav_cny'], 'opening_marks_raw_cny': day['marks_raw_cny'],
                   'origin_initial_cash_cny': '10000000', 'period_start': dates[0], 'available_quantity_as_of': dates[0],
                   'analysis_anchor': {'parent_results': report, 'parent_account_id': parent['account_id'],
                       'parent_terminal_nav_cny': '3123.45', 'origin_initial_cash_cny': '10000000',
                       'prior_nav_high_water_cny': '11000000', 'prior_cumulative_max_drawdown': '0.3',
                       'prior_cumulative_fees_cny': '100', 'parent_nav_days_verified': 181, 'account_days_recomputed': 0}}
        opening_refs.append(put(root/f'opening_accounts/{parent["arm_id"]}.json', opening))
    protected = [cost_ref, inventory, report, spec, *(v[1] for v in values)]
    inspection = put(root/'source_inspection.json', {'stage': 'COMPLETE_SOURCE_BATCH_INSPECTED', 'protected_inputs': protected})
    market = put(root/'market.json', {'blockers': []})
    sell = put(root/'sell.json', {'historical_sell_symbols': ['600000.SH']})
    reuse = put(root/'reuse.json', {'fixture': 'not an economic compatibility assertion'})
    p = {'schema_version': natural.PREFLIGHT, 'identity': identity, 'preparation_request': request_ref,
         'source_inspection': inspection, 'initial_accounts': opening_refs, 'sell_union': sell,
         'market_source_preflight': market, 'reuse_evidence': reuse, 'trade_dates': dates, 'native_target_count': 850,
         'blockers': [], 'status': 'WAITING_PRICE_PLANS', 'executable_study_request': False,
         'replay_started': False, 'new_period_outcomes_read': False, 'account_days_recomputed': 0, 'new_economic_commits': 0}
    p_ref = put(root/'preflight.json', p)
    put(root/'preparation_receipt.json', {'schema_version': 'localsim_natural_period_preparation_receipt_v1',
                                        'identity': identity, 'preflight': p_ref, 'preparation_request': request_ref})
    return FileRef.model_validate(p_ref), root


def test_exact_retry_never_enters_source_rebuild_market_or_runner(prepared, monkeypatch):
    source, root = prepared

    def forbidden(*args, **kwargs):
        pytest.fail('Exact retry attempted preparation or source/market recomputation')

    monkeypatch.setattr(natural, 'prepare', forbidden)
    monkeypatch.setattr(natural, 'CandidateReader', forbidden)
    monkeypatch.setattr(natural.diagnostics, 'source_preflight', forbidden)
    before = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob('*.json')}
    result = natural.exact_retry(source)
    assert result['status'] == 'EXACT_PREPARATION_ARTIFACT_RETRY_VERIFIED'
    assert result['account_days_recomputed'] == result['new_economic_commits'] == 0
    assert before == {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob('*.json')}


@pytest.mark.parametrize('target', ['source', 'opening', 'receipt'])
def test_inspect_rejects_mutated_frozen_bytes(prepared, target):
    source, root = prepared
    p = read_json(Path(source.path))
    path = {'source': root.parents[1]/'sources/cost.json',
            'opening': Path(p['initial_accounts'][0]['path']), 'receipt': root/'preparation_receipt.json'}[target]
    path.write_text('{}', encoding='utf-8')
    with pytest.raises((ValueError, KeyError, IntradayError)):
        natural.inspect(source)


def test_no_partial_preparation_success_without_commit_marker(prepared):
    source, root = prepared
    (root/'preparation_receipt.json').unlink()
    with pytest.raises((FileNotFoundError, ValueError, IntradayError)):
        natural.inspect(source)


def test_reuse_requires_full_state_and_contract_even_if_nav_matches(prepared):
    source, _ = prepared
    p = read_json(Path(source.path))
    a = read_json(Path(p['initial_accounts'][0]['path']))
    old = deepcopy(a)
    old['account_id'] = 'old'
    old['positions']['600000.SH']['portfolio_id'] = 'old'
    assert natural.opening_state(a) == natural.opening_state(old)
    old['positions']['600000.SH']['avg_cost'] += 0.01
    assert natural.opening_state(a) != natural.opening_state(old)
    assert a['opening_nav_cny'] == old['opening_nav_cny']


def test_delivery_hash_failure_precedes_any_history_economic_read(tmp_path, monkeypatch):
    bad = FileRef(path=str(tmp_path/'missing.json'), sha256='0'*64)
    monkeypatch.setattr(natural.diagnostics, 'source_preflight', lambda *_: pytest.fail('premature economic read'))
    with pytest.raises((FileNotFoundError, ValueError, IntradayError)):
        natural.prepare(delivery_ref=bad, old_period_spec_ref=bad, output_root=tmp_path/'out', producer_commit='1'*40)
    assert not (tmp_path/'out').exists()


@pytest.mark.parametrize('schema', ['localsim_wait_budget_day_v1', 'localsim_adverse_wait_day_v1'])
def test_explicit_history_schema_adapter_retains_strict_default(schema):
    checkpoint = {'schema_version': 'localsim_timing_portfolio_day_account_v1',
                  'cash_cny': '123.45', 'positions': {'SH600000': {'quantity': 100, 'avg_cost': 12.3}}}
    exported = {**deepcopy(checkpoint), 'schema_version': schema, 'export_metadata': 'preserved'}
    assert not period._checkpoint_account_matches(exported, checkpoint)
    assert period._checkpoint_account_matches(exported, checkpoint, schema)
    assert not period._checkpoint_account_matches(exported, checkpoint, 'unapproved_schema')
    assert period._checkpoint_account_matches(checkpoint, checkpoint)


@pytest.mark.parametrize('field', ['cash_cny', 'positions', 'schema_version'])
def test_history_schema_adapter_never_masks_economic_or_native_contract_difference(field):
    native = {'schema_version': 'localsim_timing_portfolio_day_account_v1',
              'cash_cny': '123.45', 'positions': {'SH600000': {'quantity': 100, 'avg_cost': 12.3}}}
    exported = {**deepcopy(native), 'schema_version': 'localsim_wait_budget_day_v1'}
    native[field] = {'cash_cny': '123.46', 'positions': {}, 'schema_version': 'unknown_native_schema'}[field]
    assert not period._checkpoint_account_matches(exported, native, exported['schema_version'])
