"""Matched economic reports must retain missing and no-fill plans."""
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.research import ARMS, DELAYS, freeze, prepare, run
from backend.services.position_timing.intraday.research_results import EconomicResults, compare, summarize
from backend.services.position_timing.intraday.store import IntradayError, IntradayStore
from test_intraday_core import at
from test_intraday_research import study


def economic_case():
    cases, rows = [], []
    for arm in ARMS:
        for delay in DELAYS:
            for side in ('BUY', 'SELL'):
                case_id = digest([arm, delay, side])
                cases.append({'case_id': case_id, 'plan_id': side, 'original_plan_sha256': digest(side),
                              'symbol': '000001.SZ', 'trade_date': str(at().date()), 'side': side,
                              'target_quantity': 1000, 'policy_id': arm, 'delay_seconds': delay, 'applicable': True})
                baseline = arm == ARMS[0]
                rows.append({'case_id': case_id, 'initial_account_sha256': digest(side), 'initial_nav_cny': '20000',
                             'target_quantity': 1000, 'target_notional_cny': '10000', 'filled_quantity': 1000,
                             'average_price_raw': '10' if baseline else '9.9' if side == 'BUY' else '10.1',
                             'fees_cny': '5', 'terminal_as_of': at('14:57:00'),
                             'terminal_nav_cny': '20000' if baseline else '20100', 'max_drawdown': '0.10' if baseline else '0.08',
                             'mean_wait_seconds': '110', 'status': 'FILLED', 'reason_code': 'MATCHED_FILLED', 'account_receipt_sha256': 'a' * 64})
    results = EconomicResults(producer='LOCALSIM', producer_source_commit='1' * 40, release_identity='fixture-only',
                              study_manifest_sha256='b' * 64, cost_policy_file_sha256='c' * 64,
                              execution_contract_sha256='d' * 64, max_drawdown_definition='POSITIVE_PEAK_TO_TROUGH_FRACTION', rows=rows)
    return {'cases': cases}, results


def test_buy_sell_direction_and_account_lift_have_hand_calculated_values():
    consumption, results = economic_case()
    report = summarize(consumption, results)
    row = next(r for r in report['summaries'] if r['policy_id'] == ARMS[2] and r['delay_seconds'] == 60 and r['side'] == 'ALL')
    assert row['price_lift_bps']['equal_weighted'] == Decimal('100')
    assert row['net_lift_bps']['equal_weighted'] == Decimal('50')
    assert row['mdd_reduction']['equal_weighted'] == Decimal('0.02')


def test_missing_duplicate_and_mismatched_accounts_cannot_produce_comparison():
    consumption, results = economic_case()
    for rows in (results.rows[:-1], (*results.rows, results.rows[0])):
        with pytest.raises(IntradayError, match='POPULATION_MISMATCH'):
            summarize(consumption, results.model_copy(update={'rows': rows}))
    changed = results.rows[-1].model_copy(update={'initial_account_sha256': 'e' * 64})
    with pytest.raises(IntradayError, match='NOT_MATCHED'):
        summarize(consumption, results.model_copy(update={'rows': (*results.rows[:-1], changed)}))
    changed_delay = tuple(r.model_copy(update={'initial_nav_cny': Decimal('21000')})
                          if c['delay_seconds'] == 300 else r for c, r in zip(consumption['cases'], results.rows))
    with pytest.raises(IntradayError, match='NOT_MATCHED'):
        summarize(consumption, results.model_copy(update={'rows': changed_delay}))


def test_unavailable_is_visible_not_zero_and_no_fill_terminal_is_counted():
    consumption, results = economic_case()
    missing = results.rows[-1].model_copy(update={'status': 'UNAVAILABLE', 'terminal_nav_cny': None, 'max_drawdown': None})
    no_fill = results.rows[-2].model_copy(update={'status': 'NO_FILL', 'filled_quantity': 0, 'average_price_raw': None,
                                                'mean_wait_seconds': None, 'fees_cny': Decimal(0), 'terminal_nav_cny': Decimal('19000')})
    report = summarize(consumption, results.model_copy(update={'rows': (*results.rows[:-2], no_fill, missing)}))
    row = next(r for r in report['summaries'] if r['policy_id'] == ARMS[2] and r['delay_seconds'] == 300 and r['side'] == 'ALL')
    assert row['original_plan_count'] == 2 and row['status_counts'] == {'NO_FILL': 1, 'UNAVAILABLE': 1}
    assert row['net_lift_bps']['count'] == 1 and row['net_lift_bps']['equal_weighted'] == Decimal('-500')
    assert row['price_lift_bps']['count'] == 0


def test_comparison_binds_bundle_cost_and_inspects_sources_before_outcomes(tmp_path):
    bundle = run(prepare(study(tmp_path), tmp_path / 'artifacts')['request'])
    consumption = IntradayStore().read(Path(bundle['bundle']) / 'consumption.json')
    cases, template = economic_case()
    by_case = {r.case_id: r for r in template.rows}
    templates = {(c['policy_id'], c['delay_seconds'], c['side']): by_case[c['case_id']] for c in cases['cases']}
    rows = [templates[(c['policy_id'], c['delay_seconds'], c['side'])].model_copy(update={'case_id': c['case_id']}) for c in consumption['cases']]
    result = template.model_copy(update={'rows': tuple(rows), 'study_manifest_sha256': bundle['manifest_sha256'],
                                         'cost_policy_file_sha256': consumption['cost_policy']['sha256'],
                                         'execution_contract_sha256': consumption['execution_contract_sha256']})
    result_ref = freeze(tmp_path / 'results.json', result)
    first = compare(bundle['bundle'], result_ref)
    retry = compare(bundle['bundle'], result_ref)
    assert retry['status'] == 'ALREADY_MATERIALIZED' and retry['comparison_sha256'] == first['comparison_sha256']
    assert first['report']['outcomes_read_stage'] == 'AFTER_COMPLETE_SOURCE_AND_BUNDLE_INSPECTION'
    wrong_ref = freeze(tmp_path / 'wrong.json', result.model_copy(update={'cost_policy_file_sha256': 'e' * 64}))
    with pytest.raises(IntradayError, match='CONTRACT_IDENTITY_MISMATCH'):
        compare(bundle['bundle'], wrong_ref)
    (tmp_path / 'cost.json').write_text('{}', encoding='utf-8')
    (tmp_path / 'results.json').write_text('not readable outcomes', encoding='utf-8')
    with pytest.raises(IntradayError, match='SOURCE_IDENTITY_MISMATCH'):
        compare(bundle['bundle'], result_ref)
