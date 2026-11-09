"""Prepare four real period openings and source coverage; never execute accounts.

This separate preparation contract deliberately cannot be submitted to the old
13-account period runner or the common-opening three-account consumer.
"""

from datetime import date
from decimal import Decimal
import hashlib
from pathlib import Path
import re

from backend.services.position_timing.intraday import adverse_wait_diagnostics as diagnostics
from backend.services.position_timing.intraday import continuous_results as continuous
from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import CostContract
from backend.services.position_timing.intraday.research_inputs import CandidateReader
from backend.services.trading_core.ledger import InMemoryLedger
from backend.services.trading_core.models import PositionLot

from . import period_continuation as period
from .timing_research import fee_model, ref
from .timing_store import publish

SCHEMA = "localsim_natural_period_preparation_v1"
PREFLIGHT = "localsim_natural_period_preflight_v1"
ARMS = ("B0", "W15", "C2", "P2")
RULES = ("FIRST_OBSERVABLE_V1", "WAIT_BUDGET_CONTROL_15M_V1",
         "BOUNDED_REVERSION_V1", "ADVERSE_TREND_WAIT_15M_V1")


def require(condition, reason):
    if not condition:
        raise ValueError("LOCALSIM_NATURAL_PERIOD_" + reason)


def transport_ref(raw):
    """Resolve Windows delivery locators in WSL without altering frozen bytes."""
    source = FileRef.model_validate(raw)
    path = source.path
    if Path('/mnt').is_dir() and re.match(r'^[A-Za-z]:[\\/]', path):
        path = '/mnt/' + path[0].lower() + '/' + path[3:].replace('\\', '/')
    return FileRef(path=path, sha256=source.sha256)


def delivery_sources(source):
    delivery = period._read(source)
    require(delivery['schema_version'] == 'position_timing_mechanism_continuation_source_delivery_v1', 'DELIVERY_SCHEMA')

    def collect(value):
        if isinstance(value, dict):
            if {'path', 'sha256'} <= value.keys():
                yield transport_ref({'path': value['path'], 'sha256': value['sha256']})
            else:
                for child in value.values():
                    yield from collect(child)
        elif isinstance(value, list):
            for child in value:
                yield from collect(child)

    pins = period._verify_refs([source, *collect(delivery)])
    return delivery, pins


def settled_positions(day, account_id, start, cost):
    ledger = InMemoryLedger(portfolio_id=account_id, initial_cash=Decimal('10000000'), fee_model=fee_model(cost))
    ledger.cash = Decimal(day['cash_cny'])
    ledger.positions = {s: PositionLot.model_validate({**p, 'portfolio_id': account_id})
                        for s, p in day['positions'].items()}
    ledger.settle_trade_date(date.fromisoformat(start))
    return {s: p.model_dump(mode='json') for s, p in ledger.positions.items()}


def opening_state(opening):
    """Only account namespace differs; costs, quantities and lot dates stay exact."""
    return {**{k: opening[k] for k in ('cash_cny', 'opening_nav_cny', 'opening_marks_raw_cny',
                                     'origin_initial_cash_cny', 'rule', 'delay_seconds', 'operation')},
            'positions': {s: {k: v for k, v in p.items() if k != 'portfolio_id'}
                          for s, p in opening['positions'].items()},
            'analysis_anchor': {k: opening['analysis_anchor'][k] for k in
                                ('parent_terminal_nav_cny', 'prior_nav_high_water_cny',
                                 'prior_cumulative_max_drawdown', 'prior_cumulative_fees_cny')}}


def reuse_evidence(openings, old_openings, plan, old_preparation, old_preflight, old_facts, inventory):
    rows = []
    for opening in openings:
        if opening['arm_id'] not in ('B0', 'C2'):
            continue
        matches = [a for a in old_openings if a['rule'] == opening['rule'] and a['delay_seconds'] == 60]
        require(len(matches) == 1, 'OLD_PROFILE_AMBIGUOUS')
        old = matches[0]
        state = opening_state(opening) == opening_state(old)
        checks = {
            'own_opening_state_cost_and_anchors': state,
            'execution_queue_quantity_contract': plan['execution_contract'] == old_preparation['execution_contract'],
            'fee_file': plan['cost_policy'] == old_preflight['inputs']['cost_policy'],
            'native_selection_inventory': inventory == period._read(old_preflight['selection_inventory']),
            'ordered_trade_dates': [d['trade_date'] for d in inventory['days']] == old_preflight['trade_dates'],
            'candidate': old_preparation['candidate_manifest'] == old_preflight['inputs']['candidate_manifest'],
            'ttl_terminal_delay': old_preparation['decision_ttl_seconds'] == 90
                and old_preparation['common_terminal_local'] == '14:57'
                and old_preparation['primary_delay_seconds'] == 60,
        }
        account = next(a for a in old_facts['accounts'] if a['account_id'] == old['account_id'])
        ordered = [period._read(source) for source in account['days']]
        period._verify_refs([*period._refs(ordered)])
        checks['ordered_account_day_identity'] = all(
            row['trade_date'] == stamp and all(row[k] == old[k] for k in ('account_id', 'rule', 'delay_seconds', 'operation'))
            for stamp, row in zip(old_preflight['trade_dates'], ordered, strict=True))
        rows.append({'arm_id': opening['arm_id'], 'status': 'EXACT_COMPATIBLE_READ_ONLY_REUSE' if all(checks.values())
                     else 'INCOMPATIBLE_DO_NOT_REUSE', 'checks': checks,
                     'old_account_id': old['account_id'], 'parent_day': old['parent_day'],
                     'new_parent_day': opening['parent_day'], 'old_opening_state_sha256': digest(opening_state(old)),
                     'own_opening_state_sha256': digest(opening_state(opening)), 'ordered_days': account['days'],
                     'source_refs': [r.model_dump() for r in period._refs(ordered)],
                     'account_namespace_remapping_only': True, 'account_days_recomputed': 0})
    return {'schema_version': 'localsim_natural_period_reuse_evidence_v1', 'accounts': rows,
            'common_b0_opening_three_arm_research_used': False, 'new_period_returns_compared': False}


def prepare(*, delivery_ref: FileRef, old_period_spec_ref: FileRef, output_root: Path, producer_commit: str):
    require(re.fullmatch('[a-f0-9]{40}', producer_commit), 'PRODUCER')
    delivery, delivery_pins = delivery_sources(delivery_ref)
    diagnostic_ref = transport_ref(delivery['spec'])
    spec = diagnostics.DiagnosticSpec.model_validate(period._read(diagnostic_ref))
    # Both public inspectors hash complete source batches before economic reads.
    plan, accounts, report, history_refs = diagnostics.source_preflight(spec)
    old_spec = continuous.PeriodAnalysisSpec.model_validate(period._read(old_period_spec_ref))
    old_p, old_r, old_openings, old_facts, old_refs = continuous.period_source_preflight(old_spec)
    inventory = period._read(old_p['selection_inventory'])
    days = [d['trade_date'] for d in inventory['days']]
    require(len(days) == 17 and days == sorted(set(days)) and days[0] == '2026-10-08'
            and days[-1] == '2026-10-30' and sum(d['target_count'] for d in inventory['days']) == 850, 'NATIVE_WINDOW')
    require(plan['trade_dates'][-1] == '2026-09-30' and len(plan['trade_dates']) == 181
            and len(accounts) == 4 and [a.arm_id for a in accounts] == list(ARMS), 'PARENT_POPULATION')
    cost = CostContract.model_validate(period._read(plan['cost_policy']))
    require(plan['cost_policy'] == old_p['inputs']['cost_policy']
            and plan['execution_contract'] == old_r['execution_contract'], 'PERIOD_CONTRACT_CHANGED')
    protected = period._verify_refs([*delivery_pins, diagnostic_ref, old_period_spec_ref, *history_refs, *old_refs])
    destination = Path(output_root).resolve()
    require(all(destination != Path(p['path']).resolve().parent and
                Path(p['path']).resolve().parent not in destination.parents for p in protected), 'OUTPUT_OVERLAPS_INPUT')
    request = {'schema_version': SCHEMA, 'producer': 'LOCALSIM', 'producer_source_commit': producer_commit,
               'implementation_sha256': hashlib.sha256(Path(__file__).read_bytes().replace(b'\r\n', b'\n')).hexdigest(),
               'source_delivery': delivery_ref.model_dump(), 'diagnostic_spec': diagnostic_ref.model_dump(),
               'old_period_analysis_spec': old_period_spec_ref.model_dump(), 'trade_dates': days,
               'candidate_manifest': old_p['inputs']['candidate_manifest'], 'parent_candidate_manifest': plan['candidate_manifest'],
               'cost_policy': plan['cost_policy'], 'execution_contract': plan['execution_contract'],
               'selection_inventory': old_p['selection_inventory'], 'package_id': old_p['package_id'],
               'manifest_sha256': old_p['manifest_sha256'], 'parent_accounts': [a.model_dump(mode='json') for a in accounts],
               'control': plan['control'], 'wait_budget_minutes': 15, 'price_research_threshold_fraction': '0.005',
               'primary_delay_seconds': 60, 'decision_ttl_seconds': 90, 'common_terminal_local': '14:57',
               'executable_study_request': False, 'account_count': 4, 'native_target_count': 850}
    identity = digest(request)
    request['identity'] = identity
    root = destination / identity
    openings = []
    for account, summary, rule in zip(accounts, report['account_rows'], RULES, strict=True):
        day_ref = account.days[-1].model_dump()
        day = period._read(day_ref)
        require(summary['arm_id'] == account.arm_id and summary['account_id'] == account.account_id
                and summary['rule'] == rule and day['delay_seconds'] == 60, 'PROFILE')
        normalized = {**summary, 'days': [r.model_dump() for r in account.days],
                      'fees_cny': summary['cumulative_fees_cny'], 'max_drawdown': summary['cumulative_max_drawdown']}
        parent = {'request': {'identity': day['study_identity'], 'package_id': plan['package_id'],
                             'initial_cash_cny': summary['origin_initial_cash_cny'], 'trade_dates': plan['trade_dates']},
                  'results': {'accounts': [normalized]}, 'results_ref': spec.analysis_report, 'cost': cost}
        endpoint = {'account_id': account.account_id, 'day': day_ref, 'account_receipt': day['account_receipt']}
        inherited = period._endpoint(parent, endpoint, {k: summary[k] for k in ('rule', 'delay_seconds', 'operation')}, '2026-09-30')
        anchor = period._nav_anchor(parent, inherited)
        require(Decimal(anchor['prior_nav_high_water_cny']) == Decimal(summary['nav_high_water_cny']), 'HIGH_WATER')
        aid = 'lsnatural_' + digest([identity, account.account_id])[:40]
        opening = {'schema_version': 'localsim_natural_period_opening_v1', 'preparation_identity': identity,
                   'arm_id': account.arm_id, 'account_id': aid, 'parent_account_id': account.account_id,
                   **{k: day[k] for k in ('rule', 'delay_seconds', 'operation')},
                   'parent_day': day_ref, 'parent_account_receipt': day['account_receipt'],
                   'parent_terminal_as_of': day['terminal_as_of'],
                   'parent_state': {k: day[k] for k in ('cash_cny', 'positions')},
                   'origin_initial_cash_cny': summary['origin_initial_cash_cny'], 'new_capital_added_cny': '0',
                   'cash_cny': day['cash_cny'], 'positions': settled_positions(day, aid, days[0], cost),
                   'opening_nav_cny': day['terminal_nav_cny'], 'opening_marks_raw_cny': day['marks_raw_cny'],
                   'period_start': days[0], 'available_quantity_as_of': days[0], 'analysis_anchor': anchor}
        openings.append(opening)
    held = {s for a in openings for s in a['positions']}
    targets = {t['symbol'] for d in inventory['days'] for t in d['targets']}
    reader = CandidateReader(FileRef.model_validate(request['candidate_manifest']))
    prior_reader = None
    try:
        prior_reader = CandidateReader(FileRef.model_validate(plan['candidate_manifest']))
        calendar = [v.date().isoformat() for v in reader.calendar('day')]
        require([d for d in calendar if days[0] <= d <= days[-1]] == days and
                max(d for d in calendar if d < days[0]) == '2026-09-30', 'CALENDAR')
        market = period._market_preflight(reader, inventory, held, '2026-09-30', prior_reader)
        reader.verify_pins()
        prior_reader.verify_pins()
    finally:
        reader.close()
        if prior_reader is not None:
            prior_reader.close()
    protected = period._verify_refs([*protected, *market['candidate_source_pins']])
    reuse = reuse_evidence(openings, old_openings, plan, old_r, old_p, old_facts, inventory)
    protected = period._verify_refs([*protected, *period._refs(reuse)])
    sell_union = {'schema_version': 'localsim_natural_period_sell_union_v1',
                  'accounts': [{'arm_id': a['arm_id'], 'parent_day': a['parent_day'],
                                'historical_sell_symbols': sorted(a['positions'])} for a in openings],
                  'historical_sell_symbols': sorted(held), 'october_target_symbols': sorted(targets),
                  'prior_only_sell_symbols': sorted(held - targets),
                  'old_13_historical_sell_symbols': old_p['historical_sell_symbols'],
                  'additional_historical_sell_symbols': sorted(held - set(old_p['historical_sell_symbols'])),
                  'price_plan_coverage': 'UNKNOWN_PENDING_TIMING_OWNER_SUPPLEMENT',
                  'missing_eligibility_semantics': 'UNKNOWN_NO_SUBSTITUTION'}
    for name, value in [('preparation_request', request), ('reuse_evidence', reuse), ('sell_union', sell_union),
                        ('market_source_preflight', market)]:
        publish(root / (name + '.json'), value)
    opening_refs = []
    for opening in openings:
        path = root / 'opening_accounts' / (opening['arm_id'] + '.json')
        publish(path, opening)
        opening_refs.append(ref(path))
    inspection = {'schema_version': 'localsim_natural_period_source_inspection_v1',
                  'stage': 'COMPLETE_SOURCE_BATCH_INSPECTED', 'protected_inputs': protected,
                  'old_period_facts': old_spec.facts.model_dump(), 'history_source_ref_count': len(history_refs),
                  'new_period_outcomes_read': False, 'account_days_recomputed': 0}
    publish(root / 'source_inspection.json', inspection)
    preflight = {'schema_version': PREFLIGHT, 'identity': identity, 'preparation_request': ref(root/'preparation_request.json'),
                 'source_inspection': ref(root/'source_inspection.json'), 'initial_accounts': opening_refs,
                 'selection_inventory': request['selection_inventory'], 'trade_dates': days, 'native_target_count': 850,
                 'sell_union': ref(root/'sell_union.json'), 'reuse_evidence': ref(root/'reuse_evidence.json'),
                 'market_source_preflight': ref(root/'market_source_preflight.json'), 'blockers': market['blockers'],
                 'status': 'BLOCKED_SOURCE_INPUTS' if market['blockers'] else 'WAITING_PRICE_PLANS',
                 'candidate_manifest': request['candidate_manifest'], 'parent_candidate_manifest': plan['candidate_manifest'],
                 'candidate_identity_changed': request['candidate_manifest'] != plan['candidate_manifest'],
                 'source_declarations': old_p['source_declarations'],
                 'pending_owner_contracts': ['FOUR_ARM_PERIOD_PRICE_PLAN_SUPPLEMENT', 'FOUR_ARM_PERIOD_CONSUMER'],
                 'minimum_interface_difference': {'preflight_schema': PREFLIGHT, 'account_count': 4,
                     'opening_schema': 'localsim_natural_period_opening_v1',
                     'shared_statistical_reducer': 'continuous_results.period_account_rows',
                     'old_13_validator_unchanged': True, 'executable_request_supplied': False},
                 'executable_study_request': False, 'replay_started': False, 'new_period_outcomes_read': False,
                 'account_days_recomputed': 0, 'new_economic_commits': 0}
    publish(root/'preflight.json', preflight)
    receipt = {'schema_version': 'localsim_natural_period_preparation_receipt_v1', 'identity': identity,
               'preflight': ref(root/'preflight.json'), 'preparation_request': ref(root/'preparation_request.json')}
    publish(root/'preparation_receipt.json', receipt)
    return receipt


def inspect(preflight_ref: FileRef):
    """Verify immutable preparation and pins, without market/account recomputation."""
    p = period._read(preflight_ref)
    r = period._read(p['preparation_request'])
    require(p['schema_version'] == PREFLIGHT and r['schema_version'] == SCHEMA and
            digest({k: v for k, v in r.items() if k != 'identity'}) == p['identity'] == r['identity'], 'IDENTITY')
    receipt_ref = ref(Path(preflight_ref.path).with_name('preparation_receipt.json'))
    require(period._read(receipt_ref) == {'schema_version': 'localsim_natural_period_preparation_receipt_v1',
            'identity': p['identity'], 'preflight': preflight_ref.model_dump(),
            'preparation_request': p['preparation_request']}, 'UNCOMMITTED')
    require(p['replay_started'] is False and p['executable_study_request'] is False and
            r['executable_study_request'] is False and p['new_period_outcomes_read'] is False and
            p['account_days_recomputed'] == p['new_economic_commits'] == 0, 'NO_REPLAY')
    inspection = period._read(p['source_inspection'])
    protected = period._verify_refs(inspection['protected_inputs'])
    require(inspection['stage'] == 'COMPLETE_SOURCE_BATCH_INSPECTED', 'SOURCE_INSPECTION')
    inventory = period._read(r['selection_inventory'])
    require(p['trade_dates'] == r['trade_dates'] == [d['trade_date'] for d in inventory['days']] and
            len(p['trade_dates']) == 17 and p['native_target_count'] == sum(d['target_count'] for d in inventory['days']) == 850,
            'NATIVE_WINDOW')
    openings = [period._read(a) for a in p['initial_accounts']]
    require([a['arm_id'] for a in openings] == list(ARMS) and r['account_count'] == 4 and
            [a['rule'] for a in openings] == list(RULES) and
            r['primary_delay_seconds'] == 60 and r['decision_ttl_seconds'] == 90 and
            r['common_terminal_local'] == '14:57' and r['wait_budget_minutes'] == 15 and
            r['price_research_threshold_fraction'] == '0.005', 'POPULATION')
    cost = CostContract.model_validate(period._read(r['cost_policy']))
    spec = diagnostics.DiagnosticSpec.model_validate(period._read(r['diagnostic_spec']))
    report = period._read(spec.analysis_report)
    for opening, parent, summary in zip(openings, r['parent_accounts'], report['account_rows'], strict=True):
        day = period._read(opening['parent_day'])
        require(opening['schema_version'] == 'localsim_natural_period_opening_v1' and
                opening['parent_day'] == parent['days'][-1] and opening['parent_account_id'] == parent['account_id'] and
                opening['parent_account_receipt'] == day['account_receipt'] and opening['preparation_identity'] == p['identity'] and
                opening['account_id'] == 'lsnatural_' + digest([p['identity'], parent['account_id']])[:40] and
                opening['new_capital_added_cny'] == '0' and opening['cash_cny'] == day['cash_cny'] and
                opening['parent_state'] == {k: day[k] for k in ('cash_cny', 'positions')} and
                opening['positions'] == settled_positions(day, opening['account_id'], p['trade_dates'][0], cost) and
                opening['opening_nav_cny'] == day['terminal_nav_cny'] and
                opening['opening_marks_raw_cny'] == day['marks_raw_cny'], 'OPENING_LINEAGE')
        anchor = opening['analysis_anchor']
        require(all(opening[k] == day[k] == summary[k] for k in ('rule', 'delay_seconds', 'operation')) and
                opening['arm_id'] == summary['arm_id'] == parent['arm_id'] and
                opening['origin_initial_cash_cny'] == anchor['origin_initial_cash_cny'] == summary['origin_initial_cash_cny'] and
                Decimal(opening['origin_initial_cash_cny']) == 10000000 and
                opening['period_start'] == opening['available_quantity_as_of'] == p['trade_dates'][0] and
                opening['parent_terminal_as_of'] == day['terminal_as_of'] and
                anchor['parent_results'] == spec.analysis_report.model_dump() and
                anchor['parent_account_id'] == parent['account_id'] == summary['account_id'] and
                anchor['parent_terminal_nav_cny'] == summary['terminal_nav_cny'] == opening['opening_nav_cny'] and
                anchor['prior_nav_high_water_cny'] == summary['nav_high_water_cny'] and
                anchor['prior_cumulative_max_drawdown'] == summary['cumulative_max_drawdown'] and
                anchor['prior_cumulative_fees_cny'] == summary['cumulative_fees_cny'] and
                anchor['parent_nav_days_verified'] == 181 and anchor['account_days_recomputed'] == 0, 'ANCHOR')
    union = period._read(p['sell_union'])
    require(union['historical_sell_symbols'] == sorted({s for a in openings for s in a['positions']}), 'SELL_UNION')
    market = period._read(p['market_source_preflight'])
    require(p['blockers'] == market['blockers'] and p['status'] ==
            ('BLOCKED_SOURCE_INPUTS' if market['blockers'] else 'WAITING_PRICE_PLANS'), 'SOURCE_STATUS')
    period._read(p['reuse_evidence'])
    pins = period._verify_refs([preflight_ref, receipt_ref, *period._refs(p), *protected])
    return {'schema_version': 'localsim_natural_period_inspection_v1', 'status': 'PREPARATION_INSPECTED',
            'preflight': preflight_ref.model_dump(), 'preparation_receipt': receipt_ref,
            'source_ref_count': len(pins), 'account_count': 4, 'trade_day_count': 17, 'native_target_count': 850,
            'account_days_recomputed': 0, 'new_economic_commits': 0, 'replay_started': False}


def exact_retry(preflight_ref: FileRef):
    result = inspect(preflight_ref)
    return {**result, 'status': 'EXACT_PREPARATION_ARTIFACT_RETRY_VERIFIED',
            'immutable_artifacts_reused': True, 'market_inputs_recomputed': False,
            'account_days_recomputed': 0, 'new_economic_commits': 0}
