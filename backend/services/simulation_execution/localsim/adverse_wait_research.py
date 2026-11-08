"""One immutable P2 account; frozen B0/W15/C2 facts are read-only comparators."""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import hashlib
from pathlib import Path
import re

from filelock import FileLock

from backend.services.position_timing.intraday.adverse_wait_control import (
    CONTROL_ID, AdverseWaitDecision, AdverseWaitSpec, AdverseWaitState,
)
from backend.services.position_timing.intraday.adverse_wait_research import inspect_adverse_plan
from backend.services.position_timing.intraday.contracts import Policy, digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import CostContract
from backend.services.position_timing.intraday.research_inputs import CandidateReader, checked_file
from backend.services.strategy_package.manifest import compute_manifest_json_sha256
from backend.services.strategy_package.models import StrategyPackageManifest

from . import dual_mode_research as dual
from . import period_research as period
from . import wait_budget_research as shared
from .adverse_wait_execution import release_evidence_from_account
from .research_performance import ResearchAccountPool, ResearchMarketWorkspace, ResearchPerformanceOptions
from .timing_campaign import inspect_price_templates
from .timing_research import ref
from .timing_store import publish

REQUEST_SCHEMA = 'localsim_adverse_wait_history_request_v1'
DAY_SCHEMA = 'localsim_adverse_wait_day_v1'
SCOPE = 'JAN_SEP_2026_P2_SINGLE_CASH_ORIGIN_181D_9050_TARGETS'
RELEASE_PREFIX = 'localsim_adverse_wait_v1_'
BOUND_FIELDS = ('common_opening', 'trade_dates', 'candidate_manifest', 'price_templates', 'cost_policy',
                'execution_contract', 'operation', 'selection_inventory', 'package_id', 'package_manifest_sha256',
                'baseline_facts', 'baseline_analysis_spec', 'base_history_plan')
read = shared.read


def require(condition, reason):
    if not condition:
        raise ValueError('LOCALSIM_ADVERSE_' + reason)


def implementation_identity():
    return {'native_account_engine': period.implementation_identity(), 'consumer': {
        name: hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n', b'\n')).hexdigest()
        for name in ('adverse_wait_execution.py', 'adverse_wait_research.py', 'wait_budget_research.py',
                     'wait_budget_execution.py')}}


def _file_refs(value):
    if isinstance(value, dict):
        if 'path' in value and 'sha256' in value:
            yield FileRef(path=value['path'], sha256=value['sha256'])
        for child in value.values():
            yield from _file_refs(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            yield from _file_refs(child)


def _flat(value, keys=()):
    if isinstance(value, dict):
        return {k: v for name, item in value.items() for k, v in _flat(item, (*keys, name)).items()}
    return {keys: value}


def _compatibility_body(plan, before_ref, after_ref):
    # Both whole artifacts are pinned before interpreting their functional economics.
    checked_file(before_ref)
    checked_file(after_ref)
    before, after = read(before_ref), read(after_ref)
    baseline, current = plan['baseline_shared_economic_implementation'], period.implementation_identity()
    require(before['native_account_engine'] == baseline and after['native_account_engine'] == current,
            'COMPATIBILITY_IMPLEMENTATION_CONFLICT')
    keys = ('scope', 'trade_dates', 'symbols', 'reaction_delay_seconds', 'initial_cash_cny', 'base_timing_source_commit')
    base = read(plan['base_history_plan'])['timing_source_commit']
    require(before['schema_version'] == after['schema_version'] == 'localsim_fixed_legacy_economic_smoke_v1'
            and all(before[k] == after[k] for k in keys)
            and before['scope'] == 'FIXED_SYNTHETIC_TWO_DAY_NATIVE_ACCOUNT_SMOKE_NOT_543_REPLAY'
            and before['trade_dates'] == ['2026-09-21', '2026-09-22']
            and before['symbols'] == ['000001.SZ', '600000.SH']
            and before['reaction_delay_seconds'] == 60 and before['initial_cash_cny'] == '15010'
            and before['base_timing_source_commit'] == base
            and set(before['accounts']) == {'B0', 'W15', 'C2'}
            and all(len(days) == 2 for days in before['accounts'].values())
            and before['accounts'] == after['accounts']
            and before['economic_payload_sha256'] == after['economic_payload_sha256'] == digest(before['accounts'])
            and before['historical_account_days_recomputed'] == after['historical_account_days_recomputed'] == 0
            and before['research_economics'] is after['research_economics'] is False,
            'LEGACY_SHORT_WINDOW_ECONOMICS_CONFLICT')
    left, right = _flat(baseline), _flat(current)
    differences = {key: {'baseline': left.get(key), 'current': right.get(key)}
                   for key in left.keys() | right.keys() if left.get(key) != right.get(key)}
    allowed = {('shared_account_engine', 'dual_mode', name)
               for name in ('dual_mode_research.py', 'human_portfolio.py')}
    for name in ('adverse_wait_control.py', 'adverse_wait_research.py'):
        key = ('shared_account_engine', 'shared_authorities',
               'backend/services/position_timing/intraday/' + name)
        allowed.add(key)
        require(right.get(key) == plan['implementation']['intraday/' + name], 'CONTROL_CLOSURE_CONFLICT')
    require(set(differences) <= allowed, 'SHARED_ECONOMICS_CHANGE_REQUIRES_OWNER')
    return {'schema_version': 'localsim_adverse_wait_economic_compatibility_v1',
        'research_plan_identity': plan['identity'], 'baseline_native_account_engine': baseline,
        'current_native_account_engine': current, 'before': before_ref.model_dump(), 'after': after_ref.model_dump(),
        'fixed_window': {key: before[key] for key in keys}, 'compared_account_days': 6,
        'economic_payload_sha256': digest(before['accounts']), 'economic_fields_equal': True,
        'implementation_differences': [{'path': list(k), **differences[k]} for k in sorted(differences)],
        'old_543_account_days_recomputed': 0, 'new_research_outcomes': False}


def freeze_compatibility(*, research_plan, before, after, output_path):
    body = _compatibility_body(read(research_plan), before, after)
    publish(output_path, body)
    return ref(output_path)


def _market_pins(plan, inspected):
    reader = CandidateReader(FileRef.model_validate(plan['candidate_manifest']))
    try:
        require([str(d.date()) for d in reader.calendar('day')
                 if plan['trade_dates'][0] <= str(d.date()) <= plan['trade_dates'][-1]] == plan['trade_dates'],
                'MARKET_CALENDAR_CONFLICT')
        reader.calendar('1min')
        for name in ('components/minute_bin_candidate/instruments/all.txt',
                     'components/suspend_d_daily_candidate_v2/suspend_d.parquet'):
            reader.file(name)
        symbols = sorted({s for source in inspected['days'].values() for s, _ in source['series']})
        for symbol in symbols:
            for group, frequency, fields in (('daily', 'day', CandidateReader.DAILY_FIELDS),
                                             ('minute', '1min', CandidateReader.MINUTE_FIELDS)):
                for field in fields:
                    reader.file(f'components/{group}_bin_candidate/features/{symbol.lower()}/{field}.{frequency}.bin')
        reader.verify_pins()
        return [FileRef(path=str(reader.root / name), sha256=pin['sha256']) for name, pin in sorted(reader.pins.items())]
    finally:
        reader.close()


def _sources(plan_ref, delivery_ref, compatibility_ref):
    verified = inspect_adverse_plan(plan_ref)
    require(verified['integrity'] == 'VERIFIED' and verified['p2_outcomes_read'] is False, 'PLAN_NOT_VERIFIED')
    plan, delivery = verified['plan'], read(delivery_ref)
    require(delivery['schema_version'] == 'position_timing_adverse_wait_source_delivery_v1'
            and delivery['research_plan'] == plan_ref.model_dump()
            and all(delivery[k] == plan[k] for k in ('base_history_plan', 'baseline_analysis_spec', 'baseline_facts',
                                                      'common_opening', 'candidate_manifest', 'price_templates')),
            'DELIVERY_BINDING_CONFLICT')
    base = read(plan['base_history_plan'])
    parent = read(base['source_spec']['request'])
    original = read(parent['source_request'])
    inspected = inspect_price_templates(FileRef.model_validate(original['preflight']),
                                        FileRef.model_validate(plan['price_templates']))
    p, common = inspected['preflight'], read(plan['common_opening'])
    control = AdverseWaitSpec.model_validate(plan['control'])
    require(plan['input_kind'] == 'REAL_HISTORY' and plan['candidate_profile'] == {'arm_id': 'P2', 'rule': CONTROL_ID}
            and plan['execution_contract'] == parent['execution_contract'] == dual.execution_contract()
            and plan['trade_dates'] == base['trade_dates'] == parent['trade_dates'] == list(inspected['days'])
            and plan['candidate_manifest'] == base['candidate_manifest'] == parent['candidate_manifest']
            and plan['selection_inventory'] == p['selection_inventory']
            and plan['package_id'] == p['package_id']
            and plan['package_manifest_sha256'] == p['manifest_sha256']
            and plan['cost_policy'] == p['inputs']['cost_policy']
            and plan['operation'] == next(row['operation'] for row in parent['population']
                                         if row['rule'] == 'FIRST_OBSERVABLE_V1' and row['delay_seconds'] == 60)
            and plan['primary_delay_seconds'] == 60 and plan['decision_ttl_seconds'] == 90
            and plan['common_terminal_local'] == '14:57' and plan['allow_split'] is False
            and control.source_commit == plan['timing_source_commit']
            and plan['common_opening'] == base['common_opening']
            and all(Decimal(common[k]) == Decimal('10000000') for k in
                    ('cash_cny', 'opening_nav_cny', 'origin_initial_cash_cny'))
            and common['positions'] == common['opening_marks_raw_cny'] == {}
            and Decimal(common['new_capital_added_cny']) == 0, 'INPUT_OR_CASH_ORIGIN_CONFLICT')
    compatibility = read(compatibility_ref)
    require(compatibility == _compatibility_body(plan, FileRef.model_validate(compatibility['before']),
                                                FileRef.model_validate(compatibility['after'])),
            'COMPATIBILITY_RECEIPT_CONFLICT')
    refs = [plan_ref, delivery_ref, compatibility_ref, *_file_refs(plan), *_file_refs(delivery),
            *_file_refs(compatibility), *inspected['source_refs'], *_market_pins(plan, inspected)]
    # Old native journals are immutable source bytes, never re-executed accounts.
    for account in plan['baseline_accounts']:
        for day_ref in account['days']:
            refs.append(FileRef.model_validate(read(day_ref)['account_receipt']))
    unique = {}
    for source in refs:
        require(source.path not in unique or unique[source.path] == source, 'SOURCE_PIN_CONFLICT')
        checked_file(source)
        unique[source.path] = source
    inspected['source_refs'] = [unique[k] for k in sorted(unique)]
    return plan, base, p, common, inspected


def _clone(common, plan_ref, producer_commit, operation):
    return deepcopy(common) | {'schema_version': 'position_timing_adverse_wait_opening_v1',
        'account_id': 'lsaw_' + digest([plan_ref, producer_commit, 'P2'])[:40], 'arm_id': 'P2',
        'rule': CONTROL_ID, 'delay_seconds': 60, 'operation': deepcopy(operation),
        'common_opening': read(plan_ref)['common_opening']}


def freeze_adverse(*, research_plan, source_delivery, shared_economics_compatibility, output_root, producer_commit):
    require(re.fullmatch(r'[0-9a-f]{40}', producer_commit) is not None, 'PRODUCER_COMMIT_INVALID')
    plan, base, p, common, _ = _sources(research_plan, source_delivery, shared_economics_compatibility)
    destination = output_root.resolve()
    for source in (research_plan, FileRef.model_validate(plan['baseline_facts']),
                   FileRef.model_validate(base['source_spec']['request']),
                   *[FileRef.model_validate(plan[k]) for k in ('candidate_manifest', 'price_templates', 'common_opening')]):
        protected = checked_file(source).parent.resolve()
        require(destination != protected and protected not in destination.parents, 'OUTPUT_OVERLAPS_SOURCE')
    opening = _clone(common, research_plan, producer_commit, plan['operation'])
    opening_path = output_root / 'openings' / digest([research_plan, producer_commit]) / 'P2.json'
    publish(opening_path, opening)
    body = {'schema_version': REQUEST_SCHEMA, 'producer': 'LOCALSIM', 'research_plan': research_plan.model_dump(),
        'source_delivery': source_delivery.model_dump(), 'producer_source_commit': producer_commit,
        'source_identity': implementation_identity(), 'history_scope': SCOPE,
        'shared_economics_compatibility': shared_economics_compatibility.model_dump(),
        **{k: plan[k] for k in BOUND_FIELDS}, 'p2_control_spec': plan['control'],
        'p2_timing_source_commit': plan['timing_source_commit'],
        'timing_source_commit': base['timing_source_commit'], 'base_timing_source_commit': base['timing_source_commit'],
        'initial_accounts': [ref(opening_path)], 'account_count': 1, 'profiles': [plan['candidate_profile']],
        'manifest_sha256': p['manifest_sha256'], 'primary_delay_seconds': 60, 'decision_ttl_seconds': 90,
        'common_terminal_local': '14:57', 'automatic_economic_recompute': False}
    identity = digest(body)
    body.update(identity=identity, release_identity=RELEASE_PREFIX + identity[:24])
    path = output_root / 'studies' / identity / 'request.json'
    existed = path.exists()
    publish(path, body)
    return {'status': 'ALREADY_FROZEN' if existed else 'FROZEN', 'request': ref(path),
            'account_count': 1, 'account_day_count': len(plan['trade_dates']), 'old_account_days_recomputed': 0}


def _request_scope(request_ref):
    request = read(request_ref)
    identity = digest({k:v for k,v in request.items() if k not in {'identity', 'release_identity'}})
    require(request.get('schema_version') == REQUEST_SCHEMA and request.get('producer') == 'LOCALSIM'
            and request.get('history_scope') == SCOPE and request.get('identity') == identity
            and Path(request_ref.path).name == 'request.json' and Path(request_ref.path).parent.name == identity
            and request.get('release_identity') == RELEASE_PREFIX + identity[:24], 'REQUEST_IDENTITY_CONFLICT')
    return request


def _inspect_request(request_ref):
    request = _request_scope(request_ref)
    require(request['source_identity'] == implementation_identity(), 'REQUEST_SOURCE_CONFLICT')
    plan, base, p, common, inspected = _sources(FileRef.model_validate(request['research_plan']),
        FileRef.model_validate(request['source_delivery']), FileRef.model_validate(request['shared_economics_compatibility']))
    require(all(request[k] == plan[k] for k in BOUND_FIELDS)
            and request['p2_control_spec'] == plan['control']
            and request['p2_timing_source_commit'] == plan['timing_source_commit']
            and request['timing_source_commit'] == request['base_timing_source_commit'] == base['timing_source_commit']
            and request['profiles'] == [plan['candidate_profile']]
            and request['account_count'] == len(request['initial_accounts']) == 1
            and request['manifest_sha256'] == p['manifest_sha256']
            and request['primary_delay_seconds'] == 60 and request['decision_ttl_seconds'] == 90
            and request['common_terminal_local'] == '14:57' and request['automatic_economic_recompute'] is False
            and re.fullmatch(r'[0-9a-f]{40}', request['producer_source_commit']), 'REQUEST_BINDING_CONFLICT')
    opening = read(request['initial_accounts'][0])
    require(opening == _clone(common, FileRef.model_validate(request['research_plan']),
                             request['producer_source_commit'], plan['operation']), 'OPENING_CONFLICT')
    return request, p, [opening], inspected


def _raw_source(request_ref, request, inspected):
    return shared._raw_source(request_ref, request, inspected) | {
        'schema_version': 'localsim_adverse_wait_raw_source_inspection_v1',
        'shared_economics_compatibility': request['shared_economics_compatibility'],
        'baseline_facts': request['baseline_facts'], 'old_account_days_recomputed': 0}


def _source_projection(request_ref, request, accounts):
    return shared._source_projection(request_ref, request, accounts) | {
        'schema_version': 'localsim_adverse_wait_source_inspection_v1',
        'shared_economics_compatibility': request['shared_economics_compatibility'],
        'baseline_facts': request['baseline_facts'], 'base_timing_source_commit': request['base_timing_source_commit'],
        'p2_control_spec': request['p2_control_spec'], 'old_account_days_recomputed': 0}


def _facts(request_ref, request, accounts):
    return {'schema_version': 'localsim_adverse_wait_history_facts_v1', 'producer': 'LOCALSIM',
        'research_plan': request['research_plan'], 'producer_request': request_ref.model_dump(),
        'producer_source_commit': request['producer_source_commit'], 'study_identity': request['identity'],
        'source_inspection': ref(Path(request_ref.path).parent / 'source_inspection.json'),
        'shared_economics_compatibility': request['shared_economics_compatibility'], 'accounts': accounts,
        'baseline_facts': request['baseline_facts'], 'source_batch_verified': True,
        'outcomes_read_stage': 'AFTER_COMPLETE_SOURCE_BATCH_INSPECTION',
        'nav_observation_scope': 'ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS', 'economic_confirmation': False}


def _result_receipt(root, request_ref, request):
    return {'schema_version': 'localsim_adverse_wait_result_receipt_v1', 'request': request_ref.model_dump(),
        **{k:ref(root / (k + '.json')) for k in ('facts', 'source_inspection', 'raw_source_inspection', 'ordered_days')},
        'opening': request['initial_accounts'][0], 'shared_economics_compatibility': request['shared_economics_compatibility'],
        'account_count': 1, 'account_day_count': len(request['trade_dates']), 'old_account_days_recomputed': 0,
        'economic_confirmation': False, 'database_accessed': False, 'dataset_mutated': False,
        'service_process_control': False}


def _verify_releases(request, accounts):
    days = [day for account in accounts for day in account['days']]
    for day_ref in days:
        row = read(day_ref)
        checked_file(FileRef.model_validate(row['p2_release_evidence_ref']))
    for day_ref in days:
        row = read(day_ref)
        expected = {'schema_version': 'localsim_adverse_wait_release_evidence_v1',
            'study_identity': request['identity'], 'account_id': row['account_id'], 'trade_date': row['trade_date'],
            'control_spec': request['p2_control_spec'], 'releases': release_evidence_from_account(row)}
        require(read(row['p2_release_evidence_ref']) == expected
                and row['p2_release_evidence'] == expected['releases'], 'RELEASE_EVIDENCE_CONFLICT')
        base_policy_hash = digest(Policy(policy_id='FIRST_OBSERVABLE_V1', source_commit=request['base_timing_source_commit']))
        for audit in expected['releases']:
            state = AdverseWaitState.model_validate(audit['control_state'])
            decision = AdverseWaitDecision.model_validate(audit['control_decision'])
            require(state.control_spec_sha256 == decision.control_spec_sha256 == digest(request['p2_control_spec'])
                    and decision.next_state_sha256 == digest(state)
                    and decision.release == state.release and decision.underlying_decision.policy_sha256 == base_policy_hash
                    and (state.release is None or state.release.observed_at <= decision.as_of)
                    and decision.as_of <= datetime.fromisoformat(row['terminal_as_of']), 'RELEASE_IDENTITY_OR_CLOCK_CONFLICT')


def _inspect_completed(request_ref, request, openings, inspected, *, allow_prefix=False):
    root = Path(request_ref.path).parent
    require(read(ref(root / 'raw_source_inspection.json')) == _raw_source(request_ref, request, inspected), 'RAW_SOURCE_CONFLICT')
    accounts = shared._account_refs(root, request, openings, allow_prefix=allow_prefix)
    if allow_prefix and not (root / 'result_receipt.json').exists():
        _verify_releases(request, accounts)
        return {'status': 'VERIFIED_PREFIX', 'computed_account_days': 0,
                'verified_account_days': shared._verify_days(root, request, openings, accounts, day_schema=DAY_SCHEMA)}
    require(sum(len(a['days']) for a in accounts) == len(request['trade_dates']), 'INCOMPLETE_ACCOUNT_DAYS')
    receipt = read(ref(root / 'result_receipt.json'))
    require(receipt == _result_receipt(root, request_ref, request), 'RESULT_RECEIPT_CONFLICT')
    require(read(receipt['source_inspection']) == _source_projection(request_ref, request, accounts)
            and read(receipt['facts']) == _facts(request_ref, request, accounts)
            and read(receipt['ordered_days']) == accounts[0], 'RESULT_BINDING_CONFLICT')
    _verify_releases(request, accounts)
    return {**receipt, 'status': 'VERIFIED', 'computed_account_days': 0,
            'verified_account_days': shared._verify_days(root, request, openings, accounts, day_schema=DAY_SCHEMA)}


def inspect_adverse(request_ref, *, allow_prefix=False):
    request, _, openings, inspected = _inspect_request(request_ref)
    return _inspect_completed(request_ref, request, openings, inspected, allow_prefix=allow_prefix)


def run_adverse(request_ref, *, performance=None, stop_after_account_days=None):
    performance = performance or ResearchPerformanceOptions()
    require(isinstance(performance, ResearchPerformanceOptions) and performance.account_workers == 1
            and (stop_after_account_days is None or type(stop_after_account_days) is int and stop_after_account_days >= 0),
            'OPTIONS_INVALID')
    _request_scope(request_ref)
    root = checked_file(request_ref).parent
    with FileLock(root / 'study-writer.lock', timeout=0):
        request, p, openings, inspected = _inspect_request(request_ref)
        publish(root / 'raw_source_inspection.json', _raw_source(request_ref, request, inspected))
        if (root / 'result_receipt.json').exists():
            return {**_inspect_completed(request_ref, request, openings, inspected),
                    'status': 'ALREADY_MATERIALIZED', 'recomputed_account_days': 0}
        package = read(p['inputs']['package'])['package']
        require(compute_manifest_json_sha256(package['manifest']) == request['manifest_sha256'], 'PACKAGE_CONFLICT')
        metrics = {'market_preparation_seconds': 0.0, 'commit_readback_seconds': 0.0}
        aid = openings[0]['account_id']
        with ResearchAccountPool(1) as pool, ExitStack() as resources:
            candidate = resources.enter_context(ResearchMarketWorkspace(
                CandidateReader(FileRef.model_validate(request['candidate_manifest'])), performance.batch_days))
            result = dual.execute_account_days(request=request, inspected=inspected,
                initials=period._initials(request, openings), manifest=StrategyPackageManifest.model_validate(package['manifest']),
                cost=CostContract.model_validate(read(request['cost_policy'])), candidate=candidate, root=root, pool=pool,
                metrics=metrics, performance=performance, stop_after_account_days=stop_after_account_days,
                day_schema=DAY_SCHEMA, research_controls={aid:CONTROL_ID}, research_audit=True,
                research_control_specs={aid:request['p2_control_spec']})
            candidate.verify_pins()
        if 'status' in result:
            return result
        for source in inspected['source_refs']:
            checked_file(source)
        require(request['source_identity'] == implementation_identity(), 'SOURCE_CHANGED_DURING_RUN')
        accounts = shared._account_refs(root, request, openings)
        _verify_releases(request, accounts)
        count = shared._verify_days(root, request, openings, accounts, day_schema=DAY_SCHEMA)
        publish(root / 'ordered_days.json', accounts[0])
        publish(root / 'source_inspection.json', _source_projection(request_ref, request, accounts))
        publish(root / 'facts.json', _facts(request_ref, request, accounts))
        receipt = _result_receipt(root, request_ref, request)
        publish(root / 'result_receipt.json', receipt)
        return {**receipt, 'status': 'MATERIALIZED', 'computed_account_days': result['computed'],
                'verified_account_days': count, 'recomputed_account_days': 0, 'performance': metrics}


def exact_artifact_retry(request_ref):
    _request_scope(request_ref)
    root = checked_file(request_ref).parent
    with FileLock(root / 'study-writer.lock', timeout=0):
        verified = inspect_adverse(request_ref)
        body = {'schema_version': 'localsim_adverse_wait_artifact_retry_v1', 'status': 'EXACT_ARTIFACT_RETRY_VERIFIED',
            'producer_request': request_ref.model_dump(), 'result_receipt': ref(root / 'result_receipt.json'),
            **{k:verified[k] for k in ('facts', 'source_inspection', 'ordered_days', 'opening', 'shared_economics_compatibility')},
            'verified_account_days': verified['verified_account_days'], 'new_committed_account_days': 0,
            'recomputed_account_days': 0, 'old_account_days_recomputed': 0, 'producer_economic_retry': 'NOT_SUPPLIED'}
        publish(root / 'artifact_retry_receipt.json', body)
        return {**body, 'artifact_retry_receipt': ref(root / 'artifact_retry_receipt.json')}


def main():
    import json
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('compatibility', 'freeze', 'run', 'inspect', 'exact-retry'))
    parser.add_argument('--plan')
    parser.add_argument('--plan-sha256')
    parser.add_argument('--source-delivery')
    parser.add_argument('--source-delivery-sha256')
    parser.add_argument('--compatibility')
    parser.add_argument('--compatibility-sha256')
    parser.add_argument('--before')
    parser.add_argument('--before-sha256')
    parser.add_argument('--after')
    parser.add_argument('--after-sha256')
    parser.add_argument('--producer-commit')
    parser.add_argument('--output-root', type=Path)
    parser.add_argument('--request')
    parser.add_argument('--request-sha256')
    parser.add_argument('--batch-days', type=int, default=3)
    parser.add_argument('--stop-after-account-days', type=int)
    parser.add_argument('--allow-prefix', action='store_true')
    args = parser.parse_args()
    def arg_ref(name):
        return FileRef(path=getattr(args, name), sha256=getattr(args, name + '_sha256'))
    if args.command == 'compatibility':
        result = freeze_compatibility(research_plan=arg_ref('plan'), before=arg_ref('before'), after=arg_ref('after'),
                                      output_path=args.output_root / 'shared_economics_compatibility.json')
    elif args.command == 'freeze':
        result = freeze_adverse(research_plan=arg_ref('plan'), source_delivery=arg_ref('source_delivery'),
            shared_economics_compatibility=arg_ref('compatibility'), output_root=args.output_root,
            producer_commit=args.producer_commit)
    elif args.command == 'run':
        result = run_adverse(arg_ref('request'), performance=ResearchPerformanceOptions(optimized=True, batch_days=args.batch_days),
                             stop_after_account_days=args.stop_after_account_days)
    elif args.command == 'inspect':
        result = inspect_adverse(arg_ref('request'), allow_prefix=args.allow_prefix)
    else:
        result = exact_artifact_retry(arg_ref('request'))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
