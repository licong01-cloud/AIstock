"""Four own-opening October accounts using the existing native day engine.

Only this independent request may consume the natural-period preparation.
Historical accounts and price templates are immutable inputs, never fallbacks.
"""

from contextlib import ExitStack
import hashlib
from pathlib import Path
import re

from filelock import FileLock

from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.natural_period_context import load_context
from backend.services.position_timing.intraday.natural_period_prices import inspect as inspect_prices
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.research import CostContract
from backend.services.position_timing.intraday.research_inputs import CandidateReader, checked_file
from backend.services.strategy_package.models import StrategyPackageManifest

from . import adverse_wait_research as adverse
from . import dual_mode_research as dual
from . import period_research as period
from . import wait_budget_research as shared
from .research_performance import ResearchAccountPool, ResearchMarketWorkspace, ResearchPerformanceOptions
from .timing_campaign import inspect_price_templates
from .timing_research import ref
from .timing_store import publish

REQUEST_SCHEMA = 'localsim_natural_period_request_v1'
DAY_SCHEMA = 'localsim_natural_period_day_v1'
SCOPE = 'NATURAL_OCTOBER_2026_FOUR_OWN_OPENINGS_68_ACCOUNT_DAYS'
TRADE_DATES = [f'2026-10-{day:02}' for day in (8, 9, 12, 13, 14, 15, 16, 19, 20, 21, 22, 23, 26, 27, 28, 29, 30)]
PROFILES = [{'arm_id': arm, 'rule': rule} for arm, rule in (
    ('B0', 'FIRST_OBSERVABLE_V1'), ('W15', 'WAIT_BUDGET_CONTROL_15M_V1'),
    ('C2', 'BOUNDED_REVERSION_V1'), ('P2', 'ADVERSE_TREND_WAIT_15M_V1'))]
read = shared.read


def require(condition, reason):
    if not condition:
        raise ValueError('LOCALSIM_NATURAL_' + reason)


def implementation_identity():
    return {'shared_account_engine': period.implementation_identity(),
            'adverse_facade': adverse.implementation_identity(),
            'natural_runner': hashlib.sha256(Path(__file__).read_bytes().replace(b'\r\n', b'\n')).hexdigest()}


def _pins(*values):
    unique = {}
    for source in adverse._file_refs(values):
        require(Path(source.path).is_absolute(), 'RELATIVE_SOURCE')
        require(source.path not in unique or unique[source.path] == source, 'SOURCE_PIN_CONFLICT')
        unique[source.path] = source
    return [source for _, source in sorted(unique.items())]


def _verify(refs):
    for source in refs:
        checked_file(source)


def _fixed_scope(fields):
    require(fields['trade_dates'] == TRADE_DATES and fields['native_target_count'] == 850
            and fields['account_count'] == len(fields['initial_accounts']) == 4
            and fields['profiles'] == PROFILES and fields['new_capital_added_cny'] == '0'
            and fields['jan_september_account_days_recomputed'] == 0
            and fields['primary_delay_seconds'] == 60 and fields['decision_ttl_seconds'] == 90
            and fields['common_terminal_local'] == '14:57' and fields['wait_budget_minutes'] == 15
            and fields['price_research_threshold_fraction'] == '0.005'
            and fields['execution_contract'] == dual.execution_contract(), 'FIXED_SCOPE')


def _base_identity(preparation):
    diagnostic = read(preparation['diagnostic_spec'])
    analysis = read(diagnostic['analysis_spec'])
    delivery = read(analysis['source_delivery'])
    parent = read(delivery['request'])
    base = read(parent['base_history_plan'])
    require(base['schema_version'] == 'position_timing_wait_budget_history_plan_v1'
            and parent['base_timing_source_commit'] == base['timing_source_commit']
            and parent['p2_control_spec'] == preparation['control'], 'BASE_HISTORY_IDENTITY')
    return parent['base_history_plan'], base['timing_source_commit'], _pins(
        preparation['diagnostic_spec'], diagnostic['analysis_spec'], analysis['source_delivery'],
        delivery['request'], parent['base_history_plan'])


def _sources(delivery_ref):
    d = read(delivery_ref)
    require(d['schema_version'] == 'position_timing_natural_period_source_delivery_receipt_v1'
            and d['old_b0_c2_reuse'] is False and d['october_account_days_run_by_timing'] == 0
            and d['jan_september_account_days_recomputed'] == 0, 'DELIVERY_SCOPE')
    delivery_refs = _pins(delivery_ref.model_dump(), d)
    _verify(delivery_refs)
    contract = read(d['consumer_contract'])
    require(contract['schema_version'] == 'position_timing_natural_period_consumer_handoff_v1'
            and contract['request_schema'] == REQUEST_SCHEMA and contract['account_days_to_run_by_localsim'] == 68
            and contract['old_b0_c2_reuse'] is contract['old_account_namespace_alias_allowed'] is False
            and contract['jan_september_replay_allowed'] is False, 'CONSUMER_CONTRACT')
    c = load_context(FileRef.model_validate(d['input_preparation_delivery']))
    p, preparation = c['preflight'], c['request']
    price = inspect_prices(FileRef.model_validate(d['price_manifest']))
    require(price['manifest'] == d['price_manifest'] and price['templates'] == d['price_templates']
            and d['preflight'] == c['delivery']['preflight'], 'PRICE_BINDING')
    inspected = inspect_price_templates(FileRef.model_validate(d['preflight']),
        FileRef.model_validate(d['price_templates']), natural_context=c)
    require(inspected['stage'] == 'COMPLETE_SOURCE_BATCH_INSPECTED' and inspected['outcomes_read'] is False
            and inspected['reference_counts'] == {'BUY': 850, 'SELL': 2141, 'UNKNOWN': 0, 'AVAILABLE': 2991},
            'PRICE_COVERAGE')
    expected = {**{k: preparation[k] for k in (
        'candidate_manifest', 'parent_candidate_manifest', 'cost_policy', 'execution_contract', 'selection_inventory',
        'package_id', 'manifest_sha256', 'control', 'trade_dates', 'primary_delay_seconds', 'decision_ttl_seconds',
        'common_terminal_local', 'wait_budget_minutes', 'price_research_threshold_fraction', 'native_target_count')},
        'preflight': d['preflight'], 'preparation_request': p['preparation_request'],
        'initial_accounts': p['initial_accounts'], 'price_templates': d['price_templates'], 'profiles': PROFILES,
        'account_count': 4, 'new_capital_added_cny': '0', 'jan_september_account_days_recomputed': 0}
    require(contract['request_frozen_fields'] == expected, 'FROZEN_FIELDS_CONFLICT')
    _fixed_scope(expected)
    base_ref, base_commit, base_refs = _base_identity(preparation)
    require(preparation['execution_contract'] == dual.execution_contract(), 'EXECUTION_CONTRACT_DRIFT')
    inspected['source_refs'] = _pins(delivery_refs, [r.model_dump() for r in c['refs']],
        [r.model_dump() for r in inspected['source_refs']], [r.model_dump() for r in base_refs])
    return d, contract, c, inspected, base_ref, base_commit


def freeze(*, source_delivery: FileRef, output_root: Path, producer_commit: str):
    require(re.fullmatch('[a-f0-9]{40}', producer_commit), 'PRODUCER_COMMIT')
    d, contract, c, inspected, base_ref, base_commit = _sources(source_delivery)
    destination = Path(output_root).resolve()
    require(all(destination != Path(p.path).resolve().parent and Path(p.path).resolve().parent not in destination.parents
                for p in inspected['source_refs']), 'OUTPUT_OVERLAPS_INPUT')
    request = {'schema_version': REQUEST_SCHEMA, 'scope': SCOPE, 'producer': 'LOCALSIM',
        'producer_source_commit': producer_commit, 'source_identity': implementation_identity(),
        'source_delivery': source_delivery.model_dump(), 'consumer_contract': d['consumer_contract'],
        'input_preparation_delivery': d['input_preparation_delivery'], 'price_manifest': d['price_manifest'],
        'base_history_plan': base_ref, 'base_timing_source_commit': base_commit, 'timing_source_commit': base_commit,
        'p2_timing_source_commit': c['request']['control']['source_commit'], 'p2_control_spec': c['request']['control'],
        'old_b0_c2_reuse': False, 'automatic_economic_recompute': False, **contract['request_frozen_fields']}
    request['identity'] = digest(request)
    request['release_identity'] = 'localsim_natural_period_v1_' + request['identity'][:24]
    root = destination/request['identity']
    publish(root/'request.json', request)
    return {'status': 'FROZEN', 'request': ref(root/'request.json'), 'account_count': 4,
            'trade_days': 17, 'native_targets': 850, 'account_replay_started': False}


def _request_scope(request_ref):
    r = read(request_ref)
    require(r['schema_version'] == REQUEST_SCHEMA and r['scope'] == SCOPE
            and r['identity'] == digest({k: v for k, v in r.items() if k not in {'identity', 'release_identity'}})
            and r['release_identity'] == 'localsim_natural_period_v1_' + r['identity'][:24]
            and Path(request_ref.path).name == 'request.json' and Path(request_ref.path).parent.name == r['identity'],
            'REQUEST_IDENTITY')
    contract = read(r['consumer_contract'])
    base = read(r['base_history_plan'])
    _fixed_scope(r)
    require(all(r[k] == v for k, v in contract['request_frozen_fields'].items())
            and r['producer'] == 'LOCALSIM' and re.fullmatch('[0-9a-f]{40}', r['producer_source_commit'])
            and r['old_b0_c2_reuse'] is r['automatic_economic_recompute'] is False
            and r['source_identity'] == implementation_identity()
            and r['base_timing_source_commit'] == r['timing_source_commit'] == base['timing_source_commit']
            and r['p2_control_spec'] == r['control']
            and r['p2_timing_source_commit'] == r['control']['source_commit'], 'REQUEST_BINDING')
    return r


def _raw_source(request_ref, request, inspected):
    return {'schema_version': 'localsim_natural_period_raw_source_inspection_v1',
        'stage': 'COMPLETE_SOURCE_BATCH_INSPECTED', 'producer_request': request_ref.model_dump(),
        'source_identity': request['source_identity'], 'source_delivery': request['source_delivery'],
        'source_refs': [r.model_dump() for r in inspected['source_refs']], 'new_period_outcomes_read': False,
        'jan_september_account_days_recomputed': 0, 'old_b0_c2_reuse': False}


def _projection(request_ref, request, accounts, input_refs):
    day_refs = []
    for account in accounts:
        for source in account['days']:
            row = read(source)
            day_refs.append(row['account_receipt'])
            if account['arm_id'] == 'P2':
                day_refs.append(row['p2_release_evidence_ref'])
    sources = _pins([r.model_dump() for r in input_refs], accounts, day_refs, request_ref.model_dump())
    return {'schema_version': 'localsim_natural_period_result_source_inspection_v1',
        'stage': 'COMPLETE_SOURCE_BATCH_INSPECTED', 'producer_request': request_ref.model_dump(),
        **{k: request[k] for k in ('preflight', 'preparation_request', 'price_templates', 'candidate_manifest',
                                 'trade_dates', 'producer_source_commit')},
        'study_identity': request['identity'], 'account_days_sha256': digest(accounts),
        'initial_accounts_sha256': digest(request['initial_accounts']),
        'cost_policy_file_sha256': request['cost_policy']['sha256'],
        'execution_contract_sha256': digest(request['execution_contract']),
        'source_refs': [r.model_dump() for r in sources], 'jan_september_account_days_recomputed': 0}


def _facts(root, request_ref, request, accounts):
    return {'schema_version': 'position_timing_natural_period_facts_v1',
        'preflight': request['preflight'], 'price_templates': request['price_templates'],
        'producer_request': request_ref.model_dump(), 'source_inspection': ref(root/'source_inspection.json'),
        'producer_source_commit': request['producer_source_commit'], 'study_identity': request['identity'],
        'source_batch_verified': True, 'outcomes_read_stage': 'AFTER_COMPLETE_SOURCE_BATCH_INSPECTION',
        'nav_observation_scope': 'ALL_PRODUCER_ACCOUNT_NAV_OBSERVATIONS', 'accounts': accounts}


def _receipt(root, request_ref, request):
    return {'schema_version': 'localsim_natural_period_result_receipt_v1', 'producer_request': request_ref.model_dump(),
        **{k: ref(root/(k+'.json')) for k in ('facts', 'source_inspection', 'raw_source_inspection', 'ordered_accounts')},
        'account_count': 4, 'account_days': 68, 'new_capital_added_cny': '0',
        'jan_september_account_days_recomputed': 0, 'old_b0_c2_reuse': False, 'economic_confirmation': False}


def _verify_days(root, request, openings, accounts):
    count = shared._verify_days(root, request, openings, accounts, day_schema=DAY_SCHEMA)
    adverse._verify_releases(request, [a for a in accounts if a['arm_id'] == 'P2'])
    return count


def run(request_ref: FileRef, *, performance=None, stop_after_account_days=None):
    performance = performance or ResearchPerformanceOptions()
    require(isinstance(performance, ResearchPerformanceOptions) and (stop_after_account_days is None or
            type(stop_after_account_days) is int and stop_after_account_days >= 0), 'OPTIONS')
    request = _request_scope(request_ref)
    root = Path(request_ref.path).parent
    with FileLock(root/'study-writer.lock', timeout=0):
        if (root/'result_receipt.json').exists():
            return {**_inspect_completed(request_ref), 'status': 'ALREADY_MATERIALIZED'}
        d, contract, c, inspected, base_ref, base_commit = _sources(FileRef.model_validate(request['source_delivery']))
        require(all(request[k] == v for k, v in contract['request_frozen_fields'].items())
                and request['base_history_plan'] == base_ref and request['timing_source_commit'] == base_commit,
                'RUN_SOURCE_BINDING')
        require(all(request[k] == d[k] for k in ('consumer_contract', 'input_preparation_delivery', 'price_manifest')),
                'RUN_DELIVERY_BINDING')
        publish(root/'raw_source_inspection.json', _raw_source(request_ref, request, inspected))
        openings = c['openings']
        controls = {a['account_id']: a['rule'] for a in openings if a['arm_id'] in {'W15', 'P2'}}
        specs = {a['account_id']: request['control'] for a in openings if a['arm_id'] == 'P2'}
        metrics = {'market_preparation_seconds': 0.0, 'commit_readback_seconds': 0.0}
        package = read(c['package'])['package']
        with ResearchAccountPool(performance.account_workers) as pool, ExitStack() as resources:
            candidate = resources.enter_context(ResearchMarketWorkspace(
                CandidateReader(FileRef.model_validate(request['candidate_manifest'])), performance.batch_days))
            result = dual.execute_account_days(request=request, inspected=inspected,
                initials=period._initials(request, openings), manifest=StrategyPackageManifest.model_validate(package['manifest']),
                cost=CostContract.model_validate(read(request['cost_policy'])), candidate=candidate, root=root, pool=pool,
                metrics=metrics, performance=performance, stop_after_account_days=stop_after_account_days,
                day_schema=DAY_SCHEMA, research_controls=controls, research_control_specs=specs, research_audit=True)
            candidate.verify_pins()
        if 'status' in result:
            return result
        _verify(inspected['source_refs'])
        require(request['source_identity'] == implementation_identity(), 'SOURCE_CHANGED_DURING_RUN')
        accounts = shared._account_refs(root, request, openings)
        count = _verify_days(root, request, openings, accounts)
        inspection = _projection(request_ref, request, accounts, inspected['source_refs'])
        _verify([FileRef.model_validate(r) for r in inspection['source_refs']])
        publish(root/'source_inspection.json', inspection)
        publish(root/'ordered_accounts.json', accounts)
        publish(root/'facts.json', _facts(root, request_ref, request, accounts))
        receipt = _receipt(root, request_ref, request)
        publish(root/'result_receipt.json', receipt)
        return {**receipt, 'status': 'MATERIALIZED', 'computed_account_days': result['computed'],
                'verified_account_days': count, 'recomputed_account_days': result['recomputed'], 'performance': metrics}


def _inspect_completed(request_ref, *, allow_prefix=False):
    """Hash source/artifact bytes and reconcile native facts; never run strategies."""
    request = _request_scope(request_ref)
    root = Path(request_ref.path).parent
    raw = read(ref(root/'raw_source_inspection.json'))
    require(raw['schema_version'] == 'localsim_natural_period_raw_source_inspection_v1'
            and raw['stage'] == 'COMPLETE_SOURCE_BATCH_INSPECTED' and raw['producer_request'] == request_ref.model_dump()
            and raw['source_identity'] == request['source_identity'] and raw['new_period_outcomes_read'] is False,
            'RAW_SOURCE_BINDING')
    input_refs = [FileRef.model_validate(r) for r in raw['source_refs']]
    require(bool(input_refs) and raw == _raw_source(request_ref, request, {'source_refs': input_refs})
            and all(source in input_refs for source in _pins(
                request['source_delivery'], request['consumer_contract'], request['preflight'],
                request['preparation_request'], request['price_manifest'], request['price_templates'],
                request['candidate_manifest'], request['cost_policy'], request['initial_accounts'],
                request['base_history_plan'])), 'RAW_SOURCE_CLOSURE')
    _verify(input_refs)
    openings = [read(r) for r in request['initial_accounts']]
    accounts = shared._account_refs(root, request, openings, allow_prefix=allow_prefix)
    if allow_prefix and not (root/'result_receipt.json').exists():
        return {'status': 'VERIFIED_PREFIX', 'verified_account_days': _verify_days(root, request, openings, accounts),
                'computed_account_days': 0, 'recomputed_account_days': 0}
    inspection_ref = ref(root/'source_inspection.json')
    inspection = read(inspection_ref)
    _verify([FileRef.model_validate(r) for r in inspection['source_refs']])
    require(inspection == _projection(request_ref, request, accounts, input_refs)
            and read(ref(root/'ordered_accounts.json')) == accounts
            and read(ref(root/'facts.json')) == _facts(root, request_ref, request, accounts)
            and read(ref(root/'result_receipt.json')) == _receipt(root, request_ref, request), 'RESULT_BINDING')
    count = _verify_days(root, request, openings, accounts)
    require(count == 68, 'INCOMPLETE_ACCOUNT_DAYS')
    return {**_receipt(root, request_ref, request), 'status': 'VERIFIED',
            'verified_account_days': count, 'computed_account_days': 0, 'recomputed_account_days': 0}


def inspect(request_ref, *, allow_prefix=False):
    return _inspect_completed(request_ref, allow_prefix=allow_prefix)


def exact_artifact_retry(request_ref):
    _request_scope(request_ref)
    root = Path(request_ref.path).parent
    with FileLock(root/'study-writer.lock', timeout=0):
        verified = _inspect_completed(request_ref)
        body = {'schema_version': 'localsim_natural_period_artifact_retry_v1',
            'status': 'EXACT_ARTIFACT_RETRY_VERIFIED', 'producer_request': request_ref.model_dump(),
            **{k: verified[k] for k in ('facts', 'source_inspection')}, 'verified_account_days': 68,
            'recomputed_account_days': 0, 'new_committed_account_days': 0,
            'jan_september_account_days_recomputed': 0, 'market_inputs_recomputed': False,
            'producer_economic_retry': 'NOT_SUPPLIED'}
        publish(root/'artifact_retry_receipt.json', body)
        return {**body, 'artifact_retry_receipt': ref(root/'artifact_retry_receipt.json')}
