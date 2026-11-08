"""Functional native P2 accounts; real-history scope remains the owner SDK's job."""
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.simulation_execution.localsim import adverse_wait_research as study
from backend.services.simulation_execution.localsim import dual_mode_research as dual
from backend.services.simulation_execution.localsim.research_performance import ResearchPerformanceOptions
from backend.services.simulation_execution.localsim.timing_research import ref
from backend.services.simulation_execution.localsim.timing_store import publish
from backend.tests.simulation_execution.test_localsim_dual_mode_comparison import dual_fixture


@pytest.fixture
def p2(tmp_path, monkeypatch):
    parent_ref, _ = dual_fixture(tmp_path, monkeypatch, matrix='BASIC_2')
    parent = study.read(parent_ref)
    original = study.read(parent['source_request'])
    inspected = study.inspect_price_templates(FileRef.model_validate(original['preflight']),
                                             FileRef.model_validate(original['price_templates']))
    p = inspected['preflight']
    root = tmp_path / 'functional-source'
    common = {'schema_version': 'position_timing_wait_budget_history_common_opening_v1',
        'basis': 'DECLARED_NEW_RESEARCH_CASH_ORIGIN_NOT_AN_EXISTING_ACCOUNT',
        'cash_cny': '10000000', 'opening_nav_cny': '10000000', 'origin_initial_cash_cny': '10000000',
        'positions': {}, 'opening_marks_raw_cny': {}, 'new_capital_added_cny': '0',
        'analysis_anchor': {'prior_nav_high_water_cny': '10000000', 'prior_cumulative_max_drawdown': '0',
                            'prior_cumulative_fees_cny': '0'},
        'parent_account_id': 'HISTORY_CASH_ORIGIN', 'period_start': parent['trade_dates'][0],
        'available_quantity_as_of': parent['trade_dates'][0] + 'T09:00:00+08:00'}
    publish(root / 'common.json', common)
    base = {'timing_source_commit': '7' * 40, 'common_opening': ref(root / 'common.json'),
            'source_spec': {'request': parent_ref.model_dump()}}
    publish(root / 'base.json', base)
    plan = {'schema_version': 'position_timing_adverse_wait_research_plan_v1', 'identity': 'e' * 64,
        'input_kind': 'REAL_HISTORY', 'control': study.AdverseWaitSpec(source_commit='9' * 40).model_dump(),
        'candidate_profile': {'arm_id': 'P2', 'rule': study.CONTROL_ID}, 'base_history_plan': ref(root / 'base.json'),
        'timing_source_commit': '9' * 40, 'common_opening': ref(root / 'common.json'),
        'trade_dates': parent['trade_dates'], 'candidate_manifest': parent['candidate_manifest'],
        'price_templates': original['price_templates'], 'cost_policy': p['inputs']['cost_policy'],
        'execution_contract': parent['execution_contract'], 'operation': parent['population'][1]['operation'],
        'selection_inventory': p['selection_inventory'], 'package_id': p['package_id'],
        'package_manifest_sha256': p['manifest_sha256'],
        'baseline_facts': parent_ref.model_dump(), 'baseline_analysis_spec': parent_ref.model_dump(),
        'baseline_accounts': [], 'primary_delay_seconds': 60, 'decision_ttl_seconds': 90,
        'allow_split': False, 'common_terminal_local': '14:57'}
    publish(root / 'plan.json', plan)
    plan_ref = FileRef.model_validate(ref(root / 'plan.json'))
    publish(root / 'delivery.json', {'schema_version': 'FUNCTIONAL_SOURCE_STUB', 'research_plan': plan_ref.model_dump()})
    publish(root / 'compatibility.json', {'schema_version': 'FUNCTIONAL_COMPATIBILITY_SOURCE_STUB'})
    # Stub only fixed historical-source admission; quantities, P2/core evaluation,
    # queues, ledger, matching, journal commits and projections all execute natively.
    def sources(*args):
        for source in (plan_ref, *args[1:]):
            study.checked_file(source)
        return plan, base, p, common, inspected
    monkeypatch.setattr(study, '_sources', sources)
    monkeypatch.setattr(study, 'CandidateReader', dual.CandidateReader)
    frozen = study.freeze_adverse(research_plan=plan_ref,
        source_delivery=FileRef.model_validate(ref(root / 'delivery.json')),
        shared_economics_compatibility=FileRef.model_validate(ref(root / 'compatibility.json')),
        output_root=tmp_path.with_name(tmp_path.name + '-p2'), producer_commit='8' * 40)
    return FileRef.model_validate(frozen['request'])


def test_p2_only_cash_origin_and_three_code_identities_freeze_reuses(p2):
    request = study.read(p2)
    opening = study.read(request['initial_accounts'][0])
    assert request['account_count'] == len(request['initial_accounts']) == 1
    assert opening['cash_cny'] == opening['opening_nav_cny'] == '10000000' and opening['positions'] == {}
    assert opening['arm_id'] == 'P2' and opening['rule'] == study.CONTROL_ID
    assert request['timing_source_commit'] == request['base_timing_source_commit'] == '7' * 40
    assert request['p2_control_spec']['source_commit'] == request['p2_timing_source_commit'] == '9' * 40
    assert request['producer_source_commit'] == '8' * 40
    again = study.freeze_adverse(research_plan=FileRef.model_validate(request['research_plan']),
        source_delivery=FileRef.model_validate(request['source_delivery']),
        shared_economics_compatibility=FileRef.model_validate(request['shared_economics_compatibility']),
        output_root=Path(p2.path).parents[2], producer_commit=request['producer_source_commit'])
    assert again['status'] == 'ALREADY_FROZEN' and again['request'] == p2.model_dump()


def test_sequential_native_prefix_resume_release_evidence_and_zero_recompute(p2, monkeypatch):
    calls, native = [], dual._compute_account_day
    def compute(job):
        assert (Path(p2.path).parent / 'raw_source_inspection.json').exists()
        assert job['timing_source_commit'] == '7' * 40
        assert job['research_control_spec']['source_commit'] == '9' * 40
        calls.append(job['day'])
        return native(job)
    monkeypatch.setattr(dual, '_compute_account_day', compute)
    options = ResearchPerformanceOptions(optimized=True)
    first = study.run_adverse(p2, performance=options, stop_after_account_days=1)
    assert first['status'] == 'CHECKPOINTED' and first['computed_account_days'] == 1
    assert study.inspect_adverse(p2, allow_prefix=True)['verified_account_days'] == 1
    final = study.run_adverse(p2, performance=options)
    assert final['verified_account_days'] == 2 and final['computed_account_days'] == 1
    account = study.read(final['ordered_days'])
    before, after = map(study.read, account['days'])
    assert before['initial_account']['positions'] == {}
    assert after['initial_account']['positions'] and Decimal(after['initial_account']['cash_cny']) == Decimal(before['cash_cny'])
    assert len(set(calls)) == len(calls) == 2
    assert all(study.read(row['p2_release_evidence_ref'])['control_spec']['source_commit'] == '9' * 40 for row in (before, after))
    assert final['old_account_days_recomputed'] == 0


def test_completed_exact_retry_preserves_every_artifact_and_never_computes(p2, monkeypatch):
    first = study.run_adverse(p2, performance=ResearchPerformanceOptions(optimized=True))
    root = Path(p2.path).parent
    before = {p:p.read_bytes() for p in root.rglob('*.json')}
    def forbidden(*args, **kwargs):
        pytest.fail('artifact retry cannot recompute accounts or build market views')
    monkeypatch.setattr(dual, '_compute_account_day', forbidden)
    monkeypatch.setattr(study, 'CandidateReader', forbidden)
    retry = study.exact_artifact_retry(p2)
    assert retry['recomputed_account_days'] == retry['new_committed_account_days'] == retry['old_account_days_recomputed'] == 0
    assert retry['facts'] == first['facts'] and study.exact_artifact_retry(p2) == retry
    assert study.run_adverse(p2)['status'] == 'ALREADY_MATERIALIZED'
    assert all(p.read_bytes() == raw for p, raw in before.items())


def test_committed_journal_survives_interruption_before_projection_without_duplicate_execution(p2, monkeypatch):
    native, publisher, calls = dual._compute_account_day, dual.publish, []
    def compute(job):
        calls.append(job['day'])
        return native(job)
    def interrupted(path, value):
        if Path(path).parent.parent.name == 'release_evidence':
            raise OSError('INTERRUPTED_AFTER_NATIVE_COMMIT')
        return publisher(path, value)
    monkeypatch.setattr(dual, '_compute_account_day', compute)
    monkeypatch.setattr(dual, 'publish', interrupted)
    with pytest.raises(OSError, match='INTERRUPTED_AFTER_NATIVE_COMMIT'):
        study.run_adverse(p2, performance=ResearchPerformanceOptions(optimized=True))
    journal = next((Path(p2.path).parent / 'journals').glob('*/generations/00000001.json'))
    before = journal.read_bytes()
    monkeypatch.setattr(dual, 'publish', publisher)
    result = study.run_adverse(p2, performance=ResearchPerformanceOptions(optimized=True))
    assert result['computed_account_days'] == 1 and result['verified_account_days'] == 2
    assert len(calls) == len(set(calls)) == 2 and journal.read_bytes() == before


@pytest.mark.parametrize('damage', ['opening', 'base_commit', 'p2_spec', 'account_count'])
def test_identity_and_opening_forgery_fail_before_economic_computation(p2, monkeypatch, damage):
    request = deepcopy(study.read(p2))
    if damage == 'opening':
        opening = study.read(request['initial_accounts'][0]) | {'parent_state': {}}
        path = Path(p2.path).parents[2] / 'forged-opening.json'
        publish(path, opening)
        request['initial_accounts'] = [ref(path)]
    else:
        request[{'base_commit':'timing_source_commit', 'p2_spec':'p2_control_spec', 'account_count':'account_count'}[damage]] = (
            '8' * 40 if damage == 'base_commit' else {'source_commit':'8' * 40} if damage == 'p2_spec' else 3)
    request['identity'] = digest({k:v for k,v in request.items() if k not in {'identity','release_identity'}})
    request['release_identity'] = study.RELEASE_PREFIX + request['identity'][:24]
    path = Path(p2.path).parent.parent / request['identity'] / 'request.json'
    publish(path, request)
    monkeypatch.setattr(dual, '_compute_account_day', lambda *args: pytest.fail('forgery cannot compute'))
    with pytest.raises(ValueError, match='OPENING_CONFLICT|REQUEST_BINDING_CONFLICT'):
        study.run_adverse(FileRef.model_validate(ref(path)))


def test_full_source_failure_precedes_new_nav_and_corrupt_result_cannot_be_accepted(p2, monkeypatch):
    study.run_adverse(p2, performance=ResearchPerformanceOptions(optimized=True))
    def forbidden(*args, **kwargs):
        pytest.fail('new NAV cannot precede complete source and binding inspection')
    monkeypatch.setattr(study.shared, '_verify_days', forbidden)
    with monkeypatch.context() as context:
        def unavailable(*args):
            raise ValueError('MISSING_LAST_FROZEN_SOURCE')
        context.setattr(study, '_sources', unavailable)
        with pytest.raises(ValueError, match='MISSING_LAST_FROZEN_SOURCE'):
            study.inspect_adverse(p2)
    path = Path(p2.path).parent / 'result_receipt.json'
    path.write_text('{}', encoding='utf-8')
    with pytest.raises(ValueError, match='RESULT_RECEIPT_CONFLICT'):
        study.inspect_adverse(p2)


@pytest.mark.parametrize('schema', ['localsim_wait_budget_history_request_v1', 'localsim_wait_budget_request_v1', 'localsim_period_study_v2'])
def test_old_studies_are_rejected_before_lock_or_write(tmp_path, schema):
    path = tmp_path / 'old/request.json'
    publish(path, {'schema_version': schema})
    original = path.read_bytes()
    for call in (study.run_adverse, study.exact_artifact_retry):
        with pytest.raises(ValueError, match='REQUEST_IDENTITY_CONFLICT'):
            call(FileRef.model_validate(ref(path)))
        assert path.read_bytes() == original and not (path.parent / 'study-writer.lock').exists()


def test_invalid_control_spec_cannot_hide_in_cash_only_or_native_day():
    for control, spec, mode in [(study.CONTROL_ID, None, 'HUMAN_SIM'),
                                (study.CONTROL_ID, {'source_commit':'9' * 40}, 'NATIVE'),
                                (None, {'source_commit':'9' * 40}, 'HUMAN_SIM')]:
        job = {'initial': {'rule': study.CONTROL_ID, 'operation': {'operation_mode': mode}},
               'prior': {}, 'research_control': control, 'research_control_spec': spec}
        with pytest.raises(ValueError):
            dual._compute_account_day(job)


def test_public_plan_rejection_precedes_all_consumer_source_reads(tmp_path, monkeypatch):
    source = FileRef(path=str(tmp_path / 'absent.json'), sha256='f' * 64)
    monkeypatch.setattr(study, 'inspect_adverse_plan', lambda ref: {'integrity':'BLOCKED', 'p2_outcomes_read':False})
    monkeypatch.setattr(study, 'read', lambda *args: pytest.fail('rejected plan must stop before other sources'))
    with pytest.raises(ValueError, match='PLAN_NOT_VERIFIED'):
        study._sources(source, source, source)


def test_release_source_corruption_precedes_native_nav_verification(p2, monkeypatch):
    from backend.services.position_timing.intraday.store import IntradayError
    result = study.run_adverse(p2, performance=ResearchPerformanceOptions(optimized=True))
    row = study.read(study.read(result['ordered_days'])['days'][-1])
    path = Path(row['p2_release_evidence_ref']['path'])
    path.write_bytes(path.read_bytes() + b' ')
    monkeypatch.setattr(study.shared, '_verify_days', lambda *args, **kwargs: pytest.fail('new NAV before release hash'))
    with pytest.raises(IntradayError, match='SOURCE_IDENTITY_MISMATCH'):
        study.inspect_adverse(p2)


@pytest.mark.parametrize('damage', ['cash', 'base_commit', 'engine'])
def test_compatibility_rejects_unequal_payload_or_unapproved_economic_engine_change(tmp_path, monkeypatch, damage):
    current = study.period.implementation_identity()
    baseline = deepcopy(current)
    plan = {'identity':'a' * 64, 'baseline_shared_economic_implementation':baseline,
            'implementation': {'intraday/' + name:current['shared_account_engine']['shared_authorities'][
                'backend/services/position_timing/intraday/' + name]
                for name in ('adverse_wait_control.py','adverse_wait_research.py')}}
    path = tmp_path / 'base.json'
    publish(path, {'timing_source_commit':'7' * 40})
    plan['base_history_plan'] = ref(path)
    # These compact payloads isolate the byte/evidence verifier; real economic
    # equivalence uses the separately frozen six native functional account-days.
    before = {'schema_version':'localsim_fixed_legacy_economic_smoke_v1',
        'scope':'FIXED_SYNTHETIC_TWO_DAY_NATIVE_ACCOUNT_SMOKE_NOT_543_REPLAY',
        'trade_dates':['2026-09-21','2026-09-22'], 'symbols':['000001.SZ','600000.SH'],
        'reaction_delay_seconds':60, 'initial_cash_cny':'15010', 'base_timing_source_commit':'7' * 40,
        'native_account_engine':baseline, 'accounts':{a:[{'cash':'1'},{'cash':'2'}] for a in ('B0','W15','C2')},
        'historical_account_days_recomputed':0, 'research_economics':False}
    before['economic_payload_sha256'] = digest(before['accounts'])
    after = deepcopy(before)
    for label, value in (('before',before),('after',after)):
        publish(tmp_path / (label + '.json'), value)
    left, right = (FileRef.model_validate(ref(tmp_path / (label + '.json'))) for label in ('before','after'))
    assert study._compatibility_body(plan, left, right)['economic_fields_equal'] is True
    if damage == 'cash':
        after['accounts']['B0'][1]['cash'] = '3'
        after['economic_payload_sha256'] = digest(after['accounts'])
    elif damage == 'base_commit':
        after['base_timing_source_commit'] = '9' * 40
    else:
        after['native_account_engine']['shared_account_engine']['native_execution']['backend/execution_algos/twap_algo.py'] = 'f' * 64
        monkeypatch.setattr(study.period, 'implementation_identity', lambda: after['native_account_engine'])
    changed = tmp_path / 'changed.json'
    publish(changed, after)
    with pytest.raises(ValueError, match='ECONOMICS_CONFLICT|CHANGE_REQUIRES_OWNER'):
        study._compatibility_body(plan, left, FileRef.model_validate(ref(changed)))
