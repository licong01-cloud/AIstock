"""Native four-account contracts; synthetic fixtures are not historical results."""

from copy import deepcopy
from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.store import IntradayError
from backend.services.simulation_execution.localsim import dual_mode_research as dual
from backend.services.simulation_execution.localsim import natural_period_research as study
from backend.services.simulation_execution.localsim import timing_campaign as campaign
from backend.services.simulation_execution.localsim.human_operation import HumanOperationPolicy, operation_snapshot
from backend.services.simulation_execution.localsim.research_performance import ResearchPerformanceOptions
from backend.services.simulation_execution.localsim.timing_research import ref
from backend.services.simulation_execution.localsim.timing_store import publish
from backend.tests.simulation_execution.test_localsim_dual_mode_comparison import dual_fixture
from backend.tests.simulation_execution.test_localsim_timing_campaign import price_campaign_fixture


def put(path, value):
    publish(path, value)
    return ref(path)


@pytest.fixture
def natural(tmp_path, monkeypatch):
    # Reuse native market/quantity/fee/matching fixtures. Only admission of
    # historical source files is substituted; no account/fill routine is stubbed.
    old_ref, _ = dual_fixture(tmp_path, monkeypatch, matrix='BASIC_2')
    old = study.read(old_ref)
    original = study.read(old['source_request'])
    inspected = campaign.inspect_price_templates(FileRef.model_validate(original['preflight']),
                                                 FileRef.model_validate(original['price_templates']))
    p = inspected['preflight']
    old_days = inspected['days']
    stamps = [str(v.date()) for v in pd.bdate_range('2026-10-08', periods=17)]
    # The real input inspector enforces the frozen Chinese exchange calendar;
    # this fixture's generated dates stay within the same 17-day contract.
    sources = tmp_path/'natural_sources'
    native_days = {}
    for stamp in stamps:
        prototype = deepcopy(next(iter(old_days.values())))
        native = prototype['native']
        projection = study.read(native['projection'])
        projection['runtime_config']['point_in_time_context'].update(effective_trade_date=stamp, cutoff_date='2026-09-30')
        native = {**native, 'trade_date': stamp, 'data_cutoff': '2026-09-30',
                  'projection': put(sources/stamp/'projection.json', projection)}
        delta = date.fromisoformat(stamp)-date(2026, 9, 21)
        series = {key: [event.model_copy(update={
            'trade_date': date.fromisoformat(stamp),
            **{k: getattr(event,k)+delta for k in ('available_at','valid_from','valid_until','source_available_at')},
            'price_constraint': event.price_constraint.model_copy(update={
                'valid_from': event.price_constraint.valid_from+delta,
                'valid_until': event.price_constraint.valid_until+delta})}) for event in events]
            for key,events in prototype['series'].items()}
        native_days[stamp] = {'native':native,'series':series,'unknown_references':{}}
    inspected = {**inspected, 'days':native_days}
    reader = dual.CandidateReader

    class Candidate(reader):
        def market(self, symbol, stamp):
            value = super().market(symbol,date(2026,9,21))
            delta = stamp-date(2026,9,21)
            return value.model_copy(update={'trade_date':stamp,
                'calendar':value.calendar.model_copy(update={'trade_date':stamp,
                    'available_at':value.calendar.available_at+delta}),
                'day_facts':value.day_facts.model_copy(update={'trade_date':stamp,
                    'available_at':value.day_facts.available_at+delta}),
                'bars':tuple(bar.model_copy(update={k:getattr(bar,k)+delta for k in ('start','end','received_at')})
                             for bar in value.bars)})

    monkeypatch.setattr(study,'CandidateReader',Candidate)
    base_ref = put(sources/'base.json', {'timing_source_commit':'7'*40})
    control = study.adverse.AdverseWaitSpec(source_commit='9'*40).model_dump(mode='json')
    operation = operation_snapshot(HumanOperationPolicy(reaction_delay_seconds=60))
    opening_refs, openings = [], []
    for index,profile in enumerate(study.PROFILES):
        aid = 'own_' + profile['arm_id']
        quantity = 100*(index+1)
        cash = str(100000+index*10000)
        opening = {'schema_version':'localsim_natural_period_opening_v1',**profile,
            'account_id':aid,'parent_account_id':'history_'+profile['arm_id'], 'cash_cny':cash,
            'origin_initial_cash_cny':'10000000','new_capital_added_cny':'0',
            'positions':{'600000.SH':{'portfolio_id':aid,'symbol':'600000.SH','quantity':quantity,
                'available_quantity':quantity,'avg_cost':9.9,'trade_date':'2026-09-30'}},
            'opening_marks_raw_cny':{'600000.SH':'10'},'opening_nav_cny':str(int(cash)+quantity*10),
            'delay_seconds':60,'operation':operation,
            'analysis_anchor':{'prior_nav_high_water_cny':'11000000','prior_cumulative_max_drawdown':'0.2',
                               'prior_cumulative_fees_cny':str(100+index)}}
        opening_refs.append(put(sources/(profile['arm_id']+'.json'),opening))
        openings.append(opening)
    dummy = put(sources/'dummy.json',{'fixture':'synthetic historical-source admission only'})
    frozen_fields = {'preflight':dummy,'preparation_request':dummy,'initial_accounts':opening_refs,
        'price_templates':original['price_templates'],'account_count':4,'profiles':study.PROFILES,
        'new_capital_added_cny':'0','jan_september_account_days_recomputed':0,'trade_dates':stamps,
        'cost_policy':p['inputs']['cost_policy'],'candidate_manifest':old['candidate_manifest'],
        'parent_candidate_manifest':old['candidate_manifest'],'selection_inventory':p['selection_inventory'],
        'package_id':old['package_id'],'manifest_sha256':old['manifest_sha256'], 'control':control,
        'execution_contract':old['execution_contract'],'primary_delay_seconds':60,'decision_ttl_seconds':90,
        'common_terminal_local':'14:57','wait_budget_minutes':15,'price_research_threshold_fraction':'0.005',
        'native_target_count':850}
    contract = {'request_frozen_fields':frozen_fields}
    contract_ref = put(sources/'contract.json',contract)
    delivery = {'consumer_contract':contract_ref,'input_preparation_delivery':dummy,'price_manifest':dummy}
    delivery_ref = put(sources/'delivery.json',delivery)
    pins = study._pins(frozen_fields,delivery_ref,delivery,base_ref,
        [r.model_dump() for r in inspected['source_refs']], [row['native'] for row in native_days.values()])
    inspected['source_refs'] = pins
    c = {'openings':openings,'request':{'control':control},'package':p['inputs']['package']}
    monkeypatch.setattr(study,'_sources',lambda *_:(delivery,contract,c,inspected,base_ref,'7'*40))
    frozen = study.freeze(source_delivery=FileRef.model_validate(delivery_ref),
        output_root=tmp_path.with_name(tmp_path.name+'-natural-output'),producer_commit='8'*40)
    return FileRef.model_validate(frozen['request']),openings,p


def test_native_four_account_prefix_resume_release_identity_and_immutable_retry(natural,monkeypatch):
    request_ref,openings,source = natural
    request = study.read(request_ref)
    root = Path(request_ref.path).parent
    before = {Path(r['path']):Path(r['path']).read_bytes() for r in request['initial_accounts']}
    calls = []
    engine = dual.execute_account_days

    def recording(**kwargs):
        calls.append({k:kwargs[k] for k in ('research_controls','research_control_specs')})
        return engine(**kwargs)

    monkeypatch.setattr(dual,'execute_account_days',recording)
    options = ResearchPerformanceOptions(optimized=True)
    first = study.run(request_ref,performance=options,stop_after_account_days=4)
    assert first['status']=='CHECKPOINTED' and first['computed_account_days']==4
    assert study.inspect(request_ref,allow_prefix=True)['verified_account_days']==4
    final = study.run(request_ref,performance=options)
    assert final['computed_account_days']==64 and final['verified_account_days']==68
    assert calls[-1]['research_controls']=={'own_W15':'WAIT_BUDGET_CONTROL_15M_V1','own_P2':'ADVERSE_TREND_WAIT_15M_V1'}
    assert calls[-1]['research_control_specs']=={'own_P2':request['control']}
    assert request['timing_source_commit']=='7'*40 != request['p2_timing_source_commit']
    facts = study.read(final['facts'])
    for account,opening in zip(facts['accounts'],openings,strict=True):
        assert account['account_id']==opening['account_id'] and len(account['days'])==17
        day = study.read(account['days'][0])
        assert Decimal(day['initial_account']['cash_cny'])==Decimal(opening['cash_cny'])
        assert day['initial_account']['positions']==opening['positions']
    assert all(path.read_bytes()==value for path,value in before.items())

    def forbidden(*args,**kwargs):
        pytest.fail('artifact retry entered market preparation or account execution')

    monkeypatch.setattr(study,'CandidateReader',forbidden)
    monkeypatch.setattr(study,'_sources',forbidden)
    monkeypatch.setattr(dual,'execute_account_days',forbidden)
    retry = study.exact_artifact_retry(request_ref)
    after = {p:(p.read_bytes(),p.stat().st_mtime_ns) for p in root.rglob('*.json')}
    assert retry['verified_account_days']==68 and retry['recomputed_account_days']==retry['new_committed_account_days']==0
    assert study.exact_artifact_retry(request_ref)==retry
    assert study.run(request_ref)['status']=='ALREADY_MATERIALIZED'
    assert after=={p:(p.read_bytes(),p.stat().st_mtime_ns) for p in root.rglob('*.json')}
    # Source drift must fail before the new NAV/ledger inspection begins.
    monkeypatch.setattr(study,'_verify_days',forbidden)
    path = Path(source['inputs']['candidate_manifest']['path'])
    path.write_bytes(path.read_bytes()+b' ')
    with pytest.raises(IntradayError,match='SOURCE_IDENTITY'):
        study.inspect(request_ref)


@pytest.mark.parametrize('field',['base_timing_source_commit','p2_control_spec','initial_accounts'])
def test_forged_policy_or_opening_fails_before_writer_or_computation(natural,tmp_path,field):
    request_ref,_,_ = natural
    request = deepcopy(study.read(request_ref))
    if field=='base_timing_source_commit':
        request[field]=request['p2_timing_source_commit']
    elif field=='p2_control_spec':
        request[field]={**request[field],'source_commit':'6'*40}
    else:
        request[field]=list(reversed(request[field]))
    request.pop('identity')
    request.pop('release_identity')
    request['identity']=digest(request)
    request['release_identity']='localsim_natural_period_v1_'+request['identity'][:24]
    path=tmp_path/'forged'/request['identity']/'request.json'
    put(path,request)
    with pytest.raises(ValueError,match='REQUEST_BINDING'):
        study.run(FileRef.model_validate(ref(path)))
    assert not (path.parent/'study-writer.lock').exists()


@pytest.mark.parametrize('schema',['localsim_dual_mode_study_v1','localsim_period_study_v2','localsim_adverse_wait_history_request_v1'])
def test_legacy_request_cannot_enter_new_runner_or_retry(tmp_path,schema):
    path=tmp_path/'foreign.json'
    source=FileRef.model_validate(put(path,{'schema_version':schema}))
    for call in (study.run,study.exact_artifact_retry):
        with pytest.raises(ValueError,match='REQUEST_IDENTITY'):
            call(source)
    assert not list(tmp_path.glob('*lock'))


@pytest.mark.parametrize('field,value',[('trade_dates',['2026-09-30']*17),('native_target_count',849),
                                      ('new_capital_added_cny','1'),('primary_delay_seconds',0)])
def test_self_consistent_contract_cannot_expand_fixed_october_scope(natural,tmp_path,field,value):
    request_ref,_,_ = natural
    request=deepcopy(study.read(request_ref))
    contract=study.read(request['consumer_contract'])
    contract['request_frozen_fields'][field]=value
    request[field]=value
    request['consumer_contract']=put(tmp_path/'expanded_contract.json',contract)
    request.pop('identity')
    request.pop('release_identity')
    request['identity']=digest(request)
    request['release_identity']='localsim_natural_period_v1_'+request['identity'][:24]
    path=tmp_path/'expanded'/request['identity']/'request.json'
    put(path,request)
    with pytest.raises(ValueError,match='FIXED_SCOPE'):
        study.run(FileRef.model_validate(ref(path)))
    assert not (path.parent/'study-writer.lock').exists()


def test_wrong_delivery_binding_is_rejected_before_market_or_accounts(natural,tmp_path,monkeypatch):
    request_ref,_,_=natural
    request=deepcopy(study.read(request_ref))
    request['price_manifest']=put(tmp_path/'other_manifest.json',{'unrelated':True})
    request.pop('identity')
    request.pop('release_identity')
    request['identity']=digest(request)
    request['release_identity']='localsim_natural_period_v1_'+request['identity'][:24]
    path=tmp_path/'wrong_delivery'/request['identity']/'request.json'
    put(path,request)

    def forbidden(*args,**kwargs):
        pytest.fail('invalid delivery entered market preparation')

    monkeypatch.setattr(study,'CandidateReader',forbidden)
    with pytest.raises(ValueError,match='RUN_DELIVERY_BINDING'):
        study.run(FileRef.model_validate(ref(path)))
    assert not (path.parent/'raw_source_inspection.json').exists()


def test_natural_price_view_reuses_same_generic_validator_without_old13_alias(tmp_path,monkeypatch):
    _,preflight_ref,templates_ref,_=price_campaign_fixture(tmp_path,monkeypatch)
    old=campaign.inspect_price_templates(preflight_ref,templates_ref)
    p=study.read(preflight_ref)
    p['schema_version']='localsim_natural_period_preflight_v1'
    source=FileRef.model_validate(put(tmp_path/'natural_preflight.json',p))
    c={'preflight':p,'delivery':{'preflight':source.model_dump()},'package':p['inputs']['package'],
       'request':{**p,**p['inputs'],'trade_dates':[str(d) for d in old['days']]},
       'openings':[{}, {}, {}, {}],'historical_sell_symbols':[]}
    actual=campaign.inspect_price_templates(source,templates_ref,natural_context=c)
    assert actual['days']==old['days'] and actual['reference_counts']==old['reference_counts']
    assert study.read(source)['schema_version']=='localsim_natural_period_preflight_v1'
    c['delivery']['preflight']=preflight_ref.model_dump()
    with pytest.raises(ValueError,match='NATURAL_PRICE_CONTEXT'):
        campaign.inspect_price_templates(source,templates_ref,natural_context=c)
