"""Immutable Advisory study, source-only public consumers, no QE/DB dispatch."""
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

import pandas as pd

from backend.services.advisory_model_first.economic_context_preparation_v1 import prepare_economic_context_v1
from backend.services.advisory_model_first.economic_context_value_contracts_v1 import ContextValuePlanV1
from backend.services.advisory_model_first.economic_context_value_training_v1 import (
    ContextValueFitV1, assemble_context_value_rows_v1, context_fit_identity_v1, train_context_value_v1,
)
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_pipeline import (
    _json_bytes, _parquet_bytes, file_sha256, publish_stage, read_stage,
)
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, ValueAnchorGapSupportV1, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.economic_value_anchor_labels_v1 import build_value_anchor_labels_v1
from backend.services.advisory_model_first.economic_value_anchor_pipeline_v1 import (
    _value_prices, value_anchor_implementation_sha256_v1, value_anchor_observations_v1, value_anchor_sources_v1,
)
from backend.services.advisory_model_first.economic_value_anchor_training_v1 import ValueAnchorStudyPlanV1
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

NAMES = tuple(f'economic_context_value_{name}_v1.py' for name in ('contracts','training','inference','pipeline','evaluation'))


def context_implementation_sha256_v1():
    files = {name:hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n',b'\n')).hexdigest() for name in NAMES}
    for name in ('economic_context_consumer_v1.py','economic_context_preparation_v1.py'):
        files[name] = hashlib.sha256(Path(__file__).with_name(name).read_bytes().replace(b'\r\n',b'\n')).hexdigest()
    return sha({'algorithm':'UTF8_LF_BYTES_V1','files':files,'value_source_and_shadow':value_anchor_implementation_sha256_v1()})


def _root(plan, output_root):
    declared = Path(output_root)
    root = declared.resolve()
    if not declared.is_absolute() or root != declared.absolute() or root.drive.upper() == 'C:':
        raise ValueError('context study needs an original absolute non-C root')
    return root/plan.experiment_id


def _publish(**kwargs):
    import psutil
    # Four registered stages each <=512MiB imply a <=2GiB published bundle.
    if (sum(len(body) for body in kwargs['artifacts'].values())>512*1024**2
            or psutil.Process().memory_info().rss>2*1024**3):
        raise ValueError('context stage exceeds registered artifact/RSS budget')
    return publish_stage(**kwargs)


def context_observations_v1(candidates,prices,references):
    observed=value_anchor_observations_v1(candidates,prices,references)
    joined=candidates.loc[:,KEY].merge(references.loc[:,KEY+['target_reference_raw_cny']],on=KEY,how='left',validate='one_to_one')
    joined=joined.merge(prices.loc[:,['trade_date','instrument','raw_open_cny']].rename(columns={'trade_date':KEY[1]}),on=KEY[1:],how='left',validate='many_to_one')
    # Use the same Decimal coordinate for train support and query ticks; IEEE
    # boundary noise must not turn an observed executable tick into UNKNOWN.
    observed['actual_gap_bps']=[float((Decimal(str(price))/Decimal(str(reference))-1)*10000) if pd.notna(gap) else None
        for gap,price,reference in zip(observed.actual_gap_bps,joined.raw_open_cny,joined.target_reference_raw_cny,strict=True)]
    return observed


def context_sources_v1(plan):
    if plan.implementation_sha256 != context_implementation_sha256_v1():
        raise ValueError('context implementation changed')
    # A source-validation adapter only: no legacy register/train/evaluate call.
    refs = {name:{**getattr(plan,name).model_dump(mode='json'),'role':role} for name,role in (
        ('parent_plan_ref','value_parent_plan'),('parent_prepared_manifest_ref','value_prices_snapshot'),('feature_manifest_ref','value_d_snapshot'))}
    adapter = ValueAnchorStudyPlanV1(**refs, implementation_sha256=value_anchor_implementation_sha256_v1())
    parent, frozen, identity, feature = value_anchor_sources_v1(adapter)
    context, summary = prepare_economic_context_v1(profile_path=plan.profile_path, profile_sha256=plan.profile_sha256,
        prepared_manifest_path=plan.parent_prepared_manifest_ref.artifact_uri,
        prepared_manifest_sha256=plan.parent_prepared_manifest_ref.sha256,
        universe_selection=plan.universe_selection, authority_root=plan.authority_root)
    if summary['frozen_parent']['package_id'] != identity.package_id:
        raise ValueError('context and model parent packages differ')
    return parent, frozen, identity, feature, context, summary


def _record(plan, root, parent, stage, evidence, *, generated=0, evaluated=0):
    config = parent.configuration
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id='fixed_context_value_attempt_v1',research_stage=stage,
        study_type=plan.study_type,hypothesis_family_id='economic_entry_price_value',
        parent_lineage=(parent.experiment_id,'H-VALUE-ANCHOR-1_STOPPED'),unique_variable='strict_causal_sector_classification_with_matched_price_conditioned_additive_heads',
        objective_contract=plan.objective_contract,dataset_identity=parent.dataset_identity,schema_identity=plan.schema_version,
        policy_identity=sha({'parent_source_policy':parent.policy_identity,'value_policy':value_anchor_policy_sha256_v1(),'cost':COST.policy_sha256}),
        planned_trial_count=1,generated_trial_count=generated,evaluated_trial_count=evaluated,selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(window_id='P0C_DEVELOPMENT_V1',dataset_identity=parent.dataset_identity,
            start_date=config.train_start,end_date=config.label_cutoff),),
        result_class='CONTROL_READY' if stage=='PREREGISTERED' else 'EXPLORATORY',decision_use='NAVIGATION_ONLY',
        evidence_refs=(evidence_reference_for_file(evidence,role='context_'+stage.lower()),))
    return AdvisoryResearchTrialRegistryV1(root.parent/'trial_registry.jsonl').append_batch((record,))


def _ledger(plan,root,stage,evidence):
    entries=[row for row in AdvisoryResearchTrialRegistryV1(root.parent/'trial_registry.jsonl').read()
        if row.experiment_id==plan.experiment_id and row.research_stage==stage]
    if len(entries)!=1 or len(entries[0].evidence_refs)!=1 or entries[0].evidence_refs[0].sha256!=file_sha256(evidence):
        raise ValueError('context durable stage and registry disagree')


def _source_receipt():
    repo=Path(__file__).resolve().parents[3]
    head=subprocess.run(['git','rev-parse','HEAD'],cwd=repo,check=True,capture_output=True).stdout.decode().strip()
    clean=subprocess.run(['git','diff','--quiet','HEAD','--','backend/services/advisory_model_first','backend/services/advisory_list_transition.py'],cwd=repo)
    if clean.returncode!=0:
        raise ValueError('context source dependencies differ from committed source')
    blobs={}
    for name in NAMES:
        path='backend/services/advisory_model_first/'+name
        body=subprocess.run(['git','show',f'{head}:{path}'],cwd=repo,check=True,capture_output=True).stdout
        if body.replace(b'\r\n',b'\n')!=(repo/path).read_bytes().replace(b'\r\n',b'\n'):
            raise ValueError('context source differs from committed Git blob')
        blobs[path]=hashlib.sha256(body).hexdigest()
    return {'head':head,'git_blob_content_sha256':blobs,'implementation_sha256':context_implementation_sha256_v1()}


def preregister_context_value_v1(*,plan,output_root):
    plan=ContextValuePlanV1.model_validate(plan)
    parent,_,_,_,_,summary=context_sources_v1(plan)
    root=_root(plan,output_root)
    if (root/'preregistered').exists():
        read_stage(root/'preregistered',stage='preregistered',plan_sha256=plan.plan_sha256,parent_sha256=None)
        _record(plan,root,parent,'PREREGISTERED',root/'preregistered/manifest.json')
        return root/'preregistered/plan.json'
    path=_publish(study_root=root,stage='preregistered',plan_sha256=plan.plan_sha256,parent_sha256=None,
        artifacts={'plan.json':_json_bytes(plan.model_dump(mode='json')),'recipe.json':_json_bytes(plan.parameters),
            'context_summary.json':_json_bytes(summary),'source_receipt.json':_json_bytes(_source_receipt())})
    _record(plan,root,parent,'PREREGISTERED',path/'manifest.json')
    return path/'plan.json'


def load_context_value_v1(*,plan_path,output_root):
    plan=ContextValuePlanV1.model_validate_json(Path(plan_path).read_text(encoding='utf-8'))
    root=_root(plan,output_root)
    if Path(plan_path).resolve()!=root/'preregistered/plan.json':
        raise ValueError('context requires the exact registered plan path')
    registered=read_stage(root/'preregistered',stage='preregistered',plan_sha256=plan.plan_sha256,parent_sha256=None)
    _ledger(plan,root,'PREREGISTERED',root/'preregistered/manifest.json')
    source=context_sources_v1(plan)
    pinned=json.loads((root/'preregistered/context_summary.json').read_text(encoding='utf-8'))
    if source[-1]['summary_sha256']!=pinned['summary_sha256']:
        raise ValueError('context source projection changed since registration')
    return plan,root,registered,source


def prepare_context_value_v1(*,plan_path,output_root):
    plan,root,registered,source=load_context_value_v1(plan_path=plan_path,output_root=output_root)
    parent,frozen,identity,feature,context,summary=source
    if (root/'prepared').exists():
        read_stage(root/'prepared',stage='prepared',plan_sha256=plan.plan_sha256,parent_sha256=registered['stage_sha256'])
        _record(plan,root,parent,'PREPARED',root/'prepared/manifest.json')
        return root/'prepared'
    rankings=pd.read_parquet(frozen/'frozen_rankings.parquet')
    candidates=rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)]
    if len(candidates)>plan.parameters['maximum_rows']:
        raise ValueError('context candidates exceed planned population budget')
    prices,refs=_value_prices(frozen,parent.configuration),pd.read_parquet(frozen/'references.parquet')
    labels=build_value_anchor_labels_v1(candidates=candidates,rankings=rankings,prices=prices,references=refs,
        calendar=json.loads((frozen/'calendar.json').read_text(encoding='utf-8')),parent_identity=identity)
    inputs=pd.read_parquet(feature/'features.parquet',columns=[*KEY,*D_FEATURES,'feature_visible_through','daily_input_sha256'])
    rows=assemble_context_value_rows_v1(inputs=inputs,labels=labels,observations=context_observations_v1(candidates,prices,refs),
        context=context,configuration=parent.configuration)
    from backend.services.advisory_model_first.economic_context_value_evaluation_v1 import context_train_noise_proxy_v1
    noise=context_train_noise_proxy_v1(rankings=rankings,prices=prices,
        calendar=json.loads((frozen/'calendar.json').read_text(encoding='utf-8')),configuration=parent.configuration)
    path=_publish(study_root=root,stage='prepared',plan_sha256=plan.plan_sha256,parent_sha256=registered['stage_sha256'],
        artifacts={'rows.parquet':_parquet_bytes(rows),'labels.parquet':_parquet_bytes(labels),'context_summary.json':_json_bytes(summary),
            'training_noise_proxy.json':_json_bytes(noise),
            'preparation.json':_json_bytes({'candidate_rows':len(rows),'decisions':rows[KEY[0]].nunique(),'fits':0,
                'source_evidence':identity.source_evidence,'limitations':identity.evidence_limitations,'sealed_accessed':False})})
    _record(plan,root,parent,'PREPARED',path/'manifest.json')
    return path


def train_context_value_study_v1(*,plan_path,output_root,qe_training_idle):
    if qe_training_idle is not True:
        raise ValueError('context fit cannot overlap QE training or unknown state')
    plan,root,registered,source=load_context_value_v1(plan_path=plan_path,output_root=output_root)
    parent=source[0]
    prepared=read_stage(root/'prepared',stage='prepared',plan_sha256=plan.plan_sha256,parent_sha256=registered['stage_sha256'])
    _ledger(plan,root,'PREPARED',root/'prepared/manifest.json')
    with _exclusive_file_lock(root/'fit.lock'):
        if (root/'trained').exists():
            read_stage(root/'trained',stage='trained',plan_sha256=plan.plan_sha256,parent_sha256=prepared['stage_sha256'])
            _record(plan,root,parent,'TRAINED',root/'trained/manifest.json',generated=1)
            return root/'trained'
        marker=root/'fit_attempt.json'
        if marker.exists():
            raise ValueError('context partial fit exists; no implicit refit')
        with marker.open('xb') as handle:
            handle.write(_json_bytes({'plan_sha256':plan.plan_sha256,'physical_fit_budget':4,'completion':'UNPROVEN'}))
            handle.flush()
            os.fsync(handle.fileno())
        _record(plan,root,parent,'FIT_STARTED',marker)
        started,count=time.monotonic(),0

        def before_fit(arm,head):
            import psutil
            nonlocal count
            if count>=4 or time.monotonic()-started>1800 or psutil.Process().memory_info().rss>2*1024**3:
                raise ValueError('context fit exceeds registered resource/fit budget')
            count+=1
            with (root/'fit_journal.jsonl').open('ab') as handle:
                handle.write(_json_bytes({'sequence':count,'arm':arm,'head':head,'state':'STARTED',
                    'time':datetime.now(timezone.utc).isoformat()}).replace(b'\n',b'')+b'\n')
                handle.flush()
                os.fsync(handle.fileno())

        fit=train_context_value_v1(rows=pd.read_parquet(root/'prepared/rows.parquet'),configuration=parent.configuration,before_fit=before_fit)
        if count!=4:
            raise ValueError('context physical fit count differs')
        metadata={'model_sha256':fit.model_sha256,'recipe':fit.recipe,'models':fit.models,
            'support':{code:asdict(value) for code,value in fit.support.items()},'diagnostics':fit.diagnostics,'parameters':plan.parameters}
        if time.monotonic()-started>1800:
            raise ValueError('context completed fit exceeded time budget; no publication')
        path=_publish(study_root=root,stage='trained',plan_sha256=plan.plan_sha256,parent_sha256=prepared['stage_sha256'],
            artifacts={'metadata.json':_json_bytes(metadata)})
        _record(plan,root,parent,'TRAINED',path/'manifest.json',generated=1)
        return path


def load_context_value_fit_v1(*,plan_path,output_root):
    plan,root,registered,_=load_context_value_v1(plan_path=plan_path,output_root=output_root)
    prepared=read_stage(root/'prepared',stage='prepared',plan_sha256=plan.plan_sha256,parent_sha256=registered['stage_sha256'])
    read_stage(root/'trained',stage='trained',plan_sha256=plan.plan_sha256,parent_sha256=prepared['stage_sha256'])
    _ledger(plan,root,'TRAINED',root/'trained/manifest.json')
    body=json.loads((root/'trained/metadata.json').read_text(encoding='utf-8'))
    support={code:ValueAnchorGapSupportV1(tuple(tuple(pair) for pair in value['intervals_bps'])) for code,value in body['support'].items()}
    if body['parameters']!=plan.parameters or context_fit_identity_v1(body['recipe'],body['models'],support)!=body['model_sha256']:
        raise ValueError('context fitted metadata/parameters changed')
    return ContextValueFitV1(body['recipe'],body['models'],support,body['diagnostics'],body['model_sha256'])
