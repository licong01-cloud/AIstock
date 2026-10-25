"""Artifact-only preregistration/run/retry for one joint-risk research candidate."""
from __future__ import annotations

import json
from pathlib import Path
import re
import time

import numpy as np
import pandas as pd
import psutil
import sklearn
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first.economic_entry_pipeline import (
    _json_bytes, file_sha256, publish_stage, read_stage,
)
from backend.services.advisory_model_first.group_centered_rank_5td_study_v1 import FINANCE
from backend.services.advisory_model_first.joint_risk_top10_5td_inputs_v1 import (
    CONTRACT, POLICY_SHA256, ROSTER, load_panel, validate_roster,
)
from backend.services.advisory_model_first.joint_risk_top10_5td_study_v1 import (
    audit_skill, evaluate_economics, predict,
)
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def _json(path):
    return json.loads(Path(path).read_bytes())


def run_study(*, source_root, source_plan_sha256, source_prepared_sha256, binding, output_root, source_commit):
    source,output = Path(source_root).resolve(),Path(output_root).resolve()
    if (re.fullmatch(r'[0-9a-f]{40}',source_commit) is None or not Path(output_root).is_absolute()
            or output.drive.upper()=='C:' or str(output).lower().startswith('/mnt/c/')
            or output==source or output in source.parents or source in output.parents):
        raise ValueError('explicit producer commit and separate non-C output required')
    parent = read_stage(source/'preregistered',stage='preregistered',plan_sha256=source_plan_sha256,parent_sha256=None)
    prepared = read_stage(source/'prepared',stage='prepared',plan_sha256=source_plan_sha256,parent_sha256=parent['stage_sha256'])
    if prepared['stage_sha256']!=source_prepared_sha256:
        raise ValueError('parent prepared identity mismatch')
    old = _json(source/'preregistered/plan.json')
    dates = {k:old[k] for k in ('train_start','train_end','evaluation_start','evaluation_end')}
    if not dates['train_start']<=dates['train_end']<dates['evaluation_start']<=dates['evaluation_end']:
        raise ValueError('invalid original date split')
    names = ('joint_risk_top10_5td_inputs_v1.py','joint_risk_top10_5td_study_v1.py',Path(__file__).name,
             'economic_entry_pipeline.py','research_control.py','research_control_contracts.py',
             'group_centered_rank_5td_study_v1.py','generic_price_5td_contracts_v1.py',
             'generic_price_5td_valuation_v2.py','parent_score_price_5td_evaluation_v1.py',
             'parent_score_price_5td_contracts_v1.py','parent_score_price_5td_inputs_v1.py')
    plan = dict(contract=CONTRACT,policy_sha256=POLICY_SHA256,source_root=source.as_posix(),
        source_plan_sha256=source_plan_sha256,source_prepared_sha256=source_prepared_sha256,
        source_commit=source_commit,implementation_sha256=sha({n:file_sha256(Path(__file__).with_name(n)) for n in names}),
        binding_hash=binding['binding_hash'],versions=dict(numpy=np.__version__,pandas=pd.__version__,sklearn=sklearn.__version__),
        sources=[{k:s[k] for k in ('package_id','manifest_sha256','run_id')} for s in old['sources']],
        inherited_evidence='QE_FROZEN_BACKTEST_SCORE_CURRENT_DATABASE_NON_VINTAGE_HISTORICAL_UNIVERSE_UNPROVEN',
        cumulative_prior_research_fits=160,supervised_fit_count=0,planned_candidate_count=1,**dates)
    digest = sha(plan)
    root = output/('advjointrisk_'+digest[:24])
    root.mkdir(parents=True,exist_ok=True)
    registry = AdvisoryResearchTrialRegistryV1(output/'trial_registry.jsonl')
    started,peak_rss = time.monotonic(),0

    def budget():
        nonlocal peak_rss
        memory = psutil.Process().memory_info()
        peak_rss = max(peak_rss,memory.rss,getattr(memory,'peak_wset',0))
        if peak_rss>CONTRACT['max_rss_bytes'] or time.monotonic()-started>CONTRACT['max_seconds']:
            raise ValueError('joint risk memory/time budget exceeded')

    def publish(stage,parent_sha,artifacts,*,generated=0,evaluated=0,result_class='EXPLORATORY'):
        existing = sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
        if not (root/stage).exists() and existing+sum(map(len,artifacts.values()))+16384>CONTRACT['max_bytes']:
            raise ValueError('joint risk artifact size budget exceeded before publication')
        target = publish_stage(study_root=root,stage=stage,plan_sha256=digest,parent_sha256=parent_sha,artifacts=artifacts)
        registry.append_batch((build_trial_record(experiment_id=root.name,attempt_id='exact_v1',research_stage=stage.upper(),
            study_type='LEARNABILITY_AUDIT',objective_contract='RISK_MANAGED_ADVISORY',decision_use='NAVIGATION_ONLY',
            hypothesis_family_id='ADVISORY_GP5_ENTRY_VALUE',parent_lineage=('ADVISORY_GP5_ENTRY_VALUE',source.name),
            unique_variable='JOINT_RISK_TOP10_5TD_1',dataset_identity=sha([source_prepared_sha256,binding['binding_hash']]),
            schema_identity=CONTRACT['schema'],policy_identity=POLICY_SHA256,planned_trial_count=1,
            generated_trial_count=generated,evaluated_trial_count=evaluated,selected_trial_count=0,result_class=result_class,
            consumed_windows=(dict(window_id='CONSUMED_DEVELOPMENT_ONLY',dataset_identity=source_plan_sha256,
                start_date=dates['train_start'],end_date=dates['evaluation_end']),),
            evidence_refs=(evidence_reference_for_file(root/stage/'manifest.json',role='joint_risk_'+stage),)),))
        return _json(target/'manifest.json')['stage_sha256']

    with _exclusive_file_lock(root/'study.lock'),threadpool_limits(limits=2):
        budget()
        pre = publish('preregistered',None,{'plan.json':_json_bytes(plan),'binding.json':_json_bytes(binding)})
        full = pd.read_parquet(source/'prepared/rows.parquet',columns=list(ROSTER))
        days = pd.read_parquet(source/'prepared/days.parquet')
        calendar = _json(source/'prepared/calendar.json')
        roster = validate_roster(full,days,calendar,plan['sources'],dates['train_start'],dates['evaluation_end'])
        from backend.data_service.frozen_dataset import FrozenDataset
        dataset = FrozenDataset(binding,consumer_scope='ADVISORY',run_id=root.name)
        try:
            first = dataset.calendar.index(pd.Timestamp(dates['train_start']).date())-124
            if first<0:
                raise ValueError('risk history outside frozen calendar')
            start,end = dataset.calendar[first],pd.Timestamp(dates['evaluation_end']).date()
        finally:
            dataset.close()
        # Read in place once per invocation; comparison on exact retry verifies actual
        # consumed bytes, not just file names, metadata or a stale saved panel.
        panel,input_receipt = load_panel(binding,roster.instrument.unique(),start=start,end=end,run_id=root.name)
        budget()
        receipt = dict(original_full_rows=len(full),original_top20_rows=len(roster),original_package_days=len(days),
            source_prepared_sha256=source_prepared_sha256,input=input_receipt,evaluation_targets_derived=False)
        prep = publish('prepared',pre,{'receipt.json':_json_bytes(receipt),
            'roster.parquet':parquet_bytes(roster),'panel.parquet':parquet_bytes(panel),'days.parquet':parquet_bytes(days)})
        evaluation = roster.loc[roster.decision_as_of_trade_date.between(dates['evaluation_start'],dates['evaluation_end'])].reset_index(drop=True)
        if (root/'trained').exists():
            trained = read_stage(root/'trained',stage='trained',plan_sha256=digest,parent_sha256=prep)
            predictions = pd.read_parquet(root/'trained/predictions.parquet')
            models = _json(root/'trained/model.json')
        else:
            journal = root/'update_journal.json'
            if journal.exists():
                raise ValueError('partial statistical update exists; no implicit re-estimation')
            with journal.open('x',encoding='utf-8') as stream:
                json.dump(dict(plan_sha256=digest,state='STARTED'),stream)
            predictions,forecasts,count = predict(evaluation,panel,before_update=budget)
            budget()
            models = dict(forecasts=forecasts,statistical_updates=count,supervised_fit_count=0,
                candidate_count=1,evaluation_targets_derived=False)
            trained_sha = publish('trained',prep,{'predictions.parquet':parquet_bytes(predictions),
                'model.json':_json_bytes(models)},generated=1)
            trained = dict(stage_sha256=trained_sha)
        if (root/'evaluated').exists():
            read_stage(root/'evaluated',stage='evaluated',plan_sha256=digest,parent_sha256=trained['stage_sha256'])
            return dict(_json(root/'evaluated/evaluation.json'),artifact_root=root.as_posix(),exact_retry=True)
        # Only now derive future risk labels from the already frozen historical panel.
        # The cache contains multiple dates, but prediction sliced <= its own D.
        skill = audit_skill(evaluation,models['forecasts'],panel,evaluation_start=dates['evaluation_start'],evaluation_end=dates['evaluation_end'])
        economics = None
        if skill['worth_economic_evaluation']:
            finance = pd.read_parquet(source/'prepared/rows.parquet',columns=list(ROSTER)+list(FINANCE),
                filters=[('decision_as_of_trade_date','>=',pd.Timestamp(dates['evaluation_start'])),
                         ('decision_as_of_trade_date','<=',pd.Timestamp(dates['evaluation_end'])),('selection_effective_rank','<=',20)])
            dd = days.loc[days.decision_date.between(dates['evaluation_start'],dates['evaluation_end'])]
            economics = evaluate_economics(finance,dd,predictions,evaluation_end=dates['evaluation_end'])
        verified = read_stage(source/'prepared',stage='prepared',plan_sha256=source_plan_sha256,parent_sha256=parent['stage_sha256'])
        if verified['stage_sha256']!=source_prepared_sha256:
            raise ValueError('parent source changed during study')
        budget()
        result = dict(status=economics['status'] if economics else skill['status'],skill=skill,economics=economics,
            statistical_updates=models['statistical_updates'],supervised_fit_count=0,candidate_count=1,
            cumulative_prior_research_fits=160,experiment_id=root.name,plan_sha256=digest,
            forecast_stage_sha256=trained['stage_sha256'],source_prepared_sha256=source_prepared_sha256,
            input_content_sha256=input_receipt['input_content_sha256'],source_evidence=plan['inherited_evidence'],
            elapsed_seconds=time.monotonic()-started,peak_rss_bytes=peak_rss,input_bytes=input_receipt['bytes_read'],
            sql_count=0,backend_restart_required=False,deployable=False,objective_contract='RISK_MANAGED_ADVISORY',
            decision_use='NAVIGATION_ONLY',independent_oos_evidence=False)
        publish('evaluated',trained['stage_sha256'],{'evaluation.json':_json_bytes(result)},generated=1,evaluated=1,
            result_class='EXPLORATORY' if result['status']=='WORTH_CONFIRMING' else 'NEGATIVE')
        size = sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
        if size>CONTRACT['max_bytes']:
            raise ValueError('joint risk artifact size budget exceeded')
        return dict(result,artifact_root=root.as_posix(),exact_retry=False,disk_bytes=size)
