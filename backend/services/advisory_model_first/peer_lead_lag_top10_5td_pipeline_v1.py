"""Bounded conditional peer-lag research, independent of the stopped E2 study."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import time

import pandas as pd
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first import peer_lead_lag_top10_5td_v1 as model
from backend.services.advisory_model_first import peer_residual_top10_5td_pipeline_v1 as shared
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import build_trial_record

previous = shared.previous


def implementation_sha256():
    names = ('peer_lead_lag_top10_5td_v1', 'peer_lead_lag_top10_5td_pipeline_v1')
    return model.sha(dict(shared=shared.implementation_sha256(), files={name:hashlib.sha256(
        Path(__file__).with_name(name+'.py').read_bytes().replace(b'\r\n', b'\n')).hexdigest() for name in names}))


class Study(shared.Study):
    """Reuse only immutable storage, financial projection, claims and resource guards."""
    def __init__(self, root, plan=None):
        for key in ('start', 'train_end', 'outer_start', 'end', 'max_fits', 'max_seconds', 'max_rss_bytes', 'max_output_bytes'):
            if model.CONTRACT[key] != shared.model.CONTRACT[key]:
                raise ValueError('reused infrastructure contract differs')
        self.root = previous.node_path(root)
        if not (self.root.as_posix().lower().startswith('/mnt/x/') or self.root.drive.upper() == 'X:'):
            raise ValueError('output must be on X')
        self.plan = plan or previous.load(self.root/'preregistered/plan.json')
        identity = 'advleadlagtop10_'+model.sha({k:v for k, v in self.plan.items() if k != 'experiment_id'})[:24]
        if (self.plan['experiment_id'] != identity or self.plan['contract'] != json.loads(_json_bytes(model.CONTRACT))
                or self.plan['implementation_sha256'] != implementation_sha256()
                or not re.fullmatch('[0-9a-f]{40}', self.plan['source_commit']) or self.plan['versions'] != previous.versions()):
            raise ValueError('plan, implementation or environment identity differs')
        self.source = previous.Study(self.plan['source_root'])
        self.verify_source()
        self.digest, self.started = model.sha(self.plan), time.monotonic()

    def register(self, relative, *, outer=False, evaluated=False):
        record = build_trial_record(experiment_id=self.plan['experiment_id'], attempt_id='exact_v1',
            research_stage=relative.replace('/', '_').upper(), study_type='EXPLORATORY_SCREEN', objective_contract='ALPHA_RANKING',
            decision_use='NAVIGATION_ONLY', hypothesis_family_id=model.CONTRACT['hypothesis'],
            unique_variable='CONDITIONAL_PEER_LAG1_INTERACTION', parent_lineage=(self.source.plan['experiment_id'],),
            dataset_identity=self.plan['binding_hash'], schema_identity=model.sha(model.CONTRACT),
            policy_identity=model.base.VALUATION_POLICY_SHA256, planned_trial_count=1,
            generated_trial_count=int(evaluated), evaluated_trial_count=int(evaluated), selected_trial_count=0,
            result_class='EXPLORATORY', consumed_windows=(dict(window_id='PREVIOUSLY_CONSUMED_DEVELOPMENT',
                dataset_identity=self.plan['binding_hash'], start_date=model.CONTRACT['start'],
                end_date=model.CONTRACT['end'] if outer else model.CONTRACT['train_end']),),
            evidence_refs=(evidence_reference_for_file(self.root/relative/'manifest.json', role='peer_lead_lag_'+relative.replace('/', '_')),))
        AdvisoryResearchTrialRegistryV1(self.root.parent/'trial_registry.jsonl').append_batch((record,))


def read_panel(source_plan, inputs, budget):
    dataset = shared.file_dataset(source_plan)
    try:
        calendar = pd.DatetimeIndex(dataset.calendar)
        first, last = inputs[model.base.KEY[0]].min(), inputs[model.base.KEY[0]].max()
        index = calendar.get_loc(first)
        if index < 66:
            raise ValueError('67-price history unavailable')
    finally:
        dataset.close()
    # Range-only query extends the existing 65-lookback reader by one session.
    # It is not a candidate roster; original rows/identities never pass through it.
    symbols = sorted(set(inputs.instrument))
    query = pd.DataFrame({'instrument':symbols, model.base.KEY[0]:calendar[index-1]})
    query.loc[len(query)] = [symbols[0], last]
    return shared.read_panel(source_plan, query, budget)


def preregister(output, source_root, commit):
    source = previous.Study(source_root)
    plan = dict(contract=model.CONTRACT, source_root=str(source.root), source_stage_sha256=source.read('evaluated'),
        binding_hash=source.plan['binding']['binding_hash'], source_commit=commit,
        implementation_sha256=implementation_sha256(), versions=previous.versions(), node_python_uri=source.plan['node_python_uri'])
    plan = json.loads(_json_bytes(plan))
    plan['experiment_id'] = 'advleadlagtop10_'+model.sha(plan)[:24]
    study = Study(previous.node_path(output)/plan['experiment_id'], plan)
    study.publish('preregistered', None, {'plan.json':_json_bytes(plan)})
    study.register('preregistered')
    return study.root


def prepare(study, parent, *, outer=False, progress=lambda event: None):
    relative = ('outer' if outer else 'inner')+'/prepared'
    path = study.root/relative
    if path.exists():
        digest = study.read(relative, parent)
        shared.verify_input_files(study, previous.load(path/'receipt.json'))
        return pd.read_parquet(path/'inputs.parquet'), digest
    first = model.CONTRACT['outer_start'] if outer else model.CONTRACT['start']
    last = model.CONTRACT['end'] if outer else model.CONTRACT['train_end']
    inputs = pd.read_parquet(study.source.root/'prepared/inputs.parquet', filters=[
        (model.base.KEY[0], '>=', pd.Timestamp(first)), (model.base.KEY[0], '<=', pd.Timestamp(last))])
    if len(inputs) > model.CONTRACT['max_rows']:
        raise ValueError('original population budget exceeded')
    frames, calendar, receipt = read_panel(study.source.plan, inputs, study.budget)
    frame = model.attach(inputs, frames['close'], frames['factor'], calendar, progress)
    receipt.update(original_rows=len(frame), original_days=int(frame[model.base.KEY[0]].nunique()),
        common_supported_rows=int(model.support(model.numeric(frame)).sum()),
        statistical_estimates=int(frame.lag_parameter_estimated.sum()),
        reasons={str(k):int(v) for k, v in frame.lag_reason.value_counts().items()},
        elapsed_seconds=time.monotonic()-study.started, peak_rss_bytes=previous.peak_memory_bytes())
    digest = study.publish(relative, parent, {'inputs.parquet':parquet_bytes(frame), 'receipt.json':_json_bytes(receipt)})
    progress(dict(phase=relative, rows=len(frame), seconds=receipt['elapsed_seconds']))
    return frame, digest


def fit_pair(study, parent, inputs, targets, *, prefix, cutoff, first, last, progress, evaluate=True):
    predictions, models, train = {}, {}, None
    for arm in ('matched', 'candidate'):
        key = prefix+'_'+arm
        relative = key+'/trained'
        path = study.root/relative
        if path.exists():
            parent = study.read(relative, parent)
            study.claim(key, parent)
        else:
            if (study.root/'fit_claims'/(key+'_started.json')).exists():
                raise ValueError('interrupted unpublished fit: no automatic refit')
            if train is None:
                finance = study.finance(model.CONTRACT['start'], cutoff, cutoff=cutoff)
                selected = inputs.loc[inputs[model.base.KEY[0]].le(pd.Timestamp(cutoff))]
                train = selected.merge(finance, on=model.KEY, validate='one_to_one', indicator=True)
                if not train['_merge'].eq('both').all():
                    raise ValueError('training finance population differs')
                train = train.drop(columns='_merge')
            packet = model.fit(train, cutoff, arm, before_fit=lambda: study.claim(key))
            prediction = model.predict(targets, packet)
            parent = study.publish(relative, parent, {'model.json':_json_bytes(packet), 'predictions.parquet':parquet_bytes(prediction)})
            study.claim(key, parent)
            progress(dict(phase='FIT_COMPLETED', key=key, train_rows=packet['train_rows'], iterations=packet['iterations']))
        models[arm] = previous.load(path/'model.json')
        predictions[arm] = pd.read_parquet(path/'predictions.parquet')
        model.evaluation.same_population(targets, predictions[arm])
        if models[arm]['cutoff'] != cutoff or models[arm]['arm'] != arm:
            raise ValueError('persisted fit clock/arm differs')
    if models['matched']['training_keys_sha256'] != models['candidate']['training_keys_sha256']:
        raise ValueError('arms used different supervised support')
    if not predictions['matched'].no_rank_advice.equals(predictions['candidate'].no_rank_advice):
        raise ValueError('arms used different prediction support')
    if not evaluate:
        return None, parent
    # No validation or outer financial decode until both predictions are immutable.
    finance = study.finance(first, last, cutoff=last, outer=prefix == 'final')
    return model.panel(predictions['candidate'], predictions['matched'], finance), parent


def evaluate_inner(study, parent, inputs, progress):
    panels = []
    path = study.root/'inner/evaluated'
    for fold, (cutoff, first, last) in enumerate(model.base.FOLDS, 1):
        targets = inputs.loc[inputs[model.base.KEY[0]].between(pd.Timestamp(first), pd.Timestamp(last))].reset_index(drop=True)
        cohort, parent = fit_pair(study, parent, inputs, targets, prefix='fold'+str(fold), cutoff=cutoff,
            first=first, last=last, progress=progress, evaluate=not path.exists())
        if cohort is not None:
            cohort['fold'] = fold
            panels.append(cohort)
    if path.exists():
        parent = study.read('inner/evaluated', parent)
        return previous.load(path/'report.json'), parent
    cohorts = pd.concat(panels, ignore_index=True)
    report, daily = model.summarize(cohorts)
    coefficients, gamma = {}, {}
    for fold, (cutoff, _, _) in enumerate(model.base.FOLDS, 1):
        packet = previous.load(study.root/f'fold{fold}_candidate/trained/model.json')
        coefficients[str(fold)] = packet['coef'][packet['fields'].index(model.NEW)]
        gamma[str(fold)] = inputs.loc[inputs[model.base.KEY[0]].le(pd.Timestamp(cutoff)), 'lag_peer_gamma60'].quantile([.1,.5,.9]).to_dict()
    # Attribution only: neither sign nor matched delta can override the native endpoint.
    report.update(interaction_coefficients=coefficients, training_gamma_quantiles=gamma)
    parent = study.publish('inner/evaluated', parent, {'report.json':_json_bytes(report),
        'panel.parquet':parquet_bytes(cohorts), 'daily.parquet':parquet_bytes(daily)})
    return report, parent


def run(root, progress=lambda event: None):
    study = Study(root)
    study.node()
    with _exclusive_file_lock(study.root/'study.lock'), threadpool_limits(limits=2):
        parent = study.read('preregistered', None)
        inputs, parent = prepare(study, parent, progress=progress)
        inner, parent = evaluate_inner(study, parent, inputs, progress)
        study.register('inner/evaluated', evaluated=True)
        report, go = inner, inner['proceed_outer']
        if go:
            targets, parent = prepare(study, parent, outer=True, progress=progress)
            cohorts, parent = fit_pair(study, parent, inputs, targets, prefix='final', cutoff=model.CONTRACT['train_end'],
                first=model.CONTRACT['outer_start'], last=model.CONTRACT['end'], progress=progress,
                evaluate=not (study.root/'outer/evaluated').exists())
            if (study.root/'outer/evaluated').exists():
                parent = study.read('outer/evaluated', parent)
                report = previous.load(study.root/'outer/evaluated/report.json')
            else:
                report, daily = model.summarize(cohorts)
                parent = study.publish('outer/evaluated', parent, {'report.json':_json_bytes(report),
                    'panel.parquet':parquet_bytes(cohorts), 'daily.parquet':parquet_bytes(daily)})
            study.register('outer/evaluated', outer=True, evaluated=True)
        expected = {f'{prefix}_{arm}' for prefix in (('fold1','fold2','fold3','final') if go else ('fold1','fold2','fold3'))
            for arm in ('matched','candidate')}
        for suffix in ('_started.json','_finished.json'):
            if {p.name.removesuffix(suffix) for p in (study.root/'fit_claims').glob('*'+suffix)} != expected:
                raise ValueError('actual fit identities differ from terminal state')
        study.verify_source()
        if (study.root/'evaluated').exists():
            study.read('evaluated', parent)
            return {**previous.load(study.root/'evaluated/result.json'), 'exact_retry':True}
        result = dict(experiment_id=study.plan['experiment_id'], classification=report['classification'],
            inner=inner, outer=report if go else None, physical_fit_count=len(expected), prior_physical_fits=model.CONTRACT['prior_fits'],
            cumulative_physical_fits=model.CONTRACT['prior_fits']+len(expected), historical_index_count=1,
            new_information_candidates=0, representation_candidates=1, outer_finance_decoded=go, sealed_finance_decoded=False,
            sql_count=0, actual_fill_proven=False, activation_evidence=False, independent_oos_evidence=False,
            elapsed_seconds=time.monotonic()-study.started, peak_rss_bytes=previous.peak_memory_bytes(),
            previous_output_bytes=sum(p.stat().st_size for p in study.root.rglob('*') if p.is_file()),
            consumed_range_end=model.CONTRACT['end'] if go else model.CONTRACT['train_end'], code_commit=study.plan['source_commit'])
        result['input_statistical_estimates'] = sum(previous.load(study.root/(stage+'/prepared/receipt.json'))['statistical_estimates']
            for stage in (('inner','outer') if go else ('inner',)))
        study.publish('evaluated', parent, {'result.json':_json_bytes(result)})
        study.register('evaluated', outer=go, evaluated=True)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    register = commands.add_parser('preregister')
    for name in ('output','source-root','source-commit'):
        register.add_argument('--'+name, required=True)
    execute = commands.add_parser('run')
    execute.add_argument('--study-root', required=True)
    args = parser.parse_args()
    if args.command == 'preregister':
        print(json.dumps(dict(study_root=str(preregister(args.output,args.source_root,args.source_commit)))), flush=True)
    else:
        result = run(args.study_root, lambda event: print(json.dumps(event), flush=True))
        print(json.dumps({k:result[k] for k in ('experiment_id','classification','physical_fit_count','outer_finance_decoded',
            'elapsed_seconds','peak_rss_bytes')}), flush=True)


if __name__ == '__main__':
    main()
