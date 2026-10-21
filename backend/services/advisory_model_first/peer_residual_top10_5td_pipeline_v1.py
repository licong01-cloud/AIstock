"""One immutable peer-residual study; no production activation or SQL."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from backend.data_service.frozen_dataset import inside, stamp
from backend.services.advisory_model_first import multiperiod_top10_5td_pipeline_v1 as previous
from backend.services.advisory_model_first import peer_residual_top10_5td_v1 as model
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import file_dataset
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.prequential_top10_5td_study_v1 import projected_finance
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import build_trial_record


def implementation_sha256():
    names = ('peer_residual_top10_5td_v1', 'peer_residual_top10_5td_pipeline_v1',
        'native_anchored_top10_5td_v1', 'prequential_top10_5td_study_v1')
    return model.sha(dict(previous=previous.implementation_sha256(), files={n:hashlib.sha256(
        Path(__file__).with_name(n+'.py').read_bytes().replace(b'\\r\\n', b'\\n')).hexdigest() for n in names}))


def read_panel(source_plan, inputs, budget):
    """FrozenDataset locator override; open each stock/field once, bounded reads."""
    dataset = file_dataset(source_plan)
    symbols = sorted(set(inputs.instrument))
    try:
        calendar = pd.DatetimeIndex(dataset.calendar)
        first_d, last_d = inputs[model.base.KEY[0]].min(), inputs[model.base.KEY[0]].max()
        first, last = calendar.get_loc(first_d)-65, calendar.get_loc(last_d)
        if first < 0 or (last-first+1)*len(symbols) > 2_000_000:
            raise ValueError('history unavailable or 2M input cell budget exceeded')
        root = dataset.file('day_meta_export').parent
        arrays = {f:np.full((last-first+1,len(symbols)), np.nan) for f in ('close','factor')}
        pins = []
        for j, symbol in enumerate(symbols):
            budget()
            dataset._symbol(symbol)
            for field, values in arrays.items():
                rel = (root.relative_to(dataset.root)/'features'/symbol.lower()/f'{field}.day.bin').as_posix()
                path = inside(dataset.root, rel)
                before = stamp(path)
                if dataset._bin_lease.get(rel) != before or before[0] < 4 or before[0] % 4:
                    raise ValueError('daily generation or size mismatch')
                with path.open('rb') as stream:
                    header = stream.read(4)
                    offset = float(np.frombuffer(header, dtype='<f4')[0])
                    length = before[0]//4-1
                    if not np.isfinite(offset) or offset < 0 or offset != int(offset) or offset+length > len(calendar):
                        raise ValueError('invalid daily header')
                    lo, hi = max(first,int(offset)), min(last+1,int(offset)+length)
                    content = b''
                    if hi > lo:
                        stream.seek(4*(1+lo-int(offset)))
                        content = stream.read(4*(hi-lo))
                        if len(content) != 4*(hi-lo):
                            raise ValueError('truncated daily input')
                        values[lo-first:hi-first,j] = np.frombuffer(content, dtype='<f4')
                if stamp(path) != before:
                    raise ValueError('daily data drift during read')
                pins.append(dict(path=rel, stat=list(before), start_index=first, end_index=last,
                    bin_offset=int(offset), consumed_bytes=len(header)+len(content),
                    consumed_sha256=hashlib.sha256(header+content).hexdigest()))
        for pin in pins:
            if stamp(inside(dataset.root,pin['path'])) != tuple(pin['stat']):
                raise ValueError('daily data drift after read')
        dataset.assert_unchanged()
        dates = calendar[first:last+1]
        frames = {f:pd.DataFrame(v,index=dates,columns=symbols) for f,v in arrays.items()}
        return frames, calendar, dict(binding_hash=dataset.identity, pins=pins,
            consumed_bytes=sum(x['consumed_bytes'] for x in pins), consumed_slice_sha256=model.sha(pins),
            identity_semantics='PINNED_METADATA_STAT_LEASE_AND_CONSUMED_SLICES_NOT_FULL_BIN_HASH', sql_count=0)
    finally:
        dataset.close()


def verify_input_files(study, receipt):
    """Recovery checks original locator/generation; it does not resolve active profile."""
    dataset = file_dataset(study.source.plan)
    try:
        if receipt['binding_hash'] != dataset.identity:
            raise ValueError('input binding drift')
        for pin in receipt['pins']:
            path = inside(dataset.root, pin['path'])
            if stamp(path) != tuple(pin['stat']) or dataset._bin_lease.get(pin['path']) != tuple(pin['stat']):
                raise ValueError('input file drift on recovery')
            lo = max(pin['start_index'], pin['bin_offset'])
            hi = min(pin['end_index']+1, pin['bin_offset']+pin['stat'][0]//4-1)
            with path.open('rb') as stream:
                header = stream.read(4)
                content = b''
                if hi > lo:
                    stream.seek(4*(1+lo-pin['bin_offset']))
                    content = stream.read(4*(hi-lo))
            if hashlib.sha256(header+content).hexdigest() != pin['consumed_sha256'] or stamp(path) != tuple(pin['stat']):
                raise ValueError('input consumed content drift on recovery')
        dataset.assert_unchanged()
    finally:
        dataset.close()


class Study:
    def __init__(self, root, plan=None):
        self.root = previous.node_path(root)
        if not (self.root.as_posix().lower().startswith('/mnt/x/') or self.root.drive.upper() == 'X:'):
            raise ValueError('output must be on X')
        self.plan = plan or previous.load(self.root/'preregistered/plan.json')
        identity = 'advpeertop10_'+model.sha({k:v for k, v in self.plan.items() if k != 'experiment_id'})[:24]
        if (self.plan['experiment_id'] != identity or self.plan['contract'] != json.loads(_json_bytes(model.CONTRACT))
                or self.plan['implementation_sha256'] != implementation_sha256()
                or not re.fullmatch('[0-9a-f]{40}', self.plan['source_commit']) or self.plan['versions'] != previous.versions()):
            raise ValueError('plan, implementation or environment identity differs')
        self.source = previous.Study(self.plan['source_root'])
        self.verify_source()
        self.digest, self.started = model.sha(self.plan), time.monotonic()

    def verify_source(self):
        # Hash verification is not financial value decoding.
        if (self.source.read('evaluated') != self.plan['source_stage_sha256']
                or self.source.plan['binding']['binding_hash'] != self.plan['binding_hash']):
            raise ValueError('frozen source identity drift')

    def budget(self):
        if previous.peak_memory_bytes() > model.CONTRACT['max_rss_bytes'] or time.monotonic()-self.started > model.CONTRACT['max_seconds']:
            raise ValueError('2GiB/30-minute resource budget exceeded')

    def node(self):
        if os.name == 'nt' or Path(sys.executable).resolve() != Path(self.plan['node_python_uri']).resolve():
            raise ValueError('use the frozen existing WSL environment')

    def read(self, relative, parent):
        path = self.root/relative
        return read_stage(path, stage=path.name, plan_sha256=self.digest, parent_sha256=parent)['stage_sha256']

    def publish(self, relative, parent, files):
        self.budget()
        if sum(map(len, files.values()))+sum(p.stat().st_size for p in self.root.rglob('*') if p.is_file()) > model.CONTRACT['max_output_bytes']:
            raise ValueError('200MiB output budget exceeded')
        path = self.root/relative
        publish_stage(study_root=path.parent, stage=path.name, plan_sha256=self.digest, parent_sha256=parent, artifacts=files)
        return self.read(relative, parent)

    def finance(self, first, last, *, cutoff, outer=False):
        if (pd.Timestamp(first) < pd.Timestamp(model.CONTRACT['start']) or pd.Timestamp(last) > pd.Timestamp(model.CONTRACT['end'])
                or pd.Timestamp(cutoff) > pd.Timestamp(model.CONTRACT['end'])):
            raise ValueError('financial window outside development contract')
        path = self.source.root/('evaluated/finance.parquet' if outer else 'prepared/train_finance.parquet')
        filters = [(model.base.KEY[0], '>=', pd.Timestamp(first)), (model.base.KEY[0], '<=', pd.Timestamp(last))]
        meta = pd.read_parquet(path, columns=[*model.KEY, 'label_information_end'], filters=filters)
        observed = pd.read_parquet(path, filters=filters+[('label_information_end', '<=', pd.Timestamp(cutoff))])
        return projected_finance(meta, observed, cutoff)

    def claim(self, key, stage_hash=None):
        allowed = {f'{prefix}_{arm}' for prefix in ('fold1', 'fold2', 'fold3', 'final') for arm in ('matched', 'candidate')}
        if key not in allowed:
            raise ValueError('unregistered fit')
        self.budget()
        folder = self.root/'fit_claims'
        folder.mkdir(exist_ok=True)
        start = folder/(key+'_started.json')
        if stage_hash is None:
            if len(list(folder.glob('*_started.json'))) >= model.CONTRACT['max_fits']:
                raise ValueError('fit budget exhausted')
            path = start
        else:
            expected = previous.load(start)
            if expected['key'] != key or expected['plan_sha256'] != self.digest or expected['stage_sha256'] is not None:
                raise ValueError('fit claim identity drift')
            path = folder/(key+'_finished.json')
            if path.exists():
                done = previous.load(path)
                if done['key'] != key or done['plan_sha256'] != self.digest or done['stage_sha256'] != stage_hash:
                    raise ValueError('fit completion identity drift')
                return
        payload = dict(key=key, plan_sha256=self.digest, stage_sha256=stage_hash, at=datetime.now(timezone.utc).isoformat())
        with path.open('xb') as stream:
            stream.write(_json_bytes(payload))
            stream.flush()
            os.fsync(stream.fileno())

    def register(self, relative, *, outer=False, evaluated=False):
        record = build_trial_record(experiment_id=self.plan['experiment_id'], attempt_id='exact_v1',
            research_stage=relative.replace('/', '_').upper(), study_type='EXPLORATORY_SCREEN', objective_contract='ALPHA_RANKING',
            decision_use='NAVIGATION_ONLY', hypothesis_family_id=model.CONTRACT['hypothesis'], unique_variable='PEER_BETA_RESIDUAL_REVERSAL_REPRESENTATION',
            parent_lineage=(self.source.plan['experiment_id'],), dataset_identity=self.plan['binding_hash'], schema_identity=model.sha(model.CONTRACT),
            policy_identity=model.base.VALUATION_POLICY_SHA256, planned_trial_count=1, generated_trial_count=int(evaluated),
            evaluated_trial_count=int(evaluated), selected_trial_count=0, result_class='EXPLORATORY',
            consumed_windows=(dict(window_id='PREVIOUSLY_CONSUMED_DEVELOPMENT', dataset_identity=self.plan['binding_hash'],
                start_date=model.CONTRACT['start'], end_date=model.CONTRACT['end'] if outer else model.CONTRACT['train_end']),),
            evidence_refs=(evidence_reference_for_file(self.root/relative/'manifest.json', role='peer_residual_'+relative.replace('/', '_')),))
        AdvisoryResearchTrialRegistryV1(self.root.parent/'trial_registry.jsonl').append_batch((record,))


def preregister(output, source_root, commit):
    source = previous.Study(source_root)
    plan = dict(contract=model.CONTRACT, source_root=str(source.root), source_stage_sha256=source.read('evaluated'),
        binding_hash=source.plan['binding']['binding_hash'], source_commit=commit,
        implementation_sha256=implementation_sha256(), versions=previous.versions(), node_python_uri=source.plan['node_python_uri'])
    plan = json.loads(_json_bytes(plan))
    plan['experiment_id'] = 'advpeertop10_'+model.sha(plan)[:24]
    study = Study(previous.node_path(output)/plan['experiment_id'], plan)
    study.publish('preregistered', None, {'plan.json':_json_bytes(plan)})
    study.register('preregistered')
    return study.root


def prepare(study, parent, *, outer=False, progress=lambda event: None):
    relative = ('outer' if outer else 'inner')+'/prepared'
    path = study.root/relative
    if path.exists():
        digest = study.read(relative, parent)
        verify_input_files(study, previous.load(path/'receipt.json'))
        return pd.read_parquet(path/'inputs.parquet'), digest
    first = model.CONTRACT['outer_start'] if outer else model.CONTRACT['start']
    last = model.CONTRACT['end'] if outer else model.CONTRACT['train_end']
    inputs = pd.read_parquet(study.source.root/'prepared/inputs.parquet', filters=[
        (model.base.KEY[0], '>=', pd.Timestamp(first)), (model.base.KEY[0], '<=', pd.Timestamp(last))])
    frames, calendar, receipt = read_panel(study.source.plan, inputs, study.budget)
    frame = model.attach(inputs, frames['close'], frames['factor'], calendar, progress)
    receipt.update(original_rows=len(frame), original_days=int(frame[model.base.KEY[0]].nunique()),
        common_supported_rows=int(model.support(model.numeric(frame)).sum()),
        statistical_estimates=int(frame.peer_reason.isin(['AVAILABLE','ZERO_RESIDUAL_VARIANCE']).sum()),
        reasons={str(k):int(v) for k, v in frame.peer_reason.value_counts().items()},
        elapsed_seconds=time.monotonic()-study.started, peak_rss_bytes=previous.peak_memory_bytes())
    digest = study.publish(relative, parent, {'inputs.parquet':parquet_bytes(frame), 'receipt.json':_json_bytes(receipt)})
    progress(dict(phase=relative, rows=len(frame), seconds=receipt['elapsed_seconds']))
    return frame, digest


def fit_pair(study, parent, inputs, targets, *, prefix, cutoff, first, last, progress, evaluate=True):
    predictions, models = {}, {}
    train = None
    for arm in ('matched', 'candidate'):
        key = prefix+'_'+arm
        relative = key+'/trained'
        path = study.root/relative
        if path.exists():
            parent = study.read(relative, parent)
            study.claim(key, parent)  # Publication succeeded but completion receipt may have been interrupted.
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
    # Both predictions are immutable before validation/outer financial values are decoded.
    finance = study.finance(first, last, cutoff=last, outer=prefix == 'final')
    return model.panel(predictions['candidate'], predictions['matched'], finance), parent


def evaluate_inner(study, parent, inputs, progress):
    panels = []
    path = study.root/'inner/evaluated'
    for fold, (cutoff, first, last) in enumerate(model.base.FOLDS, 1):
        targets = inputs.loc[inputs[model.base.KEY[0]].between(pd.Timestamp(first), pd.Timestamp(last))].reset_index(drop=True)
        panel, parent = fit_pair(study, parent, inputs, targets, prefix='fold'+str(fold), cutoff=cutoff, first=first, last=last,
            progress=progress, evaluate=not path.exists())
        if panel is not None:
            panel['fold'] = fold
            panels.append(panel)
    if path.exists():
        parent = study.read('inner/evaluated', parent)
        return previous.load(path/'report.json'), parent
    panels = pd.concat(panels, ignore_index=True)
    report, daily = model.summarize(panels)
    coefficients = {}
    for fold in range(1,4):
        packet = previous.load(study.root/f'fold{fold}_candidate/trained/model.json')
        coefficients[str(fold)] = packet['coef'][packet['fields'].index(model.NEW)]
    direction = model.direction_supported(coefficients)
    report.update(reversal_coefficients=coefficients, mechanism_direction_supported=direction,
        proceed_outer=bool(report['proceed_outer'] and direction))
    report['classification'] = 'INNER_SUPPORTED' if report['proceed_outer'] else 'INNER_NOT_SUPPORTED'
    parent = study.publish('inner/evaluated', parent, {'report.json':_json_bytes(report),
        'panel.parquet':parquet_bytes(panels), 'daily.parquet':parquet_bytes(daily)})
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
            panels, parent = fit_pair(study, parent, inputs, targets, prefix='final', cutoff=model.CONTRACT['train_end'],
                first=model.CONTRACT['outer_start'], last=model.CONTRACT['end'], progress=progress,
                evaluate=not (study.root/'outer/evaluated').exists())
            if (study.root/'outer/evaluated').exists():
                parent = study.read('outer/evaluated', parent)
                report = previous.load(study.root/'outer/evaluated/report.json')
            else:
                report, daily = model.summarize(panels)
                parent = study.publish('outer/evaluated', parent, {'report.json':_json_bytes(report),
                    'panel.parquet':parquet_bytes(panels), 'daily.parquet':parquet_bytes(daily)})
            study.register('outer/evaluated', outer=True, evaluated=True)
        expected = {f'{prefix}_{arm}' for prefix in (('fold1', 'fold2', 'fold3', 'final') if go else ('fold1', 'fold2', 'fold3'))
            for arm in ('matched', 'candidate')}
        claims = study.root/'fit_claims'
        for suffix in ('_started.json', '_finished.json'):
            if {p.name.removesuffix(suffix) for p in claims.glob('*'+suffix)} != expected:
                raise ValueError('actual fit identities differ from terminal state')
        study.verify_source()
        if (study.root/'evaluated').exists():
            study.read('evaluated', parent)
            return {**previous.load(study.root/'evaluated/result.json'), 'exact_retry':True}
        result = dict(experiment_id=study.plan['experiment_id'], classification=report['classification'],
            inner=inner, outer=report if go else None, physical_fit_count=len(expected), prior_physical_fits=195,
            cumulative_physical_fits=195+len(expected), historical_index_count=1, new_information_candidates=1,
            outer_finance_decoded=go, sealed_finance_decoded=False, sql_count=0, actual_fill_proven=False,
            activation_evidence=False, independent_oos_evidence=False, elapsed_seconds=time.monotonic()-study.started,
            peak_rss_bytes=previous.peak_memory_bytes(), previous_output_bytes=sum(p.stat().st_size for p in study.root.rglob('*') if p.is_file()),
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
    for name in ('output', 'source-root', 'source-commit'):
        register.add_argument('--'+name, required=True)
    execute = commands.add_parser('run')
    execute.add_argument('--study-root', required=True)
    args = parser.parse_args()
    if args.command == 'preregister':
        print(json.dumps(dict(study_root=str(preregister(args.output, args.source_root, args.source_commit)))), flush=True)
    else:
        result = run(args.study_root, lambda event: print(json.dumps(event), flush=True))
        print(json.dumps({k:result[k] for k in ('experiment_id', 'classification', 'physical_fit_count', 'outer_finance_decoded',
            'elapsed_seconds', 'peak_rss_bytes')}), flush=True)


if __name__ == '__main__':
    main()
