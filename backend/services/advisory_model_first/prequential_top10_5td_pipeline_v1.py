"""Bounded sequential Advisory research using existing immutable file outputs."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

import pandas as pd
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first import multiperiod_top10_5td_pipeline_v1 as previous
from backend.services.advisory_model_first import prequential_top10_5td_study_v1 as core
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import finance_inputs, file_dataset, parquet_bytes
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import node_path
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def load(path):
    return json.loads(Path(path).read_bytes())


def implementation_sha256():
    files = [Path(__file__), Path(core.__file__)]
    return sha(dict(current={p.name: hashlib.sha256(p.read_bytes().replace(b'\r\n', b'\n')).hexdigest() for p in files},
        reused=previous.implementation_sha256()))


def x_path(path):
    result = node_path(path).resolve()
    if not (result.drive.upper() == 'X:' or result.as_posix().startswith('/mnt/x/')):
        raise ValueError('study output must resolve inside the approved X volume')
    return result


class Source:
    def __init__(self, root):
        self.old = previous.Study(root)
        self.root = self.old.root
        self.stage_hash = self.old.read('evaluated')
        self.calendar = pd.to_datetime(load(self.root/'prepared/calendar.json')).tolist()
        self.axes = pd.read_parquet(self.root/'prepared/inputs.parquet', columns=['package_id', KEY[0]])
        days = self.axes.groupby('package_id')[KEY[0]].unique()
        if len(days) != 2 or sorted(days.iloc[0]) != sorted(days.iloc[1]):
            raise ValueError('source packages do not share the complete original D axis')
        self.days = sorted(days.iloc[0])
        self.metadata = pd.concat([pd.read_parquet(p, columns=['package_id', *KEY, 'label_information_end']) for p in self.finance_paths])
        if len(self.metadata) != len(self.axes) or self.metadata.duplicated(['package_id', *KEY]).any():
            raise ValueError('source financial metadata lost original candidates')
        old = pd.read_parquet(self.finance_paths[0], columns=['package_id', *KEY, 'label_information_end', 'valuation_status'])
        self.gaps = old.loc[old.valuation_status.eq('IMMATURE')].drop(columns='valuation_status')

    @property
    def finance_paths(self):
        return [self.root/'prepared/train_finance.parquet', self.root/'evaluated/finance.parquet']

    def inputs(self, first, last):
        return pd.read_parquet(self.root/'prepared/inputs.parquet', filters=[
            (KEY[0], '>=', pd.Timestamp(first)), (KEY[0], '<=', pd.Timestamp(last))])

    def finance(self, first, last, cutoff, boundary=None):
        filters = [(KEY[0], '>=', pd.Timestamp(first)), (KEY[0], '<=', pd.Timestamp(last)),
            ('label_information_end', '<=', pd.Timestamp(cutoff))]
        observed = pd.concat([pd.read_parquet(p, filters=filters) for p in self.finance_paths], ignore_index=True)
        meta = self.metadata.loc[self.metadata[KEY[0]].between(pd.Timestamp(first), pd.Timestamp(last))]
        if boundary is not None:
            eligible = boundary.loc[boundary[KEY[0]].between(pd.Timestamp(first), pd.Timestamp(last))
                & boundary.label_information_end.le(pd.Timestamp(cutoff))]
            keys = ['package_id', *KEY]
            replace = pd.MultiIndex.from_frame(eligible[keys])
            observed = pd.concat([observed.loc[~pd.MultiIndex.from_frame(observed[keys]).isin(replace)], eligible], ignore_index=True)
        return core.projected_finance(meta, observed, cutoff)

    def training(self, cutoff, boundary=None):
        start = core.base.CONTRACT['start']
        return self.inputs(start, cutoff).merge(self.finance(start, cutoff, cutoff, boundary),
            on=['package_id', *KEY], how='left', validate='one_to_one')

    def verify(self):
        if self.old.read('evaluated') != self.stage_hash:
            raise ValueError('source stage identity drifted during consumption')


def schedules(source):
    inner = []
    for fold, (cutoff, first, last) in enumerate(core.base.FOLDS, 1):
        days = [d for d in source.days if pd.Timestamp(first) <= d <= pd.Timestamp(last)]
        inner.extend([dict(b, fold=fold) for b in core.schedule(days, source.calendar, cutoff, f'inner{fold}')])
    days = [d for d in source.days if d >= pd.Timestamp(core.base.CONTRACT['outer_start'])]
    outer = core.schedule(days, source.calendar, core.base.CONTRACT['train_end'], 'outer')
    if len(inner) != 8 or len(outer) != 11:
        raise ValueError('actual original calendar differs from the registered 8+10 fit budget')
    return dict(inner=inner, outer=outer)


class Study:
    def __init__(self, root, plan=None):
        self.root = x_path(root)
        self.plan = plan or load(self.root/'preregistered/plan.json')
        identity = 'advpreqtop10_'+sha({k: v for k, v in self.plan.items() if k != 'experiment_id'})[:24]
        if (self.plan['experiment_id'] != identity or self.plan['contract'] != json.loads(json.dumps(core.CONTRACT))
                or self.plan['implementation_sha256'] != implementation_sha256()
                or not re.fullmatch('[0-9a-f]{40}', self.plan['source_commit'])):
            raise ValueError('research plan, source commit or implementation identity differs')
        self.source = Source(self.plan['source_root'])
        if (self.source.stage_hash != self.plan['source_stage_sha256'] or schedules(self.source) != self.plan['schedules']
                or self.plan['fit_versions'] != self.source.old.plan['fit_versions']):
            raise ValueError('parent stage, schedule or environment identity drift')
        self.digest = sha(self.plan)
        self.started = time.monotonic()

    def budget(self):
        if previous.peak_memory_bytes() > 2*1024**3 or time.monotonic()-self.started > 1800:
            raise ValueError('registered memory/time research budget exceeded')

    def read(self, relative, native, parent):
        return read_stage(self.root/relative, stage=native, plan_sha256=self.digest, parent_sha256=parent)['stage_sha256']

    def publish(self, relative, native, parent, files):
        self.budget()
        if sum(len(v) for v in files.values()) > 200*1024**2:
            raise ValueError('research artifact budget exceeded')
        publish_stage(study_root=(self.root/relative).parent, stage=native, plan_sha256=self.digest,
            parent_sha256=parent, artifacts=files)
        return self.read(relative, native, parent)

    def claim(self, key, *, complete=False):
        expected = {b['key'] for b in self.plan['schedules']['inner']+self.plan['schedules']['outer'][1:]}
        if key not in expected:
            raise ValueError('fit key is outside the frozen schedule')
        folder = self.root/'fit_claims'
        folder.mkdir(exist_ok=True)
        start = folder/(key+'_started.json')
        if complete and (not start.exists() or load(start)['plan_sha256'] != self.digest):
            raise ValueError('fit completion is missing its matching start')
        target = folder/(key+('_finished.json' if complete else '_started.json'))
        if not complete and start.exists():
            raise ValueError('started fit has no published block; explicit recovery required, no refit')
        if len(list(folder.glob('*_started.json'))) >= 18 and not complete:
            raise ValueError('physical fit budget exhausted')
        with target.open('xb') as stream:
            stream.write(_json_bytes(dict(key=key, plan_sha256=self.digest, complete=complete,
                captured_at=datetime.now(timezone.utc).isoformat())))
            stream.flush()
            os.fsync(stream.fileno())

    def fit_count(self, prefix=''):
        expected = {b['key'] for b in self.plan['schedules']['inner']+self.plan['schedules']['outer'][1:]}
        folder = self.root/'fit_claims'
        starts, ends = sorted(folder.glob('*_started.json')), sorted(folder.glob('*_finished.json'))
        if len(starts) != len(ends) or len(starts) > 18:
            raise ValueError('incomplete or excess fit claims')
        for path in starts:
            before = load(path)
            after = load(folder/(before['key']+'_finished.json'))
            if (before['key'] not in expected or before['complete'] is not False or after['complete'] is not True
                    or path.name != before['key']+'_started.json'
                    or before['key'] != after['key'] or before['plan_sha256'] != self.digest or after['plan_sha256'] != self.digest):
                raise ValueError('fit claim identity differs')
        return sum(path.name.startswith(prefix) for path in starts)

    def node(self):
        if (os.name == 'nt' or str(Path(sys.executable).resolve()) != self.plan['node_python_uri']
                or previous.versions() != self.plan['fit_versions']):
            raise ValueError('research fits require the frozen existing WSL environment')

    def register(self, stage, path, evaluated=False, selected=False):
        record = build_trial_record(experiment_id=self.plan['experiment_id'], attempt_id='exact_attempt_v1', research_stage=stage,
            study_type='EXPLORATORY_SCREEN', objective_contract='ALPHA_RANKING', decision_use='NAVIGATION_ONLY',
            hypothesis_family_id=core.CONTRACT['hypothesis'], unique_variable='PAST_MATURE_LABEL_EXPANDING_UPDATE_EVERY_20_D',
            parent_lineage=(self.source.old.plan['experiment_id'],), dataset_identity=self.source.old.plan['binding']['binding_hash'],
            schema_identity=sha(core.CONTRACT), policy_identity=core.base.VALUATION_POLICY_SHA256,
            planned_trial_count=1, generated_trial_count=int(evaluated), evaluated_trial_count=int(evaluated),
            selected_trial_count=int(selected), result_class='EXPLORATORY', consumed_windows=(dict(window_id='CONSUMED_DEVELOPMENT_ONLY',
                dataset_identity=self.source.old.plan['binding']['binding_hash'], start_date=core.base.CONTRACT['start'],
                end_date=core.base.CONTRACT['end']),), evidence_refs=(evidence_reference_for_file(path/'manifest.json', role='prequential_'+stage.lower()),))
        AdvisoryResearchTrialRegistryV1(self.root.parent/'trial_registry.jsonl').append_batch((record,))


def preregister(output, source_root, commit):
    source = Source(source_root)
    plan = dict(contract=core.CONTRACT, source_root=str(source.root), source_stage_sha256=source.stage_hash,
        source_commit=commit, implementation_sha256=implementation_sha256(), schedules=schedules(source),
        fit_versions=source.old.plan['fit_versions'], node_python_uri=source.old.plan['node_python_uri'], prior_research_fit_count=181)
    plan['experiment_id'] = 'advpreqtop10_'+sha(plan)[:24]
    study = Study(x_path(output)/plan['experiment_id'], json.loads(json.dumps(plan)))
    study.publish('preregistered', 'preregistered', None, {'plan.json': _json_bytes(study.plan)})
    study.register('PREREGISTERED', study.root/'preregistered')
    return str(study.root)


def block(study, spec, parent, boundary=None, initial=False):
    location = 'blocks/'+spec['key']+'/trained'
    path = study.root/location
    if path.exists():
        digest = study.read(location, 'trained', parent)
        receipt = load(path/'receipt.json')
        if receipt['block'] != spec:
            raise ValueError('published block differs from its frozen schedule')
        return pd.read_parquet(path/'predictions.parquet'), digest
    rows = study.source.training(spec['cutoff'], boundary)
    part, _, _ = core.base.training_population(rows, spec['cutoff'])
    if initial:
        packet = load(study.source.root/'predicted/trained/model.json')
        model, encoding = core.own_model(packet, study.plan['fit_versions'])
    else:
        model, encoding = core.base.fit_model(rows, spec['cutoff'], core.CONFIG,
            before_fit=lambda: study.claim(spec['key']), after_fit=lambda: study.claim(spec['key'], complete=True))
        packet = core.model_packet(model, encoding, study.plan['fit_versions'])
    inputs = study.source.inputs(spec['first'], spec['last'])
    prediction = core.base.predict(inputs, model, encoding)
    core.assert_population(inputs, prediction)
    if initial:
        old = pd.read_parquet(study.source.root/'predicted/trained/predictions.parquet', filters=[
            (KEY[0], '>=', pd.Timestamp(spec['first'])), (KEY[0], '<=', pd.Timestamp(spec['last']))])
        if not prediction.equals(old):
            raise ValueError('initial block is not identical to its frozen model predictions')
    if 'fold' in spec:
        prediction['fold'] = spec['fold']
    receipt = dict(block=spec, fit_mode='REUSE_VERIFIED_INITIAL_MODEL' if initial else 'NEW_MATURE_LABEL_FIT',
        training_rows=len(part), train_max_D=str(part[KEY[0]].max().date()), train_max_H=str(part.label_information_end.max().date()),
        training_rows_sha256=hashlib.sha256(parquet_bytes(part)).hexdigest(), sql_count=0)
    digest = study.publish(location, 'trained', parent, {'predictions.parquet': parquet_bytes(prediction),
        'model.json': _json_bytes(packet), 'receipt.json': _json_bytes(receipt)})
    return prediction, digest


def inner(study):
    parent = study.read('preregistered', 'preregistered', None)
    predictions = []
    for spec in study.plan['schedules']['inner']:
        p, parent = block(study, spec, parent)
        predictions.append(p)
    if study.fit_count(prefix='inner') != 8:
        raise ValueError('inner phase must have exactly eight completed fits')
    if (study.root/'inner/evaluated').exists():
        digest = study.read('inner/evaluated', 'evaluated', parent)
        return load(study.root/'inner/evaluated/report.json'), digest
    static = pd.read_parquet(study.source.root/'selected/trained/cv_predictions.parquet', filters=[('config_id', '==', core.CONFIG['id'])])
    finances = {i: study.source.finance(first, last, last) for i, (_, first, last) in enumerate(core.base.FOLDS, 1)}
    report, daily = core.inner_report(pd.concat(predictions, ignore_index=True), static, finances)
    report['physical_fit_count'] = study.fit_count(prefix='inner')
    digest = study.publish('inner/evaluated', 'evaluated', parent, {'report.json': _json_bytes(report), 'daily.parquet': parquet_bytes(daily)})
    study.register('INNER', study.root/'inner/evaluated', evaluated=True)
    return report, digest


def boundary_labels(study, parent):
    path = study.root/'boundary/prepared'
    if path.exists():
        digest = study.read('boundary/prepared', 'prepared', parent)
        return pd.read_parquet(path/'finance.parquet'), digest
    gap = study.source.gaps
    cutoff = pd.Timestamp(study.plan['schedules']['outer'][1]['cutoff'])
    if gap.empty or gap.label_information_end.isna().any() or gap.label_information_end.max() > cutoff:
        raise ValueError('source boundary labels lack a legal maturity cutoff')
    first, last = gap[KEY[0]].min(), gap[KEY[0]].max()
    inputs = study.source.inputs(first, last)
    if set(map(tuple, inputs[['package_id', *KEY]].to_numpy())) != set(map(tuple, gap[['package_id', *KEY]].to_numpy())):
        raise ValueError('boundary materialization must match only the original immature rows')
    dataset = file_dataset(dict(study.source.old.plan, experiment_id=study.plan['experiment_id']))
    try:
        finance, receipt = finance_inputs(dataset, inputs, gap.label_information_end.max(), study.budget)
    finally:
        dataset.close()
    if finance.valuation_status.eq('IMMATURE').any():
        raise ValueError('boundary labels remained immature after their full declared horizon')
    digest = study.publish('boundary/prepared', 'prepared', parent, {'finance.parquet': parquet_bytes(finance), 'receipt.json': _json_bytes(receipt)})
    return finance, digest


def run(root):
    study = Study(root)
    study.node()
    with _exclusive_file_lock(study.root/'study.lock'), threadpool_limits(limits=2):
        inner_result, parent = inner(study)
        if inner_result['proceed_outer']:
            initial = study.plan['schedules']['outer'][0]
            first_prediction, parent = block(study, initial, parent, initial=True)
            boundary, parent = boundary_labels(study, parent)
            predictions = [first_prediction]
            for spec in study.plan['schedules']['outer'][1:]:
                p, parent = block(study, spec, parent, boundary)
                predictions.append(p)
            if study.fit_count() != 18:
                raise ValueError('complete outer protocol must have exactly eighteen new fits')
        else:
            boundary, predictions = None, []
        if (study.root/'evaluated').exists():
            study.read('evaluated', 'evaluated', parent)
            return dict(load(study.root/'evaluated/result.json'), exact_retry=True)
        if predictions:
            first, last = core.base.CONTRACT['outer_start'], core.base.CONTRACT['end']
            inputs = study.source.inputs(first, last)
            keys = ['package_id', *KEY]
            p = pd.concat(predictions, ignore_index=True).set_index(keys).loc[pd.MultiIndex.from_frame(inputs[keys])].reset_index()
            core.assert_population(inputs, p)
            finance = study.source.finance(first, last, last)
            result, panel, daily = core.base.evaluate(p, finance)
            static = pd.read_parquet(study.source.root/'predicted/trained/predictions.parquet')
            result['update_mechanism'] = core.mechanism_delta(panel, core.base.cohort(static, finance))
            files = {'predictions.parquet': parquet_bytes(p), 'panel.parquet': parquet_bytes(panel), 'daily.parquet': parquet_bytes(daily)}
        else:
            result, files = dict(classification='INNER_PROTOCOL_NOT_SUPPORTED', outer_started=False), {}
        study.source.verify()
        result.update(experiment_id=study.plan['experiment_id'], source_commit=study.plan['source_commit'], inner=inner_result,
            physical_fit_count=study.fit_count(), cumulative_research_fits=181+study.fit_count(),
            independent_oos_evidence=False, activation_evidence=False, decision_use='NAVIGATION_ONLY',
            sql_count=0, wall_seconds=time.monotonic()-study.started, peak_rss_bytes=previous.peak_memory_bytes(),
            sealed_finance_decoded=False, outer_started=bool(predictions), outer_past_mature_feedback=bool(predictions),
            immutable_predictions_before_finance=False,
            prediction_blocks_frozen_before_their_target_outcomes=True,
            all_outer_predictions_before_aggregate_evaluation=bool(predictions))
        study.publish('evaluated', 'evaluated', parent, dict(files, **{'result.json': _json_bytes(result)}))
        study.register('EVALUATED', study.root/'evaluated', evaluated=True,
            selected=result['classification'] == 'WORTH_INDEPENDENT_CONFIRMATION')
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['preregister', 'run'])
    parser.add_argument('--root', required=True)
    parser.add_argument('--source-root')
    parser.add_argument('--commit')
    args = parser.parse_args()
    if args.action == 'preregister':
        result = dict(root=preregister(args.root, args.source_root, args.commit))
    else:
        result = run(args.root)
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == '__main__':
    main()
