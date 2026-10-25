"""One fixed native-anchored rank policy; offline, zero new model fits."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import time

import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first import multiperiod_top10_5td_pipeline_v1 as previous
from backend.services.advisory_model_first import multiperiod_top10_5td_study_v1 as base
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.prequential_top10_5td_study_v1 import projected_finance
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import build_trial_record

KEY = ['package_id', *previous.KEY]
CONTRACT = dict(schema='advisory_native_anchored_top10_5td_v1',
    hypothesis='NATIVE_ANCHORED_TOP20_TOP10_FIXED_3_TO_1_V1', config_id='ridge_100.0',
    native_weight=3, model_weight=1, rerank_limit=20, top_k=10, planned_candidates=1, physical_fit_budget=0,
    folds=base.FOLDS, inherited_evaluation=base.CONTRACT,
    inner_rule='POSITIVE_NATIVE_MEAN_TWO_POSITIVE_FOLDS_COVERAGE80_INTERVENTION20D_20PCT_TWO_FOLDS',
    objective_contract='ALPHA_RANKING', study_type='EXPLORATORY_SCREEN', decision_use='NAVIGATION_ONLY')


def combine(predictions):
    """Preserve native slots without advice and every native rank above twenty."""
    if predictions.duplicated(KEY).any():
        raise ValueError('duplicate candidate identity')
    result = predictions.copy().reset_index(drop=True)
    result['advisory_rank'] = result.selection_effective_rank.copy()
    for _, g in result.groupby(['package_id', previous.KEY[0]], sort=False):
        n = len(g)
        if (n < 20 or sorted(g.selection_effective_rank) != list(range(1, n+1))
                or not g.candidate_group_size.eq(n).all()
                or not g.no_rank_advice.map(lambda v: isinstance(v, (bool, np.bool_))).all()
                or not np.isfinite(g.predicted_relative_net_bps).all()):
            raise ValueError('invalid native population, advice or score')
        eligible = g.loc[g.selection_effective_rank.le(20) & ~g.no_rank_advice]
        slots = sorted(eligible.selection_effective_rank)
        ordered = eligible.sort_values(['predicted_relative_net_bps', 'selection_effective_rank'],
            ascending=[False, True], kind='stable').index
        model_rank = pd.Series(slots, index=ordered, dtype=float)
        fusion = eligible.assign(distance=3*eligible.selection_effective_rank+model_rank)
        order = fusion.sort_values(['distance', 'selection_effective_rank'], kind='stable').index
        result.loc[order, 'advisory_rank'] = slots
    result['selected'] = result.advisory_rank.le(10)
    return result


def same_population(inputs, predictions):
    if not inputs.loc[:, IDENTITY].reset_index(drop=True).equals(predictions.loc[:, IDENTITY].reset_index(drop=True)):
        raise ValueError('prediction changed original identity, order or population')


def comparison_panel(candidate, control, finance):
    same_population(candidate, control)
    a, b = base.cohort(candidate, finance), base.cohort(control, finance)
    return a.merge(b[['package_id', 'decision_date', 'candidate']], on=['package_id', 'decision_date'],
        validate='one_to_one', suffixes=('', '_full_model'))


def summarize(panel):
    """Reuse native cohort returns, intervals and verdict; aggregate one common D axis."""
    groups = panel.groupby('decision_date', sort=True)
    packages = set(panel.package_id)
    if len(packages) != 2 or panel.duplicated(['package_id', 'decision_date']).any():
        raise ValueError('two original packages and unique daily cohorts required')
    if any(set(g.package_id) != packages for _, g in groups):
        raise ValueError('daily package axis changed')
    fields = ['candidate', 'baseline', 'candidate_full_model']
    mature = groups.mature.all()
    complete = groups[fields].count().eq(2).all(axis=1) & mature
    independent = groups[fields].mean().where(groups[fields].count().eq(2))
    daily = independent.where(complete, np.nan)
    daily['delta'] = daily.candidate-daily.baseline
    daily['versus_full_model'] = daily.candidate-daily.candidate_full_model
    interventions = groups.intervention_count.sum().gt(0) & mature
    daily['mature'], daily['intervention'] = mature, interventions
    stats = base.paired_interval(daily.delta.to_numpy())
    cand, native = base._distribution(daily.candidate), base._distribution(daily.baseline)
    tail = cand['lower_tail10_mean_bps']-native['lower_tail10_mean_bps'] if cand['known'] else None
    mature_panel = panel.loc[panel.mature]
    excess = float((mature_panel.candidate_unknown-mature_panel.baseline_unknown).sum()/(10*len(mature_panel))) if len(mature_panel) else 0.
    coverage = float(complete.sum()/mature.sum()) if mature.any() else 0.
    fraction = float(interventions.sum()/mature.sum()) if mature.any() else 0.
    blocks = len(set(pd.DatetimeIndex(interventions.index[interventions]).to_period('Q')))
    report = dict(statistics=stats, versus_full_model=base.paired_interval(daily.versus_full_model.to_numpy()),
        candidate=cand, baseline=native, full_model=base._distribution(daily.candidate_full_model),
        tail_delta_bps=tail, intervention_days=int(interventions.sum()), intervention_fraction=fraction,
        intervention_blocks=blocks, common_mature_coverage=coverage, unknown_excess=excess,
        original_package_days=len(panel), original_candidates=int(panel.original_candidates.sum()),
        original_decision_days=len(daily), mature_decision_days=int(mature.sum()),
        candidate_unknown_slots=int(panel.candidate_unknown.sum()), baseline_unknown_slots=int(panel.baseline_unknown.sum()),
        comparison_population='THREE_ARMS_COMMON_D_TWO_PACKAGES', actual_fill_proven=False,
        independent_oos_evidence=False, activation_evidence=False, decision_use='NAVIGATION_ONLY',
        individual_available_d={c:base._distribution(independent[c]) for c in fields},
        classification=base.classify(stats, tail_delta=tail, intervention_days=int(interventions.sum()),
            intervention_fraction=fraction, intervention_blocks=blocks, coverage=coverage, unknown_excess=excess))
    chosen = panel.loc[panel.decision_date.isin(complete.index[complete])]
    report['by_package_common_d'] = {str(p): {c:base._distribution(g[c]) for c in [*fields, 'delta']}
        for p, g in chosen.groupby('package_id')}
    report['by_period_common_d'] = {str(p):base._distribution(g.delta) for p, g in daily.groupby(daily.index.to_period('Q'))}
    for name in ('avoided_loss', 'missed_profit', 'added_profit', 'added_loss'):
        report[name+'_bps'] = float(chosen[name].mean()) if name in chosen and len(chosen) else None
    if 'fold' in panel:
        if not groups.fold.nunique().eq(1).all():
            raise ValueError('fold identities overlap on a decision day')
        daily['fold'] = groups.fold.first()
        means = daily.groupby('fold').delta.mean()
        report['by_fold'] = {str(int(f)):float(v) if pd.notna(v) else None for f, v in means.items()}
        go = bool(stats['mean_bps'] is not None and stats['mean_bps'] > 0 and means.gt(0).sum() >= 2
            and set(means.index) == {1, 2, 3} and means.notna().all() and coverage >= .8
            and interventions.sum() >= 20 and fraction >= .2 and daily.loc[interventions, 'fold'].nunique() >= 2)
        report.update(proceed_outer=go, classification='INNER_SUPPORTED' if go else 'INNER_NOT_SUPPORTED')
    return report, daily.reset_index()


def implementation_sha256():
    helper = Path(__file__).with_name('prequential_top10_5td_study_v1.py')
    return previous.sha(dict(base=previous.implementation_sha256(),
        helper=hashlib.sha256(helper.read_bytes().replace(b'\r\n', b'\n')).hexdigest(),
        self=hashlib.sha256(Path(__file__).read_bytes().replace(b'\r\n', b'\n')).hexdigest()))


class Study:
    def __init__(self, root, plan=None):
        self.root = previous.node_path(root)
        if not (self.root.as_posix().lower().startswith('/mnt/x/') or self.root.drive.upper() == 'X:'):
            raise ValueError('output must be on X')
        self.plan = plan or previous.load(self.root/'preregistered/plan.json')
        identity = 'advanchortop10_'+previous.sha({k:v for k, v in self.plan.items() if k != 'experiment_id'})[:24]
        if (self.plan['experiment_id'] != identity or self.plan['contract'] != json.loads(json.dumps(CONTRACT))
                or self.plan['implementation_sha256'] != implementation_sha256()
                or not re.fullmatch('[0-9a-f]{40}', self.plan['source_commit'])
                or self.plan['versions'] != previous.versions()):
            raise ValueError('study identity, implementation or environment differs')
        self.source = previous.Study(self.plan['source_root'])
        self.verify_source()
        selected = previous.load(self.source.root/'selected/trained/selection.json')
        if selected['config']['id'] != CONTRACT['config_id']:
            raise ValueError('original selected model differs; no reselection')
        self.digest = previous.sha(self.plan)
        self.started = time.monotonic()

    def verify_source(self):
        if self.source.read('evaluated') != self.plan['source_stage_sha256']:
            raise ValueError('source stage identity drift')

    def read(self, relative, native, parent):
        return read_stage(self.root/relative, stage=native, plan_sha256=self.digest, parent_sha256=parent)['stage_sha256']

    def publish(self, relative, native, parent, files):
        if previous.peak_memory_bytes() > 2*1024**3 or time.monotonic()-self.started > 1800:
            raise ValueError('2GiB/30 minute resource budget exceeded')
        if sum(map(len, files.values()))+sum(p.stat().st_size for p in self.root.rglob('*') if p.is_file()) > 100*1024**2:
            raise ValueError('100MiB output budget exceeded')
        publish_stage(study_root=(self.root/relative).parent, stage=native, plan_sha256=self.digest,
            parent_sha256=parent, artifacts=files)
        return self.read(relative, native, parent)

    def predictions(self, first, last, fold=None):
        filters = [(previous.KEY[0], '>=', pd.Timestamp(first)), (previous.KEY[0], '<=', pd.Timestamp(last))]
        expected = pd.read_parquet(self.source.root/'prepared/inputs.parquet', columns=list(IDENTITY), filters=filters)
        path = 'predicted/trained/predictions.parquet' if fold is None else 'selected/trained/cv_predictions.parquet'
        if fold is not None:
            filters += [('config_id', '==', CONTRACT['config_id']), ('fold', '==', fold)]
        p = pd.read_parquet(self.source.root/path, filters=filters)
        same_population(expected, p)
        return p

    def finance(self, first, last, *, outer=False):
        # H filters are pushed down before any future target is decoded.
        path = self.source.root/('evaluated/finance.parquet' if outer else 'prepared/train_finance.parquet')
        filters = [(previous.KEY[0], '>=', pd.Timestamp(first)), (previous.KEY[0], '<=', pd.Timestamp(last))]
        meta = pd.read_parquet(path, columns=[*KEY, 'label_information_end'], filters=filters)
        observed = pd.read_parquet(path, filters=filters+[('label_information_end', '<=', pd.Timestamp(last))])
        return projected_finance(meta, observed, last)

    def register(self, stage, path, evaluated=False):
        record = build_trial_record(experiment_id=self.plan['experiment_id'], attempt_id='exact_v1', research_stage=stage,
            study_type='EXPLORATORY_SCREEN', objective_contract='ALPHA_RANKING', decision_use='NAVIGATION_ONLY',
            hypothesis_family_id=CONTRACT['hypothesis'], unique_variable='TOP20_FIXED_NATIVE_MODEL_3_TO_1_RANK_FUSION',
            parent_lineage=(self.source.plan['experiment_id'],), dataset_identity=self.source.plan['binding']['binding_hash'],
            schema_identity=previous.sha(CONTRACT), policy_identity=base.VALUATION_POLICY_SHA256,
            planned_trial_count=1, generated_trial_count=int(evaluated), evaluated_trial_count=int(evaluated), selected_trial_count=0,
            result_class='EXPLORATORY', consumed_windows=(dict(window_id='PREVIOUSLY_CONSUMED_DEVELOPMENT',
                dataset_identity=self.source.plan['binding']['binding_hash'], start_date='2025-04-01',
                end_date=base.CONTRACT['end'] if stage == 'OUTER_EVALUATED' else base.CONTRACT['train_end']),),
            evidence_refs=(evidence_reference_for_file(path/'manifest.json', role='anchored_'+stage.lower()),))
        AdvisoryResearchTrialRegistryV1(self.root.parent/'trial_registry.jsonl').append_batch((record,))


def preregister(output, source_root, commit):
    source = previous.Study(source_root)
    plan = dict(contract=CONTRACT, source_root=str(source.root), source_stage_sha256=source.read('evaluated'),
        source_commit=commit, implementation_sha256=implementation_sha256(), versions=previous.versions())
    plan = json.loads(_json_bytes(plan))
    plan['experiment_id'] = 'advanchortop10_'+previous.sha(plan)[:24]
    study = Study(previous.node_path(output)/plan['experiment_id'], plan)
    study.publish('preregistered', 'preregistered', None, {'plan.json':_json_bytes(plan)})
    study.register('PREREGISTERED', study.root/'preregistered')
    return study


def inner(study, parent):
    path = study.root/'inner/evaluated'
    if path.exists():
        digest = study.read('inner/evaluated', 'evaluated', parent)
        return previous.load(path/'report.json'), digest
    panels, predictions = [], []
    for fold, (_, first, last) in enumerate(base.FOLDS, 1):
        control = study.predictions(first, last, fold)
        candidate = combine(control)
        p = comparison_panel(candidate, control, study.finance(first, last))
        p['fold'] = fold
        panels.append(p)
        predictions.append(candidate)
    panel = pd.concat(panels, ignore_index=True)
    report, daily = summarize(panel)
    digest = study.publish('inner/evaluated', 'evaluated', parent, {'report.json':_json_bytes(report),
        'panel.parquet':parquet_bytes(panel), 'daily.parquet':parquet_bytes(daily),
        'predictions.parquet':parquet_bytes(pd.concat(predictions, ignore_index=True))})
    return report, digest


def run(root):
    study = Study(root)
    with _exclusive_file_lock(study.root/'study.lock'), threadpool_limits(limits=2):
        start = study.read('preregistered', 'preregistered', None)
        inner_result, parent = inner(study, start)
        study.register('INNER_EVALUATED', study.root/'inner/evaluated', True)
        go = inner_result['proceed_outer']
        if go:
            first, last = base.CONTRACT['outer_start'], base.CONTRACT['end']
            path = study.root/'outer/prepared'
            if path.exists():
                parent = study.read('outer/prepared', 'prepared', parent)
            else:
                original = study.predictions(first, last)
                parent = study.publish('outer/prepared', 'prepared', parent, {
                    'predictions.parquet':parquet_bytes(combine(original)), 'control.parquet':parquet_bytes(original)})
        if (study.root/'evaluated').exists():
            study.read('evaluated', 'evaluated', parent)
            result = previous.load(study.root/'evaluated/result.json')
            study.register('OUTER_EVALUATED' if go else 'INNER_STOPPED', study.root/'evaluated', True)
            return dict(result, exact_retry=True)
        files = {}
        if go:
            candidate, control = [pd.read_parquet(path/f) for f in ['predictions.parquet', 'control.parquet']]
            finance = study.finance(first, last, outer=True)
            panel = comparison_panel(candidate, control, finance)
            result, daily = summarize(panel)
            files.update(panel_parquet=parquet_bytes(panel), daily_parquet=parquet_bytes(daily))
        else:
            result = dict(classification='INNER_NOT_SUPPORTED')
        result.update(experiment_id=study.plan['experiment_id'], inner=inner_result, outer_started=go,
            physical_fit_count=0, evaluated_candidates=1, elapsed_seconds=time.monotonic()-study.started,
            peak_rss_bytes=previous.peak_memory_bytes(), sql_count=0, new_h5_bin_reads=0,
            sealed_finance_decoded=False, independent_oos_evidence=False, activation_evidence=False,
            decision_use='NAVIGATION_ONLY', outer_predictions_frozen_before_finance=go)
        study.verify_source()
        files = {k.replace('_parquet', '.parquet'):v for k, v in files.items()}
        study.publish('evaluated', 'evaluated', parent, {'result.json':_json_bytes(result), **files})
        study.register('OUTER_EVALUATED' if go else 'INNER_STOPPED', study.root/'evaluated', True)
        return result


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='command', required=True)
    pre = sub.add_parser('preregister')
    for arg in ('output', 'source-root', 'commit'):
        pre.add_argument('--'+arg, required=True)
    sub.add_parser('run').add_argument('--root', required=True)
    args = parser.parse_args()
    result = {'root':str(preregister(args.output, args.source_root, args.commit).root)} if args.command == 'preregister' else run(args.root)
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == '__main__':
    main()
