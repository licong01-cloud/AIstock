"""Immutable file-only prepare -> WSL fit/predict -> once-only finance evaluation.

Run with python -m <this module> --help. No services, database or QE submission.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import re
import time

import numpy as np
import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, file_sha256, publish_stage, read_stage
from backend.services.advisory_model_first.group_centered_rank_5td_study_v1 import FINANCE
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import node_path
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import build_trial_record
from backend.services.advisory_model_first.serial_price_pressure_top10_5td_inputs_v1 import (
    CANDIDATE, CONTRACT, DAILY, KEY, POLICY_SHA256, ROSTER, attach_features, read_minute_inputs, validate_roster,
)
from backend.services.advisory_model_first.serial_price_pressure_top10_5td_study_v1 import (
    evaluate, fit_encoding, market_stratum, numeric, predict, train_models, training_population,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def load(path):
    return json.loads(node_path(path).read_bytes())


def versions():
    return {name: importlib.metadata.version(name) for name in ('numpy', 'pandas', 'scikit-learn', 'pyarrow')}


def implementation_sha256():
    folder = Path(__file__).parent
    names = (Path(__file__).name, 'serial_price_pressure_top10_5td_inputs_v1.py', 'serial_price_pressure_top10_5td_study_v1.py',
        'generic_minute_price_5td_source_v1.py', 'generic_volume_path_price_5td_source_v1.py',
        'generic_ordered_path_price_5td_source_v1.py', 'joint_risk_top10_5td_inputs_v1.py',
        'generic_daily_price_input_v1.py', 'generic_price_5td_contracts_v1.py', 'generic_price_5td_valuation_v2.py',
        'group_centered_rank_5td_study_v1.py', 'economic_entry_pipeline.py', 'research_control.py', 'research_control_contracts.py',
        'parent_score_price_5td_inputs_v1.py', 'parent_score_price_5td_contracts_v1.py')
    return sha({name: file_sha256(folder/name) for name in names} | {'frozen_dataset.py': file_sha256(folder.parent.parent/'data_service/frozen_dataset.py')})


class Study:
    def __init__(self, root, *, plan=None):
        self.root = node_path(root)
        self.plan = load(self.root/'preregistered/plan.json') if plan is None else plan
        self.digest = sha(self.plan)
        if self.plan['implementation_sha256'] != implementation_sha256() or self.plan['policy_sha256'] != POLICY_SHA256:
            raise ValueError('study source/policy drift; no implicit new lineage or fit')
        self.started, self.peak = time.monotonic(), 0

    def budget(self):
        mem = psutil.Process().memory_info()
        self.peak = max(self.peak, mem.rss, getattr(mem, 'peak_wset', 0))
        if os.name != 'nt':
            import resource
            self.peak = max(self.peak, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024)
        if self.peak > CONTRACT['max_rss_bytes'] or time.monotonic()-self.started > CONTRACT['max_seconds']:
            raise ValueError('study memory/time budget exceeded')

    def read(self, stage, parent):
        return read_stage(self.root/stage, stage=stage, plan_sha256=self.digest, parent_sha256=parent)['stage_sha256']

    def publish(self, stage, parent, artifacts, *, generated=0, evaluated=0):
        self.budget()
        path = publish_stage(study_root=self.root, stage=stage, plan_sha256=self.digest, parent_sha256=parent, artifacts=artifacts)
        register(self.root, self.plan, stage, generated=generated, evaluated=evaluated)
        return load(path/'manifest.json')['stage_sha256']


def register(root, plan, stage, *, generated=0, evaluated=0):
    AdvisoryResearchTrialRegistryV1(root.parent/'trial_registry.jsonl').append_batch((build_trial_record(
        experiment_id=root.name, attempt_id='exact_v1', research_stage=stage.upper(),
        study_type='LEARNABILITY_AUDIT', objective_contract='ALPHA_RANKING', decision_use='NAVIGATION_ONLY',
        hypothesis_family_id='ADVISORY_GP5_ENTRY_VALUE', parent_lineage=('ADVISORY_GP5_ENTRY_VALUE', Path(plan['source_root']).name),
        unique_variable=CONTRACT['hypothesis'], dataset_identity=sha([plan['source_prepared_sha256'], plan['binding_hash']]),
        schema_identity=CONTRACT['schema'], policy_identity=POLICY_SHA256, planned_trial_count=1,
        generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0, result_class='EXPLORATORY',
        consumed_windows=(dict(window_id='CONSUMED_DEVELOPMENT_ONLY', dataset_identity=plan['source_plan_sha256'],
            start_date=plan['train_start'], end_date=plan['evaluation_end']),),
        evidence_refs=(evidence_reference_for_file(root/stage/'manifest.json', role='serial_price_pressure_'+stage),)),))


def prepare(*, source_root, source_plan_sha256, source_prepared_sha256, binding, output_root, source_commit, fit_versions):
    source, output = node_path(source_root), node_path(output_root)
    if (re.fullmatch('[a-f0-9]{40}', source_commit) is None or source == output
            or source in output.parents or output in source.parents
            or set(fit_versions) != set(versions()) or any(not str(v) for v in fit_versions.values())):
        raise ValueError('explicit isolated producer/output and existing fit versions required')
    parent = read_stage(source/'preregistered', stage='preregistered', plan_sha256=source_plan_sha256, parent_sha256=None)
    prepared = read_stage(source/'prepared', stage='prepared', plan_sha256=source_plan_sha256, parent_sha256=parent['stage_sha256'])
    if prepared['stage_sha256'] != source_prepared_sha256:
        raise ValueError('source prepared identity mismatch')
    old = load(source/'preregistered/plan.json')
    dates = {k: old[k] for k in ('train_start', 'train_end', 'evaluation_start', 'evaluation_end')}
    if not dates['train_start'] <= dates['train_end'] < dates['evaluation_start'] <= dates['evaluation_end']:
        raise ValueError('source train/evaluation clocks differ')
    plan = dict(contract=CONTRACT, policy_sha256=POLICY_SHA256, source_root=source.as_posix(), source_commit=source_commit,
        source_plan_sha256=source_plan_sha256, source_prepared_sha256=source_prepared_sha256,
        implementation_sha256=implementation_sha256(), binding_hash=binding['binding_hash'],
        prepare_versions=versions(), fit_versions=fit_versions, sources=[{k: s[k] for k in ('package_id', 'manifest_sha256', 'run_id')} for s in old['sources']],
        physical_fit_budget=2, prior_supervised_fits=160, prior_diagnostic_index_count=1,
        inherited_evidence='QE_FROZEN_BACKTEST_SCORE_CURRENT_DATABASE_NON_VINTAGE_HISTORICAL_UNIVERSE_UNPROVEN', **dates)
    digest = sha(plan)
    output.mkdir(parents=True, exist_ok=True)
    with _exclusive_file_lock(output/'prepare.lock'), threadpool_limits(limits=2):
        identity = output/'study_identity.json'
        if identity.exists() and load(identity) != dict(plan_sha256=digest):
            raise ValueError('output already belongs to another study; no result-driven new lineage')
        if not identity.exists():
            with identity.open('xb') as stream:
                stream.write(_json_bytes(dict(plan_sha256=digest)))
        root = output/('advserialprice_'+digest[:24])
        started = time.monotonic()
        full = pd.read_parquet(source/'prepared/rows.parquet', columns=[*ROSTER, *DAILY])
        days, calendar = pd.read_parquet(source/'prepared/days.parquet'), load(source/'prepared/calendar.json')
        roster = validate_roster(full, days, calendar, plan['sources'], dates['train_start'], dates['evaluation_end'])
        if len(roster) > CONTRACT['max_rows']:
            raise ValueError('original Top20 exceeds preregistered input budget')
        # Do not decode evaluation finance. Source integrity above hashes bytes only.
        train_finance = pd.read_parquet(source/'prepared/rows.parquet', columns=[*ROSTER, *FINANCE], filters=[
            ('selection_effective_rank', '<=', 20), ('decision_as_of_trade_date', '<=', pd.Timestamp(dates['train_end'])),
            ('label_information_end', '<=', pd.Timestamp(dates['train_end']))])
        root.mkdir(parents=True, exist_ok=True)
        study = Study(root, plan=plan)
        minute, receipt = read_minute_inputs(binding, roster, run_id=root.name, budget=study.budget)
        inputs = attach_features(roster, full.loc[full.selection_effective_rank.le(20)], minute)
        training = inputs.loc[inputs.decision_as_of_trade_date.le(dates['train_end'])].merge(
            train_finance, on=list(ROSTER), how='left', validate='one_to_one', indicator=True)
        horizon_map = days.set_index(['package_id', 'decision_date']).horizon_end
        expected_horizon = pd.MultiIndex.from_frame(training[['package_id', 'decision_as_of_trade_date']]).map(horizon_map)
        expected_horizon = pd.Series(expected_horizon, index=training.index)
        if (expected_horizon.isna().any() or
                (training['_merge'].ne('both') & expected_horizon.le(pd.Timestamp(dates['train_end']))).any() or
                (training['_merge'].eq('both') & training.label_information_end.ne(expected_horizon)).any()):
            raise ValueError('training source identity differs')
        training['label_information_end'] = expected_horizon
        training.loc[training['_merge'].eq('left_only'), 'valuation_status'] = 'UNKNOWN_OUTSIDE_TRAIN_CUTOFF'
        training = training.drop(columns='_merge')
        train, supervised = training_population(training, train_start=dates['train_start'], train_end=dates['train_end'])
        day_y = supervised.groupby('decision_as_of_trade_date').hypothetical_liquidation_net_bps.mean()
        training_spec = dict(training_key_sha256=sha([[str(p), pd.Timestamp(d).isoformat(), str(s)] for p, d, s in supervised[KEY].itertuples(index=False, name=None)]),
            encoding=fit_encoding(numeric(train, CANDIDATE)), raw_columns=list(CANDIDATE),
            training_scale_mde_proxy_bps=float(3.083*day_y.std(ddof=1)/np.sqrt(len(day_y))),
            mde_semantics='TRAIN_LABEL_DAY_SCALE_PROXY_NOT_PAIRED_CONFIRMATORY_POWER',
            training_rows=len(train), supervised_rows=len(supervised), physical_fit_budget=2,
            strata={s: dict(rows=int(train.csi300_ret_5.map(market_stratum).eq(s).sum()),
                days=int(train.loc[train.csi300_ret_5.map(market_stratum).eq(s), 'decision_as_of_trade_date'].nunique()))
                for s in ('POSITIVE', 'NEGATIVE', 'ZERO', 'UNKNOWN')})
        payload = {'inputs.parquet': parquet_bytes(inputs), 'training.parquet': parquet_bytes(training),
            'days.parquet': parquet_bytes(days), 'training_spec.json': _json_bytes(training_spec)}
        receipt.update(original_rows=len(roster), original_package_days=len(days), features=list(CANDIDATE),
            original_score_dtype=str(roster.parent_score.dtype), source_rows=len(full), outcome_columns_decoded='TRAIN_ONLY',
            evaluation_finance_decoded=False, fit_count=0, wall_seconds=time.monotonic()-started,
            peak_rss_bytes=study.peak, unknown_counts=inputs[list(CANDIDATE)].isna().sum().to_dict())
        # Full input validation completed before experiment registration, and both
        # training encoding and source rows are published before the first fit.
        pre = publish_stage(study_root=root, stage='preregistered', plan_sha256=digest, parent_sha256=None,
            artifacts={'plan.json': _json_bytes(plan), 'binding.json': _json_bytes(binding)})
        register(root, plan, 'preregistered')
        pre_sha = load(pre/'manifest.json')['stage_sha256']
        if (root/'prepared').exists():
            study.read('prepared', pre_sha)
            if any(file_sha256(root/'prepared'/name) != hashlib_sha(content) for name, content in payload.items()) or load(root/'prepared/receipt.json')['consumed_slice_sha256'] != receipt['consumed_slice_sha256']:
                raise ValueError('prepared actual input drift on exact retry')
            register(root, plan, 'prepared')
        else:
            study.publish('prepared', pre_sha, {**payload, 'receipt.json': _json_bytes(receipt)})
        return dict(study_root=root.as_posix(), plan_sha256=digest, status='PREPARED_NO_FITS', input_receipt=receipt)


def hashlib_sha(content):
    import hashlib
    return hashlib.sha256(content).hexdigest()


def fit(root):
    if os.name == 'nt':
        raise ValueError('supervised fitting requires the existing WSL environment')
    study = Study(root)
    if versions() != study.plan['fit_versions']:
        raise ValueError('fit library identity differs from preregistration')
    with _exclusive_file_lock(study.root/'study.lock'), threadpool_limits(limits=2):
        pre = study.read('preregistered', None)
        prep = study.read('prepared', pre)
        if (study.root/'trained').exists():
            study.read('trained', prep)
            register(study.root, study.plan, 'trained', generated=1)
            return dict(status='EXACT_RETRY_NO_REFIT', study_root=study.root.as_posix())
        journal = study.root/'fit_journal.jsonl'
        if journal.exists():
            raise ValueError('partial physical fit journal exists; no implicit refit')
        with journal.open('xb') as stream:
            stream.write(_json_bytes(dict(event='STUDY_STARTED', plan_sha256=study.digest))+b'\n')
        def event(arm, kind):
            study.budget()
            with journal.open('ab') as stream:
                stream.write(_json_bytes(dict(arm=arm, event=kind))+b'\n')
        training = pd.read_parquet(study.root/'prepared/training.parquet')
        spec = load(study.root/'prepared/training_spec.json')
        fitted = train_models(training, train_start=study.plan['train_start'], train_end=study.plan['train_end'],
            frozen_encoding=spec['encoding'],
            before_fit=lambda a: event(a, 'FIT_STARTED'), after_fit=lambda a: event(a, 'FIT_FINISHED'))
        if fitted['supervision_key_sha256'] != load(study.root/'prepared/training_spec.json')['training_key_sha256']:
            raise ValueError('actual supervision differs from pre-fit registration')
        inputs = pd.read_parquet(study.root/'prepared/inputs.parquet')
        evaluation = inputs.loc[inputs.decision_as_of_trade_date.between(study.plan['evaluation_start'], study.plan['evaluation_end'])].reset_index(drop=True)
        predictions = predict(evaluation, fitted)
        fitted.update(fit_versions=versions(), source_commit=study.plan['source_commit'],
            wall_seconds=time.monotonic()-study.started, peak_rss_bytes=study.peak, evaluation_finance_decoded=False)
        study.publish('trained', prep, {'model.json': _json_bytes(fitted), 'predictions.parquet': parquet_bytes(predictions)}, generated=1)
        return dict(status='TRAINED_PREDICTIONS_FROZEN', study_root=study.root.as_posix(), physical_fit_count=fitted['physical_fit_count'])


def finish(root):
    study = Study(root)
    with _exclusive_file_lock(study.root/'study.lock'), threadpool_limits(limits=2):
        pre = study.read('preregistered', None)
        prep = study.read('prepared', pre)
        trained = study.read('trained', prep)
        if (study.root/'evaluated').exists():
            study.read('evaluated', trained)
            register(study.root, study.plan, 'evaluated', generated=1, evaluated=1)
            return dict(load(study.root/'evaluated/evaluation.json'), exact_retry=True)
        source = node_path(study.plan['source_root'])
        parent = read_stage(source/'preregistered', stage='preregistered', plan_sha256=study.plan['source_plan_sha256'], parent_sha256=None)
        bound = read_stage(source/'prepared', stage='prepared', plan_sha256=study.plan['source_plan_sha256'], parent_sha256=parent['stage_sha256'])
        if bound['stage_sha256'] != study.plan['source_prepared_sha256']:
            raise ValueError('finance source prepared drift')
        # Trained stage, predictions and choices are immutable before this first Y decode.
        finance = pd.read_parquet(source/'prepared/rows.parquet', columns=[*ROSTER, *FINANCE], filters=[
            ('selection_effective_rank', '<=', 20), ('decision_as_of_trade_date', '>=', pd.Timestamp(study.plan['evaluation_start'])),
            ('decision_as_of_trade_date', '<=', pd.Timestamp(study.plan['evaluation_end']))])
        inputs = pd.read_parquet(study.root/'prepared/inputs.parquet')
        inputs = inputs.loc[inputs.decision_as_of_trade_date.between(study.plan['evaluation_start'], study.plan['evaluation_end'])].reset_index(drop=True)
        days = pd.read_parquet(study.root/'prepared/days.parquet')
        days = days.loc[days.decision_date.between(study.plan['evaluation_start'], study.plan['evaluation_end'])]
        result = evaluate(inputs, finance, days, pd.read_parquet(study.root/'trained/predictions.parquet'), evaluation_end=study.plan['evaluation_end'])
        result.update(physical_fit_count=load(study.root/'trained/model.json')['physical_fit_count'], candidate_count=1,
            inherited_evidence=study.plan['inherited_evidence'], source_commit=study.plan['source_commit'],
            wall_seconds=time.monotonic()-study.started, peak_rss_bytes=study.peak, sql_count=0)
        study.publish('evaluated', trained, {'evaluation.json': _json_bytes(result)}, generated=1, evaluated=1)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage', choices=('prepare', 'fit', 'evaluate'))
    parser.add_argument('--config', help='prepare keyword arguments JSON, absolute non-C path')
    parser.add_argument('--study-root', help='immutable root returned by prepare')
    args = parser.parse_args()
    result = prepare(**load(args.config)) if args.stage == 'prepare' else fit(args.study_root) if args.stage == 'fit' else finish(args.study_root)
    print(json.dumps({k: v for k, v in result.items() if k not in ('daily', 'package_days', 'skills', 'input_receipt')}, ensure_ascii=False, allow_nan=False))


if __name__ == '__main__':
    main()
