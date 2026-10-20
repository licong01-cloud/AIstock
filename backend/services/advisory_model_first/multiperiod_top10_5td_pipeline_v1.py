"""Staged, X-only Advisory research. No training dispatch or production writes."""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import pickle
import re
import sys
import time

import pandas as pd
import psutil
from threadpoolctl import threadpool_limits

from backend.mcp.common import AIstockApiClient
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import (
    IDENTITY, build_features, file_dataset, finance_inputs, original_rosters, read_daily,
)
from backend.services.advisory_model_first.multiperiod_top10_5td_study_v1 import (
    CONFIGS, CONTRACT, evaluate, fit_model, predict, select_configuration,
)
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import ParentScoreSourceV1, node_path
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

STAGES = ('preregistered', 'prepared', 'selected', 'predicted', 'evaluated')


def load(path):
    return json.loads(Path(path).read_bytes())


def versions():
    return {name: version(name) for name in ('numpy', 'pandas', 'scikit-learn', 'pyarrow')}


def implementation_sha256():
    root = Path(__file__).parents[2]
    names = ['services/advisory_model_first/multiperiod_top10_5td_'+n+'_v1.py' for n in ('inputs', 'study', 'pipeline')]
    names += ['data_service/frozen_dataset.py', 'services/strategy_package/runtime_variant.py']
    names += ['services/advisory_model_first/'+n+'.py' for n in ('cross_package_validation_inputs_v1',
        'cross_package_validation_source_v1', 'generic_daily_price_input_v1', 'generic_price_5td_contracts_v1',
        'generic_price_5td_labels_v1', 'generic_price_5td_valuation_v2', 'parent_score_price_5td_inputs_v1',
        'parent_score_price_5td_contracts_v1', 'group_centered_rank_5td_study_v1', 'economic_entry_pipeline',
        'research_control', 'research_control_contracts')]
    return sha({n: hashlib.sha256((root/n).read_bytes().replace(b'\r\n', b'\n')).hexdigest() for n in names})


def stage_location(root, stage):
    # Reuse the existing four-name publisher without changing shared code.
    if stage in ('selected', 'predicted'):
        return root/stage/'trained', 'trained'
    return root/stage, stage


class Study:
    def __init__(self, root, plan=None):
        self.root = node_path(root)
        if not (self.root.as_posix().lower().startswith('/mnt/x/') or self.root.drive.upper() == 'X:'):
            raise ValueError('study output must be on the approved X volume')
        self.plan = plan or load(self.root/'preregistered/plan.json')
        if (self.plan['contract'] != json.loads(json.dumps(CONTRACT))
                or self.plan['implementation_sha256'] != implementation_sha256()
                or not re.fullmatch(r'[0-9a-f]{40}', self.plan['source_commit'])
                or self.plan['end'] >= CONTRACT['sealed_start'] or self.plan['start'] != CONTRACT['start']
                or self.plan['end'] != CONTRACT['end'] or len(self.plan['sources']) != 2
                or len({s['package_id'] for s in self.plan['sources']}) != 2):
            raise ValueError('research plan/implementation/window identity differs')
        self.digest = sha(self.plan)
        for source in self.plan['sources']:
            ParentScoreSourceV1.model_validate(source)
        if self.plan['experiment_id'] != 'advmultitop10_'+sha({k: v for k, v in self.plan.items() if k != 'experiment_id'})[:24]:
            raise ValueError('experiment identity does not bind the frozen plan')
        self.started, self.peak = time.monotonic(), 0

    def budget(self):
        self.peak = max(self.peak, psutil.Process().memory_info().rss)
        if self.peak > 2*1024**3 or time.monotonic()-self.started > 1800:
            raise ValueError('explicit 2GiB/30-minute stage resource budget exceeded')

    def read(self, stage):
        parent = None
        for item in STAGES:
            path, native = stage_location(self.root, item)
            header = read_stage(path, stage=native, plan_sha256=self.digest, parent_sha256=parent)
            parent = header['stage_sha256']
            if item == stage:
                return parent
        raise ValueError('unknown research stage')

    def publish(self, stage, artifacts):
        index = STAGES.index(stage)
        parent = self.read(STAGES[index-1]) if index else None
        path, native = stage_location(self.root, stage)
        if sum(map(len, artifacts.values())) > 2*1024**3:
            raise ValueError('stage artifact budget exceeded')
        self.budget()
        target = publish_stage(study_root=path.parent, stage=native, plan_sha256=self.digest,
            parent_sha256=parent, artifacts=artifacts)
        self.register(stage)
        return target

    def register(self, stage):
        generated = 6 if STAGES.index(stage) >= STAGES.index('selected') else 0
        path, _ = stage_location(self.root, stage)
        record = build_trial_record(experiment_id=self.plan['experiment_id'], attempt_id='exact_attempt_v1',
            research_stage=stage.upper(), study_type='EXPLORATORY_SCREEN', objective_contract='ALPHA_RANKING',
            decision_use='NAVIGATION_ONLY', hypothesis_family_id='MULTIPERIOD_TOP10_FIXED5_FINITE_MODELS_V1',
            unique_variable='MULTIPERIOD_INPUTS_AND_PREREGISTERED_FINITE_LINEAR_NONLINEAR_COMPARISON',
            parent_lineage=tuple(s['run_id'] for s in self.plan['sources']), dataset_identity=self.plan['binding']['binding_hash'],
            schema_identity=sha(CONTRACT), policy_identity=CONTRACT['valuation_policy_sha256'], planned_trial_count=6,
            generated_trial_count=generated, evaluated_trial_count=generated, selected_trial_count=int(bool(generated)),
            result_class='EXPLORATORY', consumed_windows=(dict(window_id='CONSUMED_DEVELOPMENT_ONLY',
                dataset_identity=self.plan['binding']['binding_hash'], start_date=self.plan['start'], end_date=self.plan['end']),),
            evidence_refs=(evidence_reference_for_file(path/'manifest.json', role='multiperiod_'+stage),))
        AdvisoryResearchTrialRegistryV1(self.root.parent/'trial_registry.jsonl').append_batch((record,))

    def fit_event(self, key, *, complete=False):
        self.budget()
        folder = self.root/'fit_claims'
        folder.mkdir(exist_ok=True)
        claims = list(folder.glob('*_started.json'))
        target = folder/(key+('_finished.json' if complete else '_started.json'))
        if not re.fullmatch(r'[a-z0-9_.]+', key):
            raise ValueError('invalid fit identity')
        if complete:
            if not (folder/(key+'_started.json')).exists():
                raise ValueError('fit completion has no claim')
        elif len(claims) >= CONTRACT['physical_fit_budget']:
            raise ValueError('physical fit budget exhausted')
        payload = dict(fit_key=key, plan_sha256=self.digest, event='COMPLETED' if complete else 'STARTED',
            captured_at=datetime.now(timezone.utc).isoformat())
        # Exclusive claims mean an interrupted fit cannot silently fit again.
        with target.open('xb') as stream:
            stream.write(_json_bytes(payload))
            stream.flush()
            os.fsync(stream.fileno())

    def verify_fits(self, *, final=False):
        expected = {c['id']+'_fold'+str(i) for c in CONFIGS for i in range(1, 4)}
        if final:
            expected.add('final')
        folder = self.root/'fit_claims'
        actual = set()
        for path in folder.glob('*_started.json'):
            entry = load(path)
            key = entry['fit_key']
            if (entry['plan_sha256'] != self.digest or path.name != key+'_started.json'
                    or entry['event'] != 'STARTED' or key in actual):
                raise ValueError('fit claim identity changed')
            done = load(folder/(key+'_finished.json'))
            if done['fit_key'] != key or done['plan_sha256'] != self.digest or done['event'] != 'COMPLETED':
                raise ValueError('fit completion identity changed')
            actual.add(key)
        if actual != expected or len(list(folder.glob('*_finished.json'))) != len(expected):
            raise ValueError('actual physical fits differ from the frozen budget/phase')
        return len(actual)

    def node(self):
        if os.name == 'nt' or str(Path(sys.executable).resolve()) != str(Path(self.plan['node_python_uri']).resolve()):
            raise ValueError('fit requires its explicit existing WSL Python')
        if versions() != self.plan['fit_versions']:
            raise ValueError('fit environment changed since preregistration')


def preregister(output_root, plan):
    plan = json.loads(json.dumps(plan))
    plan['experiment_id'] = 'advmultitop10_'+sha(plan)[:24]
    study = Study(node_path(output_root)/plan['experiment_id'], plan)
    study.publish('preregistered', {'plan.json': _json_bytes(plan)})
    return study.root


def prepare(root):
    study = Study(root)
    with _exclusive_file_lock(study.root/'study.lock'):
        study.read('preregistered')
        if (study.root/'prepared').exists():
            study.read('prepared')
            study.register('prepared')
            return dict(status='EXACT_RETRY_NO_PREPARE')
        dataset = file_dataset(study.plan)
        try:
            calendar = list(dataset.calendar)
            roster, days, sources = original_rosters(study.plan, calendar, AIstockApiClient(study.plan['api_base'], timeout=30))
            first = calendar.index(pd.Timestamp(study.plan['start']).date())-19
            if first < 0:
                raise ValueError('original D history has no real warmup')
            daily = read_daily(dataset, sorted(set(roster.instrument)), calendar[first], pd.Timestamp(study.plan['end']).date())
            inputs = build_features(roster, daily, calendar)
            train = roster.loc[roster[KEY[0]].le(pd.Timestamp(CONTRACT['train_end']))].reset_index(drop=True)
            finance, finance_receipt = finance_inputs(dataset, train, CONTRACT['train_end'], study.budget)
            receipt = dict(original_candidates=len(roster), package_days=len(days), scores=sources,
                train_finance=finance_receipt, outer_finance_decoded=False, sealed_finance_decoded=False,
                database_queries=0, physical_fit_count=0, wall_seconds=time.monotonic()-study.started,
                peak_rss_bytes=psutil.Process().memory_info().rss, prepare_versions=versions())
            dataset.assert_unchanged()
            study.publish('prepared', {'inputs.parquet': parquet_bytes(inputs), 'train_finance.parquet': parquet_bytes(finance),
                'days.parquet': parquet_bytes(days), 'calendar.json': _json_bytes([str(d) for d in calendar]), 'receipt.json': _json_bytes(receipt)})
            return {k: v for k, v in receipt.items() if k not in ('scores', 'train_finance')}
        finally:
            dataset.close()


def select(root):
    study = Study(root)
    study.node()
    with _exclusive_file_lock(study.root/'study.lock'), threadpool_limits(limits=2):
        study.read('prepared')
        path, _ = stage_location(study.root, 'selected')
        if path.exists():
            study.read('selected')
            study.register('selected')
            return dict(status='EXACT_RETRY_NO_SELECTION')
        inputs = pd.read_parquet(study.root/'prepared/inputs.parquet', filters=[(KEY[0], '<=', pd.Timestamp(CONTRACT['train_end']))])
        finance = pd.read_parquet(study.root/'prepared/train_finance.parquet')
        rows = inputs.merge(finance, on=['package_id', *KEY], validate='one_to_one')
        selected, predictions, panel = select_configuration(rows, before_fit=study.fit_event,
            after_fit=lambda key: study.fit_event(key, complete=True))
        study.verify_fits()
        selected.update(wall_seconds=time.monotonic()-study.started, peak_rss_bytes=study.peak)
        study.publish('selected', {'selection.json': _json_bytes(selected), 'cv_predictions.parquet': parquet_bytes(predictions),
            'cv_panel.parquet': parquet_bytes(panel)})
        return selected


def predict_outer(root):
    study = Study(root)
    study.node()
    with _exclusive_file_lock(study.root/'study.lock'), threadpool_limits(limits=2):
        study.read('selected')
        path, _ = stage_location(study.root, 'predicted')
        if path.exists():
            study.read('predicted')
            study.verify_fits(final=True)
            study.register('predicted')
            return dict(status='EXACT_RETRY_NO_REFIT')
        selected_path, _ = stage_location(study.root, 'selected')
        selected = load(selected_path/'selection.json')
        if selected['config'] not in CONFIGS:
            raise ValueError('selection is outside the registered model family')
        study.verify_fits()
        inputs = pd.read_parquet(study.root/'prepared/inputs.parquet')
        train = inputs.loc[inputs[KEY[0]].le(pd.Timestamp(CONTRACT['train_end']))].merge(
            pd.read_parquet(study.root/'prepared/train_finance.parquet'), on=['package_id', *KEY], validate='one_to_one')
        model, encoding = fit_model(train, CONTRACT['train_end'], selected['config'],
            before_fit=lambda: study.fit_event('final'), after_fit=lambda: study.fit_event('final', complete=True))
        outer = inputs.loc[inputs[KEY[0]].ge(pd.Timestamp(CONTRACT['outer_start']))]
        predictions = predict(outer, model, encoding)
        payload = pickle.dumps(model, protocol=5)  # Own fitted model only; not loaded from arbitrary caller input.
        model_packet = dict(pickle_base64=base64.b64encode(payload).decode(), pickle_sha256=hashlib.sha256(payload).hexdigest(),
            encoding=encoding, config=selected['config'], fit_versions=versions(), source_commit=study.plan['source_commit'])
        receipt = dict(physical_fit_count=19, outer_finance_decoded=False, wall_seconds=time.monotonic()-study.started,
            peak_rss_bytes=study.peak, model_sha256=model_packet['pickle_sha256'])
        receipt['physical_fit_count'] = study.verify_fits(final=True)
        study.publish('predicted', {'model.json': _json_bytes(model_packet), 'predictions.parquet': parquet_bytes(predictions), 'receipt.json': _json_bytes(receipt)})
        return receipt


def finish(root):
    study = Study(root)
    with _exclusive_file_lock(study.root/'study.lock'):
        study.read('predicted')  # Frozen selection and predictions precede this first outer Y read.
        physical_fit_count = study.verify_fits(final=True)
        if (study.root/'evaluated').exists():
            study.read('evaluated')
            study.register('evaluated')
            return dict(load(study.root/'evaluated/evaluation.json'), exact_retry=True)
        prediction_path, _ = stage_location(study.root, 'predicted')
        predictions = pd.read_parquet(prediction_path/'predictions.parquet')
        inputs = pd.read_parquet(study.root/'prepared/inputs.parquet', filters=[(KEY[0], '>=', pd.Timestamp(CONTRACT['outer_start']))])
        if not inputs.loc[:, IDENTITY].reset_index(drop=True).equals(predictions.loc[:, IDENTITY].reset_index(drop=True)):
            raise ValueError('outer predictions changed original identities/ranks')
        dataset = file_dataset(study.plan)
        try:
            finance, receipt = finance_inputs(dataset, inputs, study.plan['end'], study.budget)
            result, panel, daily = evaluate(predictions, finance)
            result.update(wall_seconds=time.monotonic()-study.started, peak_rss_bytes=psutil.Process().memory_info().rss,
                experiment_id=study.plan['experiment_id'], source_commit=study.plan['source_commit'], physical_fit_count=physical_fit_count)
            study.publish('evaluated', {'evaluation.json': _json_bytes(result), 'panel.parquet': parquet_bytes(panel),
                'daily.parquet': parquet_bytes(daily), 'finance.parquet': parquet_bytes(finance), 'source_receipt.json': _json_bytes(receipt)})
            return result
        finally:
            dataset.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage', choices=('prepare', 'select', 'predict', 'finish'))
    parser.add_argument('--root', required=True)
    args = parser.parse_args()
    result = {'prepare': prepare, 'select': select, 'predict': predict_outer, 'finish': finish}[args.stage](args.root)
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))


if __name__ == '__main__':
    main()
