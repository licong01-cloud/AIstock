"""One-owner historical factor-information experiment; no live rank publication."""
import argparse
from datetime import datetime, timezone
import importlib.metadata
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
import psutil  # noqa: E402
import requests  # noqa: E402
from threadpoolctl import threadpool_limits  # noqa: E402

from scripts import advisory_lstm_rank_value_review as files  # noqa: E402
from backend.services.advisory_model_first import lstm_factor_information_v1 as core  # noqa: E402
from backend.services.advisory_model_first import lstm_specialist_spo_v1 as old  # noqa: E402
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage, read_stage  # noqa: E402
from backend.services.advisory_model_first.lstm_historical_engineering_replay import verify_fees  # noqa: E402
from backend.services.advisory_model_first.research_control import (  # noqa: E402
    AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import build_trial_record  # noqa: E402

OUTPUT = Path('X:/AIstock_temp/ADVISORY/lstm-factor-information-20261102')
P19 = Path('X:/AIstock_temp/ADVISORY/p19-lstm-specialist-20261027/recovery-v2')
P18 = Path('X:/AIstock_temp/ADVISORY/p18-lstm-complementarity-20261027')
OLD_STRESS = Path('X:/AIstock_temp/ADVISORY/lstm-frozen-specialist-stress-20261102/recovery-v2')
SOURCE_FILES = (
    'scripts/advisory_lstm_factor_information.py',
    'backend/services/advisory_model_first/lstm_factor_information_v1.py',
    'backend/services/advisory_model_first/spo_top10_5td_v1.py',
    'backend/services/advisory_model_first/lstm_specialist_spo_v1.py',
    'backend/services/advisory_model_first/lstm_complementarity_v1.py',
    'backend/services/advisory_model_first/peer_residual_top10_5td_v1.py',
    'backend/services/advisory_model_first/peer_lead_lag_top10_5td_v1.py',
    'backend/services/advisory_model_first/native_anchored_top10_5td_v1.py',
    'backend/services/advisory_model_first/multiperiod_top10_5td_study_v1.py',
    'backend/services/advisory_model_first/multiperiod_top10_5td_inputs_v1.py',
    'backend/services/advisory_model_first/economic_entry_pipeline.py',
    'backend/services/advisory_model_first/lstm_historical_engineering_replay.py',
    'backend/services/advisory_model_first/research_control.py',
    'backend/services/advisory_model_first/research_control_contracts.py',
    'scripts/advisory_lstm_rank_value_review.py',
)


def load(path):
    return json.loads(Path(path).read_bytes())


def ref(path):
    return dict(path=str(path), sha256=files.file_sha(path), size_bytes=path.stat().st_size)


def source_paths():
    result = {n: OUTPUT / (n + '.json') for n in ('input_review', 'novelty_review', 'package_identity', 'formula_review', 'decision')}
    result.update(inputs=OUTPUT / 'factor_inputs.parquet', old_protocol=P19 / 'protocol.json',
        old_inputs=P19 / 'study/prepared/inputs.parquet', finance=P19 / 'study/evaluated/finance.parquet',
        stress_inputs=P18 / 'study/prepared/target_inputs.parquet',
        stress_finance=P18 / 'study/evaluated/target_finance.parquet',
        stress_comparator=OLD_STRESS / 'trained/predictions.parquet')
    for fold in (1, 2, 3):
        for name, filename in [('model', 'model.json'), ('prediction', 'predictions.parquet'), ('train', 'train_finance.parquet')]:
            result[name + str(fold)] = P19 / f'study/fold{fold}/trained' / filename
    return result


def closure(committed=False):
    result = {n: files.code_sha((ROOT / n).read_bytes()) for n in SOURCE_FILES}
    if committed:
        for name, digest in result.items():
            if files.code_sha(subprocess.check_output(['git', 'show', f'HEAD:{name}'], cwd=ROOT)) != digest:
                raise ValueError('uncommitted producer source: ' + name)
    return result


def versions():
    return dict(python=sys.version, executable=sys.executable,
                **{n: importlib.metadata.version(n) for n in ('numpy', 'pandas', 'scipy', 'pyarrow')})


def checked(plan, name):
    path = source_paths()[name]
    if ref(path) != plan['sources'][name]:
        raise ValueError('immutable source drift: ' + name)
    return path


def verify(plan):
    if (plan['contract'] != json.loads(files.json_bytes(core.CONTRACT))
            or plan['closure'] != closure() or plan['versions'] != versions()):
        raise ValueError('contract, code or environment drift')
    for name in source_paths():
        checked(plan, name)


def qe_idle():
    endpoints = ('/quantevolver/experiments?status=running&limit=200',
                 '/quantevolver/evolution/tasks?status=running', '/quantevolver/evolution/tasks?status=pending')
    rows = []
    for endpoint in endpoints:
        response = requests.get('http://127.0.0.1:8001/api/v1' + endpoint, timeout=20)
        response.raise_for_status()
        value = response.json()
        if 'experiments?' in endpoint:
            if value.get('ok') is not True or not isinstance(value.get('total'), int):
                raise ValueError('invalid QE experiment status response')
            count = value['total']
        else:
            if value.get('status') != 'success' or not isinstance(value.get('data'), list):
                raise ValueError('invalid QE task status response')
            count = len(value['data'])
        rows.append(dict(endpoint=endpoint, active=count))
    if any(r['active'] for r in rows):
        raise ValueError('QE training owner is active; no research fit started')
    return rows


def freeze():
    if subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT).strip():
        raise ValueError('freeze from a clean committed producer')
    sources = {n: ref(p) for n, p in source_paths().items()}
    review = load(sources['input_review']['path'])
    if (review['original_rows'] != 21150 or len(review['fields']) != 46
            or not review['source_metadata_unchanged'] or review['fit_count'] != 0
            or review['projected_input'] != sources['inputs']):
        raise ValueError('zero-fit input audit incomplete')
    if (load(sources['decision']['path'])['state'] != 'ONE_CANDIDATE_DESIGN_READY'
            or load(sources['formula_review']['path'])['status'] != 'PASS_VERSIONED_RECONSTRUCTION_CONTRACT'):
        raise ValueError('input prerequisite decision or formula review incomplete')
    expected = [n.removeprefix('factor__') for n in core.EXTRA]
    if [f['name'] for f in review['fields']] != expected:
        raise ValueError('factor roster differs')
    # Verify the already completed archive, without financial value decoding.
    protocol = load(P19 / 'protocol.json')
    parent = None
    for stage in ('preregistered', 'prepared', 'fold1_trained', 'fold2_trained', 'fold3_trained', 'evaluated', 'completed'):
        path = old.stage_location(P19 / 'study', stage)
        parent = read_stage(path, stage=path.name, plan_sha256=core.spo.sha(protocol), parent_sha256=parent)['stage_sha256']
    if parent != files.TERMINAL_SHA:
        raise ValueError('old P19 terminal differs')
    plan = dict(schema=core.CONTRACT['schema'], contract=core.CONTRACT, sources=sources,
        closure=closure(True), versions=versions(), archive_terminal=parent, max_new_fits=3,
        producer_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        created_at=datetime.now(timezone.utc).isoformat(), experiment_id='advlstmfactor_20261102_v1',
        input_status='READY_FOR_ONE_NON_VINTAGE_EXPLORATORY_CANDIDATE',
        alpha158_semantics='VERSIONED_RECONSTRUCTION_NOT_ORIGINAL_TENSOR_PARITY',
        inf_policy='DOCUMENTED_MOMENTUM_DIVISION_BY_ZERO_TO_MISSING_BIT_SOURCE_UNCHANGED')
    publish_stage(study_root=OUTPUT / 'study', stage='preregistered', plan_sha256=core.spo.sha(plan),
                  parent_sha256=None, artifacts={'protocol.json': files.json_bytes(plan)})
    print(json.dumps(dict(status='PREREGISTERED', plan_sha256=core.spo.sha(plan), new_fits=0)))


def run():
    study = OUTPUT / 'study'
    plan = load(study / 'preregistered/protocol.json')
    digest = core.spo.sha(plan)
    verify(plan)
    terminal = study / 'terminal/evaluated/receipt.json'
    if terminal.exists():
        parent = None
        for relative in ['preregistered', 'prepared', 'fold1/trained', 'fold2/trained', 'fold3/trained', 'evaluated', 'terminal/evaluated']:
            p = study / relative
            parent = read_stage(p, stage=p.name, plan_sha256=digest, parent_sha256=parent)['stage_sha256']
        print(json.dumps(dict(status='STUDY_REUSED', new_fits=0, terminal_sha256=parent)))
        return
    if list((study / 'fit_claims').glob('*_started.json')):
        raise ValueError('partial prior attempt: no automatic refit/recovery')
    started = time.monotonic()
    claims = old.FitClaims(study / 'fit_claims', digest)
    telemetry = []

    def budget():
        memory = psutil.Process().memory_info()
        if max(memory.rss, getattr(memory, 'peak_wset', memory.rss)) > 2 * 1024**3 or time.monotonic() - started > 1800:
            raise ValueError('frozen 2GiB/1800s research budget exceeded')

    def publish(relative, parent, artifacts):
        budget()
        verify(plan)
        if sum(p.stat().st_size for p in study.rglob('*') if p.is_file()) + sum(map(len, artifacts.values())) > 200 * 1024**2:
            raise ValueError('research output exceeds 200MiB')
        path = study / relative
        publish_stage(study_root=path.parent, stage=path.name, plan_sha256=digest,
                      parent_sha256=parent, artifacts=artifacts)
        return read_stage(path, stage=path.name, plan_sha256=digest, parent_sha256=parent)['stage_sha256']

    try:
        parent = read_stage(study / 'preregistered', stage='preregistered', plan_sha256=digest,
                            parent_sha256=None)['stage_sha256']
        inputs = core.ordered(pd.read_parquet(checked(plan, 'inputs')))
        core.check_inputs(inputs)
        dev = inputs.loc[inputs[core.DAY].le('2025-09-30')].reset_index(drop=True)
        stress = inputs.loc[inputs[core.DAY].ge(core.prior.FIRST)].reset_index(drop=True)
        original = core.ordered(pd.read_parquet(checked(plan, 'old_inputs')))
        pd.testing.assert_frame_equal(dev[list(original.columns)], original)
        stress_original = core.ordered(pd.read_parquet(checked(plan, 'stress_inputs')))
        pd.testing.assert_frame_equal(stress[list(stress_original.columns)], stress_original)
        if len(dev) != 15250 or len(stress) != 5900:
            raise ValueError('original population differs')
        parent = publish('prepared', parent, {'inputs.parquet': files.parquet_bytes(inputs),
            'receipt.json': files.json_bytes(dict(original_rows=21150, new_labels_read=False, fit_count=0))})
        for fold, (cutoff, first, last) in enumerate(core.spo.base.FOLDS, 1):
            finance = pd.read_parquet(checked(plan, 'train' + str(fold)))
            selected = dev.loc[dev[core.DAY].le(cutoff)]
            train = selected.merge(finance, on=core.KEY, how='left', validate='one_to_one', indicator=True)
            if not train._merge.eq('both').all():
                raise ValueError('original training population lost')
            train = train.drop(columns='_merge')
            old_packet = load(checked(plan, 'model' + str(fold)))
            targets = dev.loc[dev[core.DAY].between(first, last)].reset_index(drop=True)
            comparator = core.spo.predict(targets, old_packet['model'])
            saved = core.ordered(pd.read_parquet(checked(plan, 'prediction' + str(fold))))
            pd.testing.assert_frame_equal(comparator, saved, check_exact=False, rtol=1e-12, atol=1e-12)

            def before_fit(fold=fold):
                budget()
                telemetry.append(dict(fold=fold, moment='BEFORE_START', qe=qe_idle()))
                claims.write(fold)

            packet = core.fit(train, fold, old_packet['model'], before_fit=before_fit, budget=budget)
            checkpoints = study / 'fit_checkpoints'
            checkpoints.mkdir(exist_ok=True)
            with (checkpoints / f'fold{fold}.json').open('xb') as checkpoint:
                checkpoint.write(files.json_bytes(packet))
            prediction = core.predict(targets, packet, phase='fold')
            artifacts = {'model.json': files.json_bytes(packet), 'predictions.parquet': files.parquet_bytes(prediction),
                'comparator.parquet': files.parquet_bytes(comparator),
                'receipt.json': files.json_bytes(dict(fold=fold, new_fits=1, training_support=packet['training_support'],
                    reused_comparator=True, evaluation_labels_decoded=False, certificate=packet['certificate']))}
            if fold == 3:
                artifacts['stress_predictions.parquet'] = files.parquet_bytes(core.predict(stress, packet, phase='stress'))
            parent = publish(f'fold{fold}/trained', parent, artifacts)
            claims.write(fold, parent)
            telemetry.append(dict(fold=fold, moment='AFTER_FINISH', qe=qe_idle()))
            print(json.dumps(dict(status='FIT_FINISHED', fold=fold, certificate=packet['certificate'])), flush=True)
        panels, old_panels = [], []
        for fold in (1, 2, 3):
            # Evaluation values are opened only after all corresponding predictions exist.
            finance = pd.read_parquet(checked(plan, 'finance'), filters=[('fold', '==', fold)]).drop(columns='fold')
            verify_fees(finance)
            prediction = pd.read_parquet(study / f'fold{fold}/trained/predictions.parquet')
            comparator = pd.read_parquet(study / f'fold{fold}/trained/comparator.parquet')
            for target, sink in ((prediction, panels), (comparator, old_panels)):
                p = core.spo.base.cohort(target, finance)
                p['fold'] = fold
                sink.append(p)
        panel, old_panel = pd.concat(panels, ignore_index=True), pd.concat(old_panels, ignore_index=True)
        if len(panel) != 126 or int(panel.original_candidates.sum()) != 6300:
            raise ValueError('development evaluation axis differs')
        development = core.summarize(panel, old_panel)
        fold_means = {str(int(f)): float(g.delta.mean()) if g.delta.notna().any() else None for f, g in panel.groupby('fold')}
        stress_finance = pd.read_parquet(checked(plan, 'stress_finance'))
        verify_fees(stress_finance)
        stress_pred = pd.read_parquet(study / 'fold3/trained/stress_predictions.parquet')
        stress_old = pd.read_parquet(checked(plan, 'stress_comparator'))
        pd.testing.assert_frame_equal(core.spo.predict(stress, load(checked(plan, 'model3'))['model']),
            stress_old, check_exact=False, rtol=1e-12, atol=1e-12)
        stress_report, stress_panel = core.prior.stress_report(stress_pred, stress_finance, sorted(stress[core.DAY].unique()))
        stress_old_panel = core.spo.base.cohort(stress_old, stress_finance)
        pressure = core.summarize(stress_panel, stress_old_panel)
        result = dict(development=development, by_fold=fold_means, stress=pressure,
            legacy_stress_classification=stress_report['classification'],
            decision=core.decision(development, pressure, fold_means), actual_new_fits=3,
            cumulative_actual_fits=251, old_index_count=1, original_rows=21150,
            parent_tensor_parity_proven=False, historical_pit_proven=False,
            independent_economic_confirmation=False, production_ready=False)
        parent = publish('evaluated', parent, {'report.json': files.json_bytes(result),
            'development.parquet': files.parquet_bytes(panel), 'comparator.parquet': files.parquet_bytes(old_panel),
            'stress.parquet': files.parquet_bytes(stress_panel), 'stress_comparator.parquet': files.parquet_bytes(stress_old_panel),
            'resource.json': files.json_bytes(dict(qe_checks=telemetry, elapsed_seconds=time.monotonic() - started,
                rss_bytes=psutil.Process().memory_info().rss, cpu_threads=2, gpu=0))})
        receipt = dict(status='COMPLETED', **result, evaluated_sha256=parent)
        parent = publish('terminal/evaluated', parent, {'receipt.json': files.json_bytes(receipt)})
        record = build_trial_record(experiment_id=plan['experiment_id'], attempt_id='exact_v1', research_stage='COMPLETED',
            study_type='EXPLORATORY_SCREEN', objective_contract='ALPHA_RANKING', decision_use='NAVIGATION_ONLY',
            hypothesis_family_id=core.CONTRACT['hypothesis'], unique_variable='FIXED_PARENT_46_FACTOR_INFORMATION',
            parent_lineage=(load(P19 / 'protocol.json')['experiment_id'],), dataset_identity=plan['sources']['inputs']['sha256'],
            schema_identity=core.spo.sha(core.CONTRACT), policy_identity=core.spo.base.VALUATION_POLICY_SHA256,
            planned_trial_count=1, generated_trial_count=1, evaluated_trial_count=1, selected_trial_count=0,
            result_class='EXPLORATORY', consumed_windows=(dict(window_id='ALREADY_CONSUMED_DEVELOPMENT_AND_STRESS',
                dataset_identity=plan['sources']['inputs']['sha256'], start_date='2024-07-04', end_date='2026-08-28'),),
            evidence_refs=(evidence_reference_for_file(study / 'terminal/evaluated/manifest.json', role='factor_information_terminal'),))
        AdvisoryResearchTrialRegistryV1(OUTPUT / 'trial_registry.jsonl').append_batch((record,))
        print(json.dumps(dict(status='COMPLETED', decision=result['decision'], new_fits=3,
            terminal_sha256=parent, development_bps=development['statistics']['mean_bps'], stress_bps=pressure['statistics']['mean_bps'])))
    except BaseException as exc:
        with (OUTPUT / 'execution_failure.json').open('x', encoding='utf8') as f:
            json.dump(dict(status='FAILED_NO_AUTOMATIC_REFIT', error_type=type(exc).__name__, error=str(exc),
                actual_started_claims=len(list(claims.root.glob('*_started.json'))),
                actual_finished_claims=len(list(claims.root.glob('*_finished.json')))), f, indent=2)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['freeze', 'run'])
    args = parser.parse_args()
    with threadpool_limits(limits=2), _exclusive_file_lock(OUTPUT / 'runner.lock'):
        freeze() if args.action == 'freeze' else run()


if __name__ == '__main__':
    main()
