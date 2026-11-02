"""Freeze and execute exactly six MSE fits on previously frozen factor inputs."""
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
from threadpoolctl import threadpool_limits  # noqa: E402

from scripts import advisory_lstm_factor_information as previous  # noqa: E402
from backend.services.advisory_model_first import lstm_factor_nonlinear_v1 as core  # noqa: E402
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage, read_stage  # noqa: E402
from backend.services.advisory_model_first.research_control import (  # noqa: E402
    AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import build_trial_record  # noqa: E402

OUTPUT = Path('X:/AIstock_temp/ADVISORY/lstm-factor-nonlinear-20261102')
PARENT = previous.OUTPUT
P19, P18 = previous.P19, previous.P18
PARENT_TERMINAL = '85f42c1c5174106e44fa1f9f50ebe6979cc1003a125a9450a4d246462125e79e'
FILES = tuple(dict.fromkeys((*previous.SOURCE_FILES, 'scripts/advisory_lstm_factor_nonlinear.py',
    'backend/services/advisory_model_first/lstm_factor_nonlinear_v1.py')))
load, ref, json_bytes, parquet_bytes = previous.load, previous.ref, previous.files.json_bytes, previous.files.parquet_bytes


def paths():
    result = dict(inputs=PARENT / 'study/prepared/inputs.parquet',
        parent_protocol=PARENT / 'study/preregistered/protocol.json', parent_terminal=PARENT / 'study/terminal/evaluated/receipt.json',
        input_review=PARENT / 'input_review.json', formula_review=PARENT / 'formula_review.json',
        novelty=PARENT / 'novelty_review_readback_utf8.json',
        finance=P19 / 'study/evaluated/finance.parquet', stress_finance=P18 / 'study/evaluated/target_finance.parquet')
    for fold in (1, 2, 3):
        result['train' + str(fold)] = P19 / f'study/fold{fold}/trained/train_finance.parquet'
        result['parent_model' + str(fold)] = PARENT / f'study/fold{fold}/trained/model.json'
    return result


def closure(committed=False):
    result = {n: previous.files.code_sha((ROOT / n).read_bytes()) for n in FILES}
    if committed:
        for name, digest in result.items():
            if previous.files.code_sha(subprocess.check_output(['git', 'show', f'HEAD:{name}'], cwd=ROOT)) != digest:
                raise ValueError('producer code is uncommitted: ' + name)
    return result


def versions():
    return dict(previous.versions(), sklearn=importlib.metadata.version('scikit-learn'))


def checked(plan, name):
    path = paths()[name]
    if ref(path) != plan['sources'][name]:
        raise ValueError('bound source changed: ' + name)
    return path


def verify(plan):
    if (plan['contract'] != json.loads(json_bytes(core.CONTRACT)) or plan['closure'] != closure()
            or plan['versions'] != versions() or plan['max_new_fits'] != 6):
        raise ValueError('contract/source/environment/budget drift')
    for name in paths():
        checked(plan, name)


def verify_parent():
    protocol = load(paths()['parent_protocol'])
    parent = None
    for relative in ('preregistered', 'prepared', 'fold1/trained', 'fold2/trained', 'fold3/trained', 'evaluated', 'terminal/evaluated'):
        path = PARENT / 'study' / relative
        parent = read_stage(path, stage=path.name, plan_sha256=core.info.spo.sha(protocol), parent_sha256=parent)['stage_sha256']
    if parent != PARENT_TERMINAL or load(paths()['parent_terminal'])['cumulative_actual_fits'] != 251:
        raise ValueError('previous completed experiment identity differs')
    return parent


def stages():
    return ['preregistered', 'prepared', *[f'fold{f}/{a}/trained' for f in (1, 2, 3) for a in core.PARAMS],
            'evaluated', 'terminal/evaluated']


def verify_registry(plan):
    rows = [json.loads(s) for s in (OUTPUT / 'trial_registry.jsonl').read_text(encoding='utf8').splitlines() if s.strip()]
    if (len(rows) != 1 or rows[0]['experiment_id'] != plan['experiment_id']
            or rows[0]['evaluated_trial_count'] != 1 or rows[0]['selected_trial_count'] != 0):
        raise ValueError('completed registry missing or incompatible')


def freeze():
    if subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT).strip():
        raise ValueError('freeze requires a clean committed producer')
    parent_terminal = verify_parent()
    for arm in core.PARAMS:
        core.factory(arm)  # Constructor and version verification only; zero fitting.
    plan = dict(schema=core.CONTRACT['schema'], contract=core.CONTRACT,
        sources={n: ref(p) for n, p in paths().items()}, closure=closure(True), versions=versions(),
        parent_terminal=parent_terminal, experiment_id='advlstmfactor_nonlin_20261102_v1', max_new_fits=6,
        producer_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        captured_at=datetime.now(timezone.utc).isoformat(),
        evidence='PREVIOUSLY_CONSUMED_NON_VINTAGE_RECONSTRUCTED_INPUT', production_ready=False)
    publish_stage(study_root=OUTPUT / 'study', stage='preregistered', plan_sha256=core.info.spo.sha(plan),
        parent_sha256=None, artifacts={'protocol.json': json_bytes(plan)})
    print(json.dumps(dict(status='PREREGISTERED', new_financial_fits=0, protocol_sha256=core.info.spo.sha(plan))))


def run():
    study = OUTPUT / 'study'
    plan = load(study / 'preregistered/protocol.json')
    digest = core.info.spo.sha(plan)
    verify(plan)
    terminal = study / 'terminal/evaluated/receipt.json'
    if terminal.exists():
        parent = None
        for relative in stages():
            path = study / relative
            parent = read_stage(path, stage=path.name, plan_sha256=digest, parent_sha256=parent)['stage_sha256']
        verify_registry(plan)
        print(json.dumps(dict(status='STUDY_REUSED', new_financial_fits=0, terminal_sha256=parent)))
        return
    if list((study / 'fit_claims').glob('*_started.json')):
        raise ValueError('partial STARTED: no implicit refit or extra financial budget')
    previous.qe_idle()  # No training labels or claims while another training owner is active.
    started = time.monotonic()
    claims, telemetry = core.FitClaims(study / 'fit_claims', digest), []

    def budget():
        m = psutil.Process().memory_info()
        if max(m.rss, getattr(m, 'peak_wset', m.rss)) > 2 * 1024**3 or time.monotonic() - started > 1800:
            raise ValueError('2GiB/1800s financial research resource budget exceeded')

    def publish(relative, parent, artifacts):
        budget()
        verify(plan)
        if sum(p.stat().st_size for p in study.rglob('*') if p.is_file()) + sum(map(len, artifacts.values())) > 200 * 1024**2:
            raise ValueError('200MiB output budget exceeded')
        path = study / relative
        publish_stage(study_root=path.parent, stage=path.name, plan_sha256=digest,
            parent_sha256=parent, artifacts=artifacts)
        return read_stage(path, stage=path.name, plan_sha256=digest, parent_sha256=parent)['stage_sha256']

    try:
        parent = read_stage(study / 'preregistered', stage='preregistered', plan_sha256=digest, parent_sha256=None)['stage_sha256']
        inputs = core.info.ordered(pd.read_parquet(checked(plan, 'inputs')))
        core.info.check_inputs(inputs)
        dev = inputs.loc[inputs[core.DAY].le('2025-09-30')].reset_index(drop=True)
        stress = inputs.loc[inputs[core.DAY].ge('2026-03-11')].reset_index(drop=True)
        if len(dev) != 15250 or dev[core.DAY].nunique() != 305 or len(stress) != 5900 or stress[core.DAY].nunique() != 118:
            raise ValueError('original full population differs')
        parent = publish('prepared', parent, {'inputs.parquet': parquet_bytes(inputs),
            'receipt.json': json_bytes(dict(original_rows=21150, numerical_fields=54, evaluation_labels_decoded=False))})
        for fold, (cutoff, first, last) in enumerate(core.info.spo.base.FOLDS, 1):
            finance = pd.read_parquet(checked(plan, 'train' + str(fold)))
            train = dev.loc[dev[core.DAY].le(cutoff)].merge(finance, on=core.KEY, how='left', validate='one_to_one', indicator=True)
            if not train._merge.eq('both').all():
                raise ValueError('original training keys lost')
            train = train.drop(columns='_merge')
            prior_meta = load(checked(plan, 'parent_model' + str(fold)))
            targets = dev.loc[dev[core.DAY].between(first, last)].reset_index(drop=True)
            for arm in core.PARAMS:
                def before_fit(arm=arm, fold=fold):
                    budget()
                    telemetry.append(dict(fold=fold, arm=arm, moment='BEFORE_START', qe=previous.qe_idle()))
                    claims.write(arm, fold)

                packet, oracle = core.fit(train, fold, arm, prior_meta['training_keys_sha256'], before_fit=before_fit, budget=budget)
                if (packet['train_rows'] != prior_meta['train_rows'] or packet['training_support'] != prior_meta['training_support']):
                    raise ValueError('common training support differs from the bound predecessor')
                checkpoints = study / 'fit_checkpoints'
                checkpoints.mkdir(exist_ok=True)
                with (checkpoints / f'{arm}{fold}.json').open('xb') as handle:
                    handle.write(json_bytes(packet))
                prediction = core.predict(targets, packet, phase='fold', oracle=oracle)
                artifacts = {'model.json': json_bytes(packet), 'predictions.parquet': parquet_bytes(prediction),
                    'receipt.json': json_bytes(dict(fold=fold, arm=arm, new_financial_fits=1,
                        training_keys_sha256=packet['training_keys_sha256'], training_support=packet['training_support'],
                        official_predict_parity=True, evaluation_labels_decoded=False, certificate=packet['certificate']))}
                if fold == 3:
                    artifacts['stress_predictions.parquet'] = parquet_bytes(core.predict(stress, packet, phase='stress', oracle=oracle))
                parent = publish(f'fold{fold}/{arm}/trained', parent, artifacts)
                claims.write(arm, fold, parent)
                telemetry.append(dict(fold=fold, arm=arm, moment='AFTER_FINISH', qe=previous.qe_idle()))
                print(json.dumps(dict(status='FIT_FINISHED', fold=fold, arm=arm, certificate=packet['certificate'])), flush=True)
        panels = {a: [] for a in core.PARAMS}
        for fold in (1, 2, 3):
            finance = pd.read_parquet(checked(plan, 'finance'), filters=[('fold', '==', fold)]).drop(columns='fold')
            previous.verify_fees(finance)
            for arm in core.PARAMS:
                pred = pd.read_parquet(study / f'fold{fold}/{arm}/trained/predictions.parquet')
                panel = core.info.spo.base.cohort(pred, finance)
                panel['fold'] = fold
                panels[arm].append(panel)
        panels = {a: pd.concat(p, ignore_index=True) for a, p in panels.items()}
        if any(len(p) != 126 or p.original_candidates.sum() != 6300 for p in panels.values()):
            raise ValueError('heldout evaluation axis differs')
        development = core.summarize(panels['hgb'], panels['ridge'])
        ridge_development = core.info.summarize(panels['ridge'], panels['ridge'])
        ridge_development.pop('versus_eight_fields')
        by_fold = {str(int(f)): float(g.delta.mean()) if g.delta.notna().any() else None for f, g in panels['hgb'].groupby('fold')}
        finance = pd.read_parquet(checked(plan, 'stress_finance'))
        previous.verify_fees(finance)
        pressure = {}
        for arm in core.PARAMS:
            pred = pd.read_parquet(study / f'fold3/{arm}/trained/stress_predictions.parquet')
            _legacy_report, pressure[arm] = core.info.prior.stress_report(pred, finance, sorted(stress[core.DAY].unique()))
        stress_report = core.summarize(pressure['hgb'], pressure['ridge'])
        ridge_stress = core.info.summarize(pressure['ridge'], pressure['ridge'])
        ridge_stress.pop('versus_eight_fields')
        starts = len(list(claims.root.glob('*_started.json')))
        finishes = len(list(claims.root.glob('*_finished.json')))
        if starts != 6 or finishes != 6:
            raise ValueError('financial fit claims incomplete')
        report = dict(development=development, stress=stress_report, by_fold=by_fold,
            matched_ridge=dict(development=ridge_development, stress=ridge_stress),
            decision=core.decision(development, stress_report, by_fold), actual_new_financial_fits=starts,
            cumulative_actual_fits=251 + starts, old_index_count=1, candidate_count=1,
            historical_pit=False, independent_economic_confirmation=False, production_ready=False)
        artifacts = {'report.json': json_bytes(report), 'resource.json': json_bytes(dict(qe_checks=telemetry,
            elapsed_seconds=time.monotonic() - started, rss_bytes=psutil.Process().memory_info().rss, cpu_threads=2, gpu=0))}
        for arm in core.PARAMS:
            artifacts[f'{arm}_development.parquet'] = parquet_bytes(panels[arm])
            artifacts[f'{arm}_stress.parquet'] = parquet_bytes(pressure[arm])
        parent = publish('evaluated', parent, artifacts)
        record = build_trial_record(experiment_id=plan['experiment_id'], attempt_id='exact_v1', research_stage='COMPLETED',
            study_type='EXPLORATORY_SCREEN', objective_contract='ALPHA_RANKING', decision_use='NAVIGATION_ONLY',
            hypothesis_family_id=core.CONTRACT['hypothesis'], unique_variable='RESTRICTED_NONLINEARITY_SAME54_MSE_RIDGE_CONTROL',
            parent_lineage=(load(checked(plan, 'parent_protocol'))['experiment_id'],),
            dataset_identity=plan['sources']['inputs']['sha256'], schema_identity=core.info.spo.sha(core.CONTRACT),
            policy_identity=core.info.spo.base.VALUATION_POLICY_SHA256, planned_trial_count=1,
            generated_trial_count=1, evaluated_trial_count=1, selected_trial_count=0, result_class='EXPLORATORY',
            consumed_windows=tuple(dict(window_id=label, dataset_identity=plan['sources']['inputs']['sha256'],
                start_date=first, end_date=last) for label, first, last in (
                    ('ALREADY_CONSUMED_DEVELOPMENT', '2024-07-04', '2025-09-30'),
                    ('ALREADY_CONSUMED_STRESS', '2026-03-11', '2026-08-28'))),
            evidence_refs=(evidence_reference_for_file(study / 'evaluated/manifest.json', role='nonlinear_evaluation'),))
        AdvisoryResearchTrialRegistryV1(OUTPUT / 'trial_registry.jsonl').append_batch((record,))
        verify_registry(plan)
        terminal_sha = publish('terminal/evaluated', parent, {'receipt.json': json_bytes(dict(status='COMPLETED',
            **report, evaluated_sha256=parent, registry_complete=True))})
        print(json.dumps(dict(status='COMPLETED', decision=report['decision'], actual_new_financial_fits=starts,
            development_bps=development['statistics']['mean_bps'], stress_bps=stress_report['statistics']['mean_bps'],
            terminal_sha256=terminal_sha)))
    except BaseException as exc:
        with (OUTPUT / 'execution_failure.json').open('x', encoding='utf8') as handle:
            json.dump(dict(status='FAILED_NO_AUTOMATIC_REFIT', error_type=type(exc).__name__, error=str(exc),
                actual_started_claims=len(list(claims.root.glob('*_started.json'))),
                actual_finished_claims=len(list(claims.root.glob('*_finished.json')))), handle, indent=2)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['freeze', 'run'])
    args = parser.parse_args()
    with threadpool_limits(limits=2), _exclusive_file_lock(OUTPUT / 'runner.lock'):
        freeze() if args.action == 'freeze' else run()


if __name__ == '__main__':
    main()
