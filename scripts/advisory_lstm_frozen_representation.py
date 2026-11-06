"""Three bounded readout fits; frozen parent states and original Ridge controls."""
import argparse
from datetime import datetime, timezone
import json
import math
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

from scripts import advisory_lstm_factor_nonlinear as previous  # noqa: E402
from backend.services.advisory_model_first import lstm_frozen_representation_v1 as core  # noqa: E402
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage, read_stage  # noqa: E402
from backend.services.advisory_model_first.research_control import (  # noqa: E402
    AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import build_trial_record  # noqa: E402

OUTPUT = Path('X:/AIstock_temp/ADVISORY/lstm-frozen-representation-20261106')
FILES = (*previous.FILES, 'scripts/advisory_lstm_frozen_representation.py',
         'backend/services/advisory_model_first/lstm_frozen_representation_v1.py')
load, ref, json_bytes, parquet_bytes = previous.load, previous.ref, previous.json_bytes, previous.parquet_bytes
RESOURCE_POLICY = dict(previous.RESOURCE_POLICY)


def paths():
    old = previous.paths()
    result = {n: old[n] for n in ('finance', 'stress_finance', 'train1', 'train2', 'train3')}
    result.update(inputs=OUTPUT / 'representation_inputs.parquet',
        input_review=OUTPUT / 'representation_input_review.json', independent_input=OUTPUT / 'p1_independent_receipt.json',
        clock=OUTPUT / 'asset_clock_final.json', p1_protocol=OUTPUT / 'p1_protocol.json',
        source_semantics=OUTPUT / 'p1_source_semantics_review.json',
        original_inputs=old['inputs'], control_protocol=previous.OUTPUT / 'study/preregistered/protocol.json',
        control_terminal=previous.OUTPUT / 'study/terminal/evaluated/receipt.json')
    for fold in (1, 2, 3):
        result['control' + str(fold)] = previous.OUTPUT / f'study/fold{fold}/ridge/trained/model.json'
    return result


def closure(committed=False):
    result = {name: previous.previous.files.code_sha((ROOT / name).read_bytes()) for name in FILES}
    if committed:
        for name, digest in result.items():
            if previous.previous.files.code_sha(subprocess.check_output(['git', 'show', f'HEAD:{name}'], cwd=ROOT)) != digest:
                raise ValueError('uncommitted producer: ' + name)
    return result


def verify_control_chain():
    plan = load(paths()['control_protocol'])
    parent = None
    for relative in previous.stages():
        path = previous.OUTPUT / 'study' / relative
        parent = read_stage(path, stage=path.name, plan_sha256=core.info.spo.sha(plan), parent_sha256=parent)['stage_sha256']
    previous.verify_registry(plan)
    if load(paths()['control_terminal'])['cumulative_actual_fits'] != 257:
        raise ValueError('original control experiment differs')
    for fold in (1, 2, 3):
        packet = load(paths()['control' + str(fold)])
        core.old.validate_model(packet)
        if packet['arm'] != 'ridge' or packet['fold'] != fold or len(packet['fields']) != 54:
            raise ValueError('original exact control differs')
    return parent


def checked(plan, name):
    path = paths()[name]
    if ref(path) != plan['sources'][name]:
        raise ValueError('bound source changed: ' + name)
    return path


def verify(plan):
    if (plan['contract'] != json.loads(json_bytes(core.CONTRACT)) or plan['closure'] != closure()
            or plan['versions'] != previous.versions() or plan['max_new_fits'] != 3
            or plan['resource_policy'] != RESOURCE_POLICY):
        raise ValueError('contract/code/environment/budget drift')
    for name in paths():
        checked(plan, name)
    if plan['control_terminal_sha256'] != verify_control_chain():
        raise ValueError('original control chain drift')


def resource_admission(*, require_capacity=True):
    process = psutil.Process()
    capacity = len(process.cpu_affinity()) if hasattr(process, 'cpu_affinity') else psutil.cpu_count(logical=True)
    usage = psutil.cpu_percent(interval=.5)
    snapshot = dict(cpu_capacity=capacity, cpu_percent=usage, free_cpu_threads=capacity * (1 - usage / 100),
        available_memory_bytes=psutil.virtual_memory().available, free_output_bytes=psutil.disk_usage(str(OUTPUT)).free)
    enough = all(math.isfinite(snapshot[k]) and snapshot[k] >= v for k, v in (
        ('free_cpu_threads', 2), ('available_memory_bytes', 2 * 1024**3), ('free_output_bytes', 200 * 1024**2)))
    if require_capacity and not enough:
        raise ValueError('insufficient actual local capacity before financial start')
    return dict(policy=RESOURCE_POLICY, local=snapshot, capacity_sufficient=enough,
        qe_status='NOT_QUERIED_NOT_A_VETO', captured_at=datetime.now(timezone.utc).isoformat())


def stages():
    return ['preregistered', 'prepared', 'fold1/trained', 'fold2/trained', 'fold3/trained', 'evaluated', 'terminal/evaluated']


def verify_registry(plan):
    rows = [json.loads(s) for s in (OUTPUT / 'trial_registry.jsonl').read_text(encoding='utf8').splitlines() if s.strip()]
    if (len(rows) != 1 or rows[0]['experiment_id'] != plan['experiment_id']
            or rows[0]['evaluated_trial_count'] != 1 or rows[0]['selected_trial_count'] != 0):
        raise ValueError('completed registry missing or incompatible')


def verify_claims(digest):
    root = OUTPUT / 'study/fit_claims'
    expected = {f'candidate{f}_{s}.json' for f in (1, 2, 3) for s in ('started', 'finished')}
    if {p.name for p in root.iterdir()} != expected:
        raise ValueError('exactly three completed claims required')
    for fold in (1, 2, 3):
        for state in ('started', 'finished'):
            packet = load(root / f'candidate{fold}_{state}.json')
            if packet['plan_sha256'] != digest or packet['fold'] != fold or packet['status'] != state.upper():
                raise ValueError('claim identity differs')
            if state == 'finished' and packet['stage_sha256'] != load(OUTPUT / f'study/fold{fold}/trained/manifest.json')['stage_sha256']:
                raise ValueError('finished claim stage differs')


def freeze():
    if subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT).strip():
        raise ValueError('freeze requires clean committed producer')
    control_terminal = verify_control_chain()
    sources = {name: ref(path) for name, path in paths().items()}
    review, audit = load(paths()['input_review']), load(paths()['independent_input'])
    if (sources['inputs']['sha256'] != core.CONTRACT['representation_sha256'] or audit['status'] != 'PASS'
            or review['output']['sha256'] != sources['inputs']['sha256']
            or not review['original_target_factor_values_exactly_equal'] or audit['rows'] != 21150):
        raise ValueError('frozen input acceptance differs')
    plan = dict(schema=core.CONTRACT['schema'], contract=core.CONTRACT, sources=sources,
        closure=closure(True), versions=previous.versions(), resource_policy=RESOURCE_POLICY,
        control_terminal_sha256=control_terminal, experiment_id='advlstm_frozen_state_20261106_v1', max_new_fits=3,
        producer_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        captured_at=datetime.now(timezone.utc).isoformat(), production_ready=False)
    publish_stage(study_root=OUTPUT / 'study', stage='preregistered', plan_sha256=core.info.spo.sha(plan),
        parent_sha256=None, artifacts={'protocol.json': json_bytes(plan)})
    print(json.dumps(dict(status='PREREGISTERED', new_financial_fits=0, protocol_sha256=core.info.spo.sha(plan))))


def run():
    study = OUTPUT / 'study'
    plan = load(study / 'preregistered/protocol.json')
    digest = core.info.spo.sha(plan)
    verify(plan)
    if (study / 'terminal/evaluated/receipt.json').exists():
        parent = None
        for relative in stages():
            path = study / relative
            parent = read_stage(path, stage=path.name, plan_sha256=digest, parent_sha256=parent)['stage_sha256']
        verify_registry(plan)
        verify_claims(digest)
        print(json.dumps(dict(status='STUDY_REUSED', new_financial_fits=0, terminal_sha256=parent)))
        return
    if list((study / 'fit_claims').glob('*_started.json')):
        raise ValueError('partial STARTED: no implicit refit or extra budget')
    telemetry = [dict(moment='BEFORE_LABELS', **resource_admission())]
    started, claims = time.monotonic(), core.FitClaims(study / 'fit_claims', digest)

    def budget():
        memory = psutil.Process().memory_info()
        if max(memory.rss, getattr(memory, 'peak_wset', memory.rss)) > 2 * 1024**3 or time.monotonic() - started > 1800:
            raise ValueError('2GiB/1800s research budget exceeded')

    def publish(relative, parent, artifacts):
        budget()
        verify(plan)
        if sum(p.stat().st_size for p in study.rglob('*') if p.is_file()) + sum(map(len, artifacts.values())) > 200 * 1024**2:
            raise ValueError('200MiB study output budget exceeded')
        path = study / relative
        publish_stage(study_root=path.parent, stage=path.name, plan_sha256=digest, parent_sha256=parent, artifacts=artifacts)
        return read_stage(path, stage=path.name, plan_sha256=digest, parent_sha256=parent)['stage_sha256']

    try:
        parent = read_stage(study / 'preregistered', stage='preregistered', plan_sha256=digest, parent_sha256=None)['stage_sha256']
        inputs = core.info.ordered(pd.read_parquet(checked(plan, 'inputs')))
        core.check_inputs(inputs)
        original = core.info.ordered(pd.read_parquet(checked(plan, 'original_inputs')))
        pd.testing.assert_frame_equal(inputs.loc[:, original.columns], original)
        dev = inputs.loc[inputs[core.DAY].le('2025-09-30')].reset_index(drop=True)
        stress = inputs.loc[inputs[core.DAY].ge('2026-03-11')].reset_index(drop=True)
        if (len(dev), dev[core.DAY].nunique(), len(stress), stress[core.DAY].nunique()) != (15250, 305, 5900, 118):
            raise ValueError('original population differs')
        parent = publish('prepared', parent, {'inputs.parquet': parquet_bytes(inputs),
            'receipt.json': json_bytes(dict(original_rows=21150, hidden_dimensions=64, evaluation_labels_decoded=False))})
        for fold, (cutoff, first, last) in enumerate(core.info.spo.base.FOLDS, 1):
            finance = pd.read_parquet(checked(plan, 'train' + str(fold)))
            previous.previous.verify_fees(finance)
            # At most 13,100 original training rows; exact KEY one-to-one join prevents expansion.
            train = dev.loc[dev[core.DAY].le(cutoff)].merge(finance, on=core.KEY, how='left', validate='one_to_one', indicator=True)
            if not train._merge.eq('both').all():
                raise ValueError('original training keys lost')
            train = train.drop(columns='_merge')
            control = load(checked(plan, 'control' + str(fold)))

            def before_fit(fold=fold):
                budget()
                verify(plan)
                telemetry.append(dict(fold=fold, moment='BEFORE_START', **resource_admission()))
                claims.write(fold)

            packet, _ = core.fit(train, fold, control, before_fit=before_fit, budget=budget)
            checkpoints = study / 'fit_checkpoints'
            checkpoints.mkdir(exist_ok=True)
            with (checkpoints / f'candidate{fold}.json').open('xb') as stream:
                stream.write(json_bytes(packet))
            targets = dev.loc[dev[core.DAY].between(first, last)].reset_index(drop=True)
            artifacts = {'model.json': json_bytes(packet), 'receipt.json': json_bytes(dict(fold=fold,
                new_financial_fits=1, control_fits=0, evaluation_labels_decoded=False, certificate=packet['certificate']))}
            for arm, model in (('candidate', packet), ('control', control)):
                artifacts[f'{arm}_predictions.parquet'] = parquet_bytes(core.predict(targets, model, phase='fold', control=arm == 'control'))
                if fold == 3:
                    artifacts[f'{arm}_stress_predictions.parquet'] = parquet_bytes(core.predict(stress, model, phase='stress', control=arm == 'control'))
            parent = publish(f'fold{fold}/trained', parent, artifacts)
            claims.write(fold, parent)
            print(json.dumps(dict(status='FIT_FINISHED', fold=fold, train_rows=packet['train_rows'], certificate=packet['certificate'])), flush=True)
        verify_claims(digest)
        # Every model and both arms' predictions are immutable before evaluation label decoding.
        panels = {a: [] for a in ('candidate', 'control')}
        for fold in (1, 2, 3):
            finance = pd.read_parquet(checked(plan, 'finance'), filters=[('fold', '==', fold)]).drop(columns='fold')
            previous.previous.verify_fees(finance)
            for arm in panels:
                pred = pd.read_parquet(study / f'fold{fold}/trained/{arm}_predictions.parquet')
                panel = core.info.spo.base.cohort(pred, finance)
                panel['fold'] = fold
                panels[arm].append(panel)
        panels = {a: pd.concat(p, ignore_index=True) for a, p in panels.items()}
        if any(len(p) != 126 or p.original_candidates.sum() != 6300 for p in panels.values()):
            raise ValueError('evaluation axis differs')
        finance = pd.read_parquet(checked(plan, 'stress_finance'))
        previous.previous.verify_fees(finance)
        pressure = {}
        for arm in panels:
            pred = pd.read_parquet(study / f'fold3/trained/{arm}_stress_predictions.parquet')
            _, pressure[arm] = core.info.prior.stress_report(pred, finance, sorted(stress[core.DAY].unique()))
        development = core.summarize(panels['candidate'], panels['control'])
        stress_report = core.summarize(pressure['candidate'], pressure['control'])
        by_fold = {str(int(f)): float(g.delta.mean()) if g.delta.notna().any() else None for f, g in panels['candidate'].groupby('fold')}
        report = dict(development=development, stress=stress_report, by_fold=by_fold,
            frozen_ridge={n: core.summarize(p['control'], p['control']) for n, p in (('development', panels), ('stress', pressure))},
            decision=core.decision(development, stress_report, by_fold), actual_new_financial_fits=3,
            cumulative_actual_fits=266, old_index_count=1, candidate_count=1, control_fits=0, parent_fits=0,
            historical_pit=False, independent_economic_confirmation=False, production_ready=False,
            confirmation_source='CONFIRMATION_SOURCE_NOT_READY')
        artifacts = {'report.json': json_bytes(report), 'resource.json': json_bytes(dict(observations=telemetry,
            elapsed_seconds=time.monotonic() - started, rss_bytes=psutil.Process().memory_info().rss,
            peak_rss_bytes=getattr(psutil.Process().memory_info(), 'peak_wset', psutil.Process().memory_info().rss), cpu_threads=2, gpu=0))}
        for arm in panels:
            artifacts[f'{arm}_development.parquet'] = parquet_bytes(panels[arm])
            artifacts[f'{arm}_stress.parquet'] = parquet_bytes(pressure[arm])
        parent = publish('evaluated', parent, artifacts)
        record = build_trial_record(experiment_id=plan['experiment_id'], attempt_id='exact_v1', research_stage='COMPLETED',
            study_type='EXPLORATORY_SCREEN', objective_contract='ALPHA_RANKING', decision_use='NAVIGATION_ONLY',
            hypothesis_family_id=core.CONTRACT['hypothesis'], unique_variable='FROZEN_PARENT_H64_READOUT_FIXED5',
            parent_lineage=(load(checked(plan, 'control_protocol'))['experiment_id'],),
            dataset_identity=plan['sources']['inputs']['sha256'], schema_identity=core.info.spo.sha(core.CONTRACT),
            policy_identity=core.info.spo.base.VALUATION_POLICY_SHA256, planned_trial_count=1,
            generated_trial_count=1, evaluated_trial_count=1, selected_trial_count=0, result_class='EXPLORATORY',
            consumed_windows=tuple(dict(window_id=label, dataset_identity=plan['sources']['inputs']['sha256'], start_date=first, end_date=last)
                for label, first, last in (('ALREADY_CONSUMED_DEVELOPMENT', '2024-07-04', '2025-09-30'),
                                          ('ALREADY_CONSUMED_STRESS', '2026-03-11', '2026-08-28'))),
            evidence_refs=(evidence_reference_for_file(study / 'evaluated/manifest.json', role='representation_evaluation'),))
        AdvisoryResearchTrialRegistryV1(OUTPUT / 'trial_registry.jsonl').append_batch((record,))
        verify_registry(plan)
        terminal = publish('terminal/evaluated', parent, {'receipt.json': json_bytes(dict(status='COMPLETED', **report,
            evaluated_sha256=parent, registry_complete=True))})
        print(json.dumps(dict(status='COMPLETED', decision=report['decision'], new_financial_fits=3,
            development_bps=development['statistics']['mean_bps'], stress_bps=stress_report['statistics']['mean_bps'], terminal_sha256=terminal)))
    except BaseException as exc:
        with (OUTPUT / 'execution_failure.json').open('x', encoding='utf8') as stream:
            json.dump(dict(status='FAILED_NO_AUTOMATIC_REFIT', error_type=type(exc).__name__, error=str(exc),
                actual_started_claims=len(list(claims.root.glob('*_started.json'))),
                actual_finished_claims=len(list(claims.root.glob('*_finished.json')))), stream, indent=2)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['freeze', 'run'])
    args = parser.parse_args()
    with threadpool_limits(limits=2), _exclusive_file_lock(OUTPUT / 'runner.lock'):
        freeze() if args.action == 'freeze' else run()


if __name__ == '__main__':
    main()
