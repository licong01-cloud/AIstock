"""Read-only preflight and explicitly authorized, three-fit overnight experiment."""
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

from scripts import advisory_lstm_factor_nonlinear as legacy  # noqa: E402
from backend.services.advisory_model_first import lstm_overnight_increment_v1 as core  # noqa: E402
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage, read_stage  # noqa: E402
from backend.services.advisory_model_first.research_control import (  # noqa: E402
    AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import build_trial_record  # noqa: E402

OUTPUT = Path('X:/AIstock_temp/ADVISORY/lstm-overnight-increment-20261108-v1')
INTAKE = Path('X:/AIstock_temp/ADVISORY/single-signal-intake-20261108-v1')
CONTROL_TERMINAL = 'c6fb9df908e7250e7acf05d2023fea628682cf16028376139098f6bd73392ddb'
INPUT_SHA = '6685741751b73a554923826190809bba50c296d2e49e25fb0a5af551ae036f67'
SIGNAL_SHA = '1662ab1cabc092907dfcc60d52d31e5070ab4acf444210c94d1f4315b2eda9ec'
FILES = tuple(dict.fromkeys((*legacy.FILES, 'scripts/advisory_lstm_overnight_increment.py',
    'backend/services/advisory_model_first/lstm_overnight_increment_v1.py',
    'backend/services/strategy_package/runtime_variant.py')))
load, ref, json_bytes, parquet_bytes = legacy.load, legacy.ref, legacy.json_bytes, legacy.parquet_bytes
versions = legacy.versions


def paths():
    result = dict(legacy.paths(), signal=INTAKE / 'signal_projection.parquet',
        intake_delivery=INTAKE / 'delivery_receipt.json', intake_input=INTAKE / 'input_review.json',
        intake_audit=INTAKE / 'independent_audit.json',
        control_protocol=legacy.OUTPUT / 'study/preregistered/protocol.json')
    for fold in (1, 2, 3):
        result['control' + str(fold)] = legacy.OUTPUT / f'study/fold{fold}/ridge/trained/model.json'
    return result


def closure(committed=False):
    result = {name: legacy.previous.files.code_sha((ROOT / name).read_bytes()) for name in FILES}
    if committed:
        for name, value in result.items():
            if legacy.previous.files.code_sha(subprocess.check_output(['git', 'show', f'HEAD:{name}'], cwd=ROOT)) != value:
                raise ValueError('uncommitted producer: ' + name)
    return result


def checked(plan, name):
    path = paths()[name]
    if ref(path) != plan['sources'][name]:
        raise ValueError('bound source drift: ' + name)
    return path


def authorization(reference, max_new_fits):
    if not isinstance(reference, str) or not reference.strip() or type(max_new_fits) is not int or max_new_fits != 3:
        raise ValueError('explicit experiment authorization reference and exact three-fit budget required')
    return dict(reference=reference.strip(), max_new_fits=3, contract_sha256=core.info.spo.sha(core.CONTRACT))


def verify_control():
    plan = load(paths()['control_protocol'])
    legacy.verify(plan)  # Requires the original executable/library versions; never silently substitutes them.
    legacy.verify_parent()
    parent = None
    for relative in legacy.stages():
        path = legacy.OUTPUT / 'study' / relative
        parent = read_stage(path, stage=path.name, plan_sha256=core.info.spo.sha(plan), parent_sha256=parent)['stage_sha256']
    if parent != CONTROL_TERMINAL:
        raise ValueError('original control terminal differs')
    for fold in (1, 2, 3):
        packet = load(paths()['control' + str(fold)])
        core.control.validate_model(packet)
        if packet['arm'] != 'ridge' or packet['fold'] != fold:
            raise ValueError('original control learner/fold differs')
    return parent


def preflight():
    """Validate hashes, old stages and input projection; never decode financial arrays."""
    before = {name: ref(path) for name, path in paths().items()}
    if before['inputs']['sha256'] != INPUT_SHA or before['signal']['sha256'] != SIGNAL_SHA:
        raise ValueError('frozen original input/signal identity differs')
    control_terminal = verify_control()
    delivery = load(paths()['intake_delivery'])
    for name, digest in delivery['evidence_hashes'].items():
        if Path(name).name != name or ref(INTAKE / name)['sha256'] != digest:
            raise ValueError('intake protected artifact differs')
    inputs = pd.read_parquet(paths()['inputs'], columns=list(core.INPUT_COLUMNS))
    signal = pd.read_parquet(paths()['signal'], columns=list(core.SIGNAL_COLUMNS))
    assembled = core.assemble(inputs, signal)
    dev = assembled.loc[assembled[core.DAY].le('2025-09-30')]
    stress = assembled.loc[assembled[core.DAY].ge('2026-03-11')]
    if (len(assembled) != 21150 or assembled[core.DAY].nunique() != 423 or len(dev) != 15250
            or dev[core.DAY].nunique() != 305 or len(stress) != 5900 or stress[core.DAY].nunique() != 118
            or int(assembled[core.NEW].notna().sum()) != 21124 or int(stress[core.NEW].notna().sum()) != 5900):
        raise ValueError('original population/signal support differs')
    if {name: ref(path) for name, path in paths().items()} != before:
        raise ValueError('source changed during preflight')
    result = dict(status='PASS_INPUT_AND_CONTROL_CHAIN', sources=before, control_terminal=control_terminal,
        original_rows=21150, original_days=423, signal_finite=21124, signal_missing=26, stress_finite=5900,
        complete_training_target_parity='NOT_EVALUATED_WITHOUT_LABEL_AUTHORIZATION',
        financial_label_arrays_decoded=False, new_financial_fits=0, new_rankings=0,
        economic_acceptance='NOT_EVALUATED', historical_pit=False, production_ready=False)
    return assembled, result


def verify(plan, auth):
    if (plan['contract'] != json.loads(json_bytes(core.CONTRACT)) or plan['closure'] != closure()
            or plan['versions'] != versions() or plan['execution_authorization'] != auth
            or plan['max_new_fits'] != 3 or set(plan['sources']) != set(paths())):
        raise ValueError('protocol/code/environment/authorization drift')
    for name in paths():
        checked(plan, name)
    verify_control()


def stages():
    return ['preregistered', 'prepared', 'fold1/trained', 'fold2/trained', 'fold3/trained', 'evaluated', 'terminal/evaluated']


def verify_registry(plan):
    records = [json.loads(line) for line in (OUTPUT / 'trial_registry.jsonl').read_text(encoding='utf-8').splitlines() if line.strip()]
    if (len(records) != 1 or records[0]['experiment_id'] != plan['experiment_id']
            or records[0]['evaluated_trial_count'] != 1 or records[0]['selected_trial_count'] != 0):
        raise ValueError('complete registry missing or incompatible')


def completed(plan):
    parent = None
    for relative in stages():
        path = OUTPUT / 'study' / relative
        parent = read_stage(path, stage=path.name, plan_sha256=core.info.spo.sha(plan), parent_sha256=parent)['stage_sha256']
    verify_registry(plan)
    claims = OUTPUT / 'study/fit_claims'
    if ({p.name for p in claims.iterdir()} != {f'fold{f}_{s}.json' for f in (1, 2, 3) for s in ('started', 'finished')}):
        raise ValueError('complete three-fit claims missing')
    for fold in (1, 2, 3):
        trained = OUTPUT / f'study/fold{fold}/trained'
        checkpoint = OUTPUT / f'study/fit_checkpoints/fold{fold}.json'
        if ref(checkpoint) != load(trained / 'receipt.json')['model_checkpoint']:
            raise ValueError('completed model checkpoint differs')
        for state in ('started', 'finished'):
            claim = load(claims / f'fold{fold}_{state}.json')
            if claim['plan_sha256'] != core.info.spo.sha(plan) or claim['fold'] != fold or claim['status'] != state.upper():
                raise ValueError('foreign completed fit claim')
            if state == 'finished' and claim['stage_sha256'] != load(OUTPUT / f'study/fold{fold}/trained/manifest.json')['stage_sha256']:
                raise ValueError('finished claim does not bind its stage')
    return dict(status='STUDY_REUSED', new_financial_fits=0, terminal_sha256=parent)


def freeze(reference, max_new_fits):
    auth = authorization(reference, max_new_fits)
    if subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT).strip():
        raise ValueError('freeze requires a clean committed producer')
    if (OUTPUT / 'study/preregistered').exists():
        plan = load(OUTPUT / 'study/preregistered/protocol.json')
        verify(plan, auth)
        read_stage(OUTPUT / 'study/preregistered', stage='preregistered', plan_sha256=core.info.spo.sha(plan), parent_sha256=None)
        return dict(status='PREREGISTRATION_REUSED', new_financial_fits=0)
    _, audit = preflight()
    plan = dict(schema=core.CONTRACT['schema'], contract=core.CONTRACT, sources=audit['sources'],
        control_terminal=audit['control_terminal'], closure=closure(True), versions=versions(),
        execution_authorization=auth, max_new_fits=3, experiment_id='advlstmovernightincrement_20261108_v1',
        producer_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        captured_at=datetime.now(timezone.utc).isoformat(), evidence='NON_VINTAGE_PREVIOUSLY_CONSUMED_EXPLORATION',
        production_ready=False)
    publish_stage(study_root=OUTPUT / 'study', stage='preregistered', plan_sha256=core.info.spo.sha(plan),
                  parent_sha256=None, artifacts={'protocol.json': json_bytes(plan)})
    return dict(status='PREREGISTERED', new_financial_fits=0, protocol_sha256=core.info.spo.sha(plan))


def local_capacity():
    proc = psutil.Process()
    count = len(proc.cpu_affinity()) if hasattr(proc, 'cpu_affinity') else psutil.cpu_count(logical=True)
    return dict(free_cpu_threads=count * (1 - psutil.cpu_percent(interval=.5) / 100),
        available_memory_bytes=psutil.virtual_memory().available, free_output_bytes=psutil.disk_usage(str(OUTPUT)).free)


def resource_admission(require_capacity=True):
    local = local_capacity()
    enough = all(math.isfinite(local[k]) and local[k] >= minimum for k, minimum in (
        ('free_cpu_threads', 2), ('available_memory_bytes', 2 * 1024**3), ('free_output_bytes', 200 * 1024**2)))
    if require_capacity and not enough:
        raise ValueError('insufficient local CPU/memory/output capacity')
    return dict(local=local, capacity_sufficient=enough, qe=legacy.qe_observation(), qe_is_observation_only=True)


def run(reference, max_new_fits):
    auth = authorization(reference, max_new_fits)  # Before any study write, financial read or resource/API query.
    study = OUTPUT / 'study'
    plan = load(study / 'preregistered/protocol.json')
    verify(plan, auth)
    digest = core.info.spo.sha(plan)
    if (study / 'terminal/evaluated').exists():
        return completed(plan)
    if list((study / 'fit_claims').glob('*_started.json')):
        raise ValueError('partial STARTED: no implicit refit or extra financial budget')
    initial = resource_admission()
    started = time.monotonic()
    claims, telemetry = core.FitClaims(study / 'fit_claims', digest), [dict(moment='BEFORE_LABELS', **initial)]

    def budget():
        memory = psutil.Process().memory_info()
        if max(memory.rss, getattr(memory, 'peak_wset', memory.rss)) > 2 * 1024**3 or time.monotonic() - started > 1800:
            raise ValueError('2GiB/1800s research budget exceeded')

    def publish(relative, parent, artifacts):
        budget()
        verify(plan, auth)
        size = sum(p.stat().st_size for p in OUTPUT.rglob('*') if p.is_file()) + sum(map(len, artifacts.values()))
        if size > 200 * 1024**2:
            raise ValueError('200MiB output budget exceeded')
        path = study / relative
        publish_stage(study_root=path.parent, stage=path.name, plan_sha256=digest, parent_sha256=parent, artifacts=artifacts)
        return read_stage(path, stage=path.name, plan_sha256=digest, parent_sha256=parent)['stage_sha256']

    try:
        inputs, _ = preflight()
        dev = inputs.loc[inputs[core.DAY].le('2025-09-30')].reset_index(drop=True)
        stress = inputs.loc[inputs[core.DAY].ge('2026-03-11')].reset_index(drop=True)
        parent = read_stage(study / 'preregistered', stage='preregistered', plan_sha256=digest, parent_sha256=None)['stage_sha256']
        if (study / 'prepared').exists():
            parent = read_stage(study / 'prepared', stage='prepared', plan_sha256=digest, parent_sha256=parent)['stage_sha256']
        else:
            parent = publish('prepared', parent, {'inputs.parquet': parquet_bytes(inputs),
                'receipt.json': json_bytes(dict(original_rows=21150, candidate_fields=55, new_financial_fits=0,
                    financial_labels_decoded=False, execution_authorization=auth))})
        for fold, (cutoff, first, last) in enumerate(core.info.spo.base.FOLDS, 1):
            finance = pd.read_parquet(checked(plan, 'train' + str(fold)))
            train = dev.loc[dev[core.DAY].le(cutoff)].merge(finance, on=core.KEY, how='left', validate='one_to_one', indicator=True)
            if not train._merge.eq('both').all():
                raise ValueError('original training KEY lost')
            train = train.drop(columns='_merge')
            old = load(checked(plan, 'control' + str(fold)))

            def before_fit(fold=fold):
                budget()
                telemetry.append(dict(fold=fold, moment='BEFORE_START', **resource_admission()))
                verify(plan, auth)
                claims.write(fold)

            packet, estimator = core.fit(train, fold, old, before_fit=before_fit, budget=budget)
            packet['source_bindings'] = {name: plan['sources'][name] for name in ('inputs', 'signal', 'train' + str(fold), 'control' + str(fold))}
            core.sign(packet)
            checkpoints = study / 'fit_checkpoints'
            checkpoints.mkdir(exist_ok=True)
            checkpoint = checkpoints / f'fold{fold}.json'
            with checkpoint.open('xb') as handle:
                handle.write(json_bytes(packet))  # Preserve completed weights even if prediction/publication fails.
            target = dev.loc[dev[core.DAY].between(first, last)].reset_index(drop=True)
            candidate = core.predict(target, packet, phase='fold', oracle=estimator)
            baseline = core.control.predict(target, old, phase='fold')
            artifacts = {'model.json': json_bytes(packet), 'predictions.parquet': parquet_bytes(candidate),
                'control_predictions.parquet': parquet_bytes(baseline),
                'receipt.json': json_bytes(dict(fold=fold, new_financial_fits=1, evaluation_labels_decoded=False,
                    certificate=packet['certificate'], training_keys_sha256=packet['training_keys_sha256'],
                    model_checkpoint=ref(checkpoint)))}
            if fold == 3:
                artifacts['stress_predictions.parquet'] = parquet_bytes(core.predict(stress, packet, phase='stress', oracle=estimator))
                artifacts['control_stress_predictions.parquet'] = parquet_bytes(core.control.predict(stress, old, phase='stress'))
            parent = publish(f'fold{fold}/trained', parent, artifacts)
            claims.write(fold, parent)
            telemetry.append(dict(fold=fold, moment='AFTER_FINISH', **resource_admission(False)))
        # Every fold/stress prediction is immutable before the first evaluation label is decoded.
        panels = {arm: [] for arm in ('candidate', 'control')}
        for fold in (1, 2, 3):
            finance = pd.read_parquet(checked(plan, 'finance'), filters=[('fold', '==', fold)]).drop(columns='fold')
            legacy.previous.verify_fees(finance)
            for arm, filename in (('candidate', 'predictions'), ('control', 'control_predictions')):
                pred = pd.read_parquet(study / f'fold{fold}/trained/{filename}.parquet')
                panel = core.info.spo.base.cohort(pred, finance)
                panel['fold'] = fold
                panels[arm].append(panel)
        panels = {arm: pd.concat(rows, ignore_index=True) for arm, rows in panels.items()}
        if any(len(panel) != 126 or panel.original_candidates.sum() != 6300 for panel in panels.values()):
            raise ValueError('original full evaluation axis differs')
        development = core.summarize(panels['candidate'], panels['control'])
        fold_means = {str(int(f)): float(g.delta.mean()) if g.delta.notna().any() else None for f, g in panels['candidate'].groupby('fold')}
        interventions = {str(int(f)): int((g.mature & g.intervention_count.gt(0)).sum()) for f, g in panels['candidate'].groupby('fold')}
        finance = pd.read_parquet(checked(plan, 'stress_finance'))
        legacy.previous.verify_fees(finance)
        pressure = {}
        for arm, filename in (('candidate', 'stress_predictions'), ('control', 'control_stress_predictions')):
            pred = pd.read_parquet(study / f'fold3/trained/{filename}.parquet')
            _, pressure[arm] = core.info.prior.stress_report(pred, finance, sorted(stress[core.DAY].unique()))
        stress_report = core.summarize(pressure['candidate'], pressure['control'])
        starts = len(list(claims.root.glob('*_started.json')))
        finishes = len(list(claims.root.glob('*_finished.json')))
        if starts != 3 or finishes != 3:
            raise ValueError('three-fold financial claims incomplete')
        report = dict(development=development, stress=stress_report, by_fold=fold_means,
            fold_interventions=interventions, decision=core.decision(development, stress_report, fold_means, interventions),
            actual_new_financial_fits=starts, cumulative_actual_fits=266 + starts, old_index_count=1,
            candidate_count=1, historical_pit=False, independent_economic_confirmation=False, production_ready=False)
        artifacts = {'report.json': json_bytes(report), 'resources.json': json_bytes(dict(snapshots=telemetry,
            elapsed_seconds=time.monotonic() - started, rss_bytes=psutil.Process().memory_info().rss, cpu_threads=2, gpu=0))}
        for arm in panels:
            artifacts[arm + '_development.parquet'] = parquet_bytes(panels[arm])
            artifacts[arm + '_stress.parquet'] = parquet_bytes(pressure[arm])
        parent = publish('evaluated', parent, artifacts)
        record = build_trial_record(experiment_id=plan['experiment_id'], attempt_id='exact_v1', research_stage='COMPLETED',
            study_type='EXPLORATORY_SCREEN', objective_contract='ALPHA_RANKING', decision_use='NAVIGATION_ONLY',
            hypothesis_family_id=core.CONTRACT['hypothesis'], unique_variable='ONE_NATIVE_OVERNIGHT_COORDINATE_SAME_RIDGE',
            parent_lineage=(load(checked(plan, 'control_protocol'))['experiment_id'],),
            dataset_identity=plan['sources']['inputs']['sha256'], schema_identity=core.info.spo.sha(core.CONTRACT),
            policy_identity=core.info.spo.base.VALUATION_POLICY_SHA256, planned_trial_count=1, generated_trial_count=1,
            evaluated_trial_count=1, selected_trial_count=0, result_class='EXPLORATORY',
            consumed_windows=tuple(dict(window_id=label, dataset_identity=plan['sources']['inputs']['sha256'], start_date=first, end_date=last)
                for label, first, last in (('ALREADY_CONSUMED_DEVELOPMENT', '2024-07-04', '2025-09-30'),
                    ('ALREADY_CONSUMED_STRESS', '2026-03-11', '2026-08-28'))),
            evidence_refs=(evidence_reference_for_file(study / 'evaluated/manifest.json', role='overnight_increment_evaluation'),))
        AdvisoryResearchTrialRegistryV1(OUTPUT / 'trial_registry.jsonl').append_batch((record,))
        verify_registry(plan)
        parent = publish('terminal/evaluated', parent, {'receipt.json': json_bytes(dict(status='COMPLETED',
            **report, evaluated_sha256=parent, registry_complete=True))})
        return dict(status='COMPLETED', decision=report['decision'], actual_new_financial_fits=starts, terminal_sha256=parent)
    except BaseException as exc:
        path = OUTPUT / 'execution_failure.json'
        if not path.exists():
            with path.open('x', encoding='utf-8') as handle:
                json.dump(dict(status='ENGINEERING_NOT_COMPLETED', economic_acceptance='NOT_EVALUATED',
                    error_type=type(exc).__name__, error=str(exc),
                    actual_started_claims=len(list(claims.root.glob('*_started.json'))),
                    actual_finished_claims=len(list(claims.root.glob('*_finished.json')))), handle, indent=2)
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['preflight', 'freeze', 'run'])
    parser.add_argument('--authorization-ref')
    parser.add_argument('--max-new-fits', type=int)
    parser.add_argument('--receipt', type=Path, help='Create-only read-only-preflight receipt; never a source asset')
    args = parser.parse_args(argv)
    if args.action == 'preflight':
        if args.authorization_ref or args.max_new_fits is not None:
            parser.error('preflight has zero fit budget and needs no experiment authorization')
        _, result = preflight()
        if args.receipt:
            with args.receipt.open('xb') as handle:
                handle.write(json_bytes(result))
        print(json.dumps({k: v for k, v in result.items() if k != 'sources'}))
        return
    if args.receipt:
        parser.error('--receipt is only for preflight')
    authorization(args.authorization_ref, args.max_new_fits)
    with threadpool_limits(limits=2), _exclusive_file_lock(OUTPUT / 'runner.lock'):
        result = freeze(args.authorization_ref, args.max_new_fits) if args.action == 'freeze' else run(args.authorization_ref, args.max_new_fits)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
