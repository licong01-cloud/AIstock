"""One bounded local-peer study; source helpers never invoke predecessor experiments."""
import argparse
from datetime import datetime, timezone
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
from scripts import advisory_lstm_factor_nonlinear as prior  # noqa: E402
from backend.services.advisory_model_first import lstm_pool_relation_v1 as core  # noqa: E402
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage, read_stage  # noqa: E402
from backend.services.advisory_model_first.research_control import (  # noqa: E402
    AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import build_trial_record  # noqa: E402

OUTPUT = Path('X:/AIstock_temp/ADVISORY/lstm-pool-relation-20261106')
load, ref, json_bytes, parquet_bytes = prior.load, prior.ref, prior.json_bytes, prior.parquet_bytes
FILES = tuple(dict.fromkeys((*prior.FILES, 'backend/services/advisory_model_first/lstm_pool_relation_v1.py',
                            'scripts/advisory_lstm_pool_relation.py')))


def paths():
    result = prior.paths()
    for f in (1, 2, 3):
        result['encoder' + str(f)] = prior.OUTPUT / f'study/fold{f}/ridge/trained/model.json'
    for name in ('input_contract', 'novelty_review', 'synthetic_receipt', 'input_support_receipt', 'independent_screen_audit'):
        result[name] = OUTPUT / (name + '.json')
    result['screen_rows'] = OUTPUT / 'screen_rows.parquet'
    return result


def closure(committed=False):
    result = {n: prior.previous.files.code_sha((ROOT / n).read_bytes()) for n in FILES}
    if committed:
        for n, digest in result.items():
            if prior.previous.files.code_sha(subprocess.check_output(['git', 'show', f'HEAD:{n}'], cwd=ROOT)) != digest:
                raise ValueError('uncommitted source: ' + n)
    return result


def checked(plan, name):
    p = paths()[name]
    if ref(p) != plan['sources'][name]:
        raise ValueError('source drift: ' + name)
    return p


def verify(plan):
    if (plan['contract'] != json.loads(json_bytes(core.CONTRACT)) or plan['closure'] != closure()
            or plan['versions'] != prior.versions()):
        raise ValueError('contract/code/environment drift')
    for name in paths():
        checked(plan, name)


def stages():
    return ['preregistered', 'prepared', *[f'fold{f}/{a}/trained' for f in (1, 2, 3) for a in core.ARMS],
            'evaluated', 'terminal/evaluated']


def verify_registry(plan):
    rows = [json.loads(s) for s in (OUTPUT / 'trial_registry.jsonl').read_text(encoding='utf8').splitlines() if s.strip()]
    if len(rows) != 1 or rows[0]['experiment_id'] != plan['experiment_id'] or rows[0]['evaluated_trial_count'] != 1:
        raise ValueError('registry missing or incompatible')


def freeze():
    if subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT).strip():
        raise ValueError('clean committed producer required')
    prior.verify_parent()  # Read-only original 54-input chain; no labels or fitting.
    old_plan = load(prior.OUTPUT / 'study/preregistered/protocol.json')
    parent = None
    for relative in prior.stages():
        path = prior.OUTPUT / 'study' / relative
        parent = read_stage(path, stage=path.name, plan_sha256=core.info.spo.sha(old_plan), parent_sha256=parent)['stage_sha256']
    if parent != 'c6fb9df908e7250e7acf05d2023fea628682cf16028376139098f6bd73392ddb':
        raise ValueError('257-fit predecessor terminal differs')
    for name in ('input_support_receipt', 'independent_screen_audit', 'synthetic_receipt'):
        if load(paths()[name])['status'] != 'PASS':
            raise ValueError('P1/P2 admission missing')
    core.previous.factory('ridge')  # Constructor only.
    plan = dict(schema=core.CONTRACT['schema'], contract=core.CONTRACT, sources={n: ref(p) for n, p in paths().items()},
        closure=closure(True), versions=prior.versions(), predecessor_terminal=parent,
        experiment_id='advlstm_pool_relation_20261106_v1', max_new_fits=6,
        producer_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        captured_at=datetime.now(timezone.utc).isoformat(), production_ready=False,
        evidence='PREVIOUSLY_CONSUMED_NON_VINTAGE_RECONSTRUCTED_INPUT')
    publish_stage(study_root=OUTPUT / 'study', stage='preregistered', plan_sha256=core.info.spo.sha(plan),
                  parent_sha256=None, artifacts={'protocol.json': json_bytes(plan)})
    print(json.dumps(dict(status='PREREGISTERED', fits=0, protocol_sha256=core.info.spo.sha(plan))))


def screen_alignment(inputs, meta):
    source = inputs.loc[inputs[core.DAY].between('2024-07-04', '2025-03-31')].reset_index(drop=True)
    result = core.contexts(source, meta)
    original = pd.read_parquet(OUTPUT / 'screen_rows.parquet')
    columns = [*core.KEY, 'selection_effective_rank', *[c for c in result if c not in core.KEY and c != 'selection_effective_rank']]
    expected = source[core.KEY].merge(original[columns], on=core.KEY, validate='one_to_one', how='left')
    pd.testing.assert_frame_equal(result[columns].reset_index(drop=True), expected[columns].reset_index(drop=True),
                                  check_dtype=False, check_exact=False, rtol=1e-12, atol=1e-10)


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
        print(json.dumps(dict(status='STUDY_REUSED', new_financial_fits=0, terminal_sha256=parent)))
        return
    if list((study / 'fit_claims').glob('*_started.json')):
        raise ValueError('partial STARTED: no implicit refit')
    # Prior resource helper is read-only, uses the same X volume and never gates on QE idle.
    initial = prior.resource_admission()
    started = time.monotonic()
    claims = core.FitClaims(study / 'fit_claims', digest)
    telemetry = [dict(moment='BEFORE_LABELS', **initial)]

    def budget():
        mem = psutil.Process().memory_info()
        if max(mem.rss, getattr(mem, 'peak_wset', mem.rss)) > 2 * 1024**3 or time.monotonic() - started > 1800:
            raise ValueError('2GiB/1800s budget exceeded')

    def publish(relative, parent, artifacts):
        budget()
        verify(plan)
        if sum(p.stat().st_size for p in study.rglob('*') if p.is_file()) + sum(map(len, artifacts.values())) > 200 * 1024**2:
            raise ValueError('200MiB artifact budget exceeded')
        path = study / relative
        publish_stage(study_root=path.parent, stage=path.name, plan_sha256=digest, parent_sha256=parent, artifacts=artifacts)
        return read_stage(path, stage=path.name, plan_sha256=digest, parent_sha256=parent)['stage_sha256']

    try:
        parent = read_stage(study / 'preregistered', stage='preregistered', plan_sha256=digest, parent_sha256=None)['stage_sha256']
        inputs = core.info.ordered(pd.read_parquet(checked(plan, 'inputs')))
        core.info.check_inputs(inputs)
        dev = inputs.loc[inputs[core.DAY].le('2025-09-30')].reset_index(drop=True)
        stress = inputs.loc[inputs[core.DAY].ge('2026-03-11')].reset_index(drop=True)
        if (len(dev), dev[core.DAY].nunique(), len(stress), stress[core.DAY].nunique()) != (15250, 305, 5900, 118):
            raise ValueError('original population differs')
        screen_alignment(dev, load(checked(plan, 'encoder1')))
        parent = publish('prepared', parent, {'inputs.parquet': parquet_bytes(inputs), 'receipt.json': json_bytes(
            dict(original_rows=21150, screen_alignment=True, evaluation_labels_decoded=False))})
        for fold, (cutoff, first, last) in enumerate(core.info.spo.base.FOLDS, 1):
            finance = pd.read_parquet(checked(plan, 'train' + str(fold)))
            train = dev.loc[dev[core.DAY].le(cutoff)].merge(finance, on=core.KEY, how='left', validate='one_to_one', indicator=True)
            if not train._merge.eq('both').all():
                raise ValueError('training keys lost')
            train = train.drop(columns='_merge')
            encoder = load(checked(plan, 'encoder' + str(fold)))
            target = dev.loc[dev[core.DAY].between(first, last)].reset_index(drop=True)
            for arm in core.ARMS:
                def before_fit(arm=arm, fold=fold):
                    budget()
                    telemetry.append(dict(fold=fold, arm=arm, moment='BEFORE_START', **prior.resource_admission()))
                    claims.write(arm, fold)
                packet, estimator = core.fit(train, fold, arm, encoder, before_fit=before_fit, budget=budget)
                checkpoints = study / 'fit_checkpoints'
                checkpoints.mkdir(exist_ok=True)
                with (checkpoints / f'{arm}{fold}.json').open('xb') as f:
                    f.write(json_bytes(packet))
                pred = core.predict(target, packet, phase='fold', oracle=estimator)
                artifacts = {'model.json': json_bytes(packet), 'predictions.parquet': parquet_bytes(pred),
                    'context.parquet': parquet_bytes(core.contexts(target, packet)),
                    'receipt.json': json_bytes(dict(fold=fold, arm=arm, new_financial_fits=1,
                        certificate=packet['certificate'], evaluation_labels_decoded=False))}
                if fold == 3:
                    artifacts['stress_predictions.parquet'] = parquet_bytes(core.predict(stress, packet, phase='stress', oracle=estimator))
                    artifacts['stress_context.parquet'] = parquet_bytes(core.contexts(stress, packet))
                parent = publish(f'fold{fold}/{arm}/trained', parent, artifacts)
                claims.write(arm, fold, parent)
                telemetry.append(dict(fold=fold, arm=arm, moment='AFTER_FINISH', **prior.resource_admission(require_capacity=False)))
                print(json.dumps(dict(status='FIT_FINISHED', fold=fold, arm=arm, certificate=packet['certificate'])), flush=True)
        panels = {a: [] for a in core.ARMS}
        for fold in (1, 2, 3):
            finance = pd.read_parquet(checked(plan, 'finance'), filters=[('fold', '==', fold)]).drop(columns='fold')
            prior.previous.verify_fees(finance)
            for arm in core.ARMS:
                pred = pd.read_parquet(study / f'fold{fold}/{arm}/trained/predictions.parquet')
                panel = core.info.spo.base.cohort(pred, finance)
                panel['fold'] = fold
                panels[arm].append(panel)
        panels = {a: pd.concat(v, ignore_index=True) for a, v in panels.items()}
        if any(len(p) != 126 or p.original_candidates.sum() != 6300 for p in panels.values()):
            raise ValueError('evaluation dates differ')
        development = core.summarize(panels['candidate'], panels['matched'])
        fold_means = {str(int(f)): float(g.delta.mean()) if g.delta.notna().any() else None
                      for f, g in panels['candidate'].groupby('fold')}
        finance = pd.read_parquet(checked(plan, 'stress_finance'))
        prior.previous.verify_fees(finance)
        pressure = {}
        for arm in core.ARMS:
            pred = pd.read_parquet(study / f'fold3/{arm}/trained/stress_predictions.parquet')
            _, pressure[arm] = core.info.prior.stress_report(pred, finance, sorted(stress[core.DAY].unique()))
        stress_report = core.summarize(pressure['candidate'], pressure['matched'])
        starts = len(list(claims.root.glob('*_started.json')))
        if starts != 6 or len(list(claims.root.glob('*_finished.json'))) != 6:
            raise ValueError('incomplete claims')
        report = dict(development=development, stress=stress_report, by_fold=fold_means,
            matched=dict(development=core.summarize(panels['matched'], panels['matched']),
                         stress=core.summarize(pressure['matched'], pressure['matched'])),
            decision=core.decision(development, stress_report, fold_means), actual_new_financial_fits=starts,
            cumulative_actual_fits=257 + starts, old_index_count=1, candidate_count=1,
            historical_pit=False, independent_economic_confirmation=False, production_ready=False)
        artifacts = {'report.json': json_bytes(report), 'resource.json': json_bytes(dict(observations=telemetry,
            elapsed_seconds=time.monotonic() - started, rss_bytes=psutil.Process().memory_info().rss, cpu_threads=2, gpu=0))}
        for arm in core.ARMS:
            artifacts[arm + '_development.parquet'] = parquet_bytes(panels[arm])
            artifacts[arm + '_stress.parquet'] = parquet_bytes(pressure[arm])
        parent = publish('evaluated', parent, artifacts)
        record = build_trial_record(experiment_id=plan['experiment_id'], attempt_id='exact_v1', research_stage='COMPLETED',
            study_type='EXPLORATORY_SCREEN', objective_contract='ALPHA_RANKING', decision_use='NAVIGATION_ONLY',
            hypothesis_family_id=core.CONTRACT['hypothesis'], unique_variable='FACTOR_NEIGHBORS_VS_RANK_NEIGHBORS',
            parent_lineage=('advlstmfactor_nonlin_20261102_v1',), dataset_identity=plan['sources']['inputs']['sha256'],
            schema_identity=core.info.spo.sha(core.CONTRACT), policy_identity=core.info.spo.base.VALUATION_POLICY_SHA256,
            planned_trial_count=1, generated_trial_count=1, evaluated_trial_count=1, selected_trial_count=0,
            result_class='EXPLORATORY', consumed_windows=tuple(dict(window_id=label,
                dataset_identity=plan['sources']['inputs']['sha256'], start_date=first, end_date=last) for label, first, last in (
                    ('ALREADY_CONSUMED_DEVELOPMENT', '2024-07-04', '2025-09-30'),
                    ('ALREADY_CONSUMED_STRESS', '2026-03-11', '2026-08-28'))),
            evidence_refs=(evidence_reference_for_file(study / 'evaluated/manifest.json', role='pool_relation_evaluation'),))
        AdvisoryResearchTrialRegistryV1(OUTPUT / 'trial_registry.jsonl').append_batch((record,))
        verify_registry(plan)
        terminal = publish('terminal/evaluated', parent, {'receipt.json': json_bytes(dict(status='COMPLETED', **report,
            evaluated_sha256=parent, registry_complete=True))})
        print(json.dumps(dict(status='COMPLETED', decision=report['decision'], fits=starts,
            development_bps=development['statistics']['mean_bps'], stress_bps=stress_report['statistics']['mean_bps'],
            terminal_sha256=terminal)))
    except BaseException as exc:
        with (OUTPUT / 'execution_failure.json').open('x', encoding='utf-8') as f:
            json.dump(dict(error_type=type(exc).__name__, error=str(exc),
                actual_started_claims=len(list(claims.root.glob('*_started.json'))), automatic_refit=False), f, indent=2)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['freeze', 'run'])
    args = parser.parse_args()
    with threadpool_limits(limits=2), _exclusive_file_lock(OUTPUT / 'runner.lock'):
        (freeze if args.action == 'freeze' else run)()


if __name__ == '__main__':
    main()
