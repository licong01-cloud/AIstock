"""BUG-2178: exact linear ordering of frozen pool models; no fit or return labels.

The historical producer stays immutable. This versioned correction replay ranks
the same encoded binary64 inputs and coefficients using exact rational sums.
Subtracting a group constant cannot change a linear model's mathematical order.
The float decision_score is diagnostic only; ranking_score_exact is authoritative.
"""
import argparse
from fractions import Fraction
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from threadpoolctl import threadpool_limits  # noqa: E402
from scripts import advisory_lstm_pool_relation as frozen  # noqa: E402
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage, read_stage  # noqa: E402
from backend.services.advisory_model_first.research_control import _exclusive_file_lock  # noqa: E402

core = frozen.core
OUTPUT = Path('X:/AIstock_temp/ADVISORY/linear-rank-stability-20261106')
TERMINAL = '631350e9a3d6ab654695cf9fa1181827e6f9e477da34474b1f600ea441865e77'
SEMANTICS = 'EXACT_RATIONAL_UNCENTERED_LINEAR_V1'


def exact_scores(matrix, coefficients):
    """No product rounding, overflow, underflow, or group-offset cancellation."""
    x, c = np.asarray(matrix, dtype=float), np.asarray(coefficients, dtype=float)
    if x.ndim != 2 or c.ndim != 1 or x.shape[1] != len(c):
        raise ValueError('linear dimensions differ')
    if not np.isfinite(x).all() or not np.isfinite(c).all():
        raise ValueError('nonfinite linear input')
    active = [(j, Fraction(float(value))) for j, value in enumerate(c) if value != 0]
    return [sum((Fraction(float(row[j])) * weight for j, weight in active), Fraction(0)) for row in x]


def encoded_inputs(inputs, context, packet):
    pd.testing.assert_frame_equal(inputs[core.KEY].reset_index(drop=True), context[core.KEY].reset_index(drop=True), check_dtype=False)
    with np.errstate(over='raise', invalid='raise', divide='raise'):
        raw = core.info.numeric(inputs, packet['fields'])
        base = core.info.transform(raw, np.asarray(packet['median']), np.asarray(packet['scale']))
        positions = [core.RELATIVE.index(f) for f in packet['relative_fields']]
        relative = core.relative_raw(context, packet['arm'])[:, positions]
        block = core.info.transform(relative, np.asarray(packet['relative_median']), np.asarray(packet['relative_scale']))
    return np.column_stack((base, block))


def predict(inputs, packet, *, phase):
    core.validate_model(packet)
    _, first, last = core.info.spo.base.FOLDS[packet['fold'] - 1]
    if phase == 'stress' and packet['fold'] == 3:
        first, last = core.CONTRACT['stress']
    elif phase != 'fold':
        raise ValueError('registered offline phase required')
    if not inputs[core.DAY].between(pd.Timestamp(first), pd.Timestamp(last)).all():
        raise ValueError('prediction outside fixed phase')
    context = core.contexts(inputs, packet)  # Full original Top50, no label projection.
    eligible = np.isfinite(core.info.spo.numeric(inputs)).any(axis=1)
    active = inputs.loc[eligible].reset_index(drop=True)
    x = encoded_inputs(active, context.loc[eligible].reset_index(drop=True), packet)
    exact = exact_scores(x, packet['learner']['coef'])
    result = inputs.loc[:, [*core.info.spo.IDENTITY, 'parent_score', 'source_id']].copy().reset_index(drop=True)
    result['decision_score'] = np.nan
    result['ranking_score_exact'] = None
    result['ranking_semantics'] = SEMANTICS
    result['no_rank_advice'] = ~eligible
    result['advisory_rank'] = result.selection_effective_rank.copy()
    lookup = dict(zip(np.flatnonzero(eligible), exact, strict=True))
    for _, group in result.groupby(core.GROUP, sort=False):
        known = group.loc[~group.no_rank_advice]
        if known.empty:
            continue
        mean = sum((lookup[i] for i in known.index), Fraction(0)) / len(known)
        for i in known.index:
            try:
                diagnostic = float(lookup[i] - mean)
            except OverflowError as exc:
                raise ValueError('centered diagnostic outside binary64 range') from exc
            if not np.isfinite(diagnostic):
                raise ValueError('nonfinite diagnostic')
            result.at[i, 'decision_score'] = diagnostic
            result.at[i, 'ranking_score_exact'] = str(lookup[i])
        # Exact ties alone use parent rank; rounded diagnostic ties do not.
        ordered = sorted(known.index, key=lambda i: (-lookup[i], int(result.at[i, 'selection_effective_rank'])))
        result.loc[ordered, 'advisory_rank'] = sorted(known.selection_effective_rank)
    result['selected'] = result.advisory_rank.le(10)
    core.info.spo.evaluation.same_population(inputs, result)
    return result, context


def source_identity():
    """Verify the immutable completed chain without decoding any finance panel."""
    study = frozen.OUTPUT / 'study'
    plan = frozen.load(study / 'preregistered/protocol.json')
    frozen.verify(plan)
    parent = None
    for relative in frozen.stages():
        path = study / relative
        parent = read_stage(path, stage=path.name, plan_sha256=core.info.spo.sha(plan), parent_sha256=parent)['stage_sha256']
    if parent != TERMINAL:
        raise ValueError('frozen research terminal differs')
    frozen.verify_registry(plan)
    starts = len(list((study / 'fit_claims').glob('*_started.json')))
    finished = len(list((study / 'fit_claims').glob('*_finished.json')))
    if (starts, finished) != (6, 6):
        raise ValueError('frozen six-fit ledger differs')
    return dict(terminal_sha256=parent, protocol_sha256=core.info.spo.sha(plan), original_fit_claims=starts,
                input=frozen.ref(study / 'prepared/inputs.parquet'))


def code_identity(committed=False):
    names = (*frozen.FILES, 'scripts/advisory_lstm_linear_rank_replay.py')
    result = {n: frozen.prior.previous.files.code_sha((ROOT / n).read_bytes()) for n in names}
    if committed:
        for name, digest in result.items():
            if frozen.prior.previous.files.code_sha(subprocess.check_output(['git', 'show', f'HEAD:{name}'], cwd=ROOT)) != digest:
                raise ValueError('uncommitted source: ' + name)
    return result


def run():
    identity = source_identity()
    contract = dict(schema='advisory_linear_rank_correction_v1', semantics=SEMANTICS, source=identity,
        closure=code_identity(), versions=frozen.prior.versions(), scope='FROZEN_POOL_MATCHED_AND_CANDIDATE_ONLY',
        new_fits=0, new_return_labels=0, original_result_unchanged=True, production_ready=False)
    digest = core.info.spo.sha(contract)
    target = OUTPUT / 'evaluated'
    if target.exists():
        stage = read_stage(target, stage='evaluated', plan_sha256=digest, parent_sha256=TERMINAL)
        print(json.dumps(dict(status='REPLAY_REUSED', new_fits=0, stage_sha256=stage['stage_sha256'])))
        return
    if subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT).strip():
        raise ValueError('clean committed producer required')
    code_identity(True)
    started = time.monotonic()
    study = frozen.OUTPUT / 'study'
    inputs = pd.read_parquet(study / 'prepared/inputs.parquet')
    artifacts, details = {'contract.json': frozen.json_bytes(contract)}, []
    for arm in core.ARMS:
        for fold, (_, first, last) in enumerate(core.info.spo.base.FOLDS, 1):
            path = study / f'fold{fold}/{arm}/trained'
            packet = frozen.load(path / 'model.json')
            phases = [('fold', first, last, 'predictions.parquet', 'context.parquet')]
            if fold == 3:
                phases.append(('stress', *core.CONTRACT['stress'], 'stress_predictions.parquet', 'stress_context.parquet'))
            for phase, first, last, filename, context_name in phases:
                source = core.info.ordered(inputs.loc[inputs[core.DAY].between(first, last)])
                corrected, context = predict(source, packet, phase=phase)
                original = pd.read_parquet(path / filename)
                pd.testing.assert_frame_equal(corrected[core.KEY], original[core.KEY])
                pd.testing.assert_frame_equal(context, pd.read_parquet(path / context_name))
                if not np.array_equal(corrected.no_rank_advice, original.no_rank_advice):
                    raise ValueError('eligibility changed')
                rank_diff = corrected.advisory_rank.ne(original.advisory_rank)
                member_diff = corrected.selected.ne(original.selected)
                dates = sorted(corrected.loc[rank_diff, core.DAY].dt.strftime('%Y-%m-%d').unique())
                details.append(dict(arm=arm, fold=fold, phase=phase, rows=len(corrected), days=source[core.DAY].nunique(),
                    changed_rank_rows=int(rank_diff.sum()), changed_selection_rows=int(member_diff.sum()), changed_dates=dates,
                    original_prediction=frozen.ref(path / filename), model=frozen.ref(path / 'model.json'),
                    original_context=frozen.ref(path / context_name)))
                artifacts[f'{arm}_fold{fold}_{phase}.parquet'] = frozen.parquet_bytes(corrected)
                print(json.dumps(dict(status='REPLAYED', **{k: v for k, v in details[-1].items() if not isinstance(v, dict)})), flush=True)
    if sum(d['rows'] for d in details) != 24400 or source_identity() != identity or code_identity() != contract['closure']:
        raise ValueError('population/source/code drift')
    report = dict(status='CORRECTNESS_REPLAY_COMPLETED', details=details, source=identity,
        total_rows=24400, new_fits=0, new_return_labels=0, economic_evaluation='NOT_PERFORMED',
        prior_economic_decision='UNCHANGED_STOP_EXACT_CONTEXT_CANDIDATE', production_ready=False,
        elapsed_seconds=time.monotonic() - started,
        producer_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip())
    artifacts['receipt.json'] = frozen.json_bytes(report)
    if sum(map(len, artifacts.values())) > 200 * 1024**2:
        raise ValueError('bounded correction artifact budget exceeded')
    publish_stage(study_root=OUTPUT, stage='evaluated', plan_sha256=digest, parent_sha256=TERMINAL, artifacts=artifacts)
    print(json.dumps(dict(status=report['status'], rows=24400, new_fits=0, new_return_labels=0,
        elapsed_seconds=report['elapsed_seconds'])))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['run'])
    parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with threadpool_limits(limits=2), _exclusive_file_lock(OUTPUT / 'runner.lock'):
        run()


if __name__ == '__main__':
    main()
