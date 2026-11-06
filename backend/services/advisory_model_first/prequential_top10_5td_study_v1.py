"""One frozen mature-label update protocol; no new selection algorithm."""
import base64
import hashlib
import pickle

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY
from backend.services.advisory_model_first import multiperiod_top10_5td_study_v1 as base
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import FINANCE, IDENTITY

CONFIG = dict(id='ridge_100.0', family='ridge', alpha=100.)
CONTRACT = dict(schema='advisory_prequential_top10_5td_v1', hypothesis='MATURE_LABEL_UPDATE_TOP10_5TD_1',
    inherited=base.CONTRACT, config=CONFIG, update_sessions=20, inner_fit_budget=8, physical_fit_budget=18,
    objective_contract='ALPHA_RANKING', study_type='EXPLORATORY_SCREEN', decision_use='NAVIGATION_ONLY',
    inner_rule='POSITIVE_NATIVE_AND_STATIC_MEANS_TWO_POSITIVE_FOLDS_COVERAGE_80')


def schedule(days, calendar, initial_cutoff, prefix):
    """The original exchange D axis, never just rows with observed outcomes."""
    days, calendar = list(pd.to_datetime(days)), list(pd.to_datetime(calendar))
    cutoff = pd.Timestamp(initial_cutoff)
    if (not days or days != sorted(set(days)) or calendar != sorted(set(calendar))
            or not set(days).issubset(calendar) or days != [d for d in calendar if days[0] <= d <= days[-1]]):
        raise ValueError('update schedule needs complete ordered original trading dates')
    first = calendar.index(days[0])
    if first == 0 or calendar[first-1] != cutoff:
        raise ValueError('initial fit cutoff must precede the first prediction session')
    blocks = []
    for index, start in enumerate(range(0, len(days), 20)):
        chunk = days[start:start+20]
        c = cutoff if start == 0 else days[start-1]
        blocks.append(dict(key=f'{prefix}_{index:02}', cutoff=str(c.date()),
            first=str(chunk[0].date()), last=str(chunk[-1].date()), days=[str(d.date()) for d in chunk]))
    return blocks


def assert_population(inputs, predictions):
    a = inputs.loc[:, IDENTITY].reset_index(drop=True)
    b = predictions.loc[:, IDENTITY].reset_index(drop=True)
    if a.duplicated(['package_id', *KEY]).any() or not a.equals(b):
        raise ValueError('updated predictions changed original identity, order or population')


def model_packet(model, encoding, versions):
    payload = pickle.dumps(model, protocol=5)
    return dict(pickle_base64=base64.b64encode(payload).decode(), pickle_sha256=hashlib.sha256(payload).hexdigest(),
        encoding=encoding, config=CONFIG, fit_versions=versions)


def own_model(packet, versions):
    """Caller must first verify its own immutable stage chain; no external pickle URI."""
    if packet['config'] != CONFIG or packet['fit_versions'] != versions:
        raise ValueError('frozen model configuration or fit environment changed')
    payload = base64.b64decode(packet['pickle_base64'], validate=True)
    if hashlib.sha256(payload).hexdigest() != packet['pickle_sha256']:
        raise ValueError('frozen own-model payload hash changed')
    model = pickle.loads(payload)
    if (type(model) is not base.Ridge or model.get_params()['alpha'] != 100.
            or any(model.get_params()[k] != v for k, v in base.CONTRACT['ridge'].items())):
        raise ValueError('frozen own-model is not the declared Ridge100')
    return model, packet['encoding']


def projected_finance(metadata, observed, cutoff):
    """Future H rows stay IMMATURE without decoding their outcome payload."""
    keys = ['package_id', *KEY]
    if metadata.duplicated(keys).any() or observed.duplicated(keys).any():
        raise ValueError('duplicate persisted finance identity')
    if not set(map(tuple, observed[keys].to_numpy())).issubset(set(map(tuple, metadata[keys].to_numpy()))):
        raise ValueError('observed labels are outside their metadata population')
    clocks = observed[[*keys, 'label_information_end']].merge(metadata, on=keys, validate='one_to_one', suffixes=('_observed', '_declared'))
    if not clocks.label_information_end_observed.equals(clocks.label_information_end_declared):
        raise ValueError('observed financial maturity identity differs from its original metadata')
    if observed.label_information_end.isna().any() or observed.label_information_end.gt(pd.Timestamp(cutoff)).any():
        raise ValueError('future or unbounded financial label reached cutoff')
    result = metadata.merge(observed.drop(columns='label_information_end'), on=keys, how='left', validate='one_to_one')
    immature = result.label_information_end.isna() | result.label_information_end.gt(pd.Timestamp(cutoff))
    missing_mature = ~immature & result.valuation_status.isna()
    if missing_mature.any():
        raise ValueError('mature finance disappeared; cannot fabricate UNKNOWN or cash')
    result.loc[immature, ['valuation_status', 'exit_execution_status']] = 'IMMATURE'
    result.loc[immature, 'valuation_policy_sha256'] = base.VALUATION_POLICY_SHA256
    return result.loc[:, [*keys, *FINANCE]]


def inner_report(candidate, static, finance_by_fold):
    """All three arms share the same known D/package population for both deltas."""
    panels, expected_days = [], 0
    for fold in range(1, 4):
        p, s = candidate.loc[candidate.fold.eq(fold)], static.loc[static.fold.eq(fold)]
        f = finance_by_fold[fold]
        n = p.package_id.nunique()
        if n != 2 or set(map(tuple, p[['package_id', *KEY]].to_numpy())) != set(map(tuple, s[['package_id', *KEY]].to_numpy())):
            raise ValueError('inner static and updating populations or packages differ')
        a, b = base.cohort(p, f), base.cohort(s, f)
        paired = a.merge(b[['package_id', 'decision_date', 'candidate']], on=['package_id', 'decision_date'],
            validate='one_to_one', suffixes=('', '_static'))
        paired['versus_static'] = paired.candidate-paired.candidate_static
        by_d = paired.groupby('decision_date', sort=True)
        full = by_d[['delta', 'versus_static']].count().eq(n).all(axis=1)
        mature = by_d.mature.all()
        daily = by_d[['delta', 'versus_static']].mean()
        daily.loc[~(full & mature)] = np.nan
        daily['fold'] = fold
        daily['mature'] = mature
        panels.append(daily)
        expected_days += int(mature.sum())
    panel = pd.concat(panels).sort_index()
    means = panel.groupby('fold')[['delta', 'versus_static']].mean()
    known = panel.delta.notna()
    coverage = float(known.sum()/expected_days) if expected_days else 0.
    native = float(panel.delta.mean()) if known.any() else None
    static_delta = float(panel.versus_static.mean()) if known.any() else None
    positive = int(means.delta.gt(0).sum())
    go = bool(native is not None and native > 0 and static_delta > 0 and positive >= 2
        and means.delta.notna().all() and coverage >= .8)
    report = dict(classification='INNER_PROTOCOL_SUPPORTED_FOR_EXPLORATORY_OUTER' if go else 'INNER_PROTOCOL_NOT_SUPPORTED',
        proceed_outer=go, candidate_minus_native_bps=native, candidate_minus_static_bps=static_delta,
        positive_native_folds=positive, common_days=int(known.sum()), mature_days=expected_days, common_coverage=coverage,
        by_fold={str(i): {k: float(v) if pd.notna(v) else None for k, v in row.items()} for i, row in means.iterrows()},
        independent_oos_evidence=False, activation_evidence=False, decision_use='NAVIGATION_ONLY')
    return report, panel.reset_index()


def mechanism_delta(candidate_panel, static_panel):
    joined = candidate_panel.merge(static_panel[['package_id', 'decision_date', 'candidate']],
        on=['package_id', 'decision_date'], suffixes=('', '_static'), validate='one_to_one')
    joined['update_minus_static'] = joined.candidate-joined.candidate_static
    groups = joined.groupby('decision_date', sort=True)
    all_arms = groups[['candidate', 'baseline', 'candidate_static']].count().eq(2).all(axis=1)
    daily = groups.update_minus_static.mean().where(all_arms)
    return dict(population='THREE_ARMS_COMMON_D_TWO_PACKAGES', statistics=base.paired_interval(daily.to_numpy()))
