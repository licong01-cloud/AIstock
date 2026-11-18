"""One prespecified nine-fold, paired Ridge study of frozen recovery inputs."""
from __future__ import annotations

import math

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from backend.services.advisory_model_first import minute_recovery_v1 as features
from backend.services.advisory_model_first.minute_recovery_inputs_v1 import DAY, KEY
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY, INPUT_FEATURES
from backend.services.advisory_model_first.multiperiod_top10_5td_study_v1 import _validate_finance, cohort
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

FOLDS = (("2025-03-31", "2025-04-01", "2025-05-31"),
         ("2025-05-31", "2025-06-01", "2025-07-31"),
         ("2025-07-31", "2025-08-01", "2025-09-30"),
         ("2025-09-30", "2025-10-01", "2025-11-30"),
         ("2025-11-30", "2025-12-01", "2026-01-31"),
         ("2026-01-31", "2026-02-01", "2026-03-31"),
         ("2026-03-31", "2026-04-01", "2026-05-31"),
         ("2026-05-31", "2026-06-01", "2026-07-31"),
         ("2026-07-31", "2026-08-01", "2026-08-28"))
RIDGE = dict(alpha=100., solver="lsqr", tol=1e-6, max_iter=2000, fit_intercept=False)
CONTRACT = dict(schema="advisory_minute_recovery_experiment_v1", folds=FOLDS, ridge=RIDGE,
                start="2024-07-04", end="2026-08-28", evaluation_start="2025-04-01",
                matched=features.MATCHED, candidate=features.FEATURES, max_fits=18, top_k=10,
                buy_bps=.95, sell_bps=5.95, horizon_sessions=5,
                valuation_policy_sha256=VALUATION_POLICY_SHA256,
                encoding="TRAIN_MEDIAN_MISSING_BITS_IQR_ELSE_STD_ELSE_ONE_D_CENTERED",
                all_missing_columns="INACTIVE_TRAIN_ONLY", sample_weight="EQUAL_DECISION_DAY_NORMALIZED",
                objective="WITHIN_D_RELATIVE_NET_BPS", decision_use="NAVIGATION_ONLY",
                bootstrap=dict(block_sessions=10, replications=10000, seed=20261118),
                criteria=dict(mean_bps=100., ci_lower_bps=0., coverage=.8, intervention_days=20,
                              intervention_fraction=.2, max_unknown_excess=.02, max_tail_deterioration_bps=100.))


def support(frame):
    raw = frame[list(features.FEATURES)].to_numpy(float)
    if np.isinf(raw).any():
        raise ValueError("infinite known feature")
    expected = np.isfinite(frame[list(INPUT_FEATURES)].to_numpy(float)).any(axis=1)
    expected &= np.isfinite(frame[[*features.CONTROLS, *features.NEW]].to_numpy(float)).all(axis=1)
    if not np.array_equal(frame.model_support.to_numpy(), expected):
        raise ValueError("frozen feature support differs")
    return expected


def center(values, rows):
    groups = pd.factorize(rows[DAY], sort=False)[0]
    return values - pd.DataFrame(values).groupby(groups, sort=False).transform("mean").to_numpy()


def train(rows, cutoff, arm, *, before_fit=lambda: None):
    if arm not in ("matched", "candidate"):
        raise ValueError("unregistered arm")
    cutoff = pd.Timestamp(cutoff)
    # Project D and H before any feature/target numeric decoding.
    part = rows.loc[rows[DAY].between(CONTRACT["start"], cutoff)
                    & rows.label_information_end.le(cutoff)].copy().reset_index(drop=True)
    _validate_finance(part)
    if part.duplicated(KEY).any():
        raise ValueError("duplicate training key")
    part = part.loc[part.valuation_status.isin(["AVAILABLE", "CASH_ENTRY_NOT_EXECUTABLE"])
                    & support(part)].reset_index(drop=True)
    if len(part) < 2 or part[DAY].nunique() < 2:
        raise ValueError("insufficient common mature training support")
    fields = list(features.MATCHED if arm == "matched" else features.FEATURES)
    raw = part[fields].to_numpy(float)
    active = np.isfinite(raw).any(axis=0)
    inactive = [n for n, yes in zip(fields, active, strict=True) if not yes]
    fields = [n for n, yes in zip(fields, active, strict=True) if yes]
    raw = raw[:, active]
    q = np.nanquantile(raw, [.25, .5, .75], axis=0)
    std = np.nanstd(raw, axis=0)
    scale = np.where(q[2] > q[0], q[2]-q[0], np.where(std > 0, std, 1.))
    x = np.column_stack(((np.where(np.isnan(raw), q[1], raw)-q[1])/scale, np.isnan(raw).astype(float)))
    x = center(x, part)
    y = center(part.hypothetical_liquidation_net_bps.to_numpy(float)[:, None], part).ravel()
    weights = 1 / part.groupby(DAY)[DAY].transform("size").to_numpy(float)
    weights /= weights.mean()
    before_fit()
    model = Ridge(**RIDGE).fit(x, y, sample_weight=weights)
    iterations = np.atleast_1d(model.n_iter_)
    if (not np.isfinite(model.coef_).all() or np.max(iterations) >= RIDGE["max_iter"]
            or not np.allclose(model.predict(x), x @ model.coef_, rtol=1e-12, atol=1e-9)):
        raise ValueError("Ridge convergence or JSON prediction parity failed")
    return dict(contract_sha256=sha(CONTRACT), arm=arm, cutoff=str(cutoff.date()), fields=fields,
                inactive_fields=inactive, median=q[1].tolist(), scale=scale.tolist(), coef=model.coef_.tolist(),
                iterations=iterations.tolist(), train_rows=len(part), train_days=int(part[DAY].nunique()),
                training_keys_sha256=sha([[str(v) for v in r] for r in part[KEY].itertuples(index=False, name=None)]),
                training_values_sha256=sha(dict(y=y.tolist(), weights=weights.tolist())),
                encoding_sha256=sha(dict(fields=fields, median=q[1].tolist(), scale=scale.tolist())))


def predict(inputs, packet):
    arm = packet["arm"]
    expected_fields = list(features.MATCHED if arm == "matched" else features.FEATURES)
    fields, inactive = packet["fields"], packet["inactive_fields"]
    if (arm not in ("matched", "candidate") or packet["contract_sha256"] != sha(CONTRACT)
            or fields != [n for n in expected_fields if n not in inactive]
            or len(set(fields + inactive)) != len(expected_fields)
            or set(fields + inactive) != set(expected_fields)):
        raise ValueError("model field/contract identity differs")
    known = support(inputs)
    result = inputs[list(IDENTITY)].copy().reset_index(drop=True)
    result["no_rank_advice"] = ~known
    result["predicted_relative_net_bps"] = np.nan
    result["advisory_rank"] = result.selection_effective_rank.copy()
    selected = inputs.loc[known].reset_index(drop=True)
    raw = selected[fields].to_numpy(float)
    median, scale, coef = (np.asarray(packet[n], float) for n in ("median", "scale", "coef"))
    if (median.shape != (len(fields),) or scale.shape != median.shape or coef.shape != (2*len(fields),)
            or not np.isfinite(np.r_[median, scale, coef]).all() or (scale <= 0).any()):
        raise ValueError("invalid persisted model arithmetic")
    if len(selected):
        x = np.column_stack(((np.where(np.isnan(raw), median, raw)-median)/scale, np.isnan(raw).astype(float)))
        values = center(x, selected) @ coef
        if not np.isfinite(values).all():
            raise ValueError("nonfinite prediction")
        result.loc[known, "predicted_relative_net_bps"] = values
    for _, group in result.groupby(DAY, sort=False):
        if (len(group) != 50 or sorted(group.selection_effective_rank) != list(range(1, 51))
                or not group.candidate_group_size.eq(50).all()):
            raise ValueError("prediction population is not original Top50")
        active = group.loc[~group.no_rank_advice]
        order = active.sort_values(["predicted_relative_net_bps", "selection_effective_rank"],
                                   ascending=[False, True], kind="stable").index
        result.loc[order, "advisory_rank"] = sorted(active.selection_effective_rank)
    result["selected"] = result.advisory_rank.le(10)
    return result


def paired_interval(values):
    """Circular 10-D blocks sample the full original date axis, preserving holes."""
    values = np.asarray(values, float)
    if values.ndim != 1 or np.isinf(values).any():
        raise ValueError("invalid paired date axis")
    count = int(np.isfinite(values).sum())
    result = dict(mean_bps=float(np.nanmean(values)) if count else None, ci95_bps=None,
                  paired_days=count, original_days=len(values), block_sessions=10)
    if count < 2:
        return result
    rng = np.random.default_rng(20261118)
    means = []
    for _ in range(100):
        starts = rng.integers(0, len(values), (100, math.ceil(len(values)/10)))
        ids = ((starts[:, :, None] + np.arange(10)) % len(values)).reshape(100, -1)[:, :len(values)]
        samples = values[ids]
        n = np.isfinite(samples).sum(axis=1)
        means.extend((np.nansum(samples, axis=1)[n > 0]/n[n > 0]).tolist())
    result["ci95_bps"] = np.quantile(means, [.025, .975]).tolist()
    result["bootstrap_valid"] = len(means)
    return result


def distribution(values):
    a = np.asarray(values, float)
    a = a[np.isfinite(a)]
    if not len(a):
        return dict(known=0, mean_bps=None, median_bps=None, win_rate=None, std_bps=None, tail10_mean_bps=None)
    return dict(known=len(a), mean_bps=float(a.mean()), median_bps=float(np.median(a)),
                win_rate=float((a > 0).mean()), std_bps=float(a.std(ddof=0)),
                tail10_mean_bps=float(np.sort(a)[:max(1, math.ceil(len(a)*.1))].mean()))


def comparison(candidate, matched, finance):
    if not candidate[list(IDENTITY)].equals(matched[list(IDENTITY)]):
        raise ValueError("paired prediction population changed")
    if not candidate.no_rank_advice.equals(matched.no_rank_advice):
        raise ValueError("paired advice support differs")
    a, b = cohort(candidate, finance), cohort(matched, finance)
    result = a.rename(columns={"candidate": "candidate_net_bps", "baseline": "native_net_bps"})
    result["matched_net_bps"] = b.candidate.to_numpy()
    result["matched_unknown"] = b.candidate_unknown.to_numpy()
    for name in ("native", "matched"):
        result["delta_"+name+"_bps"] = result.candidate_net_bps - result[name+"_net_bps"]
    return result


def window_report(panel):
    valid = panel.mature & panel[["candidate_net_bps", "native_net_bps", "matched_net_bps"]].notna().all(axis=1)
    primary = panel.delta_native_bps.where(valid)
    auxiliary = panel.delta_matched_bps.where(valid)
    mature_count = int(panel.mature.sum())
    intervention = panel.mature & panel.intervention_count.gt(0)
    coverage = float(valid.sum()/mature_count) if mature_count else 0.
    unknown = float((panel.loc[panel.mature, "candidate_unknown"]-
                     panel.loc[panel.mature, "baseline_unknown"]).sum()/(10*mature_count)) if mature_count else None
    stats = paired_interval(primary)
    dists = {n: distribution(panel[n+"_net_bps"].where(valid)) for n in ("native", "matched", "candidate")}
    tail = (dists["candidate"]["tail10_mean_bps"]-dists["native"]["tail10_mean_bps"]) if valid.any() else None
    return dict(original_days=len(panel), mature_days=mature_count, common_complete_days=int(valid.sum()),
                common_mature_coverage=coverage, versus_native=stats, versus_matched=paired_interval(auxiliary),
                distributions=dists, intervention_days=int(intervention.sum()),
                intervention_fraction=float(intervention.sum()/mature_count) if mature_count else 0.,
                unknown_excess=unknown, tail_delta_bps=tail,
                decomposition_bps={n:float(panel.loc[valid, n].mean()) if valid.any() else None
                                   for n in ("avoided_loss", "missed_profit", "added_profit", "added_loss")})


def summarize(panel):
    if panel.decision_date.duplicated().any() or set(panel.fold) != set(range(1, 10)):
        raise ValueError("all nine unique date blocks required")
    windows = {"development": panel.loc[panel.decision_date.le("2025-09-30")],
               "expanded": panel.loc[panel.decision_date.ge("2025-10-01")]}
    reports = {name: window_report(p) for name, p in windows.items()}
    criteria = CONTRACT["criteria"]
    def supported(r):
        s = r["versus_native"]
        return (s["mean_bps"] is not None and s["mean_bps"] >= 100 and s["ci95_bps"] is not None
                and s["ci95_bps"][0] > 0 and r["common_mature_coverage"] >= .8
                and r["intervention_days"] >= 20 and r["intervention_fraction"] >= .2
                and r["tail_delta_bps"] is not None and r["tail_delta_bps"] >= -100
                and r["unknown_excess"] is not None and r["unknown_excess"] <= .02)
    negative = any((r["versus_native"]["mean_bps"] is not None and r["versus_native"]["mean_bps"] <= 0)
                   or (r["tail_delta_bps"] is not None and r["tail_delta_bps"] < -100)
                   or (r["unknown_excess"] is not None and r["unknown_excess"] > .02) for r in reports.values())
    verdict = "SUPPORTED_FOR_NEW_CONFIRMATION_DESIGN" if all(map(supported, reports.values())) else (
        "CLOSED_NEGATIVE_EXACT_CANDIDATE" if negative else "INCONCLUSIVE_NO_PROMOTION_SUPPORT")
    return dict(classification=verdict, windows=reports, overall=window_report(panel), criteria=criteria,
                by_quarter={str(q): window_report(g) for q, g in panel.groupby(panel.decision_date.dt.to_period("Q"))},
                by_fold={str(i):window_report(g) for i, g in panel.groupby("fold")},
                actual_fill_proven=False, independent_oos_evidence=False, economic_confirmation=False,
                production_ready=False, decision_use="NAVIGATION_ONLY")
