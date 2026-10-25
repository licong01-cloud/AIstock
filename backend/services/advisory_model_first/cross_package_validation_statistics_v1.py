"""Paired original-day block inference, not a NAV or profitability promise."""
from __future__ import annotations

import math

import numpy as np


def holm_adjust(pvalues):
    """Prespecified family size includes untestable members (p=1), not cherry-picks."""
    p = np.asarray(pvalues, dtype=float)
    if p.ndim != 1 or not np.isfinite(p).all() or ((p < 0) | (p > 1)).any():
        raise ValueError("family p-values must be finite probabilities")
    order = np.argsort(p, kind="stable")
    out = np.empty_like(p)
    running = 0.
    for position, i in enumerate(order):
        running = max(running, min(1., (len(p)-position)*p[i]))
        out[i] = running
    return out


def synchronous_block_inference(*, daily_values, original_days, expected_days, block_span,
                                replicates=10000, seed=20261009, confidence=.95):
    """Simultaneous max-standardized-error intervals across a frozen matrix.

    All test columns resample the exact same original day indices. Incomplete
    columns retain descriptive means but no CI; dates may never be compressed.
    A max-error bootstrap is a finite-sample research approximation, not exact
    coverage, and cannot erase upstream/data/window selection.
    """
    values = np.asarray(daily_values, dtype=float)
    if (values.ndim != 2 or values.shape[0] != len(original_days) or list(original_days) != list(expected_days)
            or len(set(original_days)) != len(original_days) or list(original_days) != sorted(original_days)
            or type(block_span) is not int or block_span < 1 or type(replicates) is not int or replicates < 1000
            or not 0 < confidence < 1 or np.isinf(values).any()):
        raise ValueError("original day axis / matrix / block / confidence differs")
    n, m = values.shape
    complete = np.isfinite(values).all(axis=0)
    means = [float(column[np.isfinite(column)].mean()) if np.isfinite(column).any() else None for column in values.T]
    indices = np.flatnonzero(complete)
    result = dict(status="EXPLORATORY_BLOCK_INFERENCE", family_size=m, original_day_count=n,
        complete_column_count=int(complete.sum()), block_span=block_span, replicates=replicates, seed=seed,
        means=means, intervals=[None]*m, raw_pvalues=[1.]*m, adjusted_pvalues=[1.]*m,
        standard_errors=[None]*m, mde_80pct_unadjusted_bps=[None]*m, mde_80pct_family_approx_bps=[None]*m,
        simultaneous_critical=None,
        method="SYNCHRONOUS_MOVING_BLOCK_MAX_ERROR", exact_coverage_claimed=False, activation_evidence=False)
    if n < 2*block_span or m == 0:
        result["status"] = "INSUFFICIENT_AXIS"
        return result
    if not len(indices):
        result["status"] = "INCOMPLETE_AXIS"
        return result
    x = values[:, indices]
    original = x.mean(axis=0)
    rng = np.random.default_rng(seed)
    blocks = math.ceil(n/block_span)
    boots = np.empty((replicates, len(indices)))
    offsets = np.arange(block_span)
    for first in range(0, replicates, 100):
        size = min(100, replicates-first)
        starts = rng.integers(0, n-block_span+1, size=(size, blocks))
        sample = (starts[:, :, None]+offsets).reshape(size, -1)[:, :n]
        boots[first:first+size] = x[sample].mean(axis=1)
    errors = boots-original
    se = boots.std(axis=0, ddof=1)
    stochastic = se > 0
    if stochastic.any():
        maxima = np.max(np.abs(errors[:, stochastic]/se[stochastic]), axis=1)
        critical = float(np.quantile(maxima, confidence, method="higher"))
    else:
        critical = 0.
    for j, i in enumerate(indices):
        # Constant paths do not constitute infinitely certain alpha estimates.
        if not stochastic[j]:
            continue
        result["intervals"][int(i)] = [float(original[j]-critical*se[j]), float(original[j]+critical*se[j])]
        result["raw_pvalues"][int(i)] = float((1+np.count_nonzero(np.abs(errors[:, j]) >= abs(original[j])))/(replicates+1))
    result["adjusted_pvalues"] = holm_adjust(result["raw_pvalues"]).tolist()
    result["simultaneous_critical"] = critical
    result["standard_errors"] = [float(se[np.where(indices == i)[0][0]]) if complete[i] else None for i in range(m)]
    result["mde_80pct_unadjusted_bps"] = [float((1.95996398454+.84162123357)*v) if v is not None and v > 0 else None
        for v in result["standard_errors"]]
    result["mde_80pct_family_approx_bps"] = [float((critical+.84162123357)*v) if v is not None and v > 0 else None
        for v in result["standard_errors"]]
    return result


def paired_skip_attribution(*, baseline_net_bps, model_net_bps, known_skip, unknown_skip):
    base, model = np.asarray(baseline_net_bps, dtype=float), np.asarray(model_net_bps, dtype=float)
    known, unknown = np.asarray(known_skip, dtype=bool), np.asarray(unknown_skip, dtype=bool)
    if base.shape != model.shape or base.shape != known.shape or base.shape != unknown.shape or (known & unknown).any():
        raise ValueError("attribution original paired population differs")
    if not np.isfinite([base, model]).all() or not np.allclose(model[known | unknown], 0):
        raise ValueError("attribution needs settled pairs with declared cash skips")
    increment = model-base
    avoided = float(-np.minimum(base[known], 0).sum())
    missed = float(np.maximum(base[known], 0).sum())
    unknown_value = float(increment[unknown].sum())
    other = float(increment[~(known | unknown)].sum())
    total = float(increment.sum())
    if not math.isclose(total, avoided-missed+unknown_value+other, abs_tol=1e-8):
        raise ValueError("attribution does not reconcile on original denominator")
    return dict(paired_count=len(base), total_increment_bps=total, known_avoided_loss_bps=avoided,
        known_missed_profit_bps=missed, unknown_cash_increment_bps=unknown_value, other_action_increment_bps=other)


def package_axis_aggregates(*, daily_values, package_ids, expected_packages, source_groups):
    """Equal-package and equal-parent-cluster estimands, with no NA renormalizing.

    Related component signals remain different policies within a parent cluster;
    clustering is not a claim that their predictions are identical.
    """
    values = np.asarray(daily_values, dtype=float)
    if (values.ndim != 2 or values.shape[1] != len(package_ids) or len(set(package_ids)) != len(package_ids)
            or list(package_ids) != list(expected_packages) or set(source_groups) != set(package_ids)
            or np.isinf(values).any() or any(not isinstance(v, str) or not v for v in source_groups.values())):
        raise ValueError("aggregate must preserve the full declared package axis")
    complete = np.isfinite(values).all(axis=1)
    equal_package = np.full(values.shape[0], np.nan)
    equal_parent = np.full(values.shape[0], np.nan)
    groups = sorted(set(source_groups.values()))
    if complete.any():
        known = values[complete]
        equal_package[complete] = known.mean(axis=1)
        cluster_columns = [known[:, [i for i,p in enumerate(package_ids) if source_groups[p] == group]].mean(axis=1) for group in groups]
        equal_parent[complete] = np.column_stack(cluster_columns).mean(axis=1)
    return dict(equal_package_daily=equal_package, equal_parent_cluster_daily=equal_parent,
        complete_day_count=int(complete.sum()), expected_package_count=len(package_ids), parent_cluster_count=len(groups),
        available_only_renormalization=False, independent_package_count_claimed=False)
