"""A bounded SPO+ decision-loss study on the existing eight input fields."""

import numpy as np
import pandas as pd
from scipy.optimize import minimize, nnls

from backend.services.advisory_model_first import peer_residual_top10_5td_v1 as shared
from backend.services.advisory_model_first.peer_lead_lag_top10_5td_v1 import summarize as native_summary
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY, INPUT_FEATURES

base, evaluation, sha = shared.base, shared.evaluation, shared.sha
KEY, GROUP = shared.KEY, shared.GROUP
validate_population, group_center, panel = shared.validate_population, shared.group_center, shared.panel
FEATURES = INPUT_FEATURES
CONTRACT = dict(
    schema="spo_top10_5td_v1", hypothesis="SPO_PLUS_TOP10_5TD_1",
    objective_contract="ALPHA_RANKING", study_type="EXPLORATORY_SCREEN", decision_use="NAVIGATION_ONLY",
    features=FEATURES, original_topk=50, top_k=10, l2=0.01, label_scale_bps=100.0,
    encoding="TRAIN_MEDIAN_IQR_ELSE_STD_ELSE_ONE_V1",
    solver=dict(max_cuts=512, max_qp_iterations=2000, qp_ftol=1e-9, gap_tolerance=1e-5,
                qp_form="PRIMAL_EPIGRAPH", active_tolerance=1e-7, dual_recovery="ACTIVE_NNLS_SIMPLEX"),
    start="2024-07-04", train_end="2025-09-30", end="2025-09-30", folds=base.FOLDS,
    max_fits=7, planned_candidates=1, prior_fits=237, prior_index=1, max_rows=30500,
    max_rss_bytes=2 * 1024**3, max_seconds=1800, max_output_bytes=200 * 1024**2,
    final_allowed="ONLY_AFTER_INNER_SUPPORTED", outer_allowed=False, sealed_allowed=False,
    valuation_policy_sha256=base.VALUATION_POLICY_SHA256,
    evaluation_contract=base.CONTRACT["worth_confirming"], bootstrap=base.CONTRACT["bootstrap"],
    inner_rule="NATIVE_POSITIVE_TWO_FOLDS_COVERAGE80_INTERVENTION20D_20PCT_TWO_FOLDS_RISK100_UNKNOWN2PCT",
)


def numeric(frame):
    x = frame.loc[:, FEATURES].apply(pd.to_numeric, errors="raise").to_numpy(float)
    if np.isinf(x).any():
        raise ValueError("infinite known feature")
    return x


def support(x):
    return np.isfinite(x).any(axis=1)


def topk_weights(values, k):
    """Stable positional ties; callers supply original-rank row order."""
    values = np.asarray(values, float)
    if values.ndim != 1 or not np.isfinite(values).all() or not 0 < k <= len(values):
        raise ValueError("invalid top-k oracle input")
    weights = np.zeros_like(values)
    weights[np.argsort(-values, kind="stable")[:k]] = 1.0 / k
    return weights


def spo_oracle(x, y, groups, weights, coef, k):
    loss, gradient = 0.0, np.zeros(x.shape[1])
    for indexes, mass in zip(groups, weights, strict=True):
        a, r = x[indexes], y[indexes]
        s = a @ coef
        truth, adversary = topk_weights(r, k), topk_weights(2 * s - r, k)
        value = (2 * s - r) @ adversary - 2 * s @ truth + r @ truth
        if value < -1e-10 or not np.isfinite(value):
            raise ValueError("invalid SPO+ objective")
        loss += mass * max(0.0, value)
        gradient += mass * 2 * a.T @ (adversary - truth)
    return float(loss), gradient


def solve_spo(x, y, groups, weights, *, k=10, budget=lambda: None):
    """Convex cutting planes with an explicit feasible-dual lower bound."""
    x, y, weights = np.asarray(x, float), np.asarray(y, float), np.asarray(weights, float)
    if (x.ndim != 2 or y.shape != (len(x),) or not np.isfinite(x).all()
            or not np.isfinite(y).all() or weights.shape != (len(groups),)
            or (weights <= 0).any() or not np.isclose(weights.sum(), 1.0)):
        raise ValueError("invalid convex training input")
    flat = np.concatenate(groups)
    if not np.array_equal(np.sort(flat), np.arange(len(x))) or any(len(g) < k for g in groups):
        raise ValueError("groups must partition complete supervised rows")
    lam, options = CONTRACT["l2"], CONTRACT["solver"]
    coef, best = np.zeros(x.shape[1]), np.zeros(x.shape[1])
    # The zero plane is valid because SPO+ is nonnegative.
    planes, offsets = [np.zeros(x.shape[1])], [0.0]
    lower, upper, qp_iterations = 0.0, np.inf, 0
    for iteration in range(1, options["max_cuts"] + 1):
        budget()
        loss, grad = spo_oracle(x, y, groups, weights, coef, k)
        objective = loss + lam * np.dot(coef, coef) / 2
        if objective < upper:
            upper, best = objective, coef.copy()
        gap = upper - lower
        if gap < -1e-7 * max(1.0, abs(upper)):
            raise ValueError("dual lower bound exceeds primal upper bound")
        if gap <= options["gap_tolerance"] * max(1.0, abs(upper)):
            return best, dict(upper=float(upper), lower=float(lower), gap=float(max(0.0, gap)),
                              cuts=iteration - 1, qp_iterations=qp_iterations, converged=True)
        planes.append(grad)
        offsets.append(loss - grad @ coef)
        a, b = np.asarray(planes), np.asarray(offsets)
        # The primal has at most 17 variables, independent of the cut count.
        # A simplex-dual QP becomes degenerate as redundant cuts accumulate.
        def objective_primal(v):
            return float(v[-1] + lam * (v[:-1] @ v[:-1]) / 2), np.r_[lam * v[:-1], 1.0]

        initial = np.r_[coef, np.max(a @ coef + b)]
        jacobian = np.column_stack((-a, np.ones(len(a))))
        fitted = minimize(objective_primal, initial, method="SLSQP", jac=True,
                          constraints=[dict(type="ineq", fun=lambda v: v[-1] - a @ v[:-1] - b,
                                            jac=lambda v: jacobian)],
                          options=dict(maxiter=options["max_qp_iterations"], ftol=options["qp_ftol"]))
        qp_iterations += int(fitted.nit)
        if not fitted.success or not np.isfinite(fitted.x).all():
            raise ValueError("SPO+ primal QP did not converge feasibly: " + str(fitted.message))
        coef, height = fitted.x[:-1], fitted.x[-1]
        slack = height - a @ coef - b
        tol = options["active_tolerance"] * max(1.0, abs(height))
        if slack.min() < -tol:
            raise ValueError("SPO+ primal QP is infeasible")
        active = slack <= tol
        # Recover nonnegative active multipliers, then normalize. Even when
        # stationarity is approximate, this is an actual feasible dual point.
        scale = max(1.0, float(np.max(np.abs(a[active]))))
        matrix = np.vstack((a[active].T / scale, np.ones(active.sum())))
        alpha, _ = nnls(matrix, np.r_[-lam * coef / scale, 1.0], maxiter=2000)
        if not np.isfinite(alpha).all() or alpha.sum() <= 0:
            raise ValueError("SPO+ dual certificate recovery failed")
        alpha /= alpha.sum()
        mean = alpha @ a[active]
        lower = max(lower, float(b[active] @ alpha - mean @ mean / (2 * lam)))
    raise ValueError("SPO+ primal-dual certificate not reached within frozen cut budget")


def training(rows, cutoff):
    if pd.Timestamp(cutoff) > pd.Timestamp(CONTRACT["end"]):
        raise ValueError("training cutoff outside inner window")
    rows = rows.loc[rows[base.KEY[0]].between(pd.Timestamp(CONTRACT["start"]), pd.Timestamp(cutoff))].copy()
    validate_population(rows)
    mature = rows.label_information_end.le(pd.Timestamp(cutoff))
    checked = rows.loc[mature].copy()
    base._validate_finance(checked)
    if checked.duplicated(KEY).any():
        raise ValueError("duplicate supervised identity")
    cash = checked.valuation_status.eq("CASH_ENTRY_NOT_EXECUTABLE")
    if not checked.loc[cash, "hypothetical_liquidation_net_bps"].eq(0.0).all():
        raise ValueError("invalid cash valuation")
    if checked.groupby(list(base.KEY))["hypothetical_liquidation_net_bps"].nunique(dropna=False).gt(1).any():
        raise ValueError("contradictory labels across packages")
    good = mature & rows.valuation_status.isin(["AVAILABLE", "CASH_ENTRY_NOT_EXECUTABLE"]) & support(numeric(rows))
    flags = rows.loc[:, GROUP].copy()
    flags["eligible"] = good
    complete = flags.groupby(GROUP, sort=False).eligible.transform("all")
    part = rows.loc[complete].sort_values([*GROUP, "selection_effective_rank"], kind="stable").reset_index(drop=True)
    if len(part) < 100 or part[base.KEY[0]].nunique() < 2:
        raise ValueError("insufficient complete mature training groups")
    if not np.isfinite(part.hypothetical_liquidation_net_bps.to_numpy(float)).all():
        raise ValueError("nonfinite training labels")
    stats = dict(original_train_rows=len(rows), eligible_train_rows=len(part),
                 excluded_train_groups=int((~complete).sum() // 50), train_groups=len(part) // 50)
    return part, stats


def fit(rows, cutoff, arm, *, before_fit=lambda: None, budget=lambda: None):
    if arm not in ("matched", "candidate"):
        raise ValueError("unregistered arm")
    part, stats = training(rows, cutoff)
    raw = numeric(part)
    active = np.isfinite(raw).any(axis=0)
    fields = [name for name, keep in zip(FEATURES, active, strict=True) if keep]
    raw = raw[:, active]
    q = np.nanquantile(raw, [0.25, 0.5, 0.75], axis=0)
    median, spread = q[1], np.nanstd(raw, axis=0)
    scale = np.where(q[2] > q[0], q[2] - q[0], np.where(spread > 0, spread, 1.0))
    x = np.column_stack(((np.where(np.isnan(raw), median, raw) - median) / scale, np.isnan(raw).astype(float)))
    x = group_center(x, part)
    y = group_center(part.hypothetical_liquidation_net_bps.to_numpy(float)[:, None], part).ravel() / CONTRACT["label_scale_bps"]
    groups = list(part.groupby(GROUP, sort=False).indices.values())
    dates = part[base.KEY[0]].nunique()
    counts = part.groupby(base.KEY[0]).package_id.nunique()
    weights = np.asarray([1 / (dates * counts.loc[part.iloc[g[0]][base.KEY[0]]]) for g in groups])
    before_fit()
    if arm == "candidate":
        coef, certificate = solve_spo(x, y, groups, weights, k=CONTRACT["top_k"], budget=budget)
    else:
        row_weights = np.zeros(len(part))
        for g, mass in zip(groups, weights, strict=True):
            row_weights[g] = mass / len(g)
        lhs = x.T @ (row_weights[:, None] * x) + CONTRACT["l2"] * np.eye(x.shape[1])
        rhs = x.T @ (row_weights * y)
        coef = np.linalg.solve(lhs, rhs)
        residual = float(np.max(np.abs(lhs @ coef - rhs)))
        if residual > 1e-8 * max(1.0, float(np.max(np.abs(rhs)))):
            raise ValueError("matched normal equation residual exceeds tolerance")
        certificate = dict(converged=True, normal_equation_residual=residual)
    if not np.isfinite(coef).all():
        raise ValueError("nonfinite model coefficients")
    packet = dict(contract_sha256=sha(CONTRACT), arm=arm, fields=fields, median=median.tolist(),
                  scale=scale.tolist(), coef=coef.tolist(), cutoff=str(cutoff), train_rows=len(part),
                  train_days=int(dates), training_support=stats, certificate=certificate,
                  score_semantics="UNCALIBRATED_DECISION_SCORE_100BPS_SCALE", iterations=[certificate.get("cuts", 1)],
                  inactive_fields=[name for name, keep in zip(FEATURES, active, strict=True) if not keep],
                  training_keys_sha256=sha([[str(v) for v in row] for row in part[KEY].itertuples(index=False, name=None)]))
    packet["model_payload_sha256"] = sha(packet)
    return packet


def predict(inputs, model):
    validate_population(inputs)
    if (model.get("contract_sha256") != sha(CONTRACT) or model.get("arm") not in ("matched", "candidate")
            or model.get("model_payload_sha256") != sha({k: v for k, v in model.items() if k != "model_payload_sha256"})):
        raise ValueError("model identity differs")
    fields = model["fields"]
    if not fields or len(set(fields)) != len(fields) or not set(fields).issubset(FEATURES):
        raise ValueError("model field identity differs")
    median, scale, coef = (np.asarray(model[k], float) for k in ("median", "scale", "coef"))
    if (median.shape != (len(fields),) or scale.shape != median.shape or coef.shape != (2 * len(fields),)
            or not np.isfinite(np.r_[median, scale, coef]).all() or (scale <= 0).any()):
        raise ValueError("invalid numeric model")
    eligible = np.isfinite(inputs.loc[:, fields].to_numpy(float)).any(axis=1)
    numeric(inputs)
    result = inputs.loc[:, [*IDENTITY, "parent_score", "source_id"]].copy().reset_index(drop=True)
    result["decision_score"], result["no_rank_advice"] = np.nan, ~eligible
    result["advisory_rank"] = result.selection_effective_rank.copy()
    chosen = inputs.loc[eligible].copy()
    raw = chosen.loc[:, fields].to_numpy(float)
    x = np.column_stack(((np.where(np.isnan(raw), median, raw) - median) / scale, np.isnan(raw).astype(float)))
    scores = group_center(x, chosen) @ coef if len(chosen) else np.array([])
    if not np.isfinite(scores).all():
        raise ValueError("nonfinite decision score")
    result.loc[eligible, "decision_score"] = scores
    for _, group in result.groupby(GROUP, sort=False):
        active = group.loc[~group.no_rank_advice]
        order = active.sort_values(["decision_score", "selection_effective_rank"], ascending=[False, True], kind="stable").index
        result.loc[order, "advisory_rank"] = sorted(active.selection_effective_rank)
    result["selected"] = result.advisory_rank.le(CONTRACT["top_k"])
    evaluation.same_population(inputs, result)
    return result


def summarize(cohorts):
    if "fold" not in cohorts or set(cohorts.fold) != {1, 2, 3}:
        raise ValueError("all three folds required")
    report, daily = native_summary(cohorts)
    report["supports_future_confirmation_design"] = report["classification"] == "INNER_SUPPORTED"
    report["proceed_outer"] = False
    return report, daily
