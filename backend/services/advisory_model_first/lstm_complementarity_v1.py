"""Frozen parent/reranker diagnostics and one explicitly scoped LSTM stress test."""

import numpy as np
import pandas as pd

from backend.services.advisory_model_first import spo_top10_5td_v1 as spo
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import FINANCE

DAY, KEY, GROUP = spo.base.KEY[0], spo.KEY, spo.GROUP
LSTM = dict(package_id="pkg_cc7eccb6202e4816b9d7c7d6b748bd37",
            manifest_sha256="93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6",
            run_id="qe_20261007_141342_0221_L4", source_id="qe_20261007_141342_0221")
FIRST, LAST, CUTOFF = "2026-03-11", "2026-08-28", "2025-09-30"
METRICS = ("parent_score_correlation", "parent_rank_ic", "rerank_rank_ic", "partial_rank_ic",
           "rank_error_correlation", "stock_parent_contribution_correlation")


def correlation(a, b):
    a, b = np.asarray(a, float), np.asarray(b, float)
    if a.shape != b.shape or a.ndim != 1 or len(a) < 2 or not np.isfinite(np.r_[a, b]).all():
        return None
    a, b = a - a.mean(), b - b.mean()
    scale = np.linalg.norm(a) * np.linalg.norm(b)
    return float(np.clip(a @ b / scale, -1., 1.)) if scale > 1e-12 else None


def ranks(a):
    return pd.Series(np.asarray(a, float)).rank(method="average").to_numpy()


def partial_rank_correlation(parent, score, outcome):
    """Two descriptive projections; their coefficients NEVER enter predictions."""
    p, s, y = (np.asarray(a, float) for a in (parent, score, outcome))
    if (p.ndim != 1 or p.shape != s.shape or p.shape != y.shape or len(p) < 3
            or not np.isfinite(np.r_[p, s, y]).all()):
        return dict(value=None, status="UNKNOWN_INPUT", projections=0)
    p, s, y = (ranks(a) for a in (p, s, y))
    p, s, y = p - p.mean(), s - s.mean(), y - y.mean()
    denominator = p @ p
    if denominator <= 1e-12:
        return dict(value=None, status="DEGENERATE_PARENT", projections=0)
    sr, yr = s - p * ((p @ s) / denominator), y - p * ((p @ y) / denominator)
    if np.linalg.norm(sr) <= 1e-10 * max(1., np.linalg.norm(s)):
        return dict(value=None, status="NO_REMAINING_RANK_VARIATION", projections=2)
    if np.linalg.norm(yr) <= 1e-10 * max(1., np.linalg.norm(y)):
        return dict(value=None, status="NO_REMAINING_OUTCOME_VARIATION", projections=2)
    return dict(value=correlation(sr, yr), status="AVAILABLE", projections=2)


def score_components(inputs, predictions, model):
    """Decompose the existing score, without changing a coefficient or policy."""
    spo.validate_population(inputs)
    spo.evaluation.same_population(inputs, predictions)
    if (model.get("contract_sha256") != spo.sha(spo.CONTRACT)
            or model.get("model_payload_sha256") != spo.sha({k: v for k, v in model.items() if k != "model_payload_sha256"})
            or model["fields"] != list(spo.FEATURES)):
        raise ValueError("frozen SPO model identity/fields differ")
    raw = spo.numeric(inputs)
    eligible = np.isfinite(raw).any(axis=1)
    median, scale, coef = (np.asarray(model[k], float) for k in ("median", "scale", "coef"))
    if (median.shape != (8,) or scale.shape != (8,) or coef.shape != (16,)
            or not np.isfinite(np.r_[median, scale, coef]).all() or (scale <= 0).any()):
        raise ValueError("invalid frozen encoding")
    x = np.column_stack(((np.where(np.isnan(raw[eligible]), median, raw[eligible]) - median) / scale,
                         np.isnan(raw[eligible]).astype(float)))
    if len(x):
        x = spo.group_center(x, inputs.loc[eligible])
    stock, parent = [0, 1, 2, 3, 4, 5, 8, 9, 10, 11, 12, 13], [6, 7, 14, 15]
    result = inputs.loc[:, KEY].copy().reset_index(drop=True)
    result["stock_component"], result["parent_component"] = np.nan, np.nan
    result.loc[eligible, "stock_component"] = x[:, stock] @ coef[stock]
    result.loc[eligible, "parent_component"] = x[:, parent] @ coef[parent]
    if (not predictions.no_rank_advice.eq(~eligible).all()
            or not np.allclose(result.stock_component + result.parent_component,
                               predictions.decision_score, rtol=1e-11, atol=1e-10, equal_nan=True)):
        raise ValueError("components do not reconstruct the frozen prediction")
    return result


def relation_panel(inputs, predictions, finance, model):
    components = score_components(inputs, predictions, model)
    # Existing valuation contract checks exact population, policy, costs and unknowns.
    economics = spo.base.cohort(predictions, finance)
    joined = predictions.merge(finance.loc[:, [*KEY, *FINANCE]], on=KEY, validate="one_to_one")
    joined = joined.merge(components, on=KEY, validate="one_to_one")
    rows = []
    for (pid, d), g in joined.groupby(GROUP, sort=True):
        score_known = np.isfinite(g.decision_score.to_numpy(float)).all()
        known = (g.valuation_status.isin(["AVAILABLE", "CASH_ENTRY_NOT_EXECUTABLE"])
                 & g.hypothetical_liquidation_net_bps.notna())
        row = dict(package_id=pid, decision_date=d, original_candidates=len(g),
                   score_rows=int(g.decision_score.notna().sum()), known_finance_rows=int(known.sum()),
                   relation_status="INCOMPLETE_FINANCE", statistical_projections=0,
                   **{k: None for k in METRICS})
        p = g.parent_score.to_numpy(float)
        s = g.decision_score.to_numpy(float)
        if score_known:
            row.update(parent_score_correlation=correlation(ranks(p), ranks(s)),
                       stock_component_std=float(g.stock_component.std(ddof=0)),
                       parent_component_std=float(g.parent_component.std(ddof=0)),
                       stock_parent_contribution_correlation=correlation(g.stock_component, g.parent_component))
        else:
            row["relation_status"] = "INCOMPLETE_SCORE"
        if score_known and known.all():
            y = g.hypothetical_liquidation_net_bps.to_numpy(float)
            pr, sr, yr = ranks(p), ranks(s), ranks(y)
            partial = partial_rank_correlation(p, s, y)
            row.update(parent_rank_ic=correlation(pr, yr), rerank_rank_ic=correlation(sr, yr),
                       partial_rank_ic=partial["value"], relation_status=partial["status"],
                       statistical_projections=partial["projections"],
                       rank_error_correlation=correlation((pr - yr) / 50, (sr - yr) / 50))
        rows.append(row)
    return economics.merge(pd.DataFrame(rows).drop(columns="original_candidates"),
                           on=["package_id", "decision_date"], validate="one_to_one")


def correlation_summary(panel):
    if panel.duplicated(["package_id", "decision_date"]).any() or not set(panel.fold).issubset({1, 2, 3}):
        raise ValueError("relation axis or fold differs")
    result = {}
    for pid, g in panel.groupby("package_id", sort=True):
        g = g.sort_values("decision_date")
        item = dict(original_days=len(g), complete_finance_days=int(g.known_finance_rows.eq(50).sum()),
                    relation_statuses={str(k): int(v) for k, v in g.relation_status.value_counts().items()},
                    statistical_projections=int(g.statistical_projections.sum()), metrics={}, by_fold={})
        for metric in METRICS:
            stats = spo.base.paired_interval(g[metric].to_numpy(float))
            item["metrics"][metric] = dict(mean=stats["mean_bps"], ci95=stats["ci95_bps"],
                                           available_days=stats["paired_days"], declared_days=stats["declared_days"])
        for fold, part in g.groupby("fold"):
            item["by_fold"][str(int(fold))] = {
                metric: float(part[metric].mean()) if part[metric].notna().any() else None for metric in METRICS}
        result[pid] = item
    return dict(packages=result, interval_use="DESCRIPTIVE_NOT_SELECTION_ADJUSTED", model_fits=0,
                statistical_projection_count=int(panel.statistical_projections.sum()), architecture_causality_proven=False)


def predict_lstm(inputs, model, dates):
    spo.validate_population(inputs)
    days = list(pd.to_datetime(dates))
    if (not days or days != sorted(set(days)) or days[0] != pd.Timestamp(FIRST) or days[-1] != pd.Timestamp(LAST)
            or set(inputs[DAY]) != set(days) or not inputs[DAY].between(pd.Timestamp(FIRST), pd.Timestamp(LAST)).all()):
        raise ValueError("LSTM stress target original date axis differs")
    if any(not inputs[name].eq(value).all() for name, value in LSTM.items()):
        raise ValueError("LSTM package/manifest/run identity differs")
    if model.get("cutoff") != CUTOFF or model.get("arm") != "candidate":
        raise ValueError("frozen final candidate clock/arm differs")
    return spo.predict(inputs, model)


def stress_report(predictions, finance, dates):
    if any(not predictions[name].eq(value).all() for name, value in LSTM.items()):
        raise ValueError("stress result includes another package identity")
    observed = finance.valuation_status.ne("IMMATURE")
    if (finance.loc[observed, "label_information_end"].isna().any()
            or finance.loc[observed, "label_information_end"].gt(pd.Timestamp(LAST)).any()):
        raise ValueError("future or unbounded target finance")
    panel = spo.base.cohort(predictions, finance).sort_values("decision_date").reset_index(drop=True)
    if (list(panel.decision_date) != list(pd.to_datetime(dates)) or not panel.original_candidates.eq(50).all()
            or not panel.decision_date.between(pd.Timestamp(FIRST), pd.Timestamp(LAST)).all()):
        raise ValueError("stress report changed the original date axis")
    known = panel.mature & panel.candidate.notna() & panel.baseline.notna()
    delta = panel.delta.where(known)
    stats = spo.base.paired_interval(delta.to_numpy())
    candidate = spo.base._distribution(panel.candidate.where(known))
    baseline = spo.base._distribution(panel.baseline.where(known))
    tail = candidate["lower_tail10_mean_bps"] - baseline["lower_tail10_mean_bps"] if known.any() else None
    mature = panel.loc[panel.mature]
    excess = float((mature.candidate_unknown - mature.baseline_unknown).sum() / (10 * len(mature))) if len(mature) else None
    interventions = panel.mature & panel.intervention_count.gt(0)
    blocks = len(set(panel.loc[interventions, "decision_date"].dt.to_period("Q")))
    coverage = float(known.sum() / len(mature)) if len(mature) else 0.
    fraction = float(interventions.sum() / len(mature)) if len(mature) else 0.
    prior_class = spo.base.classify(stats, tail_delta=tail, intervention_days=int(interventions.sum()),
                                   intervention_fraction=fraction, intervention_blocks=blocks, coverage=coverage,
                                   unknown_excess=excess if excess is not None else 1.)
    if prior_class == "WORTH_INDEPENDENT_CONFIRMATION":
        state = "STRESS_SUPPORTS_LSTM_PROSPECTIVE"
    elif prior_class == "NOT_WORTH_CONTINUING_THIS_CANDIDATE" or (excess is not None and excess > .02):
        state = "STRESS_NOT_SUPPORTED_CURRENT_PAIR"
    else:
        state = "STRESS_INCONCLUSIVE"
    return dict(classification=state, statistics=stats, candidate=candidate, baseline=baseline,
                original_days=len(panel), mature_days=len(mature), common_days=int(known.sum()), coverage=coverage,
                tail_delta_bps=tail, unknown_excess=excess, intervention_days=int(interventions.sum()),
                intervention_fraction=fraction, intervention_quarters=blocks,
                candidate_unknown_slots=int(panel.candidate_unknown.sum()), baseline_unknown_slots=int(panel.baseline_unknown.sum()),
                by_quarter={str(q): spo.base._distribution(delta.loc[g.index])
                            for q, g in panel.groupby(panel.decision_date.dt.to_period("Q"))},
                interpretation="PREVIOUSLY_CONSUMED_DEVELOPMENT_LSTM_STRESS_ONLY", model_fits=0,
                independent_oos_evidence=False, production_ready=False), panel
