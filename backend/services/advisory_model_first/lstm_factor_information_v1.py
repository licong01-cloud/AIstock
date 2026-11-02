"""One fixed parent-factor information experiment; never a production ranker."""
import numpy as np
import pandas as pd

from . import lstm_complementarity_v1 as prior
from . import spo_top10_5td_v1 as spo

DAY, KEY, GROUP = prior.DAY, list(spo.KEY), list(spo.GROUP)
DYNAMIC = (
    "neg_momentum_price_volume", "m_turnover_percentile_250d", "neg_high_amount_turnover_momentum_5d",
    "industry_stock_momentum_diff_10d", "neg_Composite_Factor_Multi_Dim", "m_intraday_range_60d_min_ratio",
    "m_ind_pb_rel_mom", "m_ind_neutral_rev_5d", "m_turnover_abnormal_20d", "m_turnover_accel",
    "m_turnover_zscore_60d", "m_atr_compression", "m_free_turnover_ind_neutral",
    "dynamic_flow_volatility_sentiment", "neg_volatility_breakout_momentum_v2",
    "ChipWinnerRateEliteBuyIntensity", "LargeOrder_Cost_Interaction", "m_ind_rel_turnover",
    "m_turnover_breakout_ratio", "m_volume_contraction", "neg_PriceMomentum20D", "m_free_turnover_rate",
    "Industry_Volatility_Liquidity_Cross_Factor", "m_intraday_range_ratio_5d",
    "neg_TurnoverVolatilityEnhancement", "m_tech_atr_ratio_14d",
)
ALIASES = ("RESI5", "WVMA5", "RSQR5", "KLEN", "RSQR10", "CORR5", "CORD5", "CORR10", "ROC60", "RESI10",
           "VSTD5", "RSQR60", "CORR60", "WVMA60", "STD5", "RSQR20", "CORD60", "CORD10", "CORR20", "KLOW")
EXTRA = tuple("factor__" + f for f in DYNAMIC + ALIASES)
FEATURES = (*spo.FEATURES, *EXTRA)
CONTRACT = dict(schema="lstm_parent_factor_information_v1", hypothesis="LSTM_PARENT_FACTOR_INFORMATION_FIXED5_V1",
    target=prior.LSTM, fields=FEATURES, parent_contract_sha256=spo.sha(spo.CONTRACT), folds=spo.base.FOLDS,
    support="ORIGINAL_EIGHT_FIELDS_UNCHANGED", missing="NAN_AND_DOCUMENTED_NONFINITE_WITH_FLAGS",
    max_new_fits=3, prior_fits=248, prior_index=1, final_fit=False, top_k=10, target_bps=100.,
    development=("2024-07-04", "2025-09-30"), stress=(prior.FIRST, prior.LAST),
    evidence="NON_VINTAGE_RECONSTRUCTED_INPUT_PREVIOUSLY_CONSUMED_DEVELOPMENT",
    independent_confirmation=False, production_ready=False)


def ordered(frame):
    return frame.sort_values([*GROUP, "selection_effective_rank"], kind="stable").reset_index(drop=True)


def check_inputs(frame):
    spo.validate_population(frame)
    if frame.empty or any(not frame[k].eq(v).all() for k, v in prior.LSTM.items()):
        raise ValueError("exact LSTM package identity required")
    allowed = frame[DAY].between(*map(pd.Timestamp, CONTRACT["development"]))
    allowed |= frame[DAY].between(*map(pd.Timestamp, CONTRACT["stress"]))
    if not allowed.all() or not set(FEATURES).issubset(frame.columns):
        raise ValueError("input field or development/stress date differs")
    spo.numeric(frame)  # Old eight fields retain their stricter finite-or-NaN contract.
    for f in EXTRA:
        if frame[f].dtype.kind not in "fiu":
            raise ValueError("factor coordinate must be numeric, excluding bool")
    return frame


def numeric(frame, fields=FEATURES):
    raw = frame.loc[:, fields].to_numpy(dtype=float, copy=True)
    # Only the explicitly reviewed division-by-zero coordinate can contain Inf.
    for j, name in enumerate(fields):
        bad = np.isinf(raw[:, j])
        if bad.any() and name != "factor__neg_momentum_price_volume":
            raise ValueError("unreviewed nonfinite source coordinate")
        raw[bad, j] = np.nan
    return raw


def encoding(raw):
    active = np.isfinite(raw).any(axis=0)
    part = raw[:, active]
    q = np.nanquantile(part, [.25, .5, .75], axis=0)
    spread = np.nanstd(part, axis=0)
    scale = np.where(q[2] > q[0], q[2] - q[0], np.where(spread > 0, spread, 1.))
    return active, q[1], scale


def transform(raw, median, scale):
    missing = np.isnan(raw)
    return np.column_stack(((np.where(missing, median, raw) - median) / scale, missing.astype(float)))


def matched_training(rows, cutoff, old):
    check_inputs(rows)
    part, stats = spo.training(rows, cutoff)
    keys = spo.sha([[str(v) for v in row] for row in part[KEY].itertuples(index=False, name=None)])
    active, median, scale = encoding(spo.numeric(part))
    fields = [f for f, a in zip(spo.FEATURES, active, strict=True) if a]
    if (old.get("cutoff") != cutoff or old.get("arm") != "candidate" or old.get("fields") != fields
            or old.get("training_keys_sha256") != keys or old.get("training_support") != stats
            or old.get("train_rows") != len(part) or old.get("train_days") != part[DAY].nunique()):
        raise ValueError("old eight-field comparator training identity differs")
    np.testing.assert_allclose(median, old["median"], rtol=1e-12, atol=1e-12)
    np.testing.assert_allclose(scale, old["scale"], rtol=1e-12, atol=1e-12)
    # Validate the actual frozen model identity without fitting or reading outcomes.
    spo.predict(part, old)
    return part, stats, keys


def fit(rows, fold, comparator, *, before_fit, budget=lambda: None):
    if type(fold) is not int or fold not in (1, 2, 3):
        raise ValueError("one of the three registered folds required")
    cutoff = spo.base.FOLDS[fold - 1][0]
    if not rows[DAY].le(pd.Timestamp(cutoff)).all():
        raise ValueError("future training rows")
    part, stats, keys = matched_training(rows, cutoff, comparator)
    raw = numeric(part)
    active, median, scale = encoding(raw)
    fields = [f for f, a in zip(FEATURES, active, strict=True) if a]
    x = spo.group_center(transform(raw[:, active], median, scale), part)
    y = spo.group_center(part.hypothetical_liquidation_net_bps.to_numpy(float)[:, None], part).ravel() / 100.
    groups = list(part.groupby(GROUP, sort=False).indices.values())
    weights = np.full(len(groups), 1. / len(groups))
    before_fit()
    coef, certificate = spo.solve_spo(x, y, groups, weights, budget=budget)
    if not np.isfinite(coef).all():
        raise ValueError("nonfinite fitted coefficients")
    packet = dict(schema=CONTRACT["schema"], contract_sha256=spo.sha(CONTRACT), target=prior.LSTM,
        fold=fold, cutoff=cutoff, fields=fields, median=median.tolist(), scale=scale.tolist(), coef=coef.tolist(),
        training_keys_sha256=keys, training_support=stats, train_rows=len(part), train_days=len(groups),
        comparator_sha256=comparator["model_payload_sha256"], certificate=certificate,
        nonfinite_source_cells=int(np.isinf(part.loc[:, EXTRA].to_numpy(float)).sum()),
        inactive_fields=[f for f, a in zip(FEATURES, active, strict=True) if not a], production_ready=False)
    packet["payload_sha256"] = spo.sha(packet)
    return packet


def predict(inputs, packet, *, phase):
    check_inputs(inputs)
    fold = packet.get("fold")
    if type(fold) is not int or fold not in (1, 2, 3):
        raise ValueError("unregistered model fold")
    cutoff, first, last = spo.base.FOLDS[fold - 1]
    if (packet.get("schema") != CONTRACT["schema"] or packet.get("contract_sha256") != spo.sha(CONTRACT)
            or packet.get("payload_sha256") != spo.sha({k: v for k, v in packet.items() if k != "payload_sha256"})
            or packet.get("target") != prior.LSTM or packet.get("cutoff") != cutoff):
        raise ValueError("factor model identity differs")
    if phase == "stress" and fold == 3:
        first, last = CONTRACT["stress"]
    elif phase != "fold":
        raise ValueError("offline registered fold/stress only; no live inference")
    if not inputs[DAY].between(pd.Timestamp(first), pd.Timestamp(last)).all():
        raise ValueError("prediction date outside registered fold")
    fields = packet["fields"]
    if not fields or fields != [f for f in FEATURES if f in fields]:
        raise ValueError("model field order/duplicates differ")
    median, scale, coef = (np.asarray(packet[k], float) for k in ("median", "scale", "coef"))
    if (median.shape != (len(fields),) or scale.shape != median.shape or coef.shape != (2 * len(fields),)
            or not np.isfinite(np.r_[median, scale, coef]).all() or (scale <= 0).any()):
        raise ValueError("invalid model encoding")
    eligible = np.isfinite(spo.numeric(inputs)).any(axis=1)
    result = inputs.loc[:, [*spo.IDENTITY, "parent_score", "source_id"]].copy().reset_index(drop=True)
    result["decision_score"], result["no_rank_advice"] = np.nan, ~eligible
    result["advisory_rank"] = result.selection_effective_rank.copy()
    raw = numeric(inputs.loc[eligible], fields)
    x = spo.group_center(transform(raw, median, scale), inputs.loc[eligible]) if len(raw) else np.empty((0, len(coef)))
    scores = x @ coef
    if not np.isfinite(scores).all():
        raise ValueError("nonfinite decision scores")
    result.loc[eligible, "decision_score"] = scores
    for _, group in result.groupby(GROUP, sort=False):
        active = group.loc[~group.no_rank_advice]
        order = active.sort_values(["decision_score", "selection_effective_rank"], ascending=[False, True], kind="stable").index
        result.loc[order, "advisory_rank"] = sorted(active.selection_effective_rank)
    result["selected"] = result.advisory_rank.le(10)
    spo.evaluation.same_population(inputs, result)
    return result


def summarize(panel, comparator):
    p = panel.sort_values("decision_date").reset_index(drop=True)
    old = comparator.sort_values("decision_date").reset_index(drop=True)
    pd.testing.assert_frame_equal(p[["package_id", "decision_date", "original_candidates", "mature"]],
                                  old[["package_id", "decision_date", "original_candidates", "mature"]])
    np.testing.assert_allclose(p.baseline, old.baseline, equal_nan=True)
    known = p.mature & p.delta.notna()
    auxiliary = known & old.candidate.notna()
    delta = p.delta.where(known)
    candidate, native = (spo.base._distribution(p[k].where(known)) for k in ("candidate", "baseline"))
    mature, interventions = p.mature, p.mature & p.intervention_count.gt(0)
    tail = candidate["lower_tail10_mean_bps"] - native["lower_tail10_mean_bps"] if known.any() else None
    return dict(statistics=spo.base.paired_interval(delta.to_numpy(float)), candidate=candidate, native=native,
        versus_eight_fields=spo.base.paired_interval((p.candidate - old.candidate).where(auxiliary).to_numpy(float)),
        auxiliary_complete_days=int(auxiliary.sum()), original_days=len(p), mature_days=int(mature.sum()),
        complete_days=int(known.sum()), coverage=float(known.sum() / mature.sum()) if mature.any() else 0.,
        intervention_days=int(interventions.sum()), intervention_fraction=float(interventions.sum() / mature.sum()) if mature.any() else 0.,
        candidate_unknown_slots=int(p.candidate_unknown.sum()), native_unknown_slots=int(p.baseline_unknown.sum()),
        unknown_excess=float((p.loc[mature, "candidate_unknown"] - p.loc[mature, "baseline_unknown"]).sum() / (10 * mature.sum())) if mature.any() else None,
        tail_delta_bps=tail, by_quarter={str(q): spo.base._distribution(delta.loc[g.index])
            for q, g in p.groupby(p.decision_date.dt.to_period("Q"))},
        economic_components={k: float(p.loc[known, k].mean()) if known.any() else None
            for k in ("avoided_loss", "added_loss", "added_profit", "missed_profit")})


def decision(development, stress, fold_means):
    records = (development, stress)
    means = [r["statistics"]["mean_bps"] for r in records]
    if any(v is None for v in means):
        action = "INCONCLUSIVE_NO_EXPANSION"
    elif any(v <= 0 for v in means) or any(
            r["tail_delta_bps"] is not None and r["tail_delta_bps"] < -100 or
            r["unknown_excess"] is not None and r["unknown_excess"] > .02 for r in records):
        action = "STOP_THIS_FACTOR_CANDIDATE"
    elif any(v < 100 for v in means):
        action = "BELOW_BUSINESS_TARGET_NO_EXPANSION"
    elif all(r["coverage"] >= .8 and r["intervention_days"] >= 20 and r["intervention_fraction"] >= .2 for r in records) and sum(v is not None and v > 0 for v in fold_means.values()) >= 2:
        action = "DESIGN_INDEPENDENT_CONFIRMATION_ONLY"
    else:
        action = "INCONCLUSIVE_NO_EXPANSION"
    return dict(action=action, target_bps=100., independent_economic_confirmation=False,
                production_ready=False, extra_fit_budget=0, v2_changed=False)
