"""One exact-package training-population change; the original SPO+ solver is reused."""

from datetime import datetime, timezone
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.advisory_model_first import spo_top10_5td_v1 as spo
from backend.services.advisory_model_first.lstm_complementarity_v1 import LSTM
from backend.services.advisory_model_first.prequential_top10_5td_study_v1 import projected_finance

DAY, KEY = spo.base.KEY[0], spo.KEY
CONTRACT = dict(schema="lstm_specialist_spo_v1", hypothesis="LSTM_ONLY_TRAINING_POPULATION_SPO_V1",
                target=LSTM, parent_contract_sha256=spo.sha(spo.CONTRACT), folds=spo.base.FOLDS,
                max_fits=3, prior_fits=244, prior_index=1, input_rows=15250, input_days=305,
                evaluation_rows=6300, evaluation_days=126, final_fit=False, outer=False,
                decision_use="NAVIGATION_ONLY", independent_confirmation=False, production_ready=False)


def check_population(frame):
    spo.validate_population(frame)
    if frame.empty or any(not frame[k].eq(v).all() for k, v in LSTM.items()):
        raise ValueError("exact LSTM package/manifest/run/source required")
    if not frame[DAY].between(pd.Timestamp(spo.CONTRACT["start"]), pd.Timestamp(spo.CONTRACT["end"])).all():
        raise ValueError("outside original development window")


def fold_window(fold):
    if type(fold) is not int or fold not in (1, 2, 3):
        raise ValueError("unregistered fold; no final or fourth fit")
    return spo.base.FOLDS[fold - 1]


def fit_specialist(rows, fold, *, before_fit, budget):
    cutoff, _, _ = fold_window(fold)
    check_population(rows)
    if not rows[DAY].le(pd.Timestamp(cutoff)).all():
        raise ValueError("training rows exceed the frozen fold cutoff")
    model = spo.fit(rows, cutoff, "candidate", before_fit=before_fit, budget=budget)
    packet = dict(schema=CONTRACT["schema"], contract_sha256=spo.sha(CONTRACT), target=LSTM,
                  fold=fold, model=model)
    packet["payload_sha256"] = spo.sha(packet)
    return packet


def predict_specialist(inputs, packet):
    check_population(inputs)
    cutoff, first, last = fold_window(packet.get("fold"))
    if (packet.get("schema") != CONTRACT["schema"] or packet.get("contract_sha256") != spo.sha(CONTRACT)
            or packet.get("target") != LSTM
            or packet.get("payload_sha256") != spo.sha({k: v for k, v in packet.items() if k != "payload_sha256"})
            or packet["model"].get("cutoff") != cutoff or packet["model"].get("arm") != "candidate"):
        raise ValueError("specialist model identity or training clock differs")
    if not inputs[DAY].between(pd.Timestamp(first), pd.Timestamp(last)).all():
        raise ValueError("prediction dates outside the frozen held-out fold")
    return spo.predict(inputs, packet["model"])


def finance_projection(path, first, last, cutoff):
    if (first < spo.CONTRACT["start"] or last > spo.CONTRACT["end"] or first > last
            or cutoff != last):
        raise ValueError("finance projection outside frozen development clock")
    filters = [("package_id", "==", LSTM["package_id"]), (DAY, ">=", pd.Timestamp(first)),
               (DAY, "<=", pd.Timestamp(last))]
    meta = pd.read_parquet(path, columns=[*KEY, "label_information_end"], filters=filters)
    observed = pd.read_parquet(path, filters=filters + [("label_information_end", "<=", pd.Timestamp(cutoff))])
    return projected_finance(meta, observed, cutoff)


class FitClaims:
    """Create-only claims count physical starts and forbid implicit retries."""

    def __init__(self, root, plan_sha256):
        self.root, self.digest = Path(root), plan_sha256

    def write(self, fold, stage_sha256=None):
        fold_window(fold)
        self.root.mkdir(parents=True, exist_ok=True)
        starts = list(self.root.glob("fold*_started.json"))
        for old in starts:
            payload = json.loads(old.read_bytes())
            if payload["plan_sha256"] != self.digest:
                raise ValueError("fit claim belongs to a different plan")
        start = self.root / f"fold{fold}_started.json"
        if stage_sha256 is None:
            if len(starts) >= CONTRACT["max_fits"]:
                raise ValueError("three-fit budget exhausted")
            path = start
        else:
            prior = json.loads(start.read_bytes())
            if prior["fold"] != fold or prior["plan_sha256"] != self.digest or prior["stage_sha256"] is not None:
                raise ValueError("unmatched fit completion")
            path = self.root / f"fold{fold}_finished.json"
        with path.open("x", encoding="utf-8") as stream:
            json.dump(dict(fold=fold, plan_sha256=self.digest, stage_sha256=stage_sha256,
                           at=datetime.now(timezone.utc).isoformat()), stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())


def decision(stats, by_fold, *, coverage, intervention_days, intervention_fraction,
             intervention_folds, tail, unknown_excess):
    means = [by_fold.get(str(i)) for i in (1, 2, 3)]
    mean = stats["mean_bps"]
    risk_failed = (tail is not None and tail < -100) or unknown_excess > .02
    supported = (mean is not None and mean > 0 and all(v is not None for v in means)
                 and sum(v > 0 for v in means) >= 2 and coverage >= .8 and intervention_days >= 20
                 and intervention_fraction >= .2 and intervention_folds >= 2 and tail is not None and not risk_failed)
    if supported:
        return "INNER_SUPPORTED"
    return "INNER_NOT_SUPPORTED" if risk_failed or (mean is not None and mean <= 0) else "INNER_INCONCLUSIVE"


def summarize(cohorts):
    if (set(cohorts.package_id) != {LSTM["package_id"]} or cohorts.decision_date.duplicated().any()
            or set(cohorts.fold) != {1, 2, 3} or not cohorts.original_candidates.eq(50).all()):
        raise ValueError("exact single-package original three-fold cohort required")
    p = cohorts.sort_values("decision_date").reset_index(drop=True)
    for fold, g in p.groupby("fold"):
        _, first, last = fold_window(int(fold))
        if not g.decision_date.between(pd.Timestamp(first), pd.Timestamp(last)).all():
            raise ValueError("cohort fold dates differ")
    fields = ["candidate", "baseline", "candidate_full_model"]
    known = p.mature & p[fields].notna().all(axis=1)
    daily = p[["decision_date", "fold", "mature"]].copy()
    daily[fields] = p[fields].where(known, np.nan)
    daily = daily.rename(columns={"candidate_full_model": "shared"})
    daily["delta"] = daily.candidate - daily.baseline
    daily["versus_shared"] = daily.candidate - daily.shared
    native_stats = spo.base.paired_interval(daily.delta.to_numpy())
    shared_stats = spo.base.paired_interval(daily.versus_shared.to_numpy())
    dist = {name: spo.base._distribution(daily[name]) for name in ("candidate", "baseline", "shared")}
    tail = dist["candidate"]["lower_tail10_mean_bps"] - dist["baseline"]["lower_tail10_mean_bps"] if known.any() else None
    mature = p.loc[p.mature]
    excess = float((mature.candidate_unknown - mature.baseline_unknown).sum() / (10 * len(mature))) if len(mature) else 0.
    interventions = p.mature & p.intervention_count.gt(0)
    shared_interventions = p.mature & p.matched_intervention_count.gt(0)
    coverage = float(known.sum() / len(mature)) if len(mature) else 0.
    fraction = float(interventions.sum() / len(mature)) if len(mature) else 0.
    def fold_means(metric):
        return {str(int(f)): float(g[metric].mean()) if g[metric].notna().any() else None
                for f, g in daily.groupby("fold")}
    by_fold, versus_fold = fold_means("delta"), fold_means("versus_shared")
    state = decision(native_stats, by_fold, coverage=coverage, intervention_days=int(interventions.sum()),
                     intervention_fraction=fraction, intervention_folds=p.loc[interventions, "fold"].nunique(),
                     tail=tail, unknown_excess=excess)
    shared_mean = shared_stats["mean_bps"]
    mechanism = "SPECIALIZATION_INCONCLUSIVE"
    if shared_mean is not None and shared_mean <= 0:
        mechanism = "SPECIALIZATION_NOT_SUPPORTED"
    elif shared_mean is not None and all(v is not None for v in versus_fold.values()) and sum(v > 0 for v in versus_fold.values()) >= 2:
        mechanism = "SPECIALIZATION_SUPPORTED_DESCRIPTIVE"
    report = dict(classification=state, specialization=mechanism, statistics=native_stats, versus_shared=shared_stats,
                  **dist, by_fold=by_fold, versus_shared_by_fold=versus_fold, original_days=len(p), mature_days=len(mature),
                  common_days=int(known.sum()), coverage=coverage, tail_delta_bps=tail, unknown_excess=excess,
                  intervention_days=int(interventions.sum()), intervention_fraction=fraction,
                  shared_intervention_days=int(shared_interventions.sum()),
                  candidate_unknown_slots=int(p.candidate_unknown.sum()), baseline_unknown_slots=int(p.baseline_unknown.sum()),
                  shared_unknown_slots=int(p.shared_unknown.sum()), comparison_population="THREE_ARMS_COMMON_D_SINGLE_EXACT_PACKAGE",
                  pairwise_native=spo.base.paired_interval(p.delta.where(p.mature).to_numpy()),
                  by_quarter={str(q): spo.base._distribution(g.delta) for q, g in daily.groupby(daily.decision_date.dt.to_period("Q"))},
                  economic_components={name: float(p.loc[known, name].mean()) if known.any() else None
                                       for name in ("avoided_loss", "added_loss", "added_profit", "missed_profit")},
                  next_step="DESIGN_FINAL_AND_NEW_PROSPECTIVE_ONLY" if state == "INNER_SUPPORTED" and mechanism == "SPECIALIZATION_SUPPORTED_DESCRIPTIVE"
                  else "STOP_EXACT_SPECIALIST_NO_REFIT", independent_oos_evidence=False, production_ready=False,
                  final_model_fitted=False, decision_use="NAVIGATION_ONLY")
    return report, daily
