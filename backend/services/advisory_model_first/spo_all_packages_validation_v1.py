"""Zero-fit applicability checks for a frozen SPO+ model on an explicit package inventory."""

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.advisory_model_first import spo_top10_5td_v1 as spo

KEY, GROUP = spo.KEY, spo.GROUP
DAY = spo.base.KEY[0]
FIELDS = ("candidate", "baseline", "candidate_full_model")


def file_sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def checked_file(ref):
    path = Path(ref["path"])
    if (not path.is_absolute() or path.resolve() != path or not path.is_file()
            or path.stat().st_size != ref["size_bytes"] or file_sha(path) != ref["sha256"]):
        raise ValueError("frozen file identity differs")
    return path


def inventory_packages(inventory):
    active = [p for p in inventory["programs"] if p["status"] == "ENABLED"]
    expected = {pid for p in active for pid in p["package_ids"]}
    packages = {p["package_id"]: p for p in inventory["packages"]}
    if (not expected or expected != set(packages) or len(packages) != len(inventory["packages"])
            or expected != set(inventory["enabled_package_ids"])):
        raise ValueError("enabled package inventory differs")
    return packages


def score_roster(source, package, dates, calendar):
    """Project the existing score-only artifact, without executing a package model."""
    if (source["package_id"] != package["package_id"]
            or source["manifest_sha256"] != package["manifest_sha256"]
            or source["run_id"] != package["run_id"]):
        raise ValueError("score/package identity differs")
    if (dates != sorted(set(dates)) or not dates or calendar != sorted(set(calendar))
            or any(d not in calendar for d in dates)):
        raise ValueError("original date axis differs")
    ref = source["file"]
    if not 0 < ref["size_bytes"] <= 128 * 1024**2:
        raise ValueError("score file exceeds bounded source budget")
    path = checked_file(ref)
    # Only approved AIstock-owned files whose exact descriptor was frozen in the protocol.
    frame = pd.read_pickle(path)
    if isinstance(frame, pd.Series):
        frame = frame.to_frame(name="score")
    if (not isinstance(frame, pd.DataFrame) or not isinstance(frame.index, pd.MultiIndex)
            or frame.index.names != ["datetime", "instrument"] or len(frame) > 5000000
            or list(frame.columns) not in [["score"], [0]]):
        raise ValueError("original score-only schema differs")
    frame.columns = ["score"]
    rows = frame.reset_index()
    rows[DAY] = pd.to_datetime(rows.datetime)
    if not rows[DAY].eq(rows[DAY].dt.normalize()).all():
        raise ValueError("score decision clock is not daily")
    rows = rows.loc[rows[DAY].isin(pd.to_datetime(dates)), [DAY, "instrument", "score"]].copy()
    if (rows.duplicated([DAY, "instrument"]).any()
            or not rows.instrument.str.fullmatch(r"\d{6}\.(SH|SZ|BJ)").all()
            or not np.isfinite(rows.score.to_numpy(float)).all()):
        raise ValueError("duplicate or invalid original scores")
    counts = rows.groupby(DAY).size()
    if set(counts.index) != set(pd.to_datetime(dates)) or counts.lt(50).any():
        raise ValueError("missing original date or fewer than fifty scores")
    rows = rows.sort_values([DAY, "score", "instrument"], ascending=[True, False, True], kind="stable")
    rows["selection_effective_rank"] = rows.groupby(DAY).cumcount() + 1
    rows = rows.loc[rows.selection_effective_rank.le(50)].copy()
    positions = {d: i for i, d in enumerate(calendar)}
    if any(positions[d] + 1 >= len(calendar) for d in dates):
        raise ValueError("original next trading session unavailable")
    rows["target_trade_date"] = rows[DAY].map(lambda d: pd.Timestamp(calendar[positions[d.date()] + 1]))
    rows["candidate_group_size"] = 50
    rows = rows.rename(columns={"score": "parent_score"})
    for name in ("package_id", "manifest_sha256", "run_id", "source_id"):
        rows[name] = package[name]
    checked_file(ref)
    return rows.reset_index(drop=True)


def predict_fold(inputs, *, package, models, fold):
    if type(fold) is not int or fold not in (1, 2, 3):
        raise ValueError("unregistered fold")
    cutoff, first, last = spo.base.FOLDS[fold - 1]
    if (inputs.empty or not inputs[DAY].between(pd.Timestamp(first), pd.Timestamp(last)).all()
            or not inputs[DAY].gt(pd.Timestamp(cutoff)).all() or set(models) != {"matched", "candidate"}):
        raise ValueError("prediction outside frozen post-training fold")
    for name in ("package_id", "manifest_sha256", "run_id", "source_id"):
        if not inputs[name].eq(package[name]).all():
            raise ValueError("prediction package identity differs")
    packets = {}
    for arm, ref in models.items():
        packet = json.loads(checked_file(ref).read_bytes())
        if packet["cutoff"] != cutoff or packet["arm"] != arm:
            raise ValueError("model fold or arm differs; final model cannot self-test")
        packets[arm] = packet
    for field in ("fields", "median", "scale", "training_keys_sha256"):
        if packets["matched"][field] != packets["candidate"][field]:
            raise ValueError("matched frozen training population or encoding differs")
    return {arm: spo.predict(inputs, packet) for arm, packet in packets.items()}


def package_summary(panel, dates):
    """Descriptive statistics only. No new selection or production verdict."""
    if (panel.empty or panel.package_id.nunique() != 1 or panel.decision_date.duplicated().any()
            or set(panel.decision_date) != set(pd.to_datetime(dates)) or set(panel.fold) != {1, 2, 3}
            or not panel.original_candidates.eq(50).all()):
        raise ValueError("package panel lost the declared original axis")
    panel = panel.sort_values("decision_date").reset_index(drop=True)
    complete = panel.mature & panel.loc[:, FIELDS].notna().all(axis=1)
    delta = (panel.candidate - panel.baseline).where(complete)
    matched = (panel.candidate - panel.candidate_full_model).where(complete)
    candidate = spo.base._distribution(panel.candidate.where(complete))
    baseline = spo.base._distribution(panel.baseline.where(complete))
    mature = panel.loc[panel.mature]
    return dict(
        status="EXPLORATORY_EVALUATED", original_days=len(panel), mature_days=len(mature),
        common_days=int(complete.sum()), coverage=float(complete.sum() / len(mature)) if len(mature) else None,
        statistics=spo.base.paired_interval(delta.to_numpy()), versus_matched=spo.base.paired_interval(matched.to_numpy()),
        candidate=candidate, baseline=baseline,
        by_fold={str(f): spo.base._distribution(delta.loc[panel.fold.eq(f)]) for f in (1, 2, 3)},
        candidate_unknown_slots=int(panel.candidate_unknown.sum()), baseline_unknown_slots=int(panel.baseline_unknown.sum()),
        mature_unknown_excess=float((mature.candidate_unknown - mature.baseline_unknown).sum() / (10 * len(mature))) if len(mature) else None,
        intervention_days=int((panel.mature & panel.intervention_count.gt(0)).sum()),
        tail_delta_bps=(candidate["lower_tail10_mean_bps"] - baseline["lower_tail10_mean_bps"]) if complete.any() else None,
        independent_oos_evidence=False, activation_evidence=False,
    )


def all_package_report(inventory, panel, unavailable, dates):
    packages = inventory_packages(inventory)
    observed = set(panel.package_id)
    if observed & set(unavailable) or observed | set(unavailable) != set(packages):
        raise ValueError("all enabled packages must be evaluated or explicitly unavailable")
    results = {}
    for pid in sorted(packages):
        if pid in unavailable:
            if not unavailable[pid]:
                raise ValueError("unavailable package needs an explicit reason")
            results[pid] = dict(status="NOT_EVALUABLE", reason=unavailable[pid])
        else:
            results[pid] = package_summary(panel.loc[panel.package_id.eq(pid)], dates)
    return dict(packages=results, inventory_count=len(packages), evaluated_count=len(observed),
                all_packages_economically_evaluated=len(observed) == len(packages),
                full_inventory_mean_bps=None, physical_fit_count=0,
                interpretation="DESCRIPTIVE_APPLICABILITY_NOT_CONFIRMATION", production_ready=False)
