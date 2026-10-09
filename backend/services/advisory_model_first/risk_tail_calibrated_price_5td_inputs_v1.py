"""Project dates and columns before financial decoding; never consume parent scores."""
import json

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from backend.services.advisory_model_first.economic_entry_pipeline import read_stage
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY
from backend.services.advisory_model_first.generic_price_5td_models_v1 import _support, feature_values
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_contracts_v1 import (
    CALIBRATION, MATRIX_ORDER, ROSTER_KEY, node_path,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

META = (*ROSTER_KEY, "selection_effective_rank", "candidate_group_size")
DAY_META = ("package_id", "decision_date", "target_date", "horizon_end", "original_candidates", "source_status")
COMMON_FINANCE = (*FEATURES, "observed_gap_bps", "valuation_status", "valuation_policy_sha256", "label_information_end")
BASE_FINANCE = (*COMMON_FINANCE, "valuation_gross_terminal_ratio", "valuation_path_min_ratio")
CAL_FINANCE = (*COMMON_FINANCE, "valuation_path_min_ratio")
EVAL_FINANCE = (*BASE_FINANCE, "hypothetical_liquidation_net_bps", "mark_to_market_net_bps", "label_status", "exit_execution_status")


def checked_source(plan, source_prepared=None):
    path = node_path(source_prepared or plan.source_prepared.artifact_uri)
    if path != node_path(plan.source_prepared.artifact_uri):
        raise ValueError("source URI differs from the immutable plan")
    body = json.loads((path/"manifest.json").read_bytes())
    header = read_stage(path, stage="prepared", plan_sha256=plan.source_prepared.plan_sha256,
        parent_sha256=body.get("parent_sha256"))
    if header["stage_sha256"] != plan.source_prepared.stage_sha256:
        raise ValueError("original prepared identity changed")
    return path, header


def sessions_v1(values):
    dates = pd.DatetimeIndex(pd.to_datetime(values))
    if (len(dates) < 10 or dates.hasnans or dates.tz is not None or not dates.equals(dates.normalize())
            or not dates.is_unique or not dates.is_monotonic_increasing):
        raise ValueError("original calendar requires sorted unique naive market sessions")
    return dates


def clocks_v1(plan, calendar):
    sessions = sessions_v1(calendar)
    train = [d for i, d in enumerate(sessions) if pd.Timestamp(plan.train_start) <= d <= pd.Timestamp(plan.train_end)
        and i+5 < len(sessions) and sessions[i+5] <= pd.Timestamp(plan.train_end)]
    cal = train[-CALIBRATION["days"]:]
    if len(cal) != 60:
        raise ValueError("original calendar cannot identify sixty mature training days")
    base = [d for d in train if sessions[sessions.get_loc(d)+5] < cal[0]]
    if len(base) < 2:
        raise ValueError("original calendar cannot identify both honest base pools")
    first = base[len(base)//2]
    structure = [d for d in base if d < first and sessions[sessions.get_loc(d)+5] < first]
    estimation = [d for d in base if d >= first]
    return dict(train_start=str(plan.train_start), train_end=sessions[sessions.get_loc(cal[0])-1].date().isoformat(),
        estimation_first_D=first.date().isoformat(), calibration_first_D=cal[0].date().isoformat(),
        calibration_train_end=str(plan.train_end), evaluation_start=str(plan.evaluation_start), evaluation_end=str(plan.evaluation_end),
        base_dates=[d.date().isoformat() for d in base], structure_dates=[d.date().isoformat() for d in structure],
        estimation_dates=[d.date().isoformat() for d in estimation], calibration_dates=[d.date().isoformat() for d in cal],
        calendar=[d.date().isoformat() for d in sessions])


def cluster_v1(d, t, instrument):
    return sha(dict(D=pd.Timestamp(d).date().isoformat(), T=pd.Timestamp(t).date().isoformat(),
        instrument=instrument, valuation_policy_sha256=VALUATION_POLICY_SHA256))


def validate_roster_v1(rows, days, plan, calendar):
    sessions = sessions_v1(calendar)
    dates = sessions[(sessions >= pd.Timestamp(plan.train_start)) & (sessions <= pd.Timestamp(plan.evaluation_end))]
    if (len(rows) > 100000 or rows.duplicated(list(ROSTER_KEY)).any()
            or rows.duplicated(["package_id", *KEY]).any() or days.duplicated(["package_id", "decision_date"]).any()
            or set(days.package_id) != {s.package_id for s in plan.sources}):
        raise ValueError("original roster keys/day population differ")
    for source in plan.sources:
        schedule = days.loc[days.package_id.eq(source.package_id)]
        if schedule.decision_date.tolist() != list(dates):
            raise ValueError("an original package/day was removed or reordered")
        group = rows.loc[rows.package_id.eq(source.package_id)]
        if not group.manifest_sha256.eq(source.manifest_sha256).all() or not group.run_id.eq(source.run_id).all():
            raise ValueError("original package manifest/run changed")
        for day in schedule.to_dict("records"):
            d = day["decision_date"]
            i = sessions.get_loc(d)
            original = group.loc[group[KEY[0]].eq(d)]
            h = sessions[i+5] if i+5 < len(sessions) else pd.NaT
            if (i+1 >= len(sessions) or day["target_date"] != sessions[i+1]
                    or not (day["horizon_end"] == h or pd.isna(day["horizon_end"]) and pd.isna(h))
                    or len(original) != day["original_candidates"] or len(original) > 50
                    or original.selection_effective_rank.tolist() != list(range(1, len(original)+1))
                    or not original.candidate_group_size.eq(len(original)).all()
                    or not original[KEY[1]].eq(day["target_date"]).all()):
                raise ValueError("original rank/candidate count/D/T/H identity differs")
    if not rows.package_id.isin([s.package_id for s in plan.sources]).all() or not rows[KEY[0]].isin(dates).all():
        raise ValueError("foreign source/date entered the original population")


def read_projection_v1(*, plan, dates, columns, top5=False):
    source, _ = checked_source(plan)
    # Predicate and explicit columns are applied by Arrow, before Pandas/number parsing.
    dates = [pd.Timestamp(d) for d in dates]
    predicate = ds.field(KEY[0]).isin(dates)
    if top5:
        predicate &= ds.field("selection_effective_rank") <= 5
    frame = ds.dataset(source/"rows.parquet", format="parquet").to_table(
        columns=list(dict.fromkeys((*META, *columns))), filter=predicate).to_pandas()
    checked_source(plan)
    return frame


def validate_finance_v1(rows, calendar, columns):
    sessions = sessions_v1(calendar)
    if not set(columns).issubset(rows) or not rows.valuation_policy_sha256.eq(VALUATION_POLICY_SHA256).all():
        raise ValueError("projected finance/valuation policy differs")
    expected = [sessions[sessions.get_loc(d)+5] if sessions.get_loc(d)+5 < len(sessions) else pd.NaT for d in rows[KEY[0]]]
    actual = pd.to_datetime(rows.label_information_end)
    if not all(a == b or pd.isna(a) and pd.isna(b) for a, b in zip(actual, expected, strict=True)):
        raise ValueError("actual label end differs from the original fixed five sessions")
    if rows.groupby(list(KEY))[list(columns)].nunique(dropna=False).gt(1).any().any():
        raise ValueError("same stock/date has contradictory financial observations")
    known = rows.loc[rows.valuation_status.eq("AVAILABLE")]
    names = [n for n in ("valuation_gross_terminal_ratio", "valuation_path_min_ratio") if n in columns]
    targets = known[names].map(lambda v: _number(v, positive=True))
    gaps = known.observed_gap_bps.map(_number)
    if (targets.isna().any().any() or gaps.isna().any() or gaps.le(-10000).any()
            or len(names) == 2 and targets[names[1]].gt(targets[names[0]]).any()):
        raise ValueError("AVAILABLE valuation has invalid price/path coordinates")


def prepare_risk_tail_inputs_v1(*, plan, source_prepared=None):
    source, header = checked_source(plan, source_prepared)
    calendar = json.loads((source/"calendar.json").read_bytes())
    clocks = clocks_v1(plan, calendar)
    sessions = sessions_v1(calendar)
    dates = sessions[(sessions >= pd.Timestamp(plan.train_start)) & (sessions <= pd.Timestamp(plan.evaluation_end))]
    roster = read_projection_v1(plan=plan, dates=dates, columns=())
    days = ds.dataset(source/"days.parquet", format="parquet").to_table(columns=list(DAY_META)).to_pandas()
    validate_roster_v1(roster, days, plan, calendar)
    roster["label_cluster"] = [cluster_v1(*v) for v in roster[list(KEY)].itertuples(index=False, name=None)]
    roster["role"] = "NOT_TRAIN_SUPERVISION"
    for role, dates in (("STRUCTURE", clocks["structure_dates"]), ("ESTIMATION", clocks["estimation_dates"]),
                        ("CALIBRATION", clocks["calibration_dates"])):
        roster.loc[roster[KEY[0]].isin(pd.to_datetime(dates)), "role"] = role
    cal = roster.loc[roster.role.eq("CALIBRATION") & roster.selection_effective_rank.le(5)].copy()
    day_clusters = cal.groupby(KEY[0]).label_cluster.transform("nunique")
    multiplicity = cal.groupby("label_cluster").label_cluster.transform("size")
    cal["original_weight"] = 1/(60*day_clusters*multiplicity)
    incomplete = days.loc[~days.source_status.eq("KNOWN_ROSTER"), "decision_date"]
    cal.loc[cal[KEY[0]].isin(incomplete), "original_weight"] = np.nan
    base = read_projection_v1(plan=plan, dates=clocks["base_dates"], columns=BASE_FINANCE)
    validate_finance_v1(base, calendar, BASE_FINANCE)
    base = base.merge(roster[[*ROSTER_KEY, "label_cluster", "role"]], on=list(ROSTER_KEY), validate="one_to_one")
    domain = base.drop_duplicates("label_cluster")
    values, _ = feature_values(domain)
    medians = [float(np.median(v[~np.isnan(v)])) if (~np.isnan(v)).any() else 0. for v in values.T]
    support = _support(domain)
    _, stock = feature_values(base)
    eligible = stock & base.valuation_status.eq("AVAILABLE") & base.observed_gap_bps.map(
        lambda v: _number(v) is not None and support.contains(float(v)))
    base["pool"] = np.where(eligible & base.role.isin(["STRUCTURE", "ESTIMATION"]), base.role, "NOT_TRAIN_SUPERVISION")
    selected = base.pool.isin(["STRUCTURE", "ESTIMATION"])
    base["cluster_mass"] = 0.
    base.loc[selected, "cluster_mass"] = 1/base.loc[selected].groupby("label_cluster").label_cluster.transform("size")
    encoding = dict(**clocks, matrix_order=list(MATRIX_ORDER), medians=medians, intervals_bps=list(support.intervals_bps),
        contexts={s.package_id: dict(manifest_sha256=s.manifest_sha256, run_id=s.run_id, context_bit=s.context_bit) for s in plan.sources},
        pools={role: int(base.pool.eq(role).sum()) for role in ("STRUCTURE", "ESTIMATION")},
        source_ref=plan.source_prepared.model_dump(mode="json"), original_roster_sha256=sha(roster.astype(str).to_dict("records")),
        calibration_original_rows=len(cal),
        calibration_roster_sha256=sha(cal.astype(str).to_dict("records")))
    checked_source(plan)
    return dict(roster=roster, days=days, base=base.loc[selected].copy(), calibration_roster=cal,
        encoding=encoding, calendar=calendar, source_stage_sha256=header["stage_sha256"])
