"""Identity/D projection first, calendar-purged original Top5 finance second."""
import json
import math

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from backend.services.advisory_model_first.economic_entry_pipeline import read_stage
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_models_v1 import _support, feature_values
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_contracts_v1 import (
    FEATURES, KEY, POLICY, ROSTER_KEY, VALUATION_POLICY_SHA256, node_path, sha,
)
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_inputs_v1 import sessions_v1

META = (*ROSTER_KEY, "selection_effective_rank", "candidate_group_size")
DAY_META = ("package_id", "decision_date", "target_date", "horizon_end", "original_candidates", "source_status")
TRAIN_FINANCE = ("observed_gap_bps", "valuation_status", "valuation_policy_sha256",
    "label_information_end", "valuation_gross_terminal_ratio")
EVAL_FINANCE = (*TRAIN_FINANCE, "valuation_path_min_ratio", "hypothetical_liquidation_net_bps",
    "mark_to_market_net_bps", "label_status", "exit_execution_status")


def checked_source(plan):
    path = node_path(plan.source_prepared.artifact_uri)
    header = read_stage(path, stage="prepared", plan_sha256=plan.source_prepared.plan_sha256,
        parent_sha256=plan.source_prepared.parent_sha256)
    if (header["stage_sha256"] != plan.source_prepared.stage_sha256
            or any(header["files"].get(n) != ref for n, ref in plan.source_prepared.files.items())):
        raise ValueError("original prepared stage/file identity differs")
    return path, header


def clocks_v1(plan, calendar):
    sessions = sessions_v1(calendar)
    def eligible(start, end, maturity):
        return [d for i, d in enumerate(sessions) if pd.Timestamp(start) <= d <= pd.Timestamp(end)
            and i+maturity < len(sessions) and sessions[i+maturity] <= pd.Timestamp(end)]
    return dict(calendar=[d.date().isoformat() for d in sessions], train_start=str(plan.train_start), train_end=str(plan.train_end),
        evaluation_start=str(plan.evaluation_start), evaluation_end=str(plan.evaluation_end),
        train_dates=[d.date().isoformat() for d in eligible(plan.train_start, plan.train_end, 5)],
        evaluation_dates=[d.date().isoformat() for d in sessions
            if pd.Timestamp(plan.evaluation_start) <= d <= pd.Timestamp(plan.evaluation_end)],
        mature_evaluation_dates=[d.date().isoformat() for d in eligible(plan.evaluation_start, plan.evaluation_end, 5)],
        gap_evaluation_dates=[d.date().isoformat() for d in eligible(plan.evaluation_start, plan.evaluation_end, 1)])


def cluster_v1(d, t, instrument):
    return sha(dict(D=pd.Timestamp(d).date().isoformat(), T=pd.Timestamp(t).date().isoformat(),
        instrument=instrument, valuation_policy_sha256=VALUATION_POLICY_SHA256))


def validate_roster_v1(rows, days, plan, calendar):
    # This helper verifies only keys/ranks/clocks, never a finance or parent score.
    from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_inputs_v1 import validate_roster_v1 as original_roster
    if not rows.columns.is_unique or not days.columns.is_unique or len(rows) > 30500:
        raise ValueError("original roster schema or bounded population differs")
    original_roster(rows, days, plan, calendar)
    if rows[list(ROSTER_KEY)].isna().any().any() or (not rows.empty and not rows.instrument.map(
            lambda v: isinstance(v, str) and bool(v.strip())).all()):
        raise ValueError("original identity cannot be unknown")


def read_projection_v1(*, plan, dates, columns, top5=False, forecast_stage=None):
    allowed = set(FEATURES) | set(EVAL_FINANCE)
    if not set(columns).issubset(allowed):
        raise ValueError("only explicit D/valuation fields may be decoded; no parent score")
    source, _ = checked_source(plan)
    calendar = json.loads((source/"calendar.json").read_bytes())
    clocks = clocks_v1(plan, calendar)
    requested = {pd.Timestamp(d) for d in dates}
    development = {pd.Timestamp(d) for d in calendar
        if pd.Timestamp(plan.train_start) <= pd.Timestamp(d) <= pd.Timestamp(plan.evaluation_end)}
    if not requested.issubset(development):
        raise ValueError("projection cannot decode outside development/sealed dates")
    finance = set(columns).intersection(EVAL_FINANCE)
    if finance:
        if not top5:
            raise ValueError("financial projections are only the original Top5")
        train = set(pd.to_datetime(clocks["train_dates"]))
        evaluation = set(pd.to_datetime(clocks["mature_evaluation_dates"]))
        allowable = train | evaluation
        if finance == {"observed_gap_bps"}:
            allowable |= set(pd.to_datetime(clocks["gap_evaluation_dates"]))
        if not requested.issubset(allowable):
            raise ValueError("projection cannot decode an immature financial/price clock")
        if requested.intersection(evaluation) and finance != {"observed_gap_bps"}:
            if forecast_stage is None:
                raise ValueError("evaluation financial projection requires frozen forecasts")
            path = node_path(forecast_stage)
            if path.name != "trained" or path.parent.name != "forecasts" or path.parent.parent.name != plan.experiment_id:
                raise ValueError("forecast must belong to the same original-slot study")
            root = path.parent.parent
            prior = read_stage(root/"preregistered", stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
            prepared = read_stage(root/"prepared", stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=prior["stage_sha256"])
            trained = read_stage(root/"trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
            read_stage(path, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"])
            freeze = json.loads((path/"freeze.json").read_bytes())
            if freeze.get("evaluation_finance_consumed") is not False or freeze.get("frozen_before_financial_projection") is not True:
                raise ValueError("evaluation forecasts were not frozen before outcomes")
    predicate = ds.field(KEY[0]).isin(list(requested))
    if top5:
        predicate &= ds.field("selection_effective_rank") <= 5
    frame = ds.dataset(source/"rows.parquet", format="parquet").to_table(
        columns=list(dict.fromkeys((*META, *columns))), filter=predicate).to_pandas()
    checked_source(plan)
    return frame


def validate_finance_v1(rows, calendar, *, evaluation=False):
    fields = EVAL_FINANCE if evaluation else TRAIN_FINANCE
    sessions = sessions_v1(calendar)
    if (not set(fields).issubset(rows) or not rows.valuation_policy_sha256.eq(VALUATION_POLICY_SHA256).all()
            or rows.duplicated(list(ROSTER_KEY)).any()):
        raise ValueError("projected valuation policy/schema/key differs")
    for d, t, end in rows[[*KEY[:2], "label_information_end"]].itertuples(index=False, name=None):
        i = sessions.get_loc(pd.Timestamp(d))
        if i+5 >= len(sessions) or t != sessions[i+1] or pd.Timestamp(end) != sessions[i+5]:
            raise ValueError("label clock differs from calendar-first T through T+4")
    compared = [*fields, *(n for n in FEATURES if n in rows)]
    if rows.groupby(list(KEY))[compared].nunique(dropna=False).gt(1).any().any():
        raise ValueError("duplicate stock/day has contradictory financial/input observations")
    if not rows.valuation_status.isin(["AVAILABLE", "UNKNOWN", "IMMATURE", "CASH_ENTRY_NOT_EXECUTABLE"]).all():
        raise ValueError("unknown valuation status")
    for row in rows.loc[rows.valuation_status.eq("AVAILABLE")].to_dict("records"):
        gap, y = (_number(row[n]) for n in ("observed_gap_bps", "valuation_gross_terminal_ratio"))
        if gap is None or gap <= -10000 or y is None or y <= 0:
            raise ValueError("AVAILABLE valuation requires valid positive price coordinates")
        if evaluation:
            net, mark, path = (_number(row[n]) for n in (
                "hypothetical_liquidation_net_bps", "mark_to_market_net_bps", "valuation_path_min_ratio"))
            raw = y/((1+gap/10000)*(1+POLICY["buy_bps"]/10000))
            if (net is None or mark is None or path is None or not 0 < path <= y
                    or not math.isclose(net, 10000*(raw*(1-POLICY["sell_bps"]/10000)-1), abs_tol=1e-6)
                    or not math.isclose(mark, 10000*(raw-1), abs_tol=1e-6)):
                raise ValueError("valuation path or one-time costs contradict the frozen policy")


def prepare_hurdle_inputs_v1(*, plan):
    from backend.services.advisory_model_first.original_slot_hurdle_price_5td_model_v1 import fit_encoding_v1
    source, header = checked_source(plan)
    calendar = json.loads((source/"calendar.json").read_bytes())
    clocks = clocks_v1(plan, calendar)
    sessions = sessions_v1(calendar)
    dates = sessions[(sessions >= pd.Timestamp(plan.train_start)) & (sessions <= pd.Timestamp(plan.evaluation_end))]
    roster = read_projection_v1(plan=plan, dates=dates, columns=FEATURES)
    days = ds.dataset(source/"days.parquet", format="parquet").to_table(columns=list(DAY_META),
        filter=ds.field("decision_date").isin(list(dates))).to_pandas()
    validate_roster_v1(roster, days, plan, calendar)
    if roster.groupby(list(KEY))[list(FEATURES)].nunique(dropna=False).gt(1).any().any():
        raise ValueError("same stock/D context contradicts across packages")
    roster["label_cluster"] = [cluster_v1(*v) for v in roster[list(KEY)].itertuples(index=False, name=None)]
    # H from the calendar chooses rows BEFORE financial values are decoded.
    finance = read_projection_v1(plan=plan, dates=clocks["train_dates"], columns=TRAIN_FINANCE, top5=True)
    validate_finance_v1(finance, calendar)
    domain = roster.loc[roster.selection_effective_rank.le(5)
        & roster[KEY[0]].isin(pd.to_datetime(clocks["train_dates"]))].copy()
    domain = domain.merge(finance[[*ROSTER_KEY, *TRAIN_FINANCE]], on=list(ROSTER_KEY), how="left",
        validate="one_to_one", indicator=True)
    if not domain._merge.eq("both").all() or len(finance) != len(domain) or len(domain) > 3050:
        raise ValueError("an original training slot was changed/removed")
    domain = domain.drop(columns="_merge")
    unique = domain.drop_duplicates("label_cluster")
    support = _support(unique)
    encoding = fit_encoding_v1(unique, support.intervals_bps)
    encoding.update(clocks=clocks, original_roster_sha256=sha(roster.astype(str).to_dict("records")),
        source_stage_sha256=header["stage_sha256"], source_ref=plan.source_prepared.model_dump(mode="json"))
    _, known_stock = feature_values(domain)
    gap = domain.observed_gap_bps.map(_number).astype(float)
    domain["supervision_status"] = np.where(~known_stock, "UNKNOWN_STOCK_INPUT", np.where(
        ~domain.valuation_status.eq("AVAILABLE"), "UNKNOWN_OR_NON_EXECUTABLE_VALUATION", np.where(
        ~np.isfinite(gap), "UNKNOWN_PRICE_SCENARIO", "AVAILABLE")))
    domain["cluster_mass"] = 1/domain.groupby("label_cluster").label_cluster.transform("size")
    domain["net_return_bps"] = np.nan
    known = domain.supervision_status.eq("AVAILABLE")
    raw = domain.loc[known, "valuation_gross_terminal_ratio"].astype(float)/(
        (1+gap.loc[known]/10000)*(1+POLICY["buy_bps"]/10000))
    domain.loc[known, "net_return_bps"] = 10000*(raw*(1-POLICY["sell_bps"]/10000)-1)
    checked_source(plan)
    return dict(roster=roster, days=days, domain=domain, encoding=encoding, calendar=calendar,
        source_stage_sha256=header["stage_sha256"])
