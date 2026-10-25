"""Shared original rosters, bounded D inputs and separate fixed5 valuation labels."""
import hashlib
import io
import json

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import build_shared_daily_features
from backend.services.advisory_model_first.cross_package_validation_source_v1 import read_frozen_prediction_view
from backend.services.advisory_model_first.economic_entry_pipeline import file_sha256
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, ROSTER
from backend.services.advisory_model_first.generic_price_5td_labels_v1 import PRICE_FIELDS, REFERENCE_FIELDS
from backend.services.advisory_model_first.generic_price_5td_models_v1 import _support, feature_values
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import (
    VALUATION_POLICY_SHA256, build_generic_price_5td_valuation_v2,
)
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import MATRIX_ORDERS, node_path
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def parquet_bytes(frame):
    stream = io.BytesIO()
    frame.to_parquet(stream, index=False)
    return stream.getvalue()


def original_calendar(ref):
    path = node_path(ref.artifact_uri)
    if path.stat().st_size != ref.size_bytes or file_sha256(path) != ref.sha256:
        raise ValueError("original calendar reference changed")
    days = [_day(v) for v in json.loads(path.read_bytes())]
    if not days or len(days) > 5000 or days != sorted(set(days)):
        raise ValueError("original calendar is not sorted unique sessions")
    return days


def warmup_calendar(*, calendar, train_start, connection_context_factory=None):
    """Metadata only. The original file is never changed; no fake warmup sessions."""
    if sum(d < train_start for d in calendar) >= 19:
        return list(calendar), dict(status="ORIGINAL_WARMUP_AVAILABLE", additional_sessions=[])
    from backend.db.pg_pool import get_conn
    factory = connection_context_factory or (lambda: get_conn(autocommit=False, manage_transaction=False))
    with factory() as connection:
        cursor = connection.cursor()
        try:
            connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            cursor.execute("SET LOCAL statement_timeout = %s", (30000,))
            cursor.execute("SELECT cal_date FROM market.trading_calendar WHERE is_trading=TRUE AND cal_date < %s ORDER BY cal_date DESC LIMIT %s",
                           (train_start, 19))
            prefix = sorted(_day(row[0]) for row in cursor.fetchall())
        finally:
            connection.rollback()
            cursor.close()
    if (prefix != sorted(set(prefix)) or any(d >= train_start for d in prefix)
            or [d for d in prefix if d >= calendar[0]] != [d for d in calendar if d < train_start]):
        raise ValueError("warmup calendar contradicts original overlapping sessions")
    added = [d for d in prefix if d < calendar[0]]
    combined = added+list(calendar)
    return combined, dict(status="READONLY_PREFIX_COMPLETE" if len(prefix) == 19 else "INSUFFICIENT_REAL_WARMUP_KEEP_UNKNOWN",
        additional_sessions=[d.isoformat() for d in added], readonly=True, database_written=False)


def freeze_rosters(*, plan, calendar, api):
    decisions = [d for d in calendar if plan.train_start <= d <= plan.evaluation_end]
    positions = {d: i for i, d in enumerate(calendar)}
    if not decisions or any(positions[d]+1 >= len(calendar) for d in decisions):
        raise ValueError("declared D needs its original next session, not a fabricated T")
    rows, days, receipts = [], [], []
    for source in plan.sources:
        selected, receipt = read_frozen_prediction_view(api=api, source=source.prediction_source, decision_dates=decisions)
        selected[KEY[0]] = pd.to_datetime(selected.pop("decision_date"))
        selected[KEY[1]] = selected[KEY[0]].map(lambda d: pd.Timestamp(calendar[positions[d.date()]+1]))
        selected["selection_effective_rank"] = selected.pop("rank").astype(int)
        selected["candidate_group_size"] = selected.groupby(KEY[0]).instrument.transform("size").astype(int)
        selected["parent_score"] = selected.pop("score")
        for name in ("package_id", "manifest_sha256", "run_id", "source_id"):
            selected[name] = getattr(source, name)
        rows.append(selected)
        receipts.append(receipt)
        for d in decisions:
            pos = positions[d]
            h = calendar[pos+5] if pos+5 < len(calendar) else None
            count = int(selected[KEY[0]].eq(pd.Timestamp(d)).sum())
            days.append(dict(package_id=source.package_id, decision_date=pd.Timestamp(d),
                target_date=pd.Timestamp(calendar[pos+1]), horizon_end=pd.Timestamp(h), original_candidates=count,
                source_status="KNOWN_ROSTER" if count else "UNKNOWN_SOURCE_NOT_PROVEN_EMPTY"))
    roster = pd.concat(rows, ignore_index=True)
    if roster.duplicated(["package_id", *KEY]).any():
        raise ValueError("parent score original package keys duplicate")
    return roster, pd.DataFrame(days), receipts


def build_inputs(*, plan, roster, calendar, daily, index_daily):
    """No outside-window finance is decoded; D feature and H measurement channels stay separate."""
    if roster.empty:
        raise ValueError("source contains no original roster rows; no model inputs")
    quotes = daily.copy()
    for frame in (quotes, index_daily):
        dates = frame.trade_date.map(_day)
        if any(d not in calendar or d > plan.evaluation_end for d in dates):
            raise ValueError("outside-cutoff financial rows reached the input boundary")
        if frame.duplicated(["trade_date", "instrument"]).any():
            raise ValueError("shared financial keys conflict")
    quotes["trade_date"] = pd.to_datetime(quotes.trade_date)
    quotes = quotes.set_index(["trade_date", "instrument"]).sort_index()
    positions = {d: i for i, d in enumerate(calendar)}
    warm = roster[KEY[0]].map(lambda d: positions[d.date()] >= 19)
    cold = roster.loc[~warm].copy()
    for n in FEATURES:
        cold[n] = np.nan
    cold["feature_input_status"] = "UNKNOWN_INSUFFICIENT_ORIGINAL_HISTORY"
    if warm.any():
        known, _ = build_shared_daily_features(rosters=roster.loc[warm].copy().reset_index(drop=True), calendar=calendar,
                                              daily=daily, index_daily=index_daily)
        known["feature_input_status"] = "D_ONLY_SHARED_INPUT"
        features = pd.concat([known, cold], ignore_index=True)
    else:
        features = cold
    shared = roster.loc[:, KEY].drop_duplicates()
    prices, references = [], []
    for d, group in shared.groupby(KEY[0], sort=True):
        symbols = group.instrument.tolist()
        anchor = quotes.reindex(pd.MultiIndex.from_product(([d], symbols), names=quotes.index.names))
        factors = dict(zip(symbols, anchor.adj_factor, strict=True))
        refs = group.copy()
        refs["reference_cny"] = refs.instrument.map(dict(zip(symbols, anchor.raw_close_cny, strict=True)))
        refs["reference_visible_through"] = d.date()
        references.append(refs)
        pos = positions[d.date()]
        horizon = pd.to_datetime([v for v in calendar[pos+1:pos+6] if v <= plan.evaluation_end])
        path = quotes.reindex(pd.MultiIndex.from_product((horizon, symbols), names=quotes.index.names)).dropna(how="all").reset_index()
        path[KEY[0]] = d
        path["d_anchor_factor"] = path.adj_factor/path.instrument.map(factors)
        for n in ("open", "high", "low", "close"):
            path[n] = path["raw_"+n+"_cny"]
        prices.append(path.loc[:, PRICE_FIELDS])
    prices, references = pd.concat(prices, ignore_index=True), pd.concat(references, ignore_index=True)
    labels = []
    for source in plan.sources:
        original = roster.loc[roster.package_id.eq(source.package_id)]
        dates = sorted(original[KEY[0]].dt.date.unique())
        for first in range(0, len(dates), 100):
            chosen = dates[first:first+100]
            block = original.loc[original[KEY[0]].isin(pd.to_datetime(chosen)), ROSTER]
            pp = prices.merge(block.loc[:, [KEY[0], "instrument"]], on=[KEY[0], "instrument"], validate="many_to_one")
            rr = references.merge(block.loc[:, KEY], on=list(KEY), validate="one_to_one")
            context = dict(calendar_sha256=sha([d.isoformat() for d in calendar]),
                prices_sha256=hashlib.sha256(parquet_bytes(pp)).hexdigest(),
                references_sha256=hashlib.sha256(parquet_bytes(rr)).hexdigest(),
                label_price_basis="D_REFERENCE_POLICY_RATIO", source_evidence="CURRENT_DATABASE_NON_VINTAGE")
            result, _ = build_generic_price_5td_valuation_v2(candidates=block, decision_dates=chosen,
                calendar=calendar, prices=pp, references=rr.loc[:, REFERENCE_FIELDS], source_context=context)
            immature = result.label_information_end.isna() | result.label_information_end.gt(pd.Timestamp(plan.evaluation_end))
            result.loc[immature, ["label_status", "valuation_status"]] = "IMMATURE"
            result.loc[immature, ["gross_terminal_ratio", "path_min_ratio", "valuation_gross_terminal_ratio",
                "valuation_path_min_ratio", "mark_to_market_net_bps", "hypothetical_liquidation_net_bps"]] = np.nan
            result["package_id"] = source.package_id
            labels.append(result.drop(columns=["selection_effective_rank", "candidate_group_size"]))
    result = roster.merge(features.loc[:, ["package_id", *KEY, *FEATURES, "feature_input_status"]],
        on=["package_id", *KEY], sort=False, validate="one_to_one").merge(pd.concat(labels, ignore_index=True),
        on=["package_id", *KEY], sort=False, validate="one_to_one")
    if len(result) != len(roster) or not result.loc[:, ["package_id", *ROSTER]].equals(roster.loc[:, ["package_id", *ROSTER]]):
        raise ValueError("input preparation changed original keys, order or ranks")
    result["label_cluster"] = [sha([d.date().isoformat(), t.date().isoformat(), s, VALUATION_POLICY_SHA256])
        for d, t, s in result.loc[:, KEY].itertuples(index=False, name=None)]
    return result, training_encoding(rows=result, plan=plan)


def training_encoding(*, rows, plan):
    financial = [*FEATURES, "observed_gap_bps", "valuation_gross_terminal_ratio", "valuation_path_min_ratio",
                 "valuation_status", "valuation_policy_sha256", "label_information_end"]
    if (rows.duplicated(["package_id", *KEY]).any()
            or rows.groupby("label_cluster")[financial].nunique(dropna=True).gt(1).any().any()):
        raise ValueError("same stock/D cluster has contradictory shared finance")
    d, h = rows[KEY[0]], pd.to_datetime(rows.label_information_end)
    time = d.between(pd.Timestamp(plan.train_start), pd.Timestamp(plan.train_end)) & h.le(pd.Timestamp(plan.train_end))
    # Project temporal membership before parsing any D values, score or supervision.
    domain = rows.loc[time].drop_duplicates("label_cluster")
    values, _ = feature_values(domain)
    medians = [float(np.median(v[~np.isnan(v)])) if (~np.isnan(v)).any() else 0. for v in values.T]
    support = _support(domain)
    adapters = {}
    for source in plan.sources:
        scores = rows.loc[time & rows.package_id.eq(source.package_id), "parent_score"].map(_number).dropna().to_numpy(float)
        q = np.quantile(scores, [.25, .5, .75], method="linear") if len(scores) else np.array([np.nan]*3)
        scale = q[2]-q[0]
        adapters[source.package_id] = dict(context_bit=source.context_bit, manifest_sha256=source.manifest_sha256,
            run_id=source.run_id, center=float(q[1]) if np.isfinite(q[1]) else None,
            scale=float(scale) if np.isfinite(scale) and scale > 0 else None,
            status="AVAILABLE" if np.isfinite(scale) and scale > 0 else "UNKNOWN_PARENT_SCORE_SCALE")
    _, known = feature_values(rows.loc[time])
    eligible = pd.Series(False, index=rows.index)
    eligible.loc[time] = known & rows.loc[time].valuation_status.eq("AVAILABLE") & rows.loc[time].observed_gap_bps.map(
        lambda v: _number(v) is not None and support.contains(float(v))) & h.loc[time].lt(pd.Timestamp(plan.evaluation_start))
    unique_dates = sorted(rows.loc[eligible, KEY[0]].unique())
    first = pd.Timestamp(unique_dates[len(unique_dates)//2]) if unique_dates else None
    rows["pool"] = "NOT_TRAIN_SUPERVISION"
    if first is not None:
        rows.loc[eligible & d.ge(first), "pool"] = "ESTIMATION"
        rows.loc[eligible & d.lt(first), "pool"] = "PURGED_LABEL_OVERLAP"
        rows.loc[eligible & d.lt(first) & h.lt(first), "pool"] = "STRUCTURE"
    rows["cluster_mass"] = 0.
    selected = rows.pool.isin(["STRUCTURE", "ESTIMATION"])
    rows.loc[selected, "cluster_mass"] = 1/rows.loc[selected].groupby("label_cluster").label_cluster.transform("size")
    counts = {role: int(rows.pool.eq(role).sum()) for role in ("STRUCTURE", "ESTIMATION", "PURGED_LABEL_OVERLAP", "NOT_TRAIN_SUPERVISION")}
    identifiable = counts["STRUCTURE"] > 0 and counts["ESTIMATION"] > 0 and all(a["status"] == "AVAILABLE" for a in adapters.values())
    return dict(matrix_orders={a: list(v) for a, v in MATRIX_ORDERS.items()}, medians=medians,
        intervals_bps=support.intervals_bps, adapters=adapters, pools=counts,
        estimation_first_D=first.date().isoformat() if first is not None else None,
        train_start=plan.train_start.isoformat(), train_end=plan.train_end.isoformat(),
        evaluation_start=plan.evaluation_start.isoformat(), evaluation_end=plan.evaluation_end.isoformat(),
        contrast_status="PREPARED_IDENTIFIABLE_NO_FIT" if identifiable else "UNKNOWN_CONTRAST_NOT_IDENTIFIABLE",
        physical_fit_count=0, scale_quantile="LINEAR_TRAIN_DATE_MATURE_NOT_LABEL_SELECTED")
