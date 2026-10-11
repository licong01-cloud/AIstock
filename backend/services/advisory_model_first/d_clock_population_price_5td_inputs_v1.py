"""Independent D-anchored marks, common D inputs and honest sampling masks."""
import hashlib
import json
import re

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import build_shared_daily_features
from backend.services.advisory_model_first.d_clock_population_price_5td_contracts_v1 import (
    ARMS, FEATURES, KEY, LABEL_COLUMNS, MARK_POLICY, MARK_POLICY_SHA256, MATRIX_ORDER, ROSTER,
    SOURCE_COLUMNS, node_path,
)
from backend.services.advisory_model_first.economic_entry_pipeline import file_sha256
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number, _records
from backend.services.advisory_model_first.generic_price_5td_labels_v1 import (
    PRICE_FIELDS, REFERENCE_FIELDS, _frame, validate_roster,
)
from backend.services.advisory_model_first.generic_price_5td_models_v1 import _support, feature_values
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256, build_generic_price_5td_valuation_v2
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def _fail(message):
    raise AdvisoryModelFirstError(message, reason_code="ADVISORY_D_CLOCK_MARK_INVALID")


def build_d_clock_mark_labels_v1(*, candidates, decision_dates, calendar, prices, references, source_context, cutoff):
    """No future entry test controls the mark target; verified suspension is valuation only."""
    roster, decisions, days = validate_roster(candidates, decision_dates, calendar)
    cutoff = _day(cutoff)
    required = {"calendar_sha256", "prices_sha256", "references_sha256", "label_price_basis", "source_evidence"}
    if (not isinstance(source_context, dict) or set(source_context) != required
            or source_context.get("label_price_basis") != "D_REFERENCE_POLICY_RATIO"
            or not isinstance(source_context.get("source_evidence"), str) or not source_context["source_evidence"].strip()):
        _fail("explicit source identity and D-anchor coordinate declaration required")
    for name in ("calendar_sha256", "prices_sha256", "references_sha256"):
        if not isinstance(source_context[name], str) or re.fullmatch(r"[a-f0-9]{64}", source_context[name]) is None:
            _fail("source digest is not SHA256")
    if source_context["calendar_sha256"] != sha([v.isoformat() for v in days]):
        _fail("mark calendar identity differs")
    if (hashlib.sha256(parquet_bytes(prices)).hexdigest() != source_context["prices_sha256"]
            or hashlib.sha256(parquet_bytes(references)).hexdigest() != source_context["references_sha256"]):
        _fail("known mark source digest contradicts its immutable packet")
    price = _frame(prices, PRICE_FIELDS, 500000, (KEY[0], "trade_date"))
    refs = _frame(references, REFERENCE_FIELDS, 7720, KEY[:2])
    if price.duplicated([KEY[0], "trade_date", "instrument"]).any() or refs.duplicated(list(KEY)).any():
        _fail("mark source keys conflict")
    if set(refs.loc[:, KEY].itertuples(index=False, name=None)) != set(roster.loc[:, KEY].itertuples(index=False, name=None)):
        _fail("mark references changed complete original keys")
    positions = {day: i for i, day in enumerate(days)}
    allowed = {(r.decision_as_of_trade_date, r.instrument): {
        pd.Timestamp(v) for v in days[positions[r.decision_as_of_trade_date.date()]:positions[r.target_trade_date.date()]+5]
        if v <= cutoff} for r in roster.itertuples(index=False)}
    if any(r.trade_date not in allowed.get((r.decision_as_of_trade_date, r.instrument), set()) for r in price.itertuples(index=False)):
        _fail("mark source has foreign or beyond-cutoff quotes")
    for name in ("open", "high", "low", "close", "d_anchor_factor", "up_limit", "down_limit"):
        price[name] = price[name].map(lambda v: _number(v, positive=True)).astype(float)
    if (price.low.gt(price.close).any() or price.low.gt(price.high).any() or price.high.lt(price.close).any()
            or price.low.gt(price.open).any() or price.high.lt(price.open).any()
            or price.down_limit.ge(price.up_limit).any()):
        _fail("known OHLC/limit units contradict")
    for name in ("suspended", "tradability_unknown"):
        if not price[name].map(lambda v: type(v) is bool).all():
            _fail("suspension must be explicit bool with a separate unknown flag")
    refs["reference_cny"] = refs.reference_cny.map(lambda v: _number(v, positive=True)).astype(float)
    refs["reference_visible_through"] = refs.reference_visible_through.map(lambda v: _day(v, nullable=True))
    if any(clock is not None and clock > d.date() for clock, d in zip(refs.reference_visible_through, refs[KEY[0]], strict=True)):
        _fail("D reference includes future visibility")
    quotes = price.set_index([KEY[0], "trade_date", "instrument"]).to_dict("index")
    references_by_key = refs.set_index(list(KEY)).to_dict("index")
    output = []
    for original in roster.to_dict("records"):
        d, t, symbol = (original[name] for name in KEY)
        horizon = days[positions[t.date()]:positions[t.date()]+5]
        h = pd.Timestamp(horizon[-1]) if len(horizon) == 5 else pd.NaT
        values = dict(label_information_end=h, terminal_mark_ratio=np.nan, path_mark_min_ratio=np.nan,
            mark_label_status="UNKNOWN", mark_label_reason="UNKNOWN_REFERENCE", mark_policy_sha256=MARK_POLICY_SHA256,
            endpoint_execution_status="UNKNOWN", carried_mark_sessions=0, entry_execution_status="UNKNOWN",
            entry_price_cny=np.nan, observed_gap_bps=np.nan, actual_fill_proven=False, realized_return_bps=np.nan)
        if len(horizon) != 5 or h.date() > cutoff:
            values.update(mark_label_status="IMMATURE", mark_label_reason="HORIZON_AFTER_CUTOFF", endpoint_execution_status="IMMATURE")
            output.append({**original, **values})
            continue
        reference = references_by_key[(d, t, symbol)]
        anchor, dbar = reference["reference_cny"], quotes.get((d, d, symbol))
        if dbar is not None and pd.notna(dbar["d_anchor_factor"]) and not np.isclose(dbar["d_anchor_factor"], 1., atol=1e-12, rtol=0):
            _fail("D quote has a non-unit D-anchor factor")
        if (pd.isna(anchor) or reference["reference_visible_through"] is None or dbar is None
                or pd.isna(dbar["close"]) or pd.isna(dbar["d_anchor_factor"])):
            output.append({**original, **values})
            continue
        if not np.isclose(anchor, dbar["close"], atol=1e-10, rtol=1e-10):
            _fail("D reference is not its explicit actual D close")
        bars = [quotes.get((d, pd.Timestamp(day), symbol)) for day in horizon]
        entry = bars[0]
        if entry is not None:
            if entry["tradability_unknown"]:
                values["entry_execution_status"] = "UNKNOWN"
            elif entry["suspended"]:
                values["entry_execution_status"] = "CASH_ENTRY_SUSPENDED"
            elif all(pd.notna(entry[n]) for n in ("open", "d_anchor_factor")):
                values["entry_price_cny"] = entry["open"]*entry["d_anchor_factor"]
                values["observed_gap_bps"] = 10000*(values["entry_price_cny"]/anchor-1)
                if pd.notna(entry["up_limit"]):
                    values["entry_execution_status"] = "CASH_ENTRY_UP_LIMIT" if entry["open"] >= entry["up_limit"] else "NOMINAL_ENTRY_ELIGIBLE_NOT_FILL_PROOF"
        last_mark, lows, marks, reason = anchor, [], [], None
        for bar in bars:
            if bar is None:
                reason = "UNEXPLAINED_MISSING_BAR"
                break
            if bar["tradability_unknown"]:
                reason = "UNKNOWN_SUSPENSION_STATE"
                break
            if pd.isna(bar["d_anchor_factor"]):
                reason = "UNKNOWN_CORPORATE_ACTION_BRIDGE"
                break
            if bar["suspended"]:
                # Known coordinate bridge; carry a mark, not raw OHLC, fill or zero-risk proof.
                marks.append(last_mark)
                lows.append(last_mark)
                values["carried_mark_sessions"] += 1
                continue
            if any(pd.isna(bar[n]) for n in ("low", "close")):
                reason = "UNKNOWN_TRADED_LOW_OR_CLOSE"
                break
            last_mark = bar["close"]*bar["d_anchor_factor"]
            marks.append(last_mark)
            lows.append(bar["low"]*bar["d_anchor_factor"])
        if reason is None and len(marks) == 5:
            y, lower = marks[-1]/anchor, min(lows)/anchor
            if not np.isfinite([y, lower]).all() or min(y, lower) <= 0 or lower > y:
                _fail("D-anchored mark ratios contradict")
            endpoint = bars[-1]
            execution = ("PENDING_EXIT_SUSPENDED" if endpoint["suspended"] else "UNKNOWN_EXIT_LIMIT"
                if pd.isna(endpoint["down_limit"]) else "EXIT_UNPROVEN_LIMIT_DOWN"
                if endpoint["close"] <= endpoint["down_limit"] else "NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF")
            values.update(terminal_mark_ratio=y, path_mark_min_ratio=lower, mark_label_status="AVAILABLE",
                mark_label_reason=None, endpoint_execution_status=execution)
        else:
            values["mark_label_reason"] = reason or "INCOMPLETE_FIXED_SESSION_PATH"
        output.append({**original, **values})
    labels = pd.DataFrame(output, columns=[*ROSTER, *LABEL_COLUMNS])
    receipt = dict(mark_policy=MARK_POLICY, mark_policy_sha256=MARK_POLICY_SHA256,
        source_context=source_context, roster_sha256=sha(_records(roster)), decision_dates=[d.isoformat() for d in decisions],
        counts={str(k): int(v) for k, v in labels.mark_label_status.value_counts().items()},
        actual_fill_proven=False, native_receipt_recovered=False, fit_count=0)
    return labels, receipt


def verified_parent_rosters_v1(plan):
    """Verify declared immutable references once, then project source/roster metadata only."""
    refs = (plan.parent_manifest_ref, plan.parent_rows_ref, plan.parent_days_ref, plan.calendar_ref)
    paths = []
    for ref in refs:
        path = node_path(ref.artifact_uri)
        if not path.is_file() or path.stat().st_size != ref.size_bytes or file_sha256(path) != ref.sha256:
            _fail("frozen parent reference changed")
        paths.append(path)
    manifest = json.loads(paths[0].read_bytes())
    body = {k: v for k, v in manifest.items() if k != "stage_sha256"}
    if (manifest.get("stage") != "prepared" or manifest.get("plan_sha256") != plan.parent_plan_sha256
            or manifest.get("stage_sha256") != plan.parent_stage_sha256 or sha(body) != plan.parent_stage_sha256):
        _fail("parent prepared identity differs")
    for path, ref in zip(paths[1:], refs[1:], strict=True):
        if path.parent != paths[0].parent or manifest["files"].get(path.name) != dict(sha256=ref.sha256, size_bytes=ref.size_bytes):
            _fail("parent manifest does not bind the declared roster/calendar")
    calendar = [_day(v) for v in json.loads(paths[3].read_bytes())]
    if not calendar or len(calendar) > 5000 or calendar != sorted(set(calendar)):
        _fail("original calendar differs")
    predicate = ((ds.field(KEY[0]) >= pd.Timestamp(plan.train_start)) & (ds.field(KEY[0]) <= pd.Timestamp(plan.evaluation_end)))
    roster = ds.dataset(paths[1], format="parquet").to_table(columns=list(SOURCE_COLUMNS), filter=predicate).to_pandas()
    days = pd.read_parquet(paths[2])
    for name in KEY[:2]:
        roster[name] = roster[name].map(_day).map(pd.Timestamp)
    if not 0 < len(roster) <= 50000 or roster.duplicated(["package_id", *KEY]).any():
        _fail("original package roster is empty, duplicate or over budget")
    if set(roster.package_id) != {s.package_id for s in plan.sources}:
        _fail("parent roster packages differ")
    day_columns = {"package_id", "decision_date", "target_date", "horizon_end", "original_candidates", "source_status"}
    if (set(days.columns) != day_columns or not days.columns.is_unique or days.duplicated(["package_id", "decision_date"]).any()
            or set(days.package_id) != {s.package_id for s in plan.sources}):
        _fail("parent day schedule schema/source keys differ")
    for name in ("decision_date", "target_date", "horizon_end"):
        days[name] = days[name].map(lambda v: _day(v, nullable=name == "horizon_end")).map(pd.Timestamp)
    expected_days = [pd.Timestamp(d) for d in calendar if plan.train_start <= d <= plan.train_end
                     or plan.evaluation_start <= d <= plan.evaluation_end]
    if not days.original_candidates.map(lambda v: isinstance(v, (int, np.integer)) and not isinstance(v, (bool, np.bool_)) and 0 <= v <= 50).all():
        _fail("parent original candidate counts are not bounded integers")
    positions = {d: i for i, d in enumerate(calendar)}
    for source in plan.sources:
        if days.loc[days.package_id.eq(source.package_id), "decision_date"].tolist() != expected_days:
            _fail("parent schedule removed an original package/session")
    for day in days.itertuples(index=False):
        pos = positions[day.decision_date.date()]
        t = pd.Timestamp(calendar[pos+1]) if pos+1 < len(calendar) else pd.NaT
        h = pd.Timestamp(calendar[pos+5]) if pos+5 < len(calendar) else pd.NaT
        block = roster.loc[roster.package_id.eq(day.package_id) & roster[KEY[0]].eq(day.decision_date)]
        if (len(block) != day.original_candidates or day.target_date != t
                or not (day.horizon_end == h or pd.isna(day.horizon_end) and pd.isna(h))
                or day.source_status not in ("KNOWN_ROSTER", "KNOWN_EMPTY", "UNKNOWN")
                or (bool(len(block)) and day.source_status == "KNOWN_EMPTY")
                or (not len(block) and day.source_status == "KNOWN_ROSTER")):
            _fail("parent full day schedule and roster D/T/H/counts contradict")
    for source in plan.sources:
        block = roster.loc[roster.package_id.eq(source.package_id)]
        if any(not block[name].eq(getattr(source, name)).all() for name in ("source_id", "manifest_sha256", "run_id")):
            _fail("parent package/run/source/hash differs")
        decisions = sorted(block[KEY[0]].dt.date.unique())
        for first in range(0, len(decisions), 100):
            chosen = decisions[first:first+100]
            validate_roster(block.loc[block[KEY[0]].isin(pd.to_datetime(chosen)), ROSTER], chosen, calendar)
    if set(roster.loc[:, ["package_id", KEY[0]]].itertuples(index=False, name=None)) - set(
            days.loc[:, ["package_id", "decision_date"]].itertuples(index=False, name=None)):
        _fail("parent roster has an unregistered package/day")
    return roster.reset_index(drop=True), days, calendar


def source_price_packet_v1(*, roster, calendar, daily, cutoff):
    """Explicit D quote plus original T..H quotes, not a fabricated pre-close."""
    cutoff = _day(cutoff)
    quotes = daily.copy()
    quotes["trade_date"] = quotes.trade_date.map(_day).map(pd.Timestamp)
    if (quotes.duplicated(["trade_date", "instrument"]).any() or quotes.trade_date.gt(pd.Timestamp(cutoff)).any()
            or not quotes.trade_date.isin(pd.to_datetime(calendar)).all()):
        _fail("snapshot has conflicting or beyond-cutoff daily keys")
    quotes = quotes.set_index(["trade_date", "instrument"]).sort_index()
    positions = {day: i for i, day in enumerate(calendar)}
    prices, references = [], []
    for d, full in roster.loc[:, KEY].drop_duplicates().groupby(KEY[0], sort=True):
        symbols = full.instrument.tolist()
        anchor = quotes.reindex(pd.MultiIndex.from_product(([d], symbols), names=quotes.index.names))
        factors = dict(zip(symbols, anchor.adj_factor, strict=True))
        ref = full.copy()
        ref["reference_cny"] = ref.instrument.map(dict(zip(symbols, anchor.raw_close_cny, strict=True)))
        ref["reference_visible_through"] = d.date()
        references.append(ref)
        pos = positions[d.date()]
        sessions = [v for v in calendar[pos:pos+6] if v <= cutoff]
        path = quotes.reindex(pd.MultiIndex.from_product((pd.to_datetime(sessions), symbols), names=quotes.index.names)).dropna(how="all").reset_index()
        path[KEY[0]] = d
        path["d_anchor_factor"] = path.adj_factor/path.instrument.map(factors)
        for name in ("open", "high", "low", "close"):
            path[name] = path["raw_"+name+"_cny"]
        prices.append(path.loc[:, PRICE_FIELDS])
    return pd.concat(prices, ignore_index=True), pd.concat(references, ignore_index=True)


def construct_mark_targets_v1(*, plan, roster, calendar, daily, cutoff, legacy):
    """Bounded package packets; the candidate target is never obtained by relabeling V2."""
    prices, references = source_price_packet_v1(roster=roster, calendar=calendar, daily=daily, cutoff=cutoff)
    results = []
    for source in plan.sources:
        original = roster.loc[roster.package_id.eq(source.package_id)]
        dates = sorted(original[KEY[0]].dt.date.unique())
        for first in range(0, len(dates), 100):
            chosen = dates[first:first+100]
            block = original.loc[original[KEY[0]].isin(pd.to_datetime(chosen)), ROSTER]
            pp = prices.merge(block.loc[:, [KEY[0], "instrument"]], on=[KEY[0], "instrument"], validate="many_to_one")
            rr = references.merge(block.loc[:, KEY], on=list(KEY), validate="one_to_one").loc[:, REFERENCE_FIELDS]
            context = dict(calendar_sha256=sha([d.isoformat() for d in calendar]),
                prices_sha256=hashlib.sha256(parquet_bytes(pp)).hexdigest(), references_sha256=hashlib.sha256(parquet_bytes(rr)).hexdigest(),
                label_price_basis="D_REFERENCE_POLICY_RATIO", source_evidence="CURRENT_DATABASE_NON_VINTAGE")
            labels, _ = build_d_clock_mark_labels_v1(candidates=block, decision_dates=chosen, calendar=calendar,
                prices=pp, references=rr, source_context=context, cutoff=cutoff)
            if legacy:
                lp = pp.loc[pp.trade_date.gt(pp[KEY[0]])].copy()
                old, _ = build_generic_price_5td_valuation_v2(candidates=block, decision_dates=chosen,
                    calendar=calendar, prices=lp, references=rr, source_context=context)
                labels["legacy_valuation_status"] = old.valuation_status.to_numpy()
                labels["legacy_valuation_policy_sha256"] = old.valuation_policy_sha256.to_numpy()
                labels["observed_gap_bps"] = old.observed_gap_bps.to_numpy()
            labels["package_id"] = source.package_id
            results.append(labels.drop(columns=["selection_effective_rank", "candidate_group_size"]))
    return pd.concat(results, ignore_index=True)


def build_d_clock_inputs_v1(*, plan, roster, calendar, daily, index_daily):
    """Only train labels are constructed here; evaluation has D inputs and horizon metadata."""
    for frame in (daily, index_daily):
        dates = frame.trade_date.map(_day)
        if any(v not in calendar or v > plan.evaluation_end for v in dates):
            _fail("foreign or sealed financial date reached shared D input constructor")
    positions = {d: i for i, d in enumerate(calendar)}
    warm = roster[KEY[0]].map(lambda d: positions[d.date()] >= 19)
    cold = roster.loc[~warm].copy()
    for name in FEATURES:
        cold[name] = np.nan
    cold["feature_input_status"] = "UNKNOWN_INSUFFICIENT_HISTORY"
    if warm.any():
        known, _ = build_shared_daily_features(rosters=roster.loc[warm].copy().reset_index(drop=True),
            calendar=calendar, daily=daily, index_daily=index_daily)
        known["feature_input_status"] = "D_ONLY_SHARED_SNAPSHOT"
        features = pd.concat([known, cold], ignore_index=True)
    else:
        features = cold
    out = roster.merge(features.loc[:, ["package_id", *KEY, *FEATURES, "feature_input_status"]],
        on=["package_id", *KEY], validate="one_to_one", sort=False)
    train = roster.loc[roster[KEY[0]].between(pd.Timestamp(plan.train_start), pd.Timestamp(plan.train_end))].copy()
    # Temporal projection precedes all label price decoding.
    train_daily = daily.loc[daily.trade_date.map(_day).le(plan.train_end)].copy()
    labels = construct_mark_targets_v1(plan=plan, roster=train, calendar=calendar, daily=train_daily,
                                      cutoff=plan.train_end, legacy=True)
    out = out.merge(labels, on=["package_id", *KEY], how="left", validate="one_to_one", sort=False)
    out["label_information_end"] = out[KEY[1]].map(lambda t: pd.Timestamp(calendar[positions[t.date()]+4])
        if positions[t.date()]+4 < len(calendar) else pd.NaT)
    evaluation = out[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))
    out.loc[evaluation, "mark_label_status"] = "NOT_EVALUATED_D_INPUT_ONLY"
    out.loc[evaluation, "mark_policy_sha256"] = MARK_POLICY_SHA256
    dcloses = daily.assign(trade_date=daily.trade_date.map(_day).map(pd.Timestamp)).set_index(["trade_date", "instrument"])
    out["reference_cny"] = [dcloses.raw_close_cny.get((d, s), np.nan) for d, _, s in out.loc[:, KEY].itertuples(index=False, name=None)]
    out["reference_visible_through"] = out[KEY[0]]
    out["label_cluster"] = [sha([d.date().isoformat(), t.date().isoformat(), s, MARK_POLICY_SHA256])
        for d, t, s in out.loc[:, KEY].itertuples(index=False, name=None)]
    if not out.loc[:, SOURCE_COLUMNS].equals(roster.loc[:, SOURCE_COLUMNS]):
        _fail("common input construction changed original roster order or identity")
    encoding = build_training_encoding_v1(rows=out, plan=plan)
    return out, encoding


def build_training_encoding_v1(*, rows, plan):
    """Median precedes supervision; both arms have exactly the same honest dates."""
    d, h = rows[KEY[0]], pd.to_datetime(rows.label_information_end)
    temporal = d.between(pd.Timestamp(plan.train_start), pd.Timestamp(plan.train_end)) & h.le(pd.Timestamp(plan.train_end))
    projected = rows.loc[temporal].copy()
    finance = [*FEATURES, "reference_cny", "terminal_mark_ratio", "path_mark_min_ratio", "mark_label_status",
        "mark_policy_sha256", "observed_gap_bps", "legacy_valuation_status", "legacy_valuation_policy_sha256", "label_information_end"]
    if not projected.legacy_valuation_policy_sha256.eq(VALUATION_POLICY_SHA256).all():
        _fail("known legacy sampling policy identity differs")
    if projected.groupby("label_cluster")[finance].nunique(dropna=False).gt(1).any().any():
        _fail("same economic cluster has contradictory D inputs, marks or source coordinates")
    domain = projected.drop_duplicates("label_cluster").sort_values(list(KEY))
    values, basic = feature_values(domain)
    measurable = domain.loc[basic]
    good_values = values[basic]
    medians = [float(np.median(v[~np.isnan(v)])) if (~np.isnan(v)).any() else 0. for v in good_values.T]
    support = _support(measurable)
    _, known = feature_values(projected)
    paired = projected.loc[:, ["terminal_mark_ratio", "path_mark_min_ratio"]].map(lambda v: _number(v, positive=True))
    available = projected.mark_label_status.eq("AVAILABLE")
    if (paired.loc[available].isna().any().any() or paired.loc[available].iloc[:, 1].gt(paired.loc[available].iloc[:, 0]).any()
            or not projected.loc[available, "mark_policy_sha256"].eq(MARK_POLICY_SHA256).all()):
        _fail("known mark supervision has missing/contradictory pairs or policy")
    candidate = available.to_numpy() & known
    matched = candidate & projected.legacy_valuation_status.eq("AVAILABLE").to_numpy() & projected.observed_gap_bps.map(
        lambda v: _number(v) is not None and support.contains(float(v))).to_numpy()
    roles, hashes, counts = {}, {}, {}
    for arm, member in zip(ARMS, (candidate, matched), strict=True):
        column = "pool_"+arm
        rows[column] = "NOT_TRAIN_SUPERVISION"
        selected = projected.loc[member]
        first = pd.Timestamp(plan.estimation_first_D)
        rows.loc[selected.index, column] = "PURGED_LABEL_OVERLAP"
        rows.loc[selected.index[selected[KEY[0]].ge(first)], column] = "ESTIMATION"
        rows.loc[selected.index[selected[KEY[0]].lt(first) & pd.to_datetime(selected.label_information_end).lt(first)], column] = "STRUCTURE"
        roles[arm] = {}
        for role in ("STRUCTURE", "ESTIMATION"):
            unique = rows.loc[rows[column].eq(role)].drop_duplicates("label_cluster").sort_values(list(KEY))
            roles[arm][role] = unique.label_cluster.tolist()
        hashes[arm] = sha(roles[arm])
        counts[arm] = {role: len(ids) for role, ids in roles[arm].items()}
    same = roles[ARMS[0]] == roles[ARMS[1]]
    empty = any(not n for arm_counts in counts.values() for n in arm_counts.values())
    status = ("UNKNOWN_EMPTY_HONEST_POOL" if empty else "NOT_IDENTIFIABLE_SAMPLING_CONTRAST"
              if same else "PREPARED_IDENTIFIABLE_NO_FIT")
    return dict(matrix_order=list(MATRIX_ORDER), medians=medians, intervals_bps=support.intervals_bps,
        train_start=plan.train_start.isoformat(), train_end=plan.train_end.isoformat(),
        estimation_first_D=plan.estimation_first_D.isoformat(), evaluation_start=plan.evaluation_start.isoformat(),
        evaluation_end=plan.evaluation_end.isoformat(), pools=counts, role_clusters=roles, role_sha256=hashes,
        contrast_status=status, physical_fit_count=0, median_unique_clusters=len(measurable),
        median_population="D_MEASURABLE_MATURE_TRAIN_BEFORE_TARGET_OR_ARM_SELECTION",
        added_candidate_clusters={role: sorted(set(roles[ARMS[0]][role])-set(roles[ARMS[1]][role])) for role in ("STRUCTURE", "ESTIMATION")})
