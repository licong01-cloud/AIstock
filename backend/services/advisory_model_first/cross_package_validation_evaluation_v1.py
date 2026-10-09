"""Frozen 5TD transfer: D inputs, T price queries, and separate H settlement."""
from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import time

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import (
    file_sha, publish_bytes, publish_json, readonly_connection,
)
from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import _parquet, checked_plan, prepare_daily_source
from backend.services.advisory_model_first.cross_package_validation_adapters_v1 import load_fixed5_units, query_frozen_fixed5
from backend.services.advisory_model_first.generic_daily_price_input_v1 import FEATURES, KEY, ROSTER
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY, POLICY_SHA256


def freeze_regimes(root):
    """Predefined D-only benchmark split; no stock profit or frontier inspection."""
    root = Path(root)
    plan = checked_plan(root)
    output = root/"regimes.json"
    if output.is_file():
        result = json.loads(output.read_text(encoding="utf-8"))
        if result["plan_sha256"] != file_sha(root/"plan.json"):
            raise ValueError("regime plan binding differs")
        return result
    calendar = [date.fromisoformat(d) for d in json.loads((root/"calendar.json").read_text(encoding="utf-8"))]
    start = calendar[calendar.index(date.fromisoformat(plan["decision_dates"][0]))-20]
    periods = ((date(2024, 6, 1), date(2025, 5, 30)), (start, date.fromisoformat(plan["decision_dates"][-1])))
    frames = []
    with readonly_connection() as conn:
        with conn.cursor() as cursor:
            for a, b in periods:
                cursor.execute("""SELECT c.cal_date,i.close FROM market.trading_calendar c
                    LEFT JOIN market.index_daily i ON i.trade_date=c.cal_date AND i.ts_code='000300.SH'
                    WHERE c.is_trading=TRUE AND c.cal_date BETWEEN %s AND %s ORDER BY c.cal_date""", (a, b))
                values = cursor.fetchall()
                if len(values) > 500 or len({r[0] for r in values}) != len(values):
                    raise ValueError("regime index original calendar duplicates/exceeds bound")
                series = pd.Series([np.nan if r[1] is None else float(r[1]) for r in values], index=[str(r[0]) for r in values])
                if np.isinf(series).any() or series.dropna().le(0).any():
                    raise ValueError("known benchmark price invalid")
                frames.append(pd.DataFrame(dict(ret20=series/series.shift(20)-1,
                    volatility20=series.pct_change(fill_method=None).rolling(20, min_periods=20).std(ddof=1))))
    historical = frames[0].loc["2024-07-04":"2025-05-30", "volatility20"].dropna()
    if len(historical) < 100:
        raise ValueError("predefined development volatility threshold has insufficient original support")
    threshold = float(historical.median())
    rows = []
    for day in plan["decision_dates"]:
        row = frames[1].loc[day]
        known = np.isfinite(row).all()
        rows.append(dict(decision_date=day, regime=("UP" if row.ret20 > 0 else "NONPOSITIVE")+"_"+(
            "HIGH_VOL" if row.volatility20 > threshold else "LOW_VOL") if known else "UNKNOWN_REGIME",
            ret20=None if pd.isna(row.ret20) else float(row.ret20),
            volatility20=None if pd.isna(row.volatility20) else float(row.volatility20)))
    result = dict(plan_sha256=file_sha(root/"plan.json"), threshold=threshold, threshold_development_days=len(historical),
        rows=rows, stock_outcomes_read=False, physical_fit_count=0, database_written=False)
    publish_json(output, result)
    return result


def _checked_aux(root, kind, daily_identity):
    base = Path(root)/"shared_aux"/kind
    receipt = json.loads((base/"receipt.json").read_text(encoding="utf-8"))
    if (receipt["plan_sha256"] != file_sha(Path(root)/"plan.json") or receipt["daily_source_sha256"] != daily_identity
            or file_sha(base/"features.parquet") != receipt["features_sha256"]):
        raise ValueError("frozen auxiliary D source binding differs")
    return pd.read_parquet(base/"features.parquet"), receipt["features_sha256"]


def freeze_fixed5_query_spec(root, *, original_source=None):
    root = Path(root)
    plan = checked_plan(root)
    daily = prepare_daily_source(root=root)
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    units, missing = [], []
    for item in inventory["inventory"]["items"]:
        if item["role"] != "FIXED_5TD_ENTRY":
            continue
        if original_source and item["family"] != original_source["family"]:
            continue
        try:
            loaded = load_fixed5_units(item, original_source=original_source)
        except LookupError as exc:
            missing.append(dict(model_id=item["model_id"], status="SOURCE_UNAVAILABLE", reason=str(exc)))
            continue
        units.extend(loaded)
    auxiliary = {kind: _checked_aux(root, kind, daily["new_source_identity_sha256"])[1]
        for kind in ("volume_minute", "ordered", "return_volume", "moneyflow")}
    specs = [dict(model_id=u.model_id, family=u.family, arm=u.arm, model_sha256=u.fitted.model_sha256,
        manifest_sha256=u.manifest_sha256, weight_sha256=u.weight_sha256) for u in units]
    result = dict(plan_sha256=file_sha(root/"plan.json"), source_identity=daily["new_source_identity_sha256"],
        auxiliary=auxiliary, units=specs, missing_units=missing, policy_sha256=POLICY_SHA256,
        slots=5, decision_use="NAVIGATION_ONLY", physical_fit_count=0, outcomes_read=False,
        target_price_use="T_OBSERVED_PRICE_SELECTS_FROZEN_D_FUNCTION_NO_H_INPUT")
    if original_source:
        result["original_source"] = original_source
        result["supplement_reason"] = "PREREGISTERED_UNIT_ORIGINAL_SOURCE_RECOVERED_NO_REFIT_OR_POLICY_CHANGE"
        result["other_units_same_window_outcomes_already_read"] = True
    publish_json(root/("fixed5_query_spec_selection_context.json" if original_source else "fixed5_query_spec.json"), result)
    return units, result


def _extend_quotes(root, *, until, destination, progress=None):
    """Reuse the original snapshot; append only missing later market sessions."""
    root = Path(root)
    plan = checked_plan(root)
    daily = prepare_daily_source(root=root)
    base = root/destination
    if (base/"receipt.json").exists():
        receipt = json.loads((base/"receipt.json").read_text(encoding="utf-8"))
        if (receipt["daily_source_sha256"] != daily["new_source_identity_sha256"] or receipt["until"] != str(until)
                or file_sha(base/"quotes.parquet") != receipt["quotes_sha256"]):
            raise ValueError("quote extension source/cutoff binding differs")
        return pd.read_parquet(base/"quotes.parquet"), receipt
    quotes = pd.read_parquet(root/"shared_daily/daily.parquet")
    calendar = [date.fromisoformat(d) for d in json.loads((root/"calendar.json").read_text(encoding="utf-8"))]
    last = date.fromisoformat(plan["decision_dates"][-1])
    future = [d for d in calendar if last < d <= until]
    evidence = None
    if future:
        from backend.services.advisory_model_first.generic_population_price_5td_pipeline_v1 import _read_database
        source, _, evidence = _read_database(symbols=sorted(set(quotes.instrument)), calendar=future, cutoff=until,
            connection_context_factory=readonly_connection, progress=progress)
        quotes = pd.concat([quotes, source], ignore_index=True)
    if quotes.duplicated(["trade_date", "instrument"]).any():
        raise ValueError("quote extension duplicated original snapshot rows")
    publish_bytes(base/"quotes.parquet", _parquet(quotes))
    receipt = dict(daily_source_sha256=daily["new_source_identity_sha256"], until=str(until),
        quotes_sha256=file_sha(base/"quotes.parquet"), appended_market_sessions=[str(d) for d in future],
        append_evidence=evidence, source_evidence="CURRENT_DATABASE_NON_VINTAGE", database_written=False)
    publish_json(base/"receipt.json", receipt)
    return quotes, receipt


def predict_fixed5_transfer(root, *, progress=None, original_source=None):
    root = Path(root)
    plan = checked_plan(root)
    freeze_regimes(root)
    units, spec = freeze_fixed5_query_spec(root, original_source=original_source)
    spec_path = root/("fixed5_query_spec_selection_context.json" if original_source else "fixed5_query_spec.json")
    features = pd.read_parquet(root/"shared_daily/features.parquet")
    # Original slot projection, fixed before any price/outcome is inspected.
    original = features.loc[features.selection_effective_rank.le(5)].copy().reset_index(drop=True)
    auxiliary = {}
    for kind in spec["auxiliary"]:
        frame, _ = _checked_aux(root, kind, spec["source_identity"])
        auxiliary[kind] = frame
    calendar = [date.fromisoformat(d) for d in json.loads((root/"calendar.json").read_text(encoding="utf-8"))]
    until = calendar[calendar.index(date.fromisoformat(plan["decision_dates"][-1]))+1]
    quotes, coordinate_receipt = _extend_quotes(root, until=until, destination="entry_coordinates", progress=progress)
    refs = pd.read_parquet(root/"shared_daily/references.parquet").set_index(list(KEY))
    bars = quotes.set_index(["trade_date", "instrument"])
    gaps = []
    for d, t, symbol in original.loc[:, KEY].itertuples(index=False, name=None):
        reference = refs.loc[(d, t, symbol)]
        bar = bars.loc[(t, symbol)] if (t, symbol) in bars.index else None
        fields = [reference.reference_cny, reference.d_anchor_factor]
        if bar is None or bar.suspended or bar.tradability_unknown:
            gaps.append(np.nan)
        elif not np.isfinite([*fields, bar.raw_open_cny, bar.adj_factor]).all() or min(*fields, bar.raw_open_cny, bar.adj_factor) <= 0:
            gaps.append(np.nan)
        else:
            gaps.append(10000*(bar.raw_open_cny*bar.adj_factor/reference.d_anchor_factor/reference.reference_cny-1))
    original["observed_gap_bps"] = gaps
    publish_bytes(root/"entry_coordinates/original_slot_coordinates.parquet", _parquet(original.loc[:, ["package_id", *ROSTER, "observed_gap_bps"]]))
    results = []
    source_for = {
        "advisory_generic_minute_price_5td_v1_20261006": ("volume_minute",),
        "advisory_generic_volume_path_price_5td_v1_20261006": ("volume_minute",),
        "advisory_generic_joint_distribution_price_5td_v1_20261007": ("volume_minute",),
        "advisory_generic_ordered_path_price_5td_v1_20261007": ("volume_minute", "ordered"),
        "advisory_generic_return_volume_price_5td_v1_20261007": ("return_volume",),
        "advisory_generic_moneyflow_price_5td_v1_20261008": ("moneyflow",),
    }
    for unit in units:
        started = time.monotonic()
        base = root/"fixed5_predictions"/unit.model_id/unit.arm
        if (base/"receipt.json").exists():
            receipt = json.loads((base/"receipt.json").read_text(encoding="utf-8"))
            if (receipt["spec_sha256"] != file_sha(spec_path)
                    or receipt["coordinate_source_sha256"] != coordinate_receipt["quotes_sha256"]
                    or receipt["model_sha256"] != unit.fitted.model_sha256
                    or receipt["model_id"] != unit.model_id or receipt["family"] != unit.family or receipt["arm"] != unit.arm
                    or receipt["physical_fit_count"] != 0 or receipt["H_outcomes_used"] is not False
                    or receipt["original_policy_preserved"] is not True
                    or file_sha(base/"predictions.parquet") != receipt["predictions_sha256"]):
                raise ValueError("frozen predictions checkpoint differs")
            results.append(receipt)
            continue
        query = original.loc[:, ["package_id", "source_id", *ROSTER, *FEATURES]].copy()
        for kind in source_for.get(unit.family, ()):
            query = query.merge(auxiliary[kind], on=list(KEY), how="left", validate="many_to_one", sort=False)
        if not query.loc[:, ["package_id", *ROSTER]].equals(original.loc[:, ["package_id", *ROSTER]]):
            raise ValueError("frozen inference auxiliary join changed original slots")
        if any(n in query for n in ("gross_terminal_ratio", "path_min_ratio", "label_status", "label_information_end")):
            raise ValueError("H outcome reached frozen predictor input")
        parts = []
        for first in range(0, len(query), 256):
            block = query.iloc[first:first+256].reset_index(drop=True)
            nodes = query_frozen_fixed5(unit, d_features=block, scenario_gap_bps=gaps[first:first+len(block)])
            if not nodes.loc[:, KEY].equals(block.loc[:, KEY]) or not nodes.model_sha256.eq(unit.fitted.model_sha256).all():
                raise ValueError("frozen output keys/model identity differs")
            # Some original pure query functions do not copy source/package metadata.
            nodes["package_id"] = block.package_id.to_numpy()
            nodes["selection_effective_rank"] = block.selection_effective_rank.to_numpy()
            parts.append(nodes)
        output = pd.concat(parts, ignore_index=True)
        publish_bytes(base/"predictions.parquet", _parquet(output))
        receipt = dict(model_id=unit.model_id, family=unit.family, arm=unit.arm, rows=len(output),
            spec_sha256=file_sha(spec_path), coordinate_source_sha256=coordinate_receipt["quotes_sha256"],
            model_sha256=unit.fitted.model_sha256, predictions_sha256=file_sha(base/"predictions.parquet"),
            status_counts={str(k): int(v) for k, v in output.status.value_counts().items()},
            physical_fit_count=0, H_outcomes_used=False, original_policy_preserved=True,
            elapsed_seconds=time.monotonic()-started)
        publish_json(base/"receipt.json", receipt)
        results.append(receipt)
        if progress:
            progress(dict(event="FROZEN_MODEL_TRANSFER_PREDICTED", **receipt))
    return results


def settle_fixed5_transfer(root, *, progress=None):
    """H values read only after every available frozen entry prediction is published."""
    from backend.services.advisory_model_first.generic_price_5td_labels_v1 import (
        PRICE_FIELDS, REFERENCE_FIELDS, build_generic_price_5td_labels_v1,
    )
    root = Path(root)
    plan = checked_plan(root)
    specs = [root/"fixed5_query_spec.json"]
    supplement = root/"fixed5_query_spec_selection_context.json"
    if supplement.is_file():
        specs.append(supplement)
    for path in specs:
        spec = json.loads(path.read_text(encoding="utf-8"))
        if (spec["plan_sha256"] != file_sha(root/"plan.json") or spec["policy_sha256"] != POLICY_SHA256
                or spec["physical_fit_count"] != 0 or spec["outcomes_read"] is not False):
            raise ValueError("settlement requires original frozen query plan/policy")
        for unit in spec["units"]:
            base = root/"fixed5_predictions"/unit["model_id"]/unit["arm"]
            receipt = json.loads((base/"receipt.json").read_text(encoding="utf-8"))
            if (receipt["spec_sha256"] != file_sha(path)
                    or receipt["model_sha256"] != unit["model_sha256"]
                    or receipt["physical_fit_count"] != 0 or receipt["H_outcomes_used"] is not False
                    or receipt["original_policy_preserved"] is not True
                    or file_sha(base/"predictions.parquet") != receipt["predictions_sha256"]):
                raise ValueError("settlement requires complete original frozen prediction bindings")
    calendar = [date.fromisoformat(d) for d in json.loads((root/"calendar.json").read_text(encoding="utf-8"))]
    quotes, source = _extend_quotes(root, until=date.fromisoformat(plan["settlement_cutoff"]),
        destination="settlement_coordinates", progress=progress)
    stock = quotes.set_index(["trade_date", "instrument"])
    refs = pd.read_parquet(root/"shared_daily/references.parquet")
    rosters = pd.read_parquet(root/"shared_daily/rosters.parquet")
    paths = []
    for d, group in refs.groupby(KEY[0], sort=True):
        pos = calendar.index(d.date())
        symbols = group.instrument.tolist()
        horizon = pd.to_datetime(calendar[pos+1:pos+6])
        path = stock.reindex(pd.MultiIndex.from_product((horizon, symbols), names=["trade_date", "instrument"]))
        path = path.loc[path.notna().any(axis=1)].reset_index()
        path[KEY[0]] = d
        factor = dict(zip(symbols, group.d_anchor_factor, strict=True))
        path["d_anchor_factor"] = path.adj_factor/path.instrument.map(factor)
        for name in ("open", "high", "low", "close"):
            path[name] = path["raw_"+name+"_cny"]
        paths.append(path.loc[:, PRICE_FIELDS])
    prices = pd.concat(paths, ignore_index=True)
    reference = refs.loc[:, REFERENCE_FIELDS]
    outputs, receipts = [], []
    decisions = [date.fromisoformat(d) for d in plan["decision_dates"]]
    for package_id, full in rosters.groupby("package_id", sort=False):
        selected = full.loc[:, ROSTER].copy()
        package_keys = selected.loc[:, KEY]
        package_price_keys = selected.loc[:, [KEY[0], KEY[2]]]
        labels, receipt = build_generic_price_5td_labels_v1(candidates=selected, decision_dates=decisions,
            calendar=calendar,
            prices=prices.merge(package_price_keys, on=[KEY[0], KEY[2]], validate="many_to_one"),
            references=reference.merge(package_keys, on=list(KEY), validate="one_to_one"),
            source_context=dict(calendar_sha256=plan["calendar_sha256"], prices_sha256=source["quotes_sha256"],
                references_sha256=file_sha(root/"shared_daily/references.parquet"),
                label_price_basis="D_REFERENCE_POLICY_RATIO", source_evidence="CURRENT_DATABASE_NON_VINTAGE"))
        if labels.label_information_end.dt.date.gt(date.fromisoformat(plan["settlement_cutoff"])).any():
            raise ValueError("settlement horizon extends beyond original declared cutoff")
        labels["package_id"] = package_id
        outputs.append(labels)
        receipts.append(dict(package_id=package_id, **receipt))
    output = pd.concat(outputs, ignore_index=True)
    publish_bytes(root/"fixed5_settlement/labels.parquet", _parquet(output))
    receipt = dict(spec_sha256=file_sha(root/"fixed5_query_spec.json"), packages=receipts,
        labels_sha256=file_sha(root/"fixed5_settlement/labels.parquet"), rows=len(output),
        physical_fit_count=0, sealed_read=False, database_written=False, H_outcomes_used_by_predictor=False)
    publish_json(root/"fixed5_settlement/receipt.json", receipt)
    return receipt


def account_original_fixed5_slots(*, labels, predictions, decision_dates, regimes):
    """No future-dependent roster filtering; five slots, UNKNOWN kept distinct."""
    original = labels.loc[labels.selection_effective_rank.le(5)].copy()
    original[KEY[0]] = pd.to_datetime(original[KEY[0]])
    original[KEY[1]] = pd.to_datetime(original[KEY[1]])
    keys = ["package_id", *KEY]
    if (original.duplicated(keys).any() or predictions.duplicated(keys).any()
            or set(predictions.loc[:, keys].itertuples(index=False, name=None)) != set(original.loc[:, keys].itertuples(index=False, name=None))
            or not original.policy_sha256.eq(POLICY_SHA256).all() or not predictions.policy_sha256.eq(POLICY_SHA256).all()):
        raise ValueError("fixed5 accounting original keys / policy differ")
    data = original.merge(predictions.loc[:, [*keys, "status", "expected_net_bps", "downside_q90_bps"]],
        on=keys, how="left", sort=False, validate="one_to_one")
    if not data.label_status.isin({"AVAILABLE", "UNKNOWN", "IMMATURE", "ENTRY_NOT_EXECUTABLE"}).all():
        raise ValueError("fixed5 accounting unknown label state")
    known_prediction = data.status.isin({"ACCEPTABLE", "AVOID"})
    if not (known_prediction | data.status.str.startswith("UNKNOWN_")).all():
        raise ValueError("fixed5 prediction action status differs")
    if (data.loc[known_prediction, ["expected_net_bps", "downside_q90_bps"]].isna().any().any()
            or not np.isfinite(data.loc[known_prediction, ["expected_net_bps", "downside_q90_bps"]]).all().all()
            or data.loc[known_prediction, "downside_q90_bps"].lt(0).any()
            or not data.loc[known_prediction, "status"].eq("ACCEPTABLE").equals(
                data.loc[known_prediction, "expected_net_bps"].gt(0) & data.loc[known_prediction, "downside_q90_bps"].le(POLICY["risk_bps"]))):
        raise ValueError("fixed5 prediction action arithmetic differs")
    net = 10000*(data.gross_terminal_ratio*(1-POLICY["sell_bps"]/10000)/(
        (1+data.observed_gap_bps/10000)*(1+POLICY["buy_bps"]/10000))-1)
    available = data.label_status.eq("AVAILABLE")
    if not np.isfinite(net[available]).all():
        raise ValueError("settled original AVAILABLE value is nonfinite")
    data["baseline_bps"] = net.where(available)
    data.loc[data.label_status.eq("ENTRY_NOT_EXECUTABLE"), "baseline_bps"] = 0.
    data["model_bps"] = data.baseline_bps.where(data.status.eq("ACCEPTABLE"), 0.)
    data.loc[data.label_status.eq("IMMATURE"), "model_bps"] = np.nan
    rule_take = data.observed_gap_bps.between(-300., 300.)
    data["rule_bps"] = data.baseline_bps.where(rule_take, 0.)
    data.loc[data.label_status.eq("IMMATURE"), "rule_bps"] = np.nan
    data["known_skip"] = available & data.status.eq("AVOID")
    data["unknown_skip"] = available & ~known_prediction
    data["increment_bps"] = data.model_bps-data.baseline_bps
    axis, episodes = [], []
    regime_map = {r["decision_date"]: r["regime"] for r in regimes}
    for package_id in sorted(labels.package_id.unique()):
        groups = {str(d.date()): g for d, g in data.loc[data.package_id.eq(package_id)].groupby(KEY[0], sort=False)}
        for day in decision_dates:
            g = groups.get(day)
            if g is None:
                axis.append(dict(package_id=package_id, decision_date=day, status="UNKNOWN_ORIGINAL_ROSTER",
                    baseline_bps=None, model_bps=None, rule_bps=None, increment_bps=None, known_interventions=0,
                    unknown_skips=0, label_unknown=0, regime=regime_map[day]))
                continue
            ranks = g.selection_effective_rank.tolist()
            if ranks != list(range(1, len(g)+1)) or len(g) > 5:
                raise ValueError("original slot ranks differ; no Top6 replacement allowed")
            def cohort(name):
                return float(g[name].sum()/5) if g[name].notna().all() else None
            axis.append(dict(package_id=package_id, decision_date=day, status="ORIGINAL_FIXED5_COHORT",
                baseline_bps=cohort("baseline_bps"), model_bps=cohort("model_bps"), rule_bps=cohort("rule_bps"),
                increment_bps=cohort("increment_bps"), known_interventions=int(g.known_skip.sum()),
                unknown_skips=int((~g.status.isin({"ACCEPTABLE", "AVOID"})).sum()),
                label_unknown=int(g.label_status.isin({"UNKNOWN", "IMMATURE"}).sum()), regime=regime_map[day]))
            for row in g.to_dict("records"):
                row[KEY[0]], row[KEY[1]] = str(row[KEY[0]].date()), str(row[KEY[1]].date())
                episodes.append(row)
    from backend.services.advisory_model_first.cross_package_validation_statistics_v1 import paired_skip_attribution
    day_frame, episode_frame = pd.DataFrame(axis), pd.DataFrame(episodes)
    paired_keys = set(day_frame.loc[day_frame.increment_bps.notna(), ["package_id", "decision_date"]].itertuples(index=False, name=None))
    episode_frame["paired_cohort_known"] = [(p,d) in paired_keys for p,d in zip(episode_frame.package_id,episode_frame[KEY[0]],strict=True)]
    complete = [(p,str(d.date())) in paired_keys for p,d in zip(data.package_id,data[KEY[0]],strict=True)]
    attributed = data.loc[available & pd.Series(complete,index=data.index)]
    attribution = paired_skip_attribution(baseline_net_bps=attributed.baseline_bps,
        model_net_bps=attributed.model_bps, known_skip=attributed.known_skip, unknown_skip=attributed.unknown_skip)
    attribution.update(population="COMPLETE_PAIRED_ORIGINAL_D_FIXED5_SLOTS",
        unpaired_known_episode_count=int(available.sum()-len(attributed)),paired_original_days=len(paired_keys))
    if not np.isclose(attribution["total_increment_bps"],day_frame.increment_bps.sum()*5,atol=1e-7,rtol=1e-12):
        raise ValueError("fixed5 attribution differs from paired cohort denominator")
    return day_frame, episode_frame, attribution


def evaluate_fixed5_transfer(root, *, progress=None, evaluation_attempt="v1"):
    """Report every package cell, synchronous original dates, no winner selection."""
    from backend.services.advisory_model_first.cross_package_validation_statistics_v1 import synchronous_block_inference
    root = Path(root)
    import re
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,40}", evaluation_attempt):
        raise ValueError("evaluation attempt must be a bounded token")
    plan = checked_plan(root)
    spec = json.loads((root/"fixed5_query_spec.json").read_text(encoding="utf-8"))
    settlement = json.loads((root/"fixed5_settlement/receipt.json").read_text(encoding="utf-8"))
    if (settlement["spec_sha256"] != file_sha(root/"fixed5_query_spec.json")
            or settlement["labels_sha256"] != file_sha(root/"fixed5_settlement/labels.parquet")):
        raise ValueError("fixed5 settlement identity differs")
    labels = pd.read_parquet(root/"fixed5_settlement/labels.parquet")
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    packages = [p for p in inventory["packages"] if p["disposition"] == "IN_MATRIX"]
    regimes = freeze_regimes(root)["rows"]
    bound_units = [(unit, file_sha(root/"fixed5_query_spec.json")) for unit in spec["units"]]
    missing_units = list(spec["missing_units"])
    supplemental_path = root/"fixed5_query_spec_selection_context.json"
    supplemental_sha = None
    if supplemental_path.is_file():
        supplemental = json.loads(supplemental_path.read_text(encoding="utf-8"))
        if (supplemental["plan_sha256"] != file_sha(root/"plan.json")
                or supplemental["source_identity"] != spec["source_identity"]
                or supplemental["policy_sha256"] != spec["policy_sha256"]
                or supplemental["physical_fit_count"] != 0):
            raise ValueError("supplemental original source plan/input/policy differs")
        recovered = {unit["model_id"] for unit in supplemental["units"]}
        if not recovered.issubset({unit["model_id"] for unit in missing_units}):
            raise ValueError("supplement may only recover preregistered missing units")
        supplemental_sha = file_sha(supplemental_path)
        bound_units.extend((unit, supplemental_sha) for unit in supplemental["units"])
        missing_units = [unit for unit in missing_units if unit["model_id"] not in recovered]
    comparisons, columns, daily_frames, episode_frames = [], [], [], []
    for unit, bound_spec_sha in bound_units:
        base = root/"fixed5_predictions"/unit["model_id"]/unit["arm"]
        receipt = json.loads((base/"receipt.json").read_text(encoding="utf-8"))
        if (receipt["spec_sha256"] != bound_spec_sha
                or receipt["model_sha256"] != unit["model_sha256"]
                or file_sha(base/"predictions.parquet") != receipt["predictions_sha256"]):
            raise ValueError("evaluation frozen prediction identity differs")
        predictions = pd.read_parquet(base/"predictions.parquet")
        day, episodes, attribution = account_original_fixed5_slots(labels=labels, predictions=predictions,
            decision_dates=plan["decision_dates"], regimes=regimes)
        day["model_id"], day["arm"] = unit["model_id"], unit["arm"]
        episodes["model_id"], episodes["arm"] = unit["model_id"], unit["arm"]
        daily_frames.append(day)
        episode_frames.append(episodes)
        for package in packages:
            selected = day.loc[day.package_id.eq(package["package_id"])].copy()
            item = dict(model_id=unit["model_id"], family=unit["family"], arm=unit["arm"],
                package_id=package["package_id"], model_sha256=unit["model_sha256"],
                objective_contract="RISK_MANAGED_ADVISORY", decision_use="NAVIGATION_ONLY",
                original_decision_days=len(plan["decision_dates"]), original_holding_clock=plan["clock"],
                new_fit_count=0, activation_evidence=False, nominal_NAV=None,
                NAV_unavailable_reason="OVERLAPPING_FIXED5_COHORTS_NOT_CAPITAL_LEDGER")
            if selected.empty:
                columns.append([np.nan]*len(plan["decision_dates"]))
                item.update(applicability="INPUT_UNAVAILABLE", effect_status="NOT_TESTABLE",
                    reason="NO_FROZEN_SOURCE_ROSTER_ON_PREREGISTERED_COMMON_WINDOW", paired_original_days=0)
            else:
                selected = selected.set_index("decision_date").reindex(plan["decision_dates"])
                columns.append(selected.increment_bps.to_numpy(dtype=float))
                part = episodes.loc[episodes.package_id.eq(package["package_id"])]
                known = part.label_status.eq("AVAILABLE") & part.paired_cohort_known
                action_changes = selected.known_interventions.gt(0) & selected.increment_bps.notna()
                regime_support = selected.loc[action_changes].groupby("regime").size().to_dict()
                stats = plan["statistics"]
                supports = dict(original_mature_days=int(selected.increment_bps.notna().sum()),
                    known_settled_interventions=int(part.loc[part.paired_cohort_known,"known_skip"].sum()),
                    intervention_days=int(action_changes.sum()), intervention_day_fraction=float(action_changes.mean()),
                    regime_intervention_days={str(k): int(v) for k, v in regime_support.items()})
                sufficient = (supports["original_mature_days"] >= stats["minimum_original_mature_days"]
                    and supports["known_settled_interventions"] >= stats["minimum_known_settled_interventions"]
                    and supports["intervention_day_fraction"] >= stats["minimum_intervention_day_fraction"]
                    and sum(n >= stats["minimum_intervention_days_per_regime"] for k, n in regime_support.items()
                            if k != "UNKNOWN_REGIME") >= stats["minimum_regimes"])
                from backend.services.advisory_model_first.cross_package_validation_statistics_v1 import paired_skip_attribution
                explanation = paired_skip_attribution(baseline_net_bps=part.loc[known, "baseline_bps"],
                    model_net_bps=part.loc[known, "model_bps"], known_skip=part.loc[known, "known_skip"],
                    unknown_skip=part.loc[known, "unknown_skip"])
                paired_days = int(selected.increment_bps.notna().sum())
                explanation.update(population="COMPLETE_PAIRED_ORIGINAL_D_FIXED5_SLOTS",paired_original_days=paired_days,
                    unpaired_known_episode_count=int((part.label_status.eq("AVAILABLE") & ~part.paired_cohort_known).sum()))
                if not np.isclose(explanation["total_increment_bps"],selected.increment_bps.sum()*5,atol=1e-7,rtol=1e-12):
                    raise ValueError("package attribution differs from paired cohort denominator")
                explanation["per_paired_day_fixed5_contribution_bps"] = {key:float(explanation[key]/(5*paired_days)) if paired_days else None
                    for key in ("known_avoided_loss_bps","known_missed_profit_bps","unknown_cash_increment_bps","other_action_increment_bps","total_increment_bps")}
                def mean(field):
                    # All compared means share the paired original D population.
                    values = selected.loc[selected.increment_bps.notna(), field].dropna()
                    return float(values.mean()) if len(values) else None
                item.update(applicability="APPLICABLE_READY", effect_status="PENDING_JOINT_INFERENCE",
                    paired_original_days=int(selected.increment_bps.notna().sum()), baseline_cohort_mean_bps=mean("baseline_bps"),
                    model_cohort_mean_bps=mean("model_bps"), rule_cohort_mean_bps=mean("rule_bps"),
                    descriptive_paired_increment_bps=mean("increment_bps"), intervention_support=supports,
                    intervention_support_sufficient=bool(sufficient), attribution=explanation,
                    baseline_known_episode_hit_rate=float(part.loc[known, "baseline_bps"].gt(0).mean()) if known.any() else None,
                    model_taken_known_episodes=int((known & part.status.eq("ACCEPTABLE")).sum()),
                    unknown_cash_slots=int((~part.status.isin({"ACCEPTABLE", "AVOID"})).sum()),
                    actual_label_status_counts={str(k): int(v) for k, v in part.label_status.value_counts().items()})
                if item["paired_original_days"] and not np.isclose(
                        item["model_cohort_mean_bps"]-item["baseline_cohort_mean_bps"], item["descriptive_paired_increment_bps"], atol=1e-8):
                    raise ValueError("paired cohort means fail economic reconciliation")
            comparisons.append(item)
    # Missing source/model cells remain p=1; no post-result reduction of family size.
    for missing in missing_units:
        for package in packages:
            columns.append([np.nan]*len(plan["decision_dates"]))
            comparisons.append(dict(model_id=missing["model_id"], arm="SOURCE_UNAVAILABLE", package_id=package["package_id"],
                applicability="SOURCE_UNAVAILABLE", effect_status="NOT_TESTABLE", reason=missing["reason"],
                objective_contract="RISK_MANAGED_ADVISORY", decision_use="NAVIGATION_ONLY", new_fit_count=0))
    inference = synchronous_block_inference(daily_values=np.asarray(columns).T,
        original_days=plan["decision_dates"], expected_days=plan["decision_dates"], block_span=5,
        replicates=plan["statistics"]["bootstrap_count"], seed=plan["statistics"]["seed"])
    for i, item in enumerate(comparisons):
        if item["effect_status"] == "NOT_TESTABLE":
            continue
        interval = inference["intervals"][i]
        item.update(simultaneous_increment_interval_95_bps=interval,
            raw_pvalue=inference["raw_pvalues"][i], holm_adjusted_pvalue=inference["adjusted_pvalues"][i],
            mde_80pct_family_approx_bps=inference["mde_80pct_family_approx_bps"][i])
        mean = item["descriptive_paired_increment_bps"]
        if interval is None or not item["intervention_support_sufficient"]:
            item["effect_status"] = "INSUFFICIENT_NEGATIVE" if mean is not None and mean < 0 else "INSUFFICIENT"
        elif interval[1] < 0:
            item["effect_status"] = "NEGATIVE"
        elif interval[0] > 0 and item["holm_adjusted_pvalue"] < .05:
            item["effect_status"] = "PACKAGE_SPECIFIC_CANDIDATE"
        else:
            item["effect_status"] = "INSUFFICIENT_NEGATIVE" if mean is not None and mean < 0 else "INSUFFICIENT"
    output = dict(plan_sha256=file_sha(root/"plan.json"), spec_sha256=file_sha(root/"fixed5_query_spec.json"),
        supplemental_spec_sha256=supplemental_sha,
        settlement_sha256=file_sha(root/"fixed5_settlement/receipt.json"), comparisons=comparisons, inference=inference,
        usable_package_count=len(packages), evaluated_package_count=int(labels.package_id.nunique()),
        full_cross_package_matrix_complete=False, physical_fit_count=0, sealed_read=False,
        activation_evidence=False, independent_OOS_evidence=False, nominal_NAV_claimed=False)
    destination = root/("fixed5_evaluated" if evaluation_attempt == "v1" else "fixed5_evaluated_"+evaluation_attempt)
    publish_bytes(destination/"daily.parquet", _parquet(pd.concat(daily_frames, ignore_index=True)))
    publish_bytes(destination/"episodes.parquet", _parquet(pd.concat(episode_frames, ignore_index=True)))
    publish_json(destination/"results.json", output)
    if progress:
        progress(dict(event="FROZEN_FIXED5_TRANSFER_EVALUATED", package_count=output["evaluated_package_count"],
            comparisons=len(comparisons), complete_statistical_columns=inference["complete_column_count"],
            physical_fit_count=0, independent_OOS_evidence=False))
    return output


def evaluate_joint_entry_exit(root, *, entry_attempt="review4", output_name="entry_exit_joint_review_v1.json"):
    """Same original-D inference across packages and roles, with separate policies."""
    from backend.services.advisory_model_first.cross_package_validation_statistics_v1 import synchronous_block_inference
    root = Path(root)
    plan = checked_plan(root)
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    packages = [p for p in inventory["packages"] if p["disposition"] == "IN_MATRIX"]
    source_refs, frames, descriptors, episodes, entry_cells = [], [], {}, {}, {}
    for folder in (root, root/"legacy_score_transfer_v1"):
        for role, relative in (("ENTRY", "fixed5_evaluated_"+entry_attempt), ("EXIT", "exit_transfer_v1")):
            path = folder/relative/"results.json"
            result = json.loads(path.read_text(encoding="utf-8"))
            daily_path = path.with_name("daily.parquet")
            source_refs.append(dict(role=role,path=str(path),sha256=file_sha(path),daily_sha256=file_sha(daily_path)))
            day = pd.read_parquet(daily_path)
            day["role"] = role
            if role == "EXIT":
                day["arm"] = "model"
            frames.append(day)
            for unit in result["comparisons"]:
                arm = unit.get("arm","model")
                descriptors.setdefault((role,unit["model_id"],arm),unit)
                if role == "ENTRY" and unit.get("paired_original_days",0)>0:
                    key = (unit["model_id"],arm,unit["package_id"])
                    if key in entry_cells:
                        raise ValueError("joint Entry source unexpectedly duplicates an evaluated cell")
                    entry_cells[key] = unit
                if role == "EXIT":
                    episode_path = folder/relative/unit["package_id"]/unit["model_id"]/"episodes.parquet"
                    if episode_path.is_file():
                        episodes[(unit["model_id"],unit["package_id"])] = pd.read_parquet(episode_path)
    daily = pd.concat(frames,ignore_index=True)
    identity = ["role","model_id","arm","package_id","decision_date"]
    if (daily.duplicated(identity).any() or set(daily.decision_date)-set(plan["decision_dates"])
            or set(daily.package_id)-{p["package_id"] for p in packages}):
        raise ValueError("joint original role/package/D identity differs")
    regimes = {v["decision_date"]:v["regime"] for v in freeze_regimes(root)["rows"]}
    columns, comparisons = [], []
    for (role,model_id,arm), unit in sorted(descriptors.items()):
        for package in packages:
            values = daily.loc[daily.role.eq(role)&daily.model_id.eq(model_id)&daily.arm.eq(arm)&daily.package_id.eq(package["package_id"])]
            axis = values.set_index("decision_date").reindex(plan["decision_dates"])
            increments = axis.increment_bps.to_numpy(dtype=float)
            columns.append(increments)
            known = np.isfinite(increments)
            item = dict(role=role,model_id=model_id,arm=arm,package_id=package["package_id"],
                paired_original_days=int(known.sum()),original_days=len(known),
                descriptive_increment_bps=float(increments[known].mean()) if known.any() else None,
                objective_contract="RISK_MANAGED_ADVISORY",decision_use="NAVIGATION_ONLY",activation_evidence=False)
            if role == "EXIT" and (model_id,package["package_id"]) in episodes:
                ep = episodes[(model_id,package["package_id"])]
                calendar = [date.fromisoformat(d) for d in json.loads((root/"calendar.json").read_text(encoding="utf-8"))]
                ep["decision_date"] = ep.entry_date.map(lambda d:str(calendar[calendar.index(pd.Timestamp(d).date())-1]))
                # Attribute only episodes on the same known paired cohort population.
                pair = ep.loc[ep.paired_known & ep.decision_date.isin(np.asarray(plan["decision_dates"])[known])]
                changed = pair.loc[pair.intervention]
                day_changes = sorted(set(changed.decision_date))
                regime_counts = pd.Series([regimes[d] for d in day_changes],dtype=str).value_counts().to_dict()
                base, candidate = pair.baseline_bps, pair.candidate_bps
                avoided = float(((-base).clip(lower=0)-(-candidate).clip(lower=0)).clip(lower=0).sum())
                added = float(((-candidate).clip(lower=0)-(-base).clip(lower=0)).clip(lower=0).sum())
                improved = float((candidate.clip(lower=0)-base.clip(lower=0)).clip(lower=0).sum())
                missed = float((base.clip(lower=0)-candidate.clip(lower=0)).clip(lower=0).sum())
                total = float((candidate-base).sum())
                if not np.isclose(avoided-added+improved-missed,total,atol=1e-8):
                    raise ValueError("Exit known paired intervention attribution does not reconcile")
                support = dict(known_settled_interventions=len(changed),intervention_days=len(day_changes),
                    intervention_day_fraction=len(day_changes)/len(known),regime_intervention_days=regime_counts)
                stats = plan["statistics"]
                item.update(intervention_support=support,intervention_support_sufficient=bool(
                    known.sum()>=stats["minimum_original_mature_days"]
                    and len(changed)>=stats["minimum_known_settled_interventions"]
                    and support["intervention_day_fraction"]>=stats["minimum_intervention_day_fraction"]
                    and sum(n>=stats["minimum_intervention_days_per_regime"] for k,n in regime_counts.items()
                            if k!="UNKNOWN_REGIME")>=stats["minimum_regimes"]),
                    attribution=dict(known_paired_episode_count=len(pair),total_increment_bps=total,
                        avoided_loss_bps=avoided,added_loss_bps=added,improved_profit_bps=improved,missed_profit_bps=missed,
                        population="COMPLETE_PAIRED_ORIGINAL_COHORT_EPISODE_SUM_NOT_NAV"))
            elif role == "ENTRY":
                match = entry_cells.get((model_id,arm,package["package_id"]))
                if match:
                    item.update({k:match[k] for k in ("intervention_support","intervention_support_sufficient","attribution")})
            item.setdefault("intervention_support_sufficient",False)
            comparisons.append(item)
    inference = synchronous_block_inference(daily_values=np.asarray(columns).T,original_days=plan["decision_dates"],
        expected_days=plan["decision_dates"],block_span=5,replicates=plan["statistics"]["bootstrap_count"],seed=plan["statistics"]["seed"])
    for position,item in enumerate(comparisons):
        ci = inference["intervals"][position]
        item.update(simultaneous_increment_interval_95_bps=ci,holm_adjusted_pvalue=inference["adjusted_pvalues"][position],
            mde_80pct_family_approx_bps=inference["mde_80pct_family_approx_bps"][position])
        mean = item["descriptive_increment_bps"]
        if not item["paired_original_days"]:
            item["effect_status"]="NOT_TESTABLE"
        elif ci is None or not item["intervention_support_sufficient"] or ci[0]<=0<=ci[1]:
            item["effect_status"]="INSUFFICIENT_NEGATIVE" if mean<0 else "INSUFFICIENT"
        elif ci[1]<0:
            item["effect_status"]="NEGATIVE"
        elif ci[0]>0 and item["holm_adjusted_pvalue"]<.05:
            item["effect_status"]="PACKAGE_SPECIFIC_CANDIDATE"
        else:
            item["effect_status"]="INSUFFICIENT"
    result = dict(plan_sha256=file_sha(root/"plan.json"),source_refs=source_refs,comparisons=comparisons,inference=inference,
        original_D_axis=plan["decision_dates"],same_D_resampling=True,nominal_NAV_claimed=False,
        complete_matrix_claimed=False,physical_fit_count=0,independent_OOS_evidence=False,activation_evidence=False,
        scope="FIXED5_ENTRY_AND_FIXED5_EXIT_ONLY_OTHER_REVIEW_AND_RANKING_ROLES_REMAIN_SEPARATE")
    publish_json(root/output_name,result)
    return result
