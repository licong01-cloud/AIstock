"""Revalue an existing frozen fixed5 study; no market query, prediction or fit."""
from __future__ import annotations

import argparse
import io
import json
import re
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.economic_entry_pipeline import (
    _json_bytes, file_sha256, publish_stage,
)
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY, POLICY_SHA256, ROSTER
from backend.services.advisory_model_first.generic_price_5td_labels_v1 import PRICE_FIELDS, REFERENCE_FIELDS
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import (
    VALUATION_POLICY, VALUATION_POLICY_SHA256, build_generic_price_5td_valuation_v2,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def _parquet(frame):
    stream = io.BytesIO()
    frame.to_parquet(stream, index=False)
    return stream.getvalue()


def account_frozen_fixed5_valuations(*, valuations, predictions, decision_dates):
    """Original five cash slots; unknown valuation is never a zero-return substitute."""
    keys = ["package_id", *KEY]
    if valuations.duplicated(keys).any() or predictions.duplicated(keys).any():
        raise ValueError("fixed5 revaluation duplicate original keys")
    if set(valuations.loc[:, keys].itertuples(index=False, name=None)) != set(
            predictions.loc[:, keys].itertuples(index=False, name=None)):
        raise ValueError("fixed5 revaluation must preserve every original Top5 key")
    if not predictions.status.map(lambda v: isinstance(v, str)
            and (v in {"ACCEPTABLE", "AVOID"} or v.startswith("UNKNOWN_"))).all():
        raise ValueError("fixed5 revaluation unknown original prediction status")
    if not predictions.policy_sha256.eq(POLICY_SHA256).all():
        raise ValueError("fixed5 original prediction policy changed")
    data = valuations.merge(predictions.loc[:, [*keys, "status", "policy_sha256"]].rename(
        columns={"policy_sha256": "prediction_policy_sha256"}), on=keys, validate="one_to_one", sort=False)
    identity = valuations.loc[:, [*keys, "selection_effective_rank", "candidate_group_size"]].merge(
        predictions.loc[:, [*keys, "selection_effective_rank", "candidate_group_size"]],
        on=keys, validate="one_to_one", suffixes=("_original", "_prediction"), sort=False)
    if any(not identity[name+"_original"].eq(identity[name+"_prediction"]).all()
           for name in ("selection_effective_rank", "candidate_group_size")):
        raise ValueError("fixed5 original prediction rank/group identity changed")
    if not data.valuation_policy_sha256.eq(VALUATION_POLICY_SHA256).all():
        raise ValueError("fixed5 valuation policy changed")
    data["model_valuation_bps"] = data.hypothetical_liquidation_net_bps.where(data.status.eq("ACCEPTABLE"), 0.)
    data["increment_bps"] = data.model_valuation_bps-data.hypothetical_liquidation_net_bps
    decisions = pd.to_datetime(decision_dates).tolist()
    if decisions != sorted(set(decisions)) or not set(data[KEY[0]]).issubset(decisions):
        raise ValueError("fixed5 revaluation original decision axis changed")
    rows = []
    for package_id in valuations.package_id.unique():
        groups = {d: g for d, g in data[data.package_id.eq(package_id)].groupby(KEY[0], sort=False)}
        for day in decisions:
            group = groups.get(day, data.iloc[:0])
            if group.selection_effective_rank.tolist() != list(range(1, len(group)+1)) or len(group) > 5:
                raise ValueError("fixed5 revaluation cannot replace slots or promote Top6")
            def cohort(field):
                return float(group[field].sum()/5) if group[field].notna().all() else None
            rows.append(dict(package_id=package_id, decision_date=day.date().isoformat(),
                original_slots=len(group), baseline_valuation_bps=cohort("hypothetical_liquidation_net_bps"),
                model_valuation_bps=cohort("model_valuation_bps"), increment_bps=cohort("increment_bps"),
                mark_to_market_after_buy_cost_bps=cohort("mark_to_market_net_bps"),
                unknown_valuation_slots=int(group.valuation_status.isin({"UNKNOWN", "IMMATURE"}).sum()),
                unknown_prediction_cash_slots=int(group.status.str.startswith("UNKNOWN_").sum()),
                exit_unproven_or_pending_slots=int(group.exit_execution_status.isin(
                    {"EXIT_UNPROVEN_LIMIT_DOWN", "PENDING_EXIT_SUSPENDED", "UNKNOWN_EXIT_LIMIT"}).sum())))
    return pd.DataFrame(rows), data


def revalue_frozen_cross_package_fixed5_v2(*, source_root, output_root):
    """Publish a new diagnostic lineage without writing anything into the source study."""
    source, output = Path(source_root).resolve(), Path(output_root).resolve()
    if (not Path(source_root).is_absolute() or not Path(output_root).is_absolute()
            or output.drive.upper() == "C:" or output.is_relative_to(source) or source.is_relative_to(output)):
        raise ValueError("fixed5 revaluation requires distinct absolute non-C source/output roots")
    bindings = {}
    def bind(relative, expected=None):
        path = source/relative
        if path.resolve() != path or not path.is_file():
            raise ValueError("fixed5 input is missing or resolves outside its immutable source path")
        actual = file_sha256(path)
        if expected is not None and expected != actual:
            raise ValueError(f"fixed5 frozen input hash changed: {relative}")
        bindings[relative] = actual
        return path
    def document(relative, expected=None):
        return json.loads(bind(relative, expected).read_text(encoding="utf-8"))
    plan = document("plan.json")
    if plan.get("study_type") != "EXPLORATORY_SCREEN" or plan.get("decision_use") != "NAVIGATION_ONLY":
        raise ValueError("fixed5 diagnostic only consumes an already exploratory development study")
    calendar = json.loads(bind("calendar.json", plan["calendar_sha256"]).read_text(encoding="utf-8"))
    shared = document("shared_daily/receipt.json")
    if shared["plan_sha256"] != bindings["plan.json"]:
        raise ValueError("fixed5 frozen source plan differs")
    rosters = pd.read_parquet(bind("shared_daily/rosters.parquet", shared["files"]["rosters.parquet"]))
    refs = pd.read_parquet(bind("shared_daily/references.parquet", shared["files"]["references.parquet"]))
    coordinate = document("settlement_coordinates/receipt.json")
    quotes = pd.read_parquet(bind("settlement_coordinates/quotes.parquet", coordinate["quotes_sha256"]))
    settlement = document("fixed5_settlement/receipt.json")
    original = pd.read_parquet(bind("fixed5_settlement/labels.parquet", settlement["labels_sha256"]))
    if (coordinate["until"] != plan["settlement_cutoff"] or not original.policy_sha256.eq(POLICY_SHA256).all()
            or quotes.duplicated(["trade_date", "instrument"]).any()
            or refs.duplicated(list(KEY)).any() or rosters.duplicated(["package_id", *KEY]).any()):
        raise ValueError("fixed5 original policy, horizon or source keys differ")
    price_rows = []
    lookup = quotes.set_index(["trade_date", "instrument"])
    days = pd.to_datetime(calendar).tolist()
    for d, group in refs.groupby(KEY[0], sort=False):
        pos = days.index(d)
        horizon = days[pos+1:pos+6]
        frame = lookup.reindex(pd.MultiIndex.from_product([horizon, group.instrument], names=["trade_date", "instrument"]))
        frame = frame.loc[frame.notna().any(axis=1)].reset_index()
        frame[KEY[0]] = d
        anchors = dict(zip(group.instrument, group.d_anchor_factor, strict=True))
        frame["d_anchor_factor"] = frame.adj_factor/frame.instrument.map(anchors)
        for field in ("open", "high", "low", "close"):
            frame[field] = frame["raw_"+field+"_cny"]
        price_rows.append(frame.loc[:, PRICE_FIELDS])
    prices = pd.concat(price_rows, ignore_index=True)
    valuations, receipts = [], []
    for package_id, group in rosters.groupby("package_id", sort=False):
        package_keys = group.loc[:, KEY]
        packet = dict(candidates=group.loc[:, ROSTER], decision_dates=plan["decision_dates"], calendar=calendar,
            prices=prices.merge(group.loc[:, [KEY[0], KEY[2]]], on=[KEY[0], KEY[2]], validate="many_to_one"),
            references=refs.loc[:, REFERENCE_FIELDS].merge(package_keys, on=list(KEY), validate="one_to_one"),
            source_context=dict(calendar_sha256=plan["calendar_sha256"], prices_sha256=coordinate["quotes_sha256"],
                references_sha256=shared["files"]["references.parquet"], label_price_basis="D_REFERENCE_POLICY_RATIO",
                source_evidence=coordinate["source_evidence"]))
        frame, receipt = build_generic_price_5td_valuation_v2(**packet)
        frame["package_id"] = package_id
        prior = original.loc[original.package_id.eq(package_id)].reset_index(drop=True)
        pd.testing.assert_frame_equal(frame.loc[:, prior.columns], prior)
        valuations.append(frame)
        receipts.append(dict(package_id=package_id, **receipt))
    all_values = pd.concat(valuations, ignore_index=True)
    if all_values.label_information_end.dt.date.gt(pd.Timestamp(plan["settlement_cutoff"]).date()).any():
        raise ValueError("fixed5 revaluation reads beyond the original horizon")
    top5 = all_values.loc[all_values.selection_effective_rank.le(5)].copy()
    daily_outputs, comparisons, seen = [], [], set()
    spec_names = ["fixed5_query_spec.json"]
    if (source/"fixed5_query_spec_selection_context.json").exists():
        spec_names.append("fixed5_query_spec_selection_context.json")
    for spec_name in spec_names:
        spec = document(spec_name)
        if (spec["plan_sha256"] != bindings["plan.json"] or spec["policy_sha256"] != POLICY_SHA256
                or spec["physical_fit_count"] != 0 or spec["outcomes_read"] is not False):
            raise ValueError("fixed5 original frozen query provenance differs")
        for unit in spec["units"]:
            model_id, arm = unit["model_id"], unit["arm"]
            if (not re.fullmatch("[a-f0-9]{64}", model_id)
                    or arm not in {"candidate", "matched", "candidate_transfer", "matched_anchor"}
                    or (model_id, arm) in seen):
                raise ValueError("fixed5 original model/arm identity is invalid or duplicated")
            seen.add((model_id, arm))
            prefix = f"fixed5_predictions/{model_id}/{arm}"
            receipt = document(prefix+"/receipt.json")
            if (receipt["spec_sha256"] != bindings[spec_name] or receipt["model_id"] != model_id
                    or receipt["arm"] != arm or receipt["family"] != unit["family"]
                    or receipt["model_sha256"] != unit["model_sha256"]
                    or receipt["physical_fit_count"] != 0 or receipt["H_outcomes_used"] is not False
                    or receipt["original_policy_preserved"] is not True):
                raise ValueError("fixed5 prediction does not match its original frozen query")
            predictions = pd.read_parquet(bind(prefix+"/predictions.parquet", receipt["predictions_sha256"]))
            if len(predictions) != receipt["rows"] or not predictions.model_sha256.eq(unit["model_sha256"]).all():
                raise ValueError("fixed5 frozen prediction rows or model differ")
            daily, _ = account_frozen_fixed5_valuations(valuations=top5, predictions=predictions,
                                                      decision_dates=plan["decision_dates"])
            daily["model_id"], daily["arm"] = model_id, arm
            daily_outputs.append(daily)
            for package_id, group in daily.groupby("package_id", sort=False):
                known = group.increment_bps.notna()
                comparisons.append(dict(package_id=package_id, model_id=model_id, family=unit["family"], arm=arm,
                    original_days=len(group), valuation_paired_days=int(known.sum()),
                    baseline_valuation_mean_bps=float(group.loc[known, "baseline_valuation_bps"].mean()) if known.any() else None,
                    model_valuation_mean_bps=float(group.loc[known, "model_valuation_bps"].mean()) if known.any() else None,
                    descriptive_valuation_increment_bps=float(group.loc[known, "increment_bps"].mean()) if known.any() else None,
                    decision_use="NAVIGATION_ONLY", activation_evidence=False, realized_profit_claimed=False))
    if not daily_outputs:
        raise ValueError("fixed5 revaluation has no frozen predictions; no success artifact")
    specification = dict(contract=VALUATION_POLICY, source_root=str(source), input_sha256=bindings,
        legacy_policy_sha256=POLICY_SHA256, valuation_policy_sha256=VALUATION_POLICY_SHA256,
        study_type="EXPLORATORY_SCREEN", decision_use="NAVIGATION_ONLY", outputs_seen_before_policy_revision=True,
        original_decision_dates=plan["decision_dates"], original_candidates_preserved=True,
        physical_fit_count=0, new_predictions=0, database_reads=0, database_written=False,
        sealed_holdout_read=False, activation_evidence=False, realized_profit_claimed=False,
        hypothetical_sell_fee_not_actual_paid_fee=True, source_evidence=coordinate["source_evidence"])
    result = dict(specification_sha256=sha(specification), comparisons=comparisons, package_receipts=receipts,
        economic_confirmation=False, independent_oos_evidence=False, nominal_NAV_claimed=False)
    for relative, expected in bindings.items():
        if file_sha256(source/relative) != expected:
            raise ValueError("fixed5 immutable input changed during revaluation")
    destination = publish_stage(study_root=output, stage="evaluated", plan_sha256=sha(specification),
        parent_sha256=bindings["fixed5_settlement/receipt.json"], artifacts={
            "specification.json": _json_bytes(specification), "results.json": _json_bytes(result),
            "valuations.parquet": _parquet(all_values), "daily.parquet": _parquet(pd.concat(daily_outputs, ignore_index=True)),
        })
    return destination, result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args()
    destination, result = revalue_frozen_cross_package_fixed5_v2(source_root=args.source_root, output_root=args.output_root)
    summary = dict(destination=str(destination), comparisons=len(result["comparisons"]), physical_fit_count=0,
        packages=[dict(package_id=p["package_id"], valuation_counts=p["valuation_counts"],
                       exit_execution_counts=p["exit_execution_counts"]) for p in result["package_receipts"]])
    print(_json_bytes(summary).decode("utf-8"))


if __name__ == "__main__":
    main()
