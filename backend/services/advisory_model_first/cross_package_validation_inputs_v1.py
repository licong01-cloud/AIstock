"""Read-only shared inputs, with original rosters and strict per-D projections."""
from __future__ import annotations

from datetime import date
import io
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import (
    canonical_sha, file_sha, publish_bytes, publish_json, readonly_connection,
)
from backend.services.advisory_model_first.generic_daily_price_input_v1 import (
    FEATURES, KEY, ROSTER, _day, build_generic_daily_price_input_v1,
)


def _parquet(frame):
    stream = io.BytesIO()
    frame.to_parquet(stream, index=False)
    return stream.getvalue()


def checked_plan(root):
    root = Path(root)
    plan = json.loads((root/"plan.json").read_text(encoding="utf-8"))
    for name in ("inventory", "source_metadata", "calendar"):
        if file_sha(root/plan[name+"_filename"]) != plan[name+"_sha256"]:
            raise ValueError(f"frozen {name} differs")
    days = [date.fromisoformat(d) for d in plan["decision_dates"]]
    if (days != sorted(set(days)) or not days or len(days) > 500 or plan["physical_fit_budget"] != 0
            or plan["sealed_read"] is not False or plan["decision_use"] != "NAVIGATION_ONLY"
            or any(date.fromisoformat(a) <= d <= date.fromisoformat(b)
                   for a, b in plan["existing_sealed_exclusions"] for d in days)):
        raise ValueError("frozen plan dates / evidence boundary differs")
    return plan


def register_frozen_plan(root):
    """Two objective records, no fictitious fit trials or result-dependent choice."""
    from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
    from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
    root = Path(root)
    plan = checked_plan(root)
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    package_count = sum(p["disposition"] == "IN_MATRIX" for p in inventory["packages"])
    records = []
    for objective in ("ALPHA_RANKING", "RISK_MANAGED_ADVISORY"):
        members = [item for item in plan["comparison_family_members"]
            if plan["objective_contracts"][item["role"]] == objective]
        # Budget is a bounded comparison count, not newly trained model count.
        record = build_trial_record(experiment_id=plan["experiment_id"]+"_"+objective.lower(),
            attempt_id="frozen_model_transfer_v1", research_stage="PREREGISTERED",
            study_type="EXPLORATORY_SCREEN", hypothesis_family_id="cross_package_frozen_model_validation_v1",
            parent_lineage=("advisory_blueprint_v4_91", plan["inventory_sha256"]),
            unique_variable="APPROVED_PACKAGE_CANDIDATE_SOURCE_AND_NEW_DEVELOPMENT_DATES_0_FIT",
            objective_contract=objective, dataset_identity=plan["source_metadata_sha256"],
            schema_identity=plan["inventory_sha256"], policy_identity=canonical_sha(plan["fixed5_policy"]),
            planned_trial_count=len(members)*package_count*2, generated_trial_count=0,
            evaluated_trial_count=0, selected_trial_count=0,
            consumed_windows=(ConsumedWindowV1(window_id="HISTORICAL_DEVELOPMENT_TRANSFER_NOT_CONFIRMATION",
                dataset_identity=plan["source_metadata_sha256"], start_date=plan["decision_dates"][0], end_date=plan["settlement_cutoff"]),),
            result_class="EXPLORATORY", decision_use="NAVIGATION_ONLY",
            evidence_refs=(evidence_reference_for_file(root/"plan.json", role="frozen_transfer_preregistration"),))
        records.append(record)
    return AdvisoryResearchTrialRegistryV1(root/"trial_registry.jsonl").append_batch(records)


def checked_current_canary_spec(root, *, spec_path, active_profile_path, require_finished=False):
    """Recover the declared missing-source batch, not a new date/model search."""
    from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import CANARY_DATES
    root,spec_path,profile = Path(root),Path(spec_path),Path(active_profile_path)
    plan = checked_plan(root)
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    packages = {p["package_id"]:p for p in inventory["packages"] if p["disposition"] == "IN_MATRIX"}
    ids = spec["package_ids"]
    if (spec_path.resolve().parent.parent.parent != root.resolve() or spec_path.parent.parent.name != "canary"
            or spec["parent_plan_sha256"] != file_sha(root/"plan.json")
            or spec["inventory_sha256"] != plan["inventory_sha256"]
            or Path(spec["profile_path"]).resolve() != profile.resolve() or spec["profile_sha256"] != file_sha(profile)
            or spec["decision_dates"] != [str(d) for d in CANARY_DATES]
            or spec["physical_fit_count"] != 0 or spec["all_original_D_axis_retained"] is not True
            or spec["consumer_runtime"] != "LOCAL_WSL_CPU_1_NO_GPU" or spec["other_role_outcomes_already_seen"] is not True
            or spec["source"] != "CURRENT_DATABASE_NON_VINTAGE_NOT_HISTORICAL_CAPTURE"
            or not ids or len(set(ids)) != len(ids) or not set(ids).issubset(packages)):
        raise ValueError("current source canary must preserve its declared source/profile/parent/date identity")
    if require_finished and any(not (spec_path.parent/p/(str(d)+".json")).is_file() for p in ids for d in CANARY_DATES):
        raise ValueError("current source preparation still pending; no H labels may be read")
    return spec,[packages[p] for p in ids]


def freeze_current_canary_study(parent_root, *, source_root, package_ids, child_name="missing_source_canary_v1"):
    """New source lineage; no old roster replacement, no trimming the 118D axis."""
    from backend.services.advisory_model_first.cross_package_validation_source_v1 import read_current_canary_view
    import re
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,60}", child_name):
        raise ValueError("current source child identity must be a bounded directory token")
    parent_root = Path(parent_root)
    parent_plan = checked_plan(parent_root)
    inventory = json.loads((parent_root/parent_plan["inventory_filename"]).read_text(encoding="utf-8"))
    packages = [p for p in inventory["packages"] if p["disposition"] == "IN_MATRIX"]
    if len(set(package_ids)) != len(package_ids) or not set(package_ids).issubset({p["package_id"] for p in packages}):
        raise ValueError("current source package set differs from the preregistered inventory")
    child = parent_root/child_name
    for name in (parent_plan["inventory_filename"],parent_plan["calendar_filename"]):
        publish_bytes(child/name,(parent_root/name).read_bytes())
    dates = [date.fromisoformat(day) for day in parent_plan["decision_dates"]]
    prepared, metadata = {}, []
    for package in packages:
        if package["package_id"] in package_ids:
            frame,receipt = read_current_canary_view(package=package,source_root=source_root,decision_dates=dates)
            if len(frame):
                prepared[package["package_id"]] = frame,receipt
                metadata.append(dict(package_id=package["package_id"],run_id=package.get("run_id"),
                    descriptor=receipt["descriptor"],status="CONSUMER_CURRENT_SCORE_AVAILABLE",original_source=receipt["original_source"]))
                continue
            reason = "READONLY_CURRENT_PREPARE_HAS_NO_VALID_SCORE_VIEW"
        else:
            reason = "OTHER_PACKAGE_SOURCES_REMAIN_IN_PARENT_RESULTS_NOT_COPIED_OR_COUNTED_TWICE"
        metadata.append(dict(package_id=package["package_id"],status="INPUT_UNAVAILABLE",reason=reason))
    publish_json(child/"source_metadata_current_canary_v1.json",dict(items=metadata,predictions_read=True,outcomes_read=False,physical_fit_count=0))
    plan = {**parent_plan,"experiment_id":parent_plan["experiment_id"]+"_"+child_name,
        "stage":"PREREGISTERED_CURRENT_SOURCE_CANARY_NOT_ORIGINAL_CAPTURE",
        "source_metadata_filename":"source_metadata_current_canary_v1.json",
        "source_metadata_sha256":file_sha(child/"source_metadata_current_canary_v1.json"),
        "parent_plan_sha256":file_sha(parent_root/"plan.json"),"parent_other_role_outcomes_already_seen":True,
        "current_source_packages":list(package_ids),"historical_original_receipt_created":False}
    publish_json(child/"plan.json",plan)
    sources = {source["package_id"]:source for source in metadata}
    for package_id,(frame,receipt) in prepared.items():
        base = child/"frozen_sources"/package_id
        publish_bytes(base/"roster.parquet",_parquet(frame))
        publish_json(base/"receipt.json",receipt)
        publish_json(base/"roster_binding.json",dict(plan_sha256=file_sha(child/"plan.json"),
            roster_sha256=file_sha(base/"roster.parquet"),receipt_sha256=file_sha(base/"receipt.json"),source_sha256=canonical_sha(sources[package_id])))
    register_frozen_plan(child)
    return child,dict(prepared_package_count=len(prepared),prepared_package_ids=sorted(prepared),source_evidence="CURRENT_DATABASE_NON_VINTAGE",
        original_D_axis_retained=len(dates),old_inputs_overwritten=False,physical_fit_count=0,outcomes_read=False)


def run_current_canary_fixed5(parent_root, *, child_root, active_profile_path, progress=None):
    """Checkpoint-resumable base then supplement, before any H labels are read."""
    from backend.services.advisory_model_first.cross_package_validation_evaluation_v1 import predict_fixed5_transfer, settle_fixed5_transfer, evaluate_fixed5_transfer
    parent_root, child_root = Path(parent_root), Path(child_root)
    parent = checked_plan(parent_root)
    child = checked_plan(child_root)
    if (child["parent_plan_sha256"] != file_sha(parent_root/"plan.json") or child["decision_dates"] != parent["decision_dates"]
            or child["historical_original_receipt_created"] is not False or child["parent_other_role_outcomes_already_seen"] is not True):
        raise ValueError("current canary child must preserve the original parent clock/evidence boundary")
    prepare_daily_source(root=child_root,progress=progress)
    prepare_auxiliary_features(root=child_root,active_profile_path=active_profile_path,progress=progress)
    original_source = json.loads((parent_root/"fixed5_query_spec_selection_context.json").read_text(encoding="utf-8"))["original_source"]
    # The supplement intentionally covers one recovered family, not the base
    # family. Never settle a supplement-only batch as if all arms had run.
    predict_fixed5_transfer(child_root,progress=progress)
    predict_fixed5_transfer(child_root,original_source=original_source,progress=progress)
    settle_fixed5_transfer(child_root,progress=progress)
    result = evaluate_fixed5_transfer(child_root,progress=progress,evaluation_attempt="canary_v1")
    from backend.services.advisory_model_first.cross_package_validation_exit_v1 import run_exit_transfer
    exit_source = json.loads((parent_root/"exit_query_spec_v1.json").read_text(encoding="utf-8"))["original_source"]
    run_exit_transfer(child_root,original_source=exit_source,progress=progress)
    return result


def load_original_rosters(root, plan):
    """Missing sources remain listed separately, not silently removed from scope."""
    calendar = [date.fromisoformat(d) for d in json.loads((Path(root)/plan["calendar_filename"]).read_text(encoding="utf-8"))]
    positions = {d: i for i, d in enumerate(calendar)}
    inventory = json.loads((Path(root)/plan["inventory_filename"]).read_text(encoding="utf-8"))
    metadata = json.loads((Path(root)/plan["source_metadata_filename"]).read_text(encoding="utf-8"))
    bound_sources = {item["package_id"]: item for item in metadata["items"]}
    frames, states = [], []
    for package in inventory["packages"]:
        if package["disposition"] != "IN_MATRIX":
            continue
        base = Path(root)/"frozen_sources"/package["package_id"]
        if not (base/"receipt.json").is_file() or not (base/"roster.parquet").is_file():
            states.append(dict(package_id=package["package_id"], status="SOURCE_REQUIRES_EXISTING_ROSTER_OR_CURRENT_DB"))
            continue
        receipt = json.loads((base/"receipt.json").read_text(encoding="utf-8"))
        source = bound_sources[package["package_id"]]
        binding = json.loads((base/"roster_binding.json").read_text(encoding="utf-8"))
        if (binding["plan_sha256"] != file_sha(Path(root)/"plan.json")
                or binding["roster_sha256"] != file_sha(base/"roster.parquet")
                or binding["receipt_sha256"] != file_sha(base/"receipt.json")
                or binding["source_sha256"] != canonical_sha(source)):
            raise ValueError("frozen original roster content binding differs")
        if (receipt["manifest_sha256"] != package["manifest_sha256"] or receipt["run_id"] != source["run_id"]
                or receipt["descriptor"] != source["descriptor"] or receipt["candidate_top_k"] != plan["candidate_top_k"]
                or receipt["package_id"] != package["package_id"] or receipt["declared_decision_days"] != len(plan["decision_dates"])):
            raise ValueError("frozen roster provenance differs")
        frame = pd.read_parquet(base/"roster.parquet")
        if set(frame.columns) != {"decision_date", "instrument", "score", "rank"}:
            raise ValueError("frozen original score projection differs")
        frame[KEY[0]] = frame.decision_date.map(_day).map(pd.Timestamp)
        frame[KEY[1]] = frame[KEY[0]].map(lambda d: pd.Timestamp(calendar[positions[d.date()]+1]))
        frame["selection_effective_rank"] = frame["rank"]
        frame["candidate_group_size"] = frame.groupby(KEY[0]).instrument.transform("size")
        declared = set(plan["decision_dates"])
        if (len(frame) != receipt["original_roster_rows"] or not frame[KEY[0]].dt.date.map(str).isin(declared).all()
                or frame.duplicated(list(KEY)).any() or not np.isfinite(frame.score).all()):
            raise ValueError("original roster count / keys / dates differs")
        from backend.services.advisory_model_first.generic_price_5td_labels_v1 import validate_roster
        validate_roster(frame.loc[:, ROSTER], [date.fromisoformat(d) for d in plan["decision_dates"]], calendar)
        expected_missing = sorted(declared-set(frame[KEY[0]].dt.date.map(str)))
        if expected_missing != receipt["missing_decision_dates"]:
            raise ValueError("original missing day declaration differs")
        frame["package_id"] = package["package_id"]
        frame["manifest_sha256"] = package["manifest_sha256"]
        frame["source_id"] = package["run_id"]
        frame["source_evidence"] = receipt["original_source"]
        frame["roster_sha256"] = file_sha(base/"roster.parquet")
        frames.append(frame.loc[:, [*ROSTER, "score", "package_id", "manifest_sha256", "source_id", "source_evidence", "roster_sha256"]])
        states.append(dict(package_id=package["package_id"], status="ORIGINAL_SOURCE_READY", rows=len(frame),
            missing_decision_dates=expected_missing, roster_sha256=file_sha(base/"roster.parquet")))
    if not frames:
        raise ValueError("no ready original source rosters; remaining sources need explicit resolution")
    return pd.concat(frames, ignore_index=True), states, calendar


def build_shared_daily_features(*, rosters, calendar, daily, index_daily):
    """Same stock/D computed once; future bars never enter the pure constructor."""
    positions = {d: i for i, d in enumerate(calendar)}
    keys = ["trade_date", "instrument"]
    raw = daily.copy()
    raw["trade_date"] = raw.trade_date.map(_day).map(pd.Timestamp)
    if raw.duplicated(keys).any() or rosters.duplicated(["package_id", *KEY]).any():
        raise ValueError("shared source or package original keys duplicate")
    stock = raw.set_index(keys).sort_index()
    index_daily = index_daily.loc[:, [*keys, "close"]].copy()
    index_daily["trade_date"] = index_daily.trade_date.map(_day).map(pd.Timestamp)
    unique = rosters.loc[:, KEY].drop_duplicates().sort_values(list(KEY)).reset_index(drop=True)
    features, references = [], []
    for d, full in unique.groupby(KEY[0], sort=True):
        pos = positions[d.date()]
        if pos < 19 or pos+1 >= len(calendar):
            raise ValueError("shared feature original D lacks required warmup/target calendar")
        sessions = calendar[pos-19:pos+2]
        symbols = full.instrument.tolist()
        anchor = stock.reindex(pd.MultiIndex.from_product(([d], symbols), names=keys))
        factors = dict(zip(symbols, anchor.adj_factor, strict=True))
        closes = dict(zip(symbols, anchor.raw_close_cny, strict=True))
        history = stock.reindex(pd.MultiIndex.from_product((pd.to_datetime(sessions[:-1]), symbols), names=keys)).reset_index()
        scale = history.adj_factor/history.instrument.map(factors)
        for name in ("open", "high", "low", "close"):
            history[name] = history["raw_"+name+"_cny"]*scale
        history["volume"] = history.volume_hand*100.
        bench = index_daily.loc[index_daily.trade_date.isin(pd.to_datetime(sessions[:-1]))]
        if history.trade_date.gt(d).any() or bench.trade_date.gt(d).any():
            raise ValueError("future quote reached D-only feature boundary")
        # Constructor budget is fifty; disjoint chunks are not a package re-ranking.
        for first in range(0, len(full), 50):
            original = full.iloc[first:first+50].copy().reset_index(drop=True)
            original["selection_effective_rank"] = np.arange(1, len(original)+1)
            original["candidate_group_size"] = len(original)
            panel = history.loc[history.instrument.isin(original.instrument), [*keys, "open", "high", "low", "close", "volume"]]
            block, _ = build_generic_daily_price_input_v1(candidates=original.loc[:, ROSTER], calendar=sessions,
                panel=panel, benchmark_daily=bench,
                market_state=dict(trade_date=d.date(), market_up_ratio=None, market_definition_id=None, visible_through=None),
                source_context=dict(package_id=None, run_id=None, list_version_id=None, universe_identity=None,
                    source_evidence="CURRENT_DATABASE_NON_VINTAGE", price_basis="D_ADJUSTED_CNY", volume_basis="RAW_SHARES",
                    source_visible_through=d.date(), benchmark_visible_through=d.date()))
            features.append(block.loc[:, [*KEY, *FEATURES]])
        ref = full.copy()
        ref["reference_cny"] = ref.instrument.map(closes)
        ref["d_anchor_factor"] = ref.instrument.map(factors)
        ref["reference_visible_through"] = d.date()
        references.append(ref)
    output = pd.concat(features, ignore_index=True)
    if output.duplicated(list(KEY)).any() or len(output) != len(unique):
        raise ValueError("shared feature computation lost original stock/D keys")
    joined = rosters.merge(output, on=list(KEY), validate="many_to_one", sort=False)
    if not joined.loc[:, ["package_id", *ROSTER]].equals(rosters.loc[:, ["package_id", *ROSTER]]):
        raise ValueError("shared feature join changed original package roster order")
    return joined, pd.concat(references, ignore_index=True)


def prepare_daily_source(*, root, progress=None):
    root = Path(root)
    plan = checked_plan(root)
    rosters, states, calendar = load_original_rosters(root, plan)
    destination = root/"shared_daily"
    if (destination/"receipt.json").exists():
        receipt = json.loads((destination/"receipt.json").read_text(encoding="utf-8"))
        if receipt["plan_sha256"] != file_sha(root/"plan.json") or receipt["source_states"] != states:
            raise ValueError("shared daily checkpoint source identity differs")
        for name, digest in receipt["files"].items():
            if file_sha(destination/name) != digest:
                raise ValueError("shared daily checkpoint member changed")
        return receipt
    start = calendar.index(date.fromisoformat(plan["decision_dates"][0]))-19
    end = calendar.index(date.fromisoformat(plan["decision_dates"][-1]))
    days = calendar[start:end+1]
    if start < 0:
        raise ValueError("shared daily insufficient pre-D history")
    from backend.services.advisory_model_first.generic_population_price_5td_pipeline_v1 import _read_database
    daily, indices, source = _read_database(symbols=sorted(set(rosters.instrument)), calendar=days,
        cutoff=days[-1], connection_context_factory=readonly_connection, progress=progress)
    features, references = build_shared_daily_features(rosters=rosters, calendar=calendar, daily=daily, index_daily=indices)
    files = dict()
    for name, frame in (("daily.parquet", daily), ("index_daily.parquet", indices),
                         ("rosters.parquet", rosters), ("features.parquet", features), ("references.parquet", references)):
        data = _parquet(frame)
        publish_bytes(destination/name, data)
        files[name] = file_sha(destination/name)
    receipt = dict(plan_sha256=file_sha(root/"plan.json"), source_states=states, files=files,
        original_roster_rows=len(rosters), shared_stock_D_rows=len(references), source=source,
        known_feature_counts={name: int(features[name].notna().sum()) for name in FEATURES},
        query_temporal_boundary="PER_D_ORIGINAL_20_SESSIONS", label_reader_ran=False, physical_fit_count=0,
        new_source_identity_sha256=canonical_sha(files), sealed_returns_read=False, database_written=False)
    publish_json(destination/"receipt.json", receipt)
    return receipt


def prepare_auxiliary_features(*, root, active_profile_path, progress=None):
    """Existing D-only minute/flow encoders; never refit or read future labels."""
    root = Path(root)
    checked_plan(root)
    daily_receipt = prepare_daily_source(root=root, progress=progress)
    features = pd.read_parquet(root/"shared_daily/features.parquet")
    roster = features.loc[:, KEY].drop_duplicates().sort_values(list(KEY)).reset_index(drop=True)
    daily = pd.read_parquet(root/"shared_daily/daily.parquet")
    calendar = [date.fromisoformat(d) for d in json.loads((root/"calendar.json").read_text(encoding="utf-8"))]
    profile_path = Path(active_profile_path)
    profile_sha = file_sha(profile_path)
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    from backend.services.advisory_model_first.generic_minute_price_5td_contracts_v1 import MinuteSourceIdentityV1
    identity = MinuteSourceIdentityV1(generation=profile["generation"],
        minute_root=str(Path(profile["controller_paths"]["candidate_root"])/"components/minute_bin_candidate"),
        pins=profile["components"]["minute_pins"])
    from backend.services.advisory_model_first.generic_volume_path_price_5td_source_v1 import read_d_volume_path_features_v1
    from backend.services.advisory_model_first.generic_ordered_path_price_5td_source_v1 import read_d_ordered_path_features_v1
    from backend.services.advisory_model_first.generic_return_volume_price_5td_source_v1 import build_return_volume_lag_features_v1
    from backend.services.advisory_model_first.economic_moneyflow_price_source_v1 import load_moneyflow_source_v1
    from backend.services.advisory_model_first.economic_moneyflow_price_v1 import moneyflow_rows_v1

    class ReadonlyMoneyflowSession:
        def connection(self):
            return readonly_connection()

        def close(self):
            pass  # Each connection context already rolls back and returns its lease.

    receipts = []
    for kind in ("volume_minute", "ordered", "return_volume", "moneyflow"):
        parts = []
        kind_receipts = []
        destination = root/"shared_aux"/kind
        existing = destination/"receipt.json"
        if existing.exists():
            receipt = json.loads(existing.read_text(encoding="utf-8"))
            if (receipt["plan_sha256"] != file_sha(root/"plan.json")
                    or receipt["daily_source_sha256"] != daily_receipt["new_source_identity_sha256"]
                    or receipt["profile_sha256"] != profile_sha or file_sha(destination/"features.parquet") != receipt["features_sha256"]):
                raise ValueError("auxiliary checkpoint source/code identity differs")
            receipts.append(receipt)
            continue
        for first in range(0, len(roster), 5000):
            original = roster.iloc[first:first+5000].reset_index(drop=True)
            if kind in {"volume_minute", "ordered"}:
                reader = read_d_volume_path_features_v1 if kind == "volume_minute" else read_d_ordered_path_features_v1
                block, evidence = reader(roster=original, identity=identity, active_profile_path=profile_path)
            elif kind == "return_volume":
                visible = original_key_daily_slice(roster=original, calendar=calendar, daily=daily)
                block, evidence = build_return_volume_lag_features_v1(roster=original, calendar=calendar,
                    prices=visible.loc[:, ["trade_date", "instrument", "raw_close_cny", "adj_factor"]],
                    volumes=visible.loc[:, ["trade_date", "instrument", "volume_hand"]])
            else:
                source = load_moneyflow_source_v1(candidates=original, session_factory=ReadonlyMoneyflowSession)
                block = moneyflow_rows_v1(candidates=original, amounts=source.amounts, calendar=source.calendar)
                evidence = source.receipt
            if not block.loc[:, KEY].equals(original.loc[:, KEY]):
                raise ValueError("auxiliary D-only source changed original keys")
            if file_sha(profile_path) != profile_sha:
                raise ValueError("active profile changed during auxiliary consumption")
            # Bind the existing source fingerprints without duplicating a new audit archive.
            summary = {k: v for k, v in evidence.items() if k != "slices"}
            summary["source_slice_evidence_sha256"] = canonical_sha(evidence)
            kind_receipts.append(summary)
            parts.append(block)
            if progress:
                progress(dict(event="AUXILIARY_D_INPUT_CHUNK", kind=kind, original_keys=len(original),
                    completed_keys=first+len(original), physical_fit_count=0, future_price_bars_decoded=0))
        frame = pd.concat(parts, ignore_index=True)
        if not frame.loc[:, KEY].equals(roster.loc[:, KEY]):
            raise ValueError("auxiliary D input lost shared stock/D population")
        publish_bytes(destination/"features.parquet", _parquet(frame))
        receipt = dict(kind=kind, profile_sha256=profile_sha, generation=profile["generation"],
            plan_sha256=file_sha(root/"plan.json"), daily_source_sha256=daily_receipt["new_source_identity_sha256"],
            features_sha256=file_sha(destination/"features.parquet"), original_keys=len(frame), chunks=kind_receipts,
            known_feature_counts={n: int(frame[n].notna().sum()) for n in frame if n not in KEY},
            physical_fit_count=0, sealed_returns_read=False, database_written=False)
        publish_json(existing, receipt)
        receipts.append(receipt)
    return receipts


def original_key_daily_slice(*, roster, calendar, daily):
    """Only the exact original 20-session requests, no global superset budget."""
    if len(roster) > 5000 or daily.duplicated(["trade_date", "instrument"]).any():
        raise ValueError("original auxiliary chunk/source keys differ")
    positions = {d: i for i, d in enumerate(calendar)}
    requests = set()
    for d, t, symbol in roster.loc[:, KEY].itertuples(index=False, name=None):
        pos = positions[_day(d)]
        if pos+1 >= len(calendar) or calendar[pos+1] != _day(t):
            raise ValueError("auxiliary original D/T calendar differs")
        requests.update((pd.Timestamp(day), symbol) for day in calendar[max(0, pos-19):pos+1])
    keys = daily.loc[:, ["trade_date", "instrument"]].copy()
    keys["trade_date"] = keys.trade_date.map(_day).map(pd.Timestamp)
    mask = pd.MultiIndex.from_frame(keys).isin(list(requests))
    return daily.loc[mask].copy()
