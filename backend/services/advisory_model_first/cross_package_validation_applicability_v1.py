"""Complete inventory-unit/package matrix with exact original schema evidence."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import file_sha, publish_json
from backend.services.advisory_model_first.cross_package_validation_inventory_v1 import _metadata_dates


NAMED_LEG_FIELDS = frozenset(("lstm_raw_score", "lstm_norm_score", "lstm_leg_rank", "lstm_weight",
    "fund_raw_score", "fund_norm_score", "fund_leg_rank", "fund_weight", "leg_norm_score_gap", "leg_rank_gap",
    "leg_direction_agreement", "parent_lstm_raw_score_D", "parent_fund_raw_score_D"))


def _feature_metadata(value):
    """Read declared columns from recipes/requests, never fitted trees or outcomes."""
    features = set()
    if not isinstance(value, dict):
        return features
    for key, child in value.items():
        if key in {"diagnostics", "metrics", "trees", "forest", "nodes", "test_predictions", "price_support", "feature_bounds"}:
            continue
        if (key in {"features", "feature_names", "model_feature_columns", "required_feature_columns",
                    "d_features", "candidate_names", "control_names"} or key.endswith("_features")):
            if isinstance(child, list) and all(isinstance(item, str) for item in child):
                features.update(child)
        if isinstance(child, dict):
            features.update(_feature_metadata(child))
    return features


def frozen_schema_evidence(item):
    """Read original schema/recipe or LightGBM header, not predictions or outcomes."""
    manifest = Path(item["manifest_ref"])
    if file_sha(manifest) != item["manifest_sha256"]:
        raise ValueError("applicability inventory manifest changed")
    features, required, references, windows = set(), set(), [], {}
    for name, descriptor in item["metadata"].items():
        path = manifest.parent.parent/name if name.startswith("preregistered/") else manifest.parent/name
        if file_sha(path) != descriptor["sha256"]:
            raise ValueError("original schema/window metadata changed")
        value = json.loads(path.read_text(encoding="utf-8"))
        features.update(_feature_metadata(value))
        references.append(dict(path=str(path), sha256=descriptor["sha256"]))
        windows.update({name+":"+key: date for key, date in _metadata_dates(value).items()})
        if name == "feature_schema.json":
            features.update(value.get("model_feature_columns", ()))
            required.update(value.get("required_feature_columns", ()))
        # Bounded one-hop parent. R2 children do not repeat the parent's date contract.
        ref = value.get("parent_plan_ref")
        if ref:
            parent = Path(ref["artifact_uri"])
            if (not parent.is_absolute() or parent.drive.upper() == "C:" or parent.stat().st_size > 1048576
                    or file_sha(parent) != ref["sha256"] or parent.stat().st_size != ref["size_bytes"]):
                raise ValueError("original parent plan identity differs")
            body = json.loads(parent.read_text(encoding="utf-8"))
            features.update(_feature_metadata(body))
            references.append(dict(path=str(parent), sha256=ref["sha256"]))
            windows.update({"original_parent:"+key: date for key, date in _metadata_dates(body).items()})
    for weight in item["weights"]:
        if weight["artifact_kind"] not in {"SERIALIZED_PREDICTOR", "PREDICTOR_WEIGHT", "AUXILIARY_METADATA"}:
            continue
        path = manifest.parent/weight["file"]
        if file_sha(path) != weight["sha256"]:
            raise ValueError("frozen predictor changed")
        if path.suffix == ".txt":
            with path.open(encoding="utf-8") as stream:
                for position, line in enumerate(stream):
                    if position >= 100 or line.startswith("Tree="):
                        break
                    if line.startswith("feature_names="):
                        features.update(line.strip().split("=", 1)[1].split())
        elif path.suffix == ".json":
            body = json.loads(path.read_text(encoding="utf-8"))
            features.update(_feature_metadata(body))
            windows.update({weight["file"]+":"+key: date for key, date in _metadata_dates(body).items()})
        references.append(dict(path=str(path), sha256=weight["sha256"]))
    named = sorted(features.intersection(NAMED_LEG_FIELDS))
    return dict(features=sorted(features), required_features=sorted(required), named_dual_leg_fields=named,
        original_date_metadata=windows, original_identity_refs=references,
        label_clock="ORIGINAL_POLICY_EPISODE_PER_MODEL_NOT_ASSUMED_FIVE_REVIEWS" if item["role"] == "REVIEW_CLOCK_ENTRY" else item["role"],
        outcomes_read=False)


def classify_inventory_pair(*, item, package, schema, source_state, executed):
    identity = dict(model_id=item["model_id"], family=item["family"], role=item["role"],
        package_id=package["package_id"], manifest_ref=item["manifest_ref"], manifest_sha256=item["manifest_sha256"],
        schema_evidence=schema, effect_status="NOT_TESTABLE", activation_evidence=False, physical_fit_count=0)
    if item["errors"] or item["predictor_artifact_count"] == 0:
        return {**identity, "applicability": "ARTIFACT_UNAVAILABLE",
            "reason": "NO_REUSABLE_FROZEN_TARGET_PREDICTOR_NOT_OOF_METADATA_OR_AUXILIARY_HMM",
            "missing_or_invalid_members": item["errors"], "derived_calibrator_present": item["derived_calibrator_present"]}
    if schema["named_dual_leg_fields"] and package["alpha_mode"] == "single_alpha":
        return {**identity, "applicability": "SEMANTIC_NOT_APPLICABLE",
            "reason": "ORIGINAL_MODEL_REQUIRES_NAMED_DUAL_ALPHA_INPUTS_SINGLE_ALPHA_HAS_NO_SUCH_VARIABLE",
            "missing_structural_fields": schema["named_dual_leg_fields"], "package_alpha_is_not_rejected": True}
    if executed:
        paired = any(row.get("paired_original_days", row.get("known_paired_days", 0)) > 0 for row in executed)
        return {**identity, "applicability": "APPLICABLE_READY",
            "effect_status": "EVALUATED_EXPLORATORY" if paired else "ATTEMPTED_NO_COMPLETE_PAIRED_DAY",
            "executed_arm_count": len(executed), "executed_results": executed}
    if source_state != "ORIGINAL_SOURCE_READY":
        return {**identity, "applicability": "SOURCE_UNAVAILABLE",
            "reason": "NO_EXISTING_ORIGINAL_ROSTER_ON_PREREGISTERED_COMMON_D_AXIS_CURRENT_DB_INFERENCE_WOULD_BE_A_DIFFERENT_SOURCE",
            "source_state": source_state}
    ranks = sorted(set(schema["features"]).intersection({"lstm_leg_rank", "fund_leg_rank", "leg_rank_gap"}))
    if ranks:
        return {**identity, "applicability": "INPUT_UNAVAILABLE",
            "reason": "ORIGINAL_FULL_UNIVERSE_NAMED_LEG_RANKS_ABSENT_CAPTURED_TOP50_RANK_IS_NOT_EQUIVALENT",
            "precise_missing_structural_inputs": ranks,
            "precise_input_requirements": schema["required_features"] or schema["features"],
            "full_universe_leg_ranks_reconstructed": False, "other_unprepared_fields_not_claimed_ready": True,
            "work_remaining": True}
    return {**identity, "applicability": "INPUT_UNAVAILABLE",
        "reason": "ORIGINAL_ROLE_SPECIFIC_INPUTS_AND_CLOCK_NOT_YET_PREPARED",
        "precise_input_requirements": schema["required_features"] or schema["features"],
        "requires_original_episode_clock": item["role"] in {"REVIEW_CLOCK_ENTRY", "RANKING_ADMISSION", "OUTCOME_HOLDING"},
        "work_remaining": True}


def build_applicability_matrix(root, *, result_refs, output_name="matrix_applicability_v1.json"):
    from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import checked_plan
    root = Path(root)
    plan = checked_plan(root)
    inventory = json.loads((root/"inventory_v4.json").read_text(encoding="utf-8"))
    packages = [p for p in inventory["packages"] if p["disposition"] == "IN_MATRIX"]
    executed = {}
    references = []
    for ref in result_refs:
        path = Path(ref)
        body = json.loads(path.read_text(encoding="utf-8"))
        references.append(dict(path=str(path), sha256=file_sha(path)))
        for row in body["comparisons"]:
            # A zero-complete-day review attempt is not a model failure or an
            # unexecuted unit. Its retained UNKNOWN actions remain evidence.
            if row.get("paired_original_days", 0) > 0 or "action_counts" in row or row.get("known_paired_days", 0) > 0:
                executed.setdefault((row["model_id"], row["package_id"]), []).append({**row, "result_ref": str(path)})
    sources, source_refs = {}, []
    for folder in (root, root/"legacy_score_transfer_v1"):
        path = folder/"shared_daily/receipt.json"
        if path.is_file():
            body = json.loads(path.read_text(encoding="utf-8"))
            roster_path = path.parent/"rosters.parquet"
            if body["files"][roster_path.name] != file_sha(roster_path):
                raise ValueError("matrix original roster snapshot changed")
            roster = pd.read_parquet(roster_path, columns=["package_id", "decision_as_of_trade_date"])
            source_refs.append(dict(receipt_ref=str(path), receipt_sha256=file_sha(path), roster_sha256=file_sha(roster_path),
                source_days={str(p):int(g.decision_as_of_trade_date.nunique()) for p,g in roster.groupby("package_id")}))
            for package_id in roster.package_id.unique():
                sources[package_id] = "ORIGINAL_SOURCE_READY"
    matrix = []
    for item in inventory["inventory"]["items"]:
        schema = frozen_schema_evidence(item)
        for package in packages:
            matrix.append(classify_inventory_pair(item=item, package=package, schema=schema,
                source_state=sources.get(package["package_id"], "SOURCE_NOT_PREPARED"),
                executed=executed.get((item["model_id"], package["package_id"]), [])))
    from collections import Counter
    result = dict(inventory_sha256=file_sha(root/"inventory_v4.json"), plan_sha256=file_sha(root/"plan.json"),
        result_refs=references, source_refs=source_refs, original_days=len(plan["decision_dates"]), items=matrix,
        usable_package_count=len(packages), inventory_unit_count=inventory["inventory"]["unique_inventory_unit_count"],
        model_count_claimed=False, pair_count=len(matrix), applicability_counts=dict(Counter(x["applicability"] for x in matrix)),
        full_matrix_resolved=not any(row.get("work_remaining") or row["applicability"] == "SOURCE_UNAVAILABLE" for row in matrix),
        physical_fit_count=0, activation_evidence=False)
    publish_json(root/output_name, result)
    return result


def summarize_economic_matrix(root, *, result_refs, matrix_filename, output_name="matrix_results_v1.json"):
    """Unify the result ledger, never pool incompatible episode clocks into NAV."""
    from collections import Counter
    import numpy as np
    from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import checked_plan, _parquet
    from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import publish_bytes
    from backend.services.advisory_model_first.cross_package_validation_statistics_v1 import package_axis_aggregates
    root = Path(root)
    plan = checked_plan(root)
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    packages = sorted((p for p in inventory["packages"] if p["disposition"] == "IN_MATRIX"), key=lambda p:p["package_id"])
    package_ids = [p["package_id"] for p in packages]
    parents = {p["package_id"]:p["source_type"]+":"+p["source_id"] for p in packages}
    cells, daily_frames, references = [], [], []
    for ref in result_refs:
        path = Path(ref)
        body = json.loads(path.read_text(encoding="utf-8"))
        daily_path = path.with_name("daily.parquet")
        frame = pd.read_parquet(daily_path)
        references.append(dict(path=str(path), sha256=file_sha(path), daily_sha256=file_sha(daily_path)))
        kind = "REVIEW_CLOCK_ENTRY" if body.get("role") == "REVIEW_CLOCK_ENTRY" else "EXIT" if "aggregate_inference_status" in body else "FIXED_5TD_ENTRY"
        frame["role"] = kind
        if "arm" not in frame:
            frame["arm"] = "ORIGINAL_FROZEN_FOLD"
        if kind == "REVIEW_CLOCK_ENTRY":
            # Different policy clocks are never included in fixed-session summaries.
            role_by_unit = {(row["model_id"],row["arm"]):row["policy_role"] for row in body["comparisons"]}
            frame["clock"] = [role_by_unit[(m,a)] for m,a in zip(frame.model_id,frame.arm,strict=True)]
        else:
            frame["clock"] = "ORIGINAL_T_U_E_FIXED5_EPISODE" if kind == "EXIT" else "GENERIC_ENTRY_FIXED_5TD_V1"
        for row in body["comparisons"]:
            known_days = row.get("paired_original_days", row.get("known_paired_days", 0))
            if not known_days and "action_counts" not in row:
                continue
            selected = frame.loc[frame.model_id.eq(row["model_id"]) & frame.arm.eq(row.get("arm","ORIGINAL_FROZEN_FOLD")) & frame.package_id.eq(row["package_id"])]
            clocks = selected.clock.unique()
            if len(clocks) != 1 or selected.decision_date.duplicated().any() or selected.empty:
                raise ValueError("matrix economic cell has no unique original clock/day axis")
            normalized = dict(model_id=row["model_id"],family=row["family"],arm=row.get("arm","ORIGINAL_FROZEN_FOLD"),
                package_id=row["package_id"],role=kind,clock=clocks[0],original_days=len(plan["decision_dates"]),
                known_paired_days=int(known_days),baseline_cohort_mean_bps=row.get("baseline_cohort_mean_bps"),
                candidate_cohort_mean_bps=row.get("candidate_cohort_mean_bps",row.get("model_cohort_mean_bps")),
                descriptive_increment_bps=row.get("descriptive_increment_bps",row.get("descriptive_paired_increment_bps")),
                effect_status=row.get("effect_status",row.get("status","INSUFFICIENT")),result_ref=str(path),
                activation_evidence=False,independent_OOS_evidence=False,not_a_portfolio_nav=True)
            if "action_counts" in row:
                normalized["known_action_count"] = sum(row["action_counts"].get(key,0) for key in ("ACCEPTABLE","AVOID"))
                normalized["known_node_pairs"] = row["known_node_pairs"]
            cells.append(normalized)
        daily_frames.append(frame)
    identities = [(row["model_id"],row["arm"],row["package_id"],row["clock"]) for row in cells]
    if len(set(identities)) != len(identities):
        raise ValueError("matrix result references duplicate an economic policy cell")
    daily = pd.concat(daily_frames,ignore_index=True)
    if daily.duplicated(["model_id","arm","package_id","clock","decision_date"]).any():
        raise ValueError("matrix result references duplicate daily observations")
    aggregates = []
    for (model,arm,clock), group in daily.groupby(["model_id","arm","clock"],sort=True):
        pivot = group.pivot(index="decision_date",columns="package_id",values="increment_bps").reindex(index=plan["decision_dates"],columns=package_ids)
        aggregate = package_axis_aggregates(daily_values=pivot.to_numpy(dtype=float),package_ids=package_ids,
            expected_packages=package_ids,source_groups=parents)
        known = aggregate["complete_day_count"]
        aggregates.append(dict(model_id=model,arm=arm,clock=clock,expected_package_count=len(package_ids),
            original_days=len(pivot),complete_paired_all_package_days=known,parent_cluster_count=aggregate["parent_cluster_count"],
            equal_package_increment_bps=float(np.nanmean(aggregate["equal_package_daily"])) if known else None,
            equal_parent_cluster_increment_bps=float(np.nanmean(aggregate["equal_parent_cluster_daily"])) if known else None,
            available_only_renormalization=False,status="DESCRIPTIVE_ONLY" if known else "ALL_PACKAGE_ESTIMAND_UNAVAILABLE",
            independent_package_count_claimed=False,activation_evidence=False))
    matrix_path = root/matrix_filename
    matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
    if matrix["plan_sha256"] != file_sha(root/"plan.json") or matrix["inventory_sha256"] != file_sha(root/plan["inventory_filename"]):
        raise ValueError("economic summary matrix plan/inventory identity changed")
    summary = dict(plan_sha256=file_sha(root/"plan.json"),matrix_ref=str(matrix_path),matrix_sha256=file_sha(matrix_path),
        result_refs=references,comparisons=cells,all_package_aggregates=aggregates,
        attempted_economic_cell_count=len(cells),roles=dict(Counter(row["role"] for row in cells)),
        complete_day_supported_cell_count=sum(row["known_paired_days"]>0 for row in cells),
        directional_point_counts=dict(Counter("NO_COMPLETE_PAIRED_DAY" if row["descriptive_increment_bps"] is None else "POSITIVE" if row["descriptive_increment_bps"]>0 else "NEGATIVE" if row["descriptive_increment_bps"]<0 else "ZERO" for row in cells)),
        usable_package_count=len(package_ids),package_count_with_any_paired_cell=len({row["package_id"] for row in cells if row["known_paired_days"]}),
        qualified_model_count=len({row["model_id"] for row in cells if row["effect_status"] in {"GENERALIZATION_CANDIDATE","PACKAGE_SPECIFIC_CANDIDATE"}}),
        applicability_counts=matrix["applicability_counts"],model_count_claimed=False,physical_fit_count=0,sealed_read=False,
        independent_OOS_evidence=False,activation_evidence=False,all_model_package_pairs_economically_evaluated=False,
        missing_packages_not_excluded=[p for p in package_ids if not any(c["package_id"]==p for c in cells)])
    publish_json(root/output_name,summary)
    publish_bytes(root/(Path(output_name).stem+"_cells.parquet"),_parquet(pd.DataFrame(cells)))
    return summary
