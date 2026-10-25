"""Cross-package frozen validation: metadata and readonly inference only."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import tempfile

from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import (
    CANARY_DATES, NEW_PACKAGES, SCHEMA, canonical_sha, publish_json, real_root,
)
from backend.services.advisory_model_first.cross_package_validation_inventory_v1 import inventory_frozen_models


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("inventory", "canary", "current-source-prepare", "current-source-evaluate", "source-metadata", "source-rosters", "preregister", "daily-inputs", "aux-inputs", "fixed5-predict", "fixed5-settle", "fixed5-evaluate", "review-predict", "review-settle", "review-evaluate", "matrix-summary"))
    parser.add_argument("--model-root", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--temp-root", required=True)
    parser.add_argument("--api-base", default="http://127.0.0.1:8001/api/v1")
    parser.add_argument("--env-file")
    parser.add_argument("--attempt", default="attempt1")
    parser.add_argument("--inventory-tag", default="inventory")
    parser.add_argument("--plan")
    parser.add_argument("--active-profile")
    parser.add_argument("--source-spec")
    parser.add_argument("--package-id")
    parser.add_argument("--review-batch", choices=("core", "information", "sector", "economic", "breadth", "scale"), default="core")
    parser.add_argument("--result-ref", action="append", default=[])
    parser.add_argument("--local-cpu-only", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,40}", args.attempt):
        raise ValueError("attempt identity must be a bounded directory token")
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,40}", args.inventory_tag):
        raise ValueError("inventory identity must be a bounded directory token")
    if args.env_file:
        from dotenv import load_dotenv
        env_file = Path(args.env_file).resolve(strict=True)
        if not env_file.is_file():
            raise ValueError("explicit local environment file missing")
        load_dotenv(env_file, override=False)
    if args.operation in {"canary", "current-source-prepare", "current-source-evaluate", "source-rosters", "daily-inputs", "aux-inputs", "fixed5-predict", "fixed5-settle", "review-predict", "review-settle"} and not os.getenv("TDX_DB_PASSWORD"):
        raise ValueError("canary needs existing DB credential environment; pass its non-secret --env-file location")
    temp = real_root(args.temp_root, required_drive="X:")
    temp.mkdir(parents=True, exist_ok=True)
    os.environ.update(TEMP=str(temp), TMP=str(temp), TMPDIR=str(temp), PYTHONDONTWRITEBYTECODE="1")
    tempfile.tempdir = str(temp)
    root = real_root(args.output_root)
    model_root = real_root(args.model_root)
    if root == model_root or root in model_root.parents or model_root in root.parents:
        raise ValueError("new study output must be disjoint from frozen model root")
    from backend.mcp.common import AIstockApiClient
    api = AIstockApiClient(args.api_base, timeout=30, max_response_bytes=1048576)
    def progress(value):
        print(json.dumps(value, ensure_ascii=False, allow_nan=False), flush=True)
    if args.operation == "inventory":
        response = api.get("/strategy-packages", params=dict(view="summary", limit=100))
        if response.get("ok") is not True or not isinstance(response.get("packages"), list):
            raise ValueError("public package summary incomplete")
        if len(response["packages"]) == 100:
            raise ValueError("public inventory may be truncated")
        packages = [{key: package[key] for key in ("package_id", "package_status", "manifest_sha256", "source_type",
            "source_id", "loop_id", "run_id", "alpha_mode", "portfolio_topk")} for package in response["packages"]]
        if len({p["package_id"] for p in packages}) != len(packages):
            raise ValueError("public package inventory duplicates")
        for package in packages:
            package["disposition"] = "RETIRED_NOT_RUN" if package["package_status"] == "RETIRED" else "IN_MATRIX"
        inventory = inventory_frozen_models(model_root)
        result = dict(schema_version=SCHEMA, captured_at=datetime.now(timezone.utc).isoformat(), packages=packages,
            inventory=inventory, stage="METADATA_INVENTORY", study_type="EXPLORATORY_SCREEN",
            decision_use="NAVIGATION_ONLY", database_written=False, new_fit_count=0, sealed_read=False,
            outcomes_read=False, full_matrix_complete=False)
        publish_json(root/f"{args.inventory_tag}.json", result)
        progress(dict(stage="METADATA_INVENTORY", output_ref=str(root/f"{args.inventory_tag}.json"), package_count=len(packages),
            usable_package_count=sum(p["disposition"] == "IN_MATRIX" for p in packages),
            manifest_count=inventory["manifest_count"], unique_inventory_unit_count=inventory["unique_inventory_unit_count"],
            inventory_sha256=canonical_sha(result), physical_fit_count=0))
        return
    from backend.services.advisory_model_first.cross_package_validation_source_v1 import make_readonly_signal_service, prepare_package_dates
    from backend.services.advisory_model_first.generic_population_price_5td_cli_v1 import public_qe_observation_v1
    inventory = json.loads((root/f"{args.inventory_tag}.json").read_text(encoding="utf-8"))
    packages = {p["package_id"]: p for p in inventory["packages"]}
    if args.operation in {"current-source-prepare","current-source-evaluate"}:
        from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import checked_current_canary_spec,freeze_current_canary_study,run_current_canary_fixed5
        if not args.source_spec or not args.active_profile:
            raise ValueError("current-source recovery needs explicit --source-spec and --active-profile")
        spec,selected = checked_current_canary_spec(root,spec_path=args.source_spec,active_profile_path=args.active_profile,
            require_finished=args.operation == "current-source-evaluate")
        source_root = Path(args.source_spec).parent
        if args.operation == "current-source-prepare":
            from backend.services.advisory_model_first.cross_package_validation_source_v1 import local_cpu_resource_observation
            service = make_readonly_signal_service(temp_root=temp,repo_root=Path.cwd(),local_cpu_only=True)
            for package in selected:
                records = prepare_package_dates(service=service,package=package,decision_dates=CANARY_DATES,
                    output_root=source_root,observe_resources=lambda:local_cpu_resource_observation(api_base=args.api_base,temp_root=temp),progress=progress)
                checked_current_canary_spec(root,spec_path=args.source_spec,active_profile_path=args.active_profile)
                if len(records) != len(CANARY_DATES):
                    progress(dict(event="CURRENT_SOURCE_PREPARATION_PENDING",package_id=package["package_id"],outcomes_read=False))
                    return
        else:
            child,receipt = freeze_current_canary_study(root,source_root=source_root,package_ids=spec["package_ids"])
            progress(dict(event="CURRENT_SOURCE_CHILD_READY",**receipt))
            if receipt["prepared_package_count"]:
                run_current_canary_fixed5(root,child_root=child,active_profile_path=args.active_profile,progress=progress)
        return
    if args.operation == "matrix-summary":
        if not args.result_ref:
            raise ValueError("matrix-summary requires every explicit --result-ref; no artifact-root scan")
        from backend.services.advisory_model_first.cross_package_validation_applicability_v1 import build_applicability_matrix, summarize_economic_matrix
        matrix_name = "matrix_applicability_"+args.attempt+".json"
        matrix = build_applicability_matrix(root, result_refs=args.result_ref, output_name=matrix_name)
        result = summarize_economic_matrix(root, result_refs=args.result_ref, matrix_filename=matrix_name,
            output_name="matrix_results_"+args.attempt+".json")
        progress(dict(event="MATRIX_SUMMARY_READY", pair_count=matrix["pair_count"],
            economic_cells=result["attempted_economic_cell_count"], qualified_models=result["qualified_model_count"], physical_fit_count=0))
        return
    if args.operation in {"review-predict", "review-settle", "review-evaluate"}:
        if args.package_id not in packages or packages[args.package_id]["disposition"] != "IN_MATRIX":
            raise ValueError("review operation needs an explicit usable inventory --package-id")
        from backend.services.advisory_model_first.cross_package_validation_review_v1 import predict_original_core_review, settle_original_core_review, evaluate_original_core_review
        from backend.services.advisory_model_first.cross_package_validation_review_models_v1 import predict_original_information_review
        batches = {
            "information":("information_inputs_v1", None),
            "sector":("sector_information_inputs_v1", ("M1","M19","M22","M25","CONTEXT")),
            "economic":("information_inputs_v1", ("ECONOMIC_V1",)),
            "breadth":("breadth_information_inputs_v1", ("M10",)),
            "scale":("scale_information_inputs_v1", ("M21",)),
        }
        batch = args.review_batch
        if args.operation == "review-settle":
            if batch != "core":
                raise ValueError("review settlement has only the two frozen core policy clocks")
            settle_original_core_review(root,package_id=args.package_id,progress=progress)
        elif args.operation == "review-predict":
            if batch == "core":
                predict_original_core_review(root,package_id=args.package_id,progress=progress)
            else:
                directory, models = batches[batch]
                predict_original_information_review(root,package_id=args.package_id,progress=progress,
                    input_directory=directory,scope_models=models,query_spec_filename=batch+"_query_spec_v1.json",
                    prediction_directory=batch+"_predictions")
        else:
            evaluate_original_core_review(root,package_id=args.package_id,progress=progress,
                query_spec_filename=batch+"_query_spec_v1.json",prediction_directory=batch+"_predictions",
                destination_name=batch+"_evaluated_"+args.attempt)
        progress(dict(event="ORIGINAL_REVIEW_OPERATION_COMPLETE", operation=args.operation, batch=batch, physical_fit_count=0))
        return
    if args.operation in {"fixed5-settle", "fixed5-evaluate"}:
        from backend.services.advisory_model_first.cross_package_validation_evaluation_v1 import settle_fixed5_transfer, evaluate_fixed5_transfer
        result = settle_fixed5_transfer(root, progress=progress) if args.operation == "fixed5-settle" else evaluate_fixed5_transfer(root, progress=progress, evaluation_attempt=args.attempt)
        progress(dict(event="FIXED5_OPERATION_COMPLETE", operation=args.operation, physical_fit_count=0,
            result_ref=str(root/("fixed5_settlement/receipt.json" if args.operation == "fixed5-settle" else "fixed5_evaluated_"+args.attempt+"/results.json"))))
        return
    if args.operation == "fixed5-predict":
        from backend.services.advisory_model_first.cross_package_validation_evaluation_v1 import predict_fixed5_transfer
        result = predict_fixed5_transfer(root, progress=progress)
        progress(dict(event="FROZEN_FIXED5_TRANSFER_PREDICTIONS_READY", units=len(result), physical_fit_count=0, H_outcomes_used=False))
        return
    if args.operation == "aux-inputs":
        if not args.active_profile:
            raise ValueError("D-only auxiliary inputs need explicit current --active-profile")
        from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import prepare_auxiliary_features
        receipts = prepare_auxiliary_features(root=root, active_profile_path=args.active_profile, progress=progress)
        progress(dict(event="AUXILIARY_D_INPUTS_READY", kinds=[r["kind"] for r in receipts], physical_fit_count=0))
        return
    if args.operation == "preregister":
        from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import register_frozen_plan
        progress(dict(event="PLAN_REGISTERED", registry=register_frozen_plan(root), physical_fit_count=0))
        return
    if args.operation == "daily-inputs":
        from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import prepare_daily_source
        receipt = prepare_daily_source(root=root, progress=progress)
        progress(dict(event="SHARED_DAILY_INPUTS_READY", original_rows=receipt["original_roster_rows"],
            shared_stock_D_rows=receipt["shared_stock_D_rows"], known_feature_counts=receipt["known_feature_counts"],
            label_reader_ran=False, physical_fit_count=0))
        return
    if args.operation == "source-metadata":
        from backend.services.advisory_model_first.cross_package_validation_source_v1 import frozen_prediction_metadata
        items = [frozen_prediction_metadata(api=api, package=package) for package in packages.values()
            if package["disposition"] == "IN_MATRIX"]
        result = dict(schema_version=SCHEMA, items=items, source_identity_sha256=canonical_sha(items),
            outcomes_read=False, predictions_read=False, physical_fit_count=0)
        publish_json(root/f"source_metadata_{args.attempt}.json", result)
        progress(dict(event="SOURCE_METADATA_COMPLETE", items=[{k: v for k, v in item.items()
            if k in {"package_id", "status", "reason", "run_id"}} for item in items]))
        return
    if args.operation == "source-rosters":
        from datetime import date
        import io
        from backend.services.advisory_model_first.cross_package_validation_source_v1 import read_frozen_prediction_view
        from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import file_sha, publish_bytes
        if not args.plan:
            raise ValueError("source-rosters requires a frozen explicit --plan")
        plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
        if (plan["inventory_sha256"] != file_sha(root/f"{args.inventory_tag}.json")
                or plan["decision_use"] != "NAVIGATION_ONLY" or plan["physical_fit_budget"] != 0
                or plan["sealed_read"] is not False):
            raise ValueError("source-rosters frozen plan differs")
        metadata_path = root/plan["source_metadata_filename"]
        if file_sha(metadata_path) != plan["source_metadata_sha256"]:
            raise ValueError("source metadata differs from frozen plan")
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        dates = [date.fromisoformat(d) for d in plan["decision_dates"]]
        for source in metadata["items"]:
            if source["status"] != "FROZEN_SCORE_AVAILABLE":
                progress(dict(event="SOURCE_REQUIRES_EXISTING_ROSTER_OR_CURRENT_DB", package_id=source["package_id"], reason=source["reason"]))
                continue
            frame, receipt = read_frozen_prediction_view(api=api, source=source, decision_dates=dates)
            buffer = io.BytesIO()
            frame.to_parquet(buffer, index=False)
            publish_bytes(root/"frozen_sources"/source["package_id"]/"roster.parquet", buffer.getvalue())
            publish_json(root/"frozen_sources"/source["package_id"]/"receipt.json", receipt)
            publish_json(root/"frozen_sources"/source["package_id"]/"roster_binding.json",
                dict(plan_sha256=file_sha(Path(args.plan)), roster_sha256=file_sha(root/"frozen_sources"/source["package_id"]/"roster.parquet"),
                    source_sha256=canonical_sha(source), receipt_sha256=file_sha(root/"frozen_sources"/source["package_id"]/"receipt.json")))
            progress(dict(event="FROZEN_QE_SOURCE_PROJECTED", package_id=source["package_id"],
                original_rows=len(frame), original_days=receipt["known_decision_days"], missing_days=len(receipt["missing_decision_dates"]),
                physical_fit_count=0, outcomes_read=False))
        return
    service = make_readonly_signal_service(temp_root=temp, repo_root=Path(__file__).resolve().parents[3],local_cpu_only=args.local_cpu_only)
    if args.local_cpu_only:
        from backend.services.advisory_model_first.cross_package_validation_source_v1 import local_cpu_resource_observation
        def observe():
            return local_cpu_resource_observation(api_base=args.api_base,temp_root=temp)
    else:
        def observe():
            return public_qe_observation_v1(args.api_base)
    if args.package_id and args.package_id not in NEW_PACKAGES:
        raise ValueError("canary --package-id must be one of the two preregistered new packages")
    for package_id in ((args.package_id,) if args.package_id else NEW_PACKAGES):
        prepare_package_dates(service=service, package=packages[package_id], decision_dates=CANARY_DATES,
            output_root=root/"canary"/args.attempt, observe_resources=observe, progress=progress)


if __name__ == "__main__":
    main()
