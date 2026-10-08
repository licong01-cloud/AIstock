"""Advisory offline population study; no activation, QE writes or process control."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import PopulationMetadataRequestV1, PopulationStudyPlanV1
from backend.services.advisory_model_first.generic_population_price_5td_pipeline_v1 import (
    evaluate_population_study_v1, freeze_population_source_v1, prepare_population_inputs_v1,
    prepare_population_study_v1, preregister_population_study_v1, train_population_study_v1,
)


def public_qe_observation_v1(api_base, *, get=None):
    """Only the three published list contracts, running and pending, never QE mutation."""
    if get is None:
        from backend.mcp.common import AIstockApiClient
        get = AIstockApiClient(api_base, timeout=8, max_response_bytes=1048576).get
    counts = dict(single=0, custom_evo=0, multi_alpha=0)
    began = datetime.now(timezone.utc)
    for state in ("running", "pending"):
        single = get("/quantevolver/experiments", params=dict(status=state, detail="summary", limit=1, include_children=True))
        evo = get("/quantevolver/evolution/tasks", params=dict(status=state, detail="summary", limit=1))
        multi = get("/multi-alpha/combine-backtest/runs", params=dict(status=state, limit=1))
        if (not isinstance(single, dict) or single.get("ok") is not True or type(single.get("total")) is not int
                or single["total"] < 0 or not isinstance(single.get("items"), list)
                or single["total"] < len(single["items"]) or (single["total"] == 0 and single.get("has_more") is not False)
                or not isinstance(evo, dict) or evo.get("status") != "success" or not isinstance(evo.get("data"), list)
                or not isinstance(multi, dict) or multi.get("status") != "success" or not isinstance(multi.get("data"), dict)):
            raise ValueError("population public QE observation is incomplete; no fit")
        data = multi["data"]
        if (type(data.get("count")) is not int or data["count"] < 0 or not isinstance(data.get("runs"), list)
                or data["count"] < len(data["runs"])):
            raise ValueError("population public multi-alpha observation is incomplete; no fit")
        counts["single"] += single["total"]
        counts["custom_evo"] += len(evo["data"])
        counts["multi_alpha"] += data["count"]
    if (datetime.now(timezone.utc)-began).total_seconds() > 60:
        raise ValueError("population public QE observations exceed freshness budget")
    return dict(captured_at=began.isoformat(), active_counts=counts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("snapshot", "prepare", "preregister", "prepare-study", "train", "evaluate"))
    parser.add_argument("--metadata")
    parser.add_argument("--plan")
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--source-root")
    args = parser.parse_args()
    def progress(value):
        print(json.dumps(value, ensure_ascii=False), flush=True)
    if args.operation in ("snapshot", "prepare"):
        if not args.metadata:
            parser.error("snapshot/prepare require --metadata")
        request = PopulationMetadataRequestV1.model_validate_json(Path(args.metadata).read_text(encoding="utf-8"))
        if args.operation == "snapshot":
            root = freeze_population_source_v1(metadata_request=request, output_root=args.output_root, progress=progress)
        else:
            if not args.source_root:
                parser.error("prepare requires an explicitly frozen --source-root")
            root = prepare_population_inputs_v1(metadata_request=request, source_root=args.source_root, output_root=args.output_root, progress=progress)
        progress(dict(root=str(root), physical_fit_count=0, research_run_created=False))
        return
    if not args.plan:
        parser.error("study operations require their explicit --plan")
    plan = PopulationStudyPlanV1.model_validate_json(Path(args.plan).read_bytes())
    operations = dict(preregister=preregister_population_study_v1, **{
        "prepare-study": prepare_population_study_v1, "train": train_population_study_v1, "evaluate": evaluate_population_study_v1})
    kwargs = dict(plan=plan, output_root=args.output_root)
    if args.operation == "train":
        kwargs["qe_idle_probe"] = lambda: public_qe_observation_v1(plan.qe_api_base)
    root = operations[args.operation](**kwargs)
    progress(dict(root=str(root), experiment_id=plan.experiment_id, completed_stage=args.operation,
                  deployable=False, database_written=False, process_control=False))


if __name__ == "__main__":
    main()
