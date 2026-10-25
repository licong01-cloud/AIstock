"""Offline Advisory parent-score price study. No service, QE or database mutation."""
import argparse
import json
from pathlib import Path

from backend.services.advisory_model_first.generic_population_price_5td_cli_v1 import public_qe_observation_v1
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import ParentScoreStudyPlanV1, node_path
from backend.services.advisory_model_first.parent_score_price_5td_pipeline_v1 import (
    evaluate_parent_score_study, prepare_parent_score_study, preregister_parent_score_study, train_parent_score_study,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("preregister", "prepare", "train", "evaluate", "price-set"))
    parser.add_argument("--plan")
    parser.add_argument("--output-root")
    parser.add_argument("--model")
    parser.add_argument("--query")
    args = parser.parse_args(argv)
    def report(value):
        print(json.dumps(value, ensure_ascii=False, allow_nan=False), flush=True)
    if args.operation == "price-set":
        if not args.model or not args.query:
            parser.error("price-set requires explicit --model and --query JSON; no activation")
        from backend.services.advisory_model_first.parent_score_price_5td_model_v1 import model_from_payload, parent_score_price_set
        fitted = model_from_payload(json.loads(node_path(args.model).read_bytes()))
        report(parent_score_price_set(fitted=fitted, **json.loads(node_path(args.query).read_bytes())))
        return
    if not args.plan or not args.output_root:
        parser.error("study operations require explicit --plan and non-C --output-root")
    plan = ParentScoreStudyPlanV1.model_validate_json(node_path(args.plan).read_bytes())
    functions = dict(preregister=preregister_parent_score_study, prepare=prepare_parent_score_study,
                     train=train_parent_score_study, evaluate=evaluate_parent_score_study)
    kwargs = dict(plan=plan, output_root=args.output_root)
    if args.operation == "prepare":
        from backend.mcp.common import AIstockApiClient
        kwargs.update(api=AIstockApiClient(plan.qe_api_base, timeout=30, max_response_bytes=2*1024**2), progress=report)
    elif args.operation == "train":
        kwargs["qe_idle_probe"] = lambda: public_qe_observation_v1(plan.qe_api_base)
    target = functions[args.operation](**kwargs)
    report(dict(root=str(Path(target).parent), experiment_id=plan.experiment_id, stage=args.operation,
        database_written=False, process_control=False, deployment=False))


if __name__ == "__main__":
    main()
