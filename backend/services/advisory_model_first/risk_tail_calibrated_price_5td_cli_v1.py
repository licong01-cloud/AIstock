"""One offline Advisory risk-calibration study; never activation or execution."""
import argparse
import json

from backend.services.advisory_model_first.generic_population_price_5td_cli_v1 import public_qe_observation_v1
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_contracts_v1 import ARMS, RiskTailStudyPlanV1, node_path
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_model_v1 import risk_tail_price_set_v1
from backend.services.advisory_model_first.risk_tail_calibrated_price_5td_pipeline_v1 import (
    evaluate_risk_tail_study_v1, prepare_risk_tail_study_v1, preregister_risk_tail_study_v1, train_risk_tail_study_v1,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("preregister", "prepare", "train", "evaluate", "price-set"))
    parser.add_argument("--plan")
    parser.add_argument("--output-root")
    parser.add_argument("--bundle")
    parser.add_argument("--query")
    parser.add_argument("--arm", choices=ARMS, default=ARMS[1])
    args = parser.parse_args(argv)
    if args.operation == "price-set":
        if not args.bundle or not args.query:
            parser.error("price-set requires its explicit --bundle and --query")
        result = risk_tail_price_set_v1(bundle=json.loads(node_path(args.bundle).read_bytes()),
            **json.loads(node_path(args.query).read_bytes()), arm=args.arm)
    else:
        if not args.plan or not args.output_root:
            parser.error("study stages require their explicit --plan and --output-root")
        plan = RiskTailStudyPlanV1.model_validate_json(node_path(args.plan).read_bytes())
        operations = dict(preregister=preregister_risk_tail_study_v1, prepare=prepare_risk_tail_study_v1,
            train=train_risk_tail_study_v1, evaluate=evaluate_risk_tail_study_v1)
        kwargs = dict(plan=plan, output_root=args.output_root)
        if args.operation == "train":
            kwargs["qe_idle_probe"] = lambda: public_qe_observation_v1(plan.qe_api_base)
        root = operations[args.operation](**kwargs)
        result = dict(root=str(root), experiment_id=plan.experiment_id, completed_stage=args.operation,
            deployable=False, database_written=False, process_control=False)
    print(json.dumps(result, ensure_ascii=False, allow_nan=False), flush=True)


if __name__ == "__main__":
    main()
