"""Offline original-slot price study; no training submission to QE or activation."""
import argparse
import json

from backend.services.advisory_model_first.generic_population_price_5td_cli_v1 import public_qe_observation_v1
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_contracts_v1 import OriginalSlotHurdleStudyPlanV1, node_path
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_pipeline_v1 import (
    evaluate_hurdle_study_v1, prepare_hurdle_study_v1, preregister_hurdle_study_v1, train_hurdle_study_v1,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("preregister", "prepare", "train", "evaluate"))
    parser.add_argument("--plan", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args()
    plan = OriginalSlotHurdleStudyPlanV1.model_validate_json(node_path(args.plan).read_bytes())
    operations = dict(preregister=preregister_hurdle_study_v1, prepare=prepare_hurdle_study_v1,
        train=train_hurdle_study_v1, evaluate=evaluate_hurdle_study_v1)
    kwargs = dict(plan=plan, output_root=args.output_root)
    if args.operation == "train":
        kwargs["qe_idle_probe"] = lambda: public_qe_observation_v1(plan.qe_api_base)
    root = operations[args.operation](**kwargs)
    print(json.dumps(dict(experiment_id=plan.experiment_id, completed_stage=args.operation, root=str(root),
        deployable=False, database_written=False, process_control=False), ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
