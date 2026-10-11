"""Explicit Advisory-only D-clock study; no Selection, QE writes, deployment or services."""
import argparse
import json

from backend.services.advisory_model_first.d_clock_population_price_5td_contracts_v1 import DClockPopulationStudyPlanV1, node_path
from backend.services.advisory_model_first.d_clock_population_price_5td_pipeline_v1 import (
    evaluate_d_clock_population_study_v1, prepare_d_clock_population_inputs_v1,
    preregister_d_clock_population_study_v1, train_d_clock_population_study_v1,
)
from backend.services.advisory_model_first.generic_population_price_5td_cli_v1 import public_qe_observation_v1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("preregister", "prepare", "train", "evaluate"))
    parser.add_argument("--plan", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args(argv)
    plan = DClockPopulationStudyPlanV1.model_validate_json(node_path(args.plan).read_bytes())
    operations = dict(preregister=preregister_d_clock_population_study_v1, prepare=prepare_d_clock_population_inputs_v1,
        train=train_d_clock_population_study_v1, evaluate=evaluate_d_clock_population_study_v1)
    kwargs = dict(plan=plan, output_root=args.output_root)
    if args.operation == "train":
        kwargs["qe_idle_probe"] = lambda: public_qe_observation_v1(plan.qe_api_base)
    if args.operation == "prepare":
        kwargs["progress"] = lambda value: print(json.dumps(value, ensure_ascii=False), flush=True)
    target = operations[args.operation](**kwargs)
    receipt = target/("evaluation.json" if args.operation == "evaluate" else "receipt.json")
    state = json.loads(receipt.read_bytes()) if receipt.is_file() else dict(status="PREREGISTERED_NO_FIT", physical_fit_count=0)
    print(json.dumps(dict(root=str(target), experiment_id=plan.experiment_id, completed_stage=args.operation,
        status=state.get("status"), physical_fit_count=state.get("physical_fit_count", 0), deployable=False,
        database_written=False, backend_restart_required=False, process_control=False)), flush=True)


if __name__ == "__main__":
    main()
