"""P26 source/prepare only; deliberately no train, activation or process-control command."""
import argparse
import json
from pathlib import Path

from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import PopulationMetadataRequestV1
from backend.services.advisory_model_first.generic_population_price_5td_pipeline_v1 import freeze_population_source_v1, prepare_population_inputs_v1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("snapshot", "prepare"))
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--source-root")
    args = parser.parse_args()
    request = PopulationMetadataRequestV1.model_validate_json(Path(args.metadata).read_text(encoding="utf-8"))
    def progress(value):
        print(json.dumps(value, ensure_ascii=False), flush=True)
    if args.operation == "snapshot":
        root = freeze_population_source_v1(metadata_request=request, output_root=args.output_root, progress=progress)
    else:
        if not args.source_root:
            parser.error("prepare requires an explicitly frozen --source-root")
        root = prepare_population_inputs_v1(metadata_request=request, source_root=args.source_root, output_root=args.output_root, progress=progress)
    print(json.dumps(dict(root=str(root), physical_fit_count=0, research_run_created=False), ensure_ascii=False))


if __name__ == "__main__":
    main()
