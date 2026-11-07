"""Export optional shared inputs from a read-only DB snapshot, or verify files only."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--destination', required=True, type=Path)
    parser.add_argument('--start-date')
    parser.add_argument('--end-date')
    parser.add_argument('--dataset', action='append')
    parser.add_argument('--quality-report', type=Path)
    parser.add_argument('--database-target', choices=('dev', 'production'), default='dev')
    parser.add_argument('--env-file', type=Path, default=ROOT/'.env')
    parser.add_argument('--verify-only', action='store_true')
    parser.add_argument('--expected-manifest-sha256')
    args = parser.parse_args()
    if args.verify_only:
        from backend.data_service.shared_tushare_inputs import verify_bundle
        result = verify_bundle(args.destination, expected_manifest_sha256=args.expected_manifest_sha256)
    else:
        if not args.start_date or not args.end_date:
            parser.error('export requires --start-date and --end-date')
        from dotenv import load_dotenv
        load_dotenv(args.env_file, override=False)
        from scripts.prepare_canonical_pit_monthly import _database_target_environment, _load_database_config
        config = _load_database_config(args.database_target, args.env_file)
        with _database_target_environment(config):
            from backend.data_service.shared_tushare_inputs import DATASETS, export_bundle
            result = export_bundle(args.destination, args.start_date, args.end_date,
                datasets=args.dataset if args.dataset is not None else DATASETS, quality_report=args.quality_report)
        result['database_target'] = args.database_target
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
