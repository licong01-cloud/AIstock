"""Offline VIX-only reconciliation against a captured official Cboe history file."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from backend.data_service.shared_vix_inputs import export_corrected_bundle, verify_corrected_bundle


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify')
    parser.add_argument('--expected-manifest-sha256')
    parser.add_argument('--old-root')
    parser.add_argument('--old-manifest-sha256')
    parser.add_argument('--official-csv')
    parser.add_argument('--capture-json')
    parser.add_argument('--destination')
    parser.add_argument('--source-file',action='append',default=[])
    args=parser.parse_args()
    if args.verify:
        result=verify_corrected_bundle(args.verify,expected_manifest_sha256=args.expected_manifest_sha256)
    else:
        if not all([args.old_root,args.old_manifest_sha256,args.official_csv,args.capture_json,args.destination]):
            parser.error('Build requires predecessor identity, captured official source, and a new destination')
        result=export_corrected_bundle(args.old_root,args.old_manifest_sha256,args.official_csv,
            args.capture_json,args.destination,source_files=args.source_file)
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':
    main()
