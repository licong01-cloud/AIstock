#!/usr/bin/env python
"""Freeze an approved HMM coefficient product for the monthly release worker."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.dataset_release.canonical import canonical_json_bytes  # noqa: E402
from backend.services.dataset_release.monthly_hmm_authority_bootstrap import (  # noqa: E402
    MonthlyHMMAuthorityBootstrapError,
    bootstrap_monthly_hmm_authority,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-coefficients", type=Path, required=True)
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument("--config-path", type=Path)
    parser.add_argument("--producer-script", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--authority-id", required=True)
    parser.add_argument("--asset-id", required=True)
    parser.add_argument("--expected-dataset-manifest-sha256", required=True)
    parser.add_argument("--backtest-lag-trade-days", type=int, default=1)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = bootstrap_monthly_hmm_authority(
            source_coefficients=args.source_coefficients,
            model_path=args.model_path,
            config_path=args.config_path,
            producer_script=args.producer_script,
            output_root=args.output_root,
            project_root=PROJECT_ROOT,
            authority_id=args.authority_id,
            asset_id=args.asset_id,
            expected_dataset_manifest_sha256=(
                args.expected_dataset_manifest_sha256
            ),
            backtest_lag_trade_days=args.backtest_lag_trade_days,
        )
        payload = {
            "schema_version": "aistock_monthly_hmm_authority_bootstrap_cli_v1",
            "status": "PASS",
            "root": str(result.root),
            "authority_path": str(result.authority_path),
            "authority_sha256": result.authority_sha256,
            "receipt_path": str(result.receipt_path),
            "receipt_sha256": result.receipt_sha256,
        }
        sys.stdout.buffer.write(canonical_json_bytes(payload) + b"\n")
        return 0
    except (MonthlyHMMAuthorityBootstrapError, OSError, ValueError) as exc:
        payload = {
            "schema_version": "aistock_monthly_hmm_authority_bootstrap_cli_v1",
            "status": "FAILED",
            "reason_code": "MONTHLY_HMM_AUTHORITY_BOOTSTRAP_FAILED",
            "message": str(exc),
        }
        sys.stderr.buffer.write(canonical_json_bytes(payload) + b"\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
