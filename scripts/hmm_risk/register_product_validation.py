"""Register an existing HMM product-validation result without runtime reconfiguration."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.services.hmm_risk.product_validation_store import read_receipt, register_receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument(
        "--store-root", type=Path, help="Explicit store root; default is the backend service account's home store."
    )
    args = parser.parse_args()
    record = read_receipt(args.receipt)
    result = register_receipt(record, root=args.store_root)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
