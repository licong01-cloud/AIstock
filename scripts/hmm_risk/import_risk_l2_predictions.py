#!/usr/bin/env python3
"""Validate sealed L2 historical risk, or explicitly write an existing DB target."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path

from backend.services.hmm_risk.risk_l2_prediction import (
    REASON_WRITER,
    RiskL2PredictionError,
    RiskL2PredictionRepository,
    load_product,
)


@contextmanager
def target_connection(target: str):
    """Credentials stay in an existing operator environment, never in receipts."""
    import psycopg2

    dsn = os.environ.get("AISTOCK_HMM_RISK_L2_IMPORT_DSN", "").strip()
    if not dsn or target not in {"aistock_dev", "aistock"}:
        raise RiskL2PredictionError(REASON_WRITER, "explicit existing target and import DSN required")
    conn = psycopg2.connect(dsn)
    try:
        if conn.info.dbname != target:
            raise RiskL2PredictionError(REASON_WRITER, "connected database differs from explicit target")
        conn.autocommit = False
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--mode", choices=("validate", "write"), required=True)
    parser.add_argument("--database-target", choices=("aistock_dev", "aistock"))
    args = parser.parse_args(argv)
    if args.mode == "write" and args.database_target is None:
        parser.error("write requires --database-target; no default database")
    try:
        product = load_product(args.request)
        result = {
            "schema_version": "hmm_risk_risk_l2_import_result_v1",
            "mode": args.mode,
            "run_id": product.run["run_id"],
            "row_count": len(product.rows),
            "date_start": product.run["dates"][0],
            "date_end": product.run["dates"][-1],
            "date_count": len(product.run["dates"]),
            "sector_count": len(product.run["catalog"]),
            "model_hash": product.run["model_hash"],
            "input_hash": product.run["input_hash"],
            "row_hash": product.run["compact_summary"]["row_hash"],
            "risk_l2_capability_status": product.run["risk_l2_capability_status"],
            "database_write": args.mode == "write",
            "new_fits": 0,
            "tail_accessed": False,
            "runtime_action": False,
        }
        if args.mode == "write":
            result.update(
                RiskL2PredictionRepository(conn_factory=lambda: target_connection(args.database_target)).write_product(
                    product, database_target=args.database_target
                )
            )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
        return 0
    except RiskL2PredictionError as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "reason_code": exc.reason_code,
                    "message": str(exc),
                    "database_write_requested": args.mode == "write",
                    "runtime_action": False,
                },
                ensure_ascii=False,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
