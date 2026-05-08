"""Register ScoreWeightedTopkStrategyV2CapacityV1 in aistock_strategy_catalog.

Default mode is dry-run so validation cannot write a production database by
accident.  Use ``--execute`` only after the PM/integrator explicitly authorizes
the DB asset registration.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass


STRATEGY_ID = "score_weighted_topk_v2_capacity_v1"
CLASS_NAME = "ScoreWeightedTopkStrategyV2CapacityV1"
MODULE_PATH = "score_weighted_strategy_v2_capacity_v1"
STRATEGY_FILE = Path(__file__).with_name("score_weighted_strategy_v2_capacity_v1.py")

DEFAULT_KWARGS: dict[str, Any] = {
    "topk": 50,
    "n_drop": 5,
    "weight_method": "softmax",
    "temperature": 1.0,
    "score_clip_quantile": 0.0,
    "max_weight": 0.05,
    "min_weight": 0.005,
    "max_position_ratio": 0.95,
    "max_single_order_value": 1_000_000_000.0,
    "enable_dynamic_ndrop": True,
    "max_n_drop": 5,
    "min_n_drop": 0,
    "threshold_method": "adaptive",
    "min_improvement": 0.01,
    "adaptive_multiplier": 0.5,
    "threshold_floor": 0.005,
    "hold_thresh": 2,
    "only_tradable": True,
    "forbid_all_trade_at_limit": False,
}

PORTFOLIO_CONFIG: dict[str, Any] = {
    "class": CLASS_NAME,
    "kwargs": {**DEFAULT_KWARGS, "signal": "<PRED>"},
    "module_path": MODULE_PATH,
}

PARAM_SCHEMA: list[dict[str, Any]] = [
    {"name": "topk", "type": "int", "min": 1, "max": 200, "default": 50, "desc": "Target holding count"},
    {"name": "n_drop", "type": "int", "min": 0, "max": 50, "default": 5, "desc": "Maximum replacements per rebalance"},
    {"name": "weight_method", "type": "enum", "options": ["softmax", "linear", "rank", "equal"], "default": "softmax", "desc": "Weighting method"},
    {"name": "temperature", "type": "float", "min": 0.1, "max": 5.0, "default": 1.0, "desc": "Softmax temperature"},
    {"name": "max_weight", "type": "float", "min": 0.001, "max": 0.2, "default": 0.05, "desc": "Maximum single-stock portfolio weight"},
    {"name": "min_weight", "type": "float", "min": 0.0, "max": 0.05, "default": 0.005, "desc": "Minimum positive single-stock weight"},
    {"name": "max_position_ratio", "type": "float", "min": 0.1, "max": 1.0, "default": 0.95, "desc": "Maximum total position ratio"},
    {"name": "max_single_order_value", "type": "float", "min": 10000.0, "max": 10000000000.0, "default": 1000000000.0, "desc": "Maximum RMB value for a single buy order"},
    {"name": "enable_dynamic_ndrop", "type": "bool", "default": True, "desc": "Enable dynamic n_drop"},
    {"name": "max_n_drop", "type": "int", "min": 1, "max": 50, "default": 5, "desc": "Hard upper bound for n_drop"},
    {"name": "min_n_drop", "type": "int", "min": 0, "max": 50, "default": 0, "desc": "Hard lower bound for n_drop"},
    {"name": "threshold_method", "type": "enum", "options": ["adaptive", "fixed", "percentile"], "default": "adaptive", "desc": "Dynamic n_drop threshold mode"},
    {"name": "adaptive_multiplier", "type": "float", "min": 0.1, "max": 2.0, "default": 0.5, "desc": "Adaptive threshold multiplier"},
    {"name": "threshold_floor", "type": "float", "min": 0.0, "max": 0.05, "default": 0.005, "desc": "Minimum adaptive threshold"},
    {"name": "min_improvement", "type": "float", "min": 0.0, "max": 0.1, "default": 0.01, "desc": "Fixed improvement threshold"},
    {"name": "hold_thresh", "type": "int", "min": 0, "max": 30, "default": 2, "desc": "Minimum holding days before sell"},
]


def _load_source() -> str:
    if not STRATEGY_FILE.exists():
        raise FileNotFoundError(f"strategy source not found: {STRATEGY_FILE}")
    source_code = STRATEGY_FILE.read_text(encoding="utf-8")
    match = re.search(r"class\s+(\w+)\s*\(", source_code)
    extracted_class = match.group(1) if match else None
    if extracted_class != CLASS_NAME:
        raise ValueError(f"expected class {CLASS_NAME}, found {extracted_class}")
    return source_code


def _connect():
    import psycopg2

    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("TDX_DB_PASSWORD") or os.getenv("DB_PASSWORD")
    if not password:
        raise RuntimeError("missing DB password env: POSTGRES_PASSWORD/TDX_DB_PASSWORD/DB_PASSWORD")
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST") or os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT") or os.getenv("TDX_DB_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB") or os.getenv("TDX_DB_NAME", "aistock"),
        user=os.getenv("POSTGRES_USER") or os.getenv("TDX_DB_USER", "postgres"),
        password=password,
    )


def _payload(source_code: str) -> tuple[Any, ...]:
    return (
        STRATEGY_ID,
        "2.1-capacity-v1",
        "2026-05-08T00:00:00Z",
        "aistock",
        "qlib_factor",
        "daily",
        "ScoreWeightedTopk V2 Capacity v1",
        "ScoreWeighted V2 with explicit capacity parameters; legacy score_weighted_topk_v2 keeps the 5M cap.",
        source_code,
        "scripts/score_weighted_strategy_v2_capacity_v1.py",
        json.dumps({"module_path": MODULE_PATH, "class_name": CLASS_NAME}),
        json.dumps(PORTFOLIO_CONFIG),
        json.dumps(DEFAULT_KWARGS),
        json.dumps(PARAM_SCHEMA),
        True,
    )


UPSERT_SQL = """
INSERT INTO aistock_strategy_catalog (
    strategy_id, catalog_version, generated_at_utc, catalog_source,
    scenario, strategy_type, display_name, description,
    source_code, source_code_relpath, python_implementation,
    portfolio_config, default_kwargs, param_schema, in_selection_center
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s::jsonb,
    %s::jsonb, %s::jsonb, %s::jsonb, %s
) ON CONFLICT (strategy_id) DO UPDATE SET
    source_code = EXCLUDED.source_code,
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    strategy_type = EXCLUDED.strategy_type,
    source_code_relpath = EXCLUDED.source_code_relpath,
    python_implementation = EXCLUDED.python_implementation,
    portfolio_config = EXCLUDED.portfolio_config,
    default_kwargs = EXCLUDED.default_kwargs,
    param_schema = EXCLUDED.param_schema,
    in_selection_center = EXCLUDED.in_selection_center,
    updated_at = NOW()
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="write the catalog row; default is dry-run")
    args = parser.parse_args()

    source_code = _load_source()
    digest = hashlib.sha256(source_code.encode("utf-8")).hexdigest()
    print(f"strategy_id={STRATEGY_ID}")
    print(f"class_name={CLASS_NAME}")
    print(f"source_file={STRATEGY_FILE}")
    print(f"source_sha256={digest}")
    print("default_kwargs=" + json.dumps(DEFAULT_KWARGS, sort_keys=True))
    print("capacity_fields=max_single_order_value,max_weight,max_position_ratio")

    if not args.execute:
        print("DRY_RUN: no DB write. Pass --execute only after explicit production DB authorization.")
        return 0

    conn = _connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(UPSERT_SQL, _payload(source_code))
            print(f"UPSERT affected rows: {cur.rowcount}")
            cur.execute(
                """
                SELECT strategy_id, display_name, default_kwargs->>'max_single_order_value', in_selection_center
                FROM aistock_strategy_catalog
                WHERE strategy_id = %s
                """,
                (STRATEGY_ID,),
            )
            row = cur.fetchone()
            print(f"Verification: id={row[0]} name={row[1]} max_single_order_value={row[2]} visible={row[3]}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
