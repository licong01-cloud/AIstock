"""
Register/Sync ScoreWeightedTopkStrategyV2 source_code into aistock_strategy_catalog.

V2 修复了 V1 的两个 Bug：
  Bug #1: 补仓模式绕过 max_n_drop，持仓从第一天起失控膨胀
  Bug #2: 无评分旧持仓静默积累（幽灵持仓），永远不被卖出

Usage:
    python scripts/register_score_weighted_strategy_v2.py
"""
import os
import sys
import json
import re
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

import psycopg2

STRATEGY_FILE = Path(
    r"F:\Dev\RD-Agent-main\app_tpl\all\v4\rdagent\scenarios\qlib\experiment"
    r"\factor_template\score_weighted_strategy_v2.py"
)

STRATEGY_ID = "score_weighted_topk_v2"
CLASS_NAME = "ScoreWeightedTopkStrategyV2"
MODULE_PATH = "rdagent.scenarios.qlib.experiment.factor_template.score_weighted_strategy_v2"

DEFAULT_KWARGS = {
    "topk": 50, "n_drop": 5,
    "weight_method": "softmax", "temperature": 1.0,
    "score_clip_quantile": 0.0, "max_weight": 0.05,
    "min_weight": 0.005, "max_position_ratio": 0.95,
    "enable_dynamic_ndrop": True, "max_n_drop": 5, "min_n_drop": 0,
    "threshold_method": "adaptive", "min_improvement": 0.01,
    "adaptive_multiplier": 0.5, "threshold_floor": 0.005,
    "hold_thresh": 2, "only_tradable": True, "forbid_all_trade_at_limit": False,
}

PORTFOLIO_CONFIG = {
    "class": CLASS_NAME,
    "kwargs": {**DEFAULT_KWARGS, "signal": "<PRED>"},
    "module_path": MODULE_PATH,
}

PARAM_SCHEMA = [
    {"name": "weight_method", "type": "enum", "options": ["softmax", "linear", "rank", "equal"], "default": "softmax", "desc": "Weighting method"},
    {"name": "temperature", "type": "float", "min": 0.1, "max": 5.0, "default": 1.0, "desc": "Softmax temperature"},
    {"name": "max_weight", "type": "float", "min": 0.01, "max": 0.2, "default": 0.05, "desc": "Max single stock weight"},
    {"name": "min_weight", "type": "float", "min": 0.0, "max": 0.05, "default": 0.005, "desc": "Min single stock weight"},
    {"name": "max_position_ratio", "type": "float", "min": 0.5, "max": 1.0, "default": 0.95, "desc": "Max portfolio ratio"},
    {"name": "enable_dynamic_ndrop", "type": "bool", "default": True, "desc": "Enable dynamic n_drop"},
    {"name": "max_n_drop", "type": "int", "min": 1, "max": 20, "default": 5, "desc": "Max n_drop (controls rebalance speed)"},
    {"name": "min_n_drop", "type": "int", "min": 0, "max": 10, "default": 0, "desc": "Min n_drop"},
    {"name": "threshold_method", "type": "enum", "options": ["adaptive", "fixed", "percentile"], "default": "adaptive", "desc": "Threshold method"},
    {"name": "adaptive_multiplier", "type": "float", "min": 0.1, "max": 2.0, "default": 0.5, "desc": "Adaptive multiplier"},
    {"name": "threshold_floor", "type": "float", "min": 0.0, "max": 0.05, "default": 0.005, "desc": "Adaptive threshold floor"},
    {"name": "min_improvement", "type": "float", "min": 0.0, "max": 0.1, "default": 0.01, "desc": "Fixed threshold value"},
    {"name": "hold_thresh", "type": "int", "min": 1, "max": 10, "default": 2, "desc": "Min holding days"},
]


def main():
    if not STRATEGY_FILE.exists():
        print(f"ERROR: Strategy file not found: {STRATEGY_FILE}")
        sys.exit(1)

    source_code = STRATEGY_FILE.read_text(encoding="utf-8")
    print(f"Read strategy source: {len(source_code)} chars from {STRATEGY_FILE.name}")

    m = re.search(r"class\s+(\w+)\s*\(", source_code)
    extracted_class = m.group(1) if m else None
    if extracted_class != CLASS_NAME:
        print(f"ERROR: Expected class {CLASS_NAME}, found {extracted_class}")
        sys.exit(1)
    print(f"Class extracted OK: {extracted_class}")

    password = (
        os.getenv("POSTGRES_PASSWORD")
        or os.getenv("TDX_DB_PASSWORD")
        or os.getenv("DB_PASSWORD")
    )
    if not password:
        print("ERROR: No DB password in env (POSTGRES_PASSWORD/TDX_DB_PASSWORD/DB_PASSWORD)")
        sys.exit(1)

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST") or os.getenv("TDX_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT") or os.getenv("TDX_DB_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB") or os.getenv("TDX_DB_NAME", "aistock"),
        user=os.getenv("POSTGRES_USER") or os.getenv("TDX_DB_USER", "postgres"),
        password=password,
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
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
    """, (
        STRATEGY_ID, "2.0", "2026-04-13T00:00:00Z", "custom",
        "qlib_factor", "daily",
        "ScoreWeightedTopk V2 (TopK强制执行修复)",
        "V2修复版：补仓速度受max_n_drop约束(Fix#1)，无评分旧持仓强制卖出(Fix#2)。"
        "topk持仓数量从第一天起严格受控，不再失控膨胀。参数与V1完全兼容。",
        source_code, "score_weighted_strategy_v2.py",
        json.dumps({"module_path": MODULE_PATH, "class_name": CLASS_NAME}),
        json.dumps(PORTFOLIO_CONFIG),
        json.dumps(DEFAULT_KWARGS),
        json.dumps(PARAM_SCHEMA),
        True,
    ))
    print(f"UPSERT affected rows: {cur.rowcount}")

    cur.execute("""
    SELECT strategy_id, display_name, length(source_code), in_selection_center
    FROM aistock_strategy_catalog
    WHERE strategy_id = %s
    """, (STRATEGY_ID,))
    row = cur.fetchone()
    print(f"Verification: id={row[0]} name={row[1]} src_len={row[2]} visible={row[3]}")

    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
