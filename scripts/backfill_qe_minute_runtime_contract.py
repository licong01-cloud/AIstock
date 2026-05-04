"""Backfill explicit minute runtime contracts into qe_experiments.custom_params.

Default mode is a dry-run.  Writes require both ``--write`` and the exact
confirmation token so historical daily/unknown experiments are not silently
converted into minute-backed StrategyPackage candidates.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from psycopg2.extras import Json


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.db.pg_pool import get_conn  # noqa: E402
from backend.services.quantevolver.runtime_contract import (  # noqa: E402
    merge_qe_minute_runtime_contract,
    parse_json_mapping,
    runtime_contract_missing,
)


CONFIRM_TOKEN = "QE_MINUTE_RUNTIME_CONTRACT_BACKFILL"


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip().strip('"').strip("'")
        os.environ[key] = value


def _loop_index_from_record(record: dict[str, Any]) -> int | None:
    raw = record.get("loop_index")
    if raw not in (None, ""):
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    for value in (record.get("qe_loop_id"), record.get("experiment_id")):
        match = re.search(r"(?:Loop|_L)(\d+)$", str(value or ""), flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def _load_loop_config(record: dict[str, Any]) -> dict[str, Any]:
    task_id = str(record.get("qe_task_id") or "").strip()
    qe_loop_id = str(record.get("qe_loop_id") or "").strip()
    experiment_id = str(record.get("experiment_id") or "").strip()
    if not task_id and not experiment_id:
        return {}
    loop_index = _loop_index_from_record(record)
    task_prefixed_loop_id = f"{task_id}_{qe_loop_id}" if task_id and qe_loop_id else qe_loop_id
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT l.config_json,
                       t.execution_algo AS task_execution_algo,
                       t.execution_algo_params AS task_execution_algo_params
                FROM qe_evolution_loops l
                LEFT JOIN qe_evolution_tasks t ON t.task_id = l.task_id
                WHERE l.experiment_id = %s
                   OR (
                       l.task_id = %s
                       AND (
                           l.loop_id = %s
                           OR l.loop_id = %s
                           OR (%s IS NOT NULL AND l.loop_index = %s)
                       )
                   )
                ORDER BY l.updated_at DESC NULLS LAST, l.created_at DESC NULLS LAST
                LIMIT 1
                """,
                (
                    experiment_id,
                    task_id,
                    qe_loop_id,
                    task_prefixed_loop_id,
                    loop_index,
                    loop_index,
                ),
            )
            row = cur.fetchone()
            columns = [desc[0] for desc in cur.description or []]
    if not row:
        return {}
    data = dict(zip(columns, row))
    config = parse_json_mapping(data.get("config_json"))
    if data.get("task_execution_algo") and not config.get("execution_algo"):
        config["execution_algo"] = data.get("task_execution_algo")
    task_params = parse_json_mapping(data.get("task_execution_algo_params"))
    if task_params and not config.get("execution_algo_params"):
        config["execution_algo_params"] = task_params
    return config


def _candidate_rows(experiment_ids: list[str], limit: int) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            if experiment_ids:
                cur.execute(
                    """
                    SELECT experiment_id, experiment_name, status, qe_task_id, qe_loop_id,
                           loop_index, custom_params, updated_at, created_at
                    FROM qe_experiments
                    WHERE experiment_id = ANY(%s)
                    ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                    """,
                    (experiment_ids,),
                )
            else:
                cur.execute(
                    """
                    SELECT experiment_id, experiment_name, status, qe_task_id, qe_loop_id,
                           loop_index, custom_params, updated_at, created_at
                    FROM qe_experiments
                    ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description or []]
    return [dict(zip(columns, row)) for row in rows]


def _update_custom_params(experiment_id: str, custom_params: dict[str, Any]) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE qe_experiments
                SET custom_params = %s, updated_at = NOW()
                WHERE experiment_id = %s
                """,
                (Json(custom_params, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)), experiment_id),
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-id", action="append", default=[], help="Target one experiment_id; may repeat.")
    parser.add_argument("--limit", type=int, default=1000, help="Max rows to scan when not targeting IDs.")
    parser.add_argument("--write", action="store_true", help="Apply updates. Omit for dry-run.")
    parser.add_argument("--confirm-write", default="", help=f"Required token for writes: {CONFIRM_TOKEN}")
    parser.add_argument("--json", action="store_true", help="Emit JSON summary.")
    args = parser.parse_args()

    _load_dotenv(REPO_ROOT / ".env")
    if args.write and args.confirm_write != CONFIRM_TOKEN:
        raise SystemExit(f"--write requires --confirm-write {CONFIRM_TOKEN}")

    rows = _candidate_rows(args.experiment_id, max(args.limit, 1))
    scanned = len(rows)
    missing = 0
    updatable: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in rows:
        original_custom = parse_json_mapping(row.get("custom_params"))
        if not runtime_contract_missing(original_custom):
            skipped.append({"experiment_id": row["experiment_id"], "reason": "already_has_contract"})
            continue
        missing += 1
        loop_config = _load_loop_config(row)
        merged = merge_qe_minute_runtime_contract(
            original_custom,
            config=loop_config,
            source="history_backfill_from_loop_config",
            allow_default_execution_algo=False,
        )
        if runtime_contract_missing(merged):
            skipped.append({"experiment_id": row["experiment_id"], "reason": "no_minute_runtime_evidence"})
            continue
        item = {
            "experiment_id": row["experiment_id"],
            "qe_task_id": row.get("qe_task_id"),
            "qe_loop_id": row.get("qe_loop_id"),
            "backtest_freq": merged.get("backtest_freq"),
            "execution_algo": merged.get("execution_algo"),
            "execution_algo_params_keys": sorted(str(k) for k in parse_json_mapping(merged.get("execution_algo_params")).keys()),
            "custom_params_before_keys": sorted(str(k) for k in original_custom.keys()),
            "custom_params_after_keys": sorted(str(k) for k in merged.keys()),
            "_merged": merged,
        }
        updatable.append(item)

    updated = 0
    if args.write:
        for item in updatable:
            _update_custom_params(item["experiment_id"], item["_merged"])
            updated += 1

    for item in updatable:
        item.pop("_merged", None)
    summary = {
        "mode": "write" if args.write else "dry_run",
        "scanned": scanned,
        "missing_contract": missing,
        "updatable": len(updatable),
        "updated": updated,
        "updatable_samples": updatable[:20],
        "skipped_samples": skipped[:20],
    }
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    else:
        print(
            f"mode={summary['mode']} scanned={scanned} missing={missing} "
            f"updatable={len(updatable)} updated={updated}"
        )
        for item in updatable[:20]:
            print(
                f"UPDATABLE {item['experiment_id']} "
                f"freq={item['backtest_freq']} algo={item['execution_algo']}"
            )
        for item in skipped[:20]:
            print(f"SKIP {item['experiment_id']} reason={item['reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
