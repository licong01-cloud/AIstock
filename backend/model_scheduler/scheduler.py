"""DB-driven scheduler for quant model training and inference (new program).

Design goals (aligned with docs/quant_analyst_design.md §5.3):
- Reuse the ingestion_* control-table style, but for model runs:
  - Configuration lives in app.model_schedule (new table in init_quant_schema.py).
  - Actual training/inference runs are executed by the dedicated scripts
    under next_app.backend.quant_models.* which already write to
    app.model_train_run / app.model_inference_run / app.quant_unified_signal.
- Do not modify legacy tdx_scheduler/tdx_backend; integration is via DB and
  Python CLI only.

Usage pattern:
- Define one or more rows in app.model_schedule with config_json like:

  {
    "kind": "lstm_shared_train",
    "params": {
      "universe-name": "ALL_EQ_CLEAN",
      "start": "2020-01-01T09:30:00",
      "end": "2024-01-01T15:00:00",
      "seq-len": 60
    }
  }

  The keys under "params" must correspond to the long CLI options of the
  target script, written in kebab-case (e.g. "universe-name", "seq-len").

- Then run this scheduler via:

  python -m next_app.backend.model_scheduler.scheduler run-once \
      --model-name LSTM_SHARED \
      --schedule-name weekly_shared_train \
      --task-type train

- Or periodically run:

  python -m next_app.backend.model_scheduler.scheduler run-due

  which will pick all enabled schedules whose next_run_at is NULL or in
  the past, execute them once, and advance next_run_at by `frequency::interval`.

This module intentionally keeps scheduling logic simple; higher-level
orchestration (Windows Task Scheduler, cron, or APScheduler in FastAPI)
can call the CLI entrypoints at appropriate intervals.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from next_app.backend.db.pg_pool import get_conn


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ScheduleRecord:
    id: int
    model_name: str
    schedule_name: str
    task_type: str  # "train" or "inference"
    frequency: str
    enabled: bool
    config_json: Dict[str, Any]


# ---------------------------------------------------------------------------
# Dispatch table: kind -> Python module
# ---------------------------------------------------------------------------


DISPATCH_TABLE: Dict[str, Dict[str, Any]] = {
    # LSTM per-stock
    "lstm_per_stock_train": {
        "module": "next_app.backend.quant_models.lstm.train_per_stock",
    },
    "lstm_per_stock_infer": {
        "module": "next_app.backend.quant_models.lstm.infer_per_stock",
    },
    # LSTM shared
    "lstm_shared_train": {
        "module": "next_app.backend.quant_models.lstm.train_shared",
    },
    "lstm_shared_infer": {
        "module": "next_app.backend.quant_models.lstm.infer_shared",
    },
    # LSTM refinement (mode is passed explicitly via params)
    "lstm_refinement_train": {
        "module": "next_app.backend.quant_models.lstm.refinement_per_stock",
    },
    "lstm_refinement_infer": {
        "module": "next_app.backend.quant_models.lstm.refinement_per_stock",
    },
    # DeepAR daily / 60m (freq is part of params)
    "deepar_train": {
        "module": "next_app.backend.quant_models.deepar.train",
    },
    "deepar_infer": {
        "module": "next_app.backend.quant_models.deepar.infer",
    },
}


# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Quant model scheduler (DB-driven)")
    sub = parser.add_subparsers(dest="mode", required=True)

    p_due = sub.add_parser("run-due", help="Run all enabled schedules that are due")
    p_due.add_argument("--dry-run", action="store_true", help="Only print actions, do not execute or update DB")

    p_once = sub.add_parser("run-once", help="Run a single schedule identified by id or (model,schedule,task)")
    p_once.add_argument("--id", type=int, default=None, help="Schedule id in app.model_schedule")
    p_once.add_argument("--model-name", type=str, default=None, help="Model name, e.g. LSTM_SHARED")
    p_once.add_argument("--schedule-name", type=str, default=None, help="Logical schedule name")
    p_once.add_argument("--task-type", type=str, choices=["train", "inference"], default=None)
    p_once.add_argument("--dry-run", action="store_true", help="Only print actions, do not execute or update DB")

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Core DB helpers
# ---------------------------------------------------------------------------


def _row_to_schedule(row: Any) -> ScheduleRecord:
    cfg = row[6]
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg)
        except Exception:  # noqa: BLE001
            cfg = {"raw": cfg}
    return ScheduleRecord(
        id=row[0],
        model_name=row[1],
        schedule_name=row[2],
        task_type=row[3],
        frequency=row[4],
        enabled=bool(row[5]),
        config_json=cfg or {},
    )


def _load_due_schedules(now: dt.datetime) -> List[ScheduleRecord]:
    sql = """
        SELECT
          id,
          model_name,
          schedule_name,
          task_type,
          frequency,
          enabled,
          config_json
        FROM app.model_schedule
        WHERE enabled = TRUE
          AND (next_run_at IS NULL OR next_run_at <= %s)
        ORDER BY id
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (now,))
            rows = cur.fetchall()
    return [_row_to_schedule(r) for r in rows]


def _load_single_schedule(
    schedule_id: Optional[int],
    model_name: Optional[str],
    schedule_name: Optional[str],
    task_type: Optional[str],
) -> ScheduleRecord:
    if schedule_id is not None:
        sql = """
            SELECT id, model_name, schedule_name, task_type, frequency, enabled, config_json
              FROM app.model_schedule
             WHERE id = %s
        """
        params = (schedule_id,)
    else:
        if not (model_name and schedule_name and task_type):
            raise SystemExit("run-once 模式需要提供 --id 或 (""--model-name + --schedule-name + --task-type"")")
        sql = """
            SELECT id, model_name, schedule_name, task_type, frequency, enabled, config_json
              FROM app.model_schedule
             WHERE model_name = %s AND schedule_name = %s AND task_type = %s
             LIMIT 1
        """
        params = (model_name, schedule_name, task_type)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
    if row is None:
        raise SystemExit("指定的 model_schedule 未找到")
    return _row_to_schedule(row)


def _update_schedule_run_state(schedule_id: int, status: str, error: Optional[str]) -> None:
    """Update last_run_at/next_run_at/last_status/last_error for a schedule.

    `frequency` is interpreted as a PostgreSQL interval literal and applied
    entirely on the DB side (`NOW() + frequency::interval`). This mirrors
    the style used by market.ingestion_schedules.
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE app.model_schedule
                   SET last_run_at = NOW(),
                       next_run_at = CASE
                                      WHEN frequency IS NOT NULL AND frequency <> ''
                                        THEN NOW() + frequency::interval
                                      ELSE NULL
                                    END,
                       last_status = %s,
                       last_error = %s,
                       updated_at = NOW()
                 WHERE id = %s
                """,
                (status, error, schedule_id),
            )


# ---------------------------------------------------------------------------
# Task execution
# ---------------------------------------------------------------------------


def _build_cli_args_from_params(params: Dict[str, Any]) -> List[str]:
    """Convert params dict to CLI arguments.

    Keys are expected to be in kebab-case (e.g. "universe-name"), matching the
    long options defined in the target script's argparse parser.
    """

    args: List[str] = []
    for key, value in params.items():
        opt = f"--{key}"
        if isinstance(value, bool):
            # Simple convention: True -> pass flag, False -> omit
            if value:
                args.append(opt)
            continue
        if value is None:
            continue
        args.append(opt)
        args.append(str(value))
    return args


def _run_task_for_schedule(schedule: ScheduleRecord) -> None:
    cfg = schedule.config_json or {}
    kind = cfg.get("kind")
    if not kind:
        raise ValueError("config_json.kind 缺失，无法派发任务")

    mapping = DISPATCH_TABLE.get(kind)
    if mapping is None:
        raise ValueError(f"不支持的 schedule kind: {kind}")

    module = mapping["module"]
    params = cfg.get("params") or {}
    if not isinstance(params, dict):
        raise ValueError("config_json.params 必须是对象")

    cli_args = _build_cli_args_from_params(params)

    cmd: List[str] = [sys.executable, "-m", module, *cli_args]
    print(f"[SCHED] running schedule id={schedule.id} model={schedule.model_name} "
          f"task_type={schedule.task_type} kind={kind} -> {cmd}")

    # Let subprocess raise CalledProcessError on non-zero exit
    subprocess.run(cmd, check=True)


def _execute_schedule(schedule: ScheduleRecord, dry_run: bool) -> None:
    status = "SUCCESS"
    error: Optional[str] = None

    if dry_run:
        print(
            f"[DRY-RUN] would run schedule id={schedule.id} "
            f"model={schedule.model_name} schedule={schedule.schedule_name} "
            f"task_type={schedule.task_type}"
        )
        return

    try:
        _run_task_for_schedule(schedule)
    except subprocess.CalledProcessError as exc:  # noqa: BLE001
        status = "FAILED"
        error = f"subprocess exited with code {exc.returncode}"
        print(f"[ERROR] schedule id={schedule.id} failed: {error}")
    except Exception as exc:  # noqa: BLE001
        status = "FAILED"
        error = str(exc)
        print(f"[ERROR] schedule id={schedule.id} failed: {error}")

    _update_schedule_run_state(schedule.id, status=status, error=error)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> None:
    args = _parse_args()

    if args.mode == "run-due":
        now = dt.datetime.now(dt.timezone.utc)
        schedules = _load_due_schedules(now)
        if not schedules:
            print("[INFO] no due model_schedule rows to run")
            return
        print(f"[INFO] found {len(schedules)} due schedules at {now.isoformat()}")
        for sch in schedules:
            _execute_schedule(sch, dry_run=args.dry_run)
        return

    if args.mode == "run-once":
        schedule = _load_single_schedule(
            schedule_id=args.id,
            model_name=args.model_name,
            schedule_name=args.schedule_name,
            task_type=args.task_type,
        )
        _execute_schedule(schedule, dry_run=args.dry_run)
        return


if __name__ == "__main__":
    main()
