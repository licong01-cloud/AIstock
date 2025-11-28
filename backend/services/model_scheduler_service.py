from __future__ import annotations

from typing import Any, Dict, List, Optional

import json
import subprocess
import sys

from ..db.pg_pool import get_conn


def list_schedules(
    model_name: Optional[str] = None,
    task_type: Optional[str] = None,
    enabled: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    """列出 app.model_schedule 中的调度计划。"""

    where: List[str] = []
    params: List[Any] = []

    if model_name is not None:
        where.append("model_name = %s")
        params.append(model_name)
    if task_type is not None:
        where.append("task_type = %s")
        params.append(task_type)
    if enabled is not None:
        where.append("enabled = %s")
        params.append(enabled)

    where_sql = " WHERE " + " AND ".join(where) if where else ""

    sql = f"""
        SELECT
          id,
          model_name,
          schedule_name,
          task_type,
          frequency,
          enabled,
          config_json,
          last_run_at,
          next_run_at,
          last_status,
          last_error,
          created_at,
          updated_at
        FROM app.model_schedule
        {where_sql}
        ORDER BY model_name, schedule_name, task_type
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        cfg = r[6]
        if isinstance(cfg, str):
            try:
                cfg = json.loads(cfg)
            except Exception:  # noqa: BLE001
                cfg = {"raw": cfg}
        out.append(
            {
                "id": r[0],
                "model_name": r[1],
                "schedule_name": r[2],
                "task_type": r[3],
                "frequency": r[4],
                "enabled": bool(r[5]),
                "config_json": cfg,
                "last_run_at": r[7],
                "next_run_at": r[8],
                "last_status": r[9],
                "last_error": r[10],
                "created_at": r[11],
                "updated_at": r[12],
            }
        )
    return out


def upsert_schedule(
    model_name: str,
    schedule_name: str,
    task_type: str,
    frequency: str,
    enabled: bool,
    config_json: Dict[str, Any],
) -> int:
    """创建或更新一条调度计划，按 (model_name, schedule_name, task_type) 唯一。"""

    payload = json.dumps(config_json, ensure_ascii=False)

    sql = """
        INSERT INTO app.model_schedule (
            model_name,
            schedule_name,
            task_type,
            frequency,
            enabled,
            config_json
        )
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (model_name, schedule_name, task_type)
        DO UPDATE SET
            frequency = EXCLUDED.frequency,
            enabled = EXCLUDED.enabled,
            config_json = EXCLUDED.config_json,
            updated_at = NOW()
        RETURNING id
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (model_name, schedule_name, task_type, frequency, enabled, payload),
            )
            row = cur.fetchone()
            conn.commit()
    return int(row[0])


def update_schedule(
    schedule_id: int,
    frequency: Optional[str] = None,
    enabled: Optional[bool] = None,
    config_json: Optional[Dict[str, Any]] = None,
) -> bool:
    """按 id 部分更新调度计划。"""

    sets: List[str] = []
    params: List[Any] = []

    if frequency is not None:
        sets.append("frequency = %s")
        params.append(frequency)
    if enabled is not None:
        sets.append("enabled = %s")
        params.append(enabled)
    if config_json is not None:
        sets.append("config_json = %s::jsonb")
        params.append(json.dumps(config_json, ensure_ascii=False))

    if not sets:
        return False

    sets.append("updated_at = NOW()")

    sql = f"""
        UPDATE app.model_schedule
           SET {', '.join(sets)}
         WHERE id = %s
    """
    params.append(schedule_id)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            updated = cur.rowcount
            conn.commit()
    return updated > 0


def delete_schedule(schedule_id: int) -> bool:
    sql = "DELETE FROM app.model_schedule WHERE id = %s"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (schedule_id,))
            deleted = cur.rowcount
            conn.commit()
    return deleted > 0


def get_schedule_by_id(schedule_id: int) -> Optional[Dict[str, Any]]:
    sql = """
        SELECT
          id,
          model_name,
          schedule_name,
          task_type,
          frequency,
          enabled,
          config_json,
          last_run_at,
          next_run_at,
          last_status,
          last_error,
          created_at,
          updated_at
        FROM app.model_schedule
       WHERE id = %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (schedule_id,))
            row = cur.fetchone()
    if row is None:
        return None

    cfg = row[6]
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg)
        except Exception:  # noqa: BLE001
            cfg = {"raw": cfg}
    return {
        "id": row[0],
        "model_name": row[1],
        "schedule_name": row[2],
        "task_type": row[3],
        "frequency": row[4],
        "enabled": bool(row[5]),
        "config_json": cfg,
        "last_run_at": row[7],
        "next_run_at": row[8],
        "last_status": row[9],
        "last_error": row[10],
        "created_at": row[11],
        "updated_at": row[12],
    }


def trigger_schedule_run_async(schedule_id: int, dry_run: bool = False) -> None:
    """后台触发一次 scheduler.run-once，不阻塞当前请求。"""

    cmd = [
        sys.executable,
        "-m",
        "backend.model_scheduler.scheduler",
        "run-once",
        "--id",
        str(schedule_id),
    ]
    if dry_run:
        cmd.append("--dry-run")

    # 后台启动子进程，不等待完成
    subprocess.Popen(cmd)  # noqa: S603, S607


def list_train_runs(
    model_name: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    where: List[str] = []
    params: List[Any] = []

    if model_name is not None:
        where.append("model_name = %s")
        params.append(model_name)
    if status is not None:
        where.append("status = %s")
        params.append(status)

    where_sql = " WHERE " + " AND ".join(where) if where else ""

    sql = f"""
        SELECT
          id,
          model_name,
          config_snapshot,
          status,
          start_time,
          end_time,
          duration_seconds,
          symbols_covered_count,
          time_range_start,
          time_range_end,
          data_granularity,
          metrics_json,
          log_path
        FROM app.model_train_run
        {where_sql}
        ORDER BY start_time DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        cfg = r[2]
        if isinstance(cfg, str):
            try:
                cfg = json.loads(cfg)
            except Exception:  # noqa: BLE001
                cfg = {"raw": cfg}
        metrics = r[11]
        if isinstance(metrics, str):
            try:
                metrics = json.loads(metrics)
            except Exception:  # noqa: BLE001
                metrics = {"raw": metrics}
        out.append(
            {
                "id": r[0],
                "model_name": r[1],
                "config_snapshot": cfg,
                "status": r[3],
                "start_time": r[4],
                "end_time": r[5],
                "duration_seconds": r[6],
                "symbols_covered_count": r[7],
                "time_range_start": r[8],
                "time_range_end": r[9],
                "data_granularity": r[10],
                "metrics_json": metrics,
                "log_path": r[12],
            }
        )
    return out


def list_inference_runs(
    model_name: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    where: List[str] = []
    params: List[Any] = []

    if model_name is not None:
        where.append("model_name = %s")
        params.append(model_name)
    if status is not None:
        where.append("status = %s")
        params.append(status)

    where_sql = " WHERE " + " AND ".join(where) if where else ""

    sql = f"""
        SELECT
          id,
          model_name,
          schedule_name,
          config_snapshot,
          status,
          start_time,
          end_time,
          duration_seconds,
          symbols_covered,
          time_of_data,
          metrics_json
        FROM app.model_inference_run
        {where_sql}
        ORDER BY start_time DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        cfg = r[3]
        if isinstance(cfg, str):
            try:
                cfg = json.loads(cfg)
            except Exception:  # noqa: BLE001
                cfg = {"raw": cfg}
        metrics = r[10]
        if isinstance(metrics, str):
            try:
                metrics = json.loads(metrics)
            except Exception:  # noqa: BLE001
                metrics = {"raw": metrics}
        out.append(
            {
                "id": r[0],
                "model_name": r[1],
                "schedule_name": r[2],
                "config_snapshot": cfg,
                "status": r[4],
                "start_time": r[5],
                "end_time": r[6],
                "duration_seconds": r[7],
                "symbols_covered": r[8],
                "time_of_data": r[9],
                "metrics_json": metrics,
            }
        )
    return out


def get_model_status(model_name: str) -> Dict[str, Any]:
    """返回某个模型最近一次训练与推理 run 摘要。"""

    last_train: Optional[Dict[str, Any]] = None
    last_infer: Optional[Dict[str, Any]] = None

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  id,
                  status,
                  start_time,
                  end_time,
                  duration_seconds,
                  metrics_json
                FROM app.model_train_run
                WHERE model_name = %s
                ORDER BY start_time DESC
                LIMIT 1
                """,
                (model_name,),
            )
            row = cur.fetchone()
            if row is not None:
                metrics = row[5]
                if isinstance(metrics, str):
                    try:
                        metrics = json.loads(metrics)
                    except Exception:  # noqa: BLE001
                        metrics = {"raw": metrics}
                last_train = {
                    "id": row[0],
                    "status": row[1],
                    "start_time": row[2],
                    "end_time": row[3],
                    "duration_seconds": row[4],
                    "metrics_json": metrics,
                }

            cur.execute(
                """
                SELECT
                  id,
                  schedule_name,
                  status,
                  start_time,
                  end_time,
                  duration_seconds,
                  symbols_covered,
                  time_of_data,
                  metrics_json
                FROM app.model_inference_run
                WHERE model_name = %s
                ORDER BY start_time DESC
                LIMIT 1
                """,
                (model_name,),
            )
            row2 = cur.fetchone()
            if row2 is not None:
                metrics2 = row2[8]
                if isinstance(metrics2, str):
                    try:
                        metrics2 = json.loads(metrics2)
                    except Exception:  # noqa: BLE001
                        metrics2 = {"raw": metrics2}
                last_infer = {
                    "id": row2[0],
                    "schedule_name": row2[1],
                    "status": row2[2],
                    "start_time": row2[3],
                    "end_time": row2[4],
                    "duration_seconds": row2[5],
                    "symbols_covered": row2[6],
                    "time_of_data": row2[7],
                    "metrics_json": metrics2,
                }

    return {
        "model_name": model_name,
        "last_train": last_train,
        "last_inference": last_infer,
    }
