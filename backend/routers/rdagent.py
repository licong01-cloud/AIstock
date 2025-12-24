from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..db.pg_pool import get_conn
from ..services.rdagent_import_service import import_best_workspace
from ..services.rdagent_registry_service import RDRegistryReader
from ..services.rdagent_signals_service import (
    load_signals_for_date,
    load_signals_overview,
    load_symbol_series,
)


router = APIRouter(prefix="/rdagent", tags=["rdagent"])


def _normalize_workspace_path(raw: str) -> str:
    p = (raw or "").strip()
    if os.name != "nt":
        return p
    if p.startswith("/mnt/") and len(p) > 6:
        drive = p[5]
        if p[6:7] == "/":
            rest = p[7:]
            return f"{drive.upper()}:/{rest}"
    return p


def _safe_read_json(abs_path: Path) -> Any | None:
    try:
        if not abs_path.exists() or not abs_path.is_file():
            return None
        if abs_path.stat().st_size > 2 * 1024 * 1024:
            return {"_error": "file too large", "path": str(abs_path)}
        return json.loads(abs_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {"_error": str(exc), "path": str(abs_path)}


class ImportRequest(BaseModel):
    task_run_id: str
    loop_id: int
    workspace_id: str
    strategy_name: Optional[str] = None
    strategy_kind: Optional[str] = Field(default=None, description="portfolio/single_symbol")
    output_mode: Optional[str] = Field(default=None, description="target_weight/topk")
    enabled: bool = True


@router.get("/strategies", summary="列出已导入的 RD-Agent 策略")
def list_rdagent_strategies(
    enabled: Optional[bool] = Query(None, description="按是否启用过滤"),
) -> Dict[str, Any]:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = [
                    "SELECT s.strategy_id, s.strategy_name, s.strategy_kind, s.output_mode,",
                    "       s.enabled, s.created_at, s.updated_at,",
                    "       s.source_strategy_key, ss.source_type, ss.name AS source_name",
                    "FROM trading.strategy AS s",
                    "JOIN trading.strategy_source AS ss ON s.source_id = ss.source_id",
                    "WHERE ss.source_type = 'rdagent'",
                ]
                params: list[Any] = []
                if enabled is not None:
                    sql.append("AND s.enabled = %s")
                    params.append(enabled)
                sql.append("ORDER BY s.created_at DESC")
                cur.execute("\n".join(sql), params)
                rows = cur.fetchall()

        strategies: List[Dict[str, Any]] = []
        for r in rows:
            (
                strategy_id,
                strategy_name,
                strategy_kind,
                output_mode,
                enabled_val,
                created_at,
                updated_at,
                source_strategy_key,
                source_type,
                source_name,
            ) = r
            strategies.append(
                {
                    "strategy_id": str(strategy_id),
                    "strategy_name": strategy_name,
                    "strategy_kind": strategy_kind,
                    "output_mode": output_mode,
                    "enabled": bool(enabled_val),
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "source_strategy_key": source_strategy_key,
                    "source_type": source_type,
                    "source_name": source_name,
                },
            )

        return {"items": strategies}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/strategies/{strategy_id}/result", summary="获取 RD-Agent 策略的回测结果概览")
def get_rdagent_strategy_result(strategy_id: str) -> Dict[str, Any]:
    """Return minimal backtest metrics and equity curve for a given RD-Agent strategy.

    This endpoint:
    - looks up the strategy row in trading.strategy and ensures source_type='rdagent';
    - parses source_strategy_key to extract task_run/loop/workspace identifiers;
    - uses RDRegistryReader to locate the corresponding workspace in RD-Agent registry;
    - reads backtest_metrics (qlib_res.csv) and backtest_curve (ret.pkl) if available;
    - returns key metrics and an equity curve series.
    """

    try:
        # 1) resolve RD-Agent source strategy
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.source_strategy_key
                    FROM trading.strategy AS s
                    JOIN trading.strategy_source AS ss ON s.source_id = ss.source_id
                    WHERE s.strategy_id = %s
                      AND ss.source_type = 'rdagent'
                    """,
                    (strategy_id,),
                )
                row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="strategy not found or not rdagent source")

        source_strategy_key = row[0]

        # 2) parse workspace_id from source_strategy_key: task_run:XXX/loop:YYY/workspace:ZZZ
        workspace_id: Optional[str] = None
        try:
            parts = str(source_strategy_key).split("/")
            for p in parts:
                if p.startswith("workspace:"):
                    workspace_id = p.split(":", 1)[1]
                    break
        except Exception:
            workspace_id = None

        if not workspace_id:
            raise HTTPException(status_code=400, detail="invalid source_strategy_key format (missing workspace)")

        # 3) locate workspace in RD-Agent registry
        db_path = RDRegistryReader.resolve_db_path()
        reader = RDRegistryReader(db_path)
        try:
            ws = reader.get_workspace(workspace_id)
        except KeyError as exc:  # noqa: PERF203
            raise HTTPException(status_code=404, detail=str(exc))

        raw_workspace_path = _normalize_workspace_path(ws.workspace_path)
        workspace_root = Path(raw_workspace_path)
        if not workspace_root.exists():
            raise HTTPException(status_code=404, detail=f"workspace_path not found: {workspace_root}")

        # 4) resolve artifact file paths relative to workspace
        metrics_rel = reader.find_backtest_metrics_file(workspace_id)
        curve_rel = reader.find_backtest_curve_file(workspace_id)

        metrics_abs = workspace_root / metrics_rel if metrics_rel else None
        curve_abs = workspace_root / curve_rel if curve_rel else None

        metrics: Dict[str, Any] = {}
        equity_curve: List[Dict[str, Any]] = []

        # 5) load metrics from qlib_res.csv (if present)
        if metrics_abs and metrics_abs.exists():
            try:
                df_metrics = pd.read_csv(metrics_abs)
                if not df_metrics.empty:
                    row0 = df_metrics.iloc[0].to_dict()
                    # best-effort selection of common fields
                    preferred_keys = [
                        "ann_ret",
                        "annual_return",
                        "excess_return_annual",
                        "max_drawdown",
                        "mdd",
                        "sharpe",
                        "information_ratio",
                        "info_ratio",
                        "ic",
                        "ic_mean",
                    ]
                    for k in preferred_keys:
                        if k in row0:
                            metrics[k] = row0[k]
                    # always include raw row for inspection
                    metrics["raw"] = row0
            except Exception as exc:  # noqa: BLE001
                metrics["_error"] = str(exc)

        # 6) load equity curve from ret.pkl (if present)
        if curve_abs and curve_abs.exists():
            try:
                obj = pd.read_pickle(curve_abs)
                # heuristics: Series or DataFrame
                if isinstance(obj, pd.Series):
                    series = obj
                elif isinstance(obj, pd.DataFrame):
                    col = None
                    for c in [
                        "cum",
                        "cum_ret",
                        "nav",
                        "equity",
                        "value",
                        "portfolio_value",
                    ]:
                        if c in obj.columns:
                            col = c
                            break
                    if col is None:
                        col = obj.columns[0]
                    series = obj[col]
                else:
                    series = None

                if series is not None:
                    series = series.dropna().copy()
                    # attempt to interpret as returns and build cumulative nav if looks small
                    vals = series.astype(float)
                    if (vals.abs() < 0.5).all():
                        nav = (1.0 + vals).cumprod()
                    else:
                        nav = vals
                    # convert to list of {date, nav}
                    if isinstance(nav.index, (pd.DatetimeIndex, pd.PeriodIndex)):
                        for ts, v in nav.items():
                            equity_curve.append({"date": str(ts.date()), "nav": float(v)})
                    else:
                        for i, v in enumerate(nav.values):
                            equity_curve.append({"index": int(i), "nav": float(v)})
            except Exception as exc:  # noqa: BLE001
                metrics.setdefault("curve_error", str(exc))

        return {
            "registry_db_path": db_path,
            "workspace_id": workspace_id,
            "workspace_path": ws.workspace_path,
            "metrics": metrics,
            "equity_curve": equity_curve,
        }
    except HTTPException:
        # already structured
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/strategies/{strategy_id}/versions", summary="列出指定 RD-Agent 策略的所有版本")
def list_rdagent_strategy_versions(strategy_id: str) -> Dict[str, Any]:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT strategy_version_id, version_tag, artifact_root_path,
                           import_status, created_at
                    FROM trading.strategy_version
                    WHERE strategy_id = %s
                    ORDER BY created_at DESC
                    """,
                    (strategy_id,),
                )
                rows = cur.fetchall()

        versions: List[Dict[str, Any]] = []
        for r in rows:
            strategy_version_id, version_tag, artifact_root_path, import_status, created_at = r
            versions.append(
                {
                    "strategy_version_id": str(strategy_version_id),
                    "version_tag": version_tag,
                    "artifact_root_path": artifact_root_path,
                    "import_status": import_status,
                    "created_at": created_at,
                },
            )

        return {"items": versions}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/candidates", summary="列出 RD-Agent 候选（best workspace）")
def list_candidates(
    task_run_limit: int = Query(20, ge=1, le=200),
    loops_per_task_limit: int = Query(50, ge=1, le=500),
    has_result_only: bool = Query(True),
    mode: str = Query("signals", description="candidates discovery mode: signals|best_workspace"),
) -> Dict[str, Any]:
    try:
        db_path = RDRegistryReader.resolve_db_path()
        reader = RDRegistryReader(db_path)
        candidates: List[Dict[str, Any]] = []

        if mode == "signals":
            hints = reader.list_workspaces_with_signals(limit=500)
            for h in hints:
                candidates.append(
                    {
                        "task_run_id": h.task_run_id,
                        "scenario": None,
                        "loop_id": h.loop_id,
                        "action": h.action,
                        "has_result": None,
                        "best_workspace_id": h.workspace_id,
                        "workspace_path": h.workspace_path,
                        "manifest_path": h.manifest_path,
                        "summary_path": h.summary_path,
                        "ic_mean": h.ic_mean,
                        "ann_return": h.ann_return,
                        "mdd": h.mdd,
                        "turnover": h.turnover,
                        "multi_score": h.multi_score,
                        "workspace_role": h.workspace_role,
                        "experiment_type": h.experiment_type,
                        "has_signals": True,
                    }
                )

            return {"registry_db_path": db_path, "candidates": candidates}

        # legacy: best_workspace mode
        task_runs = reader.list_task_runs(limit=task_run_limit)
        for tr in task_runs:
            loops = reader.list_loops(tr.task_run_id)
            if loops_per_task_limit and len(loops) > loops_per_task_limit:
                loops = loops[:loops_per_task_limit]

            for lp in loops:
                if has_result_only and not lp.has_result:
                    continue
                if not lp.best_workspace_id:
                    continue
                ws = reader.get_workspace(lp.best_workspace_id)
                sig_parquet, sig_json = reader.find_signal_files(lp.best_workspace_id)
                has_signals = bool(sig_parquet or sig_json)

                candidates.append(
                    {
                        "task_run_id": tr.task_run_id,
                        "scenario": tr.scenario,
                        "loop_id": lp.loop_id,
                        "action": lp.action,
                        "has_result": bool(lp.has_result),
                        "best_workspace_id": lp.best_workspace_id,
                        "workspace_path": ws.workspace_path,
                        "manifest_path": ws.manifest_path,
                        "summary_path": ws.summary_path,
                        "ic_mean": lp.ic_mean,
                        "ann_return": lp.ann_return,
                        "mdd": lp.mdd,
                        "turnover": lp.turnover,
                        "multi_score": lp.multi_score,
                        "has_signals": has_signals,
                    }
                )

        return {"registry_db_path": db_path, "candidates": candidates}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/candidates/detail", summary="获取候选详情（manifest/summary 预览）")
def candidate_detail(
    task_run_id: str = Query(...),
    loop_id: int = Query(...),
    workspace_id: str = Query(...),
) -> Dict[str, Any]:
    try:
        db_path = RDRegistryReader.resolve_db_path()
        reader = RDRegistryReader(db_path)
        ws = reader.get_workspace(workspace_id)

        raw_workspace_path = _normalize_workspace_path(ws.workspace_path)
        workspace_root = Path(raw_workspace_path)
        manifest_json = None
        summary_json = None

        if ws.manifest_path:
            manifest_json = _safe_read_json(workspace_root / ws.manifest_path)
        if ws.summary_path:
            summary_json = _safe_read_json(workspace_root / ws.summary_path)

        return {
            "registry_db_path": db_path,
            "candidate": {
                "task_run_id": task_run_id,
                "loop_id": loop_id,
                "workspace_id": workspace_id,
                "workspace_path": ws.workspace_path,
                "workspace_role": ws.workspace_role,
                "experiment_type": ws.experiment_type,
                "manifest_path": ws.manifest_path,
                "summary_path": ws.summary_path,
                "manifest_json": manifest_json,
                "summary_json": summary_json,
            },
        }
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/signals/overview", summary="signals 概览")
def signals_overview(strategy_version_id: str = Query(...)) -> Dict[str, Any]:
    try:
        return load_signals_overview(strategy_version_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/signals/by_date", summary="按日期获取 signals")
def signals_by_date(
    strategy_version_id: str = Query(...),
    trade_date: str = Query(..., description="YYYY-MM-DD"),
    k: int = Query(50, ge=1, le=5000),
) -> Dict[str, Any]:
    try:
        rows = load_signals_for_date(strategy_version_id, trade_date, k=k)
        return {"trade_date": trade_date, "rows": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/signals/symbol_series", summary="按标的获取 signals 时间序列")
def symbol_series(
    strategy_version_id: str = Query(...),
    symbol: str = Query(...),
    limit: int = Query(200, ge=1, le=5000),
) -> Dict[str, Any]:
    try:
        rows = load_symbol_series(strategy_version_id, symbol, limit=limit)
        return {"symbol": symbol, "rows": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/import", summary="导入 RD-Agent best workspace 到 AIstock")
def import_candidate(req: ImportRequest) -> Dict[str, Any]:
    try:
        db_path = RDRegistryReader.resolve_db_path()
        result = import_best_workspace(
            registry_db_path=db_path,
            task_run_id=req.task_run_id,
            loop_id=req.loop_id,
            workspace_id=req.workspace_id,
            strategy_name=req.strategy_name,
            strategy_kind=req.strategy_kind,
            output_mode=req.output_mode,
            enabled=req.enabled,
        )
        return {
            "ok": True,
            "strategy_id": result.strategy_id,
            "strategy_version_id": result.strategy_version_id,
            "artifact_root_path": result.artifact_root_path,
            "strategy_kind": result.inferred_strategy_kind,
            "output_mode": result.inferred_output_mode,
        }
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
