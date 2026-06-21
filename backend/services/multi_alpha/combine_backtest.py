"""Tier-1 multi-alpha combine-backtest orchestration.

This module upgrades the P3-B one-off script into a service-level job that can
be called by REST/MCP. It does not train alpha legs and does not modify the QE
task model; it only writes combined prediction pickles and delegates execution
to the existing pred-backtest primitive.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

import pandas as pd
from psycopg2.extras import Json, RealDictCursor
import requests

from backend.db.pg_pool import get_conn
from backend.services.model_store import ModelStoreService, PredictionStoreError
from backend.services.multi_alpha.combiner import CombinerLeg, MultiAlphaCombiner, MultiAlphaCombinerError
from backend.services.multi_alpha.orthogonality import MultiAlphaOrthogonalityError, normalize_prediction_frame
from backend.services.multi_alpha.panels import MultiAlphaPanelBuilder, MultiAlphaPanelError, PanelLegSpec


COMBINE_BACKTEST_CONFIRM = "MULTI_ALPHA_COMBINE_BACKTEST_RUN"
DEFAULT_WEIGHTING_SCHEMES = ("equal", "orthogonality_aware", "ic_weighted", "risk_parity")
RANK_FUSION_WEIGHTING_SCHEMES = ("rank_fusion_rrf", "rank_fusion_borda")
SUPPORTED_WEIGHTING_SCHEMES = DEFAULT_WEIGHTING_SCHEMES + RANK_FUSION_WEIGHTING_SCHEMES
LOGICAL_PARTIAL_FAILED_STATUS = "partial_failed"
TERMINAL_STATUSES = {"succeeded", "failed", LOGICAL_PARTIAL_FAILED_STATUS}
PREDICTION_STORE_UPLOAD_URL_ENV = "AISTOCK_PREDICTION_STORE_UPLOAD_URL"
PREDICTION_STORE_UPLOAD_TIMEOUT_ENV = "AISTOCK_PREDICTION_STORE_UPLOAD_TIMEOUT_SEC"
DEFAULT_UPLOAD_TIMEOUT_SEC = 120.0
ACTIVE_QE_LOOP_STATUSES = ("running", "processing")
_NODE_RESERVATIONS: dict[str, int] = {}
_NODE_RESERVATIONS_LOCK = threading.Lock()


class MultiAlphaCombineBacktestError(RuntimeError):
    """Raised when a combine-backtest job cannot proceed safely."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str = "combine_backtest_error",
        leg_id: str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        self.reason_code = reason_code
        self.leg_id = leg_id
        self.context = dict(context or {})
        prefix = f"reason_code={reason_code}"
        if leg_id:
            prefix += f" leg_id={leg_id}"
        super().__init__(f"{prefix}: {message}")


@dataclass(frozen=True)
class CombineBacktestRequest:
    roster: tuple[PanelLegSpec, ...]
    oos_start: str
    oos_end: str
    weighting_schemes: tuple[str, ...] = DEFAULT_WEIGHTING_SCHEMES
    normalize_method: str = "zscore"
    walk_forward: Mapping[str, Any] = field(default_factory=lambda: {"enabled": True, "window": 60, "min_periods": 2})
    rank_fusion: Mapping[str, Any] = field(default_factory=dict)
    backtest_config: Mapping[str, Any] = field(default_factory=dict)
    baseline_leg_id: str | None = None
    topk: int = 20
    min_date_coverage: float = 0.8
    run_async: bool = True


@dataclass(frozen=True)
class _PredictionTask:
    name: str
    kind: str
    frame: pd.DataFrame
    critical: bool = False
    scheme: str | None = None
    dropped_leg_id: str | None = None
    weights_json: Mapping[str, Any] | None = None
    per_window_weights_json: Sequence[Mapping[str, Any]] | None = None


@dataclass(frozen=True)
class _PredictionTaskOutcome:
    task: _PredictionTask
    metrics: Mapping[str, Any] | None = None
    error: Mapping[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None and self.metrics is not None


class BacktestExecutor(Protocol):
    def execute_pred_backtest(
        self,
        *,
        workspace: Path,
        pred_pkl: Path,
        node_id: str,
        backtest_config: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Run or parse a pred-backtest workspace and return metrics."""


class NodeCapacityChecker(Protocol):
    def ensure_slot_available(
        self,
        *,
        node_id: str,
        limit: int,
        run_id: str,
        backtest_name: str,
    ) -> dict[str, Any]:
        """Fail loudly when the selected QE node is already at configured capacity."""

    def release_slot(self, capacity: Mapping[str, Any]) -> None:
        """Release a previously acquired local reservation."""


class DatabaseQENodeCapacityChecker:
    """Read existing QE loop activity so combine backtests do not steal saturated nodes."""

    def __init__(self, connection_provider=get_conn) -> None:
        self._connection_provider = connection_provider

    def ensure_slot_available(
        self,
        *,
        node_id: str,
        limit: int,
        run_id: str,
        backtest_name: str,
    ) -> dict[str, Any]:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS active_count
                    FROM qe_evolution_loops
                    WHERE node_id = %s
                      AND status = ANY(%s)
                    """,
                    (node_id, list(ACTIVE_QE_LOOP_STATUSES)),
                )
                row = cur.fetchone() or {}
        active_count = int(row.get("active_count") or 0)
        with _NODE_RESERVATIONS_LOCK:
            local_running = int(_NODE_RESERVATIONS.get(node_id, 0))
            total_after_reserve = active_count + local_running + 1
            available = total_after_reserve <= int(limit)
            if available:
                _NODE_RESERVATIONS[node_id] = local_running + 1
        payload = {
            "node_id": node_id,
            "limit": int(limit),
            "active_count": active_count,
            "local_reserved_count": local_running,
            "active_statuses": list(ACTIVE_QE_LOOP_STATUSES),
            "run_id": run_id,
            "backtest_name": backtest_name,
        }
        if not available:
            raise MultiAlphaCombineBacktestError(
                "selected QE node is at configured node_parallelism capacity; refusing to bypass running QE work",
                reason_code="node_capacity_exhausted",
                context=payload,
            )
        payload["available"] = True
        return payload

    def release_slot(self, capacity: Mapping[str, Any]) -> None:
        node_id = str(capacity.get("node_id") or "").strip()
        if not node_id:
            return
        with _NODE_RESERVATIONS_LOCK:
            current = int(_NODE_RESERVATIONS.get(node_id, 0))
            if current <= 1:
                _NODE_RESERVATIONS.pop(node_id, None)
            else:
                _NODE_RESERVATIONS[node_id] = current - 1


class ShellPredBacktestExecutor:
    """Thin adapter around the existing qrun_limit_minute pred-backtest primitive."""

    def execute_pred_backtest(
        self,
        *,
        workspace: Path,
        pred_pkl: Path,
        node_id: str,
        backtest_config: Mapping[str, Any],
    ) -> dict[str, Any]:
        workspace.mkdir(parents=True, exist_ok=True)
        if not pred_pkl.exists():
            raise MultiAlphaCombineBacktestError(
                f"prediction pickle does not exist: {pred_pkl}",
                reason_code="pred_pkl_missing",
                context={"workspace": str(workspace), "node_id": node_id},
            )
        result_path = workspace / "qlib_results_enhanced.json"
        if bool(backtest_config.get("parse_only")):
            return ingest_enhanced_metrics(result_path)

        prepare_pred_backtest_workspace(workspace=workspace, backtest_config=backtest_config)
        command = backtest_config.get("command")
        if command is None:
            qrun = run_command(
                [sys.executable, "qrun_limit_minute.py", "conf.yaml", "--pred-backtest", pred_pkl.name],
                cwd=workspace,
                timeout_seconds=int(backtest_config.get("timeout_seconds", 6 * 60 * 60)),
                log_prefix="pred_backtest_qrun",
            )
            if qrun.returncode != 0:
                raise MultiAlphaCombineBacktestError(
                    f"pred-backtest qrun failed with exit_code={qrun.returncode}",
                    reason_code="pred_backtest_failed",
                    context={"workspace": str(workspace), "node_id": node_id, "stderr_tail": (qrun.stderr or "")[-1000:]},
                )
            env = dict(os.environ)
            env["QE_REQUIRE_RECORDER_ID"] = "1"
            read_exp = run_command(
                [sys.executable, "read_exp_res.py"],
                cwd=workspace,
                timeout_seconds=int(backtest_config.get("read_timeout_seconds", 60 * 60)),
                log_prefix="pred_backtest_read_exp_res",
                env=env,
            )
            if read_exp.returncode != 0:
                raise MultiAlphaCombineBacktestError(
                    f"read_exp_res failed with exit_code={read_exp.returncode}",
                    reason_code="pred_backtest_ingest_failed",
                    context={"workspace": str(workspace), "node_id": node_id, "stderr_tail": (read_exp.stderr or "")[-1000:]},
                )
        else:
            completed = run_command(
                command,
                cwd=workspace,
                timeout_seconds=int(backtest_config.get("timeout_seconds", 6 * 60 * 60)),
                log_prefix="pred_backtest",
            )
            if completed.returncode != 0:
                raise MultiAlphaCombineBacktestError(
                    f"pred-backtest command failed with exit_code={completed.returncode}",
                    reason_code="pred_backtest_failed",
                    context={"workspace": str(workspace), "node_id": node_id, "stderr_tail": (completed.stderr or "")[-1000:]},
                )
        return ingest_enhanced_metrics(result_path)


def prepare_pred_backtest_workspace(*, workspace: Path, backtest_config: Mapping[str, Any]) -> None:
    """Copy an explicitly configured qrun runtime template, then verify required files."""

    template_dir = backtest_config.get("runtime_template_dir") or os.getenv("AISTOCK_MULTI_ALPHA_QRUN_TEMPLATE_DIR")
    if template_dir:
        src = Path(str(template_dir))
        if not src.exists() or not src.is_dir():
            raise MultiAlphaCombineBacktestError(
                f"runtime_template_dir does not exist or is not a directory: {src}",
                reason_code="pred_backtest_runtime_template_missing",
                context={"runtime_template_dir": str(src), "workspace": str(workspace)},
            )
        for item in src.iterdir():
            target = workspace / item.name
            if item.is_file():
                if not target.exists():
                    shutil.copy2(item, target)
            elif item.is_dir() and bool(backtest_config.get("copy_runtime_dirs")) and not target.exists():
                shutil.copytree(item, target)

    missing = [
        name
        for name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py")
        if not (workspace / name).exists()
    ]
    if missing:
        raise MultiAlphaCombineBacktestError(
            "pred-backtest runtime is incomplete; provide backtest_config.runtime_template_dir "
            "or pre-populate the workspace",
            reason_code="pred_backtest_runtime_missing",
            context={"workspace": str(workspace), "missing": missing},
        )


def run_command(
    command: str | Sequence[Any],
    *,
    cwd: Path,
    timeout_seconds: int,
    log_prefix: str,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    command_for_log: str | list[str]
    if isinstance(command, str):
        command_for_log = command
    else:
        command_for_log = [str(part) for part in command]
    completed = subprocess.run(
        command_for_log,
        cwd=cwd,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        shell=isinstance(command_for_log, str),
        timeout=timeout_seconds,
        check=False,
        env=dict(env) if env is not None else None,
    )
    (cwd / f"{log_prefix}_stdout.log").write_text(completed.stdout or "", encoding="utf-8", errors="replace")
    (cwd / f"{log_prefix}_stderr.log").write_text(completed.stderr or "", encoding="utf-8", errors="replace")
    return completed


class MultiAlphaCombineBacktestRepository:
    """PostgreSQL persistence for combine-backtest runs."""

    def __init__(self, connection_provider=get_conn) -> None:
        self._connection_provider = connection_provider

    def create_run(self, *, run_id: str, request: CombineBacktestRequest, roster_hash: str) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.multi_alpha_combine_backtest_run
                        (id, roster_hash, roster_json, oos_start, oos_end, normalize_method,
                         walk_forward_json, backtest_config_json, baseline_leg_id, status, reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'running', NULL)
                    """,
                    (
                        run_id,
                        roster_hash,
                        Json(_roster_payload(request.roster)),
                        request.oos_start,
                        request.oos_end,
                        request.normalize_method,
                        Json(dict(request.walk_forward)),
                        Json(dict(request.backtest_config)),
                        request.baseline_leg_id,
                    ),
                )

    def update_run_status(self, run_id: str, *, status: str, reason: Mapping[str, Any] | None = None) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE strategy_pkg.multi_alpha_combine_backtest_run
                    SET status = %s, reason = %s
                    WHERE id = %s
                    """,
                    (status, Json(reason) if reason is not None else None, run_id),
                )

    def insert_scheme_result(self, run_id: str, row: Mapping[str, Any]) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.multi_alpha_combine_backtest_scheme_result
                        (run_id, weighting_scheme, weights_json, per_window_weights_json,
                         cagr, max_drawdown, sharpe, calmar, topk_return_20, topk_hit_rate_20,
                         turnover, vs_baseline_sharpe_delta, vs_baseline_calmar_delta,
                         pred_persisted, skipped, skipped_reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, weighting_scheme) DO UPDATE SET
                        weights_json = EXCLUDED.weights_json,
                        per_window_weights_json = EXCLUDED.per_window_weights_json,
                        cagr = EXCLUDED.cagr,
                        max_drawdown = EXCLUDED.max_drawdown,
                        sharpe = EXCLUDED.sharpe,
                        calmar = EXCLUDED.calmar,
                        topk_return_20 = EXCLUDED.topk_return_20,
                        topk_hit_rate_20 = EXCLUDED.topk_hit_rate_20,
                        turnover = EXCLUDED.turnover,
                        vs_baseline_sharpe_delta = EXCLUDED.vs_baseline_sharpe_delta,
                        vs_baseline_calmar_delta = EXCLUDED.vs_baseline_calmar_delta,
                        pred_persisted = EXCLUDED.pred_persisted,
                        skipped = EXCLUDED.skipped,
                        skipped_reason = EXCLUDED.skipped_reason
                    """,
                    (
                        run_id,
                        row.get("weighting_scheme"),
                        Json(row.get("weights_json") or {}),
                        Json(row.get("per_window_weights_json") or []),
                        row.get("cagr"),
                        row.get("max_drawdown"),
                        row.get("sharpe"),
                        row.get("calmar"),
                        row.get("topk_return_20"),
                        row.get("topk_hit_rate_20"),
                        row.get("turnover"),
                        row.get("vs_baseline_sharpe_delta"),
                        row.get("vs_baseline_calmar_delta"),
                        bool(row.get("pred_persisted")),
                        bool(row.get("skipped")),
                        row.get("skipped_reason"),
                    ),
                )

    def insert_loo(self, run_id: str, row: Mapping[str, Any]) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO strategy_pkg.multi_alpha_combine_backtest_loo
                        (run_id, weighting_scheme, dropped_leg_id, marginal_sharpe, marginal_calmar, marginal_cagr)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, weighting_scheme, dropped_leg_id) DO UPDATE SET
                        marginal_sharpe = EXCLUDED.marginal_sharpe,
                        marginal_calmar = EXCLUDED.marginal_calmar,
                        marginal_cagr = EXCLUDED.marginal_cagr
                    """,
                    (
                        run_id,
                        row.get("weighting_scheme"),
                        row.get("dropped_leg_id"),
                        row.get("marginal_sharpe"),
                        row.get("marginal_calmar"),
                        row.get("marginal_cagr"),
                    ),
                )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_run WHERE id = %s", (run_id,))
                run = cur.fetchone()
                if not run:
                    return None
                cur.execute(
                    "SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_scheme_result WHERE run_id = %s ORDER BY weighting_scheme",
                    (run_id,),
                )
                schemes = cur.fetchall()
                cur.execute(
                    "SELECT * FROM strategy_pkg.multi_alpha_combine_backtest_loo WHERE run_id = %s ORDER BY weighting_scheme, dropped_leg_id",
                    (run_id,),
                )
                loo = cur.fetchall()
        return {"run": _with_logical_status(dict(run)), "scheme_results": [dict(row) for row in schemes], "loo": [dict(row) for row in loo]}

    def list_runs(self, *, status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 200))
        params: list[Any] = []
        where = ""
        if status:
            where = "WHERE status = %s"
            params.append("failed" if status == LOGICAL_PARTIAL_FAILED_STATUS else status)
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM strategy_pkg.multi_alpha_combine_backtest_run
                    {where}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
        payloads = [_with_logical_status(dict(row)) for row in rows]
        if status in {"failed", LOGICAL_PARTIAL_FAILED_STATUS}:
            payloads = [row for row in payloads if row.get("status") == status]
        return payloads


class InMemoryCombineBacktestRepository:
    """Test repository with the same method surface as the PostgreSQL repository."""

    def __init__(self) -> None:
        self.runs: dict[str, dict[str, Any]] = {}
        self.scheme_results: list[dict[str, Any]] = []
        self.loo: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def create_run(self, *, run_id: str, request: CombineBacktestRequest, roster_hash: str) -> None:
        self.runs[run_id] = {
            "id": run_id,
            "roster_hash": roster_hash,
            "roster_json": _roster_payload(request.roster),
            "oos_start": request.oos_start,
            "oos_end": request.oos_end,
            "normalize_method": request.normalize_method,
            "walk_forward_json": dict(request.walk_forward),
            "backtest_config_json": dict(request.backtest_config),
            "baseline_leg_id": request.baseline_leg_id,
            "status": "running",
            "reason": None,
            "created_at": utc_now_iso(),
        }

    def update_run_status(self, run_id: str, *, status: str, reason: Mapping[str, Any] | None = None) -> None:
        with self._lock:
            self.runs[run_id]["status"] = status
            self.runs[run_id]["reason"] = dict(reason or {}) if reason is not None else None

    def insert_scheme_result(self, run_id: str, row: Mapping[str, Any]) -> None:
        with self._lock:
            self.scheme_results.append({"run_id": run_id, **dict(row)})

    def insert_loo(self, run_id: str, row: Mapping[str, Any]) -> None:
        with self._lock:
            self.loo.append({"run_id": run_id, **dict(row)})

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            run = self.runs.get(run_id)
            if not run:
                return None
            return {
                "run": _with_logical_status(dict(run)),
                "scheme_results": [dict(row) for row in self.scheme_results if row["run_id"] == run_id],
                "loo": [dict(row) for row in self.loo if row["run_id"] == run_id],
            }

    def list_runs(self, *, status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            rows = [_with_logical_status(dict(row)) for row in self.runs.values()]
            if status:
                rows = [row for row in rows if row["status"] == status]
        return rows[:limit]


class MultiAlphaCombineBacktestService:
    """Create, run, persist, and inspect combine-backtest jobs."""

    def __init__(
        self,
        *,
        panel_builder: MultiAlphaPanelBuilder | None = None,
        prediction_loader: Any | None = None,
        executor: BacktestExecutor | None = None,
        repository: Any | None = None,
        capacity_checker: NodeCapacityChecker | None = None,
        workspace_root: str | Path | None = None,
        clock: CallableUtc | None = None,
    ) -> None:
        self._panel_builder = panel_builder or MultiAlphaPanelBuilder()
        self._prediction_loader = prediction_loader
        self._model_store = ModelStoreService()
        self._executor = executor or ShellPredBacktestExecutor()
        self._repository = repository or MultiAlphaCombineBacktestRepository()
        self._capacity_checker = capacity_checker or DatabaseQENodeCapacityChecker()
        self._workspace_root = Path(workspace_root or os.getenv("AISTOCK_MULTI_ALPHA_BACKTEST_ROOT") or "rdagent_assets/multi_alpha_combine_backtests")
        self._clock = clock or utc_now

    def submit_run(self, payload: Mapping[str, Any], *, run_async: bool | None = None) -> dict[str, Any]:
        request = parse_request(payload)
        if run_async is not None:
            request = _replace_request(request, run_async=run_async)
        roster_hash = roster_hash_for(request.roster)
        run_id = make_run_id(roster_hash=roster_hash, oos_start=request.oos_start, oos_end=request.oos_end, ts=self._clock())
        self._repository.create_run(run_id=run_id, request=request, roster_hash=roster_hash)
        if request.run_async:
            thread = threading.Thread(target=self.execute_run, args=(run_id, request), daemon=True)
            thread.start()
        else:
            self.execute_run(run_id, request)
        return {"run_id": run_id, "status": self.get_run(run_id)["run"]["status"]}

    def execute_run(self, run_id: str, request: CombineBacktestRequest) -> dict[str, Any]:
        try:
            payload = self._execute_run(run_id, request)
        except Exception as exc:
            reason = error_payload(exc)
            self._repository.update_run_status(run_id, status="failed", reason=reason)
            raise
        status = str(payload.get("status") or "succeeded")
        reason = payload.get("reason") if isinstance(payload.get("reason"), Mapping) else None
        self._repository.update_run_status(run_id, status=_persisted_run_status(status), reason=reason)
        if status == "failed":
            first_child_error = _first_child_error(reason)
            raise MultiAlphaCombineBacktestError(
                "combine-backtest run failed; see run.reason failed_child_tasks for details",
                reason_code=str((first_child_error or {}).get("reason_code") or "combine_backtest_child_failed"),
                context={"run_id": run_id, "reason": dict(reason or {})},
            )
        return payload

    def get_run(self, run_id: str) -> dict[str, Any]:
        payload = self._repository.get_run(run_id)
        if payload is None:
            raise MultiAlphaCombineBacktestError(f"run not found: {run_id}", reason_code="run_not_found")
        return payload

    def list_runs(self, *, status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        return self._repository.list_runs(status=status, limit=limit)

    def _execute_run(self, run_id: str, request: CombineBacktestRequest) -> dict[str, Any]:
        node_id = str(request.backtest_config.get("node_id") or "wsl2-5080")
        node_parallelism = validate_node_parallelism(node_id=node_id, backtest_config=request.backtest_config)
        needs_panel_metrics = any(not is_rank_fusion_scheme(scheme) for scheme in request.weighting_schemes)
        if needs_panel_metrics:
            panels = self._panel_builder.build_combiner_legs(
                legs=request.roster,
                oos_start=request.oos_start,
                oos_end=request.oos_end,
                topk=request.topk,
                min_date_coverage=request.min_date_coverage,
            )
            prediction_legs = _strip_panel_metrics(panels)
        else:
            panels = self._build_prediction_only_legs(request)
            prediction_legs = panels
        leg_by_id = {leg.leg_id: leg for leg in panels}
        if request.baseline_leg_id and request.baseline_leg_id not in leg_by_id:
            raise MultiAlphaCombineBacktestError(
                "baseline_leg_id is not present in roster",
                reason_code="baseline_leg_missing",
                leg_id=request.baseline_leg_id,
            )

        task_specs: list[_PredictionTask] = []
        if request.baseline_leg_id:
            task_specs.append(
                _PredictionTask(
                    name=f"baseline_{request.baseline_leg_id}",
                    kind="baseline",
                    frame=leg_by_id[request.baseline_leg_id].pred_frame,
                    critical=True,
                )
            )

        for scheme in request.weighting_schemes:
            combine_input = prediction_legs if is_rank_fusion_scheme(scheme) else panels
            result = combine_legs(legs=combine_input, scheme=scheme, request=request)
            task_specs.append(
                _PredictionTask(
                    name=f"combined_{scheme}",
                    kind="scheme",
                    scheme=scheme,
                    frame=result.combined_score_frame.rename(columns={"combined_score": "score"}),
                    critical=scheme == "equal",
                    weights_json=weights_payload(result, scheme=scheme, request=request),
                    per_window_weights_json=per_window_weights_payload(result, scheme=scheme),
                )
            )
            if len(panels) <= 2:
                continue
            for dropped_leg in sorted(leg_by_id):
                source_legs = prediction_legs if is_rank_fusion_scheme(scheme) else panels
                loo_legs = [leg for leg in source_legs if leg.leg_id != dropped_leg]
                loo_result = combine_legs(legs=loo_legs, scheme=scheme, request=request)
                task_specs.append(
                    _PredictionTask(
                        name=f"loo_{scheme}_drop_{dropped_leg}",
                        kind="loo",
                        scheme=scheme,
                        dropped_leg_id=dropped_leg,
                        frame=loo_result.combined_score_frame.rename(columns={"combined_score": "score"}),
                    )
                )

        outcomes = self._run_prediction_tasks(
            run_id=run_id,
            tasks=task_specs,
            node_id=node_id,
            node_parallelism_limit=node_parallelism[node_id],
            backtest_config=request.backtest_config,
        )
        return self._persist_task_outcomes(run_id=run_id, outcomes=outcomes)

    def _build_prediction_only_legs(self, request: CombineBacktestRequest) -> list[CombinerLeg]:
        legs: list[CombinerLeg] = []
        for spec in request.roster:
            seed_frames = [self._load_prediction_frame(run_id=run_id, leg_id=spec.leg_id) for run_id in spec.seed_run_ids]
            ensemble = seed_ensemble_prediction_only(seed_frames, leg_id=spec.leg_id)
            ensemble = filter_prediction_window(
                ensemble,
                leg_id=spec.leg_id,
                oos_start=request.oos_start,
                oos_end=request.oos_end,
            )
            legs.append(CombinerLeg(leg_id=spec.leg_id, pred_frame=ensemble, metadata=dict(spec.metadata)))
        return legs

    def _load_prediction_frame(self, *, run_id: str, leg_id: str) -> pd.DataFrame:
        try:
            if self._prediction_loader is not None:
                return normalize_prediction_frame(self._prediction_loader(run_id), run_id=run_id)
            return normalize_prediction_frame(pd.read_pickle(self._model_store.prediction_path(run_id=run_id)), run_id=run_id)
        except (PredictionStoreError, MultiAlphaOrthogonalityError, OSError, ValueError, TypeError, KeyError) as exc:
            raise MultiAlphaCombineBacktestError(
                f"failed to load prediction artifact for rank-fusion: {type(exc).__name__}: {exc}",
                reason_code="prediction_missing_or_invalid",
                leg_id=leg_id,
                context={"run_id": run_id},
            ) from exc

    def _run_prediction_tasks(
        self,
        *,
        run_id: str,
        tasks: Sequence[_PredictionTask],
        node_id: str,
        node_parallelism_limit: int,
        backtest_config: Mapping[str, Any],
    ) -> list[_PredictionTaskOutcome]:
        if not tasks:
            return []
        max_workers = max(1, min(int(node_parallelism_limit), len(tasks)))
        outcomes_by_name: dict[str, _PredictionTaskOutcome] = {}
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="macb-pred") as pool:
            future_map = {
                pool.submit(
                    self._run_prediction,
                    run_id=run_id,
                    name=task.name,
                    frame=task.frame,
                    node_id=node_id,
                    node_parallelism_limit=node_parallelism_limit,
                    backtest_config=backtest_config,
                ): task
                for task in tasks
            }
            for future in as_completed(future_map):
                task = future_map[future]
                try:
                    metrics = future.result()
                    outcomes_by_name[task.name] = _PredictionTaskOutcome(task=task, metrics=metrics)
                except Exception as exc:
                    outcomes_by_name[task.name] = _PredictionTaskOutcome(task=task, error=error_payload(exc))
        return [outcomes_by_name[task.name] for task in tasks]

    def _persist_task_outcomes(self, *, run_id: str, outcomes: Sequence[_PredictionTaskOutcome]) -> dict[str, Any]:
        baseline_metrics = next(
            (dict(outcome.metrics or {}) for outcome in outcomes if outcome.task.kind == "baseline" and outcome.succeeded),
            None,
        )
        failed = [outcome for outcome in outcomes if not outcome.succeeded]
        failed_by_name = {outcome.task.name: outcome.error for outcome in failed}
        scheme_payloads: list[dict[str, Any]] = []
        full_metrics_by_scheme: dict[str, Mapping[str, Any]] = {}
        for outcome in outcomes:
            task = outcome.task
            if task.kind != "scheme":
                continue
            if outcome.succeeded:
                metrics = dict(outcome.metrics or {})
                row = {
                    "weighting_scheme": task.scheme,
                    "weights_json": dict(task.weights_json or {}),
                    "per_window_weights_json": list(task.per_window_weights_json or []),
                    **metric_columns(metrics),
                    "vs_baseline_sharpe_delta": delta(metrics.get("sharpe"), (baseline_metrics or {}).get("sharpe")),
                    "vs_baseline_calmar_delta": delta(metrics.get("calmar"), (baseline_metrics or {}).get("calmar")),
                    "pred_persisted": bool(metrics.get("pred_persisted")),
                    "skipped": False,
                    "skipped_reason": None,
                }
                full_metrics_by_scheme[str(task.scheme)] = metrics
            else:
                row = {
                    "weighting_scheme": task.scheme,
                    "weights_json": dict(task.weights_json or {}),
                    "per_window_weights_json": list(task.per_window_weights_json or []),
                    **metric_columns({}),
                    "vs_baseline_sharpe_delta": None,
                    "vs_baseline_calmar_delta": None,
                    "pred_persisted": False,
                    "skipped": True,
                    "skipped_reason": json.dumps(outcome.error or {}, ensure_ascii=False, default=str),
                }
            self._repository.insert_scheme_result(run_id, row)
            scheme_payloads.append(row)

        loo_payloads: list[dict[str, Any]] = []
        for outcome in outcomes:
            task = outcome.task
            if task.kind != "loo":
                continue
            full_metrics = full_metrics_by_scheme.get(str(task.scheme))
            if not outcome.succeeded or full_metrics is None:
                if full_metrics is None and outcome.succeeded:
                    failed_by_name[task.name] = {
                        "reason_code": "loo_full_scheme_missing",
                        "message": f"cannot compute LOO marginal because full scheme metrics are unavailable: {task.scheme}",
                        "context": {"weighting_scheme": task.scheme, "dropped_leg_id": task.dropped_leg_id},
                    }
                continue
            metrics = dict(outcome.metrics or {})
            row = {
                "weighting_scheme": task.scheme,
                "dropped_leg_id": task.dropped_leg_id,
                "marginal_sharpe": delta(full_metrics.get("sharpe"), metrics.get("sharpe")),
                "marginal_calmar": delta(full_metrics.get("calmar"), metrics.get("calmar")),
                "marginal_cagr": delta(full_metrics.get("cagr"), metrics.get("cagr")),
            }
            self._repository.insert_loo(run_id, row)
            loo_payloads.append(row)

        critical_failures = [outcome for outcome in failed if outcome.task.critical or outcome.task.kind == "baseline"]
        if critical_failures:
            status = "failed"
        elif failed_by_name:
            status = LOGICAL_PARTIAL_FAILED_STATUS
        else:
            status = "succeeded"
        reason = None
        if failed_by_name:
            reason = {
                "reason_code": "combine_backtest_child_tasks_failed",
                "failed_child_tasks": failed_by_name,
                "logical_status": status,
            }
            if status == LOGICAL_PARTIAL_FAILED_STATUS:
                reason["storage_note"] = (
                    "PostgreSQL run.status currently allows running/succeeded/failed only; "
                    "partial_failed is recorded in reason.logical_status until the schema is extended."
                )
        return {"run_id": run_id, "scheme_results": scheme_payloads, "loo": loo_payloads, "status": status, "reason": reason}

    def _run_prediction(
        self,
        *,
        run_id: str,
        name: str,
        frame: pd.DataFrame,
        node_id: str,
        node_parallelism_limit: int,
        backtest_config: Mapping[str, Any],
    ) -> dict[str, Any]:
        workspace = self._workspace_root / run_id / name
        workspace.mkdir(parents=True, exist_ok=True)
        pred_pkl = workspace / "combined_prediction.pkl"
        write_qlib_prediction(frame, pred_pkl)
        capacity = self._capacity_checker.ensure_slot_available(
            node_id=node_id,
            limit=node_parallelism_limit,
            run_id=run_id,
            backtest_name=name,
        )
        try:
            metrics = self._executor.execute_pred_backtest(
                workspace=workspace,
                pred_pkl=pred_pkl,
                node_id=node_id,
                backtest_config=backtest_config,
            )
        finally:
            self._capacity_checker.release_slot(capacity)
        metrics["workspace"] = str(workspace)
        metrics["prediction_path"] = str(pred_pkl)
        metrics["node_capacity"] = capacity
        metrics["prediction_store_manifest"] = maybe_upload_combined_prediction(
            run_id=run_id,
            backtest_name=name,
            pred_pkl=pred_pkl,
            node_id=node_id,
            backtest_config=backtest_config,
        )
        metrics["pred_persisted"] = metrics["prediction_store_manifest"] is not None
        return metrics


CallableUtc = Any


def parse_request(payload: Mapping[str, Any]) -> CombineBacktestRequest:
    roster = tuple(_coerce_panel_spec(item) for item in payload.get("roster") or [])
    if len(roster) < 2:
        raise MultiAlphaCombineBacktestError("roster requires at least two legs", reason_code="roster_too_small")
    schemes = tuple(_normalize_scheme(item) for item in (payload.get("weighting_schemes") or DEFAULT_WEIGHTING_SCHEMES))
    if not schemes:
        raise MultiAlphaCombineBacktestError("weighting_schemes cannot be empty", reason_code="scheme_missing")
    topk = int(payload.get("topk") or payload.get("backtest_config", {}).get("topk") or 20)
    return CombineBacktestRequest(
        roster=roster,
        oos_start=str(payload.get("oos_start") or ""),
        oos_end=str(payload.get("oos_end") or ""),
        weighting_schemes=schemes,
        normalize_method=str(payload.get("normalize_method") or "zscore"),
        walk_forward=dict(payload.get("walk_forward") or {"enabled": True, "window": 60, "min_periods": 2}),
        rank_fusion=dict(payload.get("rank_fusion") or {}),
        backtest_config=dict(payload.get("backtest_config") or {}),
        baseline_leg_id=str(payload.get("baseline_leg_id") or roster[0].leg_id),
        topk=topk,
        min_date_coverage=float(payload.get("min_date_coverage") or 0.8),
        run_async=bool(payload.get("run_async", True)),
    )


def _replace_request(request: CombineBacktestRequest, **updates: Any) -> CombineBacktestRequest:
    data = {
        "roster": request.roster,
        "oos_start": request.oos_start,
        "oos_end": request.oos_end,
        "weighting_schemes": request.weighting_schemes,
        "normalize_method": request.normalize_method,
        "walk_forward": request.walk_forward,
        "rank_fusion": request.rank_fusion,
        "backtest_config": request.backtest_config,
        "baseline_leg_id": request.baseline_leg_id,
        "topk": request.topk,
        "min_date_coverage": request.min_date_coverage,
        "run_async": request.run_async,
    }
    data.update(updates)
    return CombineBacktestRequest(**data)


def _coerce_panel_spec(item: Mapping[str, Any]) -> PanelLegSpec:
    if not isinstance(item, Mapping):
        raise MultiAlphaCombineBacktestError("each roster item must be an object", reason_code="invalid_roster_item")
    leg_id = str(item.get("leg_id") or item.get("id") or "").strip()
    seed_run_ids = tuple(str(value or "").strip() for value in (item.get("seed_run_ids") or item.get("run_ids") or []) if str(value or "").strip())
    if not leg_id:
        raise MultiAlphaCombineBacktestError("roster item missing leg_id", reason_code="leg_id_missing")
    if not seed_run_ids:
        raise MultiAlphaCombineBacktestError("roster item missing seed_run_ids", leg_id=leg_id, reason_code="seed_run_ids_missing")
    metadata = item.get("metadata") if isinstance(item.get("metadata"), Mapping) else {}
    return PanelLegSpec(leg_id=leg_id, seed_run_ids=seed_run_ids, metadata=metadata)


def seed_ensemble_prediction_only(frames: Sequence[pd.DataFrame], *, leg_id: str) -> pd.DataFrame:
    if not frames:
        raise MultiAlphaCombineBacktestError(
            "at least one seed prediction frame is required for rank-fusion",
            reason_code="seed_prediction_missing",
            leg_id=leg_id,
        )
    renamed: list[pd.DataFrame] = []
    for idx, frame in enumerate(frames):
        selected = frame[["trade_date", "instrument", "score"]].copy()
        selected = selected.rename(columns={"score": f"score__seed_{idx}"})
        renamed.append(selected)
    merged: pd.DataFrame | None = None
    for frame in renamed:
        merged = frame if merged is None else merged.merge(frame, on=["trade_date", "instrument"], how="outer")
    if merged is None or merged.empty:
        raise MultiAlphaCombineBacktestError(
            "seed predictions have no rows for rank-fusion",
            reason_code="seed_prediction_empty",
            leg_id=leg_id,
        )
    score_cols = [col for col in merged.columns if str(col).startswith("score__seed_")]
    merged["score"] = merged[score_cols].mean(axis=1, skipna=True)
    out = merged[["trade_date", "instrument", "score"]].dropna(subset=["score"])
    if out.empty:
        raise MultiAlphaCombineBacktestError(
            "seed ensemble produced no valid score rows for rank-fusion",
            reason_code="seed_ensemble_empty",
            leg_id=leg_id,
        )
    return out.groupby(["trade_date", "instrument"], as_index=False, sort=True)["score"].mean()


def filter_prediction_window(frame: pd.DataFrame, *, leg_id: str, oos_start: str, oos_end: str) -> pd.DataFrame:
    start = pd.to_datetime(oos_start, errors="coerce")
    end = pd.to_datetime(oos_end, errors="coerce")
    if pd.isna(start) or pd.isna(end):
        raise MultiAlphaCombineBacktestError(
            "rank-fusion prediction window requires valid oos_start/oos_end",
            reason_code="invalid_window",
            leg_id=leg_id,
            context={"oos_start": oos_start, "oos_end": oos_end},
        )
    start_date = start.date()
    end_date = end.date()
    if end_date < start_date:
        raise MultiAlphaCombineBacktestError(
            "rank-fusion prediction window requires oos_end >= oos_start",
            reason_code="invalid_window",
            leg_id=leg_id,
            context={"oos_start": oos_start, "oos_end": oos_end},
        )
    selected = frame[(frame["trade_date"] >= start_date) & (frame["trade_date"] <= end_date)].copy()
    if selected.empty:
        raise MultiAlphaCombineBacktestError(
            "seed ensemble has no prediction rows in requested OOS window for rank-fusion",
            reason_code="prediction_window_empty",
            leg_id=leg_id,
            context={"oos_start": oos_start, "oos_end": oos_end},
        )
    return selected


def _strip_panel_metrics(legs: Sequence[CombinerLeg]) -> list[CombinerLeg]:
    return [
        CombinerLeg(leg_id=leg.leg_id, pred_frame=leg.pred_frame, metadata=dict(leg.metadata))
        for leg in legs
    ]


def combine_legs(*, legs: Sequence[CombinerLeg], scheme: str, request: CombineBacktestRequest) -> Any:
    try:
        if is_rank_fusion_scheme(scheme):
            method = rank_fusion_method_for_scheme(scheme)
            return MultiAlphaCombiner().combine_rank_fusion(
                legs=legs,
                method=method,
                rrf_k=rank_fusion_rrf_k(request) if method == "rrf" else 60.0,
                leg_weights=rank_fusion_leg_weights(request),
            )
        return MultiAlphaCombiner().combine(
            legs=legs,
            weighting_scheme=scheme,
            normalize_method=request.normalize_method,
            walk_forward=request.walk_forward,
        )
    except MultiAlphaCombinerError as exc:
        raise MultiAlphaCombineBacktestError(
            f"weighting scheme cannot be computed: {exc}",
            reason_code="scheme_not_computable",
            context={"weighting_scheme": scheme},
        ) from exc


def is_rank_fusion_scheme(scheme: str) -> bool:
    return scheme in RANK_FUSION_WEIGHTING_SCHEMES


def rank_fusion_method_for_scheme(scheme: str) -> str:
    if scheme == "rank_fusion_rrf":
        return "rrf"
    if scheme == "rank_fusion_borda":
        return "borda"
    raise MultiAlphaCombineBacktestError(
        f"unsupported rank-fusion weighting_scheme={scheme!r}",
        reason_code="unsupported_scheme",
        context={"allowed": list(SUPPORTED_WEIGHTING_SCHEMES)},
    )


def rank_fusion_rrf_k(request: CombineBacktestRequest) -> float:
    raw = request.rank_fusion.get("rrf_k", 60.0) if isinstance(request.rank_fusion, Mapping) else 60.0
    try:
        parsed = float(raw)
    except (TypeError, ValueError) as exc:
        raise MultiAlphaCombineBacktestError(
            f"rank_fusion.rrf_k must be numeric, got {raw!r}",
            reason_code="rank_fusion_rrf_k_invalid",
        ) from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise MultiAlphaCombineBacktestError(
            f"rank_fusion.rrf_k must be positive and finite, got {raw!r}",
            reason_code="rank_fusion_rrf_k_invalid",
        )
    return parsed


def rank_fusion_leg_weights(request: CombineBacktestRequest) -> Mapping[str, float] | None:
    if not isinstance(request.rank_fusion, Mapping):
        return None
    raw = request.rank_fusion.get("leg_weights")
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise MultiAlphaCombineBacktestError(
            "rank_fusion.leg_weights must be an object mapping leg_id to weight",
            reason_code="rank_fusion_leg_weights_invalid",
        )
    parsed: dict[str, float] = {}
    for key, value in raw.items():
        try:
            parsed_value = float(value)
        except (TypeError, ValueError) as exc:
            raise MultiAlphaCombineBacktestError(
                f"rank_fusion.leg_weights for {key!r} must be numeric, got {value!r}",
                reason_code="rank_fusion_leg_weights_invalid",
            ) from exc
        if not math.isfinite(parsed_value) or parsed_value < 0:
            raise MultiAlphaCombineBacktestError(
                f"rank_fusion.leg_weights for {key!r} must be non-negative and finite, got {value!r}",
                reason_code="rank_fusion_leg_weights_invalid",
            )
        parsed[str(key)] = parsed_value
    return parsed


def weights_payload(result: Any, *, scheme: str, request: CombineBacktestRequest) -> dict[str, Any]:
    weights = dict(result.weights or {})
    if not is_rank_fusion_scheme(scheme):
        return weights
    method = rank_fusion_method_for_scheme(scheme)
    payload: dict[str, Any] = {
        "leg_weights": weights,
        "method": method,
    }
    if method == "rrf":
        payload["rrf_k"] = rank_fusion_rrf_k(request)
    return payload


def per_window_weights_payload(result: Any, *, scheme: str) -> list[dict[str, Any]]:
    if not is_rank_fusion_scheme(scheme):
        return list(result.per_window_weights or [])
    summary = dict(getattr(result, "summary", {}) or {})
    return [
        {
            "weighting_scheme": scheme,
            "method": rank_fusion_method_for_scheme(scheme),
            "rrf_k": summary.get("rrf_k"),
            "rank_fusion": True,
            "window_count": 0,
        }
    ]


def write_qlib_prediction(frame: pd.DataFrame, path: Path) -> None:
    if frame.empty:
        raise MultiAlphaCombineBacktestError("combined prediction frame is empty", reason_code="combined_prediction_empty")
    selected = frame.copy()
    if "combined_score" in selected.columns and "score" not in selected.columns:
        selected = selected.rename(columns={"combined_score": "score"})
    required = {"trade_date", "instrument", "score"}
    missing = sorted(required - set(selected.columns))
    if missing:
        raise MultiAlphaCombineBacktestError(
            f"prediction frame missing required columns: {missing}",
            reason_code="combined_prediction_invalid",
        )
    selected["datetime"] = pd.to_datetime(selected["trade_date"], errors="raise")
    selected["instrument"] = selected["instrument"].astype(str)
    selected["score"] = pd.to_numeric(selected["score"], errors="raise").astype("float32")
    out = selected[["datetime", "instrument", "score"]].sort_values(["datetime", "instrument"])
    path.parent.mkdir(parents=True, exist_ok=True)
    out.set_index(["datetime", "instrument"]).to_pickle(path)


def ingest_enhanced_metrics(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise MultiAlphaCombineBacktestError(
            f"enhanced metrics file missing: {path}",
            reason_code="enhanced_metrics_missing",
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    absolute = payload.get("absolute_returns") if isinstance(payload.get("absolute_returns"), Mapping) else {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    trade = payload.get("trade_diagnostics") if isinstance(payload.get("trade_diagnostics"), Mapping) else {}
    pred = payload.get("prediction_diagnostics") if isinstance(payload.get("prediction_diagnostics"), Mapping) else {}
    cagr = first_finite(absolute.get("cagr"), summary.get("cagr"))
    max_drawdown = first_finite(absolute.get("max_drawdown"), summary.get("1day.excess_return_with_cost.max_drawdown"))
    sharpe = first_finite(absolute.get("sharpe"), summary.get("1day.excess_return_with_cost.information_ratio"))
    calmar = cagr / abs(max_drawdown) if cagr is not None and max_drawdown not in (None, 0) else finite_or_none(absolute.get("calmar"))
    return {
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "sharpe": sharpe,
        "calmar": calmar,
        "topk_return_20": finite_or_none(pred.get("topk_return_20")),
        "topk_hit_rate_20": finite_or_none(pred.get("topk_hit_rate_20")),
        "turnover": first_finite(trade.get("annualized_turnover"), trade.get("avg_turnover")),
        "result_path": str(path),
    }


def metric_columns(metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "cagr": metrics.get("cagr"),
        "max_drawdown": metrics.get("max_drawdown"),
        "sharpe": metrics.get("sharpe"),
        "calmar": metrics.get("calmar"),
        "topk_return_20": metrics.get("topk_return_20"),
        "topk_hit_rate_20": metrics.get("topk_hit_rate_20"),
        "turnover": metrics.get("turnover"),
    }


def validate_node_parallelism(*, node_id: str, backtest_config: Mapping[str, Any]) -> dict[str, int]:
    raw = backtest_config.get("node_parallelism") if isinstance(backtest_config.get("node_parallelism"), Mapping) else {node_id: 1}
    if node_id not in raw:
        raise MultiAlphaCombineBacktestError(
            "node_parallelism must include selected node",
            reason_code="node_parallelism_missing_node",
            context={"node_id": node_id, "node_parallelism": dict(raw)},
        )
    try:
        limit = int(raw[node_id])
    except (TypeError, ValueError) as exc:
        raise MultiAlphaCombineBacktestError(
            "node_parallelism value must be an integer",
            reason_code="node_parallelism_invalid",
            context={"node_id": node_id, "value": raw[node_id]},
        ) from exc
    if limit < 1 or limit > 4:
        raise MultiAlphaCombineBacktestError(
            "node_parallelism value must be between 1 and 4",
            reason_code="node_parallelism_out_of_range",
            context={"node_id": node_id, "value": limit},
        )
    return {node_id: limit}


def _persisted_run_status(status: str) -> str:
    if status == LOGICAL_PARTIAL_FAILED_STATUS:
        return "failed"
    return status


def _first_child_error(reason: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if not isinstance(reason, Mapping):
        return None
    failed_child_tasks = reason.get("failed_child_tasks")
    if not isinstance(failed_child_tasks, Mapping):
        return None
    for value in failed_child_tasks.values():
        if isinstance(value, Mapping):
            return value
    return None


def _with_logical_status(row: dict[str, Any]) -> dict[str, Any]:
    reason = row.get("reason")
    if isinstance(reason, Mapping) and reason.get("logical_status") == LOGICAL_PARTIAL_FAILED_STATUS:
        row["status"] = LOGICAL_PARTIAL_FAILED_STATUS
    return row


def maybe_upload_combined_prediction(
    *,
    run_id: str,
    backtest_name: str,
    pred_pkl: Path,
    node_id: str,
    backtest_config: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Upload the generated combined pred only when the explicit upload URL is configured."""

    upload_base = (os.getenv(PREDICTION_STORE_UPLOAD_URL_ENV) or "").strip()
    if not upload_base:
        return None
    if not pred_pkl.exists():
        raise MultiAlphaCombineBacktestError(
            f"combined prediction pickle missing before store upload: {pred_pkl}",
            reason_code="combined_prediction_upload_missing_file",
            context={"run_id": run_id, "backtest_name": backtest_name},
        )

    run_key = safe_prediction_store_run_key(f"{run_id}_{backtest_name}")
    url = _prediction_store_upload_url(upload_base, run_key)
    metadata = {
        "producer": "multi_alpha_combine_backtest",
        "run_id": run_id,
        "backtest_name": backtest_name,
        "source_node_id": node_id,
        "backtest_config": dict(backtest_config),
        "prediction_path": str(pred_pkl),
    }
    try:
        with pred_pkl.open("rb") as fh:
            response = requests.post(
                url,
                data={"metadata_json": json.dumps(metadata, ensure_ascii=False, default=str), "source_node_id": node_id},
                files={"pred": ("combined_prediction.pkl", fh, "application/octet-stream")},
                timeout=_prediction_store_upload_timeout(),
            )
    except requests.RequestException as exc:
        raise MultiAlphaCombineBacktestError(
            f"prediction-store upload request failed: {type(exc).__name__}: {exc}",
            reason_code="combined_prediction_upload_failed",
            context={"run_id": run_id, "backtest_name": backtest_name, "url": url},
        ) from exc
    if response.status_code >= 400:
        raise MultiAlphaCombineBacktestError(
            f"prediction-store upload returned HTTP {response.status_code}: {response.text[:500]}",
            reason_code="combined_prediction_upload_failed",
            context={"run_id": run_id, "backtest_name": backtest_name, "url": url},
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise MultiAlphaCombineBacktestError(
            f"prediction-store upload returned non-JSON response: {response.text[:500]!r}",
            reason_code="combined_prediction_upload_bad_response",
            context={"run_id": run_id, "backtest_name": backtest_name, "url": url},
        ) from exc
    data = payload.get("data") if isinstance(payload, Mapping) else None
    manifest = (data or {}).get("manifest") if isinstance(data, Mapping) else None
    if not isinstance(manifest, Mapping):
        raise MultiAlphaCombineBacktestError(
            f"prediction-store upload response missing data.manifest: {payload!r}",
            reason_code="combined_prediction_upload_bad_response",
            context={"run_id": run_id, "backtest_name": backtest_name, "url": url},
        )
    artifact_types = {
        str(item.get("artifact_type") or "")
        for item in manifest.get("artifacts", [])
        if isinstance(item, Mapping)
    }
    if "prediction" not in artifact_types:
        raise MultiAlphaCombineBacktestError(
            "prediction-store manifest did not include uploaded prediction artifact",
            reason_code="combined_prediction_upload_bad_response",
            context={"run_id": run_id, "backtest_name": backtest_name, "url": url, "artifact_types": sorted(artifact_types)},
        )
    return dict(manifest)


def safe_prediction_store_run_key(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip()).strip("._-")
    if not cleaned:
        raise MultiAlphaCombineBacktestError("cannot build prediction-store run key", reason_code="prediction_store_run_key_empty")
    return cleaned[:180]


def _prediction_store_upload_url(upload_base: str, run_key: str) -> str:
    base = upload_base.strip()
    if not base:
        raise MultiAlphaCombineBacktestError(
            f"{PREDICTION_STORE_UPLOAD_URL_ENV} is empty",
            reason_code="prediction_store_upload_url_missing",
        )
    if "{run_key}" in base:
        return base.format(run_key=run_key)
    stripped = base.rstrip("/")
    if stripped.endswith("/artifacts"):
        return f"{stripped}/{run_key}"
    if stripped.endswith(f"/artifacts/{run_key}"):
        return stripped
    return stripped


def _prediction_store_upload_timeout() -> float:
    raw = (os.getenv(PREDICTION_STORE_UPLOAD_TIMEOUT_ENV) or "").strip()
    if not raw:
        return DEFAULT_UPLOAD_TIMEOUT_SEC
    try:
        value = float(raw)
    except ValueError as exc:
        raise MultiAlphaCombineBacktestError(
            f"{PREDICTION_STORE_UPLOAD_TIMEOUT_ENV} must be numeric seconds, got {raw!r}",
            reason_code="prediction_store_upload_timeout_invalid",
        ) from exc
    if value <= 0:
        raise MultiAlphaCombineBacktestError(
            f"{PREDICTION_STORE_UPLOAD_TIMEOUT_ENV} must be positive, got {value}",
            reason_code="prediction_store_upload_timeout_invalid",
        )
    return value


def roster_hash_for(roster: Sequence[PanelLegSpec]) -> str:
    raw = json.dumps(_roster_payload(roster), sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _roster_payload(roster: Sequence[PanelLegSpec]) -> list[dict[str, Any]]:
    return [
        {"leg_id": leg.leg_id, "seed_run_ids": list(leg.seed_run_ids), "metadata": dict(leg.metadata)}
        for leg in roster
    ]


def make_run_id(*, roster_hash: str, oos_start: str, oos_end: str, ts: datetime) -> str:
    stamp = ts.strftime("%Y%m%dT%H%M%S%fZ")
    return f"macb_{roster_hash}_{oos_start.replace('-', '')}_{oos_end.replace('-', '')}_{stamp}"


def _normalize_scheme(value: Any) -> str:
    scheme = str(value or "").strip().lower()
    if scheme not in SUPPORTED_WEIGHTING_SCHEMES:
        raise MultiAlphaCombineBacktestError(
            f"unsupported weighting_scheme={value!r}",
            reason_code="unsupported_scheme",
            context={"allowed": list(SUPPORTED_WEIGHTING_SCHEMES)},
        )
    return scheme


def finite_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def first_finite(*values: Any) -> float | None:
    for value in values:
        parsed = finite_or_none(value)
        if parsed is not None:
            return parsed
    return None


def delta(left: Any, right: Any) -> float | None:
    lval = finite_or_none(left)
    rval = finite_or_none(right)
    return lval - rval if lval is not None and rval is not None else None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def error_payload(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, MultiAlphaCombineBacktestError):
        return {"reason_code": exc.reason_code, "message": str(exc), "leg_id": exc.leg_id, "context": exc.context}
    if isinstance(exc, MultiAlphaPanelError):
        return {"reason_code": exc.reason_code, "message": str(exc), "leg_id": exc.leg_id, "context": exc.context}
    return {"reason_code": type(exc).__name__, "message": str(exc)}

