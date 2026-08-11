"""Tier-1 multi-alpha combine-backtest orchestration.

This module upgrades the P3-B one-off script into a service-level job that can
be called by REST/MCP. It does not train alpha legs and does not modify the QE
task model; it only writes combined prediction pickles and delegates execution
to the existing pred-backtest primitive.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

import pandas as pd
from psycopg2.extras import Json, RealDictCursor
import requests
import yaml

from backend.db.pg_pool import get_conn
from backend.infra.wsl_qlib_runner import win_to_wsl_path
from backend.services.model_store import ModelStoreService, PredictionStoreError
from backend.services.multi_alpha.combiner import CombinerLeg, MultiAlphaCombiner, MultiAlphaCombinerError
from backend.services.multi_alpha.orthogonality import MultiAlphaOrthogonalityError, normalize_prediction_frame
from backend.services.multi_alpha.panels import MultiAlphaPanelBuilder, MultiAlphaPanelError, PanelLegSpec
from backend.services.multi_alpha.qe_subprocess_env import (
    qe_subprocess_credential_scrub_command,
    scrubbed_qe_subprocess_env,
)
from backend.services.quantevolver.config_composer import QE_STRATEGY_RUNTIME_HELPER_FILES


COMBINE_BACKTEST_CONFIRM = "MULTI_ALPHA_COMBINE_BACKTEST_RUN"
COMBINE_BACKTEST_STALE_FAIL_CONFIRM = "MULTI_ALPHA_COMBINE_BACKTEST_STALE_FAIL"
DEFAULT_WEIGHTING_SCHEMES = ("equal", "orthogonality_aware", "ic_weighted", "risk_parity")
RANK_FUSION_WEIGHTING_SCHEMES = ("rank_fusion_rrf", "rank_fusion_borda")
SUPPORTED_WEIGHTING_SCHEMES = DEFAULT_WEIGHTING_SCHEMES + RANK_FUSION_WEIGHTING_SCHEMES
LOGICAL_PARTIAL_FAILED_STATUS = "partial_failed"
# A P0-2 successor can finish with preserved unavailable children, and a
# controlled durable run can finish cancelled.  They are terminal business
# facts just like the legacy succeeded/failed states: archive/read/retry/delete
# must not pretend they are still active or omit their preserved evidence.
TERMINAL_STATUSES = {
    "succeeded",
    "failed",
    LOGICAL_PARTIAL_FAILED_STATUS,
    "partial_recovered",
    "cancelled",
}
PREDICTION_STORE_UPLOAD_URL_ENV = "AISTOCK_PREDICTION_STORE_UPLOAD_URL"
PREDICTION_STORE_UPLOAD_TIMEOUT_ENV = "AISTOCK_PREDICTION_STORE_UPLOAD_TIMEOUT_SEC"
DEFAULT_UPLOAD_TIMEOUT_SEC = 120.0
ACTIVE_QE_LOOP_STATUSES = ("running", "processing")
DEFAULT_PRED_BACKTEST_TIMEOUT_SECONDS = 45 * 60
DEFAULT_READ_EXP_TIMEOUT_SECONDS = 15 * 60
DEFAULT_RUN_TIMEOUT_GRACE_SECONDS = 5 * 60
DEFAULT_RUNTIME_TEMPLATE_COPY_TIMEOUT_SECONDS = 15 * 60
_AISTOCK_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_SECTOR_RISK_OVERLAY_MODES = frozenset({"none", "entry_gate", "bounded_de_risk", "exit_reentry"})
RUNTIME_EXTERNAL_DATA_LINK_NAMES = frozenset(
    {
        "bak_basic.h5",
        "cyq_perf.h5",
        "daily_basic.h5",
        "daily_pv.h5",
        "margin_detail.h5",
        "moneyflow.h5",
        "sector_data.h5",
        "static_factors.parquet",
    }
)
RUN_HEARTBEAT_REASON_CODE = "combine_backtest_running"
REQUEST_SNAPSHOT_KEY = "_combine_request_v1"
RUN_EVENT_LOG_NAME = "run_events.jsonl"
MAX_LOG_FILES = 100
MAX_LOG_TAIL_CHARS = 20_000
_NODE_RESERVATIONS: dict[str, int] = {}
_NODE_RESERVATIONS_LOCK = threading.Lock()
_RUN_EVENT_LOG_LOCK = threading.Lock()
logger = logging.getLogger(__name__)


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
    prediction_task_selection: Mapping[str, bool] | None = None
    baseline_leg_id: str | None = None
    topk: int = 20
    min_date_coverage: float = 0.8
    run_async: bool = True
    scheme_timeout_seconds: int = DEFAULT_PRED_BACKTEST_TIMEOUT_SECONDS
    run_timeout_seconds: int | None = None


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
        apply_pred_backtest_overrides(workspace=workspace, backtest_config=backtest_config)
        command = backtest_config.get("command")
        if command is None:
            error_context = _pred_backtest_error_context(workspace=workspace, node_id=node_id, backtest_config=backtest_config)
            qrun_command, read_command, read_env = _default_local_pred_backtest_commands(
                workspace=workspace,
                pred_name=pred_pkl.name,
                backtest_config=backtest_config,
            )
            qrun = run_command(
                qrun_command,
                cwd=workspace,
                timeout_seconds=int(backtest_config.get("timeout_seconds", DEFAULT_PRED_BACKTEST_TIMEOUT_SECONDS)),
                log_prefix="pred_backtest_qrun",
                env=read_env,
                error_context={**error_context, "stage": "qrun"},
            )
            if qrun.returncode != 0:
                raise MultiAlphaCombineBacktestError(
                    f"pred-backtest qrun failed with exit_code={qrun.returncode}",
                    reason_code="pred_backtest_failed",
                    context={**error_context, "stderr_tail": (qrun.stderr or "")[-1000:]},
                )
            read_exp = run_command(
                read_command,
                cwd=workspace,
                timeout_seconds=int(backtest_config.get("read_timeout_seconds", DEFAULT_READ_EXP_TIMEOUT_SECONDS)),
                log_prefix="pred_backtest_read_exp_res",
                env=read_env,
                error_context={**error_context, "stage": "read_exp_res"},
            )
            if read_exp.returncode != 0:
                raise MultiAlphaCombineBacktestError(
                    f"read_exp_res failed with exit_code={read_exp.returncode}",
                    reason_code="pred_backtest_ingest_failed",
                    context={**error_context, "stderr_tail": (read_exp.stderr or "")[-1000:]},
                )
        else:
            error_context = _pred_backtest_error_context(workspace=workspace, node_id=node_id, backtest_config=backtest_config)
            completed = run_command(
                command,
                cwd=workspace,
                timeout_seconds=int(backtest_config.get("timeout_seconds", DEFAULT_PRED_BACKTEST_TIMEOUT_SECONDS)),
                log_prefix="pred_backtest",
                env=scrubbed_qe_subprocess_env(),
                error_context={**error_context, "stage": "custom_command"},
            )
            if completed.returncode != 0:
                raise MultiAlphaCombineBacktestError(
                    f"pred-backtest command failed with exit_code={completed.returncode}",
                    reason_code="pred_backtest_failed",
                    context={**error_context, "stderr_tail": (completed.stderr or "")[-1000:]},
                )
        return ingest_enhanced_metrics(result_path)


def prepare_pred_backtest_workspace(*, workspace: Path, backtest_config: Mapping[str, Any]) -> None:
    """Copy an explicitly configured qrun runtime template, then verify required files.

    Completed QE workspaces on DrvFS contain Linux symlinks to WSL-native
    factor-data files.  Windows Python cannot stat those reparse points, so a
    native ``shutil`` copy is invalid.  Route only that layout through WSL
    ``cp -a`` so links keep their Linux targets; every other layout retains the
    original native copy path.
    """

    src = _runtime_template_source(backtest_config=backtest_config, workspace=workspace)
    if src is not None:
        unreadable = _find_unreadable_runtime_template_entry(src)
        if unreadable is not None:
            if not _is_windows_host():
                item, exc = unreadable
                raise MultiAlphaCombineBacktestError(
                    f"runtime template entry is unreadable: {item}: {type(exc).__name__}: {exc}",
                    reason_code="pred_backtest_runtime_template_entry_unreadable",
                    context={"runtime_template_dir": str(src), "entry": str(item), "workspace": str(workspace)},
                ) from exc
            _copy_runtime_template_via_wsl(src=src, workspace=workspace, backtest_config=backtest_config)
        else:
            _copy_runtime_template_native(src=src, workspace=workspace, backtest_config=backtest_config)

    _refresh_sector_risk_overlay_runtime_assets(
        workspace=workspace,
        backtest_config=backtest_config,
    )

    missing = [name for name in _required_runtime_files() if not (workspace / name).exists()]
    if missing:
        raise MultiAlphaCombineBacktestError(
            "pred-backtest runtime is incomplete; provide backtest_config.runtime_template_dir "
            "or pre-populate the workspace",
            reason_code="pred_backtest_runtime_missing",
            context={"workspace": str(workspace), "missing": missing},
        )


def preflight_pred_backtest_runtime(*, backtest_config: Mapping[str, Any], node_id: str) -> None:
    """Reject unusable local runtime templates before a run row is created."""

    if bool(backtest_config.get("parse_only")):
        return
    src = _runtime_template_source(backtest_config=backtest_config, workspace=None)
    if src is None:
        raise MultiAlphaCombineBacktestError(
            "pred-backtest requires backtest_config.runtime_template_dir or AISTOCK_MULTI_ALPHA_QRUN_TEMPLATE_DIR",
            reason_code="pred_backtest_runtime_template_missing",
            context={"node_id": node_id},
        )
    missing = [name for name in _required_runtime_files() if not (src / name).exists()]
    if missing:
        raise MultiAlphaCombineBacktestError(
            "pred-backtest runtime template is incomplete",
            reason_code="pred_backtest_runtime_missing",
            context={"runtime_template_dir": str(src), "missing": missing, "node_id": node_id},
        )
    unreadable = _find_unreadable_runtime_template_entry(src)
    if unreadable is not None:
        if not _is_windows_host():
            item, exc = unreadable
            raise MultiAlphaCombineBacktestError(
                f"runtime template entry is unreadable: {item}: {type(exc).__name__}: {exc}",
                reason_code="pred_backtest_runtime_template_entry_unreadable",
                context={"runtime_template_dir": str(src), "entry": str(item), "node_id": node_id},
            ) from exc
        _probe_runtime_template_via_wsl(src=src, backtest_config=backtest_config, unreadable=unreadable)
    if _is_windows_host() and node_id.lower().startswith("wsl") and backtest_config.get("command") is None:
        _wsl_runtime_settings(backtest_config=backtest_config, require_conda=True)


def _runtime_template_source(*, backtest_config: Mapping[str, Any], workspace: Path | None) -> Path | None:
    template_dir = backtest_config.get("runtime_template_dir") or os.getenv("AISTOCK_MULTI_ALPHA_QRUN_TEMPLATE_DIR")
    if not template_dir:
        return None
    src = Path(str(template_dir))
    if not src.exists() or not src.is_dir():
        raise MultiAlphaCombineBacktestError(
            f"runtime_template_dir does not exist or is not a directory: {src}",
            reason_code="pred_backtest_runtime_template_missing",
            context={"runtime_template_dir": str(src), "workspace": str(workspace) if workspace is not None else None},
        )
    return src


def _refresh_sector_risk_overlay_runtime_assets(
    *,
    workspace: Path,
    backtest_config: Mapping[str, Any],
) -> None:
    """Overlay current strategy helpers after copying a frozen runtime template.

    A multi-alpha retry intentionally reuses its frozen qrun template, but the
    template is not authoritative for the strategy implementation selected by
    the new request.  Without this post-copy overlay an old template can
    silently replace the current sector-risk wrapper and omit its dedicated V2
    parents.  Only sector-overlay runs take this path; other frozen runtimes are
    left byte-for-byte unchanged.
    """

    strategy_kwargs = backtest_config.get("strategy_kwargs")
    if not isinstance(strategy_kwargs, Mapping):
        return
    raw_enabled = strategy_kwargs.get("sector_risk_overlay_enabled")
    enabled = (
        raw_enabled
        if isinstance(raw_enabled, bool)
        else str(raw_enabled or "").strip().lower() in {"1", "true", "yes", "on"}
    )
    mode = str(strategy_kwargs.get("sector_risk_overlay_mode") or "").strip().lower()
    if not enabled and mode not in _SECTOR_RISK_OVERLAY_MODES - {"none"}:
        return

    scripts_dir = _AISTOCK_PROJECT_ROOT / "scripts"
    missing = [
        helper_name
        for helper_name in QE_STRATEGY_RUNTIME_HELPER_FILES
        if not (scripts_dir / helper_name).is_file()
    ]
    if missing:
        raise MultiAlphaCombineBacktestError(
            "current sector-risk runtime helper set is incomplete",
            reason_code="pred_backtest_sector_risk_runtime_asset_missing",
            context={
                "workspace": str(workspace),
                "scripts_dir": str(scripts_dir),
                "missing": missing,
            },
        )
    for helper_name in QE_STRATEGY_RUNTIME_HELPER_FILES:
        shutil.copy2(scripts_dir / helper_name, workspace / helper_name)
    logger.info(
        "Refreshed sector-risk pred-backtest runtime helpers",
        extra={
            "workspace": str(workspace),
            "mode": mode or None,
            "helper_files": list(QE_STRATEGY_RUNTIME_HELPER_FILES),
        },
    )


def _required_runtime_files() -> tuple[str, ...]:
    return ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py")


def _find_unreadable_runtime_template_entry(src: Path) -> tuple[Path, OSError] | None:
    for item in src.iterdir():
        try:
            item.is_file()
            item.is_dir()
        except OSError as exc:
            return item, exc
    return None


def is_runtime_external_data_link(path: Path) -> bool:
    """Inspect a canonical QE data entry without dereferencing its target."""

    if path.name not in RUNTIME_EXTERNAL_DATA_LINK_NAMES:
        return False
    if path.is_symlink():
        return True
    metadata = path.lstat()
    attributes = int(getattr(metadata, "st_file_attributes", 0) or 0)
    reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & reparse_flag)


def _copy_runtime_template_native(*, src: Path, workspace: Path, backtest_config: Mapping[str, Any]) -> None:
    exclude_external_data_links = bool(backtest_config.get("_exclude_runtime_external_data_links"))
    for item in src.iterdir():
        if exclude_external_data_links and is_runtime_external_data_link(item):
            continue
        target = workspace / item.name
        if item.is_file():
            if not target.exists():
                shutil.copy2(item, target)
        elif item.is_dir() and bool(backtest_config.get("copy_runtime_dirs")) and not target.exists():
            shutil.copytree(item, target)


def _copy_runtime_template_via_wsl(*, src: Path, workspace: Path, backtest_config: Mapping[str, Any]) -> None:
    distro, _conda_sh, _conda_env = _wsl_runtime_settings(backtest_config=backtest_config, require_conda=False)
    workspace.mkdir(parents=True, exist_ok=True)
    src_wsl = win_to_wsl_path(str(src.resolve()))
    workspace_wsl = win_to_wsl_path(str(workspace.resolve()))
    script = _wsl_template_validation_script(src_wsl) + f"; cp -a -n -- {shlex.quote(src_wsl + '/.')} {shlex.quote(workspace_wsl + '/')}"
    if bool(backtest_config.get("_exclude_runtime_external_data_links")):
        external_link_names = [
            item.name
            for item in src.iterdir()
            if is_runtime_external_data_link(item)
        ]
        excluded_paths = " ".join(
            shlex.quote(workspace_wsl + "/" + name)
            for name in sorted(external_link_names)
        )
        if excluded_paths:
            script += f"; rm -f -- {excluded_paths}"
    completed = run_command(
        ["wsl", "-d", distro, "bash", "-lc", script],
        cwd=workspace,
        timeout_seconds=int(
            backtest_config.get("runtime_template_copy_timeout_seconds", DEFAULT_RUNTIME_TEMPLATE_COPY_TIMEOUT_SECONDS)
        ),
        log_prefix="pred_backtest_runtime_copy",
        error_context={"runtime_template_dir": str(src), "workspace": str(workspace), "stage": "runtime_template_copy"},
    )
    if completed.returncode != 0:
        raise MultiAlphaCombineBacktestError(
            f"WSL runtime template copy failed with exit_code={completed.returncode}",
            reason_code="pred_backtest_runtime_template_copy_failed",
            context={
                "runtime_template_dir": str(src),
                "workspace": str(workspace),
                "stderr_tail": (completed.stderr or "")[-1000:],
            },
        )


def _probe_runtime_template_via_wsl(
    *,
    src: Path,
    backtest_config: Mapping[str, Any],
    unreadable: tuple[Path, OSError],
) -> None:
    distro, _conda_sh, _conda_env = _wsl_runtime_settings(backtest_config=backtest_config, require_conda=False)
    src_wsl = win_to_wsl_path(str(src.resolve()))
    try:
        completed = subprocess.run(
            ["wsl", "-d", distro, "bash", "-lc", _wsl_template_validation_script(src_wsl)],
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            stdin=subprocess.DEVNULL,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        item, windows_exc = unreadable
        raise MultiAlphaCombineBacktestError(
            f"WSL runtime template preflight could not inspect {item}: {type(exc).__name__}: {exc}",
            reason_code="pred_backtest_runtime_template_preflight_failed",
            context={"runtime_template_dir": str(src), "entry": str(item), "windows_error": str(windows_exc)},
        ) from exc
    if completed.returncode != 0:
        item, windows_exc = unreadable
        raise MultiAlphaCombineBacktestError(
            f"WSL runtime template preflight failed with exit_code={completed.returncode}",
            reason_code="pred_backtest_runtime_template_preflight_failed",
            context={
                "runtime_template_dir": str(src),
                "entry": str(item),
                "windows_error": str(windows_exc),
                "stderr_tail": (completed.stderr or "")[-1000:],
            },
        )


def _wsl_template_validation_script(src_wsl: str) -> str:
    src_q = shlex.quote(src_wsl)
    required_checks = "; ".join(f"test -f {shlex.quote(src_wsl + '/' + name)}" for name in _required_runtime_files())
    return (
        "set -euo pipefail; "
        f"test -d {src_q}; "
        f"for item in {src_q}/* {src_q}/.[!.]* {src_q}/..?*; do "
        "if [ -L \"$item\" ] && [ ! -e \"$item\" ]; then echo \"broken runtime symlink: $item\" >&2; exit 42; fi; "
        "done; "
        + required_checks
    )


def _is_windows_host() -> bool:
    return os.name == "nt"


def _wsl_runtime_settings(
    *, backtest_config: Mapping[str, Any], require_conda: bool
) -> tuple[str, str | None, str | None]:
    if shutil.which("wsl") is None:
        raise MultiAlphaCombineBacktestError(
            "WSL executable is required for the selected local QE node",
            reason_code="pred_backtest_wsl_unavailable",
        )
    distro = str(backtest_config.get("wsl_distro") or os.getenv("QLIB_WSL_DISTRO") or "").strip()
    conda_sh = str(backtest_config.get("wsl_conda_sh") or os.getenv("QLIB_WSL_CONDA_SH") or "").strip()
    conda_env = str(
        backtest_config.get("wsl_conda_env")
        or backtest_config.get("conda_env")
        or os.getenv("QLIB_WSL_CONDA_ENV")
        or ""
    ).strip()
    missing = [name for name, value in (("QLIB_WSL_DISTRO", distro),) if not value]
    if require_conda:
        missing.extend(name for name, value in (("QLIB_WSL_CONDA_SH", conda_sh), ("QLIB_WSL_CONDA_ENV", conda_env)) if not value)
    if missing:
        raise MultiAlphaCombineBacktestError(
            f"missing WSL runtime configuration: {missing}",
            reason_code="pred_backtest_wsl_config_missing",
            context={"missing": missing},
        )
    return distro, conda_sh or None, conda_env or None


def _default_local_pred_backtest_commands(
    *, workspace: Path, pred_name: str, backtest_config: Mapping[str, Any]
) -> tuple[list[str], list[str], Mapping[str, str] | None]:
    # The QE data plane is file-only: qrun/read_exp_res must never inherit the
    # backend PostgreSQL credentials or have any database fallback. Start from a
    # scrubbed child env and only add the explicit recorder contract.
    read_env = scrubbed_qe_subprocess_env()
    if not _is_windows_host():
        read_env["QE_REQUIRE_RECORDER_ID"] = "1"
        return (
            [sys.executable, "qrun_limit_minute.py", "conf.yaml", "--pred-backtest", pred_name],
            [sys.executable, "read_exp_res.py"],
            read_env,
        )
    distro, conda_sh, conda_env = _wsl_runtime_settings(backtest_config=backtest_config, require_conda=True)
    workspace_wsl = win_to_wsl_path(str(workspace.resolve()))
    scrub_credentials = qe_subprocess_credential_scrub_command()
    prefix = (
        "set -eo pipefail; "
        f"source {shlex.quote(str(conda_sh))}; "
        f"conda activate {shlex.quote(str(conda_env))}; "
        "set -u; "
        f"cd {shlex.quote(workspace_wsl)}; "
        "if [ -f .factor_env ]; then . ./.factor_env; fi; "
        f"{scrub_credentials}; "
    )
    qrun_script = prefix + f"python qrun_limit_minute.py conf.yaml --pred-backtest {shlex.quote(pred_name)}"
    read_script = prefix + "QE_REQUIRE_RECORDER_ID=1 python read_exp_res.py"
    return (
        ["wsl", "-d", distro, "bash", "--noprofile", "--norc", "-c", qrun_script],
        ["wsl", "-d", distro, "bash", "--noprofile", "--norc", "-c", read_script],
        read_env,
    )


def apply_pred_backtest_overrides(*, workspace: Path, backtest_config: Mapping[str, Any]) -> None:
    """Apply explicit runtime overrides before qrun reads conf.yaml.

    The production qrun config is a Jinja template, so this must not parse and
    dump the whole YAML document. Keep edits scoped to the strategy and
    backtest mappings that the operator explicitly overrides.
    """

    topk = backtest_config.get("topk")
    initial_cash = backtest_config.get("initial_cash")
    strategy_overrides = backtest_config.get("strategy_kwargs")
    has_strategy_overrides = strategy_overrides is not None
    if topk is None and initial_cash is None and not has_strategy_overrides:
        return
    if has_strategy_overrides and not isinstance(strategy_overrides, Mapping):
        raise MultiAlphaCombineBacktestError(
            "strategy_kwargs must be a mapping before pred-backtest conf override",
            reason_code="pred_backtest_conf_override_invalid",
            context={"workspace": str(workspace), "strategy_kwargs_type": type(strategy_overrides).__name__},
        )
    topk_int = _positive_int(topk, field_name="topk") if topk is not None else None
    initial_cash_int = _positive_int(initial_cash, field_name="initial_cash") if initial_cash is not None else None
    conf_path = workspace / "conf.yaml"
    strategy_keys: list[str] = []
    if isinstance(strategy_overrides, Mapping):
        strategy_keys = [str(key) for key in strategy_overrides]
    updated_conf = _apply_pred_backtest_overrides_text(
        conf_path.read_text(encoding="utf-8"),
        workspace=workspace,
        conf_path=conf_path,
        topk=topk_int,
        initial_cash=initial_cash_int,
        strategy_overrides=strategy_overrides if isinstance(strategy_overrides, Mapping) else {},
    )
    conf_path.write_text(updated_conf, encoding="utf-8")
    logger.info(
        "Applied pred-backtest conf overrides",
        extra={
            "workspace": str(workspace),
            "effective_topk": topk_int,
            "effective_initial_cash": initial_cash_int,
            "strategy_kwargs_keys": sorted(strategy_keys),
        },
    )


def _apply_pred_backtest_overrides_text(
    text: str,
    *,
    workspace: Path,
    conf_path: Path,
    topk: int | None,
    initial_cash: int | None,
    strategy_overrides: Mapping[str, Any],
) -> str:
    lines = text.splitlines(keepends=True)
    port_idx, port_indent = _find_conf_mapping_key(
        lines,
        key="port_analysis_config",
        start=0,
        end=len(lines),
        conf_path=conf_path,
        workspace=workspace,
    )
    port_end = _conf_block_end(lines, start=port_idx + 1, parent_indent=port_indent)
    if initial_cash is not None:
        backtest_idx, backtest_indent = _find_conf_mapping_key(
            lines,
            key="backtest",
            start=port_idx + 1,
            end=port_end,
            conf_path=conf_path,
            workspace=workspace,
            field="port_analysis_config.backtest",
        )
        backtest_end = _conf_block_end(lines, start=backtest_idx + 1, parent_indent=backtest_indent)
        backtest_child_indent = _infer_conf_child_indent(
            lines,
            start=backtest_idx + 1,
            end=backtest_end,
            parent_indent=backtest_indent,
        )
        _replace_or_insert_conf_mapping_key(
            lines,
            key="account",
            value=initial_cash,
            start=backtest_idx + 1,
            end=backtest_end,
            parent_indent=backtest_indent,
            child_indent=backtest_child_indent,
            conf_path=conf_path,
            workspace=workspace,
            field_prefix="port_analysis_config.backtest",
            required=False,
        )
        port_end = _conf_block_end(lines, start=port_idx + 1, parent_indent=port_indent)
    strategy_idx, strategy_indent = _find_conf_mapping_key(
        lines,
        key="strategy",
        start=port_idx + 1,
        end=port_end,
        conf_path=conf_path,
        workspace=workspace,
    )
    strategy_end = _conf_block_end(lines, start=strategy_idx + 1, parent_indent=strategy_indent)
    kwargs_idx, kwargs_indent = _find_conf_mapping_key(
        lines,
        key="kwargs",
        start=strategy_idx + 1,
        end=strategy_end,
        conf_path=conf_path,
        workspace=workspace,
        field="port_analysis_config.strategy.kwargs",
    )
    kwargs_end = _conf_block_end(lines, start=kwargs_idx + 1, parent_indent=kwargs_indent)
    child_indent = _infer_conf_child_indent(lines, start=kwargs_idx + 1, end=kwargs_end, parent_indent=kwargs_indent)
    if topk is not None:
        delta = _replace_or_insert_conf_mapping_key(
            lines,
            key="topk",
            value=topk,
            start=kwargs_idx + 1,
            end=kwargs_end,
            parent_indent=kwargs_indent,
            child_indent=child_indent,
            conf_path=conf_path,
            workspace=workspace,
            field_prefix="port_analysis_config.strategy.kwargs",
            required=True,
        )
        kwargs_end += delta
    for key, value in strategy_overrides.items():
        delta = _replace_or_insert_conf_mapping_key(
            lines,
            key=str(key),
            value=value,
            start=kwargs_idx + 1,
            end=kwargs_end,
            parent_indent=kwargs_indent,
            child_indent=child_indent,
            conf_path=conf_path,
            workspace=workspace,
            field_prefix="port_analysis_config.strategy.kwargs",
            required=False,
        )
        kwargs_end += delta
    return "".join(lines)


def _find_conf_mapping_key(
    lines: Sequence[str],
    *,
    key: str,
    start: int,
    end: int,
    conf_path: Path,
    workspace: Path,
    field: str | None = None,
) -> tuple[int, int]:
    matches: list[tuple[int, int]] = []
    pattern = re.compile(rf"^(?P<indent>[ \t]*){re.escape(key)}\s*:(?P<rest>.*)$")
    for idx in range(start, end):
        match = pattern.match(lines[idx].rstrip("\r\n"))
        if match is None:
            continue
        matches.append((idx, len(match.group("indent"))))
    if not matches:
        raise MultiAlphaCombineBacktestError(
            f"conf.yaml missing mapping field: {field or key}",
            reason_code="pred_backtest_conf_invalid",
            context={"workspace": str(workspace), "conf_path": str(conf_path), "field": field or key},
        )
    if len(matches) > 1:
        raise MultiAlphaCombineBacktestError(
            f"conf.yaml has ambiguous mapping field: {field or key}",
            reason_code="pred_backtest_conf_invalid",
            context={"workspace": str(workspace), "conf_path": str(conf_path), "field": field or key, "match_count": len(matches)},
        )
    return matches[0]


def _conf_block_end(lines: Sequence[str], *, start: int, parent_indent: int) -> int:
    for idx in range(start, len(lines)):
        stripped = lines[idx].strip()
        if not stripped or stripped.startswith("#"):
            continue
        if _conf_line_indent(lines[idx]) <= parent_indent:
            return idx
    return len(lines)


def _infer_conf_child_indent(lines: Sequence[str], *, start: int, end: int, parent_indent: int) -> int:
    for idx in range(start, end):
        stripped = lines[idx].strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = _conf_line_indent(lines[idx])
        if indent > parent_indent:
            return indent
    return parent_indent + 2


def _replace_or_insert_conf_mapping_key(
    lines: list[str],
    *,
    key: str,
    value: Any,
    start: int,
    end: int,
    parent_indent: int,
    child_indent: int,
    conf_path: Path,
    workspace: Path,
    field_prefix: str,
    required: bool,
) -> int:
    pattern = re.compile(rf"^(?P<indent>[ \t]*){re.escape(key)}\s*:.*$")
    matches: list[tuple[int, int]] = []
    for idx in range(start, end):
        if _conf_line_indent(lines[idx]) <= parent_indent:
            continue
        match = pattern.match(lines[idx].rstrip("\r\n"))
        if match is not None:
            matches.append((idx, len(match.group("indent"))))
    if len(matches) > 1:
        raise MultiAlphaCombineBacktestError(
            f"conf.yaml has ambiguous mapping field: {field_prefix}.{key}",
            reason_code="pred_backtest_conf_invalid",
            context={"workspace": str(workspace), "conf_path": str(conf_path), "field": f"{field_prefix}.{key}", "match_count": len(matches)},
        )
    if not matches:
        if required:
            raise MultiAlphaCombineBacktestError(
                f"conf.yaml missing mapping field: {field_prefix}.{key}",
                reason_code="pred_backtest_conf_invalid",
                context={"workspace": str(workspace), "conf_path": str(conf_path), "field": f"{field_prefix}.{key}"},
            )
        rendered = _render_conf_mapping_value(key=key, value=value, indent=child_indent, newline=_conf_newline(lines))
        lines[end:end] = rendered.splitlines(keepends=True)
        return len(rendered.splitlines())
    idx, indent = matches[0]
    rendered = _render_conf_mapping_value(key=key, value=value, indent=indent, newline=_conf_line_newline(lines[idx]) or _conf_newline(lines))
    replacement = rendered.splitlines(keepends=True)
    lines[idx : idx + 1] = replacement
    return len(replacement) - 1


def _render_conf_mapping_value(*, key: str, value: Any, indent: int, newline: str) -> str:
    rendered = yaml.safe_dump({key: value}, allow_unicode=True, sort_keys=False, default_flow_style=False)
    rendered = "\n".join(line for line in rendered.splitlines() if line != "...")
    rendered = rendered.replace("\n", newline)
    return "".join((" " * indent) + line + newline for line in rendered.split(newline) if line)


def _conf_line_indent(line: str) -> int:
    return len(line) - len(line.lstrip(" \t"))


def _conf_line_newline(line: str) -> str:
    if line.endswith("\r\n"):
        return "\r\n"
    if line.endswith("\n"):
        return "\n"
    return ""


def _conf_newline(lines: Sequence[str]) -> str:
    for line in lines:
        newline = _conf_line_newline(line)
        if newline:
            return newline
    return "\n"


def _require_mapping(payload: Mapping[str, Any], key: str, *, conf_path: Path) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise MultiAlphaCombineBacktestError(
            f"conf.yaml missing mapping field: {key}",
            reason_code="pred_backtest_conf_invalid",
            context={"conf_path": str(conf_path), "field": key},
        )
    return value


def _pred_backtest_error_context(*, workspace: Path, node_id: str, backtest_config: Mapping[str, Any]) -> dict[str, Any]:
    context = {
        "workspace": str(workspace),
        "node_id": node_id,
        "backtest_name": str(backtest_config.get("backtest_name") or workspace.name),
        "timeout_seconds": backtest_config.get("timeout_seconds"),
        "topk": backtest_config.get("topk"),
        "initial_cash": backtest_config.get("initial_cash"),
    }
    if backtest_config.get("weighting_scheme"):
        context["weighting_scheme"] = backtest_config.get("weighting_scheme")
    if backtest_config.get("dropped_leg_id"):
        context["dropped_leg_id"] = backtest_config.get("dropped_leg_id")
    return context


def run_command(
    command: str | Sequence[Any],
    *,
    cwd: Path,
    timeout_seconds: int,
    log_prefix: str,
    env: Mapping[str, str] | None = None,
    error_context: Mapping[str, Any] | None = None,
) -> subprocess.CompletedProcess[str]:
    command_for_log: str | list[str]
    if isinstance(command, str):
        command_for_log = command
    else:
        command_for_log = [str(part) for part in command]
    try:
        completed = subprocess.run(
            command_for_log,
            cwd=cwd,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            stdin=subprocess.DEVNULL,
            shell=isinstance(command_for_log, str),
            timeout=timeout_seconds,
            check=False,
            env=dict(env) if env is not None else None,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = _process_output_text(exc.stdout)
        stderr = _process_output_text(exc.stderr)
        (cwd / f"{log_prefix}_stdout.log").write_text(stdout, encoding="utf-8", errors="replace")
        (cwd / f"{log_prefix}_stderr.log").write_text(stderr, encoding="utf-8", errors="replace")
        context = {
            "cwd": str(cwd),
            "command": command_for_log,
            "timeout_seconds": timeout_seconds,
            "stdout_tail": stdout[-1000:],
            "stderr_tail": stderr[-1000:],
            **dict(error_context or {}),
        }
        raise MultiAlphaCombineBacktestError(
            f"pred-backtest subprocess timed out after {timeout_seconds}s",
            reason_code="pred_backtest_timeout",
            context=context,
        ) from exc
    (cwd / f"{log_prefix}_stdout.log").write_text(completed.stdout or "", encoding="utf-8", errors="replace")
    (cwd / f"{log_prefix}_stderr.log").write_text(completed.stderr or "", encoding="utf-8", errors="replace")
    return completed


def _process_output_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


class MultiAlphaCombineBacktestRepository:
    """PostgreSQL persistence for combine-backtest runs."""

    def __init__(self, connection_provider=get_conn) -> None:
        self._connection_provider = connection_provider
        self._run_has_updated_at_cache: bool | None = None

    def _run_has_updated_at(self, conn: Any) -> bool:
        if self._run_has_updated_at_cache is not None:
            return self._run_has_updated_at_cache
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'strategy_pkg'
                  AND table_name = 'multi_alpha_combine_backtest_run'
                  AND column_name = 'updated_at'
                """
            )
            self._run_has_updated_at_cache = cur.fetchone() is not None
        return self._run_has_updated_at_cache

    def create_run(self, *, run_id: str, request: CombineBacktestRequest, roster_hash: str) -> None:
        persisted_backtest_config = persisted_backtest_config_for(request)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                if self._run_has_updated_at(conn):
                    cur.execute(
                        """
                        INSERT INTO strategy_pkg.multi_alpha_combine_backtest_run
                            (id, roster_hash, roster_json, oos_start, oos_end, normalize_method,
                             walk_forward_json, backtest_config_json, baseline_leg_id, status, reason, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'running', NULL, NOW())
                        """,
                        (
                            run_id,
                            roster_hash,
                            Json(_roster_payload(request.roster)),
                            request.oos_start,
                            request.oos_end,
                            request.normalize_method,
                            Json(dict(request.walk_forward)),
                            Json(persisted_backtest_config),
                            request.baseline_leg_id,
                        ),
                    )
                else:
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
                            Json(persisted_backtest_config),
                            request.baseline_leg_id,
                        ),
                    )

    def update_run_status(self, run_id: str, *, status: str, reason: Mapping[str, Any] | None = None) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                if self._run_has_updated_at(conn):
                    cur.execute(
                        """
                        UPDATE strategy_pkg.multi_alpha_combine_backtest_run
                        SET status = %s, reason = %s, updated_at = NOW()
                        WHERE id = %s
                        """,
                        (status, Json(reason) if reason is not None else None, run_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE strategy_pkg.multi_alpha_combine_backtest_run
                        SET status = %s, reason = %s
                        WHERE id = %s
                        """,
                        (status, Json(reason) if reason is not None else None, run_id),
                    )

    def heartbeat_run(self, run_id: str, *, reason: Mapping[str, Any]) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                if self._run_has_updated_at(conn):
                    cur.execute(
                        """
                        UPDATE strategy_pkg.multi_alpha_combine_backtest_run
                        SET reason = %s, updated_at = NOW()
                        WHERE id = %s AND status = 'running'
                        """,
                        (Json(reason), run_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE strategy_pkg.multi_alpha_combine_backtest_run
                        SET reason = %s
                        WHERE id = %s AND status = 'running'
                        """,
                        (Json(reason), run_id),
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

    def delete_run(self, run_id: str) -> bool:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM strategy_pkg.multi_alpha_combine_backtest_run WHERE id = %s RETURNING id",
                    (run_id,),
                )
                return cur.fetchone() is not None

    def mark_stale_running_runs_failed(
        self,
        *,
        max_age_seconds: int,
        dry_run: bool = True,
        reason_code: str = "combine_backtest_stale_timeout",
    ) -> dict[str, Any]:
        if max_age_seconds <= 0:
            raise MultiAlphaCombineBacktestError(
                "max_age_seconds must be positive",
                reason_code="stale_cleanup_invalid_max_age",
                context={"max_age_seconds": max_age_seconds},
            )
        with self._connection_provider() as conn:
            has_updated_at = self._run_has_updated_at(conn)
            age_expr = "COALESCE(updated_at, created_at)" if has_updated_at else "created_at"
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT id, status, reason, created_at{", updated_at" if has_updated_at else ""}
                    FROM strategy_pkg.multi_alpha_combine_backtest_run
                    WHERE status = 'running'
                      AND {age_expr} < NOW() - (%s * INTERVAL '1 second')
                    ORDER BY {age_expr} ASC
                    """,
                    (max_age_seconds,),
                )
                candidates = [dict(row) for row in cur.fetchall()]
                if dry_run or not candidates:
                    return {"dry_run": dry_run, "max_age_seconds": max_age_seconds, "candidate_count": len(candidates), "runs": candidates}
                failed_at = utc_now_iso()
                reason = {
                    "reason_code": reason_code,
                    "message": "running combine-backtest run exceeded stale-run cleanup age and was marked failed by explicit operator action",
                    "max_age_seconds": max_age_seconds,
                    "failed_at": failed_at,
                    "logical_status": "failed",
                }
                ids = [row["id"] for row in candidates]
                if has_updated_at:
                    cur.execute(
                        """
                        UPDATE strategy_pkg.multi_alpha_combine_backtest_run
                        SET status = 'failed', reason = %s, updated_at = NOW()
                        WHERE id = ANY(%s)
                        """,
                        (Json(reason), ids),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE strategy_pkg.multi_alpha_combine_backtest_run
                        SET status = 'failed', reason = %s
                        WHERE id = ANY(%s)
                        """,
                        (Json(reason), ids),
                    )
        return {"dry_run": False, "max_age_seconds": max_age_seconds, "updated_count": len(candidates), "runs": candidates, "reason": reason}


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
            "backtest_config_json": persisted_backtest_config_for(request),
            "baseline_leg_id": request.baseline_leg_id,
            "status": "running",
            "reason": None,
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }

    def update_run_status(self, run_id: str, *, status: str, reason: Mapping[str, Any] | None = None) -> None:
        with self._lock:
            self.runs[run_id]["status"] = status
            self.runs[run_id]["reason"] = dict(reason or {}) if reason is not None else None
            self.runs[run_id]["updated_at"] = utc_now_iso()

    def heartbeat_run(self, run_id: str, *, reason: Mapping[str, Any]) -> None:
        with self._lock:
            if self.runs[run_id].get("status") != "running":
                return
            self.runs[run_id]["reason"] = dict(reason)
            self.runs[run_id]["updated_at"] = utc_now_iso()

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

    def delete_run(self, run_id: str) -> bool:
        with self._lock:
            if run_id not in self.runs:
                return False
            del self.runs[run_id]
            self.scheme_results = [row for row in self.scheme_results if row["run_id"] != run_id]
            self.loo = [row for row in self.loo if row["run_id"] != run_id]
            return True

    def mark_stale_running_runs_failed(
        self,
        *,
        max_age_seconds: int,
        dry_run: bool = True,
        reason_code: str = "combine_backtest_stale_timeout",
    ) -> dict[str, Any]:
        if max_age_seconds <= 0:
            raise MultiAlphaCombineBacktestError(
                "max_age_seconds must be positive",
                reason_code="stale_cleanup_invalid_max_age",
                context={"max_age_seconds": max_age_seconds},
            )
        cutoff = utc_now() - timedelta(seconds=max_age_seconds)
        candidates: list[dict[str, Any]] = []
        with self._lock:
            for run in self.runs.values():
                if run.get("status") != "running":
                    continue
                heartbeat_raw = run.get("updated_at") or run.get("created_at")
                heartbeat = datetime.fromisoformat(str(heartbeat_raw))
                if heartbeat.tzinfo is None:
                    heartbeat = heartbeat.replace(tzinfo=timezone.utc)
                if heartbeat < cutoff:
                    candidates.append(dict(run))
            if dry_run or not candidates:
                return {"dry_run": dry_run, "max_age_seconds": max_age_seconds, "candidate_count": len(candidates), "runs": candidates}
            failed_at = utc_now_iso()
            reason = {
                "reason_code": reason_code,
                "message": "running combine-backtest run exceeded stale-run cleanup age and was marked failed by explicit operator action",
                "max_age_seconds": max_age_seconds,
                "failed_at": failed_at,
                "logical_status": "failed",
            }
            for candidate in candidates:
                run = self.runs[candidate["id"]]
                run["status"] = "failed"
                run["reason"] = dict(reason)
                run["updated_at"] = failed_at
        return {"dry_run": False, "max_age_seconds": max_age_seconds, "updated_count": len(candidates), "runs": candidates, "reason": reason}


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
        durable_submission_service: Any | None = None,
        legacy_execution_mode_for_tests: bool = False,
    ) -> None:
        self._panel_builder = panel_builder or MultiAlphaPanelBuilder()
        self._prediction_loader = prediction_loader
        self._model_store = ModelStoreService()
        self._executor = executor
        self._repository = repository or MultiAlphaCombineBacktestRepository()
        self._capacity_checker = capacity_checker or DatabaseQENodeCapacityChecker()
        self._workspace_root = Path(workspace_root or os.getenv("AISTOCK_MULTI_ALPHA_BACKTEST_ROOT") or "rdagent_assets/multi_alpha_combine_backtests")
        self._clock = clock or utc_now
        self._legacy_execution_mode_for_tests = legacy_execution_mode_for_tests
        if legacy_execution_mode_for_tests:
            if durable_submission_service is not None:
                raise ValueError(
                    "durable_submission_service cannot be combined with legacy_execution_mode_for_tests"
                )
            self._durable_submission_service = None
        else:
            if durable_submission_service is None:
                from backend.services.multi_alpha.durable_submission import (
                    DurableCombineSubmissionService,
                )

                durable_submission_service = DurableCombineSubmissionService(clock=self._clock)
            self._durable_submission_service = durable_submission_service
        self._local_executor = ShellPredBacktestExecutor()
        self._remote_executor: BacktestExecutor | None = None
        self._archive_event_capture = None
        if isinstance(self._repository, MultiAlphaCombineBacktestRepository):
            try:
                from backend.services.qe_archive.event_capture import QEArchiveEventCapture

                # Sidecar only: respect the explicit QE archive capture gate so
                # combine-backtest runs never hard-depend on qe_archive DDL.
                self._archive_event_capture = QEArchiveEventCapture()
            except Exception:
                logger.exception("multi-alpha QE archive event capture is unavailable")

    def submit_run(
        self,
        payload: Mapping[str, Any],
        *,
        run_async: bool | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        if not self._legacy_execution_mode_for_tests:
            return self._durable_submission_service.submit(
                payload,
                run_async_override=run_async,
                idempotency_key=idempotency_key,
            )
        request = parse_request(payload)
        if run_async is not None:
            request = _replace_request(request, run_async=run_async)
        if self._executor is None:
            preflight_pred_backtest_runtime(
                backtest_config=request.backtest_config,
                node_id=str(request.backtest_config.get("node_id") or "wsl2-5080"),
            )
        roster_hash = roster_hash_for(request.roster)
        run_id = make_run_id(roster_hash=roster_hash, oos_start=request.oos_start, oos_end=request.oos_end, ts=self._clock())
        self._repository.create_run(run_id=run_id, request=request, roster_hash=roster_hash)
        self._heartbeat_run(run_id, request, phase="submitted", message="combine-backtest run accepted")
        if request.run_async:
            thread = threading.Thread(target=self._execute_run_thread, args=(run_id, request), daemon=True, name=f"macb-run-{run_id[:12]}")
            thread.start()
            execution_payload: Mapping[str, Any] = {}
        else:
            execution_payload = self.execute_run(run_id, request)
        response = {"run_id": run_id, "status": self.get_run(run_id)["run"]["status"]}
        if "archive_event" in execution_payload:
            response["archive_event"] = execution_payload["archive_event"]
        return response

    def _execute_run_thread(self, run_id: str, request: CombineBacktestRequest) -> None:
        try:
            self.execute_run(run_id, request)
        except Exception as exc:
            # Daemon threads cannot return errors to the caller, so persist a
            # terminal reason here as a second line of defense without
            # overwriting a richer failure already written by execute_run.
            try:
                current_status = str((self.get_run(run_id).get("run") or {}).get("status") or "")
            except Exception:
                current_status = "running"
            if current_status == "running":
                reason = terminal_error_payload(exc, run_id=run_id)
                self._repository.update_run_status(run_id, status="failed", reason=reason)
                self._append_run_event(run_id, {"event": "terminal", "status": "failed", "reason": reason})
            return

    def execute_run(self, run_id: str, request: CombineBacktestRequest) -> dict[str, Any]:
        roster_hash = roster_hash_for(request.roster)
        try:
            payload = self._execute_run(run_id, request)
        except Exception as exc:
            reason = terminal_error_payload(exc, run_id=run_id)
            self._repository.update_run_status(run_id, status="failed", reason=reason)
            archive_event = self._emit_archive_event(
                run_id=run_id,
                roster_hash=roster_hash,
                status="failed",
                reason=reason,
            )
            reason["archive_event"] = archive_event
            self._repository.update_run_status(run_id, status="failed", reason=reason)
            self._append_run_event(run_id, {"event": "terminal", "status": "failed", "reason": reason})
            raise
        status = str(payload.get("status") or "succeeded")
        reason = payload.get("reason") if isinstance(payload.get("reason"), Mapping) else None
        self._repository.update_run_status(run_id, status=_persisted_run_status(status), reason=reason)
        archive_event = self._emit_archive_event(
            run_id=run_id,
            roster_hash=roster_hash,
            status=status,
            reason=reason,
        )
        payload["archive_event"] = archive_event
        if archive_event.get("error") or archive_event.get("skipped_reason"):
            reason = _reason_with_archive_event(reason, status=status, archive_event=archive_event)
            payload["reason"] = reason
            self._repository.update_run_status(run_id, status=_persisted_run_status(status), reason=reason)
        self._append_run_event(
            run_id,
            {
                "event": "terminal",
                "status": status,
                "reason": dict(reason or {}),
                "archive_event": dict(archive_event),
            },
        )
        if status == "failed":
            first_child_error = _first_child_error(reason)
            raise MultiAlphaCombineBacktestError(
                "combine-backtest run failed; see run.reason failed_child_tasks for details",
                reason_code=str((first_child_error or {}).get("reason_code") or "combine_backtest_child_failed"),
                context={"run_id": run_id, "reason": dict(reason or {}), "archive_event": archive_event},
            )
        return payload

    def _emit_archive_event(
        self,
        *,
        run_id: str,
        roster_hash: str,
        status: str,
        reason: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._archive_event_capture is None:
            return {"queued": False, "skipped_reason": "archive_event_capture_not_configured", "run_id": run_id}
        try:
            result = self._archive_event_capture.enqueue_multi_alpha_combine_completed_result(
                run_id=run_id,
                roster_hash=roster_hash,
                status=status,
                payload={
                    "reason_code": "multi_alpha_combine_terminal",
                    "logical_status": status,
                    "terminal_reason": dict(reason or {}),
                },
            )
        except Exception as exc:
            logger.exception("failed to enqueue multi-alpha QE archive event for run_id=%s", run_id)
            return {
                "queued": False,
                "error": f"{type(exc).__name__}: {exc}",
                "run_id": run_id,
                "status": status,
            }
        return {"queued": bool(result.get("inserted")), **result}

    def get_run(self, run_id: str) -> dict[str, Any]:
        payload = self._repository.get_run(run_id)
        if payload is None:
            raise MultiAlphaCombineBacktestError(f"run not found: {run_id}", reason_code="run_not_found")
        return payload

    def list_runs(self, *, status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        return self._repository.list_runs(status=status, limit=limit)

    def get_retry_draft(self, run_id: str) -> dict[str, Any]:
        bundle = self.get_run(run_id)
        run = dict(bundle.get("run") or {})
        return retry_draft_from_bundle(bundle, retryable=str(run.get("status") or "") in TERMINAL_STATUSES)

    def retry_run(self, run_id: str, *, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        bundle = self.get_run(run_id)
        run = dict(bundle.get("run") or {})
        status = str(run.get("status") or "")
        if status not in TERMINAL_STATUSES:
            raise MultiAlphaCombineBacktestError(
                "only terminal combine-backtest runs can be retried",
                reason_code="combine_backtest_retry_not_terminal",
                context={"run_id": run_id, "status": status},
            )
        draft = retry_draft_from_bundle(bundle, retryable=True)
        if payload is None:
            if not draft["exact"]:
                raise MultiAlphaCombineBacktestError(
                    "legacy combine run requires an explicit retry payload because the original request snapshot is incomplete",
                    reason_code="combine_backtest_retry_payload_required",
                    context={"run_id": run_id, "retry_draft": draft},
                )
            retry_payload = dict(draft["payload"])
        else:
            retry_payload = dict(payload)
        backtest_config = dict(retry_payload.get("backtest_config") or {})
        backtest_config.pop(REQUEST_SNAPSHOT_KEY, None)
        backtest_config.update(
            {
                "retry_of_run_id": run_id,
                "retry_requested_at": utc_now_iso(),
                "retry_request_source": "exact_snapshot" if draft["exact"] and payload is None else "explicit_retry_payload",
            }
        )
        retry_payload["backtest_config"] = backtest_config
        retry_payload["run_async"] = True
        retry_payload["task_id"] = (bundle.get("run") or {}).get("task_id")
        retry_payload["retry_of_run_id"] = run_id
        result = self.submit_run(retry_payload, run_async=True)
        return {**result, "retry_of_run_id": run_id, "retry_source": backtest_config["retry_request_source"]}

    def delete_run(self, run_id: str, *, cleanup_workspace: bool = True) -> dict[str, Any]:
        bundle = self.get_run(run_id)
        status = str((bundle.get("run") or {}).get("status") or "")
        if status not in TERMINAL_STATUSES:
            raise MultiAlphaCombineBacktestError(
                "running combine-backtest run cannot be deleted because deletion is not cancellation",
                reason_code="combine_backtest_delete_not_terminal",
                context={"run_id": run_id, "status": status},
            )
        if not self._legacy_execution_mode_for_tests:
            from backend.services.multi_alpha.durable_repository import (
                MultiAlphaDurableRepository,
                MultiAlphaDurableRepositoryError,
            )

            try:
                MultiAlphaDurableRepository().assert_recovery_source_delete_allowed(run_id)
            except MultiAlphaDurableRepositoryError as exc:
                if exc.reason_code == "recovery_source_copy_in_progress":
                    raise MultiAlphaCombineBacktestError(
                        "terminal source deletion is temporarily blocked while its frozen recovery artifacts publish",
                        reason_code="recovery_source_copy_in_progress",
                        context=dict(exc.context),
                    ) from exc
                raise
        workspace = self._safe_run_workspace(run_id)
        quarantine: Path | None = None
        if cleanup_workspace and workspace.exists():
            quarantine = workspace.with_name(f".{workspace.name}.deleting-{int(time.time() * 1000)}")
            try:
                workspace.rename(quarantine)
            except OSError as exc:
                raise MultiAlphaCombineBacktestError(
                    "failed to quarantine combine-backtest workspace before deleting DB rows",
                    reason_code="combine_backtest_delete_workspace_quarantine_failed",
                    context={"run_id": run_id, "workspace": str(workspace), "error": f"{type(exc).__name__}: {exc}"},
                ) from exc
        try:
            if not hasattr(self._repository, "delete_run") or not self._repository.delete_run(run_id):
                raise MultiAlphaCombineBacktestError(
                    "combine-backtest run disappeared before deletion completed",
                    reason_code="combine_backtest_delete_missing",
                    context={"run_id": run_id},
                )
        except Exception as exc:
            if quarantine is not None and quarantine.exists() and not workspace.exists():
                try:
                    quarantine.rename(workspace)
                except OSError as restore_exc:
                    raise MultiAlphaCombineBacktestError(
                        "DB deletion failed and the quarantined workspace could not be restored",
                        reason_code="combine_backtest_delete_rollback_failed",
                        context={
                            "run_id": run_id,
                            "workspace": str(workspace),
                            "delete_error": f"{type(exc).__name__}: {exc}",
                            "restore_error": f"{type(restore_exc).__name__}: {restore_exc}",
                        },
                    ) from restore_exc
            raise
        cleanup_error = None
        if quarantine is not None and quarantine.exists():
            try:
                shutil.rmtree(quarantine)
            except OSError as exc:
                cleanup_error = f"{type(exc).__name__}: {exc}"
        return {
            "run_id": run_id,
            "deleted": True,
            "workspace_removed": quarantine is not None and cleanup_error is None,
            "workspace_cleanup_error": cleanup_error,
        }

    def get_run_logs(self, run_id: str, *, tail_lines: int = 200) -> dict[str, Any]:
        bundle = self.get_run(run_id)
        run = dict(bundle.get("run") or {})
        workspace = self._safe_run_workspace(run_id)
        events_path = workspace / RUN_EVENT_LOG_NAME
        try:
            file_events = read_jsonl_tail(events_path, max(1, min(int(tail_lines), 1000)))
            files = list_run_log_files(workspace)
        except OSError as exc:
            raise MultiAlphaCombineBacktestError(
                "failed to read combine-backtest workspace logs",
                reason_code="combine_backtest_logs_read_failed",
                context={"run_id": run_id, "workspace": str(workspace), "error": f"{type(exc).__name__}: {exc}"},
            ) from exc
        durable_events: list[dict[str, Any]] = []
        if run.get("task_id") and run.get("request_hash"):
            from backend.services.multi_alpha.durable_repository import (
                MultiAlphaDurableRepository,
            )

            durable_events = MultiAlphaDurableRepository().list_events(
                run_id,
                limit=max(1, min(int(tail_lines), 500)),
            )
        events = [*file_events, *durable_events]
        reason = dict(run.get("reason") or {}) if isinstance(run.get("reason"), Mapping) else {}
        return {
            "run_id": run_id,
            "status": run.get("status"),
            "phase": run.get("phase") or reason.get("phase"),
            "progress": (
                dict(run.get("progress_json") or {})
                if isinstance(run.get("progress_json"), Mapping)
                else dict(reason.get("progress") or {})
                if isinstance(reason.get("progress"), Mapping)
                else {}
            ),
            "heartbeat_at": reason.get("heartbeat_at") or run.get("updated_at"),
            "reason": reason,
            "history_available": events_path.is_file() or bool(durable_events),
            "events": events,
            "durable_events": durable_events,
            "files": files,
        }

    def get_archive_status(self, run_id: str) -> dict[str, Any]:
        self.get_run(run_id)
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT run_id, status, research_valid, invalid_reason, archived_at
                    FROM qe_archive.run
                    WHERE run_id = %s AND run_type = 'multi_alpha_combine'
                    """,
                    (run_id,),
                )
                archive_run = cur.fetchone()
                cur.execute(
                    """
                    SELECT phase, reason_code, payload_json, created_at
                    FROM strategy_pkg.multi_alpha_combine_backtest_event
                    WHERE run_id = %s
                      AND phase IN (
                          'archive_enqueued', 'archive_duplicate',
                          'archive_skipped_disabled', 'archive_error'
                      )
                    ORDER BY event_id DESC
                    LIMIT 1
                    """,
                    (run_id,),
                )
                durable_event = cur.fetchone()
                archive_event_id = None
                if durable_event and isinstance(durable_event.get("payload_json"), Mapping):
                    archive_event_id = durable_event["payload_json"].get("archive_event_id")
                outbox = None
                if archive_event_id:
                    cur.execute(
                        """
                        SELECT event_id, status, retry_count, next_retry_at,
                               error_message, created_at, updated_at
                        FROM qe_archive.outbox_event
                        WHERE event_id = %s
                        """,
                        (archive_event_id,),
                    )
                    outbox = cur.fetchone()
        if archive_run:
            archive_status = "archived"
        elif outbox and outbox.get("status") in {"failed", "error"}:
            archive_status = "archive_error"
        elif outbox:
            archive_status = f"archive_{outbox.get('status')}"
        elif durable_event:
            archive_status = str(durable_event.get("phase") or "not_archived")
        else:
            archive_status = "not_archived"
        return {
            "run_id": run_id,
            "archive_status": archive_status,
            "archive_run": dict(archive_run) if archive_run else None,
            "archive_delivery_event": dict(durable_event) if durable_event else None,
            "archive_outbox": dict(outbox) if outbox else None,
        }

    def get_archive_snapshot(self, run_id: str) -> dict[str, Any]:
        """Return immutable Archive detail without depending on source rows.

        A terminal source run can be deliberately deleted after Archive
        delivery.  Reading its archived evidence must still work and must not
        be redirected to mutable durable tables or a recreated experiment.
        """

        from backend.services.qe_archive.repository import QEArchiveRepository

        snapshot = QEArchiveRepository().fetch_archived_multi_alpha_combine_run(run_id)
        if snapshot is None:
            raise MultiAlphaCombineBacktestError(
                "archived multi-alpha combine run was not found",
                reason_code="combine_backtest_archive_snapshot_not_found",
                context={"run_id": run_id},
            )
        return snapshot

    def archive_run(self, run_id: str, *, dry_run: bool = True) -> dict[str, Any]:
        bundle = self.get_run(run_id)
        status = str((bundle.get("run") or {}).get("status") or "")
        if status not in TERMINAL_STATUSES:
            raise MultiAlphaCombineBacktestError(
                "only terminal combine-backtest runs can be archived",
                reason_code="combine_backtest_archive_not_terminal",
                context={"run_id": run_id, "status": status},
            )
        from backend.services.qe_archive.backfill_service import QEArchiveBackfillService

        return QEArchiveBackfillService().archive_multi_alpha_combine_completed(run_id=run_id, dry_run=dry_run)

    def mark_stale_running_runs_failed(
        self,
        *,
        max_age_seconds: int,
        dry_run: bool = True,
        confirmation: str | None = None,
    ) -> dict[str, Any]:
        if not dry_run and confirmation != COMBINE_BACKTEST_STALE_FAIL_CONFIRM:
            raise MultiAlphaCombineBacktestError(
                "marking stale combine-backtest runs failed requires explicit confirmation token",
                reason_code="stale_cleanup_confirmation_required",
                context={"required_confirmation": COMBINE_BACKTEST_STALE_FAIL_CONFIRM},
            )
        if not hasattr(self._repository, "mark_stale_running_runs_failed"):
            raise MultiAlphaCombineBacktestError(
                "combine-backtest repository does not support stale-run cleanup",
                reason_code="stale_cleanup_not_supported",
            )
        return self._repository.mark_stale_running_runs_failed(max_age_seconds=max_age_seconds, dry_run=dry_run)

    def _execute_run(self, run_id: str, request: CombineBacktestRequest) -> dict[str, Any]:
        run_started = time.monotonic()
        self._raise_if_run_timed_out(run_id=run_id, request=request, started_monotonic=run_started, phase="start")
        node_id = str(request.backtest_config.get("node_id") or "wsl2-5080")
        node_parallelism = validate_node_parallelism(node_id=node_id, backtest_config=request.backtest_config)
        self._heartbeat_run(
            run_id,
            request,
            phase="loading_legs",
            message="loading seed ensemble panels and validating node capacity",
            progress={"node_id": node_id, "node_parallelism": node_parallelism},
        )
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
        self._raise_if_run_timed_out(run_id=run_id, request=request, started_monotonic=run_started, phase="legs_loaded")
        self._heartbeat_run(
            run_id,
            request,
            phase="legs_loaded",
            message="leg panels loaded and seed ensembles aligned",
            progress={"leg_count": len(panels), "needs_panel_metrics": needs_panel_metrics},
        )
        leg_by_id = {leg.leg_id: leg for leg in panels}
        if request.baseline_leg_id and request.baseline_leg_id not in leg_by_id:
            raise MultiAlphaCombineBacktestError(
                "baseline_leg_id is not present in roster",
                reason_code="baseline_leg_missing",
                leg_id=request.baseline_leg_id,
            )

        task_specs: list[_PredictionTask] = []
        task_selection = resolved_prediction_task_selection(request)
        if request.baseline_leg_id and task_selection["include_baseline"]:
            task_specs.append(
                _PredictionTask(
                    name=f"baseline_{request.baseline_leg_id}",
                    kind="baseline",
                    frame=leg_by_id[request.baseline_leg_id].pred_frame,
                    critical=True,
                )
            )

        self._heartbeat_run(
            run_id,
            request,
            phase="combining_scores",
            message="building combined score frames for requested schemes and LOO tasks",
            progress={"scheme_count": len(request.weighting_schemes)},
        )
        build_failures: list[_PredictionTaskOutcome] = []
        for scheme in request.weighting_schemes:
            self._raise_if_run_timed_out(run_id=run_id, request=request, started_monotonic=run_started, phase="combining_scores")
            combine_input = prediction_legs if is_rank_fusion_scheme(scheme) else panels
            try:
                result = combine_legs(legs=combine_input, scheme=scheme, request=request)
            except MultiAlphaCombineBacktestError as exc:
                failed_task = _PredictionTask(
                    name=f"combined_{scheme}",
                    kind="scheme",
                    scheme=scheme,
                    frame=pd.DataFrame(),
                    critical=scheme == "equal",
                )
                failure = _PredictionTaskOutcome(task=failed_task, error=error_payload(exc))
                build_failures.append(failure)
                self._heartbeat_run(
                    run_id,
                    request,
                    phase="scheme_combine_failed",
                    message=f"weighting scheme could not build a combined score frame: scheme={scheme}",
                    scheme=scheme,
                    child_task=failed_task.name,
                    progress={
                        "requested_scheme_count": len(request.weighting_schemes),
                        "build_failure_count": len(build_failures),
                        "backtest_task_count": len(task_specs),
                    },
                    extra={"error": dict(failure.error or {})},
                )
                continue
            self._heartbeat_run(
                run_id,
                request,
                phase="scheme_combined",
                message=f"combined score frame ready for scheme={scheme}",
                scheme=scheme,
                progress={"task_count": len(task_specs) + 1},
            )
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
            if len(panels) <= 2 or not task_selection["include_loo"]:
                continue
            for dropped_leg in sorted(leg_by_id):
                source_legs = prediction_legs if is_rank_fusion_scheme(scheme) else panels
                loo_legs = [leg for leg in source_legs if leg.leg_id != dropped_leg]
                loo_task_name = f"loo_{scheme}_drop_{dropped_leg}"
                try:
                    loo_result = combine_legs(legs=loo_legs, scheme=scheme, request=request)
                except MultiAlphaCombineBacktestError as exc:
                    failed_task = _PredictionTask(
                        name=loo_task_name,
                        kind="loo",
                        scheme=scheme,
                        dropped_leg_id=dropped_leg,
                        frame=pd.DataFrame(),
                    )
                    failure = _PredictionTaskOutcome(task=failed_task, error=error_payload(exc))
                    build_failures.append(failure)
                    self._heartbeat_run(
                        run_id,
                        request,
                        phase="loo_combine_failed",
                        message=f"LOO score frame could not be built: scheme={scheme}, dropped_leg={dropped_leg}",
                        scheme=scheme,
                        child_task=loo_task_name,
                        progress={
                            "requested_scheme_count": len(request.weighting_schemes),
                            "build_failure_count": len(build_failures),
                            "backtest_task_count": len(task_specs),
                        },
                        extra={"error": dict(failure.error or {})},
                    )
                    continue
                task_specs.append(
                    _PredictionTask(
                        name=loo_task_name,
                        kind="loo",
                        scheme=scheme,
                        dropped_leg_id=dropped_leg,
                        frame=loo_result.combined_score_frame.rename(columns={"combined_score": "score"}),
                    )
                )

        self._raise_if_run_timed_out(run_id=run_id, request=request, started_monotonic=run_started, phase="tasks_built")
        self._heartbeat_run(
            run_id,
            request,
            phase="backtests_running",
            message="starting pred-backtest child tasks",
            progress={
                "task_count": len(task_specs),
                "build_failure_count": len(build_failures),
                "node_id": node_id,
                "node_parallelism_limit": node_parallelism[node_id],
            },
        )
        executed_outcomes = self._run_prediction_tasks(
            run_id=run_id,
            tasks=task_specs,
            node_id=node_id,
            node_parallelism_limit=node_parallelism[node_id],
            request=request,
            run_started_monotonic=run_started,
        )
        outcomes = [*build_failures, *executed_outcomes]
        self._heartbeat_run(
            run_id,
            request,
            phase="persisting_results",
            message="pred-backtest child tasks finished; persisting scheme and LOO results",
            progress={
                "task_count": len(outcomes),
                "failed_count": sum(1 for outcome in outcomes if not outcome.succeeded),
            },
        )
        payload = self._persist_task_outcomes(run_id=run_id, outcomes=outcomes)
        self._heartbeat_run(
            run_id,
            request,
            phase="completed",
            message=f"combine-backtest execution completed with status={payload.get('status')}",
            progress={"status": payload.get("status")},
        )
        return payload

    def _heartbeat_run(
        self,
        run_id: str,
        request: CombineBacktestRequest,
        *,
        phase: str,
        message: str,
        scheme: str | None = None,
        child_task: str | None = None,
        progress: Mapping[str, Any] | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        reason: dict[str, Any] = {
            "reason_code": RUN_HEARTBEAT_REASON_CODE,
            "phase": phase,
            "message": message,
            "heartbeat_at": utc_now_iso(),
            "run_timeout_seconds": request.run_timeout_seconds,
            "scheme_timeout_seconds": request.scheme_timeout_seconds,
            "topk": request.topk,
        }
        if scheme:
            reason["weighting_scheme"] = scheme
        if child_task:
            reason["child_task"] = child_task
        if progress:
            reason["progress"] = dict(progress)
        if extra:
            reason.update(dict(extra))
        event_log_error = self._append_run_event(run_id, {"event": "heartbeat", **reason})
        if event_log_error:
            reason["event_log_error"] = event_log_error
        if hasattr(self._repository, "heartbeat_run"):
            self._repository.heartbeat_run(run_id, reason=reason)
        else:
            self._repository.update_run_status(run_id, status="running", reason=reason)

    def _safe_run_workspace(self, run_id: str) -> Path:
        if not re.fullmatch(r"macb_[A-Za-z0-9_.-]+", str(run_id or "")):
            raise MultiAlphaCombineBacktestError(
                "invalid combine-backtest run id for workspace access",
                reason_code="combine_backtest_workspace_run_id_invalid",
                context={"run_id": run_id},
            )
        root = self._workspace_root.resolve()
        workspace = (root / run_id).resolve()
        if not workspace.is_relative_to(root):
            raise MultiAlphaCombineBacktestError(
                "combine-backtest workspace escaped configured root",
                reason_code="combine_backtest_workspace_escape",
                context={"run_id": run_id, "workspace": str(workspace), "root": str(root)},
            )
        return workspace

    def _append_run_event(self, run_id: str, event: Mapping[str, Any]) -> str | None:
        workspace = self._safe_run_workspace(run_id)
        try:
            workspace.mkdir(parents=True, exist_ok=True)
            payload = {"recorded_at": utc_now_iso(), **dict(event)}
            with _RUN_EVENT_LOG_LOCK:
                with (workspace / RUN_EVENT_LOG_NAME).open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        except OSError as exc:
            logger.exception("failed to append combine-backtest run event run_id=%s", run_id)
            return f"{type(exc).__name__}: {exc}"
        return None

    def _raise_if_run_timed_out(
        self,
        *,
        run_id: str,
        request: CombineBacktestRequest,
        started_monotonic: float,
        phase: str,
    ) -> None:
        timeout_seconds = request.run_timeout_seconds
        if timeout_seconds is None:
            return
        elapsed_seconds = time.monotonic() - started_monotonic
        if elapsed_seconds <= timeout_seconds:
            return
        raise MultiAlphaCombineBacktestError(
            f"combine-backtest run exceeded run_timeout_seconds={timeout_seconds} at phase={phase}",
            reason_code="combine_backtest_run_timeout",
            context={
                "run_id": run_id,
                "phase": phase,
                "elapsed_seconds": round(elapsed_seconds, 3),
                "run_timeout_seconds": timeout_seconds,
            },
        )

    def _build_prediction_only_legs(self, request: CombineBacktestRequest) -> list[CombinerLeg]:
        return build_prediction_only_legs(
            request,
            prediction_loader=self._prediction_loader,
            model_store=self._model_store,
        )

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
        request: CombineBacktestRequest,
        run_started_monotonic: float,
    ) -> list[_PredictionTaskOutcome]:
        if not tasks:
            return []
        max_workers = max(1, min(int(node_parallelism_limit), len(tasks)))
        outcomes_by_name: dict[str, _PredictionTaskOutcome] = {}
        pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="macb-pred")
        future_map: dict[Future[dict[str, Any]], _PredictionTask] = {}
        future_started_at: dict[Future[dict[str, Any]], float] = {}
        pending: set[Future[dict[str, Any]]] = set()
        submitted = 0

        def submit_next_task() -> None:
            nonlocal submitted
            task = tasks[submitted]
            self._raise_if_run_timed_out(
                run_id=run_id,
                request=request,
                started_monotonic=run_started_monotonic,
                phase="submitting_backtest_tasks",
            )
            future = pool.submit(
                self._run_prediction,
                run_id=run_id,
                name=task.name,
                frame=task.frame,
                node_id=node_id,
                node_parallelism_limit=node_parallelism_limit,
                backtest_config=runtime_backtest_config(request),
                task=task,
            )
            submitted += 1
            future_map[future] = task
            future_started_at[future] = time.monotonic()
            pending.add(future)
            self._heartbeat_run(
                run_id,
                request,
                phase="backtest_submitted",
                message=f"submitted pred-backtest child task {task.name}",
                scheme=task.scheme,
                child_task=task.name,
                progress={"completed": len(outcomes_by_name), "submitted": submitted, "total": len(tasks), "pending": len(pending)},
            )

        try:
            while submitted < len(tasks) and len(pending) < max_workers:
                submit_next_task()
            while pending:
                self._raise_if_run_timed_out(
                    run_id=run_id,
                    request=request,
                    started_monotonic=run_started_monotonic,
                    phase="waiting_backtest_tasks",
                )
                done, _not_done = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                if not done:
                    for future in list(pending):
                        elapsed_seconds = time.monotonic() - future_started_at[future]
                        if elapsed_seconds <= request.scheme_timeout_seconds:
                            continue
                        task = future_map[future]
                        error = {
                            "reason_code": "combine_backtest_scheme_timeout",
                            "message": (
                                "pred-backtest child task exceeded scheme_timeout_seconds; "
                                "the run is failed loud instead of remaining running forever"
                            ),
                            "context": {
                                "run_id": run_id,
                                "child_task": task.name,
                                "weighting_scheme": task.scheme,
                                "dropped_leg_id": task.dropped_leg_id,
                                "node_id": node_id,
                                "scheme_timeout_seconds": request.scheme_timeout_seconds,
                                "elapsed_seconds": round(elapsed_seconds, 3),
                            },
                        }
                        self._heartbeat_run(
                            run_id,
                            request,
                            phase="backtest_failed",
                            message=f"pred-backtest child task timed out: {task.name}",
                            scheme=task.scheme,
                            child_task=task.name,
                            progress={"completed": len(outcomes_by_name), "total": len(tasks), "pending": len(pending)},
                            extra={"error": error},
                        )
                        raise MultiAlphaCombineBacktestError(
                            str(error["message"]),
                            reason_code=str(error["reason_code"]),
                            context=dict(error["context"]),
                        )
                    self._heartbeat_run(
                        run_id,
                        request,
                        phase="backtests_running",
                        message="waiting for pred-backtest child tasks",
                        progress={"completed": len(outcomes_by_name), "total": len(tasks), "pending": len(pending)},
                    )
                    continue
                for future in done:
                    pending.remove(future)
                    task = future_map[future]
                    try:
                        metrics = future.result()
                        outcomes_by_name[task.name] = _PredictionTaskOutcome(task=task, metrics=metrics)
                        self._heartbeat_run(
                            run_id,
                            request,
                            phase="backtest_finished",
                            message=f"pred-backtest child task completed: {task.name}",
                            scheme=task.scheme,
                            child_task=task.name,
                            progress={"completed": len(outcomes_by_name), "total": len(tasks), "pending": len(pending)},
                        )
                    except Exception as exc:
                        outcomes_by_name[task.name] = _PredictionTaskOutcome(task=task, error=error_payload(exc))
                        self._heartbeat_run(
                            run_id,
                            request,
                            phase="backtest_failed",
                            message=f"pred-backtest child task failed: {task.name}",
                            scheme=task.scheme,
                            child_task=task.name,
                            progress={"completed": len(outcomes_by_name), "total": len(tasks), "pending": len(pending)},
                            extra={"error": error_payload(exc)},
                        )
                    while submitted < len(tasks) and len(pending) < max_workers:
                        submit_next_task()
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
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
        successful_scheme_count = sum(1 for outcome in outcomes if outcome.task.kind == "scheme" and outcome.succeeded)
        if critical_failures or successful_scheme_count == 0:
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
        task: _PredictionTask | None = None,
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
            executor = self._executor_for_node(node_id)
            metrics = executor.execute_pred_backtest(
                workspace=workspace,
                pred_pkl=pred_pkl,
                node_id=node_id,
                backtest_config={
                    **dict(backtest_config),
                    "backtest_name": name,
                    "weighting_scheme": task.scheme if task else None,
                    "dropped_leg_id": task.dropped_leg_id if task else None,
                    "node_parallelism_limit": node_parallelism_limit,
                },
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

    def _executor_for_node(self, node_id: str) -> BacktestExecutor:
        if self._executor is not None:
            return self._executor
        from backend.services.multi_alpha.remote_dispatch import RemotePredBacktestExecutor, is_remote_compute_node

        if not is_remote_compute_node(node_id):
            return self._local_executor
        if self._remote_executor is None:
            self._remote_executor = RemotePredBacktestExecutor()
        return self._remote_executor


CallableUtc = Any


def parse_request(payload: Mapping[str, Any]) -> CombineBacktestRequest:
    roster = tuple(_coerce_panel_spec(item) for item in payload.get("roster") or [])
    if len(roster) < 2:
        raise MultiAlphaCombineBacktestError("roster requires at least two legs", reason_code="roster_too_small")
    schemes = tuple(_normalize_scheme(item) for item in (payload.get("weighting_schemes") or DEFAULT_WEIGHTING_SCHEMES))
    if not schemes:
        raise MultiAlphaCombineBacktestError("weighting_schemes cannot be empty", reason_code="scheme_missing")
    prediction_task_selection = _parse_prediction_task_selection(
        payload.get("prediction_task_selection")
    )
    selection = resolved_prediction_task_selection_value(prediction_task_selection)
    baseline_leg_id = (
        str(payload.get("baseline_leg_id") or roster[0].leg_id)
        if selection["include_baseline"]
        else None
    )
    raw_backtest_config = payload.get("backtest_config") if isinstance(payload.get("backtest_config"), Mapping) else {}
    topk = _positive_int(payload.get("topk") or raw_backtest_config.get("topk") or 20, field_name="topk")
    subprocess_timeout_seconds = _positive_int(
        raw_backtest_config.get("timeout_seconds") or payload.get("scheme_timeout_seconds") or DEFAULT_PRED_BACKTEST_TIMEOUT_SECONDS,
        field_name="timeout_seconds",
    )
    scheme_timeout_seconds = _positive_int(
        payload.get("scheme_timeout_seconds") or raw_backtest_config.get("scheme_timeout_seconds") or DEFAULT_PRED_BACKTEST_TIMEOUT_SECONDS,
        field_name="scheme_timeout_seconds",
    )
    raw_run_timeout = payload.get("run_timeout_seconds") or raw_backtest_config.get("run_timeout_seconds")
    if raw_run_timeout is None:
        task_count = (
            int(selection["include_baseline"])
            + len(schemes)
            + (
                len(schemes) * len(roster)
                if len(roster) > 2 and selection["include_loo"]
                else 0
            )
        )
        node_id = str(raw_backtest_config.get("node_id") or "wsl2-5080")
        try:
            node_parallelism = validate_node_parallelism(node_id=node_id, backtest_config=raw_backtest_config)[node_id]
        except MultiAlphaCombineBacktestError:
            node_parallelism = 1
        waves = max(1, math.ceil(task_count / max(1, node_parallelism)))
        run_timeout_seconds = (scheme_timeout_seconds * waves) + DEFAULT_RUN_TIMEOUT_GRACE_SECONDS
    else:
        run_timeout_seconds = _positive_int(raw_run_timeout, field_name="run_timeout_seconds")
    backtest_config = dict(raw_backtest_config)
    backtest_config["topk"] = topk
    if raw_backtest_config.get("initial_cash") is not None:
        backtest_config["initial_cash"] = _positive_int(raw_backtest_config.get("initial_cash"), field_name="initial_cash")
    backtest_config["timeout_seconds"] = min(subprocess_timeout_seconds, scheme_timeout_seconds)
    backtest_config.setdefault("read_timeout_seconds", DEFAULT_READ_EXP_TIMEOUT_SECONDS)
    backtest_config["run_timeout_seconds"] = run_timeout_seconds
    return CombineBacktestRequest(
        roster=roster,
        oos_start=str(payload.get("oos_start") or ""),
        oos_end=str(payload.get("oos_end") or ""),
        weighting_schemes=schemes,
        normalize_method=str(payload.get("normalize_method") or "zscore"),
        walk_forward=dict(payload.get("walk_forward") or {"enabled": True, "window": 60, "min_periods": 2}),
        rank_fusion=dict(payload.get("rank_fusion") or {}),
        backtest_config=backtest_config,
        prediction_task_selection=prediction_task_selection,
        baseline_leg_id=baseline_leg_id,
        topk=topk,
        min_date_coverage=float(payload.get("min_date_coverage") or 0.8),
        run_async=bool(payload.get("run_async", True)),
        scheme_timeout_seconds=scheme_timeout_seconds,
        run_timeout_seconds=run_timeout_seconds,
    )


def _replace_request(request: CombineBacktestRequest, **updates: Any) -> CombineBacktestRequest:
    return replace(request, **updates)


def request_snapshot_for(request: CombineBacktestRequest) -> dict[str, Any]:
    backtest_config = dict(request.backtest_config)
    backtest_config.pop(REQUEST_SNAPSHOT_KEY, None)
    snapshot = {
        "roster": _roster_payload(request.roster),
        "oos_start": request.oos_start,
        "oos_end": request.oos_end,
        "weighting_schemes": list(request.weighting_schemes),
        "normalize_method": request.normalize_method,
        "walk_forward": dict(request.walk_forward),
        "rank_fusion": dict(request.rank_fusion),
        "backtest_config": backtest_config,
        "baseline_leg_id": request.baseline_leg_id,
        "topk": request.topk,
        "min_date_coverage": request.min_date_coverage,
        "run_async": True,
        "scheme_timeout_seconds": request.scheme_timeout_seconds,
        "run_timeout_seconds": request.run_timeout_seconds,
    }
    if request.prediction_task_selection is not None:
        snapshot["prediction_task_selection"] = dict(request.prediction_task_selection)
    return snapshot


def resolved_prediction_task_selection(
    request: CombineBacktestRequest,
) -> dict[str, bool]:
    return resolved_prediction_task_selection_value(request.prediction_task_selection)


def resolved_prediction_task_selection_value(
    selection: Mapping[str, bool] | None,
) -> dict[str, bool]:
    normalized = _parse_prediction_task_selection(selection)
    if normalized is None:
        return {"include_baseline": True, "include_loo": True}
    return normalized


def _parse_prediction_task_selection(value: Any) -> dict[str, bool] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise MultiAlphaCombineBacktestError(
            "prediction_task_selection must be an object",
            reason_code="prediction_task_selection_invalid",
            context={"value_type": type(value).__name__},
        )
    supported = {"include_baseline", "include_loo"}
    unknown = sorted(str(key) for key in value if key not in supported)
    if unknown:
        raise MultiAlphaCombineBacktestError(
            "prediction_task_selection contains unsupported fields",
            reason_code="prediction_task_selection_invalid",
            context={"unsupported_fields": unknown, "supported_fields": sorted(supported)},
        )
    result: dict[str, bool] = {}
    for key in sorted(supported):
        raw = value.get(key, True)
        if not isinstance(raw, bool):
            raise MultiAlphaCombineBacktestError(
                f"prediction_task_selection.{key} must be boolean",
                reason_code="prediction_task_selection_invalid",
                context={"field": key, "value": raw},
            )
        result[key] = raw
    return result


def persisted_backtest_config_for(request: CombineBacktestRequest) -> dict[str, Any]:
    config = dict(request.backtest_config)
    config[REQUEST_SNAPSHOT_KEY] = request_snapshot_for(request)
    return config


def retry_draft_from_bundle(bundle: Mapping[str, Any], *, retryable: bool) -> dict[str, Any]:
    run = dict(bundle.get("run") or {})
    config = json_mapping(run.get("backtest_config_json"), field_name="backtest_config_json")
    snapshot = config.get(REQUEST_SNAPSHOT_KEY)
    if isinstance(snapshot, Mapping):
        return {
            "run_id": run.get("id"),
            "retryable": retryable,
            "exact": True,
            "source": "exact_snapshot",
            "assumptions": [],
            "payload": dict(snapshot),
        }

    persisted_schemes = [
        str(row.get("weighting_scheme"))
        for row in bundle.get("scheme_results") or []
        if isinstance(row, Mapping) and row.get("weighting_scheme")
    ]
    reason = json_mapping(run.get("reason"), field_name="reason")
    reason_context = json_mapping(reason.get("context"), field_name="reason.context")
    failed_scheme = str(reason_context.get("weighting_scheme") or reason.get("weighting_scheme") or "").strip()
    if failed_scheme:
        persisted_schemes.append(failed_scheme)
    if persisted_schemes:
        requested_schemes = sorted(
            set(persisted_schemes),
            key=lambda item: SUPPORTED_WEIGHTING_SCHEMES.index(item) if item in SUPPORTED_WEIGHTING_SCHEMES else len(SUPPORTED_WEIGHTING_SCHEMES),
        )
        scheme_assumption = "weighting_schemes reconstructed from persisted scheme rows and terminal reason; schemes that failed before persistence may be absent"
    else:
        requested_schemes = list(DEFAULT_WEIGHTING_SCHEMES)
        scheme_assumption = "weighting_schemes were not persisted; service defaults are prefilled"

    topk = int(config.get("topk") or 20)
    min_date_coverage = float(config.get("min_date_coverage") or 0.8)
    scheme_timeout = int(config.get("scheme_timeout_seconds") or config.get("timeout_seconds") or DEFAULT_PRED_BACKTEST_TIMEOUT_SECONDS)
    run_timeout = int(config.get("run_timeout_seconds") or (scheme_timeout + DEFAULT_RUN_TIMEOUT_GRACE_SECONDS))
    clean_config = {key: value for key, value in config.items() if key != REQUEST_SNAPSHOT_KEY}
    payload = {
        "roster": json_list(run.get("roster_json"), field_name="roster_json"),
        "oos_start": str(run.get("oos_start") or ""),
        "oos_end": str(run.get("oos_end") or ""),
        "weighting_schemes": requested_schemes,
        "normalize_method": str(run.get("normalize_method") or "zscore"),
        "walk_forward": json_mapping(run.get("walk_forward_json"), field_name="walk_forward_json"),
        "rank_fusion": json_mapping(config.get("rank_fusion"), field_name="rank_fusion"),
        "backtest_config": clean_config,
        "baseline_leg_id": run.get("baseline_leg_id"),
        "topk": topk,
        "min_date_coverage": min_date_coverage,
        "run_async": True,
        "scheme_timeout_seconds": scheme_timeout,
        "run_timeout_seconds": run_timeout,
    }
    assumptions = [
        scheme_assumption,
        "min_date_coverage was not persisted separately before request snapshots; current stored value or service default is prefilled",
        "rank_fusion request options may be incomplete for legacy runs unless present in backtest_config_json",
    ]
    return {
        "run_id": run.get("id"),
        "retryable": retryable,
        "exact": False,
        "source": "legacy_reconstruction",
        "assumptions": assumptions,
        "payload": payload,
    }


def json_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise MultiAlphaCombineBacktestError(
                f"{field_name} contains invalid JSON",
                reason_code="combine_backtest_retry_json_invalid",
                context={"field_name": field_name, "value_preview": value[:160]},
            ) from exc
        if isinstance(parsed, Mapping):
            return dict(parsed)
    raise MultiAlphaCombineBacktestError(
        f"{field_name} must be a JSON object",
        reason_code="combine_backtest_retry_json_type_invalid",
        context={"field_name": field_name, "value_type": type(value).__name__},
    )


def json_list(value: Any, *, field_name: str) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise MultiAlphaCombineBacktestError(
                f"{field_name} contains invalid JSON",
                reason_code="combine_backtest_retry_json_invalid",
                context={"field_name": field_name, "value_preview": value[:160]},
            ) from exc
        if isinstance(parsed, list):
            return parsed
    raise MultiAlphaCombineBacktestError(
        f"{field_name} must be a JSON array",
        reason_code="combine_backtest_retry_json_type_invalid",
        context={"field_name": field_name, "value_type": type(value).__name__},
    )


def read_jsonl_tail(path: Path, tail_lines: int) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[-tail_lines:]
    result: list[dict[str, Any]] = []
    for line in lines:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            result.append({"event": "invalid_event_line", "raw": line})
            continue
        result.append(dict(value) if isinstance(value, Mapping) else {"event": "event_value", "value": value})
    return result


def list_run_log_files(workspace: Path) -> list[dict[str, Any]]:
    if not workspace.is_dir():
        return []
    root = workspace.resolve()
    result: list[dict[str, Any]] = []
    candidates = sorted(
        [path for path in workspace.rglob("*.log") if path.is_file()] + ([workspace / RUN_EVENT_LOG_NAME] if (workspace / RUN_EVENT_LOG_NAME).is_file() else []),
        key=lambda path: str(path),
    )
    for path in candidates[:MAX_LOG_FILES]:
        resolved = path.resolve()
        if not resolved.is_relative_to(root):
            continue
        stat = resolved.stat()
        result.append(
            {
                "path": resolved.relative_to(root).as_posix(),
                "size": stat.st_size,
                "updated_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                "tail": read_text_tail(resolved, MAX_LOG_TAIL_CHARS),
            }
        )
    return result


def read_text_tail(path: Path, max_chars: int) -> str:
    max_bytes = max_chars * 4
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        handle.seek(max(0, size - max_bytes), os.SEEK_SET)
        data = handle.read()
    return data.decode("utf-8", errors="replace")[-max_chars:]


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


def _positive_int(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool) or (isinstance(value, float) and not value.is_integer()):
        raise MultiAlphaCombineBacktestError(
            f"{field_name} must be an integer",
            reason_code=f"{field_name}_invalid",
            context={field_name: value},
        )
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise MultiAlphaCombineBacktestError(
            f"{field_name} must be an integer",
            reason_code=f"{field_name}_invalid",
            context={field_name: value},
        ) from exc
    if parsed <= 0:
        raise MultiAlphaCombineBacktestError(
            f"{field_name} must be positive",
            reason_code=f"{field_name}_invalid",
            context={field_name: value},
        )
    return parsed


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


def build_prediction_only_legs(
    request: CombineBacktestRequest,
    *,
    prediction_loader: Any | None = None,
    model_store: ModelStoreService | None = None,
) -> list[CombinerLeg]:
    store = model_store or ModelStoreService()
    legs: list[CombinerLeg] = []
    for spec in request.roster:
        seed_frames: list[pd.DataFrame] = []
        for run_id in spec.seed_run_ids:
            try:
                raw = prediction_loader(run_id) if prediction_loader is not None else pd.read_pickle(
                    store.prediction_path(run_id=run_id)
                )
                seed_frames.append(normalize_prediction_frame(raw, run_id=run_id))
            except (
                PredictionStoreError,
                MultiAlphaOrthogonalityError,
                OSError,
                ValueError,
                TypeError,
                KeyError,
            ) as exc:
                raise MultiAlphaCombineBacktestError(
                    f"failed to load prediction artifact for rank-fusion: {type(exc).__name__}: {exc}",
                    reason_code="prediction_missing_or_invalid",
                    leg_id=spec.leg_id,
                    context={"run_id": run_id},
                ) from exc
        ensemble = seed_ensemble_prediction_only(seed_frames, leg_id=spec.leg_id)
        ensemble = filter_prediction_window(
            ensemble,
            leg_id=spec.leg_id,
            oos_start=request.oos_start,
            oos_end=request.oos_end,
        )
        legs.append(
            CombinerLeg(
                leg_id=spec.leg_id,
                pred_frame=ensemble,
                metadata=dict(spec.metadata),
            )
        )
    return legs


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


def runtime_backtest_config(request: CombineBacktestRequest) -> dict[str, Any]:
    config = dict(request.backtest_config)
    config["topk"] = request.topk
    config.setdefault("timeout_seconds", request.scheme_timeout_seconds)
    config.setdefault("read_timeout_seconds", DEFAULT_READ_EXP_TIMEOUT_SECONDS)
    config["run_timeout_seconds"] = request.run_timeout_seconds
    return config


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


def _reason_with_archive_event(
    reason: Mapping[str, Any] | None,
    *,
    status: str,
    archive_event: Mapping[str, Any],
) -> dict[str, Any]:
    payload = dict(reason or {})
    payload.setdefault("reason_code", "multi_alpha_combine_terminal")
    payload.setdefault("logical_status", status)
    payload["archive_event"] = dict(archive_event)
    return payload


def terminal_error_payload(exc: Exception, *, run_id: str) -> dict[str, Any]:
    payload = error_payload(exc)
    reason_code = payload.get("reason_code")
    if not isinstance(exc, (MultiAlphaCombineBacktestError, MultiAlphaPanelError)):
        reason_code = "combine_backtest_unhandled_exception"
    payload["reason_code"] = reason_code
    payload["run_id"] = run_id
    payload["failed_at"] = utc_now_iso()
    payload["logical_status"] = "failed"
    return payload


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

