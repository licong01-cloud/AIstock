"""Dispatch-only control plane for official full factor compute."""
from __future__ import annotations

import asyncio
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..dispatch_service import DispatchService
from .official_factor_batch_compute_service import OFFICIAL_FACTOR_WINDOW_END, OFFICIAL_FACTOR_WINDOW_START

_DEFAULT_DISPATCH_NODE_ID = os.getenv("AISTOCK_DEFAULT_GPU_NODE_ID", "wsl2-5080")


class OfficialFactorFullComputeDispatchService:
    """Submit official factor full-compute jobs to WSL/compute nodes only."""

    def __init__(self, dispatch_service: DispatchService | None = None) -> None:
        self._dispatch_service = dispatch_service or DispatchService()

    def submit(
        self,
        *,
        factor_names: Optional[List[str]],
        factor_data_dir: str,
        start_date: str = OFFICIAL_FACTOR_WINDOW_START,
        end_date: str = OFFICIAL_FACTOR_WINDOW_END,
        include_disabled: bool = False,
        batch_size: int = 16,
        workers: int = 1,
        timeout_per_factor: int = 1800,
        force: bool = False,
        qlib_bin_path: str | None = None,
        node_id: str | None = None,
        task_id: str | None = None,
        resumed_from_task_id: str | None = None,
    ) -> Dict[str, Any]:
        if not factor_data_dir:
            raise ValueError("factor_data_dir is required for official_factor_full_compute")
        payload = {
            "task_id": task_id,
            "factor_names": factor_names or [],
            "factor_data_dir": factor_data_dir,
            "start_date": start_date,
            "end_date": end_date,
            "window_train_start": start_date,
            "window_backtest_end": end_date,
            "include_disabled": include_disabled,
            "batch_size": batch_size,
            "workers": workers,
            "timeout_per_factor": timeout_per_factor,
            "force": force,
            "resumed_from_task_id": resumed_from_task_id,
            "qlib_bin_path": qlib_bin_path,
            "cache_source": "official_offline_backtest_factor_data",
            "code_source": "code_text",
        }
        created = asyncio.run(self._dispatch_service.create_and_submit_task({
            "task_id": task_id,
            "task_name": f"official_factor_full_compute_{end_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "task_type": "official_factor_full_compute",
            "node_id": node_id or _DEFAULT_DISPATCH_NODE_ID,
            "payload": payload,
            "all_duration": "72:00:00",
        }))
        return {
            "ok": created.get("status") != "failed",
            "status": created.get("status", "queued"),
            "task_id": created.get("task_id"),
            "dispatch_task_id": created.get("task_id"),
            "remote_task_id": created.get("remote_task_id"),
            "node_id": node_id or _DEFAULT_DISPATCH_NODE_ID,
            "payload": payload,
            "cache_source": "official_offline_backtest_factor_data",
            "cache_root": "rdagent_assets/factor_values",
        }
