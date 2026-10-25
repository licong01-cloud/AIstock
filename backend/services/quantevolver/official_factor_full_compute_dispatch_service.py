"""Dispatch-only control plane for official full factor compute."""
from __future__ import annotations

import asyncio
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..canonical_equity_pit import CANONICAL_PIT_UNIVERSE_KEY
from ..dataset_release.active_task_binding import (
    freeze_explicit_dataset_task_binding,
    frozen_dataset_environment,
)
from ..dispatch_service import DispatchService
from .factor_universe_mask_service import OFFICIAL_FACTOR_UNIVERSE_KEY
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
        factor_data_dir: str | None,
        start_date: str = OFFICIAL_FACTOR_WINDOW_START,
        end_date: str = OFFICIAL_FACTOR_WINDOW_END,
        include_disabled: bool = False,
        batch_size: int = 16,
        workers: int = 1,
        timeout_per_factor: int = 1800,
        force: bool = False,
        qlib_bin_path: str | None = None,
        universe_key: str = OFFICIAL_FACTOR_UNIVERSE_KEY,
        node_id: str | None = None,
        task_id: str | None = None,
        resumed_from_task_id: str | None = None,
        dataset_profile_path: str | None = None,
        expected_profile_sha256: str | None = None,
    ) -> Dict[str, Any]:
        effective_node_id = node_id or _DEFAULT_DISPATCH_NODE_ID
        frozen_binding = None
        if dataset_profile_path:
            universe_key = CANONICAL_PIT_UNIVERSE_KEY
            frozen_binding = freeze_explicit_dataset_task_binding(
                profile_path=dataset_profile_path,
                consumer_id="factor_research",
                node_id=effective_node_id,
            )
            if (
                expected_profile_sha256
                and frozen_binding["profile_sha256"] != expected_profile_sha256
            ):
                raise ValueError("explicit dataset profile SHA256 differs from the expected identity")
            if end_date != frozen_binding["cutoff"]:
                raise ValueError(
                    "official factor full compute end_date must equal the explicit profile cutoff"
                )
            dataset_env = frozen_dataset_environment(frozen_binding)
            expected_factor_dir = dataset_env["RDAGENT_FACTOR_DATA_WSL"]
            expected_qlib_path = dataset_env["QE_QLIB_DATA_PATH"]
            if factor_data_dir and factor_data_dir != expected_factor_dir:
                raise ValueError("factor_data_dir differs from the explicit dataset profile")
            if qlib_bin_path and qlib_bin_path != expected_qlib_path:
                raise ValueError("qlib_bin_path differs from the explicit dataset profile")
            factor_data_dir = expected_factor_dir
            qlib_bin_path = expected_qlib_path
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
            "universe_key": universe_key,
            "cache_source": "official_offline_backtest_factor_data",
            "code_source": "code_text",
            "dataset_profile_path": dataset_profile_path,
            "dataset_profile_sha256": (
                frozen_binding.get("profile_sha256") if frozen_binding else None
            ),
        }
        dispatch_request = {
            "task_id": task_id,
            "task_name": f"official_factor_full_compute_{end_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "task_type": "official_factor_full_compute",
            "node_id": effective_node_id,
            "payload": payload,
            "all_duration": "72:00:00",
        }
        if frozen_binding is not None:
            dispatch_request["dataset_binding"] = frozen_binding
        created = asyncio.run(self._dispatch_service.create_and_submit_task(dispatch_request))
        return {
            "ok": created.get("status") != "failed",
            "status": created.get("status", "queued"),
            "task_id": created.get("task_id"),
            "dispatch_task_id": created.get("task_id"),
            "remote_task_id": created.get("remote_task_id"),
            "node_id": effective_node_id,
            "payload": payload,
            "cache_source": "official_offline_backtest_factor_data",
            "cache_root": "rdagent_assets/factor_values",
        }
