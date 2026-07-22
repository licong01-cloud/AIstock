"""
QE Unified Engine — BacktestExecutor

封装 ConfigComposer + QEWorkspaceClient 的两步调用，
替代当前 4 套重复的参数组装代码。
"""
from __future__ import annotations

import asyncio
import re
import logging
from pathlib import Path
from enum import Enum
from typing import Any

from ..execution_manifest import build_and_audit_execution_manifest
from ..experiment_config import ExperimentConfig, extract_qe_random_seed
from ..long_trend_evaluation_bundle import build_long_trend_evaluator_bundle
from ..long_trend_evaluation_contract import get_long_trend_profile
from ..qe_active_execution_capacity import (
    QEExecutionSourceClaimFactory,
    QEWorkspaceSubmissionCoordinator,
    QEWorkspaceSubmissionCoordinatorError,
    QEWorkspaceSubmissionPayload,
    QEWorkspaceSubmissionSource,
    qe_submission_owner_id,
    submission_intent_hash_for_source,
)
from ..runtime_contract import build_qe_minute_runtime_contract, merge_qe_minute_runtime_contract
from .base import BaseExecutor, ExecutionContext, ExecutionResult

logger = logging.getLogger("aistock.quantevolver.executors.backtest")
_PRECOMPUTED_HMM_COEFF_JSON_PARAM = "_precomputed_hmm_coefficients_json"


class BacktestMode(str, Enum):
    FULL_TRAIN = "full_train"        # 完整训练 + 回测（Path 1/2/4）
    BACKTEST_ONLY = "backtest_only"  # 复用已训练模型，仅回测（Path 3）


class BacktestExecutor(BaseExecutor):
    """
    RDAgent WSL 回测执行器。

    封装 ConfigComposer + QEWorkspaceClient 的两步调用。
    无状态设计，依赖通过构造函数注入，方便测试。
    """

    def __init__(
        self,
        composer: Any,
        client: Any,
        *,
        submission_coordinator: Any | None = None,
    ):
        """
        Args:
            composer: ConfigComposer 实例
            client:   QEWorkspaceClient 实例
        """
        self.composer = composer
        self.client = client
        self.submission_coordinator = submission_coordinator or QEWorkspaceSubmissionCoordinator()

    async def submit(
        self,
        config: ExperimentConfig,
        ctx: ExecutionContext,
        mode: BacktestMode = BacktestMode.FULL_TRAIN,
        **kwargs,
    ) -> ExecutionResult:
        """
        提交回测任务到 RDAgent。

        Args:
            config: 实验配置（配置层产出）
            ctx:    执行上下文（task_id, loop_index 等）
            mode:   FULL_TRAIN 或 BACKTEST_ONLY
        """
        if mode == BacktestMode.BACKTEST_ONLY and not ctx.model_source:
            raise ValueError(
                "BACKTEST_ONLY requires ctx.model_source; refusing to assume an "
                f"implicit workspace model for task={ctx.task_id} loop={ctx.loop_index}"
            )

        # 1. 构建 custom_params（配置层唯一注入点）
        custom_params = config.build_custom_params()
        runtime_flags = config.build_runtime_flags()
        fixed_seed = extract_qe_random_seed(runtime_flags)
        require_fixed_seed = bool(ctx.require_fixed_seed or kwargs.get("require_fixed_seed"))
        if mode == BacktestMode.FULL_TRAIN and require_fixed_seed and fixed_seed is None:
            raise ValueError(
                f"FULL_TRAIN requires runtime_flags.random_seed for task={ctx.task_id} loop={ctx.loop_index}"
            )
        if fixed_seed is not None:
            custom_params["random_seed"] = fixed_seed
        seed_ensemble = runtime_flags.get("ensemble") if isinstance(runtime_flags.get("ensemble"), dict) else None
        if seed_ensemble:
            custom_params["_seed_ensemble_config"] = seed_ensemble
        strategy_params = config.build_strategy_params()
        long_trend_descriptor: dict[str, Any] | None = None
        if config.long_trend_evaluation is not None:
            if not ctx.node_id:
                raise ValueError("QE long-trend normal postprocess requires an explicit node_id")
            if not all(
                (
                    ctx.resource_session_id,
                    ctx.resource_source_run_key,
                    ctx.resource_session_token,
                )
            ):
                raise ValueError(
                    "QE long-trend normal postprocess requires the parent Loop resource session identity and token"
                )
            environment = await self.client.get_execution_environment()
            feature_dataset = await self.client.get_dataset_identity(
                node_id=ctx.node_id,
                data_root_uri=config.long_trend_evaluation.feature_data_root_uri,
            )
            outcome_dataset = await self.client.get_dataset_identity(
                node_id=ctx.node_id,
                data_root_uri=config.long_trend_evaluation.outcome_data_root_uri,
            )
            bundle = build_long_trend_evaluator_bundle(
                repo_root=Path(__file__).resolve().parents[4],
                execution_environment={
                    "execution_environment_snapshot_id": environment.execution_environment_snapshot_id,
                    "execution_environment_manifest_sha256": environment.execution_environment_manifest_sha256,
                    "manifest": environment.manifest,
                },
            )
            profile = get_long_trend_profile(config.long_trend_evaluation.profile_id)
            long_trend_descriptor = {
                "schema_version": "qe_long_trend_postprocess_descriptor_v1",
                "task_id": ctx.task_id,
                "loop_index": int(ctx.loop_index),
                "node_id": ctx.node_id,
                "run_id": None,
                "backtest_freq": config.long_trend_evaluation.backtest_freq,
                "label_horizon": config.label_horizon,
                "strategy_topk": (
                    int(strategy_params["topk"])
                    if strategy_params and strategy_params.get("topk") is not None
                    else None
                ),
                "long_trend_evaluation": config.long_trend_evaluation.model_dump(mode="json"),
                "frozen_identity": {
                    "profile_sha256": profile.profile_sha256,
                    "evaluator_source_sha256": bundle.evaluator_source_sha256,
                    "bundle_sha256": bundle.bundle_sha256,
                    "execution_environment_snapshot_id": environment.execution_environment_snapshot_id,
                    "execution_environment_manifest_sha256": environment.execution_environment_manifest_sha256,
                    "feature_dataset": {
                        "complete": feature_dataset.complete,
                        "dataset": feature_dataset.dataset,
                        "long_trend_snapshot": feature_dataset.long_trend_snapshot,
                        "long_trend_snapshot_reason": feature_dataset.long_trend_snapshot_reason,
                    },
                    "outcome_dataset": {
                        "complete": outcome_dataset.complete,
                        "dataset": outcome_dataset.dataset,
                        "long_trend_snapshot": outcome_dataset.long_trend_snapshot,
                        "long_trend_snapshot_reason": outcome_dataset.long_trend_snapshot_reason,
                    },
                },
            }

        # 2. 调用 ConfigComposer（已有统一层，不改）
        loop = asyncio.get_running_loop()
        def compose_call() -> tuple[dict[str, Any], dict[str, Any] | None]:
            stock_pool_payload = None
            if ctx.node_id:
                from ..stock_pool_sync import prepare_stock_pool_loop_payload_for_compute_node_by_id

                stock_pool_payload = prepare_stock_pool_loop_payload_for_compute_node_by_id(
                    ctx.node_id,
                    custom_params.get("stock_pool"),
                )
            compose_res_local = self.composer.compose_experiment_in_memory(
                factor_names=config.factor_names,
                model_id=config.model_id,
                strategy_id=config.strategy_id,
                data_split=config.data_split,
                custom_params=custom_params,
                experiment_name=ctx.experiment_name,
                skip_db_save=True,
                execution_algo=config.execution_algo,
                execution_algo_params=config.execution_algo_params,
                strategy_params=strategy_params if strategy_params else None,
                node_id=ctx.node_id,
                callback_url=ctx.callback_url,
                task_id=ctx.task_id,
                loop_index=ctx.loop_index,
                resource_session_id=ctx.resource_session_id,
                resource_source_run_key=ctx.resource_source_run_key,
                resource_session_token=ctx.resource_session_token,
                phase_pipeline_enabled=ctx.phase_pipeline_enabled,
                long_trend_evaluation_descriptor=long_trend_descriptor,
            )
            return compose_res_local, stock_pool_payload

        compose_res, stock_pool_payload = await loop.run_in_executor(None, compose_call)

        experiment_files: dict[str, str] = dict(compose_res.get("experiment_files", {}) or {})
        if long_trend_descriptor is not None:
            normalized_descriptor = compose_res.get("long_trend_evaluation_descriptor")
            if not isinstance(normalized_descriptor, dict):
                raise ValueError("ConfigComposer did not return the frozen QE long-trend descriptor")
            long_trend_descriptor = normalized_descriptor
        wsl_command: str = compose_res.get("wsl_command", "")
        if not wsl_command:
            raise ValueError(
                f"compose_experiment_in_memory returned empty wsl_command for "
                f"task={ctx.task_id} loop={ctx.loop_index}"
            )

        if stock_pool_payload:
            from ..stock_pool_sync import inject_stock_pool_install_command

            stock_pool_files = stock_pool_payload.get("experiment_files") or {}
            duplicate_keys = set(experiment_files).intersection(stock_pool_files)
            if duplicate_keys:
                raise ValueError(
                    "stock_pool files would overwrite generated files: "
                    f"{sorted(duplicate_keys)}"
                )
            experiment_files.update(stock_pool_files)
            wsl_command = inject_stock_pool_install_command(
                wsl_command,
                stock_pool_payload.get("install_command"),
            )

        if ctx.extra_experiment_files:
            duplicate_keys = set(experiment_files).intersection(ctx.extra_experiment_files)
            if duplicate_keys:
                raise ValueError(
                    "extra_experiment_files would overwrite generated files: "
                    f"{sorted(duplicate_keys)}"
                )
            experiment_files.update(ctx.extra_experiment_files)

        # 3. 注入 --backtest-only（Path 3 专用）
        if mode == BacktestMode.BACKTEST_ONLY:
            wsl_command = re.sub(
                r"(python\s+qrun_limit_minute\.py\s+\S+\.yaml)",
                r"\1 --backtest-only",
                wsl_command,
            )

        execution_manifest, execution_manifest_sha256 = build_and_audit_execution_manifest(
            config=config,
            ctx=ctx,
            mode=mode.value if isinstance(mode, BacktestMode) else str(mode),
            experiment_files=experiment_files,
            wsl_command=wsl_command,
        )

        # 4. 构建传给 RDAgent 的 config 记录
        persisted_model_params = {
            k: v for k, v in custom_params.items()
            if k not in {_PRECOMPUTED_HMM_COEFF_JSON_PARAM, "_seed_ensemble_config"}
        }
        if fixed_seed is not None:
            persisted_model_params.setdefault("random_seed", fixed_seed)
            persisted_model_params.setdefault("seed", fixed_seed)
            persisted_model_params.setdefault("random_state", fixed_seed)
        persisted_model_params = merge_qe_minute_runtime_contract(
            persisted_model_params,
            execution_algo=config.execution_algo,
            execution_algo_params=config.execution_algo_params,
            source="backtest_executor_model_params",
            allow_default_execution_algo=True,
        )
        runtime_contract = build_qe_minute_runtime_contract(
            custom_params=persisted_model_params,
            execution_algo=config.execution_algo,
            execution_algo_params=config.execution_algo_params,
            source="backtest_executor_config",
            allow_default_execution_algo=True,
        )
        rdagent_config = {
            "factor_list": config.factor_names,
            "model_id": config.model_id,
            "strategy_id": config.strategy_id,
            "data_split": config.data_split,
            "model_params": persisted_model_params,
            "runtime_flags": runtime_flags,
            "execution_manifest": execution_manifest,
            "execution_manifest_sha256": execution_manifest_sha256,
        }
        if fixed_seed is not None:
            rdagent_config["random_seed"] = fixed_seed
        if runtime_contract:
            rdagent_config.update(runtime_contract)
        if long_trend_descriptor is not None:
            rdagent_config["long_trend_evaluation"] = long_trend_descriptor

        # 5. Reserve the canonical cross-source slot before the QE Workspace POST.
        source = self._submission_source_for_context(ctx)
        submission_outcome = await self.submission_coordinator.submit(
            client=self.client,
            source=source,
            payload=QEWorkspaceSubmissionPayload(
                task_id=ctx.task_id,
                loop_index=ctx.loop_index,
                config=rdagent_config,
                experiment_files=experiment_files,
                wsl_command=wsl_command,
                model_source=ctx.model_source,
                callback_url=ctx.callback_url,
                postprocess_descriptor=long_trend_descriptor,
            ),
        )

        returned_experiment_files = dict(experiment_files)
        if "qe_resource_session_secret.json" in returned_experiment_files:
            returned_experiment_files["qe_resource_session_secret.json"] = "<redacted>"

        return ExecutionResult(
            job_id=submission_outcome.loop_id,
            status=submission_outcome.state,
            experiment_files=returned_experiment_files,
            wsl_command=wsl_command,
            detail={
                "execution_manifest": execution_manifest,
                "execution_manifest_sha256": execution_manifest_sha256,
                "qe_submission": {
                    "state": submission_outcome.state,
                    "reservation_id": submission_outcome.reservation_id,
                    "reservation_status": submission_outcome.reservation_status,
                    "remote_status": submission_outcome.remote_status,
                    "active_count": submission_outcome.active_count,
                    "node_capacity": submission_outcome.node_capacity,
                    "duplicate_replay": submission_outcome.duplicate_replay,
                    "remote_acceptance_unknown": submission_outcome.remote_acceptance_unknown,
                    "detail": dict(submission_outcome.detail or {}),
                },
            },
        )

    @staticmethod
    def _submission_source_for_context(ctx: ExecutionContext) -> QEWorkspaceSubmissionSource:
        node_id = str(ctx.node_id or "").strip()
        source_kind = str(ctx.submission_source_kind or "").strip()
        source_execution_id = str(ctx.submission_source_execution_id or "").strip()
        if not node_id or not source_kind or not source_execution_id:
            raise QEWorkspaceSubmissionCoordinatorError(
                "BacktestExecutor requires explicit node and source execution identity",
                reason_code="qe_execution_source_identity_missing",
                context={
                    "task_id": ctx.task_id,
                    "loop_index": ctx.loop_index,
                    "node_id": node_id or None,
                    "source_kind": source_kind or None,
                    "source_execution_id": source_execution_id or None,
                },
            )
        loop_id = f"Loop{ctx.loop_index}"
        if source_kind == "qe_evolution_loop":
            claim_source, record_waiting = QEExecutionSourceClaimFactory.evolution_loop(
                loop_id=source_execution_id,
                node_id=node_id,
            )
        elif source_kind == "qe_experiment":
            claim_source, record_waiting = QEExecutionSourceClaimFactory.experiment(
                experiment_id=source_execution_id,
                node_id=node_id,
                qe_task_id=ctx.task_id,
                qe_loop_id=loop_id,
            )
        else:
            raise QEWorkspaceSubmissionCoordinatorError(
                "BacktestExecutor source kind is not supported by its source claim adapter",
                reason_code="qe_execution_source_kind_unsupported",
                context={"source_kind": source_kind},
            )
        return QEWorkspaceSubmissionSource(
            source_kind=source_kind,
            source_execution_id=source_execution_id,
            node_id=node_id,
            submission_intent_hash=submission_intent_hash_for_source(
                source_kind=source_kind,
                source_execution_id=source_execution_id,
                node_id=node_id,
                task_id=ctx.task_id,
                loop_id=loop_id,
            ),
            owner_id=qe_submission_owner_id(),
            claim_source=claim_source,
            record_waiting_capacity=record_waiting,
            requested_node_capacity=ctx.submission_node_capacity,
        )
