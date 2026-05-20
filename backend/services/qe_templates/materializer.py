"""Materialize QE execution templates through existing backend services."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from .repository import QETemplateRepository
from .runtime_diff import build_runtime_diff
from .validator import validate_template_payload


def _generate_single_experiment_through_existing_api(payload: Mapping[str, Any]) -> dict[str, Any]:
    from backend.routers.quantevolver import GenerateConfigRequest, generate_config

    return generate_config(GenerateConfigRequest(**dict(payload)))


class QETemplateMaterializer:
    def __init__(self, repository: QETemplateRepository | None = None) -> None:
        self._repository = repository or QETemplateRepository()

    async def materialize(self, template_id: str) -> dict[str, Any]:
        template = self._require_template(template_id)
        validation = validate_template_payload(template["template_kind"], template.get("config_json") or {})
        if not validation["valid"]:
            raise ValueError("template validation failed: " + "; ".join(validation["errors"]))
        if template["template_kind"] == "single_experiment":
            return self._materialize_single(template)
        return await self._materialize_custom_evo(template)

    def _materialize_single(self, template: Mapping[str, Any]) -> dict[str, Any]:
        config = dict(template.get("config_json") or {})
        custom_params = dict(config.get("custom_params") or {})
        custom_params["archive_policy"] = template.get("archive_policy") or "AUTO"
        if template.get("archive_reason"):
            custom_params["archive_reason"] = template.get("archive_reason")
        request_payload = {
            "factor_names": list(config.get("factor_names") or []),
            "factor_sources": config.get("factor_sources"),
            "model_id": config.get("model_id"),
            "strategy_id": config.get("strategy_id"),
            "data_split": config.get("data_split"),
            "custom_params": custom_params,
            "experiment_name": config.get("experiment_name") or template.get("title"),
            "dispatch_mode": config.get("dispatch_mode"),
            "evolution_params": config.get("evolution_params"),
            "unfilled_handler": config.get("unfilled_handler"),
            "unfilled_handler_params": config.get("unfilled_handler_params"),
        }
        try:
            result = _generate_single_experiment_through_existing_api(request_payload)
        except HTTPException as exc:
            raise ValueError(str(exc.detail)) from exc
        experiment_id = result.get("experiment_id")
        runtime_config = {
            **config,
            "custom_params": custom_params,
            "experiment_id": experiment_id,
            "generate_config_result": result,
        }
        row = self._repository.mark_materialized(
            str(template["template_id"]),
            experiment_id=experiment_id,
            runtime_config=runtime_config,
            diff=build_runtime_diff(config, runtime_config),
        )
        return {"template": row, "materialized": result}

    async def _materialize_custom_evo(self, template: Mapping[str, Any]) -> dict[str, Any]:
        from backend.routers.quantevolver_evolution import CustomEvoLoopConfig, _prepare_custom_evo_loop_configs, scheduler

        config = dict(template.get("config_json") or {})
        loop_models = [CustomEvoLoopConfig(**loop) for loop in config.get("loops") or []]
        loops_config, loop1_node_id, node_parallelism = await _prepare_custom_evo_loop_configs(
            loop_models,
            request_node_id=config.get("node_id"),
            node_parallelism_payload=config.get("node_parallelism"),
        )
        for cfg in loops_config:
            runtime_flags = dict(cfg.get("runtime_flags") or {})
            runtime_flags["archive_policy"] = template.get("archive_policy") or "AUTO"
            if template.get("archive_reason"):
                runtime_flags["archive_reason"] = template.get("archive_reason")
            cfg["runtime_flags"] = runtime_flags
        task_id = await scheduler.create_custom_evo_task(
            task_name=config.get("task_name") or template.get("title"),
            target_desc=config.get("target_desc") or template.get("description") or "",
            loops_config=loops_config,
            execution_mode=config.get("execution_mode") or "serial",
            node_id=loop1_node_id,
            node_parallelism=node_parallelism,
            engine_mode="unified",
            clone_from_task_id=config.get("clone_from_task_id"),
            auto_start=False,
        )
        runtime_config = {**config, "task_id": task_id, "loops": loops_config, "node_parallelism": node_parallelism}
        row = self._repository.mark_materialized(
            str(template["template_id"]),
            task_id=task_id,
            runtime_config=runtime_config,
            diff=build_runtime_diff(config, runtime_config),
        )
        return {"template": row, "materialized": {"task_id": task_id, "auto_start": False, "total_loops": len(loops_config)}}

    def _require_template(self, template_id: str) -> dict[str, Any]:
        template = self._repository.get(template_id)
        if not template:
            raise ValueError(f"template not found: {template_id}")
        if template.get("status") not in {"approved", "materialized"}:
            raise ValueError(f"template status does not allow materialize before approval: {template.get('status')}")
        return template
