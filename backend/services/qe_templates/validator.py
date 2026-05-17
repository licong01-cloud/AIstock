"""Side-effect-free validation for QE execution templates."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def validate_template_payload(template_kind: str, config_json: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    config = dict(config_json or {})
    if template_kind == "single_experiment":
        if config.get("alpha_mode") == "multi":
            errors.append("QE MCP v1 does not support multi-alpha experiment templates")
        if not config.get("factor_names"):
            errors.append("single_experiment config requires factor_names")
        if not config.get("model_id"):
            errors.append("single_experiment config requires model_id")
    elif template_kind == "custom_evo":
        loops = config.get("loops")
        if not isinstance(loops, list) or not loops:
            errors.append("custom_evo config requires non-empty loops")
        else:
            for idx, loop in enumerate(loops, start=1):
                if not isinstance(loop, Mapping):
                    errors.append(f"Loop {idx} must be an object")
                    continue
                if not loop.get("factor_keys"):
                    errors.append(f"Loop {idx} requires factor_keys")
                if not loop.get("model_id"):
                    errors.append(f"Loop {idx} requires model_id")
                node_id = str(loop.get("node_id") or config.get("node_id") or "")
                model_id = str(loop.get("model_id") or "").lower()
                if node_id and node_id not in {"local", "wsl", "wsl2-5080"} and any(token in model_id for token in ("lstm", "gru", "transformer", "alstm")):
                    warnings.append(f"Loop {idx}: remote node should run CPU model only; treat this as a soft limit")
    else:
        errors.append(f"unsupported template_kind: {template_kind}")
    return {"valid": not errors, "errors": errors, "warnings": warnings}
