from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from ..config_manager_compat import config_manager


router = APIRouter(prefix="/config", tags=["config"])


@router.get("/env", summary="获取环境配置", response_model=Dict[str, Any])
def get_env_config() -> Dict[str, Any]:
    """Return configuration metadata and current values.

    The shape matches the legacy ConfigManager.get_config_info output:

    {KEY: {value, description, required, type, options?}}
    """

    return config_manager.get_config_info()


@router.post("/env", summary="保存环境配置")
def save_env_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and persist environment configuration.

    Expected payload shape: {KEY: VALUE, ...} or {"config": {KEY: VALUE}}.
    """

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid payload")
    if "config" in payload:
        data = payload["config"]
        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="Invalid payload")
    else:
        data = payload

    # Normalise all values to strings
    normalized: Dict[str, str] = {}
    for key, value in data.items():
        normalized_key = str(key)
        if value is None:
            normalized[normalized_key] = ""
        elif isinstance(value, bool):
            normalized[normalized_key] = "true" if value else "false"
        else:
            normalized[normalized_key] = str(value)

    ok, msg = config_manager.validate_config(normalized)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    try:
        saved = config_manager.write_env(normalized)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not saved:
        raise HTTPException(status_code=500, detail="保存配置失败，请检查服务器日志")

    try:
        config_manager.reload_config()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {"ok": True, "message": msg}
