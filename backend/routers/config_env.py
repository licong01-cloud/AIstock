from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from ..config_manager_compat import config_manager


router = APIRouter(prefix="/config", tags=["config"])


@router.get("/env", summary="获取环境配置", response_model=Dict[str, Any])
async def get_env_config() -> Dict[str, Any]:
    """Return configuration metadata and current values.

    The shape matches the legacy ConfigManager.get_config_info output:

    {KEY: {value, description, required, type, options?}}
    """

    return config_manager.get_config_info()


@router.post("/env", summary="保存环境配置")
async def save_env_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and persist environment configuration.

    Expected payload shape: {KEY: VALUE, ...} or {"config": {KEY: VALUE}}.
    """

    data = payload.get("config") if isinstance(payload.get("config"), dict) else payload
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Invalid payload")

    # Normalise all values to strings
    normalized: Dict[str, str] = {}
    for key, value in data.items():
        if value is None:
            normalized[key] = ""
        elif isinstance(value, bool):
            normalized[key] = "true" if value else "false"
        else:
            normalized[key] = str(value)

    ok, msg = config_manager.validate_config(normalized)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)

    if not config_manager.write_env(normalized):
        raise HTTPException(status_code=500, detail="保存配置失败，请检查服务器日志")

    config_manager.reload_config()

    return {"ok": True, "message": msg}
