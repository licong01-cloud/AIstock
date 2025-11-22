from fastapi import APIRouter

from ..deps import get_app_settings


router = APIRouter(tags=["health"])


@router.get("/health", summary="健康检查")
async def health_check():
    """简单健康检查端点。

    返回应用名称与状态，便于前端和部署脚本探活。
    """

    settings = get_app_settings()
    return {"status": "ok", "app": settings.app_name}
