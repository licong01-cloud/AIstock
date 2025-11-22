from .config import get_settings, Settings


def get_app_settings() -> Settings:
    """FastAPI 依赖项，用于注入全局配置。"""

    return get_settings()
