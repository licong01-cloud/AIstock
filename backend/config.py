from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """全局配置，后续可扩展数据库、鉴权等设置。

    默认端口仅用于文档说明，实际端口由 uvicorn 启动参数决定。
    """

    app_name: str = Field("Aistock Next Backend", description="应用名称")
    api_v1_prefix: str = "/api/v1"

    # 新后端建议端口（示例）：实际以启动命令为准
    host: str = "127.0.0.1"
    port: int = 8001

    # Pydantic v2 风格配置
    model_config = SettingsConfigDict(
        env_prefix="NEXT_APP_",
        env_file=".env",
        env_file_encoding="utf-8",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
