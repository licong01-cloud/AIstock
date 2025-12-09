from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pathlib import Path
import sys

# 将项目根目录指向 AIstock 仓库根，便于导入顶层模块（如 pg_monitor_repo 等）
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from .db.pg_pool import init_db_pool, close_db_pool
from .routers import (
    health,
    analysis,
    hotboard,
    watchlist,
    indicator_screening,
    cloud_screening,
    monitor,
    portfolio,
    main_force,
    sector_strategy,
    longhubang,
    model_scheduler,
    ingestion,
    quant,
    news,
    settings,
    config_env,
    smart_monitor,
)
from .qlib_exporter.router import router as qlib_router
from .ingestion.tdx_scheduler import scheduler as ingestion_scheduler


def create_app() -> FastAPI:
    app = FastAPI(title="Aistock Next Backend", version="0.1.0")

    # 允许本地前端访问（含预检请求）
    origins = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.on_event("startup")
    async def _on_startup() -> None:  # noqa: D401
        """Initialize process-wide PostgreSQL connection pool."""

        init_db_pool(minconn=1, maxconn=10)
        ingestion_scheduler.start()

    @app.on_event("shutdown")
    async def _on_shutdown() -> None:  # noqa: D401
        """Close PostgreSQL connection pool on application shutdown."""

        close_db_pool()
        ingestion_scheduler.shutdown(wait=False)

    # 业务路由（版本化）
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(analysis.router, prefix="/api/v1")
    app.include_router(hotboard.router, prefix="/api/v1")
    app.include_router(watchlist.router, prefix="/api/v1")
    app.include_router(indicator_screening.router, prefix="/api/v1")
    app.include_router(cloud_screening.router, prefix="/api/v1")
    app.include_router(monitor.router, prefix="/api/v1")
    app.include_router(portfolio.router, prefix="/api/v1")
    app.include_router(main_force.router, prefix="/api/v1")
    app.include_router(sector_strategy.router, prefix="/api/v1")
    app.include_router(longhubang.router, prefix="/api/v1")
    app.include_router(model_scheduler.router, prefix="/api/v1")
    app.include_router(news.router, prefix="/api/v1")
    app.include_router(settings.router, prefix="/api/v1")
    app.include_router(config_env.router, prefix="/api/v1")
    app.include_router(smart_monitor.router, prefix="/api/v1")
    app.include_router(qlib_router, prefix="")

    # ingestion / 本地数据管理接口：保持与旧 tdx_backend 相同的 /api/* 路径
    app.include_router(ingestion.router, prefix="")
    app.include_router(quant.router, prefix="")

    return app


app = create_app()
