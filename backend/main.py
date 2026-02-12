from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pathlib import Path
import builtins
import logging
import os
import sys
import signal
import faulthandler

from dotenv import load_dotenv

# 将项目根目录指向 AIstock 仓库根，便于导入顶层模块（如 pg_monitor_repo 等）
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env", override=True)

from .db.pg_pool import init_db_pool, close_db_pool
from .routers import (
    health,
    analysis,
    hotboard,
    watchlist,
    cloud_screening,
    monitor,
    qmt,
    portfolio,
    sector_strategy,
    model_scheduler,
    ingestion,
    quant,
    news,
    settings,
    config_env,
    smart_monitor,
    prompt_packs,
    strategies,
    rdagent,
    rdagent_catalog_admin,
    rdagent_sync_admin,
    rdagent_templates,
    rdagent_llm_config,
    rdagent_llm_config_v2,
    rdagent_llm_config_endpoints,
    tasks,
    stocks,
)
from .qlib_exporter.router import router as qlib_router
from .ingestion.tdx_scheduler import scheduler as ingestion_scheduler
from .schedulers.strategy_scheduler import scheduler as strategy_scheduler
from .infra.qmt_client import get_qmt_client_singleton


def _install_safe_print_and_logging() -> None:
    try:
        if getattr(sys.stdout, "closed", False):
            sys.stdout = sys.__stdout__
        if getattr(sys.stderr, "closed", False):
            sys.stderr = sys.__stderr__
    except Exception:
        pass

    try:
        logging.raiseExceptions = False
    except Exception:
        pass

    class SafeStreamHandler(logging.StreamHandler):
        def emit(self, record):  # type: ignore[no-untyped-def]
            try:
                return super().emit(record)
            except Exception:
                # Never let logging failures crash or spam the app.
                try:
                    self.stream = sys.__stderr__
                    return super().emit(record)
                except Exception:
                    return None

    try:
        if getattr(builtins, "_AISTOCK_ORIGINAL_PRINT", None) is None:
            builtins._AISTOCK_ORIGINAL_PRINT = builtins.print

        original_print = builtins._AISTOCK_ORIGINAL_PRINT

        def safe_print(*args, **kwargs):  # type: ignore[no-untyped-def]
            try:
                stream = kwargs.get("file", sys.stdout)
                if stream is None or getattr(stream, "closed", False):
                    raise ValueError("stream closed")
                return original_print(*args, **kwargs)
            except ValueError as e:
                if "closed file" not in str(e).lower() and "stream" not in str(e).lower():
                    raise
                try:
                    msg = " ".join(str(a) for a in args)
                    logging.getLogger("aistock.safe_print").info(msg)
                except Exception:
                    pass
                return None

        builtins.print = safe_print
    except Exception:
        pass

    try:
        for logger in (logging.getLogger(), logging.getLogger("uvicorn"), logging.getLogger("uvicorn.access"), logging.getLogger("uvicorn.error")):
            for h in list(getattr(logger, "handlers", []) or []):
                stream = getattr(h, "stream", None)
                if stream is not None and getattr(stream, "closed", False):
                    try:
                        h.stream = sys.__stderr__
                    except Exception:
                        pass
    except Exception:
        pass

    # Replace StreamHandlers with a safe variant that self-heals when streams are closed.
    try:
        for logger in (logging.getLogger(), logging.getLogger("uvicorn"), logging.getLogger("uvicorn.access"), logging.getLogger("uvicorn.error")):
            handlers = list(getattr(logger, "handlers", []) or [])
            if not handlers:
                continue
            for idx, h in enumerate(handlers):
                if isinstance(h, SafeStreamHandler):
                    continue
                if not isinstance(h, logging.StreamHandler):
                    continue
                new_h = SafeStreamHandler(stream=getattr(h, "stream", None) or sys.__stderr__)
                new_h.setLevel(h.level)
                new_h.setFormatter(h.formatter)
                # Preserve filters
                for f in getattr(h, "filters", []) or []:
                    new_h.addFilter(f)
                logger.removeHandler(h)
                logger.addHandler(new_h)
    except Exception:
        pass

    # Patch tqdm to avoid noisy __del__ crashes when half-initialized (often due to stream/tty issues).
    try:
        import tqdm.std as _tqdm_std

        if getattr(_tqdm_std.tqdm, "_AISTOCK_DEL_PATCHED", False) is False:
            _orig_del = getattr(_tqdm_std.tqdm, "__del__", None)

            def _safe_tqdm_del(self):  # type: ignore[no-untyped-def]
                try:
                    if _orig_del is not None:
                        _orig_del(self)
                except AttributeError:
                    return
                except Exception:
                    return

            _tqdm_std.tqdm.__del__ = _safe_tqdm_del
            _tqdm_std.tqdm._AISTOCK_DEL_PATCHED = True
    except Exception:
        pass


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
        """Initialize shared resources (DB pool, schedulers, QMT client)."""

        import logging

        _install_safe_print_and_logging()

        # Ensure app loggers are visible under uvicorn default logging config.
        try:
            uv_err = logging.getLogger("uvicorn.error")
            root = logging.getLogger()
            if not getattr(root, "handlers", None):
                for h in getattr(uv_err, "handlers", []) or []:
                    root.addHandler(h)
            if root.level > logging.INFO:
                root.setLevel(logging.INFO)

            for name in (
                "aistock",
                "aistock.inference",
                "aistock.rdagent_selection",
                "aistock.rdagent_router",
            ):
                lg = logging.getLogger(name)
                if lg.level > logging.INFO:
                    lg.setLevel(logging.INFO)
                lg.propagate = True
        except Exception:
            pass

        # 提高连接池上限以适配多路并发请求与后台任务
        # 同时将 minconn 提高，以减少请求高峰时频繁 _connect（psycopg2 建连在本环境下可达 2s+）
        init_db_pool(minconn=5, maxconn=40)

        if (os.getenv("DUMP_THREADS_ON_SIGNAL") or "").strip().lower() in {"1", "true", "yes", "y", "on"}:
            try:
                def _dump_threads(_signum, _frame):
                    try:
                        faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
                    except Exception:
                        pass

                signal.signal(signal.SIGINT, _dump_threads)
                signal.signal(signal.SIGTERM, _dump_threads)
            except Exception:
                pass

        # 尝试自动连接 QMT（不影响服务启动，失败仅记录告警日志）
        try:
            client = get_qmt_client_singleton()
            ok, msg = client.connect()
            # Use uvicorn.error logger so messages are visible under uvicorn default logging config.
            logger = logging.getLogger("uvicorn.error")
            if ok:
                logger.warning("QMT 自动连接成功: %s", msg)
            else:
                logger.warning("QMT 自动连接失败: %s", msg)
        except Exception as e:  # noqa: BLE001
            logging.getLogger("uvicorn.error").warning("QMT 自动连接异常: %s", e)

        disable_scheduler = (os.getenv("DISABLE_INGESTION_SCHEDULER") or "").strip().lower()
        enable_scheduler = (os.getenv("ENABLE_INGESTION_SCHEDULER") or "1").strip().lower()
        if disable_scheduler in {"1", "true", "yes", "y", "on"}:
            pass
        else:
            # REQ-SCHEDULER-P3-001: 默认开启数据调度器
            refresh_interval = int((os.getenv("AISTOCK_INGESTION_SCHEDULE_REFRESH_INTERVAL_SEC") or "30").strip() or "30")
            ingestion_scheduler.start(refresh_interval=refresh_interval)
            logging.getLogger("uvicorn.error").info("TDX 数据调度器已启动 (REQ-SCHEDULER-P3-001)")
        
        # 启动策略调度器
        disable_strategy_scheduler = (os.getenv("DISABLE_STRATEGY_SCHEDULER") or "").strip().lower()
        if disable_strategy_scheduler not in {"1", "true", "yes", "y", "on"}:
            strategy_scheduler.start()

    @app.on_event("shutdown")
    async def _on_shutdown() -> None:  # noqa: D401
        """Close PostgreSQL connection pool on application shutdown."""
        try:
            client = get_qmt_client_singleton()
            client.disconnect()
        except Exception:
            pass

        close_db_pool()
        ingestion_scheduler.shutdown(wait=False)
        strategy_scheduler.shutdown(wait=False)

    # 业务路由（版本化）
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(analysis.router, prefix="/api/v1")
    app.include_router(hotboard.router, prefix="/api/v1")
    app.include_router(watchlist.router, prefix="/api/v1")
    app.include_router(cloud_screening.router, prefix="/api/v1")
    app.include_router(monitor.router, prefix="/api/v1")
    app.include_router(qmt.router, prefix="/api/v1")
    app.include_router(strategies.router)
    app.include_router(portfolio.router, prefix="/api/v1")
    app.include_router(sector_strategy.router, prefix="/api/v1")
    app.include_router(model_scheduler.router, prefix="/api/v1")
    app.include_router(news.router, prefix="/api/v1")
    app.include_router(settings.router, prefix="/api/v1")
    app.include_router(config_env.router, prefix="/api/v1")
    app.include_router(smart_monitor.router, prefix="/api/v1")
    app.include_router(prompt_packs.router, prefix="/api/v1")
    app.include_router(rdagent.router, prefix="/api/v1")
    app.include_router(rdagent_templates.router, prefix="/api/v1")
    app.include_router(rdagent_catalog_admin.router, prefix="/api/v1")
    app.include_router(rdagent_llm_config.router, prefix="/api/v1")
    app.include_router(rdagent_llm_config_v2.router, prefix="/api/v1")
    app.include_router(rdagent_llm_config_endpoints.router, prefix="/api/v1")
    app.include_router(rdagent_sync_admin.router)
    app.include_router(qlib_router, prefix="")
    app.include_router(tasks.router, prefix="/api/v1")
    app.include_router(stocks.router, prefix="/api/v1")

    # ingestion / 本地数据管理接口：保持与旧 tdx_backend 相同的 /api/* 路径
    app.include_router(ingestion.router, prefix="")
    app.include_router(quant.router, prefix="")

    return app


app = create_app()
