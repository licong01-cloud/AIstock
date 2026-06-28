# ruff: noqa: E402

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pathlib import Path
import builtins
import asyncio
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
    analysis,
    cloud_screening,
    config_env,
    execution_policy,
    external_research,
    strategy_governance,
    model_registry,
    factor_correlation,
    factor_metrics,
    factor_library,
    health,
    ingestion,
    local_data,
    monitor,
    news,
    portfolio,
    qmt,
    qmt_strategy_ledger,
    qe_archive,
    multi_alpha,
    prediction_store,
    qe_templates,
    research_assistant,
    research_pipeline,
    quant,
    sector_strategy,
    settings,
    smart_monitor,
    watchlist,
    rdagent_templates,
    rdagent_sync_admin,
    tasks,
    stocks,
    stock_universe,
    quantevolver,
    quantevolver_evolution,
    strategies,
    strategy_packages,
    advisory,
    selection_center,
    paper_trading_v2,
    trading_calendar,
    validation,
    prometheus_admin,
    rdagent,
    rdagent_catalog_admin,
    rdagent_llm_config,
    rdagent_llm_config_v2,
    rdagent_llm_config_endpoints,
    dispatch,
    hmm_training,
    tdx_blocks,
)
from .routers import llm_config, simulation_runtime
try:
    from .routers import rl_execution
except ImportError as _rl_execution_import_exc:
    # Defense-layer fallback per T4 audit (drawer 5888d73fb9882664d531760e):
    # if backend.services.rl_execution module is missing for any reason
    # (e.g. .gitignore masking, partial clone), backend should still start
    # without the /api/v1/rl-execution endpoints rather than crashing.
    rl_execution = None
    logging.getLogger(__name__).warning(
        "rl_execution router unavailable: %s; backend starting without /api/v1/rl-execution endpoints",
        _rl_execution_import_exc,
    )
from .routers import market_regime
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
                    self.handleError(record)
                    return

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


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Lifespan context manager — startup runs before yield, shutdown after."""
    # ── STARTUP ──
    _install_safe_print_and_logging()

    # 文件日志：aistock.log（INFO+）和 errors.log（ERROR+）
    try:
        from logging.handlers import RotatingFileHandler
        _log_dir = Path(__file__).parent / "logs"
        _log_dir.mkdir(exist_ok=True)
        _fmt = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        _fh = RotatingFileHandler(
            _log_dir / "aistock.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        _fh.setLevel(logging.INFO)
        _fh.setFormatter(_fmt)
        _eh = RotatingFileHandler(
            _log_dir / "errors.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        _eh.setLevel(logging.ERROR)
        _eh.setFormatter(_fmt)
        logging.getLogger().addHandler(_fh)
        logging.getLogger().addHandler(_eh)
    except Exception:
        pass

    try:
        uv_err = logging.getLogger("uvicorn.error")
        root = logging.getLogger()
        if not getattr(root, "handlers", None):
            for h in getattr(uv_err, "handlers", []) or []:
                root.addHandler(h)
        if root.level > logging.INFO:
            root.setLevel(logging.INFO)
        for name in (
            "aistock", "aistock.inference",
            "aistock.rdagent_selection", "aistock.rdagent_router",
        ):
            lg = logging.getLogger(name)
            if lg.level > logging.INFO:
                lg.setLevel(logging.INFO)
            lg.propagate = True
    except Exception:
        pass

    init_db_pool(minconn=5, maxconn=40)
    _configure_external_research_provider()

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

    try:
        client = get_qmt_client_singleton()
        ok, msg = client.connect()
        _logger = logging.getLogger("uvicorn.error")
        if ok:
            _logger.warning("QMT 自动连接成功: %s", msg)
        else:
            _logger.warning("QMT 自动连接失败: %s", msg)
    except Exception as e:
        logging.getLogger("uvicorn.error").warning("QMT 自动连接异常: %s", e)

    disable_scheduler = (os.getenv("DISABLE_INGESTION_SCHEDULER") or "").strip().lower()
    if disable_scheduler not in {"1", "true", "yes", "y", "on"}:
        refresh_interval = int((os.getenv("AISTOCK_INGESTION_SCHEDULE_REFRESH_INTERVAL_SEC") or "30").strip() or "30")
        ingestion_scheduler.start(refresh_interval=refresh_interval)

    disable_strategy_scheduler = (os.getenv("DISABLE_STRATEGY_SCHEDULER") or "").strip().lower()
    if disable_strategy_scheduler not in {"1", "true", "yes", "y", "on"}:
        strategy_scheduler.start()


    # Paper Trading v2 session scheduler is opt-in so development ports do not
    # accidentally advance durable v2 sessions while production 8001 is running.
    enable_pt_v2 = (os.getenv("ENABLE_PAPER_TRADING_V2_SCHEDULER") or "").strip().lower()
    logging.getLogger("uvicorn.error").info(
        "Paper Trading v2 scheduler autostart=%s interval=%s auto_run=%s",
        enable_pt_v2 in {"1", "true", "yes", "y", "on"},
        os.getenv("PAPER_TRADING_V2_SCHEDULER_INTERVAL_SEC") or "30",
        os.getenv("PAPER_V2_AUTO_RUN_ENABLED") or "true",
    )
    if enable_pt_v2 in {"1", "true", "yes", "y", "on"}:
        from .services.paper_trading_v2.scheduler import paper_trading_v2_scheduler
        paper_trading_v2_scheduler.start()

    # Unified LocalSim/MiniQMT simulation lifecycle scheduler is opt-in. It
    # follows the committed simulation-runtime path and never starts by default.
    enable_sim_runtime = (os.getenv("ENABLE_SIMULATION_RUNTIME_SCHEDULER") or "").strip().lower()
    if enable_sim_runtime in {"1", "true", "yes", "y", "on"}:
        from .services.simulation_runtime import simulation_lifecycle_background_scheduler
        simulation_lifecycle_background_scheduler.start()

    # 相关性计算调度器 — 默认禁用，仅在显式开启时启动
    # 不允许后台自动计算，必须由用户在页面主动触发
    enable_corr_scheduler = (os.getenv("ENABLE_CORRELATION_SCHEDULER") or "").strip().lower()
    if enable_corr_scheduler in {"1", "true", "yes", "y", "on"}:
        from .services.quantevolver.correlation_scheduler import correlation_scheduler
        correlation_scheduler.start(refresh_interval=60)
        logging.getLogger("uvicorn.error").info("相关性计算调度器已启动")

    # 因子独立指标计算调度器
    enable_fm_scheduler = (os.getenv("ENABLE_FACTOR_METRICS_SCHEDULER") or "").strip().lower()
    if enable_fm_scheduler in {"1", "true", "yes", "y", "on"}:
        from .services.quantevolver.factor_metrics_scheduler import factor_metrics_scheduler
        factor_metrics_scheduler.start(refresh_interval=60)
        logging.getLogger("uvicorn.error").info("因子指标计算调度器已启动")

    # 节点健康调度器
    disable_node_health = (os.getenv("DISABLE_NODE_HEALTH_SCHEDULER") or "").strip().lower()
    if disable_node_health not in {"1", "true", "yes", "y", "on"}:
        from .schedulers.node_health_scheduler import node_health_scheduler
        node_health_scheduler.start(loop=asyncio.get_running_loop())
        logging.getLogger("uvicorn.error").info("节点健康调度器已启动")

    # HMM 滚动训练调度器
    disable_hmm_scheduler = (os.getenv("DISABLE_HMM_SCHEDULER") or "").strip().lower()
    if disable_hmm_scheduler not in {"1", "true", "yes", "y", "on"}:
        from .routers.hmm_training import init_hmm_scheduler
        init_hmm_scheduler()

    # Evolution loop timer scanner (fallback for webhook-based flow)
    shutdown_event = asyncio.Event()
    scan_task = None
    disable_evo_scanner = (os.getenv("DISABLE_EVOLUTION_SCANNER") or "").strip().lower()
    if disable_evo_scanner not in {"1", "true", "yes", "y", "on"}:
        async def _timer_scan_loop(stop_event: asyncio.Event):
            from .services.quantevolver.qe_evolution_service import AutoEvolutionScheduler
            scanner = AutoEvolutionScheduler()
            scan_interval = int((os.getenv("QE_EVOLUTION_SCAN_INTERVAL_SEC") or "60").strip() or "60")
            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=scan_interval)
                    break  # stop_event was set
                except asyncio.TimeoutError:
                    pass  # Normal timeout, run scan
                try:
                    await scanner.scan_running_loops()
                except Exception as e:
                    logging.getLogger("aistock.evolution_scanner").warning(f"Evolution scan error: {e}")

        scan_task = asyncio.create_task(_timer_scan_loop(shutdown_event))

    # One-off QE experiment scanner. This covers single-alpha and Multi-Alpha
    # experiments whose browser/SSE session or RD-Agent callback did not update DB.
    qe_exp_scan_task = None
    disable_qe_exp_scanner = (os.getenv("DISABLE_QE_EXPERIMENT_SCANNER") or "").strip().lower()
    if disable_qe_exp_scanner not in {"1", "true", "yes", "y", "on"}:
        async def _qe_experiment_scan_loop(stop_event: asyncio.Event):
            from .services.quantevolver.qe_experiment_status_scanner import QEExperimentStatusScanner
            scan_interval = int((os.getenv("QE_EXPERIMENT_SCAN_INTERVAL_SEC") or "30").strip() or "30")
            batch_size = int((os.getenv("QE_EXPERIMENT_SCAN_BATCH_SIZE") or "50").strip() or "50")
            scanner = QEExperimentStatusScanner(batch_size=batch_size)
            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=scan_interval)
                    break
                except asyncio.TimeoutError:
                    pass
                try:
                    stats = await scanner.scan_once()
                    if stats.get("checked") or stats.get("synced_terminal") or stats.get("errors"):
                        logging.getLogger("aistock.qe_experiment_scanner").info(
                            "QE experiment scan stats: %s", stats
                        )
                except Exception as e:
                    logging.getLogger("aistock.qe_experiment_scanner").warning(
                        "QE experiment scan error: %s", e, exc_info=True
                    )

        qe_exp_scan_task = asyncio.create_task(_qe_experiment_scan_loop(shutdown_event))

    qe_archive_worker_task = None
    try:
        from .services.qe_archive.worker_loop import autostart_enabled, run_archive_worker_loop

        if autostart_enabled():
            qe_archive_worker_task = asyncio.create_task(run_archive_worker_loop(shutdown_event))
            logging.getLogger("aistock.qe_archive.worker_loop").info("QE archive worker autostart enabled")
    except Exception as e:
        logging.getLogger("aistock.qe_archive.worker_loop").warning(
            "QE archive worker autostart setup failed: %s", e, exc_info=True
        )

    try:
        yield  # ── 应用运行中 ──
    except asyncio.CancelledError:
        pass  # uvicorn reload 发送的取消信号，正常处理
    finally:
        # ── SHUTDOWN ──
        shutdown_event.set()
        if scan_task is not None:
            scan_task.cancel()
            try:
                await scan_task
            except (asyncio.CancelledError, Exception):
                pass
        if qe_exp_scan_task is not None:
            qe_exp_scan_task.cancel()
            try:
                await qe_exp_scan_task
            except (asyncio.CancelledError, Exception):
                pass
        if qe_archive_worker_task is not None:
            qe_archive_worker_task.cancel()
            try:
                await qe_archive_worker_task
            except (asyncio.CancelledError, Exception):
                pass
        # ── 先停所有后台线程（它们可能持有 DB 连接）──
        try:
            ingestion_scheduler.shutdown(wait=False)
        except Exception:
            pass
        try:
            strategy_scheduler.shutdown(wait=False)
        except Exception:
            pass
        try:
            from .services.quantevolver.correlation_scheduler import correlation_scheduler
            correlation_scheduler.shutdown(wait=False)
        except Exception:
            pass
        try:
            from .services.quantevolver.factor_metrics_scheduler import factor_metrics_scheduler
            factor_metrics_scheduler.shutdown(wait=False)
        except Exception:
            pass
        try:
            from .schedulers.node_health_scheduler import node_health_scheduler
            node_health_scheduler.shutdown()
        except Exception:
            pass
        try:
            from .routers.hmm_training import shutdown_hmm_scheduler
            shutdown_hmm_scheduler()
        except Exception:
            pass
        try:
            from .services.paper_trading_v2.scheduler import paper_trading_v2_scheduler
            paper_trading_v2_scheduler.shutdown(wait=False)
        except Exception:
            pass
        try:
            from .services.simulation_runtime import simulation_lifecycle_background_scheduler
            simulation_lifecycle_background_scheduler.shutdown(wait=False)
        except Exception:
            pass
        # ── 后台线程已停，再关闭 DB 连接池和外部连接 ──
        try:
            client = get_qmt_client_singleton()
            client.disconnect()
        except Exception:
            pass
        try:
            close_db_pool()
        except Exception:
            pass


def create_app() -> FastAPI:
    app = FastAPI(title="Aistock Next Backend", version="0.1.0", lifespan=_lifespan)

    # 允许本地前端访问（含预检请求）
    origins = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3011",
        "http://127.0.0.1:3011",
        "http://localhost:3012",
        "http://127.0.0.1:3012",
    ]
    extra_origins = [
        origin.strip()
        for origin in os.getenv("AISTOCK_CORS_ORIGINS", "").split(",")
        if origin.strip()
    ]
    origins.extend(origin for origin in extra_origins if origin not in origins)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # 业务路由（版本化）
    app.include_router(health.router, prefix="/api/v1")
    app.include_router(analysis.router, prefix="/api/v1")
    app.include_router(watchlist.router, prefix="/api/v1")
    app.include_router(cloud_screening.router, prefix="/api/v1")
    app.include_router(monitor.router, prefix="/api/v1")
    app.include_router(qmt.router, prefix="/api/v1")
    app.include_router(qmt_strategy_ledger.router, prefix="/api/v1")
    app.include_router(strategies.router)
    app.include_router(portfolio.router, prefix="/api/v1")
    app.include_router(sector_strategy.router, prefix="/api/v1")
    app.include_router(news.router, prefix="/api/v1")
    app.include_router(settings.router, prefix="/api/v1")
    app.include_router(config_env.router, prefix="/api/v1")
    app.include_router(smart_monitor.router, prefix="/api/v1")
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
    app.include_router(stock_universe.router, prefix="/api/v1")
    app.include_router(quantevolver.router, prefix="/api/v1")
    app.include_router(quantevolver_evolution.router, prefix="/api/v1")
    app.include_router(quantevolver_evolution.factor_metrics_router, prefix="/api/v1")
    app.include_router(factor_library.router, prefix="/api/v1")
    app.include_router(factor_metrics.router, prefix="/api/v1")
    app.include_router(factor_correlation.router, prefix="/api/v1")
    app.include_router(model_registry.router, prefix="/api/v1")
    app.include_router(strategy_governance.router, prefix="/api/v1")
    app.include_router(execution_policy.router, prefix="/api/v1")
    app.include_router(external_research.router, prefix="/api/v1")
    app.include_router(qe_archive.router, prefix="/api/v1")
    app.include_router(multi_alpha.router, prefix="/api/v1")
    app.include_router(prediction_store.router, prefix="/api/v1")
    app.include_router(qe_templates.router, prefix="/api/v1")
    app.include_router(research_assistant.router, prefix="/api/v1")
    app.include_router(research_pipeline.router, prefix="/api/v1")
    app.include_router(strategy_packages.router, prefix="/api/v1")
    app.include_router(advisory.router, prefix="/api/v1")
    app.include_router(selection_center.router, prefix="/api/v1")
    app.include_router(paper_trading_v2.router, prefix="/api/v1")
    app.include_router(trading_calendar.router, prefix="/api/v1")
    app.include_router(simulation_runtime.router, prefix="/api/v1")
    app.include_router(validation.router, prefix="/api/v1")
    app.include_router(prometheus_admin.router, prefix="/api/v1")
    app.include_router(hmm_training.router, prefix="/api/v1")
    app.include_router(tdx_blocks.router, prefix="/api/v1")
    app.include_router(llm_config.router)
    app.include_router(dispatch.router, prefix="/api/v1")
    if rl_execution is not None:
        app.include_router(rl_execution.router, prefix="/api/v1")
    app.include_router(market_regime.router, prefix="/api/v1")
    app.include_router(local_data.router, prefix="/api/v1")

    # ingestion / 本地数据管理接口：保持与旧 tdx_backend 相同的 /api/* 路径
    app.include_router(ingestion.router, prefix="")
    app.include_router(quant.router, prefix="")

    return app


def _configure_external_research_provider() -> None:
    mode = (os.getenv("RA_EXTERNAL_RESEARCH_PROVIDER") or "offline").strip().lower()
    if mode in {"", "offline", "deterministic"}:
        logging.getLogger("uvicorn.error").info("RA external research provider remains offline deterministic provider")
        return
    if mode != "real":
        logging.getLogger("uvicorn.error").warning(
            "Unsupported RA_EXTERNAL_RESEARCH_PROVIDER=%s; reason_code=RA_EXTERNAL_RESEARCH_PROVIDER_UNSUPPORTED; keeping offline provider",
            mode,
        )
        return
    base_url = (os.getenv("RA_AGENTSEARCH_BASE_URL") or "").strip()
    if not base_url:
        logging.getLogger("uvicorn.error").warning(
            "RA_EXTERNAL_RESEARCH_PROVIDER=real without RA_AGENTSEARCH_BASE_URL; "
            "reason_code=RA_AGENTSEARCH_BASE_URL_MISSING; keeping offline provider"
        )
        return
    try:
        from backend.routers.external_research import set_external_research_provider
        from backend.services.research_assistant.real_external_research_provider import RealExternalResearchProvider

        set_external_research_provider(RealExternalResearchProvider.from_env())
    except Exception as exc:  # noqa: BLE001 - startup must be loud but keep offline provider as configured fallback.
        logging.getLogger("uvicorn.error").warning(
            "Failed to configure real RA external research provider; reason_code=RA_EXTERNAL_RESEARCH_PROVIDER_INIT_FAILED; error=%s",
            exc,
        )
        return
    logging.getLogger("uvicorn.error").info("RA external research provider configured as real provider")


app = create_app()
