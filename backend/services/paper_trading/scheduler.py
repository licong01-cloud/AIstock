"""实盘演练定时调度器（v4 §6.5）— 支持收盘价 / 日内实时 / 日内回放三种模式.

每个交易日 15:35 对每个 running 状态的模拟盘执行：
  Phase 1: T 日成交记录
  Phase 2: T 日信号生成（为 T+1 准备）
  Phase 3: 实盘数据收集（IC / 归因 / 模型追踪 / 个股盈亏）

日内实时模式 (exec_mode='live'):
  09:25 初始化 ExecutionEngine + 分钟级 schedule
  每 N 分钟 step() — 使用 kline_minute_raw 最新 bar
  15:05 清理日内 schedule jobs
  15:35 finalize

日内回放模式 (exec_mode='replay'):
  15:35 获取当日全部分钟线 → 逐 bar 回放 ExecutionEngine → finalize
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import date, datetime, time as dt_time
from typing import Dict, Optional

import schedule

from ...db.pg_pool import get_conn
from .minute_data_provider import MinuteDataProvider

logger = logging.getLogger("aistock.paper_trading.scheduler")

# A 股交易时段
MORNING_OPEN = dt_time(9, 30)
MORNING_CLOSE = dt_time(11, 30)
AFTERNOON_OPEN = dt_time(13, 0)
AFTERNOON_CLOSE = dt_time(15, 0)


def _in_trading_hours() -> bool:
    """判断当前是否在 A 股连续竞价时段."""
    now = datetime.now().time()
    return (MORNING_OPEN <= now <= MORNING_CLOSE) or (AFTERNOON_OPEN <= now <= AFTERNOON_CLOSE)


class PaperTradingScheduler:

    def __init__(self) -> None:
        self._scheduler = schedule.Scheduler()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        # 日内引擎 {portfolio_id: ExecutionEngine}
        self._intraday_engines: Dict[int, "ExecutionEngine"] = {}
        # 日内 schedule tags
        self._intraday_tags: list = []
        # 正在执行追赶的 portfolio_id 集合（防止并发竞争）
        self._active_catchups: set = set()

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            run_time = os.getenv("PAPER_TRADING_SCHEDULE_TIME", "15:35")
            self._scheduler.every().day.at(run_time).do(self._daily_job)
            # 日内调度：每个交易日 09:25 初始化
            self._scheduler.every().day.at("09:25").do(self._setup_intraday_schedules)
            # 日内清理：15:05
            self._scheduler.every().day.at("15:05").do(self._cleanup_intraday)
            self._thread = threading.Thread(
                target=self._run_loop, name="paper-trading-scheduler", daemon=True,
            )
            self._thread.start()
            logger.info("实盘演练调度器已启动 (每日 %s, 日内调度 09:25-15:05)", run_time)

    def shutdown(self, wait: bool = False) -> None:
        with self._lock:
            self._stop_event.set()
            if self._thread and self._thread.is_alive() and wait:
                self._thread.join(timeout=5.0)
            logger.info("实盘演练调度器已关闭")

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._scheduler.run_pending()
            time.sleep(1)

    def _daily_job(self) -> None:
        """每日定时任务."""
        today = date.today()

        if not self._is_trading_day(today):
            logger.info("非交易日，跳过: %s", today)
            return

        # 延迟导入避免循环依赖
        from .portfolio_manager import PortfolioManager
        from .trade_executor import TradeExecutor
        from .signal_generator import SignalGenerator
        from .live_ic_tracker import LiveICTracker
        from .factor_attribution import FactorAttribution
        from .stock_pnl_tracker import StockPnLTracker

        # 追赶模式的组合：继续追赶直到 caught_up（跳过已在执行中的）
        catching_up = PortfolioManager.get_catchup_portfolios()
        for portfolio in catching_up:
            pid_cu = portfolio["id"]
            if pid_cu in self._active_catchups:
                logger.info("追赶已在后台执行中，跳过: portfolio=%s", pid_cu)
                continue
            try:
                self._continue_catchup(pid_cu)
            except Exception as e:
                logger.error("追赶任务失败 portfolio=%s: %s", pid_cu, e, exc_info=True)

        running = PortfolioManager.get_running_portfolios()
        if not running:
            logger.info("无运行中的模拟盘")
            return

        logger.info("开始每日任务: %s, 运行中模拟盘=%d", today, len(running))

        for portfolio in running:
            pid = portfolio["id"]
            is_first_day = self._is_first_day(pid, today)
            enable_intraday = bool(portfolio.get("enable_intraday"))
            strategy = (portfolio.get("intraday_strategy") or "CLOSE_PRICE").upper()
            exec_mode = (portfolio.get("intraday_exec_mode") or "replay").lower()

            try:
                # Phase 1: T 日成交记录（首日跳过）
                if not is_first_day:
                    if not enable_intraday:
                        # Mode A：收盘价模式 — 使用 TradeExecutor 直接执行
                        logger.info("Phase 1 [CLOSE_PRICE] 执行成交: portfolio=%s date=%s", pid, today)
                        TradeExecutor.execute_trades(pid, today)
                    elif exec_mode == "live":
                        # Mode B-live：日内实时模式 — 由 09:25 初始化的 ExecutionEngine 已执行，此处 finalize
                        engine = self._intraday_engines.get(pid)
                        if engine:
                            logger.info("Phase 1 [%s/live] finalize: portfolio=%s date=%s", strategy, pid, today)
                            engine.finalize()
                        else:
                            raise RuntimeError(
                                f"日内实时引擎未初始化: portfolio={pid}, "
                                f"请检查 09:25 初始化流程是否正常执行"
                            )
                    else:
                        # Mode B-replay：盘后回放模式 — 获取全日分钟线逐 bar 回放
                        logger.info("Phase 1 [%s/replay] 开始回放: portfolio=%s date=%s", strategy, pid, today)
                        self._replay_intraday(pid, today)
                else:
                    logger.info("首日跳过 Phase 1: portfolio=%s", pid)

                # Phase 2: 信号生成（为 T+1 准备）
                logger.info("Phase 2 信号生成: portfolio=%s date=%s", pid, today)
                SignalGenerator.generate_signals(pid, today)

                # Phase 3: 数据收集（首日跳过）
                if not is_first_day:
                    logger.info("Phase 3 数据收集: portfolio=%s date=%s", pid, today)
                    try:
                        LiveICTracker.compute_and_save(pid, today)
                    except Exception as e:
                        logger.warning("因子 IC 计算失败 portfolio=%s: %s", pid, e)
                    try:
                        FactorAttribution.compute_and_save(pid, today)
                    except Exception as e:
                        logger.warning("因子归因失败 portfolio=%s: %s", pid, e)
                    try:
                        StockPnLTracker.update_stock_pnl(pid, today)
                    except Exception as e:
                        logger.warning("个股盈亏更新失败 portfolio=%s: %s", pid, e)
                    try:
                        self._update_model_live_track(pid, today)
                    except Exception as e:
                        logger.warning("模型追踪更新失败 portfolio=%s: %s", pid, e)

            except Exception as e:
                logger.error("模拟盘每日任务失败 portfolio=%s: %s", pid, e, exc_info=True)

        logger.info("每日任务完成: %s", today)

    def _setup_intraday_schedules(self) -> None:
        """每个交易日 09:25 调用 — 仅为 live 模式的模拟盘创建日内调度."""
        today = date.today()
        if not self._is_trading_day(today):
            return

        from .portfolio_manager import PortfolioManager
        from .execution_engine import ExecutionEngine

        running = PortfolioManager.get_running_portfolios()
        self._intraday_engines.clear()

        for portfolio in running:
            pid = portfolio["id"]
            enable_intraday = bool(portfolio.get("enable_intraday"))
            exec_mode = (portfolio.get("intraday_exec_mode") or "replay").lower()

            # 仅 live 模式需要盘中实时调度
            if not enable_intraday or exec_mode != "live":
                continue

            strategy = (portfolio.get("intraday_strategy") or "CLOSE_PRICE").upper()

            if self._is_first_day(pid, today):
                continue

            # 创建 ExecutionEngine
            engine = ExecutionEngine(pid, today)
            engine.initialize()
            self._intraday_engines[pid] = engine

            # 按 intraday_freq 设置分钟级 schedule
            freq = portfolio.get("intraday_freq", "5m")
            minutes = self._parse_freq_minutes(freq)
            tag = f"intraday_{pid}"
            self._intraday_tags.append(tag)
            self._scheduler.every(minutes).minutes.do(
                self._intraday_step, pid,
            ).tag(tag)

            logger.info(
                "日内调度已创建: portfolio=%s algo=%s freq=%s(%dm)",
                pid, strategy, freq, minutes,
            )

    def _intraday_step(self, portfolio_id: int) -> None:
        """每 N 分钟调用 — 执行一步日内交易."""
        if not _in_trading_hours():
            return

        engine = self._intraday_engines.get(portfolio_id)
        if engine is None or engine.all_complete:
            return

        # 获取当前 bar 数据
        bar_data_map = self._get_current_bar_data(engine, portfolio_id)
        market_context = self._get_market_context(engine, portfolio_id)

        fills = engine.step(bar_data_map, market_context)
        if fills:
            logger.info(
                "日内步骤执行: portfolio=%s fills=%d",
                portfolio_id, len(fills),
            )

    def _cleanup_intraday(self) -> None:
        """15:05 清理日内 schedule jobs."""
        for tag in self._intraday_tags:
            self._scheduler.clear(tag)
        self._intraday_tags.clear()
        self._intraday_engines.clear()
        logger.info("日内调度清理完成")

    def _replay_intraday(self, portfolio_id: int, trade_date: date) -> None:
        """盘后回放模式 — 获取当日全部分钟线，逐 bar 回放执行引擎."""
        from .execution_engine import ExecutionEngine

        engine = ExecutionEngine(portfolio_id, trade_date)
        engine.initialize()

        if engine.all_complete:
            logger.info("replay: 无待执行订单 portfolio=%s", portfolio_id)
            engine.finalize()
            return

        # 收集所有需要的 symbol
        symbols = [s.symbol for s in engine._orders.values()]
        if not symbols:
            engine.finalize()
            return

        # 获取当日全部分钟 bar（按 trade_time 升序）
        minute_bars = self._fetch_all_minute_bars(symbols, trade_date)

        if not minute_bars:
            # 无数据时不调用 finalize（信号应保持 pending 以便重试）
            raise RuntimeError(
                f"分钟线数据缺失，无法执行日内回放: portfolio={portfolio_id} "
                f"date={trade_date} symbols={symbols[:5]}{'...' if len(symbols) > 5 else ''}, "
                f"请检查 kline_minute_raw 表是否包含该交易日数据"
            )

        # 构建 volume_profile 和 market_context
        market_context = {
            "volume_profile": self._get_typical_volume_profile(),
            "sigma": 0.02,
        }

        # 逐时间点回放
        total_fills = 0
        try:
            for bar_time, bar_data_map in minute_bars:
                if engine.all_complete:
                    break
                fills = engine.step(bar_data_map, market_context, bar_time=bar_time)
                total_fills += len(fills)
        finally:
            # 确保 finalize 始终执行（即使中途异常，已完成的 fills 需要持久化）
            engine.finalize()

        logger.info(
            "replay 完成: portfolio=%s date=%s bars=%d fills=%d",
            portfolio_id, trade_date, len(minute_bars), total_fills,
        )

    @staticmethod
    def _fetch_all_minute_bars(
        symbols: list, trade_date: date,
    ) -> list:
        """获取当日全部分钟 bar.

        当天数据从 TDX 分钟 K 线接口获取，历史日期从 DB 获取。
        """
        is_today = trade_date == date.today()
        if is_today:
            return MinuteDataProvider.fetch_all_bars_tdx(symbols, trade_date)
        return MinuteDataProvider.fetch_all_bars(symbols, trade_date)

    def _get_current_bar_data(
        self, engine: "ExecutionEngine", portfolio_id: int,
    ) -> Dict[str, Dict]:
        """获取当前 bar 行情数据.

        当天数据从 TDX 分钟 K 线接口获取（开盘期间 kline_minute_raw 无当天数据），
        历史日期从 DB 读取。
        """
        symbols = [s.symbol for s in engine._orders.values() if not engine._algo.is_complete(s)]
        if not symbols:
            return {}

        today = engine.trade_date
        is_today = today == date.today()

        if is_today:
            bar_data_map = MinuteDataProvider.fetch_latest_bar_tdx(symbols, today)
        else:
            bar_data_map = MinuteDataProvider.fetch_latest_bar(symbols, today)

        missing = [s for s in symbols if s not in bar_data_map]
        if missing:
            source = "TDX 分钟K线接口" if is_today else "kline_minute_raw"
            raise RuntimeError(
                f"分钟线数据缺失: {missing}, "
                f"date={today}, 数据源={source}"
            )

        return bar_data_map

    def _get_market_context(
        self, engine: "ExecutionEngine", portfolio_id: int,
    ) -> Dict:
        """构建市场上下文（volume_profile, sigma, close_series）."""
        context: Dict = {}

        # volume_profile: 日内成交量分布（简化版 — 使用 A 股典型 U 型分布）
        # 48 个 5 分钟 bar 的典型成交量占比
        context["volume_profile"] = self._get_typical_volume_profile()

        # sigma: 近 20 日波动率
        context["sigma"] = 0.02  # 默认值

        return context

    @staticmethod
    def _get_typical_volume_profile() -> list:
        """A 股典型日内成交量 U 型分布（48 个 5 分钟 bar）."""
        # 上午 24 bars (09:30-11:30), 下午 24 bars (13:00-15:00)
        profile = []
        for i in range(48):
            if i < 3 or i >= 45:      # 开盘/收盘高峰
                profile.append(4.0)
            elif i < 6 or i >= 42:     # 次高峰
                profile.append(3.0)
            elif 22 <= i <= 25:        # 午间交接
                profile.append(2.5)
            else:                       # 盘中平淡
                profile.append(1.5)
        return profile

    @staticmethod
    def _parse_freq_minutes(freq: str) -> int:
        """解析频率字符串为分钟数."""
        freq = freq.strip().lower()
        if freq.endswith("m"):
            return max(1, int(freq[:-1]))
        return 5  # 默认 5 分钟

    def run_catchup(self, portfolio_id: int) -> Dict[str, Any]:
        """对 catching_up 状态的组合执行历史追赶.

        从 start_date 到最新交易日，逐日执行 Phase 1 + Phase 2。
        追赶完成后状态 → caught_up。
        """
        # 防止并发追赶同一组合
        if portfolio_id in self._active_catchups:
            logger.warning("追赶已在执行中，跳过: portfolio=%s", portfolio_id)
            return {"portfolio_id": portfolio_id, "status": "already_running", "days_processed": 0}
        self._active_catchups.add(portfolio_id)

        try:
            return self._run_catchup_inner(portfolio_id)
        except Exception as e:
            logger.error("追赶失败 portfolio=%s: %s", portfolio_id, e, exc_info=True)
            # 将错误信息写入进度，前端可通过 catchup-progress 读取
            self._update_catchup_error(portfolio_id, str(e))
            raise
        finally:
            self._active_catchups.discard(portfolio_id)

    def _run_catchup_inner(self, portfolio_id: int) -> Dict[str, Any]:
        """追赶内部实现（由 run_catchup 调用，已持有并发锁）.

        执行路径与 _daily_job 保持一致：
        - enable_intraday=True  → _replay_intraday（分钟线回放）
        - enable_intraday=False → TradeExecutor（收盘价）
        """
        from .portfolio_manager import PortfolioManager
        from .trade_executor import TradeExecutor
        from .signal_generator import SignalGenerator
        from .stock_pnl_tracker import StockPnLTracker

        config = PortfolioManager.get_portfolio(portfolio_id)
        if config is None:
            raise ValueError(f"模拟盘 {portfolio_id} 不存在")
        if config.get("status") != "catching_up":
            raise ValueError(f"仅 catching_up 状态可执行追赶，当前: {config.get('status')}")

        start_dt = config.get("start_date")
        if not start_dt:
            raise ValueError(f"模拟盘 {portfolio_id} 未设置 start_date")
        if isinstance(start_dt, str):
            start_dt = date.fromisoformat(start_dt)

        today = date.today()
        trading_days = self._get_trading_days(start_dt, today)
        if not trading_days:
            logger.info("追赶: 无交易日需要处理 portfolio=%s", portfolio_id)
            PortfolioManager.finish_catchup(portfolio_id)
            return {"portfolio_id": portfolio_id, "status": "caught_up", "days_processed": 0}

        logger.info(
            "开始追赶: portfolio=%s from=%s to=%s days=%d intraday=%s strategy=%s",
            portfolio_id, trading_days[0], trading_days[-1], len(trading_days),
            bool(config.get("enable_intraday")),
            config.get("intraday_strategy", "CLOSE_PRICE"),
        )

        # 清除上次失败的错误信息（重试场景）
        self._clear_catchup_error(portfolio_id)

        enable_intraday = bool(config.get("enable_intraday"))

        completed_days = 0
        for trade_date in trading_days:
            is_first_day = (trade_date == trading_days[0])

            # Phase 1: 成交执行（首日跳过）— 与 _daily_job 保持一致
            if not is_first_day:
                if enable_intraday:
                    self._replay_intraday(portfolio_id, trade_date)
                else:
                    TradeExecutor.execute_trades(portfolio_id, trade_date)

            # Phase 2: 信号生成
            SignalGenerator.generate_signals(portfolio_id, trade_date)

            # Phase 3: 个股盈亏（首日跳过）
            if not is_first_day:
                try:
                    StockPnLTracker.update_stock_pnl(portfolio_id, trade_date)
                except Exception as e:
                    logger.warning("追赶个股盈亏失败 portfolio=%s date=%s: %s", portfolio_id, trade_date, e)

            completed_days += 1
            # 记录进度到 DB
            self._update_catchup_progress(portfolio_id, completed_days, len(trading_days), trade_date)

            logger.info(
                "追赶进度: portfolio=%s %d/%d date=%s",
                portfolio_id, completed_days, len(trading_days), trade_date,
            )

        # 追赶完成 → caught_up
        PortfolioManager.finish_catchup(portfolio_id)
        logger.info("追赶完成: portfolio=%s days=%d", portfolio_id, completed_days)

        return {
            "portfolio_id": portfolio_id,
            "status": "caught_up",
            "days_processed": completed_days,
        }

    def _continue_catchup(self, portfolio_id: int) -> None:
        """在 _daily_job 中继续未完成的追赶."""
        self.run_catchup(portfolio_id)

    @staticmethod
    def _get_trading_days(start_dt: date, end_dt: date) -> list:
        """获取两个日期间的所有交易日."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cal_date FROM market.trading_calendar
                    WHERE cal_date >= %s AND cal_date <= %s AND is_trading = TRUE
                    ORDER BY cal_date
                    """,
                    (start_dt, end_dt),
                )
                return [r[0] for r in cur.fetchall()]

    @staticmethod
    def _update_catchup_progress(
        portfolio_id: int, completed: int, total: int, current_date: date,
    ) -> None:
        """更新追赶进度到 portfolio_config 的 JSON 字段（复用 intraday_config 或专用字段）."""
        progress = json.dumps({
            "completed_days": completed,
            "total_days": total,
            "current_date": str(current_date),
            "pct": round(completed / total * 100, 1) if total > 0 else 0,
        })
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE paper_trading.portfolio_config
                    SET updated_at = NOW(),
                        intraday_config = COALESCE(intraday_config, '{}'::jsonb) || %s::jsonb
                    WHERE id = %s
                    """,
                    (progress, portfolio_id),
                )
                conn.commit()

    @staticmethod
    def _update_catchup_error(portfolio_id: int, error_msg: str) -> None:
        """将追赶错误信息写入 intraday_config，前端可通过 catchup-progress 获取."""
        from datetime import datetime
        error_data = json.dumps({
            "catchup_error": error_msg[:500],
            "catchup_error_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE paper_trading.portfolio_config
                    SET updated_at = NOW(),
                        intraday_config = COALESCE(intraday_config, '{}'::jsonb) || %s::jsonb
                    WHERE id = %s
                    """,
                    (error_data, portfolio_id),
                )
                conn.commit()

    @staticmethod
    def _clear_catchup_error(portfolio_id: int) -> None:
        """清除 intraday_config 中的追赶错误信息（重试时调用）."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE paper_trading.portfolio_config
                    SET intraday_config = COALESCE(intraday_config, '{}'::jsonb) - 'catchup_error'
                    WHERE id = %s
                    """,
                    (portfolio_id,),
                )
                conn.commit()

    def run_once(self, portfolio_id: int) -> dict:
        """手动触发一次（API 调用）."""
        from .portfolio_manager import PortfolioManager
        from .trade_executor import TradeExecutor
        from .signal_generator import SignalGenerator
        from .live_ic_tracker import LiveICTracker
        from .factor_attribution import FactorAttribution
        from .stock_pnl_tracker import StockPnLTracker

        today = date.today()
        is_first = self._is_first_day(portfolio_id, today)
        result = {"portfolio_id": portfolio_id, "date": str(today)}

        if not is_first:
            # 检查模式
            config = TradeExecutor._get_config(portfolio_id)
            enable_intraday = bool(config.get("enable_intraday"))
            strategy = (config.get("intraday_strategy") or "CLOSE_PRICE").upper()

            if not enable_intraday:
                exec_result = TradeExecutor.execute_trades(portfolio_id, today)
                result["execution"] = exec_result
            else:
                exec_mode = (config.get("intraday_exec_mode") or "replay").lower()

                # 日内模式 run-once: 创建引擎执行全部步骤后 finalize
                from .execution_engine import ExecutionEngine

                if exec_mode == "replay":
                    # replay 模式：尝试用当日分钟线回放
                    engine = ExecutionEngine(portfolio_id, today)
                    engine.initialize()
                    symbols = [s.symbol for s in engine._orders.values()]
                    minute_bars = self._fetch_all_minute_bars(symbols, today) if symbols else []

                    if minute_bars:
                        market_context = {
                            "volume_profile": self._get_typical_volume_profile(),
                            "sigma": 0.02,
                        }
                        total_fills = []
                        for bar_time, bar_data_map in minute_bars:
                            if engine.all_complete:
                                break
                            fills = engine.step(bar_data_map, market_context, bar_time=bar_time)
                            total_fills.extend(fills)
                        result["replay_bars"] = len(minute_bars)
                    else:
                        # 无分钟数据，用收盘价跑完
                        close_prices = TradeExecutor._get_close_prices(symbols, today) if symbols else {}
                        bar_data_map = {
                            sym: {"open": p, "high": p, "low": p, "close": p, "volume": 0}
                            for sym, p in close_prices.items()
                        }
                        market_context = {"volume_profile": [], "sigma": 0.02}
                        close_time = datetime.combine(today, dt_time(15, 0))
                        for _ in range(100):
                            if engine.all_complete:
                                break
                            engine.step(bar_data_map, market_context, bar_time=close_time)

                    finalize_result = engine.finalize()
                    result["execution"] = finalize_result
                    result["exec_algo"] = strategy
                    result["exec_mode"] = "replay"
                else:
                    # live 模式 run-once：用收盘价一次性跑完
                    engine = ExecutionEngine(portfolio_id, today)
                    engine.initialize()
                    symbols = [s.symbol for s in engine._orders.values()]
                    close_prices = TradeExecutor._get_close_prices(symbols, today) if symbols else {}
                    bar_data_map = {
                        sym: {"open": p, "high": p, "low": p, "close": p, "volume": 0}
                        for sym, p in close_prices.items()
                    }
                    market_context = {"volume_profile": [], "sigma": 0.02}
                    close_time = datetime.combine(today, dt_time(15, 0))
                    for _ in range(100):
                        if engine.all_complete:
                            break
                        engine.step(bar_data_map, market_context, bar_time=close_time)
                    finalize_result = engine.finalize()
                    result["execution"] = finalize_result
                    result["exec_algo"] = strategy
                    result["exec_mode"] = "live"

        signals = SignalGenerator.generate_signals(portfolio_id, today)
        result["signals_generated"] = len(signals)

        if not is_first:
            try:
                LiveICTracker.compute_and_save(portfolio_id, today)
            except Exception as e:
                result["ic_error"] = str(e)
            try:
                FactorAttribution.compute_and_save(portfolio_id, today)
            except Exception as e:
                result["attribution_error"] = str(e)
            try:
                StockPnLTracker.update_stock_pnl(portfolio_id, today)
            except Exception as e:
                result["pnl_error"] = str(e)

        return result

    @staticmethod
    def _is_trading_day(d: date) -> bool:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM market.trading_calendar WHERE cal_date = %s AND is_trading = TRUE",
                    (d,),
                )
                return cur.fetchone() is not None

    @staticmethod
    def _is_first_day(portfolio_id: int, trade_date: date) -> bool:
        """是否为该模拟盘的首个运行日（无历史信号）."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM paper_trading.trade_signals WHERE portfolio_id = %s LIMIT 1",
                    (portfolio_id,),
                )
                return cur.fetchone() is None

    @staticmethod
    def _update_model_live_track(portfolio_id: int, trade_date: date) -> None:
        """更新模型实盘追踪."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT model_catalog_id FROM paper_trading.portfolio_config WHERE id = %s",
                    (portfolio_id,),
                )
                row = cur.fetchone()
        if not row or not row[0]:
            return

        model_catalog_id = row[0]

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT daily_return FROM paper_trading.daily_snapshot WHERE portfolio_id = %s AND trade_date = %s",
                    (portfolio_id, trade_date),
                )
                row = cur.fetchone()
        if not row:
            return

        daily_return = float(row[0]) if row[0] is not None else None

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO model_live_track (
                        model_catalog_id, portfolio_id, trade_date, daily_return
                    ) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (model_catalog_id, portfolio_id, trade_date)
                    DO UPDATE SET daily_return = EXCLUDED.daily_return
                    """,
                    (model_catalog_id, portfolio_id, trade_date, daily_return),
                )
                conn.commit()


# 全局实例
paper_trading_scheduler = PaperTradingScheduler()
