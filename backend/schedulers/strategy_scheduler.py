"""策略调度器（QMT 交易系统）

使用 schedule 库定时执行策略。
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, time as dt_time
from typing import Any, Dict, List, Optional

import schedule
from dotenv import load_dotenv

from ..db.pg_pool import get_conn
from ..strategies.ma_cross_strategy import MACrossStrategy
from ..strategies.trend_following_strategy import TrendFollowingStrategy
from ..strategies.twap_execution_strategy import TWAPExecutionStrategy
from ..strategies.ema_momentum_strategy import EMAMomentumStrategy
from ..strategies.volatility_breakout_strategy import VolatilityBreakoutStrategy
from ..infra.strategy_executor import SimpleStrategyExecutor
from ..data_service import api as data_api
from ..infra.qmt_client import get_qmt_client_singleton, QMTNotAvailableError
from ..services.trading_calendar_status import TradingCalendarStatusService

load_dotenv(override=True)

logger = logging.getLogger(__name__)


class StrategyScheduler:
    """策略调度器（支持定时调度和事件驱动）"""

    def __init__(self):
        self._scheduler = schedule.Scheduler()
        self._schedule_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._strategies: Dict[str, Any] = {}  # strategy_id -> strategy instance
        self._executor = SimpleStrategyExecutor()
        # 使用数据服务层的推送接口时，每个实时策略对应一个工作线程和停止事件
        self._realtime_enabled_strategies: Dict[str, List[str]] = {}  # strategy_id -> stocks
        self._realtime_threads: Dict[str, threading.Thread] = {}  # strategy_id -> thread
        self._realtime_stop_events: Dict[str, threading.Event] = {}  # strategy_id -> stop_event
        self._trading_calendar_status = TradingCalendarStatusService()

    def _is_trading_time(self, now: Optional[datetime] = None) -> bool:
        dt = now or datetime.now()
        try:
            calendar_status = self._trading_calendar_status.status(as_of_date=dt.date())
        except Exception:
            logger.error("official trading calendar status is unavailable; realtime strategy execution is blocked", exc_info=True)
            return False
        if not calendar_status.get("is_trading_day"):
            return False
        t = dt.time()
        morning_start = dt_time(9, 15)
        morning_end = dt_time(11, 31)
        afternoon_start = dt_time(13, 0)
        afternoon_end = dt_time(15, 0)
        return (morning_start <= t <= morning_end) or (afternoon_start <= t <= afternoon_end)

    def start(self, refresh_interval: int = 60) -> None:
        """启动调度器

        Args:
            refresh_interval: 刷新策略配置的间隔（秒）
        """
        with self._lock:
            if self._schedule_thread and self._schedule_thread.is_alive():
                return

            self._stop_event.clear()
            self.refresh_strategies()

            # 启动调度线程
            self._schedule_thread = threading.Thread(
                target=self._run_loop, name="strategy-scheduler", daemon=True
            )
            self._schedule_thread.start()

            interval = refresh_interval
            try:
                v = (os.getenv("STRATEGY_REFRESH_INTERVAL_SECONDS") or "").strip()
                if v:
                    interval = int(v)
            except Exception:
                interval = refresh_interval

            enable_refresh = (os.getenv("ENABLE_STRATEGY_REFRESH") or "").strip().lower() in {
                "1",
                "true",
                "yes",
                "y",
                "on",
            }

            if enable_refresh and interval > 0:
                refresh_thread = threading.Thread(
                    target=self._refresh_loop,
                    args=(interval,),
                    name="strategy-refresh",
                    daemon=True,
                )
                refresh_thread.start()
            else:
                logger.warning(
                    "策略配置自动刷新默认关闭（ENABLE_STRATEGY_REFRESH=%s, STRATEGY_REFRESH_INTERVAL_SECONDS=%s）",
                    os.getenv("ENABLE_STRATEGY_REFRESH"),
                    os.getenv("STRATEGY_REFRESH_INTERVAL_SECONDS"),
                )

            logger.info("策略调度器已启动")

    def shutdown(self, wait: bool = True) -> None:
        """关闭调度器"""
        with self._lock:
            self._stop_event.set()
            # 停止所有基于数据服务的实时线程
            for sid, ev in list(self._realtime_stop_events.items()):
                try:
                    ev.set()
                except Exception as e:
                    logger.warning(f"停止实时线程失败 {sid}: {e}")
            if wait:
                for sid in list(self._realtime_threads):
                    t = self._realtime_threads.get(sid)
                    if t and t.is_alive():
                        t.join(timeout=2.0)
            self._realtime_stop_events.clear()
            self._realtime_threads.clear()

            if self._schedule_thread and self._schedule_thread.is_alive():
                if wait:
                    self._schedule_thread.join(timeout=3.0)
            logger.info("策略调度器已关闭")

    def refresh_strategies(self) -> None:
        """刷新策略配置（从数据库加载）"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT strategy_id, strategy_name, strategy_type,
                               enabled, config_json, schedule_config
                        FROM trading.strategy_config
                        WHERE enabled = TRUE
                        """
                    )
                    rows = cur.fetchall()

            # 清除现有调度
            self._scheduler.clear()
            
            # 停止所有已有的实时行情线程
            for sid, ev in list(self._realtime_stop_events.items()):
                try:
                    ev.set()
                    t = self._realtime_threads.get(sid)
                    if t and t.is_alive():
                        t.join(timeout=5.0)
                except Exception as e:
                    logger.warning(f"取消实时行情线程失败 {sid}: {e}")
            self._realtime_enabled_strategies.clear()
            self._realtime_threads.clear()
            self._realtime_stop_events.clear()

            # 加载策略
            for row in rows:
                strategy_id, strategy_name, strategy_type, enabled, config_json, schedule_config = row

                if not enabled:
                    continue

                try:
                    # PostgreSQL JSONB 字段可能已经是 dict，需要判断类型
                    if isinstance(config_json, dict):
                        config = config_json
                    elif isinstance(config_json, str):
                        config = json.loads(config_json) if config_json else {}
                    else:
                        config = {}
                    
                    if isinstance(schedule_config, dict):
                        schedule_config_dict = schedule_config
                    elif isinstance(schedule_config, str):
                        schedule_config_dict = json.loads(schedule_config) if schedule_config else {}
                    else:
                        schedule_config_dict = {}

                    # 创建策略实例
                    strategy = self._create_strategy_instance(
                        strategy_type, strategy_id, config
                    )
                    if strategy is None:
                        logger.warning(f"无法创建策略实例: {strategy_id}")
                        continue

                    self._strategies[strategy_id] = strategy

                    # 添加调度任务
                    self._add_schedule(strategy_id, schedule_config_dict)

                    # 如果配置了实时行情触发，订阅实时行情
                    self._setup_realtime_subscription(strategy_id, config, schedule_config_dict)

                    logger.info(f"已加载策略: {strategy_id} ({strategy_name})")

                except Exception as e:
                    logger.error(f"加载策略失败 {strategy_id}: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"刷新策略配置失败: {e}", exc_info=True)

    def _create_strategy_instance(
        self, strategy_type: str, strategy_id: str, config: Dict[str, Any]
    ) -> Optional[Any]:
        """创建策略实例"""
        if strategy_type == "MA_CROSS":
            return MACrossStrategy(strategy_id=strategy_id, executor=self._executor, config=config)
        elif strategy_type == "TREND_FOLLOWING":
            return TrendFollowingStrategy(
                strategy_id=strategy_id, executor=self._executor, config=config
            )
        elif strategy_type == "TWAP_EXECUTION":
            return TWAPExecutionStrategy(
                strategy_id=strategy_id, executor=self._executor, config=config
            )
        elif strategy_type == "EMA_MOMENTUM":
            return EMAMomentumStrategy(
                strategy_id=strategy_id, executor=self._executor, config=config
            )
        elif strategy_type == "VOLATILITY_BREAKOUT":
            return VolatilityBreakoutStrategy(
                strategy_id=strategy_id, executor=self._executor, config=config
            )
        else:
            logger.warning(f"未知的策略类型: {strategy_type}")
            return None

    def _add_schedule(self, strategy_id: str, schedule_config: Dict[str, Any]) -> None:
        """添加调度任务"""
        schedule_type = schedule_config.get("type", "daily")
        schedule_time = schedule_config.get("time", "09:30")

        if schedule_type == "daily":
            # 每日执行
            self._scheduler.every().day.at(schedule_time).do(
                self._execute_strategy, strategy_id=strategy_id
            )
        elif schedule_type == "hourly":
            # 每小时执行
            hour = int(schedule_time.split(":")[0])
            self._scheduler.every().hour.at(f":{schedule_time.split(':')[1]}").do(
                self._execute_strategy, strategy_id=strategy_id
            )
        elif schedule_type == "minute":
            # 每分钟执行（测试用）
            minutes = int(schedule_config.get("interval", 1))
            self._scheduler.every(minutes).minutes.do(
                self._execute_strategy, strategy_id=strategy_id
            )
        elif schedule_type == "realtime":
            # 实时行情触发（不添加定时任务，通过订阅实现）
            logger.info(f"策略 {strategy_id} 使用实时行情触发")
        else:
            logger.warning(f"未知的调度类型: {schedule_type}")


    def _setup_realtime_subscription(self, strategy_id: str, config: Dict[str, Any], schedule_config: Dict[str, Any]) -> None:
        """设置实时行情订阅（事件驱动）"""
        schedule_type = schedule_config.get("type", "daily")

        if schedule_type != "realtime":
            return

        symbols = config.get("symbols", [])
        if not symbols:
            return

        stop_event = threading.Event()

        def _worker() -> None:
            try:
                # 仅在【交易时段 + QMT 已连接】时才进入 stream_quotes 订阅循环。
                last_connected: Optional[bool] = None

                while not stop_event.is_set():
                    if not self._is_trading_time():
                        time.sleep(2.0)
                        continue

                    try:
                        client = get_qmt_client_singleton()
                        status = client.status()

                        if last_connected is None:
                            last_connected = bool(status.connected)
                        elif last_connected and (not status.connected):
                            logger.warning("QMT 已断开连接，realtime 行情触发暂停，等待重连")
                            last_connected = False
                        elif (not last_connected) and status.connected:
                            logger.info("QMT 已恢复连接，realtime 行情触发恢复")
                            last_connected = True

                        if not status.connected:
                            time.sleep(2.0)
                            continue
                    except QMTNotAvailableError:
                        if last_connected is None:
                            last_connected = False
                        elif last_connected is True:
                            logger.warning("QMT 不可用，realtime 行情触发暂停，等待重连")
                            last_connected = False
                        time.sleep(2.0)
                        continue

                    for batch in data_api.stream_quotes(
                        universe=symbols,
                        fields=["close", "open", "high", "low", "volume", "amount"],
                        freq="tick",
                    ):
                        if stop_event.is_set():
                            break

                        if not self._is_trading_time():
                            break

                        try:
                            status = client.status()
                            if last_connected is None:
                                last_connected = bool(status.connected)
                            elif last_connected and (not status.connected):
                                logger.warning("QMT 已断开连接，realtime 行情触发暂停，等待重连")
                                last_connected = False
                            elif (not last_connected) and status.connected:
                                logger.info("QMT 已恢复连接，realtime 行情触发恢复")
                                last_connected = True

                            if not status.connected:
                                break
                        except QMTNotAvailableError:
                            if last_connected is None:
                                last_connected = False
                            elif last_connected is True:
                                logger.warning("QMT 不可用，realtime 行情触发暂停，等待重连")
                                last_connected = False
                            break

                        try:
                            instruments = list(batch.data.index.unique())
                        except Exception:
                            instruments = []

                        for inst in instruments:
                            try:
                                logger.info("收到 %s 实时快照，触发策略 %s", inst, strategy_id)
                                self._execute_strategy_for_symbol(strategy_id, inst)
                            except Exception as e:
                                logger.error(
                                    f"实时快照触发策略执行失败 {strategy_id} {inst}: {e}",
                                    exc_info=True,
                                )
            except Exception as e:
                logger.error(f"数据服务实时流触发策略 {strategy_id} 失败: {e}", exc_info=True)

        t = threading.Thread(
            target=_worker,
            name=f"strategy-realtime-{strategy_id}",
            daemon=True,
        )
        self._realtime_enabled_strategies[strategy_id] = symbols
        self._realtime_stop_events[strategy_id] = stop_event
        self._realtime_threads[strategy_id] = t
        t.start()
        logger.info("策略 %s 使用数据服务实时快照触发，标的: %s", strategy_id, symbols)

    def _execute_strategy(self, strategy_id: str) -> None:
        """执行策略"""
        try:
            if not self._is_trading_time():
                return
            # 若 QMT 未连接，则直接跳过本次执行，避免在模拟交易关闭时产生无效压力
            try:
                client = get_qmt_client_singleton()
                status = client.status()
                if not status.connected:
                    logger.warning("QMT 未连接，跳过策略执行: %s", strategy_id)
                    return
            except QMTNotAvailableError:
                logger.warning("QMT 不可用，跳过策略执行: %s", strategy_id)
                return

            strategy = self._strategies.get(strategy_id)
            if strategy is None:
                logger.warning(f"策略不存在: {strategy_id}")
                return

            # 获取策略配置中的股票列表
            config = strategy.config
            symbols = config.get("symbols", [])

            if not symbols:
                logger.warning(f"策略 {strategy_id} 没有配置股票列表")
                return

            signals_generated = 0
            signals_executed = 0
            symbols_processed = 0
            error_message = None

            try:
                for symbol in symbols:
                    try:
                        symbols_processed += 1
                        result = self._execute_strategy_for_symbol(strategy_id, symbol)

                        if result and result.get("success") and result.get("signals"):
                            signals_generated += len(result.get("signals", []))
                            if result.get("order_id"):
                                signals_executed += 1

                    except Exception as e:
                        logger.error(f"执行策略 {strategy_id} 股票 {symbol} 失败: {e}", exc_info=True)

                # 仅在存在实际成交（下单成功）时写入执行记录
                if signals_executed > 0:
                    execution_id = self._create_execution_record(
                        strategy_id, "SCHEDULED", "scheduler"
                    )
                    if execution_id:
                        self._update_execution_record(
                            execution_id,
                            "SUCCESS",
                            signals_generated,
                            signals_executed,
                            symbols_processed,
                        )

            except Exception as e:
                error_message = str(e)
                logger.error(f"执行策略 {strategy_id} 失败: {e}", exc_info=True)
                # 失败场景下，同样只在曾产生过实际成交时记录执行
                if signals_executed > 0:
                    execution_id = self._create_execution_record(
                        strategy_id, "SCHEDULED", "scheduler"
                    )
                    if execution_id:
                        self._update_execution_record(
                            execution_id,
                            "FAILED",
                            signals_generated,
                            signals_executed,
                            symbols_processed,
                            error_message,
                        )

        except Exception as e:
            logger.error(f"执行策略异常 {strategy_id}: {e}", exc_info=True)

    def _create_execution_record(
        self, strategy_id: str, execution_type: str, trigger_source: str
    ) -> int:
        """创建执行记录"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO trading.strategy_execution
                        (strategy_id, execution_type, trigger_source, status, start_time)
                        VALUES (%s, %s, %s, 'RUNNING', %s)
                        RETURNING id
                        """,
                        (strategy_id, execution_type, trigger_source, datetime.now()),
                    )
                    execution_id = cur.fetchone()[0]
                    conn.commit()
                    return execution_id
        except Exception as e:
            logger.error(f"创建执行记录失败: {e}", exc_info=True)
            return 0

    def _update_execution_record(
        self,
        execution_id: int,
        status: str,
        signals_generated: int,
        signals_executed: int,
        symbols_processed: int,
        error_message: Optional[str] = None,
    ) -> None:
        """更新执行记录"""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE trading.strategy_execution
                        SET status = %s, end_time = %s,
                            duration_seconds = EXTRACT(EPOCH FROM (NOW() - start_time)),
                            signals_generated = %s, signals_executed = %s,
                            symbols_processed = %s, error_message = %s
                        WHERE id = %s
                        """,
                        (
                            status,
                            datetime.now(),
                            signals_generated,
                            signals_executed,
                            symbols_processed,
                            error_message,
                            execution_id,
                        ),
                    )
                    conn.commit()
        except Exception as e:
            logger.error(f"更新执行记录失败: {e}", exc_info=True)

    def _execute_strategy_for_symbol(self, strategy_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        """执行策略（单个股票）"""
        try:
            strategy = self._strategies.get(strategy_id)
            if strategy is None:
                logger.warning(f"策略不存在: {strategy_id}")
                return None

            result = strategy.run(symbol)
            return result

        except Exception as e:
            logger.error(f"执行策略异常 {strategy_id} {symbol}: {e}", exc_info=True)
            return None

    def _run_loop(self) -> None:
        """调度循环"""
        while not self._stop_event.is_set():
            self._scheduler.run_pending()
            time.sleep(1)

    def _refresh_loop(self, interval: int) -> None:
        """刷新循环"""
        while not self._stop_event.is_set():
            time.sleep(interval)
            if not self._stop_event.is_set():
                self.refresh_strategies()


# 全局调度器实例
scheduler = StrategyScheduler()

