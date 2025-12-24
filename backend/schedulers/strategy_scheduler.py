"""策略调度器（QMT 交易系统）

使用 schedule 库定时执行策略。
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import schedule
from dotenv import load_dotenv

from ..db.pg_pool import get_conn
from ..strategies.ma_cross_strategy import MACrossStrategy
from ..strategies.trend_following_strategy import TrendFollowingStrategy
from ..infra.strategy_executor import SimpleStrategyExecutor
from ..infra.realtime_quote_subscriber import get_realtime_quote_subscriber

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
        self._realtime_subscriber = get_realtime_quote_subscriber()
        self._realtime_enabled_strategies: Dict[str, List[str]] = {}  # strategy_id -> stocks
        self._realtime_subscription_seqs: Dict[str, int] = {}  # strategy_id -> subscription_seq

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

            # 启动刷新线程
            refresh_thread = threading.Thread(
                target=self._refresh_loop,
                args=(refresh_interval,),
                name="strategy-refresh",
                daemon=True,
            )
            refresh_thread.start()

            # 启动实时行情订阅服务（如果可用）
            try:
                self._realtime_subscriber.start()
                logger.info("实时行情订阅服务已启动")
            except Exception as e:
                logger.warning(f"启动实时行情订阅服务失败: {e}")

            logger.info("策略调度器已启动")

    def shutdown(self, wait: bool = True) -> None:
        """关闭调度器"""
        with self._lock:
            self._stop_event.set()
            
            # 停止实时行情订阅
            try:
                self._realtime_subscriber.stop()
            except Exception as e:
                logger.warning(f"停止实时行情订阅服务失败: {e}")
            
            if self._schedule_thread and self._schedule_thread.is_alive():
                if wait:
                    self._schedule_thread.join(timeout=5.0)
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
            
            # 取消所有实时行情订阅（防御性检查）
            if not hasattr(self, '_realtime_subscription_seqs'):
                self._realtime_subscription_seqs: Dict[str, int] = {}
            if not hasattr(self, '_realtime_enabled_strategies'):
                self._realtime_enabled_strategies: Dict[str, List[str]] = {}
            
            for strategy_id, seq in list(self._realtime_subscription_seqs.items()):
                try:
                    self._realtime_subscriber.unsubscribe(seq)
                except Exception as e:
                    logger.warning(f"取消实时行情订阅失败 {strategy_id}: {e}")
            self._realtime_enabled_strategies.clear()
            self._realtime_subscription_seqs.clear()

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
        
        # 防御性检查：确保属性存在
        if not hasattr(self, '_realtime_subscription_seqs'):
            self._realtime_subscription_seqs: Dict[str, int] = {}
        if not hasattr(self, '_realtime_enabled_strategies'):
            self._realtime_enabled_strategies: Dict[str, List[str]] = {}
        
        # 如果配置了实时行情触发，订阅实时行情
        if schedule_type == "realtime":
            symbols = config.get("symbols", [])
            if symbols:
                try:
                    # 定义回调函数
                    def on_realtime_quote(stock_code: str, quote: Dict):
                        """实时行情回调"""
                        try:
                            logger.info(f"收到 {stock_code} 实时行情，触发策略 {strategy_id}")
                            self._execute_strategy_for_symbol(strategy_id, stock_code)
                        except Exception as e:
                            logger.error(f"实时行情触发策略执行失败: {e}", exc_info=True)
                    
                    # 订阅实时行情
                    seq = self._realtime_subscriber.subscribe(symbols, on_realtime_quote)
                    if seq:
                        self._realtime_enabled_strategies[strategy_id] = symbols
                        self._realtime_subscription_seqs[strategy_id] = seq
                        logger.info(f"策略 {strategy_id} 已订阅实时行情: {symbols}, 订阅号: {seq}")
                except Exception as e:
                    logger.error(f"设置实时行情订阅失败 {strategy_id}: {e}", exc_info=True)

    def _execute_strategy(self, strategy_id: str) -> None:
        """执行策略"""
        try:
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

            # 创建执行记录
            execution_id = self._create_execution_record(strategy_id, "SCHEDULED", "scheduler")

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

                # 更新执行记录
                self._update_execution_record(
                    execution_id, "SUCCESS", signals_generated, signals_executed, symbols_processed
                )

            except Exception as e:
                error_message = str(e)
                logger.error(f"执行策略 {strategy_id} 失败: {e}", exc_info=True)
                self._update_execution_record(
                    execution_id, "FAILED", signals_generated, signals_executed, symbols_processed, error_message
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

