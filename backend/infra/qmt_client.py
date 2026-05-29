"""QMT (miniQMT) client abstraction for AIstock.

Design goals:
- Keep `xtquant` imports isolated and optional so backend can boot without it.
- Provide a stable, minimal interface for "sim account snapshot" (cash + positions).
- Allow future evolution into a dedicated gateway process without touching callers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import os
from pathlib import Path
import sys
import threading
import time
import logging


_GLOBAL_QMT_CLIENT: Optional["BaseQMTClient"] = None


@dataclass
class QMTStatus:
    enabled: bool
    connected: bool
    mode: str  # "SIM" / "LIVE" / "UNKNOWN"
    account_id: Optional[str]
    provider: str  # "xtquant" / "simulator"
    userdata_path: Optional[str] = None
    session_id: Optional[int] = None
    last_error: Optional[str] = None


def _env_float(key: str, default: float) -> float:
    raw = (os.getenv(key) or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _call_with_timeout(fn, timeout_s: float):
    result_holder: Dict[str, Any] = {}
    error_holder: Dict[str, Any] = {}

    def _runner() -> None:
        try:
            result_holder["value"] = fn()
        except Exception as e:  # noqa: BLE001
            error_holder["error"] = e

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    t.join(timeout=max(0.0, float(timeout_s)))
    if t.is_alive():
        raise TimeoutError(f"call timed out after {timeout_s}s")
    if "error" in error_holder:
        raise error_holder["error"]
    return result_holder.get("value")
class QMTNotAvailableError(RuntimeError):
    """Raised when xtquant is missing or QMT connection is unavailable."""
class BaseQMTClient:
    """Minimal client interface used by API layer."""

    def connect(self) -> Tuple[bool, str]:
        raise NotImplementedError

    def disconnect(self) -> Tuple[bool, str]:
        raise NotImplementedError

    def status(self) -> QMTStatus:
        raise NotImplementedError

    def get_account_info(self) -> Dict[str, Any]:
        raise NotImplementedError

    def get_positions(self) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def get_local_data_range(self, stock_code: str, period: str) -> Dict[str, Any]:
        raise NotImplementedError

    def download_history_data(
        self, 
        stock_list: List[str], 
        period: str, 
        start_time: str = "", 
        end_time: str = "",
        task_id: str | None = None
    ) -> None:
        raise NotImplementedError

    def download_financial_data(self, stock_list: List[str], table_list: List[str], task_id: str | None = None) -> None:
        raise NotImplementedError

    def get_task_progress(self, task_id: str) -> Dict[str, Any]:
        raise NotImplementedError

    def update_task_status(self, task_id: str, status_data: Dict[str, Any]) -> None:
        raise NotImplementedError

    def get_latest_trading_day(self) -> str:
        raise NotImplementedError

    def get_stock_list_in_sector(self, sector_name: str) -> List[str]:
        raise NotImplementedError

    def get_trading_calendar(self, market: str = "SH") -> List[str]:
        raise NotImplementedError

    def get_full_tick(self, stock_list: List[str]) -> Dict[str, Any]:
        raise NotImplementedError


def get_qmt_client_singleton() -> BaseQMTClient:
    """Return process-wide QMT client singleton."""

    global _GLOBAL_QMT_CLIENT
    if _GLOBAL_QMT_CLIENT is None:
        _GLOBAL_QMT_CLIENT = build_qmt_client_from_env()
    return _GLOBAL_QMT_CLIENT


def reset_qmt_client_singleton() -> None:
    """Reset process-wide QMT client singleton.

    Used when .env changed and caller needs to rebuild client from latest env.
    """

    global _GLOBAL_QMT_CLIENT
    _GLOBAL_QMT_CLIENT = None

class SimulatorQMTClient(BaseQMTClient):
    """Fallback simulator: returns zero assets and empty positions."""

    def __init__(self, *, enabled: bool, account_id: str | None, mode: str, reason: str | None):
        self._enabled = bool(enabled)
        self._connected = False
        self._account_id = account_id
        self._mode = mode or "SIM"
        self._reason = reason
        self._active_tasks: Dict[str, Dict[str, Any]] = {}
        self._task_lock = threading.Lock()

    def connect(self) -> Tuple[bool, str]:
        self._connected = False
        return False, self._reason or "xtquant 未安装或不可用，已使用模拟占位实现"

    def disconnect(self) -> Tuple[bool, str]:
        self._connected = False
        return True, "已断开（模拟占位）"

    def status(self) -> QMTStatus:
        return QMTStatus(
            enabled=self._enabled,
            connected=False,
            mode=self._mode,
            account_id=self._account_id,
            provider="simulator",
            last_error=self._reason,
        )

    def get_account_info(self) -> Dict[str, Any]:
        return {
            "provider": "simulator",
            "connected": False,
            "account_id": self._account_id,
            "available_cash": 0.0,
            "total_asset": 0.0,
            "market_value": 0.0,
            "frozen_cash": 0.0,
        }

    def get_positions(self) -> List[Dict[str, Any]]:
        return []

    def get_local_data_range(self, stock_code: str, period: str) -> Dict[str, Any]:
        return {"start": None, "end": None, "count": 0}

    def download_history_data(
        self, 
        stock_list: List[str], 
        period: str, 
        start_time: str = "", 
        end_time: str = "",
        task_id: str | None = None
    ) -> None:
        pass

    def download_financial_data(self, stock_list: List[str], table_list: List[str], task_id: str | None = None) -> None:
        pass

    def get_latest_trading_day(self) -> str:
        import datetime
        return datetime.date.today().strftime("%Y%m%d")

    def get_task_progress(self, task_id: str) -> Dict[str, Any]:
        with self._task_lock:
            return self._active_tasks.get(task_id, {})

    def update_task_status(self, task_id: str, status_data: Dict[str, Any]) -> None:
        with self._task_lock:
            if task_id not in self._active_tasks:
                self._active_tasks[task_id] = {}
            self._active_tasks[task_id].update(status_data)
            self._active_tasks[task_id]["updated_at"] = time.time()

    def get_stock_list_in_sector(self, sector_name: str) -> List[str]:
        return []

    def get_trading_calendar(self, market: str = "SH") -> List[str]:
        return []

class XtQuantQMTClient(BaseQMTClient):
    """xtquant-backed QMT client.

Notes:
- We lazy-import xtquant to keep it optional.
- We serialize connect/query operations using a lock to reduce race conditions
  in typical FastAPI multi-worker/thread environments.
"""

    def __init__(
        self,
        *,
        enabled: bool,
        account_id: str | None,
        mode: str,
        userdata_path: str | None,
        session_id: int | None,
    ) -> None:
        self._enabled = bool(enabled)
        self._account_id = (account_id or "").strip() or None
        self._mode = (mode or "SIM").upper()
        self._userdata_path = (userdata_path or "").strip() or None
        self._session_id = session_id
        self._lock = threading.RLock()
        self._last_error: str | None = None

        self._xttrader_mod = None
        self._xttype_mod = None
        self._trader = None  # XtQuantTrader instance
        self._account = None  # StockAccount instance
        self._connected = False
        self._last_probe_ts: float = 0.0
        self._last_status_connected: Optional[bool] = None
        self._last_autoconnect_ts: float = 0.0
        self._active_tasks: Dict[str, Dict[str, Any]] = {}
        self._task_lock = threading.Lock()

    def _resolve_xtquant_dir(self) -> Optional[Path]:
        """Resolve xtquant directory.

        Priority:
        1) MINIQMT_XTQUANT_DIR (explicit override)
        2) Repo-bundled <repo_root>/xtquant
        """

        override = (os.getenv("MINIQMT_XTQUANT_DIR") or "").strip()
        if override:
            p = Path(override)
            if p.is_dir() and (p / "xttrader.py").exists() and (p / "__init__.py").exists():
                return p
        return self._resolve_repo_xtquant_dir()

    def _resolve_repo_xtquant_dir(self) -> Optional[Path]:
        """Try to locate repo-bundled xtquant directory."""

        try:
            repo_root = Path(__file__).resolve().parents[2]
        except Exception:
            return None
        candidate = repo_root / "xtquant"
        if (candidate / "xttrader.py").exists() and (candidate / "__init__.py").exists():
            return candidate
        return None

    def _ensure_xtquant(self) -> None:
        if self._xttrader_mod is not None and self._xttype_mod is not None:
            return
        try:
            xt_dir = self._resolve_xtquant_dir()
            if xt_dir is not None:
                repo_root = str(xt_dir.parent)
                if repo_root not in sys.path:
                    sys.path.insert(0, repo_root)
                if hasattr(os, "add_dll_directory"):
                    try:
                        os.add_dll_directory(str(xt_dir))
                    except Exception:
                        pass

            from xtquant import xttrader as xttrader_mod  # type: ignore
            from xtquant import xttype as xttype_mod  # type: ignore

            self._xttrader_mod = xttrader_mod
            self._xttype_mod = xttype_mod
        except Exception as e:  # noqa: BLE001
            raise QMTNotAvailableError(f"xtquant 导入失败: {e!r}") from e

    def connect(self) -> Tuple[bool, str]:
        if not self._enabled:
            self._connected = False
            return False, "QMT 未启用（MINIQMT_ENABLED=false）"

        if not self._account_id:
            self._connected = False
            return False, "未配置 MINIQMT_ACCOUNT_ID"

        if not self._userdata_path:
            self._connected = False
            return False, "未配置 MINIQMT_USERDATA_PATH（miniQMT 安装目录下的 userdata_mini 路径）"

        # session_id is required by XtQuantTrader; if not set, use a stable-ish default from PID.
        session_id = self._session_id
        if session_id is None:
            session_id = int(os.getpid() % 1000000)
            self._session_id = session_id

        import logging

        logger = logging.getLogger(self.__class__.__name__)
        prev_connected = bool(self._connected)

        with self._lock:
            try:
                self._ensure_xtquant()

                # 验证模块是否正确导入
                if self._xttrader_mod is None:
                    raise QMTNotAvailableError("xttrader 模块未正确导入，无法创建 XtQuantTrader")
                if self._xttype_mod is None:
                    raise QMTNotAvailableError("xttype 模块未正确导入，无法创建 StockAccount")
                
                # 验证参数（防御性编程）
                if not self._userdata_path:
                    raise ValueError("_userdata_path 为空，无法创建 XtQuantTrader")
                if session_id is None:
                    raise ValueError("session_id 为 None，无法创建 XtQuantTrader")
                
                # 验证 XtQuantTrader 类是否存在
                if not hasattr(self._xttrader_mod, 'XtQuantTrader'):
                    raise QMTNotAvailableError(f"xttrader 模块中没有 XtQuantTrader 类，可用属性: {dir(self._xttrader_mod)}")
                
                # Per bundled doc: XtQuantTrader(path, session_id)
                # 添加详细的错误信息以便诊断
                try:
                    self._trader = self._xttrader_mod.XtQuantTrader(self._userdata_path, session_id)
                except TypeError as e:
                    # 如果是参数错误，提供更详细的信息
                    raise TypeError(
                        f"创建 XtQuantTrader 失败: {e!r}\n"
                        f"参数检查: userdata_path={self._userdata_path!r} (type={type(self._userdata_path).__name__}), "
                        f"session_id={session_id!r} (type={type(session_id).__name__})\n"
                        f"XtQuantTrader 签名: {self._xttrader_mod.XtQuantTrader.__init__ if hasattr(self._xttrader_mod.XtQuantTrader, '__init__') else 'N/A'}"
                    ) from e
                
                self._account = self._xttype_mod.StockAccount(self._account_id)

                # Start API thread then connect.
                self._trader.start()

                connect_timeout_s = _env_float("MINIQMT_CONNECT_TIMEOUT_SECONDS", default=15.0)
                rc = _call_with_timeout(self._trader.connect, connect_timeout_s)
                if rc == 0:
                    # Subscribe for pushes (optional for query, but recommended by doc).
                    try:
                        self._trader.subscribe(self._account)
                    except Exception:
                        pass
                    self._connected = True
                    self._last_error = None
                    if not prev_connected:
                        logger.info("miniQMT 已连接，账户: %s", self._account_id)
                    return True, f"miniQMT 已连接，账户: {self._account_id}"

                # 某些环境下 rc=-1 可能由 session_id 冲突触发，尝试自动换 session 重连一次。
                if rc == -1 and _env_bool("MINIQMT_RETRY_NEW_SESSION_ON_RC_MINUS1", default=True):
                    retry_session_id = int(time.time() * 1000) % 1000000
                    if retry_session_id == session_id:
                        retry_session_id = (retry_session_id + 1) % 1000000
                    if retry_session_id <= 0:
                        retry_session_id = 1

                    logger.warning(
                        "miniQMT connect 返回 -1，尝试使用新 session_id 重连: old=%s, new=%s",
                        session_id,
                        retry_session_id,
                    )
                    try:
                        try:
                            stop_timeout_s = _env_float("MINIQMT_STOP_TIMEOUT_SECONDS", default=2.0)
                            _call_with_timeout(self._trader.stop, stop_timeout_s)
                        except Exception:
                            pass

                        self._trader = self._xttrader_mod.XtQuantTrader(self._userdata_path, retry_session_id)
                        self._trader.start()
                        rc2 = _call_with_timeout(self._trader.connect, connect_timeout_s)
                        if rc2 == 0:
                            try:
                                self._trader.subscribe(self._account)
                            except Exception:
                                pass
                            self._connected = True
                            self._last_error = None
                            self._session_id = retry_session_id
                            logger.info(
                                "miniQMT 已连接（自动重试成功），账户: %s, session_id: %s",
                                self._account_id,
                                retry_session_id,
                            )
                            return True, f"miniQMT 已连接，账户: {self._account_id}"
                        rc = rc2
                    except Exception as retry_error:  # noqa: BLE001
                        logger.warning("miniQMT 自动重试连接失败: %r", retry_error)

                self._connected = False
                self._last_error = f"miniQMT connect 失败，错误码: {rc}"
                if prev_connected:
                    logger.warning("miniQMT 连接已断开（connect 返回失败码: %s）", rc)
                # IMPORTANT: when connect fails, xtquant may have started background threads.
                # Stop and release the trader instance to avoid thread leaks and lock contention.
                try:
                    try:
                        stop_timeout_s = _env_float("MINIQMT_STOP_TIMEOUT_SECONDS", default=2.0)
                        _call_with_timeout(self._trader.stop, stop_timeout_s)
                    except Exception:
                        pass
                finally:
                    self._trader = None
                    self._account = None
                return False, self._last_error

            except TimeoutError as e:
                self._connected = False
                self._last_error = f"miniQMT connect 超时: {e!r}"
                try:
                    if self._trader is not None:
                        try:
                            stop_timeout_s = _env_float("MINIQMT_STOP_TIMEOUT_SECONDS", default=2.0)
                            _call_with_timeout(self._trader.stop, stop_timeout_s)
                        except Exception:
                            pass
                finally:
                    self._trader = None
                    self._account = None
                if prev_connected:
                    logger.warning("miniQMT 连接已断开（connect 超时）")
                return False, self._last_error

            except Exception as e:  # noqa: BLE001
                self._connected = False
                # 添加详细的错误信息，包括参数值（用于调试）
                import traceback
                error_details = f"连接 miniQMT 失败: {e!r}"
                if isinstance(e, TypeError) and "missing" in str(e):
                    # 如果是参数缺失错误，添加参数信息
                    error_details += (
                        f"\n参数检查: userdata_path={self._userdata_path!r} (type={type(self._userdata_path).__name__}), "
                        f"session_id={session_id!r} (type={type(session_id).__name__})\n"
                        f"模块检查: _xttrader_mod={self._xttrader_mod is not None}, "
                        f"_xttype_mod={self._xttype_mod is not None}\n"
                        f"错误堆栈:\n{traceback.format_exc()}"
                    )
                self._last_error = error_details
                # 使用 logging 记录详细错误（不输出到控制台，避免干扰）
                import logging
                logger = logging.getLogger(self.__class__.__name__)
                logger.error(f"连接 miniQMT 失败: {error_details}", exc_info=True)
                # 清理可能部分初始化的对象
                if self._trader is not None:
                    try:
                        try:
                            stop_timeout_s = _env_float("MINIQMT_STOP_TIMEOUT_SECONDS", default=2.0)
                            _call_with_timeout(self._trader.stop, stop_timeout_s)
                        except Exception:
                            pass
                    except Exception:
                        pass
                    finally:
                        self._trader = None
                self._account = None
                return False, self._last_error

    def _probe_connection_locked(self) -> bool:
        """Best-effort probe to detect miniQMT exit/disconnect.

        Must be called under self._lock.
        """

        if not self._enabled:
            self._connected = False
            return False

        if self._trader is None or self._account is None:
            self._connected = False

            # Optional: auto-connect on status() call, to support "start miniQMT then refresh page".
            auto_connect = _env_bool("MINIQMT_AUTO_CONNECT_ON_STATUS", default=True)
            if auto_connect and self._account_id and self._userdata_path:
                now = time.time()
                interval_s = _env_float("MINIQMT_STATUS_AUTOCONNECT_INTERVAL_SECONDS", default=30.0)
                if now - self._last_autoconnect_ts >= interval_s:
                    self._last_autoconnect_ts = now
                    ok, msg = self.connect()
                    if ok:
                        return True
                    self._last_error = msg

            return False

        # Rate limit probing to avoid hammering trader.
        # NOTE: Probing only happens when status() is called (e.g., UI open/refresh).
        # You can tune this to reduce overhead.
        probe_interval_s = _env_float("MINIQMT_STATUS_PROBE_INTERVAL_SECONDS", default=15.0)
        now = time.time()
        if now - self._last_probe_ts < probe_interval_s:
            return bool(self._connected)

        self._last_probe_ts = now
        try:
            # Any lightweight query should fail fast when miniQMT is gone.
            probe_timeout_s = _env_float("MINIQMT_PROBE_TIMEOUT_SECONDS", default=1.5)
            _call_with_timeout(lambda: self._trader.query_stock_asset(self._account), probe_timeout_s)
            self._connected = True
            self._last_error = None
            return True
        except TimeoutError as e:
            self._connected = False
            self._last_error = f"miniQMT 探测超时: {e!r}"
            return False
        except Exception as e:  # noqa: BLE001
            self._connected = False
            self._last_error = f"miniQMT 探测失败: {e!r}"
            return False

    def status(self) -> QMTStatus:
        import logging

        logger = logging.getLogger(self.__class__.__name__)
        with self._lock:
            connected_now = self._probe_connection_locked()
            if self._last_status_connected is None:
                self._last_status_connected = connected_now
            elif self._last_status_connected and (not connected_now):
                logger.warning("检测到 miniQMT 已断开/退出（status 探测失败）")
                self._last_status_connected = False
            elif (not self._last_status_connected) and connected_now:
                logger.info("检测到 miniQMT 已恢复连接")
                self._last_status_connected = True

            return QMTStatus(
                enabled=self._enabled,
                connected=bool(connected_now),
                mode=self._mode,
                account_id=self._account_id,
                provider="xtquant",
                userdata_path=self._userdata_path,
                session_id=self._session_id,
                last_error=self._last_error,
            )

    def _require_connected(self) -> None:
        if not self._enabled:
            raise QMTNotAvailableError("QMT 未启用（MINIQMT_ENABLED=false）")
        if not self._connected or self._trader is None or self._account is None:
            raise QMTNotAvailableError("miniQMT 未连接，请先调用 /api/v1/qmt/connect")

    def get_account_info(self) -> Dict[str, Any]:
        with self._lock:
            if not self._probe_connection_locked():
                raise QMTNotAvailableError(self._last_error or "miniQMT 未连接")
            self._require_connected()
            try:
                query_timeout_s = _env_float("MINIQMT_QUERY_TIMEOUT_SECONDS", default=2.0)
                asset = _call_with_timeout(lambda: self._trader.query_stock_asset(self._account), query_timeout_s)
                return {
                    "provider": "xtquant",
                    "connected": True,
                    "mode": self._mode,
                    "account_id": self._account_id,
                    "available_cash": float(getattr(asset, "cash", 0.0) or 0.0),
                    "total_asset": float(getattr(asset, "total_asset", 0.0) or 0.0),
                    "market_value": float(getattr(asset, "market_value", 0.0) or 0.0),
                    "frozen_cash": float(getattr(asset, "frozen_cash", 0.0) or 0.0),
                    "fetch_balance": float(getattr(asset, "fetch_balance", 0.0) or 0.0),
                }
            except Exception as e:  # noqa: BLE001
                raise QMTNotAvailableError(f"读取账户资金失败: {e!r}") from e

    def get_positions(self) -> List[Dict[str, Any]]:
        with self._lock:
            if not self._probe_connection_locked():
                raise QMTNotAvailableError(self._last_error or "miniQMT 未连接")
            self._require_connected()
            try:
                query_timeout_s = _env_float("MINIQMT_QUERY_TIMEOUT_SECONDS", default=2.0)
                positions = (
                    _call_with_timeout(lambda: self._trader.query_stock_positions(self._account), query_timeout_s)
                    or []
                )
                result: List[Dict[str, Any]] = []
                for pos in positions:
                    stock_code = getattr(pos, "stock_code", "") or ""
                    result.append(
                        {
                            "stock_code": str(stock_code),
                            "stock_name": str(getattr(pos, "instrument_name", "") or ""),
                            "quantity": int(getattr(pos, "volume", 0) or 0),
                            "can_sell": int(getattr(pos, "can_use_volume", 0) or 0),
                            "open_price": float(getattr(pos, "open_price", 0.0) or 0.0),
                            "cost_price": float(getattr(pos, "avg_price", 0.0) or 0.0),
                            "current_price": float(getattr(pos, "last_price", 0.0) or 0.0),
                            # 昨日收盘价：当前 xtquant 股票持仓结构未提供，暂固定为 0.0，仅保留字段以兼容前端
                            "prev_close": float(getattr(pos, "pre_close", 0.0) or 0.0),
                            "market_value": float(getattr(pos, "market_value", 0.0) or 0.0),
                            # position_profit: 持仓总盈亏金额（相对成本价的累计浮盈/浮亏）
                            "position_profit": float(getattr(pos, "position_profit", 0.0) or 0.0),
                            # float_profit: 当日盈亏金额（相对昨收的日内盈亏）
                            "float_profit": float(getattr(pos, "float_profit", 0.0) or 0.0),
                            # profit_rate: 持仓总盈亏比例（position_profit / 成本市值）
                            "profit_rate": float(getattr(pos, "profit_rate", 0.0) or 0.0),
                            "secu_account": str(getattr(pos, "secu_account", "") or ""),
                        }
                    )
                return result
            except Exception as e:  # noqa: BLE001
                raise QMTNotAvailableError(f"读取持仓失败: {e!r}") from e

    def get_orders(self, cancelable_only: bool = False) -> List[Dict[str, Any]]:
        with self._lock:
            if not self._probe_connection_locked():
                raise QMTNotAvailableError(self._last_error or "miniQMT 未连接")
            self._require_connected()
            try:
                query_timeout_s = _env_float("MINIQMT_QUERY_TIMEOUT_SECONDS", default=2.0)
                orders = (
                    _call_with_timeout(
                        lambda: self._trader.query_stock_orders(self._account, cancelable_only),
                        query_timeout_s,
                    )
                    or []
                )
                result: List[Dict[str, Any]] = []
                for order in orders:
                    # order_type: 23=买, 24=卖
                    order_type = getattr(order, "order_type", 0) or 0
                    result.append(
                        {
                            "order_id": str(getattr(order, "order_id", "") or ""),
                            "order_sysid": str(getattr(order, "order_sysid", "") or ""),
                            "stock_code": str(getattr(order, "stock_code", "") or ""),
                            "stock_name": str(getattr(order, "instrument_name", "") or ""),
                            "order_time": str(getattr(order, "order_time", "") or ""),
                            "order_type": int(order_type),
                            "order_type_name": "买入" if order_type == 23 else "卖出" if order_type == 24 else "未知",
                            "order_volume": int(getattr(order, "order_volume", 0) or 0),
                            "price_type": int(getattr(order, "price_type", 0) or 0),
                            "price": float(getattr(order, "price", 0.0) or 0.0),
                            "traded_volume": int(getattr(order, "traded_volume", 0) or 0),
                            "traded_price": float(getattr(order, "traded_price", 0.0) or 0.0),
                            "order_status": int(getattr(order, "order_status", 0) or 0),
                            "status_msg": str(getattr(order, "status_msg", "") or ""),
                            "strategy_name": str(getattr(order, "strategy_name", "") or ""),
                            "order_remark": str(getattr(order, "order_remark", "") or ""),
                            "secu_account": str(getattr(order, "secu_account", "") or ""),
                        }
                    )
                return result
            except Exception as e:  # noqa: BLE001
                raise QMTNotAvailableError(f"读取委托失败: {e!r}") from e

    def get_trades(self) -> List[Dict[str, Any]]:
        with self._lock:
            if not self._probe_connection_locked():
                raise QMTNotAvailableError(self._last_error or "miniQMT 未连接")
            self._require_connected()
            try:
                query_timeout_s = _env_float("MINIQMT_QUERY_TIMEOUT_SECONDS", default=2.0)
                trades = (
                    _call_with_timeout(lambda: self._trader.query_stock_trades(self._account), query_timeout_s)
                    or []
                )
                result: List[Dict[str, Any]] = []
                for trade in trades:
                    order_type = getattr(trade, "order_type", 0) or 0
                    result.append(
                        {
                            "traded_id": str(getattr(trade, "traded_id", "") or ""),
                            "stock_code": str(getattr(trade, "stock_code", "") or ""),
                            "stock_name": str(getattr(trade, "instrument_name", "") or ""),
                            "order_type": int(order_type),
                            "order_type_name": "买入" if order_type == 23 else "卖出" if order_type == 24 else "未知",
                            "traded_time": str(getattr(trade, "traded_time", "") or ""),
                            "traded_price": float(getattr(trade, "traded_price", 0.0) or 0.0),
                            "traded_volume": int(getattr(trade, "traded_volume", 0) or 0),
                            "traded_amount": float(getattr(trade, "traded_amount", 0.0) or 0.0),
                            "order_id": str(getattr(trade, "order_id", "") or ""),
                            "order_sysid": str(getattr(trade, "order_sysid", "") or ""),
                            "commission": float(getattr(trade, "commission", 0.0) or 0.0),
                            "strategy_name": str(getattr(trade, "strategy_name", "") or ""),
                            "order_remark": str(getattr(trade, "order_remark", "") or ""),
                            "secu_account": str(getattr(trade, "secu_account", "") or ""),
                        }
                    )
                return result
            except Exception as e:  # noqa: BLE001
                raise QMTNotAvailableError(f"读取成交失败: {e!r}") from e

    def get_local_data_range(self, stock_code: str, period: str) -> Dict[str, Any]:
        with self._lock:
            # 即使未连接交易端，只要 xtdata 路径正确也可以查询本地数据
            try:
                self._ensure_xtquant()
                from xtquant import xtdata
                
                # xtdata.get_local_data 返回的是 dict {field: DataFrame}
                # 我们只需要知道时间范围，所以取任意一个字段即可
                field_list = [] if period == "tick" else ["close"]
                res = xtdata.get_local_data(
                    field_list=field_list,
                    stock_list=[stock_code],
                    period=period,
                    count=-1
                )
                
                import logging
                logger = logging.getLogger(self.__class__.__name__)
                logger.info(f"get_local_data 返回: {res}")
                
                if not res:
                    logger.warning(f"get_local_data 返回空: {res}")
                    return {"start": None, "end": None, "count": 0}

                if period == "tick":
                    tick_data = res.get(stock_code)
                    if tick_data is None:
                        logger.warning(f"tick 数据不存在: {res}")
                        return {"start": None, "end": None, "count": 0}
                    if getattr(tick_data, "size", 0) == 0:
                        logger.warning("tick 数据为空")
                        return {"start": None, "end": None, "count": 0}

                    times = None
                    if getattr(tick_data, "dtype", None) is not None and tick_data.dtype.names:
                        if "time" in tick_data.dtype.names:
                            times = tick_data["time"]
                    if times is None:
                        if len(getattr(tick_data, "shape", ())) > 1:
                            times = tick_data[:, 0]
                        else:
                            times = tick_data
                    if times is None or len(times) == 0:
                        logger.warning("tick 时间列解析失败")
                        return {"start": None, "end": None, "count": 0}

                    logger.info(f"数据范围: {times[0]} ~ {times[-1]}, 共 {len(times)} 条")
                    return {
                        "start": str(times[0]),
                        "end": str(times[-1]),
                        "count": len(times)
                    }

                df = None
                if "close" in res:
                    df = res["close"]
                elif stock_code in res:
                    stock_value = res.get(stock_code)
                    if isinstance(stock_value, dict):
                        if "close" in stock_value:
                            df = stock_value.get("close")
                        elif stock_value:
                            df = next(iter(stock_value.values()))
                    else:
                        df = stock_value

                if df is None:
                    logger.warning(f"get_local_data 返回无法解析数据结构: {res}")
                    return {"start": None, "end": None, "count": 0}
                if hasattr(df, "shape"):
                    logger.info(f"DataFrame shape: {df.shape}, columns: {getattr(df, 'columns', [])}")

                if hasattr(df, "empty") and df.empty:
                    logger.warning("DataFrame 为空")
                    return {"start": None, "end": None, "count": 0}

                times = None
                if hasattr(df, "columns"):
                    columns = df.columns.tolist()
                    if columns:
                        times = columns
                if times is None and hasattr(df, "index"):
                    index = df.index.tolist()
                    if index:
                        times = index
                if times is None and hasattr(df, "__len__"):
                    times = list(df)
                if not times:
                    logger.warning("DataFrame 没有可用时间列")
                    return {"start": None, "end": None, "count": 0}

                logger.info(f"数据范围: {times[0]} ~ {times[-1]}, 共 {len(times)} 条")

                return {
                    "start": str(times[0]),
                    "end": str(times[-1]),
                    "count": len(times)
                }
            except Exception as e:
                import logging
                logger = logging.getLogger(self.__class__.__name__)
                logger.error(f"查询本地数据范围失败 ({stock_code}, {period}): {e}", exc_info=True)
                return {"start": None, "end": None, "count": 0}

    def download_history_data(
        self, 
        stock_list: List[str], 
        period: str, 
        start_time: str = "", 
        end_time: str = "",
        task_id: str | None = None
    ) -> None:
        with self._lock:
            self._ensure_xtquant()
            from xtquant import xtdata
            
            total_count = len(stock_list)
            finished_count = 0

            def on_progress(data: Any) -> None:
                nonlocal finished_count
                if not task_id:
                    return
                
                # xtdata 回调数据通常是一个字典，包含当前处理的股票信息
                # 这里的逻辑是每完成一个股票，进度加一
                with self._task_lock:
                    finished_count += 1
                    progress = int((finished_count / total_count) * 100) if total_count > 0 else 100
                    self._active_tasks[task_id] = {
                        "status": "downloading",
                        "progress": progress,
                        "finished": finished_count,
                        "total": total_count,
                        "last_stock": data.get("stock_code") if isinstance(data, dict) else None,
                        "updated_at": time.time()
                    }

            # 初始化任务状态
            if task_id:
                with self._task_lock:
                    self._active_tasks[task_id] = {
                        "status": "pending",
                        "progress": 0,
                        "finished": 0,
                        "total": total_count,
                        "updated_at": time.time()
                    }

            try:
                # 使用 download_history_data2 支持批量下载和回调
                xtdata.download_history_data2(
                    stock_list=stock_list,
                    period=period,
                    start_time=start_time,
                    end_time=end_time,
                    callback=on_progress
                )
                
                # 标记完成
                if task_id:
                    with self._task_lock:
                        self._active_tasks[task_id]["status"] = "success"
                        self._active_tasks[task_id]["progress"] = 100
                        self._active_tasks[task_id]["updated_at"] = time.time()
            except Exception as e:
                if task_id:
                    with self._task_lock:
                        self._active_tasks[task_id]["status"] = "failed"
                        self._active_tasks[task_id]["error"] = str(e)
                        self._active_tasks[task_id]["updated_at"] = time.time()
                raise

    def download_financial_data(self, stock_list: List[str], table_list: List[str], task_id: str | None = None) -> None:
        with self._lock:
            self._ensure_xtquant()
            from xtquant import xtdata
            
            if task_id:
                with self._task_lock:
                    self._active_tasks[task_id] = {
                        "status": "downloading",
                        "progress": 20,
                        "updated_at": time.time()
                    }

            try:
                # 财务数据下载（xtquant 暂不支持财务数据的细粒度进度回调）
                xtdata.download_financial_data(
                    stock_list=stock_list,
                    table_list=table_list
                )
                
                if task_id:
                    with self._task_lock:
                        self._active_tasks[task_id]["status"] = "success"
                        self._active_tasks[task_id]["progress"] = 100
                        self._active_tasks[task_id]["updated_at"] = time.time()
            except Exception as e:
                if task_id:
                    with self._task_lock:
                        self._active_tasks[task_id]["status"] = "failed"
                        self._active_tasks[task_id]["error"] = str(e)
                        self._active_tasks[task_id]["updated_at"] = time.time()
                raise

    def get_task_progress(self, task_id: str) -> Dict[str, Any]:
        with self._task_lock:
            # 返回任务进度，如果没有则返回 404 语义的空字典
            return self._active_tasks.get(task_id, {})

    def update_task_status(self, task_id: str, status_data: Dict[str, Any]) -> None:
        with self._task_lock:
            if task_id not in self._active_tasks:
                self._active_tasks[task_id] = {}
            self._active_tasks[task_id].update(status_data)
            self._active_tasks[task_id]["updated_at"] = time.time()

    def get_full_tick(self, stock_list: List[str]) -> Dict[str, Any]:
        with self._lock:
            try:
                self._ensure_xtquant()
                from xtquant import xtdata

                timeout_s = _env_float("MINIQMT_QUERY_TIMEOUT_SECONDS", default=2.0)
                return _call_with_timeout(lambda: xtdata.get_full_tick(stock_list), timeout_s) or {}
            except Exception as e:  # noqa: BLE001
                raise QMTNotAvailableError(f"?? miniQMT tick ????: {e!r}") from e

    def get_latest_trading_day(self) -> str:
        """获取最新交易日，带有超时保护."""
        import datetime
        with self._lock:
            try:
                self._ensure_xtquant()
                from xtquant import xtdata
                
                # 尝试获取交易日历 (3秒超时)
                try:
                    calendar = _call_with_timeout(lambda: xtdata.get_trading_calendar("SH"), timeout_s=3.0)
                    if calendar:
                        return str(calendar[-1])
                except Exception:
                    pass
                
                # 尝试获取最新行情的时间 (3秒超时)
                try:
                    snapshot = _call_with_timeout(lambda: xtdata.get_full_tick(["000001.SH"]), timeout_s=3.0)
                    if snapshot and "000001.SH" in snapshot:
                        last_time = snapshot["000001.SH"].get("timetag", "")
                        if last_time:
                            return str(last_time[:8])
                except Exception:
                    pass
                
                return datetime.date.today().strftime("%Y%m%d")
            except Exception as e:
                logger = logging.getLogger(self.__class__.__name__)
                logger.error(f"获取最新交易日失败: {e}")
                return datetime.date.today().strftime("%Y%m%d")

    def get_stock_list_in_sector(self, sector_name: str) -> List[str]:
        with self._lock:
            try:
                self._ensure_xtquant()
                from xtquant import xtdata
                return xtdata.get_stock_list_in_sector(sector_name) or []
            except Exception:
                return []

    def get_trading_calendar(self, market: str = "SH") -> List[str]:
        with self._lock:
            try:
                self._ensure_xtquant()
                from xtquant import xtdata
                return xtdata.get_trading_calendar(market) or []
            except Exception:
                return []

    def place_order(
        self,
        stock_code: str,
        order_type: int,
        order_volume: int,
        price_type: int,
        price: float = 0.0,
        strategy_name: str = "",
        order_remark: str = "",
    ) -> Tuple[int, str]:
        with self._lock:
            self._require_connected()
            try:
                self._ensure_xtquant()
                order_id = self._trader.order_stock(
                    self._account,
                    stock_code,
                    order_type,
                    order_volume,
                    price_type,
                    price,
                    strategy_name,
                    order_remark,
                )
                if order_id > 0:
                    return order_id, f"下单成功，订单编号：{order_id}"
                else:
                    return -1, "下单失败"
            except Exception as e:  # noqa: BLE001
                return -1, f"下单失败: {e!r}"

    def cancel_order(self, order_id: str) -> Tuple[bool, str]:
        with self._lock:
            self._require_connected()
            try:
                order_id_int = int(order_id)
                result = self._trader.cancel_order_stock(self._account, order_id_int)
                if result == 0:
                    return True, "撤单成功"
                else:
                    return False, "撤单失败"
            except Exception as e:  # noqa: BLE001
                return False, f"撤单失败: {e!r}"

    def cancel_order_by_sysid(self, market: int, order_sysid: str) -> Tuple[bool, str]:
        with self._lock:
            self._require_connected()
            try:
                result = self._trader.cancel_order_stock_sysid(self._account, market, order_sysid)
                if result == 0:
                    return True, "撤单成功"
                else:
                    return False, "撤单失败"
            except Exception as e:  # noqa: BLE001
                return False, f"撤单失败: {e!r}"

    def query_new_purchase_limit(self) -> Dict[str, Any]:
        with self._lock:
            self._require_connected()
            try:
                limit = self._trader.query_new_purchase_limit(self._account)
                if limit:
                    return {
                        "account_id": str(getattr(limit, "account_id", "") or ""),
                        "market": int(getattr(limit, "market", 0) or 0),
                        "purchase_limit": float(getattr(limit, "purchase_limit", 0.0) or 0.0),
                    }
                return {}
            except Exception as e:  # noqa: BLE001
                raise QMTNotAvailableError(f"查询新股申购额度失败: {e!r}") from e

    def query_ipo_data(self) -> List[Dict[str, Any]]:
        with self._lock:
            self._require_connected()
            try:
                # query_ipo_data 返回字典，键是股票代码
                ipo_dict = self._trader.query_ipo_data() or {}
                result: List[Dict[str, Any]] = []
                for stock_code, ipo_info in ipo_dict.items():
                    if isinstance(ipo_info, dict):
                        result.append(
                            {
                                "stock_code": str(stock_code),
                                "stock_name": str(ipo_info.get("name", "") or ""),
                                "issue_price": float(ipo_info.get("issuePrice", 0.0) or 0.0),
                                "purchase_limit": float(ipo_info.get("maxPurchaseNum", 0.0) or 0.0),
                                "purchase_date": str(ipo_info.get("purchaseDate", "") or ""),
                                "ipo_type": str(ipo_info.get("type", "") or ""),
                            }
                        )
                return result
            except Exception as e:  # noqa: BLE001
                raise QMTNotAvailableError(f"查询新股信息失败: {e!r}") from e

    def bank_transfer_in(
        self, bank_no: str, bank_account: str, bank_pwd: str, amount: float
    ) -> Tuple[bool, str]:
        with self._lock:
            self._require_connected()
            try:
                result = self._trader.bank_transfer_in(
                    self._account, bank_no, bank_account, bank_pwd, amount
                )
                if isinstance(result, tuple) and len(result) == 2:
                    success, msg = result
                    return bool(success), str(msg)
                return False, "转账失败"
            except Exception as e:  # noqa: BLE001
                return False, f"银行转证券失败: {e!r}"

    def bank_transfer_out(
        self, bank_no: str, bank_account: str, bank_pwd: str, amount: float
    ) -> Tuple[bool, str]:
        with self._lock:
            self._require_connected()
            try:
                result = self._trader.bank_transfer_out(
                    self._account, bank_no, bank_account, bank_pwd, amount
                )
                if isinstance(result, tuple) and len(result) == 2:
                    success, msg = result
                    return bool(success), str(msg)
                return False, "转账失败"
            except Exception as e:  # noqa: BLE001
                return False, f"证券转银行失败: {e!r}"

    def query_bank_info(self) -> List[Dict[str, Any]]:
        with self._lock:
            self._require_connected()
            try:
                bank_info_list = self._trader.query_bank_info(self._account) or []
                result: List[Dict[str, Any]] = []
                for bank_info in bank_info_list:
                    result.append(
                        {
                            "money_type": str(getattr(bank_info, "money_type", "") or ""),
                            "bank_no": str(getattr(bank_info, "bank_no", "") or ""),
                            "bank_account": str(getattr(bank_info, "bank_account", "") or ""),
                            "bank_name": str(getattr(bank_info, "bank_name", "") or ""),
                        }
                    )
                return result
            except Exception as e:  # noqa: BLE001
                raise QMTNotAvailableError(f"查询银行信息失败: {e!r}") from e


def _env_bool(key: str, default: bool = False) -> bool:
    v = (os.getenv(key) or "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on"}


def build_qmt_client_from_env() -> BaseQMTClient:
    """Factory: build a QMT client from environment variables.

Env keys (existing + new optional):
- MINIQMT_ENABLED: true/false
- MINIQMT_ACCOUNT_ID: account id (sim or live)
- MINIQMT_MODE: SIM/LIVE (optional, default SIM)
- MINIQMT_USERDATA_PATH: miniQMT安装目录下的userdata_mini路径（必需）
- MINIQMT_SESSION_ID: 会话ID（可选，如果不提供则使用PID）

注意：如果MINIQMT_ENABLED=true，必须正确配置所有必需参数，否则会抛出异常。
不允许fallback到模拟模式。
"""

    enabled = _env_bool("MINIQMT_ENABLED", default=False)
    
    # 如果未启用，返回模拟客户端（仅用于未启用的情况）
    if not enabled:
        return SimulatorQMTClient(
            enabled=False, 
            account_id=None, 
            mode="SIM", 
            reason="QMT未启用（MINIQMT_ENABLED=false）"
        )
    
    # 如果启用了QMT，必须正确配置所有参数
    account_id = (os.getenv("MINIQMT_ACCOUNT_ID") or "").strip() or None
    mode = (os.getenv("MINIQMT_MODE") or "SIM").strip().upper()
    userdata_path = (os.getenv("MINIQMT_USERDATA_PATH") or "").strip() or None
    session_id_raw = (os.getenv("MINIQMT_SESSION_ID") or "").strip()
    session_id: int | None = None
    if session_id_raw:
        try:
            session_id = int(session_id_raw)
        except Exception:
            session_id = None

    # 验证必需参数
    if not account_id:
        raise ValueError(
            "MINIQMT_ENABLED=true 但未配置 MINIQMT_ACCOUNT_ID。"
            "请检查 .env 文件中的配置。"
        )
    
    if not userdata_path:
        raise ValueError(
            "MINIQMT_ENABLED=true 但未配置 MINIQMT_USERDATA_PATH。"
            "请设置 miniQMT 安装目录下的 userdata_mini 路径。"
            "例如: MINIQMT_USERDATA_PATH=F:\\国金QMT交易端模拟\\userdata_mini"
        )
    
    # 如果路径不存在，给出明确错误
    if not os.path.exists(userdata_path):
        raise ValueError(
            f"MINIQMT_USERDATA_PATH 指定的路径不存在: {userdata_path}\n"
            f"请检查路径是否正确，或确认 miniQMT 已正确安装。"
        )

    # 创建客户端（如果失败，直接抛出异常，不允许fallback）
    return XtQuantQMTClient(
        enabled=enabled,
        account_id=account_id,
        mode=mode,
        userdata_path=userdata_path,
        session_id=session_id,
    )
