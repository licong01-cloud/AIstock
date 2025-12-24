from __future__ import annotations

"""QMT (miniQMT) client abstraction for AIstock.

Design goals:
- Keep `xtquant` imports isolated and optional so backend can boot without it.
- Provide a stable, minimal interface for "sim account snapshot" (cash + positions).
- Allow future evolution into a dedicated gateway process without touching callers.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import os
from pathlib import Path
import sys
import threading


class QMTNotAvailableError(RuntimeError):
    """Raised when xtquant is missing or QMT connection is unavailable."""


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

    def get_orders(self, cancelable_only: bool = False) -> List[Dict[str, Any]]:
        """查询当日委托列表"""
        raise NotImplementedError

    def get_trades(self) -> List[Dict[str, Any]]:
        """查询当日成交列表"""
        raise NotImplementedError

    def place_order(
        self,
        stock_code: str,
        order_type: int,  # 23=买入, 24=卖出
        order_volume: int,
        price_type: int,  # 限价/市价等
        price: float = 0.0,
        strategy_name: str = "",
        order_remark: str = "",
    ) -> Tuple[int, str]:
        """下单
        Returns: (order_id, message)
        - order_id > 0: 成功，返回订单编号
        - order_id == -1: 失败
        """
        raise NotImplementedError

    def cancel_order(self, order_id: str) -> Tuple[bool, str]:
        """根据订单编号撤单
        Returns: (success, message)
        """
        raise NotImplementedError

    def cancel_order_by_sysid(self, market: int, order_sysid: str) -> Tuple[bool, str]:
        """根据柜台合同编号撤单
        Returns: (success, message)
        """
        raise NotImplementedError

    def query_new_purchase_limit(self) -> Dict[str, Any]:
        """查询新股申购额度"""
        raise NotImplementedError

    def query_ipo_data(self) -> List[Dict[str, Any]]:
        """查询新股信息"""
        raise NotImplementedError

    def bank_transfer_in(
        self, bank_no: str, bank_account: str, bank_pwd: str, amount: float
    ) -> Tuple[bool, str]:
        """银行转证券"""
        raise NotImplementedError

    def bank_transfer_out(
        self, bank_no: str, bank_account: str, bank_pwd: str, amount: float
    ) -> Tuple[bool, str]:
        """证券转银行"""
        raise NotImplementedError

    def query_bank_info(self) -> List[Dict[str, Any]]:
        """查询银行信息"""
        raise NotImplementedError


class SimulatorQMTClient(BaseQMTClient):
    """Fallback simulator: returns zero assets and empty positions.

This is NOT a trading simulator; it's a safe placeholder so the backend can run
when xtquant/miniQMT are not installed.
"""

    def __init__(self, *, enabled: bool, account_id: str | None, mode: str, reason: str | None):
        self._enabled = bool(enabled)
        self._connected = False
        self._account_id = account_id
        self._mode = mode or "SIM"
        self._reason = reason

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

    def get_orders(self, cancelable_only: bool = False) -> List[Dict[str, Any]]:
        return []

    def get_trades(self) -> List[Dict[str, Any]]:
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
        return -1, "xtquant 未安装或不可用"

    def cancel_order(self, order_id: str) -> Tuple[bool, str]:
        return False, "xtquant 未安装或不可用"

    def cancel_order_by_sysid(self, market: int, order_sysid: str) -> Tuple[bool, str]:
        return False, "xtquant 未安装或不可用"

    def query_new_purchase_limit(self) -> Dict[str, Any]:
        return {}

    def query_ipo_data(self) -> List[Dict[str, Any]]:
        return []

    def bank_transfer_in(
        self, bank_no: str, bank_account: str, bank_pwd: str, amount: float
    ) -> Tuple[bool, str]:
        return False, "xtquant 未安装或不可用"

    def bank_transfer_out(
        self, bank_no: str, bank_account: str, bank_pwd: str, amount: float
    ) -> Tuple[bool, str]:
        return False, "xtquant 未安装或不可用"

    def query_bank_info(self) -> List[Dict[str, Any]]:
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
        """Try to locate repo-bundled xtquant directory.

Repo layout in this workspace: <repo_root>/xtquant/xttrader.py ...
"""
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
            # 1) Ensure repo-bundled xtquant can be imported (if present)
            xt_dir = self._resolve_xtquant_dir()
            if xt_dir is not None:
                repo_root = str(xt_dir.parent)
                if repo_root not in sys.path:
                    sys.path.insert(0, repo_root)
                # Python 3.8+ requires adding DLL dir for dependent .dll/.pyd loading in some setups.
                if hasattr(os, "add_dll_directory"):
                    try:
                        os.add_dll_directory(str(xt_dir))
                    except Exception:
                        # Best effort; some environments may restrict.
                        pass

            # 2) Import modules per bundled doc: xtquant.xttrader / xtquant.xttype
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
                rc = self._trader.connect()
                if rc == 0:
                    # Subscribe for pushes (optional for query, but recommended by doc).
                    try:
                        self._trader.subscribe(self._account)
                    except Exception:
                        pass
                    self._connected = True
                    self._last_error = None
                    return True, f"miniQMT 已连接，账户: {self._account_id}"

                self._connected = False
                self._last_error = f"miniQMT connect 失败，错误码: {rc}"
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
                            self._trader.stop()
                        except Exception:
                            pass
                    except Exception:
                        pass
                    finally:
                        self._trader = None
                self._account = None
                return False, self._last_error

    def disconnect(self) -> Tuple[bool, str]:
        with self._lock:
            try:
                if self._trader is not None:
                    try:
                        self._trader.stop()
                    except Exception:
                        pass
                    # 清理 trader 引用，避免析构时的错误
                    self._trader = None
                self._account = None
                self._connected = False
                return True, "已断开 miniQMT"
            except Exception as e:  # noqa: BLE001
                self._connected = False
                self._last_error = f"断开 miniQMT 失败: {e!r}"
                return False, self._last_error

    def status(self) -> QMTStatus:
        return QMTStatus(
            enabled=self._enabled,
            connected=bool(self._connected),
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
            self._require_connected()
            try:
                asset = self._trader.query_stock_asset(self._account)
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
            self._require_connected()
            try:
                positions = self._trader.query_stock_positions(self._account) or []
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
                            "market_value": float(getattr(pos, "market_value", 0.0) or 0.0),
                            "float_profit": float(getattr(pos, "float_profit", 0.0) or 0.0),
                            "profit_rate": float(getattr(pos, "profit_rate", 0.0) or 0.0),
                            "secu_account": str(getattr(pos, "secu_account", "") or ""),
                        }
                    )
                return result
            except Exception as e:  # noqa: BLE001
                raise QMTNotAvailableError(f"读取持仓失败: {e!r}") from e

    def get_orders(self, cancelable_only: bool = False) -> List[Dict[str, Any]]:
        with self._lock:
            self._require_connected()
            try:
                orders = self._trader.query_stock_orders(self._account, cancelable_only) or []
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
            self._require_connected()
            try:
                trades = self._trader.query_stock_trades(self._account) or []
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
                from xtquant import xtconstant  # type: ignore

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


