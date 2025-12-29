from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from ..infra.qmt_client import BaseQMTClient, get_qmt_client_singleton


_CONFIG_PATH = Path(__file__).resolve().parents[1] / "qmt_monitor_config.json"


@dataclass
class GlobalThresholds:
    """账户级 QMT 监控阈值配置.

    注意：max_total_drawdown / max_daily_loss 为**相对总资产的比例阈值**，例如
    -0.10 表示 -10% 的回撤或亏损，而非绝对金额.
    """

    max_total_drawdown: float = -0.10  # 总持仓盈亏 / 总资产 低于该比例触发告警
    max_daily_loss: float = -0.03  # 当日盈亏 / 总资产 低于该比例触发告警
    max_position_weight: float = 0.3  # 单票市值占总资产超过该比例触发告警
    min_available_cash_ratio: float = 0.05  # 可用资金占总资产低于该比例触发告警


@dataclass
class SymbolThresholds:
    """单票级 QMT 监控阈值配置."""

    max_daily_loss: Optional[float] = None
    max_position_loss: Optional[float] = None
    max_weight: Optional[float] = None


@dataclass
class MonitorConfig:
    global_thresholds: GlobalThresholds = field(default_factory=GlobalThresholds)
    per_symbol: Dict[str, SymbolThresholds] = field(default_factory=dict)


class GlobalThresholdsModel(BaseModel):
    max_total_drawdown: float
    max_daily_loss: float
    max_position_weight: float
    min_available_cash_ratio: float


class SymbolThresholdsModel(BaseModel):
    max_daily_loss: Optional[float] = None
    max_position_loss: Optional[float] = None
    max_weight: Optional[float] = None


class MonitorConfigModel(BaseModel):
    global_: GlobalThresholdsModel
    per_symbol: Dict[str, SymbolThresholdsModel] = {}

    class Config:
        fields = {"global_": "global"}


def _load_config() -> MonitorConfig:
    if not _CONFIG_PATH.exists():
        return MonitorConfig()
    try:
        data = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return MonitorConfig()

    g = data.get("global", {}) or {}
    global_cfg = GlobalThresholds(
        # 兼容旧配置：旧值会被直接当作比例使用
        max_total_drawdown=float(g.get("max_total_drawdown", -0.10)),
        max_daily_loss=float(g.get("max_daily_loss", -0.03)),
        max_position_weight=float(g.get("max_position_weight", 0.3)),
        min_available_cash_ratio=float(g.get("min_available_cash_ratio", 0.05)),
    )

    per_symbol: Dict[str, SymbolThresholds] = {}
    for code, cfg in (data.get("per_symbol") or {}).items():
        if not isinstance(cfg, dict):
            continue
        per_symbol[code] = SymbolThresholds(
            max_daily_loss=cfg.get("max_daily_loss"),
            max_position_loss=cfg.get("max_position_loss"),
            max_weight=cfg.get("max_weight"),
        )

    return MonitorConfig(global_thresholds=global_cfg, per_symbol=per_symbol)


def _save_config(cfg: MonitorConfig) -> None:
    data: Dict[str, Any] = {
        "global": asdict(cfg.global_thresholds),
        "per_symbol": {code: asdict(th) for code, th in cfg.per_symbol.items()},
    }
    try:
        _CONFIG_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # 配置写入失败不应影响主流程
        pass


def get_monitor_config_model() -> MonitorConfigModel:
    cfg = _load_config()
    return MonitorConfigModel(
        global_=GlobalThresholdsModel(**asdict(cfg.global_thresholds)),
        per_symbol={code: SymbolThresholdsModel(**asdict(th)) for code, th in cfg.per_symbol.items()},
    )


def update_monitor_config_model(payload: MonitorConfigModel) -> MonitorConfigModel:
    cfg = _load_config()

    g = payload.global_
    cfg.global_thresholds = GlobalThresholds(
        max_total_drawdown=g.max_total_drawdown,
        max_daily_loss=g.max_daily_loss,
        max_position_weight=g.max_position_weight,
        min_available_cash_ratio=g.min_available_cash_ratio,
    )

    per_symbol: Dict[str, SymbolThresholds] = {}
    for code, th in payload.per_symbol.items():
        per_symbol[code] = SymbolThresholds(
            max_daily_loss=th.max_daily_loss,
            max_position_loss=th.max_position_loss,
            max_weight=th.max_weight,
        )
    cfg.per_symbol = per_symbol

    _save_config(cfg)
    return get_monitor_config_model()


def _get_qmt_client() -> BaseQMTClient:
    return get_qmt_client_singleton()


def _load_strategy_configs() -> Dict[str, Dict[str, Any]]:
    """从策略管理模块同源的数据库表中加载策略配置.

    返回字典: {strategy_id: {"config": dict, "schedule_config": dict, "risk_config": dict}}
    这里只是为了监控用途，主要关心 config.symbols 列表。
    """

    try:
        from ..db.pg_pool import get_conn  # 延迟导入，避免循环依赖
        import json

        strategies: Dict[str, Dict[str, Any]] = {}

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT strategy_id, config_json, schedule_config, risk_config
                    FROM trading.strategy_config
                    WHERE enabled = TRUE
                    """
                )
                rows = cur.fetchall()

        def _parse_jsonb(value: Any) -> Dict[str, Any]:
            if isinstance(value, dict):
                return value
            if isinstance(value, str):
                return json.loads(value) if value else {}
            return {}

        for row in rows:
            sid = row[0]
            cfg = _parse_jsonb(row[1])
            sched_cfg = _parse_jsonb(row[2])
            risk_cfg = _parse_jsonb(row[3])
            strategies[sid] = {
                "config": cfg,
                "schedule_config": sched_cfg,
                "risk_config": risk_cfg,
            }

        return strategies
    except Exception:
        # 数据库不可用或表不存在时，不影响监控主流程
        return {}


def build_monitor_summary() -> Dict[str, Any]:
    """构建基于当前 QMT 快照的账户级监控摘要与告警列表."""

    client = _get_qmt_client()
    cfg = _load_config()

    account = client.get_account_info()
    positions = client.get_positions()

    total_asset = float(account.get("total_asset", 0.0) or 0.0)
    available_cash = float(account.get("available_cash", 0.0) or 0.0)

    total_mv = sum(float(p.get("market_value", 0.0) or 0.0) for p in positions)
    total_position_profit = sum(float(p.get("position_profit", 0.0) or 0.0) for p in positions)
    total_daily_profit = sum(float(p.get("float_profit", 0.0) or 0.0) for p in positions)

    alerts: List[Dict[str, Any]] = []

    # 全局阈值检查
    gt = cfg.global_thresholds

    # 使用相对总资产的比例来判断回撤 / 当日亏损
    total_position_profit_ratio = (total_position_profit / total_asset) if total_asset > 0 else 0.0
    total_daily_profit_ratio = (total_daily_profit / total_asset) if total_asset > 0 else 0.0

    if total_position_profit_ratio < gt.max_total_drawdown:
        alerts.append(
            {
                "level": "CRITICAL",
                "type": "TOTAL_DRAWDOWN",
                "message": (
                    f"总持仓回撤比例低于 {gt.max_total_drawdown:.2%}: "
                    f"当前 {total_position_profit_ratio:.2%} (绝对值 {total_position_profit:.2f})"
                ),
                "value": total_position_profit_ratio,
                "threshold": gt.max_total_drawdown,
            }
        )

    if total_daily_profit_ratio < gt.max_daily_loss:
        alerts.append(
            {
                "level": "CRITICAL",
                "type": "TOTAL_DAILY_LOSS",
                "message": (
                    f"当日亏损比例低于 {gt.max_daily_loss:.2%}: "
                    f"当前 {total_daily_profit_ratio:.2%} (绝对值 {total_daily_profit:.2f})"
                ),
                "value": total_daily_profit_ratio,
                "threshold": gt.max_daily_loss,
            }
        )

    cash_ratio = (available_cash / total_asset) if total_asset > 0 else 0.0
    if cash_ratio < gt.min_available_cash_ratio:
        alerts.append(
            {
                "level": "WARNING",
                "type": "LOW_AVAILABLE_CASH",
                "message": f"可用资金比例过低: 当前 {cash_ratio:.2%}, 阈值 {gt.min_available_cash_ratio:.2%}",
                "value": cash_ratio,
                "threshold": gt.min_available_cash_ratio,
            }
        )

    # 单票阈值检查
    positions_out: List[Dict[str, Any]] = []

    for p in positions:
        code = str(p.get("stock_code", ""))
        mv = float(p.get("market_value", 0.0) or 0.0)
        pos_profit = float(p.get("position_profit", 0.0) or 0.0)
        daily_profit = float(p.get("float_profit", 0.0) or 0.0)

        weight_asset = (mv / total_asset) if total_asset > 0 else 0.0
        weight_mv = (mv / total_mv) if total_mv > 0 else 0.0

        symbol_cfg = cfg.per_symbol.get(code)
        symbol_alerts: List[str] = []

        # 全局权重阈值
        if weight_asset > gt.max_position_weight:
            symbol_alerts.append("SYMBOL_WEIGHT")
            alerts.append(
                {
                    "level": "WARNING",
                    "type": "SYMBOL_WEIGHT",
                    "stock_code": code,
                    "message": f"{code} 资产占比 {weight_asset:.2%} 超过阈值 {gt.max_position_weight:.2%}",
                    "value": weight_asset,
                    "threshold": gt.max_position_weight,
                }
            )

        if symbol_cfg:
            if symbol_cfg.max_daily_loss is not None and daily_profit < symbol_cfg.max_daily_loss:
                symbol_alerts.append("SYMBOL_DAILY_LOSS")
                alerts.append(
                    {
                        "level": "WARNING",
                        "type": "SYMBOL_DAILY_LOSS",
                        "stock_code": code,
                        "message": f"{code} 当日盈亏 {daily_profit:.2f} 低于阈值 {symbol_cfg.max_daily_loss:.2f}",
                        "value": daily_profit,
                        "threshold": symbol_cfg.max_daily_loss,
                    }
                )

            if symbol_cfg.max_position_loss is not None and pos_profit < symbol_cfg.max_position_loss:
                symbol_alerts.append("SYMBOL_POSITION_LOSS")
                alerts.append(
                    {
                        "level": "WARNING",
                        "type": "SYMBOL_POSITION_LOSS",
                        "stock_code": code,
                        "message": f"{code} 持仓盈亏 {pos_profit:.2f} 低于阈值 {symbol_cfg.max_position_loss:.2f}",
                        "value": pos_profit,
                        "threshold": symbol_cfg.max_position_loss,
                    }
                )

            if symbol_cfg.max_weight is not None and weight_asset > symbol_cfg.max_weight:
                symbol_alerts.append("SYMBOL_WEIGHT_LOCAL")
                alerts.append(
                    {
                        "level": "WARNING",
                        "type": "SYMBOL_WEIGHT_LOCAL",
                        "stock_code": code,
                        "message": f"{code} 资产占比 {weight_asset:.2%} 超过单票阈值 {symbol_cfg.max_weight:.2%}",
                        "value": weight_asset,
                        "threshold": symbol_cfg.max_weight,
                    }
                )

        p_out = dict(p)
        p_out["weight_asset"] = weight_asset
        p_out["weight_market_value"] = weight_mv
        p_out["alerts"] = symbol_alerts
        positions_out.append(p_out)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "account": {
            "total_asset": total_asset,
            "market_value": total_mv,
            "available_cash": available_cash,
            "total_position_profit": total_position_profit,
            "total_daily_profit": total_daily_profit,
        },
        "positions": positions_out,
        "alerts": alerts,
    }


def build_strategy_summaries() -> Dict[str, Any]:
    """按策略ID聚合的简要监控摘要.

    这里假定 xtquant 委托/成交中的 strategy_name 字段保存的是 strategy_id。
    """

    client = _get_qmt_client()
    account = client.get_account_info()
    positions = client.get_positions()
    orders = client.get_orders(cancelable_only=False) or []
    trades = client.get_trades() or []

    total_asset = float(account.get("total_asset", 0.0) or 0.0)

    # 建立股票代码到持仓的快速索引
    pos_by_code: Dict[str, Dict[str, Any]] = {}
    for p in positions:
        code = str(p.get("stock_code", ""))
        if code:
            pos_by_code[code] = p

    # 按 strategy_id 聚合
    strategies: Dict[str, Dict[str, Any]] = {}

    def _ensure_strategy(sid: str) -> Dict[str, Any]:
        s = strategies.get(sid)
        if s is None:
            s = {
                "strategy_id": sid,
                "orders_count": 0,
                "trades_count": 0,
                "total_position_profit": 0.0,
                "total_daily_profit": 0.0,
                "symbols": set(),
            }
            strategies[sid] = s
        return s

    # 先从策略配置中预填 symbols（例如 MA_CROSS / TREND_FOLLOWING 中的 config.symbols）
    strategy_cfgs = _load_strategy_configs()
    for sid, scfg in strategy_cfgs.items():
        cfg = scfg.get("config") or {}
        cfg_symbols = cfg.get("symbols") or []
        if not isinstance(cfg_symbols, list):
            continue
        s = _ensure_strategy(sid)
        for code in cfg_symbols:
            code_str = str(code or "").strip()
            if code_str:
                s["symbols"].add(code_str)

    # 统计订单/成交数量，并记录涉及的股票代码
    for o in orders:
        sid = str(o.get("strategy_name") or "").strip()
        if not sid:
            continue
        s = _ensure_strategy(sid)
        s["orders_count"] += 1
        code = str(o.get("stock_code", ""))
        if code:
            s["symbols"].add(code)

    for t in trades:
        sid = str(t.get("strategy_name") or "").strip()
        if not sid:
            continue
        s = _ensure_strategy(sid)
        s["trades_count"] += 1
        code = str(t.get("stock_code", ""))
        if code:
            s["symbols"].add(code)

    # 按策略聚合 PnL（简单地将涉及股票的整笔持仓视作该策略相关）
    for sid, s in strategies.items():
        symbols = s["symbols"]
        total_pos_pnl = 0.0
        total_daily_pnl = 0.0
        for code in symbols:
            p = pos_by_code.get(code)
            if not p:
                continue
            total_pos_pnl += float(p.get("position_profit", 0.0) or 0.0)
            total_daily_pnl += float(p.get("float_profit", 0.0) or 0.0)

        s["total_position_profit"] = total_pos_pnl
        s["total_daily_profit"] = total_daily_pnl

        # 计算策略层面的资产权重（所涉市值总和 / 账户总资产）
        total_mv_strategy = sum(
            float(pos_by_code[c].get("market_value", 0.0) or 0.0)
            for c in symbols
            if c in pos_by_code
        )
        s["market_value"] = total_mv_strategy
        s["weight_asset"] = (total_mv_strategy / total_asset) if total_asset > 0 else 0.0

    # 输出时将 symbols 从 set 转为 list
    items = []
    for sid, s in strategies.items():
        item = dict(s)
        item["symbols"] = sorted(list(s["symbols"]))
        items.append(item)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "account": account,
        "items": items,
    }


def build_single_strategy_summary(strategy_id: str) -> Dict[str, Any]:
    """构建单个策略ID的详细监控摘要.

    注意：这里的 strategy_id 语义上等于 QMT 委托/成交的 strategy_name。
    """

    client = _get_qmt_client()
    account = client.get_account_info()
    positions = client.get_positions()
    orders = client.get_orders(cancelable_only=False) or []
    trades = client.get_trades() or []

    total_asset = float(account.get("total_asset", 0.0) or 0.0)

    # 过滤出该策略相关的订单/成交
    sid = str(strategy_id or "").strip()
    rel_orders = [o for o in orders if str(o.get("strategy_name") or "").strip() == sid]
    rel_trades = [t for t in trades if str(t.get("strategy_name") or "").strip() == sid]

    # 收集涉及的股票代码
    symbols: set[str] = set()

    # 先从策略配置中预填 symbols
    strategy_cfgs = _load_strategy_configs()
    cfg = strategy_cfgs.get(sid, {}).get("config") or {}
    cfg_symbols = cfg.get("symbols") or []
    if isinstance(cfg_symbols, list):
        for code in cfg_symbols:
            code_str = str(code or "").strip()
            if code_str:
                symbols.add(code_str)
    for o in rel_orders:
        code = str(o.get("stock_code", ""))
        if code:
            symbols.add(code)
    for t in rel_trades:
        code = str(t.get("stock_code", ""))
        if code:
            symbols.add(code)

    # 股票代码 -> 持仓
    pos_by_code: Dict[str, Dict[str, Any]] = {}
    for p in positions:
        code = str(p.get("stock_code", ""))
        if code:
            pos_by_code[code] = p

    strategy_positions: List[Dict[str, Any]] = []
    total_pos_pnl = 0.0
    total_daily_pnl = 0.0
    total_mv = 0.0

    for code in symbols:
        p = pos_by_code.get(code)
        if not p:
            # 策略配置了标的，但当前没有持仓，返回占位行
            mv = 0.0
            pos_pnl = 0.0
            daily_pnl = 0.0
            weight_asset = 0.0
            item = {
                "stock_code": code,
                "stock_name": "-",
                "current_amount": 0,
                "enable_amount": 0,
                "market_value": mv,
                "position_profit": pos_pnl,
                "float_profit": daily_pnl,
                "weight_asset": weight_asset,
            }
        else:
            mv = float(p.get("market_value", 0.0) or 0.0)
            pos_pnl = float(p.get("position_profit", 0.0) or 0.0)
            daily_pnl = float(p.get("float_profit", 0.0) or 0.0)
            weight_asset = (mv / total_asset) if total_asset > 0 else 0.0
            item = dict(p)
            item["weight_asset"] = weight_asset

        total_mv += mv
        total_pos_pnl += pos_pnl
        total_daily_pnl += daily_pnl
        strategy_positions.append(item)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "strategy_id": sid,
        "account": account,
        "pnl": {
            "total_position_profit": total_pos_pnl,
            "total_daily_profit": total_daily_pnl,
            "market_value": total_mv,
            "weight_asset": (total_mv / total_asset) if total_asset > 0 else 0.0,
        },
        "positions": strategy_positions,
        "orders": rel_orders,
        "trades": rel_trades,
    }
