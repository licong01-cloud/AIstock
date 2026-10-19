"""Explicit operator text. Never use an opaque identity as a display fallback."""
from __future__ import annotations

import re
from typing import Any

OPAQUE = re.compile(
    r"(?:\b[0-9a-f]{24,}\b|[0-9a-f]{8}-[0-9a-f-]{27,}|"
    r"[A-Za-z0-9]+_[A-Za-z0-9_\-]+|"
    r"\b\d{6}\.(?:SH|SZ|BJ)\b|[A-Za-z]:[\\/]|https?://|[{}\[\]]|"
    r"[\x00-\x1f]|\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+\b)", re.I,
)


def readable(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    if not text or len(text) > 120 or OPAQUE.search(text):
        return fallback
    # Machine-only names (including model shorthands) have no useful operator meaning.
    if not re.search(r"[\u3400-\u9fff]", text):
        return fallback
    return text


def market(symbol: str | None) -> str:
    return {"SH": "上海市场", "SZ": "深圳市场", "BJ": "北京市场"}.get(
        str(symbol or "").rsplit(".", 1)[-1], "市场待核验"
    )


STATES = {
    "ACTIVE": "启用", "PAUSED": "暂停", "RETIRED": "已停用",
    "PENDING": "等待开始", "RUNNING": "历史回放", "REPLAYING": "历史回放",
    "FAILED_TERMINAL": "回放受阻", "FAILED": "处理受阻", "BLOCKED": "回放受阻",
    "SUCCEEDED": "交易已完成", "COMPLETED": "既定区间已完成",
    "CANCELLED": "已取消", "CANCELED": "已取消", "CANCEL_REQUESTED": "等待取消",
    "CATCHUP_COMPLETED": "等待安全接续", "WAITING_SAFE_BOUNDARY": "等待安全接续",
    "LIVE": "实时模拟", "LIVE_ACTIVATED": "实时模拟", "LIVE_BOUND": "实时模拟",
    "FILLED": "全部成交", "PARTIALLY_FILLED": "部分成交", "NEW": "等待处理",
    "SUBMITTED": "已提交", "ACCEPTED": "已受理", "REJECTED": "已拒绝",
    "EXPIRED": "已到期", "EXPIRED_WITH_RESIDUAL": "交易约束下仍有剩余",
    "CREATED": "已创建", "SCHEDULED": "等待开始", "VALUATION_PENDING": "估值待完成",
    "BUY": "买入", "SELL": "卖出",
    "RUNNING_HISTORICAL": "历史回放", "CAUGHT_UP": "既定区间已完成",
    "READY_FOR_LIVE": "等待安全接续", "ACTIVATION_PENDING_SAFE_BOUNDARY": "等待安全接续",
    "LIVE_ACTIVE": "实时模拟", "FAILED_RETRYABLE": "执行受阻，等待自动重试",
    "PRECHECKING": "正在核验输入", "SIGNAL_GENERATING": "正在选股",
    "TARGET_GENERATING": "正在生成目标", "PLANNING_EXECUTION": "正在计划交易",
    "SUBMITTING": "正在提交交易", "INTRADAY_RUNNING": "正在处理交易",
    "TAIL_HANDLING": "正在处理剩余量", "RECONCILING": "正在核对交易",
    "ORDER_CREATED": "委托已创建", "ORDER_ACCEPTED": "委托已受理",
    "ORDER_CANCELLED": "委托已取消", "ORDER_REJECTED": "委托已拒绝",
}


def state(value: Any) -> str:
    return STATES.get(str(getattr(value, "value", value)), "状态暂无法识别")


def problem(code: Any) -> str:
    key = str(code or "").upper()
    if not key:
        return "等待下一处理步骤"
    if "SUSPEND" in key:
        return "停牌资格或参考价需要核验"
    if "LIMIT" in key or "CAPACITY" in key or "RESIDUAL" in key:
        return "存在交易限制或未完成数量，请核对交易记录"
    if "PRICE" in key or "MARK" in key or "MINUTE" in key or "DATA" in key:
        return "所需行情或估值尚未通过核验，资产以最后完成时点为准"
    if "POLICY" in key or "IDENTITY" in key:
        return "冻结输入或政策身份需要核验"
    return "处理受阻，请按账户和发生日期核查原任务"
