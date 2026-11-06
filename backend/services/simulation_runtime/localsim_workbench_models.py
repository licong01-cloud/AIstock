"""Read-only, display-whitelisted LocalSIM contracts; money is decimal text."""
from __future__ import annotations

from typing import Literal
from pydantic import BaseModel, ConfigDict, Field


class WorkbenchRow(BaseModel):
    model_config = ConfigDict(extra="forbid")
    key: str
    security: str | None = None
    order_key: str | None = None
    name: str = ""
    market: str = ""
    date: str = ""
    time: str = ""
    status: str = ""
    side: str = ""
    description: str = ""
    quantity: int | None = None
    available: int | None = None
    frozen: int | None = None
    filled: int | None = None
    remaining: int | None = None
    buy_quantity: int | None = None
    sell_quantity: int | None = None
    opening_quantity: int | None = None
    closing_quantity: int | None = None
    price: str | None = None
    cost: str | None = None
    value: str | None = None
    opening_value: str | None = None
    cash_delta: str | None = None
    balance: str | None = None
    amount: str | None = None
    fees: str | None = None
    commission: str | None = None
    tax: str | None = None
    realized: str | None = None
    unrealized: str | None = None
    unrealized_change: str | None = None
    pnl: str | None = None
    return_pct: str | None = None
    weight: str | None = None
    buy_amount: str | None = None
    sell_amount: str | None = None
    known: bool = True


class WorkbenchSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    strategy: str
    stage: str
    status: str
    initial_capital: str
    created_at: str
    economic_as_of: str | None = None
    valuation_kind: str = "估值尚未完成"
    cash: str | None = None
    available_cash: str | None = None
    frozen_cash: str | None = None
    value: str | None = None
    nav: str | None = None
    opening_nav: str | None = None
    closing_nav: str | None = None
    net_flows: str | None = None
    pnl: str | None = None
    daily_pnl: str | None = None
    daily_date: str | None = None
    comparable_live: bool = False
    return_pct: str | None = None
    realized: str | None = None
    unrealized: str | None = None
    unrealized_change: str | None = None
    fees: str | None = None
    position_count: int | None = None
    position_ratio: str | None = None
    completed_date: str | None = None
    next_date: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    source: str = "来源待核验"
    issue: str = ""
    cost_basis: str = "账本持仓成本；交易费用单列，不重复扣减"
    cumulative_pnl: str | None = None
    cumulative_return_pct: str | None = None
    cumulative_fees: str | None = None
    cumulative_note: str = "尚无已核验的期初资金和估值"
    valuation_day: str | None = None
    valuation_day_pnl: str | None = None
    valuation_day_note: str = "尚无权威估值"
    today_pnl: str | None = None
    today_note: str = "今天尚无权威估值"
    valuation_change: str | None = None
    previous_valuation_at: str | None = None
    change_note: str = "尚无两次可核对的估值"


class WorkbenchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["localsim_workbench_v1"] = "localsim_workbench_v1"
    account_id: str
    snapshot_set: str
    generated_at: str
    status: Literal["complete", "partial", "unavailable"]
    summary: WorkbenchSummary
    items: list[WorkbenchRow] = Field(default_factory=list)
    total_count: int
    next_cursor: str | None = None
    missing_dates: list[str]
    warnings: list[str] = Field(default_factory=list)
    requested_start: str | None = None
    requested_end: str | None = None
    actual_start: str | None = None
    actual_end: str | None = None


class WorkbenchAccount(BaseModel):
    key: str
    summary: WorkbenchSummary
    curve: list[WorkbenchRow] = Field(default_factory=list)


class WorkbenchOverview(BaseModel):
    items: list[WorkbenchAccount]
    generated_at: str
    complete: bool
    nav: str | None = None
    pnl: str | None = None
    daily_pnl: str | None = None
    return_pct: str | None = None
    economic_as_of: str | None = None
    included_count: int = 0
    excluded_count: int = 0
    explanation: str = "历史回放不纳入实时合计；仅汇总同一估值时点且证据完整的实时账户"
    curve: list[WorkbenchRow] = Field(default_factory=list)
