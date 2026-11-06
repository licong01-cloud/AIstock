"""Read-only NAV bridges at actual valuation times; no pricing or cost reconstruction."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Callable
from zoneinfo import ZoneInfo

from backend.services.trading_core.errors import DataUnavailableError
from .localsim_workbench_models import WorkbenchSummary
from .localsim_workbench_presentation import readable

ZERO = Decimal(0)
TOLERANCE = Decimal("0.02")
TZ = ZoneInfo("Asia/Shanghai")


def _day(value) -> str:
    return str(value or "")[:10]


def _money(value) -> Decimal:
    try:
        result = Decimal(str(value))
        if not result.is_finite():
            raise InvalidOperation
        return result
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise DataUnavailableError("金额数据未通过校验") from exc


def _bridge(data: dict, ending: dict, closing: dict, *, opening: dict | None = None,
            before: dict | None = None, capital: Decimal | None = None) -> tuple[Decimal | None, Decimal | None, str]:
    """Prove zero net external flow from the fill-only cash ledger and both bridges.

    Unknown flows, incomplete imports, cash adjustments and corporate-action quantity
    changes remain unknown. Never plug a residual into external cash flow.
    """
    finish = _day(ending["trade_date"])
    begin = _day(opening["trade_date"]) if opening else None
    run_dates = {r["run_id"]: _day(r["trade_date"]) for r in data["runs"]}
    if run_dates.get(ending["run_id"]) != finish or any(f["run_id"] not in run_dates for f in data["fills"]):
        return None, None, "成交与交易日归属不完整"
    cash = [c for c in data["cash"] if (not begin or _day(c["trade_date"]) > begin) and _day(c["trade_date"]) <= finish]
    fills = [f for f in data["fills"] if (not begin or run_dates[f["run_id"]] > begin) and run_dates[f["run_id"]] <= finish]
    fill_ids = {f["fill_id"]: f for f in fills}
    if (len(fill_ids) != len(fills) or len(fills) != len(cash)
            or {c.get("fill_id") for c in cash} != set(fill_ids)):
        return None, None, "成交与资金流水尚未逐笔对齐"
    start_cash = _money(opening["cash"]) if opening else capital
    if start_cash is None:
        return None, None, "正式期初资金依据尚未核验"
    quantities: dict[str, int] = defaultdict(int)
    for c in cash:
        f = fill_ids[c["fill_id"]]
        if (c.get("side") not in {"BUY", "SELL"} or not c.get("symbol")
                or c["symbol"] != f["symbol"] or c["side"] != f["side"]
                or c["run_id"] != f["run_id"] or _day(c["trade_date"]) != run_dates[f["run_id"]]
                or not isinstance(f["quantity"], int) or f["quantity"] <= 0 or _money(f["price"]) <= 0
                or _money(c["fee"]) < ZERO
                or abs(_money(c["notional"])-_money(f["price"])*f["quantity"]) > TOLERANCE
                or abs(_money(c["cash_delta"])-(_money(c["notional"])*(1 if c["side"] == "SELL" else -1)-_money(c["fee"]))) > TOLERANCE):
            return None, None, "存在未确认资金分类或成交金额差异"
        quantities[f["symbol"]] += f["quantity"]*(1 if f["side"] == "BUY" else -1)
    delta = sum((_money(c["cash_delta"]) for c in cash), ZERO)
    if abs(_money(ending["cash"])-start_cash-delta) > TOLERANCE:
        return None, None, "期初资金与累计交易流水不能对齐"
    before = before or {}
    if any(int(closing.get(s, {}).get("quantity", 0))-int(before.get(s, {}).get("quantity", 0)) != quantities[s]
           for s in set(before) | set(closing) | set(quantities)):
        return None, None, "持仓数量与累计成交流水不能对齐"
    opening_nav = _money(opening["nav"]) if opening else capital
    return _money(ending["nav"])-opening_nav, sum((_money(c["fee"]) for c in cash), ZERO), ""


def apply_valuation_metrics(data: dict, result: WorkbenchSummary, *, positions: Callable,
                            is_close: Callable, now: datetime | None = None) -> None:
    """Separate lifecycle, latest valuation day and calendar today from /pnl periods."""
    now = now or datetime.now(TZ)
    snaps = sorted(data["snapshots"], key=lambda s: (_day(s["trade_date"]), str(s.get("snapshot_time") or "")))
    if not snaps:
        return
    snap = snaps[-1]
    finish = _day(snap["trade_date"])
    result.valuation_day = finish
    ps, valid = positions(data, snap)
    if not valid:
        result.cumulative_note = result.valuation_day_note = "持仓与估值未对齐，盈亏暂不可计算"
        result.today_note = "最新估值尚未通过完整性核验"
        return
    initial = _money(data["account"]["initial_capital"])
    if initial <= ZERO:
        result.cumulative_note = "正式初始资金不符合账户合同"
        return
    origin = data.get("capital_origin") or {}
    capital = initial if data["native"] or (origin.get("initial_cash") is not None and _money(origin["initial_cash"]) == initial) else None
    pnl, fees, reason = _bridge(data, snap, ps, capital=capital)
    if pnl is not None:
        result.cumulative_pnl = format(pnl, "f")
        result.cumulative_return_pct = format(pnl/initial*100, "f") if initial > ZERO else None
        result.cumulative_fees = format(fees, "f")
        result.cumulative_note = "正式初始资金至本账户估值时点；已核对资金和持仓，实际费用已计入"
    else:
        result.cumulative_note = reason+"；累计盈亏待核验"
    previous = [s for s in snaps if _day(s["trade_date"]) < finish]
    opening = previous[-1] if previous else None
    before, opening_valid = positions(data, opening)
    if opening and opening_valid:
        change, _, gap = _bridge(data, snap, ps, opening=opening, before=before)
        if change is not None:
            result.valuation_change = format(change, "f")
            result.previous_valuation_at = str(opening["snapshot_time"])
            result.change_note = "两次实际估值之间的净变化；包含隔夜或跨日变化，不能代替日盈亏"
        else:
            result.change_note = gap
    calendar = sorted(_day(r["cal_date"]) for r in data["calendar"] if _day(r["cal_date"]) < finish)
    adjacent = bool(opening and calendar and _day(opening["trade_date"]) == calendar[-1])
    if opening and opening_valid and adjacent and is_close(opening):
        daily, _, reason = _bridge(data, snap, ps, opening=opening, before=before)
    elif not previous and data["native"] and pnl is not None and all(_day(r["trade_date"]) >= finish for r in data["runs"]):
        daily, reason = pnl, ""
    else:
        daily, reason = None, "缺少前一交易日的权威收盘估值"
    if daily is not None:
        result.valuation_day_pnl = format(daily, "f")
        result.valuation_day_note = "前一交易日收盘（账户首日为正式期初）至本次估值；已含实际费用"
    else:
        result.valuation_day_note = reason
    if finish == now.astimezone(TZ).date().isoformat():
        result.today_pnl, result.today_note = result.valuation_day_pnl, result.valuation_day_note
    else:
        result.today_note = f"今天尚无权威估值；最近资产时点 {result.economic_as_of}"


def validate_workbench_overview(payload) -> tuple[str, str | None, dict]:
    """Endpoint-owned, pure post-restart smoke; never confuse transport with activation."""
    try:
        if not isinstance(payload, dict) or payload.get("complete") is not True:
            raise ValueError("workbench overview must be complete")
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            raise ValueError("workbench smoke needs an account to verify")
        generated = datetime.fromisoformat(payload["generated_at"].replace("Z", "+00:00"))
        if generated.tzinfo is None:
            raise ValueError("generated_at must include a timezone")
        today = generated.astimezone(TZ).date().isoformat()
        keys = set()
        known = 0
        required = {"name", "strategy", "initial_capital", "nav", "cash", "value", "economic_as_of",
                    "cumulative_pnl", "cumulative_return_pct", "cumulative_note", "valuation_day",
                    "valuation_day_pnl", "valuation_day_note", "today_pnl", "today_note"}
        for item in items:
            if not isinstance(item, dict) or not isinstance(item.get("key"), str) or not item["key"] or item["key"] in keys:
                raise ValueError("account scope keys must be present and unique")
            keys.add(item["key"])
            s = item.get("summary")
            if not isinstance(s, dict) or not required.issubset(s):
                raise ValueError("workbench valuation fields have not been loaded")
            if any(not readable(s[k], "") or "待完善" in s[k] for k in ("name", "strategy")):
                raise ValueError("account and strategy names must be readable")
            if any(not readable(s[k], "") for k in ("cumulative_note", "valuation_day_note", "today_note")):
                raise ValueError("unknown values need explicit readable reasons")
            initial = _money(s["initial_capital"])
            if initial <= ZERO:
                raise ValueError("formal initial capital must be positive")
            if s["nav"] is not None:
                if abs(_money(s["cash"])+_money(s["value"])-_money(s["nav"])) > TOLERANCE:
                    raise ValueError("account asset bridge is inconsistent")
                as_of = datetime.fromisoformat(s["economic_as_of"].replace("Z", "+00:00"))
                if as_of.tzinfo is None or as_of.astimezone(TZ).date().isoformat() != s["valuation_day"]:
                    raise ValueError("valuation date and timestamp must agree")
            if s["cumulative_pnl"] is not None:
                if abs(_money(s["nav"])-initial-_money(s["cumulative_pnl"])) > TOLERANCE:
                    raise ValueError("lifecycle NAV bridge is inconsistent")
                if abs(_money(s["cumulative_pnl"])/initial*100-_money(s["cumulative_return_pct"])) > Decimal("0.000001"):
                    raise ValueError("lifecycle return denominator is inconsistent")
                known += 1
            if s["today_pnl"] is not None and (s["valuation_day"] != today or _money(s["today_pnl"]) != _money(s["valuation_day_pnl"])):
                raise ValueError("historical valuation cannot become today's profit")
        return "passed", None, {"account_count": len(items), "known_cumulative_count": known, "separate_accounts": True}
    except (KeyError, TypeError, ValueError, InvalidOperation, DataUnavailableError) as exc:
        return "failed", f"LocalSIM workbench valuation contract failed: {exc}", {}
