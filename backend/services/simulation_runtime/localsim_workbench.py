"""Read-only transaction-consistent economic views. No market-price reconstruction."""
from __future__ import annotations

import base64
import csv
import hashlib
import hmac
import io
import json
import secrets
import time
from collections import OrderedDict, defaultdict
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from threading import RLock
from typing import Any, Callable
from zoneinfo import ZoneInfo

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
from backend.services.simulation_execution.localsim.models import LocalSimMarketMarkV1
from .localsim_workbench_models import (
    WorkbenchAccount, WorkbenchOverview, WorkbenchResult, WorkbenchRow, WorkbenchSummary,
)
from .localsim_workbench_presentation import market, problem, readable, state

TZ = ZoneInfo("Asia/Shanghai")
ZERO = Decimal(0)
MAX_ROWS = 200_000
_SIGNING_KEY = secrets.token_bytes(32)
_CACHE: OrderedDict[tuple[str, str], tuple[float, dict]] = OrderedDict()
_LOCK = RLock()
READ_COLUMNS = {
    "snapshots": "snapshot_id,run_id,portfolio_id,trade_date,cash,market_value,nav,position_count,snapshot_time,metadata",
    "positions": "position_id,run_id,portfolio_id,trade_date,symbol,stock_name,quantity,available_quantity,avg_cost,market_price,market_value",
    "orders": "order_id,run_id,portfolio_id,symbol,stock_name,side,quantity,limit_price,status,filled_quantity,created_at,updated_at",
    "cash": "cash_id,run_id,portfolio_id,fill_id,trade_date,symbol,stock_name,side,notional,fee,cash_delta,cash_after,created_at",
    "runs": "run_id,portfolio_id,trade_date,status,started_at,completed_at,md5(COALESCE(runtime_config::text,'')) AS runtime_identity",
    "fills": "fill_id,run_id,order_id,symbol,stock_name,side,quantity,price,trade_time,created_at",
    "events": "event_id,run_id,order_id,event_type,event_time,reason",
}


def money(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
        if not result.is_finite():
            raise InvalidOperation
        return result
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise DataUnavailableError("金额数据未通过核验") from exc


def text(value: Decimal | Any | None) -> str | None:
    return None if value is None else format(money(value), "f")


def day(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(TZ).date().isoformat()
    return str(value or "")[:10]


def clock(value: Any) -> str:
    if not value:
        return ""
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return stamp.astimezone(TZ).isoformat()


def _fail(message: str) -> None:
    raise RuntimeConfigInvalidError(message)


def _pack(value: dict) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(hmac.digest(_SIGNING_KEY, raw, "sha256") + raw).decode()


def _unpack(value: str) -> dict:
    try:
        raw = base64.b64decode(value, altchars=b"-_", validate=True)
        if not hmac.compare_digest(raw[:32], hmac.digest(_SIGNING_KEY, raw[32:], "sha256")):
            raise ValueError
        return json.loads(raw[32:])
    except (ValueError, TypeError, KeyError) as exc:
        raise RuntimeConfigInvalidError("分页已失效，请重新查询") from exc


class LocalSimWorkbenchService:
    def __init__(self, *, repository: Any, conn_factory: Callable | None = None):
        self.repository = repository
        self.conn_factory = conn_factory or (lambda: get_conn(autocommit=False, manage_transaction=True))

    @staticmethod
    def _read(cur: Any, sql: str, params: tuple = ()) -> list[dict]:
        cur.execute(sql + " LIMIT %s", (*params, MAX_ROWS + 1))
        rows = [dict(r) for r in cur.fetchall()]
        if len(rows) > MAX_ROWS:
            raise DataUnavailableError("账户历史超过本次安全读取容量，请联系模拟盘维护人员扩展查询容量；未返回截断结果")
        return rows

    def _load(self, account_id: str, version: str | None = None) -> dict:
        account = self.repository.get_account(account_id)
        scope = self.repository.resolve_ledger_scope_for_account(account_id)
        if version:
            with _LOCK:
                saved = _CACHE.get((account_id, version))
                if saved and time.monotonic() - saved[0] < 300:
                    if saved[1]["scope"] == scope.ledger_scope_id:
                        return saved[1]
        with self.conn_factory() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cur.execute("SET LOCAL statement_timeout = '20s'")
            data = {"account": account.model_dump(mode="json"), "scope": scope.ledger_scope_id,
                    "native": str(getattr(scope.scope_kind, "value", scope.scope_kind)) == "SUCCESSOR_NATIVE"}
            for name, table, where in (
                ("snapshots", "daily_snapshots", "portfolio_id = %s"),
                ("positions", "positions", "portfolio_id = %s"),
                ("orders", "orders", "portfolio_id = %s"),
                ("cash", "cash_ledger", "portfolio_id = %s"),
                ("runs", "run", "portfolio_id = %s"),
                ("fills", "fills", "run_id IN (SELECT run_id FROM paper_v2.run WHERE portfolio_id = %s)"),
                ("events", "order_events", "run_id IN (SELECT run_id FROM paper_v2.run WHERE portfolio_id = %s)"),
            ):
                data[name] = self._read(cur, f"SELECT {READ_COLUMNS[name]} FROM paper_v2.{table} WHERE {where}", (data["scope"],))
            data["replays"] = self._read(cur, "SELECT replay_job_id,status,start_trade_date,end_trade_date,next_trade_date,completed_trade_date,activation_trade_date,historical_source_id,failure_code,created_at,updated_at FROM paper_v2.localsim_replay_job_v1 WHERE simulation_account_id = %s ORDER BY created_at DESC, replay_job_id DESC", (account_id,))
            data["valuation"] = self._read(cur, """SELECT run_id, status,
                jsonb_build_object('outbox_id',run_payload_json->'local_sim_projection_outbox_v1'->'outbox_id',
                    'projection_payload',jsonb_build_object('marks',run_payload_json->'local_sim_projection_outbox_v1'->'projection_payload'->'marks')) AS outbox,
                CASE WHEN run_payload_json->'local_sim_valuation_completion_v1' IS NOT NULL
                    THEN jsonb_build_object('marks',run_payload_json->'local_sim_valuation_completion_v1'->'marks') END AS completion
                FROM paper_v2.simulation_daily_run WHERE run_id IN
                (SELECT run_id FROM paper_v2.run WHERE portfolio_id = %s)""", (data["scope"],))
            cur.execute("SELECT display_name, package_name FROM strategy_pkg.package WHERE package_id = %s", (account.package_id,))
            data["package"] = dict(cur.fetchone() or {})
            dates = [day(s["trade_date"]) for s in data["snapshots"]]
            if dates:
                data["calendar"] = self._read(cur, "SELECT cal_date FROM market.trading_calendar WHERE is_trading = TRUE AND cal_date BETWEEN %s AND %s ORDER BY cal_date", (min(dates), max(dates)))
            else:
                data["calendar"] = []
        # Include all committed facts, not only the latest timestamp. Old-day revaluation invalidates the set.
        digest_data = {k: sorted(v, key=lambda r: json.dumps(r, sort_keys=True, default=str)) if isinstance(v, list) else v for k, v in data.items()}
        data["version"] = hashlib.sha256(json.dumps(digest_data, sort_keys=True, default=str).encode()).hexdigest()
        if version and version != data["version"]:
            _fail("经济数据集合已更新，请重新读取账户后查询")
        with _LOCK:
            _CACHE[(account_id, data["version"])] = (time.monotonic(), data)
            _CACHE.move_to_end((account_id, data["version"]))
            # Bounded, private read cache; no shared Advisory result or persisted shadow ledger.
            while len(_CACHE) > 8 or sum(sum(len(v) for v in d[1].values() if isinstance(v, list)) for d in _CACHE.values()) > 300_000:
                _CACHE.popitem(last=False)
        return data

    @staticmethod
    def _is_close(snapshot: dict) -> bool:
        value = snapshot.get("snapshot_time")
        if not value:
            return False
        stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        local = stamp.astimezone(TZ)
        return local.date().isoformat() == day(snapshot["trade_date"]) and (local.hour, local.minute) >= (15, 0)

    @staticmethod
    def _positions(data: dict, snap: dict | None) -> tuple[dict[str, dict], bool]:
        if snap is None:
            return {}, True
        rows = [p for p in data["positions"] if p["run_id"] == snap["run_id"] and day(p["trade_date"]) == day(snap["trade_date"])]
        positions = {p["symbol"]: p for p in rows}
        complete = len(positions) == len(rows) == snap["position_count"]
        if complete:
            complete = (abs(sum((money(p["market_value"]) for p in rows), ZERO) - money(snap["market_value"])) <= Decimal("0.02")
                and abs(money(snap["cash"])+money(snap["market_value"])-money(snap["nav"])) <= Decimal("0.02")
                and all(int(p["quantity"]) >= 0 and money(p["avg_cost"]) >= 0 and
                    (int(p["quantity"]) == 0 or money(p["market_price"]) > 0) and
                    abs(money(p["market_price"])*int(p["quantity"])-money(p["market_value"])) <= Decimal("0.02") for p in rows))
        return positions, complete

    @staticmethod
    def _mark_description(data: dict, snap: dict, symbol: str, price: Any) -> str:
        metadata = snap.get("metadata") or {}
        for item in data.get("valuation", []):
            if item["run_id"] != snap["run_id"]:
                continue
            outbox = item.get("outbox") or {}
            if not outbox.get("outbox_id") or metadata.get("local_sim_outbox_id") != outbox.get("outbox_id"):
                continue
            payload = item.get("completion") or outbox.get("projection_payload") or {}
            for raw in payload.get("marks") or []:
                if raw.get("symbol") != symbol:
                    continue
                try:
                    mark = LocalSimMarketMarkV1.model_validate(raw)
                    if money(mark.price) != money(price):
                        break
                except (ValueError, DataUnavailableError):
                    break
                if "SUSPENDED" in mark.provenance.value:
                    reference = mark.suspension_reference or {}
                    quote_date = day(reference.get("quote_date") or reference.get("reference_trade_date"))
                    return "停牌参考价" + (f"，报价截至 {quote_date}" if quote_date else "，历史报价日期沿用原估值凭证")
                return ("历史分钟估值，价格时点 " if mark.provenance.value == "HISTORICAL_MINUTE_CLOSE" else "实时分钟估值，价格时点 ") + clock(mark.as_of_time)
        return "账本记录估值；详细参考价来源尚未获得可匹配的原生凭证"

    @staticmethod
    def _stock(symbol: str, *rows: dict | None) -> tuple[str, str]:
        label = next((r.get("stock_name") for r in rows if r and r.get("stock_name")), None)
        return readable(label, "证券名称暂不可用"), market(symbol)

    def _summary(self, data: dict) -> WorkbenchSummary:
        a, pkg = data["account"], data["package"]
        snaps = sorted(data["snapshots"], key=lambda s: day(s["trade_date"]))
        snap = snaps[-1] if snaps else None
        replay = data["replays"][0] if data["replays"] else None
        stage = state(replay["status"]) if replay else "日常模拟，运行待核验"
        if a["status"] != "ACTIVE":
            stage = state(a["status"])
        result = WorkbenchSummary(
            name=readable(a["account_name"], "模拟账户名称待完善"),
            strategy=readable(pkg.get("display_name"), readable(pkg.get("package_name"), "策略名称待完善")),
            stage=stage, status=state(a["status"]), initial_capital=text(a["initial_capital"]),
            created_at=clock(a["created_at"]),
            cost_basis="账本平均成本不含交易费用，费用单列" if data["native"] else "历史导入成本，费用包含关系尚待核验",
            comparable_live=bool(replay and replay["status"] == "LIVE_ACTIVE" and a["status"] == "ACTIVE"),
            completed_date=day(replay.get("completed_trade_date")) or None if replay else None,
            next_date=day(replay.get("next_trade_date")) or None if replay else None,
            start_date=day(replay.get("start_trade_date")) or None if replay else None,
            end_date=day(replay.get("end_trade_date")) or None if replay else None,
            issue=problem(replay.get("failure_code")) if replay else "等待下一交易时段",
            source="实时行情" if replay and replay["status"] == "LIVE_ACTIVE" else {"dataset_files.frozen_market.v1": "数据集文件", "market.kline_minute_raw.v1": "行情数据库"}.get(str(replay.get("historical_source_id")), "来源待核验") if replay else "来源待核验",
        )
        if snap:
            ps, complete = self._positions(data, snap)
            result.comparable_live = (result.comparable_live and complete and bool(replay.get("activation_trade_date"))
                and day(snap["trade_date"]) >= day(replay["activation_trade_date"]))
            result.economic_as_of = clock(snap["snapshot_time"])
            result.valuation_kind = "已完成收盘估值" if self._is_close(snap) else "盘中估值，不能作为收盘收益"
            if not complete:
                result.valuation_kind = "估值与持仓尚未对齐，金额仅供核查"
            result.cash, result.value, result.nav = text(snap["cash"]), text(snap["market_value"]), text(snap["nav"])
            result.position_count = int(snap["position_count"]) if complete else None
            result.position_ratio = text(money(snap["market_value"])/money(snap["nav"])*100) if complete and money(snap["nav"]) else None
            if complete:
                result.unrealized = text(sum((money(p["market_value"]) - money(p["avg_cost"]) * int(p["quantity"]) for p in ps.values()), ZERO))
        return result

    def view(self, account_id: str, kind: str = "summary", *, start: date | None = None,
             end: date | None = None, as_of: date | None = None, security: str | None = None,
             side: str | None = None, status: str | None = None, group_by: str = "date",
             order_key: str | None = None, search: str | None = None, sort: str = "time", descending: bool = True,
             cursor: str | None = None, page_size: int = 50, snapshot_set: str | None = None) -> WorkbenchResult:
        if start and end and start > end:
            _fail("开始日期不能晚于结束日期")
        if group_by not in {"date", "security"} or not 1 <= page_size <= 200:
            _fail("查询选项不合法")
        if kind == "pnl" and group_by == "date" and (security or search):
            _fail("证券筛选请使用按股票统计；按日视图表示账户整体")
        if sort not in {"time", "name", "date", "value", "pnl", "unrealized", "quantity", "price", "fees"}:
            _fail("排序选项不合法")
        filters = [account_id, kind, str(start), str(end), str(as_of), security, side, status, group_by, page_size, order_key, search, sort, descending]
        previous = _unpack(cursor) if cursor else None
        if previous and snapshot_set and previous.get("version") != snapshot_set:
            _fail("分页集合与当前查询不一致，请重新查询")
        if previous and previous.get("filters") != filters:
            _fail("筛选条件已改变，请从第一页查询")
        data = self._load(account_id, previous["version"] if previous else snapshot_set)
        summary = self._summary(data)
        warnings: list[str] = []
        rows: list[WorkbenchRow] = []
        missing: list[str] = []
        actual_start = actual_end = None
        snaps = sorted(data["snapshots"], key=lambda s: day(s["trade_date"]))
        selected = next((s for s in snaps if day(s["trade_date"]) == str(as_of)), None) if as_of else (snaps[-1] if snaps else None)
        if kind in {"positions", "summary"}:
            if not selected:
                warnings.append("所选日期没有权威持仓估值集合")
            else:
                ps, complete = self._positions(data, selected)
                if not complete:
                    warnings.append("持仓集合与权威估值数量或市值不一致")
                contributions, _, _, _ = self._pnl(data, date.fromisoformat(day(selected["trade_date"])), date.fromisoformat(day(selected["trade_date"])), "security")
                by_security = {r.security: r.pnl for r in contributions}
                for symbol, p in ps.items():
                    name, venue = self._stock(symbol, p)
                    value, cost = money(p["market_value"]), money(p["avg_cost"]) * p["quantity"]
                    valid_mark = money(p["market_price"]) > 0 or p["quantity"] == 0
                    rows.append(WorkbenchRow(key=symbol, security=symbol, name=name, market=venue,
                        date=day(selected["trade_date"]), time=clock(selected["snapshot_time"]), quantity=p["quantity"],
                        available=p.get("available_quantity"), frozen=None, cost=text(p["avg_cost"]), price=text(p["market_price"]) if valid_mark else None,
                        value=text(value) if valid_mark else None, unrealized=text(value-cost) if complete else None, return_pct=text((value-cost)/cost*100) if cost and complete else None,
                        weight=text(value/money(selected["nav"])*100) if money(selected["nav"]) and complete else None,
                        pnl=by_security.get(symbol),
                        description="账本记录简称；" + self._mark_description(data, selected, symbol, p["market_price"]), known=complete))
                actual_end = day(selected["trade_date"])
            if kind == "summary":
                _, ws, _, ms = self._pnl(data, None, None, "security")
                for field in ("pnl", "fees", "return_pct"):
                    setattr(summary, field, ms.get(field))
                closes = [s for s in snaps if self._is_close(s)]
                if closes:
                    last = date.fromisoformat(day(closes[-1]["trade_date"]))
                    _, _, _, daily_metrics = self._pnl(data, last, last, "security")
                    summary.daily_pnl, summary.daily_date = daily_metrics.get("pnl"), str(last)
                warnings.extend(ws)
        elif kind in {"trades", "orders", "cash-flows"}:
            cash_by_fill = {c["fill_id"]: c for c in data["cash"] if c.get("fill_id")}
            fills_by_id = {f["fill_id"]: f for f in data["fills"]}
            run_dates = {r["run_id"]: day(r["trade_date"]) for r in data["runs"]}
            source = {"trades": data["fills"], "orders": data["orders"], "cash-flows": data["cash"]}[kind]
            for item in source:
                symbol = item.get("symbol") or ""
                name, venue = self._stock(symbol, item)
                d = day(item.get("trade_date")) or run_dates.get(item["run_id"], "")
                ts = item.get("trade_time") or item.get("created_at")
                row = WorkbenchRow(key=str(item.get("fill_id") if kind == "trades" else item.get("order_id") if kind == "orders" else item["cash_id"]),
                    security=symbol or None, name=name if symbol else "账户资金", market=venue if symbol else "", date=d, time=clock(ts), side=state(item.get("side")))
                if kind == "trades":
                    cash = cash_by_fill.get(item["fill_id"])
                    row.order_key, row.quantity, row.price = item["order_id"], item["quantity"], text(item["price"])
                    row.amount = text(cash["notional"]) if cash else text(money(item["price"])*item["quantity"])
                    row.fees = text(cash["fee"]) if cash else None
                    row.description = "费用按实际账本；分项未单独记录时不可拆分"
                    row.known = cash is not None
                elif kind == "orders":
                    row.order_key = item["order_id"]
                    row.quantity, row.filled = item["quantity"], item["filled_quantity"]
                    row.remaining = item["quantity"]-item["filled_quantity"]
                    row.status, row.price = state(item["status"]), text(item.get("limit_price"))
                    row.description = "剩余量不等于程序失败，请结合交易约束核对" if item["filled_quantity"] < item["quantity"] else "已按委托完成成交"
                else:
                    row.order_key = fills_by_id.get(item.get("fill_id"), {}).get("order_id")
                    row.amount, row.fees, row.cash_delta, row.balance = text(item["notional"]), text(item["fee"]), text(item["cash_delta"]), text(item["cash_after"])
                    row.description = "买卖净资金，已含本笔实际交易费用" if symbol else "账户资金分类待核验"
                    row.known = bool(symbol and item.get("fill_id"))
                rows.append(row)
        elif kind == "pnl":
            rows, warnings, missing, metrics = self._pnl(data, start, end, group_by)
            for field, value in metrics.items():
                if field in {"actual_start", "actual_end"}:
                    continue
                setattr(summary, field, value)
            actual_start, actual_end = metrics.get("actual_start"), metrics.get("actual_end")
        elif kind == "events":
            for event in data["events"]:
                rows.append(WorkbenchRow(key=str(event["event_id"]), order_key=event["order_id"],
                    time=clock(event["event_time"]), date=day(event["event_time"]), name="委托状态变化",
                    status=state(event["event_type"]), description=problem(event.get("reason")) if event.get("reason") else "沿用原委托处理记录"))
            for r in data["runs"]:
                rows.append(WorkbenchRow(key=str(r["run_id"]), date=day(r["trade_date"]), time=clock(r.get("completed_at") or r.get("started_at")),
                    name="交易日处理", status=state(r["status"]), description="交易结果与收盘估值分别核验"))
            for j in data["replays"]:
                rows.append(WorkbenchRow(key=j["replay_job_id"], date=day(j.get("completed_trade_date")), time=clock(j["updated_at"]),
                    name="历史回放进度", status=state(j["status"]), description=problem(j.get("failure_code"))))
        else:
            _fail("不支持的查询类型")
        rows = [r for r in rows if (kind in {"positions", "summary"} or (not start or r.date >= str(start)) and (not end or r.date <= str(end)))
                and (not security or r.security == security) and (not side or r.side == state(side)) and (not status or r.status == state(status))
                and (not order_key or r.order_key == order_key) and (not search or search.strip() in r.name)]
        numeric_sort = sort in {"value", "pnl", "unrealized", "quantity", "price", "fees"}
        rows.sort(key=lambda r: (getattr(r, sort) is not None, money(getattr(r, sort)) if numeric_sort and getattr(r, sort) is not None else getattr(r, sort) or "", r.date, r.time, r.key), reverse=descending)
        offset = int(previous["offset"]) if previous else 0
        page = rows[offset:offset+page_size]
        next_cursor = _pack({"filters": filters, "version": data["version"], "offset": offset+page_size}) if offset+page_size < len(rows) else None
        if any(not r.known for r in rows):
            warnings.append("部分记录的完整性或费用归属尚待核验")
        return WorkbenchResult(account_id=account_id, snapshot_set=data["version"], generated_at=datetime.now(UTC).isoformat(),
            status="partial" if warnings else "complete", summary=summary, items=page, total_count=len(rows), next_cursor=next_cursor,
            warnings=list(dict.fromkeys(warnings)), missing_dates=missing, requested_start=str(start) if start else None,
            requested_end=str(end) if end else None, actual_start=actual_start, actual_end=actual_end)

    def _pnl(self, data: dict, start: date | None, end: date | None, group_by: str) -> tuple:
        snaps = sorted((s for s in data["snapshots"] if self._is_close(s)), key=lambda s: day(s["trade_date"]))
        selected = [s for s in snaps if (not start or day(s["trade_date"]) >= str(start)) and (not end or day(s["trade_date"]) <= str(end))]
        if not selected:
            return [], ["所选区间没有已完成收盘估值"], [], {}
        begin, finish = day(selected[0]["trade_date"]), day(selected[-1]["trade_date"])
        calendar = [day(r["cal_date"]) for r in data["calendar"]]
        requested_start, requested_end = str(start) if start else begin, str(end) if end else finish
        missing = sorted(set(d for d in calendar if requested_start <= d <= requested_end) - {day(s["trade_date"]) for s in selected})
        warnings = ["区间缺少交易日估值，不能视为完整收益"] if missing else []
        if requested_end > finish or requested_start < min(calendar or [begin]):
            warnings.append("请求区间超出已完成估值覆盖，以下仅显示可用部分")
        previous = [s for s in snaps if day(s["trade_date"]) < begin]
        opening = previous[-1] if previous else None
        native_start = (opening is None and data["native"] and selected[0] == snaps[0]
            and not any(day(c["trade_date"]) < begin for c in data["cash"])
            and not any(day(r["trade_date"]) < begin for r in data["runs"]))
        opening_nav = money(opening["nav"]) if opening else money(data["account"]["initial_capital"]) if native_start else None
        opening_cash = money(opening["cash"]) if opening else opening_nav
        op, op_complete = self._positions(data, opening)
        ep, ep_complete = self._positions(data, selected[-1])
        cash = [c for c in data["cash"] if begin <= day(c["trade_date"]) <= finish]
        run_dates = {r["run_id"]: day(r["trade_date"]) for r in data["runs"]}
        fills = [f for f in data["fills"] if begin <= run_dates.get(f["run_id"], day(f["trade_time"])) <= finish]
        fill_by_id = {f["fill_id"]: f for f in fills}
        cash_known = (len(fill_by_id) == len(fills) == len(cash)
            and {c.get("fill_id") for c in cash} == set(fill_by_id)
            and all(c.get("symbol") and c.get("fill_id") and c.get("side") in {"BUY", "SELL"} for c in cash))
        if cash_known:
            for c in cash:
                f = fill_by_id[c["fill_id"]]
                expected = money(c["notional"])*(1 if c["side"] == "SELL" else -1)-money(c["fee"])
                if (c["symbol"] != f["symbol"] or c["side"] != f["side"] or c["run_id"] != f["run_id"]
                    or day(c["trade_date"]) != run_dates.get(f["run_id"])
                    or money(c["fee"]) < 0 or abs(expected-money(c["cash_delta"])) > Decimal("0.02")
                    or abs(money(c["notional"])-money(f["price"])*f["quantity"]) > Decimal("0.02")):
                    cash_known = False
                    break
        quantities: dict[str, int] = defaultdict(int)
        for f in fills:
            quantities[f["symbol"]] += f["quantity"]*(1 if f["side"] == "BUY" else -1)
        inventory_balanced = all(int(ep.get(s, {}).get("quantity", 0))-int(op.get(s, {}).get("quantity", 0)) == quantities[s] for s in set(op)|set(ep)|set(quantities))
        delta = sum((money(c["cash_delta"]) for c in cash), ZERO)
        cash_balanced = opening_cash is not None and abs(money(selected[-1]["cash"])-opening_cash-delta) <= Decimal("0.02")
        prior_calendar = [d for d in calendar if d < begin]
        opening_adjacent = native_start or not prior_calendar or opening is not None and day(opening["trade_date"]) == max(prior_calendar)
        complete = op_complete and ep_complete and opening_nav is not None and cash_known and cash_balanced and inventory_balanced and opening_adjacent and not missing
        if not complete:
            warnings.append("期初、持仓或资金归属证据不完整；不可计算的分项保持未知")
        fees = sum((money(c["fee"]) for c in cash), ZERO) if cash_known else None
        net = money(selected[-1]["nav"])-opening_nav if complete else None
        unreal_begin = sum((money(p["market_value"])-money(p["avg_cost"])*p["quantity"] for p in op.values()), ZERO)
        unreal_end = sum((money(p["market_value"])-money(p["avg_cost"])*p["quantity"] for p in ep.values()), ZERO)
        unreal_delta = unreal_end-unreal_begin if complete else None
        metrics = {"opening_nav": text(opening_nav), "closing_nav": text(selected[-1]["nav"]), "net_flows": "0" if complete else None,
            "pnl": text(net), "fees": text(fees), "unrealized_change": text(unreal_delta),
            "realized": text(net-unreal_delta+fees) if complete and data["native"] else None,
            "return_pct": text(net/opening_nav*100) if complete and opening_nav else None,
            "actual_start": day(opening["trade_date"]) if opening else begin, "actual_end": finish}
        rows = []
        if group_by == "security":
            by_symbol: dict[str, list] = defaultdict(list)
            fills_by_symbol: dict[str, list] = defaultdict(list)
            for c in cash:
                by_symbol[c.get("symbol")].append(c)
            for f in fills:
                fills_by_symbol[f["symbol"]].append(f)
            symbols = set(op) | set(ep) | {c["symbol"] for c in cash if c.get("symbol")}
            for symbol in sorted(symbols):
                before, after = op.get(symbol), ep.get(symbol)
                flows = by_symbol[symbol]
                name, venue = self._stock(symbol, after, before, *flows)
                bv, ev = money(before["market_value"]) if before else ZERO, money(after["market_value"]) if after else ZERO
                pnl = ev-bv+sum((money(c["cash_delta"]) for c in flows), ZERO) if complete else None
                ud = (money(after["market_value"])-money(after["avg_cost"])*after["quantity"] if after else ZERO) - (money(before["market_value"])-money(before["avg_cost"])*before["quantity"] if before else ZERO)
                fee = sum((money(c["fee"]) for c in flows), ZERO)
                rows.append(WorkbenchRow(key=symbol, security=symbol, name=name, market=venue, date=finish,
                    opening_quantity=before["quantity"] if before else 0, closing_quantity=after["quantity"] if after else 0,
                    buy_quantity=sum(f["quantity"] for f in fills_by_symbol[symbol] if f["side"] == "BUY"),
                    sell_quantity=sum(f["quantity"] for f in fills_by_symbol[symbol] if f["side"] == "SELL"),
                    opening_value=text(bv), value=text(ev), buy_amount=text(sum((money(c["notional"]) for c in flows if c["side"] == "BUY"), ZERO)),
                    sell_amount=text(sum((money(c["notional"]) for c in flows if c["side"] == "SELL"), ZERO)), fees=text(fee),
                    pnl=text(pnl), unrealized_change=text(ud) if complete else None, realized=text(pnl-ud+fee) if pnl is not None and data["native"] else None, known=complete))
            if complete and abs(sum((money(r.pnl) for r in rows), ZERO)-net) > Decimal("0.02"):
                warnings.append("逐股贡献与账户盈亏未对齐，请核对资金科目")
                for row in rows:
                    row.known = False
        else:
            for snap in selected:
                d = day(snap["trade_date"])
                _, ws, _, ms = self._pnl(data, date.fromisoformat(d), date.fromisoformat(d), "security")
                rows.append(WorkbenchRow(key=d, date=d, value=text(snap["nav"]), pnl=ms.get("pnl"), fees=ms.get("fees"),
                    opening_value=ms.get("opening_nav"), cash_delta=ms.get("net_flows"),
                    realized=ms.get("realized"), unrealized_change=ms.get("unrealized_change"), known=not ws))
            for d in missing:
                rows.append(WorkbenchRow(key=d, date=d, known=False, description="缺少权威收盘估值"))
        return rows, warnings, missing, metrics

    def overview(self) -> WorkbenchOverview:
        with self.conn_factory() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cur.execute("SET LOCAL statement_timeout = '20s'")
            records = self._read(cur, """SELECT a.account_id, a.account_name, a.initial_capital, a.created_at, a.status,
                p.display_name, p.package_name, s.nav, s.cash, s.market_value, s.snapshot_time, s.position_count,
                COALESCE(n.ledger_scope_id,l.ledger_scope_id) AS ledger_scope_id, n.scope_kind,
                j.status AS replay_status, j.completed_trade_date, j.next_trade_date, j.failure_code,
                j.start_trade_date, j.end_trade_date, j.historical_source_id, j.activation_trade_date
                FROM paper_v2.simulation_account_v1 a
                LEFT JOIN strategy_pkg.package p ON p.package_id = a.package_id
                LEFT JOIN paper_v2.simulation_ledger_scope_v1 n ON n.native_account_id = a.account_id
                LEFT JOIN paper_v2.legacy_localsim_account_lineage_v1 l ON l.account_id = a.account_id
                LEFT JOIN LATERAL (SELECT * FROM paper_v2.daily_snapshots d WHERE d.portfolio_id = COALESCE(n.ledger_scope_id,l.ledger_scope_id)
                    ORDER BY trade_date DESC LIMIT 1) s ON TRUE
                LEFT JOIN LATERAL (SELECT * FROM paper_v2.localsim_replay_job_v1 r WHERE r.simulation_account_id = a.account_id
                    ORDER BY created_at DESC, replay_job_id DESC LIMIT 1) j ON TRUE
                ORDER BY a.created_at, a.account_id""")
            scopes = list({r["ledger_scope_id"] for r in records if r["ledger_scope_id"]})
            batches = {}
            for name, table in (("snapshots", "daily_snapshots"), ("positions", "positions"), ("cash", "cash_ledger"), ("runs", "run")):
                batches[name] = self._read(cur, f"SELECT {READ_COLUMNS[name]} FROM paper_v2.{table} WHERE portfolio_id = ANY(%s)", (scopes,))
            fill_columns = ",".join("f."+column for column in READ_COLUMNS["fills"].split(","))
            batches["fills"] = self._read(cur, f"SELECT {fill_columns}, r.portfolio_id FROM paper_v2.fills f JOIN paper_v2.run r USING (run_id) WHERE r.portfolio_id = ANY(%s)", (scopes,))
            dates = [day(s["trade_date"]) for s in batches["snapshots"]]
            calendar = self._read(cur, "SELECT cal_date FROM market.trading_calendar WHERE is_trading = TRUE AND cal_date BETWEEN %s AND %s ORDER BY cal_date", (min(dates), max(dates))) if dates else []
        if len({r["account_id"] for r in records}) != len(records):
            raise DataUnavailableError("账户账本关联不唯一，请先核对归属")
        scoped: dict[str, dict[str, list]] = {s: {k: [] for k in batches} for s in scopes}
        for key, items in batches.items():
            for item in items:
                scoped[item["portfolio_id"]][key].append(item)
        counts: dict[str, int] = {}
        total_names: dict[str, int] = {}
        for r in records:
            label = readable(r["account_name"], "模拟账户名称待完善")
            total_names[label] = total_names.get(label, 0)+1
        result = []
        for r in records:
            label = readable(r["account_name"], "模拟账户名称待完善")
            counts[label] = counts.get(label, 0)+1
            if total_names[label] > 1:
                label += " · 同名账户" + str(counts[label])
            data = {**scoped.get(r["ledger_scope_id"], {k: [] for k in batches}), "account": r, "package": r,
                "native": r["scope_kind"] == "SUCCESSOR_NATIVE", "calendar": calendar, "replays": [], "events": [], "orders": []}
            if r["replay_status"]:
                data["replays"] = [{**r, "status": r["replay_status"]}]
            summary = self._summary(data)
            summary.name = label
            curve, ws, _, metrics = self._pnl(data, None, None, "date")
            summary.pnl, summary.return_pct = metrics.get("pnl"), metrics.get("return_pct")
            curve.sort(key=lambda row: row.date)
            if curve:
                summary.daily_pnl, summary.daily_date = curve[-1].pnl, curve[-1].date
            if ws and not r.get("failure_code"):
                summary.issue = "收益覆盖存在缺口，请进入账户核验"
            result.append(WorkbenchAccount(key=r["account_id"], summary=summary, curve=curve))
        return self._aggregate_overview(result)

    @staticmethod
    def _aggregate_overview(items: list[WorkbenchAccount]) -> WorkbenchOverview:
        live = [a for a in items if a.summary.comparable_live and a.summary.nav is not None and a.summary.economic_as_of]
        latest = max((a.summary.economic_as_of for a in live), default=None)
        included = [a for a in live if a.summary.economic_as_of == latest]
        result = WorkbenchOverview(items=items, generated_at=datetime.now(UTC).isoformat(), complete=True,
            economic_as_of=latest, included_count=len(included), excluded_count=len(items)-len(included))
        if not included:
            return result
        result.nav = text(sum((money(a.summary.nav) for a in included), ZERO))
        if len({a.summary.daily_date for a in included}) == 1 and all(a.summary.daily_pnl is not None for a in included):
            result.daily_pnl = text(sum((money(a.summary.daily_pnl) for a in included), ZERO))
        if all(a.curve and a.summary.pnl is not None for a in included) and len({a.curve[0].date for a in included}) == 1:
            result.pnl = text(sum((money(a.summary.pnl) for a in included), ZERO))
            capital = sum((money(a.summary.initial_capital) for a in included), ZERO)
            result.return_pct = text(money(result.pnl)/capital*100) if capital else None
        curves = [{p.date: p for p in a.curve} for a in included]
        for d in sorted({d for points in curves for d in points}):
            known = all(d in points and points[d].known and points[d].value is not None for points in curves)
            result.curve.append(WorkbenchRow(key=d, date=d, value=text(sum((money(points[d].value) for points in curves), ZERO)) if known else None, known=known))
        return result

    def export(self, account_id: str, kind: str, **params: Any) -> str:
        headers = {"name": "证券名称", "market": "市场", "date": "交易日", "time": "北京时间", "side": "买卖方向", "status": "业务状态",
            "quantity": "数量", "available": "可卖数量", "filled": "成交数量", "price": "价格", "cost": "成本价", "value": "估值市值或资产",
            "amount": "成交金额", "fees": "交易费用合计", "cash_delta": "资金净变动", "balance": "资金余额", "pnl": "净盈亏",
            "realized": "已实现费用前", "unrealized_change": "浮盈变动", "opening_quantity": "期初数量", "closing_quantity": "期末数量", "description": "业务说明"}
        collected = []
        first = self.view(account_id, kind, page_size=200, **params)
        page = first
        while True:
            collected.extend(page.items)
            if not page.next_cursor:
                break
            page = self.view(account_id, kind, page_size=200, cursor=page.next_cursor, **params)
        out = io.StringIO(newline="")
        writer = csv.writer(out)
        writer.writerow([self._csv_cell(v) for v in ["模拟盘账户", first.summary.name, "策略", first.summary.strategy]])
        writer.writerow(["查询开始", first.requested_start or "全部", "查询结束", first.requested_end or "全部", "实际起点", first.actual_start or "待核验", "实际终点", first.actual_end or "待核验"])
        writer.writerow(["覆盖说明", "完整" if first.status == "complete" else "部分数据", *first.warnings])
        writer.writerow(list(headers.values()))
        for row in collected:
            values = row.model_dump()
            writer.writerow([self._csv_cell(values.get(k)) for k in headers])
        return "\ufeff"+out.getvalue()

    @staticmethod
    def _csv_cell(value: Any) -> str:
        if value is None:
            return "暂不可计算"
        s = str(value)
        # Preserve signed decimal money; neutralize untrusted spreadsheet formulas in names.
        try:
            if Decimal(s).is_finite():
                return s
        except InvalidOperation:
            pass
        return "'"+s if s.lstrip().startswith(("=", "+", "-", "@")) else s
