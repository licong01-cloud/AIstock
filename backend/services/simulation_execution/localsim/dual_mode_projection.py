"""Bounded read-only projections of native LocalSIM research receipts."""

from __future__ import annotations

from datetime import UTC, datetime, date
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re

from backend.services.position_timing.intraday.contracts import digest
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError
from backend.services.trading_core.ledger import InMemoryLedger
from backend.services.trading_core.models import PositionLot


RULE_LABELS = {"LOCALSIM_NATIVE_TWAP": "原生分时执行", "FIRST_OBSERVABLE_V1": "简单执行",
               "PRICE_TOUCH_V1": "到价执行", "BOUNDED_REVERSION_V1": "有限等待"}
STATUS_LABELS = {"FILLED": "全部成交", "PARTIAL": "部分成交", "NO_FILL": "未成交",
                 "UNAVAILABLE": "不可用", "SUBMITTED": "已提交", "CANCELLED": "已取消",
                 "PARTIALLY_FILLED": "部分成交", "REJECTED": "已拒绝",
                 "QUEUED": "排队等待", "DISPATCHING": "正在处理", "COMPLETED": "操作已确认", "INVALIDATED": "机会已失效"}
REASON_LABELS = {"OPPORTUNITY_EXPIRED_IN_QUEUE": "响应或排队期间机会过期",
                 "STALE_OPPORTUNITY_AT_DISPATCH": "派发时机会已失效", "PLAN_REVISED": "交易计划修订",
                 "PLAN_HARD_EXPIRY": "委托到达硬期限", "COMMON_TERMINAL_1457": "共同研究终点关闭委托",
                 "HUMAN_CANCEL_CONFIRMED": "模拟操作员已确认撤单", "OPPORTUNITY_WAITING": "等待择时条件",
                 "OPPORTUNITY_UNAVAILABLE": "择时必要输入不可用", "OPPORTUNITY_WITHDRAWN": "择时机会撤回",
                 "SUSPENDED_BY_SUSPEND_D": "当日停牌", "SUSPENDED_OR_NON_TRADING_DAY": "停牌或非交易日",
                 "INSUFFICIENT_CASH": "可用现金不足", "HARD_PRICE_CONSTRAINT": "未满足硬价格限制",
                 "QUANTITY_OR_LIQUIDITY_LIMIT": "数量或分钟流动性限制", "WAITING_FOR_DELAY": "等待冻结响应时长",
                 "NO_EXECUTABLE_INTENT": "当前没有可执行意图"}
REASON_LABELS.update({"OPPORTUNITY_WAIT":"等待择时条件", "WAITING_FOR_BAR":"等待已完成分钟行情",
                     "NO_AVAILABLE_CASH":"无可用现金", "NO_SELLABLE_QUANTITY":"无可卖数量",
                     "LIMIT_UP_BUY_BLOCKED":"涨停限制买入", "LIMIT_DOWN_SELL_BLOCKED":"跌停限制卖出",
                     "SESSION_BREAK":"非连续交易时段", "NO_OBSERVED_TRADE":"该分钟没有观察到交易",
                     "PLAN_COMPLETED":"计划已按确认成交完成"})


def portable_path(value: str) -> Path:
    if os.name == "nt" and re.match(r"^/mnt/[fx]/", value):
        return Path(value[5].upper() + ":/" + value[7:])
    return Path(value)


class DualModeReadService:
    def __init__(self, *, root: Path | None = None, name_resolver=None):
        configured = str(root or os.getenv("LOCALSIM_DUAL_MODE_RESEARCH_ROOT", "")).strip()
        self.root = portable_path(configured).resolve() if configured else None
        self.name_resolver = name_resolver

    def _json(self, path: Path):
        try:
            return self._parse(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise DataUnavailableError("模拟研究文件读取或核验失败") from exc

    @staticmethod
    def _parse(raw):
        def invalid(value):
            raise ValueError("NON_FINITE_JSON")
        return json.loads(raw, parse_constant=invalid)

    def _ref(self, value: dict, *, inside: bool = True):
        if set(value) != {"path", "sha256"} or not re.fullmatch(r"[a-f0-9]{64}", str(value["sha256"])):
            raise DataUnavailableError("模拟研究收据身份无效")
        path = portable_path(value["path"]).resolve()
        if inside and (self.root is None or not path.is_relative_to(self.root)):
            raise DataUnavailableError("模拟研究收据超出本研究目录")
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise DataUnavailableError("模拟研究收据缺失") from exc
        if hashlib.sha256(raw).hexdigest() != value["sha256"]:
            raise DataUnavailableError("模拟研究收据完整性校验失败")
        try:
            return self._parse(raw)
        except ValueError as exc:
            raise DataUnavailableError("模拟研究收据格式无效") from exc

    def _study(self, key):
        try:
            return self._load_study(key)
        except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
            raise DataUnavailableError("模拟研究合同或数值核验失败") from exc

    def _load_study(self, key):
        if self.root is None or not re.fullmatch(r"[a-f0-9]{64}", key):
            raise DataUnavailableError("尚无对应的模拟研究")
        folder = (self.root / key).resolve()
        if not folder.is_relative_to(self.root):
            raise DataUnavailableError("模拟研究路径无效")
        request = self._json(folder / "request.json")
        fields = {k: v for k, v in request.items() if k not in {"identity", "release_identity"}}
        if request.get("identity") != key or digest(fields) != key or request.get("schema_version") != "localsim_dual_mode_study_v1":
            raise DataUnavailableError("模拟研究请求未通过核验")
        days = request["trade_dates"]
        if not days or days != sorted(set(days)) or any(date.fromisoformat(d).isoformat() != d for d in days):
            raise DataUnavailableError("模拟研究日期人口无效")
        results = None
        if (folder / "result_receipt.json").exists():
            receipt = self._json(folder / "result_receipt.json")
            if receipt.get("schema_version") != "localsim_dual_mode_result_receipt_v1":
                raise DataUnavailableError("模拟研究完成收据无效")
            pinned = self._ref(receipt["request"])
            if pinned != request:
                raise DataUnavailableError("模拟研究请求来源不一致")
            results = self._ref(receipt["results"])
            inspection = self._ref(results["source_inspection"])
            if (results.get("schema_version") != "localsim_dual_mode_results_v1" or results.get("study_identity") != key
                    or results.get("outcomes_read_stage") != "AFTER_COMPLETE_SOURCE_BATCH_INSPECTION"
                    or inspection.get("stage") != "COMPLETE_SOURCE_BATCH_INSPECTED"
                    or inspection.get("request") != results.get("request")
                    or inspection.get("candidate_manifest") != request["candidate_manifest"]
                    or inspection.get("source_identity") != request["source_identity"]
                    or inspection.get("trade_dates") != days
                    or results.get("request") != receipt["request"]
                    or results.get("trade_dates") != days
                    or len(results["accounts"]) != len(request["population"])):
                raise DataUnavailableError("模拟研究结果与冻结来源不一致")
            for account, profile in zip(results["accounts"], request["population"]):
                if (account["account_id"] != "lsdm_" + digest([key, profile])[:40]
                        or any(account[field] != profile[field] for field in ("operation", "rule", "delay_seconds"))
                        or len(account["days"]) != len(days)
                        or [d["date"] for d in account["daily_nav"]] != days
                        or [d["date"] for d in account["day_views"]] != days):
                    raise DataUnavailableError("模拟研究账户人口或身份无效")
                values = [account[f] for f in ("terminal_nav_cny", "initial_cash_cny", "max_drawdown", "fees_cny")]
                values += [d["nav_cny"] for d in account["daily_nav"]]
                if (any(not Decimal(str(v)).is_finite() for v in values)
                        or account["initial_cash_cny"] != "10000000" or not 0 <= Decimal(account["max_drawdown"]) <= 1
                        or Decimal(account["fees_cny"]) < 0):
                    raise DataUnavailableError("模拟研究账户数值无效")
        return request, results

    def _name(self, request):
        if self.name_resolver is None:
            return "策略包对比研究"
        snapshot = self._ref(request["package_snapshot"], inside=False)["package"]
        return self.name_resolver({"manifest_json": snapshot["manifest"],
                                   "manifest_sha256": request["manifest_sha256"],
                                   "display_name": snapshot.get("display_name"), "package_name": snapshot.get("package_name")},
                                  request["manifest_sha256"])

    def list_studies(self):
        items = []
        if self.root is not None and self.root.exists():
            for folder in sorted(self.root.iterdir()):
                if folder.is_dir() and re.fullmatch(r"[a-f0-9]{64}", folder.name) and (folder / "request.json").is_file():
                    request, results = self._study(folder.name)
                    items.append({"key": folder.name, "name": self._name(request), "start_date": request["trade_dates"][0],
                                  "end_date": request["trade_dates"][-1], "account_count": len(request["population"]),
                                  "status": "已完成回放" if results else "回放受阻，已保留完成前缀" if any((folder / "operational").glob("*.json")) else "回放尚未完成",
                                  "complete": results is not None,
                                  "purpose": "接口验证短窗口" if request["purpose"] == "ENGINEERING_SHORT_WINDOW" else "连续历史研究"})
        return {"schema_version": "localsim_dual_mode_overview_v1", "configured": self.root is not None,
                "items": items, "generated_at": datetime.now(UTC).isoformat()}

    def detail(self, key):
        request, results = self._study(key)
        accounts = []
        if results:
            for account in results["accounts"]:
                native = account["operation"]["operation_mode"] == "NATIVE"
                statuses = account["status_counts"]
                count = sum(statuses.values())
                latest = self._ref(account["days"][-1])
                if (latest["account_id"] != account["account_id"] or latest["study_identity"] != key
                        or latest["trade_date"] != request["trade_dates"][-1]
                        or Decimal(latest["terminal_nav_cny"]) != Decimal(account["terminal_nav_cny"])):
                    raise DataUnavailableError("账户最新资产收据与研究结果不一致")
                constraints = {}
                for reason, number in account["reason_counts"].items():
                    label = REASON_LABELS.get(reason, "其他已记录的执行约束")
                    constraints[label] = constraints.get(label, 0) + number
                accounts.append({"key": account["account_id"], "mode": "原生执行" if native else "模拟人工",
                    "rule": RULE_LABELS[account["rule"]], "delay_seconds": account["delay_seconds"],
                    "name": "原生分时执行" if native else RULE_LABELS[account["rule"]] + f" · {account['delay_seconds']//60}分钟响应",
                    "initial_cash": account["initial_cash_cny"], "nav": account["terminal_nav_cny"],
                    "pnl": str(Decimal(account["terminal_nav_cny"]) - Decimal(account["initial_cash_cny"])),
                    "cash": latest["cash_cny"],
                    "holdings_value": str(Decimal(latest["terminal_nav_cny"]) - Decimal(latest["cash_cny"])),
                    "last_day_pnl": str(Decimal(latest["terminal_nav_cny"]) - Decimal(latest["initial_nav_cny"])),
                    "last_trade_date": latest["trade_date"],
                    "return_pct": format((Decimal(account["terminal_nav_cny"]) / Decimal(account["initial_cash_cny"]) - 1) * 100, "f"),
                    "max_drawdown_pct": format(Decimal(account["max_drawdown"]) * 100, "f"), "fees": account["fees_cny"],
                    "completion_pct": str(Decimal(statuses.get("FILLED", 0)) * 100 / count) if count else None,
                    "statuses": [{"name": STATUS_LABELS.get(s, "状态待核验"), "count": n} for s,n in statuses.items()],
                    "constraints": [{"name": name, "count": n} for name,n in constraints.items()],
                    "curve": account["daily_nav"], "operations": account.get("human_operation_metrics", {})})
        return {"schema_version": "localsim_dual_mode_detail_v1", "key": key, "name": self._name(request),
                "complete": results is not None, "start_date": request["trade_dates"][0], "end_date": request["trade_dates"][-1],
                "accounts": accounts, "comparisons": results["comparisons"] if results else [],
                "explanation": "两模式比较包含执行规则、人工响应和持仓分化的共同影响；分钟代理不保证真实成交。收益采用冻结估值口径，未证明稳定超额收益。",
                "generated_at": datetime.now(UTC).isoformat()}

    def account_view(self, key, account_id, *, view, start=None, end=None, cursor=None, page_size=50):
        if view not in {"positions", "trades", "orders", "actions", "pnl"} or not 1 <= page_size <= 200:
            raise RuntimeConfigInvalidError("模拟研究查询参数无效")
        for value in (start,end):
            if value:
                try:
                    date.fromisoformat(value)
                except ValueError as exc:
                    raise RuntimeConfigInvalidError("查询日期格式无效") from exc
        if start and end and start > end:
            raise RuntimeConfigInvalidError("开始日期不能晚于结束日期")
        request, results = self._study(key)
        if results is None:
            raise DataUnavailableError("回放尚未完成，当前没有完整账户结果")
        account = next((a for a in results["accounts"] if a["account_id"] == account_id), None)
        if account is None:
            raise DataUnavailableError("该研究中没有对应账户")
        identity = digest([results["request"], account_id, view, start, end])
        offset = 0
        if cursor:
            parts = cursor.split(":")
            if len(parts) != 2 or parts[0] != identity or not parts[1].isdigit():
                raise RuntimeConfigInvalidError("查询游标与当前账户或条件不一致")
            offset = int(parts[1])
        selected = [r for r in account["days"] if (not start or portable_path(r["path"]).stem >= start)
                     and (not end or portable_path(r["path"]).stem <= end)]
        if view == "positions":
            selected = selected[-1:]
        counts = {d["date"]: d["counts"][view] for d in account["day_views"]}
        if any(type(v) is not int or v < 0 for v in counts.values()):
            raise DataUnavailableError("账户分页计数无效")
        total = sum(counts[portable_path(r["path"]).stem] for r in selected)
        skip = offset
        rows = []
        for reference in selected:
            count = counts[portable_path(reference["path"]).stem]
            if skip >= count:
                skip -= count
                continue
            before = len(rows)
            day = self._ref(reference)
            envelope = self._ref(day["account_receipt"])
            if set(envelope) != {"sha256", "payload"} or digest(envelope["payload"]) != envelope["sha256"]:
                raise DataUnavailableError("账户日收据信封完整性校验失败")
            record = envelope["payload"]
            if (record["checkpoint"]["account"]["portfolio_id"] != account_id
                    or record["checkpoint"]["account"]["trade_date"] != day["trade_date"]
                    or day["study_identity"] != key
                    or record["receipt"]["economic_facts"]["account_sha256"] != digest(record["checkpoint"]["account"])
                    or record["outbox"]["projection_payload"] != record["checkpoint"]):
                raise DataUnavailableError("账户日收据身份不一致")
            facts = record["facts"]
            names = {r["symbol"]: r.get("stock_name") or r.get("name") for r in day["native_quantity_evidence"]["targets"]}
            projection = self._ref(day["native_quantity_evidence"]["projection"], inside=False)
            names.update({r["symbol"]:r.get("stock_name") or r.get("name")
                          for r in projection["aggregate_results"]})
            def security_name(symbol):
                name = names.get(symbol)
                return name if isinstance(name, str) and re.search(r"[\u3400-\u9fff]", name) else "证券简称未提供"
            if view == "pnl":
                rows.append({"key": day["trade_date"], "date": day["trade_date"], "opening_nav": day["initial_nav_cny"], "nav": day["terminal_nav_cny"],
                             "pnl": str(Decimal(day["terminal_nav_cny"]) - Decimal(day["initial_nav_cny"])), "fees": day["fees_cny"]})
            elif view == "positions":
                ledger = InMemoryLedger(portfolio_id=account_id, initial_cash=Decimal("10000000"))
                ledger.cash = Decimal(day["cash_cny"])
                ledger.positions = {s: PositionLot.model_validate(p) for s,p in day["positions"].items()}
                ledger.account_snapshot(prices={s: float(p) for s,p in day["marks_raw_cny"].items()},
                                        snapshot_time=datetime.fromisoformat(day["terminal_as_of"]))
                for symbol, position in ledger.positions.items():
                    price = Decimal(day["marks_raw_cny"][symbol])
                    rows.append({"key": symbol, "name": security_name(symbol), "quantity": position.quantity,
                                 "available": position.available_quantity, "cost": str(position.avg_cost), "price": str(price),
                                 "value": str(price * position.quantity),
                                 "pnl": str((price-Decimal(str(position.avg_cost))) * position.quantity), "date": day["trade_date"]})
            elif view == "trades":
                for fill in sorted(facts["fills"].values(), key=lambda f:(f["trade_time"],f["fill_id"])):
                    fee = facts["cash_entries"][fill["fill_id"]]["fee"]
                    rows.append({"key": fill["fill_id"], "name": security_name(fill["symbol"]), "time": fill["trade_time"],
                                 "side": "买入" if fill["side"] == "BUY" else "卖出", "quantity": fill["quantity"],
                                 "price": str(fill["price"]), "fees": str(fee)})
            elif view == "orders":
                for order in facts["orders"].values():
                    rows.append({"key": order["order_id"], "name": security_name(order["symbol"]), "time": order["created_at"],
                                 "side": "买入" if order["side"] == "BUY" else "卖出", "quantity": order["quantity"],
                                 "filled": order["filled_quantity"], "status": STATUS_LABELS.get(order["status"], "状态待核验")})
            else:
                for action in day.get("human_operation_state", {}).get("actions", []):
                    rows.append({"key": action["action_id"], "name": security_name(action["symbol"]), "time": action["dispatch_at"] or action["visible_at"],
                                 "kind": "下单尝试" if action["kind"] == "SUBMIT" else "撤单尝试",
                                 "status": STATUS_LABELS[action["status"]],
                                 "waiting_seconds": str((datetime.fromisoformat(action["dispatch_at"].replace("Z", "+00:00")) -
                                                          datetime.fromisoformat(action["visible_at"].replace("Z", "+00:00"))).total_seconds()) if action["dispatch_at"] else None})
            if len(rows) - before != count or day["view_counts"][view] != count:
                raise DataUnavailableError("账户分页计数与原生事实不一致")
            if skip:
                del rows[before:before+skip]
                skip = 0
            if len(rows) >= page_size:
                break
        return {"schema_version": "localsim_dual_mode_account_view_v1", "items": rows[:page_size],
                "total_count": total, "next_cursor": f"{identity}:{offset+page_size}" if offset+page_size<total else None,
                "snapshot_set": identity, "view": view, "start": start, "end": end,
                "generated_at": datetime.now(UTC).isoformat()}
