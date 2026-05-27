"""Read-only Paper Trading v2 live observation dashboard aggregation."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from backend.services.paper_trading_v2.models import PaperRun, PaperSessionStatus, PaperTradingSession
from backend.services.paper_trading_v2.repository import PaperTradingV2Repository
from backend.services.strategy_package.selection_artifact import (
    StrategyPackageSelectionArtifactRepository,
    selection_artifact_runtime_hash,
)
from backend.services.trading_core.errors import DataUnavailableError

from .symbol_names import PaperV2SymbolNameResolver


TERMINAL_SESSION_STATUSES = {
    PaperSessionStatus.SUCCEEDED,
    PaperSessionStatus.FAILED,
    PaperSessionStatus.STOPPED,
}
TICKABLE_SESSION_STATUSES = {
    PaperSessionStatus.CREATED,
    PaperSessionStatus.PREFLIGHTING,
    PaperSessionStatus.REPLAYING,
    PaperSessionStatus.CATCHING_UP,
    PaperSessionStatus.SWITCHING_TO_LIVE,
    PaperSessionStatus.LIVE_RUNNING,
    PaperSessionStatus.LIVE_WAITING_FOR_BAR,
    PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY,
    PaperSessionStatus.LIVE_WAITING_MARKET_WINDOW,
    PaperSessionStatus.LIVE_WAITING_PLATFORM_DATA,
    PaperSessionStatus.LIVE_WAITING_BROKER,
    PaperSessionStatus.LIVE_RETRYING,
}


NO_FILL_REASON_LABELS = {
    "round_lot_zero": "本分钟计划量不足 A 股最小交易单位，不能成交",
    "limit_up_buy_blocked": "涨停，买入受限",
    "limit_down_sell_blocked": "跌停，卖出受限",
    "intraday_halt_or_no_bar": "该分钟无有效行情，可能停牌或缺分钟线",
    "suspended_by_suspend_d": "停牌数据确认不可交易",
    "missing_minute_bar": "分钟线缺失",
    "missing_day_features": "日内执行所需日频特征缺失",
    "no_fill": "本分钟没有成交",
}


class PaperTradingLiveDashboardService:
    """Aggregate persisted Paper v2 runtime details without advancing trading."""

    def __init__(
        self,
        *,
        repository: PaperTradingV2Repository | Any | None = None,
        artifact_repository: StrategyPackageSelectionArtifactRepository | Any | None = None,
        symbol_name_resolver: PaperV2SymbolNameResolver | Any | None = None,
    ) -> None:
        self.repository = repository or PaperTradingV2Repository()
        self.artifact_repository = artifact_repository or StrategyPackageSelectionArtifactRepository()
        self.symbol_name_resolver = symbol_name_resolver

    def get_dashboard(
        self,
        portfolio_id: str,
        *,
        trade_date: date | None = None,
        event_limit: int = 500,
    ) -> dict[str, Any]:
        portfolio = self.repository.get_portfolio(portfolio_id)
        sessions = self.repository.list_sessions(portfolio_id, limit=100)
        tickable_sessions = [session for session in sessions if session.status in TICKABLE_SESSION_STATUSES]
        active_sessions = [session for session in sessions if session.status not in TERMINAL_SESSION_STATUSES]
        active_session = active_sessions[0] if active_sessions else (sessions[0] if sessions else None)
        session_days = self.repository.list_session_days(active_session.session_id) if active_session else []
        selected_day = self._select_session_day(session_days, trade_date=trade_date)
        current_run = self._resolve_current_run(portfolio_id, selected_day, trade_date=trade_date)
        scheduler = self._scheduler_status()
        operability = self._operability(
            portfolio=portfolio,
            sessions=sessions,
            tickable_sessions=tickable_sessions,
            active_session=active_session,
            scheduler=scheduler,
        )
        if trade_date is not None:
            selected_trade_date = trade_date
        elif selected_day is not None:
            selected_trade_date = selected_day.trade_date
        elif current_run is not None:
            selected_trade_date = current_run.trade_date
        else:
            selected_trade_date = None

        run_id = current_run.run_id if current_run else None
        orders = [item.model_dump(mode="json") for item in self.repository.list_orders_for_run(run_id)] if run_id else []
        fills = self.repository.list_fills_for_run(run_id) if run_id else []
        order_events = self.repository.list_order_events(portfolio_id, run_id=run_id, limit=event_limit) if run_id else []
        states = (
            [
                state.model_dump(mode="json")
                for state in self.repository.list_order_execution_states(
                    session_id=active_session.session_id,
                    run_id=run_id,
                )
            ]
            if active_session and run_id
            else []
        )
        intraday_snapshots = self.repository.list_intraday_snapshots_for_portfolio(
            portfolio_id,
            trade_date=selected_trade_date,
            limit=event_limit,
        )
        positions = self._latest_positions(portfolio_id)
        run_events = self.repository.list_run_events(portfolio_id, run_id=run_id, limit=event_limit) if run_id else []
        errors = self.repository.list_errors(portfolio_id, limit=event_limit)
        daily_snapshots = self.repository.list_daily_snapshots(portfolio_id, limit=event_limit)

        warnings = []
        if not sessions:
            warnings.append({"code": "NO_SESSION", "message": "该模拟盘尚未创建运行会话"})
        elif not active_sessions:
            warnings.append({"code": "NO_ACTIVE_SESSION", "message": "当前没有正在推进的运行会话，展示最近会话的只读结果"})
        if len(active_sessions) > 1:
            warnings.append({"code": "MULTIPLE_ACTIVE_SESSIONS", "message": "同一模拟盘存在多个未结束会话，请检查调度范围"})
        if operability["no_operable_session"]:
            warnings.append({"code": "NO_OPERABLE_SESSION", "message": operability["remediation_hint"]})
        if current_run is None:
            warnings.append({"code": "NO_CURRENT_RUN", "message": "当前日期或会话尚未产生 run，无法展示信号到订单链路"})

        dashboard = {
            "portfolio": portfolio.model_dump(mode="json"),
            "package": self._package_context(portfolio),
            "active_session": active_session.model_dump(mode="json") if active_session else None,
            "other_active_sessions": [item.model_dump(mode="json") for item in active_sessions[1:]],
            "session_days": [item.model_dump(mode="json") for item in session_days],
            "current_run": current_run.model_dump(mode="json") if current_run else None,
            "scheduler": scheduler,
            "operability": operability,
            "data_freshness": self._data_freshness(selected_day, active_session),
            "daily_signal": self._daily_signal(portfolio, current_run),
            "target_rebalance": self._target_rebalance(run_events),
            "minute_execution": self._minute_execution_summary(orders, fills, order_events, states),
            "intraday_nav": {
                "status": "AVAILABLE" if intraday_snapshots else "MISSING",
                "missing_reason": None if intraday_snapshots else "尚未持久化分钟资产快照",
                "snapshots": list(reversed(intraday_snapshots)),
            },
            "positions": positions,
            "orders": orders,
            "fills": fills,
            "order_events": order_events,
            "run_events": run_events,
            "errors": errors,
            "daily_snapshots": daily_snapshots,
            "warnings": warnings,
        }
        return self._enrich_display_names(dashboard)

    @staticmethod
    def _operability(
        *,
        portfolio: Any,
        sessions: list[PaperTradingSession],
        tickable_sessions: list[PaperTradingSession],
        active_session: PaperTradingSession | None,
        scheduler: dict[str, Any],
    ) -> dict[str, Any]:
        portfolio_status = getattr(portfolio.status, "value", str(portfolio.status))
        latest_status = active_session.status.value if active_session else None
        latest_mode = active_session.mode.value if active_session else None
        latest_terminal = bool(active_session and active_session.status in TERMINAL_SESSION_STATUSES)
        no_operable = portfolio_status in {"RUNNING", "PAUSED"} and not tickable_sessions
        hint = None
        if no_operable:
            hint = (
                "Portfolio status is active but no scheduler-tickable live/replay session exists. "
                "Review the latest terminal session, then create or resume a session; intraday recovery is allowed."
            )
        return {
            "has_sessions": bool(sessions),
            "tickable_session_count": len(tickable_sessions),
            "has_tickable_session": bool(tickable_sessions),
            "latest_session_status": latest_status,
            "latest_session_mode": latest_mode,
            "latest_session_is_terminal": latest_terminal,
            "no_operable_session": no_operable,
            "scheduler_running": bool(scheduler.get("running")),
            "remediation_hint": hint,
        }

    def list_intraday_snapshots(
        self,
        portfolio_id: str,
        *,
        trade_date: date | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        self.repository.get_portfolio(portfolio_id)
        return self.repository.list_intraday_snapshots_for_portfolio(portfolio_id, trade_date=trade_date, limit=limit)

    def minute_execution(
        self,
        portfolio_id: str,
        *,
        trade_date: date | None = None,
        symbol: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        run = None
        if trade_date is not None:
            run = self.repository.get_run_by_portfolio_date(portfolio_id, trade_date)
        if run is None:
            runs = self.repository.list_runs(portfolio_id, limit=1)
            run = self.repository.get_run(str(runs[0]["run_id"])) if runs else None
        if run is None:
            raise DataUnavailableError("paper v2 portfolio has no run for minute execution timeline", context={"portfolio_id": portfolio_id})
        orders = [item.model_dump(mode="json") for item in self.repository.list_orders_for_run(run.run_id)]
        fills = self.repository.list_fills_for_run(run.run_id)
        events = self.repository.list_order_events(portfolio_id, run_id=run.run_id, limit=limit)
        if symbol:
            events = [item for item in events if str(item.get("symbol") or "") == symbol]
        return self._minute_execution_summary(orders, fills, events, [])

    @staticmethod
    def _package_context(portfolio: Any) -> dict[str, Any]:
        manifest = portfolio.frozen_manifest
        return {
            "package_id": portfolio.package_id,
            "manifest_sha256": portfolio.manifest_sha256,
            "package_name": manifest.package_name,
            "source_type": manifest.source.source_type,
            "source_id": manifest.source.source_id,
            "loop_id": manifest.source.loop_id,
            "alpha_mode": manifest.alpha_mode.value if hasattr(manifest.alpha_mode, "value") else str(manifest.alpha_mode),
        }

    @staticmethod
    def _select_session_day(session_days: list[Any], *, trade_date: date | None) -> Any | None:
        if not session_days:
            return None
        if trade_date is not None:
            matches = [day for day in session_days if day.trade_date == trade_date]
            if matches:
                return matches[-1]
            return None
        with_runs = [day for day in session_days if day.run_id]
        return with_runs[-1] if with_runs else session_days[-1]

    def _resolve_current_run(self, portfolio_id: str, selected_day: Any | None, *, trade_date: date | None) -> PaperRun | None:
        if selected_day and selected_day.run_id:
            try:
                return self.repository.get_run(selected_day.run_id)
            except DataUnavailableError:
                return None
        if trade_date is not None:
            return self.repository.get_run_by_portfolio_date(portfolio_id, trade_date)
        rows = self.repository.list_runs(portfolio_id, limit=1)
        return self.repository.get_run(str(rows[0]["run_id"])) if rows else None

    @staticmethod
    def _data_freshness(selected_day: Any | None, active_session: PaperTradingSession | None) -> dict[str, Any]:
        latest_available = selected_day.latest_available_bar_time if selected_day else None
        last_processed = selected_day.last_processed_bar_time if selected_day else None
        lag_minutes = None
        if isinstance(latest_available, datetime) and isinstance(last_processed, datetime):
            lag_minutes = max(0, int((latest_available - last_processed).total_seconds() // 60))
        status = "NO_SESSION"
        if active_session is not None:
            status = "NEAR_REALTIME" if lag_minutes is not None and lag_minutes <= 1 else active_session.status.value
        return {
            "latest_available_bar_time": latest_available,
            "last_processed_bar_time": last_processed,
            "lag_minutes": lag_minutes,
            "freshness_status": status,
        }

    def _daily_signal(self, portfolio: Any, run: PaperRun | None) -> dict[str, Any]:
        if run is None:
            return {"status": "MISSING", "missing_reason": "尚无 run，无法定位当日信号 artifact"}
        config = run.runtime_config or {}
        signal_data_source = str((config.get("paper_v2_session") or {}).get("signal_data_source") or run.data_source.value)
        try:
            artifact = self.artifact_repository.get(
                package_id=portfolio.package_id,
                manifest_sha256=portfolio.manifest_sha256,
                trade_date=run.trade_date,
                data_source=signal_data_source,
                runtime_config_hash=selection_artifact_runtime_hash(config),
            )
        except DataUnavailableError as exc:
            return {
                "status": "MISSING",
                "missing_reason": exc.message,
                "error": exc.to_dict(),
                "trade_date": run.trade_date,
                "data_source": signal_data_source,
            }
        metadata = artifact.metadata or {}
        return {
            "status": "AVAILABLE",
            "artifact_id": artifact.artifact_id,
            "artifact_sha256": artifact.artifact_sha256,
            "trade_date": artifact.trade_date,
            "data_source": artifact.data_source,
            "runtime_config_hash": artifact.runtime_config_hash,
            "candidate_count": artifact.score_count,
            "universe_count": artifact.universe_count,
            "top_score_symbol": artifact.top_score_symbol,
            "cutoff_date": metadata.get("cutoff_date"),
            "score_trade_date": metadata.get("score_trade_date"),
            "reference_price_trade_date": metadata.get("reference_price_trade_date"),
            "top_candidates": artifact.scores_json[:50],
        }

    @staticmethod
    def _target_rebalance(run_events: list[dict[str, Any]]) -> dict[str, Any]:
        target_event = next((item for item in reversed(run_events) if item.get("event_type") == "TARGETS_GENERATED"), None)
        intent_event = next((item for item in reversed(run_events) if item.get("event_type") == "ORDER_INTENTS_GENERATED"), None)
        target_context = target_event.get("context") if target_event else {}
        intent_context = intent_event.get("context") if intent_event else {}
        return {
            "status": "AVAILABLE" if target_event or intent_event else "MISSING",
            "missing_reason": None if target_event or intent_event else "尚未持久化目标仓位或调仓意图事件",
            "targets": (target_context or {}).get("targets") or [],
            "order_intents": (intent_context or {}).get("intents") or [],
            "target_count": (target_context or {}).get("target_count"),
            "order_intent_count": (intent_context or {}).get("order_intent_count"),
        }

    @staticmethod
    def _minute_execution_summary(
        orders: list[dict[str, Any]],
        fills: list[dict[str, Any]],
        order_events: list[dict[str, Any]],
        states: list[dict[str, Any]],
    ) -> dict[str, Any]:
        timeline = []
        for event in order_events:
            reason = str(event.get("reason") or (event.get("metadata") or {}).get("reason") or "").strip()
            fill_json = event.get("fill_json") or event.get("fill")
            fill_quantity = fill_json.get("quantity") if isinstance(fill_json, dict) else None
            fill_price = fill_json.get("price") if isinstance(fill_json, dict) else None
            timeline.append(
                {
                    "event_time": event.get("event_time"),
                    "order_id": event.get("order_id"),
                    "symbol": event.get("symbol"),
                    "side": event.get("side"),
                    "order_quantity": event.get("order_quantity"),
                    "event_type": event.get("event_type"),
                    "fill_quantity": fill_quantity,
                    "fill_price": fill_price,
                    "remaining_quantity": (event.get("metadata") or {}).get("remaining_quantity"),
                    "algo_step": (event.get("metadata") or {}).get("step"),
                    "reason": reason or None,
                    "reason_label": NO_FILL_REASON_LABELS.get(reason, reason or "事件未提供原因"),
                }
            )
        no_fill_count = sum(1 for item in timeline if str(item.get("event_type") or "").upper() == "NO_FILL")
        return {
            "status": "AVAILABLE" if orders or order_events or fills else "MISSING",
            "missing_reason": None if orders or order_events or fills else "尚无订单、成交或分钟执行事件",
            "summary": {
                "order_count": len(orders),
                "fill_count": len(fills),
                "event_count": len(order_events),
                "no_fill_count": no_fill_count,
                "active_order_count": sum(1 for item in states if item.get("remaining_quantity", 0) and item.get("status") not in {"FILLED", "CANCELLED", "REJECTED"}),
            },
            "timeline": timeline,
            "execution_states": states,
        }

    def _latest_positions(self, portfolio_id: str) -> list[dict[str, Any]]:
        rows = self.repository.list_positions(portfolio_id, limit=1000)
        if not rows:
            return []
        latest_date = max(str(item.get("trade_date") or "") for item in rows)
        return [item for item in rows if str(item.get("trade_date") or "") == latest_date]

    def _enrich_display_names(self, dashboard: dict[str, Any]) -> dict[str, Any]:
        resolver = self.symbol_name_resolver
        if resolver is None:
            return dashboard

        row_groups = [
            dashboard.get("orders") or [],
            dashboard.get("fills") or [],
            dashboard.get("positions") or [],
            ((dashboard.get("daily_signal") or {}).get("top_candidates") or []),
            ((dashboard.get("target_rebalance") or {}).get("targets") or []),
            ((dashboard.get("target_rebalance") or {}).get("order_intents") or []),
            ((dashboard.get("minute_execution") or {}).get("timeline") or []),
        ]
        symbols = [
            str(row.get("symbol") or "").strip()
            for rows in row_groups
            for row in rows
            if isinstance(row, dict) and str(row.get("symbol") or "").strip()
        ]
        try:
            names = resolver.resolve(symbols)
        except Exception:
            return dashboard

        def enrich(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            enriched: list[dict[str, Any]] = []
            for row in rows:
                copied = dict(row)
                symbol = str(copied.get("symbol") or "").strip()
                metadata = copied.get("metadata")
                metadata_name = metadata.get("stock_name") if isinstance(metadata, dict) else None
                name = copied.get("stock_name") or copied.get("symbol_name") or metadata_name or names.get(symbol)
                if name:
                    copied["stock_name"] = str(name)
                    copied["symbol_name"] = str(name)
                enriched.append(copied)
            return enriched

        dashboard["orders"] = enrich(dashboard.get("orders") or [])
        dashboard["fills"] = enrich(dashboard.get("fills") or [])
        dashboard["positions"] = enrich(dashboard.get("positions") or [])

        daily_signal = dict(dashboard.get("daily_signal") or {})
        daily_signal["top_candidates"] = enrich(daily_signal.get("top_candidates") or [])
        dashboard["daily_signal"] = daily_signal

        target_rebalance = dict(dashboard.get("target_rebalance") or {})
        target_rebalance["targets"] = enrich(target_rebalance.get("targets") or [])
        target_rebalance["order_intents"] = enrich(target_rebalance.get("order_intents") or [])
        dashboard["target_rebalance"] = target_rebalance

        minute_execution = dict(dashboard.get("minute_execution") or {})
        minute_execution["timeline"] = enrich(minute_execution.get("timeline") or [])
        dashboard["minute_execution"] = minute_execution
        return dashboard

    @staticmethod
    def _scheduler_status() -> dict[str, Any]:
        try:
            from backend.services.paper_trading_v2.scheduler import paper_trading_v2_scheduler

            return paper_trading_v2_scheduler.status()
        except Exception as exc:  # pragma: no cover - diagnostic only
            return {"status": "UNAVAILABLE", "message": str(exc)}
