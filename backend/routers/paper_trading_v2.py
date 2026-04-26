"""Paper Trading v2 API."""

from __future__ import annotations

import datetime as dt
from datetime import date
from typing import Any, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from backend.db.pg_pool import get_conn
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.readiness import PaperTradingReadinessService
from backend.services.paper_trading_v2.replay import PaperTradingHistoricalReplay
from backend.services.paper_trading_v2.repository import PaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.trading_core.errors import DataUnavailableError, TradingCoreError, UnsupportedFeatureError

router = APIRouter(prefix="/paper-v2", tags=["paper-v2"])


class CreatePortfolioRequest(BaseModel):
    package_id: str = Field(min_length=1)
    portfolio_name: str = Field(min_length=1)
    initial_cash: float = Field(gt=0)
    start_date: date
    data_source: MinuteDataSource
    fee_policy: dict[str, Any] | None = None
    risk_policy: dict[str, Any] | None = None
    execution_policy: dict[str, Any] | None = None


class RunDayRequest(BaseModel):
    trade_date: date
    runtime_config: dict[str, Any] = Field(default_factory=dict)


class ReplayRequest(BaseModel):
    start_date: date
    end_date: date
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    rerun_policy: Literal["reject_existing", "reset_portfolio"] = "reject_existing"
    confirm_reset: bool = False
    confirm_text: str | None = None


class ReadinessRequest(BaseModel):
    trade_date: date
    runtime_config: dict[str, Any] = Field(default_factory=dict)


class ActivateExecutionPolicyRequest(BaseModel):
    trade_date: date
    policy_id: str = Field(min_length=1)
    activated_by: str | None = None
    reason: str | None = None
    replace_existing: bool = False


def _raise_http(exc: TradingCoreError) -> None:
    status_code = 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    elif isinstance(exc, UnsupportedFeatureError):
        status_code = 422
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc


@router.get("/trading-days/defaults")
def get_trading_day_defaults(
    lookback_trading_days: int = 10,
    as_of_date: date | None = None,
    require_minute_data: bool = True,
) -> dict[str, Any]:
    if lookback_trading_days <= 0 or lookback_trading_days > 250:
        raise HTTPException(status_code=400, detail="lookback_trading_days must be between 1 and 250")
    as_of = as_of_date or dt.date.today()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                data_ready_latest_date: date | None = None
                effective_end = as_of
                if require_minute_data:
                    cur.execute(
                        """
                        SELECT MAX(trade_date)
                        FROM market.dataset_date_refresh_audit
                        WHERE dataset = 'stk_limit'
                          AND status = 'success'
                          AND row_count > 0
                          AND trade_date <= %s
                        """,
                        (as_of,),
                    )
                    data_ready_row = cur.fetchone()
                    if data_ready_row and data_ready_row[0]:
                        data_ready_latest_date = data_ready_row[0]
                        effective_end = min(as_of, data_ready_latest_date)
                cur.execute(
                    """
                    SELECT cal_date
                    FROM market.trading_calendar
                    WHERE cal_date <= %s AND is_trading = TRUE
                    ORDER BY cal_date DESC
                    LIMIT %s
                    """,
                    (effective_end, lookback_trading_days),
                )
                rows = cur.fetchall()
                cur.execute(
                    """
                    SELECT MIN(cal_date)
                    FROM market.trading_calendar
                    WHERE cal_date > %s AND is_trading = TRUE
                    """,
                    (as_of,),
                )
                next_row = cur.fetchone()
    except Exception as exc:
        _raise_http(DataUnavailableError(
            "trading calendar defaults query failed",
            context={
                "as_of_date": as_of.isoformat(),
                "lookback_trading_days": lookback_trading_days,
                "require_minute_data": require_minute_data,
                "error": str(exc),
            },
        ))
    if not rows:
        _raise_http(DataUnavailableError(
            "trading calendar has no completed trading day for defaults",
            context={
                "as_of_date": as_of.isoformat(),
                "lookback_trading_days": lookback_trading_days,
                "require_minute_data": require_minute_data,
            },
        ))
    dates = [row[0] for row in rows]
    latest = dates[0]
    replay_start = dates[-1]
    next_trading_day = next_row[0] if next_row and next_row[0] else None
    return {
        "ok": True,
        "as_of_date": as_of.isoformat(),
        "lookback_trading_days": lookback_trading_days,
        "require_minute_data": require_minute_data,
        "data_ready_latest_date": data_ready_latest_date.isoformat() if data_ready_latest_date else None,
        "latest_trading_day": latest.isoformat(),
        "replay_start_date": replay_start.isoformat(),
        "replay_end_date": latest.isoformat(),
        "available_trading_day_count": len(dates),
        "next_trading_day": next_trading_day.isoformat() if next_trading_day else None,
    }


@router.post("/portfolios")
def create_portfolio(req: CreatePortfolioRequest) -> dict[str, Any]:
    try:
        portfolio = PaperTradingV2PortfolioService().create_portfolio(
            package_id=req.package_id,
            portfolio_name=req.portfolio_name,
            initial_cash=req.initial_cash,
            start_date=req.start_date,
            data_source=req.data_source,
            fee_policy=req.fee_policy,
            risk_policy=req.risk_policy,
            execution_policy=req.execution_policy,
        )
        return {"ok": True, "portfolio": portfolio.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios")
def list_portfolios(limit: int = 100) -> dict[str, Any]:
    try:
        portfolios = PaperTradingV2PortfolioService().list_portfolios(limit=limit)
        return {"ok": True, "portfolios": [item.model_dump(mode="json") for item in portfolios]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}")
def get_portfolio(portfolio_id: str) -> dict[str, Any]:
    try:
        portfolio = PaperTradingV2PortfolioService().get_portfolio(portfolio_id)
        return {"ok": True, "portfolio": portfolio.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/portfolios/{portfolio_id}/pause")
def pause_portfolio(portfolio_id: str) -> dict[str, Any]:
    try:
        portfolio = PaperTradingV2PortfolioService().pause_portfolio(portfolio_id)
        return {"ok": True, "portfolio": portfolio.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/portfolios/{portfolio_id}/resume")
def resume_portfolio(portfolio_id: str) -> dict[str, Any]:
    try:
        portfolio = PaperTradingV2PortfolioService().resume_portfolio(portfolio_id)
        return {"ok": True, "portfolio": portfolio.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/portfolios/{portfolio_id}/complete")
def complete_portfolio(portfolio_id: str) -> dict[str, Any]:
    try:
        portfolio = PaperTradingV2PortfolioService().complete_portfolio(portfolio_id)
        return {"ok": True, "portfolio": portfolio.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/portfolios/{portfolio_id}/retire")
def retire_portfolio(portfolio_id: str) -> dict[str, Any]:
    try:
        portfolio = PaperTradingV2PortfolioService().retire_portfolio(portfolio_id)
        return {"ok": True, "portfolio": portfolio.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/portfolios/{portfolio_id}/run-day")
def run_portfolio_day(portfolio_id: str, req: RunDayRequest) -> dict[str, Any]:
    try:
        result = PaperTradingDayRunner().run_day(
            portfolio_id=portfolio_id,
            trade_date=req.trade_date,
            runtime_config=req.runtime_config,
        )
        return {"ok": True, "result": result.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/portfolios/{portfolio_id}/readiness")
def check_portfolio_day_readiness(portfolio_id: str, req: ReadinessRequest) -> dict[str, Any]:
    try:
        result = PaperTradingReadinessService().check_day(
            portfolio_id=portfolio_id,
            trade_date=req.trade_date,
            runtime_config=req.runtime_config,
        )
        return {"ok": True, "readiness": result.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/execution-policies")
def list_portfolio_execution_policies(portfolio_id: str) -> dict[str, Any]:
    try:
        policies = PaperTradingV2PortfolioService().list_execution_policies(portfolio_id)
        return {"ok": True, "portfolio_id": portfolio_id, "execution_policies": policies}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/portfolios/{portfolio_id}/execution-policy-activations")
def activate_portfolio_execution_policy(portfolio_id: str, req: ActivateExecutionPolicyRequest) -> dict[str, Any]:
    try:
        activation = PaperTradingV2PortfolioService().activate_execution_policy(
            portfolio_id=portfolio_id,
            trade_date=req.trade_date,
            policy_id=req.policy_id,
            activated_by=req.activated_by,
            reason=req.reason,
            replace_existing=req.replace_existing,
        )
        return {"ok": True, "activation": activation.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/execution-policy-activations")
def list_portfolio_execution_policy_activations(portfolio_id: str, limit: int = 100) -> dict[str, Any]:
    try:
        activations = PaperTradingV2PortfolioService().list_execution_policy_activations(portfolio_id, limit=limit)
        return {
            "ok": True,
            "portfolio_id": portfolio_id,
            "activations": [activation.model_dump(mode="json") for activation in activations],
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/portfolios/{portfolio_id}/replay")
def replay_portfolio(portfolio_id: str, req: ReplayRequest) -> dict[str, Any]:
    try:
        result = PaperTradingHistoricalReplay().run(
            portfolio_id=portfolio_id,
            start_date=req.start_date,
            end_date=req.end_date,
            runtime_config=req.runtime_config,
            rerun_policy=req.rerun_policy,
            confirm_reset=req.confirm_reset,
            confirm_text=req.confirm_text,
        )
        return {"ok": True, "result": result.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/orders")
def list_orders(portfolio_id: str, limit: int = 500) -> dict[str, Any]:
    try:
        return {"ok": True, "orders": PaperTradingV2Repository().list_orders(portfolio_id, limit=limit)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/fills")
def list_fills(portfolio_id: str, limit: int = 500) -> dict[str, Any]:
    try:
        return {"ok": True, "fills": PaperTradingV2Repository().list_fills(portfolio_id, limit=limit)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/cash-ledger")
def list_cash_ledger(portfolio_id: str, limit: int = 500) -> dict[str, Any]:
    try:
        return {"ok": True, "cash_ledger": PaperTradingV2Repository().list_cash_ledger(portfolio_id, limit=limit)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/positions")
def list_positions(portfolio_id: str, limit: int = 500) -> dict[str, Any]:
    try:
        return {"ok": True, "positions": PaperTradingV2Repository().list_positions(portfolio_id, limit=limit)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/daily-snapshots")
def list_daily_snapshots(portfolio_id: str, limit: int = 500) -> dict[str, Any]:
    try:
        return {"ok": True, "daily_snapshots": PaperTradingV2Repository().list_daily_snapshots(portfolio_id, limit=limit)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/performance-report")
def get_performance_report(portfolio_id: str) -> dict[str, Any]:
    try:
        return {"ok": True, "performance_report": PaperTradingV2PortfolioService().performance_report(portfolio_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/runs")
def list_runs(portfolio_id: str, limit: int = 500) -> dict[str, Any]:
    try:
        return {"ok": True, "runs": PaperTradingV2Repository().list_runs(portfolio_id, limit=limit)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/run-events")
def list_run_events(portfolio_id: str, run_id: str | None = None, limit: int = 500) -> dict[str, Any]:
    try:
        return {
            "ok": True,
            "run_events": PaperTradingV2Repository().list_run_events(
                portfolio_id,
                run_id=run_id,
                limit=limit,
            ),
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/errors")
def list_errors(portfolio_id: str, limit: int = 500) -> dict[str, Any]:
    try:
        return {"ok": True, "errors": PaperTradingV2Repository().list_errors(portfolio_id, limit=limit)}
    except TradingCoreError as exc:
        _raise_http(exc)
