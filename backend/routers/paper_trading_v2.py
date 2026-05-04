"""Paper Trading v2 API."""

from __future__ import annotations

import datetime as dt
from datetime import date
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.db.pg_pool import get_conn
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.live_dashboard import PaperTradingLiveDashboardService
from backend.services.paper_trading_v2.readiness import PaperTradingReadinessService
from backend.services.paper_trading_v2.replay import PaperTradingHistoricalReplay
from backend.services.paper_trading_v2.repository import PaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.paper_trading_v2.session import PaperTradingSessionRunner, PaperTradingSessionService
from backend.services.paper_trading_v2.models import PaperSessionMode
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


class CreateRuntimeProfileRequest(BaseModel):
    profile_name: str = Field(min_length=1)
    config_json: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = None
    reason: str | None = None


class CreateRuntimeProfileVersionRequest(BaseModel):
    config_json: dict[str, Any] = Field(default_factory=dict)
    created_by: str | None = None
    reason: str | None = None


class ActivateRuntimeConfigRequest(BaseModel):
    trade_date: date
    profile_version_id: str = Field(min_length=1)
    activated_by: str | None = None
    reason: str | None = None
    replace_existing: bool = False


class CreateSessionRequest(BaseModel):
    mode: PaperSessionMode
    start_date: date
    end_date: date | None = None
    historical_data_source: MinuteDataSource | None = None
    live_data_source: MinuteDataSource | None = None
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    rerun_policy: Literal["reject_existing", "reset_portfolio"] = "reject_existing"
    auto_switch_to_live: bool = False
    confirm_reset: bool = False
    confirm_text: str | None = None
    created_by: str | None = None


class TickSessionRequest(BaseModel):
    as_of_time: dt.datetime | None = None
    allow_paused: bool = False


class SchedulerStartRequest(BaseModel):
    interval_seconds: int | None = Field(default=None, ge=1, le=3600)


class SchedulerRunOnceRequest(BaseModel):
    limit: int = Field(default=50, ge=1, le=500)
    as_of_time: dt.datetime | None = None


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


@router.get("/running-summary")
def list_running_portfolio_summary(
    limit: int | None = Query(default=None, ge=1, le=500),
    page: int = Query(default=1, ge=1),
    page_size: int | None = Query(default=None, ge=1, le=50),
    snapshot_limit: int = Query(default=30, ge=1, le=240),
    position_limit: int = Query(default=8, ge=1, le=100),
    status: list[str] | None = Query(default=None),
    sort_by: str = Query(default="latest_run_time"),
    sort_dir: str = Query(default="desc"),
    search: str | None = Query(default=None),
    search_fields: list[str] | None = Query(default=None),
    min_initial_cash: float | None = Query(default=None, ge=0),
    max_initial_cash: float | None = Query(default=None, ge=0),
) -> dict[str, Any]:
    try:
        effective_page_size = min(page_size if page_size is not None else (limit or 20), 50)
        page_data = PaperTradingV2PortfolioService().running_summary_page(
            page=page,
            page_size=effective_page_size,
            snapshot_limit=snapshot_limit,
            position_limit=position_limit,
            statuses=status,
            sort_by=sort_by,
            sort_dir=sort_dir,
            search=search,
            search_fields=search_fields,
            min_initial_cash=min_initial_cash,
            max_initial_cash=max_initial_cash,
        )
        return {"ok": True, **page_data}
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


@router.post("/portfolios/{portfolio_id}/runtime-profiles")
def create_portfolio_runtime_profile(portfolio_id: str, req: CreateRuntimeProfileRequest) -> dict[str, Any]:
    try:
        profile, version = PaperTradingV2PortfolioService().create_runtime_profile(
            portfolio_id=portfolio_id,
            profile_name=req.profile_name,
            config_json=req.config_json,
            created_by=req.created_by,
            reason=req.reason,
        )
        return {
            "ok": True,
            "profile": profile.model_dump(mode="json"),
            "version": version.model_dump(mode="json"),
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/runtime-profiles")
def list_portfolio_runtime_profiles(portfolio_id: str, limit: int = 100) -> dict[str, Any]:
    try:
        profiles = PaperTradingV2PortfolioService().list_runtime_profiles(portfolio_id, limit=limit)
        return {"ok": True, "profiles": [profile.model_dump(mode="json") for profile in profiles]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/portfolios/{portfolio_id}/runtime-profiles/{profile_id}/versions")
def create_portfolio_runtime_profile_version(
    portfolio_id: str,
    profile_id: str,
    req: CreateRuntimeProfileVersionRequest,
) -> dict[str, Any]:
    try:
        version = PaperTradingV2PortfolioService().create_runtime_profile_version(
            portfolio_id=portfolio_id,
            profile_id=profile_id,
            config_json=req.config_json,
            created_by=req.created_by,
            reason=req.reason,
        )
        return {"ok": True, "version": version.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/runtime-profiles/{profile_id}/versions")
def list_portfolio_runtime_profile_versions(
    portfolio_id: str,
    profile_id: str,
    limit: int = 100,
) -> dict[str, Any]:
    try:
        profile = PaperTradingV2PortfolioService().repository.get_runtime_profile(profile_id)
        if profile.portfolio_id != portfolio_id:
            _raise_http(
                DataUnavailableError(
                    "paper v2 runtime profile does not belong to portfolio",
                    context={"portfolio_id": portfolio_id, "profile_id": profile_id},
                )
            )
        versions = PaperTradingV2PortfolioService().list_runtime_profile_versions(profile_id, limit=limit)
        return {"ok": True, "versions": [version.model_dump(mode="json") for version in versions]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/portfolios/{portfolio_id}/runtime-config-activations")
def activate_portfolio_runtime_config(portfolio_id: str, req: ActivateRuntimeConfigRequest) -> dict[str, Any]:
    try:
        activation = PaperTradingV2PortfolioService().activate_runtime_config(
            portfolio_id=portfolio_id,
            trade_date=req.trade_date,
            profile_version_id=req.profile_version_id,
            activated_by=req.activated_by,
            reason=req.reason,
            replace_existing=req.replace_existing,
        )
        return {"ok": True, "activation": activation.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/runtime-config-activations")
def list_portfolio_runtime_config_activations(portfolio_id: str, limit: int = 100) -> dict[str, Any]:
    try:
        activations = PaperTradingV2PortfolioService().list_runtime_config_activations(portfolio_id, limit=limit)
        return {
            "ok": True,
            "portfolio_id": portfolio_id,
            "activations": [activation.model_dump(mode="json") for activation in activations],
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/config-change-audit")
def list_portfolio_config_change_audit(portfolio_id: str, limit: int = 200) -> dict[str, Any]:
    try:
        rows = PaperTradingV2PortfolioService().list_config_change_audit(portfolio_id, limit=limit)
        return {"ok": True, "audit": [row.model_dump(mode="json") for row in rows]}
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


@router.post("/portfolios/{portfolio_id}/sessions")
def create_portfolio_session(portfolio_id: str, req: CreateSessionRequest) -> dict[str, Any]:
    try:
        session = PaperTradingSessionService().create_session(
            portfolio_id=portfolio_id,
            mode=req.mode,
            start_date=req.start_date,
            end_date=req.end_date,
            historical_data_source=req.historical_data_source,
            live_data_source=req.live_data_source,
            runtime_config=req.runtime_config,
            rerun_policy=req.rerun_policy,
            auto_switch_to_live=req.auto_switch_to_live,
            confirm_reset=req.confirm_reset,
            confirm_text=req.confirm_text,
            created_by=req.created_by,
        )
        return {"ok": True, "session": session.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/sessions")
def list_portfolio_sessions(portfolio_id: str, limit: int = 100) -> dict[str, Any]:
    try:
        sessions = PaperTradingSessionService().list_sessions(portfolio_id, limit=limit)
        return {"ok": True, "sessions": [session.model_dump(mode="json") for session in sessions]}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/live-dashboard")
def get_portfolio_live_dashboard(
    portfolio_id: str,
    trade_date: date | None = None,
    event_limit: int = 500,
) -> dict[str, Any]:
    try:
        return {
            "ok": True,
            "dashboard": PaperTradingLiveDashboardService().get_dashboard(
                portfolio_id,
                trade_date=trade_date,
                event_limit=event_limit,
            ),
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/intraday-snapshots")
def list_portfolio_intraday_snapshots(
    portfolio_id: str,
    trade_date: date | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    try:
        return {
            "ok": True,
            "intraday_snapshots": PaperTradingLiveDashboardService().list_intraday_snapshots(
                portfolio_id,
                trade_date=trade_date,
                limit=limit,
            ),
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/minute-execution")
def get_portfolio_minute_execution(
    portfolio_id: str,
    trade_date: date | None = None,
    symbol: str | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    try:
        return {
            "ok": True,
            "minute_execution": PaperTradingLiveDashboardService().minute_execution(
                portfolio_id,
                trade_date=trade_date,
                symbol=symbol,
                limit=limit,
            ),
        }
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/portfolios/{portfolio_id}/session-capabilities")
def get_portfolio_session_capabilities(portfolio_id: str) -> dict[str, Any]:
    try:
        return {"ok": True, "capabilities": PaperTradingSessionService().session_capabilities(portfolio_id)}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/sessions/{session_id}")
def get_trade_session(session_id: str) -> dict[str, Any]:
    try:
        session = PaperTradingSessionService().get_session(session_id)
        return {"ok": True, "session": session.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/sessions/{session_id}/progress")
def get_trade_session_progress(session_id: str, event_limit: int = 100) -> dict[str, Any]:
    try:
        progress = PaperTradingSessionService().progress(session_id, event_limit=event_limit)
        return {"ok": True, "progress": progress.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/sessions/{session_id}/tick")
def tick_trade_session(session_id: str, req: TickSessionRequest | None = None) -> dict[str, Any]:
    try:
        progress = PaperTradingSessionRunner().tick(
            session_id,
            as_of_time=req.as_of_time if req else None,
            allow_paused=req.allow_paused if req else False,
        )
        return {"ok": True, "progress": progress.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/sessions/{session_id}/pause")
def pause_trade_session(session_id: str) -> dict[str, Any]:
    try:
        session = PaperTradingSessionService().pause(session_id)
        return {"ok": True, "session": session.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/sessions/{session_id}/resume")
def resume_trade_session(session_id: str) -> dict[str, Any]:
    try:
        session = PaperTradingSessionService().resume(session_id)
        return {"ok": True, "session": session.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.post("/sessions/{session_id}/stop")
def stop_trade_session(session_id: str) -> dict[str, Any]:
    try:
        session = PaperTradingSessionService().stop(session_id)
        return {"ok": True, "session": session.model_dump(mode="json")}
    except TradingCoreError as exc:
        _raise_http(exc)


@router.get("/session-scheduler/status")
def get_session_scheduler_status() -> dict[str, Any]:
    from backend.services.paper_trading_v2.scheduler import paper_trading_v2_scheduler

    return {"ok": True, "scheduler": paper_trading_v2_scheduler.status()}


@router.post("/session-scheduler/start")
def start_session_scheduler(req: SchedulerStartRequest) -> dict[str, Any]:
    from backend.services.paper_trading_v2.scheduler import paper_trading_v2_scheduler

    return {"ok": True, "scheduler": paper_trading_v2_scheduler.start(interval_seconds=req.interval_seconds)}


@router.post("/session-scheduler/stop")
def stop_session_scheduler() -> dict[str, Any]:
    from backend.services.paper_trading_v2.scheduler import paper_trading_v2_scheduler

    return {"ok": True, "scheduler": paper_trading_v2_scheduler.shutdown(wait=False)}


@router.post("/session-scheduler/run-once")
def run_session_scheduler_once(req: SchedulerRunOnceRequest) -> dict[str, Any]:
    from backend.services.paper_trading_v2.scheduler import paper_trading_v2_scheduler

    return {"ok": True, "result": paper_trading_v2_scheduler.run_once(limit=req.limit, as_of_time=req.as_of_time)}


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
