"""Read-only operator projection; registration is owned by the Advisory router."""
from datetime import date
import logging
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.services.advisory_workbench.projection import WorkbenchError
from backend.services.advisory_workbench.service import WorkbenchService

router = APIRouter(prefix="/workbench", tags=["advisory-workbench"])
LOG = logging.getLogger(__name__)


def get_service():
    return WorkbenchService()


def invoke(function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except WorkbenchError as exc:
        raise HTTPException(exc.status, detail={"reason_code": exc.code, "message": str(exc)}) from exc
    except Exception as exc:
        LOG.exception("Advisory workbench read failed (%s)", type(exc).__name__)
        raise HTTPException(503, detail={"reason_code": "SOURCE_UNAVAILABLE", "message": "荐股数据读取失败，请稍后刷新；已有结果不能视为最新。"}) from exc


def page_args(limit: int = Query(20, ge=20, le=100), offset: int = Query(0, ge=0, le=1000000),
              snapshot_id: str | None = Query(None, pattern=r"^[0-9a-f]{64}$"), target_date: date | None = None):
    if limit not in {20, 50, 100}:
        raise HTTPException(422, detail={"reason_code": "INVALID_PAGE_SIZE", "message": "每页条数仅支持20、50、100。"})
    return dict(limit=limit, offset=offset, snapshot_id=snapshot_id, target_date=target_date.isoformat() if target_date else None)


def stats_args(source: Literal["ALL", "FORWARD", "REPLAY", "UNKNOWN"] = "ALL",
               start: date | None = None, end: date | None = None,
               binding: str | None = Query(None, max_length=160)):
    return dict(source=source, start=start.isoformat() if start else None, end=end.isoformat() if end else None, binding=binding)


@router.get("/overview")
def overview(status: Literal["ENABLED", "PAUSED", "DRAFT", "ARCHIVED", "ALL"] = "ENABLED",
             filters=Depends(stats_args), service=Depends(get_service)):
    return invoke(service.overview, status=status, **filters)


@router.get("/programs/{program_id}")
def program(program_id: str, target_date: date | None = None, filters=Depends(stats_args), service=Depends(get_service)):
    return invoke(service.program, program_id, target_date=target_date.isoformat() if target_date else None, **filters)


@router.get("/programs/{program_id}/daily")
def daily(program_id: str, group: Literal["IN_SCOPE", "REMOVED", "OBSERVATION", "OUTSIDE_PENDING", "OUTSIDE_EXIT", "UNKNOWN", "ALL"] = "IN_SCOPE",
          change: Literal["NEW", "CONTINUING", "UNKNOWN"] | None = None,
          q: str | None = Query(None, max_length=100), paging=Depends(page_args), service=Depends(get_service)):
    return invoke(service.daily, program_id, group=group, change=change, q=q, **paging)


@router.get("/programs/{program_id}/episodes")
def episodes(program_id: str, status: Literal["ALL", "ACTIVE", "EXITED", "UNSETTLED"] = "ALL",
             q: str | None = Query(None, max_length=100), exit_reason: str | None = Query(None, max_length=80),
             paging=Depends(page_args), filters=Depends(stats_args), service=Depends(get_service)):
    return invoke(service.episodes, program_id, status=status, q=q, exit_reason=exit_reason, **paging, **filters)


@router.get("/programs/{program_id}/episodes/{episode_id}")
def episode(program_id: str, episode_id: str, paging=Depends(page_args), service=Depends(get_service)):
    return invoke(service.episode, program_id, episode_id, **paging)


@router.get("/programs/{program_id}/versions")
def versions(program_id: str, start: date | None = None, end: date | None = None,
             paging=Depends(page_args), service=Depends(get_service)):
    return invoke(service.versions, program_id, start=start.isoformat() if start else None,
                  end=end.isoformat() if end else None, **paging)


@router.get("/programs/{program_id}/decisions")
def decisions(program_id: str, start: date | None = None, end: date | None = None,
              q: str | None = Query(None, max_length=100), paging=Depends(page_args), service=Depends(get_service)):
    return invoke(service.decisions, program_id, start=start.isoformat() if start else None,
                  end=end.isoformat() if end else None, q=q, **paging)


@router.get("/programs/{program_id}/readiness")
def readiness(program_id: str, data_date: date | None = None, service=Depends(get_service)):
    return invoke(service.readiness, program_id, data_date=data_date.isoformat() if data_date else None)
