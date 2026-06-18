"""Read-only multi-alpha diagnostic APIs."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from backend.services.multi_alpha import MultiAlphaOrthogonalityError, MultiAlphaOrthogonalityService


router = APIRouter(prefix="/multi-alpha", tags=["multi-alpha"])


def _parse_run_ids(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        result.extend(part.strip() for part in str(value or "").split(",") if part.strip())
    return result


@router.get("/orthogonality", summary="Compute read-only orthogonality diagnostics for finalized alpha legs")
def get_multi_alpha_orthogonality(
    run_ids: Annotated[list[str], Query(description="Run ids, either repeated or comma-separated")],
    k: Annotated[int, Query(ge=1, le=500)] = 25,
) -> dict:
    try:
        data = MultiAlphaOrthogonalityService().compute(run_ids=_parse_run_ids(run_ids), k=k)
    except MultiAlphaOrthogonalityError as exc:
        status_code = 404 if "missing" in str(exc).lower() or "not found" in str(exc).lower() else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    return {"status": "success", "data": data}
