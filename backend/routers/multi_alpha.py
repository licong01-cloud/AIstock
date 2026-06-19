"""Read-only multi-alpha diagnostic APIs."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.multi_alpha import MultiAlphaCombiner, MultiAlphaCombinerError, MultiAlphaOrthogonalityError, MultiAlphaOrthogonalityService


router = APIRouter(prefix="/multi-alpha", tags=["multi-alpha"])


class CombineLegPayload(BaseModel):
    id: str
    pred_frame: list[dict[str, Any]]
    ic: float | None = None
    topk_return: float | None = None
    realized_returns: list[float] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CombinePreviewRequest(BaseModel):
    legs: list[CombineLegPayload]
    weighting_scheme: str = "equal"
    normalize_method: str = "zscore"
    walk_forward: dict[str, Any] | None = None
    head: int = Field(default=20, ge=0, le=1000)


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


@router.post("/combine/preview", summary="Preview pure in-memory multi-alpha prediction score combination")
def preview_multi_alpha_combination(request: CombinePreviewRequest) -> dict:
    try:
        legs = [leg.model_dump() for leg in request.legs]
        data = MultiAlphaCombiner().combine(
            legs=legs,
            weighting_scheme=request.weighting_scheme,
            normalize_method=request.normalize_method,
            walk_forward=request.walk_forward,
        ).to_payload(head=request.head)
    except MultiAlphaCombinerError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "success", "data": data}
