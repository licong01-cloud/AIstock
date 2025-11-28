from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..services.quant_analyst_service import (
    QuantAnalystRequest,
    QuantAnalystResponse,
    run_quant_analyst,
)


router = APIRouter()


@router.post("/api/quant/signals", response_model=QuantAnalystResponse)
async def quant_signals(body: QuantAnalystRequest) -> QuantAnalystResponse:
    try:
        return run_quant_analyst(body)
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
