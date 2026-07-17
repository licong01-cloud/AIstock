"""Research-only API for the HMM evolution laboratory."""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import lru_cache
from typing import Annotated, Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.hmm_evolution.asset_content_policy import (
    require_text_asset,
    sanitize_asset_text,
)
from backend.services.hmm_evolution.errors import (
    HMMEvolutionError,
    InvalidSpecError,
    QEAssetTooLargeError,
    sanitized_exception_chain,
)
from backend.services.hmm_evolution.models import (
    CandidateLifecycle,
    CandidatePreview,
    CandidateSourceType,
    EvaluationSpec,
)
from backend.services.hmm_evolution.runtime import (
    HMMEvolutionRuntime,
    build_runtime,
    require_api_runtime,
)
from backend.services.hmm_evolution.scorer import (
    RECOMMENDATION_VERSION,
    RECOMMENDATION_WEIGHTS,
)

logger = logging.getLogger(__name__)


class HMMEvolutionRoute(APIRoute):
    """Keep HMM validation/dependency failures inside the stable error contract."""

    def get_route_handler(self):
        original = super().get_route_handler()

        async def handler(request: Request) -> Response:
            try:
                return await original(request)
            except RequestValidationError as exc:
                trace_id = request_trace_id(request)
                error = InvalidSpecError(
                    "HMM evolution request validation failed",
                    context={"errors": exc.errors()},
                )
                return JSONResponse(
                    status_code=error.http_status,
                    content=error.as_dict(trace_id=trace_id),
                )
            except HMMEvolutionError as exc:
                return JSONResponse(
                    status_code=exc.http_status,
                    content=exc.as_dict(trace_id=request_trace_id(request)),
                )
            except Exception as exc:
                trace_id = request_trace_id(request)
                logger.exception("unexpected HMM evolution route failure trace_id=%s", trace_id)
                error = HMMEvolutionError(
                    "HMM evolution request failed unexpectedly",
                    context={"exception_chain": sanitized_exception_chain(exc)},
                )
                return JSONResponse(
                    status_code=error.http_status,
                    content=error.as_dict(trace_id=trace_id),
                )

        return handler


router = APIRouter(
    prefix="/hmm-evolution",
    tags=["HMM Evolution Research"],
    route_class=HMMEvolutionRoute,
)

TRACE_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
TERMINAL_BATCH_STATUSES = frozenset({"completed", "partial_failed", "failed", "cancelled", "timed_out"})


@dataclass(frozen=True)
class ApiSuccess:
    data: Any
    status_code: int


class CandidateSourceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_type: CandidateSourceType
    snapshot_id: str | None = None
    artifact_name: str | None = None
    root_alias: str | None = None
    relative_path: str | None = None
    task_id: str | None = None
    loop_name: str | None = None

    @model_validator(mode="after")
    def _source_shape(self) -> "CandidateSourceRequest":
        values = {
            "snapshot_id": self.snapshot_id,
            "artifact_name": self.artifact_name,
            "root_alias": self.root_alias,
            "relative_path": self.relative_path,
            "task_id": self.task_id,
            "loop_name": self.loop_name,
        }
        if self.source_type is CandidateSourceType.EXISTING_SNAPSHOT:
            required = {"snapshot_id", "artifact_name"}
        elif self.source_type is CandidateSourceType.CONFIGURED_LOCAL:
            required = {"root_alias", "relative_path"}
        else:
            required = {"task_id", "loop_name", "relative_path"}
        missing = sorted(key for key in required if not str(values.get(key) or "").strip())
        unexpected = sorted(key for key, value in values.items() if key not in required and value is not None)
        if missing or unexpected:
            raise ValueError(
                f"candidate source fields do not match source_type: missing={missing}, unexpected={unexpected}"
            )
        return self


class CandidateRegisterRequest(CandidateSourceRequest):
    display_name: str = Field(min_length=1, max_length=160)
    description: str | None = Field(default=None, max_length=2000)
    created_by: str = Field(default="hmm_research_ui", min_length=1, max_length=160)


class CandidateRetireRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_row_version: int = Field(ge=1)


class BatchCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_ids: list[str] = Field(min_length=1, max_length=50)
    evaluation_spec: EvaluationSpec
    created_by: str = Field(default="hmm_research_ui", min_length=1, max_length=160)

    @model_validator(mode="after")
    def _unique_candidates(self) -> "BatchCreateRequest":
        normalized = [str(item or "").strip() for item in self.candidate_ids]
        if any(not item for item in normalized):
            raise ValueError("candidate_ids must contain non-empty IDs")
        if len(normalized) != len(set(normalized)):
            raise ValueError("candidate_ids must be unique")
        self.candidate_ids = normalized
        return self


class EvaluateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_id: str = Field(min_length=1)
    evaluation_spec: EvaluationSpec
    created_by: str = Field(default="hmm_research_ui", min_length=1, max_length=160)


class ActorRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requested_by: str = Field(default="hmm_research_ui", min_length=1, max_length=160)


class RetryBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    created_by: str = Field(default="hmm_research_ui", min_length=1, max_length=160)


@lru_cache(maxsize=1)
def _runtime() -> HMMEvolutionRuntime:
    return build_runtime()


def get_runtime() -> HMMEvolutionRuntime:
    require_api_runtime()
    return _runtime()


def request_trace_id(request: Request) -> str:
    supplied = str(request.headers.get("X-Request-ID") or "").strip()
    return supplied if TRACE_ID_RE.fullmatch(supplied) else uuid4().hex


RuntimeDependency = Annotated[HMMEvolutionRuntime, Depends(get_runtime)]
TraceDependency = Annotated[str, Depends(request_trace_id)]
IdempotencyHeader = Annotated[str | None, Header(alias="Idempotency-Key")]


async def _call(
    operation: Callable[[], Any | Awaitable[Any]],
    *,
    trace_id: str,
    success_status: int = 200,
) -> JSONResponse:
    try:
        value = operation()
        if inspect.isawaitable(value):
            value = await value
        if isinstance(value, ApiSuccess):
            success_status = value.status_code
            value = value.data
        return JSONResponse(
            status_code=success_status,
            content=jsonable_encoder({"status": "ok", "data": value, "trace_id": trace_id}),
        )
    except HMMEvolutionError as exc:
        return JSONResponse(status_code=exc.http_status, content=exc.as_dict(trace_id=trace_id))
    except Exception as exc:  # fail loud to logs, bounded response to clients.
        logger.exception("unexpected HMM evolution API failure trace_id=%s", trace_id)
        error = HMMEvolutionError(
            "HMM evolution request failed unexpectedly",
            context={"exception_chain": sanitized_exception_chain(exc)},
        )
        return JSONResponse(status_code=error.http_status, content=error.as_dict(trace_id=trace_id))


async def _preview_candidate(
    runtime: HMMEvolutionRuntime,
    request: CandidateSourceRequest,
) -> CandidatePreview:
    if request.source_type is CandidateSourceType.EXISTING_SNAPSHOT:
        return await asyncio.to_thread(
            runtime.service.preview_existing_snapshot,
            snapshot_id=str(request.snapshot_id),
            artifact_name=str(request.artifact_name),
        )
    if request.source_type is CandidateSourceType.CONFIGURED_LOCAL:
        return await asyncio.to_thread(
            runtime.service.preview_configured_local,
            root_alias=str(request.root_alias),
            relative_path=str(request.relative_path),
        )
    return await runtime.service.preview_qe_experiment(
        task_id=str(request.task_id),
        loop_name=str(request.loop_name),
        relative_path=str(request.relative_path),
    )


def _recommendation_spec() -> dict[str, Any]:
    return {
        "schema_version": "hmm_recommendation_spec_v1",
        "recommendation_version": RECOMMENDATION_VERSION,
        "weights": dict(RECOMMENDATION_WEIGHTS),
        "thresholds": None,
        "qe_final_review_required": True,
    }


def _evaluation_summary(row: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "eval_id",
        "candidate_id",
        "candidate_display_name",
        "candidate_source_type",
        "base_loop_ref",
        "status",
        "run_generation",
        "as_of_date",
        "window_start",
        "window_end",
        "label_horizon_days",
        "topk",
        "trading_days_count",
        "changed_day_count",
        "primary_coverage_ratio",
        "net_label_return",
        "net_db_10d",
        "positive_net_label_day_ratio",
        "evidence_quality",
        "reason_code",
        "queued_at",
        "started_at",
        "completed_at",
        "updated_at",
    )
    return {key: row.get(key) for key in keys}


@router.post("/candidates/preview")
async def preview_candidate(
    request: CandidateSourceRequest,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
) -> JSONResponse:
    return await _call(
        lambda: _preview_candidate(runtime, request),
        trace_id=trace_id,
    )


@router.post("/candidates")
async def register_candidate(
    request: CandidateRegisterRequest,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
) -> JSONResponse:
    async def operation() -> ApiSuccess:
        preview = await _preview_candidate(runtime, request)
        candidate, created = await asyncio.to_thread(
            runtime.service.register_candidate,
            preview,
            display_name=request.display_name.strip(),
            description=request.description,
            created_by=request.created_by.strip(),
        )
        return ApiSuccess(
            data={"candidate": candidate, "created": created},
            status_code=201 if created else 200,
        )

    return await _call(operation, trace_id=trace_id)


@router.get("/candidates")
async def list_candidates(
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
    lifecycle_status: CandidateLifecycle | None = None,
    source_type: CandidateSourceType | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> JSONResponse:
    return await _call(
        lambda: asyncio.to_thread(
            runtime.service.list_candidates,
            lifecycle_status=lifecycle_status,
            source_type=source_type.value if source_type else None,
            limit=limit,
            offset=offset,
        ),
        trace_id=trace_id,
    )


@router.get("/candidates/{candidate_id}")
async def get_candidate(
    candidate_id: str,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
) -> JSONResponse:
    async def operation() -> dict[str, Any]:
        candidate, evaluations = await asyncio.gather(
            asyncio.to_thread(runtime.service.get_candidate, candidate_id),
            asyncio.to_thread(
                runtime.service.list_evaluations,
                candidate_id=candidate_id,
                limit=20,
                offset=0,
            ),
        )
        return {
            "candidate": candidate,
            "recent_evaluations": [_evaluation_summary(row) for row in evaluations],
        }

    return await _call(operation, trace_id=trace_id)


@router.post("/candidates/{candidate_id}/retire")
async def retire_candidate(
    candidate_id: str,
    request: CandidateRetireRequest,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
) -> JSONResponse:
    return await _call(
        lambda: asyncio.to_thread(
            runtime.service.retire_candidate,
            candidate_id,
            expected_row_version=request.expected_row_version,
        ),
        trace_id=trace_id,
    )


@router.get("/qe-assets/{task_id}/{loop_name}")
async def list_qe_assets(
    task_id: str,
    loop_name: str,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
    require_complete: bool = False,
) -> JSONResponse:
    return await _call(
        lambda: runtime.qe_asset_reader.list_assets(
            task_id,
            loop_name,
            require_complete=require_complete,
        ),
        trace_id=trace_id,
    )


@router.get("/qe-assets/{task_id}/{loop_name}/stat")
async def stat_qe_asset(
    task_id: str,
    loop_name: str,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
    relative_path: str = Query(alias="path"),
) -> JSONResponse:
    return await _call(
        lambda: runtime.qe_asset_reader.stat_asset(task_id, loop_name, relative_path),
        trace_id=trace_id,
    )


@router.get("/qe-assets/{task_id}/{loop_name}/content")
async def read_qe_asset(
    task_id: str,
    loop_name: str,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
    relative_path: str = Query(alias="path"),
    range_header: Annotated[str | None, Header(alias="Range")] = None,
) -> Response:
    try:
        entry = await runtime.qe_asset_reader.stat_asset(task_id, loop_name, relative_path)
        content_type = str(entry.content_type or "application/octet-stream")
        require_text_asset(relative_path=entry.relative_path, content_type=content_type)
        api_limit = _positive_content_limit()
        if entry.size_bytes > api_limit:
            if range_header is None:
                raise QEAssetTooLargeError(
                    "QE asset exceeds the bounded API inspection limit",
                    context={
                        "relative_path": entry.relative_path,
                        "size_bytes": entry.size_bytes,
                        "max_bytes": api_limit,
                        "retry_condition": "retry with one bounded HTTP Range request",
                    },
                )
            start, end = _parse_range(range_header, size_bytes=entry.size_bytes, max_bytes=api_limit)
            client, upstream = await runtime.qe_read_client.open_workspace_file_range(
                task_id,
                loop_name,
                entry.relative_path,
                start=start,
                end=end,
            )

            try:
                ranged_data = await upstream.aread()
            finally:
                await upstream.aclose()
                await client.aclose()
            sanitized = sanitize_asset_text(
                ranged_data,
                relative_path=entry.relative_path,
                content_type=content_type,
                partial=True,
            )
            return JSONResponse(
                content=jsonable_encoder(
                    {
                        "status": "ok",
                        "data": {
                            "content_kind": "bounded_text_range",
                            "text": sanitized.text,
                            "schema_kind": sanitized.schema_kind,
                            "redaction_count": sanitized.redaction_count,
                            "range": {
                                "start": start,
                                "end": end,
                                "total_size_bytes": entry.size_bytes,
                            },
                        },
                        "trace_id": trace_id,
                    }
                ),
                status_code=206,
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Range": upstream.headers.get(
                        "Content-Range",
                        f"bytes {start}-{end}/{entry.size_bytes}",
                    ),
                    "X-HMM-Asset-SHA256": str(entry.sha256 or "unavailable-for-partial-read"),
                    "X-HMM-Asset-Trust-Level": entry.trust_level.value,
                    "X-HMM-Asset-Access-Mode": entry.access_mode.value,
                    "X-Request-ID": trace_id,
                },
            )
        content = await runtime.qe_asset_reader.read_asset(
            task_id,
            loop_name,
            relative_path,
            declared_entry=entry,
        )
        headers = {
            "X-HMM-Asset-SHA256": content.receipt.sha256,
            "X-HMM-Asset-Trust-Level": content.receipt.trust_level.value,
            "X-HMM-Asset-Access-Mode": content.receipt.access_mode.value,
            "X-Request-ID": trace_id,
        }
        sanitized = sanitize_asset_text(
            content.data,
            relative_path=entry.relative_path,
            content_type=content_type,
        )
        return JSONResponse(
            content=jsonable_encoder(
                {
                    "status": "ok",
                    "data": {
                        "content_kind": "bounded_text",
                        "text": sanitized.text,
                        "schema_kind": sanitized.schema_kind,
                        "redaction_count": sanitized.redaction_count,
                        "receipt": content.receipt,
                    },
                    "trace_id": trace_id,
                }
            ),
            headers=headers,
        )
    except HMMEvolutionError as exc:
        return JSONResponse(status_code=exc.http_status, content=exc.as_dict(trace_id=trace_id))
    except Exception as exc:
        logger.exception("unexpected QE asset read failure trace_id=%s", trace_id)
        error = HMMEvolutionError(
            "QE asset request failed unexpectedly",
            context={"exception_chain": sanitized_exception_chain(exc)},
        )
        return JSONResponse(status_code=error.http_status, content=error.as_dict(trace_id=trace_id))


@router.post("/evaluate")
async def create_evaluation(
    request: EvaluateRequest,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
    idempotency_key: IdempotencyHeader = None,
) -> JSONResponse:
    return await _call(
        lambda: _create_batch(runtime, [request.candidate_id], request, idempotency_key),
        trace_id=trace_id,
        success_status=202,
    )


@router.post("/batch")
async def create_batch(
    request: BatchCreateRequest,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
    idempotency_key: IdempotencyHeader = None,
) -> JSONResponse:
    return await _call(
        lambda: _create_batch(runtime, request.candidate_ids, request, idempotency_key),
        trace_id=trace_id,
        success_status=202,
    )


async def _create_batch(
    runtime: HMMEvolutionRuntime,
    candidate_ids: list[str],
    request: EvaluateRequest | BatchCreateRequest,
    idempotency_key: str | None,
) -> ApiSuccess:
    batch, created = await runtime.service.prepare_and_create_batch(
        candidate_ids=candidate_ids,
        evaluation_spec=request.evaluation_spec,
        recommendation_spec=_recommendation_spec(),
        recommendation_version=RECOMMENDATION_VERSION,
        created_by=request.created_by.strip(),
        idempotency_key=str(idempotency_key or "").strip() or None,
    )
    return ApiSuccess(
        data={"batch": batch, "created": created},
        status_code=202 if created else 200,
    )


@router.get("/evaluations/{eval_id}")
async def get_evaluation(
    eval_id: str,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
) -> JSONResponse:
    return await _call(
        lambda: asyncio.to_thread(runtime.service.get_evaluation, eval_id),
        trace_id=trace_id,
    )


@router.get("/batches")
async def list_batches(
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> JSONResponse:
    return await _call(
        lambda: asyncio.to_thread(runtime.service.list_batches, limit=limit, offset=offset),
        trace_id=trace_id,
    )


@router.get("/batches/{batch_id}")
async def get_batch(
    batch_id: str,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
) -> JSONResponse:
    return await _call(
        lambda: asyncio.to_thread(runtime.service.get_batch, batch_id),
        trace_id=trace_id,
    )


@router.post("/batches/{batch_id}/cancel")
async def cancel_batch(
    batch_id: str,
    request: ActorRequest,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
) -> JSONResponse:
    return await _call(
        lambda: asyncio.to_thread(
            runtime.service.request_batch_cancel,
            batch_id=batch_id,
            requested_by=request.requested_by.strip(),
        ),
        trace_id=trace_id,
    )


@router.post("/batches/{batch_id}/retry-failed")
async def retry_failed_batch(
    batch_id: str,
    request: RetryBatchRequest,
    runtime: RuntimeDependency,
    trace_id: TraceDependency,
    idempotency_key: IdempotencyHeader = None,
) -> JSONResponse:
    return await _call(
        lambda: asyncio.to_thread(
            runtime.service.retry_failed_batch,
            batch_id=batch_id,
            created_by=request.created_by.strip(),
            idempotency_key=str(idempotency_key or "").strip() or None,
        ),
        trace_id=trace_id,
        success_status=202,
    )


def _positive_content_limit() -> int:
    raw = os.getenv("HMM_EVOLUTION_QE_API_CONTENT_MAX_BYTES", str(1024 * 1024))
    try:
        value = int(raw)
    except ValueError as exc:
        raise InvalidSpecError("HMM_EVOLUTION_QE_API_CONTENT_MAX_BYTES must be an integer") from exc
    if value < 1:
        raise InvalidSpecError("HMM_EVOLUTION_QE_API_CONTENT_MAX_BYTES must be positive")
    return value


def _parse_range(value: str, *, size_bytes: int, max_bytes: int) -> tuple[int, int]:
    match = re.fullmatch(r"bytes=(\d+)-(\d*)", str(value or "").strip())
    if match is None:
        raise InvalidSpecError("QE asset Range must use one explicit bytes=start-end interval")
    start = int(match.group(1))
    requested_end = int(match.group(2)) if match.group(2) else start + max_bytes - 1
    if start >= size_bytes or requested_end < start:
        raise InvalidSpecError(
            "QE asset Range falls outside the asset",
            context={"start": start, "end": requested_end, "size_bytes": size_bytes},
        )
    end = min(requested_end, size_bytes - 1)
    if end - start + 1 > max_bytes:
        raise QEAssetTooLargeError(
            "QE asset Range exceeds the bounded API inspection limit",
            context={
                "requested_bytes": end - start + 1,
                "max_bytes": max_bytes,
                "retry_condition": "request a smaller byte range",
            },
        )
    return start, end


__all__ = [
    "TERMINAL_BATCH_STATUSES",
    "get_runtime",
    "router",
]
