from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Awaitable, Callable, Mapping, TypeVar

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from backend.deps import DatasetReleasePrincipal, require_dataset_release_operator
from backend.services.dataset_release.api_models import EmptyCommandRequest, MonthlyReleaseRequest
from backend.services.dataset_release.cas_store import CASStoreNotInitialized
from backend.services.dataset_release.control_service import (
    CandidateOnlyRequired,
    DatasetReleaseControlService,
    DatasetReleaseProfileBinding,
    ProfileNotAllowed,
    ReceiptNotReady,
    RecordNotFound,
)
from backend.services.dataset_release.control_store import (
    ControlStoreError,
    ControlStoreNotInitialized,
    ControlStoreSchemaMismatch,
    IdempotencyConflict,
    StateConflict,
)
from backend.services.dataset_release.cursor import CursorBinding, CursorCodec, CursorInvalid
from backend.services.dataset_release.errors import DatasetReleaseError
from backend.services.dataset_release.log_store import LogStoreError
from backend.services.dataset_release.errors import ProfileValidationError
from backend.services.dataset_release.profile import load_dataset_profile


class DatasetReleaseApiRoute(APIRoute):
    """Keep request-validation failures on the versioned error envelope."""

    def get_route_handler(self) -> Callable[[Request], Awaitable[Response]]:
        original = super().get_route_handler()

        async def versioned_handler(request: Request) -> Response:
            try:
                return await original(request)
            except RequestValidationError:
                return JSONResponse(
                    status_code=422,
                    content={
                        "detail": {
                            "error_code": "DATASET_RELEASE_REQUEST_INVALID",
                            "message": "Dataset release request validation failed.",
                            "retryable": False,
                            "context_ref": None,
                        }
                    },
                )

        return versioned_handler


router = APIRouter(
    prefix="/dataset-releases",
    tags=["dataset-releases"],
    route_class=DatasetReleaseApiRoute,
)
PROFILE_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "datasets" / "qe_backtest_monthly_v1.yaml"
DEFAULT_PROFILE = "qe_hmm_full_v1"
MAX_PAGE_SIZE = 200
T = TypeVar("T")
_SUBMISSION_FIELDS = (
    "submission_id",
    "logical_request_key",
    "actor",
    "state",
    "row_version",
    "intent_id",
    "run_id",
    "resolution_attempt_id",
    "terminal_receipt_ref",
    "next_retry_at",
    "deadline_at",
    "created_at",
    "updated_at",
)
_RUN_FIELDS = (
    "run_id",
    "intent_id",
    "run_generation_digest",
    "operation_kind",
    "lineage_root_run_id",
    "resume_ordinal",
    "state",
    "outcome",
    "terminal_receipt_ref",
    "row_version",
    "active_attempt_id",
    "resumes_run_id",
    "candidate_identity",
    "artifact_root",
    "created_at",
    "updated_at",
)
_EVENT_FIELDS = (
    "event_id",
    "submission_id",
    "resolution_attempt_id",
    "run_id",
    "attempt_id",
    "type",
    "payload_ref",
    "created_at",
)
_COMMAND_FIELDS = (
    "command_id",
    "target_type",
    "target_id",
    "submission_id",
    "run_id",
    "type",
    "state",
    "actor",
    "created_at",
    "applied_at",
    "replayed",
)


def _project(value: Mapping[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: value.get(field) for field in fields}


@lru_cache(maxsize=1)
def get_dataset_release_control_service() -> DatasetReleaseControlService:
    """Open registered control roots only; never initialize or migrate them."""

    try:
        profile = load_dataset_profile(PROFILE_CONFIG)
        return DatasetReleaseControlService([DatasetReleaseProfileBinding.from_profile(profile)])
    except (
        CASStoreNotInitialized,
        ControlStoreError,
        OSError,
        ProfileValidationError,
    ) as exc:
        # Dependency resolution happens before the endpoint body, so these
        # failures cannot be mapped by ``_call``.  Keep startup/configuration
        # failures on the same versioned, fail-closed API contract rather than
        # leaking an unstructured 500 response.
        raise HTTPException(
            status_code=503,
            detail=_error_detail(
                exc,
                "DATASET_RELEASE_CONTROL_UNAVAILABLE",
                retryable=True,
            ),
        ) from exc


def _error_detail(error: BaseException, code: str, *, retryable: bool = False) -> dict[str, Any]:
    del error
    public_messages = {
        "DATASET_RELEASE_IDEMPOTENCY_CONFLICT": "Idempotency-Key is bound to another request.",
        "DATASET_RELEASE_STATE_CONFLICT": "Dataset release state changed; refresh before retrying.",
        "DATASET_RELEASE_CONTROL_UNAVAILABLE": "Dataset release control plane is unavailable.",
        "DATASET_RELEASE_REQUEST_INVALID": "Dataset release request validation failed.",
    }
    return {
        "error_code": code,
        "message": public_messages.get(code, "Dataset release request failed."),
        "retryable": retryable,
        "context_ref": None,
    }


def _call(operation: Callable[[], T]) -> T:
    try:
        return operation()
    except (ProfileNotAllowed, CandidateOnlyRequired) as exc:
        raise HTTPException(status_code=403, detail=_error_detail(exc, exc.code)) from exc
    except RecordNotFound as exc:
        raise HTTPException(status_code=404, detail=_error_detail(exc, exc.code)) from exc
    except IdempotencyConflict as exc:
        raise HTTPException(
            status_code=409,
            detail=_error_detail(exc, exc.code),
        ) from exc
    except StateConflict as exc:
        raise HTTPException(
            status_code=409,
            detail=_error_detail(exc, exc.code),
        ) from exc
    except ReceiptNotReady as exc:
        raise HTTPException(status_code=409, detail=_error_detail(exc, exc.code)) from exc
    except CursorInvalid as exc:
        raise HTTPException(status_code=400, detail=_error_detail(exc, exc.code)) from exc
    except (ControlStoreNotInitialized, ControlStoreSchemaMismatch, CASStoreNotInitialized) as exc:
        raise HTTPException(
            status_code=503,
            detail=_error_detail(exc, "DATASET_RELEASE_CONTROL_UNAVAILABLE", retryable=True),
        ) from exc
    except DatasetReleaseError as exc:
        raise HTTPException(status_code=400, detail=_error_detail(exc, exc.code, retryable=exc.retryable)) from exc
    except LogStoreError as exc:
        raise HTTPException(
            status_code=400,
            detail=_error_detail(exc, "DATASET_RELEASE_LOG_CURSOR_INVALID"),
        ) from exc
    except ControlStoreError as exc:
        raise HTTPException(
            status_code=500,
            detail=_error_detail(exc, "DATASET_RELEASE_CONTROL_ERROR"),
        ) from exc


def _idempotency(value: Annotated[str, Header(alias="Idempotency-Key")]) -> str:
    normalized = value.strip()
    if not normalized or len(normalized) > 256:
        raise HTTPException(
            status_code=422,
            detail=_error_detail(
                ValueError("Idempotency-Key must contain 1..256 characters"),
                "DATASET_RELEASE_IDEMPOTENCY_KEY_INVALID",
            ),
        )
    return normalized


def _cursor_codec(principal: DatasetReleasePrincipal) -> CursorCodec:
    return CursorCodec(principal.cursor_signing_key)


@router.post("/preview")
def preview_release(
    request: MonthlyReleaseRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
) -> dict[str, Any]:
    del principal
    data = _call(
        lambda: service.preview_monthly(
            profile_id=request.profile,
            cutoff_policy=request.cutoff_policy,
            scope=request.scope,
            candidate_only=request.candidate_only,
        )
    )
    return {"schema_version": "dataset_release_preview_response_v1", "data": data}


@router.post("/runs", status_code=202)
def submit_release(
    request: MonthlyReleaseRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    idempotency_key: Annotated[str, Depends(_idempotency)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
) -> dict[str, Any]:
    return _call(
        lambda: service.submit_monthly(
            profile_id=request.profile,
            cutoff_policy=request.cutoff_policy,
            scope=request.scope,
            candidate_only=request.candidate_only,
            principal=principal.principal_id,
            idempotency_key=idempotency_key,
            route="POST:/api/v1/dataset-releases/runs",
            preview_token=request.preview_token,
        )
    )


@router.get("/submissions/{submission_id}")
def get_submission(
    submission_id: str,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
    profile: str = Query(DEFAULT_PROFILE, min_length=1, max_length=64),
) -> dict[str, Any]:
    del principal
    return {
        "schema_version": "dataset_release_submission_status_v1",
        "data": _project(
            _call(lambda: service.get_submission(profile, submission_id)),
            _SUBMISSION_FIELDS,
        ),
    }


def _events_page(
    *,
    service: DatasetReleaseControlService,
    principal: DatasetReleasePrincipal,
    profile: str,
    submission_id: str | None = None,
    run_id: str | None = None,
    cursor: str | None,
    limit: int,
) -> dict[str, Any]:
    endpoint = "submission-events" if submission_id is not None else "run-events"
    target = submission_id if submission_id is not None else run_id
    binding = CursorBinding(
        endpoint=endpoint,
        principal=principal.principal_id,
        filters={"profile": profile, "target": target},
        order="event_id_asc",
    )
    after = 0
    if cursor:
        position = _call(lambda: _cursor_codec(principal).decode(cursor, binding=binding))
        try:
            after = int(position["event_id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail=_error_detail(exc, "DATASET_RELEASE_CURSOR_INVALID"),
            ) from exc
    rows = _call(
        lambda: service.list_events(
            profile,
            submission_id=submission_id,
            run_id=run_id,
            after_event_id=after,
            limit=limit + 1,
        )
    )
    has_more = len(rows) > limit
    items = [_project(item, _EVENT_FIELDS) for item in rows[:limit]]
    next_cursor = None
    if has_more and items:
        next_cursor = _cursor_codec(principal).encode(
            binding=binding, position={"event_id": int(items[-1]["event_id"])}
        )
    return {
        "schema_version": "dataset_release_event_page_v1",
        "items": items,
        "next_cursor": next_cursor,
        "has_more": has_more,
    }


@router.get("/submissions/{submission_id}/events")
def get_submission_events(
    submission_id: str,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
    profile: str = Query(DEFAULT_PROFILE, min_length=1, max_length=64),
    cursor: str | None = Query(None, max_length=4096),
    limit: int = Query(50, ge=1, le=MAX_PAGE_SIZE),
) -> dict[str, Any]:
    return _events_page(
        service=service,
        principal=principal,
        profile=profile,
        submission_id=submission_id,
        cursor=cursor,
        limit=limit,
    )


@router.get("/submissions/{submission_id}/receipt")
def get_submission_receipt(
    submission_id: str,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
    profile: str = Query(DEFAULT_PROFILE, min_length=1, max_length=64),
) -> dict[str, Any]:
    del principal
    return {
        "schema_version": "dataset_release_receipt_response_v1",
        "data": _call(lambda: service.get_submission_receipt(profile, submission_id)),
    }


def _enqueue_command(
    *,
    service: DatasetReleaseControlService,
    principal: DatasetReleasePrincipal,
    profile: str,
    target_type: str,
    target_id: str,
    command_type: str,
    idempotency_key: str,
    route: str,
) -> dict[str, Any]:
    command = _call(
        lambda: service.enqueue_command(
            profile_id=profile,
            target_type=target_type,
            target_id=target_id,
            command_type=command_type,
            principal=principal.principal_id,
            route=route,
            idempotency_key=idempotency_key,
        )
    )
    return {
        "schema_version": "dataset_release_command_response_v1",
        "data": _project(command, _COMMAND_FIELDS),
    }


@router.post("/submissions/{submission_id}/cancel-request", status_code=202)
def cancel_submission(
    submission_id: str,
    request: EmptyCommandRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    idempotency_key: Annotated[str, Depends(_idempotency)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
    profile: str = Query(DEFAULT_PROFILE, min_length=1, max_length=64),
) -> dict[str, Any]:
    del request
    return _enqueue_command(
        service=service,
        principal=principal,
        profile=profile,
        target_type="submission",
        target_id=submission_id,
        command_type="CANCEL_REQUESTED",
        idempotency_key=idempotency_key,
        route="POST:/api/v1/dataset-releases/submissions/cancel-request",
    )


@router.get("/runs")
def list_runs(
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
    profile: str = Query(DEFAULT_PROFILE, min_length=1, max_length=64),
    state: list[str] = Query(default=[]),
    cursor: str | None = Query(None, max_length=4096),
    limit: int = Query(50, ge=1, le=MAX_PAGE_SIZE),
) -> dict[str, Any]:
    normalized_states = tuple(sorted(dict.fromkeys(state)))
    binding = CursorBinding(
        endpoint="runs",
        principal=principal.principal_id,
        filters={"profile": profile, "states": normalized_states},
        order="created_at_desc_run_id_desc",
    )
    before_created_at = None
    before_run_id = None
    if cursor:
        position = _call(lambda: _cursor_codec(principal).decode(cursor, binding=binding))
        before_created_at = str(position.get("created_at") or "")
        before_run_id = str(position.get("run_id") or "")
        if not before_created_at or not before_run_id:
            raise HTTPException(
                status_code=400,
                detail=_error_detail(
                    ValueError("run cursor position is incomplete"),
                    "DATASET_RELEASE_CURSOR_INVALID",
                ),
            )
    rows = _call(
        lambda: service.list_runs(
            profile,
            states=normalized_states,
            before_created_at=before_created_at,
            before_run_id=before_run_id,
            limit=limit + 1,
        )
    )
    has_more = len(rows) > limit
    items = [_project(item, _RUN_FIELDS) for item in rows[:limit]]
    next_cursor = None
    if has_more and items:
        next_cursor = _cursor_codec(principal).encode(
            binding=binding,
            position={"created_at": items[-1]["created_at"], "run_id": items[-1]["run_id"]},
        )
    return {
        "schema_version": "dataset_release_run_page_v1",
        "items": items,
        "next_cursor": next_cursor,
        "has_more": has_more,
    }


@router.get("/runs/{run_id}")
def get_run(
    run_id: str,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
    profile: str = Query(DEFAULT_PROFILE, min_length=1, max_length=64),
) -> dict[str, Any]:
    del principal
    return {
        "schema_version": "dataset_release_run_status_v1",
        "data": _project(
            _call(lambda: service.get_run(profile, run_id)),
            _RUN_FIELDS,
        ),
    }


@router.get("/runs/{run_id}/events")
def get_run_events(
    run_id: str,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
    profile: str = Query(DEFAULT_PROFILE, min_length=1, max_length=64),
    cursor: str | None = Query(None, max_length=4096),
    limit: int = Query(50, ge=1, le=MAX_PAGE_SIZE),
) -> dict[str, Any]:
    return _events_page(
        service=service,
        principal=principal,
        profile=profile,
        run_id=run_id,
        cursor=cursor,
        limit=limit,
    )


@router.get("/runs/{run_id}/receipt")
def get_run_receipt(
    run_id: str,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
    profile: str = Query(DEFAULT_PROFILE, min_length=1, max_length=64),
) -> dict[str, Any]:
    del principal
    return {
        "schema_version": "dataset_release_receipt_response_v1",
        "data": _call(lambda: service.get_run_receipt(profile, run_id)),
    }


@router.get("/runs/{run_id}/log")
def get_run_log(
    run_id: str,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
    profile: str = Query(DEFAULT_PROFILE, min_length=1, max_length=64),
    stream: str = Query("stdout", pattern="^(stdout|stderr|worker)$"),
    cursor: str | None = Query(None, max_length=4096),
    max_bytes: int = Query(256 * 1024, ge=1, le=1024**2),
    max_lines: int = Query(1000, ge=1, le=1000),
) -> dict[str, Any]:
    binding = CursorBinding(
        endpoint="run-log",
        principal=principal.principal_id,
        filters={"profile": profile, "run_id": run_id, "stream": stream},
        order="generation_asc_byte_offset_asc",
        generation=f"{run_id}:{stream}",
    )
    log_id = 0
    generation = 1
    byte_offset = 0
    if cursor:
        position = _call(lambda: _cursor_codec(principal).decode(cursor, binding=binding))
        try:
            log_id = int(position["log_id"])
            generation = int(position["generation"])
            byte_offset = int(position["byte_offset"])
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail=_error_detail(exc, "DATASET_RELEASE_CURSOR_INVALID"),
            ) from exc
    page = _call(
        lambda: service.read_run_log(
            profile,
            run_id,
            stream=stream,
            log_id=log_id,
            generation=generation,
            byte_offset=byte_offset,
            max_bytes=max_bytes,
            max_lines=max_lines,
        )
    )
    next_cursor = None
    if page["has_more"]:
        next_cursor = _cursor_codec(principal).encode(
            binding=binding,
            position={
                "log_id": page["next_log_id"],
                "generation": page["next_generation"],
                "byte_offset": page["next_byte_offset"],
            },
        )
    return {
        "schema_version": "dataset_release_log_page_v1",
        "data": page,
        "next_cursor": next_cursor,
        "has_more": page["has_more"],
    }


@router.post("/runs/{run_id}/resume", status_code=202)
def resume_run(
    run_id: str,
    request: EmptyCommandRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    idempotency_key: Annotated[str, Depends(_idempotency)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
    profile: str = Query(DEFAULT_PROFILE, min_length=1, max_length=64),
) -> dict[str, Any]:
    del request
    return _enqueue_command(
        service=service,
        principal=principal,
        profile=profile,
        target_type="run",
        target_id=run_id,
        command_type="RESUME_REQUESTED",
        idempotency_key=idempotency_key,
        route="POST:/api/v1/dataset-releases/runs/resume",
    )


@router.post("/runs/{run_id}/cancel-request", status_code=202)
def cancel_run(
    run_id: str,
    request: EmptyCommandRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    idempotency_key: Annotated[str, Depends(_idempotency)],
    service: Annotated[DatasetReleaseControlService, Depends(get_dataset_release_control_service)],
    profile: str = Query(DEFAULT_PROFILE, min_length=1, max_length=64),
) -> dict[str, Any]:
    del request
    return _enqueue_command(
        service=service,
        principal=principal,
        profile=profile,
        target_type="run",
        target_id=run_id,
        command_type="CANCEL_REQUESTED",
        idempotency_key=idempotency_key,
        route="POST:/api/v1/dataset-releases/runs/cancel-request",
    )
