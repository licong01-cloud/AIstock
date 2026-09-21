"""Thin HTTP facade over the durable unified monthly release service."""

from __future__ import annotations

from functools import lru_cache
import hashlib
from pathlib import Path
from typing import Annotated, Any, Awaitable, Callable, Mapping, TypeVar

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from backend.deps import DatasetReleasePrincipal, require_dataset_release_operator
from backend.services.dataset_release.api_models import (
    EmptyCommandRequest,
    UnifiedMonthlyActionRequest,
    UnifiedMonthlyReleaseRequest,
)
from backend.services.dataset_release.monthly_runtime import (
    MonthlyRuntimeConfigurationError,
    MonthlyRuntimeSettings,
)
from backend.services.dataset_release.monthly_unified import (
    ActionAuthorizationStore,
    MonthlyReleaseAuthorizationError,
    MonthlyReleaseConflict,
    MonthlyReleaseError,
    MonthlyReleaseNotReady,
    MonthlyReleaseRequest,
    MonthlyReleaseRequestInvalid,
    MonthlyReleaseService,
)
from backend.services.quantevolver.qe_active_dataset_profile import (
    load_qe_profile,
    validate_controller_snapshot,
)


class MonthlyDatasetReleaseApiRoute(APIRoute):
    """Keep validation failures on a compact versioned public envelope."""

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
                            "error_code": "MONTHLY_RELEASE_REQUEST_INVALID",
                            "message": "Monthly release request validation failed.",
                            "retryable": False,
                            "context": {},
                        }
                    },
                )

        return versioned_handler


router = APIRouter(
    prefix="/qlib/monthly-releases",
    tags=["monthly-dataset-releases"],
    route_class=MonthlyDatasetReleaseApiRoute,
)
T = TypeVar("T")
MAX_RECEIPT_FILES = 64
MAX_RECEIPT_BYTES = 32 * 1024 * 1024


@lru_cache(maxsize=1)
def get_monthly_release_settings() -> MonthlyRuntimeSettings:
    try:
        return MonthlyRuntimeSettings.from_env()
    except MonthlyRuntimeConfigurationError as exc:
        raise HTTPException(status_code=503, detail=_error(exc)) from exc


def get_monthly_release_service(
    settings: Annotated[MonthlyRuntimeSettings, Depends(get_monthly_release_settings)],
) -> MonthlyReleaseService:
    return settings.service()


def _error(error: MonthlyReleaseError) -> dict[str, Any]:
    return {
        "error_code": error.code,
        "message": str(error),
        "retryable": bool(error.retryable),
        "context": error.context,
    }


def _call(operation: Callable[[], T]) -> T:
    try:
        return operation()
    except MonthlyReleaseAuthorizationError as exc:
        raise HTTPException(status_code=403, detail=_error(exc)) from exc
    except MonthlyReleaseRequestInvalid as exc:
        raise HTTPException(status_code=422, detail=_error(exc)) from exc
    except (MonthlyReleaseConflict, MonthlyReleaseNotReady) as exc:
        raise HTTPException(status_code=409, detail=_error(exc)) from exc
    except MonthlyRuntimeConfigurationError as exc:
        raise HTTPException(status_code=503, detail=_error(exc)) from exc
    except MonthlyReleaseError as exc:
        raise HTTPException(status_code=400, detail=_error(exc)) from exc
    except (FileExistsError, ValueError) as exc:
        wrapped = MonthlyReleaseConflict(str(exc))
        raise HTTPException(status_code=409, detail=_error(wrapped)) from exc


def _idempotency(value: Annotated[str, Header(alias="Idempotency-Key")]) -> str:
    normalized = value.strip()
    if not normalized or len(normalized) > 256:
        raise HTTPException(
            status_code=422,
            detail={
                "error_code": "MONTHLY_RELEASE_IDEMPOTENCY_INVALID",
                "message": "invalid Idempotency-Key",
            },
        )
    return normalized


def _request(value: UnifiedMonthlyReleaseRequest, idempotency_key: str) -> MonthlyReleaseRequest:
    return MonthlyReleaseRequest(
        target_cutoff=value.target_cutoff,
        product_profile=value.product_profile,
        idempotency_key=idempotency_key,
        activation_mode=value.activation_mode,
        activation_authorization_ref=value.activation_authorization_ref,
        repair_authorization_refs=value.repair_authorization_refs,
    )


@router.post("/plan")
def preview_monthly_release(
    request: UnifiedMonthlyReleaseRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    idempotency_key: Annotated[str, Depends(_idempotency)],
    service: Annotated[MonthlyReleaseService, Depends(get_monthly_release_service)],
) -> dict[str, Any]:
    del principal
    return {
        "schema_version": "aistock_monthly_release_plan_response_v1",
        "data": _call(lambda: service.preview(_request(request, idempotency_key))),
    }


@router.post("", status_code=202)
def submit_monthly_release(
    request: UnifiedMonthlyReleaseRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    idempotency_key: Annotated[str, Depends(_idempotency)],
    service: Annotated[MonthlyReleaseService, Depends(get_monthly_release_service)],
) -> dict[str, Any]:
    data = _call(
        lambda: service.submit(
            _request(request, idempotency_key),
            principal=principal.principal_id,
        )
    )
    operation_id = str(data["operation_id"])
    return {
        "schema_version": "aistock_monthly_release_submission_v1",
        "data": {
            **data,
            "status_url": f"/api/v1/qlib/monthly-releases/{operation_id}",
        },
    }


@router.get("/{operation_id}")
def get_monthly_release(
    operation_id: str,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[MonthlyReleaseService, Depends(get_monthly_release_service)],
) -> dict[str, Any]:
    del principal
    return {
        "schema_version": "aistock_monthly_release_status_v1",
        "data": _call(lambda: service.status(operation_id)),
    }


def _receipt_index(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    if root.is_symlink() or not root.is_dir():
        raise MonthlyReleaseError("monthly receipt root is invalid")
    paths = sorted(root.glob("*.json"))
    if len(paths) > MAX_RECEIPT_FILES:
        raise MonthlyReleaseError("monthly receipt index exceeds the bounded contract")
    items: list[dict[str, Any]] = []
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise MonthlyReleaseError("monthly receipt entry is not a regular file")
        size = path.stat().st_size
        if size > MAX_RECEIPT_BYTES:
            raise MonthlyReleaseError("monthly receipt entry exceeds the bounded contract")
        items.append(
            {
                "name": path.name,
                "size": size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    return items


@router.get("/{operation_id}/receipts")
def get_monthly_release_receipts(
    operation_id: str,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[MonthlyReleaseService, Depends(get_monthly_release_service)],
) -> dict[str, Any]:
    del principal
    _call(lambda: service.status(operation_id))
    root = _call(lambda: service.store.operation_root(operation_id)) / "receipts"
    return {
        "schema_version": "aistock_monthly_release_receipt_index_v1",
        "items": _call(lambda: _receipt_index(root)),
    }


@router.post("/{operation_id}/resume", status_code=202)
def resume_monthly_release(
    operation_id: str,
    request: EmptyCommandRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[MonthlyReleaseService, Depends(get_monthly_release_service)],
) -> dict[str, Any]:
    del request, principal
    return {
        "schema_version": "aistock_monthly_release_status_v1",
        "data": _call(lambda: service.resume(operation_id)),
    }


@router.post("/{operation_id}/cancel", status_code=202)
def cancel_monthly_release(
    operation_id: str,
    request: EmptyCommandRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[MonthlyReleaseService, Depends(get_monthly_release_service)],
) -> dict[str, Any]:
    del request, principal
    return {
        "schema_version": "aistock_monthly_release_status_v1",
        "data": _call(lambda: service.cancel(operation_id)),
    }


@router.post("/{operation_id}/activate")
def activate_monthly_release(
    operation_id: str,
    request: UnifiedMonthlyActionRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[MonthlyReleaseService, Depends(get_monthly_release_service)],
    settings: Annotated[MonthlyRuntimeSettings, Depends(get_monthly_release_settings)],
) -> dict[str, Any]:
    def verify(ready: Mapping[str, Any]) -> dict[str, Any]:
        profile = load_qe_profile(settings.active_profile)
        validate_controller_snapshot(profile)
        return {
            "status": "PASS",
            "dataset_manifest_sha256": str(profile.raw["components"]["dataset_manifest_sha256"]),
            "profile_sha256": profile.profile_sha256,
            "expected_manifest_sha256": ready["dataset_manifest_sha256"],
        }

    return {
        "schema_version": "aistock_monthly_release_status_v1",
        "data": _call(
            lambda: service.activate(
                operation_id,
                authorization_store=ActionAuthorizationStore(settings.authorization_root),
                authorization_ref=request.authorization_ref,
                principal=principal.principal_id,
                verify_after=verify,
            )
        ),
    }


@router.post("/{operation_id}/rollback")
def rollback_monthly_release(
    operation_id: str,
    request: UnifiedMonthlyActionRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[MonthlyReleaseService, Depends(get_monthly_release_service)],
    settings: Annotated[MonthlyRuntimeSettings, Depends(get_monthly_release_settings)],
) -> dict[str, Any]:
    def verify(_predecessor: Mapping[str, Any]) -> dict[str, Any]:
        profile = load_qe_profile(settings.active_profile)
        validate_controller_snapshot(profile)
        return {"profile_sha256": profile.profile_sha256}

    return {
        "schema_version": "aistock_monthly_release_rollback_response_v1",
        "data": _call(
            lambda: service.rollback(
                operation_id,
                authorization_store=ActionAuthorizationStore(settings.authorization_root),
                authorization_ref=request.authorization_ref,
                principal=principal.principal_id,
                verify_after=verify,
            )
        ),
    }


__all__ = ("router",)
