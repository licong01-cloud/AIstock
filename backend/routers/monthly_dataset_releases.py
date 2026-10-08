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
    UnifiedMonthlyAdoptRequest,
    UnifiedMonthlyAuthorizationIssueRequest,
    UnifiedMonthlyReleaseRequest,
    UnifiedMonthlyRepairInputsRequest,
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
    QEActiveDatasetProfileError,
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


@router.post("/{operation_id}/source-quality-inputs")
def bind_monthly_source_quality_inputs(
    operation_id: str,
    request: UnifiedMonthlyRepairInputsRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[MonthlyReleaseService, Depends(get_monthly_release_service)],
) -> dict[str, Any]:
    return {"schema_version": "aistock_monthly_source_quality_binding_response_v1",
            "data": _call(lambda: service.bind_source_quality_inputs(operation_id,
                inputs=request.inputs, principal=principal.principal_id))}


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


@router.post("/{operation_id}/repair-inputs")
def bind_monthly_repair_inputs(
    operation_id: str,
    request: UnifiedMonthlyRepairInputsRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[MonthlyReleaseService, Depends(get_monthly_release_service)],
) -> dict[str, Any]:
    return {"schema_version": "aistock_monthly_repair_inputs_response_v1", "data": _call(
        lambda: service.bind_repair_inputs(operation_id, inputs=request.inputs, principal=principal.principal_id)
    )}


@router.post("/adopt", status_code=201)
def adopt_existing_monthly_successor(
    request: UnifiedMonthlyAdoptRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    idempotency_key: Annotated[str, Depends(_idempotency)],
    service: Annotated[MonthlyReleaseService, Depends(get_monthly_release_service)],
    settings: Annotated[MonthlyRuntimeSettings, Depends(get_monthly_release_settings)],
) -> dict[str, Any]:
    candidate_name = Path(request.candidate_root).name

    def adopt() -> dict[str, Any]:
        try:
            target_profile = load_qe_profile(Path(request.profile_candidate))
            validate_controller_snapshot(target_profile)
        except QEActiveDatasetProfileError as exc:
            raise MonthlyReleaseConflict(
                "adopted profile failed the existing controller validator"
            ) from exc
        return service.adopt_existing_successor(
            idempotency_key=idempotency_key,
            product_profile=request.product_profile,
            target_cutoff=request.target_cutoff,
            candidate_root=Path(request.candidate_root),
            profile_candidate=Path(request.profile_candidate),
            predecessor_profile_sha256=request.predecessor_profile_sha256,
            target_profile_sha256=request.target_profile_sha256,
            dataset_manifest_sha256=request.dataset_manifest_sha256,
            dataset_manifest_file_sha256=request.dataset_manifest_file_sha256,
            node_manifest_file_sha256=request.node_manifest_file_sha256,
            evidence_refs=[item.model_dump() for item in request.evidence_refs],
            controller_release_root=settings.controller_release_root,
            profile_candidate_root=settings.profile_candidate_root,
            expected_node_roots={
                "wsl2-5080": f"{settings.wsl_release_root}/{candidate_name}",
                "rdagent-node1": f"{settings.node1_release_root}/{candidate_name}",
            },
            principal=principal.principal_id,
        )

    data = _call(
        adopt
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


@router.post("/{operation_id}/authorizations", status_code=201)
def issue_monthly_release_authorization(
    operation_id: str,
    request: UnifiedMonthlyAuthorizationIssueRequest,
    principal: Annotated[DatasetReleasePrincipal, Depends(require_dataset_release_operator)],
    service: Annotated[MonthlyReleaseService, Depends(get_monthly_release_service)],
    settings: Annotated[MonthlyRuntimeSettings, Depends(get_monthly_release_settings)],
) -> dict[str, Any]:
    return {
        "schema_version": "aistock_monthly_authorization_issue_response_v1",
        "data": _call(
            lambda: service.issue_action_authorization(
                operation_id,
                authorization_store=ActionAuthorizationStore(settings.authorization_root),
                action=request.action,
                principal=principal.principal_id,
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
