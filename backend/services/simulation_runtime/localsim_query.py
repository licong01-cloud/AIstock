"""Stable-cursor read projections for successor LocalSIM products."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from backend.services.trading_core.errors import RuntimeConfigInvalidError

from .localsim_runtime_profile_repository import LocalSimRuntimeProfileRepositoryProtocol
from .models import canonical_json_sha256
from .successor_models import LocalSimReplayStatus, SimulationAccountStatus
from .successor_repository import LocalSimSuccessorRepositoryProtocol


class LocalSimListResponseV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    schema_version: str = "localsim_list_response_v1"
    items: list[Any]
    next_cursor: str | None = None
    limit: int = Field(ge=1, le=200)


class LocalSimQueryService:
    def __init__(
        self,
        *,
        repository: LocalSimSuccessorRepositoryProtocol,
        profile_repository: LocalSimRuntimeProfileRepositoryProtocol,
    ) -> None:
        self.repository = repository
        self.profile_repository = profile_repository

    def accounts(
        self,
        *,
        package_id: str | None,
        status: SimulationAccountStatus | None,
        cursor: str | None,
        limit: int,
    ) -> LocalSimListResponseV1:
        filters = {"package_id": package_id, "status": status.value if status else None}
        before = _decode_cursor(cursor, kind="accounts", filters=filters)
        rows = self.repository.list_accounts(
            package_id=package_id,
            status=status,
            before=before,
            limit=limit + 1,
        )
        return _page(rows, kind="accounts", filters=filters, limit=limit, id_field="account_id")

    def replays(
        self,
        *,
        simulation_account_id: str | None,
        status: LocalSimReplayStatus | None,
        cursor: str | None,
        limit: int,
    ) -> LocalSimListResponseV1:
        filters = {
            "simulation_account_id": simulation_account_id,
            "status": status.value if status else None,
        }
        before = _decode_cursor(cursor, kind="replays", filters=filters)
        rows = self.repository.list_replay_jobs(
            simulation_account_id=simulation_account_id,
            status=status,
            before=before,
            limit=limit + 1,
        )
        return _page(rows, kind="replays", filters=filters, limit=limit, id_field="replay_job_id")

    def profiles(
        self, *, package_id: str | None, cursor: str | None, limit: int
    ) -> LocalSimListResponseV1:
        filters = {"package_id": package_id}
        before = _decode_cursor(cursor, kind="profiles", filters=filters)
        rows = self.profile_repository.list_profiles(package_id=package_id, before=before, limit=limit + 1)
        return _page(rows, kind="profiles", filters=filters, limit=limit, id_field="profile_id")

    def profile_versions(
        self, *, profile_id: str, cursor: str | None, limit: int
    ) -> LocalSimListResponseV1:
        filters = {"profile_id": profile_id}
        before = _decode_cursor(cursor, kind="profile_versions", filters=filters)
        rows = self.profile_repository.list_versions(profile_id=profile_id, before=before, limit=limit + 1)
        return _page(
            rows,
            kind="profile_versions",
            filters=filters,
            limit=limit,
            id_field="profile_version_id",
        )


def _page(rows: list[Any], *, kind: str, filters: dict[str, Any], limit: int, id_field: str) -> LocalSimListResponseV1:
    visible = rows[:limit]
    next_cursor = None
    if len(rows) > limit and visible:
        last = visible[-1]
        next_cursor = _encode_cursor(
            kind=kind,
            filters=filters,
            created_at=last.created_at,
            object_id=str(getattr(last, id_field)),
        )
    return LocalSimListResponseV1(items=visible, next_cursor=next_cursor, limit=limit)


def _encode_cursor(*, kind: str, filters: dict[str, Any], created_at: datetime, object_id: str) -> str:
    payload = {
        "schema_version": "localsim_cursor_v1",
        "kind": kind,
        "filters_sha256": canonical_json_sha256(filters),
        "created_at": created_at.astimezone(UTC).isoformat(),
        "id": object_id,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(encoded).decode("ascii").rstrip("=")


def _decode_cursor(
    cursor: str | None, *, kind: str, filters: dict[str, Any]
) -> tuple[datetime, str] | None:
    if cursor is None:
        return None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
        created_at = datetime.fromisoformat(str(payload["created_at"]))
        object_id = str(payload["id"]).strip()
    except (ValueError, KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeConfigInvalidError(
            "LocalSIM cursor is invalid",
            context={"reason_code": "LOCALSIM_CURSOR_INVALID"},
        ) from exc
    if (
        payload.get("schema_version") != "localsim_cursor_v1"
        or payload.get("kind") != kind
        or payload.get("filters_sha256") != canonical_json_sha256(filters)
        or created_at.tzinfo is None
        or created_at.utcoffset() is None
        or not object_id
    ):
        raise RuntimeConfigInvalidError(
            "LocalSIM cursor does not match this query",
            context={"reason_code": "LOCALSIM_CURSOR_SCOPE_MISMATCH"},
        )
    return created_at.astimezone(UTC), object_id
