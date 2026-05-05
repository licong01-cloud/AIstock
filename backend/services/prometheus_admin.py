from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import requests


PROMETHEUS_HISTORY_CONFIRM_TEXT = "DELETE_PROMETHEUS_HISTORY"
DEFAULT_PROMETHEUS_URL = "http://localhost:9090"
DEFAULT_PROMETHEUS_MATCHERS = ("{__name__=~\".+\"}",)


class PrometheusAdminError(RuntimeError):
    """Raised when Prometheus rejects or cannot serve an admin request."""

    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.context = context or {}


@dataclass(frozen=True)
class PrometheusCleanupPlan:
    older_than_days: int
    start: str
    end: str
    matchers: list[str]
    clean_tombstones: bool
    confirm_text_required: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PrometheusAdminService:
    """Small fail-fast wrapper around Prometheus read/admin HTTP APIs."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        timeout_seconds: float | None = None,
        session: requests.Session | None = None,
        now_provider: Any | None = None,
    ) -> None:
        self.base_url = (base_url or os.getenv("AISTOCK_PROMETHEUS_URL") or DEFAULT_PROMETHEUS_URL).rstrip("/")
        self.timeout_seconds = timeout_seconds or float(os.getenv("AISTOCK_PROMETHEUS_TIMEOUT_SECONDS", "10"))
        self.session = session or requests.Session()
        self._now_provider = now_provider

    def get_status(self) -> dict[str, Any]:
        ready = self._request_text("GET", "/-/ready")
        flags = self._request_json("GET", "/api/v1/status/flags").get("data", {})
        runtime_info = self._request_json("GET", "/api/v1/status/runtimeinfo").get("data", {})
        tsdb_status = self._request_json("GET", "/api/v1/status/tsdb").get("data", {})

        return {
            "base_url": self.base_url,
            "ready": ready,
            "retention": {
                "time": flags.get("storage.tsdb.retention.time"),
                "size": flags.get("storage.tsdb.retention.size"),
            },
            "admin_api_enabled": str(flags.get("web.enable-admin-api", "")).lower() == "true",
            "lifecycle_enabled": str(flags.get("web.enable-lifecycle", "")).lower() == "true",
            "runtime_info": runtime_info,
            "tsdb_status": tsdb_status,
        }

    def build_cleanup_plan(
        self,
        *,
        older_than_days: int,
        matchers: Iterable[str] | None = None,
        clean_tombstones: bool = True,
    ) -> PrometheusCleanupPlan:
        if older_than_days < 1:
            raise ValueError("older_than_days must be >= 1")

        normalized_matchers = [m.strip() for m in (matchers or DEFAULT_PROMETHEUS_MATCHERS) if m.strip()]
        if not normalized_matchers:
            raise ValueError("at least one Prometheus matcher is required")

        cutoff = self._now() - timedelta(days=older_than_days)
        return PrometheusCleanupPlan(
            older_than_days=older_than_days,
            start="1970-01-01T00:00:00Z",
            end=_format_prometheus_time(cutoff),
            matchers=normalized_matchers,
            clean_tombstones=clean_tombstones,
            confirm_text_required=PROMETHEUS_HISTORY_CONFIRM_TEXT,
        )

    def cleanup_history(
        self,
        *,
        older_than_days: int,
        matchers: Iterable[str] | None = None,
        clean_tombstones: bool = True,
    ) -> dict[str, Any]:
        plan = self.build_cleanup_plan(
            older_than_days=older_than_days,
            matchers=matchers,
            clean_tombstones=clean_tombstones,
        )

        delete_params: list[tuple[str, str]] = [("match[]", matcher) for matcher in plan.matchers]
        delete_params.extend([("start", plan.start), ("end", plan.end)])
        delete_result = self._request_json("POST", "/api/v1/admin/tsdb/delete_series", params=delete_params)

        clean_result: dict[str, Any] | None = None
        if clean_tombstones:
            clean_result = self._request_json("POST", "/api/v1/admin/tsdb/clean_tombstones")

        return {
            "plan": plan.to_dict(),
            "delete_series": delete_result,
            "clean_tombstones": clean_result,
        }

    def _now(self) -> datetime:
        if self._now_provider is not None:
            return self._now_provider()
        return datetime.now(timezone.utc)

    def _request_text(self, method: str, path: str) -> dict[str, Any]:
        response = self._request_raw(method, path)
        return {
            "status_code": response.status_code,
            "body": response.text.strip(),
        }

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: list[tuple[str, str]] | dict[str, str] | None = None,
    ) -> dict[str, Any]:
        response = self._request_raw(method, path, params=params)
        if response.status_code == 204 or not response.text.strip():
            return {"status": "success", "data": None}

        try:
            payload = response.json()
        except ValueError as exc:
            raise PrometheusAdminError(
                "Prometheus returned a non-JSON response",
                context={"path": path, "status_code": response.status_code, "body": response.text[:500]},
            ) from exc

        if payload.get("status") == "error":
            raise PrometheusAdminError(
                payload.get("error") or "Prometheus API returned an error",
                context={
                    "path": path,
                    "error_type": payload.get("errorType"),
                    "status_code": response.status_code,
                },
            )
        return payload

    def _request_raw(
        self,
        method: str,
        path: str,
        *,
        params: list[tuple[str, str]] | dict[str, str] | None = None,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        try:
            response = self.session.request(method, url, params=params, timeout=self.timeout_seconds)
        except requests.RequestException as exc:
            raise PrometheusAdminError(
                "Cannot reach Prometheus",
                context={"url": url, "error": str(exc)},
            ) from exc

        if response.status_code >= 400:
            raise PrometheusAdminError(
                "Prometheus returned HTTP error",
                context={"url": url, "status_code": response.status_code, "body": response.text[:500]},
            )
        return response


def get_prometheus_admin_service() -> PrometheusAdminService:
    return PrometheusAdminService()


def _format_prometheus_time(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
