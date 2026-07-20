from __future__ import annotations

import os
import socket
import threading
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Mapping


@dataclass(frozen=True)
class DurableOrchestratorHealth:
    status: str = "not_started"
    process_id: int = os.getpid()
    host: str = socket.gethostname()
    owner_id: str | None = None
    started_at: datetime | None = None
    ready_at: datetime | None = None
    heartbeat_at: datetime | None = None
    stopped_at: datetime | None = None
    stale_after_seconds: int = 180
    last_error: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        heartbeat_age = (
            (now - self.heartbeat_at).total_seconds()
            if self.heartbeat_at is not None
            else None
        )
        ready = (
            self.status == "ready"
            and heartbeat_age is not None
            and heartbeat_age <= self.stale_after_seconds
        )
        return {
            "status": self.status,
            "ready": ready,
            "process_id": self.process_id,
            "host": self.host,
            "owner_id": self.owner_id,
            "started_at": _iso(self.started_at),
            "ready_at": _iso(self.ready_at),
            "heartbeat_at": _iso(self.heartbeat_at),
            "heartbeat_age_seconds": heartbeat_age,
            "stale_after_seconds": self.stale_after_seconds,
            "stopped_at": _iso(self.stopped_at),
            "last_error": dict(self.last_error or {}),
        }


_LOCK = threading.RLock()
_HEALTH = DurableOrchestratorHealth()


def mark_durable_orchestrator_starting(
    *,
    owner_id: str | None,
    stale_after_seconds: int,
) -> None:
    now = datetime.now(timezone.utc)
    with _LOCK:
        global _HEALTH
        _HEALTH = DurableOrchestratorHealth(
            status="starting",
            owner_id=owner_id,
            started_at=now,
            heartbeat_at=now,
            stale_after_seconds=max(1, int(stale_after_seconds)),
        )


def mark_durable_orchestrator_ready(*, owner_id: str) -> None:
    now = datetime.now(timezone.utc)
    with _LOCK:
        global _HEALTH
        _HEALTH = replace(
            _HEALTH,
            status="ready",
            owner_id=owner_id,
            ready_at=_HEALTH.ready_at or now,
            heartbeat_at=now,
            stopped_at=None,
            last_error=None,
        )


def heartbeat_durable_orchestrator(*, error: Mapping[str, Any] | None = None) -> None:
    with _LOCK:
        global _HEALTH
        _HEALTH = replace(
            _HEALTH,
            heartbeat_at=datetime.now(timezone.utc),
            last_error=dict(error) if error is not None else _HEALTH.last_error,
        )


def mark_durable_orchestrator_unavailable(
    *,
    status: str,
    error: Mapping[str, Any],
) -> None:
    if status not in {"starting", "failed"}:
        raise ValueError(f"invalid durable orchestrator unavailable status: {status}")
    with _LOCK:
        global _HEALTH
        _HEALTH = replace(
            _HEALTH,
            status=status,
            heartbeat_at=datetime.now(timezone.utc),
            last_error=dict(error),
        )


def mark_durable_orchestrator_stopped() -> None:
    now = datetime.now(timezone.utc)
    with _LOCK:
        global _HEALTH
        _HEALTH = replace(
            _HEALTH,
            status="stopped",
            heartbeat_at=now,
            stopped_at=now,
        )


def get_durable_orchestrator_health() -> dict[str, Any]:
    with _LOCK:
        return _HEALTH.as_dict()


def require_durable_orchestrator_ready() -> Mapping[str, Any]:
    health = get_durable_orchestrator_health()
    if health["ready"] is not True:
        raise DurableOrchestratorUnavailableError(health)
    return health


class DurableOrchestratorUnavailableError(RuntimeError):
    def __init__(self, health: Mapping[str, Any]) -> None:
        self.health = dict(health)
        super().__init__(
            "QE-only durable multi-alpha orchestrator is not ready in this backend process"
        )


def _iso(value: datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value is not None else None
