"""Staged performance receipt helpers for real HMM evolution executions.

Only real execution stages write receipt data.  Uncollected fields stay null
or unknown; this module never fabricates timings, cache states or RSS values.
"""

from __future__ import annotations

import platform
import socket
import sys
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import psutil

from .errors import InvalidSpecError
from .models import (
    KNOWN_STAGE_NAMES,
    CacheArtifactEvidence,
    CacheArtifactState,
    StageTiming,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def duration_ms(started_at: datetime, completed_at: datetime) -> int:
    delta = (completed_at - started_at).total_seconds()
    return max(0, int(round(delta * 1000)))


class StageRecorder:
    """Accumulate observed stage timings for one execution receipt."""

    def __init__(self) -> None:
        self._stages: dict[str, StageTiming] = {}
        self._open: dict[str, datetime] = {}

    def start(self, stage: str, *, at: datetime | None = None) -> None:
        self._require_known(stage)
        if stage in self._open or stage in self._stages:
            raise InvalidSpecError(
                "receipt stage was started twice",
                context={"stage": stage},
            )
        self._open[stage] = at or utc_now()

    def end(self, stage: str, *, at: datetime | None = None) -> None:
        self._require_known(stage)
        started_at = self._open.pop(stage, None)
        if started_at is None:
            raise InvalidSpecError(
                "receipt stage ended before it started",
                context={"stage": stage},
            )
        completed_at = at or utc_now()
        self._stages[stage] = StageTiming(
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=duration_ms(started_at, completed_at),
        )

    def record(self, stage: str, *, started_at: datetime, completed_at: datetime) -> None:
        self._require_known(stage)
        if stage in self._stages:
            raise InvalidSpecError(
                "receipt stage was recorded twice",
                context={"stage": stage},
            )
        self._stages[stage] = StageTiming(
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=duration_ms(started_at, completed_at),
        )

    def is_open(self, stage: str) -> bool:
        return stage in self._open

    def has(self, stage: str) -> bool:
        return stage in self._stages

    def stage_payload(self) -> dict[str, dict[str, Any]]:
        """JSON-ready payload merged into performance_receipt.stage_timings."""

        return {
            stage: {
                "started_at": timing.started_at.isoformat(),
                "completed_at": timing.completed_at.isoformat(),
                "duration_ms": timing.duration_ms,
            }
            for stage, timing in sorted(self._stages.items())
        }

    @staticmethod
    def _require_known(stage: str) -> None:
        if stage not in KNOWN_STAGE_NAMES:
            raise InvalidSpecError(
                "unknown receipt stage name",
                context={"stage": stage, "known_stages": sorted(KNOWN_STAGE_NAMES)},
            )


def capture_runtime_identity(*, owner_id: str | None = None, role: str) -> dict[str, Any]:
    """Observed runtime identity of the writing process."""

    process = psutil.Process()
    identity: dict[str, Any] = {
        "role": role,
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "pid": process.pid,
        "process_started_at": datetime.fromtimestamp(
            process.create_time(), tz=timezone.utc
        ).isoformat(),
    }
    if owner_id:
        identity["owner_id"] = owner_id
    return identity


def capture_hardware_identity() -> dict[str, Any]:
    """Observed hardware identity; every field comes from psutil/platform."""

    return {
        "host": socket.gethostname(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count_logical": psutil.cpu_count(logical=True),
        "cpu_count_physical": psutil.cpu_count(logical=False),
        "memory_total_bytes": int(psutil.virtual_memory().total),
    }


def current_rss_bytes() -> int:
    return int(psutil.Process().memory_info().rss)


def cache_evidence_from_artifact_info(
    artifact_info: Mapping[str, Mapping[str, Any]],
) -> tuple[CacheArtifactEvidence, ...]:
    """Map observed artifact source decisions to per-artifact cache evidence."""

    evidence: list[CacheArtifactEvidence] = []
    for name in sorted(artifact_info):
        info = dict(artifact_info[name])
        status = str(info.get("status") or "available")
        if status == "missing":
            # A failed Prediction Store probe is not a cache read; the real
            # artifact observation arrives with the fallback entry.
            continue
        source = str(info.get("source") or "").strip() or "unknown"
        zero_copy = bool(info.get("zero_copy", False))
        fallback = bool(info.get("fallback", False))
        downloaded_in_run = bool(info.get("downloaded_in_run", False))
        if zero_copy:
            state = CacheArtifactState.ZERO_COPY_BYPASS
        elif source == "qe_workspace_cache":
            if downloaded_in_run:
                state = (
                    CacheArtifactState.FALLBACK_DOWNLOAD
                    if fallback
                    else CacheArtifactState.COLD_MISS
                )
            else:
                state = CacheArtifactState.WARM_HIT
        elif source == "qe_workspace":
            state = (
                CacheArtifactState.FALLBACK_DOWNLOAD
                if fallback
                else CacheArtifactState.COLD_MISS
            )
        else:
            state = CacheArtifactState.UNKNOWN
        evidence.append(
            CacheArtifactEvidence(
                artifact=str(name),
                state=state,
                source=source,
                zero_copy=zero_copy,
            )
        )
    return tuple(evidence)


def evidence_payload(evidence: tuple[CacheArtifactEvidence, ...]) -> list[dict[str, Any]]:
    return [entry.model_dump(mode="json") for entry in evidence]
