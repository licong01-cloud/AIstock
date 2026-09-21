"""Controlled construction of the unified monthly release service.

Only filesystem locations and node path mappings are configuration.  Producer
commands and producer selection are deliberately absent; the durable worker
must receive an :class:`OfficialMonthlyProducerRegistry` built by repository
code.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import os
from pathlib import Path, PurePosixPath
import re
from typing import Callable

from .monthly_registry import OfficialMonthlyProducerRegistry
from .monthly_unified import (
    MonthlyOperationStore,
    MonthlyPipeline,
    MonthlyReleaseError,
    MonthlyReleaseRequest,
    MonthlyReleaseService,
)


_POSIX_ROOT = re.compile(r"^/(?:[^/\\\x00]+/)*[^/\\\x00]+$")


class MonthlyRuntimeConfigurationError(MonthlyReleaseError):
    code = "MONTHLY_RELEASE_RUNTIME_UNAVAILABLE"


class SubmissionOnlyPipeline(MonthlyPipeline):
    """API-side sentinel; data-bearing work is owned by the durable worker."""

    def run_stage(self, **_kwargs):  # type: ignore[no-untyped-def]
        raise MonthlyRuntimeConfigurationError(
            "monthly data stages must run in the code-registered worker"
        )


def _plain_absolute(path: Path, *, label: str, must_exist: bool) -> Path:
    if not path.is_absolute():
        raise MonthlyRuntimeConfigurationError(f"{label} must be absolute")
    if must_exist and not path.exists():
        raise MonthlyRuntimeConfigurationError(f"{label} is unavailable")
    existing = path if path.exists() else path.parent
    while True:
        if existing.is_symlink() or bool(getattr(existing, "is_junction", lambda: False)()):
            raise MonthlyRuntimeConfigurationError(f"{label} path chain must not be linked")
        if existing.parent == existing:
            break
        existing = existing.parent
    return path


@dataclass(frozen=True, slots=True)
class MonthlyRuntimeSettings:
    state_root: Path
    active_profile: Path
    controller_release_root: Path
    profile_candidate_root: Path
    artifact_root: Path
    wsl_release_root: str
    node1_release_root: str
    authorization_root: Path

    @classmethod
    def from_env(cls) -> "MonthlyRuntimeSettings":
        required = {
            "state_root": "AISTOCK_MONTHLY_RELEASE_STATE_ROOT",
            "active_profile": "AISTOCK_ACTIVE_DATASET_PROFILE_PATH",
            "controller_release_root": "AISTOCK_MONTHLY_CONTROLLER_RELEASE_ROOT",
            "profile_candidate_root": "AISTOCK_MONTHLY_PROFILE_CANDIDATE_ROOT",
            "artifact_root": "AISTOCK_MONTHLY_RELEASE_ARTIFACT_ROOT",
            "wsl_release_root": "AISTOCK_MONTHLY_WSL_RELEASE_ROOT",
            "node1_release_root": "AISTOCK_MONTHLY_NODE1_RELEASE_ROOT",
            "authorization_root": "AISTOCK_DATASET_ACTION_AUTHORIZATION_ROOT",
        }
        raw = {name: str(os.getenv(env) or "").strip() for name, env in required.items()}
        missing = sorted(required[name] for name, value in raw.items() if not value)
        if missing:
            raise MonthlyRuntimeConfigurationError(
                "monthly release environment is incomplete",
                context={"missing": missing},
            )
        paths = {
            name: Path(raw[name])
            for name in (
                "state_root",
                "active_profile",
                "controller_release_root",
                "profile_candidate_root",
                "artifact_root",
                "authorization_root",
            )
        }
        _plain_absolute(paths["active_profile"], label="active_profile", must_exist=True)
        for name in (
            "state_root",
            "controller_release_root",
            "profile_candidate_root",
            "artifact_root",
            "authorization_root",
        ):
            _plain_absolute(paths[name], label=name, must_exist=True)
            if not paths[name].is_dir():
                raise MonthlyRuntimeConfigurationError(f"{name} must be a directory")
        if paths["active_profile"].is_dir():
            raise MonthlyRuntimeConfigurationError("active_profile must be a file")
        for name in ("wsl_release_root", "node1_release_root"):
            parsed = PurePosixPath(raw[name])
            if (
                _POSIX_ROOT.fullmatch(raw[name]) is None
                or any(part in {".", ".."} for part in parsed.parts)
                or str(parsed) != raw[name].rstrip("/")
            ):
                raise MonthlyRuntimeConfigurationError(f"{name} must be a canonical POSIX root")
        release_root = paths["controller_release_root"].resolve(strict=True)
        artifact_root = paths["artifact_root"].resolve(strict=True)
        if release_root == artifact_root or release_root.is_relative_to(
            artifact_root
        ) or artifact_root.is_relative_to(release_root):
            raise MonthlyRuntimeConfigurationError(
                "controller_release_root and artifact_root must not overlap"
            )
        return cls(
            state_root=paths["state_root"],
            active_profile=paths["active_profile"],
            controller_release_root=paths["controller_release_root"],
            profile_candidate_root=paths["profile_candidate_root"],
            artifact_root=paths["artifact_root"],
            wsl_release_root=raw["wsl_release_root"].rstrip("/"),
            node1_release_root=raw["node1_release_root"].rstrip("/"),
            authorization_root=paths["authorization_root"],
        )

    def candidate_name(self, request: MonthlyReleaseRequest) -> str:
        return f"{request.target_cutoff:%Y%m%d}-qe_hmm_full_v2-direct-monthly-v2-unified-candidate"

    def generation(self, request: MonthlyReleaseRequest) -> str:
        return f"{request.target_cutoff:%Y%m%d}-monthly-v2-unified"

    def revision(self, request: MonthlyReleaseRequest) -> str:
        return f"{request.target_cutoff:%Y%m%d}-monthly-v2"

    def service(
        self,
        *,
        pipeline: MonthlyPipeline | None = None,
        cutoff_resolver: Callable[[], date] | None = None,
    ) -> MonthlyReleaseService:
        def official_cutoff() -> date:
            from .control_service import resolve_previous_month_trading_cutoff

            return resolve_previous_month_trading_cutoff(datetime.now(UTC))

        return MonthlyReleaseService(
            MonthlyOperationStore(self.state_root),
            active_profile=self.active_profile,
            pipeline=pipeline or SubmissionOnlyPipeline(),
            candidate_root_factory=lambda request: self.controller_release_root
            / self.candidate_name(request),
            profile_candidate_factory=lambda request: self.profile_candidate_root
            / f"{request.product_profile}_{request.target_cutoff:%Y%m%d}_monthly_v2.json",
            node_root_factory=lambda request: {
                "wsl2-5080": f"{self.wsl_release_root}/{self.candidate_name(request)}",
                "rdagent-node1": f"{self.node1_release_root}/{self.candidate_name(request)}",
            },
            generation_factory=self.generation,
            revision_factory=self.revision,
            allowed_cutoff_resolver=cutoff_resolver or official_cutoff,
        )

    def worker_service(
        self,
        registry: OfficialMonthlyProducerRegistry,
        *,
        cutoff_resolver: Callable[[], date] | None = None,
    ) -> MonthlyReleaseService:
        pipeline = registry.pipeline(artifact_roots=(self.artifact_root, self.controller_release_root))
        return self.service(pipeline=pipeline, cutoff_resolver=cutoff_resolver)


__all__ = (
    "MonthlyRuntimeConfigurationError",
    "MonthlyRuntimeSettings",
    "SubmissionOnlyPipeline",
)
