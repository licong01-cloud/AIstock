"""Attempt-scoped supervised tools for the unified monthly BUILD stage.

The official producer registry is process scoped, while Job/cgroup ownership
is attempt scoped.  This module is the bridge between those lifetimes: each
BUILD invocation receives a fresh supervisor and the supervisor is closed
before the stage can return evidence.
"""

from __future__ import annotations

from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, Protocol

from .daily_minute_materializer import QlibDumpToolchain
from .monthly_mature_build_runner import MonthlyBuildExecutionTools
from .monthly_supervised_build import (
    MonthlyAttemptSupervisor,
    SupervisedMonthlyConsumerSmoke,
    SupervisedMonthlyQlibWriter,
)
from .monthly_worker import ProducerContext
from .profile import DatasetProfile


class MonthlyBuildSupervisor(MonthlyAttemptSupervisor, Protocol):
    def __enter__(self) -> "MonthlyBuildSupervisor": ...

    def __exit__(self, exc_type, exc, traceback) -> None: ...  # type: ignore[no-untyped-def]


MonthlyBuildSupervisorFactory = Callable[[ProducerContext], MonthlyBuildSupervisor]


@dataclass(frozen=True, slots=True)
class ResourceSupervisedMonthlyBuildScopeFactory:
    """Create the exact writer/smoke pair under one fresh attempt supervisor."""

    profile: DatasetProfile
    project_root: Path
    toolchain: QlibDumpToolchain
    supervisor_factory: MonthlyBuildSupervisorFactory

    def __post_init__(self) -> None:
        root = self.project_root.resolve(strict=True)
        if self.project_root.is_symlink() or not root.is_dir():
            raise ValueError("monthly supervised project root is unavailable")

    def __call__(
        self, context: ProducerContext
    ) -> AbstractContextManager[MonthlyBuildExecutionTools]:
        if context.stage != "BUILD":
            raise ValueError("monthly supervised scope received another stage")
        return self._scope(context)

    @contextmanager
    def _scope(self, context: ProducerContext) -> Iterator[MonthlyBuildExecutionTools]:
        supervisor = self.supervisor_factory(context)
        expected_attempt_id = f"{context.operation_id}-build"
        if (
            supervisor.attempt_id != expected_attempt_id
            or supervisor.fence != context.attempt
        ):
            raise ValueError("monthly BUILD supervisor identity differs")
        with supervisor as entered:
            if entered is not supervisor:
                raise ValueError("monthly BUILD supervisor context identity differs")
            yield MonthlyBuildExecutionTools(
                qlib_writer=SupervisedMonthlyQlibWriter(
                    profile=self.profile,
                    project_root=self.project_root,
                    toolchain=self.toolchain,
                    supervisor=supervisor,
                ),
                consumer_smoke=SupervisedMonthlyConsumerSmoke(
                    profile=self.profile,
                    project_root=self.project_root,
                    toolchain=self.toolchain,
                    supervisor=supervisor,
                ),
            )


__all__ = (
    "MonthlyBuildSupervisor",
    "MonthlyBuildSupervisorFactory",
    "ResourceSupervisedMonthlyBuildScopeFactory",
)
