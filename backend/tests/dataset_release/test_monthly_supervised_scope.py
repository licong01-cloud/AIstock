from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.monthly_supervised_build import (
    SupervisedMonthlyConsumerSmoke,
    SupervisedMonthlyQlibWriter,
)
from backend.services.dataset_release.monthly_supervised_scope import (
    ResourceSupervisedMonthlyBuildScopeFactory,
)
from backend.services.dataset_release.monthly_worker import ProducerContext


def _context(*, stage: str = "BUILD") -> ProducerContext:
    return ProducerContext(
        stage=stage,
        operation_id="dmr_" + "1" * 32,
        attempt=3,
        request={},
        plan={},
        prior_receipts={},
    )


class _Supervisor:
    def __init__(self, *, attempt_id: str, fence: int, control_root: Path) -> None:
        self.attempt_id = attempt_id
        self.fence = fence
        self.control_root = control_root
        self.heartbeat_path = control_root / "heartbeat.json"
        self.events: list[str] = []

    def __enter__(self):  # type: ignore[no-untyped-def]
        self.events.append("enter")
        return self

    def __exit__(self, exc_type, exc, traceback):  # type: ignore[no-untyped-def]
        del exc_type, exc, traceback
        self.events.append("exit")

    def run_supervised(self, command, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError((command, kwargs))


def test_scope_creates_and_closes_one_supervisor_for_writer_and_smoke(
    tmp_path: Path,
) -> None:
    context = _context()
    supervisor = _Supervisor(
        attempt_id=f"{context.operation_id}-build",
        fence=context.attempt,
        control_root=tmp_path,
    )
    observed: list[ProducerContext] = []

    def create(value: ProducerContext) -> _Supervisor:
        observed.append(value)
        return supervisor

    factory = ResourceSupervisedMonthlyBuildScopeFactory(
        profile=SimpleNamespace(),
        project_root=tmp_path,
        toolchain=SimpleNamespace(),
        supervisor_factory=create,
    )

    with factory(context) as tools:
        assert isinstance(tools.qlib_writer, SupervisedMonthlyQlibWriter)
        assert isinstance(tools.consumer_smoke, SupervisedMonthlyConsumerSmoke)
        assert tools.qlib_writer.supervisor is supervisor
        assert tools.consumer_smoke.supervisor is supervisor
        assert supervisor.events == ["enter"]

    assert observed == [context]
    assert supervisor.events == ["enter", "exit"]


def test_scope_rejects_stale_attempt_supervisor(tmp_path: Path) -> None:
    context = _context()
    supervisor = _Supervisor(
        attempt_id=f"{context.operation_id}-build",
        fence=context.attempt - 1,
        control_root=tmp_path,
    )
    factory = ResourceSupervisedMonthlyBuildScopeFactory(
        profile=SimpleNamespace(),
        project_root=tmp_path,
        toolchain=SimpleNamespace(),
        supervisor_factory=lambda _context: supervisor,
    )

    with pytest.raises(ValueError, match="identity differs"):
        with factory(context):
            pass

    assert supervisor.events == []


def test_scope_rejects_non_build_stage(tmp_path: Path) -> None:
    factory = ResourceSupervisedMonthlyBuildScopeFactory(
        profile=SimpleNamespace(),
        project_root=tmp_path,
        toolchain=SimpleNamespace(),
        supervisor_factory=lambda _context: pytest.fail("must not construct supervisor"),
    )

    with pytest.raises(ValueError, match="another stage"):
        factory(_context(stage="DERIVE"))
