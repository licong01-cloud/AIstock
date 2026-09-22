from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import monthly_unified_dataset_release_worker as cli


class _Worker:
    def __init__(self, values: list[dict[str, object] | None]) -> None:
        self.values = list(values)
        self.calls = 0

    def run_once(self):  # type: ignore[no-untyped-def]
        self.calls += 1
        return self.values.pop(0) if self.values else None


class _Runtime:
    def __init__(self, values: list[dict[str, object] | None]) -> None:
        self.worker = _Worker(values)

    def preflight_receipt(self) -> dict[str, object]:
        return {
            "schema_version": "aistock_monthly_release_worker_preflight_v1",
            "status": "PASS",
        }


def _loader(runtime: _Runtime):
    def load(*, project_root: Path):
        assert project_root == cli.PROJECT_ROOT
        return runtime

    return load


def test_preflight_does_not_run_an_operation(capsys: pytest.CaptureFixture[str]) -> None:
    runtime = _Runtime([{"operation_id": "forbidden"}])

    assert cli.main(["--preflight"], runtime_loader=_loader(runtime)) == 0

    assert json.loads(capsys.readouterr().out)["status"] == "PASS"
    assert runtime.worker.calls == 0


def test_once_and_bounded_drain_process_only_durable_pending_operations(
    capsys: pytest.CaptureFixture[str],
) -> None:
    once = _Runtime([{"operation_id": "dmr_one", "status": "SOURCE_READY"}])
    assert cli.main(["--once"], runtime_loader=_loader(once)) == 0
    assert json.loads(capsys.readouterr().out)["processed_operation_count"] == 1

    drain = _Runtime(
        [
            {"operation_id": "dmr_a", "status": "SOURCE_READY"},
            {"operation_id": "dmr_b", "status": "READY_TO_ACTIVATE"},
            {"operation_id": "dmr_c", "status": "forbidden"},
        ]
    )
    assert (
        cli.main(
            ["--drain", "--max-operations", "2"],
            runtime_loader=_loader(drain),
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["processed_operation_count"] == 2
    assert payload["last_operation_id"] == "dmr_b"
    assert drain.worker.calls == 2


@pytest.mark.parametrize(
    "arguments",
    (
        ["--drain"],
        ["--once", "--max-operations", "2"],
        ["--once", "--poll-seconds", "nan"],
    ),
)
def test_invalid_bounds_fail_closed(
    arguments: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    runtime = _Runtime([])
    assert cli.main(arguments, runtime_loader=_loader(runtime)) == 2
    assert json.loads(capsys.readouterr().err)["status"] == "FAILED"
    assert runtime.worker.calls == 0


def test_runtime_boot_failure_is_structured_and_nonzero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail(**_kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("missing frozen authority")

    assert cli.main(["--once"], runtime_loader=fail) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["reason_code"] == "MONTHLY_RELEASE_WORKER_FAILED"
    assert "missing frozen authority" in payload["message"]


def test_runtime_boot_failure_preserves_typed_context(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from backend.services.dataset_release.monthly_runtime import (
        MonthlyRuntimeConfigurationError,
    )

    def fail(**_kwargs):  # type: ignore[no-untyped-def]
        raise MonthlyRuntimeConfigurationError(
            "monthly release environment is incomplete",
            context={"missing": ["AISTOCK_MONTHLY_RELEASE_STATE_ROOT"]},
        )

    assert cli.main(["--preflight"], runtime_loader=fail) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["reason_code"] == "MONTHLY_RELEASE_RUNTIME_UNAVAILABLE"
    assert payload["context"] == {
        "missing": ["AISTOCK_MONTHLY_RELEASE_STATE_ROOT"]
    }


def test_activation_verifier_is_wired_into_worker(monkeypatch, tmp_path: Path) -> None:
    from backend.services.dataset_release import monthly_worker_runtime as runtime_module

    settings = SimpleNamespace(
        authorization_root=tmp_path,
        active_profile=tmp_path / "active.json",
        worker_service=lambda registry: "service",
    )
    nodes = object()
    production = object()
    registry = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(runtime_module.MonthlyRuntimeSettings, "from_env", lambda: settings)
    monkeypatch.setattr(runtime_module.MonthlyNodeRuntimeSettings, "from_env", lambda: nodes)
    monkeypatch.setattr(
        runtime_module.MonthlyProductionSettings,
        "from_env",
        lambda **_kwargs: production,
    )
    monkeypatch.setattr(
        runtime_module,
        "build_monthly_production_registry",
        lambda **_kwargs: registry,
    )

    def worker(service, **kwargs):  # type: ignore[no-untyped-def]
        captured["service"] = service
        captured.update(kwargs)
        return "worker"

    monkeypatch.setattr(runtime_module, "MonthlyReleaseWorker", worker)

    value = runtime_module.build_monthly_worker_runtime(project_root=tmp_path)

    assert value.worker == "worker"
    assert captured["service"] == "service"
    assert captured["authorization_store"].root == tmp_path
    assert callable(captured["activation_verifier"])
