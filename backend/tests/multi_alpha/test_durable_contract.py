from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_durable_orchestrator_has_no_legacy_daemon_or_gpu_telemetry_fallback() -> None:
    source = (
        REPO_ROOT / "backend/services/multi_alpha/durable_orchestrator.py"
    ).read_text(encoding="utf-8")
    lowered = source.lower()

    assert "daemon=true" not in lowered
    assert "asyncio.run(" not in source
    assert "shellpredbacktestexecutor" not in lowered
    assert "nvidia-smi" not in lowered
    assert "nvml" not in lowered
    assert "gpu" not in lowered
    assert "approval" not in lowered
    assert "promotion" not in lowered
    assert "except exception: pass" not in lowered


def test_backend_lifespan_starts_qe_only_orchestrator_without_enable_gate() -> None:
    source = (REPO_ROOT / "backend/main.py").read_text(encoding="utf-8")
    block = source.split("multi_alpha_durable_task = None", maxsplit=1)[1].split(
        "try:\n        yield",
        maxsplit=1,
    )[0]

    assert "run_durable_multi_alpha_orchestrator" in block
    assert "asyncio.create_task" in block
    assert "ENABLE_MULTI_ALPHA" not in block
    assert "DISABLE_MULTI_ALPHA" not in block


def test_all_production_workspace_loop_submission_is_coordinator_owned() -> None:
    backend = REPO_ROOT / "backend"
    callsites: list[str] = []
    for path in backend.rglob("*.py"):
        if "tests" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if "create_and_run_loop(" in text:
            callsites.append(str(path.relative_to(REPO_ROOT)).replace("\\", "/"))

    assert callsites == ["backend/services/quantevolver/qe_workspace_client.py"]
