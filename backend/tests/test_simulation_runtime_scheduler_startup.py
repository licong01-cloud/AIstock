from __future__ import annotations

import asyncio
from types import SimpleNamespace

from fastapi import FastAPI

import backend.main as backend_main
import backend.services.simulation_runtime as simulation_runtime_module


class _FakeQmtClient:
    def __init__(self, calls: list[tuple[str, tuple, dict]]) -> None:
        self._calls = calls

    def connect(self):
        self._calls.append(("qmt_connect", tuple(), {}))
        return True, "ok"

    def disconnect(self):
        self._calls.append(("qmt_disconnect", tuple(), {}))


def _run_lifespan(monkeypatch, *, enable_sim_runtime: bool) -> list[tuple[str, tuple, dict]]:
    calls: list[tuple[str, tuple, dict]] = []
    scheduler = simulation_runtime_module.simulation_lifecycle_background_scheduler

    monkeypatch.setenv("DISABLE_INGESTION_SCHEDULER", "1")
    monkeypatch.setenv("DISABLE_STRATEGY_SCHEDULER", "1")
    monkeypatch.setenv("DISABLE_CORRELATION_SCHEDULER", "1")
    monkeypatch.setenv("DISABLE_FACTOR_METRICS_SCHEDULER", "1")
    monkeypatch.setenv("DISABLE_NODE_HEALTH_SCHEDULER", "1")
    monkeypatch.setenv("DISABLE_HMM_SCHEDULER", "1")
    monkeypatch.setenv("DISABLE_EVOLUTION_SCANNER", "1")
    monkeypatch.setenv("DISABLE_QE_EXPERIMENT_SCANNER", "1")
    monkeypatch.delenv("ENABLE_PAPER_TRADING_V2_SCHEDULER", raising=False)
    monkeypatch.setenv("ENABLE_SIMULATION_RUNTIME_SCHEDULER", "1" if enable_sim_runtime else "0")

    monkeypatch.setattr(backend_main, "init_db_pool", lambda *args, **kwargs: calls.append(("init_db_pool", args, kwargs)))
    monkeypatch.setattr(backend_main, "close_db_pool", lambda *args, **kwargs: calls.append(("close_db_pool", args, kwargs)))
    monkeypatch.setattr(backend_main, "ingestion_scheduler", SimpleNamespace(start=lambda *args, **kwargs: calls.append(("ingestion_start", args, kwargs)), shutdown=lambda *args, **kwargs: calls.append(("ingestion_shutdown", args, kwargs))))
    monkeypatch.setattr(backend_main, "strategy_scheduler", SimpleNamespace(start=lambda *args, **kwargs: calls.append(("strategy_start", args, kwargs)), shutdown=lambda *args, **kwargs: calls.append(("strategy_shutdown", args, kwargs))))
    monkeypatch.setattr(backend_main, "get_qmt_client_singleton", lambda: _FakeQmtClient(calls))

    monkeypatch.setattr(scheduler, "start", lambda *args, **kwargs: calls.append(("simulation_scheduler_start", args, kwargs)) or {"running": True})
    monkeypatch.setattr(scheduler, "shutdown", lambda *args, **kwargs: calls.append(("simulation_scheduler_shutdown", args, kwargs)) or {"running": False})

    app = FastAPI()
    async def _run() -> None:
        async with backend_main._lifespan(app):
            return None

    asyncio.run(_run())
    return calls


def test_main_lifespan_does_not_autostart_simulation_scheduler_by_default(monkeypatch) -> None:
    calls = _run_lifespan(monkeypatch, enable_sim_runtime=False)
    assert ("simulation_scheduler_start", tuple(), {}) not in calls
    # The scheduler is still shut down during lifespan cleanup, but that is
    # an idempotent no-op when it was never started.
    assert ("simulation_scheduler_shutdown", tuple(), {"wait": False}) in calls


def test_main_lifespan_opt_in_starts_and_stops_simulation_scheduler(monkeypatch) -> None:
    calls = _run_lifespan(monkeypatch, enable_sim_runtime=True)
    assert ("simulation_scheduler_start", tuple(), {}) in calls
    assert ("simulation_scheduler_shutdown", tuple(), {"wait": False}) in calls
