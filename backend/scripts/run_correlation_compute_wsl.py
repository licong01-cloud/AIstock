#!/usr/bin/env python3
from __future__ import annotations

import importlib
import json
import logging
import sys
import traceback
import types
from pathlib import Path

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
for noisy in ("qlib", "rdagent", "urllib3", "filelock"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

logger = logging.getLogger("run_correlation_compute_wsl")
REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env", override=True)
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

backend_pkg = importlib.import_module("backend")
routers_pkg = types.ModuleType("backend.routers")
routers_pkg.__path__ = [str(REPO_ROOT / "backend" / "routers")]
sys.modules.setdefault("backend.routers", routers_pkg)
setattr(backend_pkg, "routers", routers_pkg)

stub_qe_service = types.ModuleType("backend.services.quantevolver.qe_evolution_service")
class _DummyAutoEvolutionScheduler:
    pass
stub_qe_service.AutoEvolutionScheduler = _DummyAutoEvolutionScheduler
sys.modules.setdefault("backend.services.quantevolver.qe_evolution_service", stub_qe_service)

qe_evolution = importlib.import_module("backend.routers.quantevolver_evolution")


def _emit(obj: dict) -> None:
    print(json.dumps(obj, ensure_ascii=False), flush=True)


def main() -> int:
    if len(sys.argv) != 2:
        _emit({"type": "result", "data": {"success": False, "error": "usage: run_correlation_compute_wsl.py <payload.json>"}})
        return 1

    payload_path = Path(sys.argv[1])
    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))

        def _event_emitter(event: dict) -> None:
            _emit(event)

        qe_evolution.set_correlation_event_emitter(_event_emitter)
        result = qe_evolution._run_correlation_compute_local(
            factor_names=list(payload.get("factor_names") or []),
            as_of_date=payload.get("as_of_date"),
            job_id=payload.get("job_id"),
            data_date=payload.get("data_date"),
        )
        _emit({"type": "result", "data": result})
        return 0
    except Exception as exc:
        logger.error("correlation WSL runner failed: %s", exc, exc_info=True)
        _emit(
            {
                "type": "result",
                "data": {
                    "success": False,
                    "status": "failed",
                    "error": str(exc),
                    "traceback": traceback.format_exc().splitlines()[-20:],
                },
            }
        )
        return 1
    finally:
        qe_evolution.set_correlation_event_emitter(None)


if __name__ == "__main__":
    raise SystemExit(main())
