#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import sys
import traceback
from pathlib import Path

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
for noisy in ("qlib", "rdagent", "urllib3", "filelock"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

logger = logging.getLogger("run_official_factor_full_compute_wsl")
REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env", override=True)
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.quantevolver.official_factor_batch_compute_service import (  # noqa: E402
    OfficialFactorBatchComputeService,
)
from backend.services.quantevolver.wsl_runtime_guard import assert_wsl_runtime  # noqa: E402


def _emit(obj: dict) -> None:
    print(json.dumps(obj, ensure_ascii=False, default=str), flush=True)


def main() -> int:
    if len(sys.argv) != 2:
        _emit({"type": "result", "data": {"success": False, "error": "usage: run_official_factor_full_compute_wsl.py <payload.json>"}})
        return 1

    payload_path = Path(sys.argv[1])
    try:
        assert_wsl_runtime("run_official_factor_full_compute_wsl")
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        svc = OfficialFactorBatchComputeService(event_emitter=_emit)
        result = svc.compute(payload)
        _emit({"type": "result", "data": result})
        return 0 if result.get("success") else 1
    except Exception as exc:
        logger.error("official full factor WSL runner failed: %s", exc, exc_info=True)
        _emit({
            "type": "result",
            "data": {
                "success": False,
                "status": "failed",
                "error": str(exc),
                "traceback": traceback.format_exc().splitlines()[-20:],
            },
        })
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
