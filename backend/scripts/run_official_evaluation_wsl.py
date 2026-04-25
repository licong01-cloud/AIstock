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

logger = logging.getLogger("run_official_evaluation_wsl")
REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env", override=True)
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.quantevolver.factor_official_evaluation_service import (  # noqa: E402
    FactorOfficialEvaluationService,
)


def _emit(obj: dict) -> None:
    print(json.dumps(obj, ensure_ascii=False), flush=True)


def main() -> int:
    if len(sys.argv) != 2:
        _emit({"success": False, "error": "usage: run_official_evaluation_wsl.py <payload.json>"})
        return 1

    payload_path = Path(sys.argv[1])
    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        svc = FactorOfficialEvaluationService()
        result = svc._compute_local(
            factor_names=payload.get("factor_names"),
            data_date=payload.get("data_date") or "",
            include_disabled=bool(payload.get("include_disabled", False)),
            max_workers=int(payload.get("max_workers") or 4),
            timeout_per_factor=int(payload.get("timeout_per_factor") or 600),
        )
        _emit(result)
        return 0 if result.get("success") else 1
    except Exception as exc:
        logger.error("official evaluation WSL runner failed: %s", exc, exc_info=True)
        _emit(
            {
                "success": False,
                "error": str(exc),
                "traceback": traceback.format_exc().splitlines()[-20:],
            }
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
