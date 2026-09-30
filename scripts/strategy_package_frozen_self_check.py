"""Probe frozen StrategyPackage model assets in the target runtime environment."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend import inference_engine  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe a frozen StrategyPackage params.pkl.")
    parser.add_argument("--model-params-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    os.environ["AISTOCK_STRICT_INFERENCE"] = "1"
    model_path = Path(args.model_params_path)
    if not model_path.exists() or not model_path.is_file():
        raise FileNotFoundError(f"model params path does not exist: {model_path}")
    _model, model_kind, inner_model, expected_features = inference_engine.load_model_from_pkl(model_path)
    payload: dict[str, Any] = {
        "ok": True,
        "model_params_path": str(model_path),
        "model_kind": model_kind,
        "model_expected_features": int(expected_features or 0),
        "inner_model_type": type(inner_model).__name__ if inner_model is not None else None,
    }
    output = Path(args.output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        diagnostic = {"ok": False, "error_type": type(exc).__name__, "message": str(exc)}
        print("AISTOCK_FROZEN_SELF_CHECK_ERROR=" + json.dumps(diagnostic, ensure_ascii=False), file=sys.stderr)
        raise
