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
    dataset_path = model_path.parent / "dataset"
    if dataset_path.is_file():
        # Model loading has installed the admitted package's local code search paths.
        dataset = inference_engine._load_admitted_strategy_package_pickle(dataset_path)
        processors = list(getattr(getattr(dataset, "handler", None), "infer_processors", []) or [])
        if not processors:
            raise ValueError("frozen fitted dataset declares no inference processors")
        columns = inference_engine._saved_qe_feature_order(processors)
        if columns:
            import pandas as pd
            probe_frame = pd.DataFrame(0.0, index=range(2), columns=columns)
            processed = inference_engine._apply_saved_qe_infer_processors(
                probe_frame, task_dir=model_path.parent,
                primary_assets={"dataset_processor_relpath": "dataset"},
            )
            if processed.shape != probe_frame.shape or list(processed.columns) != columns:
                raise ValueError("frozen fitted preprocessor changed the admitted feature schema")
            payload["fitted_feature_order"] = columns
        payload["fitted_preprocessor_count"] = len(processors)
        payload["sequence_length"] = inference_engine._saved_qe_step_len(
            model_path.parent, {"dataset_processor_relpath": "dataset"},
        )
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
