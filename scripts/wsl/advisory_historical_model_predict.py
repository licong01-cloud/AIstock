#!/usr/bin/env python3
"""Load frozen LightGBM members in WSL and emit raw prediction tensors only."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd


REQUEST_SCHEMA = "advisory_historical_wsl_score_request_v1"
RESULT_SCHEMA = "advisory_historical_wsl_score_result_v1"
IDENTITY_KEYS = (
    "bundle_id",
    "bundle_manifest_sha256",
    "feature_names",
    "categorical_vocabulary",
    "matrix_records",
    "model_relative_paths",
    "model_sha256_by_relative_path",
)


def validate_request_identity(request: dict[str, object]) -> str:
    identity = {key: request.get(key) for key in IDENTITY_KEYS}
    canonical = json.dumps(
        identity,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    expected_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if request.get("request_hash") != expected_hash:
        raise ValueError("historical WSL score request hash is invalid")
    model_paths = request.get("model_paths")
    relative_paths = request.get("model_relative_paths")
    model_hashes = request.get("model_sha256_by_relative_path")
    if (
        not isinstance(model_paths, list)
        or not isinstance(relative_paths, list)
        or not isinstance(model_hashes, dict)
        or len(model_paths) != len(relative_paths)
        or set(model_hashes) != set(relative_paths)
    ):
        raise ValueError("historical WSL model path identity is incomplete")
    for raw_path, raw_relative in zip(model_paths, relative_paths):
        actual = Path(str(raw_path)).as_posix()
        relative = Path(str(raw_relative)).as_posix()
        if (
            not relative
            or relative.startswith("/")
            or ".." in Path(relative).parts
            or not actual.endswith(f"/{relative}")
        ):
            raise ValueError("historical WSL model path differs from request identity")
        digest = hashlib.sha256()
        with Path(str(raw_path)).open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest() != model_hashes[raw_relative]:
            raise ValueError("historical WSL model bytes differ from request identity")
    return expected_hash


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    request = json.loads(Path(args.request).read_text(encoding="utf-8"))
    if request.get("schema_version") != REQUEST_SCHEMA:
        raise ValueError("historical WSL score request schema is invalid")
    request_hash = validate_request_identity(request)
    feature_names = request.get("feature_names")
    records = request.get("matrix_records")
    model_paths = request.get("model_paths")
    vocabulary = request.get("categorical_vocabulary")
    if (
        not isinstance(feature_names, list)
        or not feature_names
        or not isinstance(records, list)
        or not records
        or not isinstance(model_paths, list)
        or not model_paths
        or not isinstance(vocabulary, dict)
    ):
        raise ValueError("historical WSL score request payload is incomplete")
    matrix = pd.DataFrame.from_records(records, columns=feature_names)
    for column, categories in vocabulary.items():
        if column not in matrix.columns or not isinstance(categories, list) or not categories:
            raise ValueError("historical WSL categorical vocabulary is invalid")
        matrix[column] = pd.Categorical(matrix[column], categories=categories)
    for column in matrix.columns:
        if column in vocabulary:
            continue
        matrix[column] = pd.to_numeric(matrix[column], errors="raise").astype(float)
    import lightgbm as lgb

    raw_scores = []
    raw_contributions = []
    booster_feature_names = []
    for model_path in model_paths:
        path = Path(str(model_path)).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"frozen LightGBM member is missing: {path}")
        booster = lgb.Booster(model_file=str(path))
        names = list(booster.feature_name())
        if names != feature_names:
            raise ValueError("LightGBM feature order differs from the frozen request")
        scores = np.asarray(booster.predict(matrix), dtype=float)
        contributions = np.asarray(booster.predict(matrix, pred_contrib=True), dtype=float)
        if scores.shape != (len(matrix),) or contributions.shape != (len(matrix), len(feature_names) + 1):
            raise ValueError("LightGBM output dimensions are invalid")
        if not np.isfinite(scores).all() or not np.isfinite(contributions).all():
            raise ValueError("LightGBM output contains non-finite values")
        raw_scores.append(scores.tolist())
        raw_contributions.append(contributions.tolist())
        booster_feature_names.append(names)
    result = {
        "schema_version": RESULT_SCHEMA,
        "request_hash": request_hash,
        "bundle_id": request.get("bundle_id"),
        "raw_scores": raw_scores,
        "raw_contributions": raw_contributions,
        "booster_feature_names": booster_feature_names,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{output.name}.", suffix=".tmp", dir=output.parent)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        temporary.write_text(
            json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False),
            encoding="utf-8",
        )
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
