"""WSL LightGBM prediction boundary for the historical M5A challenger."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from backend.infra.wsl_qlib_runner import win_to_wsl_path
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.model_bundle import (
    LoadedAdvisoryModelBundle,
    load_frozen_research_bundle,
)
from backend.services.advisory_model_first.model_inference import (
    prepare_frozen_feature_matrix,
    score_frozen_feature_matrix_from_booster_outputs,
)
from backend.services.advisory_model_first.meta_label_bundle import (
    format_meta_label_probabilities,
    load_exact_meta_label_runtime_bundle,
    prepare_meta_label_feature_matrix,
)
from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonicalize,
)


WSL_SCORE_REQUEST_SCHEMA = "advisory_historical_wsl_score_request_v1"
WSL_SCORE_RESULT_SCHEMA = "advisory_historical_wsl_score_result_v1"
REASON_WSL_INFERENCE_UNAVAILABLE = (
    "ADVISORY_COMPARISON_MODEL_INFERENCE_ENVIRONMENT_UNAVAILABLE"
)
REASON_WSL_INFERENCE_FAILED = "ADVISORY_COMPARISON_MODEL_INFERENCE_FAILED"
REASON_WSL_OUTPUT_INVALID = "ADVISORY_COMPARISON_MODEL_INFERENCE_OUTPUT_INVALID"


@dataclass(frozen=True)
class DeferredLightgbmBooster:
    """Verified model path whose LightGBM object is intentionally loaded in WSL."""

    path: Path


def load_deferred_frozen_research_bundle(**kwargs: Any) -> LoadedAdvisoryModelBundle:
    """Validate a frozen bundle in the host process without importing LightGBM."""

    return load_frozen_research_bundle(
        **kwargs,
        booster_factory=lambda path: DeferredLightgbmBooster(path=path.resolve()),
    )


def load_deferred_exact_meta_label_runtime_bundle(**kwargs: Any) -> dict[str, Any]:
    """Validate an exact P0-D bundle while deferring LightGBM to WSL."""

    return load_exact_meta_label_runtime_bundle(**kwargs, load_booster=False)


@dataclass(frozen=True)
class WslFrozenFeatureMatrixScorer:
    distro: str | None = None
    conda_sh: str | None = None
    conda_env: str | None = None
    repo_root: Path | str | None = None
    timeout_seconds: int = 600
    runner: Callable[..., Any] | None = None
    path_converter: Callable[[str], str] = win_to_wsl_path

    def __call__(
        self,
        bundle: LoadedAdvisoryModelBundle,
        features: pd.DataFrame,
    ) -> list[dict[str, Any]]:
        matrix = prepare_frozen_feature_matrix(bundle, features)
        model_paths = _deferred_model_paths(bundle)
        serializable = matrix.copy()
        categorical_vocabulary: dict[str, list[int]] = {}
        for column in CATEGORICAL_FEATURE_COLUMNS:
            if not isinstance(matrix[column].dtype, pd.CategoricalDtype):
                raise AdvisoryModelFirstError(
                    "historical WSL matrix lost a categorical feature contract",
                    reason_code=REASON_WSL_OUTPUT_INVALID,
                    context={"feature": column},
                )
            categorical_vocabulary[column] = [
                int(value) for value in matrix[column].cat.categories
            ]
            serializable[column] = matrix[column].astype(object)
        records = canonicalize(
            json.loads(
                serializable.to_json(
                    orient="records", date_format="iso", double_precision=15
                )
            )
        )
        model_relative_paths = [
            str(path.relative_to(bundle.bundle_path.resolve())).replace("\\", "/")
            for path in model_paths
        ]
        model_sha256_by_relative_path = {
            relative: _file_sha256(path)
            for relative, path in zip(model_relative_paths, model_paths)
        }
        request_identity = {
            "bundle_id": bundle.bundle_id,
            "bundle_manifest_sha256": bundle.manifest_file_sha256,
            "feature_names": list(MODEL_FEATURE_COLUMNS),
            "categorical_vocabulary": categorical_vocabulary,
            "matrix_records": records,
            "model_relative_paths": model_relative_paths,
            "model_sha256_by_relative_path": model_sha256_by_relative_path,
        }
        request_hash = canonical_json_sha256(request_identity)
        request = {
            "schema_version": WSL_SCORE_REQUEST_SCHEMA,
            **request_identity,
            "request_hash": request_hash,
            "model_paths": [self.path_converter(str(path)) for path in model_paths],
        }
        repo_root = Path(
            self.repo_root or Path(__file__).resolve().parents[3]
        ).resolve()
        helper = repo_root / "scripts" / "wsl" / "advisory_historical_model_predict.py"
        if not helper.is_file():
            raise AdvisoryModelFirstError(
                "historical WSL inference helper is missing",
                reason_code=REASON_WSL_INFERENCE_UNAVAILABLE,
                context={"helper": str(helper)},
            )
        with tempfile.TemporaryDirectory(
            prefix="advisory_historical_wsl_score_"
        ) as temporary_root:
            request_path = Path(temporary_root) / "request.json"
            output_path = Path(temporary_root) / "result.json"
            request_path.write_text(
                json.dumps(
                    request, ensure_ascii=True, sort_keys=True, separators=(",", ":")
                ),
                encoding="utf-8",
            )
            command = self._command(
                helper_path=self.path_converter(str(helper)),
                request_path=self.path_converter(str(request_path)),
                output_path=self.path_converter(str(output_path)),
            )
            completed = (
                self.runner(
                    command=command,
                    request_path=request_path,
                    output_path=output_path,
                    request=request,
                )
                if self.runner is not None
                else subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=self.timeout_seconds,
                    check=False,
                )
            )
            if int(completed.returncode) != 0:
                raise AdvisoryModelFirstError(
                    "historical LightGBM inference failed in WSL",
                    reason_code=REASON_WSL_INFERENCE_FAILED,
                    context={
                        "returncode": int(completed.returncode),
                        "stdout_tail": str(completed.stdout)[-4000:],
                        "stderr_tail": str(completed.stderr)[-4000:],
                    },
                )
            if not output_path.is_file():
                raise AdvisoryModelFirstError(
                    "historical WSL inference did not publish a result",
                    reason_code=REASON_WSL_OUTPUT_INVALID,
                )
            try:
                result = json.loads(output_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise AdvisoryModelFirstError(
                    "historical WSL inference result is unreadable",
                    reason_code=REASON_WSL_OUTPUT_INVALID,
                    context={"error_type": type(exc).__name__},
                ) from exc
        if (
            not isinstance(result, dict)
            or result.get("schema_version") != WSL_SCORE_RESULT_SCHEMA
            or result.get("request_hash") != request_hash
            or result.get("bundle_id") != bundle.bundle_id
        ):
            raise AdvisoryModelFirstError(
                "historical WSL inference result identity differs from the request",
                reason_code=REASON_WSL_OUTPUT_INVALID,
            )
        try:
            raw_scores = list(result["raw_scores"])
            raw_contributions = list(result["raw_contributions"])
            booster_feature_names = [
                tuple(row) for row in result["booster_feature_names"]
            ]
        except (KeyError, TypeError) as exc:
            raise AdvisoryModelFirstError(
                "historical WSL inference result payload is incomplete",
                reason_code=REASON_WSL_OUTPUT_INVALID,
            ) from exc
        return score_frozen_feature_matrix_from_booster_outputs(
            bundle,
            features,
            raw_scores=raw_scores,
            raw_contributions=raw_contributions,
            booster_feature_names=booster_feature_names,
        )

    def _command(
        self, *, helper_path: str, request_path: str, output_path: str
    ) -> list[str]:
        distro = self.distro or os.getenv("QLIB_WSL_DISTRO") or "Ubuntu"
        conda_sh = (
            self.conda_sh
            or os.getenv("QLIB_WSL_CONDA_SH")
            or "~/miniconda3/etc/profile.d/conda.sh"
        )
        conda_env = self.conda_env or os.getenv("QLIB_WSL_CONDA_ENV") or "rdagent-gpu"
        shell_command = (
            f"source {_quote_shell_path(conda_sh)} && "
            f"conda activate {shlex.quote(conda_env)} && "
            f"python {shlex.quote(helper_path)} --request {shlex.quote(request_path)} "
            f"--output {shlex.quote(output_path)}"
        )
        return ["wsl", "-d", distro, "bash", "-lc", shell_command]


@dataclass(frozen=True)
class WslMetaLabelFeatureMatrixScorer:
    """Score the exact P0-D model in the same WSL LightGBM environment."""

    distro: str | None = None
    conda_sh: str | None = None
    conda_env: str | None = None
    repo_root: Path | str | None = None
    timeout_seconds: int = 600
    runner: Callable[..., Any] | None = None
    path_converter: Callable[[str], str] = win_to_wsl_path

    def __call__(self, bundle: dict[str, Any], features: pd.DataFrame) -> pd.DataFrame:
        matrix = prepare_meta_label_feature_matrix(bundle, features)
        schema = bundle.get("feature_schema") or {}
        model_path = (Path(bundle["bundle_path"]) / "model.txt").resolve()
        bundle_root = Path(bundle["bundle_path"]).resolve()
        if not model_path.is_file() or not model_path.is_relative_to(bundle_root):
            raise AdvisoryModelFirstError(
                "historical P0-D model is outside the exact bundle",
                reason_code=REASON_WSL_OUTPUT_INVALID,
            )
        serializable = matrix.copy()
        categorical_vocabulary: dict[str, list[int]] = {}
        for column in CATEGORICAL_FEATURE_COLUMNS:
            if column not in matrix or not isinstance(
                matrix[column].dtype, pd.CategoricalDtype
            ):
                raise AdvisoryModelFirstError(
                    "historical P0-D WSL matrix lost a categorical feature contract",
                    reason_code=REASON_WSL_OUTPUT_INVALID,
                    context={"feature": column},
                )
            categorical_vocabulary[column] = [
                int(value) for value in matrix[column].cat.categories
            ]
            serializable[column] = matrix[column].astype(object)
        records = canonicalize(
            json.loads(
                serializable.to_json(
                    orient="records", date_format="iso", double_precision=15
                )
            )
        )
        relative = str(model_path.relative_to(bundle_root)).replace("\\", "/")
        feature_names = list(schema.get("trained_feature_names") or ())
        request_identity = {
            "bundle_id": str(bundle["manifest"]["bundle_id"]),
            "bundle_manifest_sha256": str(bundle["manifest_file_sha256"]),
            "feature_names": feature_names,
            "categorical_vocabulary": categorical_vocabulary,
            "matrix_records": records,
            "model_relative_paths": [relative],
            "model_sha256_by_relative_path": {relative: _file_sha256(model_path)},
        }
        request_hash = canonical_json_sha256(request_identity)
        request = {
            "schema_version": WSL_SCORE_REQUEST_SCHEMA,
            **request_identity,
            "request_hash": request_hash,
            "model_paths": [self.path_converter(str(model_path))],
        }
        repo_root = Path(
            self.repo_root or Path(__file__).resolve().parents[3]
        ).resolve()
        helper = repo_root / "scripts" / "wsl" / "advisory_historical_model_predict.py"
        if not helper.is_file():
            raise AdvisoryModelFirstError(
                "historical P0-D WSL inference helper is missing",
                reason_code=REASON_WSL_INFERENCE_UNAVAILABLE,
                context={"helper": str(helper)},
            )
        with tempfile.TemporaryDirectory(
            prefix="advisory_p0d_historical_wsl_score_"
        ) as temporary_root:
            request_path = Path(temporary_root) / "request.json"
            output_path = Path(temporary_root) / "result.json"
            request_path.write_text(
                json.dumps(
                    request, ensure_ascii=True, sort_keys=True, separators=(",", ":")
                ),
                encoding="utf-8",
            )
            command = self._command(
                helper_path=self.path_converter(str(helper)),
                request_path=self.path_converter(str(request_path)),
                output_path=self.path_converter(str(output_path)),
            )
            completed = (
                self.runner(
                    command=command,
                    request_path=request_path,
                    output_path=output_path,
                    request=request,
                )
                if self.runner is not None
                else subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=self.timeout_seconds,
                    check=False,
                )
            )
            if int(completed.returncode) != 0:
                raise AdvisoryModelFirstError(
                    "historical P0-D LightGBM inference failed in WSL",
                    reason_code=REASON_WSL_INFERENCE_FAILED,
                    context={
                        "returncode": int(completed.returncode),
                        "stdout_tail": str(completed.stdout)[-4000:],
                        "stderr_tail": str(completed.stderr)[-4000:],
                    },
                )
            if not output_path.is_file():
                raise AdvisoryModelFirstError(
                    "historical P0-D WSL inference did not publish a result",
                    reason_code=REASON_WSL_OUTPUT_INVALID,
                )
            try:
                result = json.loads(output_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise AdvisoryModelFirstError(
                    "historical P0-D WSL result is unreadable",
                    reason_code=REASON_WSL_OUTPUT_INVALID,
                    context={"error_type": type(exc).__name__},
                ) from exc
        if (
            not isinstance(result, dict)
            or result.get("schema_version") != WSL_SCORE_RESULT_SCHEMA
            or result.get("request_hash") != request_hash
            or result.get("bundle_id") != request_identity["bundle_id"]
        ):
            raise AdvisoryModelFirstError(
                "historical P0-D WSL result identity differs from the request",
                reason_code=REASON_WSL_OUTPUT_INVALID,
            )
        try:
            raw_scores = result["raw_scores"]
            probability = (
                raw_scores[0]
                if isinstance(raw_scores, list) and len(raw_scores) == 1
                else None
            )
        except (KeyError, TypeError) as exc:
            raise AdvisoryModelFirstError(
                "historical P0-D WSL result payload is incomplete",
                reason_code=REASON_WSL_OUTPUT_INVALID,
            ) from exc
        if probability is None:
            raise AdvisoryModelFirstError(
                "historical P0-D WSL result has no exact probability vector",
                reason_code=REASON_WSL_OUTPUT_INVALID,
            )
        return format_meta_label_probabilities(features, probability)

    def _command(
        self, *, helper_path: str, request_path: str, output_path: str
    ) -> list[str]:
        distro = self.distro or os.getenv("QLIB_WSL_DISTRO") or "Ubuntu"
        conda_sh = (
            self.conda_sh
            or os.getenv("QLIB_WSL_CONDA_SH")
            or "~/miniconda3/etc/profile.d/conda.sh"
        )
        conda_env = self.conda_env or os.getenv("QLIB_WSL_CONDA_ENV") or "rdagent-gpu"
        shell_command = (
            f"source {_quote_shell_path(conda_sh)} && "
            f"conda activate {shlex.quote(conda_env)} && "
            f"python {shlex.quote(helper_path)} --request {shlex.quote(request_path)} "
            f"--output {shlex.quote(output_path)}"
        )
        return ["wsl", "-d", distro, "bash", "-lc", shell_command]


def _deferred_model_paths(bundle: LoadedAdvisoryModelBundle) -> tuple[Path, ...]:
    schema_version = bundle.manifest.get("schema_version", "advisory_model_bundle_v1")
    members = (
        (bundle.booster,)
        if schema_version == "advisory_model_bundle_v1"
        else bundle.boosters
    )
    if not members or any(
        not isinstance(member, DeferredLightgbmBooster) for member in members
    ):
        raise AdvisoryModelFirstError(
            "historical WSL scorer requires deferred frozen model members",
            reason_code=REASON_WSL_OUTPUT_INVALID,
        )
    bundle_root = bundle.bundle_path.resolve()
    paths: list[Path] = []
    for member in members:
        path = member.path.resolve()
        if not path.is_file() or not path.is_relative_to(bundle_root):
            raise AdvisoryModelFirstError(
                "historical WSL model member is outside the frozen bundle",
                reason_code=REASON_WSL_OUTPUT_INVALID,
                context={"path": str(path)},
            )
        paths.append(path)
    return tuple(paths)


def _quote_shell_path(value: str) -> str:
    if value.startswith("~/"):
        return '"$HOME"/' + shlex.quote(value[2:])
    return shlex.quote(value)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
