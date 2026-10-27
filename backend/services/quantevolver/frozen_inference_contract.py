"""Explicit, hash-pinned opt-in for original fitted QE inference pipelines."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator

SHA = r"^[0-9a-f]{64}$"
FIELD = "frozen_inference"


def canonical_sha(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


class FrozenInferenceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["qe_frozen_inference_v1"]
    model_sha256: str = Field(pattern=SHA)
    dataset_sha256: str = Field(pattern=SHA)
    reference_prediction_sha256: str = Field(pattern=SHA)
    source_training_manifest_sha256: str = Field(pattern=SHA)
    input_manifest_sha256: str = Field(pattern=SHA)
    source_model_config_sha256: str = Field(pattern=SHA)
    code_sha256: dict[str, str]
    model_module_sha256: dict[str, str]
    feature_names: list[str]
    excluded_reference_columns: list[str]
    normalizer_fit_start: str
    normalizer_fit_end: str
    dataset_class: Literal["DatasetH", "TSDatasetH"]
    step_len: StrictInt = Field(ge=1)

    @model_validator(mode="after")
    def _validate_identity(self) -> "FrozenInferenceSpec":
        import re
        from datetime import date

        if not self.feature_names or len(set(self.feature_names)) != len(self.feature_names):
            raise ValueError("QE_FROZEN_FEATURE_SCHEMA_INVALID")
        if not all(isinstance(name, str) and name.strip() == name and name for name in self.feature_names):
            raise ValueError("QE_FROZEN_FEATURE_SCHEMA_INVALID")
        if set(self.feature_names) & set(self.excluded_reference_columns):
            raise ValueError("QE_FROZEN_REFERENCE_SCHEMA_OVERLAP")
        if len(set(self.excluded_reference_columns)) != len(self.excluded_reference_columns):
            raise ValueError("QE_FROZEN_REFERENCE_SCHEMA_INVALID")
        for name, digest in self.code_sha256.items():
            if (name.startswith(("/", "\\")) or "\\" in name or ":" in name
                    or any(part in {"", ".", ".."} for part in name.split("/"))
                    or not name.endswith(".py") or not re.fullmatch(SHA, digest)):
                raise ValueError("QE_FROZEN_CODE_PIN_INVALID")
        if "qe_custom_loaders.py" not in self.code_sha256:
            raise ValueError("QE_FROZEN_LOADER_PIN_REQUIRED")
        if not self.model_module_sha256 or any(
            not name.startswith("qlib.contrib.model.") or ".." in name or not re.fullmatch(r"[A-Za-z0-9_.]+", name)
            or not re.fullmatch(SHA, digest) for name, digest in self.model_module_sha256.items()
        ):
            raise ValueError("QE_FROZEN_MODEL_MODULE_PIN_REQUIRED")
        if self.dataset_class == "TSDatasetH" and self.step_len != 20:
            raise ValueError("QE_FROZEN_TEMPORAL_CONTRACT_INVALID")
        if self.dataset_class == "DatasetH" and self.step_len != 1:
            raise ValueError("QE_FROZEN_TEMPORAL_CONTRACT_INVALID")
        if date.fromisoformat(self.normalizer_fit_start) > date.fromisoformat(self.normalizer_fit_end):
            raise ValueError("QE_FROZEN_FIT_WINDOW_INVALID")
        return self


def validate_mode(value: Mapping[str, Any], *, allow_extra_params: bool = False) -> FrozenInferenceSpec | None:
    """Reject hidden opt-ins even when the public field is absent."""
    containers = ["model_params", "strategy_params", "runtime_flags", "model_params_base"]
    if allow_extra_params:
        containers.append("extra_params")
    def hidden(item):
        if isinstance(item, Mapping):
            return FIELD in item or any(hidden(child) for child in item.values())
        return isinstance(item, (list, tuple)) and any(hidden(child) for child in item)
    for field in containers:
        if hidden(value.get(field)):
            raise ValueError("QE_FROZEN_OPT_IN_MUST_BE_PUBLIC")
    raw = value.get(FIELD)
    if raw is None:
        return None
    spec = raw if isinstance(raw, FrozenInferenceSpec) else FrozenInferenceSpec.model_validate(raw)
    if (not value.get("backtest_only") or value.get("prediction_replay")
            or value.get("alpha_mode", "single") != "single"
            or not value.get("model_source_task_id") or type(value.get("model_source_loop_index")) is not int
            or value["model_source_loop_index"] < 1 or value.get("feature_processing") is not None
            or (value.get("extra_params") or {}).get("feature_processing") is not None):
        raise ValueError("QE_FROZEN_MODE_INVALID")
    return spec


async def source_assets(client, task_id: str, loop: str, spec: FrozenInferenceSpec, model_tar: bytes) -> dict[str, str]:
    """Read pinned original bytes only; never deserialize on the API process."""
    import base64
    import io
    import tarfile
    from pathlib import PurePosixPath

    with tarfile.open(fileobj=io.BytesIO(model_tar), mode="r:gz") as archive:
        params = [m for m in archive.getmembers() if m.isfile() and PurePosixPath(m.name).name == "params.pkl"]
        if len(params) != 1 or params[0].size > 512 * 1024 * 1024:
            raise ValueError("QE_FROZEN_MODEL_CARDINALITY_OR_SIZE")
        if hashlib.sha256(archive.extractfile(params[0]).read()).hexdigest() != spec.model_sha256:
            raise ValueError("QE_FROZEN_MODEL_SHA_MISMATCH")
    catalog = await client.list_workspace_files(task_id, loop)
    rows = catalog.get("files", catalog.get("assets"))
    if catalog.get("catalog_completeness") != "complete" or not isinstance(rows, list):
        raise ValueError("QE_FROZEN_SOURCE_CATALOG_INCOMPLETE")

    async def fetch(name: str, expected: str | None = None, *, artifact: bool = False) -> bytes:
        matches = []
        for row in rows:
            path = str(row.get("relative_path") or row.get("path") or row.get("filename") or "").replace("\\", "/")
            path = path.removeprefix("./")
            if path == name or (artifact and path.endswith("/artifacts/" + name)) or (artifact and path == "artifacts/" + name):
                matches.append((path, row))
        if len(matches) != 1:
            raise ValueError(f"QE_FROZEN_ASSET_CARDINALITY: {name}")
        path, row = matches[0]
        if PurePosixPath(path).is_absolute() or ".." in PurePosixPath(path).parts or row.get("is_symlink"):
            raise ValueError("QE_FROZEN_ASSET_PATH_INVALID")
        size = int(row.get("size_bytes", 0))
        if not 0 < size <= 128 * 1024 * 1024:
            raise ValueError(f"QE_FROZEN_ASSET_SIZE_INVALID: {name}")
        data = await client.download_workspace_file_bytes(task_id, loop, path)
        if len(data) != size or (expected and hashlib.sha256(data).hexdigest() != expected):
            raise ValueError(f"QE_FROZEN_ASSET_SHA_OR_SIZE_MISMATCH: {name}")
        return data

    dataset = await fetch("dataset", spec.dataset_sha256, artifact=True)
    prediction = await fetch("pred.pkl", spec.reference_prediction_sha256, artifact=True)
    for path, digest in spec.code_sha256.items():
        await fetch(path, digest)
    import yaml
    from jinja2 import Environment, StrictUndefined
    rendered = Environment(undefined=StrictUndefined).from_string((await fetch("conf.yaml")).decode("utf-8")).render(
        num_features=len(spec.feature_names), num_timesteps=spec.step_len, step_len=spec.step_len,
    )
    conf = yaml.safe_load(rendered)
    if canonical_sha(conf["task"]["model"]) != spec.source_model_config_sha256:
        raise ValueError("QE_FROZEN_SOURCE_MODEL_CONFIG_MISMATCH")
    modules = {conf["task"]["model"]["module_path"]}
    uri = conf["task"]["model"].get("kwargs", {}).get("pt_model_uri")
    if uri:
        modules.add(uri.rsplit(".", 1)[0])
    if set(spec.model_module_sha256) != modules:
        raise ValueError("QE_FROZEN_MODEL_MODULE_SET_MISMATCH")
    return {
        "qe_frozen_source_assets.json": json.dumps({
            "schema_version": "qe_frozen_source_assets_v1",
            "dataset_b64": base64.b64encode(dataset).decode("ascii"),
            "reference_prediction_b64": base64.b64encode(prediction).decode("ascii"),
            "source_model_config": conf["task"]["model"],
            "source_test_segment": [str(t) for t in conf["task"]["dataset"]["kwargs"]["segments"]["test"]],
        }, ensure_ascii=False),
    }
