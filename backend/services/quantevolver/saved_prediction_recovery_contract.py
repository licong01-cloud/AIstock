"""QE-owned explicit identity for recovering saved interrupted-run predictions."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator


class SavedPredictionRecovery(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["qe_saved_prediction_recovery_v1"]
    source_status: Literal["failed", "cancelled"]
    experiment_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,256}$")
    recorder_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,256}$")
    source_seed: StrictInt
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    profile_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_sha256: dict[str, str]
    processor_fingerprint: Literal["UNKNOWN"] = "UNKNOWN"

    @property
    def artifact_root(self) -> str:
        return f"mlruns/{self.experiment_id}/{self.recorder_id}/artifacts/"

    @model_validator(mode="after")
    def _pins(self) -> SavedPredictionRecovery:
        import re

        required = {
            "config.json", "conf.yaml", "model.py", "qe_direct_v2_dataset_binding.json",
            *(self.artifact_root + name for name in ("params.pkl", "pred.pkl", "label.pkl")),
        }
        if set(self.file_sha256) != required or any(
            not re.fullmatch(r"[0-9a-f]{64}", value) for value in self.file_sha256.values()
        ):
            raise ValueError("QE_SAVED_PREDICTION_RECOVERY_EXACT_PINS_REQUIRED")
        return self


def validate_recovery_mode(values: dict) -> None:
    spec = values.get("prediction_recovery")
    if spec is None:
        return
    spec = SavedPredictionRecovery.model_validate(spec)
    if values.get("prediction_replay") is not True or values.get("backtest_only"):
        raise ValueError("QE_SAVED_PREDICTION_RECOVERY_REQUIRES_PURE_REPLAY")
    if values.get("prediction_source_sha256") != spec.file_sha256[spec.artifact_root + "pred.pkl"]:
        raise ValueError("QE_SAVED_PREDICTION_RECOVERY_PREDICTION_PIN_CONFLICT")
