"""StrategyPackage protected asset ledger models."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrategyPackageAssetType(str, Enum):
    MODEL_WEIGHT = "model_weight"
    FACTOR_CODE = "factor_code"
    FACTOR_SCHEMA = "factor_schema"
    MODEL_CODE = "model_code"
    FEATURE_ORDER = "feature_order"
    TRAIN_CONFIG = "train_config"
    PREPROCESSOR = "preprocessor"
    PREDICTION_SCHEMA = "prediction_schema"
    EXECUTION_CONFIG = "execution_config"
    RISK_POLICY = "risk_policy"
    VALIDATION_REPORT = "validation_report"
    OTHER = "other"


class StrategyPackageAssetRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    asset_id: int | None = None
    package_id: str
    asset_type: StrategyPackageAssetType
    asset_ref: str
    asset_sha256: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    asset_role: str = "governed_asset"
    asset_size_bytes: int | None = Field(default=None, ge=0)
    protected_asset: bool = True
    source_uri: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("package_id", "asset_ref", "asset_role")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("asset ledger text fields cannot be empty")
        return value
