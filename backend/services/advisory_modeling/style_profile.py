from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_modeling.identity import (
    FrozenModel,
    set_computed_hash,
    strict_identifier,
    validated_hash,
)


STYLE_PROFILE_SCHEMA_VERSION = "advisory_strategy_style_profile_v1"
SHORT_REBOUND_HORIZONS = (1, 3, 5, 10, 20)
SHORT_REBOUND_TARGET_PACKAGE_ID = "pkg_ma_8ec5e389fa2c5e484a1ac7e9"


class StrategyStyleProfileV1(FrozenModel):
    schema_version: Literal[STYLE_PROFILE_SCHEMA_VERSION] = STYLE_PROFILE_SCHEMA_VERSION
    profile_id: str = Field(min_length=1, max_length=160)
    profile_version: str = Field(min_length=1, max_length=80)
    style_family: Literal["SHORT_REBOUND"] = "SHORT_REBOUND"
    primary_horizon_trading_days: Literal[5] = 5
    supported_horizons: tuple[int, ...] = SHORT_REBOUND_HORIZONS
    signal_decay_prior: Literal["FAST"] = "FAST"
    label_objective: Literal["RISK_AWARE_NET_RETURN_RANKING"] = (
        "RISK_AWARE_NET_RETURN_RANKING"
    )
    candidate_observation_top_k: Literal[20] = 20
    shortlist_top_n: Literal[5] = 5
    package_id: str = Field(min_length=1, max_length=160)
    package_manifest_sha256: str = Field(min_length=64, max_length=64)
    package_asset_closure_hash: str = Field(min_length=64, max_length=64)
    selection_runtime_semantics_hash: str = Field(min_length=64, max_length=64)
    effective_package_oos_cutoff: date
    profile_payload_sha256: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("profile_id", "profile_version", "package_id")
    @classmethod
    def _identifiers(cls, value: str, info: Any) -> str:
        return strict_identifier(value, field_name=info.field_name)

    @field_validator(
        "package_manifest_sha256",
        "package_asset_closure_hash",
        "selection_runtime_semantics_hash",
        "profile_payload_sha256",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @field_validator("supported_horizons")
    @classmethod
    def _horizons(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if value != SHORT_REBOUND_HORIZONS:
            raise ValueError(f"supported_horizons must equal {SHORT_REBOUND_HORIZONS}")
        return value

    @model_validator(mode="after")
    def _identity(self) -> "StrategyStyleProfileV1":
        set_computed_hash(
            self,
            field_name="profile_payload_sha256",
            exclude={"profile_payload_sha256"},
        )
        return self
