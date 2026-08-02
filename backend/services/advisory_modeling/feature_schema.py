from __future__ import annotations

import base64
import binascii
import hashlib
from datetime import datetime
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_modeling.identity import (
    FrozenModel,
    ensure_unique,
    set_computed_hash,
    strict_identifier,
    utc_datetime,
    validated_hash,
)


FEATURE_DEFINITION_SCHEMA_VERSION = "advisory_reranker_feature_definition_v1"
FEATURE_SCHEMA_VERSION = "advisory_reranker_feature_schema_v1"
FORMULA_REGISTRY_SCHEMA_VERSION = "advisory_reranker_feature_formula_registry_v1"
QUERY_REGISTRY_SCHEMA_VERSION = "advisory_reranker_feature_query_registry_v1"
FEATURE_ROW_IDENTITY_SCHEMA_VERSION = "advisory_reranker_feature_row_identity_v1"

REQUIRED_QUERY_TEMPLATE_IDS = (
    "historical_pit_universe_existing_readonly",
    "historical_trading_calendar_window",
    "historical_market_history_window",
    "historical_decision_mark_daily_market",
    "historical_decision_mark_market_state",
    "historical_fundamental_moneyflow_window",
    "historical_suspend_lookup",
    "historical_industry_membership",
)

REQUIRED_FORMULA_IDS = (
    "candidate_rank_percentile_v1",
    "candidate_score_gap_v1",
    "multi_alpha_consensus_v1",
    "adjusted_return_v1",
    "realized_volatility_v1",
    "distance_to_extreme_v1",
    "liquidity_state_v1",
    "moneyflow_state_v1",
    "industry_context_v1",
    "market_context_v1",
    "candidate_group_context_v1",
)


class FeatureDefinitionV1(FrozenModel):
    schema_version: Literal[FEATURE_DEFINITION_SCHEMA_VERSION] = FEATURE_DEFINITION_SCHEMA_VERSION
    name: str = Field(min_length=1, max_length=160)
    dtype: Literal["float64", "int64", "bool", "string", "sha256"]
    unit: str = Field(min_length=1, max_length=80)
    availability_cutoff: Literal["DECISION_CUTOFF"] = "DECISION_CUTOFF"
    source_role: str = Field(min_length=1, max_length=120)
    query_template_id: str | None = Field(default=None, min_length=1, max_length=160)
    formula_id: str = Field(min_length=1, max_length=160)
    formula_version: str = Field(min_length=1, max_length=80)
    missing_policy: Literal[
        "REQUIRED_FAIL_GROUP",
        "NULL_WITH_INDICATOR",
        "ZERO_IS_OBSERVED_FACT",
        "NOT_APPLICABLE_WITH_FLAG",
    ]
    feature_definition_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "name", "unit", "source_role", "query_template_id", "formula_id", "formula_version"
    )
    @classmethod
    def _identifiers(cls, value: str | None, info: Any) -> str | None:
        return strict_identifier(value, field_name=info.field_name) if value is not None else None

    @field_validator("feature_definition_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return validated_hash(value, field_name="feature_definition_hash")

    @model_validator(mode="after")
    def _identity(self) -> "FeatureDefinitionV1":
        set_computed_hash(
            self,
            field_name="feature_definition_hash",
            exclude={"feature_definition_hash"},
        )
        return self


class FeatureFormulaDefinitionV1(FrozenModel):
    formula_id: str = Field(min_length=1, max_length=160)
    formula_version: str = Field(min_length=1, max_length=80)
    expression: str = Field(min_length=1)
    input_roles: tuple[str, ...]
    parameters: dict[str, Any]
    pit_constraints: tuple[str, ...]
    missing_behavior: str = Field(min_length=1)
    formula_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("formula_id", "formula_version", "expression", "missing_behavior")
    @classmethod
    def _strings(cls, value: str, info: Any) -> str:
        return strict_identifier(value, field_name=info.field_name)

    @field_validator("input_roles", "pit_constraints")
    @classmethod
    def _tuples(cls, value: tuple[str, ...], info: Any) -> tuple[str, ...]:
        if not value:
            raise ValueError(f"{info.field_name} must not be empty")
        normalized = tuple(strict_identifier(item, field_name=info.field_name) for item in value)
        return ensure_unique(normalized, field_name=info.field_name)

    @field_validator("formula_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return validated_hash(value, field_name="formula_hash")

    @model_validator(mode="after")
    def _identity(self) -> "FeatureFormulaDefinitionV1":
        set_computed_hash(self, field_name="formula_hash", exclude={"formula_hash"})
        return self


class FeatureFormulaRegistryV1(FrozenModel):
    schema_version: Literal[FORMULA_REGISTRY_SCHEMA_VERSION] = FORMULA_REGISTRY_SCHEMA_VERSION
    formulas: tuple[FeatureFormulaDefinitionV1, ...]
    registry_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _identity(self) -> "FeatureFormulaRegistryV1":
        ids = tuple(item.formula_id for item in self.formulas)
        if ids != REQUIRED_FORMULA_IDS:
            raise ValueError("formula registry must contain the frozen v1 formulas in canonical order")
        set_computed_hash(self, field_name="registry_hash", exclude={"registry_hash"})
        return self


class FeatureQueryTemplateV1(FrozenModel):
    query_template_id: str = Field(min_length=1, max_length=160)
    template_version: str = Field(min_length=1, max_length=80)
    sql_bytes_base64: str = Field(min_length=1)
    sql_bytes_sha256: str = Field(min_length=64, max_length=64)
    parameter_schema: dict[str, str]
    result_schema: tuple[tuple[str, str], ...]
    repository_commit: str = Field(min_length=7, max_length=64)
    template_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("query_template_id", "template_version", "repository_commit")
    @classmethod
    def _strings(cls, value: str, info: Any) -> str:
        return strict_identifier(value, field_name=info.field_name)

    @field_validator("sql_bytes_sha256")
    @classmethod
    def _sql_hash(cls, value: str) -> str:
        return str(validated_hash(value, field_name="sql_bytes_sha256"))

    @model_validator(mode="after")
    def _identity(self) -> "FeatureQueryTemplateV1":
        try:
            sql_bytes = base64.b64decode(self.sql_bytes_base64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("sql_bytes_base64 must contain canonical base64") from exc
        if not sql_bytes:
            raise ValueError("query SQL bytes must not be empty")
        try:
            sql_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("query SQL bytes must be valid UTF-8") from exc
        if base64.b64encode(sql_bytes).decode("ascii") != self.sql_bytes_base64:
            raise ValueError("sql_bytes_base64 must use canonical encoding")
        if hashlib.sha256(sql_bytes).hexdigest() != self.sql_bytes_sha256:
            raise ValueError("sql_bytes_sha256 differs from decoded SQL bytes")
        if not self.parameter_schema or not self.result_schema:
            raise ValueError("query templates require explicit parameter and result schemas")
        if len({name for name, _dtype in self.result_schema}) != len(self.result_schema):
            raise ValueError("query result schema contains duplicate columns")
        set_computed_hash(self, field_name="template_hash", exclude={"template_hash"})
        return self


class FrozenFeatureQueryRegistryV1(FrozenModel):
    schema_version: Literal[QUERY_REGISTRY_SCHEMA_VERSION] = QUERY_REGISTRY_SCHEMA_VERSION
    templates: tuple[FeatureQueryTemplateV1, ...]
    source_repository_commit: str = Field(min_length=7, max_length=64)
    registry_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _identity(self) -> "FrozenFeatureQueryRegistryV1":
        ids = tuple(template.query_template_id for template in self.templates)
        if ids != REQUIRED_QUERY_TEMPLATE_IDS:
            raise ValueError("query registry must contain the frozen v1 templates in canonical order")
        if any(template.repository_commit != self.source_repository_commit for template in self.templates):
            raise ValueError("all query templates must come from source_repository_commit")
        set_computed_hash(self, field_name="registry_hash", exclude={"registry_hash"})
        return self


class FeatureSchemaV1(FrozenModel):
    schema_version: Literal[FEATURE_SCHEMA_VERSION] = FEATURE_SCHEMA_VERSION
    feature_schema_id: str = Field(min_length=1, max_length=160)
    definitions: tuple[FeatureDefinitionV1, ...]
    required_identity_features: tuple[str, ...]
    required_rank_features: tuple[str, ...]
    required_source_features: tuple[str, ...]
    feature_schema_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _identity(self) -> "FeatureSchemaV1":
        names = tuple(item.name for item in self.definitions)
        if not names or len(set(names)) != len(names):
            raise ValueError("feature definitions must be non-empty and uniquely named")
        name_set = set(names)
        required = (
            self.required_identity_features
            + self.required_rank_features
            + self.required_source_features
        )
        if not set(required).issubset(name_set):
            raise ValueError("required feature names must exist in definitions")
        for family_name, family in (
            ("required_identity_features", self.required_identity_features),
            ("required_rank_features", self.required_rank_features),
            ("required_source_features", self.required_source_features),
        ):
            ensure_unique(family, field_name=family_name)
        set_computed_hash(self, field_name="feature_schema_hash", exclude={"feature_schema_hash"})
        return self


class FeatureRowIdentityV1(FrozenModel):
    schema_version: Literal[FEATURE_ROW_IDENTITY_SCHEMA_VERSION] = (
        FEATURE_ROW_IDENTITY_SCHEMA_VERSION
    )
    base_snapshot_id: str = Field(min_length=1, max_length=160)
    canonical_signal_id: str = Field(min_length=1, max_length=160)
    observation_version_id: str = Field(min_length=1, max_length=160)
    symbol: str = Field(pattern=r"^[0-9]{6}\.(SH|SZ|BJ)$")
    decision_cutoff_ts: datetime
    base_candidate_hash: str = Field(min_length=64, max_length=64)
    stage_evidence_set_hash: str = Field(min_length=64, max_length=64)
    feature_payload_hash: str = Field(min_length=64, max_length=64)
    formula_registry_hash: str = Field(min_length=64, max_length=64)
    query_registry_hash: str = Field(min_length=64, max_length=64)
    feature_source_revision_set_hash: str = Field(min_length=64, max_length=64)
    builder_code_closure_hash: str = Field(min_length=64, max_length=64)
    row_identity_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("decision_cutoff_ts")
    @classmethod
    def _cutoff(cls, value: datetime) -> datetime:
        return utc_datetime(value, field_name="decision_cutoff_ts")

    @field_validator(
        "base_candidate_hash",
        "stage_evidence_set_hash",
        "feature_payload_hash",
        "formula_registry_hash",
        "query_registry_hash",
        "feature_source_revision_set_hash",
        "builder_code_closure_hash",
        "row_identity_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "FeatureRowIdentityV1":
        set_computed_hash(self, field_name="row_identity_hash", exclude={"row_identity_hash"})
        return self


def feature_payload_hash(payload: dict[str, Any]) -> str:
    if not payload:
        raise ValueError("feature payload must not be empty")
    return canonical_json_sha256(payload)
