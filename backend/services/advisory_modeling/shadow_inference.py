from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_modeling.contracts import CapabilityStatus
from backend.services.advisory_modeling.identity import (
    FrozenModel,
    set_computed_hash,
    validated_hash,
)


SHADOW_CANDIDATE_SCHEMA_VERSION = "advisory_shadow_candidate_score_v1"
SHADOW_RESULT_SCHEMA_VERSION = "advisory_shadow_inference_result_v1"


class ShadowCandidateScoreV1(FrozenModel):
    schema_version: Literal[SHADOW_CANDIDATE_SCHEMA_VERSION] = SHADOW_CANDIDATE_SCHEMA_VERSION
    symbol: str = Field(pattern=r"^[0-9]{6}\.(SH|SZ|BJ)$")
    baseline_rank: int = Field(ge=1)
    model_score: Decimal


class ShadowCandidateResultV1(FrozenModel):
    symbol: str
    baseline_rank: int = Field(ge=1)
    normalized_model_score: Decimal = Field(ge=0, le=1)
    model_rank: int = Field(ge=1)
    top5_membership: bool


class ShadowInferenceResultV1(FrozenModel):
    schema_version: Literal[SHADOW_RESULT_SCHEMA_VERSION] = SHADOW_RESULT_SCHEMA_VERSION
    candidate_group_hash: str = Field(min_length=64, max_length=64)
    bundle_id: str = Field(min_length=1, max_length=160)
    bundle_hash: str = Field(min_length=64, max_length=64)
    feature_closure_hash: str = Field(min_length=64, max_length=64)
    capability_status: CapabilityStatus
    reason_codes: tuple[str, ...] = ()
    candidates: tuple[ShadowCandidateResultV1, ...]
    result_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "candidate_group_hash", "bundle_hash", "feature_closure_hash", "result_hash"
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "ShadowInferenceResultV1":
        if self.capability_status == CapabilityStatus.MODEL_UNAVAILABLE:
            if self.candidates or not self.reason_codes:
                raise ValueError("MODEL_UNAVAILABLE requires reasons and must not fabricate candidates")
        elif not self.candidates:
            raise ValueError("available shadow inference requires candidate results")
        ranks = tuple(item.model_rank for item in self.candidates)
        if ranks and tuple(sorted(ranks)) != tuple(range(1, len(ranks) + 1)):
            raise ValueError("model ranks must form a complete one-based sequence")
        set_computed_hash(self, field_name="result_hash", exclude={"result_hash"})
        return self


def build_shadow_result(
    *,
    candidate_group_hash: str,
    bundle_id: str,
    bundle_hash: str,
    feature_closure_hash: str,
    candidates: tuple[ShadowCandidateScoreV1, ...],
) -> ShadowInferenceResultV1:
    if not candidates:
        raise ValueError("shadow inference candidate group must not be empty")
    if len({item.symbol for item in candidates}) != len(candidates):
        raise ValueError("shadow inference contains duplicate symbols")
    if len({item.baseline_rank for item in candidates}) != len(candidates):
        raise ValueError("baseline ranks must be unique")
    if tuple(sorted(item.baseline_rank for item in candidates)) != tuple(
        range(1, len(candidates) + 1)
    ):
        raise ValueError("baseline ranks must form the complete frozen candidate group")
    ordered = tuple(sorted(candidates, key=lambda item: (-item.model_score, item.symbol)))
    distinct_scores = sorted({item.model_score for item in candidates})
    if len(distinct_scores) == 1:
        normalized = {distinct_scores[0]: Decimal("0.5")}
    else:
        normalized = {
            score: Decimal(index) / Decimal(len(distinct_scores) - 1)
            for index, score in enumerate(distinct_scores)
        }
    results = tuple(
        ShadowCandidateResultV1(
            symbol=item.symbol,
            baseline_rank=item.baseline_rank,
            normalized_model_score=normalized[item.model_score],
            model_rank=index,
            top5_membership=index <= 5,
        )
        for index, item in enumerate(ordered, start=1)
    )
    return ShadowInferenceResultV1(
        candidate_group_hash=candidate_group_hash,
        bundle_id=bundle_id,
        bundle_hash=bundle_hash,
        feature_closure_hash=feature_closure_hash,
        capability_status=CapabilityStatus.RESEARCH_BUNDLE_COMPLETE,
        candidates=results,
    )
