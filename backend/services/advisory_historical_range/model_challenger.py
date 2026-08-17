"""Read-only M5A challenger over one immutable Historical Range parent day."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Callable, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.model_binding_resolution import (
    AdvisoryModelBindingResolutionV1,
)
from backend.services.advisory_model_first.model_bundle import (
    LoadedAdvisoryModelBundle,
    load_frozen_research_bundle,
)
from backend.services.advisory_model_first.model_inference import (
    build_frozen_candidate_frame,
    score_frozen_feature_matrix,
    validate_frozen_bundle_runtime,
)
from backend.services.advisory_model_first.realtime_feature_source import (
    PostgresRealtimeFeatureSource,
)
from backend.services.advisory_model_first.shared_feature_builder import (
    build_advisory_feature_matrix,
)
from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeCandidateArtifactPayloadV2,
)


CHALLENGER_SCHEMA_VERSION = "advisory_historical_model_challenger_artifact_v1"
CHALLENGER_PRODUCER_VERSION = "advisory_historical_model_challenger_v1"
REASON_PARENT_MISMATCH = "ADVISORY_COMPARISON_PARENT_CANDIDATE_MISMATCH"
REASON_FEATURE_INCOMPLETE = "ADVISORY_COMPARISON_MODEL_FEATURE_INCOMPLETE"
REASON_MODEL_OUTPUT_INVALID = "ADVISORY_COMPARISON_MODEL_OUTPUT_INVALID"


class HistoricalModelChallengerCandidateV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str = Field(min_length=1, max_length=32)
    selection_rank: int = Field(ge=1, le=20)
    selection_score: float
    model_rank: int = Field(ge=1, le=20)
    model_score: float
    is_top5: bool
    score_components: dict[str, float]
    top_feature_contributions: tuple[dict[str, Any], ...]


class HistoricalModelChallengerArtifactV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[CHALLENGER_SCHEMA_VERSION] = CHALLENGER_SCHEMA_VERSION
    producer_contract_version: Literal[CHALLENGER_PRODUCER_VERSION] = CHALLENGER_PRODUCER_VERSION
    parent_range_run_id: str
    parent_day_run_id: str
    parent_candidate_artifact_hash: str = Field(min_length=64, max_length=64)
    parent_candidate_set_hash: str = Field(min_length=64, max_length=64)
    decision_trade_date: date
    target_trade_date: date
    package_id: str
    manifest_sha256: str = Field(min_length=64, max_length=64)
    selection_runtime_semantics_hash: str = Field(min_length=64, max_length=64)
    bundle_id: str = Field(min_length=64, max_length=64)
    bundle_manifest_sha256: str = Field(min_length=64, max_length=64)
    input_identity_hash: str = Field(min_length=64, max_length=64)
    feature_matrix_hash: str = Field(min_length=64, max_length=64)
    candidate_count: int = Field(ge=1, le=20)
    shortlist_count: int = Field(ge=1, le=5)
    hmm_unavailable: tuple[dict[str, Any], ...] = ()
    candidates: tuple[HistoricalModelChallengerCandidateV1, ...]
    artifact_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _closed_identity(self) -> "HistoricalModelChallengerArtifactV1":
        if self.decision_trade_date >= self.target_trade_date:
            raise ValueError("decision_trade_date must precede target_trade_date")
        if self.candidate_count != len(self.candidates):
            raise ValueError("candidate_count differs from candidates")
        if self.candidate_count != 20:
            raise ValueError("challenger artifact must contain exactly 20 candidates")
        if self.shortlist_count != sum(item.is_top5 for item in self.candidates):
            raise ValueError("shortlist_count differs from top5 candidates")
        if self.shortlist_count != 5:
            raise ValueError("challenger artifact must contain exactly five top5 candidates")
        if len({item.symbol for item in self.candidates}) != self.candidate_count:
            raise ValueError("challenger candidate symbols are not unique")
        if sorted(item.selection_rank for item in self.candidates) != list(range(1, self.candidate_count + 1)):
            raise ValueError("selection ranks are not contiguous")
        if sorted(item.model_rank for item in self.candidates) != list(range(1, self.candidate_count + 1)):
            raise ValueError("model ranks are not contiguous")
        if any(item.is_top5 != (item.model_rank <= 5) for item in self.candidates):
            raise ValueError("top5 flag differs from model rank")
        expected = canonical_json_sha256(self.model_dump(mode="json", exclude={"artifact_hash"}))
        if self.artifact_hash is not None and self.artifact_hash != expected:
            raise ValueError("artifact_hash does not match challenger content")
        object.__setattr__(self, "artifact_hash", expected)
        return self


@dataclass(frozen=True)
class HistoricalModelChallenger:
    feature_source: Any = None
    bundle_loader: Callable[..., LoadedAdvisoryModelBundle] = load_frozen_research_bundle
    feature_builder: Callable[..., Any] = build_advisory_feature_matrix
    scorer: Callable[[LoadedAdvisoryModelBundle, pd.DataFrame], list[dict[str, Any]]] = (
        score_frozen_feature_matrix
    )

    def score_day(
        self,
        *,
        parent: HistoricalRangeCandidateArtifactPayloadV2,
        parent_candidate_artifact_hash: str,
        target_trade_date: date,
        model_root: str,
        bundle_id: str,
        expected_selection_runtime_semantics_hash: str,
    ) -> HistoricalModelChallengerArtifactV1:
        rows = _frozen_parent_rows(parent)
        bundle = self.bundle_loader(
            model_root=model_root,
            bundle_id=bundle_id,
            expected_package_id=parent.package_id,
            expected_manifest_sha256=parent.manifest_sha256,
            expected_selection_runtime_semantics_hash=expected_selection_runtime_semantics_hash,
        )
        resolution = _research_resolution(bundle)
        validate_frozen_bundle_runtime(bundle, resolution=resolution)
        candidates = build_frozen_candidate_frame(
            rows,
            program_id=resolution.program_id,
            binding_version_id=resolution.binding_version_id,
            decision_date=parent.decision_trade_date,
            target_trade_date=target_trade_date,
            target_count=20,
            bundle=bundle,
            resolution=resolution,
        )
        feature_source = self.feature_source or PostgresRealtimeFeatureSource()
        realtime = feature_source.load(
            symbols=candidates["instrument"].tolist(),
            decision_as_of_trade_date=parent.decision_trade_date,
            target_trade_date=target_trade_date,
            continuation_cutoff=date.fromisoformat(str(bundle.manifest["continuation_cutoff"])),
            hmm_models=bundle.hmm_models,
        )
        built = self.feature_builder(
            candidates=candidates,
            candidate_daily=realtime.candidate_daily,
            candidate_static=realtime.candidate_static,
            market_daily=realtime.market_daily,
            benchmark_daily=realtime.benchmark_daily,
            suspend_rows=realtime.suspend_rows,
            hmm_states=realtime.hmm_states,
            component_roles=resolution.component_roles,
        )
        if (
            len(built.coverage) != 1
            or str(built.coverage.iloc[0].get("status")) != "available"
            or len(built.features) != len(candidates)
        ):
            missing = built.coverage.iloc[0].get("required_missing_columns", []) if len(built.coverage) else []
            raise AdvisoryModelFirstError(
                "historical challenger feature matrix is incomplete",
                reason_code=REASON_FEATURE_INCOMPLETE,
                context={
                    "decision_trade_date": parent.decision_trade_date.isoformat(),
                    "candidate_count": len(candidates),
                    "feature_count": len(built.features),
                    "required_missing_columns": list(missing),
                },
            )
        scored = self.scorer(bundle, built.features)
        expected_parent = {str(row.symbol): row for row in rows}
        _validate_scored_rows(scored, expected_symbols=set(expected_parent))
        candidate_hashes = [
            str(fact.candidate_content_hash)
            for fact in sorted(parent.candidates, key=lambda item: (item.alpha_raw_rank or 10**9, item.symbol))
            if fact.alpha_raw_rank is not None and fact.alpha_raw_rank <= 20
        ]
        parent_candidate_set_hash = canonical_json_sha256(candidate_hashes)
        input_identity_hash = canonical_json_sha256(
            {
                "parent_candidate_artifact_hash": parent_candidate_artifact_hash,
                "parent_candidate_set_hash": parent_candidate_set_hash,
                "decision_trade_date": parent.decision_trade_date.isoformat(),
                "target_trade_date": target_trade_date.isoformat(),
                "bundle_id": bundle.bundle_id,
                "bundle_manifest_sha256": bundle.manifest_file_sha256,
            }
        )
        feature_records = json.loads(
            built.features.sort_values("instrument").to_json(
                orient="records",
                date_format="iso",
                double_precision=15,
            )
        )
        try:
            artifact_candidates = tuple(
                HistoricalModelChallengerCandidateV1(
                    symbol=str(item["symbol"]),
                    selection_rank=int(expected_parent[str(item["symbol"])].rank),
                    selection_score=float(expected_parent[str(item["symbol"])].score),
                    model_rank=int(item["advisory_model_rank"]),
                    model_score=float(item["advisory_model_score"]),
                    is_top5=bool(item["is_top5"]),
                    score_components={
                        str(key): float(value)
                        for key, value in (item.get("score_components") or {}).items()
                    },
                    top_feature_contributions=tuple(
                        item.get("top_feature_contributions") or ()
                    ),
                )
                for item in scored
            )
            return HistoricalModelChallengerArtifactV1(
                parent_range_run_id=parent.range_run_id,
                parent_day_run_id=parent.day_run_id,
                parent_candidate_artifact_hash=parent_candidate_artifact_hash,
                parent_candidate_set_hash=parent_candidate_set_hash,
                decision_trade_date=parent.decision_trade_date,
                target_trade_date=target_trade_date,
                package_id=parent.package_id,
                manifest_sha256=parent.manifest_sha256,
                selection_runtime_semantics_hash=str(
                    bundle.manifest["selection_runtime_semantics_hash"]
                ),
                bundle_id=bundle.bundle_id,
                bundle_manifest_sha256=str(bundle.manifest_file_sha256),
                input_identity_hash=input_identity_hash,
                feature_matrix_hash=canonical_json_sha256(feature_records),
                candidate_count=len(artifact_candidates),
                shortlist_count=sum(item.is_top5 for item in artifact_candidates),
                hmm_unavailable=tuple(realtime.hmm_unavailable),
                candidates=artifact_candidates,
            )
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            raise AdvisoryModelFirstError(
                "historical challenger model output is invalid",
                reason_code=REASON_MODEL_OUTPUT_INVALID,
                context={"decision_trade_date": parent.decision_trade_date.isoformat()},
            ) from exc


def _frozen_parent_rows(parent: HistoricalRangeCandidateArtifactPayloadV2) -> list[Any]:
    selected = sorted(
        (fact for fact in parent.candidates if fact.alpha_raw_rank is not None and fact.alpha_raw_rank <= 20),
        key=lambda fact: (int(fact.alpha_raw_rank or 0), fact.symbol),
    )
    if len(selected) != 20 or [int(fact.alpha_raw_rank or 0) for fact in selected] != list(range(1, 21)):
        raise AdvisoryModelFirstError(
            "historical challenger parent does not contain raw Top20",
            reason_code=REASON_PARENT_MISMATCH,
        )
    rows: list[Any] = []
    for fact in selected:
        if (
            fact.membership_status != "INCLUDED"
            or fact.alpha_raw_rank != fact.hmm_adjusted_rank
            or fact.alpha_raw_rank != fact.risk_policy_adjusted_rank
            or fact.alpha_raw_rank != fact.selection_effective_rank
            or not _same_decimal(fact.alpha_raw_score, fact.hmm_adjusted_score)
            or not _same_decimal(fact.alpha_raw_score, fact.risk_policy_adjusted_score)
            or not _same_decimal(fact.alpha_raw_score, fact.selection_effective_score)
        ):
            raise AdvisoryModelFirstError(
                "historical challenger parent is not the raw Selection control arm",
                reason_code=REASON_PARENT_MISMATCH,
                context={"symbol": fact.symbol},
            )
        component_scores = fact.component_lineage_json.get("component_scores")
        if not isinstance(component_scores, dict):
            raise AdvisoryModelFirstError(
                "historical challenger parent omits component scores",
                reason_code=REASON_PARENT_MISMATCH,
                context={"symbol": fact.symbol},
            )
        rows.append(
            SimpleNamespace(
                symbol=fact.symbol,
                rank=int(fact.alpha_raw_rank),
                score=float(fact.alpha_raw_score),
                component_scores=component_scores,
            )
        )
    return rows


def _research_resolution(bundle: LoadedAdvisoryModelBundle) -> AdvisoryModelBindingResolutionV1:
    manifest = bundle.manifest
    descriptor_hash = canonical_json_sha256(
        {
            "schema_version": "advisory_historical_model_research_resolution_v1",
            "bundle_id": bundle.bundle_id,
            "bundle_manifest_sha256": bundle.manifest_file_sha256,
        }
    )
    return AdvisoryModelBindingResolutionV1(
        program_id=str(manifest["program_id"]),
        binding_version_id=str(manifest["binding_version_id"]),
        package_id=str(manifest["package_id"]),
        manifest_sha256=str(manifest["manifest_sha256"]),
        style_profile_id=str(manifest["style_profile_id"]),
        style_profile_hash=str(manifest["style_profile_hash"]),
        selection_runtime_semantics_hash=str(manifest["selection_runtime_semantics_hash"]),
        feature_schema_version=str(manifest["feature_schema_version"]),
        feature_schema_hash=str(manifest["feature_schema_hash"]),
        bundle_id=bundle.bundle_id,
        bundle_manifest_sha256=str(bundle.manifest_file_sha256),
        component_roles={"lstm": LSTM_LEG_ID, "fund": FUND_LEG_ID},
        descriptor_sha256=descriptor_hash,
    )


def _validate_scored_rows(
    scored: list[dict[str, Any]],
    *,
    expected_symbols: set[str],
) -> None:
    try:
        symbols = [str(item["symbol"]) for item in scored]
        model_ranks = [int(item["advisory_model_rank"]) for item in scored]
        top5_flags = [bool(item["is_top5"]) for item in scored]
    except (KeyError, TypeError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "historical challenger model output is malformed",
            reason_code=REASON_MODEL_OUTPUT_INVALID,
        ) from exc
    if (
        len(scored) != 20
        or len(set(symbols)) != 20
        or set(symbols) != expected_symbols
        or sorted(model_ranks) != list(range(1, 21))
        or sum(top5_flags) != 5
        or any(flag != (rank <= 5) for flag, rank in zip(top5_flags, model_ranks))
    ):
        raise AdvisoryModelFirstError(
            "historical challenger model output violates Top20/Top5 identity",
            reason_code=REASON_MODEL_OUTPUT_INVALID,
        )
def _same_decimal(left: Decimal | None, right: Decimal | None) -> bool:
    return left is not None and right is not None and left == right
