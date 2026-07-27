"""Production loader for range-native outcome policy artifacts and components."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_historical_range.artifact_store import (
    HistoricalRangeArtifactStore,
)
from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeOutcomePolicyBundleV1,
    require_sha256,
)
from backend.services.advisory_historical_range.outcome_evaluator import (
    HistoricalRangeValuationPolicyBundleV1,
    HistoricalRangeValuationPolicySetV1,
)
from backend.services.advisory_phase1.label_policy import (
    BarrierPolicy,
    BenchmarkPolicy,
    CashReturnPolicy,
    CostPolicy,
    EntryExecutionPolicy,
    MarketDataPolicy,
    Projection,
    TerminalPolicy,
    TradingCalendar,
)


class HistoricalRangePolicyComponentDocumentV1(BaseModel):
    """Versioned component file whose complete payload is content addressed."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = "advisory_historical_range_policy_component_document_v1"
    component_role: str = Field(min_length=1, max_length=80)
    component_payload: dict[str, Any]
    component_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("component_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return (
            require_sha256(value, field_name="component_hash")
            if value is not None
            else None
        )

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangePolicyComponentDocumentV1":
        if not self.component_payload:
            raise ValueError("policy component document payload cannot be empty")
        digest = canonical_json_sha256(self.component_payload)
        if self.component_hash is not None and self.component_hash != digest:
            raise ValueError("policy component document hash differs from payload")
        object.__setattr__(self, "component_hash", digest)
        return self


class ArtifactHistoricalRangeOutcomePolicyProvider:
    """Resolve only explicitly registered policy refs and exact component files."""

    def __init__(
        self,
        *,
        artifact_store: HistoricalRangeArtifactStore,
        policy_bundle_refs: Mapping[str, HistoricalRangeArtifactRefV1],
        component_root: Path,
    ) -> None:
        if not component_root.is_absolute():
            raise ValueError("range policy component root must be absolute")
        self._artifact_store = artifact_store
        self._component_root = component_root.resolve(strict=True)
        self._refs = dict(policy_bundle_refs)
        for digest, ref in self._refs.items():
            require_sha256(digest, field_name="policy_bundle_hash")
            if (
                ref.artifact_kind is not HistoricalRangeArtifactKind.REQUEST
                or ref.payload_sha256 != digest
            ):
                raise ValueError("registered range policy ref/hash pair is invalid")
        self._cache: dict[str, HistoricalRangeValuationPolicySetV1] = {}

    def load(self, policy_bundle_hash: str) -> HistoricalRangeValuationPolicySetV1:
        policy_bundle_hash = require_sha256(
            policy_bundle_hash, field_name="policy_bundle_hash"
        )
        cached = self._cache.get(policy_bundle_hash)
        if cached is not None:
            return cached
        ref = self._refs.get(policy_bundle_hash)
        if ref is None:
            raise ValueError("range policy hash is not present in the explicit catalog")
        envelope = self._artifact_store.load(ref)
        bundle = HistoricalRangeOutcomePolicyBundleV1.model_validate(envelope.payload)
        if bundle.policy_bundle_hash != policy_bundle_hash:
            raise ValueError("range policy artifact payload differs from registered ref")
        documents = {
            component.component_role: self._load_component(
                component_ref=component.component_ref,
                expected_role=component.component_role,
                expected_hash=component.component_hash,
            )
            for component in bundle.components
        }
        calendar = TradingCalendar.model_validate(
            documents["CALENDAR"].component_payload
        )
        market_data = MarketDataPolicy.model_validate(
            documents["MARKET_DATA"].component_payload
        )
        execution = EntryExecutionPolicy.model_validate(
            documents["EXECUTION"].component_payload
        )
        cost = CostPolicy.model_validate(documents["COST"].component_payload)
        benchmark = BenchmarkPolicy.model_validate(
            documents["BENCHMARK"].component_payload
        )
        cash_return = CashReturnPolicy.model_validate(
            documents["CASH_RETURN"].component_payload
        )
        barrier = BarrierPolicy.model_validate(
            documents["BARRIER"].component_payload
        )
        terminal = TerminalPolicy.model_validate(
            documents["TERMINAL"].component_payload
        )
        corporate_action = documents["CORPORATE_ACTION"].component_payload
        corporate_action_hash = corporate_action.get("policy_hash")
        if not isinstance(corporate_action_hash, str):
            raise ValueError("corporate-action component lacks policy_hash")
        require_sha256(corporate_action_hash, field_name="corporate_action_policy_hash")
        if (
            canonical_json_sha256(
                {key: value for key, value in corporate_action.items() if key != "policy_hash"}
            )
            != corporate_action_hash
            or market_data.corporate_action_policy_hash != corporate_action_hash
        ):
            raise ValueError("corporate-action component differs from market-data policy")
        projections = {
            horizon: tuple(Projection(value) for value in values)
            for horizon, values in bundle.projections_by_horizon.items()
        }
        normalized_bundle = HistoricalRangeValuationPolicyBundleV1(
            policy_bundle_hash=policy_bundle_hash,
            calendar_version=bundle.calendar_version,
            calendar_hash=bundle.calendar_hash,
            component_hashes={
                item.component_role: item.component_hash for item in bundle.components
            },
            horizons=bundle.horizons,
            projections_by_horizon=projections,
            gap_1d_enabled=bundle.gap_1d_enabled,
            candidate_reference_notional=bundle.candidate_reference_notional,
            benchmark_portfolio_notional=bundle.benchmark_portfolio_notional,
        )
        policy = HistoricalRangeValuationPolicySetV1(
            bundle=normalized_bundle,
            calendar=calendar,
            market_data=market_data,
            execution=execution,
            cost=cost,
            benchmark=benchmark,
            cash_return=cash_return,
            barrier=barrier,
            terminal=terminal,
        )
        self._cache[policy_bundle_hash] = policy
        return policy

    def _load_component(
        self, *, component_ref: str, expected_role: str, expected_hash: str
    ) -> HistoricalRangePolicyComponentDocumentV1:
        relative = Path(component_ref)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("range policy component ref must be a contained relative path")
        path = (self._component_root / relative).resolve(strict=True)
        try:
            path.relative_to(self._component_root)
        except ValueError as exc:
            raise ValueError("range policy component escapes the explicit root") from exc
        if path.suffix.lower() != ".json" or not path.is_file():
            raise ValueError("range policy component must be an immutable JSON file")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError("range policy component JSON is unreadable") from exc
        document = HistoricalRangePolicyComponentDocumentV1.model_validate(payload)
        if (
            document.component_role != expected_role
            or document.component_hash != expected_hash
        ):
            raise ValueError("range policy component ref/hash/role closure failed")
        return document
