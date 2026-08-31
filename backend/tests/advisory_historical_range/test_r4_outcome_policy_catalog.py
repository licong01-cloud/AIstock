from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
from pathlib import Path

import pytest

from backend.services.advisory_historical_range.artifact_store import (
    HistoricalRangeArtifactStore,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeAdmittedComponentV1,
    HistoricalRangeAdmittedPackageProjectionV1,
    HistoricalRangeAlphaMode,
    HistoricalRangeFrozenProgramV1,
)
from backend.services.advisory_historical_range.outcome_policy_catalog import (
    R4_DEFAULT_HORIZONS,
    R4_OUTCOME_POLICY_CATALOG_ROOT,
    HistoricalRangeOutcomePolicyCatalogDocumentV1,
    freeze_historical_range_outcome_policy,
)
from backend.services.advisory_historical_range.outcome_policy_provider import (
    ArtifactHistoricalRangeOutcomePolicyProvider,
)
from backend.services.advisory_phase1.label_policy import TradingCalendar


def _hash(label: str) -> str:
    return canonical_json_sha256({"label": label})


def _program(*, style_profile: dict[str, str] | None = None) -> HistoricalRangeFrozenProgramV1:
    component = HistoricalRangeAdmittedComponentV1(
        component_id="alpha-1",
        weight="1",
        required_window=20,
        buffer_trading_days=5,
        window_resolution="trading_calendar",
        factor_order=("factor-1",),
        lookback_contract_hash=_hash("lookback"),
        runtime_input_identity_hash=_hash("runtime-input"),
    )
    projection = HistoricalRangeAdmittedPackageProjectionV1(
        package_id="pkg-test",
        package_version="1.0.0",
        manifest_sha256=_hash("manifest"),
        alpha_mode=HistoricalRangeAlphaMode.SINGLE_ALPHA,
        components=(component,),
    )
    review = {
        "take_profit_bps": 1800,
        "stop_loss_bps": 800,
        "time_stop_days": 20,
    }
    program_config = {"package_id": "pkg-test", "review_policy": review}
    runtime_config = {"selection": {"top_k": 20}}
    return HistoricalRangeFrozenProgramV1(
        research_program_id="hrp_test",
        package_id="pkg-test",
        package_version="1.0.0",
        manifest_sha256=projection.manifest_sha256,
        alpha_mode=HistoricalRangeAlphaMode.SINGLE_ALPHA,
        program_config=program_config,
        program_config_hash=canonical_json_sha256(program_config),
        runtime_config=runtime_config,
        runtime_config_hash=canonical_json_sha256(runtime_config),
        review_policy=review,
        review_policy_hash=canonical_json_sha256(review),
        style_profile_ref="style.json" if style_profile else None,
        style_profile_hash=canonical_json_sha256(style_profile) if style_profile else None,
        code_release_id="git-test",
        code_release_hash=_hash("code"),
        selection_semantics_version="selection-v1",
        selection_semantics_hash=_hash("selection"),
        list_semantics_version="list-v1",
        list_semantics_hash=_hash("list"),
        target_package_asset_root_hash=_hash("assets"),
        input_warmup_contract_hash=canonical_json_sha256(
            [{"component_id": component.component_id, "lookback_contract_hash": component.lookback_contract_hash}]
        ),
        admitted_package_projection_hash=canonical_json_sha256(projection.model_dump(mode="json")),
        admitted_package_projection=projection,
    )


def _calendar() -> TradingCalendar:
    return TradingCalendar(
        calendar_version="calendar-test-v1",
        trading_dates=tuple(date(2026, 7, day) for day in range(1, 25)),
    )


def test_freeze_policy_uses_frozen_registry_and_program_without_admission(tmp_path: Path) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "cas")
    first = freeze_historical_range_outcome_policy(
        frozen_program=_program(),
        calendar=_calendar(),
        artifact_store=store,
        component_root=(tmp_path / "components").resolve(),
        resolved_request_hash="f" * 64,
    )
    second = freeze_historical_range_outcome_policy(
        frozen_program=_program(),
        calendar=_calendar(),
        artifact_store=store,
        component_root=(tmp_path / "components").resolve(),
        resolved_request_hash="f" * 64,
    )

    assert first.bundle.style_family == "UNSPECIFIED"
    assert first.bundle.style_resolution_reason == "STYLE_PROFILE_NOT_AVAILABLE"
    assert first.bundle.horizons == R4_DEFAULT_HORIZONS
    assert first.bundle_ref == second.bundle_ref
    provider = ArtifactHistoricalRangeOutcomePolicyProvider(
        artifact_store=store,
        policy_bundle_refs={str(first.bundle.policy_bundle_hash): first.bundle_ref},
        component_root=first.component_root,
    )
    loaded = provider.load(str(first.bundle.policy_bundle_hash))
    assert loaded.cost.commission_buy_rate > 0
    assert loaded.barrier.target_return == Decimal("0.18")
    payload = store.load(first.bundle_ref).payload
    assert "phase1_handoff_bundle_hash" not in payload
    assert "admission_scope_id" not in payload


def test_freeze_policy_requires_exact_declared_style_payload(tmp_path: Path) -> None:
    profile = {"style_family": "LONG_TREND"}
    store = HistoricalRangeArtifactStore(root=tmp_path / "cas")
    with pytest.raises(ValueError, match="differs from frozen Program"):
        freeze_historical_range_outcome_policy(
            frozen_program=_program(style_profile=profile),
            style_profile={"style_family": "SHORT_REBOUND"},
            calendar=_calendar(),
            artifact_store=store,
            component_root=(tmp_path / "components").resolve(),
            resolved_request_hash="f" * 64,
        )


def test_r4_catalog_provenance_matches_the_frozen_source_registry() -> None:
    payload = json.loads((R4_OUTCOME_POLICY_CATALOG_ROOT / "v1.json").read_text(encoding="utf-8"))
    catalog = HistoricalRangeOutcomePolicyCatalogDocumentV1.model_validate(payload)
    repository_root = Path(__file__).resolve().parents[3]
    source_payload = json.loads((repository_root / catalog.source_policy_registry_ref).read_text(encoding="utf-8"))

    assert source_payload["registry_content_hash"] == catalog.source_policy_registry_content_hash
