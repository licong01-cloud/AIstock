from __future__ import annotations

import json
from datetime import date, time
from decimal import Decimal

import pytest

from backend.services.advisory_historical_range.artifact_store import (
    HistoricalRangeArtifactStore,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeOutcomePolicyBundleV1,
    HistoricalRangePolicyComponentV1,
)
from backend.services.advisory_historical_range.outcome_policy_provider import (
    ArtifactHistoricalRangeOutcomePolicyProvider,
    HistoricalRangePolicyComponentDocumentV1,
)
from backend.services.advisory_phase1.label_policy import (
    BarrierPolicy,
    BenchmarkPolicy,
    CashReturnPolicy,
    CashReturnRule,
    CostPolicy,
    EntryBasis,
    EntryExecutionPolicy,
    ExitBasis,
    MarketDataPolicy,
    Projection,
    TerminalPolicy,
    TradingCalendar,
)


def _component_payloads() -> dict[str, dict[str, object]]:
    calendar = TradingCalendar(
        calendar_version="fixture-calendar-v1",
        trading_dates=(date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)),
    )
    corporate_action = {"policy_version": "fixture-corporate-action-v1"}
    corporate_action["policy_hash"] = canonical_json_sha256(corporate_action)
    return {
        "BARRIER": BarrierPolicy(
            policy_id="fixture-barrier-v1",
            target_return=Decimal("0.10"),
            stop_return=Decimal("-0.10"),
        ).model_dump(mode="json"),
        "BENCHMARK": BenchmarkPolicy(
            universe_layer="fixture-universe"
        ).model_dump(mode="json"),
        "CALENDAR": calendar.model_dump(mode="json"),
        "CASH_RETURN": CashReturnPolicy(
            policy_id=CashReturnRule.CASH_RETURN_ZERO_V1,
            cash_return_rate=Decimal("0"),
        ).model_dump(mode="json"),
        "CORPORATE_ACTION": corporate_action,
        "COST": CostPolicy(
            policy_id="fixture-cost-v1",
            commission_buy_rate=Decimal("0.0003"),
            commission_sell_rate=Decimal("0.0003"),
            minimum_commission=Decimal("5"),
            stamp_duty_sell_rate=Decimal("0.0005"),
            transfer_fee_buy_rate=Decimal("0"),
            transfer_fee_sell_rate=Decimal("0"),
            slippage_bps=Decimal("5"),
            lot_size=100,
        ).model_dump(mode="json"),
        "EXECUTION": EntryExecutionPolicy(
            policy_id="fixture-entry-v1",
            entry_basis=EntryBasis.NEXT_OPEN_EXECUTABLE_V1,
            exit_basis=ExitBasis.HORIZON_CLOSE_V1,
            entry_time=time(9, 30),
            exit_time=time(15, 0),
        ).model_dump(mode="json"),
        "MARKET_DATA": MarketDataPolicy(
            price_policy_hash=canonical_json_sha256({"price": "fixture"}),
            adjustment_policy_hash=canonical_json_sha256({"adjustment": "fixture"}),
            corporate_action_policy_hash=str(corporate_action["policy_hash"]),
            symbol_normalization_policy_hash=canonical_json_sha256(
                {"symbol": "fixture"}
            ),
        ).model_dump(mode="json"),
        "TERMINAL": TerminalPolicy(
            policy_id="fixture-terminal-v1",
            terminal_return_rule="EXACT_SETTLEMENT_OR_UNAVAILABLE_V1",
            censor_rule="EXPLICIT_RIGHT_CENSOR_REASON_V1",
        ).model_dump(mode="json"),
    }


def _provider(tmp_path):  # type: ignore[no-untyped-def]
    artifact_store = HistoricalRangeArtifactStore(root=tmp_path / "artifacts")
    component_root = tmp_path / "policy-components"
    component_root.mkdir()
    components = []
    for role, payload in sorted(_component_payloads().items()):
        document = HistoricalRangePolicyComponentDocumentV1(
            component_role=role,
            component_payload=payload,
        )
        relative_path = f"{role.lower()}.json"
        (component_root / relative_path).write_text(
            json.dumps(document.model_dump(mode="json"), sort_keys=True),
            encoding="utf-8",
        )
        components.append(
            HistoricalRangePolicyComponentV1(
                component_role=role,
                component_ref=relative_path,
                component_hash=str(document.component_hash),
            )
        )
    calendar = TradingCalendar.model_validate(_component_payloads()["CALENDAR"])
    bundle = HistoricalRangeOutcomePolicyBundleV1(
        package_id="fixture-package",
        manifest_sha256=canonical_json_sha256({"manifest": "fixture"}),
        alpha_mode="single_alpha",
        style_family="SHORT_REBOUND",
        style_resolution_reason="EXPLICIT_FIXTURE_STYLE",
        calendar_version=calendar.calendar_version,
        calendar_hash=str(calendar.calendar_hash),
        components=tuple(components),
        horizons=(1,),
        projections_by_horizon={
            1: (
                Projection.RETURN_GROSS.value,
                Projection.RETURN_NET_ABSOLUTE.value,
                Projection.RETURN_NET_EXCESS.value,
                Projection.EXECUTABLE_MFE.value,
                Projection.EXECUTABLE_MAE.value,
            )
        },
        candidate_reference_notional=Decimal("100000"),
        benchmark_portfolio_notional=Decimal("100000"),
    )
    payload = bundle.model_dump(
        mode="json", exclude={"policy_bundle_id", "policy_bundle_hash"}
    )
    ref = artifact_store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="advisory_phase1r_r4_outcome_policy_v1",
        payload_schema_version=bundle.schema_version,
        resolved_request_hash=canonical_json_sha256({"fixture": "policy-request"}),
        payload=payload,
    ).ref
    assert ref.payload_sha256 == bundle.policy_bundle_hash
    provider = ArtifactHistoricalRangeOutcomePolicyProvider(
        artifact_store=artifact_store,
        policy_bundle_refs={str(bundle.policy_bundle_hash): ref},
        component_root=component_root,
    )
    return provider, bundle, component_root


def test_artifact_policy_provider_loads_only_the_exact_registered_bundle(tmp_path) -> None:
    provider, bundle, _ = _provider(tmp_path)

    loaded = provider.load(str(bundle.policy_bundle_hash))

    assert loaded.bundle.policy_bundle_hash == bundle.policy_bundle_hash
    assert loaded.bundle.horizons == (1,)
    assert loaded.market_data.corporate_action_policy_hash == str(
        _component_payloads()["CORPORATE_ACTION"]["policy_hash"]
    )
    with pytest.raises(ValueError, match="explicit catalog"):
        provider.load(canonical_json_sha256({"unregistered": True}))


def test_artifact_policy_provider_rejects_component_content_drift(tmp_path) -> None:
    provider, bundle, component_root = _provider(tmp_path)
    target = component_root / "cost.json"
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload["component_payload"]["lot_size"] = 200
    target.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

    with pytest.raises(ValueError, match="hash differs"):
        provider.load(str(bundle.policy_bundle_hash))
