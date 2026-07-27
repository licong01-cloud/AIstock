"""Deterministic R4 outcome-policy resolution from frozen repository policy.

The resolver binds one R3 frozen Program to immutable Phase 1 valuation
components.  It never calls package admission, Selection, Paper, simulation,
or a current-policy lookup.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_historical_range.artifact_store import (
    HistoricalRangeArtifactStore,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeFrozenProgramV1,
    HistoricalRangeOutcomePolicyBundleV1,
    HistoricalRangePolicyComponentV1,
)
from backend.services.advisory_historical_range.outcome_policy_provider import (
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


R4_OUTCOME_POLICY_CATALOG_VERSION = "advisory_historical_range_r4_policy_catalog_v1"
R4_OUTCOME_POLICY_PRODUCER_VERSION = "advisory_phase1r_r4_outcome_policy_v1"
R4_OUTCOME_POLICY_CATALOG_ROOT = Path(__file__).with_name("policy_registry") / "r4"
R4_DEFAULT_HORIZONS = (1, 3, 5, 10, 20)
R4_LONG_TREND_HORIZONS = (20, 40, 60, 120, 180)
R4_PROJECTIONS = (
    Projection.RETURN_GROSS.value,
    Projection.RETURN_NET_ABSOLUTE.value,
    Projection.RETURN_NET_EXCESS.value,
    Projection.PATH_MFE.value,
    Projection.PATH_MAE.value,
    Projection.EXECUTABLE_MFE.value,
    Projection.EXECUTABLE_MAE.value,
)


@dataclass(frozen=True)
class HistoricalRangeOutcomePolicyResolutionV1:
    bundle: HistoricalRangeOutcomePolicyBundleV1
    bundle_ref: HistoricalRangeArtifactRefV1
    component_root: Path


class HistoricalRangeOutcomePolicyCatalogDocumentV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = R4_OUTCOME_POLICY_CATALOG_VERSION
    catalog_id: str = "advisory_historical_range_r4"
    catalog_version: str = "v1"
    source_policy_registry_ref: str = Field(min_length=1, max_length=320)
    source_policy_registry_content_hash: str = Field(min_length=64, max_length=64)
    default_horizons: tuple[int, ...]
    long_trend_horizons: tuple[int, ...]
    cost_policy: dict[str, Any]
    benchmark_policy: dict[str, Any]
    catalog_content_hash: str = Field(min_length=64, max_length=64)

    @model_validator(mode="after")
    def _identity(self) -> "HistoricalRangeOutcomePolicyCatalogDocumentV1":
        if self.default_horizons != R4_DEFAULT_HORIZONS:
            raise ValueError("R4 catalog default horizons differ from the design contract")
        if self.long_trend_horizons != R4_LONG_TREND_HORIZONS:
            raise ValueError("R4 catalog long-trend horizons differ from the design contract")
        if not self.cost_policy or not self.benchmark_policy:
            raise ValueError("R4 catalog requires cost and benchmark policies")
        digest = canonical_json_sha256(self.model_dump(mode="json", exclude={"catalog_content_hash"}))
        if digest != self.catalog_content_hash:
            raise ValueError("R4 policy catalog content hash does not match")
        return self


def freeze_historical_range_outcome_policy(
    *,
    frozen_program: HistoricalRangeFrozenProgramV1,
    calendar: TradingCalendar,
    artifact_store: HistoricalRangeArtifactStore,
    component_root: Path,
    resolved_request_hash: str,
    style_profile: Mapping[str, Any] | None = None,
    catalog_version: str = "v1",
) -> HistoricalRangeOutcomePolicyResolutionV1:
    """Resolve and publish one exact range-native policy bundle.

    The repository registry supplies the already-frozen market/cost semantics;
    the R3 Program supplies package identity and explicit barrier thresholds.
    A missing style profile is represented as such and uses the catalog's
    declared default horizons.
    """

    if not component_root.is_absolute():
        raise ValueError("range policy component root must be absolute")
    if len(resolved_request_hash) != 64 or any(char not in "0123456789abcdef" for char in resolved_request_hash):
        raise ValueError("range policy requires the exact resolved request hash")
    component_root.mkdir(parents=True, exist_ok=True)
    component_root = component_root.resolve(strict=True)

    catalog = _load_catalog(version=catalog_version)

    style_family, style_reason, horizons = _resolve_style(
        frozen_program=frozen_program,
        style_profile=style_profile,
    )
    component_payloads = _component_payloads(
        frozen_program=frozen_program,
        calendar=calendar,
        cost_policy=catalog.cost_policy,
        benchmark_policy=catalog.benchmark_policy,
    )
    catalog_key = canonical_json_sha256(
        {
            "catalog_version": R4_OUTCOME_POLICY_CATALOG_VERSION,
            "catalog_content_hash": catalog.catalog_content_hash,
            "source_policy_registry_content_hash": (catalog.source_policy_registry_content_hash),
            "frozen_program_hash": frozen_program.frozen_program_hash,
            "calendar_hash": calendar.calendar_hash,
            "style_family": style_family,
            "style_resolution_reason": style_reason,
            "horizons": horizons,
        }
    )
    relative_prefix = Path(R4_OUTCOME_POLICY_CATALOG_VERSION) / catalog_key
    components: list[HistoricalRangePolicyComponentV1] = []
    for role, payload in sorted(component_payloads.items()):
        document = HistoricalRangePolicyComponentDocumentV1(
            component_role=role,
            component_payload=payload,
        )
        relative_path = relative_prefix / f"{role.lower()}.json"
        _write_exact_json(
            component_root / relative_path,
            document.model_dump(mode="json"),
        )
        components.append(
            HistoricalRangePolicyComponentV1(
                component_role=role,
                component_ref=relative_path.as_posix(),
                component_hash=str(document.component_hash),
            )
        )

    bundle = HistoricalRangeOutcomePolicyBundleV1(
        package_id=frozen_program.package_id,
        manifest_sha256=frozen_program.manifest_sha256,
        alpha_mode=frozen_program.alpha_mode.value,
        style_family=style_family,
        style_resolution_reason=style_reason,
        calendar_version=calendar.calendar_version,
        calendar_hash=str(calendar.calendar_hash),
        components=tuple(components),
        horizons=horizons,
        projections_by_horizon={horizon: R4_PROJECTIONS for horizon in horizons},
        candidate_reference_notional=Decimal(str(catalog.cost_policy["reference_notional"])),
        benchmark_portfolio_notional=Decimal(str(catalog.cost_policy["reference_notional"])),
    )
    payload = bundle.model_dump(mode="json", exclude={"policy_bundle_id", "policy_bundle_hash"})
    envelope = artifact_store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version=R4_OUTCOME_POLICY_PRODUCER_VERSION,
        payload_schema_version=bundle.schema_version,
        resolved_request_hash=resolved_request_hash,
        payload=payload,
    )
    if envelope.ref.payload_sha256 != bundle.policy_bundle_hash:
        raise ValueError("published range policy artifact differs from resolved bundle")
    return HistoricalRangeOutcomePolicyResolutionV1(
        bundle=bundle,
        bundle_ref=envelope.ref,
        component_root=component_root,
    )


def _load_catalog(*, version: str) -> HistoricalRangeOutcomePolicyCatalogDocumentV1:
    if not version or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for char in version):
        raise ValueError("R4 policy catalog version is invalid")
    root = R4_OUTCOME_POLICY_CATALOG_ROOT.resolve()
    path = (root / f"{version}.json").resolve()
    try:
        path.relative_to(root)
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"R4 frozen policy catalog is unavailable: {version}") from exc
    return HistoricalRangeOutcomePolicyCatalogDocumentV1.model_validate(payload)


def _resolve_style(
    *,
    frozen_program: HistoricalRangeFrozenProgramV1,
    style_profile: Mapping[str, Any] | None,
) -> tuple[str, str, tuple[int, ...]]:
    if frozen_program.style_profile_hash is None:
        if style_profile is not None:
            raise ValueError("style profile payload supplied for a Program without style identity")
        return "UNSPECIFIED", "STYLE_PROFILE_NOT_AVAILABLE", R4_DEFAULT_HORIZONS
    if style_profile is None:
        raise ValueError("frozen Program style identity requires its exact profile payload")
    if canonical_json_sha256(style_profile) != frozen_program.style_profile_hash:
        raise ValueError("style profile payload differs from frozen Program identity")
    family = str(style_profile.get("style_family") or "").strip().upper()
    if family == "SHORT_REBOUND":
        return family, "FROZEN_PROGRAM_STYLE_PROFILE", R4_DEFAULT_HORIZONS
    if family == "LONG_TREND":
        return family, "FROZEN_PROGRAM_STYLE_PROFILE", R4_LONG_TREND_HORIZONS
    if family:
        return family, "FROZEN_PROGRAM_STYLE_PROFILE_OTHER", R4_DEFAULT_HORIZONS
    raise ValueError("frozen style profile lacks style_family")


def _component_payloads(
    *,
    frozen_program: HistoricalRangeFrozenProgramV1,
    calendar: TradingCalendar,
    cost_policy: Mapping[str, Any],
    benchmark_policy: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    target_bps = _required_review_bps(frozen_program, "take_profit_bps", positive=True)
    stop_bps = _required_review_bps(frozen_program, "stop_loss_bps", positive=True)
    corporate_action = {
        "schema_version": "advisory_phase1_corporate_action_policy_v1",
        "policy_id": "CORPORATE_ACTION_NORMALIZED_FROM_RAW_V1",
        "quantity_rule": "EXACT_SPLIT_BONUS_RIGHTS_EVIDENCE_OR_UNAVAILABLE_V1",
        "cashflow_rule": "EXACT_CASHFLOW_EVIDENCE_OR_UNAVAILABLE_V1",
    }
    corporate_action["policy_hash"] = canonical_json_sha256(corporate_action)
    price_policy_hash = canonical_json_sha256({"policy_id": "RAW_LI_TO_YUAN_V1", "storage_scale": "li_to_yuan_1000"})
    adjustment_policy_hash = canonical_json_sha256({"policy_id": "CORPORATE_ACTION_NORMALIZED_FROM_RAW_V1"})
    symbol_policy_hash = canonical_json_sha256({"policy_id": "TS_CODE_UPPERCASE_V1"})
    return {
        "BARRIER": BarrierPolicy(
            policy_id="R3_FROZEN_REVIEW_BARRIER_V1",
            target_return=Decimal(target_bps) / Decimal("10000"),
            stop_return=-(Decimal(stop_bps) / Decimal("10000")),
        ).model_dump(mode="json"),
        "BENCHMARK": BenchmarkPolicy(
            policy_id=str(benchmark_policy["policy_id"]),
            universe_layer=str(benchmark_policy["universe_layer"]),
        ).model_dump(mode="json"),
        "CALENDAR": calendar.model_dump(mode="json"),
        "CASH_RETURN": CashReturnPolicy(
            policy_id=CashReturnRule.CASH_RETURN_ZERO_V1,
            cash_return_rate=Decimal("0"),
        ).model_dump(mode="json"),
        "CORPORATE_ACTION": corporate_action,
        "COST": CostPolicy(
            policy_id=str(cost_policy["policy_id"]),
            commission_buy_rate=Decimal(str(cost_policy["commission_buy_rate"])),
            commission_sell_rate=Decimal(str(cost_policy["commission_sell_rate"])),
            minimum_commission=Decimal(str(cost_policy["minimum_commission"])),
            stamp_duty_sell_rate=Decimal(str(cost_policy["stamp_duty_sell_rate"])),
            transfer_fee_buy_rate=Decimal(str(cost_policy["transfer_fee_rate"])),
            transfer_fee_sell_rate=Decimal(str(cost_policy["transfer_fee_rate"])),
            slippage_bps=Decimal(str(cost_policy["slippage_bps"])),
            lot_size=int(cost_policy["lot_size"]),
        ).model_dump(mode="json"),
        "EXECUTION": EntryExecutionPolicy(
            policy_id="NEXT_OPEN_TO_HORIZON_CLOSE_EXECUTABLE_V1",
            entry_basis=EntryBasis.NEXT_OPEN_EXECUTABLE_V1,
            exit_basis=ExitBasis.HORIZON_CLOSE_V1,
            entry_time=time(9, 30),
            exit_time=time(15, 0),
        ).model_dump(mode="json"),
        "MARKET_DATA": MarketDataPolicy(
            price_policy_hash=price_policy_hash,
            adjustment_policy_hash=adjustment_policy_hash,
            corporate_action_policy_hash=str(corporate_action["policy_hash"]),
            symbol_normalization_policy_hash=symbol_policy_hash,
        ).model_dump(mode="json"),
        "TERMINAL": TerminalPolicy(
            policy_id="EXACT_SETTLEMENT_OR_RIGHT_CENSOR_V1",
            terminal_return_rule="EXACT_SETTLEMENT_OR_UNAVAILABLE_V1",
            censor_rule="EXPLICIT_RIGHT_CENSOR_REASON_V1",
        ).model_dump(mode="json"),
    }


def _required_review_bps(
    frozen_program: HistoricalRangeFrozenProgramV1,
    field_name: str,
    *,
    positive: bool,
) -> str:
    value = frozen_program.review_policy.get(field_name)
    try:
        decimal = Decimal(str(value))
    except Exception as exc:  # noqa: BLE001 - normalize external frozen payload errors.
        raise ValueError(f"frozen Program review policy lacks numeric {field_name}") from exc
    if positive and decimal <= 0:
        raise ValueError(f"frozen Program review policy requires positive {field_name}")
    return str(decimal)


def _write_exact_json(path: Path, payload: Mapping[str, Any]) -> None:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if path.exists():
        try:
            existing = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(f"existing policy component is unreadable: {path}") from exc
        if existing != serialized:
            raise ValueError(f"existing policy component content conflicts: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(serialized, encoding="utf-8")
    temporary.replace(path)
