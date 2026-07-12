"""Pure raw-manifest closing-auction observation for P1-D.

This module deliberately does not accept a FiveLevelQuote.  That type boundary
prevents an adapter from accidentally synthesizing auction-only data out of
ordinary depth, last price, pre-close, limits, or a subscription success flag.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from backend.execution_algos.adaptive_is.contracts import (
    CLOSING_AUCTION_SCHEMA_VERSION,
    AuctionCapabilityState,
    AuctionFieldManifest,
    ClosingAuctionSnapshot,
    MarketPhase,
    QuoteSource,
    canonical_sha256,
)
from backend.execution_algos.adaptive_is.reasons import QuoteContractReasonCode, quote_contract_error


class ClosingAuctionCapabilityProbe:
    """Build only an explicit raw-field auction observation or loud unavailable/invalid state."""

    def observe(
        self,
        *,
        symbol: str,
        clock_event_id: str,
        market_phase: MarketPhase,
        received_at_utc: datetime,
        source: QuoteSource,
        normalized_quote_sha256: str | None,
        raw_auction_fields: Mapping[str, Any] | None,
        manifest: AuctionFieldManifest | None,
        exchange_time_utc: datetime | None,
    ) -> ClosingAuctionSnapshot:
        if manifest is None:
            return self._unavailable(
                symbol=symbol,
                clock_event_id=clock_event_id,
                market_phase=market_phase,
                received_at_utc=received_at_utc,
                source=source,
            )
        if market_phase != MarketPhase.CLOSING_AUCTION:
            raise quote_contract_error(
                QuoteContractReasonCode.PAYLOAD_INVALID,
                "closing-auction probe cannot project a non-auction market phase",
                context={"market_phase": market_phase.value},
            )
        if raw_auction_fields is None:
            raw_auction_fields = {}
        field_names = (
            manifest.indicative_match_price_field,
            manifest.indicative_match_volume_field,
            manifest.unmatched_side_field,
            manifest.unmatched_quantity_field,
        )
        if any(field_name not in raw_auction_fields for field_name in field_names) or exchange_time_utc is None:
            return ClosingAuctionSnapshot(
                schema_version=CLOSING_AUCTION_SCHEMA_VERSION,
                symbol=symbol,
                clock_event_id=clock_event_id,
                market_phase=market_phase,
                capability_state=AuctionCapabilityState.INVALID,
                exchange_time_utc=None,
                received_at_utc=received_at_utc,
                source=source,
                normalized_quote_sha256=None,
                indicative_match_price=None,
                indicative_match_volume=None,
                unmatched_side=None,
                unmatched_quantity=None,
                auction_field_manifest=manifest,
                reasons=(QuoteContractReasonCode.PAYLOAD_INVALID,),
            )
        try:
            indicative_price = _decimal(raw_auction_fields[manifest.indicative_match_price_field])
            indicative_volume = _decimal(raw_auction_fields[manifest.indicative_match_volume_field])
            unmatched_quantity = _decimal(raw_auction_fields[manifest.unmatched_quantity_field])
        except (InvalidOperation, TypeError, ValueError):
            return ClosingAuctionSnapshot(
                schema_version=CLOSING_AUCTION_SCHEMA_VERSION,
                symbol=symbol,
                clock_event_id=clock_event_id,
                market_phase=market_phase,
                capability_state=AuctionCapabilityState.INVALID,
                exchange_time_utc=None,
                received_at_utc=received_at_utc,
                source=source,
                normalized_quote_sha256=None,
                indicative_match_price=None,
                indicative_match_volume=None,
                unmatched_side=None,
                unmatched_quantity=None,
                auction_field_manifest=manifest,
                reasons=(QuoteContractReasonCode.PAYLOAD_INVALID,),
            )
        return ClosingAuctionSnapshot(
            schema_version=CLOSING_AUCTION_SCHEMA_VERSION,
            symbol=symbol,
            clock_event_id=clock_event_id,
            market_phase=market_phase,
            capability_state=AuctionCapabilityState.AVAILABLE,
            exchange_time_utc=exchange_time_utc,
            received_at_utc=received_at_utc,
            source=source,
            normalized_quote_sha256=normalized_quote_sha256,
            indicative_match_price=indicative_price,
            indicative_match_volume=indicative_volume,
            unmatched_side=str(raw_auction_fields[manifest.unmatched_side_field]),
            unmatched_quantity=unmatched_quantity,
            auction_field_manifest=manifest,
            source_payload_sha256=canonical_sha256({field_name: raw_auction_fields[field_name] for field_name in field_names}),
        )

    @staticmethod
    def _unavailable(
        *,
        symbol: str,
        clock_event_id: str,
        market_phase: MarketPhase,
        received_at_utc: datetime,
        source: QuoteSource,
    ) -> ClosingAuctionSnapshot:
        return ClosingAuctionSnapshot(
            schema_version=CLOSING_AUCTION_SCHEMA_VERSION,
            symbol=symbol,
            clock_event_id=clock_event_id,
            market_phase=market_phase,
            capability_state=AuctionCapabilityState.UNAVAILABLE,
            exchange_time_utc=None,
            received_at_utc=received_at_utc,
            source=source,
            normalized_quote_sha256=None,
            indicative_match_price=None,
            indicative_match_volume=None,
            unmatched_side=None,
            unmatched_quantity=None,
            reasons=(QuoteContractReasonCode.CLOSING_AUCTION_CAPABILITY_UNAVAILABLE,),
        )


def _decimal(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("auction field cannot be boolean")
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ValueError("auction field must be finite")
    return parsed


__all__ = ["ClosingAuctionCapabilityProbe"]
