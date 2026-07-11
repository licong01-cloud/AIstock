"""Algorithm-neutral contracts for the future MiniQMT Adaptive IS path.

This package deliberately contains no MiniQMT, database, FastAPI, or broker
dependencies.  Runtime adapters are introduced in the MiniQMT service layer.
"""

from .contracts import (
    ActionQuoteEligibility,
    AuctionCapabilityState,
    CLOSING_AUCTION_SCHEMA_VERSION,
    CalendarSnapshot,
    CalendarSnapshotSet,
    ClosingAuctionSnapshot,
    ControlRevision,
    DepthQuantityUnit,
    ExecutionClockEvent,
    FiveLevelQuote,
    MarketDataEvidenceV1,
    MARKET_DATA_EVIDENCE_SCHEMA_VERSION,
    PriceBasis,
    QuoteCapability,
    QuoteBatchAggregateState,
    QuoteSnapshotBatch,
    TradabilitySnapshot,
)
from .reasons import (
    QuoteContractError,
    QuoteContractReasonCode,
    QuoteContractStage,
    QuoteFailureRetryClass,
    QuoteFailureSeverity,
)

__all__ = [
    "ActionQuoteEligibility",
    "AuctionCapabilityState",
    "CLOSING_AUCTION_SCHEMA_VERSION",
    "CalendarSnapshot",
    "CalendarSnapshotSet",
    "ClosingAuctionSnapshot",
    "ControlRevision",
    "DepthQuantityUnit",
    "ExecutionClockEvent",
    "FiveLevelQuote",
    "MarketDataEvidenceV1",
    "MARKET_DATA_EVIDENCE_SCHEMA_VERSION",
    "PriceBasis",
    "QuoteCapability",
    "QuoteBatchAggregateState",
    "QuoteContractError",
    "QuoteContractReasonCode",
    "QuoteContractStage",
    "QuoteFailureRetryClass",
    "QuoteFailureSeverity",
    "QuoteSnapshotBatch",
    "TradabilitySnapshot",
]
