"""Phase 1 Advisory research data foundations.

This package is deliberately isolated from Selection, simulation, Paper, QMT,
brokers and real-time providers.  A source observer may append evidence here,
but this package never starts one.
"""

from .source_ledger import (
    InMemorySourceAvailabilityLedger,
    SourceAvailabilityEvent,
    SourceAvailabilityEventInput,
    SourceAvailabilityEventRequest,
    SourceAvailabilityEventType,
    SourceLedgerError,
)

__all__ = [
    "InMemorySourceAvailabilityLedger",
    "SourceAvailabilityEvent",
    "SourceAvailabilityEventInput",
    "SourceAvailabilityEventRequest",
    "SourceAvailabilityEventType",
    "SourceLedgerError",
]
