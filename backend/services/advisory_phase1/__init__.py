"""Phase 1 Advisory research data foundations.

This package deliberately has no eager re-exports. Phase 1 is an optional
Selection sidecar; importing a submodule must not load Advisory, Paper, QMT or
simulation services on an existing runtime path.
"""

from importlib import import_module
from typing import Any


_EXPORTS = {
    "InMemorySourceAvailabilityLedger": ".source_ledger",
    "SourceAvailabilityEvent": ".source_ledger",
    "SourceAvailabilityEventInput": ".source_ledger",
    "SourceAvailabilityEventRequest": ".source_ledger",
    "SourceAvailabilityEventType": ".source_ledger",
    "SourceLedgerError": ".source_ledger",
    "BoundedSelectionStageTraceSink": ".stage_trace",
    "ComponentCapability": ".stage_trace",
    "NullSelectionStageTraceSink": ".stage_trace",
    "Phase1TraceCaptureReceipt": ".stage_trace",
    "Phase1TraceCaptureService": ".stage_trace",
    "TraceCaptureBinding": ".stage_trace",
    "TraceCapturePolicy": ".stage_trace",
    "ControlBindingEvent": ".control_binding",
    "ControlBindingRequest": ".control_binding",
    "ControlType": ".control_binding",
    "InMemoryControlBindingRepository": ".control_binding",
    "PostgresControlBindingRepository": ".control_binding",
    "InMemoryTraceOutboxRepository": ".trace_outbox",
    "PostgresTraceOutboxRepository": ".trace_outbox",
    "BoundedTraceOutboxDispatcher": ".trace_outbox",
    "ExpectedTraceIdentity": ".trace_outbox",
    "ScopeAwareExpectedTraceIdentityV2": ".trace_outbox",
    "TraceCaptureReconciler": ".trace_outbox",
    "TraceDeliveryEvent": ".trace_outbox",
    "TraceDeliveryEventRequest": ".trace_outbox",
    "CaptureBatch": ".capture_foundation",
    "CaptureBatchRequest": ".capture_foundation",
    "CaptureBatchStatus": ".capture_foundation",
    "CaptureMembership": ".capture_foundation",
    "CapturePlan": ".capture_foundation",
    "InMemoryCaptureBatchRepository": ".capture_foundation",
    "InMemoryTraceAdmissionValidator": ".capture_foundation",
    "InMemoryTraceCaptureGapRepository": ".capture_foundation",
    "PostgresCaptureBatchRepository": ".capture_foundation",
    "PostgresTraceAdmissionValidator": ".capture_foundation",
    "PostgresTraceCaptureGapRepository": ".capture_foundation",
    "FrozenTradingCalendarVerifier": ".observation_capture",
    "InMemoryObservationCaptureRepository": ".observation_capture",
    "ObservationCaptureRecord": ".observation_capture",
    "expected_evidence_bundle_hash": ".observation_capture",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    return getattr(module, name)
