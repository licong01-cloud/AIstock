"""MiniQMT runtime public API with side-effect-free submodule imports."""

from __future__ import annotations

from importlib import import_module
from typing import Any


_EXPORT_MODULES = {
    "MINIQMT_EXECUTION_RUNTIME_ENV": ".config",
    "MiniQMTExecutionRuntimeKind": ".config",
    "get_miniqmt_execution_runtime_kind": ".config",
    "MiniQMTRuntimeManagedBatchSubmitResult": ".client",
    "MiniQMTRuntimeEvidence": ".client",
    "MiniQMTPlanPreviewResult": ".client",
    "MiniQMTExecutionRuntimeClient": ".client",
    "CatchUpPolicyV1": ".kernel_clock",
    "ExchangeSessionClockV1": ".kernel_clock",
    "ExchangeSessionProjectionV1": ".kernel_clock",
    "KernelClockError": ".kernel_clock",
    "KernelClockWakeReceiptV1": ".kernel_clock",
    "MiniQMTGatewayContract": ".contracts",
    "MiniQMTGatewayEventSourceContract": ".contracts",
    "MiniQMTStrategyLedgerOmsContract": ".contracts",
    "MiniQMTVnpyAlgoCoreContract": ".contracts",
    "FakeMiniQMTGateway": ".gateway",
    "MiniQMTGateway": ".gateway",
    "ClosingAuctionCapabilityProbe": ".quote_auction",
    "MarkoutAnchor": ".quote_evidence",
    "QuoteEvidenceCoordinator": ".quote_evidence",
    "QuoteEvidenceHealth": ".quote_evidence",
    "QuoteIngressHealthV1": ".quote_evidence",
    "MiniQMTGatewayCancelAck": ".gateway",
    "MiniQMTGatewayEventSink": ".gateway",
    "MiniQMTGatewayEventSource": ".gateway",
    "MiniQMTGatewayEventSourceError": ".gateway",
    "MiniQMTGatewayOrderAck": ".gateway",
    "QmtClientMiniQMTEventLoopGateway": ".gateway",
    "QmtClientMiniQMTGateway": ".gateway",
    "MiniQMTAlgoInstanceStatus": ".models",
    "MiniQMTChildOrder": ".models",
    "MiniQMTChildOrderStatus": ".models",
    "MiniQMTExecutionAlgoInstance": ".models",
    "MiniQMTExecutionEvent": ".models",
    "MiniQMTExecutionEventType": ".models",
    "MiniQMTExecutionRuntimeConfig": ".models",
    "MiniQMTExecutionRuntimeMode": ".models",
    "MiniQMTExecutionRuntimeRecord": ".models",
    "MiniQMTExecutionRuntimeState": ".models",
    "MiniQMTGatewayState": ".models",
    "MiniQMTOmsState": ".models",
    "MiniQMTOperatorCommandResult": ".models",
    "MiniQMTOperatorCommandStatus": ".models",
    "MiniQMTRuntimeRecoverySnapshot": ".models",
    "MiniQMTOmsLedger": ".oms",
    "MiniQMTOmsProjection": ".oms",
    "DEFAULT_MINIQMT_EXECUTION_RUNTIME_REPOSITORY": ".repository",
    "MINIQMT_EXECUTION_RUNTIME_JSONFILE_TEST_ONLY_ENV": ".repository",
    "MINIQMT_EXECUTION_RUNTIME_PRUNE_EVERY_WRITES_ENV": ".repository",
    "MINIQMT_EXECUTION_RUNTIME_REPOSITORY_ENV": ".repository",
    "InMemoryMiniQMTExecutionRuntimeRepository": ".repository",
    "JsonFileMiniQMTExecutionRuntimeRepository": ".repository",
    "MiniQMTExecutionRuntimeRepository": ".repository",
    "PostgresMiniQMTExecutionRuntimeRepository": ".repository",
    "default_miniqmt_execution_runtime_repository": ".repository",
    "ConfigurableMiniQMTRiskEngine": ".risk",
    "MiniQMTRiskDecision": ".risk",
    "MiniQMTRiskDecisionAction": ".risk",
    "MiniQMTRiskEngine": ".risk",
    "MiniQMTRiskPriceBand": ".risk",
    "MiniQMTRiskRuleSet": ".risk",
    "NoopMiniQMTRiskEngine": ".risk",
    "MiniQMTExecutionEventLoop": ".runtime",
    "MiniQMTExecutionRuntime": ".runtime",
}

__all__ = list(_EXPORT_MODULES)


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted((*globals(), *_EXPORT_MODULES))
