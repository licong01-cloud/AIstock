"""Typed in-process adapter boundary for official monthly stage producers.

Adapters are application objects assembled by the data-owned composition root.
They do not accept commands or module names from API requests, environment
variables, or operation state.  The outer pipeline still hashes every artifact
and enforces the stage-specific semantic contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from .canonical import ensure_sha256
from .monthly_unified import STAGES, TELEMETRY_COUNT_FIELDS
from .monthly_worker import PRODUCER_EVIDENCE_SCHEMA, ProducerContext


class MonthlyStageAdapterError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class MonthlyStageArtifact:
    artifact_id: str
    path: Path

    def __post_init__(self) -> None:
        relative = Path(self.artifact_id)
        if (
            not self.artifact_id
            or relative.is_absolute()
            or ".." in relative.parts
            or "\\" in self.artifact_id
        ):
            raise MonthlyStageAdapterError("stage artifact id is not portable")
        if not self.path.is_absolute():
            raise MonthlyStageAdapterError("stage artifact path must be absolute")

    def external_ref(self) -> dict[str, str]:
        return {"id": self.artifact_id, "path": str(self.path)}


@dataclass(frozen=True, slots=True)
class MonthlyStageResult:
    scope: Mapping[str, Any]
    input_artifacts: tuple[MonthlyStageArtifact, ...]
    output_artifacts: tuple[MonthlyStageArtifact, ...]
    counts: Mapping[str, int]
    errors: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.scope, Mapping):
            raise MonthlyStageAdapterError("stage scope must be an object")
        if not self.output_artifacts:
            raise MonthlyStageAdapterError("stage must produce an output artifact")
        ids = [item.artifact_id for item in (*self.input_artifacts, *self.output_artifacts)]
        if len(ids) != len(set(ids)):
            raise MonthlyStageAdapterError("stage artifact ids are duplicated")
        if set(self.counts) != set(TELEMETRY_COUNT_FIELDS):
            raise MonthlyStageAdapterError("stage telemetry fields differ")
        if any(type(value) is not int or value < 0 for value in self.counts.values()):
            raise MonthlyStageAdapterError("stage telemetry must be non-negative integers")
        if any(not isinstance(item, Mapping) or not str(item.get("code") or "") for item in self.errors):
            raise MonthlyStageAdapterError("stage errors must be typed objects")


class MonthlyStageAdapter(Protocol):
    stage: str
    adapter_id: str
    adapter_version: str
    contract_sha256: str

    def execute(self, context: ProducerContext) -> MonthlyStageResult: ...


@dataclass(frozen=True, slots=True)
class CodeOwnedMonthlyStageProducer:
    """Convert one trusted adapter result into the common evidence envelope."""

    adapter: MonthlyStageAdapter

    def __post_init__(self) -> None:
        if self.adapter.stage not in STAGES:
            raise MonthlyStageAdapterError("adapter stage is not registered")
        if not self.adapter.adapter_id.strip() or not self.adapter.adapter_version.strip():
            raise MonthlyStageAdapterError("adapter identity is incomplete")
        ensure_sha256(self.adapter.contract_sha256, field="adapter.contract_sha256")

    @property
    def producer_id(self) -> str:
        return self.adapter.adapter_id

    @property
    def producer_version(self) -> str:
        return self.adapter.adapter_version

    def contract_identity(self) -> Mapping[str, Any]:
        return {
            "id": self.producer_id,
            "version": self.producer_version,
            "stage": self.adapter.stage,
            "contract_sha256": self.adapter.contract_sha256,
            "execution_mode": "in_process_code_owned",
        }

    def produce(self, context: ProducerContext) -> Mapping[str, Any]:
        if context.stage != self.adapter.stage:
            raise MonthlyStageAdapterError("adapter was invoked for another stage")
        result = self.adapter.execute(context)
        return {
            "schema_version": PRODUCER_EVIDENCE_SCHEMA,
            "scope": dict(result.scope),
            "input_artifacts": [item.external_ref() for item in result.input_artifacts],
            "output_artifacts": [item.external_ref() for item in result.output_artifacts],
            "counts": {field: int(result.counts[field]) for field in TELEMETRY_COUNT_FIELDS},
            "errors": [dict(item) for item in result.errors],
        }


def code_owned_producers(
    adapters: Sequence[MonthlyStageAdapter],
) -> Mapping[str, CodeOwnedMonthlyStageProducer]:
    by_stage: dict[str, CodeOwnedMonthlyStageProducer] = {}
    for adapter in adapters:
        if adapter.stage in by_stage:
            raise MonthlyStageAdapterError("adapter stage is duplicated")
        by_stage[adapter.stage] = CodeOwnedMonthlyStageProducer(adapter)
    if set(by_stage) != set(STAGES):
        raise MonthlyStageAdapterError("adapter set must cover every monthly stage exactly")
    return {stage: by_stage[stage] for stage in STAGES}


__all__: Sequence[str] = (
    "CodeOwnedMonthlyStageProducer",
    "MonthlyStageAdapter",
    "MonthlyStageAdapterError",
    "MonthlyStageArtifact",
    "MonthlyStageResult",
    "code_owned_producers",
)
