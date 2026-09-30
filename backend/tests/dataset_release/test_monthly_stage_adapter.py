from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from backend.services.dataset_release.monthly_stage_adapter import (
    CodeOwnedMonthlyStageProducer,
    MonthlyStageAdapterError,
    MonthlyStageArtifact,
    MonthlyStageResult,
    code_owned_producers,
)
from backend.services.dataset_release.monthly_registry import OfficialMonthlyProducerRegistry
from backend.services.dataset_release.monthly_unified import STAGES, TELEMETRY_COUNT_FIELDS
from backend.services.dataset_release.monthly_worker import ProducerContext


SHA = "a" * 64


@dataclass(frozen=True)
class Adapter:
    stage: str
    artifact: Path
    adapter_id: str
    adapter_version: str = "1"
    contract_sha256: str = SHA

    def execute(self, _context: ProducerContext) -> MonthlyStageResult:
        return MonthlyStageResult(
            scope={"stage": self.stage},
            input_artifacts=(),
            output_artifacts=(MonthlyStageArtifact(self.artifact.name, self.artifact),),
            counts={field: 0 for field in TELEMETRY_COUNT_FIELDS},
        )


def _context(stage: str) -> ProducerContext:
    return ProducerContext(
        stage=stage,
        operation_id="dmr_" + "1" * 32,
        attempt=1,
        request={},
        plan={},
        prior_receipts={},
    )


def test_code_owned_adapter_emits_common_envelope(tmp_path: Path) -> None:
    artifact = tmp_path / "receipt.json"
    artifact.write_text("{}\n", encoding="utf-8")
    producer = CodeOwnedMonthlyStageProducer(Adapter("BUILD", artifact, "official.build"))
    evidence = producer.produce(_context("BUILD"))
    assert evidence["scope"] == {"stage": "BUILD"}
    assert evidence["output_artifacts"] == [{"id": "receipt.json", "path": str(artifact)}]
    assert producer.contract_identity()["execution_mode"] == "in_process_code_owned"


def test_adapter_cannot_execute_for_another_stage(tmp_path: Path) -> None:
    artifact = tmp_path / "receipt.json"
    artifact.write_text("{}\n", encoding="utf-8")
    producer = CodeOwnedMonthlyStageProducer(Adapter("BUILD", artifact, "official.build"))
    with pytest.raises(MonthlyStageAdapterError, match="another stage"):
        producer.produce(_context("SOURCE"))


def test_adapter_set_requires_exact_six_stages(tmp_path: Path) -> None:
    artifact = tmp_path / "receipt.json"
    artifact.write_text("{}\n", encoding="utf-8")
    adapters = [Adapter(stage, artifact, f"official.{stage.lower()}") for stage in STAGES]
    assert tuple(code_owned_producers(adapters)) == STAGES
    registry = OfficialMonthlyProducerRegistry.from_adapters(adapters)
    assert tuple(registry.as_mapping()) == STAGES
    with pytest.raises(MonthlyStageAdapterError, match="every monthly stage"):
        code_owned_producers(adapters[:-1])


def test_stage_result_rejects_hidden_or_partial_telemetry(tmp_path: Path) -> None:
    artifact = MonthlyStageArtifact("receipt.json", tmp_path / "receipt.json")
    with pytest.raises(MonthlyStageAdapterError, match="telemetry fields"):
        MonthlyStageResult(
            scope={},
            input_artifacts=(),
            output_artifacts=(artifact,),
            counts={"unexplained_gap_count": 0},
        )


def test_stage_result_preserves_typed_operational_error_without_faking_gap(tmp_path: Path) -> None:
    artifact = MonthlyStageArtifact("receipt.json", tmp_path / "receipt.json")
    result = MonthlyStageResult(
        scope={},
        input_artifacts=(),
        output_artifacts=(artifact,),
        counts={field: 0 for field in TELEMETRY_COUNT_FIELDS},
        errors=({"code": "NODE_UNAVAILABLE"},),
    )
    assert result.counts["unexplained_gap_count"] == 0


def test_stage_result_rejects_untyped_error(tmp_path: Path) -> None:
    artifact = MonthlyStageArtifact("receipt.json", tmp_path / "receipt.json")
    with pytest.raises(MonthlyStageAdapterError, match="typed objects"):
        MonthlyStageResult(
            scope={},
            input_artifacts=(),
            output_artifacts=(artifact,),
            counts={field: 0 for field in TELEMETRY_COUNT_FIELDS},
            errors=({"message": "failed"},),
        )
