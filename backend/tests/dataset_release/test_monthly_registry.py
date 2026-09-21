from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import pytest

from backend.services.dataset_release.monthly_registry import (
    OFFICIAL_REGISTRY_SCHEMA,
    MonthlyProducerRegistryError,
    OfficialMonthlyProducerRegistry,
)
from backend.services.dataset_release.monthly_worker import ProducerContext


@dataclass(frozen=True)
class Producer:
    producer_id: str
    producer_version: str = "1"

    def produce(self, _context: ProducerContext) -> Mapping[str, Any]:
        raise AssertionError("not executed")


def _registry() -> OfficialMonthlyProducerRegistry:
    return OfficialMonthlyProducerRegistry(
        source=Producer("official.source"),
        build=Producer("official.build"),
        derive=Producer("official.derive"),
        local_validate=Producer("official.local-validate"),
        deploy=Producer("official.deploy"),
        consumer_validate=Producer("official.consumer-validate"),
    )


def test_registry_is_exact_versioned_and_deterministic(tmp_path: Path) -> None:
    first = _registry()
    second = _registry()
    contract = first.contract()
    assert contract["schema_version"] == OFFICIAL_REGISTRY_SCHEMA
    assert contract == second.contract()
    assert len(contract["registry_sha256"]) == 64
    pipeline = first.pipeline(artifact_roots=(tmp_path,))
    assert set(pipeline.producers) == {
        "SOURCE",
        "BUILD",
        "DERIVE",
        "LOCAL_VALIDATE",
        "DEPLOY",
        "CONSUMER_VALIDATE",
    }


def test_registry_rejects_reused_generic_producer_identity() -> None:
    same = Producer("generic.shell")
    with pytest.raises(MonthlyProducerRegistryError, match="stage-unique"):
        OfficialMonthlyProducerRegistry(
            source=same,
            build=same,
            derive=same,
            local_validate=same,
            deploy=same,
            consumer_validate=same,
        )


def test_registry_digest_binds_contract_identity() -> None:
    before = _registry().contract()["registry_sha256"]
    changed = OfficialMonthlyProducerRegistry(
        source=Producer("official.source", "2"),
        build=Producer("official.build"),
        derive=Producer("official.derive"),
        local_validate=Producer("official.local-validate"),
        deploy=Producer("official.deploy"),
        consumer_validate=Producer("official.consumer-validate"),
    ).contract()["registry_sha256"]
    assert changed != before
