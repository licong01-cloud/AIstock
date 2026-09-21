"""Code-owned producer registration for unified monthly releases.

The public API and CLI are intentionally unable to choose executable commands,
producer subsets, or artifact roots.  A production composition root must build
all six producers in Python and register them as one immutable set.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Any, Mapping, Sequence

from .canonical import canonical_json_bytes
from .monthly_unified import STAGES
from .monthly_worker import RegisteredMonthlyPipeline, StageProducer


OFFICIAL_REGISTRY_SCHEMA = "aistock_monthly_release_producer_registry_v1"


class MonthlyProducerRegistryError(ValueError):
    """The code-owned monthly producer set is incomplete or ambiguous."""


def _producer_identity(producer: StageProducer) -> Mapping[str, Any]:
    reader = getattr(producer, "contract_identity", None)
    if callable(reader):
        value = reader()
        if not isinstance(value, Mapping):
            raise MonthlyProducerRegistryError("producer contract identity must be an object")
        return dict(value)
    producer_id = str(getattr(producer, "producer_id", "") or "").strip()
    producer_version = str(getattr(producer, "producer_version", "") or "").strip()
    if not producer_id or not producer_version:
        raise MonthlyProducerRegistryError("producer identity is incomplete")
    return {"id": producer_id, "version": producer_version}


@dataclass(frozen=True, slots=True)
class OfficialMonthlyProducerRegistry:
    """Exact six-stage registry assembled only by trusted application code."""

    source: StageProducer
    build: StageProducer
    derive: StageProducer
    local_validate: StageProducer
    deploy: StageProducer
    consumer_validate: StageProducer

    def as_mapping(self) -> Mapping[str, StageProducer]:
        return {
            "SOURCE": self.source,
            "BUILD": self.build,
            "DERIVE": self.derive,
            "LOCAL_VALIDATE": self.local_validate,
            "DEPLOY": self.deploy,
            "CONSUMER_VALIDATE": self.consumer_validate,
        }

    def __post_init__(self) -> None:
        producers = self.as_mapping()
        if tuple(producers) != STAGES:
            raise MonthlyProducerRegistryError("producer registry stage order differs")
        identities = {stage: _producer_identity(producer) for stage, producer in producers.items()}
        ids = [str(identity.get("id") or "") for identity in identities.values()]
        if any(not value for value in ids) or len(set(ids)) != len(ids):
            raise MonthlyProducerRegistryError("producer ids must be non-empty and stage-unique")

    def contract(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "schema_version": OFFICIAL_REGISTRY_SCHEMA,
            "stages": {
                stage: _producer_identity(producer)
                for stage, producer in self.as_mapping().items()
            },
        }
        value["registry_sha256"] = hashlib.sha256(canonical_json_bytes(value)).hexdigest()
        return value

    def pipeline(self, *, artifact_roots: Sequence[Path]) -> RegisteredMonthlyPipeline:
        return RegisteredMonthlyPipeline(self.as_mapping(), artifact_roots=artifact_roots)


__all__ = (
    "MonthlyProducerRegistryError",
    "OFFICIAL_REGISTRY_SCHEMA",
    "OfficialMonthlyProducerRegistry",
)
