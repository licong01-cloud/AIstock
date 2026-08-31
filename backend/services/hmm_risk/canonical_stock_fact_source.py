"""Generation-bound online stock-fact access for canonical PIT consumers.

The legacy/offline preparation scripts continue to construct
``PostgresStockFactReader`` explicitly.  New online callers use this adapter so
the rolling universe key and activation generation come only from the W1
authority resolver.  Frozen training/prediction modules do not import this
module and therefore cannot fall back to PostgreSQL for missing candidate data.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Iterator, Mapping

from backend.services.canonical_equity_pit import (
    CanonicalPitAuthorityResolver,
    CanonicalPitContractError,
    PitConsumerBinding,
    require_canonical_consumer_binding,
    require_canonical_rolling_universe_key,
)

from .state_model_set import StateModelSetError
from .stock_fact_repository import PostgresStockFactReader, StockFactSourceSpec


ONLINE_STOCK_FACT_CONSUMER = "hmm_risk.online_stock_facts"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True, slots=True)
class CanonicalStockFactLease:
    authority_id: str
    universe_key: str
    rule_version: str
    rule_parameters_digest: str
    activation_generation: int
    activation_envelope_digest: str
    expected_source_commit: str

    def __post_init__(self) -> None:
        if (
            not self.authority_id
            or not self.universe_key
            or not self.rule_version
            or not _SHA256_RE.fullmatch(self.rule_parameters_digest)
            or not isinstance(self.activation_generation, int)
            or isinstance(self.activation_generation, bool)
            or self.activation_generation < 1
            or not _SHA256_RE.fullmatch(self.activation_envelope_digest)
            or not _GIT_SHA_RE.fullmatch(self.expected_source_commit)
        ):
            raise StateModelSetError("hmm_risk_canonical_stock_fact_generation_identity_incomplete")

    @classmethod
    def from_binding(cls, binding: PitConsumerBinding) -> "CanonicalStockFactLease":
        try:
            validated = require_canonical_consumer_binding(
                binding,
                consumer=ONLINE_STOCK_FACT_CONSUMER,
            )
            require_canonical_rolling_universe_key(validated.universe_key)
        except CanonicalPitContractError as exc:
            raise StateModelSetError(f"hmm_risk_canonical_stock_fact_binding_invalid: {exc}") from exc
        generation = validated.activation_generation
        envelope = str(validated.activation_envelope_digest or "").strip()
        source_commit = str(validated.expected_source_commit or "").strip()
        return cls(
            authority_id=validated.authority_id,
            universe_key=validated.universe_key,
            rule_version=validated.rule_version,
            rule_parameters_digest=validated.rule_parameters_digest,
            activation_generation=generation,
            activation_envelope_digest=envelope,
            expected_source_commit=source_commit,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "authority_id": self.authority_id,
            "universe_key": self.universe_key,
            "rule_version": self.rule_version,
            "rule_parameters_digest": self.rule_parameters_digest,
            "activation_generation": self.activation_generation,
            "activation_envelope_digest": self.activation_envelope_digest,
            "expected_source_commit": self.expected_source_commit,
        }


class CanonicalRollingStockFactSource:
    """Opt-in online reader pinned to one canonical authority generation."""

    def __init__(
        self,
        conn: Any,
        *,
        source_start: date,
        source_end: date,
        security_identity_manifest: Any,
        provider_absence_manifest: Any,
        authority_resolver: CanonicalPitAuthorityResolver | None = None,
        reader_factory: Callable[..., PostgresStockFactReader] = PostgresStockFactReader,
    ) -> None:
        self._authority_resolver = authority_resolver or CanonicalPitAuthorityResolver()
        self._lease = self._resolve_lease()
        spec = StockFactSourceSpec(
            universe_key=self._lease.universe_key,
            universe_rule_version=self._lease.rule_version,
            source_start=source_start,
            source_end=source_end,
        )
        self._reader = reader_factory(
            conn,
            spec,
            security_identity_manifest=security_identity_manifest,
            provider_absence_manifest=provider_absence_manifest,
        )

    @property
    def lease(self) -> CanonicalStockFactLease:
        return self._lease

    def _resolve_lease(self) -> CanonicalStockFactLease:
        try:
            binding = self._authority_resolver.resolve_live_binding()
        except CanonicalPitContractError as exc:
            raise StateModelSetError(f"hmm_risk_canonical_stock_fact_authority_unavailable: {exc}") from exc
        except Exception as exc:
            raise StateModelSetError(
                "hmm_risk_canonical_stock_fact_authority_unavailable: "
                f"error_type={type(exc).__name__}"
            ) from exc
        return CanonicalStockFactLease.from_binding(binding)

    def assert_generation_current(self) -> CanonicalStockFactLease:
        current = self._resolve_lease()
        if current != self._lease:
            raise StateModelSetError(
                "hmm_risk_canonical_stock_fact_generation_drift: "
                f"expected_generation={self._lease.activation_generation} "
                f"actual_generation={current.activation_generation} identity_changed=true"
            )
        return current

    def validate_source(self) -> dict[str, Any]:
        self.assert_generation_current()
        result = dict(self._reader.validate_source())
        self.assert_generation_current()
        return {**result, "canonical_pit_authority": self._lease.as_dict()}

    def iter_stock_fact_rows(self, *args: Any, **kwargs: Any) -> Iterator[dict[str, Any]]:
        self.assert_generation_current()
        for row in self._reader.iter_stock_fact_rows(*args, **kwargs):
            yield {**dict(row), "canonical_pit_authority": self._lease.as_dict()}
        self.assert_generation_current()

    def iter_missing_price_rows(self, *args: Any, **kwargs: Any) -> Iterator[dict[str, Any]]:
        self.assert_generation_current()
        for row in self._reader.iter_missing_price_rows(*args, **kwargs):
            yield {**dict(row), "canonical_pit_authority": self._lease.as_dict()}
        self.assert_generation_current()

    def iter_mapping_source_rows(self, *args: Any, **kwargs: Any) -> Iterator[dict[str, Any]]:
        self.assert_generation_current()
        for row in self._reader.iter_mapping_source_rows(*args, **kwargs):
            yield {**dict(row), "canonical_pit_authority": self._lease.as_dict()}
        self.assert_generation_current()


def require_same_stock_fact_lease(
    expected: CanonicalStockFactLease | Mapping[str, Any],
    actual: CanonicalStockFactLease | Mapping[str, Any],
) -> CanonicalStockFactLease:
    try:
        left = (
            expected
            if isinstance(expected, CanonicalStockFactLease)
            else CanonicalStockFactLease(**dict(expected))
        )
        right = (
            actual
            if isinstance(actual, CanonicalStockFactLease)
            else CanonicalStockFactLease(**dict(actual))
        )
    except (TypeError, ValueError) as exc:
        raise StateModelSetError("hmm_risk_canonical_stock_fact_lease_invalid") from exc
    if left != right:
        raise StateModelSetError("hmm_risk_canonical_stock_fact_generation_drift")
    return left


__all__ = [
    "CanonicalRollingStockFactSource",
    "CanonicalStockFactLease",
    "ONLINE_STOCK_FACT_CONSUMER",
    "require_same_stock_fact_lease",
]
