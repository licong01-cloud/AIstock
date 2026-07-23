from __future__ import annotations

from backend.services.advisory_historical_range.semantics import (
    LIST_SEMANTICS_VERSION_V2,
    canonical_list_semantics_v2,
)


def test_r3_list_semantics_hash_is_recomputable_and_not_caller_supplied() -> None:
    first = canonical_list_semantics_v2()
    second = canonical_list_semantics_v2()

    assert first.schema_version == LIST_SEMANTICS_VERSION_V2
    assert first.semantics_hash == second.semantics_hash
    assert len(first.semantics_hash) == 64
