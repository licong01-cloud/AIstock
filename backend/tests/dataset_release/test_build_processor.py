from __future__ import annotations

from datetime import date

import pytest

from backend.services.dataset_release.build_processor import (
    BuildProcessorError,
    BuildStageFailed,
    _consumer_smoke_instrument,
    _release_identity,
)


def test_build_identity_is_derived_only_from_frozen_inputs() -> None:
    identity = _release_identity(
        {
            "resolved_intent_key": "d" * 64,
            "scope": "full",
            "cutoff": "2026-08-31",
            "profile": "qe_hmm_full_v2",
            "fingerprints": {
                "producer_fingerprint": "a" * 64,
                "artifact_fingerprint": "b" * 64,
            },
            "source_snapshot": {"pit_snapshot_digest": "c" * 64},
        }
    )
    assert identity.cutoff == date(2026, 8, 31)
    assert identity.scope.value == "full"
    assert identity.release_id

    with pytest.raises(BuildProcessorError, match="fingerprints/source snapshot"):
        _release_identity({})


@pytest.mark.parametrize("value", ["", "000001", "000001.BJ", "000001.SZ/escape"])
def test_consumer_smoke_instrument_fails_closed(value: str) -> None:
    with pytest.raises(BuildStageFailed, match="safe consumer smoke instrument"):
        _consumer_smoke_instrument({"consumer_smoke_instrument": value})


def test_consumer_smoke_instrument_normalizes_valid_stock_code() -> None:
    assert _consumer_smoke_instrument({"consumer_smoke_instrument": "000001.sz"}) == "000001.SZ"
