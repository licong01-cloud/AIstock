from __future__ import annotations

from datetime import date

import pytest

from backend.services.dataset_release.errors import SourceManifestError
from backend.services.dataset_release.sector_enrichment import FrozenSectorEnricher


def test_sector_enricher_uses_stable_map_and_pit_membership_switch() -> None:
    enricher = FrozenSectorEnricher.build(
        [
            {"index_code": "801020.SI", "level": "L2"},
            {"index_code": "801010.SI", "level": "L2"},
        ],
        [
            {
                "ts_code": "000001.SZ",
                "in_date": "2026-01-01",
                "out_date": "2026-01-31",
                "l2_code": "801010.SI",
            },
            {
                "ts_code": "000001.SZ",
                "in_date": "2026-02-01",
                "out_date": None,
                "l2_code": "801020.SI",
            },
        ],
    )

    assert enricher.code_map == {"801010.SI": 0, "801020.SI": 1}
    assert enricher.enrich({"ts_code": "000001.SZ", "trade_date": date(2026, 1, 31)})["l2_code_id"] == 0
    assert enricher.enrich({"ts_code": "000001.SZ", "trade_date": date(2026, 2, 1)})["l2_code_id"] == 1
    assert enricher.enrich({"ts_code": "000001.SZ", "trade_date": date(2025, 12, 31)})["l2_code_id"] == -1
    assert enricher.enrich({"ts_code": "000002.SZ", "trade_date": date(2026, 2, 1)})["l2_code_id"] == -1


def test_sector_enricher_map_digest_is_input_order_stable() -> None:
    rows = [
        {"index_code": "801010.SI", "level": "L2"},
        {"index_code": "801020.SI", "level": "L2"},
    ]
    first = FrozenSectorEnricher.build(rows, [])
    second = FrozenSectorEnricher.build(reversed(rows), [])

    assert first.code_map == second.code_map
    assert first.code_map_digest == second.code_map_digest


def test_sector_enricher_rejects_member_with_unknown_classification() -> None:
    with pytest.raises(
        SourceManifestError,
        match="unknown L2 classification",
    ):
        FrozenSectorEnricher.build(
            [{"index_code": "801010.SI", "level": "L2"}],
            [
                {
                    "ts_code": "000001.SZ",
                    "l2_code": "809999.SI",
                    "in_date": "2026-01-01",
                    "out_date": None,
                }
            ],
        )
