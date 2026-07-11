from __future__ import annotations

import json

import pandas as pd

from backend.data_service.moneyflow_contract import MONEYFLOW_UNIT_CONTRACT_VERSION
from backend.services.quantevolver.factor_value_loader import FactorValueLoader
from backend.services.quantevolver.official_factor_batch_compute_service import (
    OFFICIAL_CACHE_SCHEMA_VERSION,
)


def _write_cache(root, *, contract_version=None) -> None:
    single = root / "single"
    single.mkdir(parents=True)
    index = pd.MultiIndex.from_product(
        [[pd.Timestamp("2026-06-30")], ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    pd.DataFrame({"value": [1.0]}, index=index).to_parquet(single / "demo.parquet")
    meta = {
        "as_of_date": "2026-06-30",
        "data_start": "2026-06-30",
        "data_end": "2026-06-30",
        "factors": {
            "demo": {
                "as_of_date": "2026-06-30",
                "date_range": "2026-06-30~2026-06-30",
            }
        },
    }
    if contract_version is not None:
        meta["moneyflow_unit_contract_version"] = contract_version
    (root / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")


def test_official_cache_rejects_missing_moneyflow_contract(tmp_path) -> None:
    root = tmp_path / "factor_values"
    _write_cache(root)
    loader = FactorValueLoader(source="single", pipeline_dir=str(root))

    receipt = loader.validate_official_cache_window_hit(
        ["demo"], "2026-06-30", "2026-06-30", expected_as_of_date="2026-06-30"
    )

    assert receipt["official_cache_hit"] is False
    assert receipt["miss_reasons"]["data_contract_mismatch"] == ["demo"]


def test_official_cache_accepts_current_moneyflow_contract(tmp_path) -> None:
    root = tmp_path / "factor_values"
    _write_cache(root, contract_version=MONEYFLOW_UNIT_CONTRACT_VERSION)
    loader = FactorValueLoader(source="single", pipeline_dir=str(root))

    receipt = loader.validate_official_cache_window_hit(
        ["demo"], "2026-06-30", "2026-06-30", expected_as_of_date="2026-06-30"
    )

    assert receipt["official_cache_hit"] is True
    assert receipt["moneyflow_unit_contract_version"] == MONEYFLOW_UNIT_CONTRACT_VERSION
    assert OFFICIAL_CACHE_SCHEMA_VERSION == "official_factor_cache_v3"
