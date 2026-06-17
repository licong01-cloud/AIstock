from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.quantevolver.factor_value_loader import FactorValueLoader


def test_factor_value_loader_requires_explicit_source() -> None:
    with pytest.raises(ValueError, match="source must be explicit"):
        FactorValueLoader()


def test_factor_analyst_uses_official_single_loader(monkeypatch) -> None:
    from backend.services.quantevolver import factor_analyst as analyst_mod

    captured: dict[str, str] = {}

    class FakeLoader:
        def __init__(self, *, source: str, pipeline_dir: str):
            captured["source"] = source
            captured["pipeline_dir"] = pipeline_dir

    monkeypatch.setattr(analyst_mod, "FactorValueLoader", FakeLoader)

    loader = analyst_mod._official_factor_value_loader()

    assert isinstance(loader, FakeLoader)
    assert captured["source"] == "single"
    assert captured["pipeline_dir"].replace("\\", "/").endswith("rdagent_assets/factor_values")


def test_factor_analyst_no_legacy_correlation_writer_or_naked_loader() -> None:
    source = Path("backend/services/quantevolver/factor_analyst.py").read_text(encoding="utf-8")

    assert "FactorValueLoader()" not in source
    assert "def _estimate_by_category" not in source
    assert "category_estimation" not in source
    assert "def _upsert_correlation" not in source
    assert "INSERT INTO qe_factor_correlations" not in source
    assert "save_hdf5=False" in source


def test_correlation_router_loader_defaults_to_official_single() -> None:
    source = Path("backend/routers/quantevolver_evolution.py").read_text(encoding="utf-8")

    assert 'def _get_loader(source: str = "single")' in source
    assert 'def _get_loader(source: str = "auto")' not in source
    assert "SELECT factor_name, source, is_sota_factor, is_available,\n                               code_text, asset_path" in source
    assert "realtime_code_text, asset_path, qe_code_path" not in source
    assert 'for path_key in ["qe_code_path", "asset_path"]' not in source


def test_correlation_llm_agent_uses_official_source_only() -> None:
    source = Path("backend/services/quantevolver/correlation_llm_agent.py").read_text(encoding="utf-8")

    assert "SELECT code_text, asset_path" in source
    assert "realtime_code_text" not in source
    assert "qe_code_path" not in source


def test_correlation_service_error_text_does_not_require_transformed_code() -> None:
    source = Path("backend/services/quantevolver/correlation_compute_service.py").read_text(encoding="utf-8")

    assert "transformation_status=SUCCESS 且 qe_code_path" not in source


def test_factor_code_transformer_is_live_only() -> None:
    from backend.services.quantevolver.factor_code_transformer import (
        FactorCodeTransformer,
        NON_OFFICIAL_LIVE_TRANSFORMATION_CONTEXT,
    )

    with pytest.raises(ValueError, match="live-only"):
        FactorCodeTransformer(usage_context="official_factor_cache")

    transformer = FactorCodeTransformer(usage_context=NON_OFFICIAL_LIVE_TRANSFORMATION_CONTEXT)
    assert transformer.usage_context == NON_OFFICIAL_LIVE_TRANSFORMATION_CONTEXT


def test_backfill_factor_cache_main_is_retired() -> None:
    from scripts import backfill_factor_cache

    with pytest.raises(SystemExit, match="official_factor_full_compute"):
        backfill_factor_cache.main()
