from __future__ import annotations

from backend.inference_engine import InferenceEngine
from backend.services.strategy_package.live_inference import WslStrategyPackageInferenceProvider


def test_inference_engine_current_defaults_preserve_writes_ensure_fallback_and_cache() -> None:
    engine = object.__new__(InferenceEngine)
    captured = {}

    def fake_impl(**kwargs):  # noqa: ANN003, ANN202
        captured.update(kwargs)
        return kwargs

    engine._run_inference_impl = fake_impl  # type: ignore[method-assign]
    result = engine.run_inference()

    assert result["persist_signals"] is True
    assert result["universe_ensure"] is True
    assert result["allow_external_market_fallback"] is True
    assert result["use_selection_data_cache"] is True
    assert result["receipt_admissibility"] == "PROSPECTIVE_FIRST_OBSERVED"
    assert result["diagnostic_output_path"] is None
    assert captured == result


def test_inference_engine_historical_policy_is_forwarded_without_silent_override() -> None:
    engine = object.__new__(InferenceEngine)
    engine._run_inference_impl = lambda **kwargs: kwargs  # type: ignore[method-assign]

    result = engine.run_inference(
        persist_signals=False,
        universe_ensure=False,
        allow_external_market_fallback=False,
        use_selection_data_cache=False,
        receipt_admissibility="RETROSPECTIVE_DB_CONTENT_HASH",
        diagnostic_output_path="C:/task/diagnostics/qe.txt",
    )

    assert result["persist_signals"] is False
    assert result["universe_ensure"] is False
    assert result["allow_external_market_fallback"] is False
    assert result["use_selection_data_cache"] is False
    assert result["receipt_admissibility"] == "RETROSPECTIVE_DB_CONTENT_HASH"
    assert result["diagnostic_output_path"] == "C:/task/diagnostics/qe.txt"


def test_wsl_historical_environment_enforces_postgres_read_only_session() -> None:
    provider = WslStrategyPackageInferenceProvider()

    current = provider._build_env_exports()
    historical = provider._build_env_exports(historical_read_only=True)

    assert "PGOPTIONS" not in current
    assert "PGOPTIONS='-c default_transaction_read_only=on'" in historical
    assert provider.backend_name == "wsl"
