from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.strategy_package import live_inference
from backend.services.strategy_package.live_inference import (
    QEExperimentRuntimeAssetResolver,
    WslStrategyPackageInferenceProvider,
)
from backend.services.trading_core.errors import DataUnavailableError


def test_wsl_provider_exports_only_explicit_safe_artifact_roots(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    first = tmp_path / "first-runtime"
    second = tmp_path / "second-runtime"
    monkeypatch.setattr(
        live_inference,
        "win_to_wsl_path",
        lambda value: f"/translated/{Path(value).name}",
    )

    provider = WslStrategyPackageInferenceProvider(
        repo_root=tmp_path,
        safe_artifact_roots=(first, second),
    )
    default_provider = WslStrategyPackageInferenceProvider(repo_root=tmp_path)

    exports = provider._build_env_exports(historical_read_only=True)
    default_exports = default_provider._build_env_exports(historical_read_only=True)

    assert (
        "AISTOCK_STRATEGY_PACKAGE_RUNTIME_ROOTS='/translated/first-runtime:/translated/second-runtime'"
        in exports
    )
    assert "PGOPTIONS='-c default_transaction_read_only=on'" in exports
    assert "AISTOCK_STRATEGY_PACKAGE_RUNTIME_ROOTS" not in default_exports


def test_package_owned_runtime_sources_keep_historical_date_namespaces_isolated(monkeypatch) -> None:  # noqa: ANN001
    resolver = QEExperimentRuntimeAssetResolver.__new__(QEExperimentRuntimeAssetResolver)
    calls: list[dict] = []

    def _capture(_manifest, **kwargs):  # noqa: ANN001, ANN202
        calls.append(kwargs)
        return SimpleNamespace(cache_namespace=kwargs.get("cache_namespace"))

    monkeypatch.setattr(resolver, "_source_from_package_assets", _capture)
    manifest = SimpleNamespace()

    single = resolver.load_frozen_source_for_strategy_package(
        manifest=manifest,
        package_id="pkg-single",
        cache_namespace="historical_2026-06-16",
    )
    multi = resolver.load_source_for_strategy_package_leg(
        manifest=manifest,
        package_id="pkg-multi",
        leg_id="alpha-leg",
        model_asset=SimpleNamespace(),
        factor_set=[],
        runtime_assets=None,
        cache_namespace="historical_2026-06-17",
    )

    assert single.cache_namespace == "historical_2026-06-16"
    assert multi.cache_namespace == "leg_alpha-leg__historical_2026-06-17"
    assert calls[0]["cache_namespace"] != calls[1]["cache_namespace"]


def test_wsl_provider_exposes_stable_inference_failure_reason(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        live_inference.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1,
            stdout="factor preparation started",
            stderr="strict feature completeness failed",
        ),
    )
    provider = WslStrategyPackageInferenceProvider(repo_root=tmp_path)

    with pytest.raises(DataUnavailableError) as exc_info:
        provider.run(
            workspace=SimpleNamespace(workspace_path=tmp_path / "workspace"),
            trade_date=date(2026, 6, 16),
            cutoff_date=date(2026, 6, 16),
            historical_read_only=True,
        )

    assert exc_info.value.context["reason_code"] == "strategy_package_wsl_inference_failed"
    assert exc_info.value.context["returncode"] == 1
