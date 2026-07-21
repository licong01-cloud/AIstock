from __future__ import annotations

from pathlib import Path

from backend.services.strategy_package import live_inference
from backend.services.strategy_package.live_inference import WslStrategyPackageInferenceProvider


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
